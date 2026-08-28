use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Datelike, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::America::New_York;
use chrono_tz::Tz;
use ibapi::{Client, prelude::Contract};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::PgPool;
use yfinance_rs::NewsTab;

// NOTE: adjust this import path if EwmMean doesn't actually live alongside
// RollingMean/RollingStd/RollingDayVwap in your crate.
use crate::strategy::helpers::rolling_fn::{EwmMean, RollingDayVwap, RollingMean, RollingStd};
use crate::strategy::strategy::StrategyDetails;
use crate::{
    database::{
        crud::CRUDTrait,
        models::{
            AssetType, HistoricalStockDataPrimaryKeysWoTime, TargetStockPositionsPrimaryKeys,
            TargetStockPositionsUpdateKeys,
        },
        models_crud::{
            historical_data::historical_data::{
                HistoricalDataCRUD, HistoricalDataFullKeys, HistoricalDataOps,
                HistoricalDataPrimaryKeysWoTime, NoiseOps, VwapBarValue,
            },
            target_positions::target_positions::{
                TargetPositionsCRUD, TargetPositionsPrimaryKeys, TargetPositionsUpdateKeys,
            },
        },
    },
    market_data::{consolidator::Consolidator, traits::strategy_value::GetStrategyValue},
    strategy::{
        portfolio_functions::proportional_integer_reduce,
        strategy::{BarUpdateOutcome, StrategyExecutor},
    },
};

const NUM_BARS_PER_DAY: usize = 78;

// ===== Refinement tuning constants — kept here so they're easy to find/tweak
// without hunting through the trading logic below. =====
/// Refinement #3: how many neighboring 5-min buckets on each side to blend
/// into the diurnal avg-move estimate (triangular kernel weighting).
const DIURNAL_SMOOTH_RADIUS: usize = 2;
/// Refinement #4: percentile (0.0-1.0) of the rolling vol-history window used
/// as the high/low-vol cutoff, replacing the hardcoded 0.04 constant.
const DYNAMIC_VOL_PERCENTILE: f64 = 0.66;
/// Refinement #4: how many trailing daily vol readings to keep for the
/// percentile calc. Also the minimum sample size before it's trusted — below
/// this it falls back to the hardcoded/backtest-param threshold.
const VOL_HISTORY_CAP: usize = 60;
const VOL_HISTORY_MIN_SAMPLES: usize = 10;

/// Runtime toggles for the variance/momentum refinements. All default to
/// `false`, which reproduces the original strategy's behavior exactly.
/// Flip one at a time across backtest runs to A/B each independently —
/// these are read at both warm-up and on_bar_update time, so set them
/// once via `Noise::with_refinements` before the strategy starts running
/// (flipping mid-run works but the newly-enabled series won't have history
/// until enough new days accumulate).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoiseRefinements {
    /// #1: feed the daily vol estimate from intraday realized variance
    /// (sum of squared 5-min log returns within the session) instead of the
    /// single close/open ratio per day.
    pub realized_vol: bool,
    /// #2: aggregate the chosen vol series with an EWMA instead of a flat
    /// rolling window — reacts to a vol regime shift immediately instead of
    /// waiting for the old low-vol day to roll off the window.
    pub ewma_vol: bool,
    /// #3: smooth the diurnal avg-move-since-open curve across neighboring
    /// time buckets instead of treating each of the 78 slots as an
    /// independent estimate (each bucket only gets ~avg_move_lookback
    /// samples on its own).
    pub smooth_diurnal: bool,
    /// #4: replace the hardcoded `daily_vol_threshold` (0.04) with a rolling
    /// percentile of recent daily vol readings, so the high/low-vol cutoff
    /// self-adjusts as the regime drifts over quarters/years.
    pub dynamic_vol_threshold: bool,
}

impl NoiseRefinements {
    pub fn all_on() -> Self {
        Self {
            realized_vol: true,
            ewma_vol: false,
            smooth_diurnal: true,
            dynamic_vol_threshold: true,
        }
    }
}

#[derive(Debug)]
pub struct NoiseFnData {
    // most recent 5 minute bar with time == 9:30
    most_recent_day_bar: Option<(HistoricalDataFullKeys, HistoricalDataFullKeys)>,
    last_bar: Option<HistoricalDataFullKeys>,

    day_vwap: RollingDayVwap,
    daily_volatility: RollingStd,
    avg_moves: HashMap<NaiveTime, RollingMean>,
    avg_move_lookback: usize,

    // ===== Refinement #1: realized variance =====
    /// Last intraday price seen today, used to compute the next log return.
    intraday_last_price: Option<f64>,
    /// Running sum of squared log returns for the *current, in-progress* day.
    /// Finalized into `realized_variance` at the next day boundary.
    intraday_sq_log_ret_sum: f64,
    /// Rolling mean of *completed* daily realized-variance readings — one
    /// push per day, taken from `intraday_sq_log_ret_sum` at the day roll.
    realized_variance: RollingMean,

    // ===== Refinement #2: EWMA vol =====
    /// EWMA of squared daily deviations from the legacy close/open ratio
    /// series — i.e. an EWMA *variance* parallel to `daily_volatility`.
    ewma_vol_legacy: EwmMean,
    /// EWMA of daily realized variance — parallel to `realized_variance`.
    ewma_vol_realized: EwmMean,

    // ===== Refinement #4: dynamic vol threshold =====
    /// Bounded history of daily vol (std, not variance) readings, whichever
    /// series is currently active per refinements #1/#2, used to derive a
    /// rolling percentile cutoff instead of the fixed 0.04 constant.
    vol_history: VecDeque<f64>,
}

impl NoiseFnData {
    fn push(&mut self, bar: HistoricalDataFullKeys, refinements: &NoiseRefinements) {
        self.day_vwap.push(&bar);
        let bar_time = bar.get_time().with_timezone(&New_York).time();
        let is_new_day =
            (self.most_recent_day_bar.is_none() && bar_time.hour() == 9 && bar_time.minute() == 30)
                || self.most_recent_day_bar.as_ref().is_some_and(|bars| {
                    bar.get_time().with_timezone(&New_York).date_naive()
                        != bars.1.get_time().with_timezone(&New_York).date_naive()
                });

        if is_new_day {
            if let Some(last_bar) = &self.last_bar {
                if let Some(most_recent_day_bars) = &self.most_recent_day_bar {
                    let legacy_ratio =
                        last_bar.get_price() / most_recent_day_bars.1.get_open_price();
                    self.daily_volatility.push(legacy_ratio);

                    // ---- Refinement #1: finalize prior day's realized variance ----
                    if refinements.realized_vol {
                        self.realized_variance.push(self.intraday_sq_log_ret_sum);
                    }

                    // ---- Refinement #2: finalize EWMA vol for whichever series applies ----
                    if refinements.ewma_vol {
                        let squared_dev = (legacy_ratio - 1.0).powi(2);
                        self.ewma_vol_legacy.push(squared_dev);
                        self.ewma_vol_realized.push(self.intraday_sq_log_ret_sum);
                    }

                    // ---- Refinement #4: record the day's vol reading for the percentile history ----
                    if refinements.dynamic_vol_threshold {
                        if let Some(v) = self.current_daily_vol(refinements) {
                            self.vol_history.push_back(v);
                            if self.vol_history.len() > VOL_HISTORY_CAP {
                                self.vol_history.pop_front();
                            }
                        }
                    }
                }
                self.most_recent_day_bar = Some((last_bar.clone(), bar.clone()));
            }
            // Reset the intraday accumulator for the day that's starting now.
            // Harmless no-op cost when refinement #1 is off.
            self.intraday_sq_log_ret_sum = 0.0;
            self.intraday_last_price = Some(bar.get_price());
            self.last_bar = Some(bar);
            return;
        }

        // ---- Refinement #1: accumulate today's squared log return ----
        if refinements.realized_vol {
            if let Some(prev_price) = self.intraday_last_price {
                let log_ret = (bar.get_price() / prev_price).ln();
                self.intraday_sq_log_ret_sum += log_ret * log_ret;
            }
            self.intraday_last_price = Some(bar.get_price());
        }

        if let Some(day_bars) = &self.most_recent_day_bar {
            let day_open = day_bars.1.get_price();
            self.avg_moves
                .entry(bar.get_time().with_timezone(&New_York).time())
                .and_modify(|rolling_mean| {
                    let movement_since_open = (bar.get_price() / day_open - 1.0).abs();
                    rolling_mean.push(movement_since_open);
                })
                .or_insert_with(|| {
                    let movement_since_open = (bar.get_price() / day_open - 1.0).abs();
                    let mut mean = RollingMean::new(self.avg_move_lookback);
                    mean.push(movement_since_open);
                    mean
                });
        }
        self.last_bar = Some(bar);
    }

    /// Resolves "today's daily vol" (a standard deviation, not a variance —
    /// comparable across all four refinement combinations) according to
    /// which of #1/#2 are active:
    ///   (off, off)  -> original: RollingStd of the close/open ratio
    ///   (off, on)   -> EWMA variance of the legacy ratio's deviation, sqrt'd
    ///   (on,  off)  -> RollingMean of daily realized variance, sqrt'd
    ///   (on,  on)   -> EWMA of daily realized variance, sqrt'd
    fn current_daily_vol(&self, refinements: &NoiseRefinements) -> Option<f64> {
        match (refinements.realized_vol, refinements.ewma_vol) {
            (false, false) => self.daily_volatility.rolling_std(),
            (false, true) => self.ewma_vol_legacy.value().map(f64::sqrt),
            (true, false) => self.realized_variance.rolling_mean().map(f64::sqrt),
            (true, true) => self.ewma_vol_realized.value().map(f64::sqrt),
        }
    }

    /// Refinement #3: kernel-smoothed diurnal avg-move estimate. Averages
    /// the raw `rolling_mean()` of the `radius` nearest time buckets on each
    /// side (in chronological order within the day), weighted by a simple
    /// triangular kernel so nearer buckets count for more. Falls back
    /// gracefully near the open/close where fewer than `radius` neighbors
    /// exist. Returns `None` if `bar_time` isn't a known bucket at all, or
    /// if none of the buckets in range have enough lookback data yet.
    fn avg_move_smoothed(&self, bar_time: &NaiveTime, radius: usize) -> Option<f64> {
        if radius == 0 {
            return self.avg_moves.get(bar_time).and_then(|m| m.rolling_mean());
        }
        let mut times: Vec<&NaiveTime> = self.avg_moves.keys().collect();
        times.sort();
        let center = times.iter().position(|&&t| t == *bar_time)?;
        let lo = center.saturating_sub(radius);
        let hi = (center + radius).min(times.len().saturating_sub(1));

        let mut weighted_sum = 0.0;
        let mut weight_total = 0.0;
        for i in lo..=hi {
            let dist = (i as isize - center as isize).unsigned_abs();
            let weight = (radius + 1 - dist) as f64; // triangular kernel
            if let Some(v) = self.avg_moves.get(times[i]).and_then(|m| m.rolling_mean()) {
                weighted_sum += weight * v;
                weight_total += weight;
            }
        }
        (weight_total > 0.0).then_some(weighted_sum / weight_total)
    }

    /// Refinement #4: rolling percentile of recent daily vol readings.
    /// Returns `None` until `vol_history` has at least
    /// `VOL_HISTORY_MIN_SAMPLES` — caller should fall back to the
    /// hardcoded/backtest-param threshold until then.
    fn dynamic_vol_threshold(&self, percentile: f64) -> Option<f64> {
        if self.vol_history.len() < VOL_HISTORY_MIN_SAMPLES {
            return None;
        }
        let mut sorted: Vec<f64> = self.vol_history.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        let idx = ((sorted.len() as f64 - 1.0) * percentile).round() as usize;
        sorted.get(idx).copied()
    }
}

#[derive(Debug)]
pub struct Noise {
    priority: u32,
    name: String,
    pool: PgPool,
    tokio_handle: tokio::runtime::Handle,
    data: Option<NoiseFnData>,
    /// Generic backtest params (key -> value), read by cfg-gated branches in
    /// `on_bar_update`. Populated from `BacktestConfig.strategy_params["noise"]`
    /// via [`with_backtest_params`]. `None` under the default build + when no
    /// `NOISE_*` env vars are set (falls back to hardcoded values).
    #[cfg(feature = "backtest")]
    backtest_params: Option<std::collections::HashMap<String, f64>>,
    /// Additive variance/momentum refinements — all off by default, which
    /// reproduces the original strategy exactly. See [`NoiseRefinements`].
    refinements: NoiseRefinements,
}

impl Noise {
    pub fn new(pool: PgPool, tokio_handle: tokio::runtime::Handle) -> Self {
        Self {
            priority: 1,
            name: "noise".to_string(),
            pool,
            tokio_handle,
            data: None,
            #[cfg(feature = "backtest")]
            backtest_params: None,
            refinements: NoiseRefinements::all_on(),
        }
    }

    // /// Set which refinements are active. Call this before the strategy
    // /// starts consuming bars (i.e. before `warm_up_data`) so the relevant
    // /// history series are warm by the time `on_bar_update` runs.
    // pub fn with_refinements(mut self, refinements: NoiseRefinements) -> Self {
    //     self.refinements = refinements;
    //     self
    // }

    #[cfg(feature = "backtest")]
    pub fn with_backtest_params(mut self, params: std::collections::HashMap<String, f64>) -> Self {
        self.backtest_params = Some(params);
        self
    }

    #[cfg(feature = "backtest")]
    fn param(&self, name: &str, default: f64) -> f64 {
        self.backtest_params
            .as_ref()
            .and_then(|m| m.get(name))
            .copied()
            .unwrap_or(default)
    }
}

#[hotpath::measure_all]
#[async_trait::async_trait]
impl StrategyExecutor for Noise {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn get_strategy_details(&self) -> StrategyDetails {
        StrategyDetails::new(1, self.name.clone(), false)
    }

    fn on_bar_update(
        &mut self,
        contract: &Contract,
        bar: &HistoricalDataFullKeys,
        consolidator: &Arc<Consolidator>,
    ) -> Result<BarUpdateOutcome, String> {
        match self._on_bar_update(contract, bar, consolidator) {
            Ok(v) => Ok(v),
            Err(v) => Ok(v),
        }
    }

    fn get_contracts(&self, client: Arc<Client>) -> Vec<Contract> {
        let contract = Contract::stock("QQQ")
            .on_exchange("SMART")
            .primary("NASDAQ")
            .in_currency("USD")
            .build();
        #[cfg(not(feature = "backtest"))]
        let res = vec![
            Consolidator::_validate_contract(client, contract, Duration::from_secs(10))
                .expect("Expected to be able to get_contracts when init_app"),
        ];
        #[cfg(feature = "backtest")]
        let res = vec![contract];

        res
    }

    async fn warm_up_data(
        &mut self,
        consolidator: &Arc<Consolidator>,
        #[cfg(feature = "backtest")] bar_time: DateTime<Utc>,
    ) -> Result<(), String> {
        let avg_move_lookback = {
            #[cfg(feature = "backtest")]
            {
                self.param("avg_move_lookback", 15.0) as i64
            }
            #[cfg(not(feature = "backtest"))]
            {
                15_i64
            }
        };
        let vol_lookback = {
            #[cfg(feature = "backtest")]
            {
                self.param("vol_lookback", 14.0) as i64
            }
            #[cfg(not(feature = "backtest"))]
            {
                14_i64
            }
        };

        #[cfg(not(feature = "backtest"))]
        {
            let consolidator = consolidator.clone();
            let contract_opt = consolidator.validate_contract(
                Contract::stock("QQQ")
                    .on_exchange("SMART")
                    .primary("NASDAQ")
                    .in_currency("USD")
                    .build(),
                Duration::from_secs(10),
            );
            consolidator
                .update_at_least_n_days_data(
                    &contract_opt.expect("Expected QQQ contract"),
                    20,
                    true,
                )
                .await
                .map_err(|e| format!("Error in update_at_least_n_days_data: {}", e))?;
        }

        let num_days = avg_move_lookback.max(vol_lookback) as usize;
        let avg_moves = HashMap::new();
        let historical_data_crud = HistoricalDataCRUD::from(&AssetType::Stock, self.pool.clone());
        let last_n_bars = historical_data_crud
            .read_last_n(
                HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
                    stock: "QQQ".to_string(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                }),
                5,
                (NUM_BARS_PER_DAY * num_days + NUM_BARS_PER_DAY * 2) as u32,
                #[cfg(feature = "backtest")]
                None,
            )
            .await
            .map_err(|e| format!("{}", e))?;

        if !last_n_bars.incomplete.is_empty() {
            tracing::error!(
                "Fetching last N bars of timestep 5 returned incomplete bars for noise"
            );
            return Err(format!(
                "Fetching last N bars of timestep 5 returned incomplete bars for noise"
            ));
        }
        if last_n_bars.full.is_empty() {
            return Err("Failed to fetch any full bars during noise warmup".to_string());
        }

        let day_vwap = RollingDayVwap::new(78);
        let daily_volatility = RollingStd::new(vol_lookback as usize);

        let mut data = NoiseFnData {
            most_recent_day_bar: None,
            last_bar: None,
            day_vwap,
            daily_volatility,
            avg_moves,
            avg_move_lookback: avg_move_lookback as usize,
            intraday_last_price: None,
            intraday_sq_log_ret_sum: 0.0,
            realized_variance: RollingMean::new(vol_lookback as usize),
            ewma_vol_legacy: EwmMean::new(vol_lookback as usize),
            ewma_vol_realized: EwmMean::new(vol_lookback as usize),
            vol_history: VecDeque::with_capacity(VOL_HISTORY_CAP),
        };
        for bar in last_n_bars.full.into_iter() {
            data.push(bar, &self.refinements);
        }
        self.data = Some(data);

        Ok(())
    }
}

#[hotpath::measure_all]
impl Noise {
    fn _on_bar_update(
        &mut self,
        _contract: &Contract,
        bar: &HistoricalDataFullKeys,
        consolidator: &Arc<Consolidator>,
    ) -> Result<BarUpdateOutcome, BarUpdateOutcome> {
        let noise_data = self
            .data
            .as_ref()
            .expect("Expected sufficient data in noise fn warm up for on_bar_update");
        let bar_time = &bar.get_time().with_timezone(&New_York).time();
        if !noise_data.avg_moves.contains_key(bar_time) {
            if bar_time == &NaiveTime::from_hms_opt(9, 30, 0).unwrap() {
                let mut noise_data = self
                    .data
                    .as_mut()
                    .expect("Expected sufficient data in noise fn warm up for on_bar_update");
                noise_data.push(bar.clone(), &self.refinements);
                return Ok(BarUpdateOutcome::NoAction);
            } else {
                tracing::error!("avg_moves doesn't contain: {bar_time:?}");
                return Err(BarUpdateOutcome::NoAction);
            }
        }

        let (avg_move_since_open, most_recent_open_bars, most_recent_daily_vol, vwap) = (
            // ---- Refinement #3: smoothed vs raw single-bucket diurnal estimate ----
            if self.refinements.smooth_diurnal {
                match noise_data.avg_move_smoothed(bar_time, DIURNAL_SMOOTH_RADIUS) {
                    Some(v) => v,
                    None => {
                        let mut noise_data = self.data.as_mut().expect(
                            "Expected sufficient data in noise fn warm up for on_bar_update",
                        );
                        noise_data.push(bar.clone(), &self.refinements);
                        tracing::error!("Not enough smoothed data for {bar_time:?}");
                        return Err(BarUpdateOutcome::NoAction);
                    }
                }
            } else {
                match noise_data
                    .avg_moves
                    .get(bar_time)
                    .expect("Expected to be able to get avg_moves")
                    .rolling_mean()
                {
                    Some(v) => v,
                    None => {
                        let mut noise_data = self.data.as_mut().expect(
                            "Expected sufficient data in noise fn warm up for on_bar_update",
                        );
                        noise_data.push(bar.clone(), &self.refinements);
                        tracing::error!("Not enough data for {bar_time:?}");
                        return Err(BarUpdateOutcome::NoAction);
                    }
                }
            },
            noise_data
                .most_recent_day_bar
                .as_ref()
                .expect("Expected sufficient data for day bars"),
            // ---- Refinements #1/#2: resolved vol source, see current_daily_vol ----
            match noise_data.current_daily_vol(&self.refinements) {
                Some(v) => v,
                None => {
                    let mut noise_data = self
                        .data
                        .as_mut()
                        .expect("Expected sufficient data in noise fn warm up for on_bar_update");
                    noise_data.push(bar.clone(), &self.refinements);
                    return Err(BarUpdateOutcome::NoAction);
                }
            },
            noise_data
                .day_vwap
                .vwap()
                .expect("Expected sufficient data for vwap"),
        );

        let curr_available_funds = consolidator
            .get_strategy_sgd_value(&self.get_name())
            .map_err(|e| {
                tracing::error!("Failed to fetch strategy SGD value for noise: {e:?}");
                BarUpdateOutcome::NoAction
            })?;

        // The noise-band sensitivity: upper_noise = (1 + noise_multiplier *
        // avg_move) * open. Default 1.0 (the band = the avg move since open).
        let noise_multiplier = {
            #[cfg(feature = "backtest")]
            {
                self.param("noise_multiplier", 1.0)
            }
            #[cfg(not(feature = "backtest"))]
            {
                1.0
            }
        };
        // The act-on-bar interval (minutes). The strategy only places/closes
        // when `bar.minute % act_interval == 0`. Default 30 (the :00 + :30
        // bars). Must divide evenly into 60 (the bar minute is 0..59).
        let act_interval = {
            #[cfg(feature = "backtest")]
            {
                self.param("act_interval_minutes", 30.0) as u32
            }
            #[cfg(not(feature = "backtest"))]
            {
                30_u32
            }
        };

        // ---- Refinement #4: dynamic vs hardcoded high/low-vol threshold ----
        let hardcoded_vol_threshold = {
            #[cfg(feature = "backtest")]
            {
                self.param("daily_vol_threshold", 0.04)
            }
            #[cfg(not(feature = "backtest"))]
            {
                0.04
            }
        };
        let vol_threshold = if self.refinements.dynamic_vol_threshold {
            noise_data
                .dynamic_vol_threshold(DYNAMIC_VOL_PERCENTILE)
                .unwrap_or(hardcoded_vol_threshold)
        } else {
            hardcoded_vol_threshold
        };

        let ideal_qty = if most_recent_daily_vol >= vol_threshold {
            #[cfg(feature = "backtest")]
            {
                self.param("ideal_qty_high_vol", 70.0) as i64
            }
            #[cfg(not(feature = "backtest"))]
            {
                70 as i64
            }
        } else {
            #[cfg(feature = "backtest")]
            {
                self.param("ideal_qty_low_vol", 100.0) as i64
            }
            #[cfg(not(feature = "backtest"))]
            {
                100 as i64
            }
        };

        let allowable_positions_tuple = proportional_integer_reduce(
            &vec![ideal_qty],
            &vec![bar.get_high_price()],
            curr_available_funds,
        );
        let allowable_positions = allowable_positions_tuple.0.first().unwrap();
        let qty = {
            if *allowable_positions != ideal_qty {
                *allowable_positions
            } else {
                ideal_qty
            }
        } as f64;

        let most_recent_open_upper = most_recent_open_bars
            .1
            .get_open_price()
            .max(most_recent_open_bars.0.get_price());
        let most_recent_open_lower = most_recent_open_bars
            .1
            .get_open_price()
            .min(most_recent_open_bars.0.get_price());
        let (upper_noise, lower_noise) = (
            (1.0 + noise_multiplier * avg_move_since_open) * most_recent_open_upper,
            (1.0 - noise_multiplier * avg_move_since_open) * most_recent_open_lower,
        );

        let (bar_close, bar_time) = (bar.get_price(), bar.get_time().with_timezone(&New_York));

        // If (Either < upper noise, < VWAP && time to act) || (final time to trade)
        let last_time = New_York
            .with_ymd_and_hms(bar_time.year(), bar_time.month(), bar_time.day(), 15, 45, 0)
            .unwrap();
        if ((bar_close < upper_noise
            || Decimal::from_f64(bar_close)
                .expect("Expected bar_close conversion to Decimal to be ok")
                <= vwap)
            && bar_time.minute() % act_interval == 0)
            || bar_time >= last_time
        {
            let target_stock_positions_crud =
                TargetPositionsCRUD::from(&AssetType::Stock, self.pool.clone());
            let name = self.get_name();
            #[cfg(feature = "backtest")]
            {
                use crate::backtester::methods::in_memory::state::PositionKey;
                if let Some(state) = crate::backtester::methods::in_memory::state::current() {
                    state.delete_target(&PositionKey {
                        strategy: name.clone(),
                        stock: "QQQ".to_string(),
                        primary_exchange: "NASDAQ".to_string(),
                        currency: "USD".to_string(),
                    });
                    let mut noise_data = self
                        .data
                        .as_mut()
                        .expect("Expected sufficient data in noise fn warm up for on_bar_update");
                    noise_data.push(bar.clone(), &self.refinements);
                    return Ok(BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]));
                }
            }
            #[cfg(not(feature = "backtest"))]
            hotpath::measure_block!("noise_delete_target_position", {
                self.tokio_handle.block_on(async move {
                    target_stock_positions_crud
                        .delete(&TargetPositionsPrimaryKeys::Stock(
                            TargetStockPositionsPrimaryKeys {
                                strategy: name,
                                stock: "QQQ".to_string(),
                                primary_exchange: "NASDAQ".to_string(),
                                currency: "USD".to_string(),
                            },
                        ))
                        .await
                        .map_err(|e| {
                            tracing::error!("Failed to delete QQQ: {e:?}");
                            BarUpdateOutcome::NoAction
                        })
                })
            })?;
            let mut noise_data = self
                .data
                .as_mut()
                .expect("Expected sufficient data in noise fn warm up for on_bar_update");
            noise_data.push(bar.clone(), &self.refinements);
            return Ok(BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]));
        }

        if bar_close > upper_noise && bar_time.minute() % act_interval == 0 {
            let target_stock_positions_crud =
                TargetPositionsCRUD::from(&AssetType::Stock, self.pool.clone());
            let name = self.get_name();
            #[cfg(feature = "backtest")]
            {
                use crate::backtester::methods::in_memory::state::PositionKey;
                if let Some(state) = crate::backtester::methods::in_memory::state::current() {
                    state.set_target(
                        PositionKey {
                            strategy: name.clone(),
                            stock: "QQQ".to_string(),
                            primary_exchange: "NASDAQ".to_string(),
                            currency: "USD".to_string(),
                        },
                        qty,
                        0.0,
                    );
                }
            }
            #[cfg(not(feature = "backtest"))]
            hotpath::measure_block!("noise_create_or_update_target_position", {
                self.tokio_handle.block_on(async move {
                    target_stock_positions_crud
                        .create_or_update(
                            &TargetPositionsPrimaryKeys::Stock(TargetStockPositionsPrimaryKeys {
                                strategy: name,
                                primary_exchange: "NASDAQ".to_string(),
                                currency: "USD".to_string(),
                                stock: "QQQ".to_string(),
                            }),
                            &TargetPositionsUpdateKeys::Stock(TargetStockPositionsUpdateKeys {
                                avg_price: Some(0.0),
                                quantity: Some(qty),
                            }),
                        )
                        .await
                        .map_err(|e| {
                            tracing::error!("Failed to delete QQQ: {e:?}");
                            BarUpdateOutcome::NoAction
                        })
                })
            })?;
            let mut noise_data = self
                .data
                .as_mut()
                .expect("Expected sufficient data in noise fn warm up for on_bar_update");
            noise_data.push(bar.clone(), &self.refinements);
            return Ok(BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]));
        }

        let mut noise_data = self
            .data
            .as_mut()
            .expect("Expected sufficient data in noise fn warm up for on_bar_update");
        noise_data.push(bar.clone(), &self.refinements);
        return Ok(BarUpdateOutcome::NoAction);
    }
}
