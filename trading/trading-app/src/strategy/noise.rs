use std::collections::HashMap;
use std::{cmp::Ordering, sync::Arc, time::Duration};

use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use chrono_tz::America::New_York;
use chrono_tz::Tz;
use ibapi::{Client, prelude::Contract};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::PgPool;
use yfinance_rs::NewsTab;

use crate::strategy::helpers::rolling_fn::{RollingDayVwap, RollingMean, RollingStd};
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

#[derive(Debug)]
pub struct NoiseFnData {
    // most recent 5 minute bar with time == 9:30
    most_recent_day_bar: HistoricalDataFullKeys,
    last_close: f64,

    day_vwap: RollingDayVwap,
    daily_volatility: RollingStd,
    avg_moves: HashMap<DateTime<Tz>, RollingMean>,
}

impl NoiseFnData {
    fn push(&mut self, bar: HistoricalDataFullKeys) {
        self.day_vwap.push(&bar);
        if bar.get_time().with_timezone(&New_York).date_naive()
            != self
                .most_recent_day_bar
                .get_time()
                .with_timezone(&New_York)
                .date_naive()
        {
            self.daily_volatility
                .push(self.last_close / self.most_recent_day_bar.get_open_price());
            self.last_close = bar.get_price();
            self.most_recent_day_bar = bar;
            return;
        }

        self.last_close = bar.get_price();
        let day_open = self.most_recent_day_bar.get_open_price();
        self.avg_moves
            .entry(bar.get_time().with_timezone(&New_York))
            .and_modify(|rolling_mean| {
                let movement_since_open = (bar.get_price() / day_open - 1.0).abs();
                rolling_mean.push(movement_since_open);
            });
    }
}

#[derive(Debug)]
pub struct Noise {
    priority: u32,
    name: String,
    pool: PgPool,
    tokio_handle: tokio::runtime::Handle,
    data: Option<NoiseFnData>,
    /// Generic backtest params (key → value), read by cfg-gated branches in
    /// `on_bar_update`. Populated from `BacktestConfig.strategy_params["noise"]`
    /// via [`with_backtest_params`]. `None` under the default build + when no
    /// `NOISE_*` env vars are set (falls back to hardcoded values).
    #[cfg(feature = "backtest")]
    backtest_params: Option<std::collections::HashMap<String, f64>>,
}

// impl PartialEq for Noise {
//     fn eq(&self, other: &Self) -> bool {
//         self.priority == other.priority && self.name == other.name
//     }
// }
//
// impl Eq for Noise {}
//
// impl PartialOrd for Noise {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }
//
// impl Ord for Noise {
//     fn cmp(&self, other: &Self) -> Ordering {
//         match self.priority.cmp(&other.priority) {
//             Ordering::Equal => self.name.cmp(&other.name),
//             other => other,
//         }
//     }
// }

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
        }
    }

    /// Set the generic backtest params (key → value). The strategy's
    /// cfg-gated `on_bar_update` branches read from this map via [`param`],
    /// falling back to hardcoded values when a key is absent.
    #[cfg(feature = "backtest")]
    pub fn with_backtest_params(mut self, params: std::collections::HashMap<String, f64>) -> Self {
        self.backtest_params = Some(params);
        self
    }

    /// Read a backtest param by key, falling back to `default` if unset (or
    /// under the default build where `backtest_params` is absent).
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

    // /// The strategy's declared cache queries (the 4 NoiseOps + read_last_vwap).
    // /// The lookback params are read from `backtest_params` (the fixed
    // /// `cache_params` per-sweep) — the cache + the strategy's `on_bar_update`
    // /// use the SAME values.
    // #[cfg(feature = "backtest")]
    // fn cache_queries(&self) -> Vec<std::sync::Arc<dyn CacheQuery>> {
    //     vec![
    //         std::sync::Arc::new(AvgMoveSinceOpenQuery {
    //             avg_move_lookback: self.param("avg_move_lookback", 15.0) as i64,
    //         }),
    //         std::sync::Arc::new(DailyVolQuery {
    //             vol_lookback: self.param("vol_lookback", 14.0) as i64,
    //         }),
    //         std::sync::Arc::new(MostRecentDailyOpenQuery),
    //         std::sync::Arc::new(LastVwapQuery),
    //     ]
    // }

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
        let mut avg_moves = HashMap::new();
        let historical_data_crud = HistoricalDataCRUD::from(&AssetType::Stock, self.pool.clone());
        let last_n_bars = historical_data_crud
            .read_last_n(
                HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
                    stock: "QQQ".to_string(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                }),
                5,
                (NUM_BARS_PER_DAY * num_days + NUM_BARS_PER_DAY) as u32,
                #[cfg(feature = "backtest")]
                bar_time,
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

        let mut day_vwap = RollingDayVwap::new(78);
        let mut daily_opens = HashMap::new();
        let mut daily_volatility = RollingStd::new(vol_lookback as usize);

        // read first bar first, since loop will use windows(2) and ignore first bar
        let first_bar = last_n_bars.full.first().unwrap();
        day_vwap.push(&first_bar);
        let first_bar_time = first_bar.get_time().with_timezone(&New_York);
        if first_bar_time.hour() == 9 && first_bar_time.minute() == 30 {
            daily_opens.insert(first_bar_time.date_naive(), first_bar.clone());
        }

        let mut first_switch = true;
        for bars in last_n_bars.full.windows(2) {
            let first_bar = &bars[0];
            let second_bar = &bars[1];

            day_vwap.push(&second_bar);
            let first_bar_time = first_bar.get_time().with_timezone(&New_York);
            let second_bar_time = second_bar.get_time().with_timezone(&New_York);
            let first_bar_date = first_bar_time.date_naive();
            let second_bar_date = second_bar_time.date_naive();

            if first_bar_date != second_bar_date {
                match daily_opens.get(&first_bar_date) {
                    Some(open) => {
                        daily_volatility.push(first_bar.get_price() / open.get_open_price());
                    }
                    None => {
                        if !first_switch {
                            tracing::error!("Failed to get open of previous bar");
                        }
                    }
                }
                daily_opens.insert(second_bar_date, second_bar.clone());
                first_switch = false;
            } else {
                if let Some(open_bar) = daily_opens.get(&second_bar_date) {
                    let movement_since_open =
                        (second_bar.get_price() / open_bar.get_open_price() - 1.0).abs();
                    avg_moves
                        .entry(second_bar_time)
                        .and_modify(|rolling_mean: &mut RollingMean| {
                            rolling_mean.push(movement_since_open);
                        })
                        .or_insert(RollingMean::new(avg_move_lookback as usize));
                }
            }
        }

        let most_recent_day_open = daily_opens
            .get(
                daily_opens
                    .keys()
                    .max()
                    .expect("Expected at least one daily open in noise"),
            )
            .unwrap();

        self.data = Some(NoiseFnData {
            most_recent_day_bar: (*most_recent_day_open).clone(),
            last_close: last_n_bars.full.last().unwrap().get_price(),
            day_vwap,
            daily_volatility,
            avg_moves,
        });

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
        let mut noise_data = self
            .data
            .as_mut()
            .expect("Expected sufficient data in noise fn warm up for on_bar_update");
        noise_data.push(bar.clone());

        let (avg_move_since_open, most_recent_open, most_recent_daily_vol, vwap) = (
            noise_data
                .avg_moves
                .get(&bar.get_time().with_timezone(&New_York))
                .expect("Expected to be able to get avg_moves")
                .rolling_mean()
                .expect("Expected sufficient data for avg move since open"),
            noise_data.most_recent_day_bar.get_open_price(),
            noise_data
                .daily_volatility
                .rolling_std()
                .expect("Expected sufficient data for daily vol"),
            noise_data
                .day_vwap
                .vwap()
                .expect("Expected sufficient data for vwap"),
        );

        // Minimum required qty for decent stats is 5.0
        // 50/100 gives a decent reward-return of 5% roughly annualised returns
        // *4 is max leverage intraday
        // let curr_available_funds_wrapped = consolidator.get_current_available_funds();
        // if let Err(e) = curr_available_funds_wrapped {
        //     tracing::error!("Error trying to get current available funds in noise: {e:?}");
        //     return Ok((false, false));
        // }
        // let curr_available_funds = curr_available_funds_wrapped.unwrap();
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

        let ideal_qty = if most_recent_daily_vol >= {
            #[cfg(feature = "backtest")]
            {
                self.param("daily_vol_threshold", 0.04)
            }
            #[cfg(not(feature = "backtest"))]
            {
                0.04
            }
        } {
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
                tracing::warn!(
                    "Maximum allowable position for QQQ for Noise is: {allowable_positions:?}"
                );
                *allowable_positions
            } else {
                ideal_qty
            }
        } as f64;

        let (upper_noise, _lower_noise) = (
            (1.0 + noise_multiplier * avg_move_since_open) * most_recent_open,
            (1.0 - noise_multiplier * avg_move_since_open) * most_recent_open,
        );

        tracing::info!(
            message=%format!(
                "QQQ price is {}, upper noise is {}",
                &bar.get_price(),
                &upper_noise
            )
        );

        let (bar_close, bar_time) = (bar.get_price(), bar.get_time().with_timezone(&New_York));
        if qty != 0.0 {
            let current_time = Utc::now().with_timezone(&New_York);
            let last_time = New_York
                .with_ymd_and_hms(
                    current_time.year(),
                    current_time.month(),
                    current_time.day(),
                    15,
                    45,
                    0,
                )
                .unwrap();
            if ((bar_close < upper_noise
                || Decimal::from_f64(bar_close)
                    .expect("Expected bar_close conversion to Decimal to be ok")
                    <= vwap)
                && bar_time.minute() % act_interval == 0)
                || current_time >= last_time
            {
                let target_stock_positions_crud =
                    TargetPositionsCRUD::from(&AssetType::Stock, self.pool.clone());
                let name = self.get_name();
                #[cfg(feature = "backtest")]
                {
                    use crate::backtester::methods::in_memory::state::PositionKey;
                    if let Some(state) =
                        crate::backtester::methods::in_memory::thread_local::current()
                    {
                        state.delete_target(&PositionKey {
                            strategy: name.clone(),
                            stock: "QQQ".to_string(),
                            primary_exchange: "NASDAQ".to_string(),
                            currency: "USD".to_string(),
                        });
                        return Ok(BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]));
                    }
                }
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
                return Ok(BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]));
            }
            return Ok(BarUpdateOutcome::NoAction);
        }

        if bar_close > upper_noise && bar_time.minute() % act_interval == 0 {
            let target_stock_positions_crud =
                TargetPositionsCRUD::from(&AssetType::Stock, self.pool.clone());
            let name = self.get_name();
            #[cfg(feature = "backtest")]
            {
                use crate::backtester::methods::in_memory::state::PositionKey;
                if let Some(state) = crate::backtester::methods::in_memory::thread_local::current()
                {
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
                    return Ok(BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]));
                }
            }
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
            return Ok(BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]));
        }

        return Ok(BarUpdateOutcome::NoAction);
    }
}
