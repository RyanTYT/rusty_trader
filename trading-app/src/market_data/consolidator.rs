use std::{collections::HashMap, mem::ManuallyDrop, sync::Arc, time::Duration};

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::{America::New_York, Tz};
use ibapi::{
    Client,
    market_data::historical::ToDuration,
    prelude::{Contract, SecurityType},
};
use nyse_holiday_cal::HolidayCal;
use sqlx::PgPool;

use crate::{
    database::{
        models::AssetType,
        models_crud::historical_data::historical_data::{
            HistoricalDataCRUD, HistoricalDataOps, HistoricalDataPrimaryKeysWoTime,
        },
    },
    helpers::{contract::HashContract, sync_timeout::timeout},
    market_data::{
        handler::MarketDataHandler,
        memoise::{AnyMemoized, Memoized},
        traits::current_price::{HistoricalDataConfig, PriceSupplier},
    },
    schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler},
};

const NYSE_OPEN_TIME: (u32, u32) = (9, 30);
const NYSE_CLOSE_TIME: (u32, u32) = (16, 30);

const OPTION_OPEN_TIME: (u32, u32) = (9, 0);
const OPTION_CLOSE_TIME: (u32, u32) = (16, 0);

const STOCK_BAR_GRANULARITY_MIN: u32 = 5;
const OPTION_BAR_GRANULARITY_MIN: u32 = 5;
const FOREX_BAR_GRANULARITY_MIN: u32 = 1;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum MemoisedConsolidatorFns {
    GetPrice,
    GetStrategyValue,
    GetAvailableFunds,
}

pub struct Consolidator {
    // StrategyScheduler
    // pub(super) contract_coordinator: Arc<IbkrContractScheduler>,

    // AccountTracker
    pub pool: PgPool,
    pub(crate) market_data_handler: MarketDataHandler,
    pub(super) memoisers: Arc<HashMap<MemoisedConsolidatorFns, Arc<Box<dyn AnyMemoized>>>>,
    contract_scheduler: Arc<IbkrContractScheduler>,

    pub(crate) client: Arc<Client>,
}

impl Consolidator {
    pub async fn async_drop(&mut self) {
        self.market_data_handler.async_drop().await;
        tracing::info!("Client disconnected");
        self.client.disconnect();
    }
}

impl Drop for Consolidator {
    fn drop(&mut self) {
        if self.client.is_connected() {
            self.client.disconnect();
            tracing::info!("Client disconnected");
        }
    }
}

impl Consolidator {
    pub fn new(
        handle: tokio::runtime::Handle,
        pool: PgPool,
        client: Arc<Client>,
        market_data_handler: MarketDataHandler,
        contract_scheduler: Arc<IbkrContractScheduler>,
    ) -> Self {
        let price_ttl = Duration::from_mins(15);
        let ttl = Duration::from_secs(60);
        let mut memoisers: HashMap<MemoisedConsolidatorFns, Arc<Box<dyn AnyMemoized>>> =
            HashMap::new();
        let get_price_fn: Arc<Box<dyn AnyMemoized>> = Arc::new(Box::new(Memoized::new(
            price_ttl,
            // I = (Contract, bool, Vec<String>, bool) — everything owned,
            // so it's Send + 'static and can go through the type-erased map.
            |input: &(Arc<Client>, Contract, bool, Vec<String>, bool)| {
                let (_, contract, vwap, generic_ticks, is_second_try) = input;
                (
                    HashContract {
                        contract: contract.clone(),
                    },
                    *vwap,
                    generic_ticks.clone(),
                    *is_second_try,
                )
            },
            {
                let handle = handle.clone();
                move |input: &(Arc<Client>, Contract, bool, Vec<String>, bool)| {
                    let (client, contract, vwap, generic_ticks, is_second_try) = input;
                    let client_for_price = client.clone();
                    let ticks_refs: Vec<&str> = generic_ticks.iter().map(String::as_str).collect();
                    Self::_get_current_price(
                        client_for_price,
                        contract,
                        &handle,
                        *vwap,
                        &ticks_refs,
                        *is_second_try,
                    )
                }
            },
        )));
        memoisers.insert(MemoisedConsolidatorFns::GetPrice, get_price_fn.clone());
        let (cloned_client, cloned_handle, cloned_pool) =
            (client.clone(), handle.clone(), pool.clone());
        memoisers.insert(
            MemoisedConsolidatorFns::GetStrategyValue,
            Arc::new(Box::new(Memoized::new(
                ttl,
                // I = (Contract, bool, Vec<String>, bool) — everything owned,
                // so it's Send + 'static and can go through the type-erased map.
                |input: &String| input.clone(),
                move |input: &String| {
                    Self::_get_strategy_sgd_value(
                        &cloned_client,
                        &*get_price_fn,
                        &cloned_handle,
                        &cloned_pool,
                        &input.clone(),
                    )
                },
            ))),
        );

        Self {
            pool: pool.clone(),
            client: client.clone(),
            market_data_handler: market_data_handler,
            // contract_coordinator: Arc::new(IbkrContractScheduler::new(client)),
            memoisers: Arc::new(memoisers),
            contract_scheduler,
        }
    }

    pub fn validate_contract(
        &self,
        contract: Contract,
        timeout_duration: Duration,
    ) -> Option<Contract> {
        Self::_validate_contract(self.client.clone(), contract, timeout_duration)
    }

    pub(crate) fn _validate_contract(
        client: Arc<Client>,
        contract: Contract,
        timeout_duration: Duration,
    ) -> Option<Contract> {
        let symbol = contract.symbol.clone();
        match timeout(timeout_duration, move || client.contract_details(&contract)) {
            Ok(validated_contracts) => {
                if validated_contracts.len() == 0 {
                    return None;
                }
                return Some(validated_contracts.first().take().unwrap().contract.clone());
            }
            Err(e) => {
                tracing::error!(
                    message=%format!(
                        "Error occurred requesting contract details for {}: {}",
                        symbol,
                        e
                    )
                );
                return None;
            }
        }
    }

    async fn refresh_if_stale(&self, contract: &Contract, config: &HistoricalDataConfig) {
        let asset_type = AssetType::from_str(&contract.security_type);
        if self
            .contract_scheduler
            .is_trading(contract, &Utc::now())
            .is_ok_and(|is_trading| !is_trading)
        {
            return;
        }

        // if this returns None -> last bar was previous trading day
        let target = match last_bar_available(Utc::now().with_timezone(&New_York), &asset_type) {
            Some(v) => v,
            None => {
                return;
            }
        };

        let historical_data_crud = HistoricalDataCRUD::from(&asset_type, self.pool.clone());

        match historical_data_crud
            .read_last_n(
                HistoricalDataPrimaryKeysWoTime::from_contract(&contract),
                if asset_type == AssetType::ForexPair || asset_type == AssetType::CFD {
                    1
                } else {
                    5
                },
                1,
            )
            .await
        {
            Ok(bars) => {
                if bars.full.is_empty() {
                    tracing::error!("Full bars is empty on fetch last bar")
                } else {
                    let bar = bars.full.first().unwrap();
                    if bar.get_time() != target {
                        if let Err(e) = self.populate_historical_data(contract, config).await {
                            tracing::error!(
                                "Failed to populate_historical_data in refresh_if_stale: {e:?}"
                            );
                        };
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to fetch last available bar: {e:?}");
            }
        };
    }

    /// Assumes that each day has 78 5-min bars
    /// - today exclusive: 1 refers to yesterday/most recent trading days
    ///      - Note: most recent day of data will always be updated
    /// - gives leeway of one half day before requesting full data: 39 bars less
    /// - Always checks for most recent trading day at least
    /// - apply_batching bool should ONLY be set to true if you have opened the relevant crud
    /// channels beforehand - it WILL fail otherwise. After usage, remember to close the channel to
    /// free up the postgres connection
    /// - what_to_show: doesn't matter for ForexPair - fetches BOTH bid and ask to be updated as
    /// historical data
    ///
    /// Could be Betters
    /// - Can get last bar via historical_data, then request additional data since then, but fck it
    /// for me
    ///
    /// NOTE: Requests always for 5 minute data
    pub async fn update_at_least_n_days_data(
        &self,
        contract: &Contract,
        days: u32,
        use_batching: bool,
    ) -> Result<(), String> {
        // =====================================================
        // Calculate the required_num_bars and earliest_datetime
        // =====================================================
        const FX_BARS_PER_DAY: i64 = 1425; // 15 min of inactive trading
        const STOCK_BARS_PER_DAY: i64 = 78;

        let now_ny = Utc::now().with_timezone(&New_York);
        let mut earliest_datetime = now_ny;

        let required_num_bars = if matches!(contract.security_type, SecurityType::ForexPair)
            || matches!(contract.security_type, SecurityType::CFD)
        {
            // Start from the previous FX trading day (one minute before the day starts)
            let mut cursor = fx_trading_day_start(&now_ny.date_naive(), &New_York)
                - chrono::Duration::minutes(1);

            // Step backwards through N valid FX trading days
            let mut valid_days_found = 0;
            while valid_days_found < days {
                // Move to the previous FX trading day start (minus 1 minute to land in previous day)
                cursor = fx_trading_day_start(&cursor.date_naive(), &New_York)
                    - chrono::Duration::minutes(1);

                // Check if this is actually a valid FX trading datetime
                // (skips weekends and holidays)
                if is_fx_trading_datetime(&cursor) {
                    valid_days_found += 1;
                }
            }

            earliest_datetime = cursor;
            let required_num_bars = (days) as i64 * FX_BARS_PER_DAY;
            required_num_bars
        } else {
            let session_start = NaiveTime::from_hms_opt(9, 0, 0).unwrap();

            let required_num_bars = (days) as i64 * STOCK_BARS_PER_DAY;

            let mut days_counter = 0;
            for day in now_ny.date_naive().busday_iter().rev() {
                days_counter += 1;

                if days_counter == days {
                    earliest_datetime = New_York
                        .from_local_datetime(&day.and_time(session_start))
                        .single()
                        .unwrap();
                    break;
                }
            }
            required_num_bars
        };
        tracing::info!(
            "required_num_bars: {required_num_bars:?}, earliest_datetime: {earliest_datetime:?}"
        );
        // =====================================================
        // END OF
        // Calculate the required_num_bars and earliest_datetime
        // =====================================================

        let asset_type = AssetType::from_str(&contract.security_type);
        let historical_data_crud = HistoricalDataCRUD::from(&asset_type, self.pool.clone());
        let historical_data_pk = HistoricalDataPrimaryKeysWoTime::from_contract(&contract);
        match historical_data_crud
            .has_at_least_n_rows_since(
                historical_data_pk,
                required_num_bars as u64,
                &earliest_datetime,
            )
            .await
        {
            Ok(passed) => {
                let config = HistoricalDataConfig::new(
                    if passed {
                        1.days()
                    } else {
                        (days as i32).days()
                    },
                    if asset_type == AssetType::ForexPair || asset_type == AssetType::CFD {
                        ibapi::market_data::historical::BarSize::Min
                    } else {
                        ibapi::market_data::historical::BarSize::Min5
                    },
                    if asset_type == AssetType::ForexPair || asset_type == AssetType::CFD {
                        ibapi::market_data::historical::WhatToShow::Bid
                    } else {
                        ibapi::market_data::historical::WhatToShow::Trades
                    },
                    use_batching,
                );
                if passed {
                    self.refresh_if_stale(&contract, &config).await;
                } else {
                    if let Err(e) = self.populate_historical_data(&contract, &config).await {
                        tracing::error!(
                            "Failed to populate_historical_data in update_at_least_n_days_data: {e:?}"
                        );
                    };
                }
            }
            Err(e) => tracing::error!("Failed to check for has_at_least_n_rows_since: {e:?}"),
        };
        Ok(())
    }
}

/// pub(crate) visibility ONLY for testing purposes
pub(crate) fn last_bar_available_time_forex(now: DateTime<Tz>) -> DateTime<Tz> {
    let mut cursor = now;
    while !is_fx_trading_datetime(&cursor) {
        cursor -= Duration::from_mins(1);
    }
    cursor
}

/// Grid-based variant used by stock and option: floor-to-grid, optionally
/// cap at a session close, then gate on a session-open threshold.
/// Returns `None` if we're before the open threshold (nothing to refresh yet).
///
/// pub(crate) visibility ONLY for testing purposes
pub(crate) fn last_bar_available(
    now: DateTime<Tz>,
    asset_type: &AssetType,
) -> Option<DateTime<Tz>> {
    let (open, close, granularity_minutes) = match asset_type {
        AssetType::Stock | AssetType::Future => {
            (NYSE_OPEN_TIME, NYSE_CLOSE_TIME, STOCK_BAR_GRANULARITY_MIN)
        }
        AssetType::Option => (
            OPTION_OPEN_TIME,
            OPTION_CLOSE_TIME,
            OPTION_BAR_GRANULARITY_MIN,
        ),
        AssetType::CFD | AssetType::ForexPair => return Some(last_bar_available_time_forex(now)),
        _ => panic!("Should not be using this function for AssetType not stock or option"),
    };
    let floored_minute = now.minute() - (now.minute() % granularity_minutes);
    let mut last_bar = now
        .with_minute(floored_minute as u32)
        .expect("Expected corrected minute")
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
        - Duration::from_mins(granularity_minutes as u64);

    let close_time = now
        .date_naive()
        .and_hms_opt(close.0, close.1, 0)
        .and_then(|t| New_York.from_local_datetime(&t).single())
        .expect("Invalid NY datetime");
    if last_bar > close_time {
        last_bar = close_time - Duration::from_mins(granularity_minutes as u64);
    }

    let open_threshold = now
        .date_naive()
        .and_hms_opt(open.0, open.1, 0)
        .and_then(|t| New_York.from_local_datetime(&t).single())
        .expect("Invalid NY datetime");
    (last_bar > open_threshold).then_some(last_bar)
}

pub(crate) fn is_fx_trading_datetime(dt: &DateTime<Tz>) -> bool {
    fn is_fx_market_holiday(date: NaiveDate) -> bool {
        let year = date.year();

        // New Year's Day (observed)
        if is_observed_holiday(date, year, 1, 1) {
            return true;
        }
        // If New Year's Day falls on Sat -> Fri observes Holiday
        if is_observed_holiday(date, year + 1, 1, 1) {
            return true;
        }

        // Christmas Day (observed)
        if is_observed_holiday(date, year, 12, 25) {
            return true;
        }

        // Good Friday (Easter-based, varies each year)
        if date == calculate_good_friday(year) {
            return true;
        }

        false
    }

    /// Check if a holiday is observed on this date (handles weekend adjustments)
    fn is_observed_holiday(date: NaiveDate, year: i32, month: u32, day: u32) -> bool {
        if let Some(holiday) = NaiveDate::from_ymd_opt(year, month, day) {
            match holiday.weekday() {
                // If holiday falls on Saturday, observed on Friday
                chrono::Weekday::Sat => date == holiday - chrono::Days::new(1),
                // If holiday falls on Sunday, observed on Monday
                chrono::Weekday::Sun => date == holiday + chrono::Days::new(1),
                // Weekday holiday observed on actual day
                _ => date == holiday,
            }
        } else {
            false
        }
    }

    /// Calculate Good Friday for a given year using the Computus algorithm
    fn calculate_good_friday(year: i32) -> NaiveDate {
        // Meeus/Jones/Butcher algorithm for Gregorian calendar
        let a = year % 19;
        let b = year / 100;
        let c = year % 100;
        let d = b / 4;
        let e = b % 4;
        let f = (b + 8) / 25;
        let g = (b - f + 1) / 3;
        let h = (19 * a + b - d - g + 15) % 30;
        let i = c / 4;
        let k = c % 4;
        let l = (32 + 2 * e + 2 * i - h - k) % 7;
        let m = (a + 11 * h + 22 * l) / 451;
        let month = (h + l - 7 * m + 114) / 31;
        let day = ((h + l - 7 * m + 114) % 31) + 1;

        // Easter Sunday
        let easter = NaiveDate::from_ymd_opt(year, month as u32, day as u32).unwrap();

        // Good Friday is 2 days before Easter
        easter - chrono::Days::new(2)
    }

    // FX constants (New York time)
    const FX_WEEK_OPEN_HOUR: u32 = 17; // Sunday 17:00 NY
    const FX_WEEK_CLOSE_HOUR: u32 = 17; // Friday 17:00 NY

    // Check if it's a FX market holiday
    if is_fx_market_holiday(dt.date_naive()) {
        return false;
    }

    let weekday = dt.weekday();
    match weekday {
        chrono::Weekday::Sun => dt.hour() >= FX_WEEK_OPEN_HOUR,
        chrono::Weekday::Fri => dt.hour() < FX_WEEK_CLOSE_HOUR,
        chrono::Weekday::Sat => false,
        _ => true, // Mon–Thu always trading
    }
}

/// pub(crate) instead of private visibility ONLY for testing purposes
pub(crate) fn fx_trading_day_start(date: &NaiveDate, tz: &Tz) -> DateTime<Tz> {
    const FX_WEEK_OPEN_HOUR: u32 = 17; // Sunday 17:00 NY
    //
    let prev_day = date.pred_opt().unwrap();
    tz.from_local_datetime(&prev_day.and_hms_opt(FX_WEEK_OPEN_HOUR, 0, 0).unwrap())
        .single()
        .unwrap()
}
