// begin_bar_listening is now kinda weird since i pass myself into the mix - not sure if that is
// best practice
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    hash::Hash,
    iter::zip,
    mem,
    str::FromStr,
    sync::{Arc, Mutex, RwLock, Weak},
    thread,
    time::{Duration, Instant},
};

use chrono::{DateTime, Datelike, NaiveTime, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::America::New_York;
use ibapi::{
    Client,
    client::Subscription,
    market_data::realtime::Bar,
    prelude::{
        Contract, HistoricalBarSize, HistoricalWhatToShow, RealtimeWhatToShow, SecurityType,
        TickTypes,
    },
};
use moka::sync::Cache;
use nyse_holiday_cal::HolidayCal;
use rust_decimal::{Decimal, prelude::FromPrimitive, str};
use sqlx::PgPool;
use tokio::sync::mpsc::{Sender, channel};
use tracing::info;

use crate::{
    database::{
        models::{
            AssetType, HistoricalDataPrimaryKeys, HistoricalDataUpdateKeys,
            HistoricalForexDataPrimaryKeys, HistoricalForexDataUpdateKeys,
            HistoricalOptionsDataPrimaryKeys, HistoricalOptionsDataUpdateKeys, OptionType,
        },
        models_crud::{
            historical_data::{HistoricalDataCRUD, get_specific_historical_data_crud},
            historical_forex_data::{
                AggregatedBars, HistoricalForexDataCRUD, get_specific_historical_forex_data_crud,
            },
            historical_options_data::{
                HistoricalOptionsDataCRUD, get_specific_historical_options_data_crud,
            },
        },
    },
    execution::order_engine::OrderEngine,
    helpers::{
        contract::{HashContract, get_local_symbol},
        sync_timeout::timeout,
    },
    market_data::strategy_scheduler::StrategyScheduler,
    schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler},
    strategy::strategy::{StrategyEnum, StrategyExecutor},
};

#[allow(dead_code)]
const MINUTES_PER_DAY: i64 = 1440;

#[derive(Debug, Clone)]
pub struct StrategyNWts<T: StrategyExecutor> {
    strategy: T,
    what_to_show: RealtimeWhatToShow,
}
impl<T: StrategyExecutor> Hash for StrategyNWts<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.strategy.get_name().hash(state);
        std::mem::discriminant(&self.what_to_show).hash(state);
    }
}
impl<T: StrategyExecutor> PartialEq for StrategyNWts<T> {
    fn eq(&self, other: &Self) -> bool {
        self.strategy.eq(&other.strategy)
            && std::mem::discriminant(&self.what_to_show)
                .eq(&std::mem::discriminant(&other.what_to_show))
    }
}
impl<T: StrategyExecutor> Eq for StrategyNWts<T> {}

#[derive(Debug, Clone)]
struct ForexBar {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

#[derive(Debug, Clone)]
struct ForexBarCollector {
    bid: Option<ForexBar>,
    ask: Option<ForexBar>,
}

#[derive(Debug, Clone)]
struct UniqueSubscription {
    contract: HashContract,
    data_type: RealtimeWhatToShow,
}
impl PartialEq for UniqueSubscription {
    fn eq(&self, other: &Self) -> bool {
        self.contract.eq(&other.contract)
            && std::mem::discriminant(&self.data_type).eq(&std::mem::discriminant(&other.data_type))
    }
}
impl Eq for UniqueSubscription {}
impl Hash for UniqueSubscription {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.contract.hash(state);
        std::mem::discriminant(&self.data_type).hash(state);
    }
}

pub struct Consolidator {
    pub(super) client: Arc<Client>,

    // Strategy Value
    pub(super) past_strategy_data: Arc<Cache<String, f64>>,

    // StrategyScheduler
    pub(super) contract_coordinator: Arc<IbkrContractScheduler>,
    pub(super) strat_contracts: Arc<RwLock<HashMap<String, Vec<HashContract>>>>,

    // AccountTracker
    pub(super) available_funds: Arc<Mutex<Option<f64>>>,
    pub(super) available_funds_channel_killer: Arc<Mutex<Option<Arc<()>>>>,

    pub pool: PgPool,
    // subscriptions to -> contract -> timestep -> strategies
    subscriptions: Arc<
        RwLock<HashMap<UniqueSubscription, BTreeMap<u32, Arc<RwLock<BTreeSet<StrategyEnum>>>>>>,
    >,
    // subscriptions from contract -> subscription
    subscriptions_handler: Arc<Mutex<HashMap<UniqueSubscription, Arc<()>>>>,
    forex_subscriptions_tracker:
        Arc<tokio::sync::Mutex<HashMap<String, HashMap<DateTime<Utc>, ForexBarCollector>>>>,

    pub(super) live_data: Arc<RwLock<HashMap<HashContract, Weak<RwLock<Option<f64>>>>>>,
    pub(super) past_data: Arc<Cache<HashContract, f64>>,
    pub(super) past_data_vwap: Arc<Cache<HashContract, f64>>,

    contract_update_sender:
        Arc<RwLock<Option<Sender<(Contract, RealtimeWhatToShow, DateTime<Utc>)>>>>,
    // direct_contract_update_sender:
    //     Arc<RwLock<Option<Sender<(Contract, Bar, RealtimeWhatToShow, DateTime<Utc>)>>>>,
    historical_data_crud: HistoricalDataCRUD,
    historical_forex_data_crud: HistoricalForexDataCRUD,
    historical_options_data_crud: HistoricalOptionsDataCRUD,
    is_historical_data_crud_channel_opened: Arc<tokio::sync::Mutex<bool>>,

    bar_listener_channel_killer: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    // is_historical_options_data_crud_channel_opened: Arc<tokio::sync::Mutex<bool>>,
}

impl Consolidator {
    pub fn new(pool: PgPool, client: Arc<Client>) -> Self {
        let ttl = Duration::from_secs(60);
        let max_capacity = 10;
        let available_funds: Arc<Mutex<Option<f64>>> = Arc::new(Mutex::new(None));

        Self {
            pool: pool.clone(),
            client: client.clone(),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            subscriptions_handler: Arc::new(Mutex::new(HashMap::new())),
            forex_subscriptions_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),

            live_data: Arc::new(RwLock::new(HashMap::new())),
            past_strategy_data: Arc::new(
                Cache::builder()
                    .time_to_live(Duration::from_mins(60))
                    .max_capacity(1000)
                    .build(),
            ),
            past_data: Arc::new(
                Cache::builder()
                    .time_to_live(ttl)
                    .max_capacity(max_capacity)
                    .build(),
            ),
            past_data_vwap: Arc::new(
                Cache::builder()
                    .time_to_live(ttl)
                    .max_capacity(max_capacity)
                    .build(),
            ),
            contract_coordinator: Arc::new(IbkrContractScheduler::new(client)),
            strat_contracts: Arc::new(RwLock::new(HashMap::new())),

            available_funds: available_funds,

            contract_update_sender: Arc::new(RwLock::new(None)),

            historical_data_crud: get_specific_historical_data_crud(pool.clone()),
            historical_forex_data_crud: get_specific_historical_forex_data_crud(pool.clone()),
            historical_options_data_crud: get_specific_historical_options_data_crud(pool),
            is_historical_data_crud_channel_opened: Arc::new(tokio::sync::Mutex::new(false)),

            available_funds_channel_killer: Arc::new(Mutex::new(None)),
            bar_listener_channel_killer: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn get_last_n_forex_bars(
        &self,
        pair: &str,
        timestep: &u32,
        num_bars: &u32,
    ) -> Result<AggregatedBars, String> {
        self.historical_forex_data_crud
            .read_last_n_bars(pair, timestep, num_bars)
            .await
    }

    pub fn validate_contract(
        &self,
        contract: &Contract,
        timeout_duration: Duration,
    ) -> Option<Contract> {
        let client = self.client.clone();
        let cloned_contract = contract.clone();

        match timeout(timeout_duration, move || {
            client.contract_details(&cloned_contract)
        }) {
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
                        contract.symbol,
                        e
                    )
                );
                return None;
            }
        }
    }

    pub async fn open_historical_data_crud_channel(&self) {
        let mut is_opened = self.is_historical_data_crud_channel_opened.lock().await;
        if !*is_opened {
            self.historical_data_crud.init_channel().await;
            *is_opened = true;
        }
    }

    pub async fn close_historical_data_crud_channel(&self) {
        let mut is_opened = self.is_historical_data_crud_channel_opened.lock().await;
        if *is_opened {
            self.historical_data_crud.close_channel().await;
            *is_opened = false;
        }
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
        what_to_show: HistoricalWhatToShow,
        days: &u32,
        apply_batching: &bool,
    ) -> Result<(), String> {
        const HIGHEST_GRANULARITY_TIMESTEP: HistoricalBarSize = HistoricalBarSize::Min5;
        const HIGHEST_GRANULARITY_TIMESTEP_FOREX: HistoricalBarSize = HistoricalBarSize::Min;
        // =====================================================
        // Calculate the required_num_bars and earliest_datetime
        // =====================================================
        const FX_BARS_PER_DAY: i64 = 1425; // supposed to be 1440 but ibkr seems to regularly drop
        // 15 (due to inactivity?)
        const STOCK_BARS_PER_DAY: i64 = 78;

        let now_ny = Utc::now().with_timezone(&New_York);
        let mut earliest_datetime = now_ny;

        let (required_num_bars, is_trading_day_tdy) =
            if matches!(contract.security_type, SecurityType::ForexPair)
                || matches!(contract.security_type, SecurityType::CFD)
            {
                // Start from the previous FX trading day (one minute before the day starts)
                let mut cursor =
                    Consolidator::fx_trading_day_start(&now_ny.date_naive(), &New_York)
                        - chrono::Duration::minutes(1);

                // Step backwards through N valid FX trading days
                let mut valid_days_found = 0;
                while valid_days_found < *days {
                    // Move to the previous FX trading day start (minus 1 minute to land in previous day)
                    cursor = Consolidator::fx_trading_day_start(&cursor.date_naive(), &New_York)
                        - chrono::Duration::minutes(1);

                    // Check if this is actually a valid FX trading datetime
                    // (skips weekends and holidays)
                    if Consolidator::is_fx_trading_datetime(&cursor) {
                        valid_days_found += 1;
                    }
                }

                earliest_datetime = cursor;
                let required_num_bars = (*days) as i64 * FX_BARS_PER_DAY;
                let is_trading_day_tdy = now_ny.weekday() != Weekday::Sat;
                (required_num_bars, is_trading_day_tdy)
            } else {
                let session_start = NaiveTime::from_hms_opt(9, 0, 0).unwrap();

                let required_num_bars = (*days) as i64 * STOCK_BARS_PER_DAY;
                let is_trading_day_tdy = (now_ny - chrono::Duration::days(1))
                    .date_naive()
                    .busday_iter()
                    .next()
                    == Some(now_ny.date_naive());

                let mut days_counter = 0;
                for day in now_ny.date_naive().busday_iter().rev() {
                    days_counter += 1;

                    if days_counter == *days {
                        earliest_datetime = New_York
                            .from_local_datetime(&day.and_time(session_start))
                            .single()
                            .unwrap();
                        break;
                    }
                }
                (required_num_bars, is_trading_day_tdy)
            };
        tracing::info!(
            "required_num_bars: {required_num_bars:?}, earliest_datetime: {earliest_datetime:?}"
        );
        // =====================================================
        // END OF
        // Calculate the required_num_bars and earliest_datetime
        // =====================================================

        match AssetType::from_str(&contract.security_type) {
            AssetType::Stock | AssetType::Future => {
                let historical_data_crud = self.historical_data_crud.clone();
                let symbol = get_local_symbol(&contract);

                let n_rows_res = historical_data_crud
                    .has_at_least_n_rows_since(
                        symbol.as_str(),
                        contract.primary_exchange.as_str(),
                        contract.currency.as_str(),
                        &earliest_datetime,
                        &((required_num_bars - 39).max(0) as u32),
                    )
                    .await;

                // Return if there is enough data
                if let Ok(passed) = n_rows_res {
                    if passed {
                        info!("Enough rows in historical data");
                        if is_trading_day_tdy {
                            let time_now = Utc::now().with_timezone(&New_York);

                            // NYSE close time (same trading day)
                            let nyse_close = time_now
                                .date_naive()
                                .and_hms_opt(16, 30, 0)
                                .and_then(|t| New_York.from_local_datetime(&t).single())
                                .expect("Invalid NYSE close datetime");

                            // Compute last completed 5-min bar based on current time
                            let last_bar_min = time_now.minute() - (time_now.minute() % 5);
                            let mut last_bar_available_time = time_now
                                .with_minute(last_bar_min)
                                .expect("Expected corrected last_bar_min")
                                .with_second(0)
                                .unwrap()
                                .with_nanosecond(0)
                                .unwrap()
                                - chrono::Duration::minutes(5);

                            // Cap at NYSE close
                            if last_bar_available_time > nyse_close {
                                last_bar_available_time = nyse_close - chrono::Duration::minutes(5);
                            }

                            info!(
                                "last_bar_available_time: {last_bar_available_time:?}, greater than: dk",
                            );
                            if last_bar_available_time
                                > Utc::now()
                                    .with_timezone(&New_York)
                                    .with_hour(9)
                                    .unwrap()
                                    .with_minute(30)
                                    .unwrap()
                                    .with_second(0)
                                    .unwrap()
                                    .with_nanosecond(0)
                                    .unwrap()
                            {
                                match historical_data_crud
                                    .read_last_bar_of_stock(
                                        symbol.as_str(),
                                        contract.primary_exchange.as_str(),
                                        contract.currency.as_str(),
                                        &5,
                                    )
                                    .await
                                {
                                    Ok(last_bar) => {
                                        if let Some(bar) = last_bar {
                                            info!(
                                                message = format!(
                                                    "Local bar time: {} and last_bar_available_time: {}, Equal: {}",
                                                    bar.time,
                                                    last_bar_available_time,
                                                    bar.time == last_bar_available_time
                                                )
                                            );
                                            if bar.time == last_bar_available_time {
                                                return Ok(());
                                            }
                                        }
                                        Self::get_historical_data(
                                            self.client.clone(),
                                            &self.historical_data_crud,
                                            &self.historical_options_data_crud,
                                            &self.historical_forex_data_crud,
                                            &contract,
                                            ibapi::market_data::historical::Duration::from_str("1 D").expect("Expected to be able to parse 1 D for market data historical data"),
                                            HIGHEST_GRANULARITY_TIMESTEP,
                                            what_to_show,
                                            &false, // is_forex
                                            apply_batching
                                        )?
                                    }
                                    Err(e) => tracing::error!(
                                        "Expected to be able to select from market_data.historical_data: {}",
                                        e
                                    ),
                                };
                            }
                        }
                        return Ok(());
                    }
                }

                // Else, request all data required
                let duration_in_sec =
                    (Utc::now().with_timezone(&New_York) - earliest_datetime).num_seconds() as u64;

                let duration = if duration_in_sec > 86400 {
                    ibapi::market_data::historical::Duration::from_str(&format!(
                        "{} D",
                        (duration_in_sec / 60 / 60 / 24) as u32
                    ))
                    .expect("Expected Duration passed to historical_data method to be correct!")
                } else {
                    ibapi::market_data::historical::Duration::from_str(&format!(
                        "{} S",
                        duration_in_sec
                    ))
                    .expect("Expected Duration passed to historical_data method to be correct!")
                };
                info!(message = format!("Requesting {} duration of data", duration.to_string()));

                Self::get_historical_data(
                    self.client.clone(),
                    &self.historical_data_crud,
                    &self.historical_options_data_crud,
                    &self.historical_forex_data_crud,
                    &contract,
                    duration,
                    HIGHEST_GRANULARITY_TIMESTEP,
                    what_to_show,
                    &false, // is_forex
                    apply_batching,
                )?;

                Ok(())
            }
            AssetType::ForexPair | AssetType::CFD => {
                let historical_forex_data_crud = self.historical_forex_data_crud.clone();
                let symbol = get_local_symbol(&contract);

                let n_rows_res = historical_forex_data_crud
                    .has_at_least_n_rows_since(
                        symbol.as_str(),
                        &earliest_datetime,
                        &(required_num_bars.max(0) as u32),
                    )
                    .await;

                // Return if there is enough data
                if let Ok(passed) = n_rows_res {
                    if passed {
                        info!("Enough rows in historical data");
                        if is_trading_day_tdy {
                            let mut cursor = Utc::now().with_timezone(&New_York);
                            while !Consolidator::is_fx_trading_datetime(&cursor) {
                                cursor -= chrono::Duration::minutes(1);
                            }
                            let last_bar_available_time = cursor;
                            info!(
                                message = format!(
                                    "last_bar_available_time: {}, greater than: dk",
                                    last_bar_available_time
                                )
                            );

                            match historical_forex_data_crud.read_last_bar(&symbol, &1).await {
                                Ok(last_bar) => {
                                    if let Some(bar) = last_bar {
                                        info!(
                                            message = format!(
                                                "Local bar time: {} and last_bar_available_time: {}, Equal: {}",
                                                bar.time,
                                                last_bar_available_time,
                                                bar.time == last_bar_available_time
                                            )
                                        );
                                        if bar.time == last_bar_available_time {
                                            return Ok(());
                                        }
                                    }
                                    Self::get_historical_data(
                                        self.client.clone(),
                                        &self.historical_data_crud,
                                        &self.historical_options_data_crud,
                                        &self.historical_forex_data_crud,
                                        &contract,
                                        ibapi::market_data::historical::Duration::from_str("1 D").expect("Expected to be able to parse 1 D for market data historical data"),
                                        HIGHEST_GRANULARITY_TIMESTEP_FOREX,
                                        what_to_show,
                                        &true, // is_forex
                                        apply_batching
                                    )?;
                                }
                                Err(e) => tracing::error!(
                                    "Expected to be able to select from market_data.historical_data: {}",
                                    e
                                ),
                            };
                        }
                        return Ok(());
                    }
                }

                // Else, request all data required
                let duration_in_sec =
                    (Utc::now().with_timezone(&New_York) - earliest_datetime).num_seconds() as u64;

                let duration = if duration_in_sec > 86400 {
                    ibapi::market_data::historical::Duration::from_str(&format!(
                        "{} D",
                        (duration_in_sec / 60 / 60 / 24) as u32
                    ))
                    .expect(
                        "Expected Duration passed to historical_forex_data method to be correct!",
                    )
                } else {
                    ibapi::market_data::historical::Duration::from_str(&format!(
                        "{} S",
                        duration_in_sec
                    ))
                    .expect(
                        "Expected Duration passed to historical_forex_data method to be correct!",
                    )
                };
                info!("Requesting {duration:?} duration of forex data");
                let batching = if duration_in_sec > 7200 {
                    true
                } else {
                    *apply_batching
                };

                Self::get_historical_data(
                    self.client.clone(),
                    &self.historical_data_crud,
                    &self.historical_options_data_crud,
                    &self.historical_forex_data_crud,
                    &contract,
                    duration,
                    HIGHEST_GRANULARITY_TIMESTEP_FOREX,
                    what_to_show,
                    &true, // is_forex
                    &batching,
                )?;

                Ok(())
            }
            AssetType::Option => {
                let historical_data_crud = self.historical_options_data_crud.clone();
                let symbol = get_local_symbol(&contract);

                let n_rows_res = historical_data_crud
                    .has_at_least_n_rows_since(
                        symbol.as_str(),
                        contract.primary_exchange.as_str(),
                        contract.currency.as_str(),
                        contract.last_trade_date_or_contract_month.as_str(),
                        &contract.strike,
                        contract.multiplier.as_str(),
                        &OptionType::from_str(&contract.right)
                            .expect("Expected to be able to parse contract right"),
                        &earliest_datetime,
                        &((required_num_bars - 39).max(0) as u32),
                    )
                    .await;

                // Return if there is enough data
                if let Ok(passed) = n_rows_res {
                    if passed {
                        if is_trading_day_tdy {
                            let time_now = Utc::now().with_timezone(&New_York);
                            let last_bar_min = time_now.minute() - (time_now.minute() % 5);
                            let last_bar_available_time = time_now
                                .with_minute(last_bar_min)
                                .expect("Expected to get corrected last_bar_min")
                                - chrono::Duration::minutes(5);

                            if last_bar_available_time
                                > Utc::now()
                                    .with_timezone(&New_York)
                                    .with_hour(9)
                                    .unwrap()
                                    .with_minute(0)
                                    .unwrap()
                                    .with_second(0)
                                    .unwrap()
                                    .with_nanosecond(0)
                                    .unwrap()
                            {
                                match historical_data_crud
                                    .read_last_bar_of_contract(
                                        symbol.as_str(),
                                        contract.primary_exchange.as_str(),
                                        contract.currency.as_str(),
                                        contract.last_trade_date_or_contract_month.as_str(),
                                        &contract.strike,
                                        contract.multiplier.as_str(),
                                        &OptionType::from_str(&contract.right)
                                            .expect("Expected to be able to parse contract right"),
                                    )
                                    .await
                                {
                                    Ok(last_bar) => {
                                        if let Some(bar) = last_bar {
                                            if bar.time == last_bar_available_time {
                                                return Ok(());
                                            }
                                        }
                                        Self::get_historical_data(
                                            self.client.clone(),
                                            &self.historical_data_crud,
                                            &self.historical_options_data_crud,
                                            &self.historical_forex_data_crud,
                                            &contract,
                                            ibapi::market_data::historical::Duration::from_str("1 D").expect("Expected to be able to parse 1 D for market data historical data"),
                                            HIGHEST_GRANULARITY_TIMESTEP,
                                            what_to_show,
                                            &false, // is_forex
                                            apply_batching
                                        )?;
                                    }
                                    Err(e) => tracing::error!(
                                        "Expected to be able to select from market_data.historical_data: {e:?}",
                                    ),
                                };
                            }
                        }
                        return Ok(());
                    }
                }

                // Else, request all data required
                let duration_in_sec =
                    (Utc::now().with_timezone(&New_York) - earliest_datetime).num_seconds() as u64;
                let duration = if duration_in_sec > 86400 {
                    ibapi::market_data::historical::Duration::from_str(&format!(
                        "{} D",
                        (duration_in_sec / 60 / 60 / 24) as u32
                    ))
                    .expect("Expected Duration passed to historical_data method to be correct!")
                } else {
                    ibapi::market_data::historical::Duration::from_str(&format!(
                        "{} S",
                        duration_in_sec
                    ))
                    .expect("Expected Duration passed to historical_data method to be correct!")
                };

                Self::get_historical_data(
                    self.client.clone(),
                    &self.historical_data_crud,
                    &self.historical_options_data_crud,
                    &self.historical_forex_data_crud,
                    &contract,
                    duration,
                    HIGHEST_GRANULARITY_TIMESTEP,
                    what_to_show,
                    &false, // is_forex
                    apply_batching,
                )?;

                Ok(())
            }
            AssetType::CASH => {
                tracing::warn!(
                    "Encountered CASH AssetType in update_at_least_n_days_data:\nAssumed not part of strategy and is ignored!"
                );
                Ok(())
            }
            AssetType::Unknown => {
                tracing::warn!(
                    "Encountered Unknown AssetType in update_at_least_n_days_data:\nAssumed not part of strategy and is ignored!"
                );
                Ok(())
            }
        }
    }

    /// Opens a channel to asynchronously accept (Bar, Contract) data updates and perform upserts
    /// - for each timestep (in minutes) u subscribe to, the timestep will be triggered for each
    /// timing past 9:30am for the strategy
    /// - accordingly, this handles subscribe_to_data() updates such that the strategy
    /// on_bar_update() function ONLY has to handle updates to the TargetPosition in the database
    /// - Ideally, the order_engine is initialised with client id 0, consolidator with any other
    /// client id (so that market data subscriptions are handled in a separate thread)
    /// - Pass the client to be used to place orders for here
    pub fn begin_bar_listening(
        &self,
        weak_order_engine_borrowed: &Weak<OrderEngine>,
        weak_client_borrowed: &Weak<Client>,
        weak_consolidator_borrowed: &Weak<Consolidator>,
    ) {
        let (channel_killer, mut channel_killer_rcx) = tokio::sync::oneshot::channel();
        {
            self.bar_listener_channel_killer
                .lock()
                .expect("expected bar_listener_channel_killer lock to not be poisoned")
                .replace(channel_killer);
        }
        let (sender, mut receiver) = channel(32 * 50);
        {
            let mut bars_sender_lock = self.contract_update_sender.write();
            let bars_sender = bars_sender_lock.as_mut().expect("Expected bar_sender Mutex not to be poisoned while unlocking - begin_bar_listening");
            bars_sender.replace(sender);
            // self.contract_update_sender.as_mut().replace(sender);
        }
        let subscriptions = self.subscriptions.clone();
        let weak_order_engine = weak_order_engine_borrowed.clone();
        let weak_client = weak_client_borrowed.clone();
        let weak_consolidator = weak_consolidator_borrowed.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut channel_killer_rcx => {
                        tracing::info!("bar listening loop closed properly");
                        return;
                    }
                    Some(update) = receiver.recv() => {
                        let (contract, data_type, bar_time) = update;

                        let bar_ny = bar_time.with_timezone(&New_York);
                        let market_open = bar_ny
                            .date_naive()
                            .and_time(NaiveTime::from_hms_opt(9, 30, 0).unwrap());
                        let elapsed_min: u32 = if contract.security_type == SecurityType::ForexPair
                            || contract.security_type == SecurityType::CFD
                        {
                            (bar_ny
                                - Consolidator::fx_trading_day_start(&bar_ny.date_naive(), &New_York))
                            .num_minutes() as u32
                        } else {
                            (bar_ny.naive_local() - market_open).num_minutes() as u32
                        };

                        // Bounded access to subscriptions
                        let strats_sets = {
                            let subscription = subscriptions.read().expect(
                                "Expected Subscription read guard not to be poisoned in begin_bar_listening",
                            );
                            let contract_subscription = {
                                match subscription
                                .get(&UniqueSubscription {
                                    contract: HashContract {
                                        contract: contract.clone(),
                                    },
                                    data_type,
                                }) {
                                    Some(sub) => sub,
                                    None => {
                                        tracing::warn!("Strategy not found in hashmap of strategies - either cancelled already & this is final bar OR not registered properly!");
                                        continue;
                                    }
                                }
                            };

                            let mut strats_arcs = Vec::new();
                            contract_subscription.iter().filter(move |(timestep, _)| {
                                elapsed_min % *timestep == 0
                            }).for_each(|(_timestep, strats_arc)| strats_arcs.push(strats_arc.clone()));
                            strats_arcs
                        };

                        let mut seen_strats = HashSet::new();
                        let strats: Vec<StrategyEnum> = strats_sets
                            .iter()
                            .flat_map(|set_lock| {
                                // 1. Acquire the lock
                                let lock = set_lock.read().expect("Lock poisoned");

                                // 2. We must collect or process here because the lock drops
                                // when this flat_map iteration ends.
                                lock.iter()
                                    .filter_map(|strat| {
                                        let disc = mem::discriminant(strat);
                                        if seen_strats.insert(disc) {
                                            Some(strat.clone())
                                        } else {
                                            None
                                        }
                                    })
                                    .collect::<Vec<_>>() // Materialize the clones while lock is held
                            })
                            .collect();

                        tracing::info!("Running strategies now");
                        futures::future::join_all(strats.into_iter().map(|strategy| {
                            let weak_consolidator = weak_consolidator.clone();
                            let weak_order_engine = weak_order_engine.clone();
                            let weak_client = weak_client.clone();
                            let contract = contract.clone();

                            async move {
                                let consolidator_opt = weak_consolidator.upgrade();
                                if consolidator_opt.is_none() {
                                    return;
                                }
                                let consolidator = consolidator_opt.unwrap();
                                let bar_update_res =
                                    strategy.on_bar_update(&contract, &consolidator).await;

                                let ignore_contract_for_strat = match bar_update_res {
                                    Ok(updated) => {
                                        let (is_strat_updated, ignore_contract_for_strat) = updated;
                                        if !is_strat_updated {
                                            return;
                                        }
                                        ignore_contract_for_strat
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            message=%format!(
                                                "error in on_bar_update in strategy: {:?}: {e:?}",
                                                strategy.get_name()
                                            )
                                        );
                                        return;
                                    }
                                };

                                let asset_type = AssetType::from_str(&contract.security_type);
                                let order_engine_opt = weak_order_engine.upgrade();
                                if order_engine_opt.is_none() {
                                    tracing::warn!("Order engine died b4 placing orders for strategy");
                                    return;
                                }
                                let order_engine = order_engine_opt.unwrap();
                                order_engine.place_orders_for_strategy(
                                    &strategy,
                                    &contract,
                                    &weak_client,
                                    &asset_type,
                                    ignore_contract_for_strat,
                                    &weak_consolidator
                                );
                            }
                        }))
                        .await;
                        tracing::info!("joined all strategies");
                    }
                }
            }
        });
    }

    pub fn close_bar_listening_channel(&self) -> Result<(), String> {
        let mut sender = self.bar_listener_channel_killer.lock().map_err(|e| {
            format!("Error trying to acquire lock for bar_listener_channel_killer: {e:?}")
        })?;
        if sender.is_none() {
            Err(
                "No sender found in bar_listener_channel_killer when trying to close bar_listener_channel".to_string()
            )
        } else {
            if let Err(e) = sender.take().unwrap().send(()) {
                Err(format!(
                    "Error trying to send shutdown flag to bar_listener_channel: {e:?}"
                ))
            } else {
                Ok(())
            }
        }
    }

    pub fn cancel_subscription(
        &self,
        strategy: &StrategyEnum,
        contract: &Contract,
        timestep: &u32,
        data_type: RealtimeWhatToShow,
    ) {
        let symbol = get_local_symbol(&contract);
        let unique_sub = UniqueSubscription {
            contract: HashContract {
                contract: contract.clone(),
            },
            data_type,
        };

        {
            let mut subscriptions = self.subscriptions.write().expect("Expected to be able to acquire lock for subscriptions in Consolidator.subscribe_to_data");
            if subscriptions.contains_key(&unique_sub)
                && subscriptions[&unique_sub].contains_key(&timestep)
                && subscriptions[&unique_sub][&timestep]
                    .read()
                    .expect("Expected read lock to not be poisoned")
                    .contains(strategy)
            {
                subscriptions
                    .get_mut(&unique_sub)
                    .unwrap()
                    .get_mut(&timestep)
                    .unwrap()
                    .write()
                    .expect("Expected write lock to not be poisoned")
                    .remove(strategy);
                if subscriptions[&unique_sub][&timestep]
                    .read()
                    .expect("Expected read lock not to be poisoned")
                    .len()
                    > 0
                {
                    tracing::info!(
                        message=%format!(
                            "Cancelled subscription for {} for strategy ({}), but some other strategy is still subscribed so still receiving data!",
                            symbol,
                            strategy.get_name()
                        )
                    );
                    return;
                }
                subscriptions
                    .get_mut(&unique_sub)
                    .unwrap()
                    .remove(&timestep);
                if subscriptions[&unique_sub].len() > 0 {
                    tracing::info!(
                        message=%format!(
                            "Cancelled subscription for {} for strategy ({}), but some other strategy is still subscribed so still receiving data!",
                            symbol,
                            strategy.get_name()
                        )
                    );
                    return;
                };
                tracing::info!(
                    message=%format!(
                        "Subscription for {} cancelled completely, last cancelled for strategy ({}), no other strategy registered",
                        symbol,
                        strategy.get_name()
                    )
                );
                subscriptions.remove(&unique_sub);
            }

            {
                let mut subscriptions_handler = self
                    .subscriptions_handler
                    .lock()
                    .expect("Failed to retrieve lock for subscriptions_handler");
                subscriptions_handler.remove(&unique_sub);
            }
        }
    }

    pub fn cancel_all_subscriptions(&self) {
        tracing::info!("Cancelling all subscriptions!");
        {
            let mut subs = self
                .subscriptions
                .write()
                .expect("Expected subscriptions lock not to be poisoned");
            subs.drain();
        }
        {
            let mut sub_handler = self
                .subscriptions_handler
                .lock()
                .expect("Expected subscriptions_handler lock not to be poisoned");
            sub_handler.drain();
        }
    }

    /// Opens a channel, spawns an async task to await bar updates,
    /// then subscribes to the blocking subscription in a new OS thread
    /// - Requests 5 second real time bars to build 5 minute bars
    /// - Times out if no bar received at least every 20 seconds -> Triggering a re-subscription
    /// - NOTE: this function MUST ONLY be called AFTER begin_bar_listening as begin_bar_listening opens
    ///   the channel required
    /// - FN NOTE: saves granularity of 5-min bars for stocks, futures, options, and 1-min bars for
    /// forex
    pub fn subscribe_to_data(
        &self,
        strategy: &StrategyEnum,
        contract: &Contract,
        timestep: &u32,
        data_type: RealtimeWhatToShow,
    ) -> () {
        let highest_granularity_timestep_in_seconds = if contract.security_type
            == SecurityType::ForexPair
            || contract.security_type == SecurityType::CFD
        {
            60
        } else {
            300
        };
        let symbol = get_local_symbol(&contract);
        let hash_contract = HashContract {
            contract: contract.clone(),
        };
        let unique_sub = UniqueSubscription {
            contract: hash_contract.clone(),
            data_type,
        };

        {
            let mut subscriptions = self.subscriptions.write().expect("Expected to be able to acquire lock for subscriptions in Consolidator.subscribe_to_data");
            if subscriptions.contains_key(&unique_sub)
                && subscriptions[&unique_sub].contains_key(&timestep)
                && subscriptions[&unique_sub][&timestep]
                    .read()
                    .expect("Expected read lock to not be poisoned")
                    .contains(&strategy)
            {
                info!("Already subscribed to market data for {symbol:?}");
                return;
            }

            let mut is_non_existing_entry = false;
            if !subscriptions.contains_key(&unique_sub) {
                subscriptions.insert(unique_sub.clone(), BTreeMap::new());
                is_non_existing_entry = true;
            }
            if !subscriptions[&unique_sub].contains_key(&timestep) {
                subscriptions
                    .get_mut(&unique_sub)
                    .unwrap()
                    .insert(timestep.clone(), Arc::new(RwLock::new(BTreeSet::new())));
            }
            subscriptions
                .get_mut(&unique_sub)
                .unwrap()
                .get_mut(&timestep)
                .unwrap()
                .write()
                .expect("Expected write lock not to be poisoned")
                .insert(strategy.clone());

            // Spawn thread only if entry didn't exist before, else thread will handle updated data
            // accordingly already
            if !is_non_existing_entry {
                info!(
                    message=%format!(
                        "Already subscribed to market data for {}, but ig not for this strategy {:?}",
                        symbol, &strategy
                    )
                );
                return;
            }
        }
        {
            if !self
                .contract_coordinator
                .contains_contract(&unique_sub.contract)
            {
                tracing::warn!(message=%format!("Contract subscribed to in subscribe_to_data() not pre-found in strategies initialised! Adding schedule now! Contract: {:?}", &unique_sub.contract));
                if let Err(e) = self
                    .contract_coordinator
                    .add_schedule(&unique_sub.contract.contract)
                {
                    tracing::error!(message=%format!("Error trying to add schedule of new contract ({:?}): {e:?}", &unique_sub.contract))
                };
            }
        }

        let contract_sub_lock = {
            let mut subscriptions_handler = self
                .subscriptions_handler
                .lock()
                .expect("Failed to retrieve lock for subscriptions_handler");
            // need to ensure that if it already contains the key,
            // you do not override the Arc being held by the previous subscriptions
            if subscriptions_handler.contains_key(&unique_sub) {
                tracing::error!(
                    "Error during subscription: entry exists in subscriptions_handler but not in subscriptions"
                );
                return;
            } else {
                let sub_lock = Arc::new(());
                subscriptions_handler.insert(unique_sub.clone(), sub_lock.clone());
                sub_lock
            }
        };

        // Update forex_subscriptions_tracker appropriately
        if contract.security_type == SecurityType::ForexPair
            || contract.security_type == SecurityType::CFD
        {
            async fn ok(
                forex_subscriptions_tracker: Arc<
                    tokio::sync::Mutex<HashMap<String, HashMap<DateTime<Utc>, ForexBarCollector>>>,
                >,
                symbol: String,
            ) {
                forex_subscriptions_tracker
                    .lock()
                    .await
                    .insert(symbol, HashMap::new());
            }
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(ok(self.forex_subscriptions_tracker.clone(), symbol.clone()))
            });
        }
        info!(
            message=%format!(
                "Initiating subscription to market data for new contract ({:?}, {:?}, currency: {:?}, security_type: {:?}) in a new blocking thread.",
                contract.symbol, contract.primary_exchange, contract.currency, contract.security_type
            )
        );

        // Highest Granularity - 5 min
        let (bar_sender, mut rcx) =
            channel::<(DateTime<Utc>, f64, f64, f64, f64, f64, bool, bool)>(100);
        let contract_update_sender = {
            self.contract_update_sender
                .read()
                .expect("Expected to be able to acquire lock for contract_update_sender")
                .as_ref()
                .expect("Expected contract_update_sender to already have been initialised")
                .clone()
        };
        let historical_data_crud = self.historical_data_crud.clone();
        let historical_forex_data_crud = self.historical_forex_data_crud.clone();
        let historical_options_data_crud = self.historical_options_data_crud.clone();
        let forex_subscriptions_tracker = self.forex_subscriptions_tracker.clone();
        let cloned_client = self.client.clone();
        let cloned_contract = contract.clone();
        let cloned_data_type = data_type.clone();
        tokio::spawn(async move {
            while let Some(new_5min_bar) = rcx.recv().await {
                let has_first_bar = new_5min_bar.6;
                let is_first_bar_sent = new_5min_bar.7;
                if !has_first_bar || is_first_bar_sent {
                    if let Err(e) = Self::get_historical_data(
                        cloned_client.clone(),
                        &historical_data_crud,
                        &historical_options_data_crud,
                        &historical_forex_data_crud,
                        &cloned_contract,
                        ibapi::market_data::historical::Duration::seconds(330),
                        if highest_granularity_timestep_in_seconds == 60 {
                            ibapi::market_data::historical::BarSize::Min
                        } else {
                            ibapi::market_data::historical::BarSize::Min5
                        },
                        HistoricalWhatToShow::Trades,
                        &(cloned_contract.security_type == SecurityType::ForexPair
                            || cloned_contract.security_type == SecurityType::CFD),
                        &false,
                    ) {
                        tracing::error!(
                            "Error encountered while fetching historical data for missed bar: {e:?}",
                        );
                    };
                    tracing::info!("Sending to contract_update_sender");
                    if (cloned_contract.security_type != SecurityType::ForexPair
                        && cloned_contract.security_type != SecurityType::CFD)
                        || matches!(data_type, RealtimeWhatToShow::Ask)
                    {
                        if let Err(e) = contract_update_sender
                            .send((
                                cloned_contract.clone(),
                                cloned_data_type.clone(),
                                new_5min_bar.0,
                            ))
                            .await
                        {
                            tracing::error!(
                                message=%format!(
                                    "Error occurred while sending bar update to channel for {}:{} at {}: {}",
                                    cloned_contract.security_type,
                                    cloned_contract.symbol,
                                    new_5min_bar.0,
                                    e
                                )
                            );
                        }
                        continue;
                    };
                }

                if cloned_contract.security_type == SecurityType::ForexPair
                    || cloned_contract.security_type == SecurityType::CFD
                {
                    let symbol = get_local_symbol(&cloned_contract);
                    let mut unlocked_forex_subscriptions_tracker =
                        forex_subscriptions_tracker.lock().await;
                    let contract_updates = unlocked_forex_subscriptions_tracker.get_mut(&symbol).expect(format!("Expected {symbol:?} to already have been added to forex_subscriptions_tracker").as_str());

                    if !contract_updates.contains_key(&new_5min_bar.0) {
                        contract_updates.insert(
                            new_5min_bar.0.clone(),
                            ForexBarCollector {
                                bid: None,
                                ask: None,
                            },
                        );
                    }
                    if matches!(data_type, RealtimeWhatToShow::Bid) {
                        contract_updates
                            .get_mut(&new_5min_bar.0)
                            .unwrap()
                            .bid
                            .replace(ForexBar {
                                open: new_5min_bar.1,
                                high: new_5min_bar.2,
                                low: new_5min_bar.3,
                                close: new_5min_bar.4,
                                volume: new_5min_bar.5,
                            });
                    } else if matches!(data_type, RealtimeWhatToShow::Ask) {
                        contract_updates
                            .get_mut(&new_5min_bar.0)
                            .unwrap()
                            .ask
                            .replace(ForexBar {
                                open: new_5min_bar.1,
                                high: new_5min_bar.2,
                                low: new_5min_bar.3,
                                close: new_5min_bar.4,
                                volume: new_5min_bar.5,
                            });
                    } else {
                        tracing::error!(
                            "got contract update data for {symbol:?} but unknown data_type"
                        );
                    }

                    if contract_updates[&new_5min_bar.0].bid.is_some()
                        && contract_updates[&new_5min_bar.0].ask.is_some()
                    {
                        let bid = contract_updates[&new_5min_bar.0].bid.as_ref().unwrap();
                        let ask = contract_updates[&new_5min_bar.0].ask.as_ref().unwrap();
                        Self::on_forex_bar_update(
                            &historical_forex_data_crud,
                            &contract_update_sender,
                            &cloned_contract,
                            new_5min_bar.0.clone(),
                            bid.open,
                            bid.high,
                            bid.low,
                            bid.close,
                            bid.volume,
                            ask.open,
                            ask.high,
                            ask.low,
                            ask.close,
                            ask.volume,
                            RealtimeWhatToShow::Ask,
                            true, // is_full_bar_updated
                        )
                        .await;
                        contract_updates.remove(&new_5min_bar.0);
                    }
                    continue;
                }
                Self::on_bar_update(
                    &historical_data_crud,
                    &historical_options_data_crud,
                    &contract_update_sender,
                    &cloned_contract,
                    new_5min_bar.0,
                    new_5min_bar.1,
                    new_5min_bar.2,
                    new_5min_bar.3,
                    new_5min_bar.4,
                    new_5min_bar.5,
                    data_type,
                )
                .await;
            }
            tracing::info!("5-min bar receival loop closed!");
        });

        let current_price_lock = Arc::new(RwLock::new(None));
        {
            let mut live_data = self
                .live_data
                .write()
                .expect("Expected live data write lock not to be poisoned");
            live_data.insert(hash_contract.clone(), Arc::downgrade(&current_price_lock));
        }
        let mut collected_bars_arc = Arc::new(VecDeque::<Bar>::new());
        let weak_client = Arc::downgrade(&self.client);
        let contract = contract.clone();
        let cloned_bar_sender = bar_sender.clone();
        let contract_coordinator = self.contract_coordinator.clone();
        thread::spawn(move || {
            let mut last_error_time: Option<Instant> = None;
            let hashed_contract = HashContract {
                contract: contract.clone(),
            };
            let weak_current_price_lock = Arc::downgrade(&current_price_lock);
            macro_rules! get_next_contract_available {
                    () => {
                        {
                            match contract_coordinator.is_trading(&hashed_contract, &Utc::now()) {
                                Ok(is_trading_rn) => {
                                    if !is_trading_rn {
                                        tracing::info!(
                                            "Contract ({contract:?}) is currently not trading, sleeping until available rn!"
                                        );
                                        let now = Utc::now();
                                        let next_trading_time = {
                                            match contract_coordinator
                                                .get_next_earliest_available_data(std::slice::from_ref(&&hashed_contract), &now)
                                            {
                                                Ok(dt) => dt,
                                                Err(e) => {
                                                    tracing::error!(
                                                        "Couldn't get earliest dt for {hashed_contract:?}: {e:?}"
                                                    );
                                                    now
                                                }
                                            }
                                        };
                                        if next_trading_time > now {
                                            // tracing::info!("sleeping now for {:?}", next_trading_time - now);
                                            (next_trading_time - now)
                                                .to_std()
                                                .expect("Couldn't convert timedelta to duration")
                                        } else {
                                            Duration::from_secs(0)
                                        }
                                    } else {
                                        Duration::from_secs(0)
                                    }
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "Couldn't get schedule of contract ({contract:?}): {e:?}"
                                    );
                                    Duration::from_secs(0)
                                }
                            }
                        }
                    };
                }

            loop {
                let duration_till_next_contract_available = get_next_contract_available!();
                std::thread::sleep(duration_till_next_contract_available);

                tracing::info!("Finished sleeping for contract: {contract:?}");
                let subscription_res = {
                    let client_opt = weak_client.upgrade();
                    if client_opt.is_none() {
                        return;
                    }
                    let client = client_opt.unwrap();
                    client.realtime_bars(
                        &contract,
                        ibapi::prelude::RealtimeBarSize::Sec5,
                        data_type,
                        ibapi::prelude::TradingHours::Regular,
                    )
                };
                match subscription_res {
                    Ok(subscription) => {
                        let mut is_first_bar_sent = true;
                        loop {
                            {
                                if Arc::strong_count(&contract_sub_lock) == 1 {
                                    tracing::info!("Subscription for {contract:?} cancelled!");
                                    return;
                                }
                            }
                            match subscription.next_timeout(Duration::from_secs(20)) {
                                Some(bar) => {
                                    let was_info_sent = Self::on_new_5sec_bar(
                                        &mut collected_bars_arc,
                                        &weak_current_price_lock,
                                        bar,
                                        &cloned_bar_sender,
                                        highest_granularity_timestep_in_seconds,
                                        is_first_bar_sent,
                                    );
                                    // just to optimise branch prediction LOL
                                    if is_first_bar_sent {
                                        if was_info_sent {
                                            is_first_bar_sent = false;
                                        }
                                    }
                                }
                                None => {
                                    if let Some(e) = subscription.error() {
                                        match e {
                                            _ => {
                                                tracing::warn!(
                                                    message=%format!(
                                                        "Real time bars for {} cancelled for reason: {e:?}",
                                                        contract.symbol
                                                    )
                                                );
                                            }
                                        }
                                        last_error_time = Some(Instant::now());
                                    }
                                    {
                                        match contract_coordinator
                                            .is_trading(&hashed_contract, &Utc::now())
                                        {
                                            Ok(is_trading_rn) => {
                                                if is_trading_rn {
                                                    tracing::warn!(
                                                        "Currently trading session rn and timed out waiting for next bar for contract: {} - Trying a re-subscription",
                                                        contract.symbol
                                                    )
                                                }
                                            }
                                            Err(e) => {
                                                tracing::warn!(
                                                    "Contract ({contract:?}) timed out and couldn't get info: {e:?}"
                                                )
                                            }
                                        }
                                    }

                                    subscription.cancel();
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            message=%format!(
                                "Real time request for {} failed:\n{}. will retry again", contract.symbol, e
                            )
                        );
                        last_error_time = Some(Instant::now());
                    }
                }
                // Throttle retry to 20 seconds since last error
                if let Some(last_error) = last_error_time {
                    let elapsed = last_error.elapsed();
                    if elapsed < Duration::from_secs(20) {
                        let wait_duration = Duration::from_secs(20) - elapsed;
                        tracing::warn!(
                            message=%format!(
                                "Error occurred trying to resubscribe to contract. Waiting {:?} before retrying subscription for {}",
                                wait_duration,
                                contract.symbol
                            )
                        );
                        std::thread::sleep(wait_duration);
                    }
                }
            }
        });
    }

    /// Spawns a new OS thread to process the 5 second bars from the subscription
    /// - is called by the channel instead of directly since calling directly would be on the
    /// separate OS kernel thread which doesn't have a tokio runtime
    /// - Note: multithreading should be fine because each bar for each contract is separated by 5
    /// sec times which should be sufficient time for this whole check to complete
    /// - highest_granularity_timestep_in_seconds: granularity of bars being stored in the table
    ///     - i.e. for stocks, 5 min / 300 sec
    ///     - i.e. for forex, 1 min / 60 sec
    /// - MINOR_BUG: if data starts being loaded within a bar, start of bar data is ignored/lost
    fn on_new_5sec_bar(
        collected_bars_arc: &mut Arc<VecDeque<Bar>>,
        current_price_lock: &Weak<RwLock<Option<f64>>>,
        bar: Bar,
        bar_sender: &Sender<(DateTime<Utc>, f64, f64, f64, f64, f64, bool, bool)>,
        highest_granularity_timestep_in_seconds: u32,
        is_first_bar_sent: bool,
    ) -> bool {
        let highest_granularity_timestep_in_seconds_i64 =
            highest_granularity_timestep_in_seconds as i64;

        // only works if strong count is 1 - but shldn't ever stall for too long
        let collected_bars = Arc::get_mut(collected_bars_arc)
            .expect("Expected collected_bars_arc to only have a single Arc reference");
        collected_bars.push_back(bar.clone());

        let latest_bar_timestamp = &bar.date.unix_timestamp();
        let next_bar_timestamp = latest_bar_timestamp + 5;

        let latest_bar_no = latest_bar_timestamp
            - (latest_bar_timestamp % highest_granularity_timestep_in_seconds_i64);
        let next_bar_no =
            next_bar_timestamp - (next_bar_timestamp % highest_granularity_timestep_in_seconds_i64);
        if latest_bar_no == next_bar_no {
            return false;
        }

        while !collected_bars.is_empty() {
            // Process first bar first
            let first_bar = &collected_bars.pop_front().unwrap();
            let bar_time = first_bar.date.unix_timestamp();
            let prev_bar_time = bar_time - 5;
            let bar_no = bar_time - (bar_time % highest_granularity_timestep_in_seconds_i64);
            let prev_bar_no =
                prev_bar_time - (prev_bar_time % highest_granularity_timestep_in_seconds_i64);
            let has_first_bar = prev_bar_no != bar_no;

            let (open, mut high, mut low, mut close, mut volume) = (
                first_bar.open,
                first_bar.high,
                first_bar.low,
                first_bar.close,
                first_bar.volume,
            );

            loop {
                if collected_bars.is_empty() {
                    break;
                }
                let this_bar_date = &collected_bars.front().unwrap().date.unix_timestamp();
                let this_bar_no =
                    this_bar_date - (this_bar_date % highest_granularity_timestep_in_seconds_i64);
                if bar_no != this_bar_no {
                    break;
                }

                let first_bar = &collected_bars.pop_front().unwrap();
                high = f64::max(high, first_bar.high);
                low = f64::min(low, first_bar.low);
                close = first_bar.close;
                volume += first_bar.volume;
            }

            tracing::info!("Has first bar: {has_first_bar:?}");
            tracing::info!("Is first bar sent: {is_first_bar_sent:?}");

            if let Err(e) = bar_sender.blocking_send((
                Utc.timestamp_opt(bar_no, 0).unwrap(),
                open,
                high,
                low,
                close,
                volume,
                has_first_bar,
                is_first_bar_sent,
            )) {
                tracing::error!("Error occurred while trying to send new 5 min bar: {e:?}");
            };

            let cloned_weak_curr_price_lock = current_price_lock.clone();
            thread::spawn(move || match cloned_weak_curr_price_lock.upgrade() {
                Some(curr_price_lock) => {
                    let mut curr_price = curr_price_lock
                        .write()
                        .expect("Expected write lock for current_price_lock to not be poisoned");
                    curr_price.replace(close);
                }
                None => {
                    return;
                }
            });
        }
        true
    }

    /// Simply updates the 5 minute bar in the appropriate database
    /// Add Duration::minutes(5)
    /// - Assumption: Bar updates every 5 minutes
    async fn on_bar_update(
        historical_data_crud: &HistoricalDataCRUD,
        historical_options_data_crud: &HistoricalOptionsDataCRUD,
        sender: &Sender<(Contract, RealtimeWhatToShow, DateTime<chrono::Utc>)>,
        contract: &Contract,
        time: DateTime<chrono::Utc>,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        data_type: RealtimeWhatToShow,
    ) {
        let symbol = get_local_symbol(&contract);
        match AssetType::from_str(&contract.security_type) {
            AssetType::Option => {
                match historical_options_data_crud
                    .create_or_update(
                        &HistoricalOptionsDataPrimaryKeys {
                            stock: symbol.clone(),
                            primary_exchange: contract.primary_exchange.as_str().to_string(),
                            currency: contract.currency.as_str().to_string(),
                            expiry: contract.last_trade_date_or_contract_month.clone(),
                            strike: contract.strike.clone(),
                            multiplier: contract.multiplier.clone(),
                            option_type: OptionType::from_str(&contract.right)
                                .unwrap_or_else(|e| panic!("{}", e)),
                            time: time,
                        },
                        &HistoricalOptionsDataUpdateKeys {
                            open: Some(open),
                            high: Some(high),
                            low: Some(low),
                            close: Some(close),
                            volume: Some(
                                Decimal::from_f64(volume * 100.0)
                                    .expect("Expected to be able to parse f64 to Decimal"),
                            ),
                        },
                    )
                    .await
                {
                    Ok(_) => {
                        if let Err(e) = sender.send((contract.clone(), data_type, time)).await {
                            tracing::error!(
                                message=%format!(
                                    "Error occurred while sending bar update to channel for {}:{} at {}: {}",
                                    contract.security_type,
                                    symbol,
                                    time,
                                    e
                                )
                            );
                        }
                    }
                    Err(e) => tracing::error!(
                        "Error occurred while trying to insert new bar to HistoricalOptionsData: {e:?}"
                    ),
                };
            }
            AssetType::Stock | AssetType::Future => {
                match historical_data_crud
                    .create_or_update(
                        &HistoricalDataPrimaryKeys {
                            stock: symbol.clone(),
                            primary_exchange: contract.primary_exchange.to_string(),
                            currency: contract.currency.to_string(),

                            time: time,
                        },
                        &HistoricalDataUpdateKeys {
                            open: Some(open),
                            high: Some(high),
                            low: Some(low),
                            close: Some(close),
                            volume: Some(
                                Decimal::from_f64(volume * 100.0)
                                    .expect("Expected to be able to parse f64 to Decimal"),
                            ),
                        },
                    )
                    .await
                {
                    Ok(_) => {
                        if let Err(e) = sender.send((contract.clone(), data_type, time)).await {
                            tracing::error!(
                                message=%format!(
                                    "Error occurred while sending bar update to channel for {}:{} at {}: {}",
                                    contract.security_type,
                                    symbol,
                                    time,
                                    e
                                )
                            );
                        }
                    }
                    Err(e) => tracing::error!(
                        "Error occurred while trying to insert new bar to HistoricalStockData: {e:?}"
                    ),
                };
            }
            AssetType::ForexPair | AssetType::CFD => {
                tracing::warn!("Updating forex pair or cfd thru wrong branch");
            }
            _ => {
                tracing::error!(
                    message=%format!(
                        "Received bar update for unknown security_type: {} ({})",
                        contract.security_type,
                        symbol
                    )
                )
            }
        }
    }

    /// Simply updates the 5 minute bar in the appropriate database
    /// Add Duration::minutes(5)
    /// - Assumption: Bar updates every 5 minutes
    async fn on_forex_bar_update(
        historical_forex_data_crud: &HistoricalForexDataCRUD,
        sender: &Sender<(Contract, RealtimeWhatToShow, DateTime<chrono::Utc>)>,
        contract: &Contract,
        time: DateTime<chrono::Utc>,
        bid_open: f64,
        bid_high: f64,
        bid_low: f64,
        bid_close: f64,
        _bid_v: f64,
        ask_open: f64,
        ask_high: f64,
        ask_low: f64,
        ask_close: f64,
        _ask_v: f64,
        data_type: RealtimeWhatToShow,
        is_full_bar_updated: bool,
    ) {
        let symbol = get_local_symbol(&contract);

        match AssetType::from_str(&contract.security_type) {
            AssetType::ForexPair | AssetType::CFD => {
                match historical_forex_data_crud
                    .create_or_update(
                        &HistoricalForexDataPrimaryKeys {
                            pair: symbol.clone(),
                            time: time,
                        },
                        &HistoricalForexDataUpdateKeys {
                            bid_open: Some(bid_open),
                            bid_high: Some(bid_high),
                            bid_low: Some(bid_low),
                            bid_close: Some(bid_close),
                            ask_open: Some(ask_open),
                            ask_high: Some(ask_high),
                            ask_low: Some(ask_low),
                            ask_close: Some(ask_close),
                        },
                    )
                    .await
                {
                    Ok(_is_bar_complete) => {
                        if !is_full_bar_updated {
                            return;
                        }

                        if let Err(e) = sender.send((contract.clone(), data_type, time)).await {
                            tracing::error!(
                                message=%format!(
                                    "Error occurred while sending bar update to channel for {}:{} at {}: {}",
                                    contract.security_type,
                                    contract.symbol,
                                    time,
                                    e
                                )
                            );
                        }
                    }
                    Err(e) => tracing::error!(
                        "Error occurred while trying to insert new bar to HistoricalStockData: {e:?}",
                    ),
                };
            }

            _ => {
                tracing::error!(
                    message=%format!(
                        "Received bar update for unknown security_type: {} ({})",
                        contract.security_type,
                        symbol
                    )
                )
            }
        }
    }
}
