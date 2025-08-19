use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    f64,
    str::FromStr,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use chrono::{DateTime, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::America::New_York;
use ibapi::{
    Client,
    client::Subscription,
    market_data::realtime::Bar,
    prelude::{Contract, HistoricalWhatToShow, RealtimeWhatToShow, SecurityType, TickTypes},
};
use moka::sync::Cache;
use nyse_holiday_cal::HolidayCal;
use rust_decimal::{Decimal, prelude::FromPrimitive};
use sqlx::PgPool;
use tokio::sync::mpsc::{Sender, channel};
use tracing::info;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            AssetType, HistoricalDataPrimaryKeys, HistoricalDataUpdateKeys,
            HistoricalOptionsDataPrimaryKeys,
            HistoricalOptionsDataUpdateKeys, OptionType,
        },
        models_crud::{
            historical_data::{
                HistoricalDataCRUD, get_specific_historical_data_crud,
            },
            historical_options_data::{
                HistoricalOptionsDataCRUD, 
                get_specific_historical_options_data_crud,
            },
        },
    },
    execution::order_engine::OrderEngine,
    strategy::strategy::StrategyExecutor,
    unlock,
};

pub struct Consolidator<T: StrategyExecutor> {
    pub pool: PgPool,
    client: Arc<Client>,
    // Stock, Primary Exchange
    subscriptions: Arc<Mutex<HashMap<(String, String), HashMap<u32, BTreeSet<T>>>>>,

    live_data: Arc<Mutex<HashMap<(String, String), Arc<Mutex<VecDeque<Bar>>>>>>,
    past_data: Arc<Cache<(String, String), f64>>,
    past_data_vwap: Arc<Cache<(String, String), f64>>,

    contract_update_sender: Arc<Mutex<Option<Sender<(Contract, DateTime<Utc>)>>>>,

    historical_data_crud: HistoricalDataCRUD,
    historical_options_data_crud: HistoricalOptionsDataCRUD,
    is_historical_data_crud_channel_opened: Arc<tokio::sync::Mutex<bool>>,
    is_historical_options_data_crud_channel_opened: Arc<tokio::sync::Mutex<bool>>,
}

impl<'a, T: StrategyExecutor + 'static> Consolidator<T> {
    pub fn new(pool: PgPool, client: Arc<Client>) -> Self {
        let ttl = Duration::from_secs(20);
        let max_capacity = 10;

        Self {
            pool: pool.clone(),
            client: client,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),

            live_data: Arc::new(Mutex::new(HashMap::new())),
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
            contract_update_sender: Arc::new(Mutex::new(None)),

            historical_data_crud: get_specific_historical_data_crud(pool.clone()),
            historical_options_data_crud: get_specific_historical_options_data_crud(pool),
            is_historical_data_crud_channel_opened: Arc::new(tokio::sync::Mutex::new(false)),
            is_historical_options_data_crud_channel_opened: Arc::new(tokio::sync::Mutex::new(false)),
        }
    }

    /// Helper function to extract the price of contract from the ticker received
    pub fn _extract_price(
        &self,
        tick: TickTypes,
        contract: &Contract,
        subscription: &Subscription<'_, TickTypes>,
    ) -> Result<f64, String> {
        match tick {
            ibapi::prelude::TickTypes::Price(tick_price) => return Ok(tick_price.price),
            ibapi::prelude::TickTypes::SnapshotEnd => {
                subscription.cancel();

                tracing::error!(
                    "Got unknown ticker from request for market data: {}",
                    contract.symbol
                );
                return Err(format!(
                    "Got unknown ticker from request for market data: {}",
                    contract.symbol
                ));
            }
            _ => {
                tracing::error!(
                    "Got unknown ticker from request for market data: {}",
                    contract.symbol
                );
                return Err(format!(
                    "Got unknown ticker from request for market data: {}",
                    contract.symbol
                ));
            }
        }
    }

    /// Gets the current price of the contract from IBKR
    /// - if currently subscribed to their live data - unlocks and returns it
    ///     - Note: Each live_data subscription is wrapped behind a std::sync::Mutex so this
    ///     function could be potentially blocking for a longer period of time than expected
    /// - if requested the data in the last 20s, returns that
    /// - else, requests from IBKR
    pub fn get_current_price(&self, contract: Contract, vwap: bool) -> Result<f64, String> {
        {
            // If currently tracking, then j return latest data
            let live_data = unlock!(
                self.live_data,
                "live_data",
                "Consolidator.get_current_price"
            );
            if !vwap && live_data.contains_key(&(contract.symbol.clone(), contract.primary_exchange.clone())) {
                let live_data_for_contract = unlock!(
                    live_data.get(&(contract.symbol.clone(), contract.primary_exchange.clone())).unwrap(),
                    format!("live_data.{}", &contract.symbol),
                    "Consolidator"
                );
                if let Some(latest_bar) = live_data_for_contract.back() {
                    return Ok(latest_bar.close);
                }
            }
        }

        // If recently requested
        if vwap {
            if self.past_data_vwap.contains_key(&(contract.symbol.clone(), contract.primary_exchange.clone())) {
                return Ok(self.past_data_vwap.get(&(contract.symbol.clone(), contract.primary_exchange.clone())).expect(
                    format!("past_data_vwap lost value for {}", contract.symbol).as_str(),
                ));
            }
        } else {
            if self.past_data.contains_key(&(contract.symbol.clone(), contract.primary_exchange.clone())) {
                return Ok(self
                    .past_data
                    .get(&(contract.symbol.clone(), contract.primary_exchange.clone()))
                    .expect(format!("past_data lost value for {}", contract.symbol.clone()).as_str()));
            }
        }

        // Request data as last resort
        let subscription = self
            .client
            .market_data(&contract, if vwap { &["233"] } else { &[] }, true, false)
            .map_err(|e| {
                tracing::error!("Failed to request current price from IBKR: {}", e);
                format!("Failed to request current price from IBKR: {}", e)
            })?;

        if let Some(latest_tick) = subscription.next() {
            let price = self._extract_price(latest_tick, &contract, &subscription)?;
            if vwap {
                self.past_data_vwap
                    .insert((contract.symbol.clone(), contract.primary_exchange.clone()), price);
            } else {
                self.past_data.insert((contract.symbol.clone(), contract.primary_exchange.clone()), price);
            }

            return Ok(price);
        }

        Err(format!(
            "Could not get current price with market data request for {}",
            contract.symbol
        ))
    }

    pub fn validate_contract(&self, contract: &Contract) -> Option<Contract> {
        match self.client.contract_details(contract) {
            Ok(validated_contracts) => {
                if validated_contracts.len() == 0 { return None; }
                return Some(validated_contracts.first().unwrap().contract.clone());
            }
            Err(e) => {
                tracing::error!("Error occurred requesting contract details for {}: {}", contract.symbol, e);
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
    /// - today inclusive: 1 refers to just today/most recent trading days
    ///      - Note: if days == 1 and time now is before 9:30, nth will be updated
    /// - gives leeway of one half day before requesting full data: 39 bars less
    /// - Always checks for most recent trading day at least
    /// - apply_batching bool should ONLY be set to true if you have opened the relevant crud
    /// channels beforehand - it WILL fail otherwise. After usage, remember to close the channel to
    /// free up the postgres connection
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
        days: u32,
        apply_batching: bool,
    ) -> Result<(), String> {
        let mut required_num_bars = 0;
        let mut days_counter = 0;
        let mut earliest_datetime = Utc::now().with_timezone(&New_York);
        let naive_date_tdy = Utc::now().with_timezone(&New_York).date_naive();
        let mut is_trading_day_tdy = false;
        for day in (Utc::now().with_timezone(&New_York) + chrono::Duration::days(1))
            .date_naive()
            .busday_iter()
            .rev()
        {
            days_counter += 1;
            if days_counter == 1 {
                if naive_date_tdy == day {
                    is_trading_day_tdy = true;
                    required_num_bars += (Utc::now().with_timezone(&New_York)
                        - Utc::now()
                            .with_timezone(&New_York)
                            .with_hour(9)
                            .unwrap()
                            .with_minute(0)
                            .unwrap()
                            .with_second(0)
                            .unwrap()
                            .with_nanosecond(0)
                            .unwrap())
                    .num_minutes()
                        / 5;
                }
            }
            if days_counter == days {
                let naive_earliest_datetime =
                    &day.and_time(NaiveTime::from_hms_opt(9, 0, 0).expect(
                        "Expected supplied datetime in update_at_least_n_days_data to be valid",
                    ));
                let earliest_datetime_opt = New_York
                    .from_local_datetime(naive_earliest_datetime)
                    .single();
                earliest_datetime = earliest_datetime_opt.expect(
                    "Could not convert NaiveDateTime to TzDateTime in update_at_least_n_days_data",
                );
                break;
            }

            required_num_bars += 78;
        }

        match AssetType::from_str(contract.security_type.clone()) {
            AssetType::Stock => {
                let historical_data_crud = self.historical_data_crud.clone();

                let n_rows_res = historical_data_crud
                    .has_at_least_n_rows_since(
                        contract.symbol.clone(),
                        contract.primary_exchange.clone(),
                        earliest_datetime.clone(),
                        (required_num_bars - 39).max(0) as u32,
                    )
                    .await;

                // Return if there is enough data
                if let Ok(passed) = n_rows_res {
                    if passed {
                        info!("Enough rows in historical data");
                        if is_trading_day_tdy {
                            let time_now = Utc::now().with_timezone(&New_York);
                            let last_bar_min = time_now.minute() - (time_now.minute() % 5);
                            let last_bar_available_time = time_now
                                .with_minute(last_bar_min)
                                .expect("Expected to get corrected last_bar_min")
                                - chrono::Duration::minutes(5);

                            info!(
                                "last_bar_available_time: {}, greater than: dk",
                                last_bar_available_time
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
                                    .read_last_bar_of_stock(contract.symbol.clone(), contract.primary_exchange.clone())
                                    .await
                                {
                                    Ok(last_bar) => {
                                        if let Some(bar) = last_bar {
                                            info!(
                                                "Local bar time: {} and last_bar_available_time: {}, Equal: {}",
                                                bar.time,
                                                last_bar_available_time,
                                                bar.time == last_bar_available_time
                                            );
                                            if bar.time == last_bar_available_time {
                                                return Ok(());
                                            }
                                        }
                                        let historical_data = self
                                            .client
                                            .historical_data(
                                                &contract,
                                                None,
                                                ibapi::market_data::historical::Duration::from_str("1 D").expect("Expected to be able to parse 1 D for market data historical data"),
                                                ibapi::prelude::HistoricalBarSize::Min5,
                                                what_to_show,
                                                true,
                                            )
                                            .expect(&format!(
                                                "Expected Historical Data Request to TWS to succeed for {}",
                                                contract.symbol.clone()
                                        ));
                                        for bar in &historical_data.bars {
                                            let bar = bar.clone();
                                            let historical_data_crud =
                                                self.historical_data_crud.clone();
                                            let stock = contract.symbol.clone();
                                            let primary_exchange = contract.primary_exchange.clone();
                                            tokio::spawn(async move {
                                                if apply_batching {
                                                    if let Err(e) = historical_data_crud
                                                        .batch_create_or_update(&crate::database::models::HistoricalDataFullKeys {
                                                            stock: stock.clone(),
                                                            primary_exchange: primary_exchange.clone(),
                                                            time: DateTime::from_timestamp(
                                                                bar.date.unix_timestamp(),
                                                                bar.date.nanosecond() as u32,
                                                            )
                                                            .expect("Expected to be able to convert bar time to DateTime<Utc>"),
                                                            open: bar.open,
                                                            high: bar.high,
                                                            low: bar.low,
                                                            close: bar.close,
                                                            volume: Decimal::from_f64(
                                                                bar.volume * 100.0
                                                            ).expect("Expected to be able to parse f64 to Decimal"),
                                                    })
                                                        .await
                                                    {
                                                        tracing::error!(
                                                            "Error occurred while upserting bars into historical data for {}: {}",
                                                            stock.clone(),
                                                            e
                                                        )
                                                    }
                                                } else {
                                                    if let Err(e) = historical_data_crud
                                                        .create_or_update(&crate::database::models::HistoricalDataPrimaryKeys {
                                                            stock: stock.clone(),
                                                            primary_exchange: primary_exchange.clone(),
                                                            time: DateTime::from_timestamp(
                                                                bar.date.unix_timestamp(),
                                                                bar.date.nanosecond() as u32,
                                                            )
                                                            .expect("Expected to be able to convert bar time to DateTime<Utc>")
                                                    }, &HistoricalDataUpdateKeys {
                                                            open: Some(bar.open),
                                                            high: Some(bar.high),
                                                            low: Some(bar.low),
                                                            close: Some(bar.close),
                                                            volume: Some(Decimal::from_f64(
                                                                bar.volume * 100.0
                                                            ).expect("Expected to be able to parse f64 to Decimal")),
                                                    })
                                                        .await
                                                    {
                                                        tracing::error!(
                                                            "Error occurred while upserting bars into historical data for {}: {}",
                                                            stock.clone(),
                                                            e
                                                        )
                                                    }
                                                }
                                            });
                                        }
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
                info!("Requesting {} duration of data", duration.to_string());

                let historical_data = self
                    .client
                    .historical_data(
                        &contract,
                        None,
                        duration,
                        ibapi::prelude::HistoricalBarSize::Min5,
                        what_to_show,
                        true,
                    )
                    .map_err(|e| format!(
                        "Expected Historical Data Request to TWS to succeed for {}: {}",
                        contract.symbol.clone(),
                        e
                    ))?;

                for bar in &historical_data.bars {
                    let bar = bar.clone();
                    let historical_data_crud = self.historical_data_crud.clone();
                    let stock = contract.symbol.clone();
                    let primary_exchange = contract.primary_exchange.clone();
                    tokio::spawn(async move {
                        if apply_batching {
                            if let Err(e) = historical_data_crud
                                .batch_create_or_update(
                                    &crate::database::models::HistoricalDataFullKeys{
                                        stock: stock.clone(),
                                        primary_exchange: primary_exchange.clone(),
                                        time: DateTime::from_timestamp(
                                            bar.date.unix_timestamp(),
                                            bar.date.nanosecond() as u32,
                                        )
                                        .expect(
                                            "Expected to be able to convert bar time to DateTime<Utc>",
                                        ),
                                        open: bar.open,
                                        high: bar.high,
                                        low: bar.low,
                                        close: bar.close,
                                        volume: 
                                            Decimal::from_f64(bar.volume * 100.0)
                                                .expect("Expected to be able to parse f64 to Decimal"),
                                    },
                                )
                                .await
                            {
                                tracing::error!(
                                    "Error occurred while upserting bars into historical data for {}: {}",
                                    stock.clone(),
                                    e
                                )
                            }
                        } else {
                            if let Err(e) = historical_data_crud
                                .create_or_update(
                                    &crate::database::models::HistoricalDataPrimaryKeys {
                                        stock: stock.clone(),
                                        primary_exchange: primary_exchange.clone(),
                                        time: DateTime::from_timestamp(
                                            bar.date.unix_timestamp(),
                                            bar.date.nanosecond() as u32,
                                        )
                                        .expect(
                                            "Expected to be able to convert bar time to DateTime<Utc>",
                                        ),
                                    },
                                    &HistoricalDataUpdateKeys {
                                        open: Some(bar.open),
                                        high: Some(bar.high),
                                        low: Some(bar.low),
                                        close: Some(bar.close),
                                        volume: Some(
                                            Decimal::from_f64(bar.volume * 100.0)
                                                .expect("Expected to be able to parse f64 to Decimal"),
                                        ),
                                    },
                                )
                                .await
                            {
                                tracing::error!(
                                    "Error occurred while upserting bars into historical data for {}: {}",
                                    stock.clone(),
                                    e
                                )
                            }
                        }
                    });
                }

                Ok(())
            }
            AssetType::Option => {
                let historical_data_crud = self.historical_options_data_crud.clone();

                let n_rows_res = historical_data_crud
                    .has_at_least_n_rows_since(
                        contract.symbol.clone(),
                        contract.primary_exchange.clone(),
                        contract.last_trade_date_or_contract_month.clone(),
                        contract.strike.clone(),
                        contract.multiplier.clone(),
                        OptionType::from_str(&contract.right)
                            .expect("Expected to be able to parse contract right"),
                        earliest_datetime.clone(),
                        (required_num_bars - 39).max(0) as u32,
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
                                        contract.symbol.clone(),
                                        contract.primary_exchange.clone(),
                                        contract.last_trade_date_or_contract_month.clone(),
                                        contract.strike.clone(),
                                        contract.multiplier.clone(),
                                        OptionType::from_str(&contract.right)
                                            .expect("Expected to be able to parse contract right")
                                    )
                                    .await
                                {
                                    Ok(last_bar) => {
                                        if let Some(bar) = last_bar {
                                            if bar.time == last_bar_available_time {
                                                return Ok(());
                                            }
                                        }
                                        let historical_data = self
                                            .client
                                            .historical_data(
                                                &contract,
                                                None,
                                                ibapi::market_data::historical::Duration::from_str("1 D").expect("Expected to be able to parse 1 D for market data historical data"),
                                                ibapi::prelude::HistoricalBarSize::Min5,
                                                what_to_show,
                                                true,
                                            )
                                            .expect(&format!(
                                                "Expected Historical Data Request to TWS to succeed for {}",
                                                contract.symbol.clone()
                                        ));
                                        for bar in &historical_data.bars {
                                            let bar = bar.clone();
                                            let historical_data_crud =
                                                self.historical_options_data_crud.clone();
                                            let cloned_contract = contract.clone();
                                            tokio::spawn(async move {
                                                if apply_batching {
                                                    if let Err(e) = historical_data_crud
                                                        .batch_create_or_update(&crate::database::models::HistoricalOptionsDataFullKeys{
                                                            stock: cloned_contract.symbol.clone(),
                                                            primary_exchange: cloned_contract.primary_exchange.clone(),
                                                            expiry: cloned_contract.last_trade_date_or_contract_month.clone(),
                                                            strike: cloned_contract.strike.clone(),
                                                            multiplier: cloned_contract.multiplier.clone(),
                                                            option_type: OptionType::from_str(&cloned_contract.right).expect("Expected to be able to parse contract right in update_at_least_n_days_data for option contract"),
                                                            time: DateTime::from_timestamp(
                                                                bar.date.unix_timestamp(),
                                                                bar.date.nanosecond() as u32,
                                                            )
                                                            .expect("Expected to be able to convert bar time to DateTime<Utc>"),
                                                            open: bar.open,
                                                            high: bar.high,
                                                            low: bar.low,
                                                            close: bar.close,
                                                            volume: Decimal::from_f64(
                                                                bar.volume * 100.0
                                                            ).expect("Expected to be able to parse f64 to Decimal")
                                                        })
                                                        .await
                                                    {
                                                        tracing::error!(
                                                            "Error occurred while upserting bars into historical data for {}: {}",
                                                            cloned_contract.symbol.clone(),
                                                            e
                                                        )
                                                    }
                                                } else {
                                                    if let Err(e) = historical_data_crud
                                                        .create_or_update(&crate::database::models::HistoricalOptionsDataPrimaryKeys {
                                                            stock: cloned_contract.symbol.clone(),
                                                            primary_exchange: cloned_contract.primary_exchange.clone(),
                                                            expiry: cloned_contract.last_trade_date_or_contract_month.clone(),
                                                            strike: cloned_contract.strike.clone(),
                                                            multiplier: cloned_contract.multiplier.clone(),
                                                            option_type: OptionType::from_str(&cloned_contract.right).expect("Expected to be able to parse contract right in update_at_least_n_days_data for option contract"),
                                                            time: DateTime::from_timestamp(
                                                                bar.date.unix_timestamp(),
                                                                bar.date.nanosecond() as u32,
                                                            )
                                                            .expect("Expected to be able to convert bar time to DateTime<Utc>"),
                                                        }, &crate::database::models::HistoricalOptionsDataUpdateKeys {
                                                            open: Some(bar.open),
                                                            high: Some(bar.high),
                                                            low: Some(bar.low),
                                                            close: Some(bar.close),
                                                            volume: Some(Decimal::from_f64(
                                                                bar.volume * 100.0
                                                            ).expect("Expected to be able to parse f64 to Decimal")),
                                                        })
                                                        .await
                                                    {
                                                        tracing::error!(
                                                            "Error occurred while upserting bars into historical data for {}: {}",
                                                            cloned_contract.symbol.clone(),
                                                            e
                                                        )
                                                    }
                                                }
                                            });
                                        }
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

                let historical_data = self
                    .client
                    .historical_data(
                        &contract,
                        None,
                        duration,
                        ibapi::prelude::HistoricalBarSize::Min5,
                        what_to_show,
                        true,
                    )
                    .map_err(|e| format!(
                        "Expected Historical Data Request to TWS to succeed for {}: {}",
                        contract.symbol.clone(),
                        e
                    ))?;

                for bar in &historical_data.bars {
                    let bar = bar.clone();
                    let historical_data_crud = self.historical_options_data_crud.clone();
                    let cloned_contract = contract.clone();
                    tokio::spawn(async move {
                        if apply_batching {
                            if let Err(e) = historical_data_crud
                                .batch_create_or_update(&crate::database::models::HistoricalOptionsDataFullKeys {
                                    stock: cloned_contract.symbol.clone(),
                                    primary_exchange: cloned_contract.primary_exchange.clone(),
                                    expiry: cloned_contract.last_trade_date_or_contract_month.clone(),
                                    strike: cloned_contract.strike.clone(),
                                    multiplier: cloned_contract.multiplier.clone(),
                                    option_type: OptionType::from_str(&cloned_contract.right).expect("Expected to be able to parse contract right in update_at_least_n_days_data for option contract"),
                                    time: DateTime::from_timestamp(
                                        bar.date.unix_timestamp(),
                                        bar.date.nanosecond() as u32,
                                    )
                                    .expect("Expected to be able to convert bar time to DateTime<Utc>"),
                                    open: bar.open,
                                    high: bar.high,
                                    low: bar.low,
                                    close: bar.close,
                                    volume: Decimal::from_f64(
                                        bar.volume * 100.0
                                    ).expect("Expected to be able to parse f64 to Decimal")
                                })
                                .await
                            {
                                tracing::error!(
                                    "Error occurred while upserting bars into historical data for {}: {}",
                                    cloned_contract.symbol.clone(),
                                    e
                                )
                            }
                        } else {
                            if let Err(e) = historical_data_crud
                                .create_or_update(&crate::database::models::HistoricalOptionsDataPrimaryKeys {
                                    stock: cloned_contract.symbol.clone(),
                                    primary_exchange: cloned_contract.primary_exchange.clone(),
                                    expiry: cloned_contract.last_trade_date_or_contract_month.clone(),
                                    strike: cloned_contract.strike.clone(),
                                    multiplier: cloned_contract.multiplier.clone(),
                                    option_type: OptionType::from_str(&cloned_contract.right).expect("Expected to be able to parse contract right in update_at_least_n_days_data for option contract"),
                                    time: DateTime::from_timestamp(
                                        bar.date.unix_timestamp(),
                                        bar.date.nanosecond() as u32,
                                    )
                                    .expect("Expected to be able to convert bar time to DateTime<Utc>"),
                                }, &crate::database::models::HistoricalOptionsDataUpdateKeys {
                                    open: Some(bar.open),
                                    high: Some(bar.high),
                                    low: Some(bar.low),
                                    close: Some(bar.close),
                                    volume: Some(Decimal::from_f64(
                                        bar.volume * 100.0
                                    ).expect("Expected to be able to parse f64 to Decimal")),
                                })
                                .await
                            {
                                tracing::error!(
                                    "Error occurred while upserting bars into historical data for {}: {}",
                                    cloned_contract.symbol.clone(),
                                    e
                                )
                            }
                        }
                    });
                }
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
    pub fn begin_bar_listening(&self, order_engine: Arc<OrderEngine>, client: Arc<Client>) {
        let (sender, mut receiver) = channel(32 * 50);
        {
            let mut bars_sender_lock = self.contract_update_sender.lock();
            let bars_sender = bars_sender_lock.as_mut().expect("Expected bar_sender Mutex not to be poisoned while unlocking - begin_bar_listening");
            bars_sender.replace(sender);
        }
        let subscriptions = self.subscriptions.clone();
        let order_engine = order_engine.clone();
        let client = client.clone();
        tokio::spawn(async move {
            while let Some(update) = receiver.recv().await {
                let (contract, bar_time) = update;

                let bar_ny = bar_time.with_timezone(&New_York);
                let market_open = bar_ny
                    .date_naive()
                    .and_time(NaiveTime::from_hms_opt(9, 30, 0).unwrap());
                let elapsed = bar_ny.naive_local() - market_open;
                let elapsed_min = elapsed.num_minutes() as u32;
                if elapsed.num_minutes() < 0 {
                    continue;
                }

                let subscription = subscriptions.lock().expect(
                    "Expected Subscription guard not to be poisoned in begin_bar_listening",
                );
                let contract_subscription = subscription
                    .get(&(contract.symbol.clone(), contract.primary_exchange.clone()))
                    .expect("Expected Subscription for contract to be updated in hashmap!");
                for (timestep, strategies) in contract_subscription.iter() {
                    if elapsed_min % timestep == 0 {
                        for strategy in strategies.iter() {
                            tracing::info!("Updating for strategy: {}", strategy.get_name());
                            let order_engine = order_engine.clone();
                            let strategy = strategy.clone();
                            let contract = contract.clone();
                            let client = client.clone();
                            tokio::spawn(async move {
                                let bar_update_res = strategy.on_bar_update(&contract).await;
                                if let Ok(updated) = bar_update_res {
                                    if !updated.0 {
                                        return;
                                    }
                                }

                                let asset_type = AssetType::from_str(contract.security_type.clone());
                                order_engine.place_orders_for_strategy(
                                    strategy,
                                    contract,
                                    client,
                                    asset_type,
                                    bar_update_res.is_ok_and(|res| res.1)
                                );
                            });
                        }
                    }
                }
            }
        });
    }

    /// Opens a channel, spawns an async task to await bar updates,
    /// then subscribes to the blocking subscription in a new OS thread
    /// - Requests 5 second real time bars to build 5 minute bars
    /// - Times out if no bar received at least every 20 seconds -> Triggering a re-subscription
    /// - NOTE: this function MUST ONLY be called AFTER begin_bar_listening as begin_bar_listening opens
    /// the channel required
    pub fn subscribe_to_data(
        &self,
        strategy: T,
        contract: Contract,
        timestep: u32,
        data_type: RealtimeWhatToShow,
    ) -> () {
        {
            let mut subscriptions = self.subscriptions.lock().expect("Expected to be able to acquire lock for subscriptions in Consolidator.subscribe_to_data");
            if subscriptions.contains_key(&(contract.symbol.clone(), contract.primary_exchange.clone()))
                && subscriptions[&(contract.symbol.clone(), contract.primary_exchange.clone())].contains_key(&timestep)
                && subscriptions[&(contract.symbol.clone(), contract.primary_exchange.clone())][&timestep].contains(&strategy)
            {
                return;
            }

            let mut is_non_existing_entry = false;
            if !subscriptions.contains_key(&(contract.symbol.clone(), contract.primary_exchange.clone())) {
                subscriptions.insert((contract.symbol.clone(), contract.primary_exchange.clone()), HashMap::new());
                is_non_existing_entry = true;
            }
            if !subscriptions[&(contract.symbol.clone(), contract.primary_exchange.clone())].contains_key(&timestep) {
                subscriptions
                    .get_mut(&(contract.symbol.clone(), contract.primary_exchange.clone()))
                    .unwrap()
                    .insert(timestep.clone(), BTreeSet::new());
                is_non_existing_entry = true;
            }
            subscriptions
                .get_mut(&(contract.symbol.clone(), contract.primary_exchange.clone()))
                .unwrap()
                .get_mut(&timestep)
                .unwrap()
                .insert(strategy);

            // Spawn thread only if entry didn't exist before, else thread will handle updated data
            // accordingly already
            if !is_non_existing_entry {
                info!("Already subscribed to market data for {}", contract.symbol);
                return;
            }
        }
        info!("Initiating subscription to market data for new contract in a new blocking thread.");

        // Highest Granularity - 5 min
        let collected_bars_arc = Arc::new(Mutex::new(VecDeque::<Bar>::new()));
        {
            let mut live_data = self.live_data.lock().unwrap();
            live_data.insert((contract.symbol.clone(), contract.primary_exchange.clone()), collected_bars_arc.clone());
        }

        // let (bar_update_sender)
        let (bar_sender, mut rcx) = channel::<(DateTime<Utc>, f64, f64, f64, f64, f64)>(100);
        let contract_update_sender = {
            self.contract_update_sender
                .lock()
                .expect("Expected to be able to acquire lock for contract_update_sender")
                .as_ref()
                .expect("Expected contract_update_sender to already have been initialised")
                .clone()
        };
        let historical_data_crud = self.historical_data_crud.clone();
        let historical_options_data_crud = self.historical_options_data_crud.clone();
        let cloned_contract = contract.clone();
        tokio::spawn(async move {
            while let Some(new_5min_bar) = rcx.recv().await {
                Self::on_bar_update(
                    historical_data_crud.clone(),
                    historical_options_data_crud.clone(),
                    contract_update_sender.clone(),
                    cloned_contract.clone(),
                    new_5min_bar.0,
                    new_5min_bar.1,
                    new_5min_bar.2,
                    new_5min_bar.3,
                    new_5min_bar.4,
                    new_5min_bar.5,
                )
                .await;
            }
        });

        let cloned_collected_bars_arc = collected_bars_arc.clone();
        let client = self.client.clone();
        let contract = contract.clone();
        let cloned_bar_sender = bar_sender.clone();
        thread::spawn(move || {
            match client.realtime_bars(
                &contract,
                ibapi::prelude::RealtimeBarSize::Sec5,
                data_type,
                true,
            ) {
                Ok(mut subscription) => loop {
                    match subscription.next_timeout(Duration::from_secs(20)) {
                        Some(bar) => {
                            Self::on_new_5sec_bar(
                                cloned_collected_bars_arc.clone(),
                                bar,
                                cloned_bar_sender.clone(),
                            );
                        }
                        None => {
                            if let Some(e) = subscription.error() {
                                if format!("{}", e).contains("no security definition has been found") {
                                    tracing::warn!("Real time bars for {} cancelled", contract.symbol);
                                    break;
                                }
                            }
                            tracing::warn!(
                                "timed out waiting for next bar for contract: {} - Trying a re-subscription",
                                contract.symbol.clone()
                            );
                            subscription.cancel();
                            subscription = match client.realtime_bars(
                                &contract,
                                ibapi::prelude::RealtimeBarSize::Sec5,
                                data_type,
                                true,
                            ) {
                                Ok(sub) => sub,
                                Err(e) => {
                                    tracing::error!(
                                        "Real time request for {} failed:\n{}",
                                        contract.symbol,
                                        e
                                    );
                                    break;
                                }
                            }
                        }
                    }
                },
                Err(e) => {
                    tracing::error!("Real time request for {} failed:\n{}", contract.symbol, e)
                }
            }
        });
    }

    /// Spawns a new OS thread to process the 5 second bars from the subscription
    /// - is called by the channel instead of directly since calling directly would be on the
    /// separate OS kernel thread which doesn't have a tokio runtime
    /// - Note: multithreading should be fine because each bar for each contract is separated by 5
    /// sec times which should be sufficient time for this whole check to complete
    fn on_new_5sec_bar(
        collected_bars_arc: Arc<Mutex<VecDeque<Bar>>>,
        bar: Bar,
        bar_sender: Sender<(DateTime<Utc>, f64, f64, f64, f64, f64)>,
    ) {
        thread::spawn(move || {
            let mut collected_bars = collected_bars_arc
                .lock()
                .expect("Did not expect lock for collected_bars_arc to be poisoned");

            collected_bars.push_back(bar.clone());
            let latest_bar_timestamp = &bar.date.unix_timestamp();
            let latest_bar_no = latest_bar_timestamp - (latest_bar_timestamp % 300);
            let first_bar_timestamp = collected_bars.front().unwrap().date.unix_timestamp();
            let mut first_bar_no = first_bar_timestamp - (first_bar_timestamp % 300);

            if latest_bar_no == first_bar_no {
                return;
            }

            while first_bar_no != latest_bar_no {
                let bar_to_be_built = first_bar_no;

                // Process first bar first
                let inner_first_bar = &collected_bars.pop_front().unwrap();
                let (open, mut high, mut low, mut close, mut volume) = (
                    inner_first_bar.open,
                    inner_first_bar.high,
                    inner_first_bar.low,
                    inner_first_bar.close,
                    inner_first_bar.volume,
                );

                // Process rest of bars
                let inner_first_bar = &collected_bars.front().unwrap();
                let mut inner_first_bar_no = inner_first_bar.date.unix_timestamp()
                    - (inner_first_bar.date.unix_timestamp() % 300);
                while inner_first_bar_no == bar_to_be_built {
                    let inner_first_bar = &collected_bars.pop_front().unwrap();
                    high = f64::max(high, inner_first_bar.high);
                    low = f64::min(low, inner_first_bar.low);
                    close = inner_first_bar.close;
                    volume += inner_first_bar.volume;

                    let inner_first_bar = &collected_bars.front().unwrap();
                    inner_first_bar_no = inner_first_bar.date.unix_timestamp()
                        - (inner_first_bar.date.unix_timestamp() % 300);
                }

                // This stays blocking since across time we don't really want to muddy the waters
                if let Err(e ) = bar_sender.blocking_send((
                    Utc.timestamp_opt(bar_to_be_built, 0).unwrap(),
                    open,
                    high,
                    low,
                    close,
                    volume,
                )) {
                    tracing::error!("Error occurred while trying to send new 5 min bar: {}", e);
                };

                first_bar_no = inner_first_bar_no;
            }
        });
    }

    /// Simply updates the 5 minute bar in the appropriate database
    /// Add Duration::minutes(5)
    /// - Assumption: Bar updates every 5 minutes
    async fn on_bar_update(
        historical_data_crud: HistoricalDataCRUD,
        historical_options_data_crud: HistoricalOptionsDataCRUD,
        sender: Sender<(Contract, DateTime<chrono::Utc>)>,
        contract: Contract,
        time: DateTime<chrono::Utc>,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
    ) {
        if contract.security_type == SecurityType::Option {
            match historical_options_data_crud
                .create_or_update(&HistoricalOptionsDataPrimaryKeys {
                    stock: contract.symbol.clone(),
                    primary_exchange: contract.primary_exchange.clone(),
                    expiry: contract.last_trade_date_or_contract_month.clone(),
                    strike: contract.strike.clone(),
                    multiplier: contract.multiplier.clone(),
                    option_type: OptionType::from_str(&contract.right)
                        .unwrap_or_else(|e| panic!("{}", e)),
                    time: time,
                }, &HistoricalOptionsDataUpdateKeys {
                    open: Some(open),
                    high: Some(high),
                    low: Some(low),
                    close: Some(close),
                    volume: Some(Decimal::from_f64(volume * 100.0)
                        .expect("Expected to be able to parse f64 to Decimal")),
                })
                .await
            {
                Ok(_) => {
                    if let Err(e) = sender
                        .send((contract.clone(), time + chrono::Duration::minutes(5)))
                        .await
                    {
                        tracing::error!(
                            "Error occurred while sending bar update to channel for {}:{} at {}: {}",
                            contract.security_type,
                            contract.symbol,
                            time,
                            e
                        );
                    }
                }
                Err(e) => tracing::error!(
                    "Error occurred while trying to insert new bar to HistoricalOptionsData: {}",
                    e
                ),
            };
        } else if contract.security_type == SecurityType::Stock {
            match historical_data_crud
                .create_or_update(&HistoricalDataPrimaryKeys {
                    stock: contract.symbol.clone(),
                    primary_exchange: contract.primary_exchange.clone(),

                    time: time,
                }, &HistoricalDataUpdateKeys {
                    open: Some(open),
                    high: Some(high),
                    low: Some(low),
                    close: Some(close),
                    volume: Some(Decimal::from_f64(volume * 100.0)
                        .expect("Expected to be able to parse f64 to Decimal")),
                })
                .await
            {
                Ok(_) => {
                    if let Err(e) = sender
                        .send((contract.clone(), time + chrono::Duration::minutes(5)))
                        .await
                    {
                        tracing::error!(
                            "Error occurred while sending bar update to channel for {}:{} at {}: {}",
                            contract.security_type,
                            contract.symbol,
                            time,
                            e
                        );
                    }
                }
                Err(e) => tracing::error!(
                    "Error occurred while trying to insert new bar to HistoricalStockData: {}",
                    e
                ),
            };
        }
    }
}
