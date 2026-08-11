use std::{collections::HashMap, sync::Arc, time::Duration};

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
    pub(crate) client: Arc<Client>,

    // StrategyScheduler
    // pub(super) contract_coordinator: Arc<IbkrContractScheduler>,

    // AccountTracker
    pub pool: PgPool,
    pub(crate) market_data_handler: MarketDataHandler,
    pub(super) memoisers: Arc<HashMap<MemoisedConsolidatorFns, Arc<Box<dyn AnyMemoized>>>>,
    pub(super) handle: tokio::runtime::Handle,
}

impl Consolidator {
    pub fn new(
        handle: tokio::runtime::Handle,
        pool: PgPool,
        client: Arc<Client>,
        market_data_handler: MarketDataHandler,
    ) -> Self {
        let ttl = Duration::from_secs(60);
        let mut memoisers: HashMap<MemoisedConsolidatorFns, Arc<Box<dyn AnyMemoized>>> =
            HashMap::new();
        let get_price_fn: Arc<Box<dyn AnyMemoized>> = Arc::new(Box::new(Memoized::new(
            ttl,
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
            move |input: &(Arc<Client>, Contract, bool, Vec<String>, bool)| {
                let (client, contract, vwap, generic_ticks, is_second_try) = input;
                let client_for_price = client.clone();
                let ticks_refs: Vec<&str> = generic_ticks.iter().map(String::as_str).collect();
                Self::_get_current_price(
                    client_for_price,
                    contract,
                    *vwap,
                    &ticks_refs,
                    *is_second_try,
                )
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
            market_data_handler,
            // contract_coordinator: Arc::new(IbkrContractScheduler::new(client)),
            memoisers: Arc::new(memoisers),
            handle,
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

    fn refresh_if_stale(&self, contract: &Contract, config: &HistoricalDataConfig) {
        let asset_type = AssetType::from_str(&contract.security_type);
        let target = last_bar_available(Utc::now().with_timezone(&New_York), &asset_type)
            .expect("Expected a bar");

        let historical_data_crud = HistoricalDataCRUD::from(&asset_type, self.pool.clone());
        tokio::runtime::Handle::current().block_on(async move {
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
                            if let Err(e) = self.populate_historical_data(contract, config) {
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
        });
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
                    self.refresh_if_stale(&contract, &config);
                } else {
                    if let Err(e) = self.populate_historical_data(&contract, &config) {
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

    // /// Opens a channel to asynchronously accept (Bar, Contract) data updates and perform upserts
    // /// - for each timestep (in minutes) u subscribe to, the timestep will be triggered for each
    // /// timing past 9:30am for the strategy
    // /// - accordingly, this handles subscribe_to_data() updates such that the strategy
    // /// on_bar_update() function ONLY has to handle updates to the TargetPosition in the database
    // /// - Ideally, the order_engine is initialised with client id 0, consolidator with any other
    // /// client id (so that market data subscriptions are handled in a separate thread)
    // /// - Pass the client to be used to place orders for here
    // pub fn begin_bar_listening(
    //     &self,
    //     weak_order_engine_borrowed: &Weak<OrderEngine>,
    //     weak_client_borrowed: &Weak<Client>,
    //     weak_consolidator_borrowed: &Weak<Consolidator>,
    // ) {
    //     let (channel_killer, mut channel_killer_rcx) = tokio::sync::oneshot::channel();
    //     {
    //         self.bar_listener_channel_killer
    //             .lock()
    //             .expect("expected bar_listener_channel_killer lock to not be poisoned")
    //             .replace(channel_killer);
    //     }
    //     let (sender, mut receiver) = channel(32 * 50);
    //     {
    //         let mut bars_sender_lock = self.contract_update_sender.write();
    //         let bars_sender = bars_sender_lock.as_mut().expect("Expected bar_sender Mutex not to be poisoned while unlocking - begin_bar_listening");
    //         bars_sender.replace(sender);
    //         // self.contract_update_sender.as_mut().replace(sender);
    //     }
    //     let subscriptions = self.subscriptions.clone();
    //     let weak_order_engine = weak_order_engine_borrowed.clone();
    //     let weak_client = weak_client_borrowed.clone();
    //     let weak_consolidator = weak_consolidator_borrowed.clone();
    //
    //     tokio::spawn(async move {
    //         loop {
    //             tokio::select! {
    //                 _ = &mut channel_killer_rcx => {
    //                     tracing::info!("bar listening loop closed properly");
    //                     return;
    //                 }
    //                 Some(update) = receiver.recv() => {
    //                     let (contract, data_type, bar_time) = update;
    //
    //                     let bar_ny = bar_time.with_timezone(&New_York);
    //                     let market_open = bar_ny
    //                         .date_naive()
    //                         .and_time(NaiveTime::from_hms_opt(9, 30, 0).unwrap());
    //                     let elapsed_min: u32 = if contract.security_type == SecurityType::ForexPair
    //                         || contract.security_type == SecurityType::CFD
    //                     {
    //                         (bar_ny
    //                             - Consolidator::fx_trading_day_start(&bar_ny.date_naive(), &New_York))
    //                         .num_minutes() as u32
    //                     } else {
    //                         (bar_ny.naive_local() - market_open).num_minutes() as u32
    //                     };
    //
    //                     // Bounded access to subscriptions
    //                     let strats_sets = {
    //                         let subscription = subscriptions.read().expect(
    //                             "Expected Subscription read guard not to be poisoned in begin_bar_listening",
    //                         );
    //                         let contract_subscription = {
    //                             match subscription
    //                             .get(&UniqueSubscription {
    //                                 contract: HashContract {
    //                                     contract: contract.clone(),
    //                                 },
    //                                 data_type,
    //                             }) {
    //                                 Some(sub) => sub,
    //                                 None => {
    //                                     tracing::warn!("Strategy not found in hashmap of strategies - either cancelled already & this is final bar OR not registered properly!");
    //                                     continue;
    //                                 }
    //                             }
    //                         };
    //
    //                         let mut strats_arcs = Vec::new();
    //                         contract_subscription.iter().filter(move |(timestep, _)| {
    //                             elapsed_min % *timestep == 0
    //                         }).for_each(|(_timestep, strats_arc)| strats_arcs.push(strats_arc.clone()));
    //                         strats_arcs
    //                     };
    //
    //                     let mut seen_strats = HashSet::new();
    //                     let strats: Vec<StrategyEnum> = strats_sets
    //                         .iter()
    //                         .flat_map(|set_lock| {
    //                             // 1. Acquire the lock
    //                             let lock = set_lock.read().expect("Lock poisoned");
    //
    //                             // 2. We must collect or process here because the lock drops
    //                             // when this flat_map iteration ends.
    //                             lock.iter()
    //                                 .filter_map(|strat| {
    //                                     let disc = mem::discriminant(strat);
    //                                     if seen_strats.insert(disc) {
    //                                         Some(strat.clone())
    //                                     } else {
    //                                         None
    //                                     }
    //                                 })
    //                                 .collect::<Vec<_>>() // Materialize the clones while lock is held
    //                         })
    //                         .collect();
    //
    //                     tracing::info!("Running strategies now");
    //                     futures::future::join_all(strats.into_iter().map(|strategy| {
    //                         let weak_consolidator = weak_consolidator.clone();
    //                         let weak_order_engine = weak_order_engine.clone();
    //                         let weak_client = weak_client.clone();
    //                         let contract = contract.clone();
    //
    //                         async move {
    //                             let consolidator_opt = weak_consolidator.upgrade();
    //                             if consolidator_opt.is_none() {
    //                                 return;
    //                             }
    //                             let consolidator = consolidator_opt.unwrap();
    //                             let bar_update_res =
    //                                 strategy.on_bar_update(&contract, &consolidator).await;
    //
    //                             let ignore_contract_for_strat = match bar_update_res {
    //                                 Ok(updated) => {
    //                                     let (is_strat_updated, ignore_contract_for_strat) = updated;
    //                                     if !is_strat_updated {
    //                                         return;
    //                                     }
    //                                     ignore_contract_for_strat
    //                                 }
    //                                 Err(e) => {
    //                                     tracing::error!(
    //                                         message=%format!(
    //                                             "error in on_bar_update in strategy: {:?}: {e:?}",
    //                                             strategy.get_name()
    //                                         )
    //                                     );
    //                                     return;
    //                                 }
    //                             };
    //
    //                             let asset_type = AssetType::from_str(&contract.security_type);
    //                             let order_engine_opt = weak_order_engine.upgrade();
    //                             if order_engine_opt.is_none() {
    //                                 tracing::warn!("Order engine died b4 placing orders for strategy");
    //                                 return;
    //                             }
    //                             let order_engine = order_engine_opt.unwrap();
    //                             order_engine.place_orders_for_strategy(
    //                                 &strategy,
    //                                 &contract,
    //                                 &weak_client,
    //                                 &asset_type,
    //                                 ignore_contract_for_strat,
    //                                 &weak_consolidator
    //                             );
    //                         }
    //                     }))
    //                     .await;
    //                     tracing::info!("joined all strategies");
    //                 }
    //             }
    //         }
    //     });
    // }
    //
    // pub fn close_bar_listening_channel(&self) -> Result<(), String> {
    //     let mut sender = self.bar_listener_channel_killer.lock().map_err(|e| {
    //         format!("Error trying to acquire lock for bar_listener_channel_killer: {e:?}")
    //     })?;
    //     if sender.is_none() {
    //         Err(
    //             "No sender found in bar_listener_channel_killer when trying to close bar_listener_channel".to_string()
    //         )
    //     } else {
    //         if let Err(e) = sender.take().unwrap().send(()) {
    //             Err(format!(
    //                 "Error trying to send shutdown flag to bar_listener_channel: {e:?}"
    //             ))
    //         } else {
    //             Ok(())
    //         }
    //     }
    // }
    //
    // pub fn cancel_subscription(
    //     &self,
    //     strategy: &StrategyEnum,
    //     contract: &Contract,
    //     timestep: &u32,
    //     data_type: RealtimeWhatToShow,
    // ) {
    //     let symbol = get_local_symbol(&contract);
    //     let unique_sub = UniqueSubscription {
    //         contract: HashContract {
    //             contract: contract.clone(),
    //         },
    //         data_type,
    //     };
    //
    //     {
    //         let mut subscriptions = self.subscriptions.write().expect("Expected to be able to acquire lock for subscriptions in Consolidator.subscribe_to_data");
    //         if subscriptions.contains_key(&unique_sub)
    //             && subscriptions[&unique_sub].contains_key(&timestep)
    //             && subscriptions[&unique_sub][&timestep]
    //                 .read()
    //                 .expect("Expected read lock to not be poisoned")
    //                 .contains(strategy)
    //         {
    //             subscriptions
    //                 .get_mut(&unique_sub)
    //                 .unwrap()
    //                 .get_mut(&timestep)
    //                 .unwrap()
    //                 .write()
    //                 .expect("Expected write lock to not be poisoned")
    //                 .remove(strategy);
    //             if subscriptions[&unique_sub][&timestep]
    //                 .read()
    //                 .expect("Expected read lock not to be poisoned")
    //                 .len()
    //                 > 0
    //             {
    //                 tracing::info!(
    //                     message=%format!(
    //                         "Cancelled subscription for {} for strategy ({}), but some other strategy is still subscribed so still receiving data!",
    //                         symbol,
    //                         strategy.get_name()
    //                     )
    //                 );
    //                 return;
    //             }
    //             subscriptions
    //                 .get_mut(&unique_sub)
    //                 .unwrap()
    //                 .remove(&timestep);
    //             if subscriptions[&unique_sub].len() > 0 {
    //                 tracing::info!(
    //                     message=%format!(
    //                         "Cancelled subscription for {} for strategy ({}), but some other strategy is still subscribed so still receiving data!",
    //                         symbol,
    //                         strategy.get_name()
    //                     )
    //                 );
    //                 return;
    //             };
    //             tracing::info!(
    //                 message=%format!(
    //                     "Subscription for {} cancelled completely, last cancelled for strategy ({}), no other strategy registered",
    //                     symbol,
    //                     strategy.get_name()
    //                 )
    //             );
    //             subscriptions.remove(&unique_sub);
    //         }
    //
    //         {
    //             let mut subscriptions_handler = self
    //                 .subscriptions_handler
    //                 .lock()
    //                 .expect("Failed to retrieve lock for subscriptions_handler");
    //             subscriptions_handler.remove(&unique_sub);
    //         }
    //     }
    // }
    //
    // pub fn cancel_all_subscriptions(&self) {
    //     tracing::info!("Cancelling all subscriptions!");
    //     {
    //         let mut subs = self
    //             .subscriptions
    //             .write()
    //             .expect("Expected subscriptions lock not to be poisoned");
    //         subs.drain();
    //     }
    //     {
    //         let mut sub_handler = self
    //             .subscriptions_handler
    //             .lock()
    //             .expect("Expected subscriptions_handler lock not to be poisoned");
    //         sub_handler.drain();
    //     }
    // }
    //
    // /// Opens a channel, spawns an async task to await bar updates,
    // /// then subscribes to the blocking subscription in a new OS thread
    // /// - Requests 5 second real time bars to build 5 minute bars
    // /// - Times out if no bar received at least every 20 seconds -> Triggering a re-subscription
    // /// - NOTE: this function MUST ONLY be called AFTER begin_bar_listening as begin_bar_listening opens
    // ///   the channel required
    // /// - FN NOTE: saves granularity of 5-min bars for stocks, futures, options, and 1-min bars for
    // /// forex
    // pub fn subscribe_to_data(
    //     &self,
    //     strategy: &StrategyEnum,
    //     contract: &Contract,
    //     timestep: &u32,
    //     data_type: RealtimeWhatToShow,
    // ) -> () {
    //     let highest_granularity_timestep_in_seconds = if contract.security_type
    //         == SecurityType::ForexPair
    //         || contract.security_type == SecurityType::CFD
    //     {
    //         60
    //     } else {
    //         300
    //     };
    //     let symbol = get_local_symbol(&contract);
    //     let hash_contract = HashContract {
    //         contract: contract.clone(),
    //     };
    //     let unique_sub = UniqueSubscription {
    //         contract: hash_contract.clone(),
    //         data_type,
    //     };
    //
    //     {
    //         let mut subscriptions = self.subscriptions.write().expect("Expected to be able to acquire lock for subscriptions in Consolidator.subscribe_to_data");
    //         if subscriptions.contains_key(&unique_sub)
    //             && subscriptions[&unique_sub].contains_key(&timestep)
    //             && subscriptions[&unique_sub][&timestep]
    //                 .read()
    //                 .expect("Expected read lock to not be poisoned")
    //                 .contains(&strategy)
    //         {
    //             info!("Already subscribed to market data for {symbol:?}");
    //             return;
    //         }
    //
    //         let mut is_non_existing_entry = false;
    //         if !subscriptions.contains_key(&unique_sub) {
    //             subscriptions.insert(unique_sub.clone(), BTreeMap::new());
    //             is_non_existing_entry = true;
    //         }
    //         if !subscriptions[&unique_sub].contains_key(&timestep) {
    //             subscriptions
    //                 .get_mut(&unique_sub)
    //                 .unwrap()
    //                 .insert(timestep.clone(), Arc::new(RwLock::new(BTreeSet::new())));
    //         }
    //         subscriptions
    //             .get_mut(&unique_sub)
    //             .unwrap()
    //             .get_mut(&timestep)
    //             .unwrap()
    //             .write()
    //             .expect("Expected write lock not to be poisoned")
    //             .insert(strategy.clone());
    //
    //         // Spawn thread only if entry didn't exist before, else thread will handle updated data
    //         // accordingly already
    //         if !is_non_existing_entry {
    //             info!(
    //                 message=%format!(
    //                     "Already subscribed to market data for {}, but ig not for this strategy {:?}",
    //                     symbol, &strategy
    //                 )
    //             );
    //             return;
    //         }
    //     }
    //     {
    //         if !self
    //             .contract_coordinator
    //             .contains_contract(&unique_sub.contract)
    //         {
    //             tracing::warn!(message=%format!("Contract subscribed to in subscribe_to_data() not pre-found in strategies initialised! Adding schedule now! Contract: {:?}", &unique_sub.contract));
    //             if let Err(e) = self
    //                 .contract_coordinator
    //                 .add_schedule(&unique_sub.contract.contract)
    //             {
    //                 tracing::error!(message=%format!("Error trying to add schedule of new contract ({:?}): {e:?}", &unique_sub.contract))
    //             };
    //         }
    //     }
    //
    //     let contract_sub_lock = {
    //         let mut subscriptions_handler = self
    //             .subscriptions_handler
    //             .lock()
    //             .expect("Failed to retrieve lock for subscriptions_handler");
    //         // need to ensure that if it already contains the key,
    //         // you do not override the Arc being held by the previous subscriptions
    //         if subscriptions_handler.contains_key(&unique_sub) {
    //             tracing::error!(
    //                 "Error during subscription: entry exists in subscriptions_handler but not in subscriptions"
    //             );
    //             return;
    //         } else {
    //             let sub_lock = Arc::new(());
    //             subscriptions_handler.insert(unique_sub.clone(), sub_lock.clone());
    //             sub_lock
    //         }
    //     };
    //
    //     // Update forex_subscriptions_tracker appropriately
    //     if contract.security_type == SecurityType::ForexPair
    //         || contract.security_type == SecurityType::CFD
    //     {
    //         async fn ok(
    //             forex_subscriptions_tracker: Arc<
    //                 tokio::sync::Mutex<HashMap<String, HashMap<DateTime<Utc>, ForexBarCollector>>>,
    //             >,
    //             symbol: String,
    //         ) {
    //             forex_subscriptions_tracker
    //                 .lock()
    //                 .await
    //                 .insert(symbol, HashMap::new());
    //         }
    //         tokio::task::block_in_place(|| {
    //             tokio::runtime::Handle::current()
    //                 .block_on(ok(self.forex_subscriptions_tracker.clone(), symbol.clone()))
    //         });
    //     }
    //     info!(
    //         message=%format!(
    //             "Initiating subscription to market data for new contract ({:?}, {:?}, currency: {:?}, security_type: {:?}) in a new blocking thread.",
    //             contract.symbol, contract.primary_exchange, contract.currency, contract.security_type
    //         )
    //     );
    //
    //     // Highest Granularity - 5 min
    //     let (bar_sender, mut rcx) =
    //         channel::<(DateTime<Utc>, f64, f64, f64, f64, f64, bool, bool)>(100);
    //     let contract_update_sender = {
    //         self.contract_update_sender
    //             .read()
    //             .expect("Expected to be able to acquire lock for contract_update_sender")
    //             .as_ref()
    //             .expect("Expected contract_update_sender to already have been initialised")
    //             .clone()
    //     };
    //     let historical_data_crud = self.historical_data_crud.clone();
    //     let historical_forex_data_crud = self.historical_forex_data_crud.clone();
    //     let historical_options_data_crud = self.historical_options_data_crud.clone();
    //     let forex_subscriptions_tracker = self.forex_subscriptions_tracker.clone();
    //     let cloned_client = self.client.clone();
    //     let cloned_contract = contract.clone();
    //     let cloned_data_type = data_type.clone();
    //     tokio::spawn(async move {
    //         while let Some(new_5min_bar) = rcx.recv().await {
    //             let has_first_bar = new_5min_bar.6;
    //             let is_first_bar_sent = new_5min_bar.7;
    //             if !has_first_bar || is_first_bar_sent {
    //                 if let Err(e) = Self::get_historical_data(
    //                     cloned_client.clone(),
    //                     &historical_data_crud,
    //                     &historical_options_data_crud,
    //                     &historical_forex_data_crud,
    //                     &cloned_contract,
    //                     ibapi::market_data::historical::Duration::seconds(330),
    //                     if highest_granularity_timestep_in_seconds == 60 {
    //                         ibapi::market_data::historical::BarSize::Min
    //                     } else {
    //                         ibapi::market_data::historical::BarSize::Min5
    //                     },
    //                     HistoricalWhatToShow::Trades,
    //                     &(cloned_contract.security_type == SecurityType::ForexPair
    //                         || cloned_contract.security_type == SecurityType::CFD),
    //                     &false,
    //                 ) {
    //                     tracing::error!(
    //                         "Error encountered while fetching historical data for missed bar: {e:?}",
    //                     );
    //                 };
    //                 tracing::info!("Sending to contract_update_sender");
    //                 if (cloned_contract.security_type != SecurityType::ForexPair
    //                     && cloned_contract.security_type != SecurityType::CFD)
    //                     || matches!(data_type, RealtimeWhatToShow::Ask)
    //                 {
    //                     if let Err(e) = contract_update_sender
    //                         .send((
    //                             cloned_contract.clone(),
    //                             cloned_data_type.clone(),
    //                             new_5min_bar.0,
    //                         ))
    //                         .await
    //                     {
    //                         tracing::error!(
    //                             message=%format!(
    //                                 "Error occurred while sending bar update to channel for {}:{} at {}: {}",
    //                                 cloned_contract.security_type,
    //                                 cloned_contract.symbol,
    //                                 new_5min_bar.0,
    //                                 e
    //                             )
    //                         );
    //                     }
    //                     continue;
    //                 };
    //             }
    //
    //             if cloned_contract.security_type == SecurityType::ForexPair
    //                 || cloned_contract.security_type == SecurityType::CFD
    //             {
    //                 let symbol = get_local_symbol(&cloned_contract);
    //                 let mut unlocked_forex_subscriptions_tracker =
    //                     forex_subscriptions_tracker.lock().await;
    //                 let contract_updates = unlocked_forex_subscriptions_tracker.get_mut(&symbol).expect(format!("Expected {symbol:?} to already have been added to forex_subscriptions_tracker").as_str());
    //
    //                 if !contract_updates.contains_key(&new_5min_bar.0) {
    //                     contract_updates.insert(
    //                         new_5min_bar.0.clone(),
    //                         ForexBarCollector {
    //                             bid: None,
    //                             ask: None,
    //                         },
    //                     );
    //                 }
    //                 if matches!(data_type, RealtimeWhatToShow::Bid) {
    //                     contract_updates
    //                         .get_mut(&new_5min_bar.0)
    //                         .unwrap()
    //                         .bid
    //                         .replace(ForexBar {
    //                             open: new_5min_bar.1,
    //                             high: new_5min_bar.2,
    //                             low: new_5min_bar.3,
    //                             close: new_5min_bar.4,
    //                             volume: new_5min_bar.5,
    //                         });
    //                 } else if matches!(data_type, RealtimeWhatToShow::Ask) {
    //                     contract_updates
    //                         .get_mut(&new_5min_bar.0)
    //                         .unwrap()
    //                         .ask
    //                         .replace(ForexBar {
    //                             open: new_5min_bar.1,
    //                             high: new_5min_bar.2,
    //                             low: new_5min_bar.3,
    //                             close: new_5min_bar.4,
    //                             volume: new_5min_bar.5,
    //                         });
    //                 } else {
    //                     tracing::error!(
    //                         "got contract update data for {symbol:?} but unknown data_type"
    //                     );
    //                 }
    //
    //                 if contract_updates[&new_5min_bar.0].bid.is_some()
    //                     && contract_updates[&new_5min_bar.0].ask.is_some()
    //                 {
    //                     let bid = contract_updates[&new_5min_bar.0].bid.as_ref().unwrap();
    //                     let ask = contract_updates[&new_5min_bar.0].ask.as_ref().unwrap();
    //                     Self::on_forex_bar_update(
    //                         &historical_forex_data_crud,
    //                         &contract_update_sender,
    //                         &cloned_contract,
    //                         new_5min_bar.0.clone(),
    //                         bid.open,
    //                         bid.high,
    //                         bid.low,
    //                         bid.close,
    //                         bid.volume,
    //                         ask.open,
    //                         ask.high,
    //                         ask.low,
    //                         ask.close,
    //                         ask.volume,
    //                         RealtimeWhatToShow::Ask,
    //                         true, // is_full_bar_updated
    //                     )
    //                     .await;
    //                     contract_updates.remove(&new_5min_bar.0);
    //                 }
    //                 continue;
    //             }
    //             Self::on_bar_update(
    //                 &historical_data_crud,
    //                 &historical_options_data_crud,
    //                 &contract_update_sender,
    //                 &cloned_contract,
    //                 new_5min_bar.0,
    //                 new_5min_bar.1,
    //                 new_5min_bar.2,
    //                 new_5min_bar.3,
    //                 new_5min_bar.4,
    //                 new_5min_bar.5,
    //                 data_type,
    //             )
    //             .await;
    //         }
    //         tracing::info!("5-min bar receival loop closed!");
    //     });
    //
    //     let current_price_lock = Arc::new(RwLock::new(None));
    //     {
    //         let mut live_data = self
    //             .live_data
    //             .write()
    //             .expect("Expected live data write lock not to be poisoned");
    //         live_data.insert(hash_contract.clone(), Arc::downgrade(&current_price_lock));
    //     }
    //     let mut collected_bars_arc = Arc::new(VecDeque::<Bar>::new());
    //     let weak_client = Arc::downgrade(&self.client);
    //     let contract = contract.clone();
    //     let cloned_bar_sender = bar_sender.clone();
    //     let contract_coordinator = self.contract_coordinator.clone();
    //     thread::spawn(move || {
    //         let mut last_error_time: Option<Instant> = None;
    //         let hashed_contract = HashContract {
    //             contract: contract.clone(),
    //         };
    //         let weak_current_price_lock = Arc::downgrade(&current_price_lock);
    //         macro_rules! get_next_contract_available {
    //                 () => {
    //                     {
    //                         match contract_coordinator.is_trading(&hashed_contract, &Utc::now()) {
    //                             Ok(is_trading_rn) => {
    //                                 if !is_trading_rn {
    //                                     tracing::info!(
    //                                         "Contract ({contract:?}) is currently not trading, sleeping until available rn!"
    //                                     );
    //                                     let now = Utc::now();
    //                                     let next_trading_time = {
    //                                         match contract_coordinator
    //                                             .get_next_earliest_available_data(std::slice::from_ref(&&hashed_contract), &now)
    //                                         {
    //                                             Ok(dt) => dt,
    //                                             Err(e) => {
    //                                                 tracing::error!(
    //                                                     "Couldn't get earliest dt for {hashed_contract:?}: {e:?}"
    //                                                 );
    //                                                 now
    //                                             }
    //                                         }
    //                                     };
    //                                     if next_trading_time > now {
    //                                         // tracing::info!("sleeping now for {:?}", next_trading_time - now);
    //                                         (next_trading_time - now)
    //                                             .to_std()
    //                                             .expect("Couldn't convert timedelta to duration")
    //                                     } else {
    //                                         Duration::from_secs(0)
    //                                     }
    //                                 } else {
    //                                     Duration::from_secs(0)
    //                                 }
    //                             }
    //                             Err(e) => {
    //                                 tracing::error!(
    //                                     "Couldn't get schedule of contract ({contract:?}): {e:?}"
    //                                 );
    //                                 Duration::from_secs(0)
    //                             }
    //                         }
    //                     }
    //                 };
    //             }
    //
    //         loop {
    //             let duration_till_next_contract_available = get_next_contract_available!();
    //             std::thread::sleep(duration_till_next_contract_available);
    //
    //             tracing::info!("Finished sleeping for contract: {contract:?}");
    //             let subscription_res = {
    //                 let client_opt = weak_client.upgrade();
    //                 if client_opt.is_none() {
    //                     return;
    //                 }
    //                 let client = client_opt.unwrap();
    //                 client.realtime_bars(
    //                     &contract,
    //                     ibapi::prelude::RealtimeBarSize::Sec5,
    //                     data_type,
    //                     ibapi::prelude::TradingHours::Regular,
    //                 )
    //             };
    //             match subscription_res {
    //                 Ok(subscription) => {
    //                     let mut is_first_bar_sent = true;
    //                     loop {
    //                         {
    //                             if Arc::strong_count(&contract_sub_lock) == 1 {
    //                                 tracing::info!("Subscription for {contract:?} cancelled!");
    //                                 return;
    //                             }
    //                         }
    //                         match subscription.next_timeout(Duration::from_secs(20)) {
    //                             Some(bar) => {
    //                                 let was_info_sent = Self::on_new_5sec_bar(
    //                                     &mut collected_bars_arc,
    //                                     &weak_current_price_lock,
    //                                     bar,
    //                                     &cloned_bar_sender,
    //                                     highest_granularity_timestep_in_seconds,
    //                                     is_first_bar_sent,
    //                                 );
    //                                 // just to optimise branch prediction LOL
    //                                 if is_first_bar_sent {
    //                                     if was_info_sent {
    //                                         is_first_bar_sent = false;
    //                                     }
    //                                 }
    //                             }
    //                             None => {
    //                                 if let Some(e) = subscription.error() {
    //                                     match e {
    //                                         _ => {
    //                                             tracing::warn!(
    //                                                 message=%format!(
    //                                                     "Real time bars for {} cancelled for reason: {e:?}",
    //                                                     contract.symbol
    //                                                 )
    //                                             );
    //                                         }
    //                                     }
    //                                     last_error_time = Some(Instant::now());
    //                                 }
    //                                 {
    //                                     match contract_coordinator
    //                                         .is_trading(&hashed_contract, &Utc::now())
    //                                     {
    //                                         Ok(is_trading_rn) => {
    //                                             if is_trading_rn {
    //                                                 tracing::warn!(
    //                                                     "Currently trading session rn and timed out waiting for next bar for contract: {} - Trying a re-subscription",
    //                                                     contract.symbol
    //                                                 )
    //                                             }
    //                                         }
    //                                         Err(e) => {
    //                                             tracing::warn!(
    //                                                 "Contract ({contract:?}) timed out and couldn't get info: {e:?}"
    //                                             )
    //                                         }
    //                                     }
    //                                 }
    //
    //                                 subscription.cancel();
    //                                 break;
    //                             }
    //                         }
    //                     }
    //                 }
    //                 Err(e) => {
    //                     tracing::error!(
    //                         message=%format!(
    //                             "Real time request for {} failed:\n{}. will retry again", contract.symbol, e
    //                         )
    //                     );
    //                     last_error_time = Some(Instant::now());
    //                 }
    //             }
    //             // Throttle retry to 20 seconds since last error
    //             if let Some(last_error) = last_error_time {
    //                 let elapsed = last_error.elapsed();
    //                 if elapsed < Duration::from_secs(20) {
    //                     let wait_duration = Duration::from_secs(20) - elapsed;
    //                     tracing::warn!(
    //                         message=%format!(
    //                             "Error occurred trying to resubscribe to contract. Waiting {:?} before retrying subscription for {}",
    //                             wait_duration,
    //                             contract.symbol
    //                         )
    //                     );
    //                     std::thread::sleep(wait_duration);
    //                 }
    //             }
    //         }
    //     });
    // }
    //
    // /// Spawns a new OS thread to process the 5 second bars from the subscription
    // /// - is called by the channel instead of directly since calling directly would be on the
    // /// separate OS kernel thread which doesn't have a tokio runtime
    // /// - Note: multithreading should be fine because each bar for each contract is separated by 5
    // /// sec times which should be sufficient time for this whole check to complete
    // /// - highest_granularity_timestep_in_seconds: granularity of bars being stored in the table
    // ///     - i.e. for stocks, 5 min / 300 sec
    // ///     - i.e. for forex, 1 min / 60 sec
    // /// - MINOR_BUG: if data starts being loaded within a bar, start of bar data is ignored/lost
    // fn on_new_5sec_bar(
    //     collected_bars_arc: &mut Arc<VecDeque<Bar>>,
    //     current_price_lock: &Weak<RwLock<Option<f64>>>,
    //     bar: Bar,
    //     bar_sender: &Sender<(DateTime<Utc>, f64, f64, f64, f64, f64, bool, bool)>,
    //     highest_granularity_timestep_in_seconds: u32,
    //     is_first_bar_sent: bool,
    // ) -> bool {
    //     let highest_granularity_timestep_in_seconds_i64 =
    //         highest_granularity_timestep_in_seconds as i64;
    //
    //     // only works if strong count is 1 - but shldn't ever stall for too long
    //     let collected_bars = Arc::get_mut(collected_bars_arc)
    //         .expect("Expected collected_bars_arc to only have a single Arc reference");
    //     collected_bars.push_back(bar.clone());
    //
    //     let latest_bar_timestamp = &bar.date.unix_timestamp();
    //     let next_bar_timestamp = latest_bar_timestamp + 5;
    //
    //     let latest_bar_no = latest_bar_timestamp
    //         - (latest_bar_timestamp % highest_granularity_timestep_in_seconds_i64);
    //     let next_bar_no =
    //         next_bar_timestamp - (next_bar_timestamp % highest_granularity_timestep_in_seconds_i64);
    //     if latest_bar_no == next_bar_no {
    //         return false;
    //     }
    //
    //     while !collected_bars.is_empty() {
    //         // Process first bar first
    //         let first_bar = &collected_bars.pop_front().unwrap();
    //         let bar_time = first_bar.date.unix_timestamp();
    //         let prev_bar_time = bar_time - 5;
    //         let bar_no = bar_time - (bar_time % highest_granularity_timestep_in_seconds_i64);
    //         let prev_bar_no =
    //             prev_bar_time - (prev_bar_time % highest_granularity_timestep_in_seconds_i64);
    //         let has_first_bar = prev_bar_no != bar_no;
    //
    //         let (open, mut high, mut low, mut close, mut volume) = (
    //             first_bar.open,
    //             first_bar.high,
    //             first_bar.low,
    //             first_bar.close,
    //             first_bar.volume,
    //         );
    //
    //         loop {
    //             if collected_bars.is_empty() {
    //                 break;
    //             }
    //             let this_bar_date = &collected_bars.front().unwrap().date.unix_timestamp();
    //             let this_bar_no =
    //                 this_bar_date - (this_bar_date % highest_granularity_timestep_in_seconds_i64);
    //             if bar_no != this_bar_no {
    //                 break;
    //             }
    //
    //             let first_bar = &collected_bars.pop_front().unwrap();
    //             high = f64::max(high, first_bar.high);
    //             low = f64::min(low, first_bar.low);
    //             close = first_bar.close;
    //             volume += first_bar.volume;
    //         }
    //
    //         tracing::info!("Has first bar: {has_first_bar:?}");
    //         tracing::info!("Is first bar sent: {is_first_bar_sent:?}");
    //
    //         if let Err(e) = bar_sender.blocking_send((
    //             Utc.timestamp_opt(bar_no, 0).unwrap(),
    //             open,
    //             high,
    //             low,
    //             close,
    //             volume,
    //             has_first_bar,
    //             is_first_bar_sent,
    //         )) {
    //             tracing::error!("Error occurred while trying to send new 5 min bar: {e:?}");
    //         };
    //
    //         let cloned_weak_curr_price_lock = current_price_lock.clone();
    //         thread::spawn(move || match cloned_weak_curr_price_lock.upgrade() {
    //             Some(curr_price_lock) => {
    //                 let mut curr_price = curr_price_lock
    //                     .write()
    //                     .expect("Expected write lock for current_price_lock to not be poisoned");
    //                 curr_price.replace(close);
    //             }
    //             None => {
    //                 return;
    //             }
    //         });
    //     }
    //     true
    // }
    //
    // /// Simply updates the 5 minute bar in the appropriate database
    // /// Add Duration::minutes(5)
    // /// - Assumption: Bar updates every 5 minutes
    // async fn on_bar_update(
    //     historical_data_crud: &HistoricalDataCRUD,
    //     historical_options_data_crud: &HistoricalOptionsDataCRUD,
    //     sender: &Sender<(Contract, RealtimeWhatToShow, DateTime<chrono::Utc>)>,
    //     contract: &Contract,
    //     time: DateTime<chrono::Utc>,
    //     open: f64,
    //     high: f64,
    //     low: f64,
    //     close: f64,
    //     volume: f64,
    //     data_type: RealtimeWhatToShow,
    // ) {
    //     let symbol = get_local_symbol(&contract);
    //     match AssetType::from_str(&contract.security_type) {
    //         AssetType::Option => {
    //             match historical_options_data_crud
    //                 .create_or_update(
    //                     &HistoricalOptionsDataPrimaryKeys {
    //                         stock: symbol.clone(),
    //                         primary_exchange: contract.primary_exchange.as_str().to_string(),
    //                         currency: contract.currency.as_str().to_string(),
    //                         expiry: contract.last_trade_date_or_contract_month.clone(),
    //                         strike: contract.strike.clone(),
    //                         multiplier: contract.multiplier.clone(),
    //                         option_type: OptionType::from_str(&contract.right)
    //                             .unwrap_or_else(|e| panic!("{}", e)),
    //                         time: time,
    //                     },
    //                     &HistoricalOptionsDataUpdateKeys {
    //                         open: Some(open),
    //                         high: Some(high),
    //                         low: Some(low),
    //                         close: Some(close),
    //                         volume: Some(
    //                             Decimal::from_f64(volume * 100.0)
    //                                 .expect("Expected to be able to parse f64 to Decimal"),
    //                         ),
    //                     },
    //                 )
    //                 .await
    //             {
    //                 Ok(_) => {
    //                     if let Err(e) = sender.send((contract.clone(), data_type, time)).await {
    //                         tracing::error!(
    //                             message=%format!(
    //                                 "Error occurred while sending bar update to channel for {}:{} at {}: {}",
    //                                 contract.security_type,
    //                                 symbol,
    //                                 time,
    //                                 e
    //                             )
    //                         );
    //                     }
    //                 }
    //                 Err(e) => tracing::error!(
    //                     "Error occurred while trying to insert new bar to HistoricalOptionsData: {e:?}"
    //                 ),
    //             };
    //         }
    //         AssetType::Stock | AssetType::Future => {
    //             match historical_data_crud
    //                 .create_or_update(
    //                     &HistoricalDataPrimaryKeys {
    //                         stock: symbol.clone(),
    //                         primary_exchange: contract.primary_exchange.to_string(),
    //                         currency: contract.currency.to_string(),
    //
    //                         time: time,
    //                     },
    //                     &HistoricalDataUpdateKeys {
    //                         open: Some(open),
    //                         high: Some(high),
    //                         low: Some(low),
    //                         close: Some(close),
    //                         volume: Some(
    //                             Decimal::from_f64(volume * 100.0)
    //                                 .expect("Expected to be able to parse f64 to Decimal"),
    //                         ),
    //                     },
    //                 )
    //                 .await
    //             {
    //                 Ok(_) => {
    //                     if let Err(e) = sender.send((contract.clone(), data_type, time)).await {
    //                         tracing::error!(
    //                             message=%format!(
    //                                 "Error occurred while sending bar update to channel for {}:{} at {}: {}",
    //                                 contract.security_type,
    //                                 symbol,
    //                                 time,
    //                                 e
    //                             )
    //                         );
    //                     }
    //                 }
    //                 Err(e) => tracing::error!(
    //                     "Error occurred while trying to insert new bar to HistoricalStockData: {e:?}"
    //                 ),
    //             };
    //         }
    //         AssetType::ForexPair | AssetType::CFD => {
    //             tracing::warn!("Updating forex pair or cfd thru wrong branch");
    //         }
    //         _ => {
    //             tracing::error!(
    //                 message=%format!(
    //                     "Received bar update for unknown security_type: {} ({})",
    //                     contract.security_type,
    //                     symbol
    //                 )
    //             )
    //         }
    //     }
    // }
    //
    // /// Simply updates the 5 minute bar in the appropriate database
    // /// Add Duration::minutes(5)
    // /// - Assumption: Bar updates every 5 minutes
    // async fn on_forex_bar_update(
    //     historical_forex_data_crud: &HistoricalForexDataCRUD,
    //     sender: &Sender<(Contract, RealtimeWhatToShow, DateTime<chrono::Utc>)>,
    //     contract: &Contract,
    //     time: DateTime<chrono::Utc>,
    //     bid_open: f64,
    //     bid_high: f64,
    //     bid_low: f64,
    //     bid_close: f64,
    //     _bid_v: f64,
    //     ask_open: f64,
    //     ask_high: f64,
    //     ask_low: f64,
    //     ask_close: f64,
    //     _ask_v: f64,
    //     data_type: RealtimeWhatToShow,
    //     is_full_bar_updated: bool,
    // ) {
    //     let symbol = get_local_symbol(&contract);
    //
    //     match AssetType::from_str(&contract.security_type) {
    //         AssetType::ForexPair | AssetType::CFD => {
    //             match historical_forex_data_crud
    //                 .create_or_update(
    //                     &HistoricalForexDataPrimaryKeys {
    //                         pair: symbol.clone(),
    //                         time: time,
    //                     },
    //                     &HistoricalForexDataUpdateKeys {
    //                         bid_open: Some(bid_open),
    //                         bid_high: Some(bid_high),
    //                         bid_low: Some(bid_low),
    //                         bid_close: Some(bid_close),
    //                         ask_open: Some(ask_open),
    //                         ask_high: Some(ask_high),
    //                         ask_low: Some(ask_low),
    //                         ask_close: Some(ask_close),
    //                     },
    //                 )
    //                 .await
    //             {
    //                 Ok(_is_bar_complete) => {
    //                     if !is_full_bar_updated {
    //                         return;
    //                     }
    //
    //                     if let Err(e) = sender.send((contract.clone(), data_type, time)).await {
    //                         tracing::error!(
    //                             message=%format!(
    //                                 "Error occurred while sending bar update to channel for {}:{} at {}: {}",
    //                                 contract.security_type,
    //                                 contract.symbol,
    //                                 time,
    //                                 e
    //                             )
    //                         );
    //                     }
    //                 }
    //                 Err(e) => tracing::error!(
    //                     "Error occurred while trying to insert new bar to HistoricalStockData: {e:?}",
    //                 ),
    //             };
    //         }
    //
    //         _ => {
    //             tracing::error!(
    //                 message=%format!(
    //                     "Received bar update for unknown security_type: {} ({})",
    //                     contract.security_type,
    //                     symbol
    //                 )
    //             )
    //         }
    //     }
    // }
}

fn last_bar_available_time_forex(now: DateTime<Tz>) -> DateTime<Tz> {
    let mut cursor = now;
    while !is_fx_trading_datetime(&cursor) {
        cursor -= Duration::from_mins(1);
    }
    cursor
}

/// Grid-based variant used by stock and option: floor-to-grid, optionally
/// cap at a session close, then gate on a session-open threshold.
/// Returns `None` if we're before the open threshold (nothing to refresh yet).
fn last_bar_available(now: DateTime<Tz>, asset_type: &AssetType) -> Option<DateTime<Tz>> {
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

fn fx_trading_day_start(date: &NaiveDate, tz: &Tz) -> DateTime<Tz> {
    const FX_WEEK_OPEN_HOUR: u32 = 17; // Sunday 17:00 NY
    //
    let prev_day = date.pred_opt().unwrap();
    tz.from_local_datetime(&prev_day.and_hms_opt(FX_WEEK_OPEN_HOUR, 0, 0).unwrap())
        .single()
        .unwrap()
}
