//! Live application boot: IBKR connection, market data, sync, funding, strategies.
//!
//! [`init_app`] is the single live entry point. Its order is load-bearing:
//!
//! 1. connect the master client (id `0`, the only client allowed to own the
//!    order-update subscription);
//! 2. open the parked-order store;
//! 3. build the strategy roster (and their DB rows);
//! 4. install the contract scheduler + market-data handler and wrap them in a
//!    [`Consolidator`](crate::market_data::consolidator::Consolidator);
//! 5. create one execution-event ring per strategy and sync against IBKR
//!    (open orders, executions, positions);
//! 6. allocate the strategy funds;
//! 7. start the order-update controller, then warm up every strategy and hook
//!    its consumers.
//!
//! Nothing trades before step 7 completes.

use crate::database::tables::DbTables;
use std::{collections::HashMap, sync::Arc, time::Duration};

use ibapi::{Client, contracts::Contract, orders::ExecutionData, prelude::RealtimeWhatToShow};
use spmc_ring::ring_buffer::spmc_ring_buffer::{SpmcRingBuffer, SpmcRingBufferProducer};
use sqlx::PgPool;
use tokio::time::sleep;

use crate::{
    database::{crud::CRUDTrait, models::StrategyFullKeys},
    execution::{
        fx_backed_up_order::OrderStore,
        order_engine::OrderEngine,
        order_update_stream::controller::OrderUpdateStreamController,
        syncer::{SyncOps, SyncerEngine},
    },
    helpers::allocator,
    market_data::{
        consolidator::Consolidator,
        consumer::strategy_consumer::{IbkrBarConsumer, StrategyDataBundler},
        handler::{DataSubscription, DbSubscriptionMethod, MarketDataHandler},
    },
    schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler},
    strategy::{
        manual::Manual,
        noise::Noise,
        strategy::{StrategyEnum, StrategyExecutor, StrategyFund},
        unknown::Unknown,
    },
};

/// Ring capacity of every strategy bar/execution ring.
const BUFFER_CAPACITY: usize = 128;
/// Maximum number of consumers (strategies) attached to one subscription ring.
const MAX_NUM_CONSUMERS: usize = 10;

/// Build the Postgres connection options for the application pool.
///
/// Forwards the deployment profile to Postgres as the `trading_app.env` GUC.
/// The init migration reads that GUC in its ENVIRONMENT block to pick the
/// resource profile (`local` | `small` | `medium` | `large`; the block's comment
/// documents which RAM size each one is tuned for). Falls back to `small`,
/// matching the migration's own DEFAULT_ENV.
pub fn pg_connect_options(database_url: &str) -> sqlx::postgres::PgConnectOptions {
    let profile = std::env::var("MEMORY_LIMIT").unwrap_or_else(|_| String::from("small"));
    database_url
        .parse::<sqlx::postgres::PgConnectOptions>()
        .map(|opts| opts.options([("trading_app.env", profile)]))
        .expect("Expected DATABASE_URL to be a valid Postgres connection string")
}

/// Live runtime state: the strategy handlers, the order-update controller and
/// the consolidator.
///
/// Field order is intentional: handlers drop first and the consolidator last,
/// because the handlers (and their market-data consumers) hold weak references
/// into the consolidator and would panic if it disappeared first.
pub struct IbkrState {
    strategy_handlers: Vec<StrategyDataBundler<BUFFER_CAPACITY, MAX_NUM_CONSUMERS>>,
    order_update_stream_controller: OrderUpdateStreamController,
    /// Shared market-data facade; also cloned into strategy/order-engine code.
    pub consolidator: Arc<Consolidator>,
}

impl IbkrState {
    /// Tear the live state down in dependency order.
    ///
    /// Stops the order-update stream, joins every strategy handler, then waits
    /// until the consolidator's last strong reference is gone before asking it
    /// to shut down. Because the surrounding code may still hold clones, this
    /// spin is intentionally unbounded: either the consolidator is released, or
    /// the teardown stalls until it is.
    pub async fn async_drop(&mut self) {
        println!("Dropping order_update_stream");
        self.order_update_stream_controller.async_drop().await;
        println!("Dropping strategy handlers");
        futures::future::join_all(
            self.strategy_handlers
                .iter_mut()
                .map(|strategy_handler| strategy_handler.async_drop()),
        )
        .await;

        println!("Dropping consolidator");
        // Notably there is an infinite loop here - either consolidator is torn down
        // properly, or the thread stalls until such
        loop {
            match Arc::get_mut(&mut self.consolidator) {
                Some(consolidator) => {
                    consolidator.async_drop().await;
                    break;
                }
                None => continue,
            };
        }
    }
}

/// Synchronous drop only logs the consolidator's strong count; real teardown
/// must go through [`IbkrState::async_drop`].
impl Drop for IbkrState {
    fn drop(&mut self) {
        let count = Arc::strong_count(&self.consolidator);
        println!("Dropping IBKRState");
        println!("consolidator arc count: {:?}", count);
    }
}

/// Top-level application state; one variant today (live IBKR).
pub enum ApplicationState {
    IbkrState(IbkrState),
}

impl ApplicationState {
    /// Delegate teardown to the live state.
    pub async fn async_drop(&mut self) {
        match self {
            Self::IbkrState(state) => state.async_drop().await,
        }
    }
}

/// One roster entry: the strategy and the subscriptions it needs.
pub struct StrategyParameters {
    /// The strategy instance itself.
    ///
    /// Public (rather than `pub(crate)`) so integration tests can build rosters.
    pub strategy: StrategyEnum,
    /// Contracts to subscribe for the strategy, with their realtime stream.
    ///
    /// Public for tests. Warm-up is sized from these, so keep the list
    /// conservative — every subscription costs a ring slot.
    pub subscribed_contracts: Vec<DataSubscription>,
}

/// Build the live strategy roster.
///
/// Ensures the allocator's `UNALLOCATED` strategy row exists (cash/equity writes
/// are keyed by strategy name, though it is never a roster participant), then
/// constructs:
///
/// - `manual` and `unknown` with bid + ask subscriptions on their single
///   contract;
///
/// Every roster strategy then gets a `create_or_ignore` row (`Status::Active`);
/// DB failures are logged, not returned.
///
/// # Panics
///
/// `manual`/`unknown` resolve no contract, or sleeve construction exhausts the
/// ring producer/consumer slots.
pub async fn init_strategies(
    pool: PgPool,
    client: Arc<Client>,
    handle: tokio::runtime::Handle,
) -> Vec<StrategyParameters> {
    // The allocator's slack bucket needs a strategy row (cash/equity writes are
    // keyed by strategy name) even though it is never a roster participant.
    let mut db = crate::database::db::DbDirect::connect(&pool);
    if let Err(e) = db
        .strategy()
        .create_or_ignore(&StrategyFullKeys {
            strategy: allocator::UNALLOCATED.to_string(),
            status: crate::database::models::Status::Active,
        })
        .await
    {
        tracing::error!(
            "Error occurred trying to create {} strategy: {e:?}",
            allocator::UNALLOCATED
        )
    }

    let mut manual = StrategyEnum::Manual(Manual::new(pool.clone()));
    let mut unknown = StrategyEnum::Unknown(Unknown::new(pool.clone()));



    let manual_contract = manual
        .get_contracts(client.clone())
        .first()
        .expect("Expected Manual contract")
        .clone();
    let manual_params = StrategyParameters {
        strategy: manual,
        subscribed_contracts: vec![
            DataSubscription::new(manual_contract.clone(), RealtimeWhatToShow::Bid),
            DataSubscription::new(manual_contract.clone(), RealtimeWhatToShow::Ask),
        ],
    };

    let unknown_contract = unknown
        .get_contracts(client.clone())
        .first()
        .expect("Expected Unknown contract")
        .clone();
    let unknown_params = StrategyParameters {
        strategy: unknown,
        subscribed_contracts: vec![
            DataSubscription::new(unknown_contract.clone(), RealtimeWhatToShow::Bid),
            DataSubscription::new(unknown_contract.clone(), RealtimeWhatToShow::Ask),
        ],
    };


    // };

    let mut res = vec![
        manual_params,
        unknown_params,
    ];

    let mut db = crate::database::db::DbDirect::connect(&pool);
    for strat_param in res.iter() {
        let mut strategy_crud = db.strategy();
        if let Err(e) = strategy_crud
            .create_or_ignore(&StrategyFullKeys {
                strategy: strat_param.strategy.get_name(),
                status: crate::database::models::Status::Active,
            })
            .await
        {
            tracing::error!("Error occurred trying to create new Noise strategy: {e:?}")
        }
    }

    res
}

/// Connect one IBKR client, retrying on failure.
///
/// `retry_times` is the number of *retries* after the initial attempt
/// (`1` = try twice total), with a 30-second sleep between attempts.
///
/// # Errors
///
/// Every attempt failed; the message names the client id.
async fn connect_to_client_with_retry(
    api_port_addr: &str,
    client_id: i32,
    retry_times: u32,
) -> Result<Client, String> {
    let mut retry_time = 0;
    let client_opt = loop {
        let try_client = match Client::connect(api_port_addr, client_id) {
            Ok(connected_client) => Some(connected_client),
            Err(e) => {
                tracing::error!(
                    "Connection to TWS via \nURL: localhost:4002\n Client Id: {client_id}\n failed!\nError: {}",
                    e
                );
                retry_time += 1;
                if retry_time <= retry_times {
                    tracing::error!("Retrying for {retry_time:?} time!");
                    sleep(Duration::from_secs(30)).await;
                    continue;
                }
                None
            }
        };
        break try_client;
    };

    client_opt.ok_or(format!("Error: Could not connect to client {client_id}"))
}

/// Boot the live application and return its state.
///
/// # Arguments
///
/// - `api_port_addr` — IBKR gateway address (`host:port`);
/// - `account` — IBKR account id used by the position sync;
/// - `pool` — Postgres pool;
/// - `default_strategy` — the fallback strategy name for unattributed fills and
///   open orders.
///
/// # Errors
///
/// The master client cannot connect after its retries, or the fund allocator
/// fails. Every other step logs and continues.
///
/// # Panics
///
/// The parked-order store cannot be opened (`ORDERS_FILE_PATH`), the
/// order-update controller already exists in this process, a strategy
/// subscription was not registered, or a consumer slot is exhausted.
pub async fn init_app(
    api_port_addr: &str,
    account: &'static str,
    pool: PgPool,
    // ibc_log_file: &'static str,
    // strat_params: Vec<StrategyParameters>,
    default_strategy: String,
) -> Result<ApplicationState, String> {
    // let gateway = init_ibc_with_retry(ibc_log_file, 2).await?;

    // ===================================
    // Connect to clients
    // ===================================
    // give up to 3 minutes leeway
    let master_client_unwrapped = connect_to_client_with_retry(&api_port_addr, 0, 6).await?;
    let master_client = Arc::new(master_client_unwrapped);
    tracing::info!(message=%format!("Connected to client {}", master_client.client_id()));

    // let client_1_unwrapped = connect_to_client_with_retry(&api_port_addr, 1, 1).await?;
    // let client_1 = Arc::new(client_1_unwrapped);
    // tracing::info!(
    //     message=%format!(
    //         "Connected to client {}", client_1.client_id()
    //     )
    // );

    // ===================================
    // Initialise Consolidator/OrderEngine
    // ===================================
    let backed_up_orders =
        Arc::new(OrderStore::open().expect("Expected opening order store to work"));
    let order_engine = OrderEngine::new(pool.clone(), tokio::runtime::Handle::current());
    let mut strat_params = init_strategies(
        pool.clone(),
        master_client.clone(),
        tokio::runtime::Handle::current(),
    )
    .await;
    let strategy_details = {
        let mut raw_strategy_map = HashMap::new();
        for strat_param in strat_params.iter() {
            let strategy_detail = strat_param.strategy.get_strategy_details();
            raw_strategy_map.insert(strategy_detail.name.clone(), strategy_detail);
        }
        Arc::new(raw_strategy_map)
    };
    let mut raw_contract_scheduler = IbkrContractScheduler::new(master_client.clone());
    if let Err(e) = raw_contract_scheduler.add_all_schedules(
        strat_params
            .iter()
            .flat_map(|strat_param| {
                strat_param
                    .subscribed_contracts
                    .iter()
                    .map(|subscription| subscription.contract.clone())
            })
            .collect::<Vec<Contract>>(),
    ) {
        tracing::error!("Failed to add all schedules of contracts: {e:?}");
    };
    let contract_scheduler = Arc::new(raw_contract_scheduler);
    // populate contract_scheduler with full contract with correct contract ids
    let mut mkt_data_handler = MarketDataHandler::new(pool.clone());
    mkt_data_handler.load_all_subscription_producers(
        &Arc::downgrade(&master_client),
        contract_scheduler.clone(),
        strat_params
            .iter()
            .flat_map(|strat_param| strat_param.subscribed_contracts.clone())
            .collect(),
        DbSubscriptionMethod::GroupedPerThread,
        tokio::runtime::Handle::current(),
    );
    let consolidator = Arc::new(Consolidator::new(
        tokio::runtime::Handle::current(),
        pool.clone(),
        master_client.clone(),
        mkt_data_handler,
        contract_scheduler.clone(),
    ));

    // ===================================
    // Execution Events
    // ===================================
    let execution_events_queues: Arc<HashMap<String, Arc<SpmcRingBuffer<ExecutionData, 64, 1>>>> =
        Arc::new(
            strat_params
                .iter()
                .map(|strat_param| {
                    let execution_data_queue =
                        Arc::new(SpmcRingBuffer::<ExecutionData, 64, 1>::new());
                    (strat_param.strategy.get_name(), execution_data_queue)
                })
                .collect(),
        );
    let execution_events_producers: Arc<
        HashMap<String, SpmcRingBufferProducer<ExecutionData, 64, 1>>,
    > = Arc::new(
        execution_events_queues
            .iter()
            .map(|(strat, queue)| {
                (
                    strat.clone(),
                    queue
                        .get_new_producer()
                        .expect("Expected to be able to get producer from ExecutionData queue"),
                )
            })
            .collect(),
    );

    // ===================================
    // Sync
    // ===================================
    let syncer = SyncerEngine::new(
        pool.clone(),
        account.to_string(),
        &strat_params,
        tokio::runtime::Handle::current(),
    );
    syncer.sync_open_orders(
        &master_client,
        &consolidator,
        Some(default_strategy.clone()),
    );
    if let Err(e) = syncer.sync_executions(
        &master_client,
        execution_events_producers.clone(),
        &Arc::downgrade(&consolidator),
        Some(default_strategy.clone()),
        backed_up_orders.clone(),
    ) {
        tracing::error!("Failed to sync executions before beginning: {e:?}");
    };
    syncer
        .sync_positions(
            &master_client,
            &consolidator,
            Some(default_strategy.clone()),
        )
        .await;

    tracing::error!("Finished Syncing");

    // ===================================
    // Seed Strategy Funds
    // ===================================
    let fundings = strat_params
        .iter_mut()
        .map(|strat_param| {
            (
                strat_param.strategy.get_name(),
                strat_param.strategy.fund_strategy(),
            )
        })
        .collect::<Vec<(String, StrategyFund)>>();

    let handle = tokio::runtime::Handle::current();
    let client_cloned = master_client.clone();
    let consolidator_cloned = consolidator.clone();
    let pool_cloned = pool.clone();

    tokio::task::spawn_blocking(move || {
        allocator::fund_strategies(
            &handle,
            &client_cloned,
            &consolidator_cloned,
            &fundings,
            pool_cloned,
        )
    })
    .await
    .map_err(|e| format!("Failed to allocate funds for strategies: {e:?}"))??;

    let order_update_stream_controller = OrderUpdateStreamController::new(
        pool.clone(),
        Arc::downgrade(&consolidator),
        Arc::downgrade(&master_client),
        strategy_details,
        execution_events_producers,
        Some(default_strategy.clone()),
        tokio::runtime::Handle::current(),
        backed_up_orders.clone(),
    )
    .expect("Expected OrderUpdateStreamController initialisation to be ok");

    // let handle = tokio::runtime::Handle::current();
    let strategy_threads = strat_params.into_iter().map(|strat_param| {
        let mut strategy_data_bundler = StrategyDataBundler::new(contract_scheduler.clone());
        let cloned_consolidator = consolidator.clone();
        let cloned_order_engine = order_engine.clone();
        // let cloned_client_1 = client_1.clone();
        let cloned_master_client = master_client.clone();
        let cloned_backed_up_orders = backed_up_orders.clone();
        let execution_events_consumer = execution_events_queues
            .get(&strat_param.strategy.get_name())
            .expect(
                "Expected execution_events_queue \
                to be initialised before accessing!",
            )
            .get_new_consumer()
            .expect("Expected to be able to get consumer from execution_events_queue");
        // let handle = handle.clone();
        async move {
            let mut strategy = strat_param.strategy;
            // let strat_name = strategy.get_name();
            // let consolidator = cloned_consolidator.clone();
            if let Err(e) = strategy.warm_up_data(&cloned_consolidator).await {
                tracing::error!(
                    "Failed to warm up {:?} properly: {e:?}",
                    strategy.get_name()
                );
            };

            // if let Err(e) = tokio::task::spawn_blocking(move || {
            //     handle.block_on(strategy.warm_up_data(&consolidator))
            // })
            // .await
            // {
            //     tracing::error!("Failed to initialise strategy ({}): {e:?}", strat_name)
            // };
            let consumers = strat_param
                .subscribed_contracts
                .iter()
                .map(|subscription| {
                    IbkrBarConsumer::new(
                        subscription.contract.clone(),
                        subscription.what_to_show,
                        cloned_consolidator
                            .market_data_handler
                            .get_subsription(subscription)
                            .expect("Expected subscription to already exist beforehand")
                            .get_new_consumer()
                            .expect("Expected max no. of consumers not to be exceeded"),
                    )
                })
                .collect();
            strategy_data_bundler.hook_strategy(
                consumers,
                execution_events_consumer,
                strategy,
                cloned_order_engine.clone(),
                Arc::downgrade(&cloned_consolidator),
                Arc::downgrade(&cloned_master_client),
                Arc::downgrade(&cloned_backed_up_orders),
            );

            strategy_data_bundler
        }
    });

    let strategy_handlers = futures::future::join_all(strategy_threads).await;

    Ok(ApplicationState::IbkrState(IbkrState {
        consolidator,
        strategy_handlers,
        order_update_stream_controller,
    }))
}
