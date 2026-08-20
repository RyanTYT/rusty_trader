use std::{collections::HashMap, mem::ManuallyDrop, sync::Arc, time::Duration};

use ibapi::{Client, contracts::Contract, prelude::RealtimeWhatToShow};
use spmc_ring::bench::RingBuffer;
use sqlx::PgPool;
use tokio::time::sleep;

use crate::{
    database::{crud::CRUDTrait, models::StrategyFullKeys, models_crud::strategy::StrategyCRUD},
    execution::{
        fx_backed_up_order::OrderStore,
        order_engine::OrderEngine,
        order_update_stream::controller::OrderUpdateStreamController,
        syncer::{SyncOps, SyncerEngine},
    },
    market_data::{
        consolidator::Consolidator,
        consumer::strategy_consumer::{IbkrBarConsumer, StrategyDataBundler},
        handler::{DataSubscription, DbSubscriptionMethod, MarketDataHandler},
    },
    schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler},
    strategy::{
        manual::Manual,
        noise::Noise,
        strategy::{StrategyEnum, StrategyExecutor},
        unknown::Unknown,
    },
};

const BUFFER_CAPACITY: usize = 128;
const MAX_NUM_CONSUMERS: usize = 10;

// Order matters here: strategy_handlers dropped first
// - consolidator holding impt stuff last to drop to prevent panics
pub struct IbkrState {
    strategy_handlers: Vec<StrategyDataBundler<BUFFER_CAPACITY, MAX_NUM_CONSUMERS>>,
    order_update_stream_controller: OrderUpdateStreamController,
    pub consolidator: Arc<Consolidator>,
}

impl IbkrState {
    pub async fn async_drop(&mut self) {
        self.order_update_stream_controller.async_drop().await;
        futures::future::join_all(
            self.strategy_handlers
                .iter_mut()
                .map(|strategy_handler| strategy_handler.async_drop()),
        )
        .await;

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

impl Drop for IbkrState {
    fn drop(&mut self) {
        let count = Arc::strong_count(&self.consolidator);
        println!("Dropping IBKRState");
        println!("consolidator arc count: {:?}", count);
    }
}

pub enum ApplicationState {
    IbkrState(IbkrState),
}

impl ApplicationState {
    pub async fn async_drop(&mut self) {
        match self {
            Self::IbkrState(state) => state.async_drop().await,
        }
    }
}

#[derive(Clone)]
pub struct StrategyParameters {
    // pub visibility FROM pub(crate) visibility ONLY for testing purposes
    pub strategy: StrategyEnum,
    // pub visibility FROM pub(crate) visibility ONLY for testing purposes
    // used to provide time for warm up before market open so be conservative
    pub subscribed_contracts: Vec<DataSubscription>,
}

pub async fn init_strategies(
    pool: PgPool,
    client: Arc<Client>,
    handle: tokio::runtime::Handle,
) -> Vec<StrategyParameters> {
    let noise = StrategyEnum::Noise(Noise::new(pool.clone(), handle.clone()));
    let manual = StrategyEnum::Manual(Manual::new(pool.clone()));
    let unknown = StrategyEnum::Unknown(Unknown::new(pool.clone()));

    let noise_contract = noise
        .get_contracts(client.clone())
        .first()
        .expect("Expected QQQ contract")
        .clone();
    let noise_strat_params = StrategyParameters {
        strategy: noise.clone(),
        subscribed_contracts: vec![DataSubscription::new(
            noise_contract.clone(),
            RealtimeWhatToShow::Trades,
        )],
    };

    let manual_contract = manual
        .get_contracts(client.clone())
        .first()
        .expect("Expected Manual contract")
        .clone();
    let manual_params = StrategyParameters {
        strategy: manual.clone(),
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
        strategy: unknown.clone(),
        subscribed_contracts: vec![
            DataSubscription::new(unknown_contract.clone(), RealtimeWhatToShow::Bid),
            DataSubscription::new(unknown_contract.clone(), RealtimeWhatToShow::Ask),
        ],
    };

    let res = vec![noise_strat_params, manual_params, unknown_params];

    for strat_param in res.iter() {
        let strategy_crud = StrategyCRUD::new(pool.clone());
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

/// retry_times: 1 - retry once
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

pub async fn init_app(
    api_port_addr: &str,
    account: &'static str,
    pool: PgPool,
    // ibc_log_file: &'static str,
    // strat_params: Vec<StrategyParameters>,
    default_strategy: String,
) -> Result<ApplicationState, String> {
    // ===================================
    // Connect to clients
    // ===================================
    // give up to 3 minutes leeway
    let master_client_unwrapped = connect_to_client_with_retry(&api_port_addr, 0, 6).await?;
    let master_client = Arc::new(master_client_unwrapped);
    tracing::info!(message=%format!("Connected to client {}", master_client.client_id()));

    let client_1_unwrapped = connect_to_client_with_retry(&api_port_addr, 1, 1).await?;
    let client_1 = Arc::new(client_1_unwrapped);
    tracing::info!(
        message=%format!(
            "Connected to client {}", client_1.client_id()
        )
    );

    // ===================================
    // Initialise Consolidator/OrderEngine
    // ===================================
    let backed_up_orders =
        Arc::new(OrderStore::open().expect("Expected opening order store to work"));
    let order_engine = OrderEngine::new(pool.clone(), tokio::runtime::Handle::current());
    let strat_params = init_strategies(
        pool.clone(),
        client_1.clone(),
        tokio::runtime::Handle::current(),
    )
    .await;
    let strategy_map = {
        let mut raw_strategy_map = HashMap::new();
        for strat_param in strat_params.iter() {
            raw_strategy_map.insert(
                strat_param.strategy.get_name(),
                strat_param.strategy.clone(),
            );
        }
        Arc::new(raw_strategy_map)
    };
    let mut raw_contract_scheduler = IbkrContractScheduler::new(client_1.clone());
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
        &Arc::downgrade(&client_1),
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
        client_1.clone(),
        mkt_data_handler,
        contract_scheduler.clone(),
    ));

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

    let order_update_stream_controller = OrderUpdateStreamController::new(
        pool.clone(),
        Arc::downgrade(&master_client),
        strategy_map,
        Some(default_strategy.clone()),
        tokio::runtime::Handle::current(),
        backed_up_orders.clone(),
    )
    .expect("Expected OrderUpdateStreamController initialisation to be ok");

    let handle = tokio::runtime::Handle::current();
    let strategy_threads = strat_params.into_iter().map(|strat_param| {
        let mut strategy_data_bundler = StrategyDataBundler::new(contract_scheduler.clone());
        let cloned_consolidator = consolidator.clone();
        let cloned_order_engine = order_engine.clone();
        let cloned_client_1 = client_1.clone();
        let cloned_backed_up_orders = backed_up_orders.clone();
        let handle = handle.clone();
        async move {
            let strategy = strat_param.strategy.clone();
            let strat_name = strategy.get_name();
            let consolidator = cloned_consolidator.clone();
            if let Err(e) = tokio::task::spawn_blocking(move || {
                handle.block_on(strategy.warm_up_data(&consolidator))
            })
            .await
            {
                tracing::error!("Failed to initialise strategy ({}): {e:?}", strat_name)
            };
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
                strat_param.strategy,
                cloned_order_engine.clone(),
                Arc::downgrade(&cloned_consolidator),
                Arc::downgrade(&cloned_client_1),
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
