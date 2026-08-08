use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use chrono::{TimeDelta, Utc};
use ibapi::{
    Client,
    prelude::{Contract, RealtimeWhatToShow},
};
use sqlx::PgPool;
use tokio::{sync::oneshot::Sender, time::sleep};

use crate::{
    database::{crud::CRUDTrait, models::StrategyFullKeys, models_crud::strategy::StrategyCRUD},
    execution::{
        fx_backed_up_order::OrderStore,
        order_engine::OrderEngine,
        order_update_stream::controller::OrderUpdateStreamController,
        syncer::{SyncOps, SyncerEngine},
    },
    ibc::{IBGateway, init_ibc_with_retry},
    market_data::{
        consolidator::Consolidator,
        consumer::strategy_consumer::{IbkrBarConsumer, StrategyDataBundler},
        handler::{DataSubscription, DbSubscriptionMethod, MarketDataHandler},
    },
    schedule::contract_scheduler::IbkrContractScheduler,
    strategy::{
        manual::Manual,
        noise::Noise,
        strategy::{StrategyEnum, StrategyExecutor},
        unknown::Unknown,
    },
};

pub struct IbkrState {
    pub consolidator: Arc<Consolidator>,
}

pub enum ApplicationState {
    IbkrState(IbkrState),
}

#[derive(Clone)]
pub struct StrategyParameters {
    pub(crate) strategy: StrategyEnum,
    // used to provide time for warm up before market open so be conservative
    estimated_time_to_warm_up: Duration,
    pub(crate) subscribed_contracts: Vec<DataSubscription>,
}

pub fn init_strategies(
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
        estimated_time_to_warm_up: Duration::from_secs(40),
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
        estimated_time_to_warm_up: Duration::from_secs(1),
        subscribed_contracts: vec![DataSubscription::new(
            manual_contract.clone(),
            RealtimeWhatToShow::Trades,
        )],
    };

    let unknown_contract = unknown
        .get_contracts(client.clone())
        .first()
        .expect("Expected Unknown contract")
        .clone();
    let unknown_params = StrategyParameters {
        strategy: unknown.clone(),
        estimated_time_to_warm_up: Duration::from_secs(1),
        subscribed_contracts: vec![DataSubscription::new(
            unknown_contract.clone(),
            RealtimeWhatToShow::Trades,
        )],
    };

    let res = vec![noise_strat_params, manual_params, unknown_params];

    for strat_param in res.iter() {
        let strategy_crud = StrategyCRUD::new(pool.clone());
        handle.clone().block_on(async move {
            if let Err(e) = strategy_crud
                .create_or_ignore(&StrategyFullKeys {
                    strategy: "unknown".to_string(),
                    status: crate::database::models::Status::Active,
                })
                .await
            {
                tracing::error!("Error occurred trying to create new Noise strategy: {e:?}")
            }
        });
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
                    "Connection to TWS via \nURL: localhost:4002\n Client Id: 0\n failed!\nError: {}",
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

    client_opt.ok_or("Error: Could not connect to client".to_string())
}

pub async fn init_app(
    api_port_addr: &str,
    account: &'static str,
    pool: PgPool,
    ibc_log_file: &'static str,
    // strat_params: Vec<StrategyParameters>,
    default_strategy: String,
) -> Result<ApplicationState, String> {
    let _gateway = init_ibc_with_retry(ibc_log_file, 2).await?;

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
    let order_engine = OrderEngine::new(pool.clone(), tokio::runtime::Handle::current());
    let strat_params = init_strategies(
        pool.clone(),
        client_1.clone(),
        tokio::runtime::Handle::current(),
    );
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
    let _order_update_stream_controller = OrderUpdateStreamController::new(
        pool.clone(),
        Arc::downgrade(&master_client),
        strategy_map,
        Some(default_strategy.clone()),
    );
    let contract_scheduler = Arc::new(IbkrContractScheduler::new(client_1.clone()));
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
    );
    let consolidator = Arc::new(Consolidator::new(
        tokio::runtime::Handle::current(),
        pool.clone(),
        client_1.clone(),
        mkt_data_handler,
    ));
    let strategy_data_bundler = StrategyDataBundler::new(contract_scheduler);
    let backed_up_orders =
        Arc::new(OrderStore::open().expect("Expected opening order store to work"));

    let syncer = SyncerEngine::new(pool.clone(), account.to_string(), &strat_params);
    syncer.sync_open_orders(
        &master_client,
        &consolidator,
        Some(default_strategy.clone()),
    );
    if let Err(e) = syncer.sync_executions(&master_client, Some(default_strategy.clone())) {
        tracing::error!("Failed to sync executions before beginning: {e:?}");
    };
    syncer
        .sync_positions(
            &master_client,
            &consolidator,
            Some(default_strategy.clone()),
        )
        .await;

    // warm up the strats first
    std::thread::scope(|s| {
        for strat_param in strat_params.iter() {
            s.spawn(|| {
                if let Err(e) = strat_param.strategy.warm_up_data(&consolidator) {
                    tracing::error!(
                        "Failed to initialise strategy: {}",
                        strat_param.strategy.get_name()
                    )
                };
            });
        }
    });

    for strat_param in strat_params.iter() {
        // strat_param.subscribed_contracts
        let consumers = strat_param
            .subscribed_contracts
            .iter()
            .map(|subscription| {
                IbkrBarConsumer::new(
                    subscription.contract.clone(),
                    subscription.what_to_show,
                    consolidator
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
            strat_param.strategy.clone(),
            order_engine.clone(),
            Arc::downgrade(&consolidator),
            Arc::downgrade(&client_1),
            Arc::downgrade(&backed_up_orders),
        );
    }

    Ok(ApplicationState::IbkrState(IbkrState { consolidator }))
}
