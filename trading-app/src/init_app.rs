use std::{
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
    execution::order_engine::OrderEngine,
    ibc::{IBGateway, init_ibc_with_retry},
    market_data::{
        account_tracker::AccountTracker, consolidator::Consolidator,
        strategy_scheduler::StrategyScheduler,
    },
    strategy::{
        manual::Manual,
        noise::Noise,
        strategy::{StrategyEnum, StrategyExecutor},
        unknown::Unknown,
    },
};

pub struct IbkrState {
    pub cancel_senders: Vec<Sender<()>>,
    pub consolidator: Arc<Consolidator>,
    pub order_engine: Arc<OrderEngine>,
    pub master_client: Arc<Client>,
    pub default_strategy: String,
    pub _gateway: IBGateway,
}

impl Drop for IbkrState {
    fn drop(&mut self) {
        let senders = std::mem::take(&mut self.cancel_senders);
        for cancel_sender in senders {
            if let Err(e) = cancel_sender.send(()) {
                tracing::error!("Error sending cancel signals: {e:?}");
            };
        }
        self.consolidator.cancel_all_subscriptions();
        tracing::info!("All subscriptions cancelled");

        if let Err(e) = self.consolidator.close_bar_listening_channel() {
            tracing::error!("Error trying to kill bar_listening_channel: {e:?}");
        };
        if let Err(e) = self.order_engine.kill_order_update_stream_thread() {
            tracing::error!("Error trying to kill order_update_stream thread: {e:?}");
        };
        tracing::info!("aborted all tokio threads");

        if let Err(e) = self
            .order_engine
            .sync_executions(&self.master_client, Some(self.default_strategy.clone()))
        {
            tracing::warn!("Error trying to sync executions: {e:?}");
        };
        tracing::info!("executions synced");
        self.order_engine.sync_open_orders(
            &self.master_client,
            &self.consolidator,
            Some(self.default_strategy.clone()),
        );
        tracing::info!("open orders synced");
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.order_engine.sync_positions(
                &self.master_client,
                Some(self.default_strategy.clone()),
                &self.consolidator,
            ))
        });
        tracing::info!("positions synced");

        // ============== TEARDOWN ===================
        tracing::info!(
            "consolidator has {:?} strong references,\
            order_engine has {:?} strong references,\
            master_client has {:?} strong references,\
            ",
            Arc::strong_count(&self.consolidator),
            Arc::strong_count(&self.order_engine),
            Arc::strong_count(&self.master_client),
        );
        std::thread::sleep(Duration::from_secs(5)); // await the cascading dropping of threads
        tracing::info!("App state dropped!");
    }
}

pub enum ApplicationState {
    IbkrState(IbkrState),
}

#[derive(Debug, Clone)]
pub(crate) struct ContractSubscription {
    pub contract: Contract,
    timestep: u32,
    what_to_show: RealtimeWhatToShow,
}

#[derive(Clone)]
pub struct StrategyParameters {
    pub(crate) strategy: StrategyEnum,
    // used to provide time for warm up before market open so be conservative
    estimated_time_to_warm_up: Duration,
    pub(crate) subscribed_contracts: Vec<ContractSubscription>,
}

impl StrategyParameters {
    pub fn subscribe_to_all_data(&self, consolidator: &Arc<Consolidator>) {
        for contract_subscription in self.subscribed_contracts.iter() {
            consolidator.subscribe_to_data(
                &self.strategy,
                &contract_subscription.contract,
                &contract_subscription.timestep,
                contract_subscription.what_to_show,
            );
        }
    }

    pub fn cancel_all_subscriptions(&self, consolidator: &Arc<Consolidator>) {
        for contract_subscription in self.subscribed_contracts.iter() {
            consolidator.subscribe_to_data(
                &self.strategy,
                &contract_subscription.contract,
                &contract_subscription.timestep,
                contract_subscription.what_to_show,
            );
        }
    }
}

pub fn init_strategies(pool: PgPool, consolidator: Weak<Consolidator>) -> Vec<StrategyParameters> {
    let noise = StrategyEnum::Noise(Noise::new(pool.clone(), consolidator.clone()));
    let manual = StrategyEnum::Manual(Manual::new(pool.clone(), consolidator.clone()));
    let unknown = StrategyEnum::Unknown(Unknown::new(pool.clone(), consolidator.clone()));

    let noise_contract = noise
        .get_contracts()
        .first()
        .expect("Expected QQQ contract")
        .clone();
    let noise_strat_params = StrategyParameters {
        strategy: noise.clone(),
        estimated_time_to_warm_up: Duration::from_secs(40),
        subscribed_contracts: vec![ContractSubscription {
            contract: noise_contract.clone(),
            timestep: 5,
            what_to_show: RealtimeWhatToShow::Trades,
        }],
    };

    let manual_contract = manual
        .get_contracts()
        .first()
        .expect("Expected Manual contract")
        .clone();
    let manual_params = StrategyParameters {
        strategy: manual.clone(),
        estimated_time_to_warm_up: Duration::from_secs(1),
        subscribed_contracts: vec![ContractSubscription {
            contract: manual_contract.clone(),
            timestep: 5,
            what_to_show: RealtimeWhatToShow::Ask,
        }],
    };

    let unknown_contract = unknown
        .get_contracts()
        .first()
        .expect("Expected Unknown contract")
        .clone();
    let unknown_params = StrategyParameters {
        strategy: unknown.clone(),
        estimated_time_to_warm_up: Duration::from_secs(1),
        subscribed_contracts: vec![ContractSubscription {
            contract: unknown_contract.clone(),
            timestep: 5,
            what_to_show: RealtimeWhatToShow::Ask,
        }],
    };

    vec![
        noise_strat_params,
        manual_params,
        unknown_params,
    ]
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
    let consolidator = Arc::new(Consolidator::new(pool.clone(), client_1));
    let strat_params = init_strategies(pool.clone(), Arc::downgrade(&consolidator));
    let order_engine = Arc::new(OrderEngine::new(
        pool.clone(),
        account.to_string(),
        &strat_params,
    ));
    if let Err(e) = consolidator.add_contract_schedules(&strat_params) {
        tracing::error!(
            "Error trying to initialise contract schedules: {e:?}\ncontinuing anyway for now!"
        );
    };
    // consolidator.begin_receiving_available_funds("DU3156861", Arc::downgrade(&consolidator));
    consolidator.begin_receiving_available_funds(account, Arc::downgrade(&consolidator));
    consolidator.begin_bar_listening(
        &Arc::downgrade(&order_engine),
        &Arc::downgrade(&master_client),
        &Arc::downgrade(&consolidator),
    );
    tracing::info!("Initialised bar listening");

    // ===================================
    // Sync App State w Broker
    // ===================================
    if let Err(e) = order_engine.sync_executions(&master_client, Some(default_strategy.clone())) {
        tracing::warn!("Error trying to sync executions: {e:?}");
    };
    order_engine.sync_open_orders(
        &master_client,
        &consolidator,
        Some(default_strategy.clone()),
    );
    order_engine
        .sync_positions(
            &master_client,
            Some(default_strategy.clone()),
            &consolidator,
        )
        .await;

    // ================== Init Order Stream ===============
    // this will prevent order_update_stream from receiving updates b4 syncing all open_orders
    // which could cause issues/race conditions - i.e. orders not in order_map, ...
    order_engine.init_order_update_stream(
        Arc::downgrade(&master_client),
        Some(default_strategy.clone()),
    );
    let mut cancel_senders = Vec::new();
    tracing::info!("Initialised order update stream");
    // ================== Init Order Stream ===============

    for strat_param in strat_params {
        let (cancel_sender, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let cloned_consolidator = consolidator.clone();
        tokio::spawn(async move {
            let leeway = TimeDelta::from_std(strat_param.estimated_time_to_warm_up)
                .expect("Couldn't convert Duration to TimeDelta");
            loop {
                tokio::select! {
                    _ = &mut cancel_rx => {
                        tracing::info!("strategy cancelled!");
                        return;
                    }
                    _ = async {
                        let is_strat_active_res = cloned_consolidator.is_strategy_active(&strat_param.strategy.get_name(), &(Utc::now() + leeway));
                        if let Err(e) = is_strat_active_res {
                            tracing::error!("Strategy not pre-registered with consolidator - could not get is_strategy_active_rn: {e:?}");
                            return;
                        }
                        match is_strat_active_res.unwrap() {
                            true => {
                                // =======
                                // warm up
                                // =======
                                let start = Instant::now();

                                tracing::info!(message=%format!("Initialising {} now!", strat_param.strategy.get_name()));
                                while let Err(e) = strat_param.strategy.warm_up_data(&cloned_consolidator).await {
                                    if e.contains("Expected Historical Data Request to TWS to succeed") {
                                        tracing::warn!("Failed to retrieve historical data because {e:?}");
                                    }
                                    tracing::error!("Error: {e:?}");
                                    // to prevent FULL busy waiting
                                    tokio::time::sleep(Duration::from_secs(10)).await;
                                }
                                let duration = start.elapsed();
                                tracing::info!(message=%format!("Completed with initialising {}: took {duration:?} to initialise!", strat_param.strategy.get_name()));

                                let now = Utc::now();
                                let next_strat_active = cloned_consolidator.get_next_strategy_active(&strat_param.strategy.get_name(), &now);
                                match next_strat_active {
                                    Ok(next_active) => {
                                        if next_active > now {
                                            tokio::time::sleep((next_active - now).to_std().expect("Could not convert timedelta to duration")).await;
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("Strategy not pre-registered with consolidator - could not get is_strategy_active_rn: {e:?}");
                                        return;
                                    }
                                }
                                // =================
                                // subscribe to data
                                // =================
                                for subscription in &strat_param.subscribed_contracts {
                                    cloned_consolidator.subscribe_to_data(
                                        &strat_param.strategy,
                                        &subscription.contract,
                                        &subscription.timestep,
                                        subscription.what_to_show
                                    );
                                }
                                // account for possible minor differences in time
                                tokio::time::sleep(Duration::from_secs(10)).await;

                                // sleep duration
                                let now = Utc::now();
                                let next_sleep_time = {
                                    match cloned_consolidator.get_next_strategy_inactive(&strat_param.strategy.get_name(), &now) {
                                        Ok(res) => res,
                                        Err(e) => {
                                            tracing::error!(message=%format!("Couldn't calculate how much to sleep till inactive for strategy: {:?}: {e:?}", strat_param.strategy.get_name()));
                                            now
                                        }
                                    }
                                };
                                if next_sleep_time > now {
                                    sleep((next_sleep_time - now).to_std().expect("Failed to convert to Duration")).await;
                                }
                                // account for final bar of data
                                tokio::time::sleep(Duration::from_secs(10)).await;

                                for subscription in &strat_param.subscribed_contracts {
                                    cloned_consolidator.cancel_subscription(
                                        &strat_param.strategy,
                                        &subscription.contract,
                                        &subscription.timestep,
                                        subscription.what_to_show
                                    );
                                }
                            }
                            false => {
                                let now = Utc::now();
                                let next_sleep_time = {
                                    match cloned_consolidator.get_next_strategy_active(&strat_param.strategy.get_name(), &now) {
                                        Ok(res) => res,
                                        Err(e) => {
                                            tracing::error!(message=%format!("Couldn't calculate how much to sleep till active for strategy: {:?}: {e:?}", strat_param.strategy.get_name()));
                                            now
                                        }
                                    }
                                };
                                if next_sleep_time - leeway <= now {
                                    return;
                                }
                                let duration = (next_sleep_time - now - leeway).to_std().expect("Failed to convert to Duration");
                                tracing::info!("Sleeping for {duration:?}!");
                                sleep(duration).await;
                            }
                        }
                    } => {}
                }
            }
        });
        cancel_senders.push(cancel_sender);
    }

    Ok(ApplicationState::IbkrState(IbkrState {
        cancel_senders,
        consolidator,
        order_engine,
        master_client,
        default_strategy,
        _gateway,
    }))
}
