use std::sync::{Arc, Mutex, Weak};

use crate::{
    init_app::{ApplicationState, init_app},
    logger::{ChannelLayer, ConnectionState, IbConnectionLayer, init_db_logger},
    schedule::program_scheduler::run_program,
    server::server::init_server,
};
use async_trait::async_trait;
use chrono::Utc;
use chrono_tz::America::New_York;
use sqlx::{
    Postgres,
    postgres::{PgArguments, PgPoolOptions},
    query::QueryAs,
};
use tokio::time::Duration;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    fmt::{self, time::FormatTime},
    layer::SubscriberExt,
};
use tracing_subscriber::{layer::Layer, util::SubscriberInitExt};

pub mod database;
pub mod execution;
pub mod helpers;
pub mod ibc;
pub mod init_app;
pub mod logger;
pub mod market_data;
pub mod schedule;
pub mod server;
pub mod strategy;

#[async_trait]
pub trait Insertable {
    fn table_name() -> &'static str;
    fn pri_column_names(&self) -> Vec<&'static str>;
    fn opt_column_names(&self) -> Vec<&'static str>;
    fn bind_pri<'q>(&'q self, sql: &'q str) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;
    fn bind_pri_to_query<'q>(
        &'q self,
        query: sqlx::query::Query<'q, sqlx::Postgres, PgArguments>,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;
    fn bind_pri_to_query_as<'q, T>(
        &'q self,
        query: QueryAs<'q, Postgres, T, PgArguments>,
    ) -> QueryAs<'q, Postgres, T, PgArguments>;
    fn bind_opt<'q>(&'q self, sql: &'q str) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;
    fn bind_opt_to_query<'q>(
        &'q self,
        query: sqlx::query::Query<'q, sqlx::Postgres, PgArguments>,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;
    fn bind_opt_to_query_as<'q, T>(
        &'q self,
        query: QueryAs<'q, Postgres, T, PgArguments>,
    ) -> QueryAs<'q, Postgres, T, PgArguments>;
}

/// for logger
struct NewYorkTime {}
impl FormatTime for NewYorkTime {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = Utc::now().with_timezone(&New_York);
        write!(w, "{}", now.format("%Y-%m-%dT%H:%M:%S%.3f %Z"))
    }
}

#[tokio::main]
async fn main() {
    // ================== INITIALISATION ======================
    let database_url = std::env::var("DATABASE_URL")
        .expect("Expected DATABASE_URL environment variable to be set!");
    let trading_type = std::env::var("TRADING_TYPE")
        .expect("Expected TRADING_TYPE environment variable to be set!");
    let pool = PgPoolOptions::new()
        .max_connections(15)
        .acquire_timeout(Duration::from_secs(30))
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET synchronous_commit = off")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&database_url)
        // .connect("postgres://ryantan:admin@localhost:5432/rust_trading_system")
        .await
        .map_err(|e| format!("error {}", e))
        // we allow an expect here since it is only initialised at the beginning - so it is safe
        // for loops in the future/middle of executions
        .expect("Expected to be able to connect to PostgresDB!");

    if let Err(e) = sqlx::migrate!("./migrations").run(&pool).await {
        tracing::error!("Error intialising migrations: {}", e);
    };

    let (alert_tx, mut alert_rx) = tokio::sync::mpsc::channel(1);
    let state = Arc::new(Mutex::new(ConnectionState::new(alert_tx)));
    let stdout_layer = fmt::layer()
        .pretty()
        .with_target(true)
        .with_timer(NewYorkTime {})
        .with_filter(LevelFilter::INFO); // show function/module name
    let ib_layer = IbConnectionLayer::new(state.clone(), "America/New_York".parse().unwrap())
        .with_filter(LevelFilter::WARN);
    let db_tx = init_db_logger(pool.clone());
    let db_layer = ChannelLayer { sender: db_tx }.with_filter(LevelFilter::WARN);
    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(db_layer)
        .with(ib_layer)
        .try_init()
        .ok();
    // ================== INITIALISATION ======================

    // ================== SERVER ======================
    let (app_state_sender, app_state_rcx) = tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
    init_server(app_state_rcx);
    // ================== SERVER ======================

    // let strat_params = init_strategies(pool.clone());
    let cloned_pool = pool.clone();
    let port = if trading_type == "live" { 4001 } else { 4002 };
    let gateway_url = format!("127.0.0.1:{port:?}");
    // let cloned_strat_params = strat_params.clone();

    run_program(
        move || {
            let gateway_url_clone = gateway_url.clone();
            let cloned_pool_clone = cloned_pool.clone();

            async move {
                init_app(
                    &gateway_url_clone,
                    "DU3156861",
                    cloned_pool_clone,
                    "/tmp/ibc.log",
                    "unknown".to_string(),
                )
                .await
            }
        },
        // move || {
        //     init_app(
        //         &gateway_url,
        //         "DU3156861",
        //         cloned_pool.clone(),
        //         "/tmp/ibc.log",
        //         "noise".to_string(),
        //     )
        // },
        &mut alert_rx,
        app_state_sender,
    )
    .await;

    // let maintenance_scheduler = IbkrRegion::Apac;
    // let mut ibc_state = IBCState::Stopped;
    // let mut gateway = None;
    // loop {
    //     // ================== INITIALISATION ======================
    //     match ibc_state {
    //         IBCState::Stopped => {
    //             let ibc_try = IBGateway::start("/tmp/ibc.log").await;
    //             match ibc_try {
    //                 Ok(new_gateway) => {
    //                     gateway = Some(new_gateway);
    //                 }
    //                 Err(e) => {
    //                     tracing::warn!("Error trying to start IBGateway: {e:?} -- trying again");
    //                     continue;
    //                 }
    //             }
    //         }
    //         IBCState::Running => {
    //             tracing::warn!(
    //                 "IBCState set to running before initialisation, not sure how -> will be restarting properly!"
    //             );
    //             if let Some(underlying_gateway) = gateway.take() {
    //                 if let Err(_) = underlying_gateway.stop().await {
    //                     tracing::warn!("Couldn't stop gateway yet");
    //                 };
    //
    //                 sleep(tokio::time::Duration::from_secs(20)).await;
    //             }
    //             ibc_state = IBCState::Stopped;
    //             continue;
    //         }
    //         IBCState::AutoRestarting => {
    //             tracing::info!(
    //                 "IBCState in AutoRestarting mode! Should check if IBGateway is alive in next update before proceeding! - assuming now it is already alive!"
    //             );
    //         }
    //     }
    //     ibc_state = IBCState::Running;
    //
    //     let master_client_opt = {
    //         let mut retry_time = 0;
    //         loop {
    //             let try_client = match Client::connect("127.0.0.1:4002", 0) {
    //                 Ok(connected_client) => Some(connected_client),
    //                 Err(e) => {
    //                     tracing::error!(
    //                         "Connection to TWS via \nURL: localhost:4002\n Client Id: 0\n failed!\nError: {}",
    //                         e
    //                     );
    //                     retry_time += 1;
    //                     if retry_time < 7 {
    //                         // give up to 3 minutes leeway
    //                         tracing::error!("Retrying for {retry_time:?} time!");
    //                         sleep(Duration::from_secs(30)).await;
    //                         continue;
    //                     }
    //                     None
    //                 }
    //             };
    //             break try_client;
    //         }
    //     };
    //     if master_client_opt.is_none() {
    //         continue;
    //     }
    //     let master_client = Arc::new(master_client_opt.unwrap());
    //     tracing::info!("Connected to client {}", master_client.client_id());
    //     let client_1_opt = match Client::connect("127.0.0.1:4002", 1) {
    //         Ok(client) => Some(client),
    //         Err(e) => {
    //             tracing::error!(
    //                 "Connection to TWS via \nURL: localhost:4002\n Client Id: 1\n failed!\nError: {}",
    //                 e
    //             );
    //             None
    //         }
    //     };
    //     if client_1_opt.is_none() {
    //         continue;
    //     }
    //     let client_1 = Arc::new(client_1_opt.unwrap());
    //     tracing::info!(
    //         message=%format!(
    //             "Connected to client {}", client_1.client_id()
    //         )
    //     );
    //     // ================== INITIALISATION ======================
    //     let mut strategies: Vec<StrategyEnum> = Vec::new();
    //
    //     let noise = StrategyEnum::Noise(Noise::new(pool.clone()));
    //     let frac_mom = StrategyEnum::FractionalMomentum(FractionalMomentum::new(pool.clone()));
    //     let forex_mean_reversion =
    //         StrategyEnum::ForexMeanReversion(ForexMeanReversion::new(pool.clone()));
    //     let forex_momentum = StrategyEnum::ForexMomentum(ForexMomentum::new(pool.clone()));
    //     let gold_momentum = StrategyEnum::GoldMomentum(GoldMomentum::new(pool.clone()));
    //     let default_strategy = noise.get_name();
    //
    //     strategies.push(noise.clone());
    //     strategies.push(frac_mom.clone());
    //     strategies.push(forex_mean_reversion.clone());
    //     strategies.push(forex_momentum.clone());
    //     strategies.push(gold_momentum.clone());
    //     let order_engine = Arc::new(OrderEngine::new(pool.clone(), &strategies));
    //     drop(strategies);
    //     // ================== INITIALISATION ======================
    //     let consolidator = Arc::new(Consolidator::new(pool.clone(), client_1));
    //     consolidator.begin_receiving_available_funds("DU3156861", Arc::downgrade(&consolidator));
    //     consolidator.begin_bar_listening(
    //         &Arc::downgrade(&order_engine),
    //         &Arc::downgrade(&master_client),
    //         &Arc::downgrade(&consolidator),
    //     );
    //     tracing::info!("Initialised bar listening");
    //
    //     // ================== SYNC first ======================
    //     if let Err(e) = order_engine.sync_executions(&master_client, Some(default_strategy.clone()))
    //     {
    //         tracing::warn!("Error trying to sync executions: {e:?}");
    //     };
    //     order_engine.sync_open_orders(
    //         &master_client,
    //         &consolidator,
    //         Some(default_strategy.clone()),
    //     );
    //     order_engine
    //         .sync_positions(
    //             &master_client,
    //             Some(default_strategy.clone()),
    //             &consolidator,
    //         )
    //         .await;
    //     // ================== SYNC first ======================
    //     // ================== Init Order Stream ===============
    //     // this will prevent order_update_stream from receiving updates b4 syncing all open_orders
    //     // which could cause issues/race conditions - i.e. orders not in order_map, ...
    //     order_engine.init_order_update_stream(Arc::downgrade(&master_client));
    //     let mut cancel_senders = Vec::new();
    //     tracing::info!("Initialised order update stream");
    //     // ================== Init Order Stream ===============
    //
    //     // ============== NOISE ===================
    //     let cloned_pool = pool.clone();
    //     let cloned_consolidator = consolidator.clone();
    //     let (noise_cancel_sender, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
    //     tokio::spawn(async move {
    //         loop {
    //             tokio::select! {
    //                 _ = &mut cancel_rx => {
    //                     tracing::info!("noise strategy cancelled!");
    //                     return;
    //                 }
    //                 _ = async {
    //                     let strategy_crud = get_strategy_crud(cloned_pool.clone());
    //                     if let Err(e) = strategy_crud
    //                         .create_or_ignore(&crate::database::models::StrategyFullKeys {
    //                             strategy: "noise".to_string(),
    //                             capital: 10000.0,
    //                             initial_capital: 10000.0,
    //                             status: crate::database::models::Status::Active,
    //                         })
    //                         .await
    //                     {
    //                         tracing::error!("Error trying to create_or_ignore noise: {e:?}")
    //                     }
    //
    //                     let start = Instant::now();
    //                     while let Err(e) = noise.warm_up_data(&cloned_consolidator).await {
    //                         if e.contains("Expected Historical Data Request to TWS to succeed") {
    //                             tracing::warn!("Failed to retrieve historical data because {e:?}");
    //                         }
    //                         tracing::error!("Error: {e:?}");
    //                     }
    //                     let duration = start.elapsed();
    //                     tracing::info!(
    //                         message=%format!(
    //                             "Noise took: {:?} to warm up fully", duration
    //                         )
    //                     );
    //
    //                     sleep_until_next_stock_market_open().await;
    //                     cloned_consolidator.subscribe_to_data(
    //                         &noise,
    //                         &noise
    //                             .get_contracts()
    //                             .first()
    //                             .expect("Expected QQQ contract!"),
    //                         &5,
    //                         ibapi::prelude::RealtimeWhatToShow::Trades,
    //                     );
    //                     tracing::info!("Completed with intialising Noise!");
    //                     sleep_until_stock_market_close().await;
    //                     cloned_consolidator.cancel_subscription(
    //                         &noise,
    //                         &noise
    //                             .get_contracts()
    //                             .first()
    //                             .expect("Expected QQQ contract!"),
    //                         &5,
    //                         ibapi::prelude::RealtimeWhatToShow::Trades,
    //                     );
    //                 } => {}
    //             }
    //         }
    //     });
    //     // ============== NOISE ===================
    //     cancel_senders.push(noise_cancel_sender);
    //
    //     // ============== FRACTIONAL MOMENTUM ===================
    //     let cloned_pool = pool.clone();
    //     let cloned_consolidator = consolidator.clone();
    //     let (frac_mom_cancel_sender, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
    //     tokio::spawn(async move {
    //         loop {
    //             tokio::select! {
    //                 _ = &mut cancel_rx => {
    //                     tracing::info!("frac_mom strategy cancelled!");
    //                     return;
    //                 }
    //                 _ = async {
    //                     let strategy_crud = get_strategy_crud(cloned_pool.clone());
    //                     if let Err(e) = strategy_crud
    //                         .create_or_ignore(&crate::database::models::StrategyFullKeys {
    //                             strategy: "fractional_momentum".to_string(),
    //                             capital: 100_000.0,
    //                             initial_capital: 100_000.0,
    //                             status: crate::database::models::Status::Active,
    //                         })
    //                         .await
    //                     {
    //                         tracing::error!("Error trying to create_or_ignore fractional_momentum: {e:?}",)
    //                     }
    //
    //                     let start = Instant::now();
    //                     tracing::info!("Warming up fractional momentum data rn");
    //                     while let Err(e) = frac_mom.warm_up_data(&cloned_consolidator).await {
    //                         if e.contains("Expected Historical Data Request to TWS to succeed") {
    //                             tracing::warn!("Failed to retrieve historical data because {e:?}");
    //                         }
    //                         tracing::error!("Error: {e:?}");
    //                     }
    //                     let duration = start.elapsed();
    //                     tracing::info!("FractionalMomentum took: {:?} to warm up fully", duration);
    //
    //                     sleep_until_next_stock_market_open().await;
    //                     println!("Trying to subscribe to data for Noise");
    //                     frac_mom.get_contracts().iter().for_each(|contract| {
    //                         cloned_consolidator.subscribe_to_data(
    //                             &frac_mom,
    //                             &contract,
    //                             &5,
    //                             ibapi::prelude::RealtimeWhatToShow::Trades,
    //                         );
    //                     });
    //                     tracing::info!("Completed with intialising FractionalMomentum!");
    //                     sleep_until_stock_market_close().await;
    //
    //                     frac_mom.get_contracts().iter().for_each(|contract| {
    //                         cloned_consolidator.cancel_subscription(
    //                             &frac_mom,
    //                             &contract,
    //                             &5,
    //                             ibapi::prelude::RealtimeWhatToShow::Trades,
    //                         );
    //                     });
    //
    //                     // ============= TO BE DELETED ==================
    //                     // cloned_order_engine.place_orders_for_strategy(
    //                     //     frac_mom.clone(),
    //                     //     frac_mom.get_contracts().first().unwrap().clone(),
    //                     //     cloned_master_client,
    //                     //     database::models::AssetType::Stock,
    //                     //     true,
    //                     // );
    //                     // ============= TO BE DELETED ==================
    //                 } => {}
    //             }
    //         }
    //     });
    //     cancel_senders.push(frac_mom_cancel_sender);
    //     // ============== FRACTIONAL MOMENTUM ===================
    //
    //     // ============== FOREX MEAN REVERSION ===================
    //     let cloned_pool = pool.clone();
    //     let cloned_consolidator = consolidator.clone();
    //     let (forex_cancel_sender, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
    //     tokio::spawn(async move {
    //         loop {
    //             tokio::select! {
    //                 _ = &mut cancel_rx => {
    //                     tracing::info!("forex_mean_reversion strategy cancelled!");
    //                     return;
    //                 }
    //                 _ = async {
    //                     let strategy_crud = get_strategy_crud(cloned_pool.clone());
    //                     if let Err(e) = strategy_crud
    //                         .create_or_ignore(&crate::database::models::StrategyFullKeys {
    //                             strategy: "forex_mean_reversion".to_string(),
    //                             capital: 10000.0,
    //                             initial_capital: 10000.0,
    //                             status: crate::database::models::Status::Active,
    //                         })
    //                         .await
    //                     {
    //                         tracing::error!("Error trying to create_or_ignore noise: {e:?}")
    //                     }
    //
    //                     tracing::info!("Warming up ForexMeanReversion now!");
    //                     let start = Instant::now();
    //                     while let Err(e) = forex_mean_reversion
    //                         .warm_up_data(&cloned_consolidator)
    //                         .await
    //                     {
    //                         if e.contains("Expected Historical Data Request to TWS to succeed") {
    //                             tracing::warn!("Failed to retrieve historical data because {e:?}");
    //                         }
    //                         tracing::error!("Error: {e:?}");
    //                     }
    //                     // .expect("Expected to be able to get warmed up data for forex_mean_reversion");
    //                     let duration = start.elapsed();
    //                     tracing::info!(
    //                         message=%format!(
    //                             "ForexMeanReversion took: {:?} to warm up fully", duration
    //                         )
    //                     );
    //
    //                     sleep_until_next_forex_market_open().await;
    //                     cloned_consolidator.subscribe_to_data(
    //                         &forex_mean_reversion,
    //                         &forex_mean_reversion
    //                             .get_contract("FX:GBP/USD", "")
    //                             .expect("Expected GBP/USD contract to be ready when subscribing to data"),
    //                         &1,
    //                         ibapi::prelude::RealtimeWhatToShow::Bid,
    //                     );
    //                     cloned_consolidator.subscribe_to_data(
    //                         &forex_mean_reversion,
    //                         &forex_mean_reversion
    //                             .get_contract("FX:GBP/USD", "")
    //                             .expect("Expected GBP/USD contract to be ready when subscribing to data"),
    //                         &1,
    //                         ibapi::prelude::RealtimeWhatToShow::Ask,
    //                     );
    //                     tracing::info!("Completed with intialising ForexMeanReversion!");
    //                     sleep_until_forex_market_close().await;
    //
    //                     cloned_consolidator.cancel_subscription(
    //                         &forex_mean_reversion,
    //                         &forex_mean_reversion
    //                             .get_contract("FX:GBP/USD", "")
    //                             .expect("Expected GBP/USD contract to be ready when subscribing to data"),
    //                         &1,
    //                         ibapi::prelude::RealtimeWhatToShow::Bid,
    //                     );
    //                     cloned_consolidator.cancel_subscription(
    //                         &forex_mean_reversion,
    //                         &forex_mean_reversion
    //                             .get_contract("FX:GBP/USD", "")
    //                             .expect("Expected GBP/USD contract to be ready when subscribing to data"),
    //                         &1,
    //                         ibapi::prelude::RealtimeWhatToShow::Ask,
    //                     );
    //                 } => {}
    //             }
    //         }
    //     });
    //     cancel_senders.push(forex_cancel_sender);
    //     // ============== FOREX REVERSION ===================
    //
    //     // ============== FOREX MOMENTUM ===================
    //     let cloned_pool = pool.clone();
    //     let cloned_consolidator = consolidator.clone();
    //     let (forex_mom_cancel_sender, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
    //     tokio::spawn(async move {
    //         loop {
    //             tokio::select! {
    //                 _ = &mut cancel_rx => {
    //                     tracing::info!("forex_momentum strategy cancelled!");
    //                     return;
    //                 }
    //                 _ = async {
    //                     let strategy_crud = get_strategy_crud(cloned_pool.clone());
    //                     if let Err(e) = strategy_crud
    //                         .create_or_ignore(&crate::database::models::StrategyFullKeys {
    //                             strategy: "forex_momentum".to_string(),
    //                             capital: 10000.0,
    //                             initial_capital: 10000.0,
    //                             status: crate::database::models::Status::Active,
    //                         })
    //                         .await
    //                     {
    //                         tracing::error!("Error trying to create_or_ignore noise: {e:?}")
    //                     }
    //
    //                     tracing::info!("Warming up ForexMomentum now!");
    //                     let start = Instant::now();
    //                     while let Err(e) = forex_momentum
    //                         .warm_up_data(&cloned_consolidator)
    //                         .await
    //                     {
    //                         if e.contains("Expected Historical Data Request to TWS to succeed") {
    //                             tracing::warn!("Failed to retrieve historical data because {e:?}");
    //                         }
    //                         tracing::error!("Error: {e:?}");
    //                     }
    //                     // .expect("Expected to be able to get warmed up data for forex_mean_reversion");
    //                     let duration = start.elapsed();
    //                     tracing::info!(
    //                         message=%format!(
    //                             "ForexMomentum took: {:?} to warm up fully", duration
    //                         )
    //                     );
    //
    //                     sleep_until_next_forex_market_open().await;
    //                     cloned_consolidator.subscribe_to_data(
    //                         &forex_momentum,
    //                         &forex_momentum
    //                             .get_contract("FX:GBP/USD", "")
    //                             .expect("Expected GBP/USD contract to be ready when subscribing to data"),
    //                         &1,
    //                         ibapi::prelude::RealtimeWhatToShow::Bid,
    //                     );
    //                     cloned_consolidator.subscribe_to_data(
    //                         &forex_momentum,
    //                         &forex_momentum
    //                             .get_contract("FX:GBP/USD", "")
    //                             .expect("Expected GBP/USD contract to be ready when subscribing to data"),
    //                         &1,
    //                         ibapi::prelude::RealtimeWhatToShow::Ask,
    //                     );
    //                     tracing::info!("Completed with intialising ForexMeanReversion!");
    //                     sleep_until_forex_market_close().await;
    //
    //                     cloned_consolidator.cancel_subscription(
    //                         &forex_momentum,
    //                         &forex_momentum
    //                             .get_contract("FX:GBP/USD", "")
    //                             .expect("Expected GBP/USD contract to be ready when subscribing to data"),
    //                         &1,
    //                         ibapi::prelude::RealtimeWhatToShow::Bid,
    //                     );
    //                     cloned_consolidator.cancel_subscription(
    //                         &forex_momentum,
    //                         &forex_momentum
    //                             .get_contract("FX:GBP/USD", "")
    //                             .expect("Expected GBP/USD contract to be ready when subscribing to data"),
    //                         &1,
    //                         ibapi::prelude::RealtimeWhatToShow::Ask,
    //                     );
    //                 } => {}
    //             }
    //         }
    //     });
    //     cancel_senders.push(forex_mom_cancel_sender);
    //     // ============== FOREX MOMENTUM ===================
    //
    //     // ============== GOLD MOMENTUM ===================
    //     let cloned_pool = pool.clone();
    //     let cloned_consolidator = consolidator.clone();
    //     let (gold_momentum_cancel_sender, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
    //     tokio::spawn(async move {
    //         loop {
    //             tokio::select! {
    //                 _ = &mut cancel_rx => {
    //                     tracing::info!("gold_momentum strategy cancelled!");
    //                     return;
    //                 }
    //                 _ = async {
    //                     let strategy_crud = get_strategy_crud(cloned_pool.clone());
    //                     if let Err(e) = strategy_crud
    //                         .create_or_ignore(&crate::database::models::StrategyFullKeys {
    //                             strategy: "gold_momentum".to_string(),
    //                             capital: 10000.0,
    //                             initial_capital: 10000.0,
    //                             status: crate::database::models::Status::Active,
    //                         })
    //                         .await
    //                     {
    //                         tracing::error!("Error trying to create_or_ignore gold_momentum: {e:?}")
    //                     }
    //
    //                     tracing::info!("Warming up GoldMomentum now!");
    //                     let start = Instant::now();
    //                     while let Err(e) = gold_momentum.warm_up_data(&cloned_consolidator).await {
    //                         if e.contains("Expected Historical Data Request to TWS to succeed") {
    //                             tracing::warn!("Failed to retrieve historical data because {e:?}");
    //                         }
    //                         tracing::error!("Error: {e:?}");
    //                     }
    //                     // .expect("Expected to be able to get warmed up data for forex_mean_reversion");
    //                     let duration = start.elapsed();
    //                     tracing::info!(
    //                         message=%format!(
    //                             "GoldMomentum took: {:?} to warm up fully", duration
    //                         )
    //                     );
    //
    //                     sleep_until_next_forex_market_open().await;
    //                     let xauusd_contract = Contract {
    //                         symbol: Symbol::new("XAUUSD"),
    //                         security_type: ibapi::prelude::SecurityType::CFD,
    //                         exchange: "SMART".into(),
    //                         currency: "USD".into(),
    //                         ..Default::default()
    //                     };
    //                     cloned_consolidator.subscribe_to_data(
    //                         &gold_momentum,
    //                         &xauusd_contract,
    //                         &60,
    //                         ibapi::prelude::RealtimeWhatToShow::Bid,
    //                     );
    //                     cloned_consolidator.subscribe_to_data(
    //                         &gold_momentum,
    //                         &xauusd_contract,
    //                         &1,
    //                         ibapi::prelude::RealtimeWhatToShow::Ask,
    //                     );
    //                     tracing::info!("Completed with intialising XAUUSD!");
    //                     sleep_until_forex_market_close().await;
    //
    //                     cloned_consolidator.cancel_subscription(
    //                         &gold_momentum,
    //                         &xauusd_contract,
    //                         &1,
    //                         ibapi::prelude::RealtimeWhatToShow::Bid,
    //                     );
    //                     cloned_consolidator.cancel_subscription(
    //                         &gold_momentum,
    //                         &xauusd_contract,
    //                         &1,
    //                         ibapi::prelude::RealtimeWhatToShow::Ask,
    //                     );
    //                 } => {}
    //             }
    //         }
    //     });
    //     cancel_senders.push(gold_momentum_cancel_sender);
    //     // ============== GOLD MOMENTUM ===================
    //
    //     ibc_state = sleep_until_all_markets_closed(&mut alert_rx).await;
    //     tracing::info!("sleep returned properly");
    //     for cancel_sender in cancel_senders {
    //         if let Err(e) = cancel_sender.send(()) {
    //             tracing::error!("Error sending cancel signals: {e:?}");
    //         };
    //     }
    //     consolidator.cancel_all_subscriptions();
    //     if let Err(e) = consolidator.close_bar_listening_channel() {
    //         tracing::error!("Error trying to kill bar_listening_channel: {e:?}");
    //     };
    //     if let Err(e) = order_engine.kill_order_update_stream_thread() {
    //         tracing::error!("Error trying to kill order_update_stream thread: {e:?}");
    //     };
    //     tracing::info!("aborted all tokio threads");
    //
    //     if let Err(e) = order_engine.sync_executions(&master_client, Some(default_strategy.clone()))
    //     {
    //         tracing::warn!("Error trying to sync executions: {e:?}");
    //     };
    //     tracing::info!("executions synced");
    //     if matches!(ibc_state, IBCState::Running) {
    //         order_engine.sync_open_orders(
    //             &master_client,
    //             &consolidator,
    //             Some(default_strategy.clone()),
    //         );
    //         tracing::info!("open orders synced");
    //         order_engine
    //             .sync_positions(
    //                 &master_client,
    //                 Some(default_strategy.clone()),
    //                 &consolidator,
    //             )
    //             .await;
    //     }
    //     tracing::info!("positions synced");
    //
    //     // ============== TEARDOWN ===================
    //     tracing::info!(
    //         "consolidator has {:?} strong references,\
    //         order_engine has {:?} strong references,\
    //         master_client has {:?} strong references,\
    //         ",
    //         Arc::strong_count(&consolidator),
    //         Arc::strong_count(&order_engine),
    //         Arc::strong_count(&master_client),
    //     );
    //     drop(consolidator);
    //     tracing::info!("dropped consolidator");
    //     drop(order_engine);
    //     tracing::info!("dropped order_engine");
    //     drop(master_client);
    //     tracing::info!("dropped master_client");
    //     sleep(Duration::from_secs(10)).await; // await the cascading dropping of threads
    //
    //     match ibc_state {
    //         IBCState::Running => {
    //             let stop_opt = gateway
    //                 .take()
    //                 .expect("Expected gatway to exist")
    //                 .stop()
    //                 .await;
    //             if let Err(moved_gateway) = stop_opt {
    //                 tracing::warn!(
    //                     "Error trying to stop IBGateway: -- will try again in 1 minute, but will proceed anyway if it fails again!"
    //                 );
    //                 sleep(tokio::time::Duration::from_secs(60)).await;
    //                 if let Err(_) = moved_gateway.stop().await {
    //                     tracing::warn!(
    //                         "GG! couldn't stop IBGateway properly! fuck this, continuing anyway"
    //                     );
    //                 };
    //             }
    //             ibc_state = IBCState::Stopped;
    //         }
    //         IBCState::AutoRestarting => {
    //             sleep(tokio::time::Duration::from_secs(60 * 3)).await;
    //             ibc_state = IBCState::AutoRestarting;
    //         }
    //         IBCState::Stopped => {
    //             tracing::warn!("IBGateway stopped b4 end of loop, need to check why");
    //         }
    //     }
    //     sleep(Duration::from_secs(20)).await; // await the cascading dropping of threads
    //     tracing::info!("teardown complete");
    //
    //     tracing::info!("sleeping thru weekend system maintenance");
    //     sleep_thru_system_maintenance().await;
    //     tracing::info!("slept thru weekend system maintenance");
    // }
    // // ============== TEARDOWN ===================
}
