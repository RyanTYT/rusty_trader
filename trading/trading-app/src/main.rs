use std::sync::{Arc, Mutex, Weak};

#[cfg(not(feature = "backtest"))]
use crate::{
    init_app::{ApplicationState, init_app},
    logger::{ChannelLayer, ConnectionState, IbConnectionLayer, init_db_logger},
    schedule::program_scheduler::run_program,
    server::server::init_server,
};
use chrono::Utc;
use chrono_tz::America::New_York;
use sqlx::postgres::PgPoolOptions;
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
#[cfg(not(feature = "backtest"))]
pub mod init_app;
pub mod logger;
pub mod market_data;
pub mod schedule;
pub mod server;
pub mod strategy;

#[macro_export]
macro_rules! arc_drop_async {
    ($app_state:ident) => {
        let mut app_state =
            std::sync::Arc::into_inner($app_state).expect("Expected only 1 strong reference");
        app_state.async_drop().await;
    };
}

/// for logger
struct NewYorkTime {}
impl FormatTime for NewYorkTime {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = Utc::now().with_timezone(&New_York);
        write!(w, "{}", now.format("%Y-%m-%dT%H:%M:%S%.3f %Z"))
    }
}

#[hotpath::main]
fn main() {
    #[cfg(not(feature = "backtest"))]
    {
        let rt =
            tokio::runtime::Runtime::new().expect("Expected to be able to make main tokio thread");
        rt.block_on(async { tokio_main().await });
    }
}

#[cfg(not(feature = "backtest"))]
async fn tokio_main() {
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
        .with_ansi(true)
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
    let (app_state_sender, app_state_rcx) =
        tokio::sync::mpsc::channel::<Option<Weak<ApplicationState>>>(2);
    let server_url = std::env::var("SERVER_URL").unwrap_or("0.0.0.0:8000".to_string());
    let _server_handle = init_server(&server_url, app_state_rcx);
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
        //         "noise".to_string(),
        //     )
        // },
        &mut alert_rx,
        app_state_sender,
    )
    .await;
}
