use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use chrono_tz::{America::New_York, Asia::Novosibirsk};
use ibapi::{Client, contracts::ContractBuilder};
use nyse_holiday_cal::HolidayCal;
use sqlx::{
    Postgres,
    postgres::{PgArguments, PgPoolOptions},
    query::QueryAs,
};
use tokio::time::{Duration, Instant, sleep};

use crate::{
    database::{crud::CRUDTrait, models_crud::strategy::get_strategy_crud},
    execution::order_engine::OrderEngine,
    ibc::IBGateway,
    logger::init_logger_with_db,
    market_data::consolidator::Consolidator,
    strategy::strategy::{StrategyEnum, StrategyExecutor},
};

mod database;
mod execution;
mod ibc;
mod init;
mod logger;
mod market_data;
mod strategy;

#[macro_export]
macro_rules! unlock {
    ($variable:expr, $name:expr, $fn_name:expr) => {{
        $variable.lock().map_err(|e| {
            tracing::error!(
                "Failed to acquire lock from {} in {}: {}",
                $name,
                $fn_name,
                e
            );
            format!(
                "Failed to acquire lock from {} in {}: {}",
                $name, $fn_name, e
            )
        })?
    }};
}

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

async fn sleep_until_next_market_open() {
    let now_utc: DateTime<Utc> = Utc::now();
    let now_est = now_utc.with_timezone(&New_York);

    // Define market open time (9:30 AM EST)
    let market_open_hour = 9;
    let market_open_minute = 0;

    // Get the current date in EST
    let today = now_est.date_naive();

    tracing::info!("time is {}", now_est.hour());
    if today.is_busday().unwrap()
        && now_est.time()
            > chrono::NaiveTime::from_hms_opt(market_open_hour, market_open_minute, 0).unwrap()
        && now_est.time() < chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap()
    {
        return;
    }

    // If current time is before today's market open and today is a trading day, sleep until today's open
    if now_est.time()
        < chrono::NaiveTime::from_hms_opt(market_open_hour, market_open_minute, 0).unwrap()
        && today.is_busday().unwrap()
    {
        let next_open = New_York
            .with_ymd_and_hms(
                today.year(),
                today.month(),
                today.day(),
                market_open_hour,
                market_open_minute,
                0,
            )
            .unwrap();
        let duration = next_open - now_est;
        println!(
            "Sleeping until today's market open in {} seconds...",
            duration.num_seconds()
        );
        sleep(Duration::from_secs(duration.num_seconds() as u64)).await;
        return;
    }

    // Otherwise, find the next trading day after today
    let mut next_day = today.succ_opt().unwrap();
    while !next_day.is_busday().unwrap() {
        next_day = next_day.succ_opt().unwrap();
    }

    // Sleep until next trading day's open (9:30 AM EST)
    let next_open = New_York
        .with_ymd_and_hms(
            next_day.year(),
            next_day.month(),
            next_day.day(),
            market_open_hour,
            market_open_minute,
            0,
        )
        .unwrap();

    let duration = next_open - now_est;
    println!(
        "Sleeping until next market open on {} in {} seconds...",
        next_day,
        duration.num_seconds()
    );
    sleep(Duration::from_secs(duration.num_seconds() as u64)).await;
}

async fn sleep_until_market_close() {
    let now_eastern = Utc::now().with_timezone(&New_York);
    let close_time = New_York
        .with_ymd_and_hms(
            now_eastern.year(),
            now_eastern.month(),
            now_eastern.day(),
            16,
            5,
            0,
        )
        .unwrap();

    tracing::info!("check if is in this fn");
    if now_eastern < close_time {
        let duration = close_time - now_eastern;
        let duration = Duration::from_secs(duration.num_seconds() as u64);
        println!(
            "Sleeping until market close in {} seconds...",
            duration.as_secs()
        );
        sleep(duration).await;
    } else {
        println!("Market already closed.");
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    loop {
        sleep_until_next_market_open().await;

        // ================== INITIALISATION ======================
        let (gateway, success) = IBGateway::start("/tmp/ibc.log".to_string())
            .await
            .map_err(|e| format!("IBC error: {}", e))?;
        if success {
            println!("✅ IBC logged in successfully");
        } else {
            println!("❌ IBC exited with error");
            continue;
        }
        // ================== INITIALISATION ======================

        // ================== INITIALISATION ======================
        let database_url = std::env::var("DATABASE_URL")
            .expect("Expected DATABASE_URL environment variable to be set!");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            // .connect("postgres://ryantan:admin@localhost:5432/rust_trading_system")
            .await
            .map_err(|e| format!("error {}", e))?;

        if let Err(e) = sqlx::migrate!("./migrations").run(&pool).await {
            tracing::error!("Error intialising migrations: {}", e);
        };
        if let Err(e) = init_logger_with_db(pool.clone()).await {
            tracing::error!("Error intialising logger: {}", e);
        };
        let master_client = Arc::new(match Client::connect("127.0.0.1:4002", 0) {
        Ok(client) => Some(client),
        Err(e) => {
            tracing::error!(
                "Connection to TWS via \nURL: localhost:4002\n Client Id: 0\n failed!\nError: {}",
                e
            );
            None
        }
    }
    .expect("Expected to be able to connect to the IB Gateway instance with client id 0"));
        tracing::info!("Connected to client {}", master_client.client_id());
        let client_1 = Arc::new(match Client::connect("127.0.0.1:4002", 1) {
        Ok(client) => Some(client),
        Err(e) => {
            tracing::error!(
                "Connection to TWS via \nURL: localhost:4002\n Client Id: 1\n failed!\nError: {}",
                e
            );
            None
        }
    }
    .expect("Expected to be able to connect to the IB Gateway instance with client id 1"));
        tracing::info!("Connected to client {}", client_1.client_id());
        let client_2 = Arc::new(match Client::connect("127.0.0.1:4002", 2) {
        Ok(client) => Some(client),
        Err(e) => {
            tracing::error!(
                "Connection to TWS via \nURL: localhost:4002\n Client Id: 2\n failed!\nError: {}",
                e
            );
            None
        }
    }
    .expect("Expected to be able to connect to the IB Gateway instance with client id 2"));
        tracing::info!("Connected to client {}", client_2.client_id());
        // ================== INITIALISATION ======================
        let mut strategies: Vec<StrategyEnum> = Vec::new();

        let strat_a = StratA::new(pool.clone());
        let strat_b = StratB::new(pool.clone());

        strategies.push(StrategyEnum::StratA(strat_a.clone()));
        strategies.push(StrategyEnum::StratB(strat_b.clone()));
        let order_engine = Arc::new(OrderEngine::new(pool.clone(), strategies));
        order_engine.init_order_update_stream(master_client.clone());
        tracing::info!("Initialised order update stream");
        // ================== INITIALISATION ======================

        // ================== SYNC first ======================
        order_engine.sync_executions(&master_client);
        order_engine.sync_open_orders(&master_client);
        order_engine.sync_positions(&master_client);
        // ================== SYNC first ======================

        let consolidator = Arc::new(Consolidator::<StrategyEnum>::new(
            pool.clone(),
            client_1.clone(),
        ));
        consolidator.begin_bar_listening(order_engine.clone(), master_client.clone());
        tracing::info!("Initialised bar listening");

        // ============== strat_a ===================
        let cloned_pool = pool.clone();
        let cloned_consolidator = consolidator.clone();
        tokio::spawn(async move {
            let contract = ContractBuilder::new()
                .symbol("QQQ")
                .security_type(ibapi::prelude::SecurityType::Stock)
                .exchange("SMART")
                .currency("USD")
                .build()
                .expect("Expected to be able to build QQQ contract for strategy");
            let strategy_crud = get_strategy_crud(cloned_pool.clone());
            if let Err(e) = strategy_crud
                .create_or_ignore(&crate::database::models::StrategyFullKeys {
                    strategy: "strat_a".to_string(),
                    capital: 10000.0,
                    initial_capital: 10000.0,
                    status: crate::database::models::Status::Active,
                })
                .await
            {
                tracing::error!("Error trying to create_or_ignore : {}", e)
            }

            let start = Instant::now();
            strat_a
                .warm_up_data(cloned_consolidator.clone())
                .await
                .expect("Expected to be able to get warmed up data for ");
            let duration = start.elapsed();
            println!("FractionalMomentum took: {:?} to warm up fully", duration);

            cloned_consolidator.subscribe_to_data(
                StrategyEnum::StratA(strat_a.clone()),
                contract.clone(),
                5,
                ibapi::prelude::RealtimeWhatToShow::Trades,
            )
        });
        // ============== strat_a ===================

        // ============== strat_b ===================
        let cloned_pool = pool.clone();
        let cloned_consolidator = consolidator.clone();
        tokio::spawn(async move {
            let contract = ContractBuilder::new()
                .symbol("QQQ")
                .security_type(ibapi::prelude::SecurityType::Stock)
                .exchange("SMART")
                .currency("USD")
                .build()
                .expect("Expected to be able to build QQQ contract for strategy");
            let strategy_crud = get_strategy_crud(cloned_pool.clone());
            if let Err(e) = strategy_crud
                .create_or_ignore(&crate::database::models::StrategyFullKeys {
                    strategy: "strat_a".to_string(),
                    capital: 10000.0,
                    initial_capital: 10000.0,
                    status: crate::database::models::Status::Active,
                })
                .await
            {
                tracing::error!("Error trying to create_or_ignore : {}", e)
            }

            let start = Instant::now();
            strat_a
                .warm_up_data(cloned_consolidator.clone())
                .await
                .expect("Expected to be able to get warmed up data for ");
            let duration = start.elapsed();
            println!("FractionalMomentum took: {:?} to warm up fully", duration);

            cloned_consolidator.subscribe_to_data(
                StrategyEnum::StratB(strat_a.clone()),
                contract.clone(),
                5,
                ibapi::prelude::RealtimeWhatToShow::Trades,
            )
        });
        // ============== strat_b ===================

        sleep_until_market_close().await;
        order_engine.sync_executions(&master_client);
        order_engine.sync_open_orders(&master_client);
        order_engine.sync_positions(&master_client);

        // ============== TEARDOWN ===================
        drop(master_client);
        gateway
            .stop()
            .await
            .map_err(|e| format!("IBC error: {}", e))?;
        // ============== TEARDOWN ===================
    }
}
