use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use ibapi::Client;
use sqlx::postgres::PgPoolOptions;
use tokio::time::sleep;
use trading_app::{
    database::{
        crud::CRUDTrait, models::StrategyUpdateKeys, models_crud::strategy::get_strategy_crud,
    },
    execution::order_engine::OrderEngine,
    logger::init_logger,
    market_data::consolidator::Consolidator,
    strategy::{fractional_momentum::FractionalMomentum, strategy::StrategyExecutor},
};

#[tokio::test]
async fn test_fractional_momentum() -> Result<(), String> {
    let database_url = std::env::var("DATABASE_URL")
        .expect("Expected DATABASE_URL environment variable to be set!");
    let pool = PgPoolOptions::new()
        // .max_connections(5)
        .max_connections(50) // or 100+, depending on DB capacity
        .acquire_timeout(Duration::from_secs(30))
        .connect(&database_url)
        // .connect("postgres://ryantan:admin@localhost:5432/rust_trading_system")
        .await
        .map_err(|e| format!("error {}", e))?;
    let _ = init_logger();
    tracing::info!("Check if logger works!");

    if let Err(e) = sqlx::migrate!("./migrations").run(&pool).await {
        tracing::error!("Error intialising migrations: {}", e);
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

    let strategy_crud = get_strategy_crud(pool.clone());
    if let Err(e) = strategy_crud
        .create_or_update(
            &trading_app::database::models::StrategyPrimaryKeys {
                strategy: "fractional_momentum".to_string(),
            },
            &StrategyUpdateKeys {
                capital: Some(10000.0),
                initial_capital: Some(10000.0),
                status: Some(trading_app::database::models::Status::Active),
            },
        )
        .await
    {
        tracing::error!(
            "Error trying to create_or_update fractional_momentum: {}",
            e
        )
    }

    let frac_mom = FractionalMomentum::new(pool.clone());
    let mut strategies = Vec::new();
    strategies.push(frac_mom.clone());

    let consolidator = Arc::new(Consolidator::<FractionalMomentum>::new(
        pool.clone(),
        client_1.clone(),
    ));
    // consolidator.subscribe_to_data(
    //     noise.clone(),
    //     noise
    //         .get_contract("QQQ".to_string())
    //         .expect("Expected to be able to get contract for QQQ"),
    //     5,
    //     ibapi::prelude::RealtimeWhatToShow::Trades,
    // );
    let order_engine = Arc::new(OrderEngine::new(pool.clone(), strategies));
    consolidator.begin_bar_listening(
        order_engine.clone(),
        master_client.clone(),
        consolidator.clone(),
    );
    tracing::info!("Initialised bar listening");
    order_engine.init_order_update_stream(master_client.clone());
    tracing::info!("Initialised order update stream");

    let start = Instant::now();
    frac_mom
        .warm_up_data(consolidator.clone())
        .await
        .expect("Expected to be able to get warmed up data for noise");
    let duration = start.elapsed();
    println!("FractionalMomentum took: {:?} to warm up fully", duration);

    frac_mom.get_contracts().iter().for_each(|contract| {
        consolidator.subscribe_to_data(
            frac_mom.clone(),
            contract.clone(),
            5,
            ibapi::prelude::RealtimeWhatToShow::Trades,
        );
    });

    sleep(Duration::from_secs(500)).await;
    // consolidator
    Ok(())
}
