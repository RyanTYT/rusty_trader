use std::{sync::Arc, time::Duration};

use bigdecimal::FromPrimitive;
use ibapi::Client;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::time::{Instant, sleep};
use tracing::info;
use trading_app::{
    database::{
        crud::CRUDTrait,
        models::{
            AssetType, OpenStockOrdersFullKeys, StockTransactionsFullKeys, StrategyUpdateKeys,
            TargetStockPositionsFullKeys, TargetStockPositionsPrimaryKeys,
            TargetStockPositionsUpdateKeys,
        },
        models_crud::{
            current_stock_positions::get_current_stock_positions_crud,
            historical_data::get_historical_data_crud,
            open_stock_orders::get_open_stock_orders_crud,
            stock_transactions::get_stock_transactions_crud, strategy::get_strategy_crud,
            target_stock_positions::get_target_stock_positions_crud,
        },
    },
    execution::order_engine::OrderEngine,
    logger::init_logger,
    market_data::consolidator::Consolidator,
    strategy::strategy::StrategyExecutor,
};

async fn wait_for_order(
    pool: PgPool,
    timeout: Duration,
) -> Result<OpenStockOrdersFullKeys, anyhow::Error> {
    let open_stock_orders_crud = get_open_stock_orders_crud(pool);
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(orders) = open_stock_orders_crud.read_all().await? {
            if orders.len() == 0 {
                sleep(Duration::from_millis(200)).await;
                continue;
            }
            assert!(orders.len() == 1);
            return Ok(orders.first().unwrap().clone());
        }
        sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("Timeout waiting for open order");
}

async fn wait_for_execution(
    pool: PgPool,
    min_size: usize,
    timeout: Duration,
) -> Result<Vec<StockTransactionsFullKeys>, anyhow::Error> {
    let stock_transactions_crud = get_stock_transactions_crud(pool);
    let start = Instant::now();
    let beginning_no_rows = stock_transactions_crud
        .read_all()
        .await
        .expect("Expected no error reading num rows in StockTransactions")
        .expect("Expected some rows")
        .len();
    while start.elapsed() < timeout {
        if let Some(trans) = stock_transactions_crud.read_all().await? {
            if trans.len() == beginning_no_rows {
                sleep(Duration::from_millis(200)).await;
                continue;
            }
            assert!(trans.len() >= min_size);
            return Ok(trans);
        }
        sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("Timeout waiting for open order");
}

async fn wait_for_commission(pool: PgPool, timeout: Duration) -> Result<(), anyhow::Error> {
    let stock_transactions_crud = get_stock_transactions_crud(pool);
    let start = Instant::now();
    let beginning_no_rows = stock_transactions_crud
        .read_all()
        .await
        .expect("Expected no error reading num rows in StockTransactions")
        .expect("Expected some rows")
        .len();
    while start.elapsed() < timeout {
        if let Some(trans) = stock_transactions_crud.read_all().await? {
            if trans.iter().all(|transaction| {
                transaction.fees
                    != bigdecimal::BigDecimal::from_u8(0)
                        .expect("Expected to be able to parse 0 to BigDecimal")
            }) {
                return Ok(());
            }
            sleep(Duration::from_millis(200)).await;
        }
        sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("Timeout waiting for open order");
}

#[tokio::test]
async fn test_order_tracking_del_target_pos() {
    let _ = init_logger();
    tracing::info!("Check if logger works!");

    // Initialisation stage - for DB
    let database_url = std::env::var("DATABASE_URL")
        .expect("Expected DATABASE_URL environment variable to be set!");
    // let database_url = "postgres://ryantan:admin@localhost:5432/rust_trading_system";
    tracing::info!("db url is {}", &database_url);
    let pool = match PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
    {
        Ok(pool) => Some(pool),
        Err(_) => {
            tracing::error!("Error starting postgres connection");
            None
        }
    }
    .expect("Expected postgres connection to be started flawlessly");

    if let Err(e) = sqlx::migrate!("./migrations").run(&pool.clone()).await {
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
    info!("Connected to {}", master_client.client_id());

    let strat_a = StratA::new(pool.clone());
    let mut strategies = Vec::new();
    strategies.push(strat_a.clone());

    let consolidator = Consolidator::<StratA>::new(pool.clone(), master_client.clone());
    let order_engine = Arc::new(OrderEngine::new(pool.clone(), strategies));
    consolidator.begin_bar_listening(order_engine.clone());
    tracing::info!("Initialised bar listening");
    order_engine.init_order_update_stream(master_client.clone());
    tracing::info!("Initialised order update stream");

    let target_stock_positions_crud = get_target_stock_positions_crud(pool.clone());
    let strategy_crud = get_strategy_crud(pool.clone());
    if let Err(e) = strategy_crud
        .create_or_update(
            &trading_app::database::models::StrategyPrimaryKeys {
                strategy: "strat_a".to_string(),
            },
            &StrategyUpdateKeys {
                capital: Some(10000.0),
                initial_capital: Some(10000.0),
                status: Some(trading_app::database::models::Status::Active),
            },
        )
        .await
    {
        tracing::error!("Err: {}", e);
    };
    if let Err(e) = strategy_crud
        .create_or_update(
            &trading_app::database::models::StrategyPrimaryKeys {
                strategy: "unknown".to_string(),
            },
            &StrategyUpdateKeys {
                capital: Some(10000.0),
                initial_capital: Some(10000.0),
                status: Some(trading_app::database::models::Status::Active),
            },
        )
        .await
    {
        tracing::error!("Err: {}", e);
    };
    if let Err(e) = target_stock_positions_crud
        .create(&TargetStockPositionsFullKeys {
            strategy: "strat_a".to_string(),
            stock: "USD".to_string(),
            avg_price: 0.0,
            quantity: 5.0,
        })
        .await
    {
        tracing::error!("Err: {}", e);
    };
    info!("Inserted into Strategy and target positions!");

    // Expectation: Current Position - Nth, Target Position - 5
    // ================================ First Execution: BUY =====================================
    // Expect open market order to have been placed
    // as of now - can't tell whether it is mkt/limit order
    // - to be updated: use fill_price here to tell - 0 == mkt order
    let cloned_pool = pool.clone();
    let handle = tokio::spawn(async {
        let open_order = wait_for_order(cloned_pool, Duration::new(120, 0))
            .await
            .expect("Expected returned open order to be readable");
        assert!(open_order.stock == "USD");
        assert!(open_order.strategy == "strat_a");
        assert!(open_order.quantity == 5.0);
        assert!(open_order.filled == 0.0);
        assert!(open_order.executions.len() == 0);
        tracing::info!("Buy Open Order opened correctly");
    });
    order_engine.place_orders_for_strategy(
        strat_a.clone(),
        master_client.clone(),
        trading_app::database::models::AssetType::Stock,
    );
    handle
        .await
        .expect("Expected to be able to get an open order for BUY order");

    // Expect execution to be done within 10 minutes
    let mut expected_size = 1;
    let mut avg_price_filled = 0.0;
    loop {
        let executions = wait_for_execution(pool.clone(), expected_size, Duration::new(120, 0))
            .await
            .expect("Expected returned open order to be readable");
        // let executions
        assert!(executions.iter().all(|exec| exec.stock == "USD"));
        assert!(executions.iter().all(|exec| exec.strategy == "strat_a"));
        let cum_qty: f64 = executions.iter().map(|exec| exec.quantity).sum();
        let cum_value: f64 = executions
            .iter()
            .map(|exec| exec.quantity * exec.price)
            .sum();

        // Expect positiion to be updated after each execution
        // Give time to wait for operations to complete
        sleep(Duration::from_secs(1)).await;
        let current_stock_positions_crud = get_current_stock_positions_crud(pool.clone());
        let current_stock_pos = current_stock_positions_crud
            .read(
                &trading_app::database::models::CurrentStockPositionsPrimaryKeys {
                    stock: "USD".to_string(),
                    strategy: "strat_a".to_string(),
                },
            )
            .await
            .expect("Expected to be able to retrieve current stock position")
            .expect("Expected to get at least one row from current stock positions");
        assert!(current_stock_pos.quantity == cum_qty);
        assert!(current_stock_pos.avg_price == (cum_value / cum_qty));
        avg_price_filled = current_stock_pos.avg_price.clone();

        if executions
            .iter()
            .map(|exec| exec.quantity)
            .reduce(|accum, qty| accum + qty)
            .expect("Expected at least one entry in executions")
            >= 5.0
        {
            break;
        }
        tracing::info!(
            "Execution {} created correctly, Current Stock position updated correctly",
            expected_size
        );
        expected_size += 1;
    }

    // Expect open order to be deleted once execution is fully done
    let open_stock_orders_crud = get_open_stock_orders_crud(pool.clone());
    let all_open_stock_orders = open_stock_orders_crud
        .read_all()
        .await
        .expect("Expected to be able to get all rows from open_stock_orders after execution")
        .expect("Expected result");
    assert!(all_open_stock_orders.len() == 0);
    tracing::info!("Open Stock Order deleted correctly");
    // ================================ First Execution: BUY =====================================

    tracing::info!("Reached first checkpoint of order_tracking");

    // ================================ Second Execution: SELL =====================================
    // Expectation: Current Position - 5, Target Position - 0
    // target_stock_positions_crud.create_or_update(
    //     &trading_app::database::models::TargetStockPositionsPrimaryKeys {
    //         strategy: "strat_a".to_string(),
    //         stock: "USD".to_string(),
    //     },
    //     &TargetStockPositionsUpdateKeys {
    //         avg_price: Some(0.0),
    //         quantity: Some(0.0)
    //     }
    // ).await.expect("Expected to be able to create_or_update entry of StratA for testing purposes to TargetStockPositions");
    target_stock_positions_crud
        .delete(&TargetStockPositionsPrimaryKeys {
            strategy: "strat_a".to_string(),
            stock: "USD".to_string(),
        })
        .await
        .expect("Expected to be able to delete entry for strat_a strategy to 0.0");

    // Expect open market order to have been placed
    let cloned_pool = pool.clone();
    let handle = tokio::spawn(async {
        let open_order = wait_for_order(cloned_pool, Duration::new(120, 0))
            .await
            .expect("Expected returned open order to be readable");
        assert!(open_order.stock == "USD");
        assert!(open_order.strategy == "strat_a");
        assert!(open_order.quantity == -5.0);
        assert!(open_order.filled == 0.0);
        assert!(open_order.executions.len() == 0);
        tracing::info!("Sell Open Order opened correctly");
    });
    order_engine.place_orders_for_strategy(
        strat_a,
        master_client.clone(),
        trading_app::database::models::AssetType::Stock,
    );
    handle
        .await
        .expect("Expected to be able to get an open order for SELL order");

    // Expect execution to be done within 10 minutes
    expected_size += 1; // from previously
    loop {
        let executions = wait_for_execution(pool.clone(), expected_size, Duration::new(120, 0))
            .await
            .expect("Expected returned open order to be readable");
        // let executions
        assert!(executions.iter().all(|exec| exec.stock == "USD"));
        assert!(executions.iter().all(|exec| exec.strategy == "strat_a"));
        let cum_qty: f64 = executions
            .iter()
            .filter_map(|exec| {
                if exec.quantity < 0.0 {
                    Some(-exec.quantity)
                } else {
                    None
                }
            })
            .sum();
        // let cum_value: f64 = executions
        //     .iter()
        //     .filter_map(|exec| {
        //         if exec.quantity < 0.0 {
        //             Some(-exec.quantity * exec.price)
        //         } else {
        //             None
        //         }
        //     })
        //     .sum();

        // Expect positiion to be updated after each execution
        // Give time to wait for operations to complete
        sleep(Duration::from_secs(1)).await;
        let current_stock_positions_crud = get_current_stock_positions_crud(pool.clone());
        let current_stock_pos = current_stock_positions_crud
            .read(
                &trading_app::database::models::CurrentStockPositionsPrimaryKeys {
                    stock: "USD".to_string(),
                    strategy: "strat_a".to_string(),
                },
            )
            .await
            .expect("Expected to be able to retrieve current stock position")
            .expect("Expected to get at least one row from current stock positions");
        assert!(current_stock_pos.quantity == 5.0 - cum_qty);
        assert!(current_stock_pos.avg_price == avg_price_filled);

        if executions
            .iter()
            .filter_map(|exec| {
                if exec.quantity < 0.0 {
                    Some(-exec.quantity)
                } else {
                    None
                }
            })
            .reduce(|accum, qty| accum + qty)
            .expect("Expected at least one short entry in executions")
            >= 5.0
        {
            break;
        }
        tracing::info!(
            "Execution {} created correctly, Current Stock position updated correctly",
            expected_size
        );
        expected_size += 1;
    }

    // Expect open order to be deleted once execution is fully done
    let open_stock_orders_crud = get_open_stock_orders_crud(pool.clone());
    let all_open_stock_orders = open_stock_orders_crud
        .read_all()
        .await
        .expect("Expected to be able to get all rows from open_stock_orders after execution")
        .expect("Expected result");
    assert!(all_open_stock_orders.len() == 0);
    tracing::info!("Open Stock Order deleted correctly");

    info!("Waiting for commission updates now!");
    wait_for_commission(pool.clone(), Duration::new(120, 0))
        .await
        .expect("Expected to get updated commissions for executions");

    tracing::info!("order_tracking test finished well");

    // Expect No open orders, No current positions, >= 2 stock transactions

    // ================================ Second Execution: SELL =====================================
}

// #[tokio::test]
// async fn test_order_tracking_update_target_pos() {
//     let _ = init_logger();
//     tracing::info!("Check if logger works!");
//
//     // Initialisation stage - for DB
//     let database_url = std::env::var("DATABASE_URL")
//         .expect("Expected DATABASE_URL environment variable to be set!");
//     // let database_url = "postgres://ryantan:admin@localhost:5432/rust_trading_system";
//     tracing::info!("db url is {}", &database_url);
//     let pool = match PgPoolOptions::new()
//         .max_connections(5)
//         .connect(&database_url)
//         .await
//     {
//         Ok(pool) => Some(pool),
//         Err(_) => {
//             tracing::error!("Error starting postgres connection");
//             None
//         }
//     }
//     .expect("Expected postgres connection to be started flawlessly");
//
//     if let Err(e) = sqlx::migrate!("./migrations").run(&pool.clone()).await {
//         tracing::error!("Error intialising migrations: {}", e);
//     };
//
//     let master_client = Arc::new(match Client::connect("127.0.0.1:4002", 0) {
//         Ok(client) => Some(client),
//         Err(e) => {
//             tracing::error!(
//                 "Connection to TWS via \nURL: localhost:4002\n Client Id: 0\n failed!\nError: {}",
//                 e
//             );
//             None
//         }
//     }
//     .expect("Expected to be able to connect to the IB Gateway instance with client id 0"));
//     info!("Connected to {}", master_client.client_id());
//
//     let strat_a = StratA::new(pool.clone());
//     let mut strategies = Vec::new();
//     strategies.push(strat_a.clone());
//
//     let consolidator = Consolidator::<StratA>::new(pool.clone(), master_client.clone());
//     let order_engine = Arc::new(OrderEngine::new(pool.clone(), strategies));
//     consolidator.begin_bar_listening(order_engine.clone());
//     tracing::info!("Initialised bar listening");
//     order_engine.init_order_update_stream(master_client.clone());
//     tracing::info!("Initialised order update stream");
//
//     let target_stock_positions_crud = get_target_stock_positions_crud(pool.clone());
//     let strategy_crud = get_strategy_crud(pool.clone());
//     if let Err(e) = strategy_crud
//         .create_or_update(
//             &trading_app::database::models::StrategyPrimaryKeys {
//                 strategy: "strat_a".to_string(),
//             },
//             &StrategyUpdateKeys {
//                 capital: Some(10000.0),
//                 initial_capital: Some(10000.0),
//                 status: Some(trading_app::database::models::Status::Active),
//             },
//         )
//         .await
//     {
//         tracing::error!("Err: {}", e);
//     };
//     if let Err(e) = strategy_crud
//         .create_or_update(
//             &trading_app::database::models::StrategyPrimaryKeys {
//                 strategy: "unknown".to_string(),
//             },
//             &StrategyUpdateKeys {
//                 capital: Some(10000.0),
//                 initial_capital: Some(10000.0),
//                 status: Some(trading_app::database::models::Status::Active),
//             },
//         )
//         .await
//     {
//         tracing::error!("Err: {}", e);
//     };
//     if let Err(e) = target_stock_positions_crud
//         .create(&TargetStockPositionsFullKeys {
//             strategy: "strat_a".to_string(),
//             stock: "USD".to_string(),
//             avg_price: 0.0,
//             quantity: 5.0,
//         })
//         .await
//     {
//         tracing::error!("Err: {}", e);
//     };
//     info!("Inserted into Strategy and target positions!");
//
//     // Expectation: Current Position - Nth, Target Position - 5
//     // ================================ First Execution: BUY =====================================
//     // Expect open market order to have been placed
//     // as of now - can't tell whether it is mkt/limit order
//     // - to be updated: use fill_price here to tell - 0 == mkt order
//     let cloned_pool = pool.clone();
//     let handle = tokio::spawn(async {
//         let open_order = wait_for_order(cloned_pool, Duration::new(120, 0))
//             .await
//             .expect("Expected returned open order to be readable");
//         assert!(open_order.stock == "USD");
//         assert!(open_order.strategy == "strat_a");
//         assert!(open_order.quantity == 5.0);
//         assert!(open_order.filled == 0.0);
//         assert!(open_order.executions.len() == 0);
//         tracing::info!("Buy Open Order opened correctly");
//     });
//     order_engine.place_orders_for_strategy(
//         strat_a.clone(),
//         master_client.clone(),
//         trading_app::database::models::AssetType::Stock,
//     );
//     handle
//         .await
//         .expect("Expected to be able to get an open order for BUY order");
//
//     // Expect execution to be done within 10 minutes
//     let mut expected_size = 1;
//     let mut avg_price_filled = 0.0;
//     loop {
//         let executions = wait_for_execution(pool.clone(), expected_size, Duration::new(120, 0))
//             .await
//             .expect("Expected returned open order to be readable");
//         // let executions
//         assert!(executions.iter().all(|exec| exec.stock == "USD"));
//         assert!(executions.iter().all(|exec| exec.strategy == "strat_a"));
//         let cum_qty: f64 = executions.iter().map(|exec| exec.quantity).sum();
//         let cum_value: f64 = executions
//             .iter()
//             .map(|exec| exec.quantity * exec.price)
//             .sum();
//
//         // Expect positiion to be updated after each execution
//         // Give time to wait for operations to complete
//         sleep(Duration::from_secs(1)).await;
//         let current_stock_positions_crud = get_current_stock_positions_crud(pool.clone());
//         let current_stock_pos = current_stock_positions_crud
//             .read(
//                 &trading_app::database::models::CurrentStockPositionsPrimaryKeys {
//                     stock: "USD".to_string(),
//                     strategy: "strat_a".to_string(),
//                 },
//             )
//             .await
//             .expect("Expected to be able to retrieve current stock position")
//             .expect("Expected to get at least one row from current stock positions");
//         assert!(current_stock_pos.quantity == cum_qty);
//         assert!(current_stock_pos.avg_price == (cum_value / cum_qty));
//         avg_price_filled = current_stock_pos.avg_price.clone();
//
//         if executions
//             .iter()
//             .map(|exec| exec.quantity)
//             .reduce(|accum, qty| accum + qty)
//             .expect("Expected at least one entry in executions")
//             >= 5.0
//         {
//             break;
//         }
//         tracing::info!(
//             "Execution {} created correctly, Current Stock position updated correctly",
//             expected_size
//         );
//         expected_size += 1;
//     }
//
//     // Expect open order to be deleted once execution is fully done
//     let open_stock_orders_crud = get_open_stock_orders_crud(pool.clone());
//     let all_open_stock_orders = open_stock_orders_crud
//         .read_all()
//         .await
//         .expect("Expected to be able to get all rows from open_stock_orders after execution")
//         .expect("Expected result");
//     assert!(all_open_stock_orders.len() == 0);
//     tracing::info!("Open Stock Order deleted correctly");
//     // ================================ First Execution: BUY =====================================
//
//     tracing::info!("Reached first checkpoint of order_tracking");
//
//     // ================================ Second Execution: SELL =====================================
//     // Expectation: Current Position - 5, Target Position - 0
//     target_stock_positions_crud.create_or_update(
//         &trading_app::database::models::TargetStockPositionsPrimaryKeys {
//             strategy: "strat_a".to_string(),
//             stock: "USD".to_string(),
//         },
//         &TargetStockPositionsUpdateKeys {
//             avg_price: Some(0.0),
//             quantity: Some(0.0)
//         }
//     ).await.expect("Expected to be able to create_or_update entry of StratA for testing purposes to TargetStockPositions");
//
//     // Expect open market order to have been placed
//     let cloned_pool = pool.clone();
//     let handle = tokio::spawn(async {
//         let open_order = wait_for_order(cloned_pool, Duration::new(120, 0))
//             .await
//             .expect("Expected returned open order to be readable");
//         assert!(open_order.stock == "USD");
//         assert!(open_order.strategy == "strat_a");
//         assert!(open_order.quantity == -5.0);
//         assert!(open_order.filled == 0.0);
//         assert!(open_order.executions.len() == 0);
//         tracing::info!("Sell Open Order opened correctly");
//     });
//     order_engine.place_orders_for_strategy(
//         strat_a,
//         master_client.clone(),
//         trading_app::database::models::AssetType::Stock,
//     );
//     handle
//         .await
//         .expect("Expected to be able to get an open order for SELL order");
//
//     // Expect execution to be done within 10 minutes
//     expected_size += 1; // from previously
//     loop {
//         let executions = wait_for_execution(pool.clone(), expected_size, Duration::new(120, 0))
//             .await
//             .expect("Expected returned open order to be readable");
//         // let executions
//         assert!(executions.iter().all(|exec| exec.stock == "USD"));
//         assert!(executions.iter().all(|exec| exec.strategy == "strat_a"));
//         let cum_qty: f64 = executions
//             .iter()
//             .filter_map(|exec| {
//                 if exec.quantity < 0.0 {
//                     Some(-exec.quantity)
//                 } else {
//                     None
//                 }
//             })
//             .sum();
//         // let cum_value: f64 = executions
//         //     .iter()
//         //     .filter_map(|exec| {
//         //         if exec.quantity < 0.0 {
//         //             Some(-exec.quantity * exec.price)
//         //         } else {
//         //             None
//         //         }
//         //     })
//         //     .sum();
//
//         // Expect positiion to be updated after each execution
//         // Give time to wait for operations to complete
//         sleep(Duration::from_secs(1)).await;
//         let current_stock_positions_crud = get_current_stock_positions_crud(pool.clone());
//         let current_stock_pos = current_stock_positions_crud
//             .read(
//                 &trading_app::database::models::CurrentStockPositionsPrimaryKeys {
//                     stock: "USD".to_string(),
//                     strategy: "strat_a".to_string(),
//                 },
//             )
//             .await
//             .expect("Expected to be able to retrieve current stock position")
//             .expect("Expected to get at least one row from current stock positions");
//         assert!(current_stock_pos.quantity == 5.0 - cum_qty);
//         assert!(current_stock_pos.avg_price == avg_price_filled);
//
//         if executions
//             .iter()
//             .filter_map(|exec| {
//                 if exec.quantity < 0.0 {
//                     Some(-exec.quantity)
//                 } else {
//                     None
//                 }
//             })
//             .reduce(|accum, qty| accum + qty)
//             .expect("Expected at least one short entry in executions")
//             >= 5.0
//         {
//             break;
//         }
//         tracing::info!(
//             "Execution {} created correctly, Current Stock position updated correctly",
//             expected_size
//         );
//         expected_size += 1;
//     }
//
//     // Expect open order to be deleted once execution is fully done
//     let open_stock_orders_crud = get_open_stock_orders_crud(pool.clone());
//     let all_open_stock_orders = open_stock_orders_crud
//         .read_all()
//         .await
//         .expect("Expected to be able to get all rows from open_stock_orders after execution")
//         .expect("Expected result");
//     assert!(all_open_stock_orders.len() == 0);
//     tracing::info!("Open Stock Order deleted correctly");
//
//     info!("Waiting for commission updates now!");
//     wait_for_commission(pool.clone(), Duration::new(120, 0))
//         .await
//         .expect("Expected to get updated commissions for executions");
//
//     tracing::info!("order_tracking test finished well");
//
//     // Expect No open orders, No current positions, >= 2 stock transactions
//
//     // ================================ Second Execution: SELL =====================================
// }
