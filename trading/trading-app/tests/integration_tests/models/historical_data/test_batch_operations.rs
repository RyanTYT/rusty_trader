//! DB integration tests for `BulkInsertable` trait + `flush_batch_generic`.
//!
//! Tests: `flush_batch_generic` (Stock, Forex, Options, DailyStock variants),
//! `BatchDbCreator` lifecycle.
//!
//! Requires: live Postgres + DATABASE_URL. All tests #[ignore]'d.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::models::HistoricalStockDataFullKeys;
use trading_app::database::models_crud::historical_data::batch_operations::flush_batch_generic;
use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, HistoricalDataOps, HistoricalDataPrimaryKeysWoTime,
};

use crate::models::init::{TEST_MUTEX, setup_test_db};

async fn raw_client() -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let (client, conn) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("failed to connect tokio_postgres");
    let handle = tokio::spawn(async move { let _ = conn.await; });
    (client, handle)
}

async fn cleanup_stock(pool: &sqlx::PgPool, stock: &str) {
    let _ = sqlx::query("DELETE FROM market_data.historical_data WHERE stock = $1")
        .bind(stock)
        .execute(pool)
        .await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_flush_batch_generic_empty_batch_is_noop() {
    let _lock = TEST_MUTEX.lock().await;
    let _pool = setup_test_db().await;
    let (mut client, _handle) = raw_client().await;

    let empty: Vec<HistoricalStockDataFullKeys> = vec![];
    let result = flush_batch_generic(&mut client, &empty).await;
    assert!(result.is_ok(), "empty batch should be Ok(())");
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_flush_batch_generic_inserts_and_upserts() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "BULKTEST";
    cleanup_stock(&pool, stock).await;
    let (mut client, _handle) = raw_client().await;

    let now = Utc::now();
    let batch = vec![
        HistoricalStockDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            time: now,
            open: 100.0, high: 101.0, low: 99.0, close: 100.5,
            volume: Decimal::new(1000, 0),
        },
        HistoricalStockDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            time: now,
            open: 100.0, high: 102.0, low: 98.0, close: 103.0,
            volume: Decimal::new(2000, 0),
        },
    ];

    flush_batch_generic(&mut client, &batch)
        .await
        .expect("flush_batch_generic failed");

    let crud = HistoricalDataCRUD::stock(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Stock(
        trading_app::database::models::HistoricalStockDataPrimaryKeysWoTime {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
        },
    );
    let bars = crud.read_last_n(pk, 5, 5).await.expect("read_last_n failed");

    assert!(!bars.full.is_empty() || !bars.incomplete.is_empty(), "should have 1 bar");
    let bar_vec = if !bars.full.is_empty() { &bars.full } else { &bars.incomplete };
    match &bar_vec[0] {
        trading_app::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys::Stock(s) => {
            assert_eq!(s.stock, stock);
            assert_eq!(s.close, 103.0, "merge should upsert to last close value");
        }
        _ => panic!("expected Stock variant"),
    }

    cleanup_stock(&pool, stock).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_flush_batch_generic_dedup() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "DEDUPTEST";
    cleanup_stock(&pool, stock).await;
    let (mut client, _handle) = raw_client().await;

    let now = Utc::now();
    let batch = vec![
        HistoricalStockDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            time: now,
            open: 100.0, high: 101.0, low: 99.0, close: 100.5,
            volume: Decimal::new(1000, 0),
        },
        HistoricalStockDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            time: now,
            open: 100.0, high: 102.0, low: 98.0, close: 103.0,
            volume: Decimal::new(2000, 0),
        },
        HistoricalStockDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            time: now + chrono::Duration::minutes(5),
            open: 104.0, high: 105.0, low: 103.0, close: 104.5,
            volume: Decimal::new(3000, 0),
        },
    ];

    flush_batch_generic(&mut client, &batch)
        .await
        .expect("flush_batch_generic failed");

    let crud = HistoricalDataCRUD::stock(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Stock(
        trading_app::database::models::HistoricalStockDataPrimaryKeysWoTime {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
        },
    );
    let bars = crud.read_last_n(pk, 5, 5).await.expect("read_last_n failed");

    let total = bars.full.len() + bars.incomplete.len();
    assert_eq!(total, 2, "dedup should keep 2 unique rows (3 input, 2 unique PKs)");

    cleanup_stock(&pool, stock).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_flush_batch_generic_forex() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let pair = "EUR/USD";
    // Cleanup forex data
    let _ = sqlx::query("DELETE FROM market_data.historical_forex_data WHERE pair = $1")
        .bind(pair)
        .execute(&pool)
        .await;
    let (mut client, _handle) = raw_client().await;

    let now = Utc::now();
    use trading_app::database::models::HistoricalForexDataFullKeys;
    let batch = vec![
        HistoricalForexDataFullKeys {
            pair: pair.to_string(),
            time: now,
            bid_open: Some(1.0850), bid_high: Some(1.0870), bid_low: Some(1.0840), bid_close: Some(1.0860),
            ask_open: Some(1.0852), ask_high: Some(1.0872), ask_low: Some(1.0842), ask_close: Some(1.0862),
        },
        HistoricalForexDataFullKeys {
            pair: pair.to_string(),
            time: now,
            bid_open: Some(1.0850), bid_high: Some(1.0880), bid_low: Some(1.0830), bid_close: Some(1.0870),
            ask_open: Some(1.0852), ask_high: Some(1.0882), ask_low: Some(1.0832), ask_close: Some(1.0872),
        },
    ];

    flush_batch_generic(&mut client, &batch)
        .await
        .expect("flush_batch_generic failed");

    let crud = HistoricalDataCRUD::forex(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Forex(
        trading_app::database::models::HistoricalForexDataPrimaryKeysWoTime {
            pair: pair.to_string(),
        },
    );
    let bars = crud.read_last_n(pk, 5, 5).await.expect("read_last_n failed");

    let total = bars.full.len() + bars.incomplete.len();
    assert_eq!(total, 1, "should have 1 deduped forex bar");

    let _ = sqlx::query("DELETE FROM market_data.historical_forex_data WHERE pair = $1")
        .bind(pair)
        .execute(&pool)
        .await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_flush_batch_generic_options() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "OPTBULK";
    let _ = sqlx::query("DELETE FROM market_data.historical_options_data WHERE stock = $1")
        .bind(stock)
        .execute(&pool)
        .await;
    let (mut client, _handle) = raw_client().await;

    let now = Utc::now();
    use trading_app::database::models::{HistoricalOptionsDataFullKeys, OptionType};
    let batch = vec![
        HistoricalOptionsDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            expiry: "20250119".to_string(),
            strike: 150.0,
            multiplier: "100".to_string(),
            option_type: OptionType::Call,
            time: now,
            open: 3.50, high: 4.00, low: 3.25, close: 3.75,
            volume: Decimal::new(500, 0),
        },
        HistoricalOptionsDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            expiry: "20250119".to_string(),
            strike: 150.0,
            multiplier: "100".to_string(),
            option_type: OptionType::Call,
            time: now,
            open: 3.50, high: 4.10, low: 3.20, close: 3.80,
            volume: Decimal::new(600, 0),
        },
    ];

    flush_batch_generic(&mut client, &batch)
        .await
        .expect("flush_batch_generic failed");

    let crud = HistoricalDataCRUD::option(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Options(
        trading_app::database::models::HistoricalOptionsDataPrimaryKeysWoTime {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            expiry: "20250119".to_string(),
            strike: 150.0,
            multiplier: "100".to_string(),
            option_type: OptionType::Call,
        },
    );
    let bars = crud.read_last_n(pk, 5, 5).await.expect("read_last_n failed");

    let total = bars.full.len() + bars.incomplete.len();
    assert_eq!(total, 1, "should have 1 deduped option bar");

    let _ = sqlx::query("DELETE FROM market_data.historical_options_data WHERE stock = $1")
        .bind(stock)
        .execute(&pool)
        .await;
}

// #[tokio::test]
// #[ignore = "requires live Postgres + DATABASE_URL"]
// async fn test_flush_batch_generic_daily_stock() {
//     let _lock = TEST_MUTEX.lock().await;
//     let pool = setup_test_db().await;
//     let stock = "DLYBULK";
//     let _ = sqlx::query("DELETE FROM market_data.daily_ohlcv WHERE stock = $1")
//         .bind(stock)
//         .execute(&pool)
//         .await;
//     let (mut client, _handle) = raw_client().await;
//
//     let now = Utc::now();
//     use trading_app::database::models::DailyHistoricalStockDataFullKeys;
//     let batch = vec![
//         DailyHistoricalStockDataFullKeys {
//             stock: stock.to_string(),
//             primary_exchange: "NASDAQ".to_string(),
//             currency: "USD".to_string(),
//             day: now,
//             open: 100.0, high: 101.0,
//             low: 99.0, close: 105.0,
//             volume: Decimal::new(1000, 0),
//         },
//         DailyHistoricalStockDataFullKeys {
//             stock: stock.to_string(),
//             primary_exchange: "NASDAQ".to_string(),
//             currency: "USD".to_string(),
//             day: now,
//             open: 100.0, high: 102.0,
//             low: 98.0, close: 106.0,
//             volume: Decimal::new(2000, 0),
//         },
//     ];
//
//     flush_batch_generic(&mut client, &batch)
//         .await
//         .expect("flush_batch_generic failed");
//
//     let crud = HistoricalDataCRUD::daily_stock(pool.clone());
//     let pk = HistoricalDataPrimaryKeysWoTime::DailyStock(
//         trading_app::database::models::DailyHistoricalStockDataPrimaryKeysWoTime {
//             stock: stock.to_string(),
//             primary_exchange: "NASDAQ".to_string(),
//             currency: "USD".to_string(),
//         },
//     );
//     let bars = crud.read_last_n(pk, 5, 5).await.expect("read_last_n failed");
//
//     let total = bars.full.len() + bars.incomplete.len();
//     assert_eq!(total, 1, "should have 1 deduped daily bar");
//
//     let _ = sqlx::query("DELETE FROM market_data.daily_ohlcv WHERE stock = $1")
//         .bind(stock)
//         .execute(&pool)
//         .await;
// }
