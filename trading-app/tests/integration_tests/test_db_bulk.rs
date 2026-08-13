//! DB integration tests for batch `COPY IN` operations.
//!
//! Tests `flush_batch_generic` — the binary COPY IN + dedup + merge SQL for
//! `BulkInsertable` impls (Stock/Options/Forex/DailyStock historical data).
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

/// Helper: get a raw tokio_postgres client from DATABASE_URL.
async fn raw_client() -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
) {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let (client, conn) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("failed to connect tokio_postgres");
    let handle = tokio::spawn(async move {
        let _ = conn.await;
    });
    (client, handle)
}

/// Cleanup helper: delete all rows for a stock from market_data.historical_data.
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
    let pool = setup_test_db().await;
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
    cleanup_stock(&pool, "BULKTEST").await;
    let (mut client, _handle) = raw_client().await;

    let now = Utc::now();
    let stock = "BULKTEST";
    let batch = vec![
        HistoricalStockDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            time: now,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: Decimal::new(1000, 0),
        },
        // Same PK, different close — tests dedup (first wins) + merge upsert
        HistoricalStockDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            time: now,
            open: 100.0,
            high: 102.0,
            low: 98.0,
            close: 103.0, // different close
            volume: Decimal::new(2000, 0),
        },
    ];

    flush_batch_generic(&mut client, &batch)
        .await
        .expect("flush_batch_generic failed");

    // Read back — should have the LAST write's values (merge = upsert)
    let crud = HistoricalDataCRUD::stock(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Stock(
        trading_app::database::models::HistoricalStockDataPrimaryKeysWoTime {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
        },
    );
    let bars = crud
        .read_last_n(pk, 5, 5)
        .await
        .expect("read_last_n failed");

    assert!(!bars.full.is_empty() || !bars.incomplete.is_empty(), "should have 1 bar");
    let bar_vec = if !bars.full.is_empty() { &bars.full } else { &bars.incomplete };
    let bar = &bar_vec[0];
    // After merge, close should be 103.0 (last write wins via ON CONFLICT DO UPDATE)
    use trading_app::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
    match bar {
        HistoricalDataFullKeys::Stock(s) => {
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
    cleanup_stock(&pool, "DEDUPTEST").await;
    let (mut client, _handle) = raw_client().await;

    let now = Utc::now();
    let stock = "DEDUPTEST";
    // 3 rows, but 2 have the same PK → dedup should keep first, so only 2 unique rows
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
            time: now, // same time = same PK
            open: 100.0, high: 102.0, low: 98.0, close: 103.0,
            volume: Decimal::new(2000, 0),
        },
        HistoricalStockDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            time: now + chrono::Duration::minutes(5), // different time = different PK
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
    let bars = crud
        .read_last_n(pk, 5, 5)
        .await
        .expect("read_last_n failed");

    let total = bars.full.len() + bars.incomplete.len();
    assert_eq!(total, 2, "dedup should keep 2 unique rows (3 input, 2 unique PKs)");

    cleanup_stock(&pool, stock).await;
}
