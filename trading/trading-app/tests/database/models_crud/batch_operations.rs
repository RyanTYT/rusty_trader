//! Tests for `models_crud/historical_data/batch_operations.rs`.
//!
//! `init_channel` spawns a tokio-postgres worker that buffers `BatchDbCreator`
//! rows until `BATCH_SIZE`, one second of idleness, or an explicit `flush()`.
//! These tests drive the real worker against the test database.

use std::time::Duration;

use chrono::{TimeDelta, Utc};
use rust_decimal::Decimal;

use trading_app::database::crud::CRUDTrait;
use trading_app::database::db::DbDirect;
use trading_app::database::models::HistoricalStockDataFullKeys;
use trading_app::database::models_crud::historical_data::batch_operations::init_channel;
use trading_app::database::tables::DbTables;

use crate::common::TestDb;

fn configure_worker_from_test_url() {
    let url = std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("TEST_DATABASE_URL must be set when running batch tests");
    let rest = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .expect("postgres url");
    let (credentials, host_and_db) = rest.split_once('@').expect("credentials");
    let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
    // Trust auth ignores the password, but an empty `password=` makes the conninfo
    // parser swallow the following `dbname=` word.
    let password = if password.is_empty() { "unused" } else { password };
    let (host, database) = host_and_db.split_once('/').expect("database");
    let database = database.split('?').next().unwrap_or(database);
    // `init_channel` builds `host={} user=...`, so the port must be a conninfo word.
    let (hostname, port) = host.split_once(':').unwrap_or((host, "5432"));
    let host = format!("{hostname} port={port}");

    // `init_channel` reads these four variables; edition 2024 requires `unsafe`
    // for `set_var`. TestDb serialises DB tests with a process-wide mutex.
    unsafe {
        std::env::set_var("DATABASE_HOST", host);
        std::env::set_var("DB_USERNAME", user);
        std::env::set_var("DB_PW", password);
        std::env::set_var("DB_DB", database);
    }
}

fn row(stock: &str, offset_seconds: i64, close: f64) -> HistoricalStockDataFullKeys {
    HistoricalStockDataFullKeys {
        stock: stock.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        time: Utc::now() + TimeDelta::seconds(offset_seconds),
        open: 100.0,
        high: 110.0,
        low: 90.0,
        close,
        volume: Decimal::new(1_000, 0),
    }
}

async fn stored_rows(
    pool: &sqlx::PgPool,
) -> Vec<HistoricalStockDataFullKeys> {
    let mut db = DbDirect::connect(pool);
    db.historical_stock_data()
        .read_all()
        .await
        .expect("read historical rows")
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn a_row_is_not_visible_until_flush() {
    let db = TestDb::setup().await;
    configure_worker_from_test_url();

    let creator = init_channel::<HistoricalStockDataFullKeys>().await;
    creator
        .batch_create_or_update(&row("BATCH", 0, 101.0))
        .await
        .expect("queued");

    // The worker waits for BATCH_SIZE rows or one second of idleness.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(stored_rows(&db.pool).await.is_empty());

    creator.flush().await.expect("flush");
    let rows = stored_rows(&db.pool).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].stock, "BATCH");
    assert_eq!(rows[0].close, 101.0);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn many_rows_are_written_in_one_flush() {
    let db = TestDb::setup().await;
    configure_worker_from_test_url();

    let creator = init_channel::<HistoricalStockDataFullKeys>().await;
    for i in 0..500 {
        creator
            .batch_create_or_update(&row("BATCH", i, 100.0 + i as f64))
            .await
        .expect("queued");
    }
    creator.flush().await.expect("flush");

    assert_eq!(stored_rows(&db.pool).await.len(), 500);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn an_existing_primary_key_is_upserted() {
    let db = TestDb::setup().await;
    configure_worker_from_test_url();

    let time = Utc::now();
    let mut fk = row("BATCH", 0, 100.0);
    fk.time = time;
    {
        let mut db_conn = DbDirect::connect(&db.pool);
        db_conn
            .historical_stock_data()
            .create(&fk)
            .await
            .expect("seed row");
    }

    let mut updated = row("BATCH", 0, 123.0);
    updated.time = time;
    let creator = init_channel::<HistoricalStockDataFullKeys>().await;
    creator.batch_create_or_update(&updated).await
        .expect("queued");
    creator.flush().await.expect("flush");

    let rows = stored_rows(&db.pool).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].close, 123.0);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn flushing_an_empty_channel_is_a_noop() {
    let db = TestDb::setup().await;
    configure_worker_from_test_url();

    let creator = init_channel::<HistoricalStockDataFullKeys>().await;
    creator.flush().await.expect("flush");
    assert!(stored_rows(&db.pool).await.is_empty());
}
