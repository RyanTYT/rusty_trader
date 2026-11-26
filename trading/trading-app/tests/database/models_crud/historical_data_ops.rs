//! Ops tests for `HistoricalDataCRUD`
//! (`src/database/models_crud/historical_data/historical_data.rs`).
//!
//! Happy paths only: bucket aggregation, the latest bar, VWAP and the
//! `get_rows_since` window. Each test talks to a live database.

use chrono::{DateTime, Duration, Timelike, Utc};
use rust_decimal::Decimal;

use crate::common::{SessionKind, TestDb, TestSession, tag};
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    AssetType, HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeysWoTime,
};
use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataFullKeys, HistoricalDataOps, HistoricalDataPrimaryKeysWoTime,
    VwapBarValue,
};
use trading_app::database::tables::DbTables;

/// Minute-aligned time ten minutes in the past, so every bucket is complete.
fn base_time() -> DateTime<Utc> {
    Utc::now()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
        - Duration::minutes(10)
}

fn stock_bar(
    tag: &str,
    stock: &str,
    time: DateTime<Utc>,
    close: f64,
) -> HistoricalStockDataFullKeys {
    let mut row = super::historical_stock_data::fk(tag);
    row.stock = stock.to_string();
    row.time = time;
    row.close = close;
    row.open = close - 1.0;
    row.high = close + 1.0;
    row.low = close - 2.0;
    row.volume = Decimal::new(1000, 0);
    row
}

fn stock_pk(row: &HistoricalStockDataFullKeys) -> HistoricalDataPrimaryKeysWoTime {
    HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
        stock: row.stock.clone(),
        primary_exchange: row.primary_exchange.clone(),
        currency: row.currency.clone(),
    })
}

fn close_of(bar: &HistoricalDataFullKeys) -> f64 {
    match bar {
        HistoricalDataFullKeys::Stock(row) => row.close,
        _ => panic!("expected a stock bar"),
    }
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn read_last_n_returns_latest_full_buckets_ascending() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let stock = tag("hd_n");
    let t0 = base_time();

    let rows = [
        stock_bar(&tag("hd_n0"), &stock, t0, 100.0),
        stock_bar(&tag("hd_n1"), &stock, t0 + Duration::minutes(1), 110.0),
        stock_bar(&tag("hd_n2"), &stock, t0 + Duration::minutes(2), 120.0),
    ];
    for row in &rows {
        s.historical_stock_data().create(row).await.unwrap();
    }

    let agg = s
        .historical_data(&AssetType::Stock)
        .read_last_n(stock_pk(&rows[0]), 1, 2)
        .await
        .unwrap();

    assert!(agg.incomplete.is_empty(), "all three buckets are complete");
    assert_eq!(agg.full.len(), 2);
    let closes: Vec<f64> = agg.full.iter().map(close_of).collect();
    assert_eq!(closes, vec![110.0, 120.0]);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn read_last_bar_errors_when_empty_and_returns_latest() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let stock = tag("hd_bar");
    let row = stock_bar(&tag("hd_bar0"), &stock, base_time(), 120.0);
    let pk = stock_pk(&row);

    assert!(
        s.historical_data(&AssetType::Stock)
            .read_last_bar(pk.clone(), 1)
            .await
            .is_err(),
        "no bars means no last bar"
    );

    s.historical_stock_data().create(&row).await.unwrap();

    let last = s
        .historical_data(&AssetType::Stock)
        .read_last_bar(pk, 1)
        .await
        .unwrap();
    assert_eq!(close_of(&last), 120.0);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn read_last_vwap_returns_none_then_the_close() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let stock = tag("hd_vwap");
    let row = stock_bar(&tag("hd_vwap0"), &stock, base_time(), 120.0);
    let pk = stock_pk(&row);

    assert!(
        s.historical_data(&AssetType::Stock)
            .read_last_vwap(pk.clone(), None, VwapBarValue::Close)
            .await
            .unwrap()
            .is_none(),
        "no bars means no VWAP"
    );

    s.historical_stock_data().create(&row).await.unwrap();

    let vwap = s
        .historical_data(&AssetType::Stock)
        .read_last_vwap(pk, None, VwapBarValue::Close)
        .await
        .unwrap()
        .expect("one bar means a VWAP");
    assert!((vwap - 120.0).abs() < 1e-9, "single bar VWAP is its close");
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn get_rows_since_counts_only_the_last_n_window() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let stock = tag("hd_since");
    let t0 = base_time();

    let rows = [
        stock_bar(&tag("hd_since0"), &stock, t0, 100.0),
        stock_bar(&tag("hd_since1"), &stock, t0 + Duration::minutes(1), 110.0),
        stock_bar(&tag("hd_since2"), &stock, t0 + Duration::minutes(2), 120.0),
    ];
    for row in &rows {
        s.historical_stock_data().create(row).await.unwrap();
    }

    let pk = stock_pk(&rows[0]);
    let since_one = (t0 + Duration::minutes(1)).with_timezone(&chrono_tz::Tz::America__New_York);
    let since_two = (t0 + Duration::minutes(2)).with_timezone(&chrono_tz::Tz::America__New_York);

    let last_two = s
        .historical_data(&AssetType::Stock)
        .get_rows_since(pk.clone(), 2, &since_one)
        .await
        .unwrap();
    assert_eq!(last_two, 2);

    let last_three_from_last = s
        .historical_data(&AssetType::Stock)
        .get_rows_since(pk, 3, &since_two)
        .await
        .unwrap();
    assert_eq!(last_three_from_last, 1);
}
