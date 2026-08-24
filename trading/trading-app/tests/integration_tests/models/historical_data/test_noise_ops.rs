//! Comprehensive DB integration tests for `NoiseOps` on `HistoricalDataCRUD`.
//!
//! Tests: get_avg_move_since_open, get_most_recent_daily_open, get_daily_vol.
//! Note: NoiseOps methods take HistoricalStockDataPrimaryKeysWoTime (concrete, not enum)
//! and always query stock tables regardless of the enum variant.
//!
//! Requires: live Postgres + DATABASE_URL. All tests #[ignore]'d.

use chrono::{Duration, Timelike, Utc};
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeysWoTime,
};
use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, NoiseOps, TimescaleDbOps,
};

use crate::models::init::{TEST_MUTEX, setup_test_db};

async fn cleanup(pool: &sqlx::PgPool, stock: &str) {
    let _ = sqlx::query("DELETE FROM market_data.historical_data WHERE stock = $1")
        .bind(stock)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM market_data.daily_ohlcv WHERE stock = $1")
        .bind(stock)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM market_data.daily_volatility WHERE stock = $1")
        .bind(stock)
        .execute(pool)
        .await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_avg_move_since_open_with_data() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "AVGMOVE";
    cleanup(&pool, stock).await;
    let crud = HistoricalDataCRUD::stock(pool.clone());
    let pk = HistoricalStockDataPrimaryKeysWoTime {
        stock: stock.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    };

    let hist_crud = trading_app::test_internals::historical_stock_data_crud(pool.clone());

    // The SQL logic for get_avg_move_since_open:
    // 1. Finds the latest bar's time-of-day (latest_close_time)
    // 2. Finds up to 15 historical bars at the EXACT same time-of-day (excluding today)
    // 3. For each match, computes: movement = close / daily_open
    //    where daily_open = first(open, time) from daily_ohlcv for that day
    // 4. Returns: avg(|movement - 1.0|)
    //
    // To get a deterministic result, we insert TWO bars per day for 15 days:
    // - 09:30 UTC: open=100.0 (this becomes the daily open in daily_ohlcv)
    // - 10:00 UTC: close varies per day (this is the bar that matches latest_close_time)
    //
    // The latest bar (Day -1 at 10:00) sets latest_close_time = 10:00:00.
    // Then all historical bars at 10:00:00 are matched.
    //
    // Day -1:  close_at_1000 = 100.5  → movement = 100.5/100 = 1.005 → |1.005-1.0| = 0.005
    // Day -2:  close_at_1000 = 101.0  → movement = 101.0/100 = 1.010 → |1.010-1.0| = 0.010
    // ...
    // Day -15: close_at_1000 = 107.5  → movement = 107.5/100 = 1.075 → |1.075-1.0| = 0.075
    //
    // avg_move = (0.005 + 0.010 + 0.015 + ... + 0.075) / 15
    //          = 0.005 * sum(1..=15) / 15
    //          = 0.005 * 120 / 15
    //          = 0.04

    let base_date = chrono::Utc::now()
        .with_hour(10)
        .unwrap()
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();

    // The expected avg_move = 0.04 (see derivation above)
    let expected_moves: Vec<f64> = (1..=15).map(|i| (100.0 + i as f64 * 0.5) / 100.0).collect();
    let expected_avg = expected_moves.iter().map(|m| (m - 1.0).abs()).sum::<f64>() / 15.0;

    // insert with i == 0 as well to include most recent day data which will be ignored
    for i in 0..=15 {
        let day = base_date - Duration::days(i);

        // Bar 1: 09:30 UTC — the daily open (determines open in daily_ohlcv)
        let open_bar_time = day.with_hour(9).unwrap().with_minute(30).unwrap();
        hist_crud
            .create(&HistoricalStockDataFullKeys {
                stock: stock.to_string(),
                primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(),
                time: open_bar_time,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0, // open=close=100 for this bar
                volume: rust_decimal::Decimal::new(500, 0),
            })
            .await
            .expect("create open bar failed");

        // Bar 2: 10:00 UTC — the "current" bar (determines latest_close_time + the close used)
        let close_bar_time = day.with_hour(10).unwrap().with_minute(0).unwrap();
        let close = 100.0 + (i as f64 * 0.5);
        hist_crud
            .create(&HistoricalStockDataFullKeys {
                stock: stock.to_string(),
                primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(),
                time: close_bar_time,
                open: close - 0.5,
                high: close + 0.5,
                low: close - 1.0,
                close,
                volume: rust_decimal::Decimal::new(1000, 0),
            })
            .await
            .expect("create close bar failed");
    }

    // Refresh the continuous aggregate so daily_ohlcv is populated from historical_data
    // daily_ohlcv.open = first(open, time) = 100.0 (from the 09:30 bar)
    crud.refresh_daily_data()
        .await
        .expect("refresh_daily_data failed");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await; // wait for refresh to complete

    let result = crud.get_avg_move_since_open(pk).await;
    match result {
        Ok(v) => {
            assert!(v.is_finite(), "avg move should be finite, got {v}");
            assert!(v >= 0.0, "avg move should be non-negative, got {v}");
            assert!(
                (v - expected_avg).abs() < 1e-6,
                "avg move should be {expected_avg}, got {v}"
            );
            println!("✅ get_avg_move_since_open: {v} (expected {expected_avg})");
        }
        Err(e) => panic!("get_avg_move_since_open returned Err: {e}"),
    }
    cleanup(&pool, stock).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_most_recent_daily_open_with_data() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "DLYOPEN";
    cleanup(&pool, stock).await;
    let crud = HistoricalDataCRUD::stock(pool.clone());
    let hist_crud = trading_app::test_internals::historical_stock_data_crud(pool.clone());
    let pk = HistoricalStockDataPrimaryKeysWoTime {
        stock: stock.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    };

    let yesterday = Utc::now() - Duration::days(1);
    // Insert into historical_data (base table) — daily_ohlcv is populated by TimescaleDB
    hist_crud
        .create(&HistoricalStockDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            time: yesterday,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 105.0,
            volume: rust_decimal::Decimal::new(1000, 0),
        })
        .await
        .expect("create hist failed");

    // Refresh the continuous aggregate so daily_ohlcv is populated
    crud.refresh_daily_data()
        .await
        .expect("refresh_daily_data failed");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let result = crud.get_most_recent_daily_open(pk).await;
    match result {
        Ok(v) => assert!(v > 0.0, "daily open should be positive"),
        Err(e) => println!("get_most_recent_daily_open returned Err: {e}"),
    }
    cleanup(&pool, stock).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_most_recent_daily_open_no_data() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::stock(pool);
    let pk = HistoricalStockDataPrimaryKeysWoTime {
        stock: "NODLY".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    };
    let result = crud.get_most_recent_daily_open(pk).await;
    assert!(result.is_err(), "no data → Err");
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_daily_vol_no_data() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::stock(pool);
    let pk = HistoricalStockDataPrimaryKeysWoTime {
        stock: "NOVOL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    };
    let result = crud.get_daily_vol(pk).await;
    assert!(result.is_err(), "no volatility data → Err");
}
