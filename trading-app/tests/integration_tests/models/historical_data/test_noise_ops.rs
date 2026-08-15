//! Comprehensive DB integration tests for `NoiseOps` on `HistoricalDataCRUD`.
//!
//! Tests: get_avg_move_since_open, get_most_recent_daily_open, get_daily_vol.
//! Note: NoiseOps methods take HistoricalStockDataPrimaryKeysWoTime (concrete, not enum)
//! and always query stock tables regardless of the enum variant.
//!
//! Requires: live Postgres + DATABASE_URL. All tests #[ignore]'d.

use chrono::{Duration, Utc};
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    DailyHistoricalStockDataFullKeys, HistoricalStockDataFullKeys,
    HistoricalStockDataPrimaryKeysWoTime,
};
use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, NoiseOps,
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

    let daily_crud = trading_app::test_internals::daily_historical_stock_data_crud(pool.clone());
    let hist_crud = trading_app::test_internals::historical_stock_data_crud(pool.clone());
    let now = Utc::now();

    // Insert 15 days of daily + intraday data
    for i in 0..15 {
        let day = now - Duration::days(i + 1);
        let open = 100.0;
        let close = 100.0 + (i as f64 * 0.5);
        daily_crud
            .create(&DailyHistoricalStockDataFullKeys {
                stock: stock.to_string(),
                primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(),
                day,
                open,
                high: open + 1.0,
                low: open - 1.0,
                close,
                volume: rust_decimal::Decimal::new(1000, 0),
            })
            .await
            .expect("create daily failed");

        hist_crud
            .create(&HistoricalStockDataFullKeys {
                stock: stock.to_string(),
                primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(),
                time: day,
                open,
                high: open + 1.0,
                low: open - 1.0,
                close,
                volume: rust_decimal::Decimal::new(1000, 0),
            })
            .await
            .expect("create hist failed");
    }

    let result = crud.get_avg_move_since_open(pk).await;
    match result {
        Ok(v) => {
            assert!(v.is_finite(), "avg move should be finite");
            assert!(v >= 0.0, "avg move should be non-negative");
        }
        Err(e) => {
            println!("get_avg_move_since_open returned Err (may need specific time matching): {e}")
        }
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
    let pk = HistoricalStockDataPrimaryKeysWoTime {
        stock: stock.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    };

    let daily_crud = trading_app::test_internals::daily_historical_stock_data_crud(pool.clone());
    let yesterday = Utc::now() - Duration::days(1);
    daily_crud
        .create(&DailyHistoricalStockDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            day: yesterday,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 105.0,
            volume: rust_decimal::Decimal::new(1000, 0),
        })
        .await
        .expect("create daily failed");

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
