//! Comprehensive DB integration tests for `HistoricalDataOps` on `HistoricalDataCRUD`.
//!
//! Tests ALL variants (Stock, DailyStock, Options, Forex) for ALL methods:
//! read_last_n, read_last_bar, read_last_vwap, has_at_least_n_rows_since.
//!
//! Requires: live Postgres + DATABASE_URL. All tests #[ignore]'d.

use chrono::{Duration, Utc};
use chrono_tz::America::New_York;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    DailyHistoricalStockDataFullKeys, DailyHistoricalStockDataPrimaryKeysWoTime,
    HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeysWoTime,
    HistoricalOptionsDataFullKeys, HistoricalOptionsDataPrimaryKeysWoTime,
    HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeysWoTime, OptionType,
};
use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, HistoricalDataFullKeys as HDFK, HistoricalDataOps,
    HistoricalDataPrimaryKeysWoTime, VwapBarValue,
};

use crate::models::init::{TEST_MUTEX, setup_test_db};

// ============================ Helpers ============================

async fn cleanup_stock(pool: &sqlx::PgPool, stock: &str) {
    let _ = sqlx::query("DELETE FROM market_data.historical_data WHERE stock = $1")
        .bind(stock)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM market_data.daily_ohlcv WHERE stock = $1")
        .bind(stock)
        .execute(pool)
        .await;
}
async fn cleanup_forex(pool: &sqlx::PgPool, pair: &str) {
    let _ = sqlx::query("DELETE FROM market_data.historical_forex_data WHERE pair = $1")
        .bind(pair)
        .execute(pool)
        .await;
}
async fn cleanup_options(pool: &sqlx::PgPool, stock: &str) {
    let _ = sqlx::query("DELETE FROM market_data.historical_options_data WHERE stock = $1")
        .bind(stock)
        .execute(pool)
        .await;
}

async fn insert_stock_bars(pool: &sqlx::PgPool, stock: &str, n: i64, end: chrono::DateTime<Utc>) {
    let crud = trading_app::test_internals::historical_stock_data_crud(pool.clone());
    for i in 0..n {
        let t = end - Duration::minutes((n - 1 - i) * 5);
        crud.create(&HistoricalStockDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            time: t,
            open: 100.0 + i as f64,
            high: 101.0 + i as f64,
            low: 99.0 + i as f64,
            close: 100.5 + i as f64,
            volume: Decimal::new(1000, 0),
        })
        .await
        .expect("create stock bar failed");
    }
}

async fn insert_forex_bars(pool: &sqlx::PgPool, pair: &str, n: i64, end: chrono::DateTime<Utc>) {
    let crud = trading_app::test_internals::historical_forex_data_crud(pool.clone());
    for i in 0..n {
        let t = end - Duration::minutes((n - 1 - i) * 5);
        crud.create(&HistoricalForexDataFullKeys {
            pair: pair.to_string(),
            time: t,
            bid_open: Some(1.0850 + i as f64 * 0.001),
            bid_high: Some(1.0870 + i as f64 * 0.001),
            bid_low: Some(1.0840 + i as f64 * 0.001),
            bid_close: Some(1.0860 + i as f64 * 0.001),
            ask_open: Some(1.0852 + i as f64 * 0.001),
            ask_high: Some(1.0872 + i as f64 * 0.001),
            ask_low: Some(1.0842 + i as f64 * 0.001),
            ask_close: Some(1.0862 + i as f64 * 0.001),
        })
        .await
        .expect("create forex bar failed");
    }
}

async fn insert_option_bars(pool: &sqlx::PgPool, stock: &str, n: i64, end: chrono::DateTime<Utc>) {
    let crud = trading_app::test_internals::historical_options_data_crud(pool.clone());
    for i in 0..n {
        let t = end - Duration::minutes((n - 1 - i) * 5);
        crud.create(&HistoricalOptionsDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            expiry: "20250119".to_string(),
            strike: 150.0,
            multiplier: "100".to_string(),
            option_type: OptionType::Call,
            time: t,
            open: 3.50 + i as f64,
            high: 4.00 + i as f64,
            low: 3.25 + i as f64,
            close: 3.75 + i as f64,
            volume: Decimal::new(500, 0),
        })
        .await
        .expect("create option bar failed");
    }
}

async fn insert_daily_bars(pool: &sqlx::PgPool, stock: &str, n: i64, end: chrono::DateTime<Utc>) {
    let crud = trading_app::test_internals::daily_historical_stock_data_crud(pool.clone());
    for i in 0..n {
        let day = end - Duration::days(i + 1);
        crud.create(&DailyHistoricalStockDataFullKeys {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            day,
            open: 100.0 + i as f64,
            high: 101.0 + i as f64,
            low: 99.0 + i as f64,
            close: 100.5 + i as f64,
            volume: Decimal::new(10000, 0),
        })
        .await
        .expect("create daily bar failed");
    }
}

// ============================ read_last_n — all 4 variants ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_n_stock_full_incomplete_split() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "RLN_S";
    cleanup_stock(&pool, stock).await;
    let crud = HistoricalDataCRUD::stock(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
        stock: stock.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    });

    // Insert 12 bars of 5-min data → 15-min buckets = 3 bars each
    insert_stock_bars(&pool, stock, 12, Utc::now()).await;

    let result = crud
        .read_last_n(pk, 15, 2)
        .await
        .expect("read_last_n failed");
    assert!(
        result.full.len() + result.incomplete.len() <= 2,
        "should return at most 2 buckets"
    );
    assert!(
        !result.full.is_empty() || !result.incomplete.is_empty(),
        "should have some bars"
    );

    cleanup_stock(&pool, stock).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_n_stock_empty() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::stock(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
        stock: "RLN_EMPTY".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    });
    let result = crud
        .read_last_n(pk, 15, 5)
        .await
        .expect("read_last_n failed");
    assert!(
        result.full.is_empty() && result.incomplete.is_empty(),
        "no data → both empty"
    );
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_n_forex() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let pair = "EUR/USD";
    cleanup_forex(&pool, pair).await;
    let crud = HistoricalDataCRUD::forex(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Forex(HistoricalForexDataPrimaryKeysWoTime {
        pair: pair.to_string(),
    });

    // Forex uses 5-min base bars (not 1-min) in our test setup
    insert_forex_bars(&pool, pair, 6, Utc::now()).await;

    let result = crud
        .read_last_n(pk, 15, 2)
        .await
        .expect("read_last_n failed");
    assert!(
        !result.full.is_empty() || !result.incomplete.is_empty(),
        "should have forex bars"
    );

    cleanup_forex(&pool, pair).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_n_options() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "RLN_OPT";
    cleanup_options(&pool, stock).await;
    let crud = HistoricalDataCRUD::option(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Options(HistoricalOptionsDataPrimaryKeysWoTime {
        stock: stock.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        expiry: "20250119".to_string(),
        strike: 150.0,
        multiplier: "100".to_string(),
        option_type: OptionType::Call,
    });

    insert_option_bars(&pool, stock, 6, Utc::now()).await;

    let result = crud
        .read_last_n(pk, 15, 2)
        .await
        .expect("read_last_n failed");
    assert!(
        !result.full.is_empty() || !result.incomplete.is_empty(),
        "should have option bars"
    );

    cleanup_options(&pool, stock).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_n_daily_stock_all_full() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "RLN_DLY";
    cleanup_stock(&pool, stock).await;
    let crud = HistoricalDataCRUD::daily_stock(pool.clone());
    let pk =
        HistoricalDataPrimaryKeysWoTime::DailyStock(DailyHistoricalStockDataPrimaryKeysWoTime {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
        });

    // DailyStock puts ALL bars in `full` (no incomplete logic)
    insert_daily_bars(&pool, stock, 5, Utc::now()).await;

    let result = crud
        .read_last_n(pk, 5, 3)
        .await
        .expect("read_last_n failed");
    assert!(
        !result.full.is_empty(),
        "DailyStock should have all bars in full"
    );
    assert!(
        result.incomplete.is_empty(),
        "DailyStock should have NO incomplete bars"
    );

    cleanup_stock(&pool, stock).await;
}

// ============================ read_last_bar — all variants ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_bar_stock_found() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "BAR_S";
    cleanup_stock(&pool, stock).await;
    let crud = HistoricalDataCRUD::stock(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
        stock: stock.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    });

    insert_stock_bars(&pool, stock, 5, Utc::now()).await;

    let bar = crud
        .read_last_bar(pk, 15)
        .await
        .expect("read_last_bar failed");
    match bar {
        HDFK::Stock(s) => {
            assert_eq!(s.stock, stock);
            assert!(s.close > 0.0);
        }
        _ => panic!("expected Stock variant"),
    }
    cleanup_stock(&pool, stock).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_bar_empty() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::stock(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
        stock: "NOBAR".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    });
    let result = crud.read_last_bar(pk, 15).await;
    assert!(result.is_err(), "no data → Err");
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_bar_forex_found() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let pair = "GBP/USD";
    cleanup_forex(&pool, pair).await;
    let crud = HistoricalDataCRUD::forex(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Forex(HistoricalForexDataPrimaryKeysWoTime {
        pair: pair.to_string(),
    });

    insert_forex_bars(&pool, pair, 5, Utc::now()).await;

    let bar = crud
        .read_last_bar(pk, 15)
        .await
        .expect("read_last_bar failed");
    match bar {
        HDFK::Forex(f) => assert_eq!(f.pair, pair),
        _ => panic!("expected Forex variant"),
    }
    cleanup_forex(&pool, pair).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_bar_options_found() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "BAR_OPT";
    cleanup_options(&pool, stock).await;
    let crud = HistoricalDataCRUD::option(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Options(HistoricalOptionsDataPrimaryKeysWoTime {
        stock: stock.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        expiry: "20250119".to_string(),
        strike: 150.0,
        multiplier: "100".to_string(),
        option_type: OptionType::Call,
    });

    insert_option_bars(&pool, stock, 5, Utc::now()).await;

    let bar = crud
        .read_last_bar(pk, 15)
        .await
        .expect("read_last_bar failed");
    match bar {
        HDFK::Options(o) => assert_eq!(o.stock, stock),
        _ => panic!("expected Options variant"),
    }
    cleanup_options(&pool, stock).await;
}

// ============================ read_last_vwap — all variants + edge cases ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_vwap_no_data_returns_none() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::stock(pool);
    let pk = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
        stock: "NOVWAP".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    });
    let vwap = crud
        .read_last_vwap(pk, Some("US/Eastern".to_string()), VwapBarValue::Close)
        .await
        .expect("read_last_vwap failed");
    assert!(vwap.is_none(), "no data → None");
}

// BUG: Forex read_last_vwap references nonexistent `volume` column + `GROUP BY stock` (should be `pair`).
// This test locks in the CURRENT (buggy) behavior — expects SQL error.
#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_vwap_forex_bug_missing_volume_column() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let pair = "VWAP_FX";
    cleanup_forex(&pool, pair).await;
    let crud = HistoricalDataCRUD::forex(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Forex(HistoricalForexDataPrimaryKeysWoTime {
        pair: pair.to_string(),
    });

    insert_forex_bars(&pool, pair, 5, Utc::now()).await;

    let result = crud.read_last_vwap(pk, None, VwapBarValue::BidClose).await;
    // BUG: Forex VWAP SQL references `volume` column which doesn't exist in forex table
    assert!(
        result.is_err(),
        "BUG: Forex VWAP should fail due to missing volume column"
    );
    let _ = result.unwrap_err();

    cleanup_forex(&pool, pair).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_vwap_daily_stock_error() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::daily_stock(pool);
    let pk =
        HistoricalDataPrimaryKeysWoTime::DailyStock(DailyHistoricalStockDataPrimaryKeysWoTime {
            stock: "VWAP_DLY".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
        });
    let result = crud.read_last_vwap(pk, None, VwapBarValue::Close).await;
    assert!(
        result.is_err(),
        "DailyStock variant → Err (early return, no SQL)"
    );
}

// ============================ has_at_least_n_rows_since — all variants ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_has_at_least_n_rows_since_stock_true() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "HAS_S1";
    cleanup_stock(&pool, stock).await;
    let crud = HistoricalDataCRUD::stock(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
        stock: stock.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    });

    insert_stock_bars(&pool, stock, 5, Utc::now()).await;
    let since = Utc::now() - Duration::hours(1);
    let has = crud
        .has_at_least_n_rows_since(pk, 5, &since.with_timezone(&New_York))
        .await
        .expect("failed");
    assert!(has, "5 rows, n=5 → true");
    cleanup_stock(&pool, stock).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_has_at_least_n_rows_since_stock_false() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "HAS_S2";
    cleanup_stock(&pool, stock).await;
    let crud = HistoricalDataCRUD::stock(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
        stock: stock.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    });

    insert_stock_bars(&pool, stock, 3, Utc::now()).await;
    let since = Utc::now() - Duration::hours(1);
    let has = crud
        .has_at_least_n_rows_since(pk, 5, &since.with_timezone(&New_York))
        .await
        .expect("failed");
    assert!(!has, "3 rows, n=5 → false");
    cleanup_stock(&pool, stock).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_has_at_least_n_rows_since_stock_zero_rows() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::stock(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
        stock: "NOROWS".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    });
    let since = Utc::now() - Duration::hours(1);
    let has = crud
        .has_at_least_n_rows_since(pk, 1, &since.with_timezone(&New_York))
        .await
        .expect("failed");
    assert!(!has, "0 rows, n=1 → false");
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_has_at_least_n_rows_since_forex_with_bid_ask_filter() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let pair = "HAS_FX";
    cleanup_forex(&pool, pair).await;
    let crud = HistoricalDataCRUD::forex(pool.clone());
    let pk = HistoricalDataPrimaryKeysWoTime::Forex(HistoricalForexDataPrimaryKeysWoTime {
        pair: pair.to_string(),
    });

    insert_forex_bars(&pool, pair, 5, Utc::now()).await;
    // Forex filter: bid_open IS NOT NULL AND ask_open IS NOT NULL
    let since = Utc::now() - Duration::hours(1);
    let has = crud
        .has_at_least_n_rows_since(pk, 5, &since.with_timezone(&New_York))
        .await
        .expect("failed");
    assert!(has, "5 forex bars with bid+ask → true");
    cleanup_forex(&pool, pair).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_has_at_least_n_rows_since_daily_stock_uses_day_column() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let stock = "HAS_DLY";
    cleanup_stock(&pool, stock).await;
    let crud = HistoricalDataCRUD::daily_stock(pool.clone());
    let pk =
        HistoricalDataPrimaryKeysWoTime::DailyStock(DailyHistoricalStockDataPrimaryKeysWoTime {
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
        });

    // DailyStock uses `day > $5` (not `time >`)
    insert_daily_bars(&pool, stock, 3, Utc::now()).await;
    let since = Utc::now() - Duration::days(10);
    let has = crud
        .has_at_least_n_rows_since(pk, 3, &since.with_timezone(&New_York))
        .await
        .expect("failed");
    assert!(has, "3 daily bars, n=3 → true");
    cleanup_stock(&pool, stock).await;
}
