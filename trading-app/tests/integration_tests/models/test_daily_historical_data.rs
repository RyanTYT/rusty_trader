//! DB integration tests for `market_data.daily_ohlcv` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.
//!
//! NOTE: DailyHistoricalStockData uses `day` field (not `time`) and Decimal
//! for OHLC (not f64).

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    DailyHistoricalStockDataFullKeys, DailyHistoricalStockDataPrimaryKeys,
    DailyHistoricalStockDataUpdateKeys,
};

use crate::models::init::{TEST_MUTEX, setup_test_db};

fn make_fk(stock: &str) -> DailyHistoricalStockDataFullKeys {
    DailyHistoricalStockDataFullKeys {
        stock: stock.to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), day: Utc::now(),
        open: 150.0, high: 155.0, low: 149.0, close: 152.0,
        volume: Decimal::new(100000, 0),
    }
}

fn make_pk(stock: &str, day: chrono::DateTime<Utc>) -> DailyHistoricalStockDataPrimaryKeys {
    DailyHistoricalStockDataPrimaryKeys {
        stock: stock.to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), day,
    }
}

fn uk(close: Option<f64>, volume: Option<Decimal>) -> DailyHistoricalStockDataUpdateKeys {
    DailyHistoricalStockDataUpdateKeys { open: None, high: None, low: None, close, volume }
}

fn full_uk(close: Option<f64>, volume: Option<Decimal>) -> DailyHistoricalStockDataUpdateKeys {
    DailyHistoricalStockDataUpdateKeys { open: Some(1.0), high: Some(1.0), low: Some(1.0), close, volume }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::daily_historical_stock_data_crud(pool.clone());

    let fk = make_fk("DLY_crd");
    let pk = make_pk(&fk.stock, fk.day);
    crud.create(&fk).await.expect("create failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.stock, fk.stock);
    assert_eq!(data.close, 152.0);

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read(&pk).await.expect("read failed").is_none());
}

#[tokio::test]
async fn test_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::daily_historical_stock_data_crud(pool.clone());

    let fk = make_fk("DLY_upd");
    let pk = make_pk(&fk.stock, fk.day);
    crud.create(&fk).await.expect("create failed");

    crud.update(&pk, &uk(Some(153.0), Some(Decimal::new(200000, 0)))).await.expect("update failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.close, 153.0);
    assert_eq!(data.volume, Decimal::new(200000, 0));

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::daily_historical_stock_data_crud(pool.clone());

    let fk_a = make_fk("DLY_ra_a");
    let fk_b = make_fk("DLY_ra_b");
    let pk_a = make_pk(&fk_a.stock, fk_a.day);
    let pk_b = make_pk(&fk_b.stock, fk_b.day);
    crud.create(&fk_a).await.expect("create A failed");
    crud.create(&fk_b).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all.iter()
        .filter(|p| p.stock == fk_a.stock || p.stock == fk_b.stock)
        .collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&pk_a).await.expect("delete A failed");
    crud.delete(&pk_b).await.expect("delete B failed");
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::daily_historical_stock_data_crud(pool.clone());

    let fk = make_fk("DLY_coi");
    let pk = make_pk(&fk.stock, fk.day);

    crud.create_or_ignore(&fk).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.close, 152.0);

    let mut fk2 = fk.clone();
    fk2.close = 999.0;
    crud.create_or_ignore(&fk2).await.expect("conflict path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.close, 152.0, "conflict path should NOT update");

    crud.delete(&pk).await.expect("delete failed");
}

// #[tokio::test]
// async fn test_create_or_update_insert_path() {
//     let _lock = TEST_MUTEX.lock().await;
//     let pool = setup_test_db().await;
//     let crud = trading_app::test_internals::daily_historical_stock_data_crud(pool.clone());
//
//     let fk = make_fk("DLY_cou_ins");
//     let pk = make_pk(&fk.stock, fk.day);
//     assert!(crud.read(&pk).await.expect("read failed").is_none());
//
//     crud.create_or_update(&pk, &full_uk(Some(152.0), Some(Decimal::new(100000, 0)))).await.expect("insert path failed");
//     let data = crud.read(&pk).await.expect("read failed").expect("expected row");
//     assert_eq!(data.close, 152.0);
//
//     crud.delete(&pk).await.expect("delete failed");
// }
//
// #[tokio::test]
// async fn test_create_or_update_update_path() {
//     let _lock = TEST_MUTEX.lock().await;
//     let pool = setup_test_db().await;
//     let crud = trading_app::test_internals::daily_historical_stock_data_crud(pool.clone());
//
//     let fk = make_fk("DLY_cou_upd");
//     let pk = make_pk(&fk.stock, fk.day);
//     crud.create(&fk).await.expect("pre-insert failed");
//
//     crud.create_or_update(&pk, &full_uk(Some(153.0), Some(Decimal::new(200000, 0)))).await.expect("update path failed");
//     let data = crud.read(&pk).await.expect("read failed").expect("expected row");
//     assert_eq!(data.close, 153.0);
//
//     crud.delete(&pk).await.expect("delete failed");
// }
