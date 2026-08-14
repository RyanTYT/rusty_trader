//! DB integration tests for `market_data.historical_options_data` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    HistoricalOptionsDataFullKeys, HistoricalOptionsDataPrimaryKeys, HistoricalOptionsDataUpdateKeys,
    OptionType,
};

use crate::models::init::{TEST_MUTEX, setup_test_db};

const EXPIRY: &str = "20250119";
const STRIKE: f64 = 150.0;
const MULTIPLIER: &str = "100";

fn make_fk(stock: &str) -> HistoricalOptionsDataFullKeys {
    HistoricalOptionsDataFullKeys {
        stock: stock.to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), expiry: EXPIRY.to_string(),
        strike: STRIKE, multiplier: MULTIPLIER.to_string(), option_type: OptionType::Call,
        time: Utc::now(), open: 3.50, high: 4.00, low: 3.25, close: 3.75,
        volume: Decimal::new(500, 0),
    }
}

fn make_pk(stock: &str, time: chrono::DateTime<Utc>) -> HistoricalOptionsDataPrimaryKeys {
    HistoricalOptionsDataPrimaryKeys {
        stock: stock.to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), expiry: EXPIRY.to_string(),
        strike: STRIKE, multiplier: MULTIPLIER.to_string(),
        option_type: OptionType::Call, time,
    }
}

fn uk(close: Option<f64>, volume: Option<Decimal>) -> HistoricalOptionsDataUpdateKeys {
    HistoricalOptionsDataUpdateKeys { open: None, high: None, low: None, close, volume }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_options_data_crud(pool.clone());

    let fk = make_fk("HOD_crd");
    let pk = make_pk(&fk.stock, fk.time);
    crud.create(&fk).await.expect("create failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.stock, fk.stock);
    assert_eq!(data.close, 3.75);

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read(&pk).await.expect("read failed").is_none());
}

#[tokio::test]
async fn test_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_options_data_crud(pool.clone());

    let fk = make_fk("HOD_upd");
    let pk = make_pk(&fk.stock, fk.time);
    crud.create(&fk).await.expect("create failed");

    crud.update(&pk, &uk(Some(4.00), Some(Decimal::new(600, 0)))).await.expect("update failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.close, 4.00);

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_options_data_crud(pool.clone());

    let fk_a = make_fk("HOD_ra_a");
    let fk_b = make_fk("HOD_ra_b");
    let pk_a = make_pk(&fk_a.stock, fk_a.time);
    let pk_b = make_pk(&fk_b.stock, fk_b.time);
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
    let crud = trading_app::test_internals::historical_options_data_crud(pool.clone());

    let fk = make_fk("HOD_coi");
    let pk = make_pk(&fk.stock, fk.time);

    crud.create_or_ignore(&fk).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.close, 3.75);

    let mut fk2 = fk.clone();
    fk2.close = 999.0;
    crud.create_or_ignore(&fk2).await.expect("conflict path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.close, 3.75, "conflict path should NOT update");

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_create_or_update_insert_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_options_data_crud(pool.clone());

    let fk = make_fk("HOD_cou_ins");
    let pk = make_pk(&fk.stock, fk.time);
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &uk(Some(3.75), Some(Decimal::new(500, 0)))).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.close, 3.75);

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_options_data_crud(pool.clone());

    let fk = make_fk("HOD_cou_upd");
    let pk = make_pk(&fk.stock, fk.time);
    crud.create(&fk).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &uk(Some(4.00), Some(Decimal::new(600, 0)))).await.expect("update path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.close, 4.00);

    crud.delete(&pk).await.expect("delete failed");
}
