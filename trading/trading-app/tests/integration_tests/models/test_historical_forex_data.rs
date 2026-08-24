//! DB integration tests for `market_data.historical_forex_data` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeys, HistoricalForexDataUpdateKeys,
};

use crate::models::init::{TEST_MUTEX, setup_test_db};

const _PAIR: &str = "EUR/USD";

fn make_fk(pair: &str) -> HistoricalForexDataFullKeys {
    HistoricalForexDataFullKeys {
        pair: pair.to_string(), time: Utc::now(),
        bid_open: Some(1.0850), bid_high: Some(1.0870), bid_low: Some(1.0840), bid_close: Some(1.0860),
        ask_open: Some(1.0852), ask_high: Some(1.0872), ask_low: Some(1.0842), ask_close: Some(1.0862),
    }
}

fn make_pk(pair: &str, time: chrono::DateTime<Utc>) -> HistoricalForexDataPrimaryKeys {
    HistoricalForexDataPrimaryKeys { pair: pair.to_string(), time }
}

fn uk(bid_close: Option<f64>, ask_close: Option<f64>) -> HistoricalForexDataUpdateKeys {
    HistoricalForexDataUpdateKeys {
        bid_open: None, bid_high: None, bid_low: None, bid_close,
        ask_open: None, ask_high: None, ask_low: None, ask_close,
    }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_forex_data_crud(pool.clone());

    let fk = make_fk("EUR/USD_crd");
    let pk = make_pk(&fk.pair, fk.time);
    crud.create(&fk).await.expect("create failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.pair, fk.pair);
    assert_eq!(data.bid_close, Some(1.0860));

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read(&pk).await.expect("read failed").is_none());
}

#[tokio::test]
async fn test_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_forex_data_crud(pool.clone());

    let fk = make_fk("EUR/USD_upd");
    let pk = make_pk(&fk.pair, fk.time);
    crud.create(&fk).await.expect("create failed");

    crud.update(&pk, &uk(Some(1.0870), Some(1.0872))).await.expect("update failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.bid_close, Some(1.0870));
    assert_eq!(data.ask_close, Some(1.0872));

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_forex_data_crud(pool.clone());

    let fk_a = make_fk("EUR/USD_ra_a");
    let fk_b = make_fk("EUR/USD_ra_b");
    let pk_a = make_pk(&fk_a.pair, fk_a.time);
    let pk_b = make_pk(&fk_b.pair, fk_b.time);
    crud.create(&fk_a).await.expect("create A failed");
    crud.create(&fk_b).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all.iter()
        .filter(|p| p.pair == fk_a.pair || p.pair == fk_b.pair)
        .collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&pk_a).await.expect("delete A failed");
    crud.delete(&pk_b).await.expect("delete B failed");
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_forex_data_crud(pool.clone());

    let fk = make_fk("EUR/USD_coi");
    let pk = make_pk(&fk.pair, fk.time);

    crud.create_or_ignore(&fk).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.bid_close, Some(1.0860));

    let mut fk2 = fk.clone();
    fk2.bid_close = Some(999.0);
    crud.create_or_ignore(&fk2).await.expect("conflict path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.bid_close, Some(1.0860), "conflict path should NOT update");

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_create_or_update_insert_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_forex_data_crud(pool.clone());

    let fk = make_fk("EUR/USD_cou_ins");
    let pk = make_pk(&fk.pair, fk.time);
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &uk(Some(1.0860), Some(1.0862))).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.bid_close, Some(1.0860));

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_forex_data_crud(pool.clone());

    let fk = make_fk("EUR/USD_cou_upd");
    let pk = make_pk(&fk.pair, fk.time);
    crud.create(&fk).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &uk(Some(1.0870), Some(1.0872))).await.expect("update path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.bid_close, Some(1.0870));
    assert_eq!(data.ask_close, Some(1.0872));

    crud.delete(&pk).await.expect("delete failed");
}
