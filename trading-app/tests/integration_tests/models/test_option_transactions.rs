//! DB integration tests for `trading.option_transactions` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys, OptionTransactionsUpdateKeys,
    OptionType,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

const STRATEGY: &str = "noise";
const EXPIRY: &str = "20250119";
const STRIKE: f64 = 150.0;
const MULTIPLIER: &str = "100";

fn make_fk(exec_id: &str, qty: f64, price: f64) -> OptionTransactionsFullKeys {
    OptionTransactionsFullKeys {
        execution_id: exec_id.to_string(), strategy: STRATEGY.to_string(),
        stock: format!("OTX_{}", exec_id), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), expiry: EXPIRY.to_string(),
        strike: STRIKE, multiplier: MULTIPLIER.to_string(), option_type: OptionType::Call,
        order_perm_id: 12345, time: Utc::now(), price, quantity: qty,
        fees: Decimal::new(25, 2),
    }
}

fn make_pk(exec_id: &str) -> OptionTransactionsPrimaryKeys {
    OptionTransactionsPrimaryKeys { execution_id: exec_id.to_string() }
}

fn uk(price: Option<f64>, qty: Option<f64>) -> OptionTransactionsUpdateKeys {
    OptionTransactionsUpdateKeys {
        strategy: None, stock: None, primary_exchange: None, currency: None,
        expiry: None, strike: None, multiplier: None, option_type: None,
        order_perm_id: None, time: None, price, quantity: qty, fees: None,
    }
}

fn full_uk(exec_id: &str, price: Option<f64>, qty: Option<f64>) -> OptionTransactionsUpdateKeys {
    OptionTransactionsUpdateKeys {
        strategy: Some(STRATEGY.to_string()), stock: Some(format!("OTX_{}", exec_id)), 
        primary_exchange: Some("NASDAQ".to_string()),
        currency: Some("USD".to_string()), expiry: Some(EXPIRY.to_string()),
        strike: Some(STRIKE), multiplier: Some(MULTIPLIER.to_string()), option_type: Some(OptionType::Call),
        order_perm_id: Some(12345), time: Some(Utc::now()), price, quantity: qty,
        fees: Some(Decimal::new(25, 2)),
    }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::option_transactions_crud(pool.clone());

    crud.create(&make_fk("otx_crd", 5.0, 3.50)).await.expect("create failed");
    let pk = make_pk("otx_crd");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.execution_id, "otx_crd");
    assert_eq!(data.quantity, 5.0);
    assert_eq!(data.strike, STRIKE);

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read(&pk).await.expect("read failed").is_none());
    del_strat!(&pool);
}

#[tokio::test]
async fn test_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::option_transactions_crud(pool.clone());

    crud.create(&make_fk("otx_upd", 5.0, 3.50)).await.expect("create failed");
    let pk = make_pk("otx_upd");
    crud.update(&pk, &uk(Some(4.00), Some(10.0))).await.expect("update failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.price, 4.00);
    assert_eq!(data.quantity, 10.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::option_transactions_crud(pool.clone());

    crud.create(&make_fk("otx_ra_a", 5.0, 3.50)).await.expect("create A failed");
    crud.create(&make_fk("otx_ra_b", 3.0, 4.00)).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all.iter()
        .filter(|p| p.execution_id == "otx_ra_a" || p.execution_id == "otx_ra_b")
        .collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&make_pk("otx_ra_a")).await.expect("delete A failed");
    crud.delete(&make_pk("otx_ra_b")).await.expect("delete B failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::option_transactions_crud(pool.clone());

    let fk = make_fk("otx_coi", 5.0, 3.50);
    let pk = make_pk("otx_coi");

    crud.create_or_ignore(&fk).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 5.0);

    let mut fk2 = fk.clone();
    fk2.quantity = 999.0;
    crud.create_or_ignore(&fk2).await.expect("conflict path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 5.0, "conflict path should NOT update");

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_insert_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::option_transactions_crud(pool.clone());

    let pk = make_pk("otx_cou_ins");
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &full_uk("otx_cou_ins", Some(3.50), Some(5.0))).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.price, 3.50);
    assert_eq!(data.quantity, 5.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::option_transactions_crud(pool.clone());

    let pk = make_pk("otx_cou_upd");
    crud.create(&make_fk("otx_cou_upd", 5.0, 3.50)).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &full_uk("otx_cou_upd", Some(4.00), Some(10.0))).await.expect("update path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.price, 4.00);
    assert_eq!(data.quantity, 10.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
