//! DB integration tests for `trading.stock_transactions` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    StockTransactionsFullKeys, StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

const STRATEGY: &str = "noise";

fn make_fk(exec_id: &str, qty: f64, price: f64) -> StockTransactionsFullKeys {
    StockTransactionsFullKeys {
        execution_id: exec_id.to_string(), strategy: STRATEGY.to_string(),
        stock: format!("STK_{}", exec_id), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), order_perm_id: 12345,
        time: Utc::now(), price, quantity: qty, fees: Decimal::new(50, 2),
    }
}

fn make_pk(exec_id: &str) -> StockTransactionsPrimaryKeys {
    StockTransactionsPrimaryKeys { execution_id: exec_id.to_string() }
}

fn uk(price: Option<f64>, qty: Option<f64>) -> StockTransactionsUpdateKeys {
    StockTransactionsUpdateKeys {
        strategy: None, stock: None, primary_exchange: None, currency: None,
        order_perm_id: None, time: None, price, quantity: qty, fees: None,
    }
}

fn full_uk(exec_id: &str, price: Option<f64>, qty: Option<f64>) -> StockTransactionsUpdateKeys {
    StockTransactionsUpdateKeys {
        strategy: Some(STRATEGY.to_string()),
        stock: Some(format!("STK_{}", exec_id)), primary_exchange: Some("NASDAQ".to_string()),
        currency: Some("USD".to_string()), order_perm_id: Some(12345),
        time: Some(Utc::now()), price, quantity: qty, fees: Some(Decimal::new(50, 2)),
    }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::stock_transactions_crud(pool.clone());

    crud.create(&make_fk("stx_crd", 10.0, 150.0)).await.expect("create failed");
    let pk = make_pk("stx_crd");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.execution_id, "stx_crd");
    assert_eq!(data.quantity, 10.0);

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read(&pk).await.expect("read failed").is_none());
    del_strat!(&pool);
}

#[tokio::test]
async fn test_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::stock_transactions_crud(pool.clone());

    crud.create(&make_fk("stx_upd", 10.0, 150.0)).await.expect("create failed");
    let pk = make_pk("stx_upd");
    crud.update(&pk, &uk(Some(155.0), Some(20.0))).await.expect("update failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.price, 155.0);
    assert_eq!(data.quantity, 20.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::stock_transactions_crud(pool.clone());

    crud.create(&make_fk("stx_ra_a", 10.0, 150.0)).await.expect("create A failed");
    crud.create(&make_fk("stx_ra_b", 20.0, 160.0)).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all.iter()
        .filter(|p| p.execution_id == "stx_ra_a" || p.execution_id == "stx_ra_b")
        .collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&make_pk("stx_ra_a")).await.expect("delete A failed");
    crud.delete(&make_pk("stx_ra_b")).await.expect("delete B failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::stock_transactions_crud(pool.clone());

    let fk = make_fk("stx_coi", 10.0, 150.0);
    let pk = make_pk("stx_coi");

    crud.create_or_ignore(&fk).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 10.0);

    let mut fk2 = fk.clone();
    fk2.quantity = 999.0;
    crud.create_or_ignore(&fk2).await.expect("conflict path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 10.0, "conflict path should NOT update");

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_insert_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::stock_transactions_crud(pool.clone());

    let pk = make_pk("stx_cou_ins");
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &full_uk("stx_cou_ins", Some(155.0), Some(10.0))).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.price, 155.0);
    assert_eq!(data.quantity, 10.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::stock_transactions_crud(pool.clone());

    let pk = make_pk("stx_cou_upd");
    crud.create(&make_fk("stx_cou_upd", 10.0, 150.0)).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &full_uk("stx_cou_upd", Some(155.0), Some(20.0))).await.expect("update path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.price, 155.0);
    assert_eq!(data.quantity, 20.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
