//! DB integration tests for `trading.open_stock_orders` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

const STRATEGY: &str = "noise";

fn make_fk(perm_id: i32, order_id: i32, qty: f64) -> OpenStockOrdersFullKeys {
    OpenStockOrdersFullKeys {
        order_perm_id: perm_id,
        order_id,
        strategy: STRATEGY.to_string(),
        stock: format!("OSO_{perm_id}"),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        time: Utc::now(),
        quantity: qty,
        executions: vec![],
        filled: 0.0,
    }
}

fn make_pk(perm_id: i32, order_id: i32) -> OpenStockOrdersPrimaryKeys {
    OpenStockOrdersPrimaryKeys { order_perm_id: perm_id, order_id }
}

fn uk(qty: Option<f64>, filled: Option<f64>) -> OpenStockOrdersUpdateKeys {
    OpenStockOrdersUpdateKeys {
        strategy: None, stock: None, primary_exchange: None, currency: None, time: None,
        quantity: qty, executions: filled.map(|f| vec![format!("exec_{f}")]),
        filled,
    }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::open_stock_orders_crud(pool.clone());

    crud.create(&make_fk(80001, 80002, 10.0)).await.expect("create failed");
    let pk = make_pk(80001, 80002);
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.order_perm_id, 80001);
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
    let crud = trading_app::test_internals::open_stock_orders_crud(pool.clone());

    crud.create(&make_fk(80003, 80004, 10.0)).await.expect("create failed");
    let pk = make_pk(80003, 80004);
    crud.update(&pk, &uk(Some(20.0), Some(5.0))).await.expect("update failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 20.0);
    assert_eq!(data.filled, 5.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::open_stock_orders_crud(pool.clone());

    crud.create(&make_fk(80005, 80006, 10.0)).await.expect("create A failed");
    crud.create(&make_fk(80007, 80008, 20.0)).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all.iter()
        .filter(|p| p.order_perm_id == 80005 || p.order_perm_id == 80007)
        .collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&make_pk(80005, 80006)).await.expect("delete A failed");
    crud.delete(&make_pk(80007, 80008)).await.expect("delete B failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::open_stock_orders_crud(pool.clone());

    let fk = make_fk(80009, 80010, 10.0);
    let pk = make_pk(80009, 80010);

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
    let crud = trading_app::test_internals::open_stock_orders_crud(pool.clone());

    let pk = make_pk(80011, 80012);
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &uk(Some(10.0), Some(0.0))).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 10.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::open_stock_orders_crud(pool.clone());

    let pk = make_pk(80013, 80014);
    crud.create(&make_fk(80013, 80014, 10.0)).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &uk(Some(20.0), Some(5.0))).await.expect("update path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 20.0);
    assert_eq!(data.filled, 5.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
