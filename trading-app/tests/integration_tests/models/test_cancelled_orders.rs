//! DB integration tests for `logs.cancelled_orders` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys, CancelledOrdersUpdateKeys,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

const STRATEGY: &str = "noise";

fn make_fk(perm_id: i32, order_id: i32) -> CancelledOrdersFullKeys {
    CancelledOrdersFullKeys {
        time: Utc::now(), order_perm_id: perm_id, order_id,
        strategy: STRATEGY.to_string(), stock: format!("CNL_{perm_id}"),
        primary_exchange: "NASDAQ".to_string(), currency: "USD".to_string(),
        quantity: 10.0, executions: vec![], filled: 0.0,
        reason: "test cancel".to_string(),
    }
}

fn make_pk(time: chrono::DateTime<Utc>, perm_id: i32, order_id: i32) -> CancelledOrdersPrimaryKeys {
    CancelledOrdersPrimaryKeys { time, order_perm_id: perm_id, order_id }
}

fn uk(filled: Option<f64>, reason: Option<String>) -> CancelledOrdersUpdateKeys {
    CancelledOrdersUpdateKeys {
        strategy: Some("noise").to_string(), stock: None, primary_exchange: None, currency: None,
        quantity: None, executions: None, filled, reason,
    }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::cancelled_orders_crud(pool.clone());

    let fk = make_fk(70001, 70002);
    let pk = make_pk(fk.time, 70001, 70002);
    crud.create(&fk).await.expect("create failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.order_perm_id, 70001);
    assert_eq!(data.reason, "test cancel");

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read(&pk).await.expect("read failed").is_none());
    del_strat!(&pool);
}

#[tokio::test]
async fn test_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::cancelled_orders_crud(pool.clone());

    let fk = make_fk(70003, 70004);
    let pk = make_pk(fk.time, 70003, 70004);
    crud.create(&fk).await.expect("create failed");

    crud.update(&pk, &uk(Some(5.0), Some("updated reason".to_string()))).await.expect("update failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.filled, 5.0);
    assert_eq!(data.reason, "updated reason");

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::cancelled_orders_crud(pool.clone());

    let fk_a = make_fk(70005, 70006);
    let fk_b = make_fk(70007, 70008);
    let pk_a = make_pk(fk_a.time, 70005, 70006);
    let pk_b = make_pk(fk_b.time, 70007, 70008);
    crud.create(&fk_a).await.expect("create A failed");
    crud.create(&fk_b).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all.iter()
        .filter(|p| p.order_perm_id == 70005 || p.order_perm_id == 70007)
        .collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&pk_a).await.expect("delete A failed");
    crud.delete(&pk_b).await.expect("delete B failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::cancelled_orders_crud(pool.clone());

    let fk = make_fk(70009, 70010);
    let pk = make_pk(fk.time, 70009, 70010);

    crud.create_or_ignore(&fk).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.reason, "test cancel");

    let mut fk2 = fk.clone();
    fk2.reason = "999".to_string();
    crud.create_or_ignore(&fk2).await.expect("conflict path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.reason, "test cancel", "conflict path should NOT update");

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_insert_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::cancelled_orders_crud(pool.clone());

    let fk = make_fk(70011, 70012);
    let pk = make_pk(fk.time, 70011, 70012);
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &uk(Some(0.0), Some("test cancel".to_string()))).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.reason, "test cancel");

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::cancelled_orders_crud(pool.clone());

    let fk = make_fk(70013, 70014);
    let pk = make_pk(fk.time, 70013, 70014);
    crud.create(&fk).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &uk(Some(5.0), Some("updated reason".to_string()))).await.expect("update path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.filled, 5.0);
    assert_eq!(data.reason, "updated reason");

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
