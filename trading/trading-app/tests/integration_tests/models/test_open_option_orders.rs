//! DB integration tests for `trading.open_option_orders` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys, OptionType,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

const STRATEGY: &str = "noise";
const EXPIRY: &str = "20250119";
const STRIKE: f64 = 150.0;
const MULTIPLIER: &str = "100";

fn make_fk(perm_id: i32, order_id: i32, qty: f64) -> OpenOptionOrdersFullKeys {
    OpenOptionOrdersFullKeys {
        order_perm_id: perm_id, order_id,
        strategy: STRATEGY.to_string(), stock: format!("OOO_{perm_id}"),
        primary_exchange: "NASDAQ".to_string(), currency: "USD".to_string(),
        expiry: EXPIRY.to_string(), strike: STRIKE,
        multiplier: MULTIPLIER.to_string(), option_type: OptionType::Call,
        time: Utc::now(), quantity: qty, executions: vec![], filled: 0.0,
    }
}

fn make_pk(perm_id: i32, order_id: i32) -> OpenOptionOrdersPrimaryKeys {
    OpenOptionOrdersPrimaryKeys { order_perm_id: perm_id, order_id }
}

fn uk(qty: Option<f64>, filled: Option<f64>) -> OpenOptionOrdersUpdateKeys {
    OpenOptionOrdersUpdateKeys {
        strategy: None, stock: None, primary_exchange: None, currency: None,
        expiry: None, strike: None, multiplier: None, option_type: None, time: None,
        quantity: qty, executions: filled.map(|f| vec![format!("exec_{f}")]),
        filled,
    }
}

fn full_uk(qty: Option<f64>, filled: Option<f64>) -> OpenOptionOrdersUpdateKeys {
    OpenOptionOrdersUpdateKeys {
        strategy: Some(STRATEGY.to_string()), stock: Some("QQQ".to_string()), 
        primary_exchange: Some("NASDAQ".to_string()), currency: Some("USD".to_string()),
        expiry: Some(EXPIRY.to_string()), strike: Some(STRIKE), multiplier: Some(MULTIPLIER.to_string()), 
        option_type: Some(OptionType::Call), time: Some(Utc::now()),
        quantity: qty, executions: filled.map(|f| vec![format!("exec_{f}")]),
        filled,
    }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::open_option_orders_crud(pool.clone());

    crud.create(&make_fk(80015, 80016, 5.0)).await.expect("create failed");
    let pk = make_pk(80015, 80016);
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.order_perm_id, 80015);
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
    let crud = trading_app::test_internals::open_option_orders_crud(pool.clone());

    crud.create(&make_fk(80017, 80018, 5.0)).await.expect("create failed");
    let pk = make_pk(80017, 80018);
    crud.update(&pk, &uk(Some(10.0), Some(2.0))).await.expect("update failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 10.0);
    assert_eq!(data.filled, 2.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::open_option_orders_crud(pool.clone());

    crud.create(&make_fk(80019, 80020, 5.0)).await.expect("create A failed");
    crud.create(&make_fk(80021, 80022, 3.0)).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all.iter()
        .filter(|p| p.order_perm_id == 80019 || p.order_perm_id == 80021)
        .collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&make_pk(80019, 80020)).await.expect("delete A failed");
    crud.delete(&make_pk(80021, 80022)).await.expect("delete B failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::open_option_orders_crud(pool.clone());

    let fk = make_fk(80023, 80024, 5.0);
    let pk = make_pk(80023, 80024);
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
    let crud = trading_app::test_internals::open_option_orders_crud(pool.clone());

    let pk = make_pk(80025, 80026);
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &full_uk(Some(5.0), Some(0.0))).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 5.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::open_option_orders_crud(pool.clone());

    let pk = make_pk(80027, 80028);
    crud.create(&make_fk(80027, 80028, 5.0)).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &full_uk(Some(10.0), Some(2.0))).await.expect("update path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 10.0);
    assert_eq!(data.filled, 2.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
