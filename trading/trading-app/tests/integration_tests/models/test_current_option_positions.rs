//! DB integration tests for `trading.current_option_positions` CRUD lifecycle.
//!
//! Comprehensively tests all 7 CRUD methods:
//! - create, read, update, delete, read_all
//! - create_or_ignore (insert + ignore-on-conflict paths)
//! - create_or_update (insert-if-not-exists + update-if-exists paths)
//!
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys,
    CurrentOptionPositionsUpdateKeys, OptionType,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

const STOCK: &str = "COP_TEST";
const EXCHANGE: &str = "NASDAQ";
const CURRENCY: &str = "USD";
const STRATEGY: &str = "noise";
const EXPIRY: &str = "20250119";
const STRIKE: f64 = 150.0;
const MULTIPLIER: &str = "100";

fn make_fk(stock: &str, qty: f64, price: f64) -> CurrentOptionPositionsFullKeys {
    CurrentOptionPositionsFullKeys {
        stock: stock.to_string(),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        strategy: STRATEGY.to_string(),
        expiry: EXPIRY.to_string(),
        strike: STRIKE,
        multiplier: MULTIPLIER.to_string(),
        option_type: OptionType::Call,
        quantity: qty,
        avg_price: price,
        last_updated: Utc::now(),
    }
}

fn make_pk(stock: &str) -> CurrentOptionPositionsPrimaryKeys {
    CurrentOptionPositionsPrimaryKeys {
        stock: stock.to_string(),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        strategy: STRATEGY.to_string(),
        expiry: EXPIRY.to_string(),
        strike: STRIKE,
        multiplier: MULTIPLIER.to_string(),
        option_type: OptionType::Call,
    }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::current_option_positions_crud(pool.clone());

    let stock = format!("{STOCK}_crd");
    crud.create(&make_fk(&stock, 5.0, 3.50)).await.expect("create failed");

    let pk = make_pk(&stock);
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.stock, stock);
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
    let crud = trading_app::test_internals::current_option_positions_crud(pool.clone());

    let stock = format!("{STOCK}_upd");
    crud.create(&make_fk(&stock, 5.0, 3.50)).await.expect("create failed");
    let pk = make_pk(&stock);

    crud.update(&pk, &CurrentOptionPositionsUpdateKeys {
        quantity: Some(10.0),
        avg_price: Some(4.00),
        last_updated: None,
    }).await.expect("update failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 10.0);
    assert_eq!(data.avg_price, 4.00);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::current_option_positions_crud(pool.clone());

    let stock_a = format!("{STOCK}_ra_a");
    let stock_b = format!("{STOCK}_ra_b");
    crud.create(&make_fk(&stock_a, 5.0, 3.50)).await.expect("create A failed");
    crud.create(&make_fk(&stock_b, 3.0, 4.00)).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all.iter().filter(|p| p.stock == stock_a || p.stock == stock_b).collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&make_pk(&stock_a)).await.expect("delete A failed");
    crud.delete(&make_pk(&stock_b)).await.expect("delete B failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::current_option_positions_crud(pool.clone());

    let stock = format!("{STOCK}_coi");
    let fk = make_fk(&stock, 5.0, 3.50);
    let pk = make_pk(&stock);

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
    let crud = trading_app::test_internals::current_option_positions_crud(pool.clone());

    let stock = format!("{STOCK}_cou_ins");
    let pk = make_pk(&stock);
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &CurrentOptionPositionsUpdateKeys {
        quantity: Some(5.0),
        avg_price: Some(3.50),
        last_updated: Some(Utc::now()),
    }).await.expect("insert path failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 5.0);
    assert_eq!(data.avg_price, 3.50);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::current_option_positions_crud(pool.clone());

    let stock = format!("{STOCK}_cou_upd");
    let pk = make_pk(&stock);
    crud.create(&make_fk(&stock, 5.0, 3.50)).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &CurrentOptionPositionsUpdateKeys {
        quantity: Some(10.0),
        avg_price: Some(4.00),
        last_updated: Some(Utc::now()),
    }).await.expect("update path failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 10.0);
    assert_eq!(data.avg_price, 4.00);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
