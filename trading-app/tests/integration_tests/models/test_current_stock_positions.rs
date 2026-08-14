//! DB integration tests for `trading.current_stock_positions` CRUD lifecycle.
//!
//! Comprehensively tests all 7 CRUD methods:
//! - create, read, update, delete, read_all
//! - create_or_ignore (insert path + ignore-on-conflict path)
//! - create_or_update (insert-if-not-exists path + update-if-exists path)
//!
//! Requires: live Postgres + DATABASE_URL. All tests run sequentially via TEST_MUTEX.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys,
    CurrentStockPositionsUpdateKeys,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

const STOCK: &str = "CSP_TEST";
const EXCHANGE: &str = "NASDAQ";
const CURRENCY: &str = "USD";
const STRATEGY: &str = "noise";

fn make_fk(stock: &str, qty: f64, price: f64) -> CurrentStockPositionsFullKeys {
    CurrentStockPositionsFullKeys {
        stock: stock.to_string(),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        strategy: STRATEGY.to_string(),
        quantity: qty,
        avg_price: price,
        last_updated: Utc::now(),
    }
}

fn make_pk(stock: &str) -> CurrentStockPositionsPrimaryKeys {
    CurrentStockPositionsPrimaryKeys {
        stock: stock.to_string(),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        strategy: STRATEGY.to_string(),
    }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::current_stock_positions_crud(pool.clone());

    let stock = format!("{STOCK}_crd");
    crud.create(&make_fk(&stock, 100.0, 150.0)).await.expect("create failed");

    let pk = make_pk(&stock);
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.stock, stock);
    assert_eq!(data.quantity, 100.0);
    assert_eq!(data.avg_price, 150.0);

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read(&pk).await.expect("read failed").is_none(), "row should be gone after delete");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::current_stock_positions_crud(pool.clone());

    let stock = format!("{STOCK}_upd");
    crud.create(&make_fk(&stock, 100.0, 150.0)).await.expect("create failed");

    let pk = make_pk(&stock);
    // Update only some fields — others stay unchanged
    crud.update(&pk, &CurrentStockPositionsUpdateKeys {
        quantity: Some(200.0),
        avg_price: Some(155.0),
        last_updated: None, // None = don't update this field
    }).await.expect("update failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 200.0, "quantity should be updated");
    assert_eq!(data.avg_price, 155.0, "avg_price should be updated");

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::current_stock_positions_crud(pool.clone());

    let stock_a = format!("{STOCK}_ra_a");
    let stock_b = format!("{STOCK}_ra_b");
    crud.create(&make_fk(&stock_a, 100.0, 150.0)).await.expect("create A failed");
    crud.create(&make_fk(&stock_b, 200.0, 160.0)).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    // Filter to just our test stocks (table may have other rows)
    let ours: Vec<_> = all.iter()
        .filter(|p| p.stock == stock_a || p.stock == stock_b)
        .collect();
    assert_eq!(ours.len(), 2, "should return both rows");

    crud.delete(&make_pk(&stock_a)).await.expect("delete A failed");
    crud.delete(&make_pk(&stock_b)).await.expect("delete B failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::current_stock_positions_crud(pool.clone());

    let stock = format!("{STOCK}_coi");
    let fk = make_fk(&stock, 100.0, 150.0);
    let pk = make_pk(&stock);

    // Path 1: insert fresh — should succeed and create the row
    crud.create_or_ignore(&fk).await.expect("first create_or_ignore (insert path) failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 100.0, "row should exist after first insert");

    // Path 2: insert again with same PK — should be a no-op (ON CONFLICT DO NOTHING)
    let mut fk2 = fk.clone();
    fk2.quantity = 999.0; // different value, but same PK
    crud.create_or_ignore(&fk2).await.expect("second create_or_ignore (conflict path) failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 100.0, "conflict path should NOT update — original 100.0 preserved");

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_insert_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::current_stock_positions_crud(pool.clone());

    let stock = format!("{STOCK}_cou_ins");
    let pk = make_pk(&stock);

    // Row does not exist yet — create_or_update should INSERT
    assert!(crud.read(&pk).await.expect("read failed").is_none(), "row should not exist yet");

    crud.create_or_update(&pk, &CurrentStockPositionsUpdateKeys {
        quantity: Some(100.0),
        avg_price: Some(150.0),
        last_updated: Some(Utc::now()),
    }).await.expect("create_or_update (insert path) failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row after insert");
    assert_eq!(data.quantity, 100.0, "insert path should create row with qty=100");
    assert_eq!(data.avg_price, 150.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::current_stock_positions_crud(pool.clone());

    let stock = format!("{STOCK}_cou_upd");
    let pk = make_pk(&stock);

    // Pre-insert a row
    crud.create(&make_fk(&stock, 100.0, 150.0)).await.expect("pre-insert failed");

    // Row exists — create_or_update should UPDATE (upsert)
    crud.create_or_update(&pk, &CurrentStockPositionsUpdateKeys {
        quantity: Some(200.0),
        avg_price: Some(160.0),
        last_updated: Some(Utc::now()),
    }).await.expect("create_or_update (update path) failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row after update");
    assert_eq!(data.quantity, 200.0, "update path should change qty to 200");
    assert_eq!(data.avg_price, 160.0, "update path should change avg_price to 160");

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
