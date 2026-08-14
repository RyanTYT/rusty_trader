//! DB integration tests for `trading.target_stock_positions` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.

use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    TargetStockPositionsFullKeys, TargetStockPositionsPrimaryKeys,
    TargetStockPositionsUpdateKeys,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

const STRATEGY: &str = "noise";

fn make_fk(stock: &str, qty: f64, price: f64) -> TargetStockPositionsFullKeys {
    TargetStockPositionsFullKeys {
        strategy: STRATEGY.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        stock: stock.to_string(),
        avg_price: price,
        quantity: qty,
    }
}

fn make_pk(stock: &str) -> TargetStockPositionsPrimaryKeys {
    TargetStockPositionsPrimaryKeys {
        strategy: STRATEGY.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        stock: stock.to_string(),
    }
}

fn uk(qty: Option<f64>, price: Option<f64>) -> TargetStockPositionsUpdateKeys {
    TargetStockPositionsUpdateKeys { quantity: qty, avg_price: price }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::target_stock_positions_crud(pool.clone());

    let stock = "TSP_crd";
    crud.create(&make_fk(stock, 100.0, 150.0)).await.expect("create failed");
    let pk = make_pk(stock);
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.stock, stock);
    assert_eq!(data.quantity, 100.0);

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read(&pk).await.expect("read failed").is_none());
    del_strat!(&pool);
}

#[tokio::test]
async fn test_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::target_stock_positions_crud(pool.clone());

    let stock = "TSP_upd";
    crud.create(&make_fk(stock, 100.0, 150.0)).await.expect("create failed");
    let pk = make_pk(stock);
    crud.update(&pk, &uk(Some(200.0), Some(155.0))).await.expect("update failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 200.0);
    assert_eq!(data.avg_price, 155.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::target_stock_positions_crud(pool.clone());

    crud.create(&make_fk("TSP_ra_a", 100.0, 150.0)).await.expect("create A failed");
    crud.create(&make_fk("TSP_ra_b", 200.0, 160.0)).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all.iter()
        .filter(|p| p.stock == "TSP_ra_a" || p.stock == "TSP_ra_b")
        .collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&make_pk("TSP_ra_a")).await.expect("delete A failed");
    crud.delete(&make_pk("TSP_ra_b")).await.expect("delete B failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::target_stock_positions_crud(pool.clone());

    let stock = "TSP_coi";
    let fk = make_fk(stock, 100.0, 150.0);
    let pk = make_pk(stock);

    crud.create_or_ignore(&fk).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 100.0);

    let mut fk2 = fk.clone();
    fk2.quantity = 999.0;
    crud.create_or_ignore(&fk2).await.expect("conflict path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 100.0, "conflict path should NOT update");

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_insert_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::target_stock_positions_crud(pool.clone());

    let stock = "TSP_cou_ins";
    let pk = make_pk(stock);
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &uk(Some(100.0), Some(150.0))).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 100.0);
    assert_eq!(data.avg_price, 150.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::target_stock_positions_crud(pool.clone());

    let stock = "TSP_cou_upd";
    let pk = make_pk(stock);
    crud.create(&make_fk(stock, 100.0, 150.0)).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &uk(Some(200.0), Some(160.0))).await.expect("update path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.quantity, 200.0);
    assert_eq!(data.avg_price, 160.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
