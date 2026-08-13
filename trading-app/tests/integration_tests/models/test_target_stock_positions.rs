//! DB integration test for `trading.target_stock_positions` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    TargetStockPositionsFullKeys, TargetStockPositionsPrimaryKeys,
};
use trading_app::database::models_crud::target_positions::target_stock_positions::TargetStockPositionsCRUD;

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_target_stock_positions_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::target_stock_positions_crud(pool.clone());

    let fk = TargetStockPositionsFullKeys {
        strategy: "noise".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        stock: "AAPL".to_string(),
        avg_price: 150.0,
        quantity: 50.0,
    };

    crud.create(&fk).await.expect("create failed");

    let pk = TargetStockPositionsPrimaryKeys {
        strategy: "noise".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        stock: "AAPL".to_string(),
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.stock, "AAPL");
    assert_eq!(data.quantity, 50.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
