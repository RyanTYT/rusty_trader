//! DB integration test for `trading.open_stock_orders` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys};
use trading_app::database::models_crud::open_orders::open_stock_orders::OpenStockOrdersCRUD;

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_open_stock_orders_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::open_stock_orders_crud(pool.clone());

    let now = Utc::now();
    let fk = OpenStockOrdersFullKeys {
        order_perm_id: 11111,
        order_id: 22222,
        strategy: "noise".to_string(),
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        time: now,
        quantity: 10.0,
        executions: vec![],
        filled: 0.0,
    };

    crud.create(&fk).await.expect("create failed");

    let pk = OpenStockOrdersPrimaryKeys {
        order_perm_id: 11111,
        order_id: 22222,
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.order_id, 22222);
    assert_eq!(data.stock, "AAPL");

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
