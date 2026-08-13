//! DB integration test for `logs.cancelled_orders` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys};
use trading_app::database::models_crud::cancelled_orders::CancelledOrdersCRUD;

use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_cancelled_orders_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = CancelledOrdersCRUD::new(pool);

    let now = Utc::now();
    let fk = CancelledOrdersFullKeys {
        time: now,
        order_perm_id: 12345,
        order_id: 67890,
        strategy: "noise".to_string(),
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        quantity: 10.0,
        executions: vec!["exec1".to_string()],
        filled: 5.0,
        reason: "cancelled by user".to_string(),
    };

    crud.create(&fk).await.expect("create failed");

    let pk = CancelledOrdersPrimaryKeys {
        time: now,
        order_perm_id: 12345,
        order_id: 67890,
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.order_perm_id, 12345);
    assert_eq!(data.strategy, "noise");
    assert_eq!(data.reason, "cancelled by user");

    crud.delete(&pk).await.expect("delete failed");
}
