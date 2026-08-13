//! DB integration test for `trading.stock_transactions` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{StockTransactionsFullKeys, StockTransactionsPrimaryKeys};
use trading_app::database::models_crud::transactions::stock_transactions::StockTransactionsCRUD;

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_stock_transactions_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::stock_transactions_crud(pool.clone());

    let now = Utc::now();
    let fk = StockTransactionsFullKeys {
        execution_id: "exec_001".to_string(),
        strategy: "noise".to_string(),
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        order_perm_id: 12345,
        time: now,
        price: 150.0,
        quantity: 10.0,
        fees: Decimal::new(50, 2), // 0.50
    };

    crud.create(&fk).await.expect("create failed");

    let pk = StockTransactionsPrimaryKeys {
        execution_id: "exec_001".to_string(),
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.execution_id, "exec_001");
    assert_eq!(data.price, 150.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
