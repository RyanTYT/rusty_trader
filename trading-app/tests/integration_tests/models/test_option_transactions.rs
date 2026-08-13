//! DB integration test for `trading.option_transactions` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{OptionType, OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys};
use trading_app::database::models_crud::transactions::option_transactions::OptionTransactionsCRUD;

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_option_transactions_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::option_transactions_crud(pool.clone());

    let now = Utc::now();
    let fk = OptionTransactionsFullKeys {
        execution_id: "opt_exec_001".to_string(),
        strategy: "noise".to_string(),
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        expiry: "20250119".to_string(),
        strike: 150.0,
        multiplier: "100".to_string(),
        option_type: OptionType::Call,
        order_perm_id: 12345,
        time: now,
        price: 3.50,
        quantity: 5.0,
        fees: Decimal::new(25, 2), // 0.25
    };

    crud.create(&fk).await.expect("create failed");

    let pk = OptionTransactionsPrimaryKeys {
        execution_id: "opt_exec_001".to_string(),
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.execution_id, "opt_exec_001");
    assert_eq!(data.strike, 150.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
