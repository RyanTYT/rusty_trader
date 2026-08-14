//! Comprehensive DB integration tests for `TransactionsOps` on `TransactionsCRUD`.
//!
//! Tests ALL variants (Stock + Options) for `read_last_transaction`.
//!
//! Requires: live Postgres + DATABASE_URL. All tests #[ignore]'d.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    OptionType, OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys,
    StockTransactionsFullKeys, StockTransactionsPrimaryKeys,
};
use trading_app::database::models_crud::transactions::stock_transactions::StockTransactionsCRUD;
use trading_app::database::models_crud::transactions::option_transactions::OptionTransactionsCRUD;
use trading_app::database::models_crud::transactions::transactions::{
    TransactionsCRUD, TransactionsFullKeys as TxFK, TransactionsOps,
    TransactionsUnderlyingPrimaryKeys,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

// ============================ read_last_transaction — Stock ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_transaction_stock_comprehensive() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let txn_crud = trading_app::test_internals::stock_transactions_crud(pool.clone());
    let interface_crud = TransactionsCRUD::stock(pool.clone());

    let now = Utc::now();
    // Insert 3 transactions with different times
    let test_txns: Vec<(&str, chrono::DateTime<Utc>, f64, f64)> = vec![
        ("txn_old", now - chrono::Duration::hours(3), 145.0, 10.0),
        ("txn_mid", now - chrono::Duration::hours(2), 150.0, 5.0),
        ("txn_new", now - chrono::Duration::hours(1), 155.0, 8.0),
    ];

    for (exec_id, time, price, qty) in &test_txns {
        txn_crud.create(&StockTransactionsFullKeys {
            execution_id: exec_id.to_string(), strategy: "noise".to_string(),
            stock: "TXNTEST".to_string(), primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(), order_perm_id: 12345,
            time: *time, price: *price, quantity: *qty,
            fees: Decimal::new(50, 2),
        }).await.expect("create txn failed");
    }

    let pk = TransactionsUnderlyingPrimaryKeys::Stock(
        trading_app::database::models_crud::transactions::stock_transactions::StockTransactionsUnderlyingPrimaryKeys {
            stock: "TXNTEST".to_string(), primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
        },
    );

    let result = interface_crud.read_last_transaction(pk).await.expect("read_last_transaction failed")
        .expect("expected Some");

    match result {
        TxFK::Stock(s) => {
            assert_eq!(s.execution_id, "txn_new", "should return most recent txn");
            assert_eq!(s.price, 155.0);
            assert_eq!(s.quantity, 8.0);
        }
        _ => panic!("expected Stock variant"),
    }

    // Cleanup
    for (exec_id, _, _, _) in &test_txns {
        let _ = txn_crud.delete(&StockTransactionsPrimaryKeys {
            execution_id: exec_id.to_string(),
        }).await;
    }
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_transaction_stock_empty() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let interface_crud = TransactionsCRUD::stock(pool.clone());

    let pk = TransactionsUnderlyingPrimaryKeys::Stock(
        trading_app::database::models_crud::transactions::stock_transactions::StockTransactionsUnderlyingPrimaryKeys {
            stock: "NOTXN".to_string(), primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
        },
    );

    let result = interface_crud.read_last_transaction(pk).await.expect("read_last_transaction failed");
    assert!(result.is_none(), "no transactions → None");
    del_strat!(&pool);
}

// ============================ read_last_transaction — Options ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_transaction_options_comprehensive() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let txn_crud = trading_app::test_internals::option_transactions_crud(pool.clone());
    let interface_crud = TransactionsCRUD::option(pool.clone());

    let now = Utc::now();
    // Insert 2 option transactions with different times
    let test_txns: Vec<(&str, chrono::DateTime<Utc>, f64, f64)> = vec![
        ("opt_old", now - chrono::Duration::hours(2), 3.50, 5.0),
        ("opt_new", now - chrono::Duration::hours(1), 4.00, 3.0),
    ];

    for (exec_id, time, price, qty) in &test_txns {
        txn_crud.create(&OptionTransactionsFullKeys {
            execution_id: exec_id.to_string(), strategy: "noise".to_string(),
            stock: "TXNOPT".to_string(), primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(), expiry: "20250119".to_string(),
            strike: 150.0, multiplier: "100".to_string(), option_type: OptionType::Call,
            order_perm_id: 12345, time: *time, price: *price, quantity: *qty,
            fees: Decimal::new(25, 2),
        }).await.expect("create txn failed");
    }

    let pk = TransactionsUnderlyingPrimaryKeys::Options(
        trading_app::database::models_crud::transactions::option_transactions::OptionTransactionsUnderlyingPrimaryKeys {
            stock: "TXNOPT".to_string(), primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(), expiry: "20250119".to_string(),
            strike: 150.0, multiplier: "100".to_string(), option_type: OptionType::Call,
        },
    );

    let result = interface_crud.read_last_transaction(pk).await.expect("read_last_transaction failed")
        .expect("expected Some");

    match result {
        TxFK::Options(o) => {
            assert_eq!(o.execution_id, "opt_new", "should return most recent option txn");
            assert_eq!(o.price, 4.00);
            assert_eq!(o.quantity, 3.0);
            assert_eq!(o.strike, 150.0);
            assert!(matches!(o.option_type, OptionType::Call));
        }
        _ => panic!("expected Options variant"),
    }

    // Cleanup
    for (exec_id, _, _, _) in &test_txns {
        let _ = txn_crud.delete(&OptionTransactionsPrimaryKeys {
            execution_id: exec_id.to_string(),
        }).await;
    }
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_transaction_options_empty() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let interface_crud = TransactionsCRUD::option(pool.clone());

    let pk = TransactionsUnderlyingPrimaryKeys::Options(
        trading_app::database::models_crud::transactions::option_transactions::OptionTransactionsUnderlyingPrimaryKeys {
            stock: "NOOPTTXN".to_string(), primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(), expiry: "20250119".to_string(),
            strike: 150.0, multiplier: "100".to_string(), option_type: OptionType::Call,
        },
    );

    let result = interface_crud.read_last_transaction(pk).await.expect("read_last_transaction failed");
    assert!(result.is_none(), "no option transactions → None");
    del_strat!(&pool);
}
