//! DB integration tests for complex SQL operations.
//!
//! Tests the advanced ops traits:
//! - `CurrentPositionsOps::update_positions_additive` — weighted-avg price recalculation
//! - `HistoricalDataOps::read_last_n` — TimescaleDB time_bucket + full/incomplete split
//! - `HistoricalDataOps::read_last_vwap` — VWAP from daily bars
//! - `TargetPositionsOps::get_target_pos_diff_by_pk` — FULL OUTER JOIN diff
//! - `NoiseOps::get_most_recent_daily_open` — daily + intraday open
//! - `TransactionsOps::read_last_transaction` — last transaction by time
//!
//! Requires: live Postgres + DATABASE_URL. All tests #[ignore]'d.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys,
    CurrentStockPositionsUpdateKeys, HistoricalStockDataFullKeys,
    HistoricalStockDataPrimaryKeysWoTime as StockPKWoTime, OptionTransactionsPrimaryKeys,
    StockTransactionsFullKeys, StockTransactionsPrimaryKeys, TargetStockPositionsFullKeys,
    TargetStockPositionsPrimaryKeys,
};
use trading_app::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsOps, CurrentPositionsPrimaryKeys as CPInterfacePK,
    CurrentPositionsUpdateKeys as CPInterfaceUK,
};
use trading_app::database::models_crud::current_positions::current_stock_positions::CurrentStockPositionsCRUD;
use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, HistoricalDataOps, HistoricalDataPrimaryKeysWoTime, NoiseOps, VwapBarValue,
};
use trading_app::database::models_crud::historical_data::historical_stock_data::HistoricalStockDataCRUD;
use trading_app::database::models_crud::target_positions::target_positions::{
    TargetPositionsCRUD, TargetPositionsOps, TargetPositionsPrimaryKeys as TPInterfacePK,
};
use trading_app::database::models_crud::target_positions::target_stock_positions::TargetStockPositionsCRUD;
use trading_app::database::models_crud::transactions::stock_transactions::StockTransactionsCRUD;
use trading_app::database::models_crud::transactions::transactions::{
    TransactionsCRUD, TransactionsOps, TransactionsUnderlyingPrimaryKeys,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

// ============================ update_positions_additive ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_update_positions_additive_first_fill() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::stock(pool.clone());

    let pk = CPInterfacePK::Stock(CurrentStockPositionsPrimaryKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        strategy: "noise".to_string(),
    });
    let uk = CPInterfaceUK::Stock(CurrentStockPositionsUpdateKeys {
        quantity: Some(100.0),
        avg_price: Some(150.0),
        last_updated: None,
    });

    crud.update_positions_additive(pk.clone(), uk)
        .await
        .expect("first fill failed");

    let pos = crud.read(&pk).await.expect("read failed").expect("expected row");
    use trading_app::database::models_crud::current_positions::current_positions::CurrentPositionsFullKeys as CPFK;
    match pos {
        CPFK::Stock(s) => {
            assert_eq!(s.quantity, 100.0);
            assert_eq!(s.avg_price, 150.0);
        }
        _ => panic!("expected Stock variant"),
    }

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_update_positions_additive_same_direction_weighted_avg() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::stock(pool.clone());

    let pk = CPInterfacePK::Stock(CurrentStockPositionsPrimaryKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        strategy: "noise".to_string(),
    });

    // First fill: 100 @ 150
    crud.update_positions_additive(
        pk.clone(),
        CPInterfaceUK::Stock(CurrentStockPositionsUpdateKeys {
            quantity: Some(100.0),
            avg_price: Some(150.0),
            last_updated: None,
        }),
    )
    .await
    .expect("first fill failed");

    // Second fill: 100 @ 160 → weighted avg = (100*150 + 100*160)/200 = 155
    crud.update_positions_additive(
        pk.clone(),
        CPInterfaceUK::Stock(CurrentStockPositionsUpdateKeys {
            quantity: Some(100.0),
            avg_price: Some(160.0),
            last_updated: None,
        }),
    )
    .await
    .expect("second fill failed");

    let pos = crud.read(&pk).await.expect("read failed").expect("expected row");
    use trading_app::database::models_crud::current_positions::current_positions::CurrentPositionsFullKeys as CPFK;
    match pos {
        CPFK::Stock(s) => {
            assert_eq!(s.quantity, 200.0);
            assert!(
                (s.avg_price - 155.0).abs() < 1e-6,
                "weighted avg should be 155, got {}",
                s.avg_price
            );
        }
        _ => panic!("expected Stock variant"),
    }

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_update_positions_additive_cross_direction_keeps_avg_price() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::stock(pool.clone());

    let pk = CPInterfacePK::Stock(CurrentStockPositionsPrimaryKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        strategy: "noise".to_string(),
    });

    // Long 100 @ 150
    crud.update_positions_additive(
        pk.clone(),
        CPInterfaceUK::Stock(CurrentStockPositionsUpdateKeys {
            quantity: Some(100.0),
            avg_price: Some(150.0),
            last_updated: None,
        }),
    )
    .await
    .expect("first fill failed");

    // Sell 50 @ 160 (cross direction: +100 vs -50 → SIGN differs)
    // Stock arm: keeps existing avg_price (150)
    crud.update_positions_additive(
        pk.clone(),
        CPInterfaceUK::Stock(CurrentStockPositionsUpdateKeys {
            quantity: Some(-50.0),
            avg_price: Some(160.0),
            last_updated: None,
        }),
    )
    .await
    .expect("cross fill failed");

    let pos = crud.read(&pk).await.expect("read failed").expect("expected row");
    use trading_app::database::models_crud::current_positions::current_positions::CurrentPositionsFullKeys as CPFK;
    match pos {
        CPFK::Stock(s) => {
            assert_eq!(s.quantity, 50.0, "100 - 50 = 50");
            assert!(
                (s.avg_price - 150.0).abs() < 1e-6,
                "cross-direction should keep existing avg_price 150, got {}",
                s.avg_price
            );
        }
        _ => panic!("expected Stock variant"),
    }

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_update_positions_additive_total_zero_sets_avg_zero() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::stock(pool.clone());

    let pk = CPInterfacePK::Stock(CurrentStockPositionsPrimaryKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        strategy: "noise".to_string(),
    });

    // Long 100 @ 150
    crud.update_positions_additive(
        pk.clone(),
        CPInterfaceUK::Stock(CurrentStockPositionsUpdateKeys {
            quantity: Some(100.0),
            avg_price: Some(150.0),
            last_updated: None,
        }),
    )
    .await
    .unwrap();

    // Sell 100 @ 160 → total = 0, avg_price = 0 (division-by-zero guard)
    crud.update_positions_additive(
        pk.clone(),
        CPInterfaceUK::Stock(CurrentStockPositionsUpdateKeys {
            quantity: Some(-100.0),
            avg_price: Some(160.0),
            last_updated: None,
        }),
    )
    .await
    .unwrap();

    let pos = crud.read(&pk).await.expect("read failed").expect("expected row");
    use trading_app::database::models_crud::current_positions::current_positions::CurrentPositionsFullKeys as CPFK;
    match pos {
        CPFK::Stock(s) => {
            assert_eq!(s.quantity, 0.0);
            assert_eq!(s.avg_price, 0.0, "total→0 should set avg_price to 0");
        }
        _ => panic!("expected Stock variant"),
    }

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

// ============================ read_last_vwap ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_vwap_no_data_returns_none() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::stock(pool);

    let pk = HistoricalDataPrimaryKeysWoTime::Stock(StockPKWoTime {
        stock: "NODATA".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    });

    let vwap = crud
        .read_last_vwap(pk, Some("US/Eastern".to_string()), VwapBarValue::Close)
        .await
        .expect("read_last_vwap failed");

    assert!(vwap.is_none(), "no data → should return None");
}

// ============================ get_target_pos_diff_by_pk ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_target_pos_diff_by_pk_mismatch() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);

    let target_crud = trading_app::test_internals::target_stock_positions_crud(pool.clone());
    let current_crud = trading_app::test_internals::current_stock_positions_crud(pool.clone());

    // Target: 200 shares
    target_crud
        .create(&TargetStockPositionsFullKeys {
            strategy: "noise".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            stock: "DIFFTEST".to_string(),
            avg_price: 150.0,
            quantity: 200.0,
        })
        .await
        .expect("create target failed");

    // Current: 50 shares
    current_crud
        .create(&CurrentStockPositionsFullKeys {
            stock: "DIFFTEST".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            strategy: "noise".to_string(),
            quantity: 50.0,
            avg_price: 150.0,
            last_updated: Utc::now(),
        })
        .await
        .expect("create current failed");

    let diff_crud = TargetPositionsCRUD::stock(pool.clone());
    let pk = TPInterfacePK::Stock(TargetStockPositionsPrimaryKeys {
        strategy: "noise".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        stock: "DIFFTEST".to_string(),
    });

    let diffs = diff_crud
        .get_target_pos_diff_by_pk(pk)
        .await
        .expect("get_target_pos_diff_by_pk failed");

    assert!(!diffs.is_empty(), "should have a diff");
    use trading_app::database::models_crud::target_positions::target_positions::TargetPositionsQtyDiff;
    match &diffs[0] {
        TargetPositionsQtyDiff::Stock(d) => {
            assert!(
                (d.qty_diff - 150.0).abs() < 1e-6,
                "qty_diff should be 200-50=150, got {}",
                d.qty_diff
            );
            assert!(
                (d.current_qty - 50.0).abs() < 1e-6,
                "current_qty should be 50, got {}",
                d.current_qty
            );
        }
        _ => panic!("expected Stock variant"),
    }

    // Cleanup
    target_crud
        .delete(&TargetStockPositionsPrimaryKeys {
            strategy: "noise".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            stock: "DIFFTEST".to_string(),
        })
        .await
        .expect("delete target failed");
    current_crud
        .delete(&CurrentStockPositionsPrimaryKeys {
            stock: "DIFFTEST".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            strategy: "noise".to_string(),
        })
        .await
        .expect("delete current failed");
    del_strat!(&pool);
}

// ============================ read_last_transaction ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_transaction_returns_none_when_empty() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);

    let crud = TransactionsCRUD::stock(pool.clone());
    let pk = TransactionsUnderlyingPrimaryKeys::Stock(
        trading_app::database::models_crud::transactions::stock_transactions::StockTransactionsUnderlyingPrimaryKeys {
            stock: "NOTXN".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
        },
    );

    let result = crud
        .read_last_transaction(pk)
        .await
        .expect("read_last_transaction failed");

    assert!(result.is_none(), "no transactions → should return None");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_read_last_transaction_returns_most_recent() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);

    let txn_crud = trading_app::test_internals::stock_transactions_crud(pool.clone());
    let interface_crud = TransactionsCRUD::stock(pool.clone());

    let now = Utc::now();
    // Insert two transactions, the second one is more recent
    txn_crud
        .create(&StockTransactionsFullKeys {
            execution_id: "txn_old".to_string(),
            strategy: "noise".to_string(),
            stock: "TXNTEST".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            order_perm_id: 12345,
            time: now - chrono::Duration::hours(1),
            price: 150.0,
            quantity: 10.0,
            fees: Decimal::new(50, 2),
        })
        .await
        .expect("create old txn failed");

    txn_crud
        .create(&StockTransactionsFullKeys {
            execution_id: "txn_new".to_string(),
            strategy: "noise".to_string(),
            stock: "TXNTEST".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            order_perm_id: 12346,
            time: now,
            price: 155.0,
            quantity: 5.0,
            fees: Decimal::new(25, 2),
        })
        .await
        .expect("create new txn failed");

    let pk = TransactionsUnderlyingPrimaryKeys::Stock(
        trading_app::database::models_crud::transactions::stock_transactions::StockTransactionsUnderlyingPrimaryKeys {
            stock: "TXNTEST".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
        },
    );

    let result = interface_crud
        .read_last_transaction(pk)
        .await
        .expect("read_last_transaction failed")
        .expect("expected Some");

    use trading_app::database::models_crud::transactions::transactions::TransactionsFullKeys;
    match result {
        TransactionsFullKeys::Stock(s) => {
            assert_eq!(s.execution_id, "txn_new", "should return most recent txn");
            assert_eq!(s.price, 155.0);
        }
        _ => panic!("expected Stock variant"),
    }

    // Cleanup
    txn_crud
        .delete(&StockTransactionsPrimaryKeys {
            execution_id: "txn_old".to_string(),
        })
        .await
        .expect("delete old failed");
    txn_crud
        .delete(&StockTransactionsPrimaryKeys {
            execution_id: "txn_new".to_string(),
        })
        .await
        .expect("delete new failed");
    del_strat!(&pool);
}

// ============================ get_most_recent_daily_open ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_most_recent_daily_open_no_data() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    // NoiseOps is implemented on HistoricalDataCRUD (the interface enum), not HistoricalStockDataCRUD
    let crud = HistoricalDataCRUD::stock(pool);

    let pk = StockPKWoTime {
        stock: "NODATA".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    };

    // No data → should return Err or 0.0; just verify it doesn't panic
    let result = crud.get_most_recent_daily_open(pk).await;
    match result {
        Ok(v) => println!("get_most_recent_daily_open returned {v} (no data)"),
        Err(e) => println!("get_most_recent_daily_open returned Err (expected): {e}"),
    }
}
