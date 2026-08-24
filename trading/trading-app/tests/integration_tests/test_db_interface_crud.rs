//! DB integration tests for interface-CRUD variant dispatch.
//!
//! Tests standard CRUDTrait roundtrips (create→read→delete) for all 5 interface
//! enums (CurrentPositionsCRUD, OpenOrdersCRUD, TargetPositionsCRUD,
//! TransactionsCRUD, HistoricalDataCRUD) + `from(asset_type, pool)` constructor
//! dispatch + mismatched variant Err arms.
//!
//! Requires: live Postgres + DATABASE_URL. All tests #[ignore]'d.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    AssetType, CurrentOptionPositionsPrimaryKeys, CurrentStockPositionsFullKeys,
    CurrentStockPositionsPrimaryKeys, DailyHistoricalStockDataFullKeys, HistoricalForexDataFullKeys,
    HistoricalOptionsDataFullKeys, HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys,
    OpenOptionOrdersPrimaryKeys, OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OptionType,
    OptionTransactionsPrimaryKeys, StockTransactionsFullKeys, StockTransactionsPrimaryKeys,
    TargetOptionPositionsPrimaryKeys, TargetStockPositionsFullKeys,
    TargetStockPositionsPrimaryKeys,
};
use trading_app::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsFullKeys as CPFK,
    CurrentPositionsPrimaryKeys as CPInterfacePK,
};
use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, HistoricalDataFullKeys as HDFK, HistoricalDataPrimaryKeys,
};
use trading_app::database::models_crud::open_orders::open_orders::{
    OpenOrdersCRUD, OpenOrdersFullKeys as OOFK, OpenOrdersPrimaryKeys as OOInterfacePK,
};
use trading_app::database::models_crud::target_positions::target_positions::{
    TargetPositionsCRUD, TargetPositionsFullKeys as TPFK,
    TargetPositionsPrimaryKeys as TPInterfacePK,
};
use trading_app::database::models_crud::transactions::transactions::{
    TransactionsCRUD, TransactionsFullKeys as TxFK, TransactionsPrimaryKeys as TxInterfacePK,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

// ============================ from(asset_type, pool) dispatch ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_current_positions_from_stock() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = CurrentPositionsCRUD::from(&AssetType::Stock, pool);
    assert!(matches!(crud, CurrentPositionsCRUD::Stock(_)));
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_current_positions_from_option() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = CurrentPositionsCRUD::from(&AssetType::Option, pool);
    assert!(matches!(crud, CurrentPositionsCRUD::Options(_)));
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_open_orders_from_stock() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = OpenOrdersCRUD::from(&AssetType::Stock, pool);
    assert!(matches!(crud, OpenOrdersCRUD::Stock(_)));
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_transactions_from_option() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = TransactionsCRUD::from(&AssetType::Option, pool);
    assert!(matches!(crud, TransactionsCRUD::Options(_)));
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_target_positions_from_stock() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = TargetPositionsCRUD::from(&AssetType::Stock, pool);
    assert!(matches!(crud, TargetPositionsCRUD::Stock(_)));
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_historical_data_from_forex() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::from(&AssetType::ForexPair, pool);
    assert!(matches!(crud, HistoricalDataCRUD::Forex(_)));
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
#[should_panic(expected = "Unknown Asset Type")]
async fn test_current_positions_from_unknown_panics() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let _ = CurrentPositionsCRUD::from(&AssetType::Unknown, pool);
}

// ============================ Matching variant roundtrips ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_current_positions_matching_variant_roundtrip() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::stock(pool.clone());

    let fk = CurrentStockPositionsFullKeys {
        stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), strategy: "noise".to_string(),
        quantity: 100.0, avg_price: 150.0, last_updated: chrono::Utc::now(),
    };
    crud.create(&CPFK::Stock(fk)).await.expect("create failed");

    let pk = CPInterfacePK::Stock(CurrentStockPositionsPrimaryKeys {
        stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), strategy: "noise".to_string(),
    });
    let data = crud.read(&pk).await.expect("read failed");
    assert!(data.is_some(), "expected row");
    match data.unwrap() {
        CPFK::Stock(s) => assert_eq!(s.stock, "AAPL"),
        _ => panic!("expected Stock variant"),
    }
    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_open_orders_stock_roundtrip() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = OpenOrdersCRUD::stock(pool.clone());

    let now = Utc::now();
    let fk = OpenStockOrdersFullKeys {
        order_perm_id: 11111, order_id: 22222,
        strategy: "noise".to_string(), stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(), currency: "USD".to_string(),
        time: now, quantity: 10.0, executions: vec![], filled: 0.0,
    };
    crud.create(&OOFK::Stock(fk)).await.expect("create failed");

    let pk = OOInterfacePK::Stock(OpenStockOrdersPrimaryKeys { order_perm_id: 11111, order_id: 22222 });
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    match data {
        OOFK::Stock(s) => assert_eq!(s.order_id, 22222),
        _ => panic!("expected Stock variant"),
    }
    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_target_positions_stock_roundtrip() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = TargetPositionsCRUD::stock(pool.clone());

    let fk = TargetStockPositionsFullKeys {
        strategy: "noise".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), stock: "AAPL".to_string(),
        avg_price: 150.0, quantity: 50.0,
    };
    crud.create(&TPFK::Stock(fk)).await.expect("create failed");

    let pk = TPInterfacePK::Stock(TargetStockPositionsPrimaryKeys {
        strategy: "noise".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), stock: "AAPL".to_string(),
    });
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    match data {
        TPFK::Stock(s) => assert_eq!(s.stock, "AAPL"),
        _ => panic!("expected Stock variant"),
    }
    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_transactions_stock_roundtrip() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = TransactionsCRUD::stock(pool.clone());

    let now = Utc::now();
    let fk = StockTransactionsFullKeys {
        execution_id: "rt_txn_001".to_string(), strategy: "noise".to_string(),
        stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), order_perm_id: 12345, time: now,
        price: 150.0, quantity: 10.0, fees: Decimal::new(50, 2),
    };
    crud.create(&TxFK::Stock(fk)).await.expect("create failed");

    let pk = TxInterfacePK::Stock(StockTransactionsPrimaryKeys { execution_id: "rt_txn_001".to_string() });
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    match data {
        TxFK::Stock(s) => assert_eq!(s.execution_id, "rt_txn_001"),
        _ => panic!("expected Stock variant"),
    }
    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_historical_data_stock_roundtrip() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::stock(pool);

    let now = Utc::now();
    let fk = HistoricalStockDataFullKeys {
        stock: "HISTRT".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), time: now,
        open: 150.0, high: 155.0, low: 149.0, close: 152.0,
        volume: Decimal::new(100000, 0),
    };
    crud.create(&HDFK::Stock(fk)).await.expect("create failed");

    let pk = HistoricalDataPrimaryKeys::Stock(HistoricalStockDataPrimaryKeys {
        stock: "HISTRT".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), time: now,
    });
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    match data {
        HDFK::Stock(s) => assert_eq!(s.stock, "HISTRT"),
        _ => panic!("expected Stock variant"),
    }
    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_historical_data_forex_roundtrip() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::forex(pool);

    let now = Utc::now();
    let fk = HistoricalForexDataFullKeys {
        pair: "EUR/USD".to_string(), time: now,
        bid_open: Some(1.0850), bid_high: Some(1.0870), bid_low: Some(1.0840), bid_close: Some(1.0860),
        ask_open: Some(1.0852), ask_high: Some(1.0872), ask_low: Some(1.0842), ask_close: Some(1.0862),
    };
    crud.create(&HDFK::Forex(fk)).await.expect("create failed");

    let pk = HistoricalDataPrimaryKeys::Forex(
        trading_app::database::models::HistoricalForexDataPrimaryKeys { pair: "EUR/USD".to_string(), time: now },
    );
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    match data {
        HDFK::Forex(f) => assert_eq!(f.pair, "EUR/USD"),
        _ => panic!("expected Forex variant"),
    }
    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_historical_data_options_roundtrip() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::option(pool);

    let now = Utc::now();
    let fk = HistoricalOptionsDataFullKeys {
        stock: "OPTRT".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), expiry: "20250119".to_string(),
        strike: 150.0, multiplier: "100".to_string(), option_type: OptionType::Call,
        time: now, open: 3.50, high: 4.00, low: 3.25, close: 3.75,
        volume: Decimal::new(500, 0),
    };
    crud.create(&HDFK::Options(fk)).await.expect("create failed");

    let pk = HistoricalDataPrimaryKeys::Options(
        trading_app::database::models::HistoricalOptionsDataPrimaryKeys {
            stock: "OPTRT".to_string(), primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(), expiry: "20250119".to_string(),
            strike: 150.0, multiplier: "100".to_string(), option_type: OptionType::Call, time: now,
        },
    );
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    match data {
        HDFK::Options(o) => assert_eq!(o.stock, "OPTRT"),
        _ => panic!("expected Options variant"),
    }
    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_historical_data_daily_stock_roundtrip() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::daily_stock(pool);

    let now = Utc::now();
    let fk = DailyHistoricalStockDataFullKeys {
        stock: "DLYRT".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), day: now,
        open: 150.0, high: 155.0,
        low: 149.0, close: 152.0,
        volume: Decimal::new(1000000, 0),
    };
    crud.create(&HDFK::DailyStock(fk)).await.expect("create failed");

    let pk = HistoricalDataPrimaryKeys::DailyStock(
        trading_app::database::models::DailyHistoricalStockDataPrimaryKeys {
            stock: "DLYRT".to_string(), primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(), day: now,
        },
    );
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    match data {
        HDFK::DailyStock(s) => assert_eq!(s.stock, "DLYRT"),
        _ => panic!("expected DailyStock variant"),
    }
    crud.delete(&pk).await.expect("delete failed");
}

// ============================ Mismatched variant returns Err ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_current_positions_mismatched_variant_returns_err() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::stock(pool.clone());
    let mismatched_pk = CPInterfacePK::Options(CurrentOptionPositionsPrimaryKeys {
        stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), strategy: "noise".to_string(),
        expiry: "20250119".to_string(), strike: 150.0, multiplier: "100".to_string(),
        option_type: OptionType::Call,
    });
    let result = crud.read(&mismatched_pk).await;
    assert!(result.is_err(), "mismatched variant should return Err");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_open_orders_mismatched_variant_returns_err() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = OpenOrdersCRUD::stock(pool.clone());
    let mismatched_pk = OOInterfacePK::Options(OpenOptionOrdersPrimaryKeys { order_perm_id: 11111, order_id: 22222 });
    let result = crud.read(&mismatched_pk).await;
    assert!(result.is_err(), "mismatched variant should return Err");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_transactions_mismatched_variant_returns_err() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = TransactionsCRUD::stock(pool.clone());
    let mismatched_pk = TxInterfacePK::Options(OptionTransactionsPrimaryKeys { execution_id: "exec_001".to_string() });
    let result = crud.read(&mismatched_pk).await;
    assert!(result.is_err(), "mismatched variant should return Err");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_target_positions_mismatched_variant_returns_err() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = TargetPositionsCRUD::stock(pool.clone());
    let mismatched_pk = TPInterfacePK::Options(TargetOptionPositionsPrimaryKeys {
        strategy: "noise".to_string(), stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(), currency: "USD".to_string(),
        expiry: "20250119".to_string(), strike: 150.0, multiplier: "100".to_string(),
        option_type: OptionType::Put,
    });
    let result = crud.read(&mismatched_pk).await;
    assert!(result.is_err(), "mismatched variant should return Err");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_historical_data_mismatched_variant_returns_err() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::stock(pool);
    let mismatched_pk = HistoricalDataPrimaryKeys::Forex(
        trading_app::database::models::HistoricalForexDataPrimaryKeys {
            pair: "EUR/USD".to_string(), time: chrono::Utc::now(),
        },
    );
    let result = crud.read(&mismatched_pk).await;
    assert!(result.is_err(), "mismatched variant should return Err");
}
