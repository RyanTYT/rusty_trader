//! DB integration tests for interface-CRUD variant dispatch.
//!
//! The five interface enums (`CurrentPositionsCRUD`, `OpenOrdersCRUD`,
//! `TransactionsCRUD`, `TargetPositionsCRUD`, `HistoricalDataCRUD`) dispatch
//! on matching key variants. Tests verify:
//! - `from(asset_type, pool)` constructor dispatches to the right variant
//! - Matching variant pairs succeed (create→read→delete roundtrip)
//! - Mismatched variant pairs return `Err("Invalid key variant combination...")`
//!
//! Requires: live Postgres + DATABASE_URL. All tests #[ignore]'d.

use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    AssetType, CurrentOptionPositionsPrimaryKeys, CurrentStockPositionsFullKeys,
    CurrentStockPositionsPrimaryKeys, OpenOptionOrdersPrimaryKeys, OptionTransactionsPrimaryKeys,
    TargetOptionPositionsPrimaryKeys,
};
use trading_app::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsPrimaryKeys as CPInterfacePK,
};
use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, HistoricalDataPrimaryKeys,
};
use trading_app::database::models_crud::open_orders::open_orders::{
    OpenOrdersCRUD, OpenOrdersPrimaryKeys as OOInterfacePK,
};
use trading_app::database::models_crud::target_positions::target_positions::{
    TargetPositionsCRUD, TargetPositionsPrimaryKeys as TPInterfacePK,
};
use trading_app::database::models_crud::transactions::transactions::{
    TransactionsCRUD, TransactionsPrimaryKeys as TxInterfacePK,
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

// ============================ Matching variant roundtrip ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_current_positions_matching_variant_roundtrip() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::stock(pool.clone());

    let fk = CurrentStockPositionsFullKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        strategy: "noise".to_string(),
        quantity: 100.0,
        avg_price: 150.0,
        last_updated: chrono::Utc::now(),
    };
    // Wrap in the interface enum
    let interface_fk = trading_app::database::models_crud::current_positions::current_positions::CurrentPositionsFullKeys::Stock(fk);
    crud.create(&interface_fk).await.expect("create failed");

    let pk = CurrentStockPositionsPrimaryKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        strategy: "noise".to_string(),
    };
    let data = crud.read(&CPInterfacePK::Stock(pk.clone())).await.expect("read failed");
    assert!(data.is_some(), "expected row");
    let data = data.unwrap();
    use trading_app::database::models_crud::current_positions::current_positions::CurrentPositionsFullKeys as CPFK;
    match data {
        CPFK::Stock(s) => {
            assert_eq!(s.stock, "AAPL");
            assert_eq!(s.quantity, 100.0);
        }
        _ => panic!("expected Stock variant"),
    }

    crud.delete(&CPInterfacePK::Stock(pk)).await.expect("delete failed");
    del_strat!(&pool);
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
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        strategy: "noise".to_string(),
        expiry: "20250119".to_string(),
        strike: 150.0,
        multiplier: "100".to_string(),
        option_type: trading_app::database::models::OptionType::Call,
    });
    let result = crud.read(&mismatched_pk).await;
    assert!(result.is_err(), "mismatched variant should return Err");
    assert!(
        result.unwrap_err().contains("Invalid key variant combination"),
        "should mention Invalid key variant combination"
    );
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_open_orders_mismatched_variant_returns_err() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = OpenOrdersCRUD::stock(pool.clone());
    let mismatched_pk = OOInterfacePK::Options(OpenOptionOrdersPrimaryKeys {
        order_perm_id: 11111,
        order_id: 22222,
    });
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
    let mismatched_pk = TxInterfacePK::Options(OptionTransactionsPrimaryKeys {
        execution_id: "exec_001".to_string(),
    });
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
        strategy: "noise".to_string(),
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        expiry: "20250119".to_string(),
        strike: 150.0,
        multiplier: "100".to_string(),
        option_type: trading_app::database::models::OptionType::Put,
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
            pair: "EUR/USD".to_string(),
            time: chrono::Utc::now(),
        },
    );
    let result = crud.read(&mismatched_pk).await;
    assert!(result.is_err(), "mismatched variant should return Err");
}
