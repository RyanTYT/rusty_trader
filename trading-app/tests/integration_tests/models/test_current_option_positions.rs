//! DB integration test for `trading.current_option_positions` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys, OptionType,
};
use trading_app::database::models_crud::current_positions::current_option_positions::CurrentOptionPositionsCRUD;

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_current_option_positions_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::current_option_positions_crud(pool.clone());

    let fk = CurrentOptionPositionsFullKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        strategy: "noise".to_string(),
        expiry: "20250119".to_string(),
        strike: 150.0,
        multiplier: "100".to_string(),
        option_type: OptionType::Call,
        quantity: 5.0,
        avg_price: 3.50,
        last_updated: chrono::Utc::now(),
    };

    crud.create(&fk).await.expect("create failed");

    let pk = CurrentOptionPositionsPrimaryKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        strategy: "noise".to_string(),
        expiry: "20250119".to_string(),
        strike: 150.0,
        multiplier: "100".to_string(),
        option_type: OptionType::Call,
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.stock, "AAPL");
    assert_eq!(data.strike, 150.0);
    assert!(matches!(data.option_type, OptionType::Call));

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
