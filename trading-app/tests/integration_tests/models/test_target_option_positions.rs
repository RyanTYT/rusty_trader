//! DB integration test for `trading.target_option_positions` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    OptionType, TargetOptionPositionsFullKeys, TargetOptionPositionsPrimaryKeys,
};
use trading_app::database::models_crud::target_positions::target_option_positions::TargetOptionPositionsCRUD;

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_target_option_positions_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::target_option_positions_crud(pool.clone());

    let fk = TargetOptionPositionsFullKeys {
        strategy: "noise".to_string(),
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        expiry: "20250119".to_string(),
        strike: 150.0,
        multiplier: "100".to_string(),
        option_type: OptionType::Put,
        avg_price: 2.50,
        quantity: 10.0,
    };

    crud.create(&fk).await.expect("create failed");

    let pk = TargetOptionPositionsPrimaryKeys {
        strategy: "noise".to_string(),
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        expiry: "20250119".to_string(),
        strike: 150.0,
        multiplier: "100".to_string(),
        option_type: OptionType::Put,
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.stock, "AAPL");
    assert!(matches!(data.option_type, OptionType::Put));

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
