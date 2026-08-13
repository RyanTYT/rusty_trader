//! DB integration test for `trading.current_stock_positions` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.
//!
//! NOTE: `*FullKeys` structs (derived by ExtractFullKeys macro) have `Option<T>`
//! fields unwrapped to `T`. `*PrimaryKeys` includes all non-Option fields.

use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys,
};
use trading_app::database::models_crud::current_positions::current_stock_positions::CurrentStockPositionsCRUD;

// Import the init_strat! / del_strat! macros
use crate::init_strat;
use crate::del_strat;

use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_current_stock_positions_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::current_stock_positions_crud(pool.clone());

    // FullKeys: Option fields unwrapped to T
    let fk = CurrentStockPositionsFullKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        strategy: "noise".to_string(),
        quantity: 100.0,
        avg_price: 150.0,
        last_updated: chrono::Utc::now(),
    };

    crud.create(&fk).await.expect("create failed");

    // PrimaryKeys: all non-Option fields from the base struct
    let pk = CurrentStockPositionsPrimaryKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        strategy: "noise".to_string(),
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.stock, "AAPL");
    assert_eq!(data.quantity, 100.0);

    // UpdateKeys: keeps Option<T> — only set fields you want to update
    crud.update(
        &pk,
        &CurrentStockPositionsUpdateKeys {
            quantity: Some(200.0),
            avg_price: Some(155.0),
            last_updated: None,
        },
    )
    .await
    .expect("update failed");

    let updated = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(updated.quantity, 200.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
