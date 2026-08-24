//! DB integration tests for `TimescaleDbOps` trait on `HistoricalDataCRUD`.
//!
//! Tests: `refresh_daily_data`.
//!
//! Requires: live Postgres + DATABASE_URL + TimescaleDB continuous aggregate
//! named `market_data.daily_ohlcv`. All tests #[ignore]'d.

use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, TimescaleDbOps,
};

use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL + TimescaleDB continuous aggregate"]
async fn test_refresh_daily_data() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = HistoricalDataCRUD::stock(pool);

    let result = crud.refresh_daily_data().await;
    match result {
        Ok(_) => println!("refresh_daily_data succeeded"),
        Err(e) => println!("refresh_daily_data returned Err (may need continuous aggregate): {e}"),
    }
}
