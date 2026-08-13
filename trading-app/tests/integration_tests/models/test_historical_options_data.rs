//! DB integration test for `market_data.historical_options_data` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{HistoricalOptionsDataFullKeys, HistoricalOptionsDataPrimaryKeys, OptionType};
use trading_app::database::models_crud::historical_data::historical_options_data::HistoricalOptionsDataCRUD;

use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_historical_options_data_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_options_data_crud(pool);

    let now = Utc::now();
    let fk = HistoricalOptionsDataFullKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        expiry: "20250119".to_string(),
        strike: 150.0,
        multiplier: "100".to_string(),
        option_type: OptionType::Call,
        time: now,
        open: 3.50,
        high: 4.00,
        low: 3.25,
        close: 3.75,
        volume: Decimal::new(500, 0),
    };

    crud.create(&fk).await.expect("create failed");

    let pk = HistoricalOptionsDataPrimaryKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        expiry: "20250119".to_string(),
        strike: 150.0,
        multiplier: "100".to_string(),
        option_type: OptionType::Call,
        time: now,
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.stock, "AAPL");
    assert_eq!(data.close, 3.75);

    crud.delete(&pk).await.expect("delete failed");
}
