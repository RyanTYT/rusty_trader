//! DB integration test for `market_data.daily_historical_data` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    DailyHistoricalStockDataFullKeys, DailyHistoricalStockDataPrimaryKeys,
};
use trading_app::database::models_crud::historical_data::daily_historical_data::DailyHistoricalStockDataCRUD;

use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_daily_historical_data_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::daily_historical_stock_data_crud(pool);

    let now = Utc::now();
    let fk = DailyHistoricalStockDataFullKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        day: now,
        open: 15000.0,  // 150.00
        high: 15500.0,  // 155.00
        low: 14900.0,   // 149.00
        close: 15200.0, // 152.00
        volume: Decimal::new(1000000, 2),
    };

    crud.create(&fk).await.expect("create failed");

    let pk = DailyHistoricalStockDataPrimaryKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        day: now,
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.stock, "AAPL");
    assert_eq!(data.close, 15200.0);

    crud.delete(&pk).await.expect("delete failed");
}
