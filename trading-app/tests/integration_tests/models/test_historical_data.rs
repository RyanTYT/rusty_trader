//! DB integration test for `market_data.historical_data` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys};
use trading_app::database::models_crud::historical_data::historical_stock_data::HistoricalStockDataCRUD;

use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_historical_stock_data_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_stock_data_crud(pool);

    let now = Utc::now();
    let fk = HistoricalStockDataFullKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        time: now,
        open: 150.0,
        high: 155.0,
        low: 149.0,
        close: 152.0,
        volume: Decimal::new(100000, 0),
    };

    crud.create(&fk).await.expect("create failed");

    let pk = HistoricalStockDataPrimaryKeys {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        time: now,
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.stock, "AAPL");
    assert_eq!(data.close, 152.0);

    crud.delete(&pk).await.expect("delete failed");
}
