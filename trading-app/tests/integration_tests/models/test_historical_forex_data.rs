//! DB integration test for `market_data.historical_forex_data` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.
//!
//! NOTE: `HistoricalForexDataFullKeys` is a manually-defined struct (not derived
//! from `ExtractFullKeys`), so its fields remain `Option<f64>` — unlike the
//! derived `*FullKeys` structs which unwrap `Option<T>` → `T`.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeys};
use trading_app::database::models_crud::historical_data::historical_forex_data::HistoricalForexDataCRUD;

use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_historical_forex_data_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::historical_forex_data_crud(pool);

    let now = Utc::now();
    let fk = HistoricalForexDataFullKeys {
        pair: "EUR/USD".to_string(),
        time: now,
        bid_open: Some(1.0850),
        bid_high: Some(1.0870),
        bid_low: Some(1.0840),
        bid_close: Some(1.0860),
        ask_open: Some(1.0852),
        ask_high: Some(1.0872),
        ask_low: Some(1.0842),
        ask_close: Some(1.0862),
    };

    crud.create(&fk).await.expect("create failed");

    let pk = HistoricalForexDataPrimaryKeys {
        pair: "EUR/USD".to_string(),
        time: now,
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.pair, "EUR/USD");
    assert_eq!(data.bid_close, Some(1.0860));

    crud.delete(&pk).await.expect("delete failed");
}
