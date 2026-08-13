//! DB integration test for `logs.logs` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{LogsFullKeys, LogsPrimaryKeys};
use trading_app::database::models_crud::logs::LogsCRUD;

use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_logs_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::logs_crud(pool);

    let now = Utc::now();
    let fk = LogsFullKeys {
        time: now,
        level: "WARN".to_string(),
        name: "test_module".to_string(),
        message: "test warning message".to_string(),
    };

    crud.create(&fk).await.expect("create failed");

    let pk = LogsPrimaryKeys {
        time: now,
        level: "WARN".to_string(),
        name: "test_module".to_string(),
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.level, "WARN");
    assert_eq!(data.name, "test_module");
    assert_eq!(data.message, "test warning message");

    crud.delete(&pk).await.expect("delete failed");
}
