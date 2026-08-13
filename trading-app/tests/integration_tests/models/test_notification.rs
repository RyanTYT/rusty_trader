//! DB integration test for `trading.notifications` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{NotificationFullKeys, NotificationPrimaryKeys};
use trading_app::database::models_crud::notification::NotificationCRUD;

use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_notification_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::notification_crud(pool);

    let fk = NotificationFullKeys {
        title: "test_alert".to_string(),
        body: "test body".to_string(),
        alert_type: "warning".to_string(),
    };

    crud.create(&fk).await.expect("create failed");

    let pk = NotificationPrimaryKeys {
        title: "test_alert".to_string(),
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.title, "test_alert");
    assert_eq!(data.body, "test body");
    assert_eq!(data.alert_type, "warning");

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read_all().await.expect("read_all failed").is_empty());
}
