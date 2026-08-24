//! DB integration tests for `trading.notification` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.

use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys,
};

use crate::models::init::{TEST_MUTEX, setup_test_db};

fn make_fk(title: &str) -> NotificationFullKeys {
    NotificationFullKeys {
        title: title.to_string(),
        body: "test body".to_string(),
        alert_type: "INFO".to_string(),
    }
}

fn make_pk(title: &str) -> NotificationPrimaryKeys {
    NotificationPrimaryKeys { title: title.to_string() }
}

fn uk(body: Option<String>, alert_type: Option<String>) -> NotificationUpdateKeys {
    NotificationUpdateKeys { body, alert_type }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::notification_crud(pool.clone());

    let fk = make_fk("ntf_crd");
    let pk = make_pk(&fk.title);
    crud.create(&fk).await.expect("create failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.title, fk.title);
    assert_eq!(data.body, "test body");

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read(&pk).await.expect("read failed").is_none());
}

#[tokio::test]
async fn test_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::notification_crud(pool.clone());

    let fk = make_fk("ntf_upd");
    let pk = make_pk(&fk.title);
    crud.create(&fk).await.expect("create failed");

    crud.update(&pk, &uk(Some("updated body".to_string()), Some("WARNING".to_string()))).await.expect("update failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.body, "updated body");
    assert_eq!(data.alert_type, "WARNING");

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::notification_crud(pool.clone());

    let fk_a = make_fk("ntf_ra_a");
    let fk_b = make_fk("ntf_ra_b");
    let pk_a = make_pk(&fk_a.title);
    let pk_b = make_pk(&fk_b.title);
    crud.create(&fk_a).await.expect("create A failed");
    crud.create(&fk_b).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all.iter()
        .filter(|p| p.title == "ntf_ra_a" || p.title == "ntf_ra_b")
        .collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&pk_a).await.expect("delete A failed");
    crud.delete(&pk_b).await.expect("delete B failed");
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::notification_crud(pool.clone());

    let fk = make_fk("ntf_coi");
    let pk = make_pk(&fk.title);

    crud.create_or_ignore(&fk).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.body, "test body");

    let mut fk2 = fk.clone();
    fk2.body = "999".to_string();
    crud.create_or_ignore(&fk2).await.expect("conflict path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.body, "test body", "conflict path should NOT update");

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_create_or_update_insert_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::notification_crud(pool.clone());

    let fk = make_fk("ntf_cou_ins");
    let pk = make_pk(&fk.title);
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &uk(Some("test body".to_string()), Some("INFO".to_string()))).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.body, "test body");

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::notification_crud(pool.clone());

    let fk = make_fk("ntf_cou_upd");
    let pk = make_pk(&fk.title);
    crud.create(&fk).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &uk(Some("updated body".to_string()), Some("WARNING".to_string()))).await.expect("update path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.body, "updated body");
    assert_eq!(data.alert_type, "WARNING");

    crud.delete(&pk).await.expect("delete failed");
}
