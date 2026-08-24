//! DB integration tests for `logs.logs` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys};

use crate::models::init::{TEST_MUTEX, setup_test_db};

fn make_fk(name: &str) -> LogsFullKeys {
    LogsFullKeys {
        time: Utc::now(), level: "INFO".to_string(),
        name: name.to_string(), message: "test message".to_string(),
    }
}

fn make_pk(time: chrono::DateTime<Utc>, name: &str) -> LogsPrimaryKeys {
    LogsPrimaryKeys { time, level: "INFO".to_string(), name: name.to_string() }
}

fn uk(message: Option<String>) -> LogsUpdateKeys {
    LogsUpdateKeys { message }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::logs_crud(pool.clone());

    let fk = make_fk("logs_crd");
    let pk = make_pk(fk.time, &fk.name);
    crud.create(&fk).await.expect("create failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.name, fk.name);
    assert_eq!(data.message, "test message");

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read(&pk).await.expect("read failed").is_none());
}

#[tokio::test]
async fn test_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::logs_crud(pool.clone());

    let fk = make_fk("logs_upd");
    let pk = make_pk(fk.time, &fk.name);
    crud.create(&fk).await.expect("create failed");

    crud.update(&pk, &uk(Some("updated message".to_string()))).await.expect("update failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.message, "updated message");

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::logs_crud(pool.clone());

    let fk_a = make_fk("logs_ra_a");
    let fk_b = make_fk("logs_ra_b");
    let pk_a = make_pk(fk_a.time, &fk_a.name);
    let pk_b = make_pk(fk_b.time, &fk_b.name);
    crud.create(&fk_a).await.expect("create A failed");
    crud.create(&fk_b).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all.iter()
        .filter(|p| p.name == "logs_ra_a" || p.name == "logs_ra_b")
        .collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&pk_a).await.expect("delete A failed");
    crud.delete(&pk_b).await.expect("delete B failed");
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::logs_crud(pool.clone());

    let fk = make_fk("logs_coi");
    let pk = make_pk(fk.time, &fk.name);

    crud.create_or_ignore(&fk).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.message, "test message");

    let mut fk2 = fk.clone();
    fk2.message = "999".to_string();
    crud.create_or_ignore(&fk2).await.expect("conflict path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.message, "test message", "conflict path should NOT update");

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_create_or_update_insert_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::logs_crud(pool.clone());

    let fk = make_fk("logs_cou_ins");
    let pk = make_pk(fk.time, &fk.name);
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &uk(Some("test message".to_string()))).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.message, "test message");

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::logs_crud(pool.clone());

    let fk = make_fk("logs_cou_upd");
    let pk = make_pk(fk.time, &fk.name);
    crud.create(&fk).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &uk(Some("updated message".to_string()))).await.expect("update path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.message, "updated message");

    crud.delete(&pk).await.expect("delete failed");
}
