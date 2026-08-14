//! DB integration tests for `trading.strategy` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.

use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    Status, StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys,
};
use trading_app::database::models_crud::strategy::StrategyCRUD;

use crate::models::init::{TEST_MUTEX, setup_test_db};

fn make_fk(name: &str) -> StrategyFullKeys {
    StrategyFullKeys { strategy: name.to_string(), status: Status::Active }
}

fn make_pk(name: &str) -> StrategyPrimaryKeys {
    StrategyPrimaryKeys { strategy: name.to_string() }
}

fn uk(status: Option<Status>) -> StrategyUpdateKeys {
    StrategyUpdateKeys { status }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = StrategyCRUD::new(pool);

    let fk = make_fk("strat_crd");
    let pk = make_pk(&fk.strategy);
    crud.create(&fk).await.expect("create failed");

    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert_eq!(data.strategy, fk.strategy);
    assert!(matches!(data.status, Status::Active));

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read(&pk).await.expect("read failed").is_none());
}

#[tokio::test]
async fn test_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = StrategyCRUD::new(pool);

    let fk = make_fk("strat_upd");
    let pk = make_pk(&fk.strategy);
    crud.create(&fk).await.expect("create failed");

    crud.update(&pk, &uk(Some(Status::Inactive))).await.expect("update failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert!(matches!(data.status, Status::Inactive), "status should be updated to Inactive");

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = StrategyCRUD::new(pool);

    let fk_a = make_fk("strat_ra_a");
    let fk_b = make_fk("strat_ra_b");
    let pk_a = make_pk(&fk_a.strategy);
    let pk_b = make_pk(&fk_b.strategy);
    crud.create(&fk_a).await.expect("create A failed");
    crud.create(&fk_b).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all.iter()
        .filter(|p| p.strategy == "strat_ra_a" || p.strategy == "strat_ra_b")
        .collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&pk_a).await.expect("delete A failed");
    crud.delete(&pk_b).await.expect("delete B failed");
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = StrategyCRUD::new(pool);

    let fk = make_fk("strat_coi");
    let pk = make_pk(&fk.strategy);

    crud.create_or_ignore(&fk).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert!(matches!(data.status, Status::Active));

    let mut fk2 = fk.clone();
    fk2.status = Status::Inactive;
    crud.create_or_ignore(&fk2).await.expect("conflict path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert!(matches!(data.status, Status::Active), "conflict path should NOT update — still Active");

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_create_or_update_insert_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = StrategyCRUD::new(pool);

    let name = "strat_cou_ins";
    let pk = make_pk(name);
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &uk(Some(Status::Active))).await.expect("insert path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert!(matches!(data.status, Status::Active));

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = StrategyCRUD::new(pool);

    let name = "strat_cou_upd";
    let pk = make_pk(name);
    crud.create(&make_fk(name)).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &uk(Some(Status::Inactive))).await.expect("update path failed");
    let data = crud.read(&pk).await.expect("read failed").expect("expected row");
    assert!(matches!(data.status, Status::Inactive), "update path should change to Inactive");

    crud.delete(&pk).await.expect("delete failed");
}
