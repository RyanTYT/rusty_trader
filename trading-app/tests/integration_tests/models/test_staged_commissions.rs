//! DB integration tests for `trading.staged_commissions` CRUD lifecycle.
//! Comprehensively tests all 7 CRUD methods.
//! Requires: live Postgres + DATABASE_URL.

use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys,
};

use crate::models::init::{TEST_MUTEX, setup_test_db};

fn make_fk(exec_id: &str) -> StagedCommissionsFullKeys {
    StagedCommissionsFullKeys {
        execution_id: exec_id.to_string(),
        fees: Decimal::new(50, 2),
    }
}

fn make_pk(exec_id: &str) -> StagedCommissionsPrimaryKeys {
    StagedCommissionsPrimaryKeys {
        execution_id: exec_id.to_string(),
    }
}

fn uk(fees: Option<Decimal>) -> StagedCommissionsUpdateKeys {
    StagedCommissionsUpdateKeys { fees }
}

#[tokio::test]
async fn test_create_read_delete() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::staged_commissions_crud(pool.clone());

    let fk = make_fk("scm_crd");
    let pk = make_pk(&fk.execution_id);
    crud.create(&fk).await.expect("create failed");

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.execution_id, fk.execution_id);
    assert_eq!(data.fees, Decimal::new(50, 2));

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read(&pk).await.expect("read failed").is_none());
}

#[tokio::test]
async fn test_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::staged_commissions_crud(pool.clone());

    let fk = make_fk("scm_upd");
    let pk = make_pk(&fk.execution_id);
    crud.create(&fk).await.expect("create failed");

    crud.update(&pk, &uk(Some(Decimal::new(75, 2))))
        .await
        .expect("update failed");
    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.fees, Decimal::new(75, 2));

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_read_all() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::staged_commissions_crud(pool.clone());

    let fk_a = make_fk("scm_ra_a");
    let fk_b = make_fk("scm_ra_b");
    let pk_a = make_pk(&fk_a.execution_id);
    let pk_b = make_pk(&fk_b.execution_id);
    crud.create(&fk_a).await.expect("create A failed");
    crud.create(&fk_b).await.expect("create B failed");

    let all = crud.read_all().await.expect("read_all failed");
    let ours: Vec<_> = all
        .iter()
        .filter(|p| p.execution_id == "scm_ra_a" || p.execution_id == "scm_ra_b")
        .collect();
    assert_eq!(ours.len(), 2);

    crud.delete(&pk_a).await.expect("delete A failed");
    crud.delete(&pk_b).await.expect("delete B failed");
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::staged_commissions_crud(pool.clone());

    let fk = make_fk("scm_coi");
    let pk = make_pk(&fk.execution_id);

    crud.create_or_ignore(&fk)
        .await
        .expect("insert path failed");
    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.fees, Decimal::new(50, 2));

    let mut fk2 = fk.clone();
    fk2.fees = Decimal::new(999, 2);
    crud.create_or_ignore(&fk2)
        .await
        .expect("conflict path failed");
    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(
        data.fees,
        Decimal::new(50, 2),
        "conflict path should NOT update"
    );

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_create_or_update_insert_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::staged_commissions_crud(pool.clone());

    let fk = make_fk("scm_cou_ins");
    let pk = make_pk(&fk.execution_id);
    assert!(crud.read(&pk).await.expect("read failed").is_none());

    crud.create_or_update(&pk, &uk(Some(Decimal::new(50, 2))))
        .await
        .expect("insert path failed");
    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.fees, Decimal::new(50, 2));

    crud.delete(&pk).await.expect("delete failed");
}

#[tokio::test]
async fn test_create_or_update_update_path() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = trading_app::test_internals::staged_commissions_crud(pool.clone());

    let fk = make_fk("scm_cou_upd");
    let pk = make_pk(&fk.execution_id);
    crud.create(&fk).await.expect("pre-insert failed");

    crud.create_or_update(&pk, &uk(Some(Decimal::new(75, 2))))
        .await
        .expect("update path failed");
    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.fees, Decimal::new(75, 2));

    crud.delete(&pk).await.expect("delete failed");
}
