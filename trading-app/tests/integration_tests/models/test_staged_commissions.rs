//! DB integration test for `trading.staged_commissions` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys};
use trading_app::database::models_crud::staged_commissions::StagedCommissionsCRUD;

use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_staged_commissions_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = StagedCommissionsCRUD::new(pool);

    let fk = StagedCommissionsFullKeys {
        execution_id: "test_exec_001".to_string(),
        fees: Decimal::new(250, 2), // 2.50
    };

    crud.create(&fk).await.expect("create failed");

    let pk = StagedCommissionsPrimaryKeys {
        execution_id: "test_exec_001".to_string(),
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.execution_id, "test_exec_001");
    assert_eq!(data.fees, Decimal::new(250, 2));

    crud.delete(&pk).await.expect("delete failed");
    assert!(crud.read_all().await.expect("read_all failed").is_empty());
}
