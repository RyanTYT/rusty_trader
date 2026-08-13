//! DB integration test for the `strategy` table CRUD lifecycle.
//!
//! Requires: live Postgres + `DATABASE_URL` env var.
//! Run with: `DATABASE_URL=... cargo test --test integration_tests test_strategy`

use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{Status, StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys};
use trading_app::database::models_crud::strategy::StrategyCRUD;

use crate::models::init::{TEST_MUTEX, setup_test_db};

fn normal_fk() -> StrategyFullKeys {
    StrategyFullKeys {
        strategy: "noise".to_string(),
        status: Status::Active,
    }
}

fn normal_pk() -> StrategyPrimaryKeys {
    StrategyPrimaryKeys {
        strategy: "noise".to_string(),
    }
}

fn normal_uk() -> StrategyUpdateKeys {
    // StrategyUpdateKeys only has `status` (the updatable field; strategy is a PK)
    StrategyUpdateKeys {
        status: Some(Status::Active),
    }
}

#[tokio::test]
async fn test_create() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = StrategyCRUD::new(pool);
    crud.create(&normal_fk()).await.expect("create failed");
    let data = crud
        .read(&normal_pk())
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.strategy, "noise");
    assert!(matches!(data.status, Status::Active));
    crud.delete(&normal_pk()).await.expect("delete failed");
    assert!(crud.read_all().await.expect("read_all failed").is_empty());
}

#[tokio::test]
async fn test_create_or_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    let crud = StrategyCRUD::new(pool);
    crud.create_or_update(&normal_pk(), &normal_uk())
        .await
        .expect("create_or_update failed");
    let data = crud
        .read(&normal_pk())
        .await
        .expect("read failed")
        .expect("expected row");
    assert!(matches!(data.status, Status::Active));
    crud.delete(&normal_pk()).await.expect("delete failed");
}
