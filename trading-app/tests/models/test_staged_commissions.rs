use rust_decimal::prelude::FromPrimitive;
use trading_app::database::{
    crud::CRUDTrait, models_crud::staged_commissions::get_staged_commissions_crud,
};

use crate::models::init::{TEST_MUTEX, setup_test_db};

macro_rules! get_crud {
    ($pool:expr) => {
        get_staged_commissions_crud($pool)
    };
}
macro_rules! normal_fk {
    () => {
        &trading_app::database::models::StagedCommissionsFullKeys {
            execution_id: "12".to_string(),
            fees: rust_decimal::Decimal::from_f64(1.0)
                .expect("Expected commission from commission_report to be valid for Decimal"),
        }
    };
}
macro_rules! inv_fk {
    () => {
        &trading_app::database::models::StagedCommissionsFullKeys {
            execution_id: "12".to_string(),
            fees: rust_decimal::Decimal::from_f64(0.0)
                .expect("Expected commission from commission_report to be valid for Decimal"),
        }
    };
}
macro_rules! normal_pk {
    () => {
        &trading_app::database::models::StagedCommissionsPrimaryKeys {
            execution_id: "12".to_string(),
        }
    };
}
macro_rules! normal_uk {
    () => {
        &trading_app::database::models::StagedCommissionsUpdateKeys {
            fees: Some(
                rust_decimal::Decimal::from_f64(1.0).expect(
                    "Expected commission from commission_report to be valid for Decimal",
                ),
            ),
        }
    };
}
macro_rules! inv_uk {
    () => {
        &trading_app::database::models::StagedCommissionsUpdateKeys {
            fees: Some(
                rust_decimal::Decimal::from_f64(0.0).expect(
                    "Expected commission from commission_report to be valid for Decimal",
                ),
            ),
        }
    };
}
macro_rules! normal_create {
    ($crud:expr) => {
        $crud
            .create(normal_fk!())
            .await
            .expect("Expected to be able to create historical_data")
    };
}
macro_rules! normal_create_or_update {
    ($crud:expr) => {
        $crud
            .create_or_update(normal_pk!(), normal_uk!())
            .await
            .expect("Expected to be able to create historical_data")
    };
}
macro_rules! inv_create_or_update {
    ($crud:expr) => {
        $crud
            .create_or_update(normal_pk!(), inv_uk!())
            .await
            .expect("Expected to be able to create historical_data")
    };
}
macro_rules! normal_create_or_ignore {
    ($crud:expr) => {
        $crud
            .create_or_ignore(normal_fk!())
            .await
            .expect("Expected to be able to create historical_data")
    };
}
macro_rules! inv_create_or_ignore {
    ($crud:expr) => {
        $crud
            .create_or_ignore(inv_fk!())
            .await
            .expect("Expected to be able to create historical_data")
    };
}
macro_rules! inv_update {
    ($crud:expr) => {
        $crud
            .update(normal_pk!(), inv_uk!())
            .await
            .expect("Expected to be able to create historical_data")
    };
}
macro_rules! normal_read {
    ($crud:expr) => {
        $crud
            .read(normal_pk!())
            .await
            .expect("Expected to be able to read historical_data without err")
            .expect("expected to be able to get entry from historical_data")
    };
}
macro_rules! normal_read_all {
    ($crud:expr) => {
        $crud
            .read_all()
            .await
            .expect("expected to be able to get entry from historical_data")
            .expect("Expected entries")
    };
}
macro_rules! normal_del {
    ($crud:expr) => {
        $crud
            .delete(normal_pk!())
            .await
            .expect("expected to be able to delete entry from historical_data")
    };
}
macro_rules! normal_assert_opt {
    ($data:expr) => {
        assert_eq!(
            $data.fees,
            rust_decimal::Decimal::from_f64(1.0)
                .expect("Expected commission from commission_report to be valid for Decimal")
        )
    };
}
macro_rules! inv_assert_opt {
    ($data:expr) => {
        assert_eq!(
            $data.fees,
            rust_decimal::Decimal::from_f64(0.0)
                .expect("Expected commission from commission_report to be valid for Decimal")
        )
    };
}

#[tokio::test]
async fn test_create() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;

    let crud = get_crud!(pool);
    normal_create!(crud);
    let data = normal_read!(crud);
    normal_assert_opt!(data.clone());

    normal_del!(crud);
    let data_count = normal_read_all!(crud);
    assert_eq!(data_count.len(), 0)
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;

    let crud = get_crud!(pool);
    normal_create!(crud);
    inv_create_or_ignore!(crud);
    let data = normal_read!(crud);
    normal_assert_opt!(data.clone());

    normal_del!(crud);
    let data_count = normal_read_all!(crud);
    assert_eq!(data_count.len(), 0)
}

#[tokio::test]
async fn test_create_or_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;

    let crud = get_crud!(pool);
    normal_create!(crud);
    inv_create_or_update!(crud);
    let data = normal_read!(crud);
    inv_assert_opt!(data.clone());

    normal_del!(crud);
    let data_count = normal_read_all!(crud);
    assert_eq!(data_count.len(), 0)
}

#[tokio::test]
async fn test_create_update() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;

    let crud = get_crud!(pool);
    normal_create!(crud);
    inv_update!(crud);
    let data = normal_read!(crud);
    inv_assert_opt!(data.clone());

    normal_del!(crud);
    let data_count = normal_read_all!(crud);
    assert_eq!(data_count.len(), 0)
}

#[tokio::test]
async fn test_create_or_update_first() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;

    let crud = get_crud!(pool);
    normal_create_or_update!(crud);
    let data = normal_read!(crud);
    normal_assert_opt!(data.clone());

    normal_del!(crud);
    let data_count = normal_read_all!(crud);
    assert_eq!(data_count.len(), 0)
}

#[tokio::test]
async fn test_create_or_ignore_first() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;

    let crud = get_crud!(pool);
    normal_create_or_ignore!(crud);
    let data = normal_read!(crud);
    normal_assert_opt!(data.clone());

    normal_del!(crud);
    let data_count = normal_read_all!(crud);
    assert_eq!(data_count.len(), 0)
}
