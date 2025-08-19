use chrono::{Timelike, Utc};
use trading_app::database::{
    crud::CRUDTrait, models_crud::open_option_orders::get_open_option_orders_crud,
};

use crate::models::init::{setup_test_db, TEST_MUTEX};
use crate::{del_strat, init_strat};

macro_rules! get_crud {
    ($pool:expr) => {
        get_open_option_orders_crud($pool.clone())
    };
}
macro_rules! normal_fk {
    () => {
        &trading_app::database::models::OpenOptionOrdersFullKeys {
            order_perm_id: 1,
            order_id: 1,
            stock: "QQQ".to_string(),
            expiry: "20251122".to_string(),
            strike: 300.0,
            multiplier: "100".to_string(),
            option_type: trading_app::database::models::OptionType::Put,
            strategy: "strat_a".to_string(),
            time: Utc::now()
                .with_hour(0)
                .unwrap()
                .with_minute(0)
                .unwrap()
                .with_second(0)
                .unwrap()
                .with_nanosecond(0)
                .unwrap(),
            quantity: 9.0,
            executions: [].to_vec(),
            filled: 0.0,
        }
    };
}
macro_rules! inv_fk {
    () => {
        &trading_app::database::models::OpenOptionOrdersFullKeys {
            order_perm_id: 1,
            order_id: 1,
            stock: "QQQ".to_string(),
            expiry: "20251122".to_string(),
            strike: 300.0,
            multiplier: "100".to_string(),
            option_type: trading_app::database::models::OptionType::Put,
            strategy: "strat_a".to_string(),
            time: Utc::now()
                .with_hour(0)
                .unwrap()
                .with_minute(0)
                .unwrap()
                .with_second(0)
                .unwrap()
                .with_nanosecond(0)
                .unwrap(),
            quantity: 0.0,
            executions: [].to_vec(),
            filled: 9.0,
        }
    };
}
macro_rules! normal_pk {
    () => {
        &trading_app::database::models::OpenOptionOrdersPrimaryKeys {
            order_id: 1,
            order_perm_id: 1,
        }
    };
}
macro_rules! normal_uk {
    () => {
        &trading_app::database::models::OpenOptionOrdersUpdateKeys {
            stock: Some("QQQ".to_string()),
            expiry: Some("20251122".to_string()),
            strike: Some(300.0),
            multiplier: Some("100".to_string()),
            option_type: Some(trading_app::database::models::OptionType::Put),
            strategy: Some("strat_a".to_string()),
            time: Some(
                Utc::now()
                    .with_hour(0)
                    .unwrap()
                    .with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap()
                    .with_nanosecond(0)
                    .unwrap(),
            ),
            quantity: Some(9.0),
            executions: Some([].to_vec()),
            filled: Some(0.0),
        }
    };
}
macro_rules! inv_uk {
    () => {
        &trading_app::database::models::OpenOptionOrdersUpdateKeys {
            stock: Some("QQQ".to_string()),
            strategy: Some("strat_a".to_string()),
            expiry: Some("20251122".to_string()),
            strike: Some(300.0),
            multiplier: Some("100".to_string()),
            option_type: Some(trading_app::database::models::OptionType::Put),
            time: Some(
                Utc::now()
                    .with_hour(0)
                    .unwrap()
                    .with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap()
                    .with_nanosecond(0)
                    .unwrap(),
            ),
            quantity: Some(0.0),
            executions: Some([].to_vec()),
            filled: Some(9.0),
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
        assert_eq!($data.stock, "QQQ");
        assert_eq!($data.strategy, "strat_a");
        assert_eq!($data.quantity, 9.0);
        assert_eq!($data.executions.len(), 0);
        assert_eq!($data.filled, 0.0);
    };
}
macro_rules! inv_assert_opt {
    ($data:expr) => {
        assert_eq!($data.stock, "QQQ");
        assert_eq!($data.strategy, "strat_a");
        assert_eq!($data.quantity, 0.0);
        assert_eq!($data.executions.len(), 0);
        assert_eq!($data.filled, 9.0);
    };
}

#[tokio::test]
async fn test_create() {
    let _lock = TEST_MUTEX.lock().await;;
    let pool = setup_test_db().await;
    init_strat!(pool);

    let crud = get_crud!(pool);
    normal_create!(crud);
    let data = normal_read!(crud);
    normal_assert_opt!(data.clone());

    normal_del!(crud);
    let data_count = normal_read_all!(crud);
    assert_eq!(data_count.len(), 0);

    del_strat!(pool);
}

#[tokio::test]
async fn test_create_or_ignore() {
    let _lock = TEST_MUTEX.lock().await;;
    let pool = setup_test_db().await;
    init_strat!(pool);

    let crud = get_crud!(pool);
    normal_create!(crud);
    inv_create_or_ignore!(crud);
    let data = normal_read!(crud);
    normal_assert_opt!(data.clone());

    normal_del!(crud);
    let data_count = normal_read_all!(crud);
    assert_eq!(data_count.len(), 0);

    del_strat!(pool);
}

#[tokio::test]
async fn test_create_or_update() {
    let _lock = TEST_MUTEX.lock().await;;
    let pool = setup_test_db().await;
    init_strat!(pool);

    let crud = get_crud!(pool);
    normal_create!(crud);
    inv_create_or_update!(crud);
    let data = normal_read!(crud);
    inv_assert_opt!(data.clone());

    normal_del!(crud);
    let data_count = normal_read_all!(crud);
    assert_eq!(data_count.len(), 0);

    del_strat!(pool);
}

#[tokio::test]
async fn test_create_update() {
    let _lock = TEST_MUTEX.lock().await;;
    let pool = setup_test_db().await;
    init_strat!(pool);

    let crud = get_crud!(pool);
    normal_create!(crud);
    inv_update!(crud);
    let data = normal_read!(crud);
    inv_assert_opt!(data.clone());

    normal_del!(crud);
    let data_count = normal_read_all!(crud);
    assert_eq!(data_count.len(), 0);

    del_strat!(pool);
}

#[tokio::test]
async fn test_create_or_update_first() {
    let _lock = TEST_MUTEX.lock().await;;
    let pool = setup_test_db().await;
    init_strat!(pool);

    let crud = get_crud!(pool);
    normal_create_or_update!(crud);
    let data = normal_read!(crud);
    normal_assert_opt!(data.clone());

    normal_del!(crud);
    let data_count = normal_read_all!(crud);
    assert_eq!(data_count.len(), 0);

    del_strat!(pool);
}

#[tokio::test]
async fn test_create_or_ignore_first() {
    let _lock = TEST_MUTEX.lock().await;;
    let pool = setup_test_db().await;
    init_strat!(pool);

    let crud = get_crud!(pool);
    normal_create_or_ignore!(crud);
    let data = normal_read!(crud);
    normal_assert_opt!(data.clone());

    normal_del!(crud);
    let data_count = normal_read_all!(crud);
    assert_eq!(data_count.len(), 0);

    del_strat!(pool);
}
