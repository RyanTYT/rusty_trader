//! DB integration test for `trading.open_option_orders` CRUD lifecycle.
//! Requires: live Postgres + DATABASE_URL.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{OptionType, OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys};
use trading_app::database::models_crud::open_orders::open_option_orders::OpenOptionOrdersCRUD;

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

#[tokio::test]
async fn test_open_option_orders_crud() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = trading_app::test_internals::open_option_orders_crud(pool.clone());

    let now = Utc::now();
    let fk = OpenOptionOrdersFullKeys {
        order_perm_id: 33333,
        order_id: 44444,
        strategy: "noise".to_string(),
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        expiry: "20250119".to_string(),
        strike: 150.0,
        multiplier: "100".to_string(),
        option_type: OptionType::Call,
        time: now,
        quantity: 5.0,
        executions: vec![],
        filled: 0.0,
    };

    crud.create(&fk).await.expect("create failed");

    let pk = OpenOptionOrdersPrimaryKeys {
        order_perm_id: 33333,
        order_id: 44444,
    };

    let data = crud
        .read(&pk)
        .await
        .expect("read failed")
        .expect("expected row");
    assert_eq!(data.order_id, 44444);
    assert_eq!(data.strike, 150.0);

    crud.delete(&pk).await.expect("delete failed");
    del_strat!(&pool);
}
