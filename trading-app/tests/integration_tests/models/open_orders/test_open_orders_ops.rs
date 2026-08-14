//! Comprehensive DB integration tests for `OpenOrdersOps` on `OpenOrdersCRUD`.
//!
//! Tests ALL variants (Stock + Options) for `get_orders_for_strat`.
//!
//! Requires: live Postgres + DATABASE_URL. All tests #[ignore]'d.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenStockOrdersFullKeys,
    OpenStockOrdersPrimaryKeys, OptionType,
};
use trading_app::database::models_crud::open_orders::open_orders::{
    OpenOrdersCRUD, OpenOrdersFullKeys as OOFK, OpenOrdersOps,
    OpenOrdersPrimaryKeys as OOInterfacePK,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

// ============================ get_orders_for_strat — table-driven ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_orders_for_strat_stock_comprehensive() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = OpenOrdersCRUD::stock(pool.clone());

    // Test cases: (name, orders to insert, expected count)
    let test_cases: Vec<(&str, &[(i32, i32, &str)], usize)> = vec![
        ("multiple stock orders", &[(11111, 22222, "AAPL"), (33333, 44444, "MSFT")], 2),
        ("single stock order", &[(55555, 66666, "GOOG")], 1),
        ("no orders (empty)", &[], 0),
    ];

    for (name, orders, expected_count) in test_cases {
        let now = Utc::now();
        let mut pks = vec![];

        for (perm_id, order_id, stock) in orders {
            let fk = OpenStockOrdersFullKeys {
                order_perm_id: *perm_id, order_id: *order_id,
                strategy: "noise".to_string(), stock: stock.to_string(),
                primary_exchange: "NASDAQ".to_string(), currency: "USD".to_string(),
                time: now, quantity: 10.0, executions: vec![], filled: 0.0,
            };
            crud.create(&OOFK::Stock(fk)).await.expect("create failed");
            pks.push(OOInterfacePK::Stock(OpenStockOrdersPrimaryKeys {
                order_perm_id: *perm_id, order_id: *order_id,
            }));
        }

        let result = crud.get_orders_for_strat("noise").await.expect("get_orders_for_strat failed");
        let our_count = result.iter().filter(|o| {
            matches!(o, OOFK::Stock(s) if s.order_perm_id == 11111 || s.order_perm_id == 33333 || s.order_perm_id == 55555)
        }).count();
        assert_eq!(our_count, expected_count, "case '{}': expected {} orders", name, expected_count);

        for pk in &pks {
            let _ = crud.delete(pk).await;
        }
    }

    // Test nonexistent strategy
    let result = crud.get_orders_for_strat("nonexistent_strategy").await.expect("get_orders_for_strat failed");
    assert!(result.is_empty(), "nonexistent strategy → empty vec");

    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_orders_for_strat_options_comprehensive() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = OpenOrdersCRUD::option(pool.clone());

    let now = Utc::now();
    // Test cases: (name, orders, expected_count)
    // Insert 2 option orders with different strikes
    let test_orders: Vec<(i32, i32, f64, OptionType)> = vec![
        (77777, 88888, 150.0, OptionType::Call),
        (99999, 10101, 160.0, OptionType::Put),
    ];

    let mut pks = vec![];
    for (perm_id, order_id, strike, ot) in &test_orders {
        let fk = OpenOptionOrdersFullKeys {
            order_perm_id: *perm_id, order_id: *order_id,
            strategy: "noise".to_string(), stock: "AAPL".to_string(),
            primary_exchange: "NASDAQ".to_string(), currency: "USD".to_string(),
            expiry: "20250119".to_string(), strike: *strike,
            multiplier: "100".to_string(), option_type: ot.clone(),
            time: now, quantity: 5.0, executions: vec![], filled: 0.0,
        };
        crud.create(&OOFK::Options(fk)).await.expect("create failed");
        pks.push(OOInterfacePK::Options(OpenOptionOrdersPrimaryKeys {
            order_perm_id: *perm_id, order_id: *order_id,
        }));
    }

    // Test: multiple option orders
    let result = crud.get_orders_for_strat("noise").await.expect("get_orders_for_strat failed");
    let our_count = result.iter().filter(|o| {
        matches!(o, OOFK::Options(opt) if opt.order_perm_id == 77777 || opt.order_perm_id == 99999)
    }).count();
    assert_eq!(our_count, 2, "should return 2 option orders");

    // Verify the returned orders have the correct variant + fields
    for order in &result {
        if let OOFK::Options(o) = order {
            if o.order_perm_id == 77777 {
                assert_eq!(o.strike, 150.0);
                assert!(matches!(o.option_type, OptionType::Call));
            } else if o.order_perm_id == 99999 {
                assert_eq!(o.strike, 160.0);
                assert!(matches!(o.option_type, OptionType::Put));
            }
        }
    }

    // Test: nonexistent strategy
    let result = crud.get_orders_for_strat("nonexistent").await.expect("get_orders_for_strat failed");
    assert!(result.is_empty(), "nonexistent strategy → empty");

    // Cleanup
    for pk in &pks {
        let _ = crud.delete(pk).await;
    }
    del_strat!(&pool);
}
