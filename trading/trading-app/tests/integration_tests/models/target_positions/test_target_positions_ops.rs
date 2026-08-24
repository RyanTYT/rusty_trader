//! Comprehensive DB integration tests for `TargetPositionsOps` on `TargetPositionsCRUD`.
//!
//! Tests ALL variants (Stock + Options) for ALL methods with table-driven helpers.
//!
//! Methods: get_target_pos_diff_by_pk, get_target_pos_diff_by_strat, clear_strat_pos.
//!
//! Requires: live Postgres + DATABASE_URL. All tests #[ignore]'d.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    CurrentOptionPositionsFullKeys, CurrentStockPositionsFullKeys, OptionType,
    TargetOptionPositionsFullKeys, TargetOptionPositionsPrimaryKeys, TargetStockPositionsFullKeys,
    TargetStockPositionsPrimaryKeys,
};
use trading_app::database::models_crud::target_positions::target_positions::{
    TargetPositionsCRUD, TargetPositionsOps, TargetPositionsPrimaryKeys as TPInterfacePK,
    TargetPositionsQtyDiff,
};

use crate::del_strat;
use crate::init_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

// ============================ Table-driven test helpers ============================

/// A diff test case: (target_qty, current_qty) → expected (qty_diff, current_qty)
struct DiffCase {
    name: &'static str,
    target_qty: Option<f64>,  // None = no target row
    current_qty: Option<f64>, // None = no current row
    expected_qty_diff: f64,
    expected_current_qty: f64,
    should_have_diff: bool,
}

impl DiffCase {
    const EPS: f64 = 1e-6;
}

async fn cleanup_stock(pool: &sqlx::PgPool, stock: &str) {
    let _ = sqlx::query("DELETE FROM trading.target_stock_positions WHERE stock = $1")
        .bind(stock)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM trading.current_stock_positions WHERE stock = $1")
        .bind(stock)
        .execute(pool)
        .await;
}

// ============================ get_target_pos_diff_by_pk — Stock ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_target_pos_diff_by_pk_stock_comprehensive() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let target_crud = trading_app::test_internals::target_stock_positions_crud(pool.clone());
    let current_crud = trading_app::test_internals::current_stock_positions_crud(pool.clone());
    let diff_crud = TargetPositionsCRUD::stock(pool.clone());

    let cases = vec![
        DiffCase {
            name: "target only (no current)",
            target_qty: Some(200.0),
            current_qty: None,
            expected_qty_diff: 200.0,
            expected_current_qty: 0.0,
            should_have_diff: true,
        },
        DiffCase {
            name: "current only (no target)",
            target_qty: None,
            current_qty: Some(50.0),
            expected_qty_diff: -50.0,
            expected_current_qty: 50.0,
            should_have_diff: true,
        },
        DiffCase {
            name: "both exist, target > current",
            target_qty: Some(200.0),
            current_qty: Some(50.0),
            expected_qty_diff: 150.0,
            expected_current_qty: 50.0,
            should_have_diff: true,
        },
        DiffCase {
            name: "both exist, target < current",
            target_qty: Some(30.0),
            current_qty: Some(100.0),
            expected_qty_diff: -70.0,
            expected_current_qty: 100.0,
            should_have_diff: true,
        },
        DiffCase {
            name: "both exist, equal (diff=0)",
            target_qty: Some(100.0),
            current_qty: Some(100.0),
            expected_qty_diff: 0.0,
            expected_current_qty: 100.0,
            should_have_diff: true,
        },
        DiffCase {
            name: "neither exists",
            target_qty: None,
            current_qty: None,
            expected_qty_diff: 0.0,
            expected_current_qty: 0.0,
            should_have_diff: false,
        },
    ];

    for case in &cases {
        let stock = format!(
            "DIFFTEST_{}",
            case.name
                .replace(" ", "_")
                .replace("(", "")
                .replace(")", "")
        );
        cleanup_stock(&pool, &stock).await;

        // Insert target if Some
        if let Some(qty) = case.target_qty {
            target_crud
                .create(&TargetStockPositionsFullKeys {
                    strategy: "noise".to_string(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                    stock: stock.clone(),
                    avg_price: 150.0,
                    quantity: qty,
                })
                .await
                .expect("create target failed");
        }

        // Insert current if Some
        if let Some(qty) = case.current_qty {
            current_crud
                .create(&CurrentStockPositionsFullKeys {
                    stock: stock.clone(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                    strategy: "noise".to_string(),
                    quantity: qty,
                    avg_price: 150.0,
                    last_updated: Utc::now(),
                })
                .await
                .expect("create current failed");
        }

        let pk = TPInterfacePK::Stock(TargetStockPositionsPrimaryKeys {
            strategy: "noise".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            stock: stock.clone(),
        });
        let diffs = diff_crud
            .get_target_pos_diff_by_pk(pk)
            .await
            .expect("get_target_pos_diff_by_pk failed");

        if case.should_have_diff {
            assert!(
                !diffs.is_empty(),
                "case '{}': should have a diff",
                case.name
            );
            match &diffs[0] {
                TargetPositionsQtyDiff::Stock(d) => {
                    assert!(
                        (d.qty_diff - case.expected_qty_diff).abs() < DiffCase::EPS,
                        "case '{}': expected qty_diff {}, got {}",
                        case.name,
                        case.expected_qty_diff,
                        d.qty_diff
                    );
                    assert!(
                        (d.current_qty - case.expected_current_qty).abs() < DiffCase::EPS,
                        "case '{}': expected current_qty {}, got {}",
                        case.name,
                        case.expected_current_qty,
                        d.current_qty
                    );
                }
                _ => panic!("case '{}': expected Stock variant", case.name),
            }
        } else {
            assert!(
                diffs.is_empty(),
                "case '{}': neither exists → empty",
                case.name
            );
        }

        cleanup_stock(&pool, &stock).await;
    }
    del_strat!(&pool);
}

// ============================ get_target_pos_diff_by_pk — Options ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_target_pos_diff_by_pk_options_comprehensive() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let target_crud = trading_app::test_internals::target_option_positions_crud(pool.clone());
    let current_crud = trading_app::test_internals::current_option_positions_crud(pool.clone());
    let diff_crud = TargetPositionsCRUD::option(pool.clone());

    let cases = vec![
        DiffCase {
            name: "target only (no current)",
            target_qty: Some(10.0),
            current_qty: None,
            expected_qty_diff: 10.0,
            expected_current_qty: 0.0,
            should_have_diff: true,
        },
        DiffCase {
            name: "current only (no target)",
            target_qty: None,
            current_qty: Some(3.0),
            expected_qty_diff: -3.0,
            expected_current_qty: 3.0,
            should_have_diff: true,
        },
        DiffCase {
            name: "both exist, target > current",
            target_qty: Some(10.0),
            current_qty: Some(3.0),
            expected_qty_diff: 7.0,
            expected_current_qty: 3.0,
            should_have_diff: true,
        },
        DiffCase {
            name: "neither exists",
            target_qty: None,
            current_qty: None,
            expected_qty_diff: 0.0,
            expected_current_qty: 0.0,
            should_have_diff: false,
        },
    ];

    for case in &cases {
        let stock = format!(
            "OPTDIFF_{}",
            case.name
                .replace(" ", "_")
                .replace("(", "")
                .replace(")", "")
        );
        // Cleanup option tables
        let _ = sqlx::query("DELETE FROM trading.target_option_positions WHERE stock = $1")
            .bind(&stock)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM trading.current_option_positions WHERE stock = $1")
            .bind(&stock)
            .execute(&pool)
            .await;

        if let Some(qty) = case.target_qty {
            target_crud
                .create(&TargetOptionPositionsFullKeys {
                    strategy: "noise".to_string(),
                    stock: stock.clone(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                    expiry: "20250119".to_string(),
                    strike: 150.0,
                    multiplier: "100".to_string(),
                    option_type: OptionType::Call,
                    avg_price: 3.50,
                    quantity: qty,
                })
                .await
                .expect("create target failed");
        }

        if let Some(qty) = case.current_qty {
            current_crud
                .create(&CurrentOptionPositionsFullKeys {
                    stock: stock.clone(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                    strategy: "noise".to_string(),
                    expiry: "20250119".to_string(),
                    strike: 150.0,
                    multiplier: "100".to_string(),
                    option_type: OptionType::Call,
                    quantity: qty,
                    avg_price: 3.50,
                    last_updated: Utc::now(),
                })
                .await
                .expect("create current failed");
        }

        let pk = TPInterfacePK::Options(TargetOptionPositionsPrimaryKeys {
            strategy: "noise".to_string(),
            stock: stock.clone(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            expiry: "20250119".to_string(),
            strike: 150.0,
            multiplier: "100".to_string(),
            option_type: OptionType::Call,
        });
        let diffs = diff_crud
            .get_target_pos_diff_by_pk(pk)
            .await
            .expect("get_target_pos_diff_by_pk failed");

        if case.should_have_diff {
            assert!(!diffs.is_empty(), "case '{}': should have diff", case.name);
            match &diffs[0] {
                TargetPositionsQtyDiff::Options(d) => {
                    assert!(
                        (d.qty_diff - case.expected_qty_diff).abs() < DiffCase::EPS,
                        "case '{}': expected qty_diff {}, got {}",
                        case.name,
                        case.expected_qty_diff,
                        d.qty_diff
                    );
                }
                _ => panic!("case '{}': expected Options variant", case.name),
            }
        } else {
            assert!(
                diffs.is_empty(),
                "case '{}': neither exists → empty",
                case.name
            );
        }

        let _ = sqlx::query("DELETE FROM trading.target_option_positions WHERE stock = $1")
            .bind(&stock)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM trading.current_option_positions WHERE stock = $1")
            .bind(&stock)
            .execute(&pool)
            .await;
    }
    del_strat!(&pool);
}

// ============================ get_target_pos_diff_by_strat ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_target_pos_diff_by_strat_stock_multiple() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let target_crud = trading_app::test_internals::target_stock_positions_crud(pool.clone());
    let diff_crud = TargetPositionsCRUD::stock(pool.clone());

    // Insert targets for multiple stocks
    for (stock, qty) in [("STRAT1", 100.0), ("STRAT2", 200.0), ("STRAT3", 50.0)] {
        target_crud
            .create(&TargetStockPositionsFullKeys {
                strategy: "noise".to_string(),
                primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(),
                stock: stock.to_string(),
                avg_price: 150.0,
                quantity: qty,
            })
            .await
            .expect("create target failed");
    }

    let diffs = diff_crud
        .get_target_pos_diff_by_strat("noise")
        .await
        .expect("get_target_pos_diff_by_strat failed");
    let our_count = diffs.iter().filter(|d| {
        matches!(d, TargetPositionsQtyDiff::Stock(s) if s.stock == "STRAT1" || s.stock == "STRAT2" || s.stock == "STRAT3")
    }).count();
    assert_eq!(our_count, 3, "should return diffs for all 3 stocks");

    // Cleanup
    for stock in ["STRAT1", "STRAT2", "STRAT3"] {
        let _ = target_crud
            .delete(&TargetStockPositionsPrimaryKeys {
                strategy: "noise".to_string(),
                primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(),
                stock: stock.to_string(),
            })
            .await;
    }
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_target_pos_diff_by_strat_empty() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let diff_crud = TargetPositionsCRUD::stock(pool.clone());

    let diffs = diff_crud
        .get_target_pos_diff_by_strat("nonexistent_strategy")
        .await
        .expect("failed");
    assert!(diffs.is_empty(), "nonexistent strategy → empty");
    del_strat!(&pool);
}

// ============================ clear_strat_pos ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_clear_strat_pos_stock_deletes_and_noop() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let target_crud = trading_app::test_internals::target_stock_positions_crud(pool.clone());
    let diff_crud = TargetPositionsCRUD::stock(pool.clone());

    // Insert 3 targets
    for stock in ["CLEAR1", "CLEAR2", "CLEAR3"] {
        target_crud
            .create(&TargetStockPositionsFullKeys {
                strategy: "noise".to_string(),
                primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(),
                stock: stock.to_string(),
                avg_price: 150.0,
                quantity: 100.0,
            })
            .await
            .expect("create target failed");
    }

    // Clear
    diff_crud
        .clear_strat_pos("noise")
        .await
        .expect("clear_strat_pos failed");

    // Verify cleared
    let diffs = diff_crud
        .get_target_pos_diff_by_strat("noise")
        .await
        .expect("failed");
    let our_count = diffs.iter().filter(|d| {
        matches!(d, TargetPositionsQtyDiff::Stock(s) if s.stock == "CLEAR1" || s.stock == "CLEAR2" || s.stock == "CLEAR3")
    }).count();
    assert_eq!(our_count, 0, "targets should be deleted after clear");

    // Test noop on nonexistent strategy
    let result = diff_crud.clear_strat_pos("nonexistent").await;
    assert!(
        result.is_ok(),
        "clear on nonexistent strategy should be Ok(())"
    );

    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_clear_strat_pos_options() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let target_crud = trading_app::test_internals::target_option_positions_crud(pool.clone());
    let diff_crud = TargetPositionsCRUD::option(pool.clone());

    // Insert option target
    target_crud
        .create(&TargetOptionPositionsFullKeys {
            strategy: "noise".to_string(),
            stock: "OPTCLR".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            expiry: "20250119".to_string(),
            strike: 150.0,
            multiplier: "100".to_string(),
            option_type: OptionType::Call,
            avg_price: 3.50,
            quantity: 10.0,
        })
        .await
        .expect("create target failed");

    // Clear
    diff_crud
        .clear_strat_pos("noise")
        .await
        .expect("clear_strat_pos failed");

    // Verify
    let diffs = diff_crud
        .get_target_pos_diff_by_strat("noise")
        .await
        .expect("failed");
    let our_count = diffs
        .iter()
        .filter(|d| matches!(d, TargetPositionsQtyDiff::Options(o) if o.stock == "OPTCLR"))
        .count();
    assert_eq!(our_count, 0, "option targets should be deleted");

    del_strat!(&pool);
}
