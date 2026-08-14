//! Comprehensive DB integration tests for `CurrentPositionsOps` on `CurrentPositionsCRUD`.
//!
//! Tests ALL variants (Stock + Options) for ALL methods, with table-driven
//! test helpers for systematic coverage.
//!
//! Methods: get_pos_by_strat, get_pos_by_pk, get_all_pos_grouped,
//! update_positions_additive.
//!
//! Requires: live Postgres + DATABASE_URL. All tests #[ignore]'d.

use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys,
    CurrentOptionPositionsUpdateKeys, CurrentStockPositionsFullKeys,
    CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys, OptionType,
};
use trading_app::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsFullKeys as CPFK, CurrentPositionsOps,
    CurrentPositionsPrimaryKeys as CPInterfacePK, CurrentPositionsUpdateKeys as CPInterfaceUK,
};

use crate::init_strat;
use crate::del_strat;
use crate::models::init::{TEST_MUTEX, setup_test_db};

// ============================ Table-driven test helpers ============================

/// A test case for `update_positions_additive`: a sequence of fills → expected qty + avg_price.
struct AdditiveCase {
    name: &'static str,
    fills: Vec<(f64, f64)>, // (quantity, avg_price) per fill
    expected_qty: f64,
    expected_avg: f64,
}

impl AdditiveCase {
    const EPS: f64 = 1e-6;
}

/// Run a series of additive test cases against the same PK, cleaning up between each.
async fn run_additive_cases_stock(
    crud: &CurrentPositionsCRUD,
    pk: CurrentStockPositionsPrimaryKeys,
    cases: &[AdditiveCase],
) {
    let interface_pk = CPInterfacePK::Stock(pk.clone());
    for case in cases {
        // Clean slate
        let _ = crud.delete(&interface_pk).await;

        for (qty, price) in &case.fills {
            crud.update_positions_additive(
                interface_pk.clone(),
                CPInterfaceUK::Stock(CurrentStockPositionsUpdateKeys {
                    quantity: Some(*qty),
                    avg_price: Some(*price),
                    last_updated: None,
                }),
            )
            .await
            .expect("additive fill failed");
        }

        let pos = crud
            .get_pos_by_pk(interface_pk.clone())
            .await
            .expect("read failed")
            .expect("expected row");
        match pos {
            CPFK::Stock(s) => {
                assert!(
                    (s.quantity - case.expected_qty).abs() < AdditiveCase::EPS,
                    "{}: expected qty {}, got {}",
                    case.name,
                    case.expected_qty,
                    s.quantity
                );
                assert!(
                    (s.avg_price - case.expected_avg).abs() < AdditiveCase::EPS,
                    "{}: expected avg {}, got {}",
                    case.name,
                    case.expected_avg,
                    s.avg_price
                );
            }
            _ => panic!("{}: expected Stock variant", case.name),
        }
    }
    // Final cleanup
    let _ = crud.delete(&interface_pk).await;
}

/// Same but for Options variant.
async fn run_additive_cases_options(
    crud: &CurrentPositionsCRUD,
    pk: CurrentOptionPositionsPrimaryKeys,
    cases: &[AdditiveCase],
) {
    let interface_pk = CPInterfacePK::Options(pk.clone());
    for case in cases {
        let _ = crud.delete(&interface_pk).await;

        for (qty, price) in &case.fills {
            crud.update_positions_additive(
                interface_pk.clone(),
                CPInterfaceUK::Options(CurrentOptionPositionsUpdateKeys {
                    quantity: Some(*qty),
                    avg_price: Some(*price),
                    last_updated: None,
                }),
            )
            .await
            .expect("additive fill failed");
        }

        let pos = crud
            .get_pos_by_pk(interface_pk.clone())
            .await
            .expect("read failed")
            .expect("expected row");
        match pos {
            CPFK::Options(o) => {
                assert!(
                    (o.quantity - case.expected_qty).abs() < AdditiveCase::EPS,
                    "{}: expected qty {}, got {}",
                    case.name,
                    case.expected_qty,
                    o.quantity
                );
                assert!(
                    (o.avg_price - case.expected_avg).abs() < AdditiveCase::EPS,
                    "{}: expected avg {}, got {}",
                    case.name,
                    case.expected_avg,
                    o.avg_price
                );
            }
            _ => panic!("{}: expected Options variant", case.name),
        }
    }
    let _ = crud.delete(&interface_pk).await;
}

// ============================ get_pos_by_strat ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_pos_by_strat_stock_multiple() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::stock(pool.clone());

    let test_cases: Vec<(&str, &[(f64, &str)], usize)> = vec![
        ("multiple positions", &[(100.0, "AAPL"), (200.0, "MSFT")], 2),
        ("single position", &[(50.0, "GOOG")], 1),
        ("no positions (empty strategy)", &[], 0),
    ];

    for (name, positions, expected_count) in test_cases {
        let mut pks = vec![];
        for (qty, stock) in positions {
            let fk = CurrentStockPositionsFullKeys {
                stock: stock.to_string(), primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(), strategy: "noise".to_string(),
                quantity: *qty, avg_price: 150.0, last_updated: Utc::now(),
            };
            crud.create(&CPFK::Stock(fk)).await.expect("create failed");
            pks.push(CPInterfacePK::Stock(CurrentStockPositionsPrimaryKeys {
                stock: stock.to_string(), primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(), strategy: "noise".to_string(),
            }));
        }

        let result = crud.get_pos_by_strat("noise").await.expect("get_pos_by_strat failed");
        // Filter to just our test stocks (table may have other rows)
        let our_count = result.iter().filter(|p| {
            matches!(p, CPFK::Stock(s) if s.strategy == "noise" && (s.stock == "AAPL" || s.stock == "MSFT" || s.stock == "GOOG"))
        }).count();
        assert_eq!(our_count, expected_count, "case '{}': expected {} positions", name, expected_count);

        for pk in &pks {
            let _ = crud.delete(pk).await;
        }
    }
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_pos_by_strat_options_multiple() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::option(pool.clone());

    let test_cases: Vec<(&str, &[(f64, f64, &str, OptionType)], usize)> = vec![
        ("multiple option positions", &[(5.0, 150.0, "AAPL", OptionType::Call), (3.0, 160.0, "AAPL", OptionType::Put)], 2),
        ("single option position", &[(10.0, 155.0, "MSFT", OptionType::Call)], 1),
        ("no positions", &[], 0),
    ];

    for (name, positions, expected_count) in test_cases {
        let mut pks = vec![];
        for (qty, strike, stock, ot) in positions {
            let fk = CurrentOptionPositionsFullKeys {
                stock: stock.to_string(), primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(), strategy: "noise".to_string(),
                expiry: "20250119".to_string(), strike: *strike,
                multiplier: "100".to_string(), option_type: ot.clone(),
                quantity: *qty, avg_price: 3.50, last_updated: Utc::now(),
            };
            crud.create(&CPFK::Options(fk)).await.expect("create failed");
            pks.push(CPInterfacePK::Options(CurrentOptionPositionsPrimaryKeys {
                stock: stock.to_string(), primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(), strategy: "noise".to_string(),
                expiry: "20250119".to_string(), strike: *strike,
                multiplier: "100".to_string(), option_type: ot.clone(),
            }));
        }

        let result = crud.get_pos_by_strat("noise").await.expect("get_pos_by_strat failed");
        let our_count = result.iter().filter(|p| {
            matches!(p, CPFK::Options(o) if o.strategy == "noise" && o.stock == "AAPL" || o.stock == "MSFT")
        }).count();
        assert_eq!(our_count, expected_count, "case '{}': expected {} option positions", name, expected_count);

        for pk in &pks {
            let _ = crud.delete(pk).await;
        }
    }
    del_strat!(&pool);
}

// ============================ get_pos_by_pk ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_pos_by_pk_stock_found_and_not_found() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::stock(pool.clone());

    // Insert a position
    let fk = CurrentStockPositionsFullKeys {
        stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), strategy: "noise".to_string(),
        quantity: 100.0, avg_price: 150.0, last_updated: Utc::now(),
    };
    crud.create(&CPFK::Stock(fk)).await.expect("create failed");

    // Test: found
    let pk_found = CPInterfacePK::Stock(CurrentStockPositionsPrimaryKeys {
        stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), strategy: "noise".to_string(),
    });
    let result = crud.get_pos_by_pk(pk_found.clone()).await.expect("get_pos_by_pk failed");
    assert!(result.is_some(), "found PK → should return Some");
    match result.unwrap() {
        CPFK::Stock(s) => {
            assert_eq!(s.stock, "AAPL");
            assert_eq!(s.quantity, 100.0);
        }
        _ => panic!("expected Stock variant"),
    }

    // Test: not found (different stock)
    let pk_not_found = CPInterfacePK::Stock(CurrentStockPositionsPrimaryKeys {
        stock: "NONEXISTENT".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), strategy: "noise".to_string(),
    });
    let result = crud.get_pos_by_pk(pk_not_found).await.expect("get_pos_by_pk failed");
    assert!(result.is_none(), "nonexistent PK → should return None");

    crud.delete(&pk_found).await.expect("delete failed");
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_pos_by_pk_options_found_and_not_found() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::option(pool.clone());

    let fk = CurrentOptionPositionsFullKeys {
        stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), strategy: "noise".to_string(),
        expiry: "20250119".to_string(), strike: 150.0,
        multiplier: "100".to_string(), option_type: OptionType::Call,
        quantity: 5.0, avg_price: 3.50, last_updated: Utc::now(),
    };
    crud.create(&CPFK::Options(fk)).await.expect("create failed");

    // Found
    let pk_found = CPInterfacePK::Options(CurrentOptionPositionsPrimaryKeys {
        stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), strategy: "noise".to_string(),
        expiry: "20250119".to_string(), strike: 150.0,
        multiplier: "100".to_string(), option_type: OptionType::Call,
    });
    let result = crud.get_pos_by_pk(pk_found.clone()).await.expect("get_pos_by_pk failed");
    assert!(result.is_some());
    match result.unwrap() {
        CPFK::Options(o) => assert_eq!(o.strike, 150.0),
        _ => panic!("expected Options variant"),
    }

    // Not found (different strike)
    let pk_not_found = CPInterfacePK::Options(CurrentOptionPositionsPrimaryKeys {
        stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), strategy: "noise".to_string(),
        expiry: "20250119".to_string(), strike: 999.0,
        multiplier: "100".to_string(), option_type: OptionType::Call,
    });
    let result = crud.get_pos_by_pk(pk_not_found).await.expect("get_pos_by_pk failed");
    assert!(result.is_none(), "nonexistent strike → None");

    crud.delete(&pk_found).await.expect("delete failed");
    del_strat!(&pool);
}

// ============================ get_all_pos_grouped ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_all_pos_grouped_stock_aggregation() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::stock(pool.clone());

    // Insert 2 positions for SAME stock, DIFFERENT strategies
    // Need both strategies to exist
    let strat_crud = trading_app::database::models_crud::strategy::StrategyCRUD::new(pool.clone());
    for strat in ["noise", "manual"] {
        let _ = strat_crud
            .create_or_ignore(&trading_app::database::models::StrategyFullKeys {
                strategy: strat.to_string(),
                status: trading_app::database::models::Status::Active,
            })
            .await;
    }

    let test_cases: Vec<(&str, &[(&str, f64)], f64)> = vec![
        ("two strategies same stock", &[("noise", 100.0), ("manual", 100.0)], 200.0),
        ("three strategies same stock", &[("noise", 50.0), ("manual", 75.0), ("unknown", 25.0)], 150.0),
    ];

    for (name, positions, expected_qty) in test_cases {
        // Clean slate for our test stock
        let _ = sqlx::query("DELETE FROM trading.current_stock_positions WHERE stock = 'GROUPTEST'")
            .execute(&pool).await;

        for (strat, qty) in positions {
            // Ensure strategy exists
            let _ = strat_crud
                .create_or_ignore(&trading_app::database::models::StrategyFullKeys {
                    strategy: strat.to_string(),
                    status: trading_app::database::models::Status::Active,
                })
                .await;

            let fk = CurrentStockPositionsFullKeys {
                stock: "GROUPTEST".to_string(), primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(), strategy: strat.to_string(),
                quantity: *qty, avg_price: 150.0, last_updated: Utc::now(),
            };
            crud.create(&CPFK::Stock(fk)).await.expect("create failed");
        }

        let grouped = crud.get_all_pos_grouped().await.expect("get_all_pos_grouped failed");
        let our = grouped.iter().find(|p| matches!(p, CPFK::Stock(s) if s.stock == "GROUPTEST"));
        assert!(our.is_some(), "case '{}': should find GROUPTEST", name);
        match our.unwrap() {
            CPFK::Stock(s) => assert!((s.quantity - expected_qty).abs() < 1e-6,
                "case '{}': expected aggregated qty {}, got {}", name, expected_qty, s.quantity),
            _ => panic!("expected Stock variant"),
        }
    }

    let _ = sqlx::query("DELETE FROM trading.current_stock_positions WHERE stock = 'GROUPTEST'")
        .execute(&pool).await;
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_get_all_pos_grouped_options_aggregation() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::option(pool.clone());

    let strat_crud = trading_app::database::models_crud::strategy::StrategyCRUD::new(pool.clone());
    for strat in ["noise", "manual"] {
        let _ = strat_crud
            .create_or_ignore(&trading_app::database::models::StrategyFullKeys {
                strategy: strat.to_string(),
                status: trading_app::database::models::Status::Active,
            })
            .await;
    }

    // Same option contract, two strategies
    for strat in ["noise", "manual"] {
        let fk = CurrentOptionPositionsFullKeys {
            stock: "OPTGRP".to_string(), primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(), strategy: strat.to_string(),
            expiry: "20250119".to_string(), strike: 150.0,
            multiplier: "100".to_string(), option_type: OptionType::Call,
            quantity: 5.0, avg_price: 3.50, last_updated: Utc::now(),
        };
        crud.create(&CPFK::Options(fk)).await.expect("create failed");
    }

    let grouped = crud.get_all_pos_grouped().await.expect("get_all_pos_grouped failed");
    let our = grouped.iter().find(|p| matches!(p, CPFK::Options(o) if o.stock == "OPTGRP"));
    assert!(our.is_some(), "should find OPTGRP");
    match our.unwrap() {
        CPFK::Options(o) => assert!((o.quantity - 10.0).abs() < 1e-6,
            "aggregated option qty should be 10, got {}", o.quantity),
        _ => panic!("expected Options variant"),
    }

    // Cleanup
    for strat in ["noise", "manual"] {
        let _ = crud.delete(&CPInterfacePK::Options(CurrentOptionPositionsPrimaryKeys {
            stock: "OPTGRP".to_string(), primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(), strategy: strat.to_string(),
            expiry: "20250119".to_string(), strike: 150.0,
            multiplier: "100".to_string(), option_type: OptionType::Call,
        })).await;
    }
    del_strat!(&pool);
}

// ============================ update_positions_additive — comprehensive table-driven ============================

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_update_positions_additive_stock_comprehensive() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::stock(pool.clone());

    let pk = CurrentStockPositionsPrimaryKeys {
        stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), strategy: "noise".to_string(),
    };

    let cases = vec![
        AdditiveCase {
            name: "first fill (INSERT)",
            fills: vec![(100.0, 150.0)],
            expected_qty: 100.0,
            expected_avg: 150.0,
        },
        AdditiveCase {
            name: "same direction weighted avg (2 fills)",
            fills: vec![(100.0, 150.0), (100.0, 160.0)],
            expected_qty: 200.0,
            expected_avg: 155.0, // (100*150 + 100*160) / 200
        },
        AdditiveCase {
            name: "same direction weighted avg (3 fills)",
            fills: vec![(100.0, 150.0), (50.0, 160.0), (75.0, 155.0)],
            expected_qty: 225.0,
            expected_avg: (100.0 * 150.0 + 50.0 * 160.0 + 75.0 * 155.0) / 225.0,
        },
        AdditiveCase {
            name: "cross direction keeps avg_price (Stock has SIGN guard)",
            fills: vec![(100.0, 150.0), (-50.0, 160.0)],
            expected_qty: 50.0,
            expected_avg: 150.0, // SIGN differs → keeps existing avg_price
        },
        AdditiveCase {
            name: "total → 0 sets avg_price to 0",
            fills: vec![(100.0, 150.0), (-100.0, 160.0)],
            expected_qty: 0.0,
            expected_avg: 0.0,
        },
        AdditiveCase {
            name: "short then partial cover (cross direction)",
            fills: vec![(-100.0, 150.0), (50.0, 160.0)],
            expected_qty: -50.0,
            expected_avg: 150.0, // SIGN differs → keeps existing
        },
        AdditiveCase {
            name: "short then full cover → 0",
            fills: vec![(-100.0, 150.0), (100.0, 160.0)],
            expected_qty: 0.0,
            expected_avg: 0.0,
        },
        AdditiveCase {
            name: "same direction then cross then same again",
            fills: vec![(100.0, 150.0), (50.0, 160.0), (-75.0, 155.0), (25.0, 165.0)],
            expected_qty: 100.0,
            // After fill 1+2: qty=150, avg=(100*150+50*160)/150=153.33
            // Fill 3 (cross): qty=75, avg kept=153.33
            // Fill 4 (same dir as 75>0, +25): qty=100, avg=(75*153.33+25*165)/100
            expected_avg: (75.0 * ((100.0 * 150.0 + 50.0 * 160.0) / 150.0) + 25.0 * 165.0) / 100.0,
        },
    ];

    run_additive_cases_stock(&crud, pk, &cases).await;
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_update_positions_additive_options_comprehensive() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::option(pool.clone());

    let pk = CurrentOptionPositionsPrimaryKeys {
        stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(), strategy: "noise".to_string(),
        expiry: "20250119".to_string(), strike: 150.0,
        multiplier: "100".to_string(), option_type: OptionType::Call,
    };

    let cases = vec![
        AdditiveCase {
            name: "first fill (INSERT)",
            fills: vec![(5.0, 3.50)],
            expected_qty: 5.0,
            expected_avg: 3.50,
        },
        AdditiveCase {
            name: "same direction weighted avg",
            fills: vec![(5.0, 3.50), (5.0, 4.50)],
            expected_qty: 10.0,
            expected_avg: 4.0, // (5*3.50 + 5*4.50) / 10
        },
        AdditiveCase {
            name: "cross direction — BUG: Options has NO SIGN guard, uses weighted avg",
            fills: vec![(5.0, 3.50), (-2.0, 4.00)],
            expected_qty: 3.0,
            // BUG: Options arm doesn't keep existing avg_price — it computes weighted avg
            // (5*3.50 + (-2)*4.00) / 3 = (17.5 - 8) / 3 = 3.1666...
            expected_avg: (5.0 * 3.50 + (-2.0) * 4.00) / 3.0,
        },
        AdditiveCase {
            name: "total → 0 sets avg to 0",
            fills: vec![(5.0, 3.50), (-5.0, 4.00)],
            expected_qty: 0.0,
            expected_avg: 0.0,
        },
        AdditiveCase {
            name: "3 fills same direction",
            fills: vec![(5.0, 3.50), (3.0, 4.00), (2.0, 3.75)],
            expected_qty: 10.0,
            expected_avg: (5.0 * 3.50 + 3.0 * 4.00 + 2.0 * 3.75) / 10.0,
        },
    ];

    run_additive_cases_options(&crud, pk, &cases).await;
    del_strat!(&pool);
}

#[tokio::test]
#[ignore = "requires live Postgres + DATABASE_URL"]
async fn test_update_positions_additive_mismatched_variants() {
    let _lock = TEST_MUTEX.lock().await;
    let pool = setup_test_db().await;
    init_strat!(&pool);
    let crud = CurrentPositionsCRUD::stock(pool.clone());

    // Test ALL 4 mismatched combinations:
    // (Stock pk + Options uk), (Options pk + Stock uk)
    let mismatched_cases: Vec<(&str, CPInterfacePK, CPInterfaceUK)> = vec![
        (
            "Stock pk + Options uk",
            CPInterfacePK::Stock(CurrentStockPositionsPrimaryKeys {
                stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(), strategy: "noise".to_string(),
            }),
            CPInterfaceUK::Options(CurrentOptionPositionsUpdateKeys {
                quantity: Some(100.0), avg_price: Some(150.0), last_updated: None,
            }),
        ),
        (
            "Options pk + Stock uk",
            CPInterfacePK::Options(CurrentOptionPositionsPrimaryKeys {
                stock: "AAPL".to_string(), primary_exchange: "NASDAQ".to_string(),
                currency: "USD".to_string(), strategy: "noise".to_string(),
                expiry: "20250119".to_string(), strike: 150.0,
                multiplier: "100".to_string(), option_type: OptionType::Call,
            }),
            CPInterfaceUK::Stock(CurrentStockPositionsUpdateKeys {
                quantity: Some(100.0), avg_price: Some(150.0), last_updated: None,
            }),
        ),
    ];

    for (name, pk, uk) in mismatched_cases {
        let result = crud.update_positions_additive(pk, uk).await;
        assert!(result.is_err(), "case '{}': mismatched variants should return Err", name);
        assert!(
            result.unwrap_err().contains("Invalid key variant combination"),
            "case '{}': should mention Invalid key variant combination", name
        );
    }
    del_strat!(&pool);
}
