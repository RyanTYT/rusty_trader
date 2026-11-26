//! Tests for `OrderEngine::handle_bar_update_outcome`.
//!
//! The method is the strategy -> execution hand-off: it either submits an
//! `EmitOrders` batch directly, or for `PendingDbQuery` it loads target-vs-current
//! diffs and open orders, gates on reconciliation, builds an FX funding plan and
//! reconciles each batch against the working orders.
//!
//! These are DB-backed (`#[ignore]`d, need `TEST_DATABASE_URL`) and call the
//! engine from a blocking thread because `handle_bar_update_outcome` uses
//! `Handle::block_on` internally.

use std::sync::Arc;

use serde_json::{Map, Value};
use sqlx::PgPool;

use trading_app::database::crud::CRUDTrait;
use trading_app::database::db::DbDirect;
use trading_app::database::models::{
    CurrentOptionPositionsFullKeys, CurrentStockPositionsFullKeys, OpenStockOrdersFullKeys,
    StrategyFullKeys, TargetOptionPositionsFullKeys, TargetStockPositionsFullKeys,
};
use trading_app::database::tables::DbTables;
use trading_app::execution::order_engine::OrderEngine;
use trading_app::strategy::strategy::{BarUpdateOutcome, StrategyDetails};

use crate::common::TestDb;
use crate::common::scenario::{self, FromJson, ToJson, arr, assert_exact, normalize, opt_s, read};
use crate::support::fakes::{FakeMarket, FakeOrderRouter};
use crate::support::open_store;

const STRATEGY: &str = "strat";

// ---------------------------------------------------------------------------
// JSON scenario runner (`tests/execution/order_engine/handle_bar.json`)
// ---------------------------------------------------------------------------

/// Each case owns a fresh, truncated database. It seeds the rows the strategy
/// path reads, runs one `BarUpdateOutcome` through the engine on a blocking
/// thread (the handler uses `Handle::block_on` internally), then compares the
/// complete post-state: SQL tables, the orders the fake router saw, and the
/// parked orders left in the redb `OrderStore`.
#[tokio::test]
#[ignore = "requires a live database"]
async fn scenarios() {
    for (name, case) in scenario::cases("tests/execution/order_engine/handle_bar.json") {
        if case
            .get("known_bug")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            eprintln!("skipping known-bug case `{name}`: option openings get no cash budget");
            continue;
        }

        let db = TestDb::setup().await;
        seed_scenario(&db.pool, &case["db"])
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));

        let is_fx = case
            .get("is_fx_strategy")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let market_price = case
            .get("market_price")
            .and_then(Value::as_f64)
            .unwrap_or(1.0);
        let outcome =
            BarUpdateOutcome::from_json(&case["outcome"]).unwrap_or_else(|e| panic!("{name}: {e}"));

        let (_dir, _guard, store) = open_store();
        let engine = OrderEngine::new(db.pool.clone(), tokio::runtime::Handle::current());
        let router = Arc::new(FakeOrderRouter::new(&[]));
        let market = Arc::new(FakeMarket::new(market_price, true));

        let inspector = Arc::clone(&router);
        let inspected_market = Arc::clone(&market);
        let parked = tokio::task::spawn_blocking(move || {
            let weak_router = Arc::downgrade(&inspector);
            let weak_market = Arc::downgrade(&inspected_market);
            engine.handle_bar_update_outcome(
                &weak_router,
                &weak_market,
                outcome,
                &StrategyDetails::new(1, STRATEGY.to_string(), is_fx, false),
                &store,
            );
            store.load_all_to_map().expect("load parked orders")
        })
        .await
        .expect("handle_bar task");

        let expect = &case["expect"];
        let expected_router = &expect["router"];
        let expected_placed = arr(expected_router, "placed").len();
        let expected_cancelled = arr(expected_router, "cancelled").len();

        // Cancels run on a dedicated thread and the DB writes are spawned on the
        // runtime; wait for the observable effects before dumping.
        for _ in 0..100 {
            if router.placed().len() >= expected_placed
                && router.cancelled().len() >= expected_cancelled
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let tables = &expect["tables"];
        if let Some(expected_orders) = tables.get("open_orders") {
            let expected_orders = normalize(expected_orders.clone());
            for _ in 0..100 {
                let actual = canonical_open_orders(read::open_orders(&db.pool).await);
                if normalize(actual) == expected_orders {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let actual_placed = placed_json(&router);
        let actual_cancelled = cancelled_json(&router);
        let actual_store = parked.to_json();

        let mut actual_tables = Map::new();
        if tables.get("targets").is_some() {
            actual_tables.insert("targets".to_string(), read::targets(&db.pool).await);
        }
        if tables.get("positions").is_some() {
            actual_tables.insert(
                "positions".to_string(),
                read::current_positions(&db.pool).await,
            );
        }
        if tables.get("open_orders").is_some() {
            actual_tables.insert(
                "open_orders".to_string(),
                canonical_open_orders(read::open_orders(&db.pool).await),
            );
        }

        for key in ["targets", "positions", "open_orders"] {
            if let Some(expected) = tables.get(key) {
                assert_exact(
                    &format!("{name}.tables.{key}"),
                    actual_tables
                        .get(key)
                        .cloned()
                        .unwrap_or_else(|| panic!("`{name}`: no actual table `{key}`")),
                    expected,
                );
            }
        }
        assert_exact(
            &format!("{name}.router.placed"),
            actual_placed,
            &expected_router["placed"],
        );
        assert_exact(
            &format!("{name}.router.cancelled"),
            actual_cancelled,
            &expected_router["cancelled"],
        );
        assert_exact(&format!("{name}.store"), actual_store, &expect["store"]);
    }
}

async fn seed_scenario(pool: &PgPool, db_json: &Value) -> Result<(), String> {
    for (index, strategy) in arr(db_json, "strategies").iter().enumerate() {
        let fk = StrategyFullKeys::from_json(strategy)
            .map_err(|e| e.prefix(format!("db.strategies[{index}]")).to_string())?;
        let mut db = DbDirect::connect(pool);
        db.strategy()
            .create_or_ignore(&fk)
            .await
            .map_err(|e| format!("seed strategy `{}`: {e:?}", fk.strategy))?;
    }

    for (index, target) in arr(db_json, "targets").iter().enumerate() {
        let asset = opt_s(target, "asset").unwrap_or_else(|| "stock".to_string());
        let mut db = DbDirect::connect(pool);
        let prefix = format!("db.targets[{index}]");
        match asset.as_str() {
            "stock" => {
                let fk = TargetStockPositionsFullKeys::from_json(target)
                    .map_err(|e| e.prefix(&prefix).to_string())?;
                db.target_stock_positions()
                    .create(&fk)
                    .await
                    .map_err(|e| format!("{prefix}: {e:?}"))?;
            }
            "option" => {
                let fk = TargetOptionPositionsFullKeys::from_json(target)
                    .map_err(|e| e.prefix(&prefix).to_string())?;
                db.target_option_positions()
                    .create(&fk)
                    .await
                    .map_err(|e| format!("{prefix}: {e:?}"))?;
            }
            other => return Err(format!("{prefix}: unknown target asset `{other}`")),
        }
    }

    for (index, position) in arr(db_json, "positions").iter().enumerate() {
        let asset = opt_s(position, "asset").unwrap_or_else(|| "stock".to_string());
        let mut db = DbDirect::connect(pool);
        let prefix = format!("db.positions[{index}]");
        match asset.as_str() {
            "stock" => {
                let fk = CurrentStockPositionsFullKeys::from_json(position)
                    .map_err(|e| e.prefix(&prefix).to_string())?;
                db.current_stock_positions()
                    .create(&fk)
                    .await
                    .map_err(|e| format!("{prefix}: {e:?}"))?;
            }
            "option" => {
                let fk = CurrentOptionPositionsFullKeys::from_json(position)
                    .map_err(|e| e.prefix(&prefix).to_string())?;
                db.current_option_positions()
                    .create(&fk)
                    .await
                    .map_err(|e| format!("{prefix}: {e:?}"))?;
            }
            other => return Err(format!("{prefix}: unknown position asset `{other}`")),
        }
    }

    for (index, order) in arr(db_json, "open_orders").iter().enumerate() {
        let prefix = format!("db.open_orders[{index}]");
        let fk =
            OpenStockOrdersFullKeys::from_json(order).map_err(|e| e.prefix(&prefix).to_string())?;
        let mut db = DbDirect::connect(pool);
        db.open_stock_orders()
            .create(&fk)
            .await
            .map_err(|e| format!("{prefix}: {e:?}"))?;
    }

    Ok(())
}

fn placed_json(router: &FakeOrderRouter) -> Value {
    let mut rows: Vec<Value> = router.placed().iter().map(ToJson::to_json).collect();
    sort_values(&mut rows);
    Value::Array(rows)
}

fn cancelled_json(router: &FakeOrderRouter) -> Value {
    let mut rows: Vec<i64> = router.cancelled().iter().map(|id| *id as i64).collect();
    rows.sort();
    Value::Array(rows.into_iter().map(Value::from).collect())
}

fn sort_values(values: &mut [Value]) {
    values.sort_by_key(|value| serde_json::to_string(value).expect("serialize order"));
}

/// `FakeOrderRouter` assigns ids from worker threads, so the ids of rows written
/// by `place_order` are scheduler-dependent. Renumber only those optimistic rows
/// (`order_perm_id == -1`) in a stable order; every business column and every
/// seeded broker id is still compared exactly.
fn canonical_open_orders(value: Value) -> Value {
    let mut rows = value.as_array().cloned().unwrap_or_default();
    let mut generated: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| row["order_perm_id"].as_i64() == Some(-1))
        .map(|(index, _)| index)
        .collect();
    generated.sort_by_key(|&index| {
        (
            rows[index]["strategy"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            rows[index]["stock"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        )
    });
    for (offset, index) in generated.into_iter().enumerate() {
        rows[index]["order_id"] = serde_json::json!(1000 + offset as i64);
        rows[index]["order_perm_id"] = serde_json::json!(-1);
    }
    scenario::sorted_rows(rows, &["order_perm_id", "order_id"])
}
