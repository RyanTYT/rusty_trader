//! Mocked syncer tests: scripted `PositionsTracker` events, a fake
//! `ContractBroker`/`PriceSupplier`, and the test database. No IB Gateway.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;

use serde_json::Value;
use sqlx::PgPool;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::db::DbDirect;
use trading_app::database::models::{
    CurrentOptionPositionsFullKeys, CurrentStockPositionsFullKeys, Status, StrategyFullKeys,
};
use trading_app::database::tables::DbTables;
use trading_app::execution::syncer::{SyncOps, SyncerEngine};
use trading_app::init_app::StrategyParameters;
use trading_app::strategy::manual::Manual;
use trading_app::strategy::strategy::StrategyEnum;

use crate::common::TestDb;
use crate::common::scenario::{self, FromJson, arr, assert_exact, normalize, read, s};
use crate::support::fakes::{FakeBroker, FakeMarket};
use crate::support::open_store;

const SCENARIO_STRATEGY: &str = "manual";

// ---------------------------------------------------------------------------
// JSON scenario runner (`tests/execution/syncer.json`)
// ---------------------------------------------------------------------------

/// The JSON cases live next to this module. Every case seeds a truncated
/// database, replays the scripted streams through a real `SyncerEngine`, then
/// compares whole tables with [`assert_exact`].
#[tokio::test]
#[ignore = "requires a live database"]
async fn scenarios() {
    for (name, case) in scenario::cases("tests/execution/syncer.json") {
        if case
            .get("known_bug")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            eprintln!("skipping known-bug syncer case `{name}`");
            continue;
        }

        let db = TestDb::setup().await;
        let outcome = AssertUnwindSafe(run_scenario(&db, &case))
            .catch_unwind()
            .await;
        drop(db);

        if let Err(payload) = outcome {
            let detail = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            panic!("syncer scenario `{name}` failed: {detail}");
        }
    }
}

async fn run_scenario(db: &TestDb, case: &Value) {
    seed_scenario(db, case)
        .await
        .unwrap_or_else(|e| panic!("{}: {e}", s(case, "name")));

    let engine = scenario_engine(db);
    let streams = case.get("streams").cloned().unwrap_or(Value::Null);
    let run_positions = streams.get("account").is_some() || streams.get("positions").is_some();
    let run_open_orders = streams.get("open_orders").is_some();
    let run_executions = streams.get("executions").is_some();

    // Keep the temp store (and its env lock) alive for the whole case. The
    // execution handler may hold the Arc from a spawned re-placement task.
    let store = run_executions.then(|| {
        let (dir, guard, store) = open_store();
        (dir, guard, Arc::new(store))
    });

    let repeat = case
        .get("repeat")
        .and_then(Value::as_i64)
        .unwrap_or(1)
        .max(1);
    for iteration in 0..repeat {
        if iteration > 0 {
            // Let the previous run's spawned writes land before replaying,
            // so an idempotency case tests the duplicate path.
            tokio::time::sleep(Duration::from_millis(400)).await;
        }

        let broker = Arc::new(
            FakeBroker::from_json(&streams)
                .unwrap_or_else(|e| panic!("{}: streams: {e}", s(case, "name"))),
        );
        let market = Arc::new(FakeMarket::new(1.0, true));

        // Executions first: a fill usually precedes the position snapshot in
        // a session, and a mixed case expects the snapshot to confirm it.
        if let Some((_, _, store)) = &store {
            engine
                .sync_executions(
                    &broker,
                    Arc::new(HashMap::new()),
                    &Arc::downgrade(&market),
                    Some(SCENARIO_STRATEGY.to_string()),
                    store.clone(),
                )
                .expect("sync executions");
            if run_open_orders || run_positions {
                // The handler applies the fill on a spawned task. Let it land
                // before the position snapshot is reconciled against it.
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
        }
        if run_open_orders {
            engine.sync_open_orders(
                broker.as_ref(),
                &market,
                Some(SCENARIO_STRATEGY.to_string()),
            );
        }
        if run_positions {
            engine
                .sync_positions(
                    broker.as_ref(),
                    &market,
                    Some(SCENARIO_STRATEGY.to_string()),
                )
                .await;
        }
    }

    let expected = expected_tables(case);
    let all_empty = expected
        .iter()
        .all(|(_, want)| want.as_array().is_some_and(|rows| rows.is_empty()));
    let matched = wait_for_tables(&db.pool, &expected, Duration::from_secs(3)).await;
    if !matched || all_empty {
        // Nothing was expected: give any stray background write time to land
        // before proving the table stayed empty.
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    for (table, want) in &expected {
        assert_exact(table, dump_table(&db.pool, table).await, want);
    }

    drop(store);
}

fn scenario_engine(db: &TestDb) -> SyncerEngine {
    let params = StrategyParameters {
        strategy: StrategyEnum::Manual(Manual::new(db.pool.clone())),
        subscribed_contracts: Vec::new(),
    };
    SyncerEngine::new(
        db.pool.clone(),
        "DU123".to_string(),
        &vec![params],
        tokio::runtime::Handle::current(),
    )
}

async fn seed_scenario(db: &TestDb, case: &Value) -> Result<(), String> {
    let mut conn = DbDirect::connect(&db.pool);
    let spec = case.get("db").cloned().unwrap_or(Value::Null);

    conn.strategy()
        .create_or_ignore(&StrategyFullKeys {
            strategy: SCENARIO_STRATEGY.to_string(),
            status: Status::Active,
        })
        .await
        .map_err(|e| format!("seed default strategy: {e:?}"))?;

    for name in arr(&spec, "strategies") {
        let fk = StrategyFullKeys::from_json(name).map_err(|e| e.to_string())?;
        if fk.strategy == SCENARIO_STRATEGY {
            continue;
        }
        conn.strategy()
            .create_or_ignore(&fk)
            .await
            .map_err(|e| format!("seed strategy `{}`: {e:?}", fk.strategy))?;
    }

    for (index, p) in arr(&spec, "positions").iter().enumerate() {
        let fk = CurrentStockPositionsFullKeys::from_json(p)
            .map_err(|e| e.prefix(format!("db.positions[{index}]")).to_string())?;
        conn.current_stock_positions()
            .create(&fk)
            .await
            .map_err(|e| format!("seed stock position {index}: {e:?}"))?;
    }

    for (index, p) in arr(&spec, "option_positions").iter().enumerate() {
        let fk = CurrentOptionPositionsFullKeys::from_json(p).map_err(|e| {
            e.prefix(format!("db.option_positions[{index}]"))
                .to_string()
        })?;
        conn.current_option_positions()
            .create(&fk)
            .await
            .map_err(|e| format!("seed option position {index}: {e:?}"))?;
    }

    Ok(())
}

fn expected_tables(case: &Value) -> Vec<(&'static str, Value)> {
    const TABLES: [&str; 3] = ["positions", "open_orders", "transactions"];
    let expect = &case["expect"];
    TABLES
        .iter()
        .filter_map(|table| expect.get(table).map(|want| (*table, want.clone())))
        .collect()
}

async fn dump_table(pool: &PgPool, table: &str) -> Value {
    match table {
        "positions" => read::current_positions(pool).await,
        "open_orders" => read::open_orders(pool).await,
        "transactions" => read::transactions(pool).await,
        other => panic!("unknown dump table `{other}`"),
    }
}

/// Waits (bounded) until every expected table matches exactly, so the
/// detached reconcile writes have landed before the assertion compares.
async fn wait_for_tables(pool: &PgPool, expected: &[(&str, Value)], timeout: Duration) -> bool {
    let start = Instant::now();
    loop {
        let mut matches = true;
        for (table, want) in expected {
            let got = normalize(dump_table(pool, table).await);
            if got != normalize(want.clone()) {
                matches = false;
                break;
            }
        }
        if matches {
            return true;
        }
        if start.elapsed() >= timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
