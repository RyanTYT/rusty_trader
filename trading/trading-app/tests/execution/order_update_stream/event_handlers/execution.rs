//! Tests for `order_update_stream/event_handlers/execution.rs`.
//!
//! An execution fill is the most consistency-sensitive event in the system: it
//! touches the transaction log, the open-order row, the instrument position and
//! the currency position. The handler runs them in one `DbTxn`; these scenarios
//! assert the all-or-nothing outcome and the duplicate/fill guards.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use ibapi::orders::{CommissionReport, ExecutionData};
use serde_json::{json, Value};
use sqlx::PgPool;

use trading_app::database::crud::CRUDTrait;
use trading_app::database::db::DbDirect;
use trading_app::database::models::{
    CurrentStockPositionsFullKeys, OpenStockOrdersFullKeys, Status, StrategyFullKeys,
};
use trading_app::database::tables::DbTables;
use trading_app::execution::fx_backed_up_order::OrderStore;
use trading_app::execution::order_engine::OrderIBKR;
use trading_app::execution::order_update_stream::event_handlers::commission_report::on_commission_update;
use trading_app::execution::order_update_stream::event_handlers::execution::on_execution_update;
use trading_app::strategy::strategy::StrategyDetails;

use crate::common::scenario::{self, arr, assert_exact, read, FromJson, ToJson};
use crate::common::TestDb;
use crate::support::fakes::{FakeMarket, FakeOrderRouter};
use crate::support::open_store;

const STRATEGY: &str = "manual";

// ---------------------------------------------------------------------------
// JSON scenario runner (`tests/execution/order_update_stream/event_handlers/execution.json`)
// ---------------------------------------------------------------------------

/// Each case seeds a truncated database, replays a sequence of execution and
/// commission events, and compares the whole durable state with the `expect`
/// tables. `expect` may also carry `router` (orders the fake broker accepted)
/// and `store` (the parked order book) for backed-up-order placement cases.
#[tokio::test]
#[ignore = "requires a live database"]
async fn scenarios() {
    for (name, case) in
        scenario::cases("tests/execution/order_update_stream/event_handlers/execution.json")
    {
        if case
            .get("known_bug")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            eprintln!("skipping known-bug case `{name}`");
            continue;
        }
        run_scenario(&case)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}

async fn run_scenario(case: &Value) -> Result<(), String> {
    let db = TestDb::setup().await;
    // The store and its env guard must outlive every spawned placement task.
    let (_dir, _guard, store) = open_store();
    let store = Arc::new(store);

    seed_scenario(&db.pool, case).await?;
    seed_store(&store, case)?;

    let details = strategy_details(case);
    let market = &case["market"];
    let price = market.get("price").and_then(Value::as_f64).unwrap_or(100.0);
    let validates = market
        .get("validates")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let expects_error = case
        .get("expects_error")
        .and_then(Value::as_bool)
        .unwrap_or(!validates);
    let dead_router = case
        .get("dead_router")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let market = Arc::new(FakeMarket::new(price, validates));
    let router = Arc::new(FakeOrderRouter::new(&router_failures(case)?));
    let weak_router = Arc::downgrade(&router);
    // `dead_router` drops the only strong owner, so `Weak::upgrade` fails in
    // the handler's spawned placement path.
    let router = if dead_router { None } else { Some(router) };

    for (index, event) in arr(case, "events").iter().enumerate() {
        match event.get("type").and_then(Value::as_str) {
            Some("execution") => {
                let data = ExecutionData::from_json(event)
                    .map_err(|e| e.prefix(format!("events[{index}]")).to_string())?;
                let result =
                    run_scenario_execution(&db.pool, data, &details, &market, &weak_router, &store)
                        .await;
                if expects_error {
                    if result.is_ok() {
                        return Err(format!(
                            "events[{index}]: handler accepted an execution it should reject"
                        ));
                    }
                } else {
                    result?;
                }
            }
            Some("commission") => {
                let report = CommissionReport::from_json(event)
                    .map_err(|e| e.prefix(format!("events[{index}]")).to_string())?;
                on_commission_update(db.pool.clone(), &report)?;
            }
            other => return Err(format!("event with unknown type {other:?}: {event}")),
        }
        // The handlers spawn their writes, so make sure this event has fully
        // landed before the next one starts changing the same rows.
        settle(&db.pool).await?;
    }
    settle(&db.pool).await?;

    let expect = &case["expect"];
    // Placement is spawned after the execution transaction commits, so wait for
    // the whole expected state (tables + router + store) before asserting.
    wait_for_expect(&db.pool, router.as_deref(), &store, expect).await;
    assert_expect(&db.pool, router.as_deref(), &store, case).await
}

async fn seed_scenario(pool: &PgPool, case: &Value) -> Result<(), String> {
    let mut db = DbDirect::connect(pool);

    for (index, strategy) in arr(&case["db"], "strategies").iter().enumerate() {
        let fk = StrategyFullKeys::from_json(strategy)
            .map_err(|e| e.prefix(format!("db.strategies[{index}]")).to_string())?;
        db.strategy()
            .create_or_ignore(&fk)
            .await
            .map_err(|e| format!("seed strategy `{}`: {e:?}", fk.strategy))?;
    }

    for (index, order) in arr(&case["db"], "open_orders").iter().enumerate() {
        let fk = OpenStockOrdersFullKeys::from_json(order)
            .map_err(|e| e.prefix(format!("db.open_orders[{index}]")).to_string())?;
        db.open_stock_orders()
            .create(&fk)
            .await
            .map_err(|e| format!("seed open order {index}: {e:?}"))?;
    }
    Ok(())
}

/// Seeds the parked-order book: `"store": { "<strategy>": [<compact OrderIBKR row>] }`.
fn seed_store(store: &OrderStore, case: &Value) -> Result<(), String> {
    let Some(strategies) = case.get("store").and_then(Value::as_object) else {
        return Ok(());
    };

    for (strategy, rows) in strategies {
        let rows = rows
            .as_array()
            .ok_or_else(|| format!("store.{strategy}: expected array"))?;
        let mut orders = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            let order = OrderIBKR::from_json(row)
                .map_err(|e| e.prefix(format!("store.{strategy}[{index}]")).to_string())?;
            orders.push(order);
        }
        store
            .store_orders(strategy, &orders)
            .map_err(|e| format!("seed store.{strategy}: {e}"))?;
    }
    Ok(())
}

fn router_failures(case: &Value) -> Result<Vec<usize>, String> {
    let Some(rows) = case
        .get("router_failures")
        .or_else(|| case.get("fail_indices"))
    else {
        return Ok(Vec::new());
    };
    let rows = rows
        .as_array()
        .ok_or_else(|| "router_failures: expected array".to_string())?;
    rows.iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .as_u64()
                .map(|n| n as usize)
                .ok_or_else(|| format!("router_failures[{index}]: expected non-negative integer"))
        })
        .collect()
}

/// The handler resolves `execution.order_reference` through this map, falling
/// back to the default strategy. Every seeded strategy is registered so a case
/// can reference any of them.
fn strategy_details(case: &Value) -> Arc<HashMap<String, StrategyDetails>> {
    let mut details = HashMap::new();
    for strategy in arr(&case["db"], "strategies") {
        if let Some(name) = strategy.as_str() {
            details.insert(
                name.to_string(),
                StrategyDetails::new(1, name.to_string(), false, false),
            );
        }
    }
    details
        .entry(STRATEGY.to_string())
        .or_insert_with(|| StrategyDetails::new(1, STRATEGY.to_string(), false, false));
    Arc::new(details)
}

async fn run_scenario_execution(
    pool: &PgPool,
    exec: ExecutionData,
    details: &Arc<HashMap<String, StrategyDetails>>,
    market: &Arc<FakeMarket>,
    weak_router: &Weak<FakeOrderRouter>,
    store: &Arc<OrderStore>,
) -> Result<(), String> {
    on_execution_update(
        &Arc::downgrade(market),
        pool.clone(),
        exec,
        Arc::new(HashMap::new()),
        details,
        STRATEGY,
        tokio::runtime::Handle::current(),
        weak_router,
        store.clone(),
    )
}

/// The observable state for one `expect` table. `router` and `store` are the
/// in-memory fakes; the rest are whole-table database dumps.
async fn actual_for(
    pool: &PgPool,
    router: Option<&FakeOrderRouter>,
    store: &OrderStore,
    table: &str,
) -> Option<Value> {
    Some(match table {
        "open_orders" => read::open_orders(pool).await,
        "positions" => read::current_positions(pool).await,
        "transactions" => read::transactions(pool).await,
        "staged_commissions" => read::staged_commissions(pool).await,
        "router" => router_json(router),
        "store" => store_json(store),
        _ => return None,
    })
}

fn router_json(router: Option<&FakeOrderRouter>) -> Value {
    let Some(router) = router else {
        return Value::Array(Vec::new());
    };
    let mut rows: Vec<Value> = router.placed().iter().map(ToJson::to_json).collect();
    rows.sort_by_key(|row| serde_json::to_string(row).expect("serialize placed order"));
    Value::Array(rows)
}

fn store_json(store: &OrderStore) -> Value {
    let mut parked = store.load_all_to_map().expect("load parked orders");
    // `remove_all_valid_orders` leaves an empty vector behind for a drained
    // strategy; the book is empty, so report it as an empty map.
    parked.retain(|_, orders| !orders.is_empty());
    parked.to_json()
}

async fn assert_expect(
    pool: &PgPool,
    router: Option<&FakeOrderRouter>,
    store: &OrderStore,
    case: &Value,
) -> Result<(), String> {
    let label = case["name"].as_str().unwrap_or("case");
    let Some(expected) = case["expect"].as_object() else {
        return Ok(());
    };
    for (table, want) in expected {
        let got = actual_for(pool, router, store, table)
            .await
            .ok_or_else(|| format!("{label}: unknown expectation table `{table}`"))?;
        assert_exact(&format!("{label}/{table}"), got, want);
    }
    Ok(())
}

/// Waits (bounded) until every expected table matches exactly, so the placement
/// task spawned after commit has landed before the assertion compares.
async fn wait_for_expect(
    pool: &PgPool,
    router: Option<&FakeOrderRouter>,
    store: &OrderStore,
    expect: &Value,
) {
    let Some(expected) = expect.as_object() else {
        return;
    };
    for _ in 0..120 {
        let mut matches = true;
        for (table, want) in expected {
            let Some(got) = actual_for(pool, router, store, table).await else {
                matches = false;
                break;
            };
            if scenario::normalize(got) != scenario::normalize(want.clone()) {
                matches = false;
                break;
            }
        }
        if matches {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn snapshot(pool: &PgPool) -> Value {
    serde_json::json!({
        "open_orders": read::open_orders(pool).await,
        "positions": read::current_positions(pool).await,
        "transactions": read::transactions(pool).await,
        "staged_commissions": read::staged_commissions(pool).await,
    })
}

/// Waits until the observably durable state has been identical for two
/// consecutive polls (max ~3s). No-op events settle immediately; events whose
/// handlers spawned tasks wait for those writes.
async fn settle(pool: &PgPool) -> Result<(), String> {
    let mut last = snapshot(pool).await;
    let mut stable = 0;
    for _ in 0..60 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let now = snapshot(pool).await;
        if now == last {
            stable += 1;
            if stable >= 2 {
                return Ok(());
            }
        } else {
            stable = 0;
            last = now;
        }
    }
    Err("observable state did not settle within 3s".to_string())
}

// ---------------------------------------------------------------------------
// Concurrency: an execution and its commission can arrive as separate tasks.
// ---------------------------------------------------------------------------

/// Applies ten execution/commission pairs with fresh execution ids. Both
/// handlers run under one `tokio::join!`, and consecutive pairs alternate which
/// handler is delayed, so both arrival orders are exercised: a commission
/// staged before its transaction (applied by the transaction trigger) and a
/// commission that lands after the transaction (applied by the staged trigger).
/// Each id must end with exactly one transaction carrying the fee, no staged
/// row, and one position move.
#[tokio::test]
#[ignore = "requires a live database"]
async fn concurrent_execution_and_commission_are_applied_once() {
    run_concurrent_pairs(true).await;
}

/// The unstaggered variant: both handlers start together. The trigger merge
/// takes a row lock on the transaction, so the commission is applied whichever
/// handler wins the race.
#[tokio::test]
async fn concurrent_execution_and_commission_without_stagger_are_applied_once() {
    run_concurrent_pairs(false).await;
}

async fn run_concurrent_pairs(stagger_arrival: bool) {
    const PAIRS: usize = 10;
    const COMMISSION: f64 = 1.5;

    let db = TestDb::setup().await;
    let (_dir, _guard, store) = open_store();
    let store = Arc::new(store);

    let mut conn = DbDirect::connect(&db.pool);
    conn.strategy()
        .create_or_ignore(&StrategyFullKeys {
            strategy: STRATEGY.to_string(),
            status: Status::Active,
        })
        .await
        .expect("seed strategy");
    conn.current_stock_positions()
        .create(&CurrentStockPositionsFullKeys {
            stock: "CASH:USD".to_string(),
            primary_exchange: String::new(),
            currency: "SGD".to_string(),
            strategy: STRATEGY.to_string(),
            quantity: 5000.0,
            avg_price: 0.0,
            last_updated: chrono::Utc::now(),
        })
        .await
        .expect("seed currency position");

    let mut details = HashMap::new();
    details.insert(
        STRATEGY.to_string(),
        StrategyDetails::new(1, STRATEGY.to_string(), false, false),
    );
    let details = Arc::new(details);
    let market = Arc::new(FakeMarket::new(100.0, true));
    let router = Arc::new(FakeOrderRouter::new(&[]));

    for index in 0..PAIRS {
        let execution_id = format!("CONC{index}");
        let data = ExecutionData::from_json(&json!({
            "id": execution_id,
            "order_id": 500 + index,
            "perm_id": 600 + index,
            "shares": 1.0,
            "cumulative": 1.0,
            "price": 100.0,
            "reference": STRATEGY,
            "side": "BOT",
        }))
        .expect("execution fixture");
        let report = CommissionReport::from_json(&json!({
            "execution_id": execution_id,
            "fees": COMMISSION,
        }))
        .expect("commission fixture");

        // Alternate which side lands first when staggered. The delay is longer
        // than a local insert+commit, so the first handler has settled before
        // the second one runs.
        let commission_first = index % 2 == 1;
        let execution_delayed = stagger_arrival && commission_first;
        let commission_delayed = stagger_arrival && !commission_first;
        let (execution_result, commission_result) = tokio::join!(
            async {
                if execution_delayed {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                on_execution_update(
                    &Arc::downgrade(&market),
                    db.pool.clone(),
                    data,
                    Arc::new(HashMap::new()),
                    &details,
                    STRATEGY,
                    tokio::runtime::Handle::current(),
                    &Arc::downgrade(&router),
                    store.clone(),
                )
            },
            async {
                if commission_delayed {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                on_commission_update(db.pool.clone(), &report)
            },
        );
        execution_result.expect("execution handler");
        commission_result.expect("commission handler");

        wait_for_concurrent_rows(&db.pool, index + 1, COMMISSION)
            .await
            .unwrap_or_else(|e| panic!("pair {index} ({execution_id}): {e}"));
    }

    let mut expected_transactions = Vec::with_capacity(PAIRS);
    for index in 0..PAIRS {
        expected_transactions.push(json!({
            "asset": "stock",
            "execution_id": format!("CONC{index}"),
            "strategy": STRATEGY,
            "stock": "AAPL",
            "primary_exchange": "",
            "currency": "USD",
            "order_perm_id": 600 + index,
            "time": "2026-11-25T15:00:00+00:00",
            "price": 100.0,
            "quantity": 1.0,
            "fees": COMMISSION,
        }));
    }

    assert_exact(
        "concurrent/transactions",
        read::transactions(&db.pool).await,
        &Value::Array(expected_transactions),
    );
    assert_exact(
        "concurrent/positions",
        read::current_positions(&db.pool).await,
        &json!([
            {
                "asset": "stock",
                "stock": "AAPL",
                "primary_exchange": "",
                "currency": "USD",
                "strategy": STRATEGY,
                "quantity": 10.0,
                "avg_price": 100.0
            },
            {
                "asset": "stock",
                "stock": "CASH:USD",
                "primary_exchange": "",
                "currency": "SGD",
                "strategy": STRATEGY,
                "quantity": 4000.0,
                "avg_price": 0.0
            }
        ]),
    );
    assert_exact(
        "concurrent/staged_commissions",
        read::staged_commissions(&db.pool).await,
        &json!([]),
    );
    assert_exact(
        "concurrent/open_orders",
        read::open_orders(&db.pool).await,
        &json!([]),
    );
    assert_exact("concurrent/router", router_json(Some(&router)), &json!([]));
    assert_exact("concurrent/store", store_json(&store), &json!({}));
}

/// The pair is settled when `applied` transactions each carry the commission
/// and nothing is left staged.
async fn wait_for_concurrent_rows(
    pool: &PgPool,
    applied: usize,
    commission: f64,
) -> Result<(), String> {
    for _ in 0..120 {
        let transactions = read::transactions(pool).await;
        let staged = read::staged_commissions(pool).await;
        let applied_ok = transactions.as_array().is_some_and(|rows| {
            rows.len() == applied && rows.iter().all(|row| row["fees"] == json!(commission))
        });
        let staged_empty = staged.as_array().is_some_and(Vec::is_empty);
        if applied_ok && staged_empty {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    Err(format!(
        "{applied} execution/commission pairs did not settle within 3s\n\
         transactions: {}\nstaged_commissions: {}",
        read::transactions(pool).await,
        read::staged_commissions(pool).await
    ))
}
