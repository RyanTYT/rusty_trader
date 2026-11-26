//! Tests for `order_update_stream/event_handlers/order_status.rs`.
//!
//! `submitted` is a read-only consistency check; `cancelled` deletes the row from
//! both the stock and option tables. Neither is transactional.

use std::time::Duration;

use ibapi::orders::OrderStatus;
use serde_json::Value;
use sqlx::PgPool;

use trading_app::database::crud::CRUDTrait;
use trading_app::database::db::DbDirect;
use trading_app::database::models::{
    OpenOptionOrdersFullKeys, OpenStockOrdersFullKeys, StrategyFullKeys,
};
use trading_app::database::tables::DbTables;
use trading_app::execution::order_update_stream::event_handlers::order_status;

use crate::common::TestDb;
use crate::common::scenario::{self, FromJson, arr, assert_exact, read};

// ---------------------------------------------------------------------------
// JSON scenario runner (`tests/execution/order_update_stream/event_handlers/order_status.json`)
// ---------------------------------------------------------------------------

/// Each case seeds open stock/option rows, dispatches `submitted` / `cancelled`
/// statuses, and compares the complete open-order state.
#[tokio::test]
#[ignore = "requires a live database"]
async fn scenarios() {
    for (name, case) in
        scenario::cases("tests/execution/order_update_stream/event_handlers/order_status.json")
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
    seed_scenario(&db.pool, case).await?;

    for (index, event) in arr(case, "events").iter().enumerate() {
        let status = OrderStatus::from_json(event)
            .map_err(|e| e.prefix(format!("events[{index}]")).to_string())?;
        match event.get("action").and_then(Value::as_str) {
            Some("submitted") => order_status::submitted(db.pool.clone(), &status),
            Some("cancelled") => order_status::cancelled(db.pool.clone(), &status),
            other => return Err(format!("event with unknown action {other:?}: {event}")),
        }
        settle(&db.pool).await?;
    }
    settle(&db.pool).await?;

    if let Some(expected) = case["expect"].get("open_orders") {
        assert_exact(
            &format!("{}/open_orders", case["name"].as_str().unwrap_or("case")),
            read::open_orders(&db.pool).await,
            expected,
        );
    }
    Ok(())
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
        let prefix = format!("db.open_orders[{index}]");
        let asset = order
            .get("asset")
            .and_then(Value::as_str)
            .unwrap_or("stock");
        match asset {
            "option" => {
                let fk = OpenOptionOrdersFullKeys::from_json(order)
                    .map_err(|e| e.prefix(&prefix).to_string())?;
                db.open_option_orders()
                    .create(&fk)
                    .await
                    .map_err(|e| format!("{prefix}: {e:?}"))?;
            }
            "stock" => {
                let fk = OpenStockOrdersFullKeys::from_json(order)
                    .map_err(|e| e.prefix(&prefix).to_string())?;
                db.open_stock_orders()
                    .create(&fk)
                    .await
                    .map_err(|e| format!("{prefix}: {e:?}"))?;
            }
            other => return Err(format!("{prefix}: unknown asset `{other}`")),
        }
    }
    Ok(())
}

/// The handlers are fire-and-forget; wait until the durable state has been
/// identical for two consecutive polls (max ~3s).
async fn settle(pool: &PgPool) -> Result<(), String> {
    let mut last = read::open_orders(pool).await;
    let mut stable = 0;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let now = read::open_orders(pool).await;
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
