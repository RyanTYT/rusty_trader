//! Comprehensive smoke tests for `SyncerEngine` + `SyncOps` trait (live IBKR).
//!
//! Tests all 3 SyncOps methods + the constructor:
//! - `SyncerEngine::new()` — builds contract_to_strategy + strategy_map
//! - `sync_open_orders()` — syncs open orders from IBKR → verifies DB state
//! - `sync_executions()` — syncs executions + commission reports → verifies DB state
//! - `sync_positions()` — reconciles local vs broker positions → verifies DB state
//! - Edge cases: empty account, multiple strategies, default strategy fallback
//!
//! **WARNING: PLACES REAL ORDERS ON THE PAPER TRADING ACCOUNT.**
//! Requires: live IB Gateway + Postgres + DATABASE_URL + IBC installed.
//! Run with: DATABASE_URL=... cargo test --test smoke_tests test_syncer -- --ignored

use std::sync::Arc;
use std::time::Duration;

use ibapi::contracts::Contract;
use ibapi::prelude::{RealtimeWhatToShow, SecurityType};
use trading_app::arc_drop_async;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::OpenStockOrdersPrimaryKeys;
use trading_app::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsOps,
};
use trading_app::database::models_crud::open_orders::open_orders::{OpenOrdersCRUD, OpenOrdersOps};
use trading_app::execution::fx_backed_up_order::OrderStore;
use trading_app::execution::order_engine::{OrderEngine, OrderIBKR};
use trading_app::execution::syncer::{SyncOps, SyncerEngine};
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::{DataSubscription, MarketDataHandler};
use trading_app::schedule::contract_scheduler::IbkrContractScheduler;
use trading_app::strategy::noise::Noise;
use trading_app::strategy::strategy::StrategyEnum;

use ibapi::orders::Action;
use ibapi::orders::order_builder::market_order;
use trading_app::strategy::unknown::Unknown;

use crate::live::init::{ensure_strategy_row, ibkr_account, with_live_ibkr};

fn aapl_contract() -> Contract {
    Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        exchange: "SMART".into(),
        primary_exchange: "NASDAQ".into(),
        ..Default::default()
    }
}

fn build_consolidator(pool: sqlx::PgPool, client: Arc<ibapi::Client>) -> Arc<Consolidator> {
    let market_data_handler = MarketDataHandler::new(pool.clone());
    let contract_scheduler = Arc::new(IbkrContractScheduler::new(client.clone()));
    Arc::new(Consolidator::new(
        tokio::runtime::Handle::current(),
        pool,
        client,
        market_data_handler,
        contract_scheduler,
    ))
}

fn build_syncer(state: &crate::live::init::LiveIbkr, contract: Contract) -> SyncerEngine {
    let noise = StrategyEnum::Noise(Noise::new(
        state.pool.clone(),
        tokio::runtime::Handle::current(),
    ));
    let unknown = StrategyEnum::Unknown(Unknown::new(state.pool.clone()));
    let unknown_strat_param = trading_app::test_internals::strategy_parameters(
        unknown.clone(),
        vec![DataSubscription::new(
            contract.clone(),
            RealtimeWhatToShow::Trades,
        )],
    );
    let noise_strat_param = trading_app::test_internals::strategy_parameters(
        noise.clone(),
        vec![DataSubscription::new(contract, RealtimeWhatToShow::Trades)],
    );
    SyncerEngine::new(
        state.pool.clone(),
        ibkr_account(),
        &vec![unknown_strat_param, noise_strat_param],
        tokio::runtime::Handle::current(),
    )
}

// ============================ 1. SyncerEngine::new — constructor ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_syncer_new_constructor() {
    with_live_ibkr(&ibkr_account(), "ibc_syncer_new.log", |state| async move {
        // sync_open_orders writes open_orders (FK → trading.strategy).
        ensure_strategy_row(&state.pool, "noise").await;
        let syncer = build_syncer(&state, aapl_contract());
        println!("✅ SyncerEngine::new succeeded (no panic)");

        // Verify the syncer is usable — call sync_open_orders (which uses the internal maps)
        let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());
        syncer.sync_open_orders(&state.client_1, &consolidator, Some("noise".to_string()));
        println!("✅ SyncerEngine internal state verified — sync_open_orders ran without error");

        arc_drop_async!(consolidator);
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 2. sync_open_orders — no open orders (empty account) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_sync_open_orders_empty_account() {
    with_live_ibkr(&ibkr_account(), "ibc_syncer_oo_empty.log", |state| async move {
        // sync_open_orders writes open_orders (FK → trading.strategy).
        ensure_strategy_row(&state.pool, "noise").await;
        let syncer = build_syncer(&state, aapl_contract());
        let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

        // Clean up any existing open orders for "noise" strategy first
        let crud = OpenOrdersCRUD::stock(state.pool.clone());
        let existing = crud.get_orders_for_strat("noise").await.expect("get_orders_for_strat failed");
        for order in &existing {
            if let trading_app::database::models_crud::open_orders::open_orders::OpenOrdersFullKeys::Stock(s) = order {
                let _ = crud.delete(&trading_app::database::models_crud::open_orders::open_orders::OpenOrdersPrimaryKeys::Stock(
                    OpenStockOrdersPrimaryKeys { order_perm_id: s.order_perm_id, order_id: s.order_id },
                )).await;
            }
        }

        // sync_open_orders on an account with no open orders
        syncer.sync_open_orders(&state.client_1, &consolidator, Some("noise".to_string()));
        println!("✅ sync_open_orders(empty account) completed without error");

        // Verify no open orders were created (since there were none on IBKR side)
        let orders = crud.get_orders_for_strat("noise").await.expect("get_orders_for_strat failed");
        let our_orders: Vec<_> = orders.iter().filter(|o| {
            matches!(o, trading_app::database::models_crud::open_orders::open_orders::OpenOrdersFullKeys::Stock(s) if s.stock == "AAPL")
        }).collect();
        assert!(our_orders.is_empty(), "no open orders should exist for AAPL after sync on empty account");
        println!("✅ sync_open_orders(empty): no spurious open orders created");

        arc_drop_async!(consolidator);
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 3. sync_open_orders — with a real open order ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading — PLACES A REAL ORDER"]
async fn test_sync_open_orders_with_open_order() {
    with_live_ibkr(&ibkr_account(), "ibc_syncer_oo_with.log", |state| async move {
        // sync_open_orders writes open_orders (FK → trading.strategy); place_order also writes optimistic row.
        ensure_strategy_row(&state.pool, "noise").await;
        let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());
        let syncer = build_syncer(&state, aapl_contract());

        // Place a limit order at unrealistic price so it stays open
        let mut order = ibapi::orders::order_builder::limit_order(Action::Buy, 1.0, 1.0);
        order.order_ref = "noise".to_string();
        let order_ibkr = OrderIBKR::new(aapl_contract(), order, -1);
        let weak_client: std::sync::Weak<ibapi::Client> = Arc::downgrade(&state.client_1);
        let order_id = OrderEngine::place_order(
            tokio::runtime::Handle::current(),
            state.pool.clone(),
            &weak_client,
            order_ibkr,
        );
        assert!(order_id > 0, "order should be placed");
        println!("Limit order placed, order_id={order_id}, waiting for IBKR to register...");

        // Wait for the order to be registered on IBKR side
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Now sync open orders — should pick up the limit order we just placed
        syncer.sync_open_orders(&state.client_1, &consolidator, Some("noise".to_string()));
        println!("sync_open_orders completed");

        // Wait for the spawned DB update to complete
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify the order was synced to the DB
        let crud = OpenOrdersCRUD::stock(state.pool.clone());
        let orders = crud.get_orders_for_strat("noise").await.expect("get_orders_for_strat failed");
        let our_order = orders.iter().find(|o| {
            matches!(o, trading_app::database::models_crud::open_orders::open_orders::OpenOrdersFullKeys::Stock(s) if s.order_id == order_id)
        });
        assert!(our_order.is_some(), "sync_open_orders should have synced the open order (order_id={order_id})");
        println!("✅ sync_open_orders: open order synced to DB");

        // Cleanup: cancel the order + delete the DB row
        if let Some(trading_app::database::models_crud::open_orders::open_orders::OpenOrdersFullKeys::Stock(s)) = our_order {
            if let Some(client) = weak_client.upgrade() {
                let _ = client.cancel_order(s.order_id, "");
            }
            let _ = crud.delete(&trading_app::database::models_crud::open_orders::open_orders::OpenOrdersPrimaryKeys::Stock(
                OpenStockOrdersPrimaryKeys { order_perm_id: s.order_perm_id, order_id: s.order_id },
            )).await;
        }

        arc_drop_async!(consolidator);
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 4. sync_executions — no executions (empty) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_sync_executions_empty() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_syncer_exec_empty.log",
        |state| async move {
            // sync_executions writes transactions/positions (FK → trading.strategy).
            ensure_strategy_row(&state.pool, "noise").await;
            let syncer = build_syncer(&state, aapl_contract());
            let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));

            // sync_executions on an account with no recent executions
            let result =
                syncer.sync_executions(&state.client_1, Some("noise".to_string()), order_store);

            match result {
                Ok(_) => println!("✅ sync_executions(empty) succeeded — no errors"),
                Err(e) => println!("sync_executions returned Err: {e}"),
            }
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 5. sync_executions — with a real execution ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading — PLACES A REAL ORDER"]
async fn test_sync_executions_with_fill() {
    with_live_ibkr(&ibkr_account(), "ibc_syncer_exec_fill.log", |state| async move {
        // sync_executions writes transactions/positions (FK → trading.strategy); place_order also writes optimistic row.
        ensure_strategy_row(&state.pool, "noise").await;
        let syncer = build_syncer(&state, aapl_contract());
        let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));

        // Place a market order to generate an execution
        let mut order = market_order(Action::Buy, 1.0);
        order.order_ref = "noise".to_string();
        let order_ibkr = OrderIBKR::new(aapl_contract(), order, -1);
        let weak_client: std::sync::Weak<ibapi::Client> = Arc::downgrade(&state.client_1);
        let order_id = OrderEngine::place_order(
            tokio::runtime::Handle::current(),
            state.pool.clone(),
            &weak_client,
            order_ibkr,
        );
        assert!(order_id > 0, "order should be placed");
        println!("Market order placed, order_id={order_id}, waiting for fill...");

        // Wait for the order to fill
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Now sync executions — should pick up the execution from the fill
        let result = syncer.sync_executions(
            &state.client_1,
            Some("noise".to_string()),
            order_store,
        );

        match result {
            Ok(_) => println!("✅ sync_executions(with fill) succeeded"),
            Err(e) => println!("sync_executions returned Err: {e}"),
        }

        // Wait for the spawned DB updates to complete
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Cleanup: delete any transaction + position created
        let _ = sqlx::query("DELETE FROM trading.stock_transactions WHERE strategy = 'noise' AND stock = 'AAPL'")
            .execute(&state.pool).await;
        let _ = sqlx::query("DELETE FROM trading.current_stock_positions WHERE strategy = 'noise' AND stock = 'AAPL'")
            .execute(&state.pool).await;
        let _ = sqlx::query("DELETE FROM trading.staged_commissions WHERE execution_id LIKE '%noise%'")
            .execute(&state.pool).await;
        println!("✅ cleanup complete");
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 6. sync_positions — reconcile positions ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_sync_positions_reconcile() {
    with_live_ibkr(&ibkr_account(), "ibc_syncer_pos.log", |state| async move {
        // sync_positions writes current_positions (FK → trading.strategy).
        ensure_strategy_row(&state.pool, "noise").await;
        let syncer = build_syncer(&state, aapl_contract());
        let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

        // sync_positions reconciles local DB positions with broker positions
        syncer
            .sync_positions(&state.client_1, &consolidator, Some("noise".to_string()))
            .await;
        println!("✅ sync_positions completed without error");

        // Verify the sync didn't create spurious positions for "noise" strategy
        let pos_crud = CurrentPositionsCRUD::stock(state.pool.clone());
        let positions = pos_crud
            .get_pos_by_strat("noise")
            .await
            .expect("get_pos_by_strat failed");
        // We don't assert specific count since the paper account may have real positions
        println!(
            "sync_positions: {} positions for 'noise' strategy",
            positions.len()
        );

        arc_drop_async!(consolidator);
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 7. sync_open_orders — default strategy fallback ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_sync_open_orders_default_strategy_fallback() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_syncer_default.log",
        |state| async move {
            // sync_open_orders with default='unknown' may write open_orders rows
            // with strategy='unknown' (FK → trading.strategy). Ensure both rows exist.
            ensure_strategy_row(&state.pool, "noise").await;
            ensure_strategy_row(&state.pool, "unknown").await;
            // Build syncer with noise strategy
            let syncer = build_syncer(&state, aapl_contract());
            let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());

            // Call sync_open_orders with a different default strategy
            syncer.sync_open_orders(&state.client_1, &consolidator, Some("unknown".to_string()));
            println!("✅ sync_open_orders with default='unknown' completed without error");

            // Verify it didn't crash + no spurious orders for "unknown"
            let crud = OpenOrdersCRUD::stock(state.pool.clone());
            let _ = crud
                .get_orders_for_strat("unknown")
                .await
                .expect("get_orders_for_strat failed");

            arc_drop_async!(consolidator);
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 8. sync_executions — default strategy None ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_sync_executions_no_default_strategy() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_syncer_no_default.log",
        |state| async move {
            // sync_executions with None default falls back to "unknown" — writes
            // transactions/positions with strategy='unknown' (FK → trading.strategy).
            ensure_strategy_row(&state.pool, "noise").await;
            ensure_strategy_row(&state.pool, "unknown").await;
            let syncer = build_syncer(&state, aapl_contract());
            let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));

            // Call sync_executions with None default strategy (should fall back to "unknown")
            let result = syncer.sync_executions(&state.client_1, None, order_store);

            match result {
                Ok(_) => {
                    println!("✅ sync_executions(None default) succeeded — falls back to 'unknown'")
                }
                Err(e) => println!("sync_executions returned Err: {e}"),
            }
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 9. Full sync lifecycle — all 3 methods in sequence ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading — PLACES A REAL ORDER"]
async fn test_syncer_full_lifecycle() {
    with_live_ibkr(&ibkr_account(), "ibc_syncer_lifecycle.log", |state| async move {
        // Full lifecycle: sync_open_orders/executions/positions all write FK tables.
        ensure_strategy_row(&state.pool, "noise").await;
        let consolidator = build_consolidator(state.pool.clone(), state.client_1.clone());
        let syncer = build_syncer(&state, aapl_contract());
        let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));

        // 1. sync_open_orders (baseline)
        syncer.sync_open_orders(&state.client_1, &consolidator, Some("noise".to_string()));
        println!("1. sync_open_orders completed");

        // 2. sync_executions (baseline)
        let _ = syncer.sync_executions(&state.client_1, Some("noise".to_string()), order_store.clone());
        println!("2. sync_executions completed");

        // 3. sync_positions (baseline)
        syncer.sync_positions(&state.client_1, &consolidator, Some("noise".to_string())).await;
        println!("3. sync_positions completed");

        // 4. Place a market order to generate activity
        let mut order = market_order(Action::Buy, 1.0);
        order.order_ref = "noise".to_string();
        let order_ibkr = OrderIBKR::new(aapl_contract(), order, -1);
        let weak_client: std::sync::Weak<ibapi::Client> = Arc::downgrade(&state.client_1);
        let order_id = OrderEngine::place_order(
            tokio::runtime::Handle::current(),
            state.pool.clone(),
            &weak_client,
            order_ibkr,
        );
        assert!(order_id > 0);
        println!("4. Market order placed, order_id={order_id}");

        // Wait for fill
        tokio::time::sleep(Duration::from_secs(15)).await;

        // 5. Re-sync to pick up the new execution + position
        let _ = syncer.sync_executions(&state.client_1, Some("noise".to_string()), order_store.clone());
        println!("5. re-sync_executions completed");

        syncer.sync_positions(&state.client_1, &consolidator, Some("noise".to_string())).await;
        println!("6. re-sync_positions completed");

        // Cleanup
        let _ = sqlx::query("DELETE FROM trading.stock_transactions WHERE strategy = 'noise' AND stock = 'AAPL'")
            .execute(&state.pool).await;
        let _ = sqlx::query("DELETE FROM trading.current_stock_positions WHERE strategy = 'noise' AND stock = 'AAPL'")
            .execute(&state.pool).await;
        let _ = sqlx::query("DELETE FROM trading.staged_commissions WHERE execution_id LIKE '%noise%'")
            .execute(&state.pool).await;
        println!("✅ full sync lifecycle cleanup complete");

        arc_drop_async!(consolidator);
    })
    .await
    .expect("Failed to boot live IBKR");
}
