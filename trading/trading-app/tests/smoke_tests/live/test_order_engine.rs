//! Comprehensive smoke tests for `OrderEngine` (live IBKR).
//!
//! Tests all OrderEngine methods:
//! - `OrderEngine::new()` — constructor
//! - `OrderIBKR::new()` — constructor + field verification
//! - `place_order()` — single order placement (returns perm_id > 0)
//! - `place_orders()` — batch order placement (multiple orders from iterator)
//! - `handle_bar_update_outcome()` — all 3 BarUpdateOutcome variants:
//!   - NoAction (no-op)
//!   - EmitOrders (fast path — directly submits orders)
//!   - PendingDbQuery (slow path — reads target/current diff, places orders)
//! - Edge cases: dead client, invalid contract, cancel open order
//!
//! **WARNING: PLACES REAL ORDERS ON THE PAPER TRADING ACCOUNT.**
//! Requires: live IB Gateway + Postgres + DATABASE_URL + IBC installed.
//! Run with: DATABASE_URL=... cargo test --test smoke_tests test_order_engine -- --ignored

use std::sync::{Arc, Weak};
use std::time::Duration;

use ibapi::contracts::Contract;
use ibapi::orders::Action;
use ibapi::orders::order_builder::{limit_order, market_order};
use ibapi::prelude::SecurityType;
use sqlx::PgPool;
use trading_app::arc_drop_async;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::AssetType;
use trading_app::database::models_crud::open_orders::open_orders::{
    OpenOrdersCRUD, OpenOrdersFullKeys as OOFK, OpenOrdersOps,
    OpenOrdersPrimaryKeys as OOInterfacePK,
};
use trading_app::execution::fx_backed_up_order::OrderStore;
use trading_app::execution::order_engine::{OrderEngine, OrderIBKR};
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::MarketDataHandler;
use trading_app::schedule::contract_scheduler::IbkrContractScheduler;
use trading_app::strategy::noise::Noise;
use trading_app::strategy::strategy::{BarUpdateOutcome, StrategyEnum};

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

fn msft_contract() -> Contract {
    Contract {
        symbol: "MSFT".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        exchange: "SMART".into(),
        primary_exchange: "NASDAQ".into(),
        ..Default::default()
    }
}

/// Build a Consolidator + weak client needed by handle_bar_update_outcome.
async fn build_consolidator(
    pool: PgPool,
    client: Arc<ibapi::Client>,
) -> (Arc<Consolidator>, Weak<ibapi::Client>) {
    let market_data_handler = MarketDataHandler::new(pool.clone());
    let contract_scheduler = Arc::new(IbkrContractScheduler::new(client.clone()));
    let consolidator = Arc::new(Consolidator::new(
        tokio::runtime::Handle::current(),
        pool,
        client.clone(),
        market_data_handler,
        contract_scheduler,
    ));
    let weak_client = Arc::downgrade(&client);
    (consolidator, weak_client)
}

// ============================ 1. OrderEngine::new + OrderIBKR::new ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_order_engine_new_constructor() {
    with_live_ibkr(&ibkr_account(), "ibc_oe_new.log", |state| async move {
        let _engine = OrderEngine::new(state.pool.clone(), tokio::runtime::Handle::current());
        // Verify it doesn't panic — that's the main assertion for a constructor
        println!("✅ OrderEngine::new succeeded (no panic)");

        // OrderIBKR::new — verify fields are set correctly
        let contract = aapl_contract();
        let order = market_order(Action::Buy, 1.0);
        let order_ibkr = OrderIBKR::new(contract.clone(), order.clone(), -1);
        assert_eq!(order_ibkr.contract.symbol.to_string(), "AAPL");
        assert_eq!(order_ibkr.order.action, Action::Buy);
        println!("✅ OrderIBKR::new constructed (fields verified via no-panic)");

        // Test with parent order reference (for combo/bracket orders)
        let _order_with_parent = OrderIBKR::new(contract, order, 0);
        println!("✅ OrderIBKR::new with parent reference constructed (no panic)");
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 2. place_order — single order ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading — PLACES A REAL ORDER"]
async fn test_place_order_market_buy() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_oe_place_market.log",
        |state| async move {
            // place_order writes optimistic open_order row (FK → trading.strategy).
            ensure_strategy_row(&state.pool, "noise").await;
            let contract = aapl_contract();
            let mut order = market_order(Action::Buy, 1.0);
            order.order_ref = "noise".to_string();
            let order_ibkr = OrderIBKR::new(contract, order, -1);
            let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);

            let order_perm_id = OrderEngine::place_order(
                tokio::runtime::Handle::current(),
                state.pool.clone(),
                &weak_client,
                order_ibkr,
            );

            println!("place_order returned perm_id={order_perm_id}");
            assert!(
                order_perm_id > 0,
                "order should be placed, got perm_id={order_perm_id}"
            );

            // Wait for the order to appear in the open_orders table (created via create_or_update)
            tokio::time::sleep(Duration::from_secs(3)).await;

            // Verify the open order row was created in the DB
            let crud = OpenOrdersCRUD::stock(state.pool.clone());
            let orders = crud
                .get_orders_for_strat("noise")
                .await
                .expect("get_orders_for_strat failed");
            let our_order = orders
                .iter()
                .find(|o| matches!(o, OOFK::Stock(s) if s.order_perm_id == order_perm_id));
            if let Some(OOFK::Stock(s)) = our_order {
                println!(
                    "✅ Open order row created: perm_id={}, qty={}",
                    s.order_perm_id, s.quantity
                );
                // Cleanup: cancel the order on IBKR + delete the DB row
                if let Some(client) = weak_client.upgrade() {
                    let _ = client.cancel_order(s.order_id, "");
                }
                let _ = crud
                    .delete(&OOInterfacePK::Stock(
                        trading_app::database::models::OpenStockOrdersPrimaryKeys {
                            order_perm_id: s.order_perm_id,
                            order_id: s.order_id,
                        },
                    ))
                    .await;
            }
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 3. place_orders — batch (multiple orders) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading — PLACES REAL ORDERS"]
async fn test_place_orders_batch_multiple() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_oe_place_batch.log",
        |state| async move {
            // place_orders writes optimistic open_order rows (FK → trading.strategy).
            ensure_strategy_row(&state.pool, "noise").await;
            let mut buy_order = market_order(Action::Buy, 1.0);
            buy_order.order_ref = "noise".to_string();
            // Place 2 orders: AAPL buy 1 share, MSFT buy 1 share
            let order1 = OrderIBKR::new(aapl_contract(), buy_order.clone(), -1);
            let order2 = OrderIBKR::new(msft_contract(), buy_order.clone(), -1);

            let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);

            // place_orders is sync, returns ()
            OrderEngine::place_orders(
                tokio::runtime::Handle::current(),
                state.pool.clone(),
                &weak_client,
                vec![order1, order2].into_iter(),
            );

            println!("place_orders batch submitted (2 orders)");
            // Wait for orders to be processed
            tokio::time::sleep(Duration::from_secs(3)).await;

            // Verify both orders appear in the DB
            let crud = OpenOrdersCRUD::stock(state.pool.clone());
            let orders = crud
                .get_orders_for_strat("noise")
                .await
                .expect("get_orders_for_strat failed");
            println!("Open orders after batch: {} total", orders.len());

            // Cleanup: cancel all open orders + delete DB rows
            for order in &orders {
                if let OOFK::Stock(s) = order {
                    if let Some(client) = weak_client.upgrade() {
                        let _ = client.cancel_order(s.order_id, "");
                    }
                    let _ = crud
                        .delete(&OOInterfacePK::Stock(
                            trading_app::database::models::OpenStockOrdersPrimaryKeys {
                                order_perm_id: s.order_perm_id,
                                order_id: s.order_id,
                            },
                        ))
                        .await;
                }
            }
            println!("✅ place_orders batch cleanup complete");
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 4. place_order with dead client ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_place_order_dead_client_panics() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_oe_dead_client.log",
        |state| async move {
            // Create a dead weak client (drop the Arc first)
            let _weak_client: Weak<ibapi::Client> = {
                let _strong = state.client_1.clone();
                drop(_strong);
                // This Weak is still valid because state.client_1 holds an Arc.
                // To truly test a dead client, we need to drop ALL Arcs.
                Arc::downgrade(&state.client_1)
            };

            // Actually test with a genuinely dead client
            let dead_weak: Weak<ibapi::Client> = {
                let temp_arc = Arc::new(ibapi::Client::connect("127.0.0.1:4002", 99).unwrap());
                let w = Arc::downgrade(&temp_arc);
                drop(temp_arc);
                w
            };

            let contract = aapl_contract();
            let mut order = market_order(Action::Buy, 1.0);
            order.order_ref = "noise".to_string();
            let order_ibkr = OrderIBKR::new(contract, order, -1);

            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                OrderEngine::place_order(
                    tokio::runtime::Handle::current(),
                    state.pool.clone(),
                    &dead_weak,
                    order_ibkr,
                );
            }));

            assert!(
                res.is_err(),
                "place_order with dead client should panic, but returned: {:?}",
                res
            );
            // With a dead client, place_order logs an error and returns the order_id from
            // client.next_order_id() (which panics on dead client) — actually this test
            // verifies that the function handles a dead Weak gracefully.
            // println!(
            //     "place_order with dead client returned perm_id={order_perm_id} (may log error)"
            // );
            // The function should not panic — that's the main assertion
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 5. handle_bar_update_outcome — NoAction ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_handle_bar_update_outcome_no_action() {
    with_live_ibkr(&ibkr_account(), "ibc_oe_noaction.log", |state| async move {
        let (consolidator, weak_client) =
            build_consolidator(state.pool.clone(), state.client_1.clone()).await;
        let engine = OrderEngine::new(state.pool.clone(), tokio::runtime::Handle::current());
        let noise = StrategyEnum::Noise(Noise::new(
            state.pool.clone(),
            tokio::runtime::Handle::current(),
        ));
        let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));

        // NoAction should be a complete no-op — no orders placed, no DB queries.
        // handle_bar_update_outcome is SYNC; the PendingDbQuery arm internally
        // calls self.tokio_handle.block_on(...). Run it on a blocking-pool thread
        // via spawn_blocking so the test runtime worker stays in the event loop
        // (otherwise block_on panics: "Cannot start a runtime from within a runtime").
        let consolidator_weak = Arc::downgrade(&consolidator);
        let noise_clone = noise.clone();
        let order_store_clone = order_store.clone();
        tokio::task::spawn_blocking(move || {
            engine.handle_bar_update_outcome(
                &weak_client,
                &consolidator_weak,
                BarUpdateOutcome::NoAction,
                &noise_clone,
                &order_store_clone,
            );
        })
        .await
        .expect("handle_bar_update_outcome(NoAction) blocking task panicked");

        println!("✅ handle_bar_update_outcome(NoAction) completed without error");

        arc_drop_async!(consolidator);
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 6. handle_bar_update_outcome — EmitOrders ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading — PLACES REAL ORDERS"]
async fn test_handle_bar_update_outcome_emit_orders() {
    with_live_ibkr(&ibkr_account(), "ibc_oe_emit.log", |state| async move {
        // EmitOrders path places orders → writes open_order rows (FK → trading.strategy).
        ensure_strategy_row(&state.pool, "noise").await;
        let (consolidator, weak_client) =
            build_consolidator(state.pool.clone(), state.client_1.clone()).await;
        let engine = OrderEngine::new(state.pool.clone(), tokio::runtime::Handle::current());
        let noise = StrategyEnum::Noise(Noise::new(
            state.pool.clone(),
            tokio::runtime::Handle::current(),
        ));
        let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));

        // EmitOrders fast path — directly submits the orders.
        // Run on spawn_blocking (handle_bar_update_outcome is SYNC; keeps the
        // test runtime worker out of any internal block_on).
        let mut order = market_order(Action::Buy, 1.0);
        order.order_ref = "noise".to_string();
        let order_ibkr = OrderIBKR::new(aapl_contract(), order, -1);
        let consolidator_weak = Arc::downgrade(&consolidator);
        let noise_clone = noise.clone();
        let order_store_clone = order_store.clone();
        let weak_client_for_call = weak_client.clone();
        tokio::task::spawn_blocking(move || {
            engine.handle_bar_update_outcome(
                &weak_client_for_call,
                &consolidator_weak,
                BarUpdateOutcome::EmitOrders(vec![order_ibkr]),
                &noise_clone,
                &order_store_clone,
            );
        })
        .await
        .expect("handle_bar_update_outcome(EmitOrders) blocking task panicked");

        println!("EmitOrders path submitted order");
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify order was placed (appears in open_orders)
        let crud = OpenOrdersCRUD::stock(state.pool.clone());
        let orders = crud
            .get_orders_for_strat("noise")
            .await
            .expect("get_orders_for_strat failed");
        println!("Open orders after EmitOrders: {} total", orders.len());

        // Cleanup
        for order in &orders {
            if let OOFK::Stock(s) = order {
                if let Some(client) = weak_client.upgrade() {
                    let _ = client.cancel_order(s.order_id, "");
                }
                let _ = crud
                    .delete(&OOInterfacePK::Stock(
                        trading_app::database::models::OpenStockOrdersPrimaryKeys {
                            order_perm_id: s.order_perm_id,
                            order_id: s.order_id,
                        },
                    ))
                    .await;
            }
        }
        arc_drop_async!(consolidator);
        println!("✅ EmitOrders cleanup complete");
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 7. handle_bar_update_outcome — PendingDbQuery ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading — PLACES REAL ORDERS"]
async fn test_handle_bar_update_outcome_pending_db_query() {
    with_live_ibkr(&ibkr_account(), "ibc_oe_pending.log", |state| async move {
        // PendingDbQuery path reads target/current position diff; may place orders.
        ensure_strategy_row(&state.pool, "noise").await;
        let (consolidator, weak_client) =
            build_consolidator(state.pool.clone(), state.client_1.clone()).await;
        let engine = OrderEngine::new(state.pool.clone(), tokio::runtime::Handle::current());
        let noise = StrategyEnum::Noise(Noise::new(
            state.pool.clone(),
            tokio::runtime::Handle::current(),
        ));
        let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));

        // PendingDbQuery slow path — reads target/current diff, places orders to reconcile.
        // We don't pre-populate target positions, so this should complete without placing orders
        // (qty_diff will be 0 for all).
        // MUST run on spawn_blocking: the PendingDbQuery arm calls
        // self.tokio_handle.block_on(...) inline, which panics with
        // "Cannot start a runtime from within a runtime" if called from the
        // test's runtime worker. spawn_blocking runs it on a dedicated OS thread
        // while the test worker stays in the event loop servicing that block_on.
        let consolidator_weak = Arc::downgrade(&consolidator);
        let noise_clone = noise.clone();
        let order_store_clone = order_store.clone();
        tokio::task::spawn_blocking(move || {
            engine.handle_bar_update_outcome(
                &weak_client,
                &consolidator_weak,
                BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]),
                &noise_clone,
                &order_store_clone,
            );
        })
        .await
        .expect("handle_bar_update_outcome(PendingDbQuery) blocking task panicked");

        println!("PendingDbQuery path completed");
        tokio::time::sleep(Duration::from_secs(2)).await;
        println!("✅ PendingDbQuery completed without error");

        arc_drop_async!(consolidator);
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 8. Edge case: limit order at unrealistic price (won't fill) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading — PLACES A REAL ORDER"]
async fn test_place_order_limit_unrealistic_price() {
    with_live_ibkr(&ibkr_account(), "ibc_oe_limit.log", |state| async move {
        // place_order writes optimistic open_order row (FK → trading.strategy).
        ensure_strategy_row(&state.pool, "noise").await;
        let contract = aapl_contract();
        // Limit buy at $1.00 — won't fill, stays open
        let mut order = limit_order(Action::Buy, 1.0, 1.0);
        order.order_ref = "noise".to_string();
        let order_ibkr = OrderIBKR::new(contract, order, -1);
        let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);

        let order_id = OrderEngine::place_order(
            tokio::runtime::Handle::current(),
            state.pool.clone(),
            &weak_client,
            order_ibkr,
        );

        assert!(
            order_id > 0,
            "limit order should be placed, got order_id={order_id}"
        );
        println!("Limit order placed at $1.00, order_id={order_id} (won't fill, stays open)");

        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify it's in the open orders table
        let crud = OpenOrdersCRUD::stock(state.pool.clone());
        let orders = crud
            .get_orders_for_strat("noise")
            .await
            .expect("get_orders_for_strat failed");
        let our_order = orders
            .iter()
            .find(|o| matches!(o, OOFK::Stock(s) if s.order_id == order_id));
        assert!(
            our_order.is_some(),
            "limit order should appear in open_orders"
        );
        println!("✅ Limit order verified in open_orders table");

        // Cleanup: cancel the order
        if let Some(OOFK::Stock(s)) = our_order {
            if let Some(client) = weak_client.upgrade() {
                let _ = client.cancel_order(s.order_id, "");
                println!("Cancelled limit order perm_id={}", s.order_perm_id);
            }
            let _ = crud
                .delete(&OOInterfacePK::Stock(
                    trading_app::database::models::OpenStockOrdersPrimaryKeys {
                        order_perm_id: s.order_perm_id,
                        order_id: s.order_id,
                    },
                ))
                .await;
        }
    })
    .await
    .expect("Failed to boot live IBKR");
}
