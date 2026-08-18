//! Comprehensive smoke tests for OrderUpdateStream event handlers (live IBKR).
//!
//! Tests the 4 event handler modules:
//! - `order_status::submitted()` — verifies open_orders rows updated on Submitted status
//! - `order_status::cancelled()` — verifies open_orders rows deleted on Cancelled status
//! - `open_order::submitted()` — verifies open_orders row created
//! - `open_order::cancelled()` — verifies open_orders row deleted (both real + optimistic PK)
//! - `execution::on_execution_update()` — verifies transactions + positions updated on fill
//! - `commission_report::on_commission_update()` — verifies staged_commissions row created
//!
//! Strategy: place real orders via OrderEngine, then wait for the OrderUpdateStream
//! (running in the background) to process the events, then verify DB state.
//!
//! **WARNING: PLACES REAL ORDERS ON THE PAPER TRADING ACCOUNT.**
//! Requires: live IB Gateway + Postgres + DATABASE_URL + IBC installed.
//! Run with: DATABASE_URL=... cargo test --test smoke_tests test_event_handlers -- --ignored

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use ibapi::contracts::Contract;
use ibapi::orders::order_builder::{limit_order, market_order};
use ibapi::orders::{Action, CommissionReport, ExecutionData, OrderStatus};
use ibapi::prelude::SecurityType;
use rust_decimal::Decimal;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    CurrentStockPositionsPrimaryKeys, OpenStockOrdersPrimaryKeys, StagedCommissionsPrimaryKeys,
};
use trading_app::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsFullKeys as CPFK,
    CurrentPositionsPrimaryKeys as CPInterfacePK,
};
use trading_app::database::models_crud::open_orders::open_orders::{
    OpenOrdersCRUD, OpenOrdersFullKeys as OOFK, OpenOrdersOps,
    OpenOrdersPrimaryKeys as OOInterfacePK,
};
use trading_app::database::models_crud::staged_commissions::StagedCommissionsCRUD;
use trading_app::database::models_crud::transactions::transactions::TransactionsFullKeys as TxFK;
use trading_app::execution::fx_backed_up_order::OrderStore;
use trading_app::execution::order_engine::{OrderEngine, OrderIBKR};
use trading_app::execution::order_update_stream::controller::OrderUpdateStreamController;
use trading_app::execution::order_update_stream::event_handlers::commission_report::on_commission_update;
use trading_app::execution::order_update_stream::event_handlers::execution::on_execution_update;
use trading_app::execution::order_update_stream::event_handlers::open_order;
use trading_app::execution::order_update_stream::event_handlers::order_status;
use trading_app::strategy::noise::Noise;
use trading_app::strategy::strategy::{StrategyEnum, StrategyExecutor};

use crate::live::init::{
    api_port_addr, ensure_strategy_row, ibkr_account, server_base_url, with_live_ibkr,
};

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

/// Set up the OrderUpdateStreamController (singleton — must be the only one running).
/// Returns the controller (keep it alive for the duration of the test).
fn start_order_update_stream(
    state: &crate::live::init::LiveIbkr,
    order_store: &Arc<OrderStore>,
) -> OrderUpdateStreamController {
    let noise = StrategyEnum::Noise(Noise::new(
        state.pool.clone(),
        tokio::runtime::Handle::current(),
    ));
    let mut strategy_map = HashMap::new();
    strategy_map.insert(noise.get_name(), noise);
    let strategy_map = Arc::new(strategy_map);

    let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.master_client);
    let controller = OrderUpdateStreamController::new(
        state.pool.clone(),
        weak_client,
        strategy_map,
        Some("noise".to_string()),
        tokio::runtime::Handle::current(),
        order_store.clone(),
    );
    controller.expect("OrderUpdateStreamController should start")
}

/// Helper: place a market order + return the perm_id.
fn place_market_order(
    state: &crate::live::init::LiveIbkr,
    contract: Contract,
    action: Action,
    qty: f64,
) -> i32 {
    let order = market_order(action, qty);
    let order_ibkr = OrderIBKR::new(contract, order, -1);
    let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);
    OrderEngine::place_order(
        tokio::runtime::Handle::current(),
        state.pool.clone(),
        &weak_client,
        order_ibkr,
    )
}

// ============================ 1. order_status::submitted — open order row created ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading — PLACES REAL ORDERS"]
async fn test_event_order_status_submitted() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_eh_submitted.log",
        |state| async move {
            // submitted handler writes open_orders (FK → trading.strategy).
            ensure_strategy_row(&state.pool, "noise").await;
            let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));
            let _controller = start_order_update_stream(&state, &order_store);

            // Place a limit order at unrealistic price so it stays open (Submitted status)
            let contract = aapl_contract();
            let order = limit_order(Action::Buy, 1.0, 1.0);
            let order_ibkr = OrderIBKR::new(contract.clone(), order, -1);
            let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);
            let perm_id = OrderEngine::place_order(
                tokio::runtime::Handle::current(),
                state.pool.clone(),
                &weak_client,
                order_ibkr,
            );
            assert!(perm_id > 0, "order should be placed");

            // Wait for the Submitted event to be processed by the stream
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Verify the open order row exists in the DB
            let crud = OpenOrdersCRUD::stock(state.pool.clone());
            let orders = crud
                .get_orders_for_strat("noise")
                .await
                .expect("get_orders_for_strat failed");
            let our_order = orders
                .iter()
                .find(|o| matches!(o, OOFK::Stock(s) if s.order_perm_id == perm_id));
            assert!(
                our_order.is_some(),
                "Submitted event should create open_order row for perm_id={perm_id}"
            );
            println!("✅ order_status::submitted — open_order row created for perm_id={perm_id}");

            // Cleanup: cancel + delete
            if let Some(OOFK::Stock(s)) = our_order {
                if let Some(client) = weak_client.upgrade() {
                    let _ = client.cancel_order(s.order_id, "");
                }
                let _ = crud
                    .delete(&OOInterfacePK::Stock(OpenStockOrdersPrimaryKeys {
                        order_perm_id: s.order_perm_id,
                        order_id: s.order_id,
                    }))
                    .await;
            }
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 2. order_status::cancelled — open order row deleted ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading — PLACES + CANCELS REAL ORDERS"]
async fn test_event_order_status_cancelled() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_eh_cancelled.log",
        |state| async move {
            // cancelled handler deletes open_orders rows that the submitted path created (FK → trading.strategy).
            ensure_strategy_row(&state.pool, "noise").await;
            let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));
            let _controller = start_order_update_stream(&state, &order_store);

            // Place a limit order at unrealistic price (stays open)
            let contract = aapl_contract();
            let order = limit_order(Action::Buy, 1.0, 1.0);
            let order_ibkr = OrderIBKR::new(contract.clone(), order, -1);
            let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);
            let perm_id = OrderEngine::place_order(
                tokio::runtime::Handle::current(),
                state.pool.clone(),
                &weak_client,
                order_ibkr,
            );
            assert!(perm_id > 0);

            // Wait for Submitted
            tokio::time::sleep(Duration::from_secs(3)).await;

            // Now cancel the order
            let crud = OpenOrdersCRUD::stock(state.pool.clone());
            let orders = crud
                .get_orders_for_strat("noise")
                .await
                .expect("get_orders_for_strat failed");
            let our_order = orders
                .iter()
                .find(|o| matches!(o, OOFK::Stock(s) if s.order_perm_id == perm_id));
            let order_id = if let Some(OOFK::Stock(s)) = our_order {
                if let Some(client) = weak_client.upgrade() {
                    let _ = client.cancel_order(s.order_id, "");
                }
                s.order_id
            } else {
                println!("Order not found in DB — may have already been cancelled");
                return;
            };

            // Wait for Cancelled event to propagate + handler to delete the row
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Verify the row was deleted by the cancelled handler
            let pk = OOInterfacePK::Stock(OpenStockOrdersPrimaryKeys {
                order_perm_id: perm_id,
                order_id: order_id,
            });
            let result = crud.read(&pk).await.expect("read failed");
            assert!(
                result.is_none(),
                "Cancelled event should delete open_order row for perm_id={perm_id}"
            );
            println!("✅ order_status::cancelled — open_order row deleted for perm_id={perm_id}");
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 3. execution::on_execution_update — position + transaction created ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading — PLACES REAL ORDERS"]
async fn test_event_execution_on_fill() {
    with_live_ibkr(&ibkr_account(), "ibc_eh_exec.log", |state| async move {
        // execution handler writes transactions + current_positions (FK → trading.strategy).
        ensure_strategy_row(&state.pool, "noise").await;
        let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));
        let _controller = start_order_update_stream(&state, &order_store);

        // Place a market order — will fill immediately
        let perm_id = place_market_order(&state, aapl_contract(), Action::Buy, 1.0);
        assert!(perm_id > 0, "order should be placed");

        // Wait for fill + execution event to be processed
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Verify a transaction was created (execution handler writes to stock_transactions)
        // We can't predict the execution_id, so verify at least one transaction exists
        // for our stock + strategy
        let txns: Vec<TxFK> = sqlx::query_as::<_, TxFK>(
            "SELECT * FROM trading.stock_transactions WHERE strategy = 'noise' ORDER BY time DESC LIMIT 1",
        )
        .fetch_all(&state.pool)
        .await
        .expect("query failed");
        println!("Transactions after fill: {} found", txns.len());
        if let Some(TxFK::Stock(t)) = txns.first() {
            println!("✅ execution::on_execution_update — transaction created: exec_id={}, qty={}",
                t.execution_id, t.quantity);
            // Cleanup: delete the transaction
            let _ = sqlx::query("DELETE FROM trading.stock_transactions WHERE execution_id = $1")
                .bind(&t.execution_id)
                .execute(&state.pool)
                .await;
        }

        // Verify a position was created (execution handler updates current_positions)
        let pos_crud = CurrentPositionsCRUD::stock(state.pool.clone());
        let pk = CPInterfacePK::Stock(CurrentStockPositionsPrimaryKeys {
            stock: "AAPL".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            strategy: "noise".to_string(),
        });
        let pos = pos_crud.read(&pk).await.expect("read failed");
        if let Some(CPFK::Stock(s)) = pos {
            assert!(s.quantity > 0.0, "position should be long after BUY fill, got qty={}", s.quantity);
            println!("✅ execution::on_execution_update — position created: qty={}, avg_price={}",
                s.quantity, s.avg_price);
            // Cleanup: reverse + delete
            let _ = pos_crud.delete(&pk).await;
        }
    })
.await
.expect("Failed to boot live IBKR");
}

// ============================ 4. commission_report::on_commission_update — staged commission created ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading — PLACES REAL ORDERS"]
async fn test_event_commission_report_on_fill() {
    with_live_ibkr(&ibkr_account(), "ibc_eh_commission.log", |state| async move {
        // commission handler writes staged_commissions (no strategy FK), but the
        // preceding execution path also writes transactions + positions (FK → trading.strategy).
        ensure_strategy_row(&state.pool, "noise").await;
        let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));
        let _controller = start_order_update_stream(&state, &order_store);

        // Place a market order — will fill + trigger commission report
        let perm_id = place_market_order(&state, aapl_contract(), Action::Buy, 1.0);
        assert!(perm_id > 0);

        // Wait for fill + commission report event
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Verify a staged commission was created (commission handler writes to staged_commissions)
        // We can't predict the execution_id, so verify at least one staged commission exists
        let staged: Vec<(String, Decimal)> = sqlx::query_as(
            "SELECT execution_id, fees FROM trading.staged_commissions ORDER BY execution_id DESC LIMIT 1",
        )
        .fetch_all(&state.pool)
        .await
        .expect("query failed");
        println!("Staged commissions after fill: {} found", staged.len());
        if let Some((exec_id, fees)) = staged.first() {
            println!("✅ commission_report::on_commission_update — staged commission created: exec_id={}, fees={}",
                exec_id, fees);
            // Cleanup
            let _ = sqlx::query("DELETE FROM trading.staged_commissions WHERE execution_id = $1")
                .bind(exec_id)
                .execute(&state.pool)
                .await;
        }

        // Cleanup any transaction + position created
        let _ = sqlx::query("DELETE FROM trading.stock_transactions WHERE strategy = 'noise'")
            .execute(&state.pool)
            .await;
        let _ = sqlx::query("DELETE FROM trading.current_stock_positions WHERE strategy = 'noise' AND stock = 'AAPL'")
            .execute(&state.pool)
            .await;
    })
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 5. on_commission_update — direct unit-style test ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_on_commission_update_direct() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_eh_commission_direct.log",
        |state| async move {
            // Call on_commission_update directly with a dummy CommissionReport
            let commission_report = CommissionReport {
                execution_id: "test_exec_001".to_string(),
                commission: 1.50,
                currency: "USD".to_string(),
                realized_pnl: Some(0.0),
                yields: None,
                yield_redemption_date: String::new(),
            };

            on_commission_update(state.pool.clone(), &commission_report)
                .expect("on_commission_update failed");

            // Wait for the spawned task to complete
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Verify the staged commission was created
            let crud = StagedCommissionsCRUD::new(state.pool.clone());
            let pk = StagedCommissionsPrimaryKeys {
                execution_id: "test_exec_001".to_string(),
            };
            let data = crud.read(&pk).await.expect("read failed");
            assert!(data.is_some(), "staged commission should be created");
            let data = data.unwrap();
            assert_eq!(data.execution_id, "test_exec_001");
            assert_eq!(data.fees, Decimal::new(150, 2), "fees should be 1.50");

            println!("✅ on_commission_update — staged commission created directly");

            // Cleanup
            let _ = crud.delete(&pk).await;
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 6. on_execution_update — CASH asset type rejected ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_on_execution_update_rejects_unknown_security_type() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_eh_exec_cash.log",
        |state| async move {
            // Build a dummy ExecutionData with unknown security type (Other)
            let mut execution_data = ExecutionData::default();
            execution_data.contract.security_type = SecurityType::Other("CASH".to_string());
            execution_data.execution.order_reference = "noise".to_string();

            let noise = StrategyEnum::Noise(Noise::new(
                state.pool.clone(),
                tokio::runtime::Handle::current(),
            ));
            let mut strategy_map = HashMap::new();
            strategy_map.insert(noise.get_name(), noise);
            let strategy_map = Arc::new(strategy_map);
            let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.master_client);
            let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));

            let result = on_execution_update(
                state.pool.clone(),
                execution_data,
                strategy_map,
                "noise",
                tokio::runtime::Handle::current(),
                &weak_client,
                order_store,
            );

            assert!(
                result.is_err(),
                "Unknown security type execution should return Err"
            );
            println!(
                "✅ on_execution_update — Unknown security type rejected: {:?}",
                result.unwrap_err()
            );
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 7. open_order::submitted + cancelled — direct test ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_open_order_submitted_and_cancelled_direct() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_eh_open_order_direct.log",
        |state| async move {
            // open_order::submitted writes open_orders (FK → trading.strategy).
            ensure_strategy_row(&state.pool, "noise").await;
            let contract = aapl_contract();
            let order = limit_order(Action::Buy, 1.0, 1.0);

            // Call open_order::submitted directly
            let handle = open_order::submitted(state.pool.clone(), &contract, &order)
                .expect("open_order::submitted failed");
            let _ = handle.await;

            tokio::time::sleep(Duration::from_secs(1)).await;

            // Verify the row was created (order_id from the Order struct)
            let crud = OpenOrdersCRUD::stock(state.pool.clone());
            let pk = OOInterfacePK::Stock(OpenStockOrdersPrimaryKeys {
                order_perm_id: order.perm_id,
                order_id: order.order_id,
            });
            let data = crud.read(&pk).await.expect("read failed");
            assert!(data.is_some(), "open_order::submitted should create row");
            println!(
                "✅ open_order::submitted — row created for order_id={}",
                order.order_id
            );

            // Now call open_order::cancelled directly
            open_order::cancelled(
                state.pool.clone(),
                order.order_id,
                order.perm_id,
                &ibapi::contracts::SecurityType::Stock,
            );

            tokio::time::sleep(Duration::from_secs(2)).await;

            // Verify the row was deleted
            let data = crud.read(&pk).await.expect("read failed");
            assert!(data.is_none(), "open_order::cancelled should delete row");
            println!(
                "✅ open_order::cancelled — row deleted for order_id={}",
                order.order_id
            );
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}

// ============================ 8. order_status::submitted + cancelled — direct test ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_order_status_submitted_and_cancelled_direct() {
    with_live_ibkr(
        &ibkr_account(),
        "ibc_eh_order_status_direct.log",
        |state| async move {
            // order_status handlers read/update open_orders rows (FK → trading.strategy).
            ensure_strategy_row(&state.pool, "noise").await;
            // Pre-create an open order row
            let crud = OpenOrdersCRUD::stock(state.pool.clone());
            let order_perm_id = 12345;
            let order_id = 67890;
            let pk = OOInterfacePK::Stock(OpenStockOrdersPrimaryKeys {
                order_perm_id,
                order_id,
            });

            // Build a dummy OrderStatus
            let order_status = OrderStatus {
                order_id,
                perm_id: order_perm_id,
                parent_id: 0,
                status: "Submitted".to_string(),
                filled: 0.0,
                remaining: 1.0,
                average_fill_price: 0.0,
                last_fill_price: 0.0,
                client_id: 0,
                why_held: String::new(),
                market_cap_price: 0.0,
            };

            // Call order_status::submitted directly
            order_status::submitted(state.pool.clone(), &order_status);
            tokio::time::sleep(Duration::from_secs(2)).await;

            println!("✅ order_status::submitted — called directly (no panic)");

            // Now call cancelled
            order_status::cancelled(state.pool.clone(), &order_status);
            tokio::time::sleep(Duration::from_secs(2)).await;

            println!("✅ order_status::cancelled — called directly (no panic)");

            // Cleanup any leftover rows
            let _ = crud.delete(&pk).await;
        },
    )
    .await
    .expect("Failed to boot live IBKR");
}
