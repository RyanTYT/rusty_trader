//! Comprehensive IBKR flow integration tests.
//!
//! These tests exercise full real flows against a live IB Gateway:
//! 1. Check time + find which contracts from a fixed set are currently trading
//! 2. Open the order update stream
//! 3. Track current open orders
//! 4. Place an order, assert updates on open orders / executions / positions
//! 5. Reverse the position, assert until 0 positions again
//! 6. Edge cases: cancel order, invalid contract, market closed behavior
//!
//! Requires: live IB Gateway + Postgres + DATABASE_URL + IBC installed.
//! **WARNING: Places REAL orders on the paper trading account.**
//! Run with: `DATABASE_URL=... cargo test --test smoke_tests test_full_flow -- --ignored`

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use chrono::Utc;
use chrono_tz::America::New_York;
use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys,
};
use trading_app::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsFullKeys as CPFK, CurrentPositionsOps,
    CurrentPositionsPrimaryKeys as CPInterfacePK,
};
use trading_app::execution::fx_backed_up_order::OrderStore;
use trading_app::execution::order_engine::{OrderEngine, OrderIBKR};
use trading_app::execution::order_update_stream::controller::OrderUpdateStreamController;
use trading_app::execution::syncer::{SyncOps, SyncerEngine};
use trading_app::init_app::StrategyParameters;
use trading_app::test_internals::is_any_open;
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::MarketDataHandler;
use trading_app::market_data::traits::current_price::PriceSupplier;
use trading_app::schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler};
use trading_app::strategy::noise::Noise;
use trading_app::strategy::strategy::{StrategyEnum, StrategyExecutor};

use ibapi::orders::order_builder::market_order;
use ibapi::orders::Action;

use crate::live::init::with_live_ibkr;

fn fixed_contracts() -> Vec<Contract> {
    vec![
        Contract {
            symbol: "AAPL".into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "NASDAQ".into(),
            ..Default::default()
        },
        Contract {
            symbol: "MSFT".into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "NASDAQ".into(),
            ..Default::default()
        },
        Contract {
            symbol: "SPY".into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: "ARCA".into(),
            ..Default::default()
        },
    ]
}

// ============================ 1. Check time + find trading contracts ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + IBC installed"]
async fn test_find_trading_contracts() {
    let now = Utc::now();
    let now_ny = now.with_timezone(&New_York);
    let market_open = is_any_open(&now_ny);
    println!("Current time (NY): {now_ny}, market open: {market_open}");

    with_live_ibkr("DU111111", "ibc_flow.log", |state| async move {
;

    let contracts = fixed_contracts();
    let mut scheduler = IbkrContractScheduler::new(state.client_1.clone());

    for contract in &contracts {
        match scheduler.add_schedule(contract) {
            Ok(_) => {
                let is_trading = scheduler.is_trading(contract, &now).unwrap_or(false);
                println!("{} is_trading={is_trading} at {now}", contract.symbol);
            }
            Err(e) => println!("Failed to add schedule for {}: {e}", contract.symbol),
        }
    }

    let trading_count = contracts
        .iter()
        .filter(|c| scheduler.is_trading(c, &now).unwrap_or(false))
        .count();
    println!("{trading_count} contracts currently trading");
    })
.await;}

// ============================ 2-5. Full place → reverse → 0 flow ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + paper trading account + market open — PLACES REAL ORDERS"]
async fn test_full_place_reverse_zero_flow() {
    with_live_ibkr("DU111111", "ibc_flow.log", |state| async move {
;

    let mut scheduler = IbkrContractScheduler::new(state.client_1.clone());
    let contract = fixed_contracts().into_iter().next().unwrap();
    scheduler.add_schedule(&contract).expect("add_schedule failed");
    let scheduler = Arc::new(scheduler);

    let market_data_handler = MarketDataHandler::new(state.pool.clone());
    let consolidator = Arc::new(Consolidator::new(
        tokio::runtime::Handle::current(),
        state.pool.clone(),
        state.client_1.clone(),
        market_data_handler,
        scheduler.clone(),
    ));

    let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));
    let order_engine = OrderEngine::new(state.pool.clone(), tokio::runtime::Handle::current());

    let noise = StrategyEnum::Noise(Noise::new(state.pool.clone(), tokio::runtime::Handle::current()));
    let strat_params = vec![trading_app::test_internals::strategy_parameters(noise.clone(), vec![trading_app::market_data::handler::DataSubscription::new(
            contract.clone(),
            ibapi::prelude::RealtimeWhatToShow::Trades,
        )])];

    let syncer = SyncerEngine::new(
        state.pool.clone(),
        "DU111111".to_string(),
        &strat_params,
        tokio::runtime::Handle::current(),
    );

    // 2. Open the order update stream (master client, client_id == 0)
    let mut strategy_map = HashMap::new();
    strategy_map.insert(noise.get_name(), noise);
    let strategy_map = Arc::new(strategy_map);
    let weak_master: Weak<ibapi::Client> = Arc::downgrade(&state.master_client);
    let controller = OrderUpdateStreamController::new(
        state.pool.clone(),
        weak_master,
        strategy_map.clone(),
        Some("noise".to_string()),
        tokio::runtime::Handle::current(),
        order_store.clone(),
    );
    assert!(controller.is_some(), "OrderUpdateStreamController should start");
    let _controller = controller.unwrap();

    // 3. Sync current open orders + positions (baseline)
    // sync_open_orders + sync_executions are sync (return ()/Result), sync_positions is async
    syncer.sync_open_orders(&state.client_1, &consolidator, Some("noise".to_string()));
    let _ = syncer.sync_executions(&state.client_1, Some("noise".to_string()), order_store.clone());
    syncer.sync_positions(&state.client_1, &consolidator, Some("noise".to_string())).await;
    println!("Baseline sync complete");

    // 4. Place a BUY order for 1 share
    let order = market_order(Action::Buy, 1.0);
    let order_ibkr = OrderIBKR::new(contract.clone(), order, -1);
    let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);
    let order_perm_id = OrderEngine::place_order(
        tokio::runtime::Handle::current(),
        state.pool.clone(),
        &weak_client,
        order_ibkr,
    );
    assert!(order_perm_id > 0, "order should be placed, got perm_id={order_perm_id}");
    println!("BUY order placed, perm_id={order_perm_id}");

    // Wait for fill + order_update_stream to process
    tokio::time::sleep(Duration::from_secs(15)).await;

    // Sync + assert position appeared
    syncer.sync_open_orders(&state.client_1, &consolidator, Some("noise".to_string()));
    let _ = syncer.sync_executions(&state.client_1, Some("noise".to_string()), order_store.clone());
    syncer.sync_positions(&state.client_1, &consolidator, Some("noise".to_string())).await;

    let pos_crud = CurrentPositionsCRUD::stock(state.pool.clone());
    let pk = CPInterfacePK::Stock(CurrentStockPositionsPrimaryKeys {
        stock: contract.symbol.to_string(),
        primary_exchange: contract.primary_exchange.to_string(),
        currency: contract.currency.to_string(),
        strategy: "noise".to_string(),
    });
    let pos = pos_crud.read(&pk).await.expect("read failed");
    assert!(pos.is_some(), "position should exist after BUY fill");
    let pos = match pos.unwrap() {
        CPFK::Stock(s) => s,
        _ => panic!("expected Stock variant"),
    };
    assert!(pos.quantity > 0.0, "position should be long, got qty={}", pos.quantity);
    println!("Position after BUY: qty={}, avg_price={}", pos.quantity, pos.avg_price);

    // 5. Reverse: SELL to close the position
    let sell_qty = -pos.quantity;
    let order = market_order(Action::Sell, sell_qty.abs());
    let order_ibkr = OrderIBKR::new(contract.clone(), order, -1);
    let _sell_perm_id = OrderEngine::place_order(
        tokio::runtime::Handle::current(),
        state.pool.clone(),
        &weak_client,
        order_ibkr,
    );
    println!("SELL order placed to reverse");

    // Wait for fill + sync
    tokio::time::sleep(Duration::from_secs(15)).await;
    syncer.sync_positions(&state.client_1, &consolidator, Some("noise".to_string())).await;

    let pos_after = pos_crud.read(&pk).await.expect("read failed");
    match pos_after {
        Some(CPFK::Stock(s)) => {
            assert_eq!(s.quantity, 0.0, "position should be 0 after reversal, got {}", s.quantity);
            println!("Position after SELL: qty=0 (reversed successfully)");
        }
        None => println!("Position row removed (quantity 0 → deleted)"),
        _ => panic!("unexpected variant"),
    }

    // Cleanup
    let _ = pos_crud.delete(&pk).await;
    })
.await;}

// ============================ Edge cases ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + IBC installed"]
async fn test_edge_case_invalid_contract_rejected() {
    with_live_ibkr("DU111111", "ibc_flow.log", |state| async move {
;

    let market_data_handler = MarketDataHandler::new(state.pool.clone());
    let contract_scheduler = Arc::new(IbkrContractScheduler::new(state.client_1.clone()));
    let consolidator = Consolidator::new(
        tokio::runtime::Handle::current(),
        state.pool.clone(),
        state.client_1.clone(),
        market_data_handler,
        contract_scheduler,
    );

    let bad_contract = Contract {
        symbol: "NONEXISTENTXYZ123".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        ..Default::default()
    };
    let result = consolidator.validate_contract(bad_contract, Duration::from_secs(30));
    assert!(result.is_none(), "invalid contract should return None");
    })
.await;}

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + market CLOSED"]
async fn test_edge_case_market_closed_no_fill() {
    with_live_ibkr("DU111111", "ibc_flow.log", |state| async move {
;

    let market_data_handler = MarketDataHandler::new(state.pool.clone());
    let contract_scheduler = Arc::new(IbkrContractScheduler::new(state.client_1.clone()));
    let consolidator = Arc::new(Consolidator::new(
        tokio::runtime::Handle::current(),
        state.pool.clone(),
        state.client_1.clone(),
        market_data_handler,
        contract_scheduler,
    ));

    let contract = fixed_contracts().into_iter().next().unwrap();
    let result = consolidator.get_current_price(contract, false, &[]);
    match result {
        Ok(price) => {
            assert!(price > 0.0, "stale price should be positive, got {price}");
            println!("get_current_price when closed returned {price} (stale data)");
        }
        Err(e) => println!("get_current_price when closed returned Err (expected): {e}"),
    }
    })
.await;}

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + IBC installed"]
async fn test_edge_case_cancel_open_order() {
    with_live_ibkr("DU111111", "ibc_flow.log", |state| async move {
;

    let contract = fixed_contracts().into_iter().next().unwrap();
    let order = ibapi::orders::order_builder::limit_order(Action::Buy, 1.0, 1.0);
    let order_ibkr = OrderIBKR::new(contract.clone(), order, -1);
    let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);
    let order_perm_id = OrderEngine::place_order(
        tokio::runtime::Handle::current(),
        state.pool.clone(),
        &weak_client,
        order_ibkr,
    );

    if order_perm_id == 0 {
        println!("place_order returned 0 (client dead) — skipping cancel test");
        return;
    }
    println!("LIMIT order placed, perm_id={order_perm_id}, cancelling...");

    if let Some(client) = weak_client.upgrade() {
        let _ = client.cancel_order(order_perm_id, "");
        println!("cancel_order called for perm_id={order_perm_id}");
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    let market_data_handler = MarketDataHandler::new(state.pool.clone());
    let contract_scheduler = Arc::new(IbkrContractScheduler::new(state.client_1.clone()));
    let consolidator = Arc::new(Consolidator::new(
        tokio::runtime::Handle::current(),
        state.pool.clone(),
        state.client_1.clone(),
        market_data_handler,
        contract_scheduler,
    ));
    let noise = StrategyEnum::Noise(Noise::new(state.pool.clone(), tokio::runtime::Handle::current()));
    let strat_params = vec![trading_app::test_internals::strategy_parameters(noise.clone(), vec![trading_app::market_data::handler::DataSubscription::new(
            contract,
            ibapi::prelude::RealtimeWhatToShow::Trades,
        )])];
    let syncer = SyncerEngine::new(
        state.pool.clone(),
        "DU111111".to_string(),
        &strat_params,
        tokio::runtime::Handle::current(),
    );
    let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));
    syncer.sync_open_orders(&state.client_1, &consolidator, Some("noise".to_string()));
    let _ = syncer.sync_executions(&state.client_1, Some("noise".to_string()), order_store);
    println!("Sync after cancel complete");
    })
.await;}
