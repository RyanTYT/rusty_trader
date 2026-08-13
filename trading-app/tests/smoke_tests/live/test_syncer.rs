//! Smoke test: syncer (live IBKR).
//! `SyncerEngine::sync_open_orders/sync_executions/sync_positions`.
//! Requires: live IB Gateway + Postgres. Run with: `cargo test --test smoke_tests test_syncer -- --ignored`

use std::sync::Arc;

use ibapi::contracts::Contract;
use ibapi::prelude::{RealtimeWhatToShow, SecurityType};
use trading_app::execution::fx_backed_up_order::OrderStore;
use trading_app::execution::syncer::{SyncOps, SyncerEngine};
use trading_app::init_app::StrategyParameters;
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::{DataSubscription, MarketDataHandler};
use trading_app::schedule::contract_scheduler::IbkrContractScheduler;
use trading_app::strategy::noise::Noise;
use trading_app::strategy::strategy::{StrategyEnum, StrategyExecutor};

use crate::live::init::live_ibkr;

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + IBC installed"]
async fn test_syncer_live() {
    let state = live_ibkr("DU111111", "ibc_live.log")
        .await
        .expect("Failed to boot live IBKR");

    let contract = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        exchange: "SMART".into(),
        primary_exchange: "NASDAQ".into(),
        ..Default::default()
    };

    let contract_scheduler = Arc::new(IbkrContractScheduler::new(state.client_1.clone()));
    let market_data_handler = MarketDataHandler::new(state.pool.clone());
    let consolidator = Arc::new(Consolidator::new(
        tokio::runtime::Handle::current(),
        state.pool.clone(),
        state.client_1.clone(),
        market_data_handler,
        contract_scheduler,
    ));
    let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));

    let noise = StrategyEnum::Noise(Noise::new(state.pool.clone(), tokio::runtime::Handle::current()));
    let strat_params = vec![StrategyParameters {
        strategy: noise.clone(),
        subscribed_contracts: vec![DataSubscription::new(contract.clone(), RealtimeWhatToShow::Trades)],
    }];

    let syncer = SyncerEngine::new(
        state.pool.clone(),
        "DU111111".to_string(),
        &strat_params,
        tokio::runtime::Handle::current(),
    );

    // sync_open_orders — sync, takes (client, consolidator, default_strategy)
    syncer.sync_open_orders(
        &state.client_1,
        &consolidator,
        Some("noise".to_string()),
    );
    println!("sync_open_orders completed");

    // sync_executions — sync, takes (client, default_strategy, backed_up_orders)
    let result = syncer.sync_executions(
        &state.client_1,
        Some("noise".to_string()),
        order_store.clone(),
    );
    match &result {
        Ok(_) => println!("sync_executions succeeded"),
        Err(e) => println!("sync_executions returned error: {e}"),
    }

    // sync_positions — async, returns ()
    syncer
        .sync_positions(&state.client_1, &consolidator, Some("noise".to_string()))
        .await;
    println!("sync_positions completed");
}
