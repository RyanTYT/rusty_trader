//! Smoke test: hook_strategy (live IBKR).
//! `StrategyDataBundler::hook_strategy` — full stack integration.
//! Requires: live IB Gateway + market open. Run with: `cargo test --test smoke_tests test_hook_strategy -- --ignored`

use std::sync::{Arc, Weak};
use std::time::Duration;

use ibapi::contracts::Contract;
use ibapi::prelude::{RealtimeWhatToShow, SecurityType};
use trading_app::execution::fx_backed_up_order::OrderStore;
use trading_app::execution::order_engine::OrderEngine;
use trading_app::market_data::consumer::strategy_consumer::{IbkrBarConsumer, StrategyDataBundler};
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::MarketDataHandler;
use trading_app::market_data::producer::subscribe_to_data;
use trading_app::schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler};
use trading_app::strategy::noise::Noise;
use trading_app::strategy::strategy::{StrategyEnum, StrategyExecutor};

use crate::live::init::live_ibkr;

const BUFFER_SIZE: usize = 128;
const MAX_NO_OF_CONSUMERS: usize = 4;

#[tokio::test]
#[ignore = "requires live IB Gateway + market open + IBC installed"]
async fn test_hook_strategy_live() {
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

    let mut scheduler = IbkrContractScheduler::new(state.client_1.clone());
    scheduler.add_schedule(&contract).expect("add_schedule failed");
    let scheduler = Arc::new(scheduler);

    let (ring_buffer, _producer) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        contract.clone(),
        RealtimeWhatToShow::Trades,
        scheduler.clone(),
    );

    // IbkrBarConsumer::new(contract, what_to_show, consumer)
    let consumer = ring_buffer
        .get_new_consumer()
        .expect("expected consumer");
    let bar_consumer = IbkrBarConsumer::new(contract, RealtimeWhatToShow::Trades, consumer);

    // MarketDataHandler::new(pool)
    let market_data_handler = MarketDataHandler::new(state.pool.clone());
    let consolidator = Arc::new(Consolidator::new(
        tokio::runtime::Handle::current(),
        state.pool.clone(),
        state.client_1.clone(),
        market_data_handler,
        scheduler.clone(),
    ));
    let weak_consolidator: Weak<Consolidator> = Arc::downgrade(&consolidator);

    let mut bundler = StrategyDataBundler::<BUFFER_SIZE>::new(scheduler);
    let order_engine = OrderEngine::new(state.pool.clone(), tokio::runtime::Handle::current());
    let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));
    let weak_order_store: Weak<OrderStore> = Arc::downgrade(&order_store);
    let weak_client: Weak<ibapi::Client> = Arc::downgrade(&state.client_1);

    let noise = StrategyEnum::Noise(Noise::new(state.pool.clone(), tokio::runtime::Handle::current()));

    // hook_strategy(consumers, strategy, order_engine, consolidator, client, order_store)
    bundler.hook_strategy(
        vec![bar_consumer],
        noise,
        order_engine,
        weak_consolidator,
        weak_client,
        weak_order_store,
    );

    tokio::time::sleep(Duration::from_secs(30)).await;
    println!("hook_strategy ran for 30 seconds without panic — smoke test passed");
}
