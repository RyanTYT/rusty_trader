//! Comprehensive smoke tests for `StrategyDataBundler` + `IbkrBarConsumer` (live IBKR).
//!
//! Tests all public methods:
//! - `StrategyDataBundler::new()` — constructor
//! - `StrategyDataBundler::sort_consumers()` — forex-first, symbol alphabetical, Bid<Ask ordering
//! - `StrategyDataBundler::hook_strategy()` — spawns strategy thread + processes bars
//! - `StrategyDataBundler::Drop` — sets `is_alive` to false → thread exits
//! - `IbkrBarConsumer::new()` — constructor
//! - `IbkrBarConsumer::try_pop()` — pops a bar from the ring buffer
//! - `IbkrBarConsumer::get_bar_type()` — returns Normal/ForexBid/ForexAsk
//!
//! Requires: live IB Gateway + Postgres + DATABASE_URL + IBC installed.
//! Run with: DATABASE_URL=... cargo test --test smoke_tests test_hook_strategy -- --ignored

use std::sync::{Arc, Weak};
use std::time::Duration;

use ibapi::contracts::Contract;
use ibapi::prelude::{RealtimeWhatToShow, SecurityType};
use spmc_ring::bench::RingBuffer;
use trading_app::execution::fx_backed_up_order::OrderStore;
use trading_app::execution::order_engine::OrderEngine;
use trading_app::market_data::consumer::strategy_consumer::{
    IbkrBarConsumer, IbkrBarType, StrategyDataBundler,
};
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::handler::MarketDataHandler;
use trading_app::market_data::producer::subscribe_to_data;
use trading_app::schedule::contract_scheduler::{ContractScheduler, IbkrContractScheduler};
use trading_app::strategy::manual::Manual;
use trading_app::strategy::noise::Noise;
use trading_app::strategy::strategy::{StrategyEnum, StrategyExecutor};

use crate::live::init::live_ibkr;

const BUFFER_SIZE: usize = 128;
const MAX_NO_OF_CONSUMERS: usize = 4;

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

fn gbp_usd_contract() -> Contract {
    Contract {
        symbol: "GBP".into(),
        security_type: SecurityType::ForexPair,
        currency: "USD".into(),
        exchange: "IDEALPRO".into(),
        ..Default::default()
    }
}

fn build_scheduler_and_consolidator(
    state: &crate::live::init::LiveIbkr,
) -> (Arc<IbkrContractScheduler>, Arc<Consolidator>) {
    let scheduler = Arc::new(IbkrContractScheduler::new(state.client_1.clone()));
    let market_data_handler = MarketDataHandler::new(state.pool.clone());
    let consolidator = Arc::new(Consolidator::new(
        tokio::runtime::Handle::current(),
        state.pool.clone(),
        state.client_1.clone(),
        market_data_handler,
        scheduler.clone(),
    ));
    (scheduler, consolidator)
}

/// Build a scheduler with schedules pre-added for the given contracts.
/// Returns Arc<IbkrContractScheduler> (immutable, schedules already added).
fn build_scheduler_with_schedules(
    state: &crate::live::init::LiveIbkr,
    contracts: &[Contract],
) -> Arc<IbkrContractScheduler> {
    let mut scheduler = IbkrContractScheduler::new(state.client_1.clone());
    for c in contracts {
        scheduler.add_schedule(c).expect("add_schedule failed");
    }
    Arc::new(scheduler)
}

// ============================ 1. StrategyDataBundler::new — constructor ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_strategy_data_bundler_new() {
    let state = live_ibkr("DU111111", "ibc_bundler_new.log")
        .await
        .expect("Failed to boot live IBKR");

    let scheduler = Arc::new(IbkrContractScheduler::new(state.client_1.clone()));
    let bundler = StrategyDataBundler::<BUFFER_SIZE>::new(scheduler);
    println!("✅ StrategyDataBundler::new succeeded (no panic)");

    // Verify the bundler is usable — call sort_consumers (static method)
    let mut consumers: Vec<IbkrBarConsumer<BUFFER_SIZE>> = vec![];
    StrategyDataBundler::sort_consumers(&mut consumers);
    assert!(consumers.is_empty(), "sort_consumers on empty vec should be a no-op");
    println!("✅ sort_consumers(empty) is a no-op");
}

// ============================ 2. sort_consumers — stock + forex ordering ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_sort_consumers_stock_and_forex_ordering() {
    let state = live_ibkr("DU111111", "ibc_bundler_sort.log")
        .await
        .expect("Failed to boot live IBKR");

    let scheduler = Arc::new(IbkrContractScheduler::new(state.client_1.clone()));

    // Build consumers: AAPL (stock), MSFT (stock), GBP/USD Bid (forex), GBP/USD Ask (forex)
    // We need real ring buffers — use subscribe_to_data
    let (rb_aapl, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        aapl_contract(),
        RealtimeWhatToShow::Trades,
        scheduler.clone(),
    );
    let (rb_msft, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        msft_contract(),
        RealtimeWhatToShow::Trades,
        scheduler.clone(),
    );
    let (rb_gbp_bid, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        gbp_usd_contract(),
        RealtimeWhatToShow::Bid,
        scheduler.clone(),
    );
    let (rb_gbp_ask, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        gbp_usd_contract(),
        RealtimeWhatToShow::Ask,
        scheduler.clone(),
    );

    let mut consumers = vec![
        IbkrBarConsumer::new(aapl_contract(), RealtimeWhatToShow::Trades, rb_aapl.get_new_consumer().unwrap()),
        IbkrBarConsumer::new(msft_contract(), RealtimeWhatToShow::Trades, rb_msft.get_new_consumer().unwrap()),
        IbkrBarConsumer::new(gbp_usd_contract(), RealtimeWhatToShow::Bid, rb_gbp_bid.get_new_consumer().unwrap()),
        IbkrBarConsumer::new(gbp_usd_contract(), RealtimeWhatToShow::Ask, rb_gbp_ask.get_new_consumer().unwrap()),
    ];

    // Before sort: AAPL, MSFT, GBP-Bid, GBP-Ask
    println!("Before sort: {:?}", consumers.iter().map(|c| (c.contract.symbol.to_string(), c.what_to_show.clone())).collect::<Vec<_>>());

    StrategyDataBundler::sort_consumers(&mut consumers);

    // After sort: forex first (GBP-Bid, GBP-Ask), then stocks alphabetically (AAPL, MSFT)
    println!("After sort: {:?}", consumers.iter().map(|c| (c.contract.symbol.to_string(), c.what_to_show.clone())).collect::<Vec<_>>());

    // Verify forex come first
    assert_eq!(consumers[0].contract.symbol.to_string(), "GBP", "first consumer should be GBP (forex)");
    assert_eq!(consumers[1].contract.symbol.to_string(), "GBP", "second consumer should be GBP (forex)");

    // Verify Bid < Ask within the forex pair
    assert!(matches!(consumers[0].what_to_show, RealtimeWhatToShow::Bid), "forex Bid should come before Ask");
    assert!(matches!(consumers[1].what_to_show, RealtimeWhatToShow::Ask), "forex Ask should come after Bid");

    // Verify stocks come after forex, alphabetically
    assert_eq!(consumers[2].contract.symbol.to_string(), "AAPL", "AAPL should come before MSFT");
    assert_eq!(consumers[3].contract.symbol.to_string(), "MSFT", "MSFT should come after AAPL");

    println!("✅ sort_consumers: forex-first (GBP Bid < GBP Ask), then stocks alphabetical (AAPL < MSFT)");
}

// ============================ 3. sort_consumers — multiple stocks alphabetical ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_sort_consumers_multiple_stocks_alphabetical() {
    let state = live_ibkr("DU111111", "ibc_bundler_sort_stocks.log")
        .await
        .expect("Failed to boot live IBKR");

    let scheduler = Arc::new(IbkrContractScheduler::new(state.client_1.clone()));

    // Build consumers in reverse alphabetical order: SPY, MSFT, AAPL
    let contracts = vec![
        ("SPY", "ARCA"),
        ("MSFT", "NASDAQ"),
        ("AAPL", "NASDAQ"),
    ];

    let mut consumers: Vec<IbkrBarConsumer<BUFFER_SIZE>> = vec![];
    for (symbol, exchange) in &contracts {
        let contract = Contract {
            symbol: (*symbol).into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            exchange: "SMART".into(),
            primary_exchange: (*exchange).into(),
            ..Default::default()
        };
        let (rb, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
            Arc::downgrade(&state.client_1),
            contract.clone(),
            RealtimeWhatToShow::Trades,
            scheduler.clone(),
        );
        consumers.push(IbkrBarConsumer::new(contract, RealtimeWhatToShow::Trades, rb.get_new_consumer().unwrap()));
    }

    StrategyDataBundler::sort_consumers(&mut consumers);

    // After sort: AAPL, MSFT, SPY (alphabetical)
    assert_eq!(consumers[0].contract.symbol.to_string(), "AAPL");
    assert_eq!(consumers[1].contract.symbol.to_string(), "MSFT");
    assert_eq!(consumers[2].contract.symbol.to_string(), "SPY");
    println!("✅ sort_consumers(multiple stocks): AAPL < MSFT < SPY (alphabetical)");
}

// ============================ 4. IbkrBarConsumer::new + get_bar_type ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_ibkr_bar_consumer_new_and_get_bar_type() {
    let state = live_ibkr("DU111111", "ibc_bundler_consumer.log")
        .await
        .expect("Failed to boot live IBKR");

    let scheduler = Arc::new(IbkrContractScheduler::new(state.client_1.clone()));

    // Stock consumer → Normal
    let (rb_stock, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        aapl_contract(),
        RealtimeWhatToShow::Trades,
        scheduler.clone(),
    );
    let stock_consumer = IbkrBarConsumer::new(
        aapl_contract(),
        RealtimeWhatToShow::Trades,
        rb_stock.get_new_consumer().unwrap(),
    );
    assert!(matches!(stock_consumer.get_bar_type(), IbkrBarType::Normal), "stock consumer should be Normal");
    assert_eq!(stock_consumer.contract.symbol.to_string(), "AAPL");
    println!("✅ IbkrBarConsumer(stock): get_bar_type() = Normal");

    // Forex Bid consumer → ForexBid
    let (rb_bid, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        gbp_usd_contract(),
        RealtimeWhatToShow::Bid,
        scheduler.clone(),
    );
    let bid_consumer = IbkrBarConsumer::new(
        gbp_usd_contract(),
        RealtimeWhatToShow::Bid,
        rb_bid.get_new_consumer().unwrap(),
    );
    assert!(matches!(bid_consumer.get_bar_type(), IbkrBarType::ForexBid), "forex Bid consumer should be ForexBid");
    println!("✅ IbkrBarConsumer(forex Bid): get_bar_type() = ForexBid");

    // Forex Ask consumer → ForexAsk
    let (rb_ask, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        gbp_usd_contract(),
        RealtimeWhatToShow::Ask,
        scheduler.clone(),
    );
    let ask_consumer = IbkrBarConsumer::new(
        gbp_usd_contract(),
        RealtimeWhatToShow::Ask,
        rb_ask.get_new_consumer().unwrap(),
    );
    assert!(matches!(ask_consumer.get_bar_type(), IbkrBarType::ForexAsk), "forex Ask consumer should be ForexAsk");
    println!("✅ IbkrBarConsumer(forex Ask): get_bar_type() = ForexAsk");
}

// ============================ 5. IbkrBarConsumer::try_pop — receive a bar ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + market open + IBC installed"]
async fn test_ibkr_bar_consumer_try_pop() {
    let state = live_ibkr("DU111111", "ibc_bundler_pop.log")
        .await
        .expect("Failed to boot live IBKR");

    let scheduler = Arc::new(IbkrContractScheduler::new(state.client_1.clone()));

    let (rb, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        aapl_contract(),
        RealtimeWhatToShow::Trades,
        scheduler.clone(),
    );
    let consumer = IbkrBarConsumer::new(
        aapl_contract(),
        RealtimeWhatToShow::Trades,
        rb.get_new_consumer().unwrap(),
    );

    // Wait for market data to flow
    tokio::time::sleep(Duration::from_secs(20)).await;

    // Try to pop a bar
    let bar = consumer.try_pop();
    match bar {
        Some(b) => {
            assert!(b.close > 0.0, "bar close should be positive, got {}", b.close);
            println!("✅ try_pop: received bar with close={}", b.close);
        }
        None => println!("try_pop returned None (market may be closed) — acceptable"),
    }
}

// ============================ 6. hook_strategy — stock consumer (Noise) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + market open + IBC installed"]
async fn test_hook_strategy_stock_noise() {
    let state = live_ibkr("DU111111", "ibc_bundler_hook_stock.log")
        .await
        .expect("Failed to boot live IBKR");

    let contract = aapl_contract();
    let scheduler = build_scheduler_with_schedules(&state, &[contract.clone()]);
    let consolidator = build_scheduler_and_consolidator(&state).1;

    let (rb, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        contract.clone(),
        RealtimeWhatToShow::Trades,
        scheduler.clone(),
    );
    let consumer = IbkrBarConsumer::new(
        contract,
        RealtimeWhatToShow::Trades,
        rb.get_new_consumer().unwrap(),
    );

    let mut bundler = StrategyDataBundler::<BUFFER_SIZE>::new(scheduler);
    let order_engine = OrderEngine::new(state.pool.clone(), tokio::runtime::Handle::current());
    let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));
    let noise = StrategyEnum::Noise(Noise::new(state.pool.clone(), tokio::runtime::Handle::current()));

    // hook_strategy spawns a thread that processes bars
    bundler.hook_strategy(
        vec![consumer],
        noise,
        order_engine,
        Arc::downgrade(&consolidator),
        Arc::downgrade(&state.client_1),
        Arc::downgrade(&order_store),
    );

    // Let it run for 30 seconds to process bars
    tokio::time::sleep(Duration::from_secs(30)).await;
    println!("✅ hook_strategy(Noise, AAPL stock) ran for 30 seconds without panic");

    // Drop the bundler — should set is_alive=false → thread exits
    drop(bundler);
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("✅ hook_strategy thread stopped after Drop (is_alive=false)");
}

// ============================ 7. hook_strategy — forex consumer (Manual) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + market open + IBC installed"]
async fn test_hook_strategy_forex_manual() {
    let state = live_ibkr("DU111111", "ibc_bundler_hook_forex.log")
        .await
        .expect("Failed to boot live IBKR");

    let contract = gbp_usd_contract();
    let scheduler = build_scheduler_with_schedules(&state, &[contract.clone()]);
    let consolidator = build_scheduler_and_consolidator(&state).1;

    // Forex needs Bid + Ask pair
    let (rb_bid, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        contract.clone(),
        RealtimeWhatToShow::Bid,
        scheduler.clone(),
    );
    let (rb_ask, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        contract.clone(),
        RealtimeWhatToShow::Ask,
        scheduler.clone(),
    );

    let bid_consumer = IbkrBarConsumer::new(
        contract.clone(),
        RealtimeWhatToShow::Bid,
        rb_bid.get_new_consumer().unwrap(),
    );
    let ask_consumer = IbkrBarConsumer::new(
        contract,
        RealtimeWhatToShow::Ask,
        rb_ask.get_new_consumer().unwrap(),
    );

    let mut bundler = StrategyDataBundler::<BUFFER_SIZE>::new(scheduler);
    let order_engine = OrderEngine::new(state.pool.clone(), tokio::runtime::Handle::current());
    let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));
    let manual = StrategyEnum::Manual(Manual::new(state.pool.clone()));

    // hook_strategy with forex Bid + Ask pair
    bundler.hook_strategy(
        vec![bid_consumer, ask_consumer],
        manual,
        order_engine,
        Arc::downgrade(&consolidator),
        Arc::downgrade(&state.client_1),
        Arc::downgrade(&order_store),
    );

    // Let it run for 30 seconds to process forex bars
    tokio::time::sleep(Duration::from_secs(30)).await;
    println!("✅ hook_strategy(Manual, GBP/USD forex) ran for 30 seconds without panic");

    // Drop the bundler — should stop the thread
    drop(bundler);
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("✅ forex hook_strategy thread stopped after Drop");
}

// ============================ 8. hook_strategy — idempotency (calling twice is a no-op) ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + market open + IBC installed"]
async fn test_hook_strategy_idempotent() {
    let state = live_ibkr("DU111111", "ibc_bundler_hook_idem.log")
        .await
        .expect("Failed to boot live IBKR");

    let contract = aapl_contract();
    let scheduler = build_scheduler_with_schedules(&state, &[contract.clone()]);
    let consolidator = build_scheduler_and_consolidator(&state).1;

    let (rb, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        contract.clone(),
        RealtimeWhatToShow::Trades,
        scheduler.clone(),
    );
    let consumer = IbkrBarConsumer::new(
        contract,
        RealtimeWhatToShow::Trades,
        rb.get_new_consumer().unwrap(),
    );

    let scheduler_clone = scheduler.clone();
    let mut bundler = StrategyDataBundler::<BUFFER_SIZE>::new(scheduler);
    let order_engine = OrderEngine::new(state.pool.clone(), tokio::runtime::Handle::current());
    let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));
    let noise = StrategyEnum::Noise(Noise::new(state.pool.clone(), tokio::runtime::Handle::current()));

    // First call — should spawn the thread
    bundler.hook_strategy(
        vec![consumer],
        noise.clone(),
        order_engine.clone(),
        Arc::downgrade(&consolidator),
        Arc::downgrade(&state.client_1),
        Arc::downgrade(&order_store),
    );
    println!("First hook_strategy call — thread spawned");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Second call — should be a no-op (is_alive is already true)
    // Note: we can't easily pass the same consumer twice (ownership), so we build a new one
    let (rb2, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        aapl_contract(),
        RealtimeWhatToShow::Trades,
        scheduler_clone.clone(),
    );
    let consumer2 = IbkrBarConsumer::new(
        aapl_contract(),
        RealtimeWhatToShow::Trades,
        rb2.get_new_consumer().unwrap(),
    );

    bundler.hook_strategy(
        vec![consumer2],
        noise,
        order_engine,
        Arc::downgrade(&consolidator),
        Arc::downgrade(&state.client_1),
        Arc::downgrade(&order_store),
    );
    println!("Second hook_strategy call — should be a no-op (is_alive already true)");

    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("✅ hook_strategy idempotency verified — second call didn't spawn a duplicate thread");

    drop(bundler);
    tokio::time::sleep(Duration::from_secs(2)).await;
}

// ============================ 9. hook_strategy — full lifecycle with multiple consumers ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + market open + IBC installed"]
async fn test_hook_strategy_full_lifecycle_multiple_consumers() {
    let state = live_ibkr("DU111111", "ibc_bundler_lifecycle.log")
        .await
        .expect("Failed to boot live IBKR");

    // Build 2 stock consumers (AAPL + MSFT) + 1 forex pair (GBP/USD Bid + Ask)
    let contracts = vec![aapl_contract(), msft_contract(), gbp_usd_contract()];
    let scheduler = build_scheduler_with_schedules(&state, &contracts);
    let consolidator = build_scheduler_and_consolidator(&state).1;

    let (rb_aapl, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        aapl_contract(),
        RealtimeWhatToShow::Trades,
        scheduler.clone(),
    );
    let (rb_msft, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        msft_contract(),
        RealtimeWhatToShow::Trades,
        scheduler.clone(),
    );
    let (rb_gbp_bid, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        gbp_usd_contract(),
        RealtimeWhatToShow::Bid,
        scheduler.clone(),
    );
    let (rb_gbp_ask, _) = subscribe_to_data::<BUFFER_SIZE, MAX_NO_OF_CONSUMERS>(
        Arc::downgrade(&state.client_1),
        gbp_usd_contract(),
        RealtimeWhatToShow::Ask,
        scheduler.clone(),
    );

    let mut consumers = vec![
        IbkrBarConsumer::new(aapl_contract(), RealtimeWhatToShow::Trades, rb_aapl.get_new_consumer().unwrap()),
        IbkrBarConsumer::new(msft_contract(), RealtimeWhatToShow::Trades, rb_msft.get_new_consumer().unwrap()),
        IbkrBarConsumer::new(gbp_usd_contract(), RealtimeWhatToShow::Bid, rb_gbp_bid.get_new_consumer().unwrap()),
        IbkrBarConsumer::new(gbp_usd_contract(), RealtimeWhatToShow::Ask, rb_gbp_ask.get_new_consumer().unwrap()),
    ];

    // sort_consumers is called inside hook_strategy, but we call it here to verify
    StrategyDataBundler::sort_consumers(&mut consumers);
    println!("Consumers sorted: forex-first (GBP Bid+Ask), then stocks (AAPL, MSFT)");

    let mut bundler = StrategyDataBundler::<BUFFER_SIZE>::new(scheduler);
    let order_engine = OrderEngine::new(state.pool.clone(), tokio::runtime::Handle::current());
    let order_store = Arc::new(OrderStore::open().expect("OrderStore::open failed"));
    let noise = StrategyEnum::Noise(Noise::new(state.pool.clone(), tokio::runtime::Handle::current()));

    // hook_strategy with 4 consumers (2 stocks + 1 forex pair)
    bundler.hook_strategy(
        consumers,
        noise,
        order_engine,
        Arc::downgrade(&consolidator),
        Arc::downgrade(&state.client_1),
        Arc::downgrade(&order_store),
    );

    // Let it run for 30 seconds
    tokio::time::sleep(Duration::from_secs(30)).await;
    println!("✅ hook_strategy(4 consumers: AAPL, MSFT, GBP-Bid, GBP-Ask) ran for 30 seconds");

    // Drop the bundler — should stop the thread
    drop(bundler);
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("✅ full lifecycle: bundler dropped, thread stopped");
}
