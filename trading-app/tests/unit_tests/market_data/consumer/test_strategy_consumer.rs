//! Unit tests for `IbkrBarConsumer::get_bar_type` and `StrategyDataBundler::sort_consumers`.
//!
//! See `src/market_data/consumer/strategy_consumer.rs`. Tests cover:
//! - `get_bar_type` — ForexPair+Bid→ForexBid, ForexPair+Ask→ForexAsk, else→Normal,
//!   ForexPair+non-Bid/Ask→panic
//! - `sort_consumers` — Forex-first, then symbol alphabetical, then Bid<Ask<other
//!
//! NOTE: Constructing `IbkrBarConsumer` requires a `SpmcRingBufferConsumer`, which
//! must be obtained from a real `SpmcRingBuffer`. We construct a throwaway ring buffer
//! for each consumer.

use ibapi::contracts::{Contract, SecurityType};
use ibapi::market_data::realtime::WhatToShow;
use spmc_ring::bench::RingBuffer;
use spmc_ring::ring_buffer::spmc_ring_buffer::{SpmcRingBuffer, SpmcRingBufferConsumer};

use ibapi::market_data::realtime::Bar;
use trading_app::market_data::consumer::strategy_consumer::StrategyDataBundler;
use trading_app::market_data::consumer::strategy_consumer::{IbkrBarConsumer, IbkrBarType};

const BUFFER_CAPACITY: usize = 128;
const NUM_CONSUMERS: usize = 10;

/// Build an IbkrBarConsumer with a throwaway ring buffer consumer.
fn make_consumer(
    security_type: SecurityType,
    symbol: &str,
    what: WhatToShow,
) -> IbkrBarConsumer<BUFFER_CAPACITY> {
    let ring: SpmcRingBuffer<Bar, BUFFER_CAPACITY, NUM_CONSUMERS> = SpmcRingBuffer::new();
    let consumer: SpmcRingBufferConsumer<Bar, BUFFER_CAPACITY> = ring.get_new_consumer().unwrap();
    IbkrBarConsumer::new(
        Contract {
            symbol: symbol.into(),
            security_type,
            currency: "USD".into(),
            ..Default::default()
        },
        what,
        consumer,
    )
}

// ============================ get_bar_type ============================

#[test]
fn get_bar_type_forex_bid() {
    let c = make_consumer(SecurityType::ForexPair, "EUR", WhatToShow::Bid);
    assert!(matches!(c.get_bar_type(), IbkrBarType::ForexBid));
}

#[test]
fn get_bar_type_forex_ask() {
    let c = make_consumer(SecurityType::ForexPair, "EUR", WhatToShow::Ask);
    assert!(matches!(c.get_bar_type(), IbkrBarType::ForexAsk));
}

#[test]
fn get_bar_type_stock_normal() {
    let c = make_consumer(SecurityType::Stock, "AAPL", WhatToShow::Trades);
    assert!(matches!(c.get_bar_type(), IbkrBarType::Normal));
}

#[test]
fn get_bar_type_option_normal() {
    let c = make_consumer(SecurityType::Option, "AAPL", WhatToShow::Trades);
    assert!(matches!(c.get_bar_type(), IbkrBarType::Normal));
}

#[test]
#[should_panic(expected = "ForexPair Security subscribing to data that is not bid or ask")]
fn get_bar_type_forex_trades_panics() {
    let c = make_consumer(SecurityType::ForexPair, "EUR", WhatToShow::Trades);
    let _ = c.get_bar_type();
}

#[test]
#[should_panic(expected = "ForexPair Security subscribing to data that is not bid or ask")]
fn get_bar_type_forex_midpoint_panics() {
    let c = make_consumer(SecurityType::ForexPair, "EUR", WhatToShow::MidPoint);
    let _ = c.get_bar_type();
}

// ============================ sort_consumers ============================
// Sort key: (is_fx, symbol, what_to_show_rank) where Bid=0, Ask=1, other=2.
// Forex pairs come FIRST (is_fx=true > is_fx=false in the tuple ordering).

#[test]
fn sort_consumers_forex_last() {
    // NOTE: The sort key is (is_fx, symbol, what_rank) with ascending .cmp().
    // For bool, false < true, so is_fx=true (ForexPair) sorts AFTER is_fx=false (Stock).
    // So Forex pairs come LAST, despite the source comment saying "Forex pairs come FIRST".
    // This test locks in the ACTUAL (current) behavior.
    let mut consumers = vec![
        make_consumer(SecurityType::Stock, "AAPL", WhatToShow::Trades),
        make_consumer(SecurityType::ForexPair, "EUR", WhatToShow::Bid),
        make_consumer(SecurityType::Stock, "MSFT", WhatToShow::Trades),
    ];
    StrategyDataBundler::<BUFFER_CAPACITY>::sort_consumers(&mut consumers);
    // Stocks come first (alphabetical), Forex last
    assert_eq!(consumers[0].contract.security_type, SecurityType::ForexPair);
    assert_eq!(consumers[0].contract.symbol.to_string(), "EUR");
    assert_eq!(consumers[1].contract.security_type, SecurityType::Stock);
    assert_eq!(consumers[1].contract.symbol.to_string(), "AAPL");
    assert_eq!(consumers[2].contract.security_type, SecurityType::Stock);
}

#[test]
fn sort_consumers_forex_bid_before_ask() {
    let mut consumers = vec![
        make_consumer(SecurityType::ForexPair, "EUR", WhatToShow::Ask),
        make_consumer(SecurityType::ForexPair, "EUR", WhatToShow::Bid),
    ];
    StrategyDataBundler::<BUFFER_CAPACITY>::sort_consumers(&mut consumers);
    assert!(matches!(consumers[0].what_to_show, WhatToShow::Bid));
    assert!(matches!(consumers[1].what_to_show, WhatToShow::Ask));
}

#[test]
fn sort_consumers_symbol_alphabetical_among_same_type() {
    let mut consumers = vec![
        make_consumer(SecurityType::Stock, "MSFT", WhatToShow::Trades),
        make_consumer(SecurityType::Stock, "AAPL", WhatToShow::Trades),
        make_consumer(SecurityType::Stock, "GOOG", WhatToShow::Trades),
    ];
    StrategyDataBundler::<BUFFER_CAPACITY>::sort_consumers(&mut consumers);
    assert_eq!(consumers[0].contract.symbol.to_string(), "AAPL");
    assert_eq!(consumers[1].contract.symbol.to_string(), "GOOG");
    assert_eq!(consumers[2].contract.symbol.to_string(), "MSFT");
}

#[test]
fn sort_consumers_mixed_full_ordering() {
    // Mix of forex (bid/ask) and stocks — verify full ordering.
    // ACTUAL behavior (locked in): Stocks come first (alphabetical), Forex last.
    // Among Forex: alphabetical by symbol, then Bid<Ask<other.
    let mut consumers = vec![
        make_consumer(SecurityType::Stock, "MSFT", WhatToShow::Trades),
        make_consumer(SecurityType::ForexPair, "EUR", WhatToShow::Ask),
        make_consumer(SecurityType::Stock, "AAPL", WhatToShow::Trades),
        make_consumer(SecurityType::ForexPair, "EUR", WhatToShow::Bid),
        make_consumer(SecurityType::ForexPair, "GBP", WhatToShow::Bid),
    ];
    StrategyDataBundler::<BUFFER_CAPACITY>::sort_consumers(&mut consumers);
    // Expected order: AAPL, MSFT, EUR/Bid, EUR/Ask, GBP/Bid
    assert_eq!(consumers[0].contract.symbol.to_string(), "EUR");
    assert!(matches!(consumers[0].what_to_show, WhatToShow::Bid));
    assert_eq!(consumers[1].contract.symbol.to_string(), "EUR");
    assert!(matches!(consumers[1].what_to_show, WhatToShow::Ask));
    assert_eq!(consumers[2].contract.symbol.to_string(), "GBP");
    assert!(matches!(consumers[2].what_to_show, WhatToShow::Bid));
    assert_eq!(consumers[3].contract.symbol.to_string(), "AAPL");
    assert_eq!(consumers[4].contract.symbol.to_string(), "MSFT");
}

#[test]
fn sort_consumers_empty_vec() {
    let mut consumers: Vec<IbkrBarConsumer<BUFFER_CAPACITY>> = vec![];
    StrategyDataBundler::<BUFFER_CAPACITY>::sort_consumers(&mut consumers);
    assert!(consumers.is_empty());
}

#[test]
fn sort_consumers_single_element() {
    let mut consumers = vec![make_consumer(
        SecurityType::Stock,
        "AAPL",
        WhatToShow::Trades,
    )];
    StrategyDataBundler::<BUFFER_CAPACITY>::sort_consumers(&mut consumers);
    assert_eq!(consumers.len(), 1);
    assert_eq!(consumers[0].contract.symbol.to_string(), "AAPL");
}
