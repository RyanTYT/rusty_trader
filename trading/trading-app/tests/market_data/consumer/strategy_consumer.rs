//! Tests for `src/market_data/consumer/strategy_consumer.rs`.
//!
//! Covered here (pure, deterministic — no IB, no DB, no network):
//!   - `IbkrBarConsumer::new` (src/market_data/consumer/strategy_consumer.rs:45)
//!   - `IbkrBarConsumer::try_pop` (:68) — thin delegation to the ring consumer
//!   - `IbkrBarConsumer::get_bar_type` (:71) — ForexPair+Bid/Ask → ForexBid/
//!     ForexAsk, every other security type → Normal, ForexPair+other panics
//!   - `StrategyDataBundler::sort_consumers` (:132) — key is
//!     `(is_fx, symbol, currency, primary_exchange, what_to_show_rank)` with
//!     `is_fx` inverted in the comparator so Forex pairs come first, rank
//!     Bid=0 < Ask=1 < other=2, and the sort is stable for full ties.
//!
//! NOT covered — need a live `ibapi::Client`, execution stream or spawned
//! thread:
//!   - `StrategyDataBundler::new` (:123) — takes `Arc<IbkrContractScheduler>`,
//!     whose only constructor is `IbkrContractScheduler::new(Arc<Client>)`
//!     (src/schedule/contract_scheduler.rs:60).
//!   - `StrategyDataBundler::async_drop` (:92) / `Drop` (:110) — no instance
//!     can be built without the above.
//!   - `StrategyDataBundler::hook_strategy` (:167) — spawns the strategy thread
//!     and needs `StrategyEnum`, `OrderEngine`, `Weak<Consolidator>`,
//!     `Weak<Client>`, `Weak<OrderStore>`, plus a
//!     `SpmcRingBufferConsumer<ExecutionData, 64, 1>`.
//!   - `StrategyDataBundler::dispatch_bar` (:406, private) — the FX bid/ask
//!     pairing buffer (`buffer_bars` insert/match at :429-476, including the
//!     `idx - 1` sibling lookup at :325) is therefore not reachable; only the
//!     300s `aggregate_bars` step it calls is covered in `consumer/helper.rs`.
//!   - the steady-state loop (:235-398) — spawned, depends on a live
//!     scheduler/strategy.

use std::sync::Arc;

use ibapi::contracts::{Contract, SecurityType};
use ibapi::market_data::realtime::{Bar, WhatToShow};
use spmc_ring::ring_buffer::spmc_ring_buffer::{
    SpmcRingBuffer, SpmcRingBufferConsumer, SpmcRingBufferProducer,
};
use time::OffsetDateTime;
use trading_app::market_data::consumer::strategy_consumer::{
    IbkrBarConsumer, IbkrBarType, StrategyDataBundler,
};

const CAP: usize = 8;
const NC: usize = 10;

type Ring = SpmcRingBuffer<Bar, CAP, NC>;

// ──────────────────────────── helpers ────────────────────────────

fn contract(symbol: &str, security_type: SecurityType, currency: &str, pe: &str) -> Contract {
    Contract {
        symbol: symbol.into(),
        security_type,
        currency: currency.into(),
        primary_exchange: pe.into(),
        exchange: "SMART".into(),
        ..Default::default()
    }
}

fn new_ring() -> Arc<Ring> {
    Arc::new(Ring::new())
}

fn make_consumer(
    symbol: &str,
    security_type: SecurityType,
    currency: &str,
    pe: &str,
    what: WhatToShow,
) -> IbkrBarConsumer<CAP, NC> {
    let ring = new_ring();
    IbkrBarConsumer::new(
        contract(symbol, security_type, currency, pe),
        what,
        ring.get_new_consumer().expect("fresh ring has a consumer slot"),
    )
}

/// Consumer + the producer for the same ring, so tests can push.
fn pair(
    symbol: &str,
    security_type: SecurityType,
    currency: &str,
    pe: &str,
    what: WhatToShow,
) -> (IbkrBarConsumer<CAP, NC>, SpmcRingBufferProducer<Bar, CAP, NC>) {
    let ring = new_ring();
    let consumer = IbkrBarConsumer::new(
        contract(symbol, security_type, currency, pe),
        what,
        ring.get_new_consumer().expect("fresh ring has a consumer slot"),
    );
    let producer = ring.get_new_producer().expect("fresh ring has a producer slot");
    (consumer, producer)
}

fn bar(secs: i64, close: f64) -> Bar {
    Bar {
        date: OffsetDateTime::from_unix_timestamp(secs).expect("valid unix timestamp"),
        open: close - 1.0,
        high: close + 1.0,
        low: close - 2.0,
        close,
        volume: 1.0,
        wap: close,
        count: 1,
    }
}

fn label(c: &IbkrBarConsumer<CAP, NC>) -> (String, String, String) {
    (
        c.contract.symbol.as_str().to_string(),
        c.contract.currency.as_str().to_string(),
        c.contract.primary_exchange.as_str().to_string(),
    )
}

fn rank(c: &IbkrBarConsumer<CAP, NC>) -> u8 {
    match c.what_to_show {
        WhatToShow::Bid => 0,
        WhatToShow::Ask => 1,
        _ => 2,
    }
}

// ──────────────────────────── get_bar_type ────────────────────────────

#[test]
fn get_bar_type_table() {
    let cases: &[(SecurityType, WhatToShow, IbkrBarType)] = &[
        (SecurityType::Stock, WhatToShow::Trades, IbkrBarType::Normal),
        (SecurityType::Stock, WhatToShow::Bid, IbkrBarType::Normal),
        (SecurityType::Stock, WhatToShow::Ask, IbkrBarType::Normal),
        (SecurityType::Stock, WhatToShow::MidPoint, IbkrBarType::Normal),
        (SecurityType::Option, WhatToShow::Trades, IbkrBarType::Normal),
        (SecurityType::Future, WhatToShow::Bid, IbkrBarType::Normal),
        (SecurityType::CFD, WhatToShow::Ask, IbkrBarType::Normal),
        (SecurityType::ForexPair, WhatToShow::Bid, IbkrBarType::ForexBid),
        (SecurityType::ForexPair, WhatToShow::Ask, IbkrBarType::ForexAsk),
    ];

    for (security_type, what, expected) in cases {
        let c = make_consumer("X", security_type.clone(), "USD", "SMART", *what);
        assert_eq!(
            std::mem::discriminant(&c.get_bar_type()),
            std::mem::discriminant(expected),
            "{security_type:?} + {what:?}"
        );
    }
}

#[test]
#[should_panic(expected = "ForexPair Security subscribing to data that is not bid or ask")]
fn get_bar_type_forex_trades_panics() {
    let c = make_consumer("EUR", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::Trades);
    let _ = c.get_bar_type();
}

#[test]
#[should_panic(expected = "ForexPair Security subscribing to data that is not bid or ask")]
fn get_bar_type_forex_midpoint_panics() {
    let c = make_consumer("EUR", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::MidPoint);
    let _ = c.get_bar_type();
}

// ──────────────────────────── try_pop ────────────────────────────

#[test]
fn try_pop_delegates_to_ring_in_order() {
    let (consumer, producer) = pair("AAPL", SecurityType::Stock, "USD", "NASDAQ", WhatToShow::Trades);
    assert_eq!(consumer.try_pop(), None);

    let sent = vec![bar(0, 10.0), bar(5, 11.0), bar(10, 12.0)];
    for b in &sent {
        producer.try_push(b.clone()).expect("ring has space");
    }

    let mut got = Vec::new();
    while let Some(b) = consumer.try_pop() {
        got.push(b);
    }
    assert_eq!(got, sent);
    assert_eq!(consumer.try_pop(), None);
}

#[test]
fn new_preserves_contract_and_what_to_show() {
    let c = make_consumer("EUR", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::Ask);
    assert_eq!(c.contract.symbol.as_str(), "EUR");
    assert_eq!(c.contract.currency.as_str(), "USD");
    assert_eq!(c.contract.primary_exchange.as_str(), "IDEALPRO");
    assert_eq!(c.contract.security_type, SecurityType::ForexPair);
    assert!(matches!(c.what_to_show, WhatToShow::Ask));
}

// ──────────────────────────── sort_consumers ────────────────────────────

#[test]
fn sort_puts_forex_pairs_before_equities() {
    // Forex "ZZZ" must still precede stock "AAA": the FX tier dominates.
    let mut v = vec![
        make_consumer("AAA", SecurityType::Stock, "USD", "NASDAQ", WhatToShow::Trades),
        make_consumer("ZZZ", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::Bid),
        make_consumer("BBB", SecurityType::Stock, "USD", "NASDAQ", WhatToShow::Trades),
    ];
    StrategyDataBundler::<CAP, NC>::sort_consumers(&mut v);

    let syms: Vec<String> = v.iter().map(|c| c.contract.symbol.as_str().to_string()).collect();
    assert_eq!(syms, vec!["ZZZ", "AAA", "BBB"]);
    assert_eq!(v[0].contract.security_type, SecurityType::ForexPair);
}

#[test]
fn sort_orders_forex_by_symbol_then_currency_then_exchange_then_side() {
    let mut v = vec![
        make_consumer("GBP", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::Bid),
        make_consumer("EUR", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::Ask),
        make_consumer("EUR", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::Bid),
        make_consumer("EUR", SecurityType::ForexPair, "GBP", "IDEALPRO", WhatToShow::Bid),
        make_consumer("EUR", SecurityType::ForexPair, "USD", "ARCA", WhatToShow::Bid),
    ];
    StrategyDataBundler::<CAP, NC>::sort_consumers(&mut v);

    let got: Vec<(String, String, String, u8)> = v
        .iter()
        .map(|c| {
            let (s, cur, pe) = label(c);
            (s, cur, pe, rank(c))
        })
        .collect();

    assert_eq!(
        got,
        vec![
            ("EUR".into(), "GBP".into(), "IDEALPRO".into(), 0), // currency GBP < USD
            ("EUR".into(), "USD".into(), "ARCA".into(), 0),     // exchange ARCA < IDEALPRO
            ("EUR".into(), "USD".into(), "IDEALPRO".into(), 0), // Bid rank 0
            ("EUR".into(), "USD".into(), "IDEALPRO".into(), 1), // Ask rank 1
            ("GBP".into(), "USD".into(), "IDEALPRO".into(), 0),
        ]
    );
}

#[test]
fn sort_orders_bid_before_ask_before_other() {
    let mut v = vec![
        make_consumer("EUR", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::Trades),
        make_consumer("EUR", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::Ask),
        make_consumer("EUR", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::Bid),
        make_consumer("EUR", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::MidPoint),
    ];
    StrategyDataBundler::<CAP, NC>::sort_consumers(&mut v);

    let ranks: Vec<u8> = v.iter().map(rank).collect();
    assert_eq!(ranks, vec![0, 1, 2, 2], "Bid < Ask < Trades/MidPoint (ties stable)");
    assert!(matches!(v[2].what_to_show, WhatToShow::Trades), "Trades came first in the input");
    assert!(matches!(v[3].what_to_show, WhatToShow::MidPoint));
}

#[test]
fn sort_what_to_show_rank_applies_to_equities_too() {
    // The rank tier is not FX-gated: Bid/Ask sort ahead of Trades for stocks.
    let mut v = vec![
        make_consumer("AAPL", SecurityType::Stock, "USD", "NASDAQ", WhatToShow::Trades),
        make_consumer("AAPL", SecurityType::Stock, "USD", "NASDAQ", WhatToShow::Ask),
        make_consumer("AAPL", SecurityType::Stock, "USD", "NASDAQ", WhatToShow::Bid),
    ];
    StrategyDataBundler::<CAP, NC>::sort_consumers(&mut v);

    let ranks: Vec<u8> = v.iter().map(rank).collect();
    assert_eq!(ranks, vec![0, 1, 2]);
}

#[test]
fn sort_orders_symbols_alphabetically_within_a_tier() {
    let mut v = vec![
        make_consumer("MSFT", SecurityType::Stock, "USD", "NASDAQ", WhatToShow::Trades),
        make_consumer("AAPL", SecurityType::Stock, "USD", "NASDAQ", WhatToShow::Trades),
        make_consumer("GOOG", SecurityType::Stock, "USD", "NASDAQ", WhatToShow::Trades),
    ];
    StrategyDataBundler::<CAP, NC>::sort_consumers(&mut v);

    let syms: Vec<String> = v.iter().map(|c| c.contract.symbol.as_str().to_string()).collect();
    assert_eq!(syms, vec!["AAPL", "GOOG", "MSFT"]);
}

#[test]
fn sort_is_stable_for_full_key_ties() {
    // Same key (symbol/currency/exchange/side/type); only contract_id and
    // `exchange` differ, neither of which is in the sort key.
    let mut v = vec![
        IbkrBarConsumer::new(
            Contract {
                contract_id: 99,
                exchange: "ARCA".into(),
                ..contract("AAPL", SecurityType::Stock, "USD", "NASDAQ")
            },
            WhatToShow::Trades,
            new_ring().get_new_consumer().unwrap(),
        ),
        IbkrBarConsumer::new(
            Contract {
                contract_id: 7,
                exchange: "SMART".into(),
                ..contract("AAPL", SecurityType::Stock, "USD", "NASDAQ")
            },
            WhatToShow::Trades,
            new_ring().get_new_consumer().unwrap(),
        ),
    ];
    StrategyDataBundler::<CAP, NC>::sort_consumers(&mut v);

    assert_eq!(v[0].contract.contract_id, 99, "sort_by is stable: input order kept");
    assert_eq!(v[1].contract.contract_id, 7);
}

#[test]
fn sort_is_deterministic_and_idempotent() {
    let build = || {
        vec![
            make_consumer("MSFT", SecurityType::Stock, "USD", "NASDAQ", WhatToShow::Trades),
            make_consumer("EUR", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::Ask),
            make_consumer("AAPL", SecurityType::Stock, "USD", "NASDAQ", WhatToShow::Bid),
            make_consumer("EUR", SecurityType::ForexPair, "USD", "IDEALPRO", WhatToShow::Bid),
        ]
    };

    let mut a = build();
    StrategyDataBundler::<CAP, NC>::sort_consumers(&mut a);
    let once: Vec<(String, String, String, u8)> =
        a.iter().map(|c| { let (s, cur, pe) = label(c); (s, cur, pe, rank(c)) }).collect();

    let mut b = build();
    StrategyDataBundler::<CAP, NC>::sort_consumers(&mut b);
    StrategyDataBundler::<CAP, NC>::sort_consumers(&mut b);
    let twice: Vec<(String, String, String, u8)> =
        b.iter().map(|c| { let (s, cur, pe) = label(c); (s, cur, pe, rank(c)) }).collect();

    assert_eq!(once, twice);
    assert_eq!(
        once,
        vec![
            ("EUR".into(), "USD".into(), "IDEALPRO".into(), 0),
            ("EUR".into(), "USD".into(), "IDEALPRO".into(), 1),
            ("AAPL".into(), "USD".into(), "NASDAQ".into(), 0),
            ("MSFT".into(), "USD".into(), "NASDAQ".into(), 2),
        ]
    );
}

#[test]
fn sort_empty_and_single_are_noops() {
    let mut empty: Vec<IbkrBarConsumer<CAP, NC>> = vec![];
    StrategyDataBundler::<CAP, NC>::sort_consumers(&mut empty);
    assert!(empty.is_empty());

    let mut one = vec![make_consumer("AAPL", SecurityType::Stock, "USD", "NASDAQ", WhatToShow::Trades)];
    StrategyDataBundler::<CAP, NC>::sort_consumers(&mut one);
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].contract.symbol.as_str(), "AAPL");
}

// Keep the imported type used even if a future edit drops the try_pop test.
#[allow(dead_code)]
fn _assert_consumer_type(_: SpmcRingBufferConsumer<Bar, CAP, NC>) {}
