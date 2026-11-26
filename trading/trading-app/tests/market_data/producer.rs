//! Tests for `src/market_data/producer.rs` and the producer → consumer ring
//! mechanics the producer layer sits on.
//!
//! Covered here (pure, deterministic — no IB client, no DB, no network):
//!   - `IbkrBarProducer::new` (src/market_data/producer.rs:36) — construction
//!     only. The wrapped `producer` field is private, so the only observable
//!     surface is `contract` / `what_to_show` plus whatever was pushed into the
//!     ring *before* the handle is moved into the struct.
//!   - ring capacity / full / empty / wrap-around / FIFO ordering / multi
//!     consumer fan-out, exercised through the same
//!     `SpmcRingBuffer<Bar, CAP, NC>` the producer and consumers are built from
//!     (src/market_data/handler.rs:188-198 is the production construction path).
//!
//! NOT covered — require a live `ibapi::Client` and/or a spawned thread:
//!   - `begin_producer_thread_grouped` (src/market_data/producer.rs:77) — the
//!     spawned closure immediately does `weak_client.upgrade()` (:92) and
//!     `client.realtime_bars(..)` (:100); there is no client-free constructor.
//!   - `align_and_prime_schedule_producers` (src/market_data/producer.rs:380)
//!     — takes `&IbkrContractScheduler` (which needs `Arc<Client>`,
//!     src/schedule/contract_scheduler.rs:60) and `&[(Subscription<Bar>, ...)]`,
//!     and `Subscription` cannot be fabricated outside `ibapi` (see
//!     `traits/current_price.rs` for the same blocker).
//!   - `resubscribe_bar` (src/market_data/producer.rs:332) — private + needs
//!     `Weak<Client>` + `Subscription<Bar>`.
//!   - `MarketDataProducer` (src/market_data/producer.rs:49) — fields are
//!     private and there is no constructor; the only producer is the spawn at
//!     :326, so `Drop` / `async_drop` shutdown flags cannot be observed from a
//!     test without a running thread.
//!   - `sleep_until_system_time` (src/market_data/producer.rs:516) — private.
//!
//! Ring semantics worth knowing: capacity only applies once at least one
//! consumer is registered. With zero consumers the cached min-head is
//! initialised to `0` and then set to `usize::MAX` on the first full check
//! (spmc-ring/src/ring_buffer/spmc_ring_buffer.rs:239-253), so the next
//! `tail - last_slowest_head` underflows. In debug that panics
//! (`attempt to subtract with overflow`); in release it wraps and the ring
//! never reports full. Production always registers a consumer in the same
//! breath as the producer (src/market_data/handler.rs:188-198), so this is
//! pinned by an ignored known-bug test rather than relied upon.

use std::sync::Arc;

use ibapi::contracts::{Contract, SecurityType};
use ibapi::market_data::realtime::{Bar, WhatToShow};
use spmc_ring::ring_buffer::spmc_ring_buffer::{
    SpmcRingBuffer, SpmcRingBufferConsumer, SpmcRingBufferProducer,
};
use time::OffsetDateTime;
use trading_app::market_data::consumer::strategy_consumer::IbkrBarConsumer;
use trading_app::market_data::producer::IbkrBarProducer;

const CAP: usize = 4;
const NC: usize = 2;

type Ring = SpmcRingBuffer<Bar, CAP, NC>;
type Prod = SpmcRingBufferProducer<Bar, CAP, NC>;
type Cons = SpmcRingBufferConsumer<Bar, CAP, NC>;

// ──────────────────────────── helpers ────────────────────────────

fn bar(secs: i64, o: f64, h: f64, l: f64, c: f64, v: f64) -> Bar {
    Bar {
        date: OffsetDateTime::from_unix_timestamp(secs).expect("valid unix timestamp"),
        open: o,
        high: h,
        low: l,
        close: c,
        volume: v,
        wap: c,
        count: 3,
    }
}

fn px_bar(secs: i64, close: f64) -> Bar {
    bar(secs, close, close + 1.0, close - 1.0, close, 10.0)
}

fn stock() -> Contract {
    Contract {
        contract_id: 265598,
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        exchange: "SMART".into(),
        primary_exchange: "NASDAQ".into(),
        currency: "USD".into(),
        ..Default::default()
    }
}

fn ring() -> Arc<Ring> {
    Arc::new(Ring::new())
}

fn producer(r: &Arc<Ring>) -> Prod {
    r.get_new_producer().expect("producer slot free")
}

fn consumer(r: &Arc<Ring>) -> Cons {
    r.get_new_consumer().expect("consumer slot free")
}

// ──────────────── IbkrBarProducer construction ────────────────

#[test]
fn producer_new_exposes_contract_and_what_to_show() {
    let ring = ring();
    let contract = stock();
    let p = IbkrBarProducer::<CAP, NC>::new(contract.clone(), WhatToShow::Bid, producer(&ring));

    assert_eq!(p.contract.contract_id, 265598);
    assert_eq!(p.contract.symbol.as_str(), "AAPL");
    assert_eq!(p.contract.security_type, SecurityType::Stock);
    assert_eq!(p.contract.primary_exchange.as_str(), "NASDAQ");
    assert!(matches!(p.what_to_show, WhatToShow::Bid));
}

#[test]
fn producer_new_does_not_consume_ring_slots_twice() {
    // The producer handle is moved into `IbkrBarProducer`; the ring must not
    // hand out a second producer.
    let ring = ring();
    let p = IbkrBarProducer::<CAP, NC>::new(stock(), WhatToShow::Trades, producer(&ring));
    assert!(ring.get_new_producer().is_none());
    drop(p);
    assert!(ring.get_new_producer().is_none(), "ring keeps its producer slot taken for its lifetime");
}

// ──────────────── producer → consumer delivery ────────────────

#[test]
fn producer_to_consumer_empty_ring_pops_none() {
    let ring = ring();
    let c = IbkrBarConsumer::<CAP, NC>::new(stock(), WhatToShow::Trades, consumer(&ring));
    assert_eq!(c.try_pop(), None);
    assert_eq!(c.try_pop(), None, "empty pops are repeatable");
}

#[test]
fn producer_to_consumer_preserves_fifo_order() {
    let ring = ring();
    let c = IbkrBarConsumer::<CAP, NC>::new(stock(), WhatToShow::Trades, consumer(&ring));
    let raw = producer(&ring);

    let pushed = vec![px_bar(0, 100.0), px_bar(5, 101.0), px_bar(10, 102.0)];
    for b in &pushed {
        assert_eq!(raw.try_push(b.clone()), Ok(()));
    }
    // move the producer into the market-data wrapper (no further pushes needed)
    let _p = IbkrBarProducer::<CAP, NC>::new(stock(), WhatToShow::Trades, raw);

    let mut popped = Vec::new();
    while let Some(b) = c.try_pop() {
        popped.push(b);
    }
    assert_eq!(popped, pushed);
    assert_eq!(c.try_pop(), None, "drained ring stays empty");
}

#[test]
fn producer_to_consumer_preserves_exact_bar_fields() {
    let ring = ring();
    let c = IbkrBarConsumer::<CAP, NC>::new(stock(), WhatToShow::Trades, consumer(&ring));
    let raw = producer(&ring);

    let sent = bar(1_700_000_000, 12.5, 13.25, 11.75, 13.0, 4_321.0);
    raw.try_push(sent.clone()).expect("ring has space");

    let got = c.try_pop().expect("one bar pushed");
    assert_eq!(got.date.unix_timestamp(), 1_700_000_000);
    assert_eq!(got.open, 12.5);
    assert_eq!(got.high, 13.25);
    assert_eq!(got.low, 11.75);
    assert_eq!(got.close, 13.0);
    assert_eq!(got.volume, 4_321.0);
    assert_eq!(got.wap, 13.0);
    assert_eq!(got.count, 3);
    assert_eq!(got, sent);
}

// ──────────────── capacity / full / slot reuse ────────────────

#[test]
fn ring_accepts_exactly_capacity_items() {
    let ring = ring();
    let _c = consumer(&ring); // capacity is only enforced once a consumer exists
    let raw = producer(&ring);

    for i in 0..CAP {
        assert_eq!(raw.try_push(px_bar(i as i64, 100.0)), Ok(()), "push {i}");
    }
}

#[test]
fn ring_full_push_returns_item_unchanged() {
    let ring = ring();
    let _c = consumer(&ring);
    let raw = producer(&ring);

    for i in 0..CAP {
        raw.try_push(px_bar(i as i64, 100.0)).expect("within capacity");
    }

    let overflow = px_bar(99, 42.0);
    assert_eq!(
        raw.try_push(overflow.clone()),
        Err(overflow),
        "a full ring returns the caller's item (same value) instead of dropping it"
    );
}

#[test]
fn ring_pop_frees_exactly_one_slot() {
    let ring = ring();
    let c = consumer(&ring);
    let raw = producer(&ring);

    let pushed: Vec<Bar> = (0..CAP).map(|i| px_bar(i as i64, 100.0 + i as f64)).collect();
    for b in &pushed {
        raw.try_push(b.clone()).expect("within capacity");
    }

    let extra = px_bar(50, 999.0);
    assert!(raw.try_push(extra.clone()).is_err(), "ring is full");

    assert_eq!(c.try_pop(), Some(pushed[0].clone()));

    assert_eq!(raw.try_push(extra.clone()), Ok(()), "one pop frees one slot");

    let expected: Vec<Bar> = pushed[1..].iter().cloned().chain(std::iter::once(extra)).collect();
    let mut got = Vec::new();
    while let Some(b) = c.try_pop() {
        got.push(b);
    }
    assert_eq!(got, expected);
}

#[test]
fn ring_wraparound_preserves_fifo_order() {
    let ring = ring();
    let c = consumer(&ring);
    let raw = producer(&ring);

    // Fill, drain half, then push twice more: tail indices wrap past CAP.
    let first: Vec<Bar> = (0..CAP).map(|i| px_bar(i as i64, 10.0 + i as f64)).collect();
    for b in &first {
        raw.try_push(b.clone()).expect("within capacity");
    }
    assert_eq!(c.try_pop(), Some(first[0].clone()));
    assert_eq!(c.try_pop(), Some(first[1].clone()));

    let wrapped: Vec<Bar> = (0..2).map(|i| px_bar(100 + i, 50.0 + i as f64)).collect();
    for b in &wrapped {
        raw.try_push(b.clone()).expect("two slots freed");
    }

    let expected: Vec<Bar> = first[2..].iter().cloned().chain(wrapped).collect();
    let mut got = Vec::new();
    while let Some(b) = c.try_pop() {
        got.push(b);
    }
    assert_eq!(got, expected, "wrap-around must not reorder or lose bars");
}

#[test]
fn ring_repeated_laps_preserve_order() {
    let ring = ring();
    let c = consumer(&ring);
    let raw = producer(&ring);

    for lap in 0..3 {
        for i in 0..CAP {
            let b = px_bar((lap * CAP + i) as i64, 1.0);
            raw.try_push(b.clone()).expect("one in, one out");
            assert_eq!(c.try_pop(), Some(b));
        }
    }
    assert_eq!(c.try_pop(), None);
}

// ──────────────── multiple consumers ────────────────

#[test]
fn ring_two_consumers_each_see_every_item_in_order() {
    let ring = ring();
    let c0 = IbkrBarConsumer::<CAP, NC>::new(stock(), WhatToShow::Bid, consumer(&ring));
    let c1 = IbkrBarConsumer::<CAP, NC>::new(stock(), WhatToShow::Ask, consumer(&ring));
    let raw = producer(&ring);

    let pushed = vec![px_bar(0, 1.0), px_bar(5, 2.0), px_bar(10, 3.0)];
    for b in &pushed {
        raw.try_push(b.clone()).expect("within capacity");
    }

    let mut got0 = Vec::new();
    while let Some(b) = c0.try_pop() {
        got0.push(b);
    }
    let mut got1 = Vec::new();
    while let Some(b) = c1.try_pop() {
        got1.push(b);
    }
    assert_eq!(got0, pushed);
    assert_eq!(got1, pushed, "spmc = each consumer observes the full stream");
}

#[test]
fn ring_fullness_is_governed_by_the_slowest_consumer() {
    let ring = ring();
    let fast = consumer(&ring);
    let _slow = consumer(&ring);
    let raw = producer(&ring);

    for i in 0..CAP {
        raw.try_push(px_bar(i as i64, 1.0)).expect("within capacity");
    }
    // Drain only the fast consumer: the slow head still pins the tail.
    for _ in 0..CAP {
        assert!(fast.try_pop().is_some());
    }
    assert!(
        raw.try_push(px_bar(50, 9.0)).is_err(),
        "slow consumer still at head 0 -> ring stays full"
    );
}

// ──────────────── slot limits ────────────────

#[test]
fn ring_rejects_second_producer() {
    let ring = ring();
    assert!(ring.get_new_producer().is_some());
    assert!(ring.get_new_producer().is_none(), "SPMC = single producer");
}

#[test]
fn ring_rejects_consumer_beyond_num_consumers() {
    let ring = ring();
    for i in 0..NC {
        assert!(ring.get_new_consumer().is_some(), "consumer {i} within limit");
    }
    assert!(ring.get_new_consumer().is_none(), "NC={NC} consumers max");

    let single = Arc::new(SpmcRingBuffer::<Bar, CAP, 1>::new());
    assert!(single.get_new_consumer().is_some());
    assert!(single.get_new_consumer().is_none(), "NUM_CONSUMERS=1 -> second is None");
}

#[test]
fn ring_without_registered_consumer_reports_full_after_capacity() {
    // With no consumer there is nobody to reclaim slots, so the ring must
    // apply backpressure (Err) once CAPACITY items are buffered instead of
    // underflowing `tail - last_slowest_head`. It must never panic, and a
    // consumer must not later start at an overwritten position.
    let ring = ring();
    let raw = producer(&ring);
    for i in 0..CAP {
        assert_eq!(
            raw.try_push(px_bar(i as i64, 1.0)),
            Ok(()),
            "push {i} is within capacity"
        );
    }
    for i in CAP..(CAP * 2 + 1) {
        assert!(
            raw.try_push(px_bar(i as i64, 1.0)).is_err(),
            "consumer-less ring must refuse push {i} at/after capacity"
        );
    }
}
