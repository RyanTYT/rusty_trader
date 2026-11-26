//! Tests for `src/market_data/consumer/helper.rs`.
//!
//! Covered here (pure, deterministic — no IB, no DB, no network):
//!   - `next_boundary` (src/market_data/consumer/helper.rs:224) — round a
//!     `SystemTime` up to the next multiple of `interval` since the epoch.
//!   - `aggregate_bars` (src/market_data/consumer/helper.rs:29) — 5-second
//!     `Bar`s → N-second OHLCV buckets. The guard at :41-48 uses the *latest*
//!     bar: if `latest_bucket == bucket(latest + 5)` the in-progress bucket is
//!     not emitted and the deque is left untouched.
//!
//! NOT covered — need a live `ibapi::Client` / spawned threads:
//!   - `align_and_prime_schedule` (src/market_data/consumer/helper.rs:115) —
//!     takes `&IbkrContractScheduler`, whose only constructor is
//!     `IbkrContractScheduler::new(Arc<Client>)`
//!     (src/schedule/contract_scheduler.rs:60), plus `IbkrBarConsumer`s whose
//!     `try_pop` blocks on a producer thread that needs a subscription.
//!
//! Known quirk pinned by
//! `aggregate_bars_partial_start_is_not_backfilled`: a bucket that starts
//! mid-window (first bar at :20, width 60) is opened at that first bar's price;
//! the missing bars before it are not synthesised (documented "MINOR_BUG" at
//! src/market_data/consumer/helper.rs:28).

use std::collections::VecDeque;
use std::time::{Duration, UNIX_EPOCH};

use chrono::{DateTime, TimeZone, Utc};
use ibapi::contracts::{Contract, SecurityType};
use ibapi::market_data::realtime::{Bar, WhatToShow};
use rust_decimal::Decimal;
use time::{OffsetDateTime, UtcOffset};
use trading_app::database::models::OptionType;
use trading_app::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use trading_app::test_internals::{aggregate_bars, next_boundary};

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
        count: 1,
    }
}

/// `count` bars spaced 5s apart starting at `start`, where bar `i` has
/// open = base+i, high = base+100+i, low = base-100-i, close = base+50+i,
/// volume = 1+i. Deliberately non-monotonic OHLC so open/high/low/close
/// cannot be confused with one another.
fn series(start: i64, count: i64, base: f64) -> Vec<Bar> {
    (0..count)
        .map(|i| {
            let b = base + i as f64;
            bar(start + 5 * i, b, b + 100.0, b - 100.0, b + 50.0, 1.0 + i as f64)
        })
        .collect()
}

fn stock() -> Contract {
    Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        primary_exchange: "NASDAQ".into(),
        exchange: "SMART".into(),
        ..Default::default()
    }
}

fn forex(base: &str, quote: &str) -> Contract {
    Contract {
        symbol: base.into(),
        security_type: SecurityType::ForexPair,
        currency: quote.into(),
        exchange: "IDEALPRO".into(),
        primary_exchange: "IDEALPRO".into(),
        ..Default::default()
    }
}

fn option() -> Contract {
    Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Option,
        currency: "USD".into(),
        primary_exchange: "NASDAQ".into(),
        exchange: "SMART".into(),
        right: "C".into(),
        last_trade_date_or_contract_month: "20261218".into(),
        strike: 150.0,
        multiplier: "100".into(),
        ..Default::default()
    }
}

fn stamp(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).single().expect("valid timestamp")
}

fn run(
    contract: &Contract,
    what: &WhatToShow,
    bars: Vec<Bar>,
    width: u32,
) -> (Vec<HistoricalDataFullKeys>, usize) {
    let mut deque: VecDeque<Bar> = bars.into();
    let out = aggregate_bars(contract, what, &mut deque, width);
    (out, deque.len())
}

// ──────────────────────────── next_boundary ────────────────────────────

#[test]
fn next_boundary_table() {
    // (from_nanos, interval_nanos, expected_nanos)
    let cases: &[(u64, u64, u64)] = &[
        (0, 1_000, 0),                       // exactly on boundary
        (1, 1_000, 1_000),                   // 1ns past → next
        (999, 1_000, 1_000),                 // 1ns before → next
        (1_000, 1_000, 1_000),               // exactly on boundary
        (1_001, 1_000, 2_000),               // just past
        (3_500_000_000, 1_000_000_000, 4_000_000_000), // 3.5s → 4s
        (120_000_000_000, 60_000_000_000, 120_000_000_000),
        (121_000_000_000, 60_000_000_000, 180_000_000_000),
        (5_000_000_000, 5_000_000_000, 5_000_000_000), // BAR_INTERVAL
        (4_000, 3_000, 6_000),               // interval does not divide the second
        (10, 7, 14),
        (1, 5, 5),
    ];

    for &(from, interval, expected) in cases {
        let got = next_boundary(
            UNIX_EPOCH + Duration::from_nanos(from),
            Duration::from_nanos(interval),
        );
        assert_eq!(
            got,
            UNIX_EPOCH + Duration::from_nanos(expected),
            "next_boundary(from={from}ns, interval={interval}ns)"
        );
    }
}

#[test]
fn next_boundary_result_is_always_a_multiple_of_the_interval() {
    for from_ms in [0u64, 1, 4_999, 5_000, 5_001, 10_001, 123_456] {
        let got = next_boundary(
            UNIX_EPOCH + Duration::from_millis(from_ms),
            Duration::from_secs(5),
        );
        let got_ms = got.duration_since(UNIX_EPOCH).expect("post-epoch").as_millis() as u64;
        assert_eq!(got_ms % 5_000, 0, "from_ms={from_ms} -> {got_ms}ms");
        assert!(got_ms >= from_ms, "rounds up, never down");
        assert!(got_ms - from_ms < 5_000, "rounds to the *next* boundary only");
    }
}

#[test]
fn next_boundary_is_idempotent() {
    for from in [
        UNIX_EPOCH,
        UNIX_EPOCH + Duration::from_nanos(1),
        UNIX_EPOCH + Duration::from_secs(12_345),
        UNIX_EPOCH + Duration::from_secs(1_700_000_000),
    ] {
        let once = next_boundary(from, Duration::from_secs(5));
        let twice = next_boundary(once, Duration::from_secs(5));
        assert_eq!(once, twice, "already-rounded input must be a fixed point");
    }
}

#[test]
#[should_panic(expected = "SystemTimeError")]
fn next_boundary_before_the_epoch_panics() {
    // `from.duration_since(UNIX_EPOCH).unwrap()` at
    // src/market_data/consumer/helper.rs:225 — pre-epoch input is not handled.
    next_boundary(UNIX_EPOCH - Duration::from_secs(1), Duration::from_secs(5));
}

// ─────────────────────── aggregate_bars: guard ───────────────────────

#[test]
fn aggregate_bars_empty_deque_returns_empty() {
    let (out, left) = run(&stock(), &WhatToShow::Trades, vec![], 60);
    assert!(out.is_empty());
    assert_eq!(left, 0);
}

#[test]
fn aggregate_bars_guard_table() {
    // A single bar is emitted only when its bucket has rolled over, i.e. when
    // bucket(ts) != bucket(ts + 5).
    // (width_secs, ts, expected_number_of_bars)
    let cases: &[(u32, i64, usize)] = &[
        (60, 0, 0),
        (60, 50, 0),
        (60, 55, 1),
        (300, 290, 0),
        (300, 295, 1),
        (5, 0, 1),
        (5, 4, 1),
        (1, 7, 1),
        (60, 60 + 55, 1),
        (60, 60 + 50, 0),
    ];

    for &(width, ts, expected) in cases {
        let (out, left) = run(&stock(), &WhatToShow::Trades, vec![bar(ts, 1.0, 2.0, 0.5, 1.5, 1.0)], width);
        assert_eq!(out.len(), expected, "width={width} ts={ts}");
        if expected == 0 {
            assert_eq!(left, 1, "guard must not consume bars");
        } else {
            assert_eq!(left, 0, "aggregation consumes the deque");
        }
    }
}

#[test]
fn aggregate_bars_does_not_emit_partial_trailing_bucket() {
    // 0..=55 (bucket 0) + 60,65 (bucket 60, still in progress at latest=65:
    // bucket(65)=60 == bucket(70)=60) → nothing emitted, nothing consumed.
    let mut bars = series(0, 12, 100.0);
    bars.extend(series(60, 2, 200.0));
    let expected_dates: Vec<i64> = bars.iter().map(|b| b.date.unix_timestamp()).collect();

    let mut deque: VecDeque<Bar> = bars.into();
    let out = aggregate_bars(&stock(), &WhatToShow::Trades, &mut deque, 60);

    assert!(out.is_empty(), "no complete bucket at the tail -> no emission");
    assert_eq!(deque.len(), 14);
    let dates: Vec<i64> = deque.iter().map(|b| b.date.unix_timestamp()).collect();
    assert_eq!(dates, expected_dates, "bars must be preserved in order");
}

// ─────────────────────── aggregate_bars: OHLCV ───────────────────────

#[test]
fn aggregate_bars_single_bucket_exact_ohlcv() {
    // bar i: open=100+i, high=200+i, low=0+i, close=150+i, volume=1+i
    let bars: Vec<Bar> = (0..12)
        .map(|i| {
            let i = i as f64;
            bar(5 * i as i64, 100.0 + i, 200.0 + i, i, 150.0 + i, 1.0 + i)
        })
        .collect();

    let (out, left) = run(&stock(), &WhatToShow::Trades, bars, 60);
    assert_eq!(left, 0);
    assert_eq!(out.len(), 1);

    match &out[0] {
        HistoricalDataFullKeys::Stock(s) => {
            assert_eq!(s.stock, "AAPL");
            assert_eq!(s.primary_exchange, "NASDAQ");
            assert_eq!(s.currency, "USD");
            assert_eq!(s.time, stamp(0), "bucket time is the bucket start");
            assert_eq!(s.open, 100.0, "open = first bar's open");
            assert_eq!(s.high, 211.0, "high = max over the bucket");
            assert_eq!(s.low, 0.0, "low = min over the bucket");
            assert_eq!(s.close, 161.0, "close = last bar's close");
            assert_eq!(s.volume, Decimal::new(78, 0), "volume = sum(1..=12)");
        }
        other => panic!("expected Stock, got {other:?}"),
    }
}

#[test]
fn aggregate_bars_two_buckets_in_one_call() {
    let mut bars = series(0, 12, 100.0);
    bars.extend(series(60, 12, 500.0));

    let (out, left) = run(&stock(), &WhatToShow::Trades, bars, 60);
    assert_eq!(left, 0);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].get_time(), stamp(0));
    assert_eq!(out[1].get_time(), stamp(60));

    // Bucket 0: base 100 → open 100, close 150+11=161, high 211, low 0.
    match &out[0] {
        HistoricalDataFullKeys::Stock(s) => {
            assert_eq!((s.open, s.high, s.low, s.close), (100.0, 211.0, 0.0, 161.0));
            assert_eq!(s.volume, Decimal::new(78, 0));
        }
        other => panic!("expected Stock, got {other:?}"),
    }
    // Bucket 1: base 500 → open 500, close 561, high 611, low 400.
    match &out[1] {
        HistoricalDataFullKeys::Stock(s) => {
            assert_eq!((s.open, s.high, s.low, s.close), (500.0, 611.0, 400.0, 561.0));
            assert_eq!(s.volume, Decimal::new(78, 0));
        }
        other => panic!("expected Stock, got {other:?}"),
    }
}

#[test]
fn aggregate_bars_output_is_ordered_by_bucket() {
    let mut bars = series(0, 12, 1.0);
    bars.extend(series(60, 12, 2.0));
    bars.extend(series(120, 12, 3.0));

    let (out, _) = run(&stock(), &WhatToShow::Trades, bars, 60);
    let times: Vec<i64> = out.iter().map(|b| b.get_time().timestamp()).collect();
    assert_eq!(times, vec![0, 60, 120]);
}

#[test]
fn aggregate_bars_gap_produces_no_synthetic_buckets() {
    // 0..=55 then 300..=355 — the empty 60..295 range must not be filled.
    let mut bars = series(0, 12, 100.0);
    bars.extend(series(300, 12, 200.0));

    let (out, left) = run(&stock(), &WhatToShow::Trades, bars, 60);
    assert_eq!(left, 0);
    let times: Vec<i64> = out.iter().map(|b| b.get_time().timestamp()).collect();
    assert_eq!(times, vec![0, 300]);
}

#[test]
fn aggregate_bars_bucket_is_not_split_by_holes() {
    // Bars 0 and 5, then a hole, then 55 (still bucket 0) and 119 (bucket 60):
    // bucket membership is by timestamp, not adjacency.
    let bars = vec![
        bar(0, 10.0, 20.0, 5.0, 15.0, 1.0),
        bar(5, 11.0, 21.0, 6.0, 16.0, 2.0),
        bar(55, 12.0, 25.0, 4.0, 18.0, 4.0),
        bar(119, 30.0, 40.0, 20.0, 35.0, 8.0),
    ];

    let (out, _) = run(&stock(), &WhatToShow::Trades, bars, 60);
    assert_eq!(out.len(), 2);
    match (&out[0], &out[1]) {
        (HistoricalDataFullKeys::Stock(a), HistoricalDataFullKeys::Stock(b)) => {
            assert_eq!(a.time, stamp(0));
            assert_eq!((a.open, a.high, a.low, a.close), (10.0, 25.0, 4.0, 18.0));
            assert_eq!(a.volume, Decimal::new(7, 0));
            assert_eq!(b.time, stamp(60));
            assert_eq!((b.open, b.high, b.low, b.close), (30.0, 40.0, 20.0, 35.0));
            assert_eq!(b.volume, Decimal::new(8, 0));
        }
        other => panic!("expected two Stock bars, got {other:?}"),
    }
}

#[test]
fn aggregate_bars_partial_start_is_not_backfilled() {
    // First bar arrives at :20 (bucket 0). No bars before it are invented;
    // open is the first *observed* bar's open. Pins the documented MINOR_BUG
    // at src/market_data/consumer/helper.rs:28.
    let bars = series(20, 8, 100.0); // 20,25,...,55
    let (out, _) = run(&stock(), &WhatToShow::Trades, bars, 60);

    assert_eq!(out.len(), 1);
    match &out[0] {
        HistoricalDataFullKeys::Stock(s) => {
            assert_eq!(s.time, stamp(0), "bucket time is still the window start");
            assert_eq!(s.open, 100.0, "open = first observed bar, not null/filled");
            assert_eq!(s.volume, Decimal::new(36, 0), "sum(1..=8)");
        }
        other => panic!("expected Stock, got {other:?}"),
    }
}

#[test]
fn aggregate_bars_width_5_emits_one_bar_per_input() {
    let bars = vec![
        bar(0, 1.0, 2.0, 0.5, 1.5, 10.0),
        bar(5, 2.0, 3.0, 1.5, 2.5, 20.0),
        bar(10, 3.0, 4.0, 2.5, 3.5, 30.0),
        bar(15, 4.0, 5.0, 3.5, 4.5, 40.0),
    ];
    let (out, _) = run(&stock(), &WhatToShow::Trades, bars, 5);
    let times: Vec<i64> = out.iter().map(|b| b.get_time().timestamp()).collect();
    assert_eq!(times, vec![0, 5, 10, 15]);
    for (i, agg) in out.iter().enumerate() {
        match agg {
            HistoricalDataFullKeys::Stock(s) => {
                assert_eq!(s.close, 1.5 + i as f64);
                assert_eq!(s.volume, Decimal::new(10 * (i as i64 + 1), 0));
            }
            other => panic!("expected Stock, got {other:?}"),
        }
    }
}

#[test]
fn aggregate_bars_width_300_aggregates_a_full_minute_of_5s_bars() {
    let bars = series(0, 60, 100.0); // 0..=295
    let (out, _) = run(&stock(), &WhatToShow::Trades, bars, 300);
    assert_eq!(out.len(), 1);
    match &out[0] {
        HistoricalDataFullKeys::Stock(s) => {
            assert_eq!(s.time, stamp(0));
            assert_eq!(s.open, 100.0);
            assert_eq!(s.close, 150.0 + 59.0);
            assert_eq!(s.high, 200.0 + 59.0);
            assert_eq!(s.low, 0.0);
            assert_eq!(s.volume, Decimal::new(1_830, 0), "sum(1..=60)");
        }
        other => panic!("expected Stock, got {other:?}"),
    }
}

#[test]
fn aggregate_bars_uses_the_instant_not_the_clock_offset() {
    // Same instant expressed in UTC+8 must land in the same epoch bucket.
    let shifted = OffsetDateTime::from_unix_timestamp(55)
        .unwrap()
        .to_offset(UtcOffset::from_hms(8, 0, 0).unwrap());
    let bars = vec![Bar {
        date: shifted,
        open: 1.0,
        high: 2.0,
        low: 0.5,
        close: 1.5,
        volume: 1.0,
        wap: 1.5,
        count: 1,
    }];

    let (out, _) = run(&stock(), &WhatToShow::Trades, bars, 60);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get_time(), stamp(0));
    assert_eq!(out[0].get_time(), DateTime::<Utc>::from_timestamp(0, 0).unwrap());
}

// ────────────────────── aggregate_bars: asset types ──────────────────────

#[test]
fn aggregate_bars_option_contract_produces_options_variant() {
    let bars = series(0, 12, 100.0);
    let (out, _) = run(&option(), &WhatToShow::Trades, bars, 60);

    assert_eq!(out.len(), 1);
    match &out[0] {
        HistoricalDataFullKeys::Options(o) => {
            assert_eq!(o.stock, "AAPL");
            assert_eq!(o.primary_exchange, "NASDAQ");
            assert_eq!(o.currency, "USD");
            assert_eq!(o.expiry, "20261218");
            assert_eq!(o.strike, 150.0);
            assert_eq!(o.multiplier, "100");
            assert!(matches!(o.option_type, OptionType::Call), "right C -> Call");
            assert_eq!(o.time, stamp(0));
            assert_eq!((o.open, o.high, o.low, o.close), (100.0, 211.0, 0.0, 161.0));
            assert_eq!(o.volume, Decimal::new(78, 0));
        }
        other => panic!("expected Options, got {other:?}"),
    }
}

#[test]
fn aggregate_bars_forex_bid_fills_bid_side_only() {
    let bars = series(0, 60, 100.0); // width 300 = forex bucket
    let (out, _) = run(&forex("EUR", "USD"), &WhatToShow::Bid, bars, 300);

    assert_eq!(out.len(), 1);
    match &out[0] {
        HistoricalDataFullKeys::Forex(f) => {
            assert_eq!(f.pair, "FX:EUR/USD");
            assert_eq!(f.time, stamp(0));
            assert_eq!(f.bid_open, Some(100.0));
            assert_eq!(f.bid_high, Some(259.0));
            assert_eq!(f.bid_low, Some(0.0));
            assert_eq!(f.bid_close, Some(209.0));
            assert_eq!(f.ask_open, None);
            assert_eq!(f.ask_high, None);
            assert_eq!(f.ask_low, None);
            assert_eq!(f.ask_close, None);
        }
        other => panic!("expected Forex, got {other:?}"),
    }
}

#[test]
fn aggregate_bars_forex_ask_fills_ask_side_only() {
    let bars = series(0, 60, 100.0);
    let (out, _) = run(&forex("EUR", "USD"), &WhatToShow::Ask, bars, 300);

    assert_eq!(out.len(), 1);
    match &out[0] {
        HistoricalDataFullKeys::Forex(f) => {
            assert_eq!(f.pair, "FX:EUR/USD");
            assert_eq!(f.bid_open, None);
            assert_eq!(f.bid_high, None);
            assert_eq!(f.bid_low, None);
            assert_eq!(f.bid_close, None);
            assert_eq!(f.ask_open, Some(100.0));
            assert_eq!(f.ask_high, Some(259.0));
            assert_eq!(f.ask_low, Some(0.0));
            assert_eq!(f.ask_close, Some(209.0));
        }
        other => panic!("expected Forex, got {other:?}"),
    }
}

#[test]
#[should_panic(expected = "Tried to construct Forex from trades/midpoint")]
fn aggregate_bars_forex_with_trades_panics() {
    let bars = series(0, 60, 100.0);
    let _ = run(&forex("EUR", "USD"), &WhatToShow::Trades, bars, 300);
}

#[test]
#[should_panic(expected = "Tried to construct Forex from trades/midpoint")]
fn aggregate_bars_forex_with_midpoint_panics() {
    let bars = series(0, 60, 100.0);
    let _ = run(&forex("EUR", "USD"), &WhatToShow::MidPoint, bars, 300);
}

// ───────────────── aggregate_bars: accessor consistency ─────────────────

#[test]
fn aggregated_stock_price_accessors_agree_with_fields() {
    let bars = series(0, 12, 100.0);
    let (out, _) = run(&stock(), &WhatToShow::Trades, bars, 60);
    let agg = &out[0];

    assert_eq!(agg.get_price(), 161.0);
    assert_eq!(agg.get_open_price(), 100.0);
    assert_eq!(agg.get_high_price(), 211.0);
    assert_eq!(agg.get_low_price(), 0.0);
    assert_eq!(agg.get_volume(), Decimal::new(78, 0));
}

#[test]
fn aggregated_forex_price_prefers_ask_then_bid() {
    let bid_bars = series(0, 60, 100.0);
    let (bid_out, _) = run(&forex("EUR", "USD"), &WhatToShow::Bid, bid_bars, 300);
    let bid = &bid_out[0];
    assert_eq!(bid.get_price(), 209.0, "bid-only bar falls back to bid_close");

    let ask_bars = series(0, 60, 300.0);
    let (ask_out, _) = run(&forex("EUR", "USD"), &WhatToShow::Ask, ask_bars, 300);
    let ask = &ask_out[0];
    assert_eq!(ask.get_price(), 409.0, "ask_close wins when present");
}
