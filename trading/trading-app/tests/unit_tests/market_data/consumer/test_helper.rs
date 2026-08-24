//! Unit tests for `aggregate_bars` and `next_boundary` — pure bar-aggregation logic.
//!
//! See `src/market_data/consumer/helper.rs`. Tests cover:
//! - `next_boundary` — epoch rounding up to interval multiple
//! - `aggregate_bars` — 5-sec bars → N-sec OHLCV aggregation, boundary crossing

use std::collections::VecDeque;
use std::time::{Duration, UNIX_EPOCH};

use ibapi::contracts::Contract;
use ibapi::market_data::realtime::{Bar, WhatToShow};
use ibapi::prelude::SecurityType;
use time::OffsetDateTime;

use trading_app::test_internals::{aggregate_bars, next_boundary};

// ============================ next_boundary ============================

#[test]
fn next_boundary_already_on_boundary_returns_same() {
    // 1000 ns since epoch, interval=1000 ns → already on boundary → return same
    let from = UNIX_EPOCH + Duration::from_nanos(1000);
    let result = next_boundary(from, Duration::from_nanos(1000));
    assert_eq!(result, from);
}

#[test]
fn next_boundary_mid_interval_rounds_up() {
    // 1500 ns since epoch, interval=1000 ns → next boundary = 2000 ns
    let from = UNIX_EPOCH + Duration::from_nanos(1500);
    let result = next_boundary(from, Duration::from_nanos(1000));
    let expected = UNIX_EPOCH + Duration::from_nanos(2000);
    assert_eq!(result, expected);
}

#[test]
fn next_boundary_one_second_interval() {
    // 3.5 seconds since epoch, interval=1 sec → next boundary = 4 sec
    let from = UNIX_EPOCH + Duration::from_millis(3500);
    let result = next_boundary(from, Duration::from_secs(1));
    let expected = UNIX_EPOCH + Duration::from_secs(4);
    assert_eq!(result, expected);
}

#[test]
fn next_boundary_zero_remainder_returns_input() {
    // Exactly on a 60-sec boundary → no change
    let from = UNIX_EPOCH + Duration::from_secs(120);
    let result = next_boundary(from, Duration::from_secs(60));
    assert_eq!(result, from);
}

// ============================ aggregate_bars ============================
// aggregate_bars(contract, what_to_show, collected_bars, bar_time_width_u32)
// - bars are 5-sec ticks; bar_time_width_u32 is the aggregation window (60 or 300)
// - if latest bar's window number == next bar's window number → return [] (no boundary crossed)
// - otherwise, aggregate all bars in completed windows into OHLCV

fn make_bar(timestamp_secs: i64, o: f64, h: f64, l: f64, c: f64, v: f64) -> Bar {
    Bar {
        date: OffsetDateTime::from_unix_timestamp(timestamp_secs).unwrap(),
        open: o,
        high: h,
        low: l,
        close: c,
        volume: v,
        wap: c,
        count: 1,
    }
}

fn stock_contract() -> Contract {
    Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        ..Default::default()
    }
}

#[test]
fn aggregate_bars_empty_returns_empty() {
    let contract = stock_contract();
    let mut bars: VecDeque<Bar> = VecDeque::new();
    let result = aggregate_bars(&contract, &WhatToShow::Trades, &mut bars, 60);
    assert!(result.is_empty());
}

#[test]
fn aggregate_bars_no_boundary_crossed_returns_empty() {
    // Two 5-sec bars within the same 60-sec window (timestamps 10 and 15, window=60)
    // latest bar @ 15, next would be @ 20. latest_bar_no = 0, next_bar_no = 0 → same → empty
    let contract = stock_contract();
    let mut bars: VecDeque<Bar> = VecDeque::new();
    bars.push_back(make_bar(10, 100.0, 105.0, 99.0, 102.0, 1000.0));
    bars.push_back(make_bar(15, 102.0, 106.0, 101.0, 104.0, 500.0));
    let result = aggregate_bars(&contract, &WhatToShow::Trades, &mut bars, 60);
    assert!(result.is_empty(), "no boundary crossed → empty");
    // Bars should NOT be consumed (no aggregation happened)
    assert_eq!(bars.len(), 2);
}

#[test]
fn aggregate_bars_boundary_crossed_aggregates() {
    // Bar at 55 (window 0-60), bar at 60 (window 60-120), bar at 65 (window 60-120)
    // latest @ 65, next would be @ 70. latest_bar_no=60, next_bar_no=60 → SAME → empty!
    // Wait — we need latest and next to be in DIFFERENT windows.
    // Bar at 55, bar at 65: latest @ 65, next @ 70. latest_bar_no=60, next_bar_no=60 → same → empty.
    // To cross boundary: latest bar must be in a later window than the next bar would start.
    // Actually: latest_bar_no = 65 - (65 % 60) = 60. next_bar_timestamp = 65+5=70.
    // next_bar_no = 70 - (70 % 60) = 60. SAME. Hmm.
    //
    // Let me re-read: the check is latest_bar_no == next_bar_no → return empty.
    // For a boundary cross: latest @ 115, next @ 120. latest_bar_no = 60, next_bar_no = 120. DIFFERENT → aggregate.
    let contract = stock_contract();
    let mut bars: VecDeque<Bar> = VecDeque::new();
    // bars at 110, 115 (window 60-120) and 120 (window 120-180)
    bars.push_back(make_bar(110, 100.0, 105.0, 99.0, 101.0, 1000.0));
    bars.push_back(make_bar(115, 101.0, 106.0, 100.0, 103.0, 500.0));
    bars.push_back(make_bar(120, 103.0, 107.0, 102.0, 105.0, 800.0));
    // latest @ 120, next @ 125. latest_bar_no = 120, next_bar_no = 120 → SAME → empty.
    // Hmm, that's still same. Let me use 125 as latest:
    bars.clear();
    bars.push_back(make_bar(110, 100.0, 105.0, 99.0, 101.0, 1000.0));
    bars.push_back(make_bar(115, 101.0, 106.0, 100.0, 103.0, 500.0));
    bars.push_back(make_bar(125, 103.0, 107.0, 102.0, 105.0, 800.0));
    // latest @ 125, next @ 130. latest_bar_no = 120, next_bar_no = 120 → SAME → still empty!
    //
    // The logic: latest_bar_no = latest_ts - (latest_ts % width).
    // For latest @ 125, width=60: 125 - (125 % 60) = 125 - 5 = 120.
    // next_ts = 130. next_bar_no = 130 - (130 % 60) = 130 - 10 = 120. SAME.
    //
    // For a cross: latest @ 175, next @ 180. latest_bar_no = 120, next_bar_no = 180. DIFFERENT.
    bars.clear();
    bars.push_back(make_bar(110, 100.0, 105.0, 99.0, 101.0, 1000.0)); // window 60-120
    bars.push_back(make_bar(115, 101.0, 106.0, 100.0, 103.0, 500.0)); // window 60-120
    bars.push_back(make_bar(175, 103.0, 107.0, 102.0, 105.0, 800.0)); // window 120-180
    // latest @ 175, next @ 180. latest_bar_no = 120, next_bar_no = 180 → DIFFERENT → aggregate!
    let result = aggregate_bars(&contract, &WhatToShow::Trades, &mut bars, 60);
    // Should aggregate the two bars in window 60-120 into one OHLCV bar
    assert!(!result.is_empty(), "boundary crossed → should aggregate");
    // The bars should be consumed
    assert!(
        bars.is_empty(),
        "all bars should be consumed after aggregation"
    );
    // The aggregated bar should have:
    // open = first bar's open = 100.0
    // high = max(105, 106) = 106
    // low = min(99, 100) = 99
    // close = last bar's close = 103
    // volume = 1000 + 500 = 1500
    let agg = &result[0];
    let price = agg.get_price();
    assert!(
        (price - 103.0).abs() < 1e-9,
        "close should be 103, got {price}"
    );
}
