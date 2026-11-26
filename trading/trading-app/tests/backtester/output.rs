//! `src/backtester/output` tests — equity snapshots, the equity curve, and the
//! result metrics (PnL/return/drawdown/Sharpe/Sortino/trade pairing).
//!
//! The metric helpers (`build`, `max_drawdown_pct`, `sharpe_sortino`,
//! `realized_per_trade_pnl`) are private, so every case drives the public
//! `BacktestResults::compute_in_memory` (or
//! `output::equity::compute_snapshot_from_positions`) instead. That also keeps
//! the tests honest: they assert what the produced JSON/report actually says.
//!
//! BLOCKED (documented, not tested):
//! - `output/equity.rs:60` `compute_snapshot` DB arm — calls
//!   `get_pos_by_strat` on a `PgPool`. The in-memory branch is
//!   covered through `compute_snapshot_from_positions`.
//! - `output/results.rs:58` `BacktestResults::compute` — reads all
//!   `stock_transactions` from Postgres.

use chrono::{Duration, TimeZone, Utc};
use trading_app::backtester::methods::in_memory::state::{
    InMemoryPosition, InMemoryState, InMemoryTransaction, PositionKey,
};
use trading_app::backtester::output::equity::{
    EquityCurve, EquitySnapshot, compute_snapshot_from_positions,
};
use trading_app::backtester::output::results::BacktestResults;

use crate::support::{FakePrices, assert_close, utc};

fn key(stock: &str, primary_exchange: &str, currency: &str) -> PositionKey {
    PositionKey {
        strategy: "test".to_string(),
        stock: stock.to_string(),
        primary_exchange: primary_exchange.to_string(),
        currency: currency.to_string(),
    }
}

fn pos(quantity: f64, avg_price: f64) -> InMemoryPosition {
    InMemoryPosition {
        quantity,
        avg_price,
    }
}

fn cash_key() -> PositionKey {
    key("CASH:SGD", "", "SGD")
}

fn curve(equities: &[f64]) -> EquityCurve {
    let mut c = EquityCurve::new();
    let t0 = utc(2024, 6, 14, 14, 30, 0);
    for (i, e) in equities.iter().enumerate() {
        c.push(EquitySnapshot {
            time: t0 + Duration::minutes(i as i64),
            cash: *e,
            positions_value: 0.0,
            equity: *e,
        });
    }
    c
}

fn state_with_trades(trades: &[(f64, f64, f64)]) -> InMemoryState {
    // (quantity, price, fees)
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    let t0 = utc(2024, 6, 14, 14, 30, 0);
    for (i, (qty, price, fees)) in trades.iter().enumerate() {
        state.record_transaction(InMemoryTransaction {
            strategy: "test".to_string(),
            stock: "QQQ".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            time: t0 + Duration::minutes(i as i64),
            price: *price,
            quantity: *qty,
            fees: *fees,
            action: if *qty >= 0.0 { "Buy" } else { "Sell" }.to_string(),
        });
    }
    state
}

fn results(
    equities: &[f64],
    capital: f64,
    trades: &[(f64, f64, f64)],
    interval: Duration,
) -> BacktestResults {
    BacktestResults::compute_in_memory(
        &curve(equities),
        &state_with_trades(trades),
        capital,
        interval,
    )
}

// ── EquityCurve ───────────────────────────────────────────────────────────

#[test]
fn empty_equity_curve_has_no_last_equity() {
    let c = EquityCurve::default();
    assert_eq!(c.last_equity(), None);
    assert!(c.snapshots.is_empty());
}

#[test]
fn equity_curve_returns_the_most_recent_snapshot() {
    let mut c = EquityCurve::new();
    let t = utc(2024, 6, 14, 14, 30, 0);
    c.push(EquitySnapshot {
        time: t,
        cash: 1.0,
        positions_value: 2.0,
        equity: 3.0,
    });
    c.push(EquitySnapshot {
        time: t + Duration::minutes(5),
        cash: 4.0,
        positions_value: 5.0,
        equity: 9.0,
    });
    assert_eq!(c.last_equity(), Some(9.0));
    assert_eq!(c.snapshots.len(), 2);
}

// ── compute_snapshot_from_positions ───────────────────────────────────────

#[test]
fn snapshot_of_no_positions_is_all_zero() {
    let prices = FakePrices::new();
    let t = utc(2024, 6, 14, 14, 30, 0);
    let snap = compute_snapshot_from_positions(vec![], &prices, t, 0.0);
    assert_eq!(snap.time, t);
    assert_eq!(snap.cash, 0.0);
    assert_eq!(snap.positions_value, 0.0);
    assert_eq!(snap.equity, 0.0);
}

#[test]
fn cash_sgd_is_valued_at_one_and_not_queried_from_the_supplier() {
    // The fake prices mistake would value cash at 99 if it were consulted.
    let prices = FakePrices::new().with_quote("CASH:SGD", "SGD", 99.0);
    let t = utc(2024, 6, 14, 14, 30, 0);
    let snap =
        compute_snapshot_from_positions(vec![(cash_key(), pos(1_000.0, 1.0))], &prices, t, 0.0);
    assert_eq!(snap.cash, 1_000.0);
    assert_eq!(snap.positions_value, 0.0);
    assert_eq!(snap.equity, 1_000.0);
}

#[test]
fn stock_position_is_marked_to_market_and_fx_converted_to_sgd() {
    let prices = FakePrices::new()
        .with_quote("QQQ", "USD", 102.0)
        .with_fx("USD", "SGD", 1.35);
    let t = utc(2024, 6, 14, 14, 30, 0);
    let snap = compute_snapshot_from_positions(
        vec![(key("QQQ", "NASDAQ", "USD"), pos(10.0, 100.0))],
        &prices,
        t,
        0.0,
    );
    assert_eq!(snap.cash, 0.0);
    assert_close(snap.positions_value, 10.0 * 102.0 * 1.35, 1e-12);
    assert_close(snap.equity, 1_377.0, 1e-12);
}

#[test]
fn cash_and_positions_are_summed_including_a_negative_mark() {
    let prices = FakePrices::new()
        .with_quote("AAPL", "USD", 50.0)
        .with_fx("USD", "SGD", 2.0);
    let t = utc(2024, 6, 14, 14, 30, 0);
    let snap = compute_snapshot_from_positions(
        vec![
            (cash_key(), pos(1_000.0, 1.0)),
            (key("AAPL", "NASDAQ", "USD"), pos(-4.0, 60.0)),
        ],
        &prices,
        t,
        0.0,
    );
    assert_eq!(snap.cash, 1_000.0);
    assert_close(snap.positions_value, -4.0 * 50.0 * 2.0, 1e-12);
    assert_close(snap.equity, 600.0, 1e-12);
}

#[test]
fn missing_quote_uses_fallback_close_and_missing_fx_uses_default_rate() {
    // No QQQ quote and no USD/SGD pair in the fake.
    let prices = FakePrices::new();
    let t = utc(2024, 6, 14, 14, 30, 0);
    let snap = compute_snapshot_from_positions(
        vec![(key("QQQ", "NASDAQ", "USD"), pos(10.0, 100.0))],
        &prices,
        t,
        50.0,
    );
    assert_close(snap.positions_value, 10.0 * 50.0 * 1.2713, 1e-12);
    assert_close(snap.equity, 635.65, 1e-12);
}

#[test]
fn non_sgd_cash_positions_are_priced_through_their_fx_pair() {
    // Only CASH:SGD has the fixed rate 1.0. CASH:USD is rebuilt by
    // build_contract_from_stock as an IDEALPRO USD/SGD pair, so it is marked
    // with the FX rate (2.0 here), not with a "CASH:USD" quote.
    let prices = FakePrices::usd_sgd(2.0);
    let t = utc(2024, 6, 14, 14, 30, 0);
    let snap = compute_snapshot_from_positions(
        vec![(key("CASH:USD", "", "USD"), pos(100.0, 1.0))],
        &prices,
        t,
        0.0,
    );
    assert_eq!(snap.cash, 200.0);
    assert_eq!(snap.positions_value, 0.0);
}

#[test]
fn near_zero_positions_are_skipped() {
    let prices = FakePrices::new().with_quote("QQQ", "USD", 100.0);
    let t = utc(2024, 6, 14, 14, 30, 0);
    let snap = compute_snapshot_from_positions(
        vec![
            (cash_key(), pos(1_000.0, 1.0)),
            (key("QQQ", "NASDAQ", "USD"), pos(1e-12, 100.0)),
        ],
        &prices,
        t,
        0.0,
    );
    assert_eq!(snap.positions_value, 0.0);
    assert_eq!(snap.equity, 1_000.0);
}

// ── BacktestResults: PnL / return / drawdown ──────────────────────────────

#[test]
fn empty_curve_falls_back_to_starting_capital() {
    let r = results(&[], 1_000.0, &[], Duration::minutes(5));
    assert_eq!(r.starting_capital, 1_000.0);
    assert_eq!(r.final_equity, 1_000.0);
    assert_eq!(r.total_pnl, 0.0);
    assert_eq!(r.total_return_pct, 0.0);
    assert_eq!(r.max_drawdown_pct, 0.0);
    assert_eq!(r.sharpe, 0.0);
    assert_eq!(r.sortino, 0.0);
    assert_eq!(r.num_trades, 0);
    assert_eq!(r.num_closed_trades, 0);
    assert_eq!(r.num_winning_trades, 0);
    assert_eq!(r.win_rate_pct, 0.0);
    assert!(r.equity_curve.is_empty());
}

#[test]
fn pnl_and_return_use_the_last_equity_point() {
    let r = results(
        &[10_000.0, 10_050.0, 10_100.0],
        10_000.0,
        &[],
        Duration::minutes(5),
    );
    assert_eq!(r.final_equity, 10_100.0);
    assert_eq!(r.total_pnl, 100.0);
    assert_close(r.total_return_pct, 1.0, 1e-12);
}

#[test]
fn zero_starting_capital_does_not_divide_by_zero() {
    let r = results(&[0.0, 5.0], 0.0, &[], Duration::minutes(5));
    assert_eq!(r.final_equity, 5.0);
    assert_eq!(r.total_pnl, 5.0);
    assert_eq!(r.total_return_pct, 0.0);
}

#[test]
fn max_drawdown_is_peak_to_trough_over_the_whole_curve() {
    // Peak 120 -> trough 90 = 25%; later 130 -> 117 = 10%; max is 25%.
    let r = results(
        &[100.0, 120.0, 90.0, 130.0, 117.0],
        100.0,
        &[],
        Duration::minutes(5),
    );
    assert_close(r.max_drawdown_pct, 25.0, 1e-12);
}

#[test]
fn max_drawdown_of_a_monotonic_curve_is_zero() {
    let r = results(
        &[100.0, 101.0, 102.0, 103.0],
        100.0,
        &[],
        Duration::minutes(5),
    );
    assert_eq!(r.max_drawdown_pct, 0.0);
}

#[test]
fn max_drawdown_ignores_a_non_positive_peak() {
    // Equity <= 0 never divides by the peak, so no drawdown is reported.
    let r = results(&[-100.0, -50.0], -100.0, &[], Duration::minutes(5));
    assert_eq!(r.max_drawdown_pct, 0.0);
}

// ── Sharpe / Sortino / annualization ─────────────────────────────────────

#[test]
fn sharpe_and_sortino_match_the_population_formula() {
    // returns: +10%, -10%, +10%
    let equities = [100.0, 110.0, 99.0, 108.9];
    let r = results(&equities, 100.0, &[], Duration::minutes(5));

    let returns = [0.1, -0.1, 0.09999999999999998];
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let std = (returns.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n).sqrt();
    let downside = (returns
        .iter()
        .filter(|x| **x < 0.0)
        .map(|x| x * x)
        .sum::<f64>()
        / n)
        .sqrt();
    let annualization = (390.0_f64 * 252.0 / 5.0).sqrt();

    assert_close(r.sharpe, mean / std * annualization, 1e-9);
    assert_close(r.sortino, mean / downside * annualization, 1e-9);
    assert_eq!(r.bars_per_year, 19_656.0);
}

#[test]
fn one_snapshot_or_a_flat_curve_yields_zero_risk_adjusted_metrics() {
    for equities in [vec![100.0], vec![100.0, 100.0, 100.0, 100.0]] {
        let r = results(&equities, 100.0, &[], Duration::minutes(5));
        assert_eq!(r.sharpe, 0.0, "equities {equities:?}");
        assert_eq!(r.sortino, 0.0, "equities {equities:?}");
    }
}

#[test]
fn bars_per_year_table() {
    let cases: &[(Duration, f64)] = &[
        (Duration::minutes(1), 98_280.0),
        (Duration::minutes(5), 19_656.0),
        (Duration::minutes(30), 3_276.0),
        (Duration::minutes(60), 1_638.0),
        (Duration::minutes(1439), 390.0 * 252.0 / 1439.0),
        (Duration::minutes(1440), 252.0),
        (Duration::minutes(2880), 252.0),
        // Sub-minute intervals clamp to 1 minute (num_minutes == 0).
        (Duration::seconds(30), 98_280.0),
    ];
    for (interval, expected_bpy) in cases {
        let r = results(&[100.0, 110.0], 100.0, &[], *interval);
        assert_eq!(r.bar_interval_minutes, interval.num_minutes().max(1));
        assert_close(r.bars_per_year, *expected_bpy, 1e-9);
    }
}

// ── Trade pairing / win rate ─────────────────────────────────────────────

#[test]
fn buy_only_trades_count_but_are_not_closed() {
    let r = results(
        &[10_000.0],
        10_000.0,
        &[(10.0, 100.0, 1.0)],
        Duration::minutes(5),
    );
    assert_eq!(r.num_trades, 1);
    assert_eq!(r.num_closed_trades, 0);
    assert_eq!(r.num_winning_trades, 0);
    assert_eq!(r.win_rate_pct, 0.0);
}

#[test]
fn sell_realizes_against_the_running_average_cost_minus_fees() {
    // buy 10 @ 100 (fee 1, ignored on the buy), sell 5 @ 110 (fee 2)
    // -> realized = (110 - 100) * 5 - 2 = 48.
    let trades = [(10.0, 100.0, 1.0), (-5.0, 110.0, 2.0)];
    let r = results(
        &[10_000.0, 10_048.0],
        10_000.0,
        &trades,
        Duration::minutes(5),
    );
    assert_eq!(r.num_trades, 2);
    assert_eq!(r.num_closed_trades, 1);
    assert_eq!(r.num_winning_trades, 1);
    assert_close(r.win_rate_pct, 100.0, 0.0);
}

#[test]
fn trade_pairing_blends_buys_and_reports_losses_too() {
    // buy 10@100, sell 5@110  -> +48
    // buy 5@120 (avg cost blends to 110), sell 10@90 fee 6 -> -206
    let trades = [
        (10.0, 100.0, 1.0),
        (-5.0, 110.0, 2.0),
        (5.0, 120.0, 0.0),
        (-10.0, 90.0, 6.0),
    ];
    let r = results(
        &[10_000.0, 9_842.0],
        10_000.0,
        &trades,
        Duration::minutes(5),
    );
    assert_eq!(r.num_trades, 4);
    assert_eq!(r.num_closed_trades, 2);
    assert_eq!(r.num_winning_trades, 1);
    assert_close(r.win_rate_pct, 50.0, 0.0);
}

#[test]
fn fees_are_charged_only_on_the_sell_side_of_a_realized_trade() {
    // Same prices, different fees: only the sell fee moves the realized PnL.
    let base = [(10.0, 100.0, 0.0), (-10.0, 110.0, 0.0)];
    // A 200 fee is larger than the 100 gross profit on BOTH sides.
    let big_buy_fee = [(10.0, 100.0, 200.0), (-10.0, 110.0, 0.0)];
    let big_sell_fee = [(10.0, 100.0, 0.0), (-10.0, 110.0, 200.0)];

    let a = results(&[10_000.0], 10_000.0, &base, Duration::minutes(5));
    let b = results(&[10_000.0], 10_000.0, &big_buy_fee, Duration::minutes(5));
    let c = results(&[10_000.0], 10_000.0, &big_sell_fee, Duration::minutes(5));

    assert_eq!(a.num_winning_trades, 1);
    // The buy-side fee is ignored by the avg-cost pairing -> still a winner.
    assert_eq!(b.num_winning_trades, 1);
    // The sell-side fee is subtracted -> the trade is a loss.
    assert_eq!(c.num_winning_trades, 0);
    assert_eq!(c.win_rate_pct, 0.0);
}

#[test]
fn separate_symbols_are_paired_independently() {
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    let t = utc(2024, 6, 14, 14, 30, 0);
    let push = |stock: &str, qty: f64, price: f64, fees: f64| {
        state.record_transaction(InMemoryTransaction {
            strategy: "test".to_string(),
            stock: stock.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            time: t,
            price,
            quantity: qty,
            fees,
            action: "x".to_string(),
        });
    };
    push("QQQ", 10.0, 100.0, 0.0);
    push("AAPL", 10.0, 50.0, 0.0);
    push("QQQ", -10.0, 120.0, 0.0); // +200 winner
    push("AAPL", -10.0, 40.0, 0.0); // -100 loser

    let r = BacktestResults::compute_in_memory(
        &curve(&[10_000.0]),
        &state,
        10_000.0,
        Duration::minutes(5),
    );
    assert_eq!(r.num_closed_trades, 2);
    assert_eq!(r.num_winning_trades, 1);
    assert_close(r.win_rate_pct, 50.0, 0.0);
}

#[test]
fn short_round_trip_is_realized() {
    // Intended: sell 10 @ 100 (fee 1), buy 10 @ 90 -> (100 - 90) * 10 - 1 = 99.
    let trades = [(-10.0, 100.0, 1.0), (10.0, 90.0, 0.0)];
    let r = results(&[10_000.0], 10_000.0, &trades, Duration::minutes(5));
    assert_eq!(r.num_closed_trades, 1);
    assert_eq!(r.num_winning_trades, 1);
    // Observed: realized_qty = min(10, 0) = 0 -> pnl -1 -> 0 winners.
}

#[test]
fn equity_curve_is_serialized_with_epoch_seconds() {
    let t0 = utc(2024, 6, 14, 14, 30, 0);
    let r = results(&[1.0, 2.0], 1.0, &[], Duration::minutes(5));
    assert_eq!(r.equity_curve.len(), 2);
    assert_eq!(r.equity_curve[0].time, t0.timestamp());
    assert_eq!(r.equity_curve[1].time, t0.timestamp() + 60); // one bar apart
    assert_eq!(r.equity_curve[0].equity, 1.0);
    assert_eq!(r.equity_curve[1].equity, 2.0);
}

#[test]
fn write_json_round_trips_every_public_field() {
    let r = results(&[10_000.0, 10_100.0], 10_000.0, &[], Duration::minutes(5));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("results.json");
    r.write_json(path.to_str().unwrap()).unwrap();

    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(
        raw.starts_with("{\n  \"starting_capital\""),
        "expected pretty JSON, got {raw:.80}"
    );
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(v["starting_capital"], 10_000.0);
    assert_eq!(v["final_equity"], 10_100.0);
    assert_eq!(v["total_pnl"], 100.0);
    assert_eq!(v["total_return_pct"], 1.0);
    assert_eq!(v["bar_interval_minutes"], 5);
    assert_eq!(v["bars_per_year"], 19_656.0);
    assert_eq!(v["equity_curve"].as_array().unwrap().len(), 2);
    assert!(v["equity_curve"][0]["time"].is_i64());
}

#[test]
fn write_json_reports_io_errors() {
    let r = results(&[1.0], 1.0, &[], Duration::minutes(5));
    let dir = tempfile::tempdir().unwrap();
    let missing_parent = dir.path().join("no-such-dir").join("r.json");
    assert!(r.write_json(missing_parent.to_str().unwrap()).is_err());
}

#[test]
fn snapshot_time_is_echoed_verbatim() {
    let prices = FakePrices::new();
    let t = utc(1999, 12, 31, 23, 59, 59);
    let snap = compute_snapshot_from_positions(vec![], &prices, t, 0.0);
    assert_eq!(snap.time, t);
    assert_eq!(snap.time.timestamp(), 946_684_799);
    let _ = Utc.timestamp_opt(0, 0);
}
