//! `src/backtester/execution` tests — the pure fill/commission models plus the
//! synthesized execution report. These are the deterministic core of the
//! simulated broker; no DB or tokio handle is involved.
//!
//! BLOCKED (documented, not tested):
//! - `execution/broker.rs:58` `BacktestBroker` — construction needs a `PgPool`
//!   + `tokio::runtime::Handle`, and every submission/fill path writes
//!   transactions / positions / open-orders. Its pure decisions delegate to
//!   `decide_fill` + `commission`, covered here.
//! - `OrderSubmitter` itself is trivially exercised only through the broker
//!   (above); the trait has no standalone logic.

use chrono::{TimeZone, Utc};
use ibapi::orders::{Action, Order};
use rust_decimal::Decimal;
use trading_app::backtester::execution::execution_report::synthetic_execution;
use trading_app::backtester::execution::fill_model::{
    CommissionModel, FillEval, FillModel, FillOutcome, LimitFillModel, MarketFillModel,
    MidPriceFillModel, OptimisticLimitFillModel, StopLimitFillModel, StopMarketFillModel,
    commission, decide_fill, fill_model_for,
};
use trading_app::database::models::HistoricalOptionsDataFullKeys;
use trading_app::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;

use crate::support::{assert_close, bar, forex, fx_bar, stock, utc};

fn order(action: Action, qty: f64) -> Order {
    Order {
        action,
        total_quantity: qty,
        ..Default::default()
    }
}

fn limit_order(action: Action, qty: f64, limit: f64) -> Order {
    Order {
        action,
        total_quantity: qty,
        order_type: "LMT".to_string(),
        limit_price: Some(limit),
        ..Default::default()
    }
}

/// Price of a `FillEval::Fill`, if it filled (float comparisons go through
/// `assert_close`, never `==`).
fn eval_price(ev: FillEval) -> Option<f64> {
    match ev {
        FillEval::Fill { price } => Some(price),
        FillEval::Rest => None,
    }
}

// ── decide_fill ───────────────────────────────────────────────────────────

#[test]
fn market_buy_fills_at_close_plus_slippage() {
    let b = bar(utc(2024, 6, 14, 14, 30, 0), 100.0, 105.0, 95.0, 102.0);
    let out = decide_fill(&order(Action::Buy, 10.0), &b, 10.0);
    assert!(out.filled);
    assert_eq!(out.fill_qty, 10.0);
    assert_close(out.fill_price, 102.0 * 1.001, 1e-12);
}

#[test]
fn market_sell_fills_at_close_minus_slippage() {
    let b = bar(utc(2024, 6, 14, 14, 30, 0), 100.0, 105.0, 95.0, 102.0);
    let out = decide_fill(&order(Action::Sell, 10.0), &b, 10.0);
    assert!(out.filled);
    assert_eq!(out.fill_qty, -10.0);
    assert_close(out.fill_price, 102.0 * 0.999, 1e-12);
}

#[test]
fn sell_short_is_treated_as_a_sell() {
    let b = bar(utc(2024, 6, 14, 14, 30, 0), 100.0, 105.0, 95.0, 100.0);
    let out = decide_fill(&order(Action::SellShort, 3.0), &b, 0.0);
    assert_eq!(out.fill_qty, -3.0);
    assert_eq!(out.fill_price, 100.0);
}

#[test]
fn zero_slippage_fills_exactly_at_close() {
    let b = bar(utc(2024, 6, 14, 14, 30, 0), 10.0, 11.0, 9.0, 10.25);
    for action in [Action::Buy, Action::Sell] {
        let out = decide_fill(&order(action, 5.0), &b, 0.0);
        assert_eq!(out.fill_price, 10.25);
    }
}

#[test]
fn buy_limit_crosses_when_low_touches_or_under_cuts_and_fills_at_limit() {
    let cases = [
        (95.0, 95.0, true),   // touch
        (94.0, 95.0, true),   // through
        (95.01, 95.0, false), // above
    ];
    for (low, limit, expect_fill) in cases {
        let b = bar(utc(2024, 6, 14, 14, 30, 0), 100.0, 105.0, low, 102.0);
        let out = decide_fill(&limit_order(Action::Buy, 2.0, limit), &b, 50.0);
        assert_eq!(out.filled, expect_fill, "low={low} limit={limit}");
        if expect_fill {
            // Limit fills ignore slippage entirely.
            assert_eq!(out.fill_price, limit);
            assert_eq!(out.fill_qty, 2.0);
        } else {
            assert_eq!(out, FillOutcome::NO_FILL);
        }
    }
}

#[test]
fn sell_limit_crosses_when_high_touches_or_exceeds_and_fills_at_limit() {
    let cases = [
        (105.0, 105.0, true),
        (106.0, 105.0, true),
        (104.99, 105.0, false),
    ];
    for (high, limit, expect_fill) in cases {
        let b = bar(utc(2024, 6, 14, 14, 30, 0), 100.0, high, 95.0, 102.0);
        let out = decide_fill(&limit_order(Action::Sell, 2.0, limit), &b, 50.0);
        assert_eq!(out.filled, expect_fill, "high={high} limit={limit}");
        if expect_fill {
            assert_eq!(out.fill_price, limit);
            assert_eq!(out.fill_qty, -2.0);
        } else {
            assert_eq!(out.fill_qty, 0.0);
            assert_eq!(out.fill_price, 0.0);
        }
    }
}

#[test]
fn non_positive_limit_price_means_market_order() {
    let b = bar(utc(2024, 6, 14, 14, 30, 0), 100.0, 105.0, 95.0, 102.0);
    for lp in [None, Some(0.0), Some(-1.0)] {
        let mut o = order(Action::Buy, 1.0);
        o.order_type = "LMT".to_string();
        o.limit_price = lp;
        let out = decide_fill(&o, &b, 10.0);
        assert!(out.filled, "lp={lp:?}");
        assert_close(out.fill_price, 102.0 * 1.001, 1e-12);
    }
}

#[test]
fn forex_bars_never_fill() {
    let out = decide_fill(
        &order(Action::Buy, 1000.0),
        &fx_bar(utc(2024, 6, 14, 14, 30, 0)),
        0.0,
    );
    assert!(!out.filled);
    assert_eq!(out.fill_price, 0.0);
    assert_eq!(out.fill_qty, 0.0);
}

#[test]
fn options_bars_use_their_ohlc() {
    let time = utc(2024, 6, 14, 14, 30, 0);
    let b = HistoricalDataFullKeys::Options(HistoricalOptionsDataFullKeys {
        stock: "QQQ".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        expiry: "20240621".to_string(),
        strike: 400.0,
        multiplier: "100".to_string(),
        option_type: trading_app::database::models::OptionType::Call,
        time,
        open: 5.0,
        high: 6.0,
        low: 4.0,
        close: 5.5,
        volume: Decimal::new(10, 0),
    });
    let out = decide_fill(&order(Action::Buy, 1.0), &b, 0.0);
    assert!(out.filled);
    assert_eq!(out.fill_price, 5.5);
}

#[test]
fn zero_quantity_is_a_zero_quantity_fill() {
    let b = bar(utc(2024, 6, 14, 14, 30, 0), 100.0, 105.0, 95.0, 102.0);
    let out = decide_fill(&order(Action::Buy, 0.0), &b, 10.0);
    assert!(out.filled);
    assert_eq!(out.fill_qty, 0.0);
}

// ── CommissionModel / commission ─────────────────────────────────────────

#[test]
fn commission_model_rates_and_parsing() {
    assert_eq!(CommissionModel::Fixed.per_share(), 0.005);
    assert_eq!(CommissionModel::Tiered.per_share(), 0.0035);
    assert_eq!(CommissionModel::Fixed.min_per_order(), 1.00);
    assert_eq!(CommissionModel::Tiered.min_per_order(), 0.35);

    for (input, expected) in [
        ("fixed", CommissionModel::Fixed),
        ("FIXED", CommissionModel::Fixed),
        ("tiered", CommissionModel::Tiered),
        ("TiErEd", CommissionModel::Tiered),
    ] {
        assert_eq!(
            CommissionModel::from_str(input).unwrap(),
            expected,
            "input {input}"
        );
    }
    assert_eq!(
        CommissionModel::from_str("vip").unwrap_err(),
        "unknown commission model 'vip' (expected 'fixed' or 'tiered')"
    );
    assert!(CommissionModel::from_str(" fixed ").is_err());
}

#[test]
fn commission_matches_ibkr_fixed_and_tiered_tables() {
    // (qty, price, model, expected total)
    let cases: &[(f64, f64, CommissionModel, f64)] = &[
        // Fixed buys: max(0.005*qty, 1.00) + CAT; 1% cap binds for penny lots.
        (100.0, 100.0, CommissionModel::Fixed, 1.0003),
        (1_000.0, 100.0, CommissionModel::Fixed, 5.003),
        (10.0, 100.0, CommissionModel::Fixed, 1.00003),
        (1.0, 1.0, CommissionModel::Fixed, 0.010003),
        // Fixed sells add SEC (2.06 bps of proceeds) + FINRA TAF (0.000195/sh).
        (-100.0, 100.0, CommissionModel::Fixed, 1.2258),
        // TAF cap 9.79 binds on a $1M notional sell.
        (-1_000_000.0, 1.0, CommissionModel::Fixed, 5_033.39),
        // Tiered buys: 0.35 min + CAT + NSCC/DTC + NYSE/FINRA pass-through.
        (100.0, 100.0, CommissionModel::Tiered, 0.37055725),
        (10.0, 100.0, CommissionModel::Tiered, 0.35228725),
        // Tiered sell also pays SEC + TAF.
        (-100.0, 100.0, CommissionModel::Tiered, 0.59605725),
        // Zero quantity never pays a minimum.
        (0.0, 100.0, CommissionModel::Tiered, 0.0),
        (0.0, 100.0, CommissionModel::Fixed, 0.0),
    ];
    for (qty, px, model, expected) in cases {
        let got = commission(*qty, *px, *model);
        assert_close(got, *expected, 1e-12);
    }
}

#[test]
fn commission_is_symmetric_in_sign_except_for_sell_only_fees() {
    let buy = commission(100.0, 50.0, CommissionModel::Tiered);
    let sell = commission(-100.0, 50.0, CommissionModel::Tiered);
    assert!(sell > buy, "sell={sell} buy={buy}");
    // The difference is exactly SEC + TAF at $50.
    let expected_delta = 0.0000206 * 100.0 * 50.0 + 0.000195 * 100.0;
    assert_close(sell - buy, expected_delta, 1e-12);
}

#[test]
fn commission_cap_uses_absolute_notional() {
    // 10 shares at $0.10 -> 1% of $1 = $0.01 beats the $0.35 minimum.
    let got = commission(10.0, 0.10, CommissionModel::Tiered);
    let expected = 0.01 + 0.000003 * 10.0 + 0.00020 * 10.0 + (0.000175 + 0.00056) * 0.01;
    assert_close(got, expected, 1e-12);
}

// ── FillModel implementations ─────────────────────────────────────────────

#[test]
fn market_fill_model_applies_directional_slippage() {
    let b = bar(utc(2024, 6, 14, 14, 30, 0), 100.0, 105.0, 95.0, 102.0);
    let mut buy = MarketFillModel {
        slippage_bps: 10.0,
        is_buy: true,
    };
    let mut sell = MarketFillModel {
        slippage_bps: 10.0,
        is_buy: false,
    };
    assert_close(eval_price(buy.evaluate(&b)).unwrap(), 102.0 * 1.001, 1e-12);
    assert_close(eval_price(sell.evaluate(&b)).unwrap(), 102.0 * 0.999, 1e-12);
    assert!(buy.is_immediate());
    assert!(sell.is_immediate());
}

#[test]
fn midprice_fill_model_fills_at_bar_midpoint_immediately() {
    let b = bar(utc(2024, 6, 14, 14, 30, 0), 100.0, 105.0, 95.0, 102.0);
    let mut m = MidPriceFillModel;
    assert_close(eval_price(m.evaluate(&b)).unwrap(), 100.0, 0.0);
    assert!(m.is_immediate());
}

#[test]
fn limit_fill_model_rests_until_crossed_then_fills_at_limit() {
    let time = utc(2024, 6, 14, 14, 30, 0);
    let above = bar(time, 100.0, 105.0, 101.0, 102.0);
    let through = bar(time, 100.0, 105.0, 99.0, 102.0);

    let mut buy = LimitFillModel {
        limit: 100.0,
        is_buy: true,
    };
    assert_eq!(buy.evaluate(&above), FillEval::Rest);
    assert_close(eval_price(buy.evaluate(&through)).unwrap(), 100.0, 0.0);
    assert!(!buy.is_immediate());
}

#[test]
fn optimistic_limit_fills_at_open_when_the_bar_gaps_through() {
    let time = utc(2024, 6, 14, 14, 30, 0);
    let mut buy = OptimisticLimitFillModel {
        limit: 100.0,
        is_buy: true,
    };
    // Gap down through the buy limit -> better fill at the open.
    assert_close(
        eval_price(buy.evaluate(&bar(time, 95.0, 99.0, 90.0, 92.0))).unwrap(),
        95.0,
        0.0,
    );
    // Opens above the limit, trades through -> limit price.
    assert_close(
        eval_price(buy.evaluate(&bar(time, 101.0, 103.0, 99.0, 100.0))).unwrap(),
        100.0,
        0.0,
    );
    // No cross -> rest.
    assert_eq!(
        buy.evaluate(&bar(time, 105.0, 108.0, 101.0, 104.0)),
        FillEval::Rest
    );

    let mut sell = OptimisticLimitFillModel {
        limit: 100.0,
        is_buy: false,
    };
    // Gap up through the sell limit -> open.
    assert_close(
        eval_price(sell.evaluate(&bar(time, 105.0, 110.0, 104.0, 108.0))).unwrap(),
        105.0,
        0.0,
    );
    // Opens below, trades up -> limit.
    assert_close(
        eval_price(sell.evaluate(&bar(time, 99.0, 101.0, 98.0, 100.5))).unwrap(),
        100.0,
        0.0,
    );
    assert_eq!(
        sell.evaluate(&bar(time, 95.0, 99.0, 90.0, 94.0)),
        FillEval::Rest
    );
    assert!(!sell.is_immediate());
}

#[test]
fn stop_market_fills_on_breach_with_adverse_pct_of_bar_slippage() {
    let time = utc(2024, 6, 14, 14, 30, 0);
    let breached = bar(time, 100.0, 106.0, 94.0, 105.0);
    let not_breached = bar(time, 100.0, 104.0, 96.0, 100.0);

    let mut buy = StopMarketFillModel {
        stop: 105.0,
        pct_of_bar: 0.5,
        is_buy: true,
    };
    assert_eq!(buy.evaluate(&not_breached), FillEval::Rest);
    assert_close(
        eval_price(buy.evaluate(&breached)).unwrap(),
        105.0 + 0.5 * (106.0 - 94.0),
        0.0,
    );

    let mut sell = StopMarketFillModel {
        stop: 95.0,
        pct_of_bar: 0.5,
        is_buy: false,
    };
    assert_eq!(sell.evaluate(&not_breached), FillEval::Rest);
    assert_close(
        eval_price(sell.evaluate(&breached)).unwrap(),
        95.0 - 0.5 * (106.0 - 94.0),
        0.0,
    );
}

#[test]
fn stop_limit_requires_trigger_then_one_full_rest_bar_then_a_cross() {
    let time = utc(2024, 6, 14, 14, 30, 0);
    let mut m = StopLimitFillModel {
        stop: 105.0,
        limit: 100.0,
        is_buy: true,
        triggered: false,
        rest_bars: 0,
    };
    // Bar 1: high == stop exactly -> strict `>` is NOT a breach.
    assert_eq!(
        m.evaluate(&bar(time, 100.0, 105.0, 99.0, 104.0)),
        FillEval::Rest
    );
    assert!(!m.triggered);
    // Bar 2: strict breach -> trigger; mandatory rest.
    assert_eq!(
        m.evaluate(&bar(time, 100.0, 106.0, 99.0, 105.5)),
        FillEval::Rest
    );
    assert!(m.triggered);
    // Bar 3: still resting (one full bar).
    assert_eq!(
        m.evaluate(&bar(time, 100.0, 106.0, 91.0, 101.0)),
        FillEval::Rest
    );
    // Bar 4: crosses the limit -> fill at limit.
    assert_close(
        eval_price(m.evaluate(&bar(time, 100.0, 106.0, 99.0, 100.5))).unwrap(),
        100.0,
        0.0,
    );
}

#[test]
fn stop_limit_rests_after_trigger_until_a_cross_even_with_gaps() {
    let time = utc(2024, 6, 14, 14, 30, 0);
    let mut m = StopLimitFillModel {
        stop: 95.0,
        limit: 97.0,
        is_buy: false,
        triggered: false,
        rest_bars: 0,
    };
    // Sell stop breaches on low < 95.
    assert_eq!(
        m.evaluate(&bar(time, 100.0, 101.0, 94.0, 95.0)),
        FillEval::Rest
    );
    assert!(m.triggered);
    // Mandatory rest bar (even though high already crosses).
    assert_eq!(
        m.evaluate(&bar(time, 100.0, 110.0, 96.0, 108.0)),
        FillEval::Rest
    );
    // Then a crossing bar fills at the limit.
    assert_close(
        eval_price(m.evaluate(&bar(time, 100.0, 110.0, 96.0, 108.0))).unwrap(),
        97.0,
        0.0,
    );
}

#[test]
fn clone_box_preserves_fill_model_behaviour() {
    let time = utc(2024, 6, 14, 14, 30, 0);
    let b = bar(time, 100.0, 105.0, 99.0, 102.0);
    let original: Box<dyn FillModel> = Box::new(LimitFillModel {
        limit: 100.0,
        is_buy: true,
    });
    let mut cloned = original.clone();
    assert_close(eval_price(cloned.evaluate(&b)).unwrap(), 100.0, 0.0);
}

// ── fill_model_for dispatch ───────────────────────────────────────────────

#[test]
fn fill_model_for_dispatches_by_order_type_and_limit() {
    // LMT with a positive limit -> resting limit model.
    let m = fill_model_for(&limit_order(Action::Buy, 1.0, 100.0), 5.0, 0.5).expect("LMT model");
    assert!(!m.is_immediate());

    // LMT without a limit -> not price-gated.
    let mut o = order(Action::Buy, 1.0);
    o.order_type = "LMT".to_string();
    assert!(fill_model_for(&o, 5.0, 0.5).is_none());

    // STP with aux only -> market stop (0.5 pct-of-bar), rests.
    let mut o = order(Action::Sell, 1.0);
    o.order_type = "STP".to_string();
    o.aux_price = Some(95.0);
    let mut m = fill_model_for(&o, 5.0, 0.5).expect("STP model");
    let b = bar(utc(2024, 6, 14, 14, 30, 0), 100.0, 106.0, 94.0, 104.0);
    assert_close(eval_price(m.evaluate(&b)).unwrap(), 95.0 - 0.5 * 12.0, 0.0);

    // STP + limit -> stop-limit (left alone until triggered).
    let mut o = order(Action::Sell, 1.0);
    o.order_type = "STP".to_string();
    o.aux_price = Some(95.0);
    o.limit_price = Some(94.0);
    let mut m = fill_model_for(&o, 5.0, 0.5).expect("STP LMT model");
    assert_eq!(
        m.evaluate(&bar(utc(2024, 6, 14, 14, 30, 0), 100.0, 100.0, 99.0, 99.5)),
        FillEval::Rest
    );

    // Market / midprice / midclose orders are caller-handled.
    for ty in ["MKT", "MIDPRICE", "MIDCLOSE", ""] {
        let o = Order {
            order_type: ty.to_string(),
            ..order(Action::Buy, 1.0)
        };
        assert!(fill_model_for(&o, 5.0, 0.5).is_none(), "order_type {ty}");
    }
}

#[test]
fn fill_model_for_uses_aux_price_default_zero_when_missing() {
    let mut o = order(Action::Buy, 1.0);
    o.order_type = "STP".to_string();
    // aux_price None -> stop 0.0. A buy stop breaches whenever high >= 0, so
    // every bar fills at 0 in the market-stop model (documented fallback, not
    // a live-order shape; a sell stop at 0 would never breach).
    let mut m = fill_model_for(&o, 0.0, 0.0).expect("STP model");
    let b = bar(utc(2024, 6, 14, 14, 30, 0), 100.0, 101.0, 99.0, 100.0);
    assert_close(eval_price(m.evaluate(&b)).unwrap(), 0.0, 0.0);
}

#[test]
fn fill_model_for_stop_limit_ignores_zero_or_negative_limit() {
    let mut o = order(Action::Sell, 1.0);
    o.order_type = "STP".to_string();
    o.aux_price = Some(95.0);
    o.limit_price = Some(0.0);
    let mut m = fill_model_for(&o, 0.0, 0.0).expect("market stop");
    // Market-stop semantics: breach at the low, fill at the stop itself.
    assert_close(
        eval_price(m.evaluate(&bar(utc(2024, 6, 14, 14, 30, 0), 100.0, 101.0, 94.0, 95.0)))
            .unwrap(),
        95.0,
        0.0,
    );
}

// ── synthetic_execution ───────────────────────────────────────────────────

#[test]
fn synthetic_execution_mirrors_prod_execution_conventions() {
    let contract = stock("QQQ");
    let at = utc(2024, 6, 14, 20, 30, 0); // 16:30 US/Eastern (EDT)
    let data = synthetic_execution(7, &contract, &Action::Buy, 10.5, 412.34, at, "noise");

    assert_eq!(data.execution.order_id, 7);
    assert_eq!(data.execution.execution_id, "bt-7");
    assert_eq!(data.execution.time, "20240614 16:30:00");
    assert_eq!(data.execution.exchange, "SMART");
    assert_eq!(data.execution.side, "BOT");
    assert_eq!(data.execution.shares, 10.5);
    assert_eq!(data.execution.price, 412.34);
    assert_eq!(data.execution.order_reference, "noise");
    assert_eq!(data.execution.cumulative_quantity, 10.5);
    assert_eq!(data.execution.average_price, 412.34);
    assert_eq!(data.execution.account_number, "backtest");
    assert_eq!(data.execution.client_id, 0);
    assert_eq!(data.execution.perm_id, 0);
    assert_eq!(data.execution.liquidation, 0);
    assert_eq!(data.execution.ev_rule, "");
    assert!(data.execution.ev_multiplier.is_none());
    assert_eq!(data.execution.model_code, "");
    assert!(!data.execution.pending_price_revision);
    assert_eq!(data.execution.submitter, "");
    assert_eq!(data.contract.symbol.to_string(), "QQQ");
}

#[test]
fn synthetic_execution_sell_sides_are_all_sld_and_qty_is_absolute() {
    for action in [Action::Sell, Action::SellShort, Action::SellLong] {
        let data = synthetic_execution(
            1,
            &stock("QQQ"),
            &action,
            -3.0,
            100.0,
            utc(2024, 6, 14, 14, 30, 0),
            "s",
        );
        assert_eq!(data.execution.side, "SLD", "action {action:?}");
        assert_eq!(data.execution.shares, 3.0);
        assert_eq!(data.execution.cumulative_quantity, 3.0);
    }
}

#[test]
fn synthetic_execution_uses_new_york_wall_time_across_dst_and_midnight() {
    // Winter: 14:31 UTC = 09:31 EST.
    let winter = synthetic_execution(
        1,
        &stock("QQQ"),
        &Action::Buy,
        1.0,
        1.0,
        utc(2024, 1, 15, 14, 31, 0),
        "s",
    );
    assert_eq!(winter.execution.time, "20240115 09:31:00");

    // Summer: 20:30 UTC = 16:30 EDT.
    let summer = synthetic_execution(
        1,
        &stock("QQQ"),
        &Action::Buy,
        1.0,
        1.0,
        utc(2024, 6, 14, 20, 30, 0),
        "s",
    );
    assert_eq!(summer.execution.time, "20240614 16:30:00");

    // Crossing UTC midnight rolls the ET date back one day.
    let midnight = synthetic_execution(
        1,
        &stock("QQQ"),
        &Action::Buy,
        1.0,
        1.0,
        utc(2024, 6, 15, 3, 59, 0),
        "s",
    );
    assert_eq!(midnight.execution.time, "20240614 23:59:00");
}

#[test]
fn synthetic_execution_escapes_no_fields_and_keeps_contract_identity() {
    let mut c = stock("AAPL");
    c.exchange = "ARCA".into();
    let data = synthetic_execution(
        42,
        &c,
        &Action::Buy,
        1.0,
        1.0,
        utc(2024, 6, 14, 14, 30, 0),
        "relative",
    );
    assert_eq!(data.execution.exchange, "ARCA");
    assert_eq!(data.execution.execution_id, "bt-42");
    assert_eq!(data.execution.order_reference, "relative");
    // request_id default (no historical stream in backtest).
    assert_eq!(data.request_id, 0);
}

#[test]
fn synthetic_execution_keeps_float_precision() {
    let data = synthetic_execution(
        1,
        &stock("QQQ"),
        &Action::Buy,
        0.000_001,
        123.456_789_012_345,
        utc(2024, 6, 14, 14, 30, 0),
        "s",
    );
    assert_eq!(data.execution.shares, 0.000_001);
    assert_eq!(data.execution.price, 123.456_789_012_345);
    assert_eq!(data.execution.average_price, 123.456_789_012_345);
}

#[test]
fn synthetic_execution_accepts_forex_contract_identity() {
    let data = synthetic_execution(
        3,
        &forex("USD", "SGD"),
        &Action::Sell,
        -1000.0,
        1.2713,
        utc(2024, 6, 14, 14, 30, 0),
        "fx",
    );
    assert_eq!(data.execution.side, "SLD");
    assert_eq!(data.execution.exchange, "IDEALPRO");
    assert_eq!(data.contract.symbol.to_string(), "USD");
    assert_eq!(data.contract.currency.to_string(), "SGD");
}

/// Keep `DateTime`/`Decimal` imports honest for the fixtures above.
#[allow(dead_code)]
fn _type_markers(t: chrono::DateTime<Utc>, d: Decimal) -> (chrono::DateTime<Utc>, Decimal) {
    (t, d)
}

#[allow(dead_code)]
fn _tz_marker() {
    let _ = Utc.timestamp_opt(0, 0);
}
