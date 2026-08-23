//! Pure fill-decision logic for the simulated broker. No I/O — unit-testable.
//!
//! Market orders: fill at bar close +/- slippage (bps), quantity signed by
//! direction (+ve buy / -ve sell). Limit orders: fill iff the bar's
//! [low, high] range crosses the limit price, at the limit price. Partial
//! fills are not yet modelled.

use ibapi::orders::Order;

use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;

#[derive(Debug, Clone, PartialEq)]
pub struct FillOutcome {
    pub filled: bool,
    pub fill_price: f64,
    /// Signed quantity: +ve for buys, -ve for sells (matches `Action`).
    pub fill_qty: f64,
}

impl FillOutcome {
    pub const NO_FILL: FillOutcome = FillOutcome {
        filled: false,
        fill_price: 0.0,
        fill_qty: 0.0,
    };
}

/// Extract (low, high, close) from a stock/option/daily bar. Forex bars have
/// bid/ask OHLC, not a single OHLC, so they return None (FX fill modelling is
/// deferred).
fn bar_ohlc(bar: &HistoricalDataFullKeys) -> Option<(f64, f64, f64)> {
    match bar {
        HistoricalDataFullKeys::Stock(v) => Some((v.low, v.high, v.close)),
        HistoricalDataFullKeys::Options(v) => Some((v.low, v.high, v.close)),
        HistoricalDataFullKeys::DailyStock(v) => Some((v.low, v.high, v.close)),
        HistoricalDataFullKeys::Forex(_) => None,
    }
}

/// Decide a fill for `order` given the current `bar` and `slippage_bps`.
///
/// A limit order is detected by a positive `limit_price` (the `market_order`
/// builder leaves it zero); this avoids depending on ibapi's `OrderType` enum
/// representation, which varies across versions.
pub fn decide_fill(order: &Order, bar: &HistoricalDataFullKeys, slippage_bps: f64) -> FillOutcome {
    let is_buy = matches!(order.action, ibapi::orders::Action::Buy);
    let qty = order.total_quantity;
    let (low, high, close) = match bar_ohlc(bar) {
        Some(v) => v,
        None => return FillOutcome::NO_FILL,
    };
    let signed_qty = if is_buy { qty } else { -qty };

    // `limit_price` is `Option<f64>`: Some(p > 0) => limit order; None/0 => market.
    match order.limit_price {
        Some(limit) if limit > 0.0 => {
            // Buy limit fills if low <= limit; sell limit fills if high >= limit.
            let crosses = if is_buy { low <= limit } else { high >= limit };
            if crosses {
                FillOutcome {
                    filled: true,
                    fill_price: limit,
                    fill_qty: signed_qty,
                }
            } else {
                FillOutcome::NO_FILL
            }
        }
        _ => {
            let slip = close * (slippage_bps / 10_000.0);
            let fill_price = if is_buy { close + slip } else { close - slip };
            FillOutcome {
                filled: true,
                fill_price,
                fill_qty: signed_qty,
            }
        }
    }
}

/// IBKR-style commission: per_share * |qty|, with a per-order floor.
pub fn commission(qty: f64, _price: f64, per_share: f64, min_per_order: f64) -> f64 {
    (qty.abs() * per_share).max(min_per_order)
}
