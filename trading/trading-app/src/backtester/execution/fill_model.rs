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

/// IBKR Pro commission model (US stocks), mirroring the rates cited on IBKR's
/// commissions-stocks page (commissions.html).
///
/// - **Fixed**: $0.005/share, $1.00 min/order, + regulatory fees (SEC, FINRA
///   TAF, FINRA CAT). Exchange/clearing/pass-through fees are included.
/// - **Tiered**: $0.0035/share, $0.35 min/order, + regulatory + clearing
///   (NSCC/DTC) + pass-through (NYSE, FINRA) fees.
///
/// Both plans apply a 1% of trade-value cap on the base commission (per
/// IBKR footnote 7 — binds for sub-dollar stocks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommissionModel {
    Fixed,
    Tiered,
}

impl CommissionModel {
    pub fn per_share(&self) -> f64 {
        match self {
            Self::Fixed => 0.005,
            Self::Tiered => 0.0035,
        }
    }
    pub fn min_per_order(&self) -> f64 {
        match self {
            Self::Fixed => 1.00,
            Self::Tiered => 0.35,
        }
    }
    /// Parse from a string ("fixed" / "tiered"), case-insensitive.
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "fixed" => Ok(Self::Fixed),
            "tiered" => Ok(Self::Tiered),
            _ => Err(format!(
                "unknown commission model '{s}' (expected 'fixed' or 'tiered')"
            )),
        }
    }
}

/// IBKR Pro commission for a single fill, per the chosen `CommissionModel`.
///
/// `qty` is signed (+buy, −sell); `price` is the fill price. Returns the total
/// commission in the contract's currency (e.g., USD for a US stock) — the
/// caller converts to SGD via the FX rate when settling CASH:SGD.
///
/// Rates are from IBKR's commissions-stocks page (FY2025; the SEC fee changes
/// annually each Oct — adjust `SEC_FEE_RATE` when it does).
pub fn commission(qty: f64, price: f64, model: CommissionModel) -> f64 {
    // Regulatory fees (both Fixed + Tiered).
    const SEC_FEE_RATE: f64 = 0.0000206; // $/USD of aggregate sales (sells only).
    const FINRA_TAF_PER_SHARE: f64 = 0.000195; // sells only.
    const FINRA_TAF_CAP: f64 = 9.79; // max per trade.
    const FINRA_CAT_PER_SHARE: f64 = 0.000003; // both buys + sells.
    // Tiered-only: clearing + pass-through.
    const NSCC_DTC_PER_SHARE: f64 = 0.00020; // clearing.
    const NYSE_PASS_THROUGH: f64 = 0.000175; // × commission.
    const FINRA_PASS_THROUGH: f64 = 0.00056; // × commission.
    const TRADE_VALUE_CAP: f64 = 0.01; // 1% of trade value (sub-dollar stocks).

    let trade_value = qty.abs() * price;
    // Base: max(per_share × |qty|, min_per_order), capped at 1% of trade value.
    let base = (qty.abs() * model.per_share())
        .max(model.min_per_order())
        .min(TRADE_VALUE_CAP * trade_value);

    // FINRA Consolidated Audit Trail fee — both buys + sells.
    let mut total = base + FINRA_CAT_PER_SHARE * qty.abs();

    // SEC fee + FINRA TAF — sells only.
    if qty < 0.0 {
        let sec_fee = SEC_FEE_RATE * trade_value;
        let finra_taf = (FINRA_TAF_PER_SHARE * qty.abs()).min(FINRA_TAF_CAP);
        total += sec_fee + finra_taf;
    }

    // Tiered-only: clearing + pass-through (Fixed includes these in the base).
    if matches!(model, CommissionModel::Tiered) {
        let nscc_dtc = NSCC_DTC_PER_SHARE * qty.abs();
        let nyse_pass = NYSE_PASS_THROUGH * base;
        let finra_pass = FINRA_PASS_THROUGH * base;
        total += nscc_dtc + nyse_pass + finra_pass;
    }

    total
}
