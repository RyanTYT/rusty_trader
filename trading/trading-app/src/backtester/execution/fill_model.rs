//! Pure fill-decision logic for the simulated broker. No I/O — unit-testable.
//!
//! Market orders: fill at bar close +/- slippage (bps), quantity signed by
//! direction (+ve buy / -ve sell). Limit orders: fill iff the bar's
//! [low, high] range crosses the limit price, at the limit price. Partial
//! fills are not yet modelled.

use ibapi::orders::{Action, Order};

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

// ── Order-driven fill models (the limit-order family) ───────────────────────
// A *resting* order has a FillModel that is evaluated against successive bars
// until it fills (Fill{price}) or is cancelled (EOD). Market + Midprice fill
// immediately; Limit rests until crossed; StopLimit triggers, then rests one
// more bar, then fills only when a later bar crosses its limit. Limit family
// fills at the exact limit price — NO slippage (commission is charged on top
// by the caller, but there is no spread/slippage modeled for limit fills).

// /// The OHLC fields a fill model needs from one bar.
// #[derive(Debug, Clone, Copy)]
// pub struct BarOhlc {
//     pub open: f64,
//     pub high: f64,
//     pub low: f64,
//     pub close: f64,
// }
//
// impl BarOhlc {
//     pub fn from_bar(bar: &HistoricalDataFullKeys) -> Option<Self> {
//         match bar {
//             HistoricalDataFullKeys::Stock(v) => Some(BarOhlc {
//                 open: v.open,
//                 high: v.high,
//                 low: v.low,
//                 close: v.close,
//             }),
//             HistoricalDataFullKeys::Options(v) => Some(BarOhlc {
//                 open: v.open,
//                 high: v.high,
//                 low: v.low,
//                 close: v.close,
//             }),
//             HistoricalDataFullKeys::DailyStock(v) => Some(BarOhlc {
//                 open: v.open,
//                 high: v.high,
//                 low: v.low,
//                 close: v.close,
//             }),
//             _ => None,
//         }
//     }
// }

/// Result of advancing a fill model one bar.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FillEval {
    Fill { price: f64 },
    Rest,
}

/// The behavior contract for how a resting order fills.
pub trait FillModel: Send + Sync {
    /// Advance by one bar. `&mut self` lets StopLimit persist its trigger state.
    fn evaluate(&mut self, bar: &HistoricalDataFullKeys) -> FillEval;
    /// Whether this order fills immediately on placement (market/midprice) rather
    /// than resting until crossed (limit/stop). Used to fill market entries on the
    /// signal bar instead of deferring them to the next bar.
    fn is_immediate(&self) -> bool {
        false
    }
    /// Deep-copy this model into a new trait object (for cloning the order book).
    fn clone_box(&self) -> Box<dyn FillModel>;
}

impl Clone for Box<dyn FillModel> {
    fn clone(&self) -> Self {
        (**self).clone_box()
    }
}

/// Market order — fills immediately at the bar close ± `slippage_bps` (the
/// real cost of crossing the spread with a market order).
#[derive(Clone)]
pub struct MarketFillModel {
    pub slippage_bps: f64,
    pub is_buy: bool,
}
impl FillModel for MarketFillModel {
    fn clone_box(&self) -> Box<dyn FillModel> {
        Box::new((*self).clone())
    }
    fn evaluate(&mut self, bar: &HistoricalDataFullKeys) -> FillEval {
        let dir = if self.is_buy { 1.0 } else { -1.0 };
        FillEval::Fill {
            price: bar.get_price() * (1.0 + dir * self.slippage_bps / 1e4),
        }
    }
    fn is_immediate(&self) -> bool {
        true
    }
}

/// Midprice order — fills immediately at (high + low) / 2.
#[derive(Clone)]
pub struct MidPriceFillModel;
impl FillModel for MidPriceFillModel {
    fn clone_box(&self) -> Box<dyn FillModel> {
        Box::new((*self).clone())
    }
    fn evaluate(&mut self, bar: &HistoricalDataFullKeys) -> FillEval {
        FillEval::Fill {
            price: (bar.get_high_price() + bar.get_low_price()) / 2.0,
        }
    }
    fn is_immediate(&self) -> bool {
        true
    }
}

/// Limit order — rests until the bar's range crosses the limit, then fills at
/// the limit price (no slippage).
#[derive(Clone)]
pub struct LimitFillModel {
    pub limit: f64,
    pub is_buy: bool,
}
impl FillModel for LimitFillModel {
    fn clone_box(&self) -> Box<dyn FillModel> {
        Box::new((*self).clone())
    }
    fn evaluate(&mut self, bar: &HistoricalDataFullKeys) -> FillEval {
        let crossed = if self.is_buy {
            bar.get_low_price() <= self.limit
        } else {
            bar.get_high_price() >= self.limit
        };
        if crossed {
            FillEval::Fill { price: self.limit }
        } else {
            FillEval::Rest
        }
    }
}

/// OptimisticLimitFillModel — on cross, fills at open if open is at better price
#[derive(Clone)]
pub struct OptimisticLimitFillModel {
    pub limit: f64,
    pub is_buy: bool,
}
impl FillModel for OptimisticLimitFillModel {
    fn clone_box(&self) -> Box<dyn FillModel> {
        Box::new((*self).clone())
    }
    fn evaluate(&mut self, bar: &HistoricalDataFullKeys) -> FillEval {
        let crossed = if self.is_buy {
            bar.get_low_price() <= self.limit
        } else {
            bar.get_high_price() >= self.limit
        };
        if crossed {
            let price = if self.is_buy {
                if bar.get_open_price() <= self.limit {
                    bar.get_open_price()
                } else {
                    self.limit
                }
            } else {
                if bar.get_open_price() >= self.limit {
                    bar.get_open_price()
                } else {
                    self.limit
                }
            };
            FillEval::Fill { price }
        } else {
            FillEval::Rest
        }
    }
    fn is_immediate(&self) -> bool {
        false
    }
}

/// Engine-level LMT fill semantics, chosen ONCE at initialisation (first
/// `fill_model_for` use reads the seam and caches it). Default = classic
/// at-limit cap (`LimitFillModel`). `LIMIT_FILL=optimistic` selects
/// [`OptimisticLimitFillModel`] for ALL LMT orders run-wide — the
/// tape-set pricing form (an open that gaps through the limit fills at
/// the open, py `thorough.py::_process_pending_limits` port). Strategies
/// are untouched: they always emit LMT as normal; only the fill model
/// that decides how the order is filled changes.
static LIMIT_FILL_OPTIMISTIC: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
pub(crate) fn optimistic_limits() -> bool {
    *LIMIT_FILL_OPTIMISTIC.get_or_init(|| {
        std::env::var("LIMIT_FILL")
            .map(|v| v.eq_ignore_ascii_case("optimistic"))
            .unwrap_or(false)
    })
}

/// Market stop — fills on breach at `stop ± pct_of_bar×(high−low)` (adverse),
/// the pre-existing orb_general model (used when STOP has no limit_price).
#[derive(Clone)]
pub struct StopMarketFillModel {
    pub stop: f64,
    pub pct_of_bar: f64,
    pub is_buy: bool,
}
impl FillModel for StopMarketFillModel {
    fn clone_box(&self) -> Box<dyn FillModel> {
        Box::new((*self).clone())
    }
    fn evaluate(&mut self, bar: &HistoricalDataFullKeys) -> FillEval {
        // A close-buy (is_buy=true) exits a short: stop is ABOVE → breach on high.
        // A close-sell (is_buy=false) exits a long: stop is BELOW → breach on low.
        let breached = if self.is_buy {
            bar.get_high_price() >= self.stop
        } else {
            bar.get_low_price() <= self.stop
        };
        if breached {
            let range = bar.get_high_price() - bar.get_low_price();
            let slip = self.pct_of_bar * range;
            let price = if self.is_buy {
                self.stop + slip // buy back higher = worse
            } else {
                self.stop - slip // sell lower = worse
            };
            FillEval::Fill { price }
        } else {
            FillEval::Rest
        }
    }
}

/// Stop-limit — rests until the stop is breached; then (conservatively) rests
/// one MORE bar before it may fill at the limit, only when a later bar crosses
/// it. Fills at the exact limit price (no slippage); gap-through flows to EOD.
#[derive(Clone)]
pub struct StopLimitFillModel {
    pub stop: f64,
    pub limit: f64,
    pub is_buy: bool,
    pub triggered: bool,
    pub rest_bars: u8,
}
impl FillModel for StopLimitFillModel {
    fn clone_box(&self) -> Box<dyn FillModel> {
        Box::new((*self).clone())
    }
    fn evaluate(&mut self, bar: &HistoricalDataFullKeys) -> FillEval {
        if !self.triggered {
            let breached = if self.is_buy {
                bar.get_high_price() > self.stop
            } else {
                bar.get_low_price() < self.stop
            };
            if breached {
                self.triggered = true;
                self.rest_bars = 1; // mandatorily rest this + the next bar
            }
            return FillEval::Rest;
        }
        // Triggered: conservatively rest, then fill on a later crossing bar.
        if self.rest_bars > 0 {
            self.rest_bars -= 1;
            return FillEval::Rest;
        }
        let crossed = if self.is_buy {
            bar.get_low_price() <= self.limit
        } else {
            bar.get_high_price() >= self.limit
        };
        if crossed {
            FillEval::Fill { price: self.limit }
        } else {
            FillEval::Rest
        }
    }
}

/// Build a price-gated fill model from an emitted order's fields. Returns None
/// for orders that are NOT price-gated (the time-gated close), handled by the
/// caller via `good_after_time`/order_type.
pub fn fill_model_for(
    order: &Order,
    slippage_bps: f64,
    stop_pct_of_bar: f64,
) -> Option<Box<dyn FillModel>> {
    let is_buy = matches!(order.action, Action::Buy);
    match order.order_type.as_str() {
        "LMT" => order.limit_price.filter(|&p| p > 0.0).map(|limit| {
            // Semantics of plain LMT fills are an ENGINE-LEVEL setting,
            // chosen once at initialisation (LIMIT_FILL env; default =
            // touch = the classic at-limit cap). Strategies always emit
            // LMT as normal; the fill model decides how it fills.
            if optimistic_limits() {
                Box::new(OptimisticLimitFillModel { limit, is_buy }) as Box<dyn FillModel>
            } else {
                Box::new(LimitFillModel { limit, is_buy }) as Box<dyn FillModel>
            }
        }),
        "STP" => {
            let stop = order.aux_price.unwrap_or(0.0);
            match order.limit_price.filter(|&p| p > 0.0) {
                // STP with a limit price = stop-limit (vp_touch).
                Some(limit) => Some(Box::new(StopLimitFillModel {
                    stop,
                    limit,
                    is_buy,
                    triggered: false,
                    rest_bars: 0,
                }) as Box<dyn FillModel>),
                // Plain STP = market stop (orb_general — preserved).
                None => Some(Box::new(StopMarketFillModel {
                    stop,
                    pct_of_bar: stop_pct_of_bar,
                    is_buy,
                }) as Box<dyn FillModel>),
            }
        }
        // MIDPRICE / MIDCLOSE / market → time-gated or immediate, handled by caller.
        _ => None,
    }
}
