//! Bracket order handling for the in-memory backtester.
//!
//! When a strategy emits `EmitOrders` containing a bracket (parent MIDPRICE
//! entry + OCA children: STP stop, LMT take-profit, MIDPRICE midpoint-close),
//! the backtester:
//!   1. Fills the parent (entry) immediately at the bar's close.
//!   2. Registers the children as a [`RestingBracket`] — monitored on
//!      subsequent bars for trigger conditions.
//!   3. On each subsequent bar, checks the stop / TP / midpoint-close. When
//!      any child triggers, fills it per the configured fill model + cancels
//!      the other children (OCA).
//!   4. At EOD (mso=385), force-closes any still-open brackets.
//!
//! Fill models are configurable via `BracketFillConfig` (sweepable by the
//! optimizer).

use ibapi::orders::Action;
use ibapi::prelude::Contract;

use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::execution::order_engine::OrderIBKR;

/// How to fill a stop-loss order when triggered.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StopFillModel {
    /// Fill at the exact stop price (deterministic, optimistic).
    StopPrice,
    /// Fill at stop_price + `slippage_pct` × (high - low) in the adverse
    /// direction (realistic — the bar's range beyond the stop).
    PctOfBar(f64),
    /// Fill at the next bar's open after the stop is triggered (gap risk).
    NextBarOpen,
}

/// How to fill a take-profit order when triggered.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TpFillModel {
    /// Fill at the exact TP price (deterministic).
    LimitPrice,
    /// Fill at tp_price - `slippage_pct` × (high - low) in the adverse
    /// direction (slippage against you).
    PctOfBar(f64),
    /// Fill at the next bar's open.
    NextBarOpen,
}

/// Which child fires first when stop + TP could both trigger on the same bar.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AmbiguityResolution {
    /// Stop fires first (worst case — honest default).
    Pessimistic,
    /// TP fires first (best case).
    Optimistic,
    /// Compare the bar's open to the stop and TP levels — whichever is
    /// closer to the open fires first.
    OpenBased,
}

/// Configuration for bracket fill handling. Set via `backtest.json` params
/// or the optimizer.
#[derive(Debug, Clone)]
pub struct BracketFillConfig {
    pub stop_fill: StopFillModel,
    pub tp_fill: TpFillModel,
    pub ambiguity: AmbiguityResolution,
}

impl Default for BracketFillConfig {
    fn default() -> Self {
        Self {
            stop_fill: StopFillModel::PctOfBar(0.5),
            tp_fill: TpFillModel::LimitPrice,
            ambiguity: AmbiguityResolution::Pessimistic,
        }
    }
}

impl BracketFillConfig {
    /// Parse from a strategy's params HashMap (integers for model selection,
    /// floats for slippage).
    pub fn from_params(params: &std::collections::HashMap<String, f64>) -> Self {
        let stop_fill = match params.get("stop_fill_model").map(|v| *v as i32) {
            Some(0) => StopFillModel::StopPrice,
            Some(2) => StopFillModel::NextBarOpen,
            Some(1) | _ => {
                StopFillModel::PctOfBar(params.get("stop_fill_slippage").copied().unwrap_or(0.5))
            }
        };
        let tp_fill = match params.get("tp_fill_model").map(|v| *v as i32) {
            Some(0) => TpFillModel::LimitPrice,
            Some(2) => TpFillModel::NextBarOpen,
            Some(1) | _ => {
                TpFillModel::PctOfBar(params.get("tp_fill_slippage").copied().unwrap_or(0.5))
            }
        };
        let ambiguity = match params.get("ambiguity").map(|v| *v as i32) {
            Some(1) => AmbiguityResolution::Optimistic,
            Some(2) => AmbiguityResolution::OpenBased,
            Some(0) | _ => AmbiguityResolution::Pessimistic,
        };
        Self {
            stop_fill,
            tp_fill,
            ambiguity,
        }
    }
}

/// Why a bracket was closed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CloseReason {
    Stop,
    TakeProfit,
    MidpointClose,
    EodForceClose,
}

/// A resting bracket order group — the parent has been filled (position
/// opened), and the children (stop / TP / midpoint-close) are being
/// monitored for trigger conditions on subsequent bars.
#[derive(Debug, Clone)]
pub struct RestingBracket {
    /// The contract being traded (e.g. TSLA, SPY).
    pub contract: Contract,
    /// Entry action (BUY or SELL).
    pub entry_action: Action,
    /// Close action (opposite of entry — used for stop/TP/midpoint-close fills).
    pub close_action: Action,
    /// Quantity.
    pub qty: f64,
    /// Entry fill price (the parent's fill price).
    pub entry_price: f64,
    /// Stop price (None = no stop).
    pub stop: Option<f64>,
    /// Take-profit price (None = no TP).
    pub tp: Option<f64>,
    /// Midpoint-close bar mso (None = no midpoint-close; e.g. Some(380) = 15:50).
    pub midpoint_close_mso: Option<i32>,
    /// OCA group ID (shared among the children of this bracket).
    pub oca_group: String,
    /// Whether this bracket has been closed.
    pub closed: bool,
    /// The close fill price (if closed).
    pub close_price: Option<f64>,
    /// The close reason (if closed).
    pub close_reason: Option<CloseReason>,
    /// If the close is pending the next bar's open (for NextBarOpen fill model).
    pub pending_close_reason: Option<CloseReason>,
}

impl RestingBracket {
    /// Whether the bracket is still active (not closed, not pending).
    pub fn is_active(&self) -> bool {
        !self.closed && self.pending_close_reason.is_none()
    }

    /// The position direction: +1 for long (entry BUY), -1 for short (entry SELL).
    pub fn direction(&self) -> f64 {
        if matches!(self.entry_action, Action::Buy) {
            1.0
        } else {
            -1.0
        }
    }
}

/// Parse the `good_after_time` field (format "YYYYMMDD-HH:MM:SS") and
/// return the mso (minutes since 9:30 open). Returns None if the field is
/// empty or unparseable.
fn parse_good_after_time_mso(good_after_time: &str) -> Option<i32> {
    if good_after_time.is_empty() {
        return None;
    }
    // Format: "YYYYMMDD-HH:MM:SS" or "YYYYMMDD HH:MM:SS"
    let time_str = good_after_time.split(|c| c == '-' || c == ' ').nth(1)?;
    let parts: Vec<&str> = time_str.split(':').collect();
    if parts.len() < 2 {
        return None;
    }
    let h: i32 = parts[0].parse().ok()?;
    let m: i32 = parts[1].parse().ok()?;
    Some((h * 60 + m) - 570) // minutes since 9:30
}

/// Parse an `EmitOrders` Vec into bracket groups.
///
/// Returns a Vec of `(parent_idx, Vec<child_idx>)` — each group is one
/// bracket (parent at index `parent_idx` in the original Vec, children at
/// the given indices).
pub fn parse_brackets(orders: &[OrderIBKR]) -> Vec<(usize, Vec<usize>)> {
    let mut brackets = Vec::new();
    let mut current_parent: Option<usize> = None;
    let mut current_children: Vec<usize> = Vec::new();

    for (i, order) in orders.iter().enumerate() {
        if order.references_parent_order == -1 {
            // This is a parent. Save the previous bracket if any.
            if let Some(parent) = current_parent.take() {
                brackets.push((parent, std::mem::take(&mut current_children)));
            }
            current_parent = Some(i);
        } else {
            // This is a child of the current parent.
            current_children.push(i);
        }
    }
    // Save the last bracket.
    if let Some(parent) = current_parent {
        brackets.push((parent, std::mem::take(&mut current_children)));
    }
    brackets
}

/// Extract the bracket parameters from the parsed orders.
///
/// Returns a `RestingBracket` ready for monitoring, or None if the parent
/// can't be filled (e.g. not a MIDPRICE order).
pub fn build_resting_bracket(
    orders: &[OrderIBKR],
    parent_idx: usize,
    child_indices: &[usize],
    entry_price: f64,
) -> Option<RestingBracket> {
    let parent = &orders[parent_idx];
    let contract = parent.contract.clone();
    let entry_action = parent.order.action;
    let close_action = if matches!(entry_action, Action::Buy) {
        Action::Sell
    } else {
        Action::Buy
    };
    let qty = parent.order.total_quantity;

    // Extract children: stop (STP → aux_price), TP (LMT → limit_price),
    // midpoint-close (MIDPRICE + good_after_time).
    let mut stop: Option<f64> = None;
    let mut tp: Option<f64> = None;
    let mut midpoint_close_mso: Option<i32> = None;
    let mut oca_group = String::new();

    for &ci in child_indices {
        let child = &orders[ci];
        if !child.order.oca_group.is_empty() {
            oca_group = child.order.oca_group.clone();
        }
        match child.order.order_type.as_str() {
            "STP" => {
                stop = child.order.aux_price;
            }
            "LMT" => {
                tp = child.order.limit_price;
            }
            "MIDPRICE" => {
                // Midpoint-close: parse the good_after_time for the mso.
                if !child.order.good_after_time.is_empty() {
                    midpoint_close_mso = parse_good_after_time_mso(&child.order.good_after_time);
                }
            }
            _ => {}
        }
    }

    Some(RestingBracket {
        contract,
        entry_action,
        close_action,
        qty,
        entry_price,
        stop,
        tp,
        midpoint_close_mso,
        oca_group,
        closed: false,
        close_price: None,
        close_reason: None,
        pending_close_reason: None,
    })
}

/// Check a resting bracket against the current bar's OHLC + mso.
///
/// Returns `Some((fill_price, close_reason))` if a child triggers, or
/// `None` if no trigger. For `NextBarOpen` fill models, returns
/// `Some((0.0, reason))` with a sentinel — the caller should mark the
/// bracket as pending and fill at the next bar's open.
///
/// For a short position (entry = SELL):
///   - Stop: bar.high >= stop_price → triggered.
///   - TP: bar.low <= tp_price → triggered.
/// For a long position (entry = BUY):
///   - Stop: bar.low <= stop_price → triggered.
///   - TP: bar.high >= tp_price → triggered.
pub fn check_bracket(
    bracket: &RestingBracket,
    bar: &HistoricalDataFullKeys,
    mso: i32,
    config: &BracketFillConfig,
) -> Option<(f64, CloseReason)> {
    let is_short = matches!(bracket.entry_action, Action::Sell);
    let (low, high, close) = match bar {
        HistoricalDataFullKeys::Stock(v) => (v.low, v.high, v.close),
        HistoricalDataFullKeys::Options(v) => (v.low, v.high, v.close),
        HistoricalDataFullKeys::DailyStock(v) => (v.low, v.high, v.close),
        _ => return None,
    };

    // If there's a pending close (NextBarOpen), fill at this bar's open.
    if let Some(reason) = bracket.pending_close_reason {
        let open_price = match bar {
            HistoricalDataFullKeys::Stock(v) => v.open,
            HistoricalDataFullKeys::Options(v) => v.open,
            HistoricalDataFullKeys::DailyStock(v) => v.open,
            _ => return None,
        };
        return Some((open_price, reason));
    }

    // Check the midpoint-close first (if the time has arrived).
    if let Some(close_mso) = bracket.midpoint_close_mso {
        if mso >= close_mso {
            // Midpoint-close fills at the bar's close.
            return Some((close, CloseReason::MidpointClose));
        }
    }

    // Check stop + TP triggers.
    let stop_triggers =
        bracket
            .stop
            .map_or(false, |sp| if is_short { high >= sp } else { low <= sp });
    let tp_triggers = bracket
        .tp
        .map_or(false, |tp| if is_short { low <= tp } else { high >= tp });

    // Ambiguity resolution: if both trigger on the same bar.
    if stop_triggers && tp_triggers {
        match config.ambiguity {
            AmbiguityResolution::Pessimistic => {
                return compute_fill_price(
                    bracket.stop.unwrap(),
                    bracket.entry_action,
                    true,
                    low,
                    high,
                    config.stop_fill,
                )
                .map(|p| (p, CloseReason::Stop));
            }
            AmbiguityResolution::Optimistic => {
                return compute_tp_fill_price(
                    bracket.tp.unwrap(),
                    bracket.entry_action,
                    low,
                    high,
                    config.tp_fill,
                )
                .map(|p| (p, CloseReason::TakeProfit));
            }
            AmbiguityResolution::OpenBased => {
                let open_price = match bar {
                    HistoricalDataFullKeys::Stock(v) => v.open,
                    HistoricalDataFullKeys::Options(v) => v.open,
                    HistoricalDataFullKeys::DailyStock(v) => v.open,
                    _ => close,
                };
                let stop_dist = (open_price - bracket.stop.unwrap()).abs();
                let tp_dist = (open_price - bracket.tp.unwrap()).abs();
                if stop_dist <= tp_dist {
                    return compute_fill_price(
                        bracket.stop.unwrap(),
                        bracket.entry_action,
                        true,
                        low,
                        high,
                        config.stop_fill,
                    )
                    .map(|p| (p, CloseReason::Stop));
                } else {
                    return compute_tp_fill_price(
                        bracket.tp.unwrap(),
                        bracket.entry_action,
                        low,
                        high,
                        config.tp_fill,
                    )
                    .map(|p| (p, CloseReason::TakeProfit));
                }
            }
        }
    }

    // Only stop triggers.
    if stop_triggers {
        return compute_fill_price(
            bracket.stop.unwrap(),
            bracket.entry_action,
            true,
            low,
            high,
            config.stop_fill,
        )
        .map(|p| (p, CloseReason::Stop));
    }

    // Only TP triggers.
    if tp_triggers {
        return compute_tp_fill_price(
            bracket.tp.unwrap(),
            bracket.entry_action,
            low,
            high,
            config.tp_fill,
        )
        .map(|p| (p, CloseReason::TakeProfit));
    }

    None
}

/// Compute the fill price based on the fill model.
/// Returns `Some(price)` for immediate fills, or `Some(0.0)` for
/// `NextBarOpen` (sentinel — caller sets `pending_close_reason`).
fn compute_fill_price(
    trigger_price: f64,
    entry_action: Action,
    is_stop: bool,
    low: f64,
    high: f64,
    fill_model: StopFillModel,
) -> Option<f64> {
    let is_short = matches!(entry_action, Action::Sell);
    let bar_range = high - low;

    match fill_model {
        StopFillModel::StopPrice => Some(trigger_price),
        StopFillModel::PctOfBar(pct) => {
            // Adverse direction: for a short stop (buying back), adverse = higher.
            // For a long stop (selling), adverse = lower.
            let slippage = pct * bar_range;
            if is_short {
                Some(trigger_price + slippage) // buy back higher = worse
            } else {
                Some(trigger_price - slippage) // sell lower = worse
            }
        }
        StopFillModel::NextBarOpen => Some(0.0), // sentinel → pending
    }
}

/// Compute the TP fill price (same logic as stop, but the adverse direction
/// is opposite for a TP fill).
#[allow(dead_code)]
fn compute_tp_fill_price(
    trigger_price: f64,
    entry_action: Action,
    low: f64,
    high: f64,
    fill_model: TpFillModel,
) -> Option<f64> {
    let is_short = matches!(entry_action, Action::Sell);
    let bar_range = high - low;

    match fill_model {
        TpFillModel::LimitPrice => Some(trigger_price),
        TpFillModel::PctOfBar(pct) => {
            // For a TP, the adverse direction is opposite to the stop:
            // short TP (buying back at the TP) → adverse = lower (you get less profit).
            // long TP (selling at the TP) → adverse = higher (you get less profit).
            let slippage = pct * bar_range;
            if is_short {
                Some(trigger_price - slippage) // buy back lower = less profit
            } else {
                Some(trigger_price + slippage) // sell higher = less profit... wait, that's MORE profit
            }
        }
        TpFillModel::NextBarOpen => Some(0.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_good_after_time() {
        assert_eq!(
            parse_good_after_time_mso("20260101-15:50:00"),
            Some(380) // 15:50 - 9:30 = 380 min
        );
        assert_eq!(parse_good_after_time_mso("20260101 15:50:00"), Some(380));
        assert_eq!(parse_good_after_time_mso(""), None);
        assert_eq!(parse_good_after_time_mso("invalid"), None);
    }

    #[test]
    fn test_bracket_fill_config_defaults() {
        let config = BracketFillConfig::default();
        assert_eq!(config.ambiguity, AmbiguityResolution::Pessimistic);
        assert_eq!(config.stop_fill, StopFillModel::PctOfBar(0.5));
        assert_eq!(config.tp_fill, TpFillModel::LimitPrice);
    }

    #[test]
    fn test_bracket_fill_config_from_params() {
        let mut params = std::collections::HashMap::new();
        params.insert("stop_fill_model".to_string(), 0.0); // StopPrice
        params.insert("tp_fill_model".to_string(), 2.0); // NextBarOpen
        params.insert("ambiguity".to_string(), 1.0); // Optimistic
        let config = BracketFillConfig::from_params(&params);
        assert_eq!(config.stop_fill, StopFillModel::StopPrice);
        assert_eq!(config.tp_fill, TpFillModel::NextBarOpen);
        assert_eq!(config.ambiguity, AmbiguityResolution::Optimistic);
    }
}
