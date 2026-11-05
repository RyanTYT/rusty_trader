//! Order-driven bracket handling for the in-memory backtester.
//!
//! A strategy emits `EmitOrders` with a bracket (parent entry + OCA children:
//! stop, take-profit, close). Each emitted order is turned into a
//! [`RestingOrder`] carrying a [`FillModel`] describing HOW it fills:
//!
//!   - market              → fills immediately at close ± slippage
//!   - limit               → rests, fills when the bar crosses the limit (no slip)
//!   - stop-limit          → rests, triggers on breach, rests one more bar, then
//!                            fills only when a later bar crosses the limit (no slip)
//!   - market stop         → fills on breach at stop ± pct-of-bar (orb_general legacy)
//!   - midprice/close      → time-gated: fills at `good_after_time` mso (close or mid)
//!
//! The entry is `active` from the start; its exits (`role = Exit(..)`) stay
//! inactive until the entry fills (the OCA parent/child chain). When any exit
//! fills, its OCA siblings are cancelled. At EOD all still-open orders are
//! cancelled.

use std::sync::atomic::{AtomicUsize, Ordering};

use ibapi::orders::Action;
use ibapi::prelude::Contract;

use crate::backtester::execution::fill_model::{
    FillModel, LimitFillModel, MarketFillModel, OptimisticLimitFillModel, StopLimitFillModel,
    StopMarketFillModel, optimistic_limits,
};
use crate::execution::order_engine::OrderIBKR;

/// Globally-unique bracket id source. `parse_brackets`'s positional index is
/// only unique WITHIN one `EmitOrders` batch (every vp_touch entry is a single
/// bracket, so the index is always 0); entry↔exit coordination and OCA sibling
/// cancellation key on `bracket_id`, so it must be unique across the WHOLE book
/// or one symbol's entry fill arms another symbol's exits.
pub(crate) static NEXT_BRACKET_ID: AtomicUsize = AtomicUsize::new(0);

/// Why a bracket was closed (kept for transaction tagging).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CloseReason {
    Stop,
    TakeProfit,
    MidpointClose,
    EodForceClose,
}

/// What role an order plays within its bracket.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderRole {
    Entry,
    Exit(CloseReason),
}

/// Order lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RestingStatus {
    Resting,
    Filled,
    Cancelled,
}

/// A single open (resting or immediate-fill) order in the book.
#[derive(Clone)]
pub struct RestingOrder {
    pub contract: Contract,
    /// The side this order transacts (Buy or Sell).
    pub action: Action,
    pub qty: f64,
    /// Owning strategy's name (engine account). §8 pooled emission: fills
    /// settle into the OWNING sleeve's account — the reconcile resolves the
    /// settlement state + realized-fill mailbox by this field
    /// (`state::routed_state`).
    pub order_ref: String,
    /// OCA group (siblings to cancel when one exits) — empty for the entry.
    pub oca_group: String,
    /// Links an entry to its exit orders.
    pub bracket_id: usize,
    pub role: OrderRole,
    /// Exits start inactive until the entry fills.
    pub active: bool,
    /// Price-gated fill model. `None` only for the time-gated close.
    pub fill_model: Option<Box<dyn FillModel>>,
    /// Time-gated close trigger mso (`good_after_time`), if this is the close.
    pub close_mso: Option<i32>,
    /// Close fill price: `true` = (high+low)/2 (mid), `false` = bar close.
    pub close_is_mid: bool,
    pub status: RestingStatus,
}

/// Parse the `good_after_time` field (format "YYYYMMDD-HH:MM:SS") → mso.
fn parse_good_after_time_mso(good_after_time: &str) -> Option<i32> {
    if good_after_time.is_empty() {
        return None;
    }
    let time_str = good_after_time.split(|c| c == '-' || c == ' ').nth(1)?;
    let parts: Vec<&str> = time_str.split(':').collect();
    if parts.len() < 2 {
        return None;
    }
    let h: i32 = parts[0].parse().ok()?;
    let m: i32 = parts[1].parse().ok()?;
    Some((h * 60 + m) - 570) // minutes since 9:30
}

/// Parse an `EmitOrders` Vec into bracket groups `(parent_idx, Vec<child_idx>)`.
pub fn parse_brackets(orders: &[OrderIBKR]) -> Vec<(usize, Vec<usize>)> {
    let mut brackets = Vec::new();
    let mut current_parent: Option<usize> = None;
    let mut current_children: Vec<usize> = Vec::new();

    for (i, order) in orders.iter().enumerate() {
        if order.references_parent_order == -1 {
            if let Some(parent) = current_parent.take() {
                brackets.push((parent, std::mem::take(&mut current_children)));
            }
            current_parent = Some(i);
        } else {
            current_children.push(i);
        }
    }
    if let Some(parent) = current_parent {
        brackets.push((parent, std::mem::take(&mut current_children)));
    }
    brackets
}

/// Turn the emitted bracket orders into the book's resting orders.
pub fn build_resting_orders(
    orders: &[OrderIBKR],
    slippage_bps: f64,
    stop_pct_of_bar: f64,
) -> Vec<RestingOrder> {
    let mut out = Vec::new();
    for (parent_idx, child_indices) in parse_brackets(orders) {
        let bracket_id = NEXT_BRACKET_ID.fetch_add(1, Ordering::SeqCst);
        let parent = &orders[parent_idx];
        let contract = parent.contract.clone();
        let entry_action = parent.order.action;
        let close_action = if matches!(entry_action, Action::Buy) {
            Action::Sell
        } else {
            Action::Buy
        };
        let qty = parent.order.total_quantity;

        // ── Entry: limit if `LMT` with a limit price, else market ──
        // Fill semantics are an engine-level choice (LIMIT_FILL=optimistic
        // → tape-set open pricing on gap-throughs, py pending-limits port);
        // strategies emit plain LMT either way, only the model differs.
        let is_buy = matches!(entry_action, Action::Buy);
        let entry_model: Box<dyn FillModel> = if parent.order.order_type == "LMT" {
            match parent.order.limit_price {
                Some(lp) if lp > 0.0 => {
                    if optimistic_limits() {
                        Box::new(OptimisticLimitFillModel { limit: lp, is_buy })
                    } else {
                        Box::new(LimitFillModel { limit: lp, is_buy })
                    }
                }
                _ => Box::new(MarketFillModel { slippage_bps, is_buy }),
            }
        } else {
            Box::new(MarketFillModel { slippage_bps, is_buy })
        };
        out.push(RestingOrder {
            contract: contract.clone(),
            action: entry_action,
            qty,
            order_ref: parent.order.order_ref.clone(),
            oca_group: parent.order.oca_group.clone(),
            bracket_id,
            role: OrderRole::Entry,
            active: true,
            fill_model: Some(entry_model),
            close_mso: None,
            close_is_mid: false,
            status: RestingStatus::Resting,
        });

        // ── Exits (inactive until entry fills) ──
        for ci in child_indices {
            let child = &orders[ci];
            let child_is_buy = matches!(child.order.action, Action::Buy);
            let _ = close_action; // exits are already opposite of entry
            match child.order.order_type.as_str() {
                "STP" => {
                    let stop = child.order.aux_price.unwrap_or(0.0);
                    let model: Box<dyn FillModel> = match child.order.limit_price {
                        // STP with a limit = stop-limit (vp_touch).
                        Some(limit) if limit > 0.0 => {
                            Box::new(StopLimitFillModel { stop, limit, is_buy: child_is_buy, triggered: false, rest_bars: 0 })
                        }
                        // plain STP = market stop (orb_general legacy).
                        _ => Box::new(StopMarketFillModel { stop, pct_of_bar: stop_pct_of_bar, is_buy: child_is_buy }),
                    };
                    out.push(RestingOrder {
                        contract: contract.clone(),
                        action: child.order.action,
                        qty: child.order.total_quantity,
                        order_ref: parent.order.order_ref.clone(),
                        oca_group: child.order.oca_group.clone(),
                        bracket_id,
                        role: OrderRole::Exit(CloseReason::Stop),
                        active: false,
                        fill_model: Some(model),
                        close_mso: None,
                        close_is_mid: false,
                        status: RestingStatus::Resting,
                    });
                }
                "LMT" => {
                    let tp = child.order.limit_price.unwrap_or(0.0);
                    if tp > 0.0 {
                        out.push(RestingOrder {
                            contract: contract.clone(),
                            action: child.order.action,
                            qty: child.order.total_quantity,
                            order_ref: parent.order.order_ref.clone(),
                            oca_group: child.order.oca_group.clone(),
                            bracket_id,
                            role: OrderRole::Exit(CloseReason::TakeProfit),
                            active: false,
                            fill_model: Some(if optimistic_limits() {
                                Box::new(OptimisticLimitFillModel { limit: tp, is_buy: child_is_buy })
                                    as Box<dyn FillModel>
                            } else {
                                Box::new(LimitFillModel { limit: tp, is_buy: child_is_buy })
                                    as Box<dyn FillModel>
                            }),
                            close_mso: None,
                            close_is_mid: false,
                            status: RestingStatus::Resting,
                        });
                    }
                }
                "MIDPRICE" | "MIDCLOSE" => {
                    let close_is_mid = child.order.order_type == "MIDCLOSE";
                    out.push(RestingOrder {
                        contract: contract.clone(),
                        action: child.order.action,
                        qty: child.order.total_quantity,
                        order_ref: parent.order.order_ref.clone(),
                        oca_group: child.order.oca_group.clone(),
                        bracket_id,
                        role: OrderRole::Exit(CloseReason::MidpointClose),
                        active: false,
                        fill_model: None, // time-gated — handled by close_mso
                        close_mso: parse_good_after_time_mso(&child.order.good_after_time),
                        close_is_mid,
                        status: RestingStatus::Resting,
                    });
                }
                _ => {}
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_good_after_time() {
        assert_eq!(parse_good_after_time_mso("20260101-15:45:00"), Some(375));
        assert_eq!(parse_good_after_time_mso("20260101 15:50:00"), Some(380));
        assert_eq!(parse_good_after_time_mso(""), None);
    }
}
