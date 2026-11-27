//! Payload types of the wrapper chains (v2).
//!
//! Split out of the old monolith `mod.rs` (restructure 2026-10-25): `BarFrame`
//! is the app-native bar frame, `Payload` the mid-chain emission, and
//! `RealisedFill` one engine-cleared execution routed down the chain by the
//! fund's `on_fill` — there is NO inferred-fill machinery anymore.

use std::collections::BTreeMap;

use super::intent::IntentMap;

use chrono::NaiveDate;

pub use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;

/// Per-symbol printable-bar frame for one tick — the NATIVE app bar type
/// (`StrategyExecutor.on_bar_update` already consumes it; zero new bar code).
/// `BTreeMap` for deterministic iteration (parity-risk ledger #1).
pub type BarFrame = BTreeMap<String, HistoricalDataFullKeys>;

/// Post-`SharesGate` emission entry: symbol, signed quantity, attributed
/// owner. `Some(name)` = pooled cross-sleeve attribution (§8) — the intent
/// carried `strategy: Some(name)` and rides verbatim to `order_ref` at the
/// sleeve's order assembly. `None` = the owning sleeve (filled with its own
/// name there — `SharesGate` is name-free by contract).
///
/// The last two fields carry the OrderGate hybrid machinery (§4 rollout —
/// Section 2 widening, inert until the OrderGate exists):
/// - `limit_px`: `Some(px)` = rest as a passive LMT at `px` (OrderGate
///   hybrid mode); `None` = plain MKT.
/// - `oca_group`: the OrderGate emit descriptor (`og-…`), cancel marker
///   (`ogcancel-…`), or engine OCA group; `None` = no OCA.
#[derive(Debug, Clone, PartialEq)]
pub struct ShareOut {
    /// Instrument symbol.
    pub sym: String,
    /// Signed target quantity: positive long, negative short, 0 flatten.
    pub qty: i64,
    /// `Some(name)` = pooled cross-sleeve emission owned by `name`; `None` =
    /// the sleeve that owns the chain and fills its own name at order assembly.
    pub owner: Option<String>,
    /// `Some(px)` = rest passively at `px`; `None` = market. Ignored by size
    /// arithmetic.
    pub limit_px: Option<f64>,
    /// OrderGate descriptor/cancel marker or engine OCA group; `None` = none.
    pub oca_group: Option<String>,
}

impl ShareOut {
    /// Build one emission entry; all fields are taken verbatim (no sizing or
    /// attribution logic here).
    pub fn new(
        sym: String,
        qty: i64,
        owner: Option<String>,
        limit_px: Option<f64>,
        oca_group: Option<String>,
    ) -> Self {
        Self { sym, qty, owner, limit_px, oca_group }
    }
}

/// OrderGate emit-descriptor prefix: stamped on every OrderGate-placed
/// entry's oca_group. Doubles as the fill-join key in the gate's
/// `on_realised` and the engine entry-sibling cancel trigger in the
/// reconcile. Lives here (shared home) so both the engine and the gate
/// import the same literals; order_gate.rs re-exports them.
pub const OG_PREFIX: &str = "og-";
/// OrderGate CANCEL marker prefix: an emitted order carrying this prefix
/// cancels the engine's working resting entries for (symbol, order_ref,
/// opposite action) — the reconcile EmitOrders arm honors it.
pub const OG_CANCEL_PREFIX: &str = "ogcancel-";

/// Mid-chain emission: sizing intents (before the shares tail) or engine
/// shares (post `SharesGate`). Python duck-types this ("dict of intents" /
/// "{sym: qty}"); Rust splits them so the tail conversion is type-checked.
#[derive(Debug, Clone, PartialEq)]
pub enum Payload {
    Intents(IntentMap),
    Shares(Vec<ShareOut>),
}

impl Payload {
    /// Borrow the intent list, or `None` when this payload is already shares
    /// (i.e. it has passed the `SharesGate` tail).
    pub fn intents(&self) -> Option<&IntentMap> {
        match self {
            Payload::Intents(v) => Some(v),
            Payload::Shares(_) => None,
        }
    }

    /// Borrow the share list, or `None` while the payload is still intents.
    pub fn shares(&self) -> Option<&Vec<ShareOut>> {
        match self {
            Payload::Shares(v) => Some(v),
            Payload::Intents(_) => None,
        }
    }
}

/// One execution actually cleared by the ENGINE, routed by the fund's
/// `on_fill` (never inferred from position diffs). Head-first down the chain.
#[derive(Debug, Clone, PartialEq)]
pub struct RealisedFill {
    pub sym: String,
    /// Signed: +buy / -sell.
    pub qty: i64,
    /// Fill price (bar close +/- engine slippage).
    pub px: f64,
    pub date: NaiveDate,
    /// Engine order-id counter value stamped into the synthetic execution —
    /// mailbox id == execution.order_id by construction (HYBRID_ROLLOUT.md
    /// §1). Live IB fills carry the broker's order id.
    pub order_id: i64,
    /// OCA group carried from the emitted Order ("" = none). The OrderGate
    /// joins fills to its resting entries on this field (§4).
    pub oca_group: String,
}
