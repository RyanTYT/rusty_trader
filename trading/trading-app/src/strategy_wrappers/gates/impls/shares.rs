//! SharesGate — tail conversion gate of a sleeve chain (battery-exact mirror
//! of `promoted/blended5_fund/sleeves/base.py::SharesGate`).
//!
//! PURE conversion — no equity read, no scaling of its own. The battery
//! compound scale must already sit on every intent (`it.sc`, stamped by an
//! EqScaleGate for compound legs; absent chain => sc=1.0 fixed path). This is
//! where the single battery truncation happens; the runner memoises the
//! SHARES output for the realised-fill diff.
//!
//! `emit_zero_flat` reproduces the battery CompBook/SigStrat tail (B9
//! 272-306, SH 448-451): named cells truncating to 0 shares emit an explicit
//! 0 target. Default false (production connors anchor semantics: skip).

use crate::strategy_wrappers::gate::GateLogic;
use crate::strategy_wrappers::intent::intent_share;
use crate::strategy_wrappers::payload::{BarFrame, Payload, ShareOut};
use crate::strategy_wrappers::strategy::ClockCtx;

#[derive(Debug, Clone, PartialEq)]
pub struct SharesGate {
    pub emit_zero_flat: bool,
}

impl SharesGate {
    pub const fn new(emit_zero_flat: bool) -> Self {
        Self { emit_zero_flat }
    }
}

impl GateLogic for SharesGate {
    fn on_bar_update(
        &mut self,
        up: Option<&Payload>,
        bars: &BarFrame,
        _ctx: &ClockCtx,
    ) -> Result<Payload, String> {
        let up = match up {
            None => return Ok(Payload::Shares(Vec::new())), // Python `if up is None: return {}`
            Some(u) => u,
        };
        let items = match up {
            Payload::Intents(v) => v,
            Payload::Shares(_) => return Err("SharesGate consumes Intents, not Shares".into()),
        };
        // §8 pooled emission: foreign-attributed intents (intent.strategy =
        // Some(owner)) ride VERBATIM through the tail as per-share book
        // counts — NO printable-omit (foreign syms are absent from THIS
        // sleeve's frame by construction), explicit flats kept, order_ref =
        // owner at the sleeve's order assembly. Own intents (strategy None)
        // take the exact battery `shares_from_intents` arithmetic. ONE
        // ordered loop: emission order is parity-bearing.
        let mut out = Vec::with_capacity(items.len());
        for (sym, it) in items.iter() {
            let sym = sym.clone();
            if let Some(owner) = &it.strategy {
                debug_assert!(
                    it.per_share,
                    "pooled foreign intents are per-share book counts by construction"
                );
                if it.side == 0 {
                    out.push(ShareOut::new(sym, 0, Some(owner.clone()), None, it.oca_group.clone()));
                } else {
                    let raw = (it.n_base * it.w) * it.sc;
                    let q = if raw.is_finite() && raw > 0.0 {
                        raw.trunc() as i64 * it.side as i64
                    } else {
                        0
                    };
                    // limit_px/oca_group ride verbatim (OrderGate §4; inert
                    // until the OrderGate exists in the chain).
                    out.push(ShareOut::new(sym, q, Some(owner.clone()), it.limit_px, it.oca_group.clone()));
                }
            } else if let Some(q) = intent_share(&sym, it, bars, self.emit_zero_flat) {
                out.push(ShareOut::new(sym, q, None, it.limit_px, it.oca_group.clone()));
            }
        }
        Ok(Payload::Shares(out))
    }
}
