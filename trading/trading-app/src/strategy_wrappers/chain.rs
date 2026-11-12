//! Chain driver (v2 restructure).
//!
//! WHY THIS EXISTS: `GateEnum` is a tree of pure transforms — something must
//! enter it at the head and exit it at the tail. This file IS that driver, as
//! FREE FUNCTIONS (no runner struct: after the `on_fill` redesign there is no
//! runner-owned state — no memo, no inferred fills).
//!
//! - [`run_chain`]: one forward pass, head → tail. The tail MUST be a
//!   `SharesGate` (`Payload::Shares` out); anything else is a construction
//!   error (the single battery truncation happens at that tail and nowhere
//!   else — intent.py docstring contract). Realised fills route head-first
//!   through every gate via `GateLogic::on_realised` (fund's `on_fill` →
//!   per-sleeve chains; streaming builders accumulate their maps there).

use super::gate::{GateEnum, GateLogic};
use super::payload::{BarFrame, Payload, ShareOut};
use super::strategy::ClockCtx;

/// Drive the chain for one tick: head produces, gates transform, tail
/// converts (single truncation). Returns the per-sleeve absolute share
/// targets for the fund's book — attributed (`ShareOut.strategy`: `None` =
/// owning sleeve, `Some(name)` = pooled cross-sleeve emission, §8).
pub fn run_chain(
    chain: &mut GateEnum,
    frame: &BarFrame,
    ctx: &ClockCtx,
) -> Result<Vec<ShareOut>, String> {
    match GateLogic::on_bar_update(chain, None, frame, ctx)? {
        Payload::Shares(out) => Ok(out),
        Payload::Intents(_) => Err(
            "chain must end in SharesGate: the SHARE payload is the runnable \
             tail contract (Python _targets_of semantics)"
                .to_string(),
        ),
    }
}
