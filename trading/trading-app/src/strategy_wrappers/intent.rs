//! Inter-gate emission payload for strategy_wrappers (battery-exact mirror of
//! `stratlab/stratlab/strategy_wrappers/intent.py`).
//!
//! Gates between the chain head and the root transform SIZING INTENTS, not
//! truncated share counts. Rationale = battery-exact arithmetic (ARCH-0 byte
//! parity, promoted/blended5_fund/ARCHITECTURE.md §4):
//!
//!   battery: q = floor(N * sc * W_product / px)     (one truncation)
//!   share-level weighting would give q = floor(floor(N/px) * W) — a different
//!   integer where frac(N/px) * W crosses a unit boundary.
//!
//! So gate inputs/outputs are `{sym: Intent}`; the chain TAIL (`SharesGate`)
//! converts to engine shares exactly once (`shares_from_intents`), which is
//! where the single truncation happens.

use super::BarFrame;
use super::payload::ShareOut;

/// Pre-truncation sizing intent for one symbol on one tick.
///
/// - `side`: +1 long, -1 short, 0 = explicit flatten (engine MUST see 0).
/// - `n_base`: sizing notional BEFORE weights and BEFORE eq-scaling; for
///   per-share intents this is the vol-target size `vt`.
/// - `w`: cumulative weight product so far (head emits 1.0; weight gates
///   multiply/clip this).
/// - `sc`: compound scale stamped by an EqScale gate (eq/se_ref, battery
///   `sc = eq/SE`, finite>0 else 1.0). Heads emit sc = 1.0; a chain with NO
///   EqScale gate stays a fixed-path book because `shares_from_intents`
///   consumes ONLY this.
/// - `strategy`: cross-sleeve attribution (§8 pooled emission). `None` =
///   the owning sleeve (`SharesGate` passes it unattributed; the sleeve
///   fills its own name when building order_ref). `Some(name)` = this
///   intent belongs to sleeve `name` (the central pooled trim emits the
///   whole fund's sheet through the completing sleeve's chain — order_ref
///   = attribution, fills route by order_ref). Foreign intents ride the
///   SharesGate verbatim as PER-SHARE book counts: no printable-omit, no
///   dollars conversion, explicit flats kept.
/// - `limit_px`: `Some(px)` = the OrderGate (§4 hybrid) should rest this
///   entry as a passive LMT at `px` instead of crossing at market. `None`
///   = market. Ignored by every qty arithmetic path.
/// - `oca_group`: the OrderGate's emit descriptor (`og-…`), cancel marker
///   (`ogcancel-…`), or engine OCA group; `None` = plain market / no OCA.
///   Carried through SharesGate into `ShareOut.oca_group` so fills can be
///   joined back by `order_id`/group. Ignored by qty arithmetic.
#[derive(Debug, Clone, PartialEq)]
pub struct Intent {
    pub side: i32,
    pub n_base: f64,
    pub w: f64,
    pub per_share: bool,
    pub sc: f64,
    pub unscaled: bool,
    pub strategy: Option<String>,
    pub limit_px: Option<f64>,
    pub oca_group: Option<String>,
}

impl Intent {
    /// Entry intent, dollars path (`n_base` notional, weights applied later).
    pub const fn long(n_base: f64, w: f64) -> Self {
        Self { side: 1, n_base, w, per_share: false, sc: 1.0, unscaled: false, strategy: None, limit_px: None, oca_group: None }
    }

    /// Short entry intent on the dollars path (`side = -1`).
    pub const fn short(n_base: f64, w: f64) -> Self {
        Self { side: -1, n_base, w, per_share: false, sc: 1.0, unscaled: false, strategy: None, limit_px: None, oca_group: None }
    }

    /// Explicit flatten target (zero notional — sc/w irrelevant).
    pub const fn flat() -> Self {
        Self { side: 0, n_base: 0.0, w: 1.0, per_share: false, sc: 1.0, unscaled: false, strategy: None, limit_px: None, oca_group: None }
    }

    /// Vol-target intent (`n_base` = vt size; raw = (vt*|w|)*sc, no px).
    ///
    /// `side` is +1 long / -1 short / 0 flat; `vt` is the per-share target
    /// size and `w` the weight accumulated so far.
    pub const fn per_share(side: i32, vt: f64, w: f64) -> Self {
        Self { side, n_base: vt, w, per_share: true, sc: 1.0, unscaled: false, strategy: None, limit_px: None, oca_group: None }
    }

    /// Battery SigStrat dollars-fallback: plain notional, NEVER eq-stamped
    /// (battery SH 445-447; EqScaleGate skips `unscaled` intents).
    ///
    /// Use for fallbacks that must not be scaled by the fund's `sc`.
    pub const fn unscaled(side: i32, n_base: f64, w: f64) -> Self {
        Self { side, n_base, w, per_share: false, sc: 1.0, unscaled: true, strategy: None, limit_px: None, oca_group: None }
    }
}

/// Ordered `{sym: Intent}` map — insertion order preserved (Python dict
/// iteration drives the battery's stream/seeding order; parity-risk ledger #1).
pub type IntentMap = Vec<(String, Intent)>;

/// Battery truncation association per mode:
/// - dollars:   `raw = ((N * sc) * w) / px`  -> qty
///              (CompBook leg, or fixed-path book when sc == 1.0)
/// - per-share: `raw = ((vt * |w|) * sc)`    -> qty, NO px (px = 1.0)
///
/// `zero_is_flat` reproduces the battery CompBook/SigStrat tail semantics
/// (B9 272-306 / SH 448-451): a NAMED cell whose truncation yields 0 shares
/// emits an EXPLICIT 0 target (forces the engine to sell-to-flat). A symbol
/// with no printable bar is OMITTED (engine keeps the position — silent-skip).
///
/// Attribution is stamped at the CONSUMER (`SharesGate` + pooled sheet):
/// own intents → `ShareOut.strategy = None`; the single share-count
/// arithmetic stays here.
pub fn shares_from_intents(intents: &IntentMap, bars: &BarFrame, zero_is_flat: bool) -> Vec<ShareOut> {
    let mut out: Vec<ShareOut> = Vec::with_capacity(intents.len());
    for (sym, it) in intents {
        if let Some(q) = intent_share(sym, it, bars, zero_is_flat) {
            out.push(ShareOut::new(sym.clone(), q, None, it.limit_px, it.oca_group.clone()));
        }
    }
    out
}

/// ONE intent's battery conversion (the shares_from_intents body verbatim,
/// extracted so the pooled foreign-attribution path can reuse the exact
/// arithmetic without reordering the emission stream — parity-risk #1).
/// `None` = clipped by the battery semantics (unprintable / non-finite).
pub fn intent_share(sym: &str, it: &Intent, bars: &BarFrame, zero_is_flat: bool) -> Option<i64> {
    if it.side == 0 {
        // explicit flatten — SharesGate emits (sym, 0) iff printable;
        // unprintable → omitted → carry stands.
        return if bars.contains_key(sym) { Some(0) } else { None };
    }
    if it.per_share {
        // px = 1.0, never used (battery SH 438-441)
        let raw = (it.n_base * it.w) * it.sc;
        if !raw.is_finite() || raw <= 0.0 {
            return None; // unnamed -> engine keeps position
        }
        let q = raw.trunc(); // battery: int(raw / 1.0), trunc toward zero
        if q <= 0.0 {
            // zero_is_flat=true on every sleeve chain: explicit 0 iff
            // printable, else omitted.
            return if zero_is_flat && bars.contains_key(sym) {
                Some(0)
            } else {
                None
            };
        }
        Some(q as i64 * it.side as i64)
    } else {
        // Native app bar (`HistoricalDataFullKeys`); absent key = unprintable = skip.
        let px = bars.get(sym).map(|b| b.get_price()).filter(|p| p.is_finite() && *p > 0.0)?;
        let raw = (it.n_base * it.sc) * it.w; // battery association: (N*sc)*w
        if !raw.is_finite() || raw <= 0.0 {
            return None;
        }
        let q = (raw / px).trunc(); // trunc toward zero (raw > 0, px > 0)
        if q <= 0.0 {
            return if zero_is_flat && bars.contains_key(sym) {
                Some(0)
            } else {
                None
            };
        }
        Some(q as i64 * it.side as i64)
    }
}
