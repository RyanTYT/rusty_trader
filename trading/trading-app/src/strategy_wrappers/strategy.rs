//! `BasicStrategy` — the base contract for a plain strategy (v2 restructure).
//!
//! There is NO separate `NaiveSource` adapter: a concrete strategy implements
//! this trait directly and it is used AS the chain head (the `GateEnum` head
//! variant holds `dyn BasicStrategy`). `blended5_fund/strategies/*` implement
//! it; `blended5_fund/enhancements/*` wrap them in gate chains.
//!
//! Push-model context: the chain does not pull from the strategy — the fund
//! builds one [`ClockCtx`] per flush (date, decision mso, scale computed
//! once from equity) and pushes it in; strategies emit pre-stamped intents.

use chrono::NaiveDate;

use super::payload::{BarFrame, RealisedFill};

/// Per-flush decision context — everything an emission may read, PUSHED in by
/// the fund (replaces the deleted `GateCtx` trait; no adapter layer).
///
/// - `et_date` / `mso`: decision identity of the current tick (grid-key
///   parity: `(et_date, mso)`).
/// - `scale`: the fund's compound scale `eq_sgd / SE_SLEEVE` (RAW SGD
///   equity exactly like the deleted EqScaleGate read `ctx.account.equity`;
///   the fund divides by USD_FX only for its CAP work), computed ONCE per
///   flush; heads stamp it onto non-`unscaled` intents (finite>0 else 1.0).
/// - `day_last`: intrinsic session-last edge (flush-on-advance proof; P4
///   amendment to the locked ctx shape). The battery GridSleeve looked the
///   emission row up in a PREBUILT grid — cells stamped on each date's LAST
///   tape row (mso 385 regular / 205 half-day). The Rust head re-derives,
///   so the fund PUSHES that same edge (`FundRow::last`). Heads finalize
///   their daily cross-section and emit on `day_last == true`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClockCtx {
    pub et_date: NaiveDate,
    pub mso: i32,
    pub scale: f64,
    pub day_last: bool,
    /// F2 (session end-times): this row sits in the session's final-K-bars
    /// CLOSE WINDOW (the second-last/last bars relative to the session's
    /// final bar). Derived by the sleeve from the SessionCalendar
    /// (`BLENDED5_CLOSE_AWARE=1`; a constant `false` everywhere else —
    /// default OFF keeps every pin verbatim). `fund_gate.rs` switches its
    /// cap basis (EOD 1.95 vs INTRA 4.0) from `day_last` to this window
    /// when the env seam is on; live strategies read the same field from
    /// the contract end times.
    pub close_window: bool,
    /// The owning sleeve's OWN SGD equity for this row (fund-pool push
    /// input; the pooled trim + the ledger reconcile consume it). A
    /// constant `0.0` in the legacy/test ctx literals (pool consumers
    /// treat ≤0 as absent).
    pub own_eq_sgd: f64,
}

/// Base contract for a plain strategy: signal emission + warm-up lifecycle.
///
/// `on_row` yields SIZING INTENTS (sc pre-stamped from `ctx.scale`, skip
/// `unscaled`); the enhancement chain transforms them and the `SharesGate`
/// tail converts once. Fill feedback enters via `on_realised`, routed by the
/// fund's `on_fill`.
pub trait BasicStrategy: Send + Sync + std::any::Any {
    fn name(&self) -> &str;

    fn warmup_bars_required(&self) -> usize;

    /// Emission for the current tick.
    fn on_row(&mut self, frame: &BarFrame, ctx: &ClockCtx) -> Result<crate::strategy_wrappers::intent::IntentMap, String>;

    /// Fill feedback (default no-op).
    fn on_realised(&mut self, _fills: &[RealisedFill], _ctx: &ClockCtx) {}
}
