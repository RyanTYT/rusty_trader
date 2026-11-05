//! FundGate — the §8 sleeve-autonomy fund layer as a per-sleeve `GateLogic`.
//!
//! PLAN.md §9.3 S2: the legacy `FundBook` (fund.rs) re-weighing is decomposed
//! into ONE gate instantiated per sleeve, chained AFTER the sleeve's weight
//! gates and BEFORE its `SharesGate`. The gate ports the legacy fire
//! classification + scale pass EXACTLY (fund.rs `on_row` steps 4-8 / fund.py
//! order), operating on a SHARE-SPACE book it carries itself:
//!
//! **The book carry (the load-bearing legacy semantics).** Heads are
//! delta-emitters — connors emits only entries + exit flats; held positions
//! are carried implicitly. The legacy `FundBook` kept each sub's FULL book
//! (`sub_cur`: fresh chain outputs overwrite, unnamed syms retain their last
//! value, explicit zeros KEPT) and re-emitted the full basket every row, so
//! the scale pass reached HELD positions. Without the same carry, a cap
//! scales only fresh entries and the held book compounds unbounded (the
//! first P2 solo run: connors 10k → 737k, cash −3.9M — the cap never touched
//! held positions). FundGate therefore keeps `book: BTreeMap<sym, i64>`
//! (signed shares) with exactly the legacy `sub_cur` semantics:
//!
//! - fresh intents are converted to shares at the gate boundary with the
//!   `shares_from_intents` arithmetic (dollars: printable required, else the
//!   carry stands; per-share: no bar needed; trunc-toward-zero; q≤0 +
//!   printable → explicit 0) and OVERWRITE the book (absolute);
//! - the gross is `Σ |q| · px` over the FULL book priced fresh-close →
//!   `last_px` cache → 0 — the legacy `gross_pv` idiom verbatim
//!   (`fixed ≡ 0` in both engines, kept structurally);
//! - the scale pass mutates the book in place (CUMULATIVE scaling across
//!   fires, exactly like the legacy pass re-scaling already-scaled
//!   `sub_cur` values): liquidate → 0; `px>0 ∧ q≠0 ∧ scale<1` →
//!   `floor(|q|·scale)·sign`; else kept;
//! - the OUTPUT is the FULL scaled book (fresh syms re-emitted at their
//!   scaled size, carried syms as frozen-share per-share intents —
//!   SharesGate truncates them to the exact carried count), so the engine
//!   reconcile trues up held positions every flush.
//!
//! Fire classification (exact port):
//! - `eq_usd <= 0` → scale 0, LIQUIDATE (the whole book → explicit 0s).
//! - `gross > cap·eq_usd ∧ priced > 0` → scale = ((cap·eq_usd − fixed) /
//!   priced).max(0.0).
//! - `gross > cap·eq_usd` with `priced == 0` → scale 1.0, violation logged,
//!   no resize (legacy branch; unreachable while `fixed ≡ 0`, kept for
//!   structural fidelity).
//! - else → scale 1.0 (no-op pass-through).
//!
//! `cap` = EOD 1.95 when `ctx.day_last` (intrinsic session-last edge) else
//! INTRA 4.0 — battery-exact constants re-used from `fund.rs`.
//!
//! Fund-level inputs come from the SLOT-START registry
//! (`backtester::methods::in_memory::state::registry`), written by the replay
//! runner before any strategy runs each slot:
//!
//! - fund equity = residual + Σ sleeve equities (identical to the legacy
//!   single-account equity by conservation: residual + Σ seeds + Σ pnl).
//! - peer gross = each peer's slot-start absolute targets priced at the
//!   current bars (the same gross_pv idiom).
//!
//! Vintage (PLAN.md §9.5 item 1, user-recommended): SLOT-START for peers —
//! deterministic and independent of sleeve execution order, so "the same
//! logic working over each strategy does not affect the re-weighing output".
//! The scale is therefore PER-SLEEVE (each gate mixes its own fresh intents
//! with slot-start peers) — a declared P2 baseline difference from the
//! legacy single global scale (which saw all sleeves' fresh intents at
//! once; unreachable in a sequential single-pass engine without a two-phase
//! reconcile).
//!
//! Sizing basis note (PLAN.md §8.3/D1): the sleeve's own equity drives its
//! `sc` stamping upstream (FundRow scale = own eq / SE_SLEEVE); the
//! fund-level CAP basis here is the full fund equity (residual + Σ sleeves)
//! — the legacy constraint.
//!
//! `on_realised` is the default no-op: the fund gate never consumes fills.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use crate::backtester::methods::in_memory::{diag, state};
use crate::execution::order_engine::OrderIBKR;
use crate::strategy::blended5_fund::fund::{
    EOD_CAP, INTRA_CAP, SE_SLEEVE, USD_FX, ibkr_primary_exchange,
};
use crate::strategy::strategy::BarUpdateOutcome;
use crate::strategy_wrappers::gate::GateLogic;
use crate::strategy_wrappers::intent::Intent;
use crate::strategy_wrappers::payload::{BarFrame, Payload, ShareOut};
use crate::strategy_wrappers::strategy::ClockCtx;
use ibapi::contracts::Contract;
use ibapi::orders::{Action, Order, TimeInForce};

/// Global fire/violation instrumentation (aggregated across instances; the
/// legacy FundBook kept per-book recs — the multi runner logs these once at
/// run end via [`log_counters`]).
static N_FIRE_EOD: AtomicU64 = AtomicU64::new(0);
static N_FIRE_INTRA: AtomicU64 = AtomicU64::new(0);
static N_VIOL_EOD: AtomicU64 = AtomicU64::new(0);
static N_VIOL_INTRA: AtomicU64 = AtomicU64::new(0);
/// Warn-once flag for a missing pool (pass-through fail-open).
static WARNED_NO_REGISTRY: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Default)]
pub struct FundGateCounters {
    pub fire_eod: u64,
    pub fire_intra: u64,
    pub viol_eod: u64,
    pub viol_intra: u64,
}

/// Snapshot + reset the global counters (run-end reporting seam).
pub fn take_counters() -> FundGateCounters {
    FundGateCounters {
        fire_eod: N_FIRE_EOD.swap(0, Ordering::Relaxed),
        fire_intra: N_FIRE_INTRA.swap(0, Ordering::Relaxed),
        viol_eod: N_VIOL_EOD.swap(0, Ordering::Relaxed),
        viol_intra: N_VIOL_INTRA.swap(0, Ordering::Relaxed),
    }
}

/// Log the aggregated counters at run end (called by the backtest binaries).
pub fn log_counters(tag: &str) {
    let c = take_counters();
    tracing::info!(
        tag,
        fire_eod = c.fire_eod,
        fire_intra = c.fire_intra,
        viol_eod = c.viol_eod,
        viol_intra = c.viol_intra,
        "FundGate cap instrumentation (aggregated across sleeves)"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// FUND POOL — cross-sleeve vintage pool (user-designed, strategy-layer).
//
// TWO-PHASE reconcile (collect → central trim): each sleeve's FundGate
// PUSHES its own UNTRIMMED slot state here from inside its own
// `on_bar_update` (the gate's natural lifecycle: flush-on-advance = one push
// per slot per sleeve), into a lock-free ring array indexed by
// `ts_mod = (num_days_from_ce << 16 | mso) % RING` with overwriting writes.
// The COMPLETING push of a slot (mask reaching all participants) runs the
// ONE uniform central trim over the complete vintage (legacy single-book
// semantics: one factor for every entry) and hands the scaled sheet to the
// sleeve layer: ONE pooled emission on behalf of ALL sleeves (EmitOrders
// variant; fills route to the right sleeve account by order_ref — the
// approved engine seam). Scaled books are written back to LAST_TRIMMED so
// each gate's next flush carries the trimmed book (the cumulative ratchet,
// order-independent).
//
// Single-thread-class engine (sequential single-pass replay): the atomics
// exist for static sharing, not for cross-thread synchronisation.
// ═══════════════════════════════════════════════════════════════════════

use std::sync::Arc;

pub const FUND_RING: usize = 128;
pub const FUND_MAX_PARTICIPANTS: usize = 8;

/// One ring slot: everything the pooled trim needs for ONE (date, mso)
/// across ALL participants. `key == 0` = never written. The mask bit i set
/// = participant i has pushed for this exact key.
#[derive(Default)]
pub struct PoolSlot {
    /// (et_date_ordinal << 16) | mso — 0 = empty slot.
    pub key: AtomicU64,
    /// Participant presence bitmask (bit i = participant i pushed).
    pub mask: AtomicU32,
    /// Per-participant sleeve equity (SGD, f64 bits).
    pub eq: [AtomicU64; FUND_MAX_PARTICIPANTS],
    /// Per-participant ABSOLUTE intent book (signed shares) — Arc ptr.
    pub book: [AtomicU64; FUND_MAX_PARTICIPANTS],
    /// Per-participant priced / fixed gross components (f64 bits).
    pub priced: [AtomicU64; FUND_MAX_PARTICIPANTS],
    pub fixed: [AtomicU64; FUND_MAX_PARTICIPANTS],
}

struct PoolRing {
    slots: Vec<PoolSlot>,
    names: Vec<String>,
}

static POOL: std::sync::OnceLock<PoolRing> = std::sync::OnceLock::new();
/// Unbooked fund capital (f64 bits) — set once at init.
static POOL_RESIDUAL: AtomicU64 = AtomicU64::new(0);
/// FundGate REGISTRATION: every `FundGate::new(strategy)` appends its own
/// sleeve name here (deduped) — the pool's participant set is exactly this
/// registry, so the ONE init input the binary supplies is the fund capital.
/// Registration is folded into `new`; no separate init/registration call.
static REGISTRY: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

    /// Packed (et_date, mso) into a u64 ring key. Year-safe: ordinal()
    /// wraps every calendar year — 2021-01-03 and 2022-01-03 produced the
    /// SAME key, so the first slot of a new year started from a STALE-FULL
    /// mask and completed against the previous year's books (BUG-1, seen in
    /// the m1.6 diag run). `num_days_from_ce` is unique per calendar day
    /// (~738k for 2021 — far below the 48-bit slot budget).
fn pack_key(et_date: chrono::NaiveDate, mso: i32) -> u64 {
    use chrono::Datelike;
    ((et_date.num_days_from_ce() as u64) << 16) | (mso.max(0) as u64 & 0xFFFF)
}

/// Initialise the pool ONCE per program lifetime. Participants come from
/// the REGISTRY (every `FundGate::new(strategy)` registered its sleeve —
/// the binary constructs all strategies before calling this, so the set is
/// complete); the ONE input is the fund capital. Residual is derived
/// inside: the binary seeds each sleeve `SE_SLEEVE`, so
/// `residual = capital − N·SE_SLEEVE` (the same math the binary used to
/// pass explicitly). Also wires the fund LEDGER (see the ledger section at
/// the bottom of this file) — the ledger reads the pool's vintage instead
/// of any engine state.
pub fn pool_init(ledger_capital: f64) {
    let names: Vec<String> =
        REGISTRY.lock().expect("fund registry poisoned").clone();
    let residual = (ledger_capital - names.len() as f64 * SE_SLEEVE).max(0.0);
    let ring = PoolRing {
        slots: (0..FUND_RING).map(|_| PoolSlot::default()).collect(),
        names: names.clone(),
    };
    let _ = POOL.set(ring);
    POOL_RESIDUAL.store(residual.to_bits(), Ordering::Relaxed);
    ledger_init(&names, ledger_capital);
    tracing::info!(
        participants = names.len(),
        residual,
        "fund pool initialised (strategy-layer cross-sleeve vintage)"
    );
}

#[cfg(test)]
fn pool_reset() {
    // Test-only: rebuild an empty pool. POOL is OnceLock — tests that need
    // a fresh pool run in their own process (libtest runs each #[test] on
    // its own thread but shares the process); we expose the pieces the
    // ledger/gate tests need through the thread-local LEDGER reset and
    // accept the OnceLock constraint by initialising identical pools.
    POOL_RESIDUAL.store(0.0f64.to_bits(), Ordering::Relaxed);
    POOL_SEEN.store(0, Ordering::Relaxed);
    for s in POOL_LAST_EQ.iter().chain(POOL_LAST_PRICED.iter()).chain(POOL_LAST_FIXED.iter()) {
        s.store(0, Ordering::Relaxed);
    }
    for s in LAST_TRIMMED.iter().chain(LAST_TRIMMED_KEY.iter()) {
        s.store(0, Ordering::Relaxed);
    }
    if let Some(ring) = POOL.get() {
        for s in &ring.slots {
            s.key.store(0, Ordering::Relaxed);
            s.mask.store(0, Ordering::Relaxed);
        }
    }
}

/// Test-only explicit init: the ledger tests drive the pool WITHOUT live
/// sleeve constructions (no FundGate::new registrations), so names are
/// supplied directly + the ledger wiring is identical to production.
#[cfg(test)]
pub fn pool_test_init(names: &[String], residual: f64, ledger_capital: f64) {
    let ring = PoolRing {
        slots: (0..FUND_RING).map(|_| PoolSlot::default()).collect(),
        names: names.to_vec(),
    };
    let _ = POOL.set(ring);
    POOL_RESIDUAL.store(residual.to_bits(), Ordering::Relaxed);
    ledger_init(names, ledger_capital);
}

/// Test-only push simulation (production pushes happen inside the gate's
/// own `on_bar_update` or the break-sleeve `pool_push_carried`).
#[cfg(test)]
pub fn pool_test_push(
    name: &str,
    et_date: chrono::NaiveDate,
    mso: i32,
    eq: f64,
    book: &BTreeMap<String, i64>,
    priced: f64,
    fixed: f64,
) {
    if let Some(idx) = pool_own_idx(name) {
        pool_push(idx, pack_key(et_date, mso), eq, book, priced, fixed);
    }
}

fn pool() -> Option<&'static PoolRing> {
    POOL.get()
}

/// Packed key for the current slot + this gate's participant index.
/// Returns None when the pool is not initialised or `own` is unregistered.
fn pool_own_idx(own: &str) -> Option<usize> {
    pool()?.names.iter().position(|n| n == own)
}

/// Sleeve-side seam: is `own` a pool participant (pooled mode ON for that
/// sleeve)? Drives the sleeve's pooled vs legacy flush arms.
pub(crate) fn pool_is_participant(own: &str) -> bool {
    pool_own_idx(own).is_some()
}

/// FUND equity at SLOT-START vintage (the legacy engine-registry value):
/// residual + Σ per-sleeve equity, where an UNPUSHED sleeve contributes its
/// seed (SE_SLEEVE — the binary seeds every sleeve SE_SLEEVE, and the
/// engine's slot-start snapshot counted the seed balances the same way).
/// A gate classifies its OWN gross against THIS fund equity (legacy
/// semantics: per-sleeve gross vs FUND-level cap — NOT a pooled gross).
fn pool_fund_equity() -> f64 {
    let Some(ring) = pool() else { return f64::NAN };
    let residual = f64::from_bits(POOL_RESIDUAL.load(Ordering::Relaxed));
    let seen = POOL_SEEN.load(Ordering::Relaxed);
    let mut sum = residual;
    for i in 0..ring.names.len() {
        let eq = if seen & (1u32 << i) != 0 {
            f64::from_bits(POOL_LAST_EQ[i].load(Ordering::Relaxed))
        } else {
            SE_SLEEVE
        };
        sum += eq;
    }
    sum
}

/// Peers' freshest known gross (priced + fixed) — every OTHER participant's
/// last pushed gross (this slot if it already flushed, else the rolling
/// vintage). Legacy: the engine-registry slot-start snapshot gave every
/// gate the SAME peer gross; here late flushers see fresher peers, so the
/// fund total pins under the cap at the LAST trim of each slot (legacy's
/// observable EOD property, u ≤ cap).
fn pool_peer_gross(own_idx: usize) -> f64 {
    let n = pool().map(|r| r.names.len()).unwrap_or(0);
    let mut s = 0.0f64;
    for (i, c) in POOL_LAST_PRICED.iter().enumerate() {
        if i >= n || i == own_idx {
            continue;
        }
        s += f64::from_bits(c.load(Ordering::Relaxed))
            + f64::from_bits(POOL_LAST_FIXED[i].load(Ordering::Relaxed));
    }
    s
}

/// Sleeve-side direct push for the BROKE sleeve: its flush early-returns
/// BEFORE `run_chain`, so its gate never pushes — the pool mask must
/// complete EVERY slot or the whole fund stalls. Pushes the sleeve's
/// ENGINE-side carried positions as an absolute book (snapshot of
/// `state::current()` positions priced off THIS flush's frame; absent bars
/// contribute `fixed`, exactly like the gate's `last_px` miss path).
/// Returns the slot key when THIS push completed it — the sleeve then
/// runs the ONE central trim itself via [`pool_take_sheet`] (the gate
/// path is unavailable: no chain ran).
pub fn pool_push_carried(
    own: &str,
    et_date: chrono::NaiveDate,
    mso: i32,
    eq: f64,
    frame: &BarFrame,
) -> Option<u64> {
    let idx = pool_own_idx(own)?;
    let mut book: BTreeMap<String, i64> = BTreeMap::new();
    let mut priced = 0.0f64;
    let mut fixed = 0.0f64;
    if let Some(st) = state::current() {
        let guard = st
            .current_positions
            .read()
            .expect("InMemoryState current_positions poisoned");
        for (k, v) in guard.iter() {
            if k.strategy != own
                || k.stock.starts_with("CASH:")
                || book.contains_key(&k.stock)
                || v.quantity == 0.0
            {
                continue;
            }
            let q = v.quantity.trunc() as i64;
            book.insert(k.stock.clone(), q);
            let px = frame
                .get(&k.stock)
                .map(|b| b.get_price())
                .filter(|p| p.is_finite() && *p > 0.0)
                .unwrap_or(0.0);
            let g = q.abs() as f64 * if px > 0.0 { px } else { 0.0 };
            if px > 0.0 {
                priced += g;
            } else {
                fixed += g;
            }
        }
    }
    let key = pack_key(et_date, mso);
    if pool_push(idx, key, eq, &book, priced, fixed) {
        Some(key)
    } else {
        None
    }
}

/// Push THIS participant's slot state; returns true when THIS push
/// completed the slot (mask full) — the completing gate/sleeve owns the
/// central trim for the slot.
fn pool_push(
    idx: usize,
    key: u64,
    eq: f64,
    book: &BTreeMap<String, i64>,
    priced: f64,
    fixed: f64,
) -> bool {
    let Some(ring) = pool() else { return false };
    let slot = &ring.slots[(key as usize) % FUND_RING];
    let prev = slot.key.load(Ordering::Relaxed);
    if prev != key {
        // New (or recycled) slot: this push re-seeds it. Single-thread-class
        // engine: no other writer can race this slot for the same key.
        slot.key.store(key, Ordering::Relaxed);
        slot.mask.store(0, Ordering::Relaxed);
    }
    slot.eq[idx].store(eq.to_bits(), Ordering::Relaxed);
    POOL_LAST_EQ[idx].store(eq.to_bits(), Ordering::Relaxed);
    POOL_LAST_PRICED[idx].store(priced.to_bits(), Ordering::Relaxed);
    POOL_LAST_FIXED[idx].store(fixed.to_bits(), Ordering::Relaxed);
    POOL_SEEN.fetch_or(1u32 << idx, Ordering::Relaxed);
    slot.priced[idx].store(priced.to_bits(), Ordering::Relaxed);
    slot.fixed[idx].store(fixed.to_bits(), Ordering::Relaxed);
    // Arc<BTreeMap> published as a raw pointer; the writer clones a fresh
    // Arc per push and leaks the previous one deliberately: steady-state
    // books are tiny and overwritten per slot (the engine keeps no handle
    // and the ring recycles every RING slots). Documented single-pass
    // class — no reader can observe a half-published pointer.
    let arc: Arc<BTreeMap<String, i64>> = Arc::new(book.clone());
    slot.book[idx].store(Arc::into_raw(arc) as u64, Ordering::Relaxed);
    let full = (1u32 << ring.names.len()) - 1;
    let m = slot.mask.fetch_or(1u32 << idx, Ordering::Relaxed) | (1u32 << idx);
    let completed = m == full;
    if diag::diag_fills() {
        let owner: Option<&String> = ring.names.get(idx);
        tracing::info!(
            key,
            owner = owner.map(|s| s.as_str()).unwrap_or("?"),
            eq,
            priced,
            fixed,
            book_len = book.len(),
            completed,
            "[POOLED] push"
        );
    }
    completed
}

/// The pooled view of ONE slot: per-participant (name, eq, book, priced,
/// fixed) + fund inputs. `None` when the pool is absent or the slot's mask
/// is not complete (the caller falls back / skips).
fn pool_take_slot(key: u64) -> Option<FundSlotView> {
    let ring = pool()?;
    let slot = &ring.slots[(key as usize) % FUND_RING];
    if slot.key.load(Ordering::Relaxed) != key {
        return None;
    }
    let full = (1u32 << ring.names.len()) - 1;
    if slot.mask.load(Ordering::Relaxed) != full {
        return None;
    }
    let mut parts = Vec::with_capacity(ring.names.len());
    for (i, name) in ring.names.iter().enumerate() {
        let eq = f64::from_bits(slot.eq[i].load(Ordering::Relaxed));
        let priced = f64::from_bits(slot.priced[i].load(Ordering::Relaxed));
        let fixed = f64::from_bits(slot.fixed[i].load(Ordering::Relaxed));
        let ptr = slot.book[i].load(Ordering::Relaxed) as *const BTreeMap<String, i64>;
        let book: Arc<BTreeMap<String, i64>> = if ptr.is_null() {
            Arc::new(BTreeMap::new())
        } else {
            // Reconstruct an owned view (single-pass engine: the writer
            // cannot race). Clone out; the ring slot keeps its original.
            let borrowed: &BTreeMap<String, i64> = unsafe { &*ptr };
            Arc::new(borrowed.clone())
        };
        parts.push((name.clone(), eq, book, priced, fixed));
    }
    let residual = f64::from_bits(POOL_RESIDUAL.load(Ordering::Relaxed));
    Some(FundSlotView {
        key,
        residual,
        participants: parts,
    })
}

/// Complete-slot snapshot handed to the pooled trim / ledger.
struct FundSlotView {
    key: u64,
    residual: f64,
    /// (name, sleeve eq SGD, absolute book, priced gross, fixed gross).
    participants: Vec<(String, f64, Arc<BTreeMap<String, i64>>, f64, f64)>,
}

impl FundSlotView {
    /// Fund equity (SGD) = residual + Σ sleeve equities.
    fn fund_eq_sgd(&self) -> f64 {
        self.residual + self.participants.iter().map(|(_, e, _, _, _)| e).sum::<f64>()
    }
    /// Total priced / fixed gross over ALL participants' books.
    fn pooled_gross(&self) -> (f64, f64) {
        let priced = self.participants.iter().map(|(_, _, _, p, _)| p).sum::<f64>();
        let fixed = self.participants.iter().map(|(_, _, _, _, f)| f).sum::<f64>();
        (priced, fixed)
    }
}

/// Slot-level EOD edge shared by the central trim: BLENDED5_CLOSE_AWARE=1
/// reads the session close window, else the day-last edge. The completer's
/// ctx decides for the whole slot (all sleeves flush the same bar).
pub(crate) fn slot_eod_edge(close_window: bool, day_last: bool) -> bool {
    if std::env::var("BLENDED5_CLOSE_AWARE").ok().as_deref() == Some("1") {
        close_window
    } else {
        day_last
    }
}

/// Pure scale kernel: legacy per-sleeve classification.
/// Returns (scale, violated, liquidate).
fn classify_scale(
    fund_eq_usd: f64,
    priced: f64,
    fixed: f64,
    peer_gross: f64,
    cap: f64,
) -> (f64, bool, bool) {
    let mut scale = 1.0f64;
    let mut liquidate = false;
    let mut viol = false;
    if fund_eq_usd <= 0.0 || !fund_eq_usd.is_finite() {
        if fund_eq_usd.is_finite() {
            scale = 0.0;
            liquidate = true;
            viol = true;
        }
    } else if priced + fixed + peer_gross > cap * fund_eq_usd && priced > 0.0 {
        scale = ((cap * fund_eq_usd - fixed - peer_gross) / priced).max(0.0);
        viol = true;
    } else if priced + fixed + peer_gross > cap * fund_eq_usd {
        scale = 1.0;
        viol = true;
    }
    (scale, viol, liquidate)
}

/// In-place cumulative scale over a persistent gate book (legacy step 8
/// verbatim; px from the owner's last_px cache).
fn apply_scale_to_book(
    book: &mut BTreeMap<String, i64>,
    px_of: &dyn Fn(&str) -> f64,
    scale: f64,
    liquidate: bool,
) {
    for (sym, q) in book.iter_mut() {
        let newq = if liquidate {
            0
        } else {
            let px = px_of(sym);
            if !px.is_finite() || px <= 0.0 || *q == 0 || scale >= 1.0 {
                *q
            } else {
                ((q.abs() as f64 * scale).floor() as i64) * (if *q > 0 { 1 } else { -1 })
            }
        };
        *q = newq;
    }
}

/// Uniform global scale over the concatenated slot books (legacy single-book
/// semantics): ONE factor for every entry. Pure + hermetic (no statics) so
/// unit tests pin the math without a pool.
fn central_scale_panel(
    priced_total: f64,
    fixed_total: f64,
    fund_eq_usd: f64,
    cap: f64,
) -> (f64, bool, bool) {
    if !fund_eq_usd.is_finite() {
        return (1.0, false, false);
    }
    if fund_eq_usd <= 0.0 {
        return (0.0, true, true);
    }
    if priced_total + fixed_total > cap * fund_eq_usd {
        if priced_total > 0.0 {
            return (
                ((cap * fund_eq_usd - fixed_total) / priced_total).max(0.0),
                true,
                false,
            );
        }
        return (1.0, true, false);
    }
    (1.0, false, false)
}

/// Uniform scale applied to one book copy (no prices needed — the factor is
/// already decided; carried zeros stay zeros).
fn apply_uniform_scale(
    book: &BTreeMap<String, i64>,
    scale: f64,
    liquidate: bool,
) -> BTreeMap<String, i64> {
    book.iter()
        .map(|(s, q)| {
            let nq = if liquidate {
                0
            } else if *q == 0 || scale >= 1.0 {
                *q
            } else {
                ((q.abs() as f64 * scale).floor() as i64) * (if *q > 0 { 1 } else { -1 })
            };
            (s.clone(), nq)
        })
        .collect()
}

/// Centrally-trimmed book write-back: the cumulative ratchet now lives here
/// (the completer trims books belonging to OTHER gate objects, so the result
/// must be visible at their next flush). Per-participant (slot key, book).
static LAST_TRIMMED: [AtomicU64; FUND_MAX_PARTICIPANTS] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static LAST_TRIMMED_KEY: [AtomicU64; FUND_MAX_PARTICIPANTS] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

fn last_trimmed_for(idx: usize) -> Option<(u64, Arc<BTreeMap<String, i64>>)> {
    let ptr = LAST_TRIMMED[idx].load(Ordering::Relaxed) as *const BTreeMap<String, i64>;
    if ptr.is_null() {
        return None;
    }
    let key = LAST_TRIMMED_KEY[idx].load(Ordering::Relaxed);
    let borrowed: &BTreeMap<String, i64> = unsafe { &*ptr };
    Some((key, Arc::new(borrowed.clone())))
}

fn store_last_trimmed(idx: usize, key: u64, book: &BTreeMap<String, i64>) {
    let arc: Arc<BTreeMap<String, i64>> = Arc::new(book.clone());
    LAST_TRIMMED[idx].store(Arc::into_raw(arc) as u64, Ordering::Relaxed);
    LAST_TRIMMED_KEY[idx].store(key, Ordering::Relaxed);
}

// ── §8 CENTRAL POOLED TRIM — the ONE place fund-level emission logic lives ──
//
// Built sheet = the completed slot's ABSOLUTE CENTRALLY-TRIMMED book: per
// participant, per entry INCLUDING explicit zeros (BUG-2 fix: a flatten
// target rides the sheet so the engine true-ups held positions — dropping
// q==0 entries left positions orphaned forever). `Some(name)` attribution
// rides every entry — the order_ref source at the sleeve's order assembly
// (user ruling: "fills the order_reference field correctly — ie specific to
// each strategy, not all from one strategy").
//
// TWO-PHASE reconcile: every sleeve's gate pushes its UNTRIMMED book (phase
// 1, collect); the COMPLETING push runs the ONE uniform central trim over
// the complete vintage (phase 2, reconcile) and the scaled books are written
// back to LAST_TRIMMED so each gate's next flush carries the trimmed book
// (the cumulative ratchet, order-independent). Fire/viol counters are
// slot-level now: one viol per over-budget slot, one fire per sleeve whose
// book actually changed.
//
// TWO consumption pathways, ONE trim:
// - the COMPLETING GATE (`pooled_intents`): sheet → attributed per-share
//   intents → SharesGate (foreign pass-through, verbatim) → the chain
//   output IS the fund's sheet → the completing sleeve computes deltas →
//   MKT orders with order_ref = owner;
// - a BROKE completer (`pool_push_carried` → this): no chain ran, so the
//   sleeve converts the sheet to orders directly.
pub fn pool_take_sheet(
    key: u64,
    slot_date: chrono::NaiveDate,
) -> Option<Vec<ShareOut>> {
    let view = pool_take_slot(key)?;
    let mut sheet: Vec<ShareOut> = Vec::new();
    for (name, _eq, book, _priced, _fixed) in view.participants.iter() {
        for (sym, q) in book.iter() {
            sheet.push((sym.clone(), *q, Some(name.clone())));
        }
    }
    if diag::diag_fills() {
        let fund_eq = view.fund_eq_sgd();
        let per_owner: Vec<String> = view
            .participants
            .iter()
            .map(|(n, eq, b, p, f)| {
                format!(
                    "{} eq={:.0} gross={:.0}(|q|={} book={})",
                    n,
                    eq,
                    p + f,
                    b.values().map(|q| q.abs()).sum::<i64>(),
                    b.len()
                )
            })
            .collect();
        let sheet_q: i64 = sheet.iter().map(|(_, q, _)| q.abs()).sum();
        tracing::info!(
            slot = %slot_date,
            fund_eq_sgd = format!("{fund_eq:.2}"),
            sheet_len = sheet.len(),
            sheet_abs_q = sheet_q,
            participants = per_owner.join(" | "),
            "[POOLED] sheet"
        );
    }
    Some(sheet)
}

/// Phase 2 of the two-phase reconcile: uniform central trim over the
/// completed slot's UNTRIMMED books, write-back to LAST_TRIMMED, scaled
/// sheet out. `eod_edge` comes from the completer's ctx (one edge per slot).
pub fn pool_take_trimmed_sheet(
    key: u64,
    slot_date: chrono::NaiveDate,
    eod_edge: bool,
) -> Option<Vec<ShareOut>> {
    let view = pool_take_slot(key)?;
    let cap = if eod_edge { EOD_CAP } else { INTRA_CAP };
    let fund_eq_usd = view.fund_eq_sgd() / USD_FX;
    let (priced_total, fixed_total) = view.pooled_gross();
    let (scale, viol, liquidate) =
        central_scale_panel(priced_total, fixed_total, fund_eq_usd, cap);
    if viol {
        if eod_edge {
            N_VIOL_EOD.fetch_add(1, Ordering::Relaxed);
        } else {
            N_VIOL_INTRA.fetch_add(1, Ordering::Relaxed);
        }
    }
    let mut sheet: Vec<ShareOut> = Vec::new();
    for (i, (name, _eq, book, _p, _f)) in view.participants.iter().enumerate() {
        let scaled = apply_uniform_scale(book, scale, liquidate);
        if (liquidate || scale < 1.0) && scaled != **book {
            if eod_edge {
                N_FIRE_EOD.fetch_add(1, Ordering::Relaxed);
            } else {
                N_FIRE_INTRA.fetch_add(1, Ordering::Relaxed);
            }
        }
        store_last_trimmed(i, key, &scaled);
        for (sym, q) in scaled.iter() {
            sheet.push((sym.clone(), *q, Some(name.clone())));
        }
    }
    if diag::diag_fills() {
        let sheet_q: i64 = sheet.iter().map(|(_, q, _)| q.abs()).sum();
        let per_owner: Vec<String> = view
            .participants
            .iter()
            .map(|(n, eq, b, p, f)| {
                format!(
                    "{} eq={:.0} gross={:.0}(|q|={} book={})",
                    n,
                    eq,
                    p + f,
                    b.values().map(|q| q.abs()).sum::<i64>(),
                    b.len()
                )
            })
            .collect();
        tracing::info!(
            slot = %slot_date,
            fund_eq_sgd = format!("{:.2}", view.fund_eq_sgd()),
            scale = format!("{scale:.4}"),
            liquidate,
            sheet_len = sheet.len(),
            sheet_abs_q = sheet_q,
            participants = per_owner.join(" | "),
            bg = if eod_edge { "close|day_last" } else { "intra" },
            "[POOLED] central trim"
        );
    }
    Some(sheet)
}

/// The COMPLETING gate's pooled branch: sheet → ATTRIBUTED per-share
/// intents (absolute book counts). Emitting through the chain tail keeps
/// the payload contract: `run_chain` returns `Payload::Shares` and the
/// sleeve reads the attributed sheet from there. Empty sheet (e.g. every
/// book all-zero) → no intents → the sleeve emits `NoAction`.
fn pooled_intents(key: u64, slot_date: chrono::NaiveDate, eod_edge: bool) -> Payload {
    let Some(sheet) = pool_take_trimmed_sheet(key, slot_date, eod_edge) else {
        return Payload::Intents(Vec::new());
    };
    let mut out: crate::strategy_wrappers::intent::IntentMap =
        Vec::with_capacity(sheet.len());
    for (sym, q, owner) in sheet {
        // q == 0 → explicit flatten. Encoded as per_share(side=0, |q|=0):
        // the foreign SharesGate lane debug-asserts per-share book counts,
        // so the flat MUST stay per-share — side==0 routes it to
        // (sym, 0, owner) and the sleeve's delta order turns the zero
        // target into a full liquidation of the owner's carried position.
        let it = Intent::per_share(q.signum() as i32, q.abs() as f64, 1.0);
        out.push((sym, Intent { strategy: owner, ..it }));
    }
    Payload::Intents(out)
}

/// The per-sleeve fund cap gate. One instance per sleeve, chained before
/// `SharesGate` (see `sleeve.rs` `with_fund_gate`).
pub struct FundGate {
    /// Owning sleeve's engine strategy name ("blended5_<key>") — excluded
    /// from the peer gross (its own gross comes from its own book).
    own: String,
    eod_cap: f64,
    intra_cap: f64,
    /// Last-known-price cache (legacy `FundBook::last_px` idiom: freshly
    /// printed closes extend the map; unprinted syms price at the last
    /// seen close; never-priced syms contribute 0 = `fixed`).
    last_px: HashMap<String, f64>,
    /// The sleeve's FULL target book in share space (legacy `sub_cur`):
    /// signed share counts, fresh emissions overwrite, unnamed syms retain,
    /// explicit zeros KEPT, cumulative scaling on fire (legacy path) — in
    /// pooled mode the book is the UNTRIMMED collect panel and the trimmed
    /// book is adopted from the pool write-back at the next flush.
    book: BTreeMap<String, i64>,
    /// Slot key of the LAST_TRIMMED write-back already adopted (0 = none).
    last_trim_key: u64,
}

impl FundGate {
    pub fn new(own: String) -> Self {
        // Registration fold (user ruling): `new` is the ONLY registration
        // point — every sleeve's chain construction lands here, so by the
        // time the binary calls `pool_init`, REGISTRY holds exactly the
        // active sleeve set (deduped, construction order).
        {
            let mut reg = REGISTRY.lock().expect("fund registry poisoned");
            if !reg.iter().any(|n| *n == own) {
                reg.push(own.clone());
            }
        }
        Self {
            own,
            eod_cap: EOD_CAP,
            intra_cap: INTRA_CAP,
            last_px: HashMap::new(),
            book: BTreeMap::new(),
            last_trim_key: 0,
        }
    }

    /// Fresh close for `sym` (extending `last_px`, legacy gross_pv idiom).
    fn price_for(&mut self, sym: &str, bars: &BarFrame) -> f64 {
        match bars.get(sym).map(|b| b.get_price()).filter(|p| p.is_finite() && *p > 0.0) {
            Some(p) => {
                self.last_px.insert(sym.to_string(), p);
                p
            }
            None => self.last_px.get(sym).copied().unwrap_or(0.0),
        }
    }

    /// Convert ONE fresh intent to its raw share count exactly as
    /// `shares_from_intents` does. Returns `None` when SharesGate would
    /// OMIT the sym (carry stands), `Some(q)` (signed, may be 0 = explicit
    /// flat) when it would EMIT.
    fn fresh_shares(it: &Intent, sym: &str, bars: &BarFrame) -> Option<i64> {
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
                return if bars.contains_key(sym) { Some(0) } else { None };
            }
            Some(q as i64 * it.side as i64)
        } else {
            // dollars: absent/unprintable key = OMIT (carry stands).
            let px = bars.get(sym).map(|b| b.get_price()).filter(|p| p.is_finite() && *p > 0.0)?;
            let raw = (it.n_base * it.sc) * it.w; // battery association: (N*sc)*w
            if !raw.is_finite() || raw <= 0.0 {
                return None;
            }
            let q = (raw / px).trunc();
            if q <= 0.0 {
                return if bars.contains_key(sym) { Some(0) } else { None };
            }
            Some(q as i64 * it.side as i64)
        }
    }
}

impl GateLogic for FundGate {
    fn on_bar_update(
        &mut self,
        up: Option<&Payload>,
        bars: &BarFrame,
        ctx: &ClockCtx,
    ) -> Result<Payload, String> {
        let items = match up {
            Some(Payload::Intents(v)) => v,
            Some(Payload::Shares(_)) => {
                return Err("FundGate consumes Intents, not Shares".into())
            }
            None => return Err("FundGate requires upstream intents (never a chain head)".into()),
        };
        // DIAG seam: the chain output AT THE GATE BOUNDARY splits the signal
        // blackout: n_intents==0 → upstream chain silent; n_intents>0 with
        // the book staying empty → fresh_shares omitted every entry.
        if crate::backtester::methods::in_memory::diag::diag_fills() {
            let n = items.len();
            let abs_n: f64 = items.iter().map(|(_, it)| it.n_base.abs()).sum();
            tracing::info!(
                slot = %ctx.et_date,
                owner = %self.own,
                n_intents = n,
                abs_n_base = format!("{abs_n:.2}"),
                "[GATE] up"
            );
        }

        // Slot edge shared by the pooled collect + central trim (the
        // completer's ctx decides for the whole slot).
        let eod_edge = slot_eod_edge(ctx.close_window, ctx.day_last);

        // Adopt the previous slot's centrally-trimmed book BEFORE the fresh
        // overwrite (the cumulative ratchet lives in the pool LAST_TRIMMED
        // write-back now — the completer trims books belonging to OTHER
        // gate objects, so each gate reloads its trimmed carry here; fresh
        // emissions overwrite absolutely in step 1).
        if let Some(idx) = pool_own_idx(&self.own) {
            if let Some((tkey, tbook)) = last_trimmed_for(idx) {
                if tkey != self.last_trim_key {
                    self.book = tbook.as_ref().clone();
                    self.last_trim_key = tkey;
                }
            }
        }

        // ── 1) fresh emissions overwrite the book (absolute, legacy
        //       `sub_cur[si][sym] = q`); unnamed syms retain. Track which
        //       syms are fresh (output order = fresh first). ──
        let mut fresh_syms: HashSet<String> = HashSet::new();
        let mut fresh_order: Vec<String> = Vec::with_capacity(items.len());
        for (sym, it) in items.iter() {
            if let Some(q) = Self::fresh_shares(it, sym, bars) {
                if fresh_syms.insert(sym.clone()) {
                    fresh_order.push(sym.clone());
                }
                self.book.insert(sym.clone(), q);
            }
        }

        // ── 1b) legacy step 3 all-zero early gate on the PRE-scale book ──
        // (basket all-zero → emit NOTHING — legacy short-circuit before the
        // classification/scale pass; the engine accounts true up from the
        // sheet's explicit zeros when the strategy had held positions).
        // POOLED mode pushes even the all-zero book (completion counting)
        // before the early return; if THIS push completes the slot, the
        // sheet assembles HERE (an all-zero sleeve can be the completer).
        let all_zero = self.book.values().all(|q| *q == 0);
        if all_zero {
            if let Some(idx) = pool_own_idx(&self.own) {
                if pool_push(
                    idx,
                    pack_key(ctx.et_date, ctx.mso),
                    ctx.own_eq_sgd,
                    &self.book,
                    0.0,
                    0.0,
                ) {
                    return Ok(pooled_intents(
                        pack_key(ctx.et_date, ctx.mso),
                        ctx.et_date,
                        eod_edge,
                    ));
                }
            }
            return Ok(Payload::Intents(Vec::new()));
        }

        // ── 2) gross over the FULL book (legacy gross_pv verbatim) ──
        let mut priced = 0.0f64;
        let mut fixed = 0.0f64; // ≡ 0 by construction (legacy parity), kept
        let book_items: Vec<(String, i64)> = self
            .book
            .iter()
            .map(|(s, q)| (s.clone(), *q))
            .collect();
        for (sym, q) in book_items.iter() {
            if *q == 0 {
                continue;
            }
            let px = self.price_for(sym, bars);
            let g = (*q).abs() as f64 * if px > 0.0 { px } else { 0.0 };
            if px > 0.0 {
                priced += g;
            } else {
                fixed += g;
            }
        }

        // ── 2b) POOLED MODE — TWO-PHASE reconcile, phase 1 (collect):
        // push THIS slot's UNTRIMMED book (carry adopted at the top of the
        // flush, fresh overwritten in step 1, grossed in step 2). The
        // completing push runs phase 2 (the ONE uniform central trim over
        // the complete vintage) and emits the scaled sheet; non-completing
        // sleeves emit NOTHING (their books entered the pool via the push). ──
        if let Some(idx) = pool_own_idx(&self.own) {
            let fund_eq_sgd = pool_fund_equity();
            // Pre-trim panel (diagnostic only — the trim decision happens
            // once, centrally, at complete vintage).
            let peer_gross = pool_peer_gross(idx);
            let gross = priced + fixed;
            if crate::backtester::methods::in_memory::diag::diag_fills() {
                tracing::info!(
                    slot = %ctx.et_date,
                    owner = %self.own,
                    fund_eq = format!("{fund_eq_sgd:.2}"),
                    own_gross = format!("{gross:.2}"),
                    own_priced = format!("{priced:.2}"),
                    own_fixed = format!("{fixed:.2}"),
                    peer_gross = format!("{peer_gross:.2}"),
                    bg = if eod_edge { "close|day_last" } else { "intra" },
                    "[POOLED] collect"
                );
            }

            if pool_push(
                idx,
                pack_key(ctx.et_date, ctx.mso),
                ctx.own_eq_sgd,
                &self.book,
                priced,
                fixed,
            ) {
                return Ok(pooled_intents(
                    pack_key(ctx.et_date, ctx.mso),
                    ctx.et_date,
                    eod_edge,
                ));
            }
            return Ok(Payload::Intents(Vec::new()));
        }

        // ── 3) fund equity + cap + fire classification (exact port) ──
        // The engine registry is GONE (strategy-layer pool owns the fund
        // state now). A FundGate running WITHOUT pool initialisation
        // (bare unit constructions) fail-opens: peer gross 0, fund equity
        // NaN → pass-through unscaled.
        let fund_eq_sgd = {
            if !WARNED_NO_REGISTRY.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "FundGate: no fund pool initialised — cap DISABLED (fail-open pass-through)"
                );
            }
            f64::NAN
        };
        let eq_usd = fund_eq_sgd / USD_FX;
        let cap = if eod_edge { self.eod_cap } else { self.intra_cap };

        let (scale, viol, liquidate) = classify_scale(eq_usd, priced, fixed, 0.0, cap);
        if viol {
            if eod_edge {
                N_VIOL_EOD.fetch_add(1, Ordering::Relaxed);
                if liquidate || scale < 1.0 {
                    N_FIRE_EOD.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                N_VIOL_INTRA.fetch_add(1, Ordering::Relaxed);
                if liquidate || scale < 1.0 {
                    N_FIRE_INTRA.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // ── 4) scale pass over the FULL book (legacy step 8 verbatim:
        //       in-place, CUMULATIVE across fires, explicit zeros kept) ──
        apply_scale_to_book(
            &mut self.book,
            &|sym| self.last_px.get(sym).copied().unwrap_or(0.0),
            scale,
            liquidate,
        );

        // ── 5) emit the FULL scaled book: fresh syms at their scaled size,
        //       carried syms as frozen-share per-share intents (SharesGate
        //       truncates them to the exact carried count — legacy carried
        //       SHARES, not re-truncated notionals). ──
        let out = Self::book_to_intents(&self.book, &fresh_order, &fresh_syms);
        Ok(Payload::Intents(out))
    }
}

impl FundGate {
    /// The FULL book as the emit format: fresh syms first (at their — here
    /// unscaled — size), carried syms after; zeros → `Intent::flat()`,
    /// else frozen-share intents (raw = |q| shares at sc=1).
    fn book_to_intents(
        book: &BTreeMap<String, i64>,
        fresh_order: &[String],
        fresh_syms: &HashSet<String>,
    ) -> crate::strategy_wrappers::intent::IntentMap {
        let mut out: crate::strategy_wrappers::intent::IntentMap = Vec::new();
        for sym in fresh_order {
            let q = book[sym];
            if q == 0 {
                out.push((sym.clone(), Intent::flat()));
            } else {
                out.push((
                    sym.clone(),
                    Intent::per_share(q.signum() as i32, q.abs() as f64, 1.0),
                ));
            }
        }
        for (sym, q) in book.iter() {
            if fresh_syms.contains(sym) {
                continue;
            }
            if *q == 0 {
                out.push((sym.clone(), Intent::flat()));
            } else {
                out.push((
                    sym.clone(),
                    Intent::per_share(q.signum() as i32, q.abs() as f64, 1.0),
                ));
            }
        }
        out
    }
}

// ═══════════════════════════════════════════════════════════════════════
// FUND LEDGER — sizing-base state machine (py `Blended5Fund`
// SleeveEqLedger port). SEPARATE struct, same file (user ruling): the
// ledger IS fund logic and the FundGate/pool wiring requires it, but it
// drives a DIFFERENT lever (the sleeve `sc` sizing stamp upstream of the
// chain, env BLENDED5_FUND_LEDGER) versus the cap gate's in-chain trim.
//
// - `BLENDED5_FUND_LEDGER=1` → each sleeve sizes off its fund-share book
//   `e_i` (init = capital / N = 12.5k each on the 50k fund) instead of its
//   own account equity — the py single-fund-account compounding base.
// - default (unset) → §8 own-eq sizing, byte parity with the pins.
//
// Session-boundary reconcile `e_i += Δown_eq_i` fires at the first flush
// of a new ET date; the vintage = each sleeve's LAST POOL PUSH (the
// previous session's day-last flush — no trades in between, exactly the
// old slot-start registry value). Month-end EW re-anchor
// `e_i = (residual + Σeq − cum_sub)/N` fires when the session date enters
// a new calendar month. Invariant: Σ e_i == fund equity − cum_sub.
//
// Formula exactness (QDEL decode, 2026-10-30): per-trip notional
// `2000·w·(e_i/10k)·s` — at m3.2 (s=4.0, e_i init 12.5k) = `10,000·w`,
// identical to py MEW m3.20 (`8000·w·1.25`).
// ═══════════════════════════════════════════════════════════════════════

use std::cell::RefCell;

thread_local! {
    static FUND_LEDGER: RefCell<Option<FundLedger>> = const { RefCell::new(None) };
}

/// Ledger state machine (the py SleeveEqLedger book).
pub struct FundLedger {
    /// Sleeve name → fund-share equity book (SGD).
    e: HashMap<String, f64>,
    /// Previous flushed ET date — reconcile fires when the date advances.
    prev_date: Option<chrono::NaiveDate>,
    /// Month (year, month) of the last reconciled session — anchor trigger.
    prev_month: Option<(i32, u32)>,
    /// Own equities saved at the previous reconcile (the pool vintage).
    saved_own: HashMap<String, f64>,
    /// Monthly subscription (SGD) deducted from the fund at each month-start
    /// anchor (env BLENDED5_SUBSCRIPTION; 0 = off). Reduces the ledger base
    /// the fund compounds from — the sizing path carries the compounding
    /// cost; the cash itself leaves the rollup only in net-of-sub reporting.
    sub_monthly: f64,
    /// Cumulative subscription paid so far.
    cum_sub: f64,
}

impl FundLedger {
    /// py init: e_i = capital / N for every sleeve (50k fund → 12.5k each).
    pub fn new(names: &[String], capital: f64) -> Self {
        let share = capital / names.len() as f64;
        Self {
            e: names.iter().map(|n| (n.clone(), share)).collect(),
            prev_date: None,
            prev_month: None,
            saved_own: HashMap::new(),
            sub_monthly: 0.0,
            cum_sub: 0.0,
        }
    }

    /// Set the monthly subscription (SGD/month), deducted at every
    /// month-start anchor.
    pub fn set_subscription(&mut self, sgd_per_month: f64) {
        self.sub_monthly = sgd_per_month.max(0.0);
    }

    /// Cumulative subscription paid (SGD) — for net-of-sub reporting.
    pub fn cumulative_subscription(&self) -> f64 {
        self.cum_sub
    }

    /// Sleeve's fund-share equity book (None when the ledger doesn't cover
    /// the name — caller falls back to own-eq sizing).
    pub fn eq_for(&self, name: &str) -> Option<f64> {
        self.e.get(name).copied()
    }

    /// Session-boundary reconcile + month-end re-anchor. Driven by the
    /// sleeves' flushes; the FIRST flush of a new ET date does the
    /// reconcile against the current pool vintage (each sleeve's last
    /// pushed equity = the previous session's day-last); same-day flushes
    /// are no-ops.
    pub fn on_slot(&mut self, residual: f64, equities: &HashMap<String, f64>, d: chrono::NaiveDate) {
        use chrono::Datelike;
        if self.prev_date == Some(d) {
            return; // same session — reconcile fires once per day
        }
        let month = (d.year(), d.month());
        let new_month = self.prev_month.is_some_and(|pm| month > pm);
        if self.prev_date.is_some() {
            // Daily reconcile: e_i += Δown_eq_i (pool vintage = the
            // previous session's day-last). saved_own is seeded by the
            // first flush's pool observation (the 10k seeds — nothing has
            // traded yet), so the first session boundary reconciles
            // against the seed exactly like py.
            for (n, e) in self.e.iter_mut() {
                if let (Some(eq), Some(prev_eq)) = (equities.get(n), self.saved_own.get(n)) {
                    *e += eq - prev_eq;
                }
            }
            // Month-end EW re-anchor: py anchors at the last bar of the
            // month's last session; the next session's first slot carries
            // the same post-close equity (no trades in between). The
            // monthly subscription (if set) is deducted here — the fund
            // pays sub_monthly out of its equity before the EW split.
            if new_month {
                self.cum_sub += self.sub_monthly;
                let fund = residual + equities.values().sum::<f64>() - self.cum_sub;
                let share = fund / self.e.len() as f64;
                for e in self.e.values_mut() {
                    *e = share;
                }
            }
            let sum: f64 = self.e.values().sum();
            let fund = residual + equities.values().sum::<f64>() - self.cum_sub;
            debug_assert!(
                (sum - fund).abs() <= 1e-6 * fund.abs().max(1.0),
                "fund ledger invariant broken: Σe {sum} vs fund {fund}"
            );
        }
        self.prev_date = Some(d);
        self.prev_month = Some(month);
        self.saved_own = equities.clone();
    }
}

/// Env gate: fund-equity ledger sizing mode (default OFF = §8 own-eq).
pub fn env_fund_ledger() -> bool {
    std::env::var("BLENDED5_FUND_LEDGER").ok().as_deref() == Some("1")
}

/// Env gate: size every sleeve off the TOTAL pool equity (Python shared-fund
/// parity) instead of the per-sleeve fund-share e_i. Default OFF (unset or
/// any other value = today's e_i behavior, byte-identical). When ON the pool
/// must exist (multi replay runner); bare constructions fall back to the
/// e_i path. Rationale: Python sizes each leg off the full shared fund
/// equity, so per-sleeve e_i (= fund/N) prices books at ~1/4 scale and the
/// FundGate caps never bind — starving the floor-to-zero fade-cancel path
/// of cap pressure. Pool sizing restores Python-scale deployment; the caps,
/// floors, flats and cancel path are untouched (already ported + verified).
pub fn env_pool_sizing() -> bool {
    std::env::var("BLENDED5_POOL_SIZING").ok().as_deref() == Some("1")
}

/// Env gate: monthly subscription in SGD (default 0 = off).
pub fn env_subscription() -> f64 {
    std::env::var("BLENDED5_SUBSCRIPTION")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(0.0)
}

/// Wire the ledger from the pool init (names + fund capital). Reads
/// BLENDED5_SUBSCRIPTION. Called by `pool_init` — the backtest binaries
/// make NO separate ledger call.
fn ledger_init(names: &[String], capital: f64) {
    let mut led = FundLedger::new(names, capital);
    let sub = env_subscription();
    if sub > 0.0 {
        led.set_subscription(sub);
        tracing::info!("fund ledger: monthly subscription {sub:.2} SGD enabled");
    }
    FUND_LEDGER.with(|l| *l.borrow_mut() = Some(led));
    tracing::info!(
        "fund ledger installed: {} sleeves, e_i = {:.2} SGD each (capital {capital:.2})",
        names.len(),
        capital / names.len() as f64
    );
}

/// Clear the thread-local ledger (run end / tests).
#[allow(dead_code)]
pub fn ledger_clear() {
    FUND_LEDGER.with(|l| *l.borrow_mut() = None);
}

/// Per-participant last-pushed equity (the running vintage the ledger
/// reconciles against) — updated on every pool push.
static POOL_LAST_EQ: [AtomicU64; FUND_MAX_PARTICIPANTS] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
/// Presence mask: bit i set once participant i has pushed AT LEAST ONCE.
/// The ledger reconcile never fires before EVERY participant has pushed —
/// pre-push reads are zeros (a sleeve's flush reads the vintage BEFORE its
/// own chain pushes), and seeding `saved_own` from the day-1 zero vintage
/// would corrupt every later reconcile by +SE_SLEEVE per sleeve.
static POOL_SEEN: AtomicU32 = AtomicU32::new(0);

/// Per-participant last-pushed priced / fixed gross (f64 bits) — the peers'
/// freshest known gross for the per-sleeve cap classification. Legacy used
/// the engine-registry SLOT-START snapshot (peer gross identical for every
/// gate in the slot); here the statics roll forward on every push, so a
/// late completer sees its peers' CURRENT-slot (already-trimmed) pushes —
/// strictly tighter enforcement, same observable property: the LAST trim
/// of each slot pins the fund total under the cap.
static POOL_LAST_PRICED: [AtomicU64; FUND_MAX_PARTICIPANTS] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static POOL_LAST_FIXED: [AtomicU64; FUND_MAX_PARTICIPANTS] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

/// Fund inputs for the ledger reconcile: (residual, {name → last-pushed
/// sleeve eq SGD}). No-op-safe: empty map when the pool is absent.
pub(crate) fn pool_fund_inputs() -> (f64, HashMap<String, f64>) {
    let Some(ring) = pool() else { return (0.0, HashMap::new()) };
    let eqs = ring
        .names
        .iter()
        .enumerate()
        .map(|(i, n)| {
            (
                n.clone(),
                f64::from_bits(POOL_LAST_EQ[i].load(Ordering::Relaxed)),
            )
        })
        .collect();
    (f64::from_bits(POOL_RESIDUAL.load(Ordering::Relaxed)), eqs)
}

/// Sleeve flush hook: advance the ledger against the CURRENT pool vintage
/// (each sleeve's last push = the previous session's day-last). No-op when
/// the ledger is not installed (default env-off §8 mode) or the pool is
/// absent (bare unit constructions).
pub fn ledger_on_flush(et_date: chrono::NaiveDate) {
    FUND_LEDGER.with(|l| {
        if let Some(l) = l.borrow_mut().as_mut() {
            // Vintage gate: the reconcile fires only once EVERY participant
            // has pushed at least once (pre-push POOL_LAST_EQ reads are
            // zeros — seeding saved_own from them would add the full seed
            // to every e_i at the next session boundary).
            let Some(ring) = pool() else { return };
            let full = (1u32 << ring.names.len()) - 1;
            if POOL_SEEN.load(Ordering::Relaxed) != full {
                return;
            }
            let (residual, eqs) = pool_fund_inputs();
            if eqs.is_empty() {
                return;
            }
            l.on_slot(residual, &eqs, et_date);
        }
    });
}

/// Total pool equity (residual + Σ sleeve eq) for pool-share sizing.
/// `None` when the pool is absent (bare unit constructions, solo runs).
pub fn pool_total_equity() -> Option<f64> {
    let (residual, eqs) = pool_fund_inputs();
    if eqs.is_empty() {
        return None;
    }
    let total = residual + eqs.values().sum::<f64>();
    if total.is_finite() {
        Some(total)
    } else {
        None
    }
}

/// Sleeve's sizing base: the fund-share book when the ledger mode is on and
/// covers the sleeve, else `None` (caller falls back to own equity).
/// `BLENDED5_POOL_SIZING=1` overrides with total pool equity (Python
/// shared-fund parity); absent pool falls back to the e_i path.
pub fn sizing_base(name: &str) -> Option<f64> {
    if !env_fund_ledger() {
        return None;
    }
    if env_pool_sizing() {
        if let Some(total) = pool_total_equity() {
            return Some(total);
        }
    }
    FUND_LEDGER.with(|l| l.borrow().as_ref().and_then(|l| l.eq_for(name)))
}

#[cfg(test)]
mod two_phase_tests {
    use super::*;

    fn book(pairs: &[(&str, i64)]) -> BTreeMap<String, i64> {
        pairs.iter().map(|(s, q)| (s.to_string(), *q)).collect()
    }

    #[test]
    fn classify_scale_under_budget_passes_through() {
        let (s, v, l) = classify_scale(100_000.0, 50_000.0, 0.0, 20_000.0, 1.95);
        assert_eq!((s, v, l), (1.0, false, false));
    }

    #[test]
    fn classify_scale_peer_deduction_matches_legacy_math() {
        // (cap*eq − fixed − peer)/priced = (1.95*100k − 0 − 100k)/100k = 0.95
        let (s, v, l) = classify_scale(100_000.0, 100_000.0, 0.0, 100_000.0, 1.95);
        assert!((s - 0.95).abs() < 1e-12);
        assert!((v, l) == (true, false));
    }

    #[test]
    fn classify_scale_nan_fund_fail_open() {
        let (s, v, l) = classify_scale(f64::NAN, 50_000.0, 0.0, 0.0, 1.95);
        assert_eq!((s, v, l), (1.0, false, false));
    }

    #[test]
    fn central_scale_uniform_single_factor() {
        // Totals 200k priced vs cap 1.95*100k=195k → scale 0.975; every
        // nonzero entry floored, zeros kept.
        let (s, v, l) = central_scale_panel(200_000.0, 0.0, 100_000.0, 1.95);
        assert!((s - 0.975).abs() < 1e-12);
        assert!((v, l) == (true, false));
        let b = book(&[("A", 100), ("B", -51), ("C", 0)]);
        let out = apply_uniform_scale(&b, s, false);
        assert_eq!(out["A"], 97); // floor(97.5)
        assert_eq!(out["B"], -49); // floor(49.725), sign kept
        assert_eq!(out["C"], 0);
    }

    #[test]
    fn central_scale_under_budget_identity() {
        let (s, v, l) = central_scale_panel(100_000.0, 0.0, 100_000.0, 1.95);
        assert_eq!((s, v, l), (1.0, false, false));
        let b = book(&[("A", 100)]);
        assert_eq!(apply_uniform_scale(&b, s, false), b);
    }

    #[test]
    fn central_scale_insolvent_liquidates() {
        let (s, v, l) = central_scale_panel(50_000.0, 0.0, 0.0, 1.95);
        assert_eq!((s, v, l), (0.0, true, true));
        let b = book(&[("A", 100)]);
        let out = apply_uniform_scale(&b, s, true);
        assert_eq!(out["A"], 0);
    }
}

#[cfg(test)]
mod pool_sizing_tests {
    use super::*;

    #[test]
    fn pool_sizing_flag_defaults_off() {
        // No other test in this binary touches BLENDED5_POOL_SIZING, so the
        // process env is clean: default must be OFF (today's behavior).
        assert!(!env_pool_sizing());
    }

    #[test]
    fn sizing_base_ignores_pool_flag_without_ledger() {
        // LEDGER unset in unit tests (env-gated) → None regardless of the
        // pool-sizing seam: default-off paths are untouched.
        assert_eq!(sizing_base("blended5_tc1"), None);
    }
}