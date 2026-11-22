//! FundGate — the §8 sleeve-autonomy fund layer as a per-sleeve `GateLogic`.
//!
//! PLAN.md §9.3 S2: the legacy `FundBook` (fund.rs) re-weighing is decomposed
//! into ONE gate instantiated per sleeve, chained AFTER the sleeve's weight
//! gates and BEFORE its `SharesGate`. The gate ports the legacy fire
//! classification + scale pass EXACTLY (fund.rs `on_row` steps 4-8 / fund.py
//! order), operating on a SHARE-SPACE book the POOL owns for it:
//!
//! **The book carry (the load-bearing legacy semantics).** Heads are
//! delta-emitters — connors emits only entries + exit flats; held positions
//! are carried implicitly. The legacy `FundBook` kept each sub's FULL book
//! (`sub_cur`: fresh chain outputs overwrite, unnamed syms retain their last
//! value, explicit zeros KEPT) and re-emitted the full basket every row, so
//! the scale pass reached HELD positions. Without the same carry, a cap
//! scales only fresh entries and the held book compounds unbounded (the
//! first P2 solo run: connors 10k → 737k, cash −3.9M — the cap never touched
//! held positions). The POOL therefore owns ONE `BTreeMap<sym, i64>` per
//! participant (inside `PoolState`, keyed by producer idx) with exactly the legacy
//! `sub_cur` semantics:
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
//! `cap` = EOD 1.95 when the row is in the session CLOSE WINDOW
//! (`ctx.close_window`, stamped by the sleeve from the SessionCalendar)
//! else INTRA 4.0 — battery-exact constants re-used from `fund.rs`.
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

use chrono::Datelike;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::mpsc::{MpscRingBuffer, MpscRingBufferProducer};
// DIAG seam — gated: the diag module lives in the backtest-only in-memory
// engine. Live runs with it off (`diag_on()` helper below).
#[cfg(feature = "backtest")]
use crate::backtester::methods::in_memory::diag;
#[cfg(feature = "backtest")]
use crate::backtester::setup::config::BacktestMode;
use crate::strategy::blended5_fund::fund::{EOD_CAP, INTRA_CAP, USD_FX};
use crate::strategy_wrappers::gate::GateLogic;
use crate::strategy_wrappers::intent::Intent;
use crate::strategy_wrappers::payload::{BarFrame, Payload, ShareOut};
use crate::strategy_wrappers::strategy::ClockCtx;

/// DIAG seam (backtest-only; live = off).
#[cfg(feature = "backtest")]
fn diag_on() -> bool {
    diag::diag_fills()
}

#[cfg(not(feature = "backtest"))]
fn diag_on() -> bool {
    false
}

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
// per slot per sleeve) through the `MpscRingBuffer` vintage carrier
// (`PoolState.ring`, Pass 3b): `FundInput` snapshots move BY VALUE through the
// ring's MaybeUninit cells; ONE ring position = ONE 5-min flush
// (`bucket = mso / 5` — 78 positions per session < FUND_RING = 128, so a
// session never self-collides) and the flush key
// `(days_from_year_start << 16) | mso` recycles positions across sessions
// through the ring's mismatch branch. The COMPLETING push of a slot (mask
// reaching the ring's ACTIVE count = the registered participant set)
// returns the complete vintage in producer-idx order == sleeve-name order
// and runs the ONE uniform central trim over it (legacy single-book
// semantics: one factor for every entry), handing the scaled sheet to the
// sleeve layer: ONE pooled emission on behalf of ALL sleeves (EmitOrders
// variant; fills route to the right sleeve account by order_ref — the
// approved engine seam). Scaled books are written back into the POOL-OWNED
// per-participant books (`PoolState.books`) so each gate's next flush carries the
// trimmed book (order-independent).
//
// Single-thread-class engine (sequential single-pass replay): the atomics
// exist for static sharing, not for cross-thread synchronisation.
// ═══════════════════════════════════════════════════════════════════════

use std::cell::UnsafeCell;
use std::sync::{Arc, OnceLock};

pub const FUND_RING: usize = 128;
/// Minutes per flush position: one ring position = one 5-min bar flush
/// (the fund's bar cadence). 390-min session / 5 = 78 positions < 128.
pub const FUND_BUCKET_SPAN: u32 = 5;

/// Ring producer capacity = the fund's sleeve count (const generic on
/// `MpscRingBuffer` — minting beyond it panics by design; the blended5 fund
/// has exactly four sleeves).
pub const FUND_PRODUCERS: usize = 4;

/// One participant's slot vintage — the ring's payload (Pass 3b: the push
/// runs through `MpscRingBuffer`, `FundInput` moves BY VALUE through its
/// MaybeUninit cells — no f64 bit-packing anywhere). Frozen snapshot at
/// push time: owner + sleeve equity + gross components + the FULL absolute
/// book (self-describing — the trim attributes straight from the vintage).
#[derive(Debug)]
pub struct FundInput {
    pub own: String,
    pub eq_sgd: f64,
    pub priced: f64,
    pub fixed: f64,
    pub book: BTreeMap<String, i64>,
}

/// THE pool: ring carrier + participant names + pool-owned books + fund
/// residual + the sizing vintage. Every piece of cross-sleeve fund state
/// lives here (user ruling: no bare pool statics) — the struct defines its
/// own interior mutability (`UnsafeCell` per field; the ONE safety argument
/// below covers all of them).
pub struct PoolState {
    ring: Arc<MpscRingBuffer<FundInput, FUND_RING, FUND_PRODUCERS>>,
    /// Participant names, appended at mint; index == producer idx == mint
    /// order == sleeve-name order (Parity-risk #1 by construction). This
    /// list IS the registry — no separate registration static.
    names: UnsafeCell<Vec<String>>,
    /// Pool-owned per-participant books (shared-state ruling): absolute
    /// signed share counts, fresh emissions overwrite, unnamed syms
    /// retain, explicit zeros KEPT — the UNTRIMMED collect panel whose
    /// centrally-trimmed form the completer writes back at `trim`.
    books: [UnsafeCell<BTreeMap<String, i64>>; FUND_PRODUCERS],
    /// Unbooked fund capital (set once at `init`).
    residual: UnsafeCell<f64>,
    /// The fund capital this pool was initialised with (the ONE capital
    /// source — init_app's const / the binary's config). The LIVE month-end
    /// reallocation derives `residual = capital − N·SE_SLEEVE` from it.
    capital: UnsafeCell<f64>,
    /// Month-end equal-weight reallocation trigger (LIVE): the last month
    /// (year, month) the reallocation ran. A new calendar month's first
    /// flush re-allocates the strategies' capital across the sleeves in
    /// the DB (`realloc_equal_weights`); restarts reset this (in-memory),
    /// which simply re-anchors — restart-as-anchor semantics.
    realloc_month: UnsafeCell<Option<(i32, u32)>>,
    /// Ledger vintage: each sleeve's LAST-PUSHED equity (name → eq SGD)
    /// plus pushed-at-least-once flags. The FLUSH-TIME day reconcile reads
    /// this pair — at the day's first flush it holds the PREVIOUS
    /// session's day-last equities (today's pushes have not happened yet),
    /// exactly the vintage `FundLedger::on_slot` documents.
    last_eqs: UnsafeCell<HashMap<String, f64>>,
    seen: UnsafeCell<Vec<bool>>,
    /// FUND-level run params recorded at mint (one per participant,
    /// parallel to `names`): the m_scale lever and the monthly
    /// subscription. `init` asserts all participants agree (a mismatch is
    /// a config bug — fail loud) and installs the ledger with the agreed
    /// subscription.
    fund_scales: UnsafeCell<Vec<f64>>,
    fund_subs: UnsafeCell<Vec<f64>>,
    /// `true` once `init` ran (the binary calls it after all sleeves
    /// construct). Bare constructions mint a pool but never ready it —
    /// unready pools fail CLOSED at the gate (emit nothing).
    ready: UnsafeCell<bool>,
    /// The lane this pool runs in (backtest builds only). The cfg'd CRUD
    /// seams (the sleeve's working-state reads, the realloc's CASH write)
    /// pass it through instead of assuming the in-memory lane — the
    /// Db-multi replay initialises the pool with `BacktestMode::Db`.
    #[cfg(feature = "backtest")]
    mode: UnsafeCell<BacktestMode>,
}

// SAFETY (single-threaded engine — ONE argument covers every field): the
// UnsafeCells/atomics exist for static sharing, not for cross-thread
// synchronisation. Each participant's cell (names slot, books[idx]) is
// written by its own gate's flush and — for the book — by the same-thread
// completer's trim; the ring is the user-owned `MpscRingBuffer` (its own
// Send/Sync impls assert the same discipline). No reference derived from a
// shared `&PoolState` crosses a thread boundary in this engine.
unsafe impl Sync for PoolState {}

static POOL: OnceLock<PoolState> = OnceLock::new();

impl PoolState {
    /// The pool singleton accessor (`None` only before the first
    /// `FundGate::new` / `init`).
    pub fn current() -> Option<&'static PoolState> {
        POOL.get()
    }

    fn new() -> Self {
        Self {
            ring: Arc::new(MpscRingBuffer::new()),
            names: UnsafeCell::new(Vec::new()),
            books: core::array::from_fn(|_| UnsafeCell::new(BTreeMap::new())),
            residual: UnsafeCell::new(0.0),
            capital: UnsafeCell::new(0.0),
            realloc_month: UnsafeCell::new(None),
            last_eqs: UnsafeCell::new(HashMap::new()),
            seen: UnsafeCell::new(Vec::new()),
            fund_scales: UnsafeCell::new(Vec::new()),
            fund_subs: UnsafeCell::new(Vec::new()),
            ready: UnsafeCell::new(false),
            #[cfg(feature = "backtest")]
            mode: UnsafeCell::new(BacktestMode::InMemory),
        }
    }

    /// The lane the pool was initialised for (backtest builds only). The
    /// cfg'd CRUD call sites pass it through — `InMemory` for the pinned
    /// in-memory replay, `Db` for the Db-multi replay.
    #[cfg(feature = "backtest")]
    pub fn mode(&self) -> BacktestMode {
        unsafe { *self.mode.get() }
    }

    /// Live unallocated-residual report (the realloc rewrites it through
    /// `&self`, so readers must go through the UnsafeCell too). Used by the
    /// Db-multi rollup: after the strategy-layer month-end realloc zeroes
    /// and distributes the residual, the rollup must stop adding the frozen
    /// initial value.
    pub fn residual(&self) -> f64 {
        unsafe { *self.residual.get() }
    }

    /// Mint ONE producer handle + append the participant name — identity
    /// and registration in ONE act (`FundGate::new`). The mint happens
    /// FIRST so a panic (beyond `FUND_PRODUCERS`) cannot leave a name
    /// behind; duplicates are a loud construction error (a second
    /// `FundGate::new` for one sleeve would double-mint and desync the
    /// name/idx mapping).
    fn mint(&self, own: &str, m_scale: f64, subscription: f64) -> MpscRingBufferProducer<FundInput, FUND_RING, FUND_PRODUCERS> {
        let producer = self.ring.get_new_producer();
        let names = unsafe { &mut *self.names.get() };
        assert!(
            !names.iter().any(|n| n == own),
            "fund pool: duplicate participant registration '{own}'"
        );
        names.push(own.to_string());
        unsafe { &mut *self.seen.get() }.push(false);
        unsafe { &mut *self.fund_scales.get() }.push(m_scale);
        unsafe { &mut *self.fund_subs.get() }.push(subscription);
        producer
    }

    /// Participant `idx`'s pool-owned book (read view): the push snapshot +
    /// the `fund_gross` sweep.
    fn book(&self, idx: usize) -> &BTreeMap<String, i64> {
        unsafe { &*self.books[idx].get() }
    }

    /// THE one mutation entry into a pool-owned book. The fresh overwrite
    /// collects fresh pairs then inserts them in ONE call; the trim
    /// write-back replaces the map wholesale (`|b| *b = scaled`).
    fn update_book(&self, idx: usize, f: impl FnOnce(&mut BTreeMap<String, i64>)) {
        unsafe { f(&mut *self.books[idx].get()) }
    }

    /// `true` once `init` ran — the gate's participant check (unready pools
    /// fail CLOSED at the gate).
    fn ready(&self) -> bool {
        unsafe { *self.ready.get() }
    }

    /// Month reallocation trigger (called from the sleeve's flush path in
    /// EVERY regime): `true` on the FIRST flush of a new calendar month,
    /// at which point the caller runs `realloc_equal_weights`. The first
    /// recorded month does NOT trigger (month 1 of a run never reallocates
    /// — the ledger's original behavior). Memory-only: a restart records
    /// the current month without reallocating (restart-safety is
    /// structural — the equalized accounts persist).
    pub fn advance_month_anchor(&self, d: chrono::NaiveDate) -> bool {
        let month = (d.year(), d.month());
        let prev = unsafe { &mut *self.realloc_month.get() };
        match *prev {
            Some(pm) if month > pm => {
                *prev = Some(month);
                true
            }
            Some(_) => false,
            None => {
                *prev = Some(month);
                false
            }
        }
    }

    /// UNIFIED month-end equal-weight reallocation (the engine's allocation
    /// engine, BOTH regimes): the vintage is the pool's LAST-PUSHED equity
    /// (previous day-last — the identical source in the replay and live,
    /// since the trigger fires before the day's first push); fund =
    /// residual + Σ vintage; share = fund/N; each sleeve's `CASH:SGD` row
    /// moves by share − eq (positions untouched) via the mode-blind
    /// `update_positions_additive` (SQL additive upsert live/Db, the
    /// strategy's InMemoryState additive update in the replay). The pool's
    /// residual is distributed into the sleeves (set to 0) — the moved
    /// accounts carry what the ledger's notional re-anchor used to carry,
    /// so the sizing trajectories agree. Subscriptions are not yet
    /// supported here (the pool's recorded subscription, if any, is
    /// ignored with a warning).
    pub fn realloc_equal_weights(
        &self,
        handle: &tokio::runtime::Handle,
        pool: &sqlx::PgPool,
    ) -> Result<(), String> {
        #[cfg(feature = "backtest")]
        use crate::backtester::setup::config::BacktestMode;
        use crate::database::models::{
            AssetType, CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys,
        };
        use crate::database::models_crud::current_positions::current_positions::{
            CurrentPositionsCRUD, CurrentPositionsOps, CurrentPositionsPrimaryKeys,
            CurrentPositionsUpdateKeys,
        };

        let names = unsafe { &*self.names.get() }.clone();
        let sub = unsafe { &*self.fund_subs.get() }.first().copied().unwrap_or(0.0);
        if sub > 0.0 {
            tracing::warn!(
                "fund subscription set — subscriptions are not yet \
                 supported in the month reallocation; ignoring"
            );
        }
        // Vintage = last-pushed equity per sleeve (the flush-time day
        // reconcile's source). An incomplete vintage (a sleeve has never
        // pushed — month 1 of a run) skips the reallocation; the month
        // marker is already consumed, matching the old ledger where month 1
        // never anchored.
        let vintage = unsafe { &*self.last_eqs.get() }.clone();
        if vintage.len() < names.len() {
            return Ok(());
        }
        let residual = unsafe { *self.residual.get() };
        let fund = residual + vintage.values().sum::<f64>();
        let share = fund / names.len() as f64;
        tracing::info!(
            fund = format!("{fund:.2}"),
            share = format!("{share:.2}"),
            "month-end equal-weight reallocation"
        );
        for name in &names {
            let eq = vintage.get(name).copied().unwrap_or(0.0);
            let delta = share - eq;
            if delta.abs() < 0.005 {
                continue;
            }
            // Bridge split (see the sleeve's load_db_working_state): the
            // write bridges depend on the LANE, not just the feature — the
            // in-memory lane's CRUD write arm is an instant thread-local
            // account write (poll it INLINE on the owning thread); a Db
            // write (live OR the Db-multi replay) is a sqlx query needing
            // the reactor — spawn onto the runtime.
            {
                #[cfg(feature = "backtest")]
                let write = if matches!(self.mode(), BacktestMode::InMemory) {
                    futures::executor::block_on(async {
                        let cp = CurrentPositionsCRUD::from(&AssetType::Stock, pool.clone());
                        let pk =
                            CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
                                strategy: name.clone(),
                                stock: "CASH:SGD".to_string(),
                                primary_exchange: "".to_string(),
                                currency: "SGD".to_string(),
                            });
                        let uk =
                            CurrentPositionsUpdateKeys::Stock(CurrentStockPositionsUpdateKeys {
                                quantity: Some(delta),
                                avg_price: Some(1.0),
                                last_updated: None,
                            });
                        cp.update_positions_additive(pk, uk, BacktestMode::InMemory)
                            .await
                    })
                } else {
                    let jh = handle.spawn({
                        let pool = pool.clone();
                        let name = name.clone();
                        async move {
                            let cp = CurrentPositionsCRUD::from(&AssetType::Stock, pool);
                            let pk = CurrentPositionsPrimaryKeys::Stock(
                                CurrentStockPositionsPrimaryKeys {
                                    strategy: name.clone(),
                                    stock: "CASH:SGD".to_string(),
                                    primary_exchange: "".to_string(),
                                    currency: "SGD".to_string(),
                                },
                            );
                            let uk = CurrentPositionsUpdateKeys::Stock(
                                CurrentStockPositionsUpdateKeys {
                                    quantity: Some(delta),
                                    avg_price: Some(1.0),
                                    last_updated: None,
                                },
                            );
                            cp.update_positions_additive(pk, uk, BacktestMode::Db).await
                        }
                    });
                    futures::executor::block_on(jh)
                        .map_err(|e| format!("realloc write join: {e}"))?
                };
                #[cfg(not(feature = "backtest"))]
                let write = {
                    let jh = handle.spawn({
                        let pool = pool.clone();
                        let name = name.clone();
                        async move {
                            let cp = CurrentPositionsCRUD::from(&AssetType::Stock, pool);
                            let pk = CurrentPositionsPrimaryKeys::Stock(
                                CurrentStockPositionsPrimaryKeys {
                                    strategy: name.clone(),
                                    stock: "CASH:SGD".to_string(),
                                    primary_exchange: "".to_string(),
                                    currency: "SGD".to_string(),
                                },
                            );
                            let uk = CurrentPositionsUpdateKeys::Stock(
                                CurrentStockPositionsUpdateKeys {
                                    quantity: Some(delta),
                                    avg_price: Some(1.0),
                                    last_updated: None,
                                },
                            );
                            cp.update_positions_additive(pk, uk).await
                        }
                    });
                    futures::executor::block_on(jh)
                        .map_err(|e| format!("realloc write join: {e}"))?
                };
                write
            }
            .map_err(|e| format!("realloc write '{name}': {e}"))?;
        }
        // The pool's unallocated capital is now distributed into the
        // sleeves' accounts.
        unsafe { *self.residual.get() = 0.0 };
        // The vintage moves with the accounts: the ledger's next daily
        // reconcile (e_i += eq − saved_own) must not ingest the reallocation
        // delta as P&L — re-base last_eqs to the post-move share so the
        // drift resumes from the anchor exactly like the pre-unified ledger.
        unsafe {
            let le = self.last_eqs.get().as_mut().unwrap();
            for name in &names {
                le.insert(name.clone(), share);
            }
        }
        Ok(())
    }

    /// Wire the ledger from the pool init (names + fund capital +
    /// subscription). Called by `init`/`init_live` — the binaries make NO
    /// separate ledger call.
    fn install_ledger(names: &[String], capital: f64, sub: f64) {
        let mut led = FundLedger::new(names, capital);
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

    /// Push THIS participant's slot state through the pooled ring carrier;
    /// returns the complete vintage when THIS push completed the slot (the
    /// completing gate/sleeve owns the central trim for the slot).
    fn push(
        &self,
        producer: &MpscRingBufferProducer<FundInput, FUND_RING, FUND_PRODUCERS>,
        own: &str,
        et_date: chrono::NaiveDate,
        mso: i32,
        eq: f64,
        book: &BTreeMap<String, i64>,
        priced: f64,
        fixed: f64,
    ) -> Option<Vec<FundInput>> {
        // One ring position = one flush bucket (`FUND_BUCKET_SPAN` minutes —
        // the fund's bar cadence: mso multiples of 5 → buckets 0..=77 <
        // FUND_RING, so a session never self-collides).
        let bucket = (mso.max(0) as u32) / FUND_BUCKET_SPAN;
        // Flush key (days_from_year_start << 16) | bucket, keyed INSIDE the
        // ring. BUG-1-class note: ordinal() wraps every calendar year, but
        // slots complete + clear every session, so a wrapped key never finds a
        // stale-full mask.
        let days = et_date.ordinal() as u32;
        let input = FundInput {
            own: own.to_string(),
            eq_sgd: eq,
            priced,
            fixed,
            // Snapshot clone (vintage determinism): the pool book persists
            // after the push (unnamed-sym retention; the completer's trim
            // write-back replaces it wholesale).
            book: book.clone(),
        };
        let vintage = producer.push(input, bucket, days);
        // Ledger vintage: last-pushed equity per sleeve + pushed-at-least-once
        // flag — the pair the flush-time day reconcile reads (`reconcile_day`).
        unsafe {
            self.last_eqs
                .get()
                .as_mut()
                .unwrap()
                .insert(own.to_string(), eq);
            let seen = &mut *self.seen.get();
            let idx = (&*self.names.get())
                .iter()
                .position(|n| n == own)
                .expect("pusher must be a minted participant");
            seen[idx] = true;
        }
        if diag_on() {
            tracing::info!(
                slot = %et_date,
                bucket,
                owner = own,
                eq,
                priced,
                fixed,
                book_len = book.len(),
                completed = vintage.is_some(),
                "[POOLED] push"
            );
        }
        vintage
    }

    /// Phase 2 of the two-phase reconcile: uniform central trim over the
    /// completed slot's UNTRIMMED vintage (the ring's returned
    /// `Vec<FundInput>` in producer-idx order == sleeve-name order),
    /// write-back into the pool-owned per-participant books (direct
    /// `update_book` store), scaled sheet out. `eod_edge` comes from the
    /// completer's ctx (one edge per slot).
    fn trim(
        &self,
        vintage: Vec<FundInput>,
        slot_date: chrono::NaiveDate,
        eod_edge: bool,
    ) -> Vec<ShareOut> {
        let cap = if eod_edge { EOD_CAP } else { INTRA_CAP };
        let residual = unsafe { *self.residual.get() };
        let fund_eq_sgd = residual + vintage.iter().map(|v| v.eq_sgd).sum::<f64>();
        let fund_eq_usd = fund_eq_sgd / USD_FX;
        let priced_total = vintage.iter().map(|v| v.priced).sum::<f64>();
        let fixed_total = vintage.iter().map(|v| v.fixed).sum::<f64>();
        let (scale, viol, liquidate) =
            central_scale_panel(priced_total, fixed_total, fund_eq_usd, cap);
        if viol {
            if eod_edge {
                N_VIOL_EOD.fetch_add(1, Ordering::Relaxed);
            } else {
                N_VIOL_INTRA.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Diag strings collected BEFORE the consuming loop as well.
        let per_owner: Vec<String> = if diag_on() {
            vintage
                .iter()
                .map(|v| {
                    format!(
                        "{} eq={:.0} gross={:.0}(|q|={} book={})",
                        v.own,
                        v.eq_sgd,
                        v.priced + v.fixed,
                        v.book.values().map(|q| q.abs()).sum::<i64>(),
                        v.book.len()
                    )
                })
                .collect()
        } else {
            Vec::new()
        };
        let mut sheet: Vec<ShareOut> = Vec::new();
        for (i, input) in vintage.into_iter().enumerate() {
            let name = input.own;
            let scaled = apply_uniform_scale(&input.book, scale, liquidate);
            if (liquidate || scale < 1.0) && scaled != input.book {
                if eod_edge {
                    N_FIRE_EOD.fetch_add(1, Ordering::Relaxed);
                } else {
                    N_FIRE_INTRA.fetch_add(1, Ordering::Relaxed);
                }
            }
            for (sym, q) in scaled.iter() {
                sheet.push(ShareOut::new(
                    sym.clone(),
                    *q,
                    Some(name.clone()),
                    None,
                    None,
                ));
            }
            // Direct write-back into the pool-owned book — MOVED, no clone:
            // the completer (same thread) is the sanctioned cross-index
            // writer, and the next flush of participant i reads THIS book
            // (no key guard needed — the write happens exactly once per
            // slot trim).
            self.update_book(i, |b| *b = scaled);
        }
        if diag_on() {
            let sheet_q: i64 = sheet.iter().map(|s| s.qty.unsigned_abs() as i64).sum();
            tracing::info!(
                slot = %slot_date,
                fund_eq_sgd = format!("{fund_eq_sgd:.2}"),
                scale = format!("{scale:.4}"),
                liquidate,
                sheet_len = sheet.len(),
                sheet_abs_q = sheet_q,
                participants = per_owner.join(" | "),
                bg = if eod_edge { "close_window" } else { "intra" },
                "[POOLED] central trim"
            );
        }
        sheet
    }

    /// FLUSH-TIME day reconcile (the old `ledger_on_flush` seam — restored
    /// after the trim-time placement proved to change pinned results: the
    /// trim of the day's first slot reconciles against TODAY-OPEN
    /// equities, re-anchoring every daily e_i update to open-anchored
    /// windows and diverging the Nov-6 pins at the 2021-01-08 first slot).
    /// Each sleeve's flush calls this BEFORE sizing; `on_slot`'s per-day
    /// guard makes it once per ET date. At the day's first flush the
    /// vintage is every sleeve's LAST-PUSHED equity = the previous
    /// session's day-last (today's pushes have not happened yet) — the
    /// documented contract. The full-mask gate mirrors the old POOL_SEEN:
    /// pre-push reads are zeros, and seeding `saved_own` from them would
    /// corrupt every later reconcile by +SE_SLEEVE per sleeve.
    pub fn reconcile_day(&self, et_date: chrono::NaiveDate) {
        let residual = unsafe { *self.residual.get() };
        FUND_LEDGER.with(|l| {
            if let Some(l) = l.borrow_mut().as_mut() {
                let seen = unsafe { &*self.seen.get() };
                if seen.iter().any(|&s| !s) {
                    return;
                }
                let names = unsafe { &*self.names.get() };
                if names.is_empty() {
                    return;
                }
                let eqs = unsafe { &*self.last_eqs.get() }.clone();
                l.on_slot(residual, &eqs, et_date);
            }
        });
    }
}

/// Test-only explicit init: the ledger tests drive the pool WITHOUT live
/// sleeve constructions (no FundGate::new registrations), so names are
/// supplied directly + the ledger wiring is identical to production. A
/// second call in the test binary is a silent OnceLock no-op (same names).
#[cfg(test)]
pub fn pool_test_init(names: &[String], residual: f64, ledger_capital: f64) {
    let p = POOL.get_or_init(PoolState::new);
    unsafe {
        *p.names.get() = names.to_vec();
        // Default fund-level params (m_scale 2.0 / subscription 0.0 — the
        // pin defaults; the tests exercise the ledger, not the params).
        *p.fund_scales.get() = vec![2.0; names.len()];
        *p.fund_subs.get() = vec![0.0; names.len()];
        *p.residual.get() = residual;
        *p.ready.get() = true;
    }
    PoolState::install_ledger(names, ledger_capital, 0.0);
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
// 1, `PoolState::push`); the COMPLETING push runs the ONE uniform central
// trim over the complete vintage (phase 2, `PoolState::trim`) and the
// scaled books are written back DIRECTLY into the pool-owned per-participant
// books so each gate's next flush carries the trimmed book
// (order-independent). Fire/viol counters are slot-level now: one viol per
// over-budget slot, one fire per sleeve whose book actually changed.
//
// TWO consumption pathways, ONE trim:
// - the COMPLETING GATE (`pooled_intents`): sheet → attributed per-share
//   intents → SharesGate (foreign pass-through, verbatim) → the chain
//   output IS the fund's sheet → the completing sleeve computes deltas →
//   MKT orders with order_ref = owner;
// - a BROKE completer (`push_carried` → this): no chain ran, so the
//   sleeve converts the sheet to orders directly.
/// The COMPLETING gate's pooled branch: sheet → ATTRIBUTED per-share
/// intents (absolute book counts). Emitting through the chain tail keeps
/// the payload contract: `run_chain` returns `Payload::Shares` and the
/// sleeve reads the attributed sheet from there. Empty sheet (e.g. every
/// book all-zero) → no intents → the sleeve emits `NoAction`.
fn pooled_intents(
    p: &PoolState,
    vintage: Vec<FundInput>,
    slot_date: chrono::NaiveDate,
    eod_edge: bool,
) -> Payload {
    let sheet = p.trim(vintage, slot_date, eod_edge);
    let mut out: crate::strategy_wrappers::intent::IntentMap = Vec::with_capacity(sheet.len());
    for s in sheet {
        let ShareOut {
            sym, qty: q, owner, ..
        } = s;
        // q == 0 → explicit flatten. Encoded as per_share(side=0, |q|=0):
        // the foreign SharesGate lane debug-asserts per-share book counts,
        // so the flat MUST stay per-share — side==0 routes it to
        // (sym, 0, owner) and the sleeve's delta order turns the zero
        // target into a full liquidation of the owner's carried position.
        // limit_px/oca_group explicitly reset: trimmed-sheet intents never
        // inherit stale tags (sheet entries here carry None by construction).
        let it = Intent::per_share(q.signum() as i32, q.abs() as f64, 1.0);
        out.push((
            sym,
            Intent {
                strategy: owner,
                limit_px: None,
                oca_group: None,
                ..it
            },
        ));
    }
    Payload::Intents(out)
}

/// The per-sleeve fund cap gate. One instance per sleeve, chained before
/// `SharesGate` (see `sleeve.rs` `with_fund_gate`).
pub struct FundGate {
    /// Owning sleeve's engine strategy name ("blended5_<key>") — excluded
    /// from the peer gross (its own gross comes from its own book).
    own: String,
    /// Last-known-price cache (legacy `FundBook::last_px` idiom: freshly
    /// printed closes extend the map; unprinted syms price at the last
    /// seen close; never-priced syms contribute 0 = `fixed`).
    last_px: HashMap<String, f64>,
    /// Participant identity: the producer handle minted from the pool at
    /// construction — `get_producer_idx()` is the pool index keying the
    /// pool-owned books (mint order == names order == sleeve-name order,
    /// Parity-risk #1 by construction). The FULL target book itself lives
    /// in the pool, legacy `sub_cur` semantics: signed share counts, fresh
    /// emissions overwrite, unnamed syms retain, explicit zeros KEPT — the
    /// UNTRIMMED collect panel whose centrally-trimmed form the completer
    /// writes back directly.
    producer: MpscRingBufferProducer<FundInput, FUND_RING, FUND_PRODUCERS>,
}

// SAFETY: single-threaded engine discipline. Strategies structurally
// require `Sync` (`strategy_enum!` / `SleeveRunner` impls), and the
// producer handle inside `FundGate` is auto-`!Sync` because its raw
// pointers alias ring memory. The contract this impl asserts: every
// `FundGate` lives on its owning sleeve's thread; the handle is used by
// that ONE thread only (concurrent `push` from two threads would race the
// plain payload cells — a contract break, documented the same way on
// `MpscRingBufferProducer`); the pool state it reaches through the idx is
// `PoolState` (one `unsafe impl Sync`, same single-thread argument) and
// only ever touched same-thread (flush overwrite + same-thread completer
// trim). No reference derived through a `&FundGate` crosses a thread
// boundary in this engine.
unsafe impl Sync for FundGate {}

impl FundGate {
    pub fn new(own: String, m_scale: f64, subscription: f64) -> Self {
        // Identity + registration in ONE act: the producer handle minted
        // from the pool IS the participant identity (`get_producer_idx()`),
        // and `mint` appends the sleeve name + its FUND-level run params
        // (m_scale / subscription) to the pool in the same act (mint order
        // == names order == sleeve-name order, Parity-risk #1 by
        // construction; minting beyond FUND_PRODUCERS panics — loud, never
        // silent). The FIRST gate construction creates the pool;
        // `pool_init` readies it before any flush.
        let producer = POOL.get_or_init(PoolState::new).mint(&own, m_scale, subscription);
        Self {
            own,
            last_px: HashMap::new(),
            producer,
        }
    }

    /// Fresh close for `sym` (extending `last_px`, legacy gross_pv idiom).
    fn price_for(&mut self, sym: &str, bars: &BarFrame) -> f64 {
        match bars
            .get(sym)
            .map(|b| b.get_price())
            .filter(|p| p.is_finite() && *p > 0.0)
        {
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
            return if bars.contains_key(sym) {
                Some(0)
            } else {
                None
            };
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
                return if bars.contains_key(sym) {
                    Some(0)
                } else {
                    None
                };
            }
            Some(q as i64 * it.side as i64)
        } else {
            // dollars: absent/unprintable key = OMIT (carry stands).
            let px = bars
                .get(sym)
                .map(|b| b.get_price())
                .filter(|p| p.is_finite() && *p > 0.0)?;
            let raw = (it.n_base * it.sc) * it.w; // battery association: (N*sc)*w
            if !raw.is_finite() || raw <= 0.0 {
                return None;
            }
            let q = (raw / px).trunc();
            if q <= 0.0 {
                return if bars.contains_key(sym) {
                    Some(0)
                } else {
                    None
                };
            }
            Some(q as i64 * it.side as i64)
        }
    }

    /// Gross over the FULL pool-owned book (legacy `gross_pv` verbatim):
    /// fresh rows (q == 0) skip; px ≤ 0 rows accrue `fixed` notional
    /// (structurally ≡ 0 — legacy parity, kept) while px > 0 rows accrue
    /// `priced`. Extends `last_px` this flush (`price_for`), as legacy.
    /// Returns `(priced, fixed)`.
    fn fund_gross(&mut self, book: &BTreeMap<String, i64>, bars: &BarFrame) -> (f64, f64) {
        let mut priced = 0.0f64;
        let mut fixed = 0.0f64; // ≡ 0 by construction (legacy parity), kept
        let book_items: Vec<(String, i64)> = book.iter().map(|(s, q)| (s.clone(), *q)).collect();
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
        (priced, fixed)
    }

    /// POOLED MODE — TWO-PHASE reconcile, phase 1 (collect): push THIS
    /// slot's UNTRIMMED book (pool-owned at `p.book(idx)`; fresh
    /// overwritten in step 1, grossed in `fund_gross`) and the sleeve
    /// equity + gross components for the vintage. The completing push runs
    /// phase 2 (the ONE uniform central trim over the complete vintage) and
    /// emits the scaled sheet for the WHOLE slot; non-completing sleeves
    /// emit NOTHING (their books entered the pool via the push).
    fn participate_and_trim(
        &self,
        p: &PoolState,
        idx: usize,
        ctx: &ClockCtx,
        priced: f64,
        fixed: f64,
    ) -> Result<Payload, String> {
        // Edge: the close window stamps the EOD basis for the pooled
        // collect + central trim (the completer's ctx decides for the
        // whole slot — all sleeves flush the same bar).
        let eod_edge = ctx.close_window;
        let gross = priced + fixed;
        if diag_on() {
            tracing::info!(
                slot = %ctx.et_date,
                owner = %self.own,
                own_gross = format!("{gross:.2}"),
                own_priced = format!("{priced:.2}"),
                own_fixed = format!("{fixed:.2}"),
                bg = if eod_edge { "close_window" } else { "intra" },
                "[POOLED] collect"
            );
        }

        match p.push(
            &self.producer,
            &self.own,
            ctx.et_date,
            ctx.mso,
            ctx.own_eq_sgd,
            p.book(idx),
            priced,
            fixed,
        ) {
            Some(vintage) => Ok(pooled_intents(p, vintage, ctx.et_date, eod_edge)),
            None => Ok(Payload::Intents(Vec::new())),
        }
    }

    /// BROKE-SLEEVE PARTICIPATION: the sleeve's flush early-returns BEFORE
    /// `run_chain`, so its gate never pushes — the pool mask must complete
    /// EVERY slot or the whole fund stalls (a broke sleeve often still
    /// holds positions, and the trim needs everyone's book). Pushes the
    /// CARRIED positions as an absolute book (derived sleeve-side — engine
    /// state in replay, DB positions in live; priced off THIS flush's
    /// frame here; absent bars contribute `fixed`, exactly like the
    /// gate's `last_px` miss path)
    /// and, when THIS push completes the slot, runs the ONE central trim
    /// itself (the gate path is unavailable: no chain ran). Does NOT touch
    /// `p.book(idx)` — write-back happens only at the trim; the fresh
    /// overwrite on the sleeve's first non-broke flush self-heals the
    /// book.
    pub fn push_carried(
        &mut self,
        et_date: chrono::NaiveDate,
        mso: i32,
        eq_sgd: f64,
        carried_book: BTreeMap<String, i64>,
        frame: &BarFrame,
        close_window: bool,
    ) -> Option<Vec<ShareOut>> {
        let Some(p) = PoolState::current() else {
            return None;
        };
        let mut book: BTreeMap<String, i64> = BTreeMap::new();
        let mut priced = 0.0f64;
        let mut fixed = 0.0f64;
        // The carried book arrives derived (engine state in replay, DB
        // positions in live — see `SleeveExecutor::carried_book`); the gate
        // prices it off THIS frame: a sym absent from the frame (or a
        // non-finite price) falls to the FIXED bucket — the legacy
        // last_px-miss parity.
        for (sym, q) in carried_book.iter() {
            if *q == 0 {
                continue;
            }
            book.insert(sym.clone(), *q);
            let px = frame
                .get(sym)
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
        let vintage = p.push(
            &self.producer,
            &self.own,
            et_date,
            mso,
            eq_sgd,
            &book,
            priced,
            fixed,
        )?;
        let eod_edge = close_window;
        Some(p.trim(vintage, et_date, eod_edge))
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
            Some(Payload::Shares(_)) => return Err("FundGate consumes Intents, not Shares".into()),
            None => return Err("FundGate requires upstream intents (never a chain head)".into()),
        };
        // DIAG seam: the chain output AT THE GATE BOUNDARY splits the signal
        // blackout: n_intents==0 → upstream chain silent; n_intents>0 with
        // the book staying empty → fresh_shares omitted every entry.
        if diag_on() {
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

        // POOLED MODE (every production gate — `FundGate::new` mints its
        // producer handle, and the binary inits the pool before any flush):
        // the participant index is the handle's producer idx, and the book
        // itself lives in the POOL (keyed by that idx).
        let idx = self.producer.get_producer_idx();
        match PoolState::current() {
            Some(p) if p.ready() => {
                // ── 1) fresh emissions overwrite the pool-owned book
                //       (absolute, legacy `sub_cur[si][sym] = q`); unnamed
                //       syms retain. ──
                let fresh: Vec<(String, i64)> = items
                    .iter()
                    .filter_map(|(sym, it)| {
                        Self::fresh_shares(it, sym, bars).map(|q| (sym.clone(), q))
                    })
                    .collect();
                p.update_book(idx, |book| {
                    for (sym, q) in fresh {
                        book.insert(sym, q);
                    }
                });
                // ── 2) gross the FULL book over all carried + fresh rows,
                //       then participate: pool push; the completing push
                //       runs the ONE uniform central trim over the complete
                //       vintage and emits the scaled sheet for the WHOLE
                //       slot (non-completing sleeves emit NOTHING — their
                //       books entered the pool via the push). ──
                // All-zero book: `fund_gross` self-yields exactly the
                // (0.0, 0.0) the legacy early gate hardcoded — the flush
                // still participates (an all-zero sleeve can be the
                // completer; the engine accounts true up from the sheet's
                // explicit zeros when the strategy had held positions).
                let (priced, fixed) = self.fund_gross(p.book(idx), bars);
                self.participate_and_trim(p, idx, ctx, priced, fixed)
            }
            // Unready pool (bare constructions, pool_init not yet called)
            // fails CLOSED: emit nothing — the legacy non-pooled fail-open
            // pass-through arm was deleted with its kernels.
            _ => {
                if !WARNED_NO_REGISTRY.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        owner = %self.own,
                        "FundGate: gate is not a pool participant — bare construction emits nothing"
                    );
                }
                Ok(Payload::Intents(Vec::new()))
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// FUND LEDGER — sizing-base state machine (py `Blended5Fund`
// SleeveEqLedger port). SEPARATE struct, same file (user ruling): the
// ledger IS fund logic and the FundGate/pool wiring requires it, but it
// drives a DIFFERENT lever (the sleeve `sc` sizing stamp upstream of the
// chain) versus the cap gate's in-chain trim.
//
// THE sizing basis: each sleeve sizes off its fund-share book `e_i`
// (init = capital / N = 12.5k each on the 50k fund) — the py single-fund-
// account compounding base. The pool-sizing (full-fund) variant was
// deleted with its env seam; a sleeve the ledger does not cover falls
// back to own-account equity (uncovered-name robustness, not a mode).
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
    /// anchor (run param, default 0 = off). Reduces the ledger base
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
    pub fn on_slot(
        &mut self,
        residual: f64,
        equities: &HashMap<String, f64>,
        d: chrono::NaiveDate,
    ) {
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

/// Clear the thread-local ledger (run end / tests).
#[allow(dead_code)]
pub fn ledger_clear() {
    FUND_LEDGER.with(|l| *l.borrow_mut() = None);
}

/// Sleeve's sizing base: the fund-share book `e_i` when the ledger covers
/// the sleeve, else `None` (caller falls back to own-account equity —
/// uncovered-name robustness, not a sizing mode).
pub fn sizing_base(name: &str) -> Option<f64> {
    FUND_LEDGER.with(|l| l.borrow().as_ref().and_then(|l| l.eq_for(name)))
}

#[cfg(test)]
mod two_phase_tests {
    use super::*;

    fn book(pairs: &[(&str, i64)]) -> BTreeMap<String, i64> {
        pairs.iter().map(|(s, q)| (s.to_string(), *q)).collect()
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

