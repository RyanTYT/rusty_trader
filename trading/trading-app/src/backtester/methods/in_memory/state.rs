//! In-memory backtest state — mirrors the DB state (current positions,
//! target positions, transactions, cash) WITHOUT any DB I/O. Held in a
//! thread-local (set by `InMemoryReplay`) so the strategy's cfg-gated CRUD
//! branches + the in-memory reconcile can read/write it.
//!
//! Single-threaded at runtime (only the replayer's `spawn_blocking` thread
//! accesses it), but `Send + Sync` is required for `Arc<InMemoryState>` to
//! cross into `spawn_blocking` — hence `RwLock` (uncontended, like the broker).

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};

use crate::backtester::methods::in_memory::bracket::RestingOrder;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PositionKey {
    pub strategy: String,
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryPosition {
    pub quantity: f64,
    pub avg_price: f64,
}

#[derive(Debug, Clone)]
pub struct InMemoryTransaction {
    pub strategy: String,
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
    pub time: DateTime<Utc>,
    pub price: f64,
    pub quantity: f64,
    pub fees: f64,
    pub action: String,
}

pub struct InMemoryState {
    pub current_positions: RwLock<HashMap<PositionKey, InMemoryPosition>>,
    pub target_positions: RwLock<HashMap<PositionKey, InMemoryPosition>>,
    pub transactions: RwLock<Vec<InMemoryTransaction>>,
    pub strategy_name: String,
    pub resting_orders: RwLock<Vec<RestingOrder>>,
}

impl InMemoryState {
    pub fn new(strategy_name: String, starting_capital_sgd: f64) -> Self {
        let mut current = HashMap::new();
        // Seed CASH:SGD = starting capital (rate 1.0). The strategy's
        // `get_strategy_sgd_value` reads this as available cash.
        current.insert(
            PositionKey {
                strategy: strategy_name.clone(),
                stock: "CASH:SGD".to_string(),
                primary_exchange: "".to_string(),
                currency: "SGD".to_string(),
            },
            InMemoryPosition {
                quantity: starting_capital_sgd,
                avg_price: 1.0,
            },
        );
        Self {
            current_positions: RwLock::new(current),
            target_positions: RwLock::new(HashMap::new()),
            transactions: RwLock::new(Vec::new()),
            strategy_name,
            resting_orders: RwLock::new(Vec::new()),
        }
    }

    /// Get the target quantity for a (strategy, stock, pe, currency) key.
    /// Returns 0.0 if no target is set (mirrors the DB `get_target_pos_diff`).
    pub fn target_qty(&self, key: &PositionKey) -> f64 {
        self.target_positions
            .read()
            .expect("InMemoryState target_positions poisoned")
            .get(key)
            .map(|p| p.quantity)
            .unwrap_or(0.0)
    }

    /// Get the current quantity for a key. Returns 0.0 if no position.
    pub fn current_qty(&self, key: &PositionKey) -> f64 {
        self.current_positions
            .read()
            .expect("InMemoryState current_positions poisoned")
            .get(key)
            .map(|p| p.quantity)
            .unwrap_or(0.0)
    }

    /// Set (create-or-replace) the target position for `key`.
    pub fn set_target(&self, key: PositionKey, qty: f64, avg_price: f64) {
        self.target_positions
            .write()
            .expect("InMemoryState target_positions poisoned")
            .insert(
                key,
                InMemoryPosition {
                    quantity: qty,
                    avg_price,
                },
            );
    }

    /// Delete the target position for `key` (mirrors `TargetPositionsCRUD::delete`).
    pub fn delete_target(&self, key: &PositionKey) {
        self.target_positions
            .write()
            .expect("InMemoryState target_positions poisoned")
            .remove(key);
    }

    /// Additively update the current position (weighted-avg cost, mirrors
    /// `update_positions_additive`). Buys (delta>0) increase qty + reweight
    /// avg_price; sells (delta<0) decrease qty (keep avg_price); zero-out
    /// sets avg_price=0.
    pub fn update_current_additive(&self, key: PositionKey, delta: f64, avg_price: f64) {
        let mut current = self
            .current_positions
            .write()
            .expect("InMemoryState current_positions poisoned");
        let pos = current.entry(key).or_insert(InMemoryPosition::default());
        let new_qty = pos.quantity + delta;
        if new_qty.abs() < 1e-9 {
            pos.quantity = 0.0;
            pos.avg_price = 0.0;
        } else if (pos.quantity >= 0.0) == (delta >= 0.0) {
            // Same direction: weighted-avg cost.
            let total_cost = pos.quantity * pos.avg_price + delta * avg_price;
            pos.avg_price = total_cost / new_qty;
            pos.quantity = new_qty;
        } else {
            // Cross-direction: keep existing avg_price (SIGN guard, mirrors prod).
            pos.quantity = new_qty;
        }
    }

    /// Record a transaction (mirrors `StockTransactionsCRUD::create`).
    pub fn record_transaction(&self, txn: InMemoryTransaction) {
        self.transactions
            .write()
            .expect("InMemoryState transactions poisoned")
            .push(txn);
    }

    /// Snapshot all current positions as `(key, position)` pairs (for the
    /// equity snapshot + `get_strategy_sgd_value`).
    pub fn current_positions_snapshot(&self) -> Vec<(PositionKey, InMemoryPosition)> {
        self.current_positions
            .read()
            .expect("InMemoryState current_positions poisoned")
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

// ── Thread-local holder ───────────────────────────────────────────────────
// Set by `InMemoryReplay` before the bar loop (on the spawn_blocking/rayon
// thread); read by the cfg-gated CRUD branches + the in-memory reconcile.
// The thread-local is the seam that avoids modifying the strategy's
// constructor (which takes a `PgPool`) — the replayer sets it, the strategy
// reads it via `current()` on the same thread.

thread_local! {
    static IN_MEMORY_STATE: RefCell<Option<Arc<InMemoryState>>> =
        const { RefCell::new(None) };
}

/// Set the thread-local in-memory state. Called by `InMemoryReplay` before
/// the bar loop (on the `spawn_blocking` thread).
pub fn set(state: Arc<InMemoryState>) {
    IN_MEMORY_STATE.with(|s| *s.borrow_mut() = Some(state));
}

/// Get the thread-local in-memory state, if set. Called by the cfg-gated CRUD
/// branches + the reconcile. Returns `None` in the default (DB-backed) mode.
pub fn current() -> Option<Arc<InMemoryState>> {
    IN_MEMORY_STATE.with(|s| s.borrow().clone())
}

/// Clear the thread-local state. Called by `InMemoryReplay` after the loop.
pub fn clear() {
    IN_MEMORY_STATE.with(|s| *s.borrow_mut() = None);
}

// ── Fill router + realized-fill mailbox (§8 pooled emission seam) ────────
// APPROVED engine seam (user, 2026-10-30): with central pooled emission
// the COMPLETING sleeve emits orders on behalf of ALL sleeves; every
// pooled order carries `order_ref = <owner strategy name>` and fills must
// settle into the OWNING sleeve's account — not the emitting sleeve's.
//
// - FILL_ROUTER: name → Arc<InMemoryState>, installed ONCE by the multi
//   replay runner (alongside the states vec it mirrors). The reconcile
//   fill paths resolve the settlement state by order_ref (fallback: the
//   passed state).
// - FILLED_MAILBOX: realized fills per owner name. Pooled fills bypass
//   every strategy hook (the completing strategy must NOT ingest peer
//   fills into its severity streams); each sleeve drains its OWN mailbox
//   at its next flush (strategy layer) and feeds its own chain — the
//   same `on_fill → apply_realised` shape as direct fills.

/// One routed realized fill (engine-side record; the sleeve layer converts
/// to its `RealisedFill`).
#[derive(Debug, Clone)]
pub struct MailFill {
    pub sym: String,
    /// Signed: +buy / −sell.
    pub qty: i64,
    pub px: f64,
    pub date: chrono::NaiveDate,
}

thread_local! {
    static FILL_ROUTER: RefCell<Option<HashMap<String, Arc<InMemoryState>>>> =
        const { RefCell::new(None) };
    static FILLED_MAILBOX: RefCell<HashMap<String, Vec<MailFill>>> =
        RefCell::new(HashMap::new());
}

/// Install the per-strategy state router (multi replay runner, once per
/// pass, BEFORE the slot loop).
pub fn set_fill_router(states: HashMap<String, Arc<InMemoryState>>) {
    FILL_ROUTER.with(|r| *r.borrow_mut() = Some(states));
}

/// Resolve the settlement state for `order_ref` (fallback `None` → caller
/// uses the passed state).
pub fn routed_state(order_ref: &str) -> Option<Arc<InMemoryState>> {
    FILL_ROUTER.with(|r| r.borrow().as_ref().and_then(|m| m.get(order_ref).cloned()))
}

/// Clear the router (run end).
pub fn clear_fill_router() {
    FILL_ROUTER.with(|r| *r.borrow_mut() = None);
}

/// Record a realized fill for the OWNING strategy's mailbox (pooled fills;
/// the owning sleeve drains at its next flush).
pub fn mailbox_push(owner: &str, f: MailFill) {
    FILLED_MAILBOX.with(|m| m.borrow_mut().entry(owner.to_string()).or_default().push(f));
}

/// Drain ALL realized fills recorded for `name` (owning sleeve, next flush).
pub fn mailbox_drain(name: &str) -> Vec<MailFill> {
    FILLED_MAILBOX.with(|m| m.borrow_mut().remove(name).unwrap_or_default())
}


