//! Thread-local holder for the in-memory backtest state. Set by `InMemoryReplay`
//! before the bar loop; read by the cfg-gated CRUD branches (so the strategy's
//! `create_or_update`/`delete`/`get_pos_by_strat` operate on the in-memory
//! state instead of the DB) + by the in-memory reconcile.
//!
//! Thread-local is the cleanest seam that avoids modifying the strategy's
//! struct/constructor (which takes a `PgPool`). The replayer sets it on the
//! `spawn_blocking` thread; the strategy (running synchronously on that same
//! thread) reads it via `current()`.

use std::sync::Arc;

use std::cell::RefCell;

thread_local! {
    static IN_MEMORY_STATE: RefCell<Option<Arc<crate::backtester::methods::in_memory::state::InMemoryState>>> =
        const { RefCell::new(None) };
}

/// Set the thread-local in-memory state. Called by `InMemoryReplay` before
/// the bar loop (on the `spawn_blocking` thread).
pub fn set(state: Arc<crate::backtester::methods::in_memory::state::InMemoryState>) {
    IN_MEMORY_STATE.with(|s| *s.borrow_mut() = Some(state));
}

/// Get the thread-local in-memory state, if set. Called by the cfg-gated CRUD
/// branches + the reconcile. Returns `None` in the default (DB-backed) mode.
pub fn current() -> Option<Arc<crate::backtester::methods::in_memory::state::InMemoryState>> {
    IN_MEMORY_STATE.with(|s| s.borrow().clone())
}

/// Clear the thread-local state. Called by `InMemoryReplay` after the loop.
pub fn clear() {
    IN_MEMORY_STATE.with(|s| *s.borrow_mut() = None);
}
