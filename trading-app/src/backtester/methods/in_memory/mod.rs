//! In-memory backtest state + reconcile — the fast-path variant of
//! `HistoricalReplay`. All state in-memory (no DB I/O per bar).
//!
//! - [`state::InMemoryState`] — mirrors the DB state in-memory.
//! - [`thread_local`] — the thread-local `InMemoryState` setter/getter.
//! - [`reconcile::handle_bar_update_outcome_in_memory`] — the NEW in-memory
//!   reconcile (reads mocked targets + adjusts positions/transactions/cash).
//! - [`replay::InMemoryReplay`] — the `BacktestMethod` impl.

pub mod historical_cache;
pub mod reconcile;
pub mod replay;
pub mod state;
pub mod thread_local;
