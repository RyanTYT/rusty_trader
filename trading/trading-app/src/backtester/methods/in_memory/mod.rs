//! In-memory backtest method — the fast-path variant of `HistoricalReplay`.
//! All state in-memory (no DB I/O per bar).
//!
//! - [`state::InMemoryState`] — mirrors the DB state in-memory + its
//!   thread-local holder (`set`/`current`/`clear`).
//! - [`bar_cache::BarCache`] — the pre-populated bar set + max-end-time
//!   tracker (the thread-local for the bar cache).
//! - [`reconcile::handle_bar_update_outcome_in_memory`] — the in-memory
//!   reconcile (reads mocked targets + adjusts positions/transactions/cash).
//! - [`replay::InMemoryReplay`] — the method (`run_with_warm_up` +
//!   `run_with_bars`).

pub mod bar_cache;
pub mod reconcile;
pub mod replay;
pub mod state;
