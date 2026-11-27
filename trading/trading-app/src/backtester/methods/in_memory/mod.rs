//! In-memory backtest method — the fast-path variant of `HistoricalReplay`.
//! All state in-memory (no DB I/O per bar).
//!
//! - [`state::InMemoryState`] — mirrors the DB state in-memory + its
//!   thread-local holder (`set`/`current`/`clear`), the pooled fill router and
//!   the realized-fill mailbox.
//! - [`bar_cache::BarCache`] — the pre-populated bar set + max-end-time
//!   tracker (the thread-local for the bar cache).
//! - [`bracket::RestingOrder`] — the simulated order book (bracket entries and
//!   their OCA exits, each with a fill model).
//! - [`reconcile::handle_bar_update_outcome_in_memory`] — the in-memory
//!   reconcile (reads mocked targets + adjusts positions/transactions/cash).
//! - [`replay::InMemoryReplay`] — the method (`run_with_warm_up` +
//!   `run_with_bars`, single and multi strategy).
//! - [`diag`] — env-gated diagnostic CSV/log seams (default off).

pub mod bar_cache;
pub mod bracket;
pub mod diag;
pub mod reconcile;
pub mod replay;
pub mod state;
