//! Scheduling: when the broker is reachable and when each contract trades.
//!
//! - [`broker_scheduler`](crate::schedule::broker_scheduler) — the IBKR weekly
//!   maintenance window (Friday 23:00 → Saturday 03:00 Eastern), shared by all
//!   regions;
//! - [`contract_scheduler`](crate::schedule::contract_scheduler) — per-contract
//!   trading intervals from IBKR contract details, cached lazily in a UTC queue;
//! - `program_scheduler` — live only: the supervisor loop that boots the app
//!   while the broker is available and sleeps through maintenance.

pub mod broker_scheduler;
pub mod contract_scheduler;
#[cfg(not(feature = "backtest"))]
pub mod program_scheduler;
