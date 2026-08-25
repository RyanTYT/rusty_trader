//! Backtest setup — the types constructed once at the start of a backtest run.
//!
//! - [`clock::BacktestClock`] — the backtest clock (bar-time tracking).
//! - [`config::BacktestConfig`] — the user-facing config (capital, period, mode, contracts).
//! - [`context`] — the execution surface ([`context::LightContext`] for the
//!   InMemory path, [`context::BacktestContext`] for the Db path).
//! - [`seed`] — the initial-capital seeding (Db mode).

pub mod clock;
pub mod config;
pub mod context;
pub mod seed;
