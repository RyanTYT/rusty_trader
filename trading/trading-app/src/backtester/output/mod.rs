//! Backtest output — the equity curve and the metrics derived from it.
//!
//! Two layers:
//!
//! - [`equity::EquityCurve`] — one [`equity::EquitySnapshot`] per replay bar
//!   (cash, marked-to-market positions, total). It is held in memory; no schema
//!   change was made for it.
//! - [`results::BacktestResults`] — the final report built from the curve plus
//!   the run's transactions: PnL, return, max drawdown, Sharpe/Sortino, trade
//!   counts and win rate. Written to `backtest_results_{name}.json`.
//!
//! [`results::BacktestResults::compute`] reads transactions from the test DB
//! (realistic lane); [`results::BacktestResults::compute_in_memory`] reads them
//! from the in-memory replay state (fast lane, no DB I/O).
//!
//! Note that the equity valuation only marks stock positions: option positions
//! are skipped by the snapshot builder, so option PnL does not reach the curve.

pub mod equity;
pub mod results;
