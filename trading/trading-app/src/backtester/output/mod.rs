//! Backtest output — the equity curve + the computed results.
//!
//! - [`equity::EquityCurve`] — the per-bar equity snapshots.
//! - [`results::BacktestResults`] — the final metrics (PnL, Sharpe, drawdown, etc.).

pub mod equity;
pub mod results;
