//! Trait for backtest methods — each method implements a different replay
//! strategy (historical bar replay, walk-forward, parameter sweep, etc.).
//! The shared execution surface is in [`BacktestContext`]; each method's
//! `run` produces an [`EquityCurve`].

use crate::backtester::context::BacktestContext;
use crate::backtester::equity::EquityCurve;

pub trait BacktestMethod {
    /// Run the backtest method against the shared `ctx`. Returns the per-bar
    /// equity curve.
    fn run(&self, ctx: &BacktestContext) -> Result<EquityCurve, String>;
}
