//! Backtest configuration: period, contracts, capital, fees.

use chrono::{DateTime, Utc};
use ibapi::contracts::Contract;

/// Parameters for a single backtest run. The strategy itself is passed
/// separately to `Replayer` (as `Box<dyn BacktestStrategy>`), so it's not a
/// field here.
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    /// Contracts whose historical bars the replayer will stream.
    pub subscribed_contracts: Vec<Contract>,
    /// Inclusive backtest start (first bar with time >= start).
    pub start: DateTime<Utc>,
    /// Inclusive backtest end (last bar with time <= end).
    pub end: DateTime<Utc>,
    /// Starting SGD cash balance for the simulated account.
    pub starting_capital_sgd: f64,
    /// Slippage applied to market fills, in basis points (1bp = 0.01%).
    pub slippage_bps: f64,
    /// Commission per share filled.
    pub commission_per_share: f64,
    /// Minimum commission per order (IBKR-style floor).
    pub commission_min_per_order: f64,
}
