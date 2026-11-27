//! Backtest execution surface — the order/fill types and the simulated broker.
//!
//! - [`fill_model`] — pure fill decisions and IBKR commission maths.
//! - [`broker`] — `BacktestBroker`, which settles fills against DB state.
//! - [`execution_report`] — IBKR-shaped reports for post-commit consumers.
//!
//! [`OrderSubmitter`] is the seam the order engine uses in backtest mode; the
//! production implementation lives on the live client.

pub mod broker;
pub mod fill_model;
pub mod execution_report;

pub use broker::BacktestBroker;
pub use fill_model::CommissionModel;

use ibapi::{contracts::Contract, orders::Order};

/// The order-submission trait — implemented by the simulated broker
/// (`BacktestBroker`) in backtest + by the real IBKR order engine in prod.
pub trait OrderSubmitter: Send + Sync {
    /// Allocate a unique order id. Ids must be monotonic per submitter so a
    /// parked order and its later fill refer to the same row.
    fn next_order_id(&self) -> i32;
    /// Submit `order` under `order_id`.
    ///
    /// Implementations are fire-and-forget: a rejected submission is reported
    /// through the order-update surface (or logged), not through this call.
    fn submit_order(&self, order_id: i32, contract: &Contract, order: &Order);
    /// Cancel `order_id`.
    ///
    /// # Errors
    /// Returns an error when the cancellation could not be delivered; the
    /// simulated broker logs instead of failing.
    fn cancel_order(&self, order_id: i32) -> Result<(), String>;
}
