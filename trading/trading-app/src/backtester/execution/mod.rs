//! Backtest execution surface — the order/fill types + the simulated broker.

pub mod broker;
pub mod fill_model;

pub use broker::BacktestBroker;
pub use fill_model::CommissionModel;

use ibapi::{contracts::Contract, orders::Order};

/// The order-submission trait — implemented by the simulated broker
/// (`BacktestBroker`) in backtest + by the real IBKR order engine in prod.
pub trait OrderSubmitter: Send + Sync {
    fn next_order_id(&self) -> i32;
    fn submit_order(&self, order_id: i32, contract: &Contract, order: &Order);
    fn cancel_order(&self, order_id: i32) -> Result<(), String>;
}
