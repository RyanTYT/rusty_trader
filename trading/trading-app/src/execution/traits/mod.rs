//! Ports between the execution engine and the broker client.
//!
//! The engine never talks to `ibapi` directly. It asks for two capabilities:
//! placing and cancelling orders ([`OrderRouter`](crate::execution::traits::order_router::OrderRouter)) and streaming
//! executions, open orders, account updates and positions
//! ([`PositionsTracker`](crate::execution::traits::positions_tracker::PositionsTracker)). Live code supplies the `Client`;
//! tests and backtests supply fakes, which is why the traits are small and
//! free of database or engine types.

pub mod order_router;
pub mod positions_tracker;
