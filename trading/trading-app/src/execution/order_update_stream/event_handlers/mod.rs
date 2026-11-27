//! Handlers for the four IBKR order-update events.
//!
//! Each submodule owns one event family and its database effect:
//!
//! - [`execution`] — fills: transactions, positions, hooks, parked-order release.
//! - [`open_order`] — acknowledged/cancelled open orders.
//! - [`order_status`] — status bookkeeping and cancellation cleanup.
//! - [`commission_report`] — fee staging for the database merge triggers.
//!
//! All handlers are fire-and-forget from the caller's perspective except
//! [`execution::on_execution_update`],
//! which validates synchronously and then spawns its database work, and
//! [`open_order::submitted`],
//! which returns the spawned task so the dispatcher can await it. Database
//! failures are logged and roll back their transaction; they do not propagate to
//! the stream loop.

pub mod commission_report;
pub mod execution;
pub mod open_order;
pub mod order_status;
