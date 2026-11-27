//! The order-submission port.
//!
//! One narrow capability: hand an [`OrderIBKR`] to the broker and cancel by
//! order id. Live execution uses the IB `Client`; tests and the backtester
//! supply a fake so placement logic can be exercised without a gateway.
//!
//! Failures are reported by value, not by `Result`: a rejected submission
//! returns `-1` and a failed cancellation is logged, because the engine treats
//! placement as fire-and-forget and reconciles from the broker's order stream.

use ibapi::Client;

use crate::execution::order_engine::OrderIBKR;

/// Places and cancels orders at the broker.
///
/// Implementations must be cheap to call from a spawned task and must not
/// block on the broker's acknowledgement; results arrive asynchronously
/// through the order-update stream.
pub trait OrderRouter {
    /// Submits `order_ibkr` and returns the broker's order id.
    ///
    /// Returns `-1` when the submission fails; callers treat that as a
    /// rejection and leave the database untouched.
    fn place_local_order(&self, order_ibkr: &OrderIBKR) -> i32;
    /// Requests cancellation of `order_id`.
    ///
    /// Fire-and-forget: a failure is logged by the implementation, never
    /// reported to the caller.
    fn cancel_local_order(&self, order_id: i32);
}

/// Live implementation backed by the IB client.
///
/// The order id is allocated before submission, so an accepted order can be
/// tracked immediately; the client's own error is logged and mapped to `-1`.
impl OrderRouter for Client {
    fn place_local_order(&self, order_ibkr: &OrderIBKR) -> i32 {
        let order_id = self.next_order_id();
        if let Err(e) = self.submit_order(order_id, &order_ibkr.contract, &order_ibkr.order) {
            tracing::error!(
                "Failed to place order for {}, order: {}, Error: {}",
                order_ibkr.contract.symbol,
                order_ibkr.order.action,
                e
            );
            return -1;
        };
        order_id
    }

    fn cancel_local_order(&self, order_id: i32) {
        if let Err(e) = self.cancel_order(order_id, "") {
            tracing::error!("Failed to cancel order in IBKR client: {e:?}");
        };
    }
}
