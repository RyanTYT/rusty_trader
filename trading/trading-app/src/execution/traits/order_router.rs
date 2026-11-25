use ibapi::Client;

use crate::execution::order_engine::OrderIBKR;

pub trait OrderRouter {
    fn place_local_order(&self, order_ibkr: &OrderIBKR) -> i32;
    fn cancel_local_order(&self, order_id: i32);
}

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
