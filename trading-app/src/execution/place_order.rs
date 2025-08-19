use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use ibapi::{Client, orders::Order, prelude::Contract};
// use tokio::sync::Mutex;
use tracing::info;

use crate::unlock;

/// Always place orders with the same client - for coordination of order ids
/// - As long as the instance for OrderEngine is the same used to place_order (same for client as
/// well), this should work well
/// - To meld with consolidator, consolidator preferably subscribes to market data from a client id
/// other than this one (ideal would be consolidator: 1, order_engine: 0)
///     - in this case, any strategy should be able to use the same order_engine and consolidator
///     instance
pub fn place_order(
    order_map: Arc<Mutex<HashMap<i32, (String, Contract, Order)>>>,
    strategy: String,
    client: Arc<Client>,
    contract: Contract,
    order: Order,
    override_others: bool,
) -> Result<(), String> {
    let order_id = client.next_order_id();
    {
        let mut order_map = unlock!(order_map, "order_map", "OrderEngine.place_order");
        order_map.insert(
            order_id,
            (strategy.clone(), contract.clone(), order.clone()),
        );
    }
    client
        .submit_order(order_id, &contract, &order)
        .map_err(|e| {
            tracing::error!(
                "Failed to place order for {}, order: {}, Error: {}",
                contract.symbol,
                order.action,
                e
            );
            format!(
                "Failed to place order for {}, order: {}, Error: {}",
                contract.symbol, order.action, e
            )
        })?;
    info!("Order submitted to IBKR");

    Ok(())
}
