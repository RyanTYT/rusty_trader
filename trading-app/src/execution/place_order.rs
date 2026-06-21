use std::{
    collections::HashMap,
    sync::{Arc, RwLock, Weak},
};

use ibapi::{Client, orders::Order, prelude::Contract};
use tracing::info;

use crate::helpers::contract::HashContract;

/// Always place orders with the same client - for coordination of order ids
/// - As long as the instance for OrderEngine is the same used to place_order (same for client as
/// well), this should work well
/// - To meld with consolidator, consolidator preferably subscribes to market data from a client id
/// other than this one (ideal would be consolidator: 1, order_engine: 0)
///     - in this case, any strategy should be able to use the same order_engine and consolidator
///     instance
pub fn place_order(
    order_map: Arc<RwLock<HashMap<i32, (String, Contract, Order)>>>,
    strategy: &str,
    weak_client: &Weak<Client>,
    contract: &Contract,
    order: &Order,
    attachments: Vec<(HashContract, Order)>,
) -> Result<(), String> {
    let client_opt = weak_client.upgrade();
    if client_opt.is_none() {
        tracing::warn!("client died while placing orders!");
        return Err("Client is dead".to_string());
    }
    let client = client_opt.unwrap();

    let order_id = client.next_order_id();
    {
        let mut order_map = order_map
            .write()
            .expect("Expected write lock for order_map not to be poisoned");
        order_map.insert(
            order_id,
            (strategy.to_string(), contract.clone(), order.clone()),
        );
    }

    // If there are attachments, this order must not self-transmit —
    // transmission is triggered by the last child order.
    let has_attachments = !attachments.is_empty();
    let parent_order = if has_attachments {
        let mut o = order.clone();
        o.transmit = false;
        o
    } else {
        order.clone()
    };

    client
        .submit_order(order_id, contract, &parent_order)
        .map_err(|e| {
            tracing::error!(
                message=%format!(
                    "Failed to place order for {}, order: {}, Error: {}",
                    contract.symbol,
                    order.action,
                    e
                )
            );
            format!(
                "Failed to place order for {}, order: {}, Error: {}",
                contract.symbol, order.action, e
            )
        })?;
    info!("Order submitted to IBKR: {order:?} for {contract:?}");

    let attachment_count = attachments.len();
    for (i, (attachment_contract, mut attachment_order)) in attachments.into_iter().enumerate() {
        let is_last = i == attachment_count - 1;
        let child_order_id = client.next_order_id();
        attachment_order.parent_id = order_id;
        attachment_order.transmit = is_last;

        {
            let mut order_map_write = order_map
                .write()
                .expect("Expected write lock for order_map not to be poisoned");
            order_map_write.insert(
                child_order_id,
                (
                    strategy.to_string(),
                    attachment_contract.contract.clone(),
                    attachment_order.clone(),
                ),
            );
        }

        client
            .submit_order(
                child_order_id,
                &attachment_contract.contract,
                &attachment_order,
            )
            .map_err(|e| {
                tracing::error!(
                    message=%format!(
                        "Failed to place child order for {}, order: {}, Error: {}",
                        attachment_contract.contract.symbol, attachment_order.action, e
                    )
                );
                format!(
                    "Failed to place child order for {}, order: {}, Error: {}",
                    attachment_contract.contract.symbol, attachment_order.action, e
                )
            })?;
        info!(
            "Child order submitted to IBKR: {attachment_order:?} for {:?}",
            attachment_contract.contract
        );
    }

    Ok(())
}
