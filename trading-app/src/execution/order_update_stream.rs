use core::str;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use ibapi::{
    Client,
    orders::{ExecutionData, Order, OrderState, OrderUpdate},
    prelude::Contract,
};
use sqlx::PgPool;
use tracing::info;

use crate::{
    execution::events::order_events::{
        on_commission_update, on_execution_update, on_new_order_submitted, on_order_cancelled,
    },
    unlock,
};

#[derive(Debug)]
enum StatusOfOrderStatus {
    ApiPending,
    PendingSubmit,
    PendingCancel,
    PreSubmitted,
    Submitted,
    ApiCancelled,
    Cancelled,
    Filled,
    Inactive,
    Unknown,
}

impl StatusOfOrderStatus {
    fn from_str(input: &str) -> StatusOfOrderStatus {
        match input {
            "ApiPending" => StatusOfOrderStatus::ApiPending,
            "PendingSubmit" => StatusOfOrderStatus::PendingSubmit,
            "PendingCancel" => StatusOfOrderStatus::PendingCancel,
            "PreSubmitted" => StatusOfOrderStatus::PendingCancel,
            "Submitted" => StatusOfOrderStatus::Submitted,
            "ApiCancelled" => StatusOfOrderStatus::ApiCancelled,
            "Cancelled" => StatusOfOrderStatus::Cancelled,
            "Filled" => StatusOfOrderStatus::Filled,
            "Inactive" => StatusOfOrderStatus::Inactive,
            _ => StatusOfOrderStatus::Unknown,
        }
    }
}

// pub fn init_order_update_stream(
//     pool: PgPool,
//     order_map: Arc<Mutex<HashMap<i32, (String, Contract, Order)>>>,
//     client: Arc<Client>,
// ) -> Result<(), String> {
//     // https://ibridgepy.com/ib-api-knowledge-base/#step1-1-17
//     // openOrder( ) is triggered twice automatically. When the order is initially accepted and when the order is fully executed. When the order is initially accepted, you would get an openOrder( ) and orderStatus( ) call back. Then if there are partial fills or any other status changes you would receive additional orderStatus( ) call back. Then if you receive additional orderStatus( ) call back, when the order fully executes you would get a final orderStatus( ) followed by an openOrder( ) and then receive the execDetails( ) and commissionReport( ). If you invoke reqOpenOrders( ), it will only relay the last orderStatus( ) of any current working order.
//     let new_rt = tokio::runtime::Builder::new_multi_thread()
//         .enable_all()
//         .build()
//         .unwrap();
//     new_rt.block_on(async move {
//         let event_subscription = {
//             assert!(client.client_id() == 0);
//             let event_subscription = client.order_update_stream().map_err(|e| {
//                 format!("Failed to begin order_update_stream in OrderEngine: {}", e)
//             })?;
//             event_subscription
//         };
//         info!("Subscribed for updates for orders");
//
//         while let Some(event) = event_subscription.next() {
//             info!("New order event received!");
//         }
//
//         Ok::<(), String>(())
//     });
//
//     Ok(())
// }

/// Async only because it has to await open order handle
pub async fn on_order_update_received(
    order_map: Arc<Mutex<HashMap<i32, (String, Contract, Order)>>>,
    pool: PgPool,
    order_update: OrderUpdate,
) -> Result<(), String> {
    macro_rules! simple_update_log {
        ($status: expr, $update: expr) => {{
            let order_map = unlock!(order_map, "order_map", "OrderEngine.order_update_stream");
            info!(
                "order {} status for order for {}",
                $update,
                order_map
                    .get(&$status.order_id)
                    .map_or("Unknown (Strategy not recorded in HashMap)", |v| v
                        .0
                        .as_str())
            );
        }};
    }
    match order_update {
        OrderUpdate::OrderStatus(status) => {
            match StatusOfOrderStatus::from_str(status.status.as_str()) {
                StatusOfOrderStatus::ApiPending => {
                    simple_update_log!(status, "ApiPending");
                }
                StatusOfOrderStatus::PendingSubmit => {
                    simple_update_log!(status, "PendingSubmit");
                }
                StatusOfOrderStatus::PendingCancel => {
                    simple_update_log!(
                        status,
                        "PendingCancel (Cancellation request sent, not yet accepted)"
                    );
                }
                StatusOfOrderStatus::PreSubmitted => {
                    simple_update_log!(
                        status,
                        "PreSubmitted (Order being transmitted to exchange)"
                    );
                }
                StatusOfOrderStatus::Submitted => {
                    simple_update_log!(status, "Submitted (Order accepted by system and active)");
                    let strategy_order = {
                        let order_map =
                            unlock!(order_map, "order_map", "OrderEngine.order_update_stream");
                        order_map.get(&status.order_id).expect("Strategy not recorded in order_map for some reason before receiving order submitted event!").clone()
                    };

                    match on_new_order_submitted(
                        pool.clone(),
                        status.order_id.clone(),
                        status.perm_id.clone(),
                        strategy_order.clone(),
                    ) {
                        Ok(handle) => {
                            if let Err(e) = handle.await {
                                tracing::error!("Error occurred on_new_order_submitted: {}", e);
                            }
                        }
                        Err(_) => (),
                    };
                }
                StatusOfOrderStatus::ApiCancelled => {
                    simple_update_log!(
                        status,
                        "ApiCancelled (Order yet to be acknowledged and was cancelled)"
                    );

                    let strategy_order = {
                        let order_map =
                            unlock!(order_map, "order_map", "OrderEngine.order_update_stream");
                        order_map.get(&status.order_id).expect("Strategy not recorded in order_map for some reason before receiving order submitted event!").clone()
                    };

                    on_order_cancelled(pool.clone(), status.clone(), strategy_order);
                }
                StatusOfOrderStatus::Cancelled => {
                    simple_update_log!(status, "Cancelled (Can occur if order is rejected)");
                    let strategy_order = {
                        let order_map =
                            unlock!(order_map, "order_map", "OrderEngine.order_update_stream");
                        order_map.get(&status.order_id).expect("Strategy not recorded in order_map for some reason before receiving order submitted event!").clone()
                    };

                    on_order_cancelled(pool.clone(), status.clone(), strategy_order);
                }
                StatusOfOrderStatus::Filled => {
                    // Filled Order - Dropping of OpenOrder row done in execution_update
                    simple_update_log!(status, "Filled");
                }
                StatusOfOrderStatus::Inactive => {
                    simple_update_log!(
                        status,
                        "Inactive (Order was received but no longer active - rejected, cancelled, ...)"
                    );
                }
                StatusOfOrderStatus::Unknown => {
                    tracing::error!(
                        "Unknown Status Code in OrderEngine.order_update_stream: {}",
                        status.status
                    );
                }
            }
        }

        // This may conflict with OrderStatus::Submitted but we'll let the DB handle
        // conflict errors and error out
        // - we need this because OpenOrder is a lot more stable than the
        // OrderStatus::Submitted event
        OrderUpdate::OpenOrder(open_order) => {
            info!(
                "New open order in OpenOrder with order status: {}",
                open_order.order_state.status
            );
            let strategy_order = {
                let order_map = unlock!(order_map, "order_map", "OrderEngine.order_update_stream");
                let strategy_order =
                                order_map.get(&open_order.order.order_id).expect("Strategy not recorded in order_map for some reason before receiving order submitted event!").clone();
                strategy_order
            };
            if open_order.order_state.status == "Submitted"
                || open_order.order_state.status == "PreSubmitted"
            {
                info!("Updated Open Orders");
                match on_new_order_submitted(
                    pool.clone(),
                    open_order.order_id.clone(),
                    open_order.order.perm_id.clone(),
                    strategy_order,
                ) {
                    Ok(handle) => {
                        if let Err(e) = handle.await {
                            tracing::error!("Error occurred on_new_order_submitted: {}", e);
                        }
                    }
                    Err(_) => (),
                };
            }
        }

        OrderUpdate::ExecutionData(execution_data) => {
            let strategy = {
                let order_map = unlock!(order_map, "order_map", "OrderEngine.order_update_stream");
                order_map.get(&execution_data.execution.order_id).map_or(
                    "Unknown strategy: not recorded in order_map".to_string(),
                    |v| v.0.clone(),
                )
            };
            tracing::info!(
                "New Execution recorded with id: {} for strategy: {}",
                &execution_data.request_id,
                &strategy
            );

            // let mut execution_map = unlock!(
            //     execution_map,
            //     "execution_map",
            //     "OrderEngine.order_update_stream"
            // );
            // execution_map.insert(
            //     execution_data.execution.execution_id.clone(),
            //     execution_data.clone(),
            // );

            on_execution_update(pool.clone(), execution_data);
        }

        OrderUpdate::CommissionReport(commission_report) => {
            // let execution_map = unlock!(
            //     execution_map,
            //     "execution_map",
            //     "OrderEngine.order_update_stream"
            // );
            // let execution_data =
            //         execution_map.get(&commission_report.execution_id).expect("Execution map in OrderEngine does not contain request_id from commission_report: Error in ordering received from IBKR!");
            // {
            //     let order_map_unlocked =
            //         unlock!(order_map, "order_map", "OrderEngine.order_update_stream");
            //     let strategy = order_map_unlocked.get(
            //         &execution_data.execution.order_id,
            //     );
            //     tracing::info!(
            //         "Commissions to be updated for execution with order_perm_id: {}, Strategy: {}, commission: {}",
            //         &execution_data.execution.perm_id,
            //         &strategy.map_or("Unknown Strategy (Strategy was not recorded)", |v| v
            //             .0
            //             .as_str()),
            //         &commission_report.commission
            //     );
            // }

            if let Err(e) = on_commission_update(pool.clone(), commission_report) {
                tracing::error!(
                    "Error while running OrderEngine.on_commission_update: {}",
                    e
                );
            };
        }

        OrderUpdate::Message(message) => {
            tracing::warn!("Message from OrderEngine.order_update_stream: {}", message);
        }
    }

    Ok(())
}
