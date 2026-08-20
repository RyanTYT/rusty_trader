use core::str;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak, atomic::AtomicUsize},
    time::Duration,
};

use ibapi::{Client, orders::OrderUpdate};
use sqlx::PgPool;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::{info, warn};

use crate::{
    execution::{fx_backed_up_order::OrderStore, order_update_stream},
    strategy::strategy::StrategyEnum,
};

static ORDER_UPDATE_STREAM_NO: AtomicUsize = AtomicUsize::new(0);

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
            "PreSubmitted" => StatusOfOrderStatus::PreSubmitted,
            "Submitted" => StatusOfOrderStatus::Submitted,
            "ApiCancelled" => StatusOfOrderStatus::ApiCancelled,
            "Cancelled" => StatusOfOrderStatus::Cancelled,
            "Filled" => StatusOfOrderStatus::Filled,
            "Inactive" => StatusOfOrderStatus::Inactive,
            _ => StatusOfOrderStatus::Unknown,
        }
    }
}

pub struct OrderUpdateStreamController {
    // order_map: Arc<RwLock<HashMap<i32, (String, Contract, Order)>>>,
    // strategy_map: Arc<HashMap<String, StrategyEnum>>,
    // weak_client: Weak<Client>,
    // default_strategy: String,
    stream_killer: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl OrderUpdateStreamController {
    /// Initialises the Order Update Stream to listen for all order events for the client
    /// Note: Should only be run once for initialisation - creates a channel on each call
    /// NOTE: initialises a synchronous thread and sends msgs to async runtime - blocking_send if
    /// not handled quickly could block up channel and stow updates indefinitely
    pub fn new(
        pool: PgPool,
        weak_client: Weak<Client>,
        strategy_map: Arc<HashMap<String, StrategyEnum>>,
        default_strategy: Option<String>,
        handle: tokio::runtime::Handle,
        backed_up_orders: Arc<OrderStore>,
    ) -> Option<Self> {
        // https://ibridgepy.com/ib-api-knowledge-base/#step1-1-17
        // openOrder( ) is triggered twice automatically. When the order is initially accepted and when the order is fully executed. When the order is initially accepted, you would get an openOrder( ) and orderStatus( ) call back. Then if there are partial fills or any other status changes you would receive additional orderStatus( ) call back. Then if you receive additional orderStatus( ) call back, when the order fully executes you would get a final orderStatus( ) followed by an openOrder( ) and then receive the execDetails( ) and commissionReport( ). If you invoke reqOpenOrders( ), it will only relay the last orderStatus( ) of any current working order.
        let is_init = ORDER_UPDATE_STREAM_NO.load(std::sync::atomic::Ordering::Acquire) > 0;
        if is_init {
            tracing::error!("Not allowed to create more than one instance of OrderUpdateStream");
            return None;
        }

        if let Err(_) = ORDER_UPDATE_STREAM_NO.compare_exchange(
            0,
            1,
            std::sync::atomic::Ordering::Release,
            std::sync::atomic::Ordering::Relaxed,
        ) {
            tracing::error!("Not allowed to create more than one instance of OrderUpdateStream");
            return None;
        }

        let (sender, mut rx) = tokio::sync::mpsc::channel::<OrderUpdate>(1024);
        let (kill_sender, mut kill_rcx) = tokio::sync::oneshot::channel::<()>();

        // spawn a new os blocking thread to await for updates synchronously
        // - send updates via channel back to async runtime
        let weak_client_cloned = weak_client.clone();
        std::thread::spawn(move || {
            let mut event_subscription = {
                let client_opt = weak_client_cloned.upgrade();
                if client_opt.is_none() {
                    tracing::error!("client is dead before init! could not subscribe!");
                    return;
                }
                let client = client_opt.unwrap();
                assert!(client.client_id() == 0);
                let event_subscription = client.order_update_stream();
                if let Err(e) = &event_subscription {
                    tracing::error!("Failed to begin order_update_stream in OrderEngine: {e:?}");
                    ORDER_UPDATE_STREAM_NO.store(0, std::sync::atomic::Ordering::Release);
                    return;
                }
                event_subscription.unwrap()
            };
            info!("Subscribed for updates for orders!");

            loop {
                if let Some(e) = event_subscription.error() {
                    warn!(
                        "order_update_stream subscription died! but not killed:{e:?}\
                        \nRetrying subscription!"
                    );
                    event_subscription = {
                        let client_opt = weak_client_cloned.upgrade();
                        if client_opt.is_none() {
                            warn!("client is dead! could not resubscribe!");
                            return;
                        }
                        let client = client_opt.unwrap();
                        let event_subscription = client.order_update_stream();
                        if let Err(e) = &event_subscription {
                            tracing::error!(
                                "Failed to restart order_update_stream in OrderEngine: {e:?}"
                            );
                            return;
                        }
                        event_subscription.unwrap()
                    };
                }
                if let Some(event) = event_subscription.next_timeout(Duration::from_secs(5)) {
                    info!("New order event received!");
                    let cloned_sender = sender.clone();
                    std::thread::spawn(move || {
                        if let Err(e) = cloned_sender.blocking_send(event) {
                            tracing::warn!(
                                "synchronous sender in init_order_update_stream failed to send event to async event loop: {e:?}"
                            )
                        };
                    });
                }
                match kill_rcx.try_recv() {
                    Ok(_) => {
                        tracing::info!("Order Update Stream Killed via kill sender");
                        return;
                    }
                    Err(e) => match e {
                        TryRecvError::Empty => continue,
                        TryRecvError::Closed => {
                            tracing::warn!(
                                "All receivers of OrderUpdateStream died => Ending OrderUpdateStream now!"
                            );
                            return;
                        }
                    },
                }
            }
        });

        // async reciever that asynchronously awaits for updates
        let pool = pool.clone();
        let def_strat = default_strategy.unwrap_or("unknown".to_string());
        let strategy_map = strategy_map.clone();
        let cloned_handle = handle.clone();
        let weak_client = weak_client.clone();
        let backed_up_orders = backed_up_orders.clone();
        handle.spawn(async move {
            loop {
                match rx.recv().await {
                    Some(order_update) => {
                        tracing::info!("Received order update in tokio runtime");
                        if let Err(e) = on_order_update_received(
                            strategy_map.clone(),
                            pool.clone(),
                            order_update,
                            def_strat.as_str(),
                            cloned_handle.clone(),
                            &weak_client,
                            backed_up_orders.clone(),
                        )
                        .await
                        {
                            tracing::error!("on_order_update_received error: {e:?}")
                        };
                    }
                    None => {
                        tracing::info!("OrderUpdateStream tokio thread killed!");
                        return;
                    }
                }
            }
        });

        Some(OrderUpdateStreamController {
            stream_killer: Arc::new(Mutex::new(Some(kill_sender))),
        })
    }
}

impl Drop for OrderUpdateStreamController {
    fn drop(&mut self) {
        ORDER_UPDATE_STREAM_NO.store(0, std::sync::atomic::Ordering::Release);
        println!("Dropping order update stream");
        // if let Err(e) = self
        //     .stream_killer
        //     .lock()
        //     .expect("Expected mutex lock for order_update_stream_killer not to be poisoned")
        //     .take()
        //     .expect("Expected stream_killer to be a Some")
        //     .send(())
        // {
        //     tracing::error!("Failed to send kill signal to OrderUpdateStream: {e:?}")
        // }
    }
}

/// Async only because it has to await open order handle
async fn on_order_update_received(
    strategy_map: Arc<HashMap<String, StrategyEnum>>,
    pool: PgPool,
    order_update: OrderUpdate,
    default_strategy: &str,
    handle: tokio::runtime::Handle,
    weak_client: &Weak<Client>,
    backed_up_orders: Arc<OrderStore>,
) -> Result<(), String> {
    match order_update {
        OrderUpdate::OrderStatus(status) => {
            tracing::info!("Order Status Received: ({})", status.status);
            match StatusOfOrderStatus::from_str(status.status.as_str()) {
                StatusOfOrderStatus::Submitted => {
                    order_update_stream::event_handlers::order_status::submitted(
                        pool.clone(),
                        &status,
                    );
                    Ok(())
                }
                StatusOfOrderStatus::ApiCancelled | StatusOfOrderStatus::Cancelled => {
                    order_update_stream::event_handlers::order_status::cancelled(
                        pool.clone(),
                        &status,
                    );
                    Ok(())
                }
                StatusOfOrderStatus::Unknown => {
                    return Err(format!(
                        "Unknown Order Update Status Code: {}",
                        status.status
                    ));
                }
                StatusOfOrderStatus::ApiPending
                | StatusOfOrderStatus::PendingSubmit
                | StatusOfOrderStatus::PendingCancel
                | StatusOfOrderStatus::Filled
                | StatusOfOrderStatus::Inactive
                | StatusOfOrderStatus::PreSubmitted => Ok(()),
            }
        }

        // This may conflict with OrderStatus::Submitted but we'll let the DB handle
        // conflict errors and error out
        // - we need this because OpenOrder is a lot more stable than the
        // OrderStatus::Submitted event
        OrderUpdate::OpenOrder(open_order) => {
            tracing::info!("Open Order update!");
            match StatusOfOrderStatus::from_str(open_order.order_state.status.as_str()) {
                StatusOfOrderStatus::Submitted | StatusOfOrderStatus::PreSubmitted => {
                    match order_update_stream::event_handlers::open_order::submitted(
                        pool.clone(),
                        &open_order.contract,
                        &open_order.order,
                    ) {
                        Ok(handle) => {
                            if let Err(e) = handle.await {
                                tracing::error!("Error occurred on_new_order_submitted: {}", e);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to handle Open Order submitted event: {e:?}")
                        }
                    };
                    Ok(())
                }
                StatusOfOrderStatus::Cancelled | StatusOfOrderStatus::ApiCancelled => Ok(()),
                _ => Ok(()),
            }
        }

        OrderUpdate::ExecutionData(execution_data) => {
            tracing::info!(
                message=%format!(
                    "New Execution recorded with id: {} for strategy: {}",
                    &execution_data.request_id,
                    &execution_data.execution.order_reference
                )
            );
            if let Err(e) = order_update_stream::event_handlers::execution::on_execution_update(
                pool.clone(),
                execution_data,
                strategy_map.clone(),
                default_strategy,
                handle,
                weak_client,
                backed_up_orders,
            ) {
                return Err(format!("Error while running on_execution_update: {e:?}"));
            };
            Ok(())
        }

        OrderUpdate::CommissionReport(commission_report) => {
            if let Err(e) =
                order_update_stream::event_handlers::commission_report::on_commission_update(
                    pool,
                    &commission_report,
                )
            {
                return Err(format!("Error while running on_commission_update: {e:?}"));
            };
            Ok(())
        }

        OrderUpdate::Message(message) => {
            tracing::warn!("Message from OrderEngine.order_update_stream: {message:?}");
            Ok(())
        }
    }
}
