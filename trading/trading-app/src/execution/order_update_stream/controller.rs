//! The single subscription that turns IBKR order callbacks into database writes.
//!
//! IBKR delivers order state through one push subscription per client. This
//! module owns that subscription for the whole process: a dedicated OS thread
//! blocks on the stream, forwards every event into a bounded Tokio channel, and
//! an async task dispatches each event to the matching handler in
//! [`super::event_handlers`].
//!
//! # Routing
//!
//! | Event | Status | Handler |
//! |---|---|---|
//! | `OrderStatus` | `Submitted` | [`event_handlers::order_status::submitted`](crate::execution::order_update_stream::event_handlers::order_status::submitted) (read-only consistency check) |
//! | `OrderStatus` | `PendingCancel`, `Inactive`, `ApiCancelled`, `Cancelled` | [`event_handlers::order_status::cancelled`](crate::execution::order_update_stream::event_handlers::order_status::cancelled) (deletes rows) |
//! | `OrderStatus` | anything else | no database write |
//! | `OpenOrder` | `Submitted`, `PreSubmitted` | [`event_handlers::open_order::submitted`](crate::execution::order_update_stream::event_handlers::open_order::submitted) (replaces the optimistic row) |
//! | `ExecutionData` | — | [`event_handlers::execution::on_execution_update`](crate::execution::order_update_stream::event_handlers::execution::on_execution_update) |
//! | `CommissionReport` | — | [`event_handlers::commission_report::on_commission_update`](crate::execution::order_update_stream::event_handlers::commission_report::on_commission_update) |
//! | `Message` | — | logged only |
//!
//! # Invariants
//!
//! - Exactly one controller may exist per process. [`OrderUpdateStreamController::new`]
//!   returns `None` on the second call until the first is dropped (the
//!   `ORDER_UPDATE_STREAM_NO` latch).
//! - The subscription belongs to client id `0`; other ids cannot receive the
//!   account-wide order stream.
//! - The stream is resubscribed after a subscription error while the controller
//!   is alive; a dead client ends the thread instead of retrying forever.
//! - Events are forwarded from a freshly spawned OS thread each time, so the
//!   relative order of two callbacks is **not** guaranteed. Handlers must be
//!   idempotent and tolerate the database already reflecting an earlier event.
//! - The channel holds 1024 events. A producer uses `blocking_send`, so a stalled
//!   consumer applies backpressure to the callback thread rather than dropping
//!   updates.
//!
//! # Lifecycle
//!
//! Construct with [`OrderUpdateStreamController::new`], keep the value alive for
//! the life of the process, and prefer
//! [`async_drop`](OrderUpdateStreamController::async_drop) on shutdown: it joins
//! the OS thread off the runtime. Dropping without it only clears the latch and
//! the alive flag; the OS thread ends on its next loop check.

use core::str;
use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicUsize},
    },
    time::Duration,
};

use ibapi::{
    Client,
    orders::{ExecutionData, OrderUpdate},
};
use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBufferProducer;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::{
    execution::{fx_backed_up_order::OrderStore, order_update_stream},
    market_data::consolidator::Consolidator,
    strategy::strategy::StrategyDetails,
};

/// Process-wide latch: `0` when no controller exists, `1` once one has been
/// constructed. Reset by [`OrderUpdateStreamController::async_drop`] and by
/// [`Drop`].
static ORDER_UPDATE_STREAM_NO: AtomicUsize = AtomicUsize::new(0);

/// The subset of IBKR's `orderStatus.status` values this crate reacts to.
///
/// IBKR sends a free-form string; anything unrecognised maps to
/// [`Unknown`](StatusOfOrderStatus::Unknown), which the dispatcher treats as an
/// error rather than silently ignoring.
#[derive(Debug, PartialEq, Eq)]
pub enum StatusOfOrderStatus {
    /// Accepted by TWS, not yet sent to the broker.
    ApiPending,
    /// Being submitted.
    PendingSubmit,
    /// A cancellation request is in flight.
    PendingCancel,
    /// Accepted by the broker but not filled.
    PreSubmitted,
    /// Working at the broker.
    Submitted,
    /// Cancelled by the API.
    ApiCancelled,
    /// Cancelled.
    Cancelled,
    /// Fully filled.
    Filled,
    /// Rejected or deactivated by the broker.
    Inactive,
    /// Unrecognised wire value.
    Unknown,
}

impl StatusOfOrderStatus {
    /// Parses the exact IBKR wire strings; matching is case-sensitive.
    ///
    /// # Example
    ///
    /// ```
    /// use trading_app::execution::order_update_stream::controller::StatusOfOrderStatus;
    ///
    /// assert_eq!(StatusOfOrderStatus::from_str("Submitted"), StatusOfOrderStatus::Submitted);
    /// assert_eq!(StatusOfOrderStatus::from_str("ApiCancelled"), StatusOfOrderStatus::ApiCancelled);
    /// // Unrecognised values (including wrong casing) become Unknown.
    /// assert_eq!(StatusOfOrderStatus::from_str("submitted"), StatusOfOrderStatus::Unknown);
    /// assert_eq!(StatusOfOrderStatus::from_str(""), StatusOfOrderStatus::Unknown);
    /// ```
    pub fn from_str(input: &str) -> StatusOfOrderStatus {
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

/// Handle to the process-wide order-update subscription.
///
/// Dropping this does not join the OS thread; call
/// [`async_drop`](Self::async_drop) for a clean shutdown that also releases the
/// singleton latch before returning.
pub struct OrderUpdateStreamController {
    /// Flipped to `false` to make the OS thread leave its loop.
    is_alive: Arc<AtomicBool>,
    /// The blocking subscription thread, joined by [`async_drop`](Self::async_drop).
    stream_handle: Option<std::thread::JoinHandle<()>>,
    /// The Tokio task that dispatches events to the handlers.
    async_handle: Option<tokio::task::JoinHandle<()>>,
}

impl OrderUpdateStreamController {
    /// Stops the subscription and waits for both the OS thread and the async
    /// dispatcher to finish.
    ///
    /// Clears the singleton latch first, flips the alive flag, joins the OS
    /// thread on a blocking-pool thread, then awaits the async task. Errors from
    /// either join are logged, not returned. Missing handles (already dropped or
    /// partially initialised) are logged as `"Stream handle is empty"`.
    pub async fn async_drop(&mut self) {
        ORDER_UPDATE_STREAM_NO.store(0, std::sync::atomic::Ordering::Release);
        self.is_alive
            .store(false, std::sync::atomic::Ordering::Release);
        let stream_handle_opt = self.stream_handle.take();
        let async_handle_opt = self.async_handle.take();
        tokio::task::spawn_blocking(move || {
            if let Some(stream_handle) = stream_handle_opt {
                if let Err(e) = stream_handle.join() {
                    tracing::error!("Failed to kill stream_handle properly: {e:?}");
                }
            } else {
                tracing::error!("Stream handle is empty");
            }
        });
        if let Some(async_handle) = async_handle_opt {
            if let Err(e) = async_handle.await {
                tracing::error!("Failed to kill async order_update_stream handle properly: {e:?}");
            }
        } else {
            tracing::error!("Stream handle is empty");
        }
    }
}

impl Drop for OrderUpdateStreamController {
    /// Clears the latch and the alive flag so another controller can be
    /// constructed. Does **not** wait for the threads; prefer
    /// [`async_drop`](OrderUpdateStreamController::async_drop) at shutdown.
    fn drop(&mut self) {
        ORDER_UPDATE_STREAM_NO.store(0, std::sync::atomic::Ordering::Release);
        self.is_alive
            .store(false, std::sync::atomic::Ordering::Release);
        println!("Dropping order update stream");
    }
}

impl OrderUpdateStreamController {
    /// Subscribes to the order stream and starts dispatching events.
    ///
    /// The blocking half runs on a dedicated OS thread named
    /// `order_update_stream`: it subscribes with a 5-second poll timeout,
    /// resubscribes after a stream error, and forwards each event through a
    /// 1024-slot channel. The async half drains that channel and calls
    /// `on_order_update_received`.
    ///
    /// `default_strategy` names the strategy used when an execution carries no
    /// order reference; `None` becomes `"unknown"`. `execution_events_queue`
    /// receives the strategy hooks for accepted fills, and `backed_up_orders`
    /// supplies parked orders for post-fill placement.
    ///
    /// Returns `None` when a controller already exists (including when a
    /// previous controller was dropped without clearing the latch, or when the
    /// first subscription attempt failed). Initialisation failures are logged.
    ///
    /// # Panics
    ///
    /// Panics on the subscription thread if the client id is not `0`, because
    /// only client `0` receives the account-wide order stream.
    pub fn new(
        pool: PgPool,
        consolidator: Weak<Consolidator>,
        weak_client: Weak<Client>,
        strategy_details: Arc<HashMap<String, StrategyDetails>>,
        execution_events_queue: Arc<HashMap<String, SpmcRingBufferProducer<ExecutionData, 64, 1>>>,
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
        let is_alive = Arc::new(AtomicBool::new(true));
        let cloned_is_alive = is_alive.clone();

        // spawn a new os blocking thread to await for updates synchronously
        // - send updates via channel back to async runtime
        let weak_client_cloned = weak_client.clone();
        let stream_handle = std::thread::Builder::new().name("order_update_stream".to_string()).spawn(move || {
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

            while is_alive.load(std::sync::atomic::Ordering::Acquire){
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
            }

            tracing::info!("Order Update Stream killed");
        }).expect("Expected to be able to boot order_update_stream thread properly");

        // async reciever that asynchronously awaits for updates
        let pool = pool.clone();
        let def_strat = default_strategy.unwrap_or("unknown".to_string());
        let cloned_handle = handle.clone();
        let weak_client = weak_client.clone();
        let backed_up_orders = backed_up_orders.clone();
        let async_handle = handle.spawn(async move {
            loop {
                match rx.recv().await {
                    Some(order_update) => {
                        tracing::info!("Received order update in tokio runtime");
                        if let Err(e) = on_order_update_received(
                            strategy_details.clone(),
                            execution_events_queue.clone(),
                            pool.clone(),
                            order_update,
                            def_strat.as_str(),
                            cloned_handle.clone(),
                            &consolidator,
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
            is_alive: cloned_is_alive,
            stream_handle: Some(stream_handle),
            async_handle: Some(async_handle),
        })
    }
}

/// Routes one IBKR order event to the handler that owns it.
///
/// Async only because `OpenOrder` waits for the spawned database task so an
/// error surfaces here (it is logged by the caller).
///
/// # Errors
///
/// - An `OrderStatus` whose status string is unrecognised.
/// - An execution handler failure (validation, strategy lookup, database).
/// - A commission-report failure (e.g. a fee that cannot be represented as a
///   decimal).
///
/// Handler failures for `OrderStatus` and `OpenOrder` are logged and reported as
/// `Ok`; only executions and commissions propagate.
async fn on_order_update_received(
    strategy_details: Arc<HashMap<String, StrategyDetails>>,
    execution_events_queue: Arc<HashMap<String, SpmcRingBufferProducer<ExecutionData, 64, 1>>>,
    pool: PgPool,
    order_update: OrderUpdate,
    default_strategy: &str,
    handle: tokio::runtime::Handle,
    consolidator: &Weak<Consolidator>,
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
                StatusOfOrderStatus::PendingCancel
                | StatusOfOrderStatus::Inactive
                | StatusOfOrderStatus::ApiCancelled
                | StatusOfOrderStatus::Cancelled => {
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
                | StatusOfOrderStatus::Filled
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
                        &consolidator,
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
                StatusOfOrderStatus::Inactive
                | StatusOfOrderStatus::PendingCancel
                | StatusOfOrderStatus::Cancelled
                | StatusOfOrderStatus::ApiCancelled => Ok(()),
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
                &consolidator,
                pool.clone(),
                execution_data,
                execution_events_queue,
                &strategy_details,
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
