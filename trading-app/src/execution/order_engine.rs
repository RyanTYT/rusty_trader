// Issues: Line 619
// - NEED to update place_order to function properly and efficiently
// await for order updates
// - once async version is released and stable - can use tokio threads instead of new os kernel
// thread
// - More issues:
// -- Need to deconflict potential revisions to executions - .02 is a revision to an execution with
// .01 and so on
// -- Finish writing the place_orders_for_strategy for options
// -- Update the place_orders_for_strategy functionality to update according to current open
// orders, ...
// -- place_orders_for_strategy fn will update open orders but if it updates it too late, an
// execution may come in with no open order and execution (NOTE: from my understanding this should
// be fine as the new execution will be filed under "unknown" for which target positions will
// always be 0 && when we delete the open order it is only because the open order is in the
// opposite direction we want to go in - i.e. under "unknown", position to go to 0 is also fine -
// just maybe different order types but that is fine - should be minimal impact)
use core::str;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread::{self, scope},
};

use ibapi::{
    Client,
    orders::{ExecutionFilter, Executions, Order, OrderStatus, OrderUpdate},
    prelude::{Contract, PositionUpdate, SecurityType},
};
use ordered_float::OrderedFloat;
use sqlx::PgPool;
use tokio::sync::mpsc::channel;
use tracing::info;

use crate::{
    database::{
        crud::CRUDTrait,
        models::{AssetType, OptionType},
        models_crud::{
            current_option_positions::get_specific_current_option_positions_crud,
            current_stock_positions::{
                get_current_stock_positions_crud, get_specific_current_stock_positions_crud,
            },
            target_option_positions::get_specific_target_option_positions_crud,
            target_stock_positions::get_specific_target_stock_positions_crud,
        },
    },
    execution::{
        events::order_events::{
            on_commission_update, on_execution_update, on_new_option_qty_diff_for_strat,
            on_new_stock_qty_diff_for_strat,
        },
        on_full_open_order_received,
        order_update_stream::on_order_update_received,
        place_order::place_order,
    },
    strategy::strategy::StrategyExecutor,
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
            "Submitted" => StatusOfOrderStatus::Submitted,
            "ApiCancelled" => StatusOfOrderStatus::ApiCancelled,
            "Cancelled" => StatusOfOrderStatus::Cancelled,
            "Filled" => StatusOfOrderStatus::Filled,
            "Inactive" => StatusOfOrderStatus::Inactive,
            _ => StatusOfOrderStatus::Unknown,
        }
    }
}

pub struct OrderEngine {
    pub pool: PgPool,
    // order_id
    // - Gotten in many places, but inserts ONLY during place_order()
    order_map: Arc<Mutex<HashMap<i32, (String, Contract, Order)>>>,
    // Security Type, Symbol
    contract_to_strategy: HashMap<(String, String), String>,
}

// Dummy implementations since in the app, only 1 should live at any point in time
impl PartialEq for OrderEngine {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for OrderEngine {}

impl PartialOrd for OrderEngine {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        Some(1.cmp(&2))
    }
}

impl Ord for OrderEngine {
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        1.cmp(&2)
    }
}

impl OrderEngine {
    // Active Strategies passed for deconflicting of executions in cases where it occurs
    pub fn new<T: StrategyExecutor>(pool: PgPool, active_strategies: Vec<T>) -> Self {
        let mut contract_to_full_strategy: HashMap<(String, String), T> = HashMap::new();
        for strategy in active_strategies {
            for contract in strategy.get_contracts() {
                let symbol = if contract.security_type == SecurityType::Future {
                    format!("FUT:{}", contract.symbol.clone())
                } else if contract.security_type == SecurityType::Stock {
                    contract.symbol.clone()
                } else if contract.security_type == SecurityType::Option {
                    contract.symbol.clone()
                } else if contract.security_type == SecurityType::ForexPair {
                    contract.symbol.clone()
                } else {
                    String::from("Unknown")
                };
                if contract_to_full_strategy
                    .contains_key(&(contract.security_type.to_string().clone(), symbol.clone()))
                {
                    // Update contract_to_strategy
                    let current_strategy = contract_to_full_strategy
                        .get(&(contract.security_type.to_string().clone(), symbol.clone()))
                        .unwrap();
                    if &strategy > current_strategy {
                        contract_to_full_strategy.insert(
                            (contract.security_type.to_string().clone(), symbol.clone()),
                            strategy.clone(),
                        );
                    }
                } else {
                    // Update contract_to_strategy
                    contract_to_full_strategy.insert(
                        (contract.security_type.to_string().clone(), symbol.clone()),
                        strategy.clone(),
                    );
                }
            }
        }
        let mut contract_to_strategy = HashMap::new();
        for (contract, full_strategy) in contract_to_full_strategy.iter() {
            contract_to_strategy.insert(contract.clone(), full_strategy.get_name());
        }
        Self {
            pool,
            order_map: Arc::new(Mutex::new(HashMap::new())),
            contract_to_strategy,
        }
    }

    // Call before sync_positions - tries its best to sync all missed orders since last session
    // - but may miss some position updates -> Have to reconcile manually and via sync_positions
    pub fn sync_executions(&self, client: &Client) -> Result<(), String> {
        let subscription = client
            .executions(ibapi::orders::ExecutionFilter {
                ..ExecutionFilter::default()
            })
            .expect("Error requesting executions for sync_executions");
        // Assumption: Will always receive execution 1st b4 associated commission
        for execution in subscription {
            match execution {
                Executions::ExecutionData(execution_data) => {
                    let strategy = {
                        let order_map =
                            unlock!(self.order_map, "order_map", "OrderEngine.sync_executions");
                        order_map
                            .get(&execution_data.execution.order_id)
                            .map_or(
                                "Unknown strategy: not recorded in order_map".to_string(),
                                |v| v.0.clone(),
                            )
                            .clone()
                    };
                    tracing::info!(
                        "Syncing Executions: New Execution recorded with id: {} for strategy: {}",
                        &execution_data.request_id,
                        &strategy
                    );

                    // {
                    //     let mut execution_map = unlock!(
                    //         self.execution_map,
                    //         "execution_map",
                    //         "OrderEngine.sync_executions"
                    //     );
                    //     execution_map.insert(
                    //         execution_data.execution.execution_id.clone(),
                    //         execution_data.clone(),
                    //     );
                    // }

                    on_execution_update(self.pool.clone(), execution_data);
                }

                Executions::CommissionReport(commission_report) => {
                    // let execution_map = unlock!(
                    //     self.execution_map,
                    //     "execution_map",
                    //     "OrderEngine.sync_executions"
                    // );
                    // let execution_data =
                    //     execution_map.get(&commission_report.execution_id).expect("Syncing Executions: Execution map in OrderEngine does not contain request_id from commission_report: Error in ordering received from IBKR!");
                    // let order_map = unlock!(
                    //     self.order_map,
                    //     "order_map",
                    //     "OrderEngine.order_update_stream"
                    // );
                    // let strategy = order_map.get(&execution_data.execution.order_id);
                    // tracing::info!(
                    //     "Syncing Executions: Commissions updated for execution with id: {}, Strategy: {}",
                    //     &execution_data.request_id,
                    //     &strategy.map_or("Unknown Strategy (Strategy was not recorded)", |v| v
                    //         .0
                    //         .as_str())
                    // );

                    if let Err(e) = on_commission_update(self.pool.clone(), commission_report) {
                        tracing::error!("Error while running OrderEngine.sync_executions: {}", e);
                    };
                }

                Executions::Notice(message) => {
                    tracing::warn!("Message from OrderEngine.sync_executions: {}", message);
                }
            }
        }

        Ok(())
    }

    // Tries to reconcile via strategy priority in cases of conflict
    pub fn sync_open_orders(&self, client: &Client) {
        let mut open_orders: HashMap<i32, (Option<Contract>, Option<Order>, Option<OrderStatus>)> =
            HashMap::new();
        let subscription = client
            .all_open_orders()
            .expect("Error requesting all_open_orders for sync_open_orders");
        for open_order in subscription {
            match open_order {
                ibapi::orders::Orders::OrderData(order_data) => {
                    if open_orders.contains_key(&order_data.order.perm_id) {
                        let entry = open_orders.get(&order_data.order.perm_id).unwrap();
                        on_full_open_order_received::on_full_open_order_received(
                            self.contract_to_strategy.clone(),
                            self.pool.clone(),
                            order_data.contract,
                            order_data.order,
                            entry
                                .2
                                .as_ref()
                                .expect("Expected OrderStatus to have already been received!")
                                .clone(),
                        );
                    } else {
                        open_orders.insert(
                            order_data.order.perm_id,
                            (Some(order_data.contract), Some(order_data.order), None),
                        );
                    }
                }
                ibapi::orders::Orders::OrderStatus(order_status) => {
                    if open_orders.contains_key(&order_status.perm_id) {
                        let entry = open_orders.get(&order_status.perm_id).unwrap();
                        on_full_open_order_received::on_full_open_order_received(
                            self.contract_to_strategy.clone(),
                            self.pool.clone(),
                            entry
                                .0
                                .as_ref()
                                .expect("Expected Contract to have already been received!")
                                .clone(),
                            entry
                                .1
                                .as_ref()
                                .expect("Expected Order to have already been received!")
                                .clone(),
                            order_status.clone(),
                        );
                    } else {
                        open_orders.insert(
                            order_status.perm_id,
                            (None, None, Some(order_status.clone())),
                        );
                    }
                }
                ibapi::orders::Orders::Notice(notice) => {
                    tracing::warn!("Notice from OrderEngine.sync_open_orders: {}", notice);
                }
            }
        }
    }

    pub fn sync_positions(&self, client: &Client) {
        let mut stock_map: HashMap<String, f64> = HashMap::new();
        let mut option_map: HashMap<(String, OrderedFloat<f64>, String, String, OptionType), f64> =
            HashMap::new();
        scope(|s| {
            s.spawn(|| async {
                let current_stock_positions_crud =
                    get_specific_current_stock_positions_crud(self.pool.clone());
                let current_stock_positions_res = &current_stock_positions_crud
                    .get_all_positions_by_stock()
                    .await;

                match current_stock_positions_res {
                    Ok(current_stock_positions) => {
                        for position in current_stock_positions {
                            stock_map.insert(position.stock.clone(), position.quantity.clone());
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error trying to read all stock positions in DB: {}", e)
                    }
                }
            });
            s.spawn(|| async {
                let current_option_positions_crud =
                    get_specific_current_option_positions_crud(self.pool.clone());
                let current_option_positions_res = current_option_positions_crud
                    .get_all_positions_by_contract()
                    .await;

                match current_option_positions_res {
                    Ok(current_option_positions) => {
                        for position in current_option_positions {
                            option_map.insert(
                                (
                                    position.stock,
                                    OrderedFloat::from(position.strike),
                                    position.expiry,
                                    position.multiplier,
                                    position.option_type,
                                ),
                                position.quantity.clone(),
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error trying to read all option positions in DB: {}", e)
                    }
                }
            });
        });

        let subscription = client
            .positions()
            .expect("Error requesting positions for sync_positions");
        for position_response in subscription.iter() {
            match position_response {
                PositionUpdate::Position(position) => {
                    match position.contract.security_type {
                        SecurityType::Stock | SecurityType::Future | SecurityType::ForexPair => {
                            match &stock_map.get(&position.contract.symbol) {
                                Some(local_pos) => {
                                    if **local_pos != position.position {
                                        tracing::warn!(
                                            "Reconciling current stock position according to broker position (Local: {}, Broker: {})",
                                            local_pos,
                                            &position.position
                                        );

                                        let current_stock_positions_crud =
                                            get_specific_current_stock_positions_crud(
                                                self.pool.clone(),
                                            );
                                        let symbol = if position.contract.security_type
                                            == SecurityType::Future
                                        {
                                            format!("FUT:{}", position.contract.symbol.clone())
                                        } else {
                                            position.contract.symbol.clone()
                                        };
                                        let discrepancy = (position.position - **local_pos).clone();
                                        tokio::spawn(async move {
                                            match current_stock_positions_crud
                                                .update_unknown_strat_positions(
                                                    symbol.clone(),
                                                    discrepancy,
                                                )
                                                .await
                                            {
                                                Ok(_) => {
                                                    tracing::warn!(
                                                        "Discrepancy in stock positions, allocated to strategy unknown: {} for qty of {}",
                                                        symbol,
                                                        position.position
                                                    )
                                                }
                                                Err(e) => {
                                                    tracing::error!(
                                                        "Error trying to reconcile Discrepancy in stock positions: {}",
                                                        e
                                                    )
                                                }
                                            };
                                        });
                                    }
                                }
                                None => {
                                    tracing::warn!(
                                        "Reconciling current stock position according to broker position (Local: {}, Broker: {})",
                                        0.0,
                                        &position.position
                                    );
                                    let current_stock_positions_crud =
                                        get_current_stock_positions_crud(self.pool.clone());
                                    let strategy = self
                                        .contract_to_strategy
                                        .get(&(
                                            position.contract.security_type.clone().to_string(),
                                            position.contract.symbol.clone(),
                                        ))
                                        .map_or(String::from("unknown"), |v| v.to_string());
                                    tokio::spawn(async move {
                                        let symbol = if position.contract.security_type
                                            == SecurityType::Future
                                        {
                                            format!("FUT:{}", position.contract.symbol.clone())
                                        } else {
                                            position.contract.symbol.clone()
                                        };
                                        if let Err(e) = current_stock_positions_crud.create(&crate::database::models::CurrentStockPositionsFullKeys {
                                        stock: symbol,
                                        primary_exchange: position.contract.primary_exchange,
                                        strategy: strategy,
                                        quantity: position.position.clone(),
                                        avg_price: position.average_cost.clone()
                                    }).await {
                                        tracing::error!("Error inserting into Current Stock Positions when reconciling stock positions (Local: {}, Broker: {}): {}", 0.0, &position.position, e)
                                    }
                                    });
                                }
                            }
                        }
                        SecurityType::Option => match &stock_map.get(&position.contract.symbol) {
                            Some(local_pos) => {
                                if **local_pos != position.position {
                                    tracing::warn!(
                                        "Reconciling current option position according to broker position (Local: {}, Broker: {})",
                                        local_pos,
                                        &position.position
                                    );

                                    let current_option_positions_crud =
                                        get_specific_current_option_positions_crud(
                                            self.pool.clone(),
                                        );
                                    let symbol = if position.contract.security_type
                                        == SecurityType::Future
                                    {
                                        format!("FUT:{}", position.contract.symbol.clone())
                                    } else {
                                        position.contract.symbol.clone()
                                    };
                                    let primary_exchange =
                                        position.contract.primary_exchange.clone();
                                    let (expiry, strike, multiplier, option_type) = (
                                        position.contract.last_trade_date_or_contract_month.clone(),
                                        position.contract.strike.clone(),
                                        position.contract.multiplier.clone(),
                                        OptionType::from_str(&position.contract.right).expect("Error decoding contract right to OptionType while Reconciling options positions"),
                                    );
                                    let discrepancy = (position.position - **local_pos).clone();
                                    tokio::spawn(async move {
                                        match current_option_positions_crud
                                            .update_unknown_strat_positions(
                                                symbol.clone(),
                                                primary_exchange,
                                                expiry,
                                                strike,
                                                multiplier,
                                                option_type,
                                                discrepancy,
                                            )
                                            .await
                                        {
                                            Ok(_) => {
                                                tracing::warn!(
                                                    "Discrepancy in stock positions, allocated to strategy unknown: {} for qty of {}",
                                                    symbol,
                                                    position.position
                                                )
                                            }
                                            Err(e) => {
                                                tracing::error!(
                                                    "Error trying to reconcile Discrepancy in stock positions: {}",
                                                    e
                                                )
                                            }
                                        };
                                    });
                                }
                            }
                            None => {
                                tracing::warn!(
                                    "Reconciling current stock position according to broker position (Local: {}, Broker: {})",
                                    0.0,
                                    &position.position
                                );
                                let current_stock_positions_crud =
                                    get_current_stock_positions_crud(self.pool.clone());
                                let strategy = self
                                    .contract_to_strategy
                                    .get(&(
                                        position.contract.security_type.clone().to_string(),
                                        position.contract.symbol.clone(),
                                    ))
                                    .map_or(String::from("unknown"), |v| v.to_string());
                                tokio::spawn(async move {
                                    let symbol = if position.contract.security_type
                                        == SecurityType::Future
                                    {
                                        format!("FUT:{}", position.contract.symbol.clone())
                                    } else {
                                        position.contract.symbol.clone()
                                    };
                                    if let Err(e) = current_stock_positions_crud.create(&crate::database::models::CurrentStockPositionsFullKeys {
                                        stock: symbol,
                                        primary_exchange: position.contract.primary_exchange,
                                        strategy: strategy,
                                        quantity: position.position.clone(),
                                        avg_price: position.average_cost.clone()
                                    }).await {
                                        tracing::error!("Error inserting into Current Stock Positions when reconciling stock positions (Local: {}, Broker: {}): {}", 0.0, &position.position, e)
                                    }
                                });
                            }
                        },
                        _ => {
                            tracing::error!(
                                "New Security Type encountered when reconciling current positions: {}",
                                position.contract.security_type
                            )
                        }
                    }
                }
                PositionUpdate::PositionEnd => {
                    println!("initial set of positions received");
                    break;
                }
            }
        }
    }

    /// Initialises the Order Update Stream to listen for all order events for the client
    /// Note: Should only be run once for initialisation - creates a channel on each call
    /// NOTE: initialises a synchronous thread and sends msgs to async runtime - blocking_send if
    /// not handled quickly could block up channel and stow updates indefinitely
    pub fn init_order_update_stream(&self, client: Arc<Client>) {
        // https://ibridgepy.com/ib-api-knowledge-base/#step1-1-17
        // openOrder( ) is triggered twice automatically. When the order is initially accepted and when the order is fully executed. When the order is initially accepted, you would get an openOrder( ) and orderStatus( ) call back. Then if there are partial fills or any other status changes you would receive additional orderStatus( ) call back. Then if you receive additional orderStatus( ) call back, when the order fully executes you would get a final orderStatus( ) followed by an openOrder( ) and then receive the execDetails( ) and commissionReport( ). If you invoke reqOpenOrders( ), it will only relay the last orderStatus( ) of any current working order.
        let (sender, mut rx) = channel::<OrderUpdate>(100);

        // spawn a new os blocking thread to await for updates synchronously - send updates via
        // channel back to app
        thread::spawn(move || {
            let event_subscription = {
                assert!(client.client_id() == 0);
                let event_subscription = client
                    .order_update_stream()
                    .map_err(|e| {
                        format!("Failed to begin order_update_stream in OrderEngine: {}", e)
                    })
                    .expect("Expected to be able to subscribe to order updates from client");
                event_subscription
            };
            info!("Subscribed for updates for orders!");

            while let Some(event) = event_subscription.next() {
                info!("New order event received!");
                let cloned_sender = sender.clone();
                thread::spawn(move || {
                    cloned_sender.blocking_send(event);
                });
            }
            info!("Order event subscription ended!");
        });

        // async reciever that asynchronously awaits for updates
        let order_map = self.order_map.clone();
        let pool = self.pool.clone();
        tokio::spawn(async move {
            while let Some(order_update) = rx.recv().await {
                // all awaitable events within this is spawned asynchronously
                if let Err(e) =
                    on_order_update_received(order_map.clone(), pool.clone(), order_update).await
                {
                    tracing::error!("on_order_update_received error: {}", e)
                };
            }
        });
    }

    pub async fn place_order(
        &self,
        strategy: String,
        client: Arc<Client>,
        contract: Contract,
        order: Order,
        override_others: bool,
    ) -> Result<(), String> {
        let cloned_order_map = self.order_map.clone();
        tokio::spawn(async move {
            place_order(
                cloned_order_map,
                strategy,
                client,
                contract,
                order,
                override_others,
            )
        });
        Ok(())
    }

    pub fn place_orders_for_strategy<T: StrategyExecutor + 'static>(
        &self,
        strategy: T,
        contract: Contract,
        client: Arc<Client>,
        asset_type: AssetType,
        ignore_contract_for_strategy: bool,
    ) {
        info!("Placing orders for {}", strategy.get_name());
        match asset_type {
            AssetType::Stock => {
                let pool = self.pool.clone();
                let client = client.clone();
                let order_map = self.order_map.clone();
                let target_stock_positions_crud =
                    get_specific_target_stock_positions_crud(self.pool.clone());
                let strategy = strategy.clone();
                tokio::spawn(async move {
                    match {
                        if ignore_contract_for_strategy {
                            target_stock_positions_crud
                                .get_target_pos_diff_strat(strategy.get_name())
                                .await
                        } else {
                            target_stock_positions_crud
                                .get_target_pos_diff(strategy.get_name(), contract.symbol.clone())
                                .await
                        }
                    } {
                        Ok(pos_diffs) => {
                            info!(
                                "Detected diff of {} between current and target",
                                &pos_diffs.len()
                            );
                            pos_diffs.iter().for_each(|pos_diff| {
                                let pool = pool.clone();
                                let client = client.clone();
                                let order_map = order_map.clone();
                                let strategy = strategy.clone();
                                let contract_opt = strategy.get_contract(
                                    pos_diff.stock.clone(),
                                    pos_diff.primary_exchange.clone(),
                                );
                                if contract_opt.is_none() {
                                    tracing::warn!(
                                        "Warning: No contract for {} found for strategy {}",
                                        contract.symbol,
                                        strategy.get_name()
                                    );
                                    return;
                                }
                                let contract = contract_opt.unwrap();
                                let (qty_diff, avg_price) = (pos_diff.qty_diff, pos_diff.avg_price);
                                tokio::spawn(async move {
                                    on_new_stock_qty_diff_for_strat(
                                        pool,
                                        contract,
                                        client,
                                        order_map,
                                        strategy.get_name(),
                                        qty_diff,
                                        avg_price,
                                    )
                                    .await;
                                });
                            });
                        }
                        Err(e) => {
                            tracing::error!(
                                "Error generating differences in stock positions for {}: {}",
                                strategy.get_name(),
                                e
                            );
                        }
                    }
                });
            }
            AssetType::Option => {
                let pool = self.pool.clone();
                let client = client.clone();
                let order_map = self.order_map.clone();
                let target_option_positions_crud =
                    get_specific_target_option_positions_crud(self.pool.clone());
                let strategy = strategy.clone();
                tokio::spawn(async move {
                    match target_option_positions_crud
                        .get_target_pos_diff(
                            strategy.get_name(),
                            contract.symbol,
                            contract.primary_exchange,
                            contract.last_trade_date_or_contract_month,
                            contract.strike,
                            contract.multiplier,
                            OptionType::from_str(&contract.right).expect(
                                "Expected to be able to parse contract right for options contract",
                            ),
                        )
                        .await
                    {
                        Ok(pos_diffs) => {
                            pos_diffs.iter().for_each(|pos_diff| {
                                let pool = pool.clone();
                                let client = client.clone();
                                let order_map = order_map.clone();
                                let strategy = strategy.clone();
                                let contract_opt = strategy.get_contract(
                                    pos_diff.stock.clone(),
                                    pos_diff.primary_exchange.clone(),
                                );
                                if contract_opt.is_none() {
                                    return;
                                }
                                let contract = contract_opt.unwrap();
                                let (qty_diff, avg_price) = (pos_diff.qty_diff, pos_diff.avg_price);
                                tokio::spawn(async move {
                                    on_new_option_qty_diff_for_strat(
                                        pool,
                                        contract,
                                        client,
                                        order_map,
                                        strategy.get_name(),
                                        qty_diff,
                                        avg_price,
                                    )
                                    .await;
                                });
                            });
                        }
                        Err(_) => {
                            tracing::error!(
                                "Error generating differences in stock positions for {}",
                                strategy.get_name()
                            );
                        }
                    }
                });
            }
        }
    }
}
