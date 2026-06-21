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
use ibapi::{
    Client,
    accounts::{AccountValue, types::AccountId},
    orders::{ExecutionFilter, Executions, Order, OrderStatus, OrderUpdate},
    prelude::{Contract, PositionUpdateMulti, SecurityType},
};
use sqlx::PgPool;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock, Weak},
    thread::{self},
    time::Duration,
};
use tokio::sync::mpsc::channel;
use tracing::{info, warn};

use crate::{
    database::{
        crud::CRUDTrait,
        models::{
            AssetType, CurrentOptionPositionsPrimaryKeys, CurrentOptionPositionsUpdateKeys,
            CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys, OptionType,
        },
        models_crud::{
            current_option_positions::{
                get_current_option_positions_crud, get_specific_current_option_positions_crud,
            },
            current_stock_positions::get_specific_current_stock_positions_crud,
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
    helpers::contract::{HashContract, get_contract_from_local_symbol, get_local_symbol},
    init_app::StrategyParameters,
    market_data::consolidator::Consolidator,
    strategy::strategy::{StrategyEnum, StrategyExecutor},
};

pub struct OrderEngine {
    pub pool: PgPool,
    // order_id
    // - Gotten in many places, but inserts ONLY during place_order() && sync_open_orders
    // - order_id -> (strategy, contract, order)
    order_map: Arc<RwLock<HashMap<i32, (String, Contract, Order)>>>,
    strategy_map: HashMap<String, StrategyEnum>,
    // Security Type, Symbol
    contract_to_strategy: HashMap<HashContract, String>,
    account: String,

    order_update_stream_killer: Arc<Mutex<Option<Arc<bool>>>>,
    order_update_stream_tokio_killer: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
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
    pub fn new(pool: PgPool, account: String, active_strategies: &Vec<StrategyParameters>) -> Self {
        let mut contract_to_full_strategy: HashMap<HashContract, StrategyEnum> = HashMap::new();
        for strategy in active_strategies {
            for contract_sub in &strategy.subscribed_contracts {
                let hashed_contract = HashContract {
                    contract: contract_sub.contract.clone(),
                };
                if contract_to_full_strategy.contains_key(&hashed_contract) {
                    // Update contract_to_strategy
                    let current_strategy = contract_to_full_strategy.get(&hashed_contract).unwrap();
                    if strategy.strategy > *current_strategy {
                        contract_to_full_strategy
                            .insert(hashed_contract, strategy.strategy.clone());
                    }
                } else {
                    // Update contract_to_strategy
                    contract_to_full_strategy.insert(hashed_contract, strategy.strategy.clone());
                }
            }
        }
        let mut contract_to_strategy = HashMap::new();
        for (contract, full_strategy) in contract_to_full_strategy.iter() {
            contract_to_strategy.insert(contract.clone(), full_strategy.get_name());
        }

        // create strategy map
        let mut strategy_map = HashMap::new();
        for strategy in active_strategies {
            strategy_map.insert(strategy.strategy.get_name(), strategy.strategy.clone());
        }

        Self {
            pool,
            order_map: Arc::new(RwLock::new(HashMap::new())),
            strategy_map,
            contract_to_strategy,
            account,

            order_update_stream_killer: Arc::new(Mutex::new(None)),
            order_update_stream_tokio_killer: Arc::new(Mutex::new(None)),
        }
    }

    // Call before sync_positions - tries its best to sync all missed orders since last session
    // - but may miss some position updates -> Have to reconcile manually and via sync_positions
    pub fn sync_executions(
        &self,
        client: &Client,
        default_strategy: Option<String>,
    ) -> Result<(), String> {
        let subscription = client
            .executions(ibapi::orders::ExecutionFilter {
                ..ExecutionFilter::default()
            })
            .expect("Error requesting executions for sync_executions");
        // Assumption: Will always receive execution 1st b4 associated commission
        let def_strat = default_strategy.unwrap_or("unknown".to_string());
        for execution in subscription.timeout_iter(std::time::Duration::from_secs(10)) {
            match execution {
                Executions::ExecutionData(execution_data) => {
                    let strategy = {
                        let order_map = self
                            .order_map
                            .read()
                            .expect("Expected read lock for order_map not to be poisoned");
                        order_map
                            .get(&execution_data.execution.order_id)
                            .map_or(&def_strat, |v| &v.0)
                            .clone()
                    };
                    tracing::info!(
                        message=%format!(
                            "Syncing Executions: New Execution recorded with id: {} for strategy: {}",
                            &execution_data.request_id,
                            &strategy
                        )
                    );

                    on_execution_update(
                        self.pool.clone(),
                        execution_data,
                        self.strategy_map.clone(),
                        &def_strat,
                    );
                }

                Executions::CommissionReport(commission_report) => {
                    if let Err(e) = on_commission_update(self.pool.clone(), &commission_report) {
                        tracing::error!("Error while running OrderEngine.sync_executions: {e:?}");
                    };
                }

                Executions::Notice(message) => {
                    tracing::warn!("Message from OrderEngine.sync_executions: {message:?}");
                }
            }
        }

        Ok(())
    }

    /// To update for default_strategy soon
    // Tries to reconcile via strategy priority in cases of conflict
    pub fn sync_open_orders(
        &self,
        client: &Client,
        consolidator: &Consolidator,
        default_strategy: Option<String>,
    ) {
        // open_orders: for syncing between OrderData && OrderStatus
        let mut open_orders: HashMap<i32, (Option<Contract>, Option<Order>, Option<OrderStatus>)> =
            HashMap::new();
        let subscription = client
            .all_open_orders()
            .expect("Error requesting all_open_orders for sync_open_orders");
        for open_order in subscription.timeout_iter(std::time::Duration::from_secs(10)) {
            match open_order {
                ibapi::orders::Orders::OrderData(order_data) => {
                    if open_orders.contains_key(&order_data.order.perm_id) {
                        let order_perm_id = order_data.order.perm_id.clone();
                        let entry = open_orders.get(&order_data.order.perm_id).unwrap();
                        let order_status = {
                            if entry.2.is_none() {
                                continue;
                            }
                            entry.2.as_ref().unwrap().clone()
                        };
                        on_full_open_order_received::on_full_open_order_received(
                            self.contract_to_strategy.clone(),
                            self.order_map.clone(),
                            self.pool.clone(),
                            consolidator,
                            &order_data.contract,
                            &order_data.order,
                            &order_status,
                            &default_strategy,
                        );
                        open_orders.remove(&order_perm_id);
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
                        let contract = {
                            if entry.0.is_none() {
                                continue;
                            }
                            entry.0.as_ref().unwrap().clone()
                        };
                        let order = {
                            if entry.1.is_none() {
                                continue;
                            }
                            entry.1.as_ref().unwrap().clone()
                        };
                        on_full_open_order_received::on_full_open_order_received(
                            self.contract_to_strategy.clone(),
                            self.order_map.clone(),
                            self.pool.clone(),
                            consolidator,
                            &contract,
                            &order,
                            &order_status,
                            &default_strategy,
                        );
                        open_orders.remove(&order_status.perm_id);
                    } else {
                        open_orders.insert(order_status.perm_id, (None, None, Some(order_status)));
                    }
                }
                ibapi::orders::Orders::Notice(notice) => {
                    tracing::warn!("Notice from OrderEngine.sync_open_orders: {notice:?}");
                }
            }
        }
        if let Some(e) = subscription.error() {
            tracing::warn!("Encountered Error while syncing open orders: {e:?}");
            return;
        }

        if !open_orders.is_empty() {
            tracing::warn!(
                message=%format!(
                    "The following key-value pairs still remain while syncing open_orders:\n{}",
                    open_orders
                        .iter_mut()
                        .map(|(key, value)| format!(
                            "{}: {}",
                            key,
                            format!(
                                "({}, {}, {})",
                                value
                                    .0
                                    .clone()
                                    .map_or("None".to_string(), |contract| format!(
                                        "{}:{}",
                                        contract.primary_exchange, contract.symbol
                                    )),
                                value.1.clone().map_or("None".to_string(), |order| format!(
                                    "action:{}:qty:{}",
                                    order.action, order.total_quantity
                                )),
                                value
                                    .2
                                    .clone()
                                    .map_or("None".to_string(), |order_status| format!(
                                        "order_status:{}:filled:{}",
                                        order_status.status, order_status.filled
                                    ))
                            )
                        ))
                        .collect::<Vec<String>>()
                        .join("\n")
                )
            );
        }
    }

    /// Syncs positions with IBKR positions - Options portion currently unimplemented as it is
    pub async fn sync_positions(
        &self,
        client: &Client,
        default_strategy: Option<String>,
        consolidator: &Consolidator,
    ) {
        let current_stock_positions_crud =
            get_specific_current_stock_positions_crud(self.pool.clone());
        let current_option_positions_crud =
            get_specific_current_option_positions_crud(self.pool.clone());
        let mut stock_map: HashMap<HashContract, f64> = HashMap::new();
        let mut option_map: HashMap<HashContract, f64> = HashMap::new();
        let mut fx_map: HashMap<String, f64> = HashMap::new();

        // Consolidate locally tracked positions
        match &current_stock_positions_crud
            .get_all_positions_by_stock()
            .await
        {
            Ok(current_stock_positions) => {
                tracing::info!("Received local stock positions, inserting into stock_map now");
                for position in current_stock_positions {
                    let built_contract = get_contract_from_local_symbol(
                        position.stock.as_str(),
                        position.primary_exchange.as_str(),
                        position.currency.as_str(),
                    );
                    if built_contract.security_type == SecurityType::ForexPair {
                        let currency = built_contract.currency.to_string();
                        let currency_val = position.quantity * position.fx_avg_price;
                        let quote = built_contract.symbol.to_string();
                        let quote_val = position.quantity;
                        if !fx_map.contains_key(&currency) {
                            fx_map.insert(currency.clone(), 0.0);
                        }
                        if !fx_map.contains_key(&quote) {
                            fx_map.insert(quote.clone(), 0.0);
                        }
                        if quote == "SGD" {
                            fx_map
                                .insert(quote.clone(), fx_map.get(&quote).unwrap() + currency_val);
                            continue;
                        }
                        fx_map.insert(
                            currency.clone(),
                            fx_map.get(&currency).unwrap() - currency_val,
                        );
                        fx_map.insert(quote.clone(), fx_map.get(&quote).unwrap() + quote_val);
                        continue;
                    }
                    stock_map.insert(
                        HashContract {
                            contract: built_contract,
                        },
                        position.quantity.clone(),
                    );
                }
            }
            Err(e) => {
                tracing::error!("Error trying to read all stock positions in DB: {e:?}")
            }
        };
        match current_option_positions_crud
            .get_all_positions_by_contract()
            .await
        {
            Ok(current_option_positions) => {
                tracing::info!("Received local option positions, inserting into stock_map now");
                for position in current_option_positions {
                    let built_contract = Contract::option(
                        &position.stock,
                        &position.expiry,
                        position.strike,
                        &position.option_type.to_string(),
                    );
                    let hashed_contract = HashContract {
                        contract: built_contract,
                    };
                    option_map.insert(hashed_contract, position.quantity.clone());
                }
            }
            Err(e) => {
                tracing::error!("Error trying to read all option positions in DB: {e:?}")
            }
        }

        // Compare FX broker positions
        let fx_subscription = client
            .account_updates(&AccountId(self.account.clone()))
            .expect("Error requesting account updates for client.account_updates");
        let mut broker_currency = HashMap::<String, f64>::new();
        while let Some(account_update) =
            fx_subscription.next_timeout(std::time::Duration::from_secs(10))
        {
            match account_update {
                ibapi::prelude::AccountUpdate::AccountValue(account_value) => {
                    if account_value.key == "CashBalance" {
                        let currency = if account_value.currency == "BASE" {
                            "SGD".to_string()
                        } else {
                            account_value.currency
                        };
                        broker_currency.insert(
                            currency,
                            account_value
                                .value
                                .parse::<f64>()
                                .expect("Expected f64 value from CashBalance AccountValue"),
                        );
                    }
                }
                ibapi::prelude::AccountUpdate::PortfolioValue(portfolio_value) => {
                    let currency = portfolio_value.contract.currency.as_str();
                    let currency_balance = portfolio_value.average_cost * portfolio_value.position;
                    if !broker_currency.contains_key(currency) {
                        broker_currency.insert(currency.to_string(), 0.0);
                    }
                    broker_currency.insert(
                        currency.to_string(),
                        broker_currency.get(currency).unwrap() + currency_balance,
                    );
                }
                ibapi::prelude::AccountUpdate::UpdateTime(_) => {
                    continue;
                }
                ibapi::prelude::AccountUpdate::End => {
                    break;
                }
            }
        }
        // Reconcile FX broker positions
        for (currency, value) in broker_currency {
            let local_value = fx_map.get(&currency).unwrap_or(&0.0);
            if (local_value - value).abs() < 1000.0 {
                continue;
            }
            tracing::error!("Local ({local_value}) | Broker ({value})");

            let adjustment = value - local_value;
            tracing::warn!(
                "Reconciling current & broker FX positions: Adding {adjustment} of {currency}"
            );
            if let Err(e) = current_stock_positions_crud
                .update_strat_positions(
                    &format!("CASH:{}", currency),
                    "",
                    "unknown",
                    "SGD",
                    &adjustment,
                    if currency == "SGD" {
                        Some(1.0)
                    } else {
                        consolidator
                            .get_current_price(
                                &get_contract_from_local_symbol(
                                    &format!("FX:{}/SGD", currency),
                                    "",
                                    "SGD",
                                ),
                                &false,
                                &[],
                            )
                            .ok()
                    },
                )
                .await
            {
                tracing::warn!(
                    "Error trying to update current stock positions for reconciliation of FX positions: {e:?}"
                );
            };
        }

        // Compare  broker positions
        let mut broker_stock_positions_received = HashSet::<HashContract>::new();
        let mut broker_option_positions_received = HashSet::<HashContract>::new();
        let subscription = client
            .positions_multi(
                Some(&ibapi::accounts::types::AccountId(self.account.clone())),
                None,
            )
            .expect("Error requesting positions for sync_positions");
        tracing::info!("Trying to get broker positions now");
        while let Some(position_response) =
            subscription.next_timeout(std::time::Duration::from_secs(10))
        {
            match position_response {
                PositionUpdateMulti::Position(position) => {
                    let contract = {
                        let validated_contract = consolidator
                            .validate_contract(&position.contract, Duration::from_secs(1));
                        validated_contract.unwrap_or(position.contract)
                    };
                    match AssetType::from_str(&contract.security_type) {
                        AssetType::Stock
                        | AssetType::CFD
                        | AssetType::ForexPair
                        | AssetType::Future => {
                            let symbol = get_local_symbol(&contract);
                            let hashed_contract = HashContract {
                                contract: contract.clone(),
                            };
                            broker_stock_positions_received.insert(hashed_contract.clone());
                            tracing::info!(
                                "Pushed broker positions into set, checking with stock_map now"
                            );
                            match stock_map.get(&hashed_contract) {
                                Some(local_pos) => {
                                    if *local_pos != position.position {
                                        tracing::warn!(
                                            message=%format!(
                                                "Reconciling current stock position according to broker position for ({}, {}) - found in stock_map: (Local: {}, Broker: {})",
                                                symbol,
                                                contract.primary_exchange.clone(),
                                                local_pos,
                                                &position.position
                                            )
                                        );

                                        let discrepancy = position.position - *local_pos;
                                        if discrepancy == 0.0 {
                                            tracing::info!("No discrepancy for {symbol:?}");
                                            continue;
                                        }
                                        let pri_exch = contract.primary_exchange.clone();
                                        let strategy = default_strategy
                                            .clone()
                                            .unwrap_or("unknown".to_string());
                                        let current_stock_positions_crud_cloned =
                                            current_stock_positions_crud.clone();
                                        let current_price = {
                                            let val = consolidator.get_current_price(
                                                &contract,
                                                &false,
                                                &[],
                                            );
                                            if let Err(e) = val {
                                                tracing::error!(
                                                    "Could not get current price for synced position: {e:?}! Falling back to 0.0!"
                                                );
                                                0.0
                                            } else {
                                                val.unwrap()
                                            }
                                        };

                                        tracing::info!("syncing a found stock position");
                                        tokio::spawn(async move {
                                            match current_stock_positions_crud_cloned
                                                .update_strat_positions(
                                                    &symbol,
                                                    pri_exch.as_str(),
                                                    &strategy,
                                                    contract.currency.as_str(),
                                                    &discrepancy,
                                                    Some(current_price),
                                                )
                                                .await
                                            {
                                                Ok(_) => {
                                                    tracing::warn!(
                                                        message=%format!(
                                                            "Discrepancy in stock positions, allocated to strategy {}: ({}, {}) for qty of {}, Discrepancy of {}",
                                                            strategy,
                                                            symbol,
                                                            pri_exch,
                                                            position.position,
                                                            discrepancy
                                                        )
                                                    )
                                                }
                                                Err(e) => {
                                                    tracing::error!(
                                                        message=%format!(
                                                            "Error trying to reconcile Discrepancy in stock positions for strategy {}: {}",
                                                            strategy,
                                                            e
                                                        )
                                                    )
                                                }
                                            };
                                        });
                                    }
                                }
                                None => {
                                    tracing::warn!(
                                        message=%format!(
                                            "Reconciling current stock position according to broker position for ({}, {}): (Local: {}, Broker: {})",
                                            symbol.clone(),
                                            contract.primary_exchange.clone(),
                                            0.0,
                                            &position.position
                                        )
                                    );
                                    let strategy =
                                        self.contract_to_strategy.get(&hashed_contract).map_or(
                                            default_strategy
                                                .clone()
                                                .unwrap_or("unknown".to_string()),
                                            |v| v.to_string(),
                                        );
                                    let current_stock_positions_crud_cloned =
                                        current_stock_positions_crud.clone();
                                    tracing::info!("syncing a None stock position");
                                    let currency = contract.currency.as_str().to_string();
                                    tokio::spawn(async move {
                                        if let Err(e) = current_stock_positions_crud_cloned.create_or_update(&crate::database::models::CurrentStockPositionsPrimaryKeys {
                                            stock: symbol,
                                            primary_exchange: contract.primary_exchange.as_str().to_string(),
                                            currency: currency,
                                            strategy: strategy,
                                        }, &CurrentStockPositionsUpdateKeys {
                                            quantity: Some(position.position.clone()),
                                            avg_price: Some(position.average_cost),
                                            last_updated: None
                                        }).await {
                                            tracing::error!(
                                                message=%format!(
                                                    "Error inserting into Current Stock Positions when reconciling stock positions (Local: {}, Broker: {}): {}", 0.0, &position.position, e
                                                )
                                            )
                                        }
                                    });
                                }
                            }
                        }
                        AssetType::Option => {
                            let hashed_contract = HashContract {
                                contract: contract.clone(),
                            };
                            broker_option_positions_received.insert(hashed_contract.clone());
                            match option_map.get(&hashed_contract) {
                                Some(local_pos) => {
                                    if *local_pos != position.position {
                                        tracing::warn!(
                                            message=%format!(
                                                "Reconciling current option position according to broker position (Local: {}, Broker: {})",
                                                local_pos,
                                                &position.position
                                            )
                                        );

                                        let symbol = get_local_symbol(&contract);
                                        let primary_exchange =
                                            contract.primary_exchange.as_str().to_string();
                                        let currency = contract.currency.as_str().to_string();
                                        let (expiry, strike, multiplier, option_type) = (
                                            contract.last_trade_date_or_contract_month.clone(),
                                            contract.strike.clone(),
                                            contract.multiplier.clone(),
                                            OptionType::from_str(&contract.right).expect("Error decoding contract right to OptionType while Reconciling options positions"),
                                        );
                                        let discrepancy = (position.position - *local_pos).clone();
                                        let strategy = default_strategy
                                            .clone()
                                            .unwrap_or("unknown".to_string());
                                        let current_option_positions_crud_cloned =
                                            current_option_positions_crud.clone();
                                        tracing::info!("syncing a found options position");
                                        tokio::spawn(async move {
                                            match current_option_positions_crud_cloned
                                                .update_strat_positions(
                                                    symbol.as_str(),
                                                    primary_exchange.as_str(),
                                                    &currency,
                                                    &expiry,
                                                    &strike,
                                                    &multiplier,
                                                    &option_type,
                                                    strategy.as_str(),
                                                    &discrepancy,
                                                )
                                                .await
                                            {
                                                Ok(_) => {
                                                    tracing::warn!(
                                                        message=%format!(
                                                            "Discrepancy in stock positions, allocated to strategy unknown: {} for qty of {}",
                                                            symbol,
                                                            position.position
                                                        )
                                                    )
                                                }
                                                Err(e) => {
                                                    tracing::error!(
                                                        "Error trying to reconcile Discrepancy in stock positions: {e:?}"
                                                    )
                                                }
                                            };
                                        });
                                    }
                                }
                                None => {
                                    tracing::warn!(
                                        message=%format!(
                                            "Reconciling current options position according to broker position (Local: {}, Broker: {})",
                                            0.0,
                                            &position.position
                                        )
                                    );
                                    let strategy =
                                        self.contract_to_strategy.get(&hashed_contract).map_or(
                                            default_strategy
                                                .clone()
                                                .unwrap_or("unknown".to_string()),
                                            |v| v.to_string(),
                                        );
                                    let current_option_positions_crud_cloned =
                                        current_option_positions_crud.clone();
                                    tracing::info!("syncing a None options position");
                                    tokio::spawn(async move {
                                        let symbol = contract.symbol.clone().as_str().to_string();
                                        if let Err(e) = current_option_positions_crud_cloned.create_or_update(&&crate::database::models::CurrentOptionPositionsPrimaryKeys {
                                            stock: symbol,
                                            primary_exchange: contract.primary_exchange.as_str().to_string(),
                                            currency: contract.currency.as_str().to_string(),
                                            expiry: contract.last_trade_date_or_contract_month,
                                            strike: contract.strike,
                                            multiplier: contract.multiplier,
                                            option_type: OptionType::from_str(&contract.right)
                                                .expect("Error decoding contract right to OptionType while Reconciling options positions"),
                                            strategy: strategy,
                                        }, &CurrentOptionPositionsUpdateKeys {
                                            quantity: Some(position.position.clone()),
                                            avg_price: Some(position.average_cost.clone()),
                                            last_updated: None
                                        }).await {
                                            tracing::error!("Error inserting into Current Stock Positions when reconciling stock positions (Local: {}, Broker: {}): {}", 0.0, &position.position, e)
                                        }
                                    });
                                }
                            }
                        }
                        _ => {
                            tracing::error!(
                                message=%format!(
                                    "New Security Type encountered when reconciling current positions: {}",
                                    contract.security_type
                                )
                            )
                        }
                    }
                }
                PositionUpdateMulti::PositionEnd => {
                    println!("initial set of positions received - cancelling request");
                    subscription.cancel();
                    break;
                }
            }
        }
        if let Some(e) = subscription.error() {
            tracing::warn!("Encountered Error while syncing positions: {e:?}");
            return;
        }

        for (hashed_contract, local_pos) in stock_map.iter() {
            if broker_stock_positions_received.contains(&hashed_contract) {
                continue;
            }
            tracing::warn!(
                message=%format!(
                    "Reconciling current stock position according to broker position ({hashed_contract:?}) (Local: {}, Broker: {})",
                    local_pos,
                    0.0,
                )
            );
            let strategy = self.contract_to_strategy.get(&hashed_contract).map_or(
                default_strategy.clone().unwrap_or("unknown".to_string()),
                |v| v.to_string(),
            );

            let symbol = get_local_symbol(&hashed_contract.contract);
            let pri_exch = hashed_contract
                .contract
                .primary_exchange
                .as_str()
                .to_string();
            let currency = hashed_contract.contract.currency.to_string();
            let local_pos = local_pos.clone();
            let current_stock_positions_crud_cloned = current_stock_positions_crud.clone();
            tokio::spawn(async move {
                if let Err(e) = current_stock_positions_crud_cloned
                    .delete(&crate::database::models::CurrentStockPositionsPrimaryKeys {
                        stock: symbol,
                        primary_exchange: pri_exch,
                        currency: currency,
                        strategy: strategy,
                    })
                    .await
                {
                    tracing::error!(
                        message=%format!(
                            "Error deleting from Current Stock Positions when reconciling stock positions (Local: {}, Broker: {}): {}",
                            &local_pos,
                            0.0,
                            e
                        )
                    )
                }
            });
        }

        for (hashed_contract, local_pos) in option_map.iter() {
            if broker_option_positions_received.contains(&hashed_contract) {
                continue;
            }
            tracing::warn!(
                message=%format!(
                    "Reconciling current stock position according to broker position (Local: {}, Broker: {})",
                    local_pos,
                    0.0,
                )
            );
            let current_option_positions_crud =
                get_current_option_positions_crud(self.pool.clone());
            let strategy = self.contract_to_strategy.get(&hashed_contract).map_or(
                default_strategy.clone().unwrap_or("unknown".to_string()),
                |v| v.to_string(),
            );

            let curr_pos_prim_key = CurrentOptionPositionsPrimaryKeys {
                stock: hashed_contract.contract.symbol.as_str().to_string(),
                primary_exchange: hashed_contract
                    .contract
                    .primary_exchange
                    .as_str()
                    .to_string(),
                currency: hashed_contract.contract.currency.to_string(),
                strike: hashed_contract.contract.strike,
                expiry: hashed_contract
                    .contract
                    .last_trade_date_or_contract_month
                    .to_string(),
                multiplier: hashed_contract.contract.multiplier.to_string(),
                option_type: OptionType::from_str(&hashed_contract.contract.right)
                    .expect("Expected to be able to represent option right as OptionType"),
                strategy: strategy,
            };
            let local_pos = local_pos.clone();
            tokio::spawn(async move {
                if let Err(e) = current_option_positions_crud
                    .delete(&curr_pos_prim_key)
                    .await
                {
                    tracing::error!(
                        message=%format!(
                            "Error deleting from Current Stock Positions when reconciling stock positions (Local: {}, Broker: {}): {}",
                            &local_pos,
                            0.0,
                            e
                        )
                    )
                }
            });
        }
    }

    /// Initialises the Order Update Stream to listen for all order events for the client
    /// Note: Should only be run once for initialisation - creates a channel on each call
    /// NOTE: initialises a synchronous thread and sends msgs to async runtime - blocking_send if
    /// not handled quickly could block up channel and stow updates indefinitely
    pub fn init_order_update_stream(
        &self,
        weak_client: Weak<Client>,
        default_strategy: Option<String>,
    ) {
        // https://ibridgepy.com/ib-api-knowledge-base/#step1-1-17
        // openOrder( ) is triggered twice automatically. When the order is initially accepted and when the order is fully executed. When the order is initially accepted, you would get an openOrder( ) and orderStatus( ) call back. Then if there are partial fills or any other status changes you would receive additional orderStatus( ) call back. Then if you receive additional orderStatus( ) call back, when the order fully executes you would get a final orderStatus( ) followed by an openOrder( ) and then receive the execDetails( ) and commissionReport( ). If you invoke reqOpenOrders( ), it will only relay the last orderStatus( ) of any current working order.
        let is_alr_init = {
            self.order_update_stream_killer
                .lock()
                .expect("Expected lock for order_update_stream_killer to not be poisoned")
                .is_some()
        };
        if is_alr_init {
            return;
        }
        let (sender, mut rx) = channel::<OrderUpdate>(100);
        let thread_killer = Arc::new(true);
        {
            self.order_update_stream_killer
                .lock()
                .expect("Expected lock for order_update_stream_killer to not be poisoned")
                .replace(thread_killer.clone());
        }

        // spawn a new os blocking thread to await for updates synchronously - send updates via
        // channel back to app
        thread::spawn(move || {
            let mut event_subscription = {
                let client_opt = weak_client.upgrade();
                if client_opt.is_none() {
                    tracing::error!("client is dead before init! could not subscribe!");
                    return;
                }
                let client = client_opt.unwrap();
                assert!(client.client_id() == 0);
                let event_subscription = client.order_update_stream();
                if let Err(e) = &event_subscription {
                    tracing::error!("Failed to being order_update_stream in OrderEngine: {e:?}");
                    return;
                }
                event_subscription.unwrap()
            };
            info!("Subscribed for updates for orders!");

            loop {
                if Arc::strong_count(&thread_killer) == 1 {
                    info!("Order Update Stream killed!");
                    return;
                } else if let Some(e) = event_subscription.error() {
                    warn!(
                        "order_update_stream subscription died! but not killed:{e:?}\
                        \nRetrying subscription!"
                    );
                    event_subscription = {
                        let client_opt = weak_client.upgrade();
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
                    thread::spawn(move || {
                        if let Err(e) = cloned_sender.blocking_send(event) {
                            tracing::warn!(
                                "synchronous sender in init_order_update_stream failed to send event to async event loop: {e:?}"
                            )
                        };
                    });
                    if Arc::strong_count(&thread_killer) == 1 {
                        info!("Order Update Stream killed!");
                        return;
                    }
                }
            }
        });

        // async reciever that asynchronously awaits for updates
        let order_map = self.order_map.clone();
        let pool = self.pool.clone();
        let (kill_sender, mut kill_rcx) = tokio::sync::oneshot::channel::<()>();
        {
            self.order_update_stream_tokio_killer
                .lock()
                .expect("Expected order_update_stream_tokio_killer lock to not be poisoned")
                .replace(kill_sender);
        }
        let def_strat = default_strategy.unwrap_or("unknown".to_string());
        let strategy_map = self.strategy_map.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut kill_rcx => {
                        tracing::info!("order_update_stream tokio thread killed!");
                        return;
                    }
                    Some(order_update) = rx.recv() => {
                        tracing::info!("Received order update in tokio runtime");
                        // all awaitable events within this is spawned asynchronously
                        if let Err(e) =
                            on_order_update_received(order_map.clone(), strategy_map.clone(), pool.clone(), order_update, def_strat.as_str())
                                .await
                        {
                            tracing::error!("on_order_update_received error: {e:?}")
                        };
                    }
                }
            }
        });
    }

    pub fn kill_order_update_stream_thread(&self) -> Result<(), String> {
        let kill_lock = {
            self.order_update_stream_killer
                .lock()
                .expect("Expected lock for order_update_stream_killer to not be poisoned")
                .take()
        };
        if kill_lock.is_none() {
            return Err(
                "order_update_stream did not seem to be initialised when trying to kill order_update_stream thread!".to_string(),
            );
        }
        drop(kill_lock);

        let kill_sender = {
            self.order_update_stream_tokio_killer
                .lock()
                .expect("Expected lock for order_update_stream_tokio_killer to not be poisoned")
                .take()
        };
        if kill_sender.is_none() {
            return Err("order_update_stream_tokio_killer not initialised when trying to kill order_update_stream tokio thread".to_string());
        }
        if let Err(e) = kill_sender.unwrap().send(()) {
            return Err(format!(
                "Error trying to send kill signal to order_update_stream tokio thread: {e:?}"
            ));
        }

        Ok(())
    }

    pub async fn place_order(
        &self,
        strategy: &str,
        weak_client: &Weak<Client>,
        contract: &Contract,
        order: &Order,
        attachments: Vec<(HashContract, Order)>,
    ) -> Result<(), String> {
        let cloned_order_map = self.order_map.clone();
        let strat = strategy.to_string();
        let contract_cloned = contract.clone();
        let order_cloned = order.clone();
        let weak_client_cloned = weak_client.clone();
        thread::spawn(move || {
            place_order(
                cloned_order_map,
                strat.as_str(),
                &weak_client_cloned,
                &contract_cloned,
                &order_cloned,
                attachments,
            )
        });
        Ok(())
    }

    pub fn place_orders_for_strategy(
        &self,
        strategy: &StrategyEnum,
        contract: &Contract,
        weak_client: &Weak<Client>,
        asset_type: &AssetType,
        ignore_contract_for_strategy: bool,
        consolidator: &Weak<Consolidator>,
    ) {
        info!(message=%format!("Placing orders for {}", strategy.get_name()));
        match asset_type {
            &AssetType::Stock
            | &AssetType::Future
            | &AssetType::ForexPair
            | &AssetType::CFD
            | &AssetType::CASH => {
                let pool = self.pool.clone();
                let order_map = self.order_map.clone();
                let target_stock_positions_crud =
                    get_specific_target_stock_positions_crud(self.pool.clone());
                let strategy = strategy.clone();
                let symbol = get_local_symbol(contract);
                let primary_exchange = contract.primary_exchange.to_string();
                let currency = contract.currency.to_string();
                let weak_client = weak_client.clone();
                let consolidator = consolidator.clone();
                tokio::spawn(async move {
                    match {
                        if ignore_contract_for_strategy {
                            target_stock_positions_crud
                                .get_target_pos_diff_strat(strategy.get_name().as_str())
                                .await
                        } else {
                            target_stock_positions_crud
                                .get_target_pos_diff(
                                    strategy.get_name().as_str(),
                                    symbol.as_str(),
                                    &primary_exchange,
                                    &currency,
                                )
                                .await
                        }
                    } {
                        Ok(pos_diffs) => {
                            info!(
                                message = %format!("Detected pos_diffs: \n{}",
                                    &pos_diffs
                                        .iter()
                                        .map(|pos_diff| format!(
                                            "    {}, {}, {} ({})",
                                            pos_diff.strategy,
                                            pos_diff.stock,
                                            pos_diff.primary_exchange,
                                            pos_diff.qty_diff
                                        ))
                                        .collect::<Vec<String>>()
                                        .join(",\n")
                                )
                            );
                            let (sell_to_fx, fx_to_buys, chained_buys) = if !strategy
                                .is_fx_strategy()
                            {
                                let mut funds = HashMap::new();
                                let mut funds_from_selling =
                                    HashMap::<HashContract, Vec<f64>>::new();
                                let mut insufficient_funds = HashMap::new();

                                let mut pos_to_open = Vec::new();
                                for pos_diff in pos_diffs.clone() {
                                    if let Some(quote) = pos_diff.stock.strip_prefix("CASH:") {
                                        funds.insert(quote.to_string(), -pos_diff.qty_diff);
                                        continue;
                                    }
                                    if pos_diff.current_qty != 0.0
                                        && pos_diff.qty_diff.signum()
                                            != pos_diff.current_qty.signum()
                                    {
                                        let hash_contract = HashContract {
                                            contract: get_contract_from_local_symbol(
                                                &pos_diff.stock,
                                                &pos_diff.primary_exchange,
                                                &pos_diff.currency,
                                            ),
                                        };
                                        if !funds_from_selling.contains_key(&hash_contract) {
                                            funds_from_selling
                                                .insert(hash_contract.clone(), Vec::new());
                                        };
                                        let funds_from_selling_currency =
                                            funds_from_selling.get_mut(&hash_contract).unwrap();
                                        funds_from_selling_currency.push(pos_diff.qty_diff.abs());
                                    } else {
                                        pos_to_open.push(pos_diff);
                                    }
                                }
                                {
                                    let strong_consolidator = {
                                        let upgraded_consolidator_opt = consolidator.upgrade();
                                        if upgraded_consolidator_opt.is_none() {
                                            tracing::error!(
                                                "Consolidator reference dropped before orders for positional differences could be submitted!"
                                            );
                                            return;
                                        }
                                        upgraded_consolidator_opt.unwrap()
                                    };
                                    for pos_diff in pos_to_open {
                                        let contract = get_contract_from_local_symbol(
                                            &pos_diff.stock,
                                            &pos_diff.primary_exchange,
                                            &pos_diff.currency,
                                        );
                                        let hash_contract = HashContract {
                                            contract: contract.clone(),
                                        };
                                        let required_currency = pos_diff.qty_diff
                                        * strong_consolidator.get_current_price(
                                            &contract,
                                            &false,
                                            &[],
                                        ).unwrap_or_else(|_| {
                                            tracing::warn!("Could not get current price of contract in order_engine!");
                                            0.0
                                        });

                                        let available_funds =
                                            funds.get(&pos_diff.currency).unwrap_or(&0.0);
                                        if available_funds >= &required_currency {
                                            continue;
                                        }

                                        insufficient_funds.insert(
                                            hash_contract,
                                            required_currency - available_funds,
                                        );
                                        funds.insert(pos_diff.currency.clone(), 0.0);
                                    }
                                }

                                // Collect all buy contracts that are handled via FX attachment chains
                                // so we can skip them in the main placement loop.
                                let (sell_to_fx, fx_to_buys) =
                                    OrderEngine::get_required_fx_attachments(
                                        funds,
                                        funds_from_selling,
                                        insufficient_funds,
                                    );

                                let chained_buys: HashSet<HashContract> = fx_to_buys
                                    .values()
                                    .flat_map(|buys| buys.iter().map(|v| v.0.clone()))
                                    .collect();

                                (sell_to_fx, fx_to_buys, chained_buys)
                            } else {
                                (HashMap::new(), HashMap::new(), HashSet::new())
                            };

                            pos_diffs.iter().for_each(|pos_diff| {
                                if pos_diff.stock.split(":").next() == Some("CASH") {
                                    return;
                                }

                                let contract = get_contract_from_local_symbol(
                                    &pos_diff.stock,
                                    &pos_diff.primary_exchange,
                                    &pos_diff.currency,
                                );
                                let hash_contract = HashContract {
                                    contract: contract.clone(),
                                };

                                // Skip buys that are placed as children of an FX attachment
                                if chained_buys.contains(&hash_contract) {
                                    return;
                                }

                                let pool = pool.clone();
                                let weak_client_cloned = weak_client.clone();
                                let order_map = order_map.clone();
                                let strat = strategy.get_name();

                                // Determine attachments for this contract:
                                //   - If it's a sell with an FX chain:    attach the FX contract(s)
                                //   - If it's an FX contract with buys:   attach the buy contract(s)
                                //   - Otherwise:                           no attachment
                                let attachments: Vec<(HashContract, Order)> = sell_to_fx
                                    .get(&hash_contract)
                                    .or_else(|| fx_to_buys.get(&hash_contract))
                                    .cloned()
                                    .unwrap_or_default();

                                let (qty_diff, avg_price) = (pos_diff.qty_diff, pos_diff.avg_price);

                                tokio::spawn(async move {
                                    on_new_stock_qty_diff_for_strat(
                                        pool,
                                        &contract,
                                        &weak_client_cloned,
                                        order_map,
                                        strat.as_str(),
                                        &qty_diff,
                                        &avg_price,
                                        attachments,
                                    )
                                    .await;
                                });
                            });
                        }
                        Err(e) => {
                            tracing::error!(message = %format!(
                                    "Error generating differences in stock positions for {}: {}",
                                    strategy.get_name(),
                                    e
                                )
                            );
                        }
                    }
                });
            }

            AssetType::Option => {
                let pool = self.pool.clone();
                let order_map = self.order_map.clone();
                let target_option_positions_crud =
                    get_specific_target_option_positions_crud(self.pool.clone());
                let strategy = strategy.clone();
                let symbol = get_local_symbol(&contract);
                let pri_exch = contract.primary_exchange.to_string();
                let currency = contract.currency.to_string();
                let expiry = contract.last_trade_date_or_contract_month.to_string();
                let strike = contract.strike;
                let mult = contract.multiplier.to_string();
                let opt_type = OptionType::from_str(&contract.right)
                    .expect("Expected to be able to parse contract right for options contract");
                let weak_client = weak_client.clone();
                let consolidator = consolidator.clone();
                tokio::spawn(async move {
                    match target_option_positions_crud
                        .get_target_pos_diff(
                            strategy.get_name().as_str(),
                            symbol.as_str(),
                            pri_exch.as_str(),
                            &currency,
                            expiry.as_str(),
                            &strike,
                            mult.as_str(),
                            &opt_type,
                        )
                        .await
                    {
                        Ok(pos_diffs) => {
                            let upgraded_consolidator = {
                                let upgraded_consolidator_opt = consolidator.upgrade();
                                if upgraded_consolidator_opt.is_none() {
                                    tracing::error!(
                                        "Consolidator reference dropped before orders for positional differences could be submitted!"
                                    );
                                    return;
                                }
                                upgraded_consolidator_opt.unwrap()
                            };
                            pos_diffs.iter().for_each(|pos_diff| {
                                let pool = pool.clone();
                                let weak_client_cloned = weak_client.clone();
                                let order_map = order_map.clone();
                                let strat = strategy.get_name();
                                let contract_opt = strategy.get_contract(
                                    &pos_diff.stock,
                                    &pos_diff.primary_exchange,
                                    &pos_diff.currency,
                                    &upgraded_consolidator,
                                );
                                if contract_opt.is_none() {
                                    return;
                                }
                                let contract = contract_opt.unwrap();
                                let (qty_diff, avg_price) = (pos_diff.qty_diff, pos_diff.avg_price);
                                tokio::spawn(async move {
                                    on_new_option_qty_diff_for_strat(
                                        pool,
                                        &contract,
                                        &weak_client_cloned,
                                        order_map,
                                        strat.as_str(),
                                        &qty_diff,
                                        &avg_price,
                                    )
                                    .await;
                                });
                            });
                        }
                        Err(_) => {
                            tracing::error!(
                                message=%format!(
                                    "Error generating differences in stock positions for {}",
                                    strategy.get_name()
                                )
                            );
                        }
                    }
                });
            }
            AssetType::Unknown => {
                tracing::warn!(
                    "Unknown AssetType encountered in place_orders_for_strategy:\nAssumed not part of any strategy and ignored!"
                )
            }
        }
    }
}
