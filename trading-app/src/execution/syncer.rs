use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Utc;
use ibapi::{
    Client,
    accounts::{PositionUpdateMulti, types::AccountId},
    contracts::SecurityType,
    orders::{ExecutionFilter, Executions, Order, OrderStatus},
    prelude::Contract,
};
use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUDTrait,
        models::{
            AssetType, CurrentOptionPositionsFullKeys, CurrentStockPositionsFullKeys,
            CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys,
            OpenOptionOrdersUpdateKeys, OpenStockOrdersUpdateKeys, OptionType,
        },
        models_crud::{
            current_positions::current_positions::{
                CurrentPositionsCRUD, CurrentPositionsFullKeys, CurrentPositionsOps,
                CurrentPositionsPrimaryKeys, CurrentPositionsUpdateKeys,
            },
            open_orders::open_orders::{
                OpenOrdersCRUD, OpenOrdersPrimaryKeys, OpenOrdersUpdateKeys,
            },
        },
    },
    execution::order_update_stream,
    helpers::contract::{HashContract, get_contract_from_local_symbol, get_local_symbol},
    init_app::StrategyParameters,
    market_data::consolidator::Consolidator,
    strategy::strategy::{StrategyEnum, StrategyExecutor},
};

pub struct SyncerEngine {
    pool: PgPool,
    account: String,

    // Strategy -> StrategyEnum (for mapping DB fields)
    strategy_map: Arc<HashMap<String, StrategyEnum>>,
    // Contract -> Strategy
    // - Prioritisation of Strategy happens here
    contract_to_strategy: HashMap<HashContract, String>,
}

impl SyncerEngine {
    pub fn new(
        pool: PgPool,
        account: String,
        active_strategies: &Vec<StrategyParameters>,
    ) -> SyncerEngine {
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
            account,
            strategy_map: Arc::new(strategy_map),
            contract_to_strategy,
        }
    }
}

pub trait SyncOps {
    fn sync_executions(
        &self,
        client: &Client,
        default_strategy: Option<String>,
    ) -> Result<(), String>;
    fn sync_open_orders(
        &self,
        client: &Client,
        consolidator: &Consolidator,
        default_strategy: Option<String>,
    );
    async fn sync_positions(
        &self,
        client: &Client,
        consolidator: &Consolidator,
        default_strategy: Option<String>,
    );
}

impl SyncOps for SyncerEngine {
    // Call before sync_positions - tries its best to sync all missed orders since last session
    // - but may miss some position updates -> Have to reconcile manually and via sync_positions
    // - luckily for this, we can simply reuse live order event handles
    fn sync_executions(
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
                    // let strategy = {
                    //     let order_map = order_map
                    //         .read()
                    //         .expect("Expected read lock for order_map not to be poisoned");
                    //     order_map
                    //         .get(&execution_data.execution.order_id)
                    //         .map_or(&def_strat, |v| &v.0)
                    //         .clone()
                    // };
                    tracing::info!(
                        message=%format!(
                            "Syncing Executions: New Execution recorded with id: {} for strategy: {}",
                            &execution_data.request_id,
                            &execution_data.execution.order_reference
                        )
                    );

                    order_update_stream::event_handlers::execution::on_execution_update(
                        self.pool.clone(),
                        execution_data,
                        self.strategy_map.clone(),
                        &def_strat,
                    );
                }

                Executions::CommissionReport(commission_report) => {
                    if let Err(e) =
                        order_update_stream::event_handlers::commission_report::on_commission_update(
                            self.pool.clone(),
                            &commission_report,
                        )
                    {
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

    fn sync_open_orders(
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
        if let Some(e) = subscription.error() {
            tracing::warn!("Encountered Error while syncing open orders: {e:?}");
            return;
        }
        for open_order in subscription.timeout_iter(std::time::Duration::from_secs(10)) {
            match open_order {
                ibapi::orders::Orders::OrderData(order_data) => {
                    let perm_id = order_data.order.perm_id;
                    open_orders.insert(
                        perm_id,
                        (
                            Some(order_data.contract),
                            Some(order_data.order),
                            open_orders.get(&perm_id).and_then(|v| v.2.clone()),
                        ),
                    );
                }
                ibapi::orders::Orders::OrderStatus(order_status) => {
                    open_orders.insert(
                        order_status.perm_id,
                        (
                            open_orders
                                .get(&order_status.perm_id)
                                .and_then(|v| v.0.clone()),
                            open_orders
                                .get(&order_status.perm_id)
                                .and_then(|v| v.1.clone()),
                            Some(order_status),
                        ),
                    );
                }
                ibapi::orders::Orders::Notice(notice) => {
                    tracing::warn!("Notice from OrderEngine.sync_open_orders: {notice:?}");
                }
            }
        }

        for data in open_orders.values() {
            if data.0.is_none() || data.1.is_none() || data.2.is_none() {
                tracing::error!("While syncing OpenOrders, HashMap have NULL values");
                continue;
            }
            on_full_open_order_received(
                &self.contract_to_strategy,
                self.pool.clone(),
                consolidator,
                &data.0.as_ref().unwrap(),
                &data.1.as_ref().unwrap(),
                &data.2.as_ref().unwrap(),
                &default_strategy,
            );
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
    async fn sync_positions(
        &self,
        client: &Client,
        consolidator: &Consolidator,
        default_strategy: Option<String>,
    ) {
        let current_stock_positions_crud =
            CurrentPositionsCRUD::from(&AssetType::Stock, self.pool.clone());
        let current_option_positions_crud =
            CurrentPositionsCRUD::from(&AssetType::Option, self.pool.clone());
        let mut stock_map: HashMap<HashContract, f64> = HashMap::new();
        let mut option_map: HashMap<HashContract, f64> = HashMap::new();
        let mut fx_map: HashMap<String, f64> = HashMap::new();
        let mut errors = Vec::new();

        // =====================================
        // Consolidate locally tracked positions
        // =====================================
        match &current_stock_positions_crud.get_all_pos_grouped().await {
            Ok(current_stock_positions) => {
                tracing::info!("Received local stock positions, inserting into stock_map now");
                for position in current_stock_positions {
                    let (stock, primary_exchange, currency, quantity, avg_price) = match position {
                        CurrentPositionsFullKeys::Stock(CurrentStockPositionsFullKeys {
                            stock,
                            primary_exchange,
                            currency,
                            quantity,
                            avg_price,
                            ..
                        }) => (stock, primary_exchange, currency, quantity, avg_price),
                        _ => {
                            return;
                        }
                    };
                    let built_contract = get_contract_from_local_symbol(
                        stock.as_str(),
                        primary_exchange.as_str(),
                        currency.as_str(),
                    );
                    if built_contract.security_type == SecurityType::ForexPair {
                        let currency = currency.to_string();
                        let currency_val = quantity * avg_price;
                        let quote = built_contract.symbol.to_string();
                        let quote_val = quantity;
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
                        quantity.clone(),
                    );
                }
            }
            Err(e) => {
                tracing::error!("Error trying to read all stock positions in DB: {e:?}");
                errors.push(format!(
                    "Error trying to read_all_pos_grouped(stock) in DB: {e:?}"
                ));
            }
        };
        match current_option_positions_crud.get_all_pos_grouped().await {
            Ok(current_option_positions) => {
                tracing::info!("Received local option positions, inserting into stock_map now");
                for position in current_option_positions {
                    let (stock, expiry, strike, option_type, quantity) = match position {
                        CurrentPositionsFullKeys::Options(CurrentOptionPositionsFullKeys {
                            stock,
                            expiry,
                            strike,
                            option_type,
                            quantity,
                            ..
                        }) => (stock, expiry, strike, option_type, quantity),
                        _ => {
                            return;
                        }
                    };
                    let built_contract =
                        Contract::option(&stock, &expiry, strike, &option_type.to_string());
                    let hashed_contract = HashContract {
                        contract: built_contract,
                    };
                    option_map.insert(hashed_contract, quantity);
                }
            }
            Err(e) => {
                tracing::error!("Error trying to read all option positions in DB: {e:?}");
                errors.push(format!(
                    "Error trying to read_all_pos_grouped(option) in DB: {e:?}"
                ));
            }
        }

        // =====================================
        // Fetch Broker Positions + Reconcile
        // =====================================
        tracing::info!("Trying to get broker positions now");

        sync_fx_positions(
            consolidator,
            client,
            &current_stock_positions_crud,
            self.account.to_string(),
            &fx_map,
            &mut errors,
        );

        sync_stock_and_option_positions(
            self.account.to_string(),
            client,
            consolidator,
            &stock_map,
            &option_map,
            &self.contract_to_strategy,
            &default_strategy.unwrap_or("unknown".to_string()),
            self.pool.clone(),
            &mut errors,
        );

        if !errors.is_empty() {
            tracing::error!("{}", errors.join("\n"));
        }
    }
}

// ===================================== SYNC POSITIONS HELPERS ============================================
/// fx_map: Local FX positions
/// broker_currency: Broker "fx_map"
async fn sync_fx_positions(
    consolidator: &Consolidator,
    client: &Client,
    current_stock_positions_crud: &CurrentPositionsCRUD,
    account: String,
    fx_map: &HashMap<String, f64>,
    errors: &mut Vec<String>,
) {
    // =====================================
    // Compare FX broker positions
    // =====================================
    let fx_subscription = client
        .account_updates(&AccountId(account))
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
            .update_positions_additive(
                CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
                    strategy: "unknown".to_string(),
                    stock: format!("CASH:{currency}"),
                    primary_exchange: "".to_string(),
                    currency: "SGD".to_string(),
                }),
                CurrentPositionsUpdateKeys::Stock(CurrentStockPositionsUpdateKeys {
                    quantity: Some(adjustment),
                    avg_price: if currency == "SGD" {
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
                    last_updated: None,
                }),
            )
            .await
        {
            tracing::error!(
                "Error trying to update current stock positions for reconciliation of FX positions: {e:?}"
            );
            errors.push(format!("Error trying to update current stock positions for reconciliation of FX positions: {e:?}"))
        };
    }
}

async fn sync_stock_and_option_positions(
    account: String,
    client: &Client,
    consolidator: &Consolidator,
    stock_map: &HashMap<HashContract, f64>,
    option_map: &HashMap<HashContract, f64>,
    contract_to_strategy: &HashMap<HashContract, String>,
    default_strategy: &str,
    pool: PgPool,
    errors: &mut Vec<String>,
) {
    let mut broker_stock_positions_received = HashMap::<HashContract, f64>::new();
    let mut broker_option_positions_received = HashMap::<HashContract, f64>::new();
    let subscription = client
        .positions_multi(Some(&ibapi::accounts::types::AccountId(account)), None)
        .expect("Error requesting positions for sync_positions");
    if let Some(e) = subscription.error() {
        tracing::warn!("Encountered Error while syncing positions: {e:?}");
        errors.push(format!("Encountered Error while syncing positions: {e:?}"));
        return;
    }

    while let Some(position_response) =
        subscription.next_timeout(std::time::Duration::from_secs(10))
    {
        match position_response {
            PositionUpdateMulti::Position(position) => {
                let contract = {
                    let validated_contract =
                        consolidator.validate_contract(&position.contract, Duration::from_secs(1));
                    validated_contract.unwrap_or(position.contract)
                };
                let asset_type = AssetType::from_str(&contract.security_type);
                let hashed_contract = HashContract { contract: contract };
                match asset_type {
                    AssetType::Stock
                    | AssetType::CFD
                    | AssetType::ForexPair
                    | AssetType::Future => {
                        broker_stock_positions_received.insert(hashed_contract, position.position);
                    }
                    AssetType::Option => {
                        broker_option_positions_received.insert(hashed_contract, position.position);
                    }
                    _ => {
                        tracing::error!(
                            message=%format!(
                                "New Security Type encountered when reconciling current positions",
                            )
                        );
                        errors.push(format!(
                            "New Security Type encountered when reconciling current positions",
                        ));
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

    fn reconcile_position(
        pool: PgPool,
        contract: &Contract,
        local_position: f64,
        broker_position: f64,
        strategy: &str,
        consolidator: &Consolidator,
    ) {
        let discrepancy = broker_position - local_position;
        if discrepancy == 0.0 {
            tracing::info!("No discrepancy for {:?}", contract.symbol);
            return;
        }
        let current_price = {
            let val = consolidator.get_current_price(&contract, &false, &[]);
            if let Err(e) = val {
                tracing::error!(
                    "Could not get current price for synced position: {e:?}! Falling back to 0.0!"
                );
                0.0
            } else {
                val.unwrap()
            }
        };
        let asset_type = AssetType::from_str(&contract.security_type);
        let stock = contract.symbol.to_string();
        let current_positions_crud = CurrentPositionsCRUD::from(&asset_type, pool);
        let current_positions_pk =
            CurrentPositionsPrimaryKeys::from_strat_and_contract(strategy, contract);
        let current_positions_uk =
            CurrentPositionsUpdateKeys::from(&asset_type, Some(discrepancy), Some(current_price));
        let default_strategy = strategy.to_string();
        tokio::spawn(async move {
            match current_positions_crud
                .update_positions_additive(current_positions_pk, current_positions_uk)
                .await
            {
                Ok(_) => {
                    tracing::warn!(
                        message=%format!(
                            "Discrepancy in stock positions, allocated to strategy {}: ({:?}), Discrepancy of {}",
                            default_strategy,
                            stock,
                            discrepancy
                        )
                    )
                }
                Err(e) => {
                    tracing::error!(
                        message=%format!(
                            "Error trying to reconcile Discrepancy in stock positions for strategy {}: {}",
                            default_strategy,
                            e
                        )
                    );
                    // errors.push(format!(
                    //         "Error trying to reconcile Discrepancy in stock positions for strategy {}: {}",
                    //         default_strategy,
                    //         e
                    //     ));
                }
            };
        });
    }

    // ====================
    // Sync Stock Positions
    // ====================
    for local_contract in stock_map.keys() {
        if !broker_stock_positions_received.contains_key(&local_contract) {
            broker_stock_positions_received.insert(local_contract.clone(), 0.0);
        }
    }
    for (hash_contract, broker_position) in broker_stock_positions_received.into_iter() {
        let local_position = stock_map.get(&hash_contract).unwrap_or(&0.0);
        let strategy = contract_to_strategy
            .get(&hash_contract)
            .map_or(default_strategy.to_string(), |v| v.to_string());
        reconcile_position(
            pool.clone(),
            &hash_contract.contract,
            *local_position,
            broker_position,
            &strategy,
            &consolidator,
        )
    }
    // ====================
    // Sync Option Positions
    // ====================
    for local_contract in option_map.keys() {
        if !broker_option_positions_received.contains_key(&local_contract) {
            broker_option_positions_received.insert(local_contract.clone(), 0.0);
        }
    }
    for (hash_contract, broker_position) in broker_option_positions_received.into_iter() {
        let local_position = option_map.get(&hash_contract).unwrap_or(&0.0);
        let strategy = contract_to_strategy
            .get(&hash_contract)
            .map_or(default_strategy.to_string(), |v| v.to_string());
        reconcile_position(
            pool.clone(),
            &hash_contract.contract,
            *local_position,
            broker_position,
            &strategy,
            &consolidator,
        )
    }
}
// ===================================== SYNC POSITIONS HELPERS ============================================

// ONLY called in conjunction with sync_open_orders
fn on_full_open_order_received(
    contract_to_strategy: &HashMap<HashContract, String>,
    pool: PgPool,
    consolidator: &Consolidator,
    unvalidated_contract: &Contract,
    borrowed_order: &Order,
    order_status: &OrderStatus,
    default_strategy: &Option<String>,
) {
    // 1. Get Associated Strategy if possible, else default strategy
    let symbol = get_local_symbol(unvalidated_contract);
    let strategy = {
        contract_to_strategy
            .get(&HashContract {
                contract: unvalidated_contract.clone(),
            })
            .unwrap_or(&default_strategy.clone().unwrap_or("unknown".to_string()))
            .to_string()
    };

    // 2. Validate and fill all fields of contract - need to check again if this is a necessary
    //    call - but low on priority list since this is performed before mkt open - non-critical
    //    path
    let contract = consolidator
        .validate_contract(unvalidated_contract, Duration::from_secs(1))
        .unwrap_or(unvalidated_contract.clone());

    // 3. Begin a detached thread to update the DB accordingly
    let order = borrowed_order.clone();
    let order_status_filled = order_status.filled;
    tokio::spawn(async move {
        let asset_type = AssetType::from_str(&contract.security_type);
        let open_orders_crud = OpenOrdersCRUD::from(&asset_type, pool);
        let open_order_pk = OpenOrdersPrimaryKeys::new(&asset_type, order.perm_id, order.order_id);
        let open_order_uk = match asset_type {
            AssetType::Option => OpenOrdersUpdateKeys::Options(OpenOptionOrdersUpdateKeys {
                strategy: Some(strategy),
                stock: Some(symbol),
                primary_exchange: Some(contract.primary_exchange.to_string()),
                currency: Some(contract.currency.to_string()),
                expiry: Some(contract.last_trade_date_or_contract_month),
                strike: Some(contract.strike),
                multiplier: Some(contract.multiplier),
                option_type: Some(
                    OptionType::from_str(&contract.right)
                        .expect("Expected contract right to be convertible to OptionType"),
                ),
                time: Some(Utc::now()),
                quantity: Some(order.total_quantity),
                executions: None,
                filled: Some(order_status_filled),
            }),
            AssetType::Stock | AssetType::Future | AssetType::ForexPair | AssetType::CFD => {
                OpenOrdersUpdateKeys::Stock(OpenStockOrdersUpdateKeys {
                    strategy: Some(strategy),
                    stock: Some(symbol),
                    primary_exchange: Some(contract.primary_exchange.to_string()),
                    currency: Some(contract.currency.to_string()),
                    time: Some(Utc::now()),
                    quantity: Some(order.total_quantity),
                    executions: None,
                    filled: Some(order_status_filled),
                })
            }
            AssetType::CASH => {
                tracing::warn!(
                    "CASH AssetType encountered while syncing open orders:\nAssumed not from any of the strategies - don't need to track!"
                );
                return;
            }
            AssetType::Unknown => {
                tracing::warn!(
                    "Unknown AssetType encountered while syncing open orders:\nAssumed not from any of the strategies - don't need to track!"
                );
                return;
            }
        };
        if let Err(e) = open_orders_crud
            .create_or_update(&open_order_pk, &open_order_uk)
            .await
        {
            tracing::error!("Failed to create_or_update on on_full_open_order_received: {e:?}");
        };
    });
}
