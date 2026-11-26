use crate::{
    database::tables::DbTables,
    execution::traits::{order_router::OrderRouter, positions_tracker::PositionsTracker},
    market_data::traits::contracts::ContractBroker,
};
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

#[cfg(feature = "backtest")]
use crate::backtester::setup::config::BacktestMode;
use async_trait::async_trait;
use ibapi::{
    accounts::{PositionUpdateMulti, types::AccountId},
    contracts::{Currency, SecurityType},
    orders::{ExecutionData, ExecutionFilter, Executions, Order, OrderStatus},
    prelude::Contract,
};
use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBufferProducer;
use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUDTrait,
        models::{
            AssetType, CurrentOptionPositionsFullKeys, CurrentStockPositionsFullKeys,
            CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys,
        },
        models_crud::{
            current_positions::current_positions::{
                CurrentPositionsFullKeys, CurrentPositionsOps, CurrentPositionsPrimaryKeys,
                CurrentPositionsUpdateKeys,
            },
            open_orders::open_orders::{OpenOrdersPrimaryKeys, OpenOrdersUpdateKeys},
        },
    },
    execution::{fx_backed_up_order::OrderStore, order_update_stream},
    helpers::contract::{HashContract, LocalContractTypes, get_contract_from},
    init_app::StrategyParameters,
    market_data::traits::current_price::PriceSupplier,
    strategy::strategy::{StrategyDetails, StrategyExecutor},
};

pub struct SyncerEngine {
    pool: PgPool,
    account: String,

    // Strategy -> StrategyEnum (for mapping DB fields)
    strategy_details: Arc<HashMap<String, StrategyDetails>>,
    // Contract -> Strategy
    // - Prioritisation of Strategy happens here
    contract_to_strategy: HashMap<HashContract<'static>, String>,
    handle: tokio::runtime::Handle,
}

impl SyncerEngine {
    pub fn new(
        pool: PgPool,
        account: String,
        active_strategies: &Vec<StrategyParameters>,
        handle: tokio::runtime::Handle,
    ) -> SyncerEngine {
        let mut strategy_details = HashMap::new();
        let mut contract_to_full_strategy: HashMap<HashContract, StrategyDetails> = HashMap::new();
        for strategy in active_strategies {
            let strategy_detail = strategy.strategy.get_strategy_details();
            strategy_details.insert(strategy_detail.name.clone(), strategy_detail.clone());

            for contract_sub in &strategy.subscribed_contracts {
                let hashed_contract = HashContract::new(contract_sub.contract.clone());
                contract_to_full_strategy
                    .entry(hashed_contract)
                    .and_modify(|current_strategy| {
                        if strategy_detail < *current_strategy {
                            *current_strategy = strategy_detail.clone();
                        }
                    })
                    .or_insert(strategy_detail.clone());
            }
        }
        let mut contract_to_strategy = HashMap::new();
        for (contract, full_strategy) in contract_to_full_strategy.into_iter() {
            contract_to_strategy.insert(contract.clone(), full_strategy.name);
        }

        Self {
            pool,
            account,
            strategy_details: Arc::new(strategy_details),
            contract_to_strategy,
            handle,
        }
    }
}

#[async_trait]
pub trait SyncOps {
    fn sync_executions<Broker, MarketBroker>(
        &self,
        client: &Arc<Broker>,
        execution_event_producers: Arc<
            HashMap<String, SpmcRingBufferProducer<ExecutionData, 64, 1>>,
        >,
        consolidator: &Weak<MarketBroker>,
        default_strategy: Option<String>,
        backed_up_orders: Arc<OrderStore>,
    ) -> Result<(), String>
    where
        Broker: PositionsTracker + OrderRouter + Send + Sync + 'static,
        MarketBroker: PriceSupplier + ContractBroker;
    fn sync_open_orders<Broker, MarketBroker>(
        &self,
        client: &Broker,
        consolidator: &Arc<MarketBroker>,
        default_strategy: Option<String>,
    ) where
        Broker: PositionsTracker,
        MarketBroker: ContractBroker;
    async fn sync_positions<Broker, MarketBroker>(
        &self,
        client: &Broker,
        consolidator: &Arc<MarketBroker>,
        default_strategy: Option<String>,
    ) where
        Broker: PositionsTracker + Send + Sync + 'static,
        MarketBroker: PriceSupplier + ContractBroker + Send + Sync + 'static;
}

#[async_trait]
impl SyncOps for SyncerEngine {
    // Call before sync_positions - tries its best to sync all missed orders since last session
    // - but may miss some position updates -> Have to reconcile manually and via sync_positions
    // - luckily for this, we can simply reuse live order event handles
    fn sync_executions<Broker, MarketBroker>(
        &self,
        client: &Arc<Broker>,
        execution_event_producers: Arc<
            HashMap<String, SpmcRingBufferProducer<ExecutionData, 64, 1>>,
        >,
        consolidator: &Weak<MarketBroker>,
        default_strategy: Option<String>,
        backed_up_orders: Arc<OrderStore>,
    ) -> Result<(), String>
    where
        Broker: PositionsTracker + OrderRouter + Send + Sync + 'static,
        MarketBroker: PriceSupplier + ContractBroker,
    {
        let subscription = client.get_executions(ibapi::orders::ExecutionFilter {
            ..ExecutionFilter::default()
        });
        // Assumption: Will always receive execution 1st b4 associated commission
        let def_strat = default_strategy.unwrap_or("unknown".to_string());
        for execution in subscription {
            match execution {
                Ok(Executions::ExecutionData(execution_data)) => {
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

                    if let Err(e) =
                        order_update_stream::event_handlers::execution::on_execution_update(
                            consolidator,
                            self.pool.clone(),
                            execution_data,
                            execution_event_producers.clone(),
                            &self.strategy_details,
                            &def_strat,
                            self.handle.clone(),
                            &Arc::downgrade(&client),
                            backed_up_orders.clone(),
                        )
                    {
                        tracing::error!(
                            "Failed to run on_execution_update while syncing executions: {e:?}"
                        );
                    };
                }

                Ok(Executions::CommissionReport(commission_report)) => {
                    if let Err(e) =
                        order_update_stream::event_handlers::commission_report::on_commission_update(
                            self.pool.clone(),
                            &commission_report,
                        )
                    {
                        tracing::error!("Error while running OrderEngine.sync_executions: {e:?}");
                    };
                }

                Ok(Executions::Notice(message)) => {
                    tracing::warn!("Message from OrderEngine.sync_executions: {message:?}");
                }
                Err(e) => {
                    tracing::error!("Error while syncing executions: {e:?}");
                    break;
                }
            }
        }

        Ok(())
    }

    fn sync_open_orders<Broker, MarketBroker>(
        &self,
        client: &Broker,
        consolidator: &Arc<MarketBroker>,
        default_strategy: Option<String>,
    ) where
        Broker: PositionsTracker,
        MarketBroker: ContractBroker,
    {
        // open_orders: for syncing between OrderData && OrderStatus
        let mut open_orders: HashMap<i32, (Option<Contract>, Option<Order>, Option<OrderStatus>)> =
            HashMap::new();
        for open_order in client.get_all_open_orders() {
            match open_order {
                Ok(ibapi::orders::Orders::OrderData(order_data)) => {
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
                Ok(ibapi::orders::Orders::OrderStatus(order_status)) => {
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
                Ok(ibapi::orders::Orders::Notice(notice)) => {
                    tracing::warn!("Notice from OrderEngine.sync_open_orders: {notice:?}");
                }
                Err(e) => {
                    tracing::warn!("Encountered Error while syncing open orders: {e:?}");
                    return;
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
            tracing::info!(
                message=%format!(
                    "Ran full_open_order_received for the following:\n{}",
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
    async fn sync_positions<Broker, MarketBroker>(
        &self,
        client: &Broker,
        consolidator: &Arc<MarketBroker>,
        default_strategy: Option<String>,
    ) where
        Broker: PositionsTracker + Send + Sync + 'static,
        MarketBroker: PriceSupplier + ContractBroker + Send + Sync + 'static,
    {
        let mut db = crate::database::db::DbDirect::connect(&self.pool);
        let mut stock_map: HashMap<HashContract, (f64, f64)> = HashMap::new();
        let mut option_map: HashMap<HashContract, (f64, f64)> = HashMap::new();
        let mut fx_map: HashMap<String, f64> = HashMap::new();
        let mut errors = Vec::new();

        // =====================================
        // Consolidate locally tracked positions
        // =====================================
        match &db
            .current_positions(&AssetType::Stock)
            .get_all_pos_grouped()
            .await
        {
            Ok(current_stock_positions) => {
                tracing::info!("Received local stock positions, inserting into stock_map now");
                for position in current_stock_positions {
                    let (quantity, avg_price) = match position {
                        CurrentPositionsFullKeys::Stock(CurrentStockPositionsFullKeys {
                            quantity,
                            avg_price,
                            ..
                        }) => (quantity, avg_price),
                        _ => {
                            return;
                        }
                    };
                    let built_contract =
                        get_contract_from(&LocalContractTypes::CurrentPosFk(position.clone()));
                    if built_contract.security_type == SecurityType::ForexPair {
                        fx_map.insert(built_contract.symbol.to_string(), *quantity);
                    } else {
                        stock_map
                            .insert(HashContract::new(built_contract), (*quantity, *avg_price));
                    }
                }
            }
            Err(e) => {
                tracing::error!("Error trying to read all stock positions in DB: {e:?}");
                errors.push(format!(
                    "Error trying to read_all_pos_grouped(stock) in DB: {e:?}"
                ));
            }
        };
        match db
            .current_positions(&AssetType::Option)
            .get_all_pos_grouped()
            .await
        {
            Ok(current_option_positions) => {
                tracing::info!("Received local option positions, inserting into stock_map now");
                for position in current_option_positions {
                    let (
                        stock,
                        expiry,
                        strike,
                        option_type,
                        quantity,
                        primary_exchange,
                        currency,
                        multiplier,
                        avg_price,
                    ) = match position {
                        CurrentPositionsFullKeys::Options(CurrentOptionPositionsFullKeys {
                            stock,
                            expiry,
                            strike,
                            option_type,
                            quantity,
                            primary_exchange,
                            currency,
                            multiplier,
                            avg_price,
                            ..
                        }) => (
                            stock,
                            expiry,
                            strike,
                            option_type,
                            quantity,
                            primary_exchange,
                            currency,
                            multiplier,
                            avg_price,
                        ),
                        _ => {
                            return;
                        }
                    };
                    let mut built_contract =
                        Contract::option(&stock, &expiry, strike, &option_type.to_string());
                    built_contract.primary_exchange = ibapi::contracts::Exchange(primary_exchange);
                    built_contract.currency = Currency::from(currency);
                    built_contract.multiplier = multiplier.clone();
                    let hashed_contract = HashContract::new(built_contract);
                    option_map.insert(hashed_contract, (quantity, avg_price));
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
            self.account.to_string(),
            &fx_map,
            &mut errors,
            &default_strategy.clone().unwrap_or("unknown".to_string()),
            &self.pool,
        )
        .await;

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
        )
        .await;

        if !errors.is_empty() {
            tracing::error!("{}", errors.join("\n"));
        }
    }
}

// ===================================== SYNC POSITIONS HELPERS ============================================
/// fx_map: Local FX positions
/// broker_currency: Broker "fx_map"
async fn sync_fx_positions<MarketBroker, PositionsBroker>(
    consolidator: &MarketBroker,
    client: &PositionsBroker,
    account: String,
    fx_map: &HashMap<String, f64>,
    errors: &mut Vec<String>,
    default_strategy: &str,
    pool: &PgPool,
) where
    MarketBroker: PriceSupplier,
    PositionsBroker: PositionsTracker,
{
    let mut db = crate::database::db::DbDirect::connect(&pool);
    let mut current_stock_positions_crud = db.current_positions(&AssetType::Stock);
    // =====================================
    // Compare FX broker positions
    // =====================================
    let mut broker_currency = HashMap::<String, f64>::new();
    for account_update in client.get_account_updates(&AccountId(account)) {
        match account_update {
            Ok(ibapi::prelude::AccountUpdate::AccountValue(account_value)) => {
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
            #[allow(unused_variables)]
            Ok(ibapi::prelude::AccountUpdate::PortfolioValue(portfolio_value)) => {
                continue;
                // let currency = portfolio_value.contract.currency.as_str();
                // let currency_balance = portfolio_value.average_cost * portfolio_value.position;
                // if !broker_currency.contains_key(currency) {
                //     broker_currency.insert(currency.to_string(), 0.0);
                // }
                // broker_currency.insert(
                //     currency.to_string(),
                //     broker_currency.get(currency).unwrap() + currency_balance,
                // );
            }
            Ok(ibapi::prelude::AccountUpdate::UpdateTime(_)) => {
                continue;
            }
            Ok(ibapi::prelude::AccountUpdate::End) => {
                break;
            }
            Err(e) => {
                tracing::warn!("Encountered Error while syncing FX positions: {e:?}");
                errors.push(format!(
                    "Encountered Error while syncing FX positions: {e:?}"
                ));
                break;
            }
        }
    }

    // Reconcile FX broker positions
    for currency in fx_map.keys() {
        if !broker_currency.contains_key(currency) {
            broker_currency.insert(currency.clone(), 0.0);
        }
    }

    for (currency, value) in broker_currency {
        let local_value = fx_map.get(&currency).unwrap_or(&0.0);
        if (local_value - value).abs() < 1000.0 {
            continue;
        }
        tracing::error!("{currency:?}: Local ({local_value}) | Broker ({value})");

        let adjustment = value - local_value;
        tracing::warn!(
            "Reconciling current & broker FX positions: Adding {adjustment} of {currency}"
        );
        let pk = CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
            strategy: default_strategy.to_string(),
            stock: format!("CASH:{currency}"),
            primary_exchange: "".to_string(),
            currency: "SGD".to_string(),
        });
        let uk = CurrentPositionsUpdateKeys::Stock(CurrentStockPositionsUpdateKeys {
            quantity: Some(adjustment),
            avg_price: if currency == "SGD" {
                Some(1.0)
            } else {
                consolidator
                    .get_current_price(
                        Contract {
                            symbol: currency.into(),
                            security_type: ibapi::prelude::SecurityType::ForexPair,
                            exchange: "IDEALPRO".into(),
                            currency: "SGD".into(),
                            ..Default::default()
                        },
                        false,
                        &[],
                    )
                    .ok()
            },
            last_updated: None,
        });
        #[cfg(feature = "backtest")]
        let reconcile = current_stock_positions_crud
            .update_positions_additive(pk, uk, BacktestMode::Db)
            .await;
        #[cfg(not(feature = "backtest"))]
        let reconcile = current_stock_positions_crud
            .update_positions_additive(pk, uk)
            .await;
        if let Err(e) = reconcile {
            tracing::error!(
                "Error trying to update current stock positions for reconciliation of FX positions: {e:?}"
            );
            errors.push(format!("Error trying to update current stock positions for reconciliation of FX positions: {e:?}"))
        };
    }
}

/// stock_map: Contract -> (current quantity, average price)
/// option_map: Contract -> (current quantity, average price)
async fn sync_stock_and_option_positions<PositionsBroker, MarketBroker>(
    account: String,
    client: &PositionsBroker,
    consolidator: &MarketBroker,
    stock_map: &HashMap<HashContract<'static>, (f64, f64)>,
    option_map: &HashMap<HashContract<'static>, (f64, f64)>,
    contract_to_strategy: &HashMap<HashContract<'static>, String>,
    default_strategy: &str,
    pool: PgPool,
    errors: &mut Vec<String>,
) where
    PositionsBroker: PositionsTracker,
    MarketBroker: ContractBroker,
{
    let mut broker_stock_positions_received = HashMap::<HashContract, (f64, f64)>::new();
    let mut broker_option_positions_received = HashMap::<HashContract, (f64, f64)>::new();
    for position_response in
        client.get_positions_multi(Some(&ibapi::accounts::types::AccountId(account)), None)
    {
        match position_response {
            Ok(PositionUpdateMulti::Position(position)) => {
                let contract = {
                    let validated_contract =
                        consolidator.get_validated_contract(position.contract.clone());
                    validated_contract.unwrap_or(position.contract)
                };
                let asset_type = AssetType::from_str(&contract.security_type);
                let hashed_contract = HashContract::new(contract);
                match asset_type {
                    AssetType::Stock
                    | AssetType::CFD
                    | AssetType::ForexPair
                    | AssetType::Future => {
                        broker_stock_positions_received
                            .insert(hashed_contract, (position.position, position.average_cost));
                    }
                    AssetType::Option => {
                        broker_option_positions_received
                            .insert(hashed_contract, (position.position, position.average_cost));
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
            Ok(PositionUpdateMulti::PositionEnd) => {
                println!("initial set of positions received - cancelling request");
                break;
            }
            Err(e) => {
                tracing::warn!("Encountered Error while syncing positions: {e:?}");
                errors.push(format!("Encountered Error while syncing positions: {e:?}"));
                break;
            }
        }
    }

    fn reconcile_position(
        pool: PgPool,
        contract: &Contract,
        local_position: (f64, f64),
        broker_position: (f64, f64),
        strategy: &str,
        // _consolidator: &Consolidator,
    ) {
        let discrepancy = broker_position.0 - local_position.0;
        if discrepancy == 0.0 {
            tracing::info!("No discrepancy for {:?}", contract.symbol);
            return;
        }
        let current_price = broker_position.1;
        // let current_price = {
        //     let val = consolidator.get_current_price(contract.clone(), false, &[]);
        //     if let Err(e) = val {
        //         tracing::error!(
        //             "Could not get current price for synced position: {e:?}! Falling back to 0.0!"
        //         );
        //         0.0
        //     } else {
        //         val.unwrap()
        //     }
        // };
        let asset_type = AssetType::from_str(&contract.security_type);
        let stock = contract.symbol.to_string();
        let current_positions_pk =
            CurrentPositionsPrimaryKeys::from_strat_and_contract(strategy, contract);
        let current_positions_uk =
            CurrentPositionsUpdateKeys::from(&asset_type, Some(discrepancy), Some(current_price));
        let default_strategy = strategy.to_string();
        tokio::spawn(async move {
            let mut db = crate::database::db::DbDirect::connect(&pool);
            let mut current_positions_crud = db.current_positions(&asset_type);
            #[cfg(feature = "backtest")]
            let res = current_positions_crud
                .update_positions_additive(
                    current_positions_pk,
                    current_positions_uk,
                    BacktestMode::Db,
                )
                .await;
            #[cfg(not(feature = "backtest"))]
            let res = current_positions_crud
                .update_positions_additive(current_positions_pk, current_positions_uk)
                .await;
            match res {
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
            broker_stock_positions_received.insert(local_contract.clone(), (0.0, 0.0));
        }
    }
    for (hash_contract, broker_position) in broker_stock_positions_received.into_iter() {
        let local_position = stock_map.get(&hash_contract).unwrap_or(&(0.0, 0.0));
        let strategy = contract_to_strategy
            .get(&hash_contract)
            .map_or(default_strategy.to_string(), |v| v.to_string());
        reconcile_position(
            pool.clone(),
            &hash_contract.contract,
            *local_position,
            broker_position,
            &strategy,
        )
    }
    // ====================
    // Sync Option Positions
    // ====================
    for local_contract in option_map.keys() {
        if !broker_option_positions_received.contains_key(&local_contract) {
            broker_option_positions_received.insert(local_contract.clone(), (0.0, 0.0));
        }
    }
    for (hash_contract, broker_position) in broker_option_positions_received.into_iter() {
        let local_position = option_map.get(&hash_contract).unwrap_or(&(0.0, 0.0));
        let strategy = contract_to_strategy
            .get(&hash_contract)
            .map_or(default_strategy.to_string(), |v| v.to_string());
        reconcile_position(
            pool.clone(),
            &hash_contract.contract,
            *local_position,
            broker_position,
            &strategy,
        )
    }
}
// ===================================== SYNC POSITIONS HELPERS ============================================

// ONLY called in conjunction with sync_open_orders
fn on_full_open_order_received<MarketBroker>(
    contract_to_strategy: &HashMap<HashContract, String>,
    pool: PgPool,
    consolidator: &MarketBroker,
    unvalidated_contract: &Contract,
    borrowed_order: &Order,
    order_status: &OrderStatus,
    default_strategy: &Option<String>,
) where
    MarketBroker: ContractBroker,
{
    // 1. Get Associated Strategy if possible, else default strategy
    let strategy = {
        if borrowed_order.order_ref == "" {
            contract_to_strategy
                .get(&HashContract::borrowed(unvalidated_contract))
                .unwrap_or(&default_strategy.clone().unwrap_or("unknown".to_string()))
                .to_string()
        } else {
            borrowed_order.order_ref.clone()
        }
    };

    // 2. Validate and fill all fields of contract - need to check again if this is a necessary
    //    call - but low on priority list since this is performed before mkt open - non-critical
    //    path
    let contract = consolidator
        .get_validated_contract(unvalidated_contract.clone())
        .unwrap_or(unvalidated_contract.clone());

    // 3. Begin a detached thread to update the DB accordingly
    let mut order = borrowed_order.clone();
    order.order_ref = strategy.clone();
    let order_status_filled = order_status.filled;
    tokio::spawn(async move {
        let asset_type = AssetType::from_str(&contract.security_type);
        let mut db = crate::database::db::DbDirect::connect(&pool);
        let mut open_orders_crud = db.open_orders(&asset_type);
        let open_order_pk = OpenOrdersPrimaryKeys::new(&asset_type, order.perm_id, order.order_id);
        let open_order_uk = OpenOrdersUpdateKeys::new_with_fill(
            &asset_type,
            &contract,
            &order,
            order_status_filled,
        );

        if let Err(e) = open_orders_crud
            .create_or_update(&open_order_pk, &open_order_uk)
            .await
        {
            tracing::error!("Failed to create_or_update on on_full_open_order_received: {e:?}");
        };
    });
}
