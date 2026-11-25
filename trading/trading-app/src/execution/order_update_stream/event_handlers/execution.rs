use crate::{
    database::tables::DbTables,
    execution::traits::order_router::OrderRouter,
    market_data::traits::{contracts::ContractBroker, current_price::PriceSupplier},
};
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

#[cfg(feature = "backtest")]
use crate::backtester::setup::config::BacktestMode;
use ibapi::orders::ExecutionData;
use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBufferProducer;
use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUDTrait,
        models::{
            AssetType, OpenOptionOrdersFullKeys, OpenOptionOrdersUpdateKeys,
            OpenStockOrdersFullKeys, OpenStockOrdersUpdateKeys,
        },
        models_crud::{
            current_positions::current_positions::{
                CurrentPositionsOps, CurrentPositionsPrimaryKeys, CurrentPositionsUpdateKeys,
            },
            open_orders::open_orders::{
                OpenOrdersFullKeys, OpenOrdersPrimaryKeys, OpenOrdersUpdateKeys,
            },
            transactions::transactions::{TransactionsFullKeys, TransactionsPrimaryKeys},
        },
    },
    execution::{
        fx_backed_up_order::OrderStore,
        order_engine::{OrderEngine, OrderIBKR},
    },
    strategy::strategy::StrategyDetails,
};

/// Should be triggered by ExecutionUpdate(ExecutionData) events
/// - calls the relevant on_execution events in on_execution_update: see there for what the
/// function actally does
/// - reads data from open_orders -> to transafer symbol (i.e. if open_order symbol is correct,
/// this is correct)
/// - also means that matching of arms is purely for database filtering (NOTE) -> depends on
/// correctness of on_new_order_submitted
pub fn on_execution_update<MarketBroker, OrderBroker>(
    consolidator: &Weak<MarketBroker>,
    pool: PgPool,
    mut execution_data: ExecutionData,
    execution_events_queue: Arc<HashMap<String, SpmcRingBufferProducer<ExecutionData, 64, 1>>>,
    strategy_details: &Arc<HashMap<String, StrategyDetails>>,
    default_strategy: &str,
    handle: tokio::runtime::Handle,
    weak_client: &Weak<OrderBroker>,
    backed_up_orders: Arc<OrderStore>,
) -> Result<(), String>
where
    MarketBroker: PriceSupplier + ContractBroker,
    OrderBroker: OrderRouter + Send + Sync + 'static,
{
    let contract = {
        consolidator
            .upgrade()
            .ok_or("Consolidator died when received submitted order update")?
            .get_validated_contract(execution_data.contract.clone())
            .ok_or("No valid contract for contract for order update")?
    };
    execution_data.contract = contract;
    let asset_type = AssetType::from_str(&execution_data.contract.security_type);
    match asset_type {
        AssetType::CASH => {
            tracing::error!(
                "New Order received for CASH asset - need to identify why ({}, {})",
                execution_data.contract.symbol,
                execution_data.contract.security_type
            );
            return Err("ERORORKOKEOKRK".to_string());
        }
        AssetType::Unknown => {
            tracing::error!(
                message=%format!(
                    "New Execution: Unknown security type encountered in system for symbol {}: {}",
                    execution_data.contract.symbol,
                    execution_data.contract.security_type
                )
            );
            return Err("ERORORKOKEOKRK".to_string());
        }
        _ => {}
    }

    let strategy_name = if execution_data.execution.order_reference != "" {
        execution_data.execution.order_reference.clone()
    } else {
        "unknown".to_string()
    };

    let strategy_detail = strategy_details
        .get(&strategy_name)
        .unwrap_or(
            strategy_details
                .get(default_strategy)
                .expect("Expected default strategy to be defined in strategy map"),
        )
        .clone();

    let weak_client = weak_client.clone();
    let cloned_handle = handle.clone();
    handle.spawn(async move {
        let mut db = match crate::database::db::DbTxn::begin(&pool).await {
            Ok(db) => db,
            Err(e) => {
                tracing::error!("Failed to begin execution transaction: {e:?}");
                return;
            }
        };

        // Check if transaction is valid - not a duplicate.
        let transaction_pk = TransactionsPrimaryKeys::from(&execution_data);
        match db.transactions(&asset_type).read(&transaction_pk).await {
            Ok(Some(_)) => {
                tracing::debug!("Already processed duplicate transaction");
                return;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!("{e:?}");
                return;
            }
        }

        // Read Associated Open Order
        tracing::info!(
            message=%format!("Execution: Looking for order with order_id {}",
            &execution_data.execution.order_id)
        );
        let open_order_pk = OpenOrdersPrimaryKeys::from_execution(&execution_data);
        let open_order_opt = match db.open_orders(&asset_type).read_for_update(&open_order_pk).await {
            Ok(val) => val,
            Err(e) => {
                tracing::error!("Failed to read open orders: {e:?}");
                return;
            }
        };

        // If No previous Open Order
        if open_order_opt.is_none() {
            tracing::warn!("OpenStockOrders does not contain required row");
            let transaction_fk =
                TransactionsFullKeys::from_strat_and_exec(&strategy_name, &execution_data);
            let currency_pk = CurrentPositionsPrimaryKeys::from_strat_and_contract_currency(
                &strategy_name,
                &execution_data.contract,
            );
            let currency_uk = CurrentPositionsUpdateKeys::from_execution_currency(&execution_data);
            let current_positions_pk = CurrentPositionsPrimaryKeys::from_strat_and_contract(
                &strategy_name,
                &execution_data.contract,
            );
            let current_positions_uk = CurrentPositionsUpdateKeys::from_execution(&execution_data);
            let placement_pk = currency_pk.clone();

            let write_res: Result<(), String> = async {
                db.transactions(&asset_type).create(&transaction_fk).await?;
                update_position(
                    &mut db,
                    &asset_type,
                    current_positions_pk,
                    current_positions_uk,
                )
                .await?;
                update_position(&mut db, &asset_type, currency_pk, currency_uk).await?;
                Ok(())
            }
            .await;

            if let Err(e) = write_res {
                tracing::error!("Failed to apply execution transaction: {e:?}");
                return;
            }
            if let Err(e) = db.commit().await {
                tracing::error!("Failed to commit execution transaction: {e:?}");
                return;
            }

            if strategy_detail.hooks_execution {
                forward_execution_hook(&execution_events_queue, &strategy_name, &execution_data);
            }

            tokio::spawn(async move {
                place_backed_up_orders(
                    cloned_handle,
                    pool,
                    &weak_client,
                    asset_type,
                    placement_pk,
                    backed_up_orders,
                    strategy_name.as_str(),
                ).await;
            });
            return;
        }

        // By here, there is an incoming execution + a matching open order.
        let open_order = open_order_opt.unwrap();
        let (mut executions, filled, quantity, strategy_name, stock) = match &open_order {
            &OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                ref executions,
                filled,
                quantity,
                ref strategy,
                ref stock,
                ..
            }) => (
                executions.clone(),
                filled,
                quantity,
                strategy.clone(),
                stock.clone(),
            ),
            &OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                ref executions,
                filled,
                quantity,
                ref strategy,
                ref stock,
                ..
            }) => (
                executions.clone(),
                filled,
                quantity,
                strategy.clone(),
                stock.clone(),
            ),
        };

        let open_order_pk = OpenOrdersPrimaryKeys::from_open_order(&open_order);
        let open_order_uk = if execution_data.execution.cumulative_quantity == quantity.abs() {
            None
        } else {
            if executions.contains(&execution_data.execution.execution_id) {
                tracing::warn!("Open Order found but execution already recorded");
                return;
            }
            executions.push(execution_data.execution.execution_id.to_string());

            if filled
                != execution_data.execution.cumulative_quantity - execution_data.execution.shares
            {
                tracing::error!(
                    message=%format!("New Execution: Cumulative Quantity does not coincide with locally tracked filled quantity (Cumulative: {}, Locally Tracked: {})",
                    execution_data.execution.cumulative_quantity
                        - execution_data.execution.shares,
                    filled
                    )
                );
            }

            Some(match &open_order {
                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                    time,
                    quantity,
                    filled,
                    ..
                }) => OpenOrdersUpdateKeys::Stock(OpenStockOrdersUpdateKeys {
                    strategy: None,
                    stock: None,
                    primary_exchange: None,
                    currency: None,
                    time: Some(*time),
                    quantity: Some(*quantity),
                    parent_order_id: None,
                    executions: Some(executions),
                    filled: Some(filled + execution_data.execution.shares),
                }),
                OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                    time,
                    quantity,
                    filled,
                    ..
                }) => OpenOrdersUpdateKeys::Options(OpenOptionOrdersUpdateKeys {
                    strategy: None,
                    stock: None,
                    primary_exchange: None,
                    currency: None,
                    expiry: None,
                    strike: None,
                    multiplier: None,
                    option_type: None,
                    time: Some(*time),
                    quantity: Some(*quantity),
                    parent_order_id: None,
                    executions: Some(executions),
                    filled: Some(filled + execution_data.execution.shares),
                }),
            })
        };

        let transaction_fk =
            TransactionsFullKeys::from_strat_and_exec(&strategy_name, &execution_data);
        let mut current_positions_pk = CurrentPositionsPrimaryKeys::from_open_order(&open_order);
        let stock = if strategy_detail.is_fx_strategy {
            stock
        } else {
            match stock.strip_prefix("FX:") {
                Some(currencies) => format!("CASH:{}", currencies.split(":").next().unwrap()),
                None => stock,
            }
        };
        current_positions_pk.with_stock(&stock);
        let current_positions_uk = CurrentPositionsUpdateKeys::from_execution(&execution_data);
        let currency_pk = CurrentPositionsPrimaryKeys::from_strat_and_contract_currency(
            &strategy_name,
            &execution_data.contract,
        );
        let currency_uk = CurrentPositionsUpdateKeys::from_execution_currency(&execution_data);
        let placement_pk = currency_pk.clone();

        let write_res: Result<(), String> = async {
            match &open_order_uk {
                Some(uk) => {
                    db.open_orders(&asset_type)
                        .update(&open_order_pk, uk)
                        .await?;
                }
                None => {
                    db.open_orders(&asset_type).delete(&open_order_pk).await?;
                }
            }
            db.transactions(&asset_type).create(&transaction_fk).await?;
            update_position(
                &mut db,
                &asset_type,
                current_positions_pk,
                current_positions_uk,
            )
            .await?;
            update_position(&mut db, &asset_type, currency_pk, currency_uk).await?;
            Ok(())
        }
        .await;

        if let Err(e) = write_res {
            tracing::error!("Failed to apply execution transaction: {e:?}");
            return;
        }
        if let Err(e) = db.commit().await {
            tracing::error!("Failed to commit execution transaction: {e:?}");
            return;
        }

        if strategy_detail.hooks_execution {
            forward_execution_hook(&execution_events_queue, &strategy_name, &execution_data);
        }

        tokio::spawn(async move {
            place_backed_up_orders(
                cloned_handle,
                pool,
                &weak_client,
                asset_type,
                placement_pk,
                backed_up_orders,
                strategy_name.as_str(),
            ).await;
        });
    });
    Ok(())
}

async fn update_position(
    db: &mut crate::database::db::DbTxn,
    asset_type: &AssetType,
    pk: CurrentPositionsPrimaryKeys,
    uk: CurrentPositionsUpdateKeys,
) -> Result<(), String> {
    let mut current_positions_crud = db.current_positions(asset_type);
    #[cfg(feature = "backtest")]
    {
        current_positions_crud
            .update_positions_additive(pk, uk, BacktestMode::Db)
            .await
    }
    #[cfg(not(feature = "backtest"))]
    {
        current_positions_crud
            .update_positions_additive(pk, uk)
            .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn place_backed_up_orders<OrderBroker>(
    handle: tokio::runtime::Handle,
    pool: PgPool,
    weak_client: &Weak<OrderBroker>,
    asset_type: AssetType,
    pk: CurrentPositionsPrimaryKeys,
    backed_up_orders: Arc<OrderStore>,
    strategy_name: &str,
) where
    OrderBroker: OrderRouter,
{
    let mut db = crate::database::db::DbDirect::connect(&pool);
    let mut current_positions_crud = db.current_positions(&asset_type);
    let current_positions_currency: Option<
        crate::database::models_crud::current_positions::current_positions::CurrentPositionsFullKeys,
    > = match current_positions_crud.read(&pk).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(
                "Failed to fetch current currency position, ignoring backed_up_orders! Err: {e:?}"
            );
            return;
        }
    };

    let currency_value = current_positions_currency
        .expect("Expected current positions of currency not to be None")
        .get_qty();
    if currency_value <= 0.0 {
        return;
    }

    let currency = pk
        .get_stock()
        .strip_prefix("CASH:")
        .expect("Expected currency stock to have CASH: prefix")
        .to_string();

    let ready = match backed_up_orders.remove_all_valid_orders(
        strategy_name,
        &currency,
        currency_value,
    ) {
        Ok(ready) => ready,
        Err(e) => {
            tracing::error!(
                "Failed to take placeable orders from backed_up_orders for {strategy_name}: {e:?}"
            );
            return;
        }
    };

    let mut failed: Vec<OrderIBKR> = Vec::new();
    for strat_order in ready {
        let mut to_place = strat_order.clone();
        // The parked copy keeps its "{strategy}:{price}" suffix so a failed placement can be
        // re-parked and re-priced; only the submitted order carries the plain strategy.
        to_place.order.order_ref = strategy_name.to_string();
        let order_id =
            OrderEngine::place_order(handle.clone(), pool.clone(), weak_client, to_place);
        if order_id == -1 {
            failed.push(strat_order);
        }
    }
    if !failed.is_empty() {
        if let Err(e) = backed_up_orders.insert_or_update_orders(strategy_name, &failed) {
            tracing::error!("Failed to re-park orders after failed placement: {e:?}");
        }
    }
}

fn forward_execution_hook(
    execution_events_queue: &Arc<HashMap<String, SpmcRingBufferProducer<ExecutionData, 64, 1>>>,
    strategy_name: &str,
    execution_data: &ExecutionData,
) {
    match execution_events_queue.get(strategy_name) {
        Some(execution_events_producer) => {
            if let Err(e) = execution_events_producer.try_push(execution_data.clone()) {
                tracing::error!("Failed to push execution event for {strategy_name}: {e:?}");
            };
        }
        None => {
            let symbol = &execution_data.contract.symbol;
            tracing::warn!("Unknown strategy's order received: {strategy_name}, {symbol}");
        }
    }
}
