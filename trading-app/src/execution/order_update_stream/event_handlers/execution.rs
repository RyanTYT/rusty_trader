use std::{collections::HashMap, sync::Arc};

use ibapi::orders::ExecutionData;
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
                CurrentPositionsCRUD, CurrentPositionsOps, CurrentPositionsPrimaryKeys,
                CurrentPositionsUpdateKeys,
            },
            open_orders::open_orders::{
                OpenOrdersCRUD, OpenOrdersFullKeys, OpenOrdersPrimaryKeys, OpenOrdersUpdateKeys,
            },
            transactions::transactions::{
                TransactionsCRUD, TransactionsFullKeys, TransactionsPrimaryKeys,
            },
        },
    },
    strategy::strategy::{StrategyEnum, StrategyExecutor},
};

/// Should be triggered by ExecutionUpdate(ExecutionData) events
/// - calls the relevant on_execution events in on_execution_update: see there for what the
/// function actally does
/// - reads data from open_orders -> to transafer symbol (i.e. if open_order symbol is correct,
/// this is correct)
/// - also means that matching of arms is purely for database filtering (NOTE) -> depends on
/// correctness of on_new_order_submitted
pub fn on_execution_update(
    pool: PgPool,
    execution_data: ExecutionData,
    strategy_map: Arc<HashMap<String, StrategyEnum>>,
    default_strategy: &str,
) -> Result<(), String> {
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

    let default_strategy = default_strategy.to_string();
    tokio::spawn(async move {
        // Check if transaction is valid
        // - not a duplicate
        let transactions_crud = TransactionsCRUD::from(&asset_type, pool.clone());
        let transaction_pk = TransactionsPrimaryKeys::from(&execution_data);
        match &transactions_crud.read(&transaction_pk).await {
            Ok(transaction) => {
                if transaction.is_some() {
                    tracing::error!("Received duplicate transaction");
                    return;
                }
            }
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
        let open_orders_crud = OpenOrdersCRUD::from(&asset_type, pool.clone());
        let open_order_pk = OpenOrdersPrimaryKeys::from_execution(&execution_data);
        let open_order_opt = match open_orders_crud.read(&open_order_pk).await {
            Ok(val) => val,
            Err(e) => {
                tracing::error!("Failed to read open orders: {e:?}");
                return;
            }
        };
        let current_positions_crud = CurrentPositionsCRUD::from(&asset_type, pool.clone());

        // If No previous Open Order
        if let None = open_order_opt {
            let strategy = {
                let strat = &execution_data.execution.order_reference;
                if strat == "" {
                    &default_strategy
                } else {
                    strat
                }
            };

            tracing::warn!("OpenStockOrders does not contain required row");
            let transaction_fk =
                TransactionsFullKeys::from_strat_and_exec(&strategy, &execution_data);
            // Update Transaction with default strategy
            tokio::spawn(async move {
                if let Err(e) = transactions_crud.create(&transaction_fk).await {
                    tracing::error!(
                        "Error inserting into Transactions Table for unknown strategy: {e:?}"
                    );
                };
            });

            // Update Current Positions under default strategy
            let current_positions_pk = CurrentPositionsPrimaryKeys::from_strat_and_contract(
                &strategy,
                &execution_data.contract,
            );
            let current_positions_uk = CurrentPositionsUpdateKeys::from_execution(&execution_data);
            tokio::spawn(async move {
                if let Err(e) = current_positions_crud
                    .update_positions_additive(current_positions_pk, current_positions_uk)
                    .await
                {
                    tracing::error!(
                        "Error inserting into CurrentPositions for unknown strategy: {e:?}"
                    )
                };
            });
            return;
        }

        // By here, there is an incoming execution + a matching open order
        // So, we update OpenOrders && Transaction && CurrentPosition

        // ===== Update Open Orders =====
        let open_order = open_order_opt.unwrap();
        let (mut executions, filled, quantity, strategy, stock) = match &open_order {
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
        if execution_data.execution.cumulative_quantity == quantity.abs() {
            // delete open order if execution is filled - i.e. cum_qty == required qty
            tokio::spawn(async move {
                if let Err(e) = open_orders_crud.delete(&open_order_pk).await {
                    tracing::error!(
                        "Error occurend while deleting open order in OpenStockOrders: {e:?}"
                    )
                }
            });
        } else {
            // update executions array
            if executions.contains(&execution_data.execution.execution_id) {
                tracing::warn!("Open Order found but execution already recorder");
                return;
            }
            executions.push(execution_data.execution.execution_id.to_string());

            // Check that OpenOrder prev filled (i.e. cum qty) = execution_data's prev cum qty
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

            let open_order_uk = match &open_order {
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
                    executions: Some(executions),
                    filled: Some(filled + execution_data.execution.shares),
                }),
            };

            tokio::spawn(async move {
                if let Err(e) = open_orders_crud
                    .update(&open_order_pk, &open_order_uk)
                    .await
                {
                    tracing::error!("Error occured while updating OpenOrders: {e:?}")
                };
            });
        }

        // ===== Update Transactions =====
        let transaction_fk = TransactionsFullKeys::from_strat_and_exec(&strategy, &execution_data);
        tokio::spawn(async move {
            if let Err(e) = transactions_crud.create(&transaction_fk).await {
                tracing::error!("Error occured while inserting into Transactions: {e:?}")
            };
        });

        // ===== Update Positions =====
        let strategy_enum = strategy_map.get(&strategy).unwrap_or({
            tracing::warn!("Strategy ({strategy:?}) is not in strategy_map - using default strat");
            strategy_map.get(&default_strategy).expect(
                "Expected valid default strategy to be used\
                - i.e. default_strategy should be in strategy_map",
            )
        });

        let mut current_positions_pk = CurrentPositionsPrimaryKeys::from_open_order(&open_order);
        let stock = if strategy_enum.is_fx_strategy() {
            stock
        } else {
            match stock.strip_prefix("FX:") {
                Some(currencies) => format!("CASH:{}", currencies.split(":").next().unwrap()),
                None => stock,
            }
        };
        current_positions_pk.with_stock(&stock);
        let current_positions_uk = CurrentPositionsUpdateKeys::from_execution(&execution_data);

        current_positions_crud
            .update_positions_additive(current_positions_pk, current_positions_uk);
    });

    Ok(())
}
