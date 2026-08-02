// NOTE: Mutex here is only used to pass to place_order
use std::{
    collections::HashMap,
    sync::{Arc, RwLock, Weak},
    thread::{self},
};

use chrono::Utc;
use ibapi::{
    Client,
    orders::{
        Action, CommissionReport, ExecutionData, Order, OrderStatus,
        order_builder::{self, attach_adjustable_to_trail},
    },
    prelude::Contract,
};
use rust_decimal::prelude::FromPrimitive;
use sqlx::PgPool;
use tokio::time::sleep;
use tracing::error;

use crate::{
    database::{
        crud::CRUDTrait,
        models::{
            AssetType, OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys,
            OpenOptionOrdersUpdateKeys, OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys,
            OpenStockOrdersUpdateKeys, OptionTransactionsPrimaryKeys, StagedCommissionsPrimaryKeys,
            StockTransactionsPrimaryKeys,
        },
        models_crud::{
            current_positions::current_positions::{
                CurrentPositionsCRUD, CurrentPositionsOps, CurrentPositionsPrimaryKeys,
                CurrentPositionsUpdateKeys,
            },
            open_orders::open_orders::{
                OpenOrdersCRUD, OpenOrdersFullKeys, OpenOrdersOps, OpenOrdersPrimaryKeys,
                OpenOrdersUpdateKeys,
            },
            staged_commissions::StagedCommissionsCRUD,
            transactions::transactions::{
                TransactionsCRUD, TransactionsFullKeys, TransactionsPrimaryKeys,
            },
        },
    },
    execution::place_order::place_order,
    helpers::contract::{HashContract, get_local_symbol},
    strategy::strategy::{StrategyEnum, StrategyExecutor},
};

/// Should be triggered by Submitted and PreSubmitted Order Events to update the local OpenOrders
/// table
pub fn on_new_order_submitted(
    pool: PgPool,
    order_id: i32,
    perm_id: i32,
    strategy_order: &(String, Contract, Order),
) -> Result<tokio::task::JoinHandle<()>, String> {
    let asset_type = AssetType::from_str(&strategy_order.1.security_type);
    let (open_orders_crud, open_order_fk) = match asset_type {
        AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
            let qty = {
                if strategy_order.2.action == Action::Sell {
                    -1.0
                } else {
                    1.0
                }
            } * strategy_order.2.total_quantity;

            (
                OpenOrdersCRUD::from(&asset_type, pool),
                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                    order_perm_id: perm_id,
                    order_id: order_id,
                    strategy: strategy_order.0.clone(),
                    stock: get_local_symbol(&strategy_order.1),
                    primary_exchange: strategy_order.1.primary_exchange.to_string(),
                    currency: strategy_order.1.currency.to_string(),
                    time: Utc::now(),
                    quantity: qty,
                    filled: 0.0,
                    executions: Vec::new(),
                }),
            )
        }
        AssetType::Option => {
            let qty = {
                if strategy_order.2.action == Action::Sell {
                    -1.0
                } else {
                    1.0
                }
            } * strategy_order.2.total_quantity;

            (
                OpenOrdersCRUD::from(&asset_type, pool),
                OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                    order_id: order_id,
                    order_perm_id: perm_id,
                    strategy: strategy_order.0.clone(),
                    stock: strategy_order.1.symbol.as_str().to_string(),
                    primary_exchange: strategy_order.1.primary_exchange.to_string(),
                    currency: strategy_order.1.currency.to_string(),
                    expiry: strategy_order
                        .1
                        .last_trade_date_or_contract_month
                        .to_string(),
                    strike: strategy_order.1.strike,
                    multiplier: strategy_order.1.multiplier.to_string(),
                    option_type: crate::database::models::OptionType::from_str(
                        &strategy_order.1.right,
                    )
                    .unwrap_or_else(|e| panic!("{}", e)),
                    time: Utc::now(),
                    quantity: qty,
                    filled: 0.0,
                    executions: Vec::new(),
                }),
            )
        }
        AssetType::CASH => {
            tracing::error!(
                "New Order received for CASH asset - need to identify why ({}, {})",
                strategy_order.1.symbol,
                strategy_order.1.security_type
            );
            return Err("Error trying to create new open order".to_string());
        }
        AssetType::Unknown => {
            tracing::error!(
                message=%format!("New Order: Unknown security type encountered in system for symbol {}: {}",
                strategy_order.1.symbol,
                strategy_order.1.security_type
                )
            );
            return Err("Error trying to create new open order".to_string());
        }
    };

    Ok(tokio::spawn(async move {
        if let Err(e) = open_orders_crud.create_or_ignore(&open_order_fk).await {
            tracing::error!("Error occured while inserting into OpenStockOrders: {e:?}")
        };
    }))
}

/// Should be triggered on "Cancelled" or "ApiCancelled"
/// - deletes the associated order in the OpenOrders table
pub fn on_order_cancelled(
    pool: PgPool,
    status: &OrderStatus,
    strategy_order: &(String, Contract, Order),
) {
    let asset_type = AssetType::from_str(&strategy_order.1.security_type);
    let open_orders_crud = OpenOrdersCRUD::from(&asset_type, pool);
    let open_orders_pk = OpenOrdersPrimaryKeys::new(&asset_type, status.perm_id, status.order_id);

    tokio::spawn(async move {
        if let Err(e) = open_orders_crud.delete(&open_orders_pk).await {
            tracing::error!("Failed to cancel Open Order")
        }
    });
}

/// Should be triggered by CommissionUpdate(CommissionReport) events
/// Simply create_or_update the row in StagedCommissions
/// - StagedCommissions should have triggers attached to update the associated transactions
/// automatically on inserts
pub fn on_commission_update(
    pool: PgPool,
    commission_report: &CommissionReport,
) -> Result<(), String> {
    let staged_commissions_crud = StagedCommissionsCRUD::new(pool);
    let prim_key = StagedCommissionsPrimaryKeys {
        execution_id: commission_report.execution_id.to_string(),
    };
    let update_key = crate::database::models::StagedCommissionsUpdateKeys {
        fees: Some(
            rust_decimal::Decimal::from_f64(commission_report.commission)
                .expect("Expected commission from commission_report to be valid for Decimal"),
        ),
    };
    tokio::spawn(async move {
        sleep(tokio::time::Duration::from_millis(10)).await;
        if let Err(e) = staged_commissions_crud
            .create_or_update(&prim_key, &update_key)
            .await
        {
            error!("Error trying to insert into StagedCommissions table: {e:?}");
        }
    });
    Ok(())
}

/// Provides the logic to handle open order
/// - i.e. cancelling and placing orders efficiently
pub async fn on_new_qty_diff_for_strat(
    pool: PgPool,
    asset_type: &AssetType,
    contract: &Contract,
    weak_client: &Weak<Client>,
    order_map: Arc<RwLock<HashMap<i32, (String, Contract, Order)>>>,
    strategy: &str,
    qty_diff: &f64,
    avg_price: &f64,
    attachments: Vec<(HashContract, Order)>,
) -> Result<(), String> {
    let open_orders_crud = OpenOrdersCRUD::from(&asset_type, pool);
    let open_orders = match open_orders_crud.get_orders_for_strat(strategy).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Failed to get orders for strategy: {e:?}");
            return Err("Failed to get orders for strategy".to_string());
        }
    };

    let tot_qty_dir = open_orders
        .iter()
        .map(|open_order| match open_order {
            OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys { quantity, .. }) => {
                quantity.signum()
            }
            OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys { quantity, .. }) => {
                quantity.signum()
            }
        })
        .sum::<f64>()
        .abs() as u64;
    if tot_qty_dir != open_orders.len() as u64 {
        error!(
            message=%format!(
                "Error: Open orders placed for {} for stock {} are not all in the same direction!",
                &strategy, &contract.symbol
            )
        );
    };

    let (curr_open_orders_filled, curr_open_orders_quantity): (f64, f64) = (
        open_orders
            .iter()
            .map(|open_order| match open_order {
                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys { filled, .. }) => filled,
                OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys { filled, .. }) => filled,
            })
            .sum(),
        open_orders
            .iter()
            .map(|open_order| match open_order {
                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys { quantity, .. }) => quantity,
                OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys { quantity, .. }) => quantity,
            })
            .sum(),
    );

    // return 1 entry
    let current_qty_diff = (curr_open_orders_quantity - curr_open_orders_filled)
        * (curr_open_orders_quantity.signum());

    // Alr correct
    if *qty_diff == 0.0 {
        // Cancel Open Orders
        open_orders.iter().for_each(|open_order| {
            let (order_perm_id, order_id) = match open_order {
                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                    order_perm_id,
                    order_id,
                    ..
                }) => (order_perm_id, order_id),
                OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                    order_perm_id,
                    order_id,
                    ..
                }) => (order_perm_id, order_id),
            };

            // Cancel on IBKR side
            let cloned_weak_client = weak_client.clone();
            let owned_order_id = *order_id;
            thread::spawn(move || {
                let client_opt = cloned_weak_client.upgrade();
                if client_opt.is_none() {
                    tracing::warn!("client died while cancelling order!");
                    return;
                }
                let client = client_opt.unwrap();
                if let Err(e) = client.cancel_order(owned_order_id, "") {
                    tracing::warn!("Could not cancel order: {e:?}");
                };
            });

            // Cancel on Local Side (i.e. DB)
            let open_orders_crud_cloned = open_orders_crud.clone();
            let open_order_pk = OpenOrdersPrimaryKeys::new(&asset_type, *order_perm_id, *order_id);
            tokio::spawn(async move {
                if let Err(e) = open_orders_crud_cloned.delete(&open_order_pk).await {
                    tracing::error!("Error trying to delete OpenOptionOrder entry: {e:?}")
                };
            });
        });
        return Ok(());
    }

    // Cancel the order if qty_diff is in wrong direction / open order qty too high
    if current_qty_diff.signum() != qty_diff.signum()
        || (current_qty_diff.signum() == qty_diff.signum()
            && current_qty_diff.abs() > qty_diff.abs())
    {
        // Cancel all open orders first
        open_orders.iter().for_each(|open_order| {
            let order_id = match open_order {
                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys { order_id, .. }) => order_id,
                OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys { order_id, .. }) => order_id,
            };
            let weak_client_cloned = weak_client.clone();
            let owned_order_id = *order_id;
            thread::spawn(move || {
                let client_opt = weak_client_cloned.upgrade();
                if client_opt.is_none() {
                    tracing::warn!("client died while cancelling multiple orders!");
                    return;
                }
                let client = client_opt.unwrap();
                if let Err(e) = client.cancel_order(owned_order_id, "") {
                    tracing::warn!("Could not cancel order: {e:?}");
                };
            });
        });

        // Place order for qty diff
        let strat = strategy.to_string();
        let contract_cloned = contract.clone();
        let action = if *qty_diff > 0.0 {
            Action::Buy
        } else {
            Action::Sell
        };
        let order = if *avg_price == 0.0 {
            order_builder::market_order(action, qty_diff.abs())
        } else {
            order_builder::limit_order(action, qty_diff.abs(), *avg_price)
        };
        let weak_client_cloned = weak_client.clone();
        let attachments_clone = attachments.clone();
        thread::spawn(move || {
            place_order(
                order_map,
                strat.as_str(),
                &weak_client_cloned,
                &contract_cloned,
                &order,
                attachments_clone,
            )
        });

        // let cloned_pool = pool.clone();
        open_orders.iter().for_each(|open_order| {
            let open_orders_crud_cloned = open_orders_crud.clone();
            let (order_perm_id, order_id) = match open_order {
                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                    order_perm_id,
                    order_id,
                    ..
                }) => (order_perm_id, order_id),
                OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                    order_perm_id,
                    order_id,
                    ..
                }) => (order_perm_id, order_id),
            };
            let open_order_pk = OpenOrdersPrimaryKeys::new(&asset_type, *order_perm_id, *order_id);
            tokio::spawn(async move {
                if let Err(e) = open_orders_crud_cloned.delete(&open_order_pk).await {
                    tracing::error!("Error trying to delete entry in OpenOrders: {e:?}")
                }
            });
        });
        return Ok(());
    }

    // If it's here: Order is in same dirction of qty_diff
    if current_qty_diff.abs() < qty_diff.abs() {
        let strat = strategy.to_string();
        let contract_cloned = contract.clone();
        let action = if *qty_diff > 0.0 {
            Action::Buy
        } else {
            Action::Sell
        };
        let order = if *avg_price == 0.0 {
            order_builder::market_order(action, (*qty_diff - current_qty_diff).abs())
        } else {
            order_builder::limit_order(action, (*qty_diff - current_qty_diff).abs(), *avg_price)
        };
        let weak_client_cloned = weak_client.clone();
        let attachments_clone = attachments.clone();
        thread::spawn(move || {
            place_order(
                order_map,
                strat.as_str(),
                &weak_client_cloned,
                &contract_cloned,
                &order,
                attachments_clone,
            )
        });
    }

    Ok(())
}

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
    strategy_map: HashMap<String, StrategyEnum>,
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
            tracing::warn!(
                "OpenStockOrders does not contain required row - updating to default strategy"
            );
            let transaction_fk =
                TransactionsFullKeys::from_strat_and_exec(&default_strategy, &execution_data);
            // Update Transaction with default strategy
            tokio::spawn(async move {
                if let Err(e) = transactions_crud.create(&transaction_fk).await {
                    tracing::error!(
                        "Error inserting into Transactions Table for unknown strategy: {e:?}"
                    );
                };
            });

            // Update Current Positions under default strategy
            let current_positions_pk = CurrentPositionsPrimaryKeys::from_strat_and_exec(
                &default_strategy,
                &execution_data,
            );
            let current_positions_uk = CurrentPositionsUpdateKeys::from(&execution_data);
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
                }) => OpenOrdersUpdateKeys::stock(OpenStockOrdersUpdateKeys {
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
        // Final CRUD operation in alr spawned thread so unnecessary to spawn
        // another thread
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
        let current_positions_uk = CurrentPositionsUpdateKeys::from(&execution_data);

        current_positions_crud
            .update_positions_additive(current_positions_pk, current_positions_uk);
    });

    Ok(())
}
