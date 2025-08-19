// NOTE: Mutex here is only used to pass to place_order
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread::{self},
};

use chrono::{NaiveDateTime, TimeZone, Utc};
use ibapi::{
    Client,
    orders::{Action, CommissionReport, ExecutionData, Order, OrderStatus, order_builder},
    prelude::{Contract, SecurityType},
};
use rust_decimal::prelude::FromPrimitive;
use sqlx::PgPool;
use tokio::time::sleep;
use tracing::{error, info};

use crate::{
    database::{
        crud::CRUDTrait,
        models::{
            AssetType, OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys,
            OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OptionTransactionsPrimaryKeys,
            OptionTransactionsUpdateKeys, OptionType, StagedCommissionsPrimaryKeys,
            StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys,
        },
        models_crud::{
            current_option_positions::{
                get_current_option_positions_crud, get_specific_current_option_positions_crud,
            },
            current_stock_positions::{
                get_current_stock_positions_crud, get_specific_current_stock_positions_crud,
            },
            open_option_orders::{get_open_option_orders_crud, get_specific_option_orders_crud},
            open_stock_orders::{get_open_stock_orders_crud, get_specific_open_stock_orders_crud},
            option_transactions::get_option_transactions_crud,
            staged_commissions::get_staged_commissions_crud,
            stock_transactions::get_stock_transactions_crud,
        },
    },
    execution::{
        events::on_execution_updates::{on_new_option_execution, on_new_stock_execution},
        place_order::place_order,
    },
    unlock,
};

/// Should be triggered by Submitted and PreSubmitted Order Events to update the local OpenOrders
/// table
pub fn on_new_order_submitted(
    pool: PgPool,
    order_id: i32,
    perm_id: i32,
    strategy_order: (String, Contract, Order),
) -> Result<tokio::task::JoinHandle<()>, String> {
    if strategy_order.1.security_type == SecurityType::Stock
        || strategy_order.1.security_type == SecurityType::Future
        || strategy_order.1.security_type == SecurityType::ForexPair
    {
        let open_stock_orders_crud = get_open_stock_orders_crud(pool.clone());
        let qty = {
            if strategy_order.2.action == Action::Sell {
                -1.0
            } else {
                1.0
            }
        } * strategy_order.2.total_quantity;
        Ok(tokio::spawn(async move {
            if let Err(e) = open_stock_orders_crud
                .create_or_ignore(&OpenStockOrdersFullKeys {
                    order_perm_id: perm_id.clone(),
                    order_id: order_id.clone(),
                    strategy: strategy_order.0.clone(),
                    stock: strategy_order.1.symbol.clone(),
                    primary_exchange: strategy_order.1.primary_exchange.clone(),
                    time: Utc::now(),
                    quantity: qty,
                    filled: 0.0,
                    executions: Vec::new(),
                })
                .await
            {
                tracing::error!("Error occured while inserting into OpenStockOrders: {}", e)
            };
        }))
    } else if strategy_order.1.security_type == SecurityType::Option {
        let open_option_orders_crud = get_open_option_orders_crud(pool.clone());
        let qty = {
            if strategy_order.2.action == Action::Sell {
                -1.0
            } else {
                1.0
            }
        } * strategy_order.2.total_quantity;
        Ok(tokio::spawn(async move {
            if let Err(e) = open_option_orders_crud
                .create_or_ignore(&OpenOptionOrdersFullKeys {
                    order_id: order_id.clone(),
                    order_perm_id: perm_id.clone(),
                    strategy: strategy_order.0.clone(),
                    stock: strategy_order.1.symbol.clone(),
                    primary_exchange: strategy_order.1.primary_exchange.clone(),
                    expiry: strategy_order.1.last_trade_date_or_contract_month,
                    strike: strategy_order.1.strike,
                    multiplier: strategy_order.1.multiplier,
                    option_type: crate::database::models::OptionType::from_str(
                        &strategy_order.1.right,
                    )
                    .unwrap_or_else(|e| panic!("{}", e)),
                    time: Utc::now(),
                    quantity: qty,

                    filled: 0.0,
                    executions: Vec::new(),
                })
                .await
            {
                tracing::error!("Error occured while inserting into OpenStockOrders: {}", e)
            };
        }))
    } else {
        tracing::error!(
            "New Order: Unknown security type encountered in system for symbol {}: {}",
            strategy_order.1.symbol,
            strategy_order.1.security_type
        );
        Err("Error trying to create new open order".to_string())
    }
}

/// Should be triggered on "Cancelled" or "ApiCancelled"
/// - deletes the associated order in the OpenOrders table
pub fn on_order_cancelled(
    pool: PgPool,
    status: OrderStatus,
    strategy_order: (String, Contract, Order),
) {
    if strategy_order.1.security_type == SecurityType::Stock
        || strategy_order.1.security_type == SecurityType::Future
    {
        let open_stock_orders_crud = get_open_stock_orders_crud(pool.clone());

        tokio::spawn(async move {
            if let Err(e) = open_stock_orders_crud
                .delete(&OpenStockOrdersPrimaryKeys {
                    order_perm_id: status.perm_id.clone(),
                    order_id: status.order_id.clone(),
                })
                .await
            {
                tracing::error!("Error occured while inserting into OpenStockOrders: {}", e)
            }
        });
    } else if strategy_order.1.security_type == SecurityType::Option {
        let open_option_orders_crud = get_open_option_orders_crud(pool.clone());

        tokio::spawn(async move {
            if let Err(e) = open_option_orders_crud
                .delete(&OpenOptionOrdersPrimaryKeys {
                    order_perm_id: status.perm_id.clone(),
                    order_id: status.order_id.clone(),
                })
                .await
            {
                tracing::error!("Error occured while inserting into OpenStockOrders: {}", e)
            }
        });
    } else {
        tracing::error!(
            "Order Cancelled: Unknown security type encountered in system for symbol {}: {}",
            strategy_order.1.symbol,
            strategy_order.1.security_type
        )
    }
}

/// Should be triggered by ExecutionUpdate(ExecutionData) events
/// - calls the relevant on_execution events in on_execution_update: see there for what the
/// function actally does
pub fn on_execution_update(pool: PgPool, execution_data: ExecutionData) {
    if execution_data.contract.security_type == SecurityType::Stock
        || execution_data.contract.security_type == SecurityType::Future
        || execution_data.contract.security_type == SecurityType::ForexPair
    {
        let open_stock_orders_crud = get_open_stock_orders_crud(pool.clone());
        let stock_transactions_crud = get_stock_transactions_crud(pool.clone());
        let current_stock_positions_crud = get_current_stock_positions_crud(pool.clone());
        let specific_current_stock_positions_crud =
            get_specific_current_stock_positions_crud(pool.clone());

        on_new_stock_execution(
            open_stock_orders_crud,
            stock_transactions_crud,
            current_stock_positions_crud,
            specific_current_stock_positions_crud,
            execution_data.clone(),
        );
    } else if execution_data.contract.security_type == SecurityType::Option {
        let open_option_orders_crud = get_open_option_orders_crud(pool.clone());
        let option_transactions_crud = get_option_transactions_crud(pool.clone());
        let current_option_positions_crud = get_current_option_positions_crud(pool.clone());
        let specific_current_option_positions_crud =
            get_specific_current_option_positions_crud(pool.clone());

        on_new_option_execution(
            open_option_orders_crud,
            option_transactions_crud,
            current_option_positions_crud,
            specific_current_option_positions_crud,
            execution_data.clone(),
        );
    } else {
        tracing::error!(
            "New Execution: Unknown security type encountered in system for symbol {}: {}",
            execution_data.contract.symbol,
            execution_data.contract.security_type
        )
    }
}

/// Should be triggered by CommissionUpdate(CommissionReport) events
/// Simply create_or_update the row in StagedCommissions
/// - StagedCommissions should have triggers attached to update the associated transactions
/// automatically on inserts
pub fn on_commission_update(
    pool: PgPool,
    // execution_data: ExecutionData,
    commission_report: CommissionReport,
) -> Result<(), String> {
    // let strategy = {
    //     let order_map = unlock!(order_map, "order_map", "OrderEngine.on_commission_update");
    //     if let Some(strategy) = order_map.get(&execution_data.execution.order_id) {
    //         Ok(strategy.clone())
    //     } else {
    //         Err("No Strategy Found".to_string())
    //     }
    // }?;
    // let naive_dt =
    //     NaiveDateTime::parse_from_str(&execution_data.execution.time, "%Y%m%d  %H:%M:%S").expect(
    //         &format!(
    //             "Failed to parse execution time: {}",
    //             &execution_data.execution.time
    //         ),
    //     );
    // let execution_time = Utc
    //     .from_local_datetime(&naive_dt)
    //     .single()
    //     .expect("Ambiguous or invalid datetime in New York timezone");

    let staged_commissions_crud = get_staged_commissions_crud(pool.clone());
    tokio::spawn(async move {
        sleep(tokio::time::Duration::from_millis(10)).await;
        if let Err(e) = staged_commissions_crud
            .create_or_update(
                &StagedCommissionsPrimaryKeys {
                    // order_perm_id: execution_data.execution.perm_id,
                    // time: execution_time,
                    execution_id: commission_report.execution_id,
                },
                &crate::database::models::StagedCommissionsUpdateKeys {
                    fees: Some(
                        rust_decimal::Decimal::from_f64(commission_report.commission).expect(
                            "Expected commission from commission_report to be valid for Decimal",
                        ),
                    ),
                },
            )
            .await
        {
            error!("Error trying to insert into StagedCommissions table: {}", e);
        }
    });
    Ok(())
    // if execution_data.contract.security_type == SecurityType::Stock
    //     || execution_data.contract.security_type == SecurityType::Future
    //     || execution_data.contract.security_type == SecurityType::ForexPair
    // {
    //     let stock_transactions_crud = get_stock_transactions_crud(pool.clone());
    //
    //     tokio::spawn(async move {
    //         let naive_dt =
    //             NaiveDateTime::parse_from_str(&execution_data.execution.time, "%Y%m%d  %H:%M:%S")
    //                 .expect(&format!(
    //                     "Failed to parse execution time: {}",
    //                     &execution_data.execution.time
    //                 ));
    //         let execution_time = Utc
    //             .from_local_datetime(&naive_dt)
    //             .single()
    //             .expect("Ambiguous or invalid datetime in New York timezone");
    //         info!(
    //             "Now updating commissions with order_perm_id: {}, time: {} with fees: {}, stock: {}, asset_type: {}, strategy: {}",
    //             &execution_data.execution.perm_id,
    //             &execution_time,
    //             &commission_report.commission,
    //             &strategy.1.symbol,
    //             &AssetType::from_str(strategy.1.security_type.clone()),
    //             &strategy.0
    //         );
    //         match stock_transactions_crud
    //             .update(
    //                 &StockTransactionsPrimaryKeys {
    //                     order_perm_id: execution_data.execution.perm_id.clone(),
    //                     strategy: strategy.0,
    //                     stock: strategy.1.symbol,
    //                     asset_type: crate::database::models::AssetType::from_str(
    //                         strategy.1.security_type,
    //                     ),
    //                     time: execution_time,
    //                 },
    //                 &StockTransactionsUpdateKeys {
    //                     price: None,
    //                     quantity: None,
    //                     fees: Some(commission_report.commission),
    //                 },
    //             )
    //             .await
    //         {
    //             Ok(rows_affected) => {
    //                 if rows_affected == 0 {
    //                     commission_map
    //                         .lock()
    //                         .expect("Expected to be able to read and update commission_map")
    //                         .insert(
    //                             execution_data.execution.execution_id.clone(),
    //                             commission_report.commission,
    //                         );
    //                 }
    //             }
    //             Err(e) => tracing::error!(
    //                 "Error while trying to update execution in StockTransactions: {}",
    //                 e
    //             ),
    //         };
    //     });
    //     Ok(())
    // } else if execution_data.contract.security_type == SecurityType::Option {
    //     let option_transactions_crud = get_option_transactions_crud(pool.clone());
    //
    //     tokio::spawn(async move {
    //         let naive_dt =
    //             NaiveDateTime::parse_from_str(&execution_data.execution.time, "%Y%m%d  %H:%M:%S")
    //                 .expect(&format!(
    //                     "Failed to parse execution time: {}",
    //                     &execution_data.execution.time
    //                 ));
    //         let execution_time = Utc
    //             .from_local_datetime(&naive_dt)
    //             .single()
    //             .expect("Ambiguous or invalid datetime in New York timezone");
    //         match option_transactions_crud
    //                 .update(
    //                     &&OptionTransactionsPrimaryKeys {
    //                         strategy: strategy.0,
    //                         order_perm_id: execution_data.execution.order_id,
    //                         stock: strategy.1.symbol,
    //                         asset_type: AssetType::from_str(strategy.1.security_type),
    //                         expiry: strategy.1.last_trade_date_or_contract_month,
    //                         strike: strategy.1.strike,
    //                         multiplier: strategy.1.multiplier,
    //                         option_type: OptionType::from_str(&strategy.1.right).expect(format!("Parse Error when parsing contract right in commission_report update: {}", &strategy.1.right).as_str()),
    //                         time: execution_time.to_utc(),
    //                     },
    //                     &&OptionTransactionsUpdateKeys {
    //                         price: None,
    //                         quantity: None,
    //                         fees: Some(commission_report.commission),
    //                     },
    //                 )
    //                 .await
    //             {
    //                 Ok(rows_affected) => {
    //                     if rows_affected == 0 {
    //                         commission_map
    //                             .lock()
    //                             .expect("Expected to be able to read and update commission_map")
    //                             .insert(
    //                                 execution_data.execution.execution_id.clone(),
    //                                 commission_report.commission,
    //                             );
    //                     }
    //                 },
    //                 Err(e) => tracing::error!(
    //                     "Error while trying to update execution in StockTransactions: {}",
    //                     e
    //                 )
    //             };
    //     });
    //
    //     Ok(())
    // } else {
    //     tracing::error!(
    //         "New Commission Report: Unknown security type encountered in system for symbol {}: {}",
    //         execution_data.contract.symbol,
    //         execution_data.contract.security_type
    //     );
    //     Err(format!(
    //         "New Commission Report: Unknown security type encountered in system for symbol {}: {}",
    //         execution_data.contract.symbol, execution_data.contract.security_type
    //     ))
    // }
}

/// Provides the logic to handle open order
/// - i.e. cancelling and placing orders efficiently
pub async fn on_new_stock_qty_diff_for_strat(
    pool: PgPool,
    contract: Contract,
    client: Arc<Client>,
    order_map: Arc<Mutex<HashMap<i32, (String, Contract, Order)>>>,
    strategy: String,
    qty_diff: f64,
    avg_price: f64,
) {
    let open_stock_orders_crud = get_specific_open_stock_orders_crud(pool.clone());
    let open_orders = open_stock_orders_crud
        .get_orders_for_strat(&strategy)
        .await
        .expect("Expected to be able to get open orders from OpenStockOrders"); // this should only

    let tot_qty_dir = open_orders
        .iter()
        .map(|open_order| open_order.quantity.signum())
        .sum::<f64>()
        .abs() as u64;
    if tot_qty_dir != open_orders.len() as u64 {
        error!(
            "Error: Open orders placed for {} for stock {} are not all in the same direction!",
            &strategy, &contract.symbol
        );
    };
    let (curr_open_orders_filled, curr_open_orders_quantity): (f64, f64) = (
        open_orders.iter().map(|open_order| open_order.filled).sum(),
        open_orders
            .iter()
            .map(|open_order| open_order.quantity)
            .sum(),
    );

    // return 1 entry
    let current_qty_diff = (curr_open_orders_quantity - curr_open_orders_filled)
        * (curr_open_orders_quantity.signum());

    if qty_diff == 0.0 {
        open_orders.iter().for_each(|open_order| {
            let order_id = open_order.order_id.clone();
            let cloned_client = client.clone();
            thread::spawn(move || {
                cloned_client.cancel_order(order_id, "");
            });
            let (perm_id, order_id) = (
                open_order.order_perm_id.clone(),
                open_order.order_id.clone(),
            );
            let pool = pool.clone();
            tokio::spawn(async move {
                let open_option_orders_crud = get_open_stock_orders_crud(pool);
                if let Err(e) = open_option_orders_crud
                    .delete(&OpenStockOrdersPrimaryKeys {
                        order_perm_id: perm_id,
                        order_id: order_id,
                    })
                    .await
                {
                    tracing::error!("Error trying to delete OpenOptionOrder entry: {}", e)
                };
            });
        });
        return;
    }

    // Cancel the order if qty_diff is in wrong direction / open order qty too high
    if current_qty_diff.signum() != qty_diff.signum()
        || (current_qty_diff.signum() == qty_diff.signum()
            && current_qty_diff.abs() > qty_diff.abs())
    {
        open_orders.iter().for_each(|open_order| {
            let order_id = open_order.order_id.clone();
            let cloned_client = client.clone();
            thread::spawn(move || {
                cloned_client.cancel_order(order_id, "");
            });
        });
        thread::spawn(move || {
            let action = if qty_diff > 0.0 {
                Action::Buy
            } else {
                Action::Sell
            };
            place_order(
                order_map,
                strategy,
                client,
                contract,
                if avg_price == 0.0 {
                    order_builder::market_order(action, qty_diff.abs())
                } else {
                    order_builder::limit_order(action, qty_diff.abs(), avg_price)
                },
                false,
            )
        });

        open_orders.iter().for_each(|open_order| {
            let open_stock_orders_crud = get_open_stock_orders_crud(pool.clone());
            let (perm_id, order_id) = (open_order.order_perm_id, open_order.order_id);
            tokio::spawn(async move {
                if let Err(e) = open_stock_orders_crud
                    .delete(&OpenStockOrdersPrimaryKeys {
                        order_perm_id: perm_id,
                        order_id: order_id,
                    })
                    .await
                {
                    tracing::error!("Error trying to delete entry in OpenStockOrders: {}", e)
                }
            });
        });
        return;
    }
    if current_qty_diff.abs() < qty_diff.abs() {
        thread::spawn(move || {
            let action = if qty_diff > 0.0 {
                Action::Buy
            } else {
                Action::Sell
            };
            place_order(
                order_map,
                strategy,
                client,
                contract,
                if avg_price == 0.0 {
                    order_builder::market_order(action, (qty_diff - current_qty_diff).abs())
                } else {
                    order_builder::limit_order(
                        action,
                        (qty_diff - current_qty_diff).abs(),
                        avg_price,
                    )
                },
                false,
            )
        });
    }
}

/// Provides the logic to handle open order
/// - i.e. cancelling and placing orders efficiently
/// - essentially the same as on_new_stock_qty_diff_for_strat
pub async fn on_new_option_qty_diff_for_strat(
    pool: PgPool,
    contract: Contract,
    client: Arc<Client>,
    order_map: Arc<Mutex<HashMap<i32, (String, Contract, Order)>>>,
    strategy: String,
    qty_diff: f64,
    avg_price: f64,
) {
    let open_option_orders_crud = get_specific_option_orders_crud(pool.clone());
    let open_orders = open_option_orders_crud
        .get_orders_for_strat(&strategy)
        .await
        .expect("Expected to be able to get open orders from OpenOptionOrders"); // this should only

    let tot_qty_dir = open_orders
        .iter()
        .map(|open_order| open_order.quantity.signum())
        .sum::<f64>()
        .abs() as u64;
    if tot_qty_dir != open_orders.len() as u64 {
        error!(
            "Error: Open orders placed for {} for stock {} are not all in the same direction!",
            &strategy, &contract.symbol
        );
    };
    let (curr_open_orders_filled, curr_open_orders_quantity): (f64, f64) = (
        open_orders.iter().map(|open_order| open_order.filled).sum(),
        open_orders
            .iter()
            .map(|open_order| open_order.quantity)
            .sum(),
    );

    // return 1 entry
    let current_qty_diff = (curr_open_orders_quantity - curr_open_orders_filled)
        * (curr_open_orders_quantity.signum());

    if qty_diff == 0.0 {
        open_orders.iter().for_each(|open_order| {
            let order_id = open_order.order_id.clone();
            let cloned_client = client.clone();
            thread::spawn(move || {
                cloned_client.cancel_order(order_id, "");
            });
            let (perm_id, order_id) = (
                open_order.order_perm_id.clone(),
                open_order.order_id.clone(),
            );
            let pool = pool.clone();
            tokio::spawn(async move {
                let open_option_orders_crud = get_open_option_orders_crud(pool);
                if let Err(e) = open_option_orders_crud
                    .delete(&OpenOptionOrdersPrimaryKeys {
                        order_perm_id: perm_id,
                        order_id: order_id,
                    })
                    .await
                {
                    tracing::error!("Error trying to delete OpenOptionOrder entry: {}", e)
                };
            });
        });
        return;
    }
    // Cancel the order if qty_diff is in wrong direction / open order qty too high
    if current_qty_diff.signum() != qty_diff.signum()
        || (current_qty_diff.signum() == qty_diff.signum()
            && current_qty_diff.abs() > qty_diff.abs())
    {
        open_orders.iter().for_each(|open_order| {
            let order_id = open_order.order_id.clone();
            let cloned_client = client.clone();
            thread::spawn(move || {
                cloned_client.cancel_order(order_id, "");
            });
        });
        thread::spawn(move || {
            let action = if qty_diff > 0.0 {
                Action::Buy
            } else {
                Action::Sell
            };
            place_order(
                order_map,
                strategy,
                client,
                contract,
                if avg_price == 0.0 {
                    order_builder::market_order(action, qty_diff.abs())
                } else {
                    order_builder::limit_order(action, qty_diff.abs(), avg_price)
                },
                false,
            )
        });

        open_orders.iter().for_each(|open_order| {
            let open_stock_orders_crud = get_open_stock_orders_crud(pool.clone());
            let (perm_id, order_id) = (open_order.order_perm_id, open_order.order_id);
            tokio::spawn(async move {
                if let Err(e) = open_stock_orders_crud
                    .delete(&OpenStockOrdersPrimaryKeys {
                        order_perm_id: perm_id,
                        order_id: order_id,
                    })
                    .await
                {
                    tracing::error!("Error trying to delete entry in OpenStockOrders: {}", e)
                }
            });
        });
        return;
    }
    if current_qty_diff < qty_diff {
        thread::spawn(move || {
            let action = if qty_diff > 0.0 {
                Action::Buy
            } else {
                Action::Sell
            };
            place_order(
                order_map,
                strategy,
                client,
                contract,
                if avg_price == 0.0 {
                    order_builder::market_order(action, (qty_diff - current_qty_diff).abs())
                } else {
                    order_builder::limit_order(
                        action,
                        (qty_diff - current_qty_diff).abs(),
                        avg_price,
                    )
                },
                false,
            )
        });
    }
}
