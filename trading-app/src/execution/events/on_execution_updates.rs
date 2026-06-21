// Issues: Need to ensure parsing of datetime from execution_data.execution.time is correct - may
// need to parse with timezone as i suspect
// Lines 108, 324: DateTime Parsing

use std::collections::HashMap;

use chrono::{NaiveDateTime, TimeZone, Utc};
use ibapi::orders::ExecutionData;
use rust_decimal::dec;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys,
            CurrentOptionPositionsUpdateKeys, CurrentStockPositionsFullKeys,
            CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys, ExecutionSide,
            OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys,
            OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys,
            OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys,
            OptionTransactionsUpdateKeys, OptionType, StockTransactionsFullKeys,
            StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys,
        },
        models_crud::{
            current_option_positions::CurrentOptionPositionsCRUD,
            current_stock_positions::CurrentStockPositionsCRUD,
        },
    },
    helpers::contract::get_local_symbol,
    strategy::strategy::{StrategyEnum, StrategyExecutor},
};

// fn parse_exec_id(exec_id: &str) -> (String, Option<u32>) {
//     // Matches things like 5432101.01 or 5432101.02
//     let re = Regex::new(r"^*+\.(\d{2})$").unwrap();
//
//     if let Some(captures) = re.captures(exec_id) {
//         let revision = captures.get(1).unwrap().as_str().parse::<u32>().ok();
//         (exec_id.to_string(), revision)
//     } else {
//         // No dot or not a correction
//         (exec_id.to_string(), None)
//     }
// }

/// Called by on_new_execution event defined in order_events
/// - Performs ALL the necessary DB operations
/// - Updates OpenOrders, if OpenOrder is filled, the entry is deleted
/// - Inserts into Transactions
/// - Updates Position if alr exists, else Inserts Position
/// - NOTE: all crud operations are done asynchronously via tokio::spawn
pub async fn on_new_stock_execution(
    open_stock_orders_crud: CRUD<
        OpenStockOrdersFullKeys,
        OpenStockOrdersPrimaryKeys,
        OpenStockOrdersUpdateKeys,
    >,
    stock_transactions_crud: CRUD<
        StockTransactionsFullKeys,
        StockTransactionsPrimaryKeys,
        StockTransactionsUpdateKeys,
    >,
    current_stock_positions_crud: CRUD<
        CurrentStockPositionsFullKeys,
        CurrentStockPositionsPrimaryKeys,
        CurrentStockPositionsUpdateKeys,
    >,
    specific_current_stock_positions_crud: CurrentStockPositionsCRUD,
    strategy_map: &HashMap<String, StrategyEnum>,
    execution_data: &ExecutionData,
    default_strategy: &str,
) {
    // let (execution_id, revision) = parse_exec_id(&execution_data.execution.execution_id);
    // if revision.is_some() {
    //     return update_stock_execution(
    //         open_stock_orders_crud,
    //         stock_transactions_crud,
    //         current_stock_positions_crud,
    //         specific_current_stock_positions_crud,
    //         execution_data,
    //         execution_id.clone(),
    //     );
    // }
    tracing::info!(
        message=%format!("Execution: Looking for order with order_id {}",
        &execution_data.execution.order_id)
    );
    match open_stock_orders_crud
        .read(&OpenStockOrdersPrimaryKeys {
            order_perm_id: execution_data.execution.perm_id,
            order_id: execution_data.execution.order_id,
        })
        .await
    {
        Ok(open_order_wrapped) => {
            if let Some(mut open_order) = open_order_wrapped {
                // If the execution is a new execution recorded
                if !open_order
                    .executions
                    .contains(&execution_data.execution.execution_id)
                {
                    open_order
                        .executions
                        .push(execution_data.execution.execution_id.to_string());

                    // ===== Update Open Orders =====
                    if open_order.filled
                        != execution_data.execution.cumulative_quantity
                            - execution_data.execution.shares
                    {
                        tracing::error!(
                            message=%format!("New Execution: Cumulative Quantity does not coincide with locally tracked filled quantity (Cumulative: {}, Locally Tracked: {})",
                            execution_data.execution.cumulative_quantity
                                - execution_data.execution.shares,
                            open_order.filled
                            )
                        );
                    }
                    let open_order_prim_keys = OpenStockOrdersPrimaryKeys {
                        order_perm_id: open_order.order_perm_id,
                        order_id: open_order.order_id,
                    };
                    let open_order_update_keys = OpenStockOrdersUpdateKeys {
                        strategy: None,
                        stock: None,
                        primary_exchange: None,
                        currency: None,
                        time: Some(open_order.time),
                        quantity: Some(open_order.quantity),
                        executions: Some(open_order.executions),
                        filled: Some(open_order.filled + execution_data.execution.shares),
                    };
                    if execution_data.execution.cumulative_quantity == open_order.quantity.abs() {
                        tokio::spawn(async move {
                            if let Err(e) =
                                open_stock_orders_crud.delete(&open_order_prim_keys).await
                            {
                                tracing::error!(
                                    "Error occurend while deleting open order in OpenStockOrders: {e:?}"
                                )
                            }
                        });
                    } else {
                        tokio::spawn(async move {
                            if let Err(e) = open_stock_orders_crud
                                .update(&open_order_prim_keys, &open_order_update_keys)
                                .await
                            {
                                tracing::error!(
                                    "Error occured while updating OpenStockOrders: {e:?}"
                                )
                            };
                        });
                    }

                    // ===== Update Transactions =====
                    tracing::info!(message=%format!("execution time is {}", &execution_data.execution.time));
                    let naive_dt = NaiveDateTime::parse_from_str(
                        &execution_data.execution.time,
                        "%Y%m%d  %H:%M:%S",
                    )
                    .expect(&format!(
                        "Failed to parse execution time: {}",
                        &execution_data.execution.time
                    ));

                    let transaction = StockTransactionsFullKeys {
                        strategy: open_order.strategy.clone(),
                        execution_id: execution_data.execution.execution_id.to_string(),
                        order_perm_id: execution_data.execution.perm_id,
                        stock: open_order.stock.clone(),
                        primary_exchange: open_order.primary_exchange.clone(),
                        currency: open_order.currency.clone(),
                        time: Utc
                            .from_local_datetime(&naive_dt)
                            .single()
                            .expect("Ambiguous or invalid datetime in New York timezone")
                            .to_utc(),
                        price: execution_data.execution.price,
                        quantity: if execution_data.execution.side == "BOT" {
                            execution_data.execution.shares
                        } else {
                            -execution_data.execution.shares
                        },
                        fees: dec!(0),
                    };
                    tokio::spawn(async move {
                        if let Err(e) = stock_transactions_crud.create(&transaction).await {
                            tracing::error!(
                                "Error occured while inserting into StockTransactions: {e:?}"
                            )
                        };
                    });

                    // ===== Update Positions =====
                    // Final CRUD operation in alr spawned thread so unnecessary to spawn
                    // another thread
                    let strat_opt = strategy_map.get(&open_order.strategy);
                    if strat_opt.is_none() {
                        tracing::warn!("Missing StrategyEnum: {}", open_order.strategy);
                    }
                    let strat = strat_opt.unwrap();
                    let curr_stock_prim_keys = CurrentStockPositionsPrimaryKeys {
                        // stock: open_order.stock,
                        stock: if strat.is_fx_strategy() {
                            open_order.stock.clone()
                        } else {
                            match open_order.stock.strip_prefix("FX:") {
                                Some(currencies) => {
                                    format!("CASH:{}", currencies.split(":").next().unwrap())
                                }
                                None => open_order.stock.clone(),
                            }
                        },
                        primary_exchange: open_order.primary_exchange.clone(),
                        strategy: open_order.strategy.clone(),
                        currency: open_order.currency.clone(),
                    };
                    match current_stock_positions_crud
                        .read(&curr_stock_prim_keys)
                        .await
                    {
                        Ok(optional_pos) => {
                            if let Some(pos) = optional_pos {
                                #[allow(unused_assignments)]
                                let (mut new_qty, mut new_avg_price) = (0.0, 0.0);
                                // ==== If dir(trade) == Current Position
                                if (matches!(
                                    ExecutionSide::from_str(&execution_data.execution.side,),
                                    ExecutionSide::Bought
                                ) && pos.quantity > 0.0)
                                    || (matches!(
                                        ExecutionSide::from_str(&execution_data.execution.side,),
                                        ExecutionSide::Sold
                                    ) && pos.quantity < 0.0)
                                {
                                    let abs_current_qty = pos.quantity.abs();
                                    new_qty = abs_current_qty + execution_data.execution.shares;
                                    new_avg_price = (abs_current_qty * pos.avg_price
                                        + &execution_data.execution.shares
                                            * &execution_data.execution.price)
                                        / new_qty;
                                    new_qty =
                                        new_qty * (if pos.quantity > 0.0 { 1.0 } else { -1.0 });
                                } else {
                                    if &execution_data.execution.shares > &pos.quantity.abs() {
                                        new_qty =
                                            &execution_data.execution.shares - &pos.quantity.abs();
                                        new_avg_price = execution_data.execution.price;
                                        new_qty =
                                            new_qty * (if pos.quantity > 0.0 { -1.0 } else { 1.0 });
                                    } else {
                                        new_qty =
                                            &pos.quantity.abs() - &execution_data.execution.shares;
                                        new_avg_price = pos.avg_price;
                                        new_qty =
                                            new_qty * (if pos.quantity > 0.0 { 1.0 } else { -1.0 });
                                    }
                                }

                                if let Err(e) = current_stock_positions_crud
                                    .update(
                                        &curr_stock_prim_keys,
                                        &CurrentStockPositionsUpdateKeys {
                                            quantity: Some(new_qty),
                                            avg_price: Some(new_avg_price),
                                            last_updated: None,
                                        },
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        "Error occured while updating CurrentStockPositions: {e:?}"
                                    )
                                }
                            } else {
                                if let Err(e) = current_stock_positions_crud
                                    .create(&CurrentStockPositionsFullKeys {
                                        stock: curr_stock_prim_keys.stock,
                                        primary_exchange: curr_stock_prim_keys.primary_exchange,
                                        currency: curr_stock_prim_keys.currency,
                                        strategy: curr_stock_prim_keys.strategy,
                                        quantity: if matches!(
                                            ExecutionSide::from_str(&execution_data.execution.side),
                                            ExecutionSide::Sold
                                        ) {
                                            -1.0
                                        } else {
                                            1.0
                                        } * execution_data.execution.shares,
                                        avg_price: execution_data.execution.price,
                                        last_updated: Utc::now(),
                                    })
                                    .await
                                {
                                    tracing::error!(
                                        "Error occured while inserting into CurrentStockPositions: {e:?}"
                                    )
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Error occured while reading from CurrentStockPositions: {e:?}"
                            )
                        }
                    }

                    if !strat.is_fx_strategy() {
                        if let Some(currencies) = open_order.stock.strip_prefix("FX:") {
                            if let Err(e) = specific_current_stock_positions_crud
                                .update_strat_positions(
                                    &format!("CASH:{}", currencies.split(":").next().unwrap()),
                                    &open_order.primary_exchange,
                                    &open_order.strategy,
                                    &open_order.currency,
                                    &(execution_data.execution.shares
                                        * match ExecutionSide::from_str(
                                            &execution_data.execution.side,
                                        ) {
                                            ExecutionSide::Bought => 1.0,
                                            ExecutionSide::Sold => -1.0,
                                        }),
                                    Some(execution_data.execution.price),
                                )
                                .await
                            {
                                tracing::error!(
                                    "Error trying to update CASH position in non-fx strategy: {e:?}"
                                )
                            };
                        }
                    }
                }
            } else {
                // Try reconcilliation by assumption of missed open order
                // this_self.try_reconcilliation_assume_missing_order();
                tracing::warn!(
                    "OpenStockOrders does not contain required row - updating to default strategy"
                );
                on_new_stock_execution_no_open_order(
                    stock_transactions_crud,
                    current_stock_positions_crud,
                    specific_current_stock_positions_crud,
                    execution_data,
                    default_strategy,
                );
            }
        }
        Err(e) => {
            tracing::error!("Error occurred when reading open stock orders: {e:?}")
        }
    };
}

/// Called by on_new_execution event defined in order_events
/// - Performs ALL the necessary DB operations
/// - Updates OpenOrders, if OpenOrder is filled, the entry is deleted
/// - Inserts into Transactions
/// - Updates Position if alr exists, else Inserts Position
/// - NOTE: all crud operations are done asynchronously via tokio::spawn
pub async fn on_new_option_execution(
    open_option_orders_crud: CRUD<
        OpenOptionOrdersFullKeys,
        OpenOptionOrdersPrimaryKeys,
        OpenOptionOrdersUpdateKeys,
    >,
    option_transactions_crud: CRUD<
        OptionTransactionsFullKeys,
        OptionTransactionsPrimaryKeys,
        OptionTransactionsUpdateKeys,
    >,
    current_option_positions_crud: CRUD<
        CurrentOptionPositionsFullKeys,
        CurrentOptionPositionsPrimaryKeys,
        CurrentOptionPositionsUpdateKeys,
    >,
    specific_current_option_positions_crud: CurrentOptionPositionsCRUD,
    execution_data: &ExecutionData,
    default_strategy: &str,
) {
    // let (execution_id, revision) = parse_exec_id(&execution_data.execution.execution_id);
    // if revision.is_some() {
    //     return update_option_execution(
    //         open_option_orders_crud,
    //         option_transactions_crud,
    //         current_option_positions_crud,
    //         specific_current_option_positions_crud,
    //         execution_data,
    //         execution_id.clone(),
    //     );
    // }
    match open_option_orders_crud
        .read(&OpenOptionOrdersPrimaryKeys {
            order_perm_id: execution_data.execution.perm_id,
            order_id: execution_data.execution.order_id,
        })
        .await
    {
        Ok(open_order_unwrapped) => {
            if let Some(mut open_order) = open_order_unwrapped {
                // If the execution is a new execution recorded
                if !open_order
                    .executions
                    .contains(&execution_data.execution.execution_id)
                {
                    open_order
                        .executions
                        .push(execution_data.execution.execution_id.to_string());

                    // ===== Update Open Orders =====
                    if open_order.filled
                        != execution_data.execution.cumulative_quantity
                            - execution_data.execution.shares
                    {
                        tracing::error!(
                            message=%format!("New Execution: Cumulative Quantity does not coincide with locally tracked filled quantity (Cumulative: {}, Locally Tracked: {})",
                            execution_data.execution.cumulative_quantity,
                            open_order.filled
                            )
                        );
                    }

                    // ===============================
                    // update open orders
                    // ===============================
                    let open_order_prim_keys = OpenOptionOrdersPrimaryKeys {
                        order_perm_id: open_order.order_perm_id,
                        order_id: open_order.order_id,
                    };
                    if execution_data.execution.cumulative_quantity == open_order.quantity.abs() {
                        tokio::spawn(async move {
                            if let Err(e) =
                                open_option_orders_crud.delete(&open_order_prim_keys).await
                            {
                                tracing::error!(
                                    "Error occurred while deleting open option order in OpenOptionOrders: {e:?}"
                                )
                            };
                        });
                    } else {
                        let open_order_update_keys = OpenOptionOrdersUpdateKeys {
                            strategy: None,
                            stock: None,
                            primary_exchange: None,
                            currency: None,
                            expiry: None,
                            strike: None,
                            multiplier: None,
                            option_type: None,
                            time: Some(open_order.time),
                            quantity: Some(open_order.quantity),
                            executions: Some(open_order.executions),
                            filled: Some(open_order.filled + execution_data.execution.shares),
                        };
                        tokio::spawn(async move {
                            if let Err(e) = open_option_orders_crud
                                .update(&open_order_prim_keys, &open_order_update_keys)
                                .await
                            {
                                tracing::error!(
                                    "Error occured while updating OpenOptionOrders: {e:?}"
                                )
                            };
                        });
                    }
                    // ===============================
                    // update open orders
                    // ===============================

                    // ===== Update Transactions =====
                    tracing::info!(message=%format!("execution time is {}", &execution_data.execution.time));
                    let naive_dt = NaiveDateTime::parse_from_str(
                        &execution_data.execution.time,
                        "%Y%m%d  %H:%M:%S",
                    )
                    .expect(&format!(
                        "Failed to parse execution time: {}",
                        &execution_data.execution.time
                    ));

                    let transaction = OptionTransactionsFullKeys {
                        strategy: open_order.strategy.clone(),
                        execution_id: execution_data.execution.execution_id.to_string(),
                        order_perm_id: execution_data.execution.perm_id,
                        stock: open_order.stock.clone(),
                        primary_exchange: open_order.primary_exchange.clone(),
                        currency: open_order.currency.clone(),
                        expiry: open_order.expiry.clone(),
                        strike: open_order.strike.clone(),
                        multiplier: open_order.multiplier.clone(),
                        option_type: open_order.option_type.clone(),
                        time: Utc
                            .from_local_datetime(&naive_dt)
                            .single()
                            .expect("Ambiguous or invalid datetime in New York timezone")
                            .to_utc(),
                        price: execution_data.execution.price.clone(),
                        quantity: if execution_data.execution.side == "BOT" {
                            execution_data.execution.shares.clone()
                        } else {
                            -execution_data.execution.shares.clone()
                        },
                        fees: dec!(0),
                    };
                    tokio::spawn(async move {
                        if let Err(e) = option_transactions_crud.create(&transaction).await {
                            tracing::error!(
                                "Error occured while inserting into OptionTransactions: {e:?}"
                            )
                        };
                    });
                    // ===== Update Transactions =====

                    // ===== Update Positions =====
                    let curr_pos_prim_key = CurrentOptionPositionsPrimaryKeys {
                        stock: open_order.stock,
                        primary_exchange: open_order.primary_exchange,
                        currency: open_order.currency,
                        strategy: open_order.strategy,
                        expiry: open_order.expiry,
                        strike: open_order.strike,
                        multiplier: open_order.multiplier,
                        option_type: open_order.option_type,
                    };
                    match current_option_positions_crud.read(&curr_pos_prim_key).await {
                        Ok(optional_pos) => {
                            if let Some(pos) = optional_pos {
                                #[allow(unused_assignments)]
                                let (mut new_qty, mut new_avg_price) = (0.0, 0.0);
                                // ==== If dir(trade) == Current Position
                                if (matches!(
                                    ExecutionSide::from_str(&execution_data.execution.side,),
                                    ExecutionSide::Bought
                                ) && pos.quantity > 0.0)
                                    || (matches!(
                                        ExecutionSide::from_str(&execution_data.execution.side,),
                                        ExecutionSide::Sold
                                    ) && pos.quantity < 0.0)
                                {
                                    let abs_current_qty = pos.quantity.abs();
                                    new_qty = abs_current_qty + execution_data.execution.shares;
                                    new_avg_price = (abs_current_qty * pos.avg_price
                                        + &execution_data.execution.shares
                                            * &execution_data.execution.price)
                                        / new_qty;
                                } else {
                                    if &execution_data.execution.shares > &pos.quantity.abs() {
                                        new_qty =
                                            &execution_data.execution.shares - &pos.quantity.abs();
                                        new_avg_price = execution_data.execution.price;
                                    } else {
                                        new_qty =
                                            &pos.quantity.abs() - &execution_data.execution.shares;
                                        new_avg_price = pos.avg_price;
                                    }
                                }

                                if let Err(e) = current_option_positions_crud
                                    .update(
                                        &curr_pos_prim_key,
                                        &CurrentOptionPositionsUpdateKeys {
                                            quantity: Some(new_qty),
                                            avg_price: Some(new_avg_price),
                                            last_updated: None,
                                        },
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        "Error occured while updating CurrentOptionPositions: {e:?}"
                                    )
                                }
                            } else {
                                if let Err(e) = current_option_positions_crud
                                    .create(&&CurrentOptionPositionsFullKeys {
                                        stock: curr_pos_prim_key.stock,
                                        primary_exchange: curr_pos_prim_key.primary_exchange,
                                        currency: curr_pos_prim_key.currency,
                                        strategy: curr_pos_prim_key.strategy,
                                        expiry: curr_pos_prim_key.expiry,
                                        strike: curr_pos_prim_key.strike,
                                        multiplier: curr_pos_prim_key.multiplier,
                                        option_type: curr_pos_prim_key.option_type,
                                        quantity: if execution_data.execution.side == "BOT" {
                                            execution_data.execution.shares
                                        } else {
                                            -execution_data.execution.shares
                                        },
                                        avg_price: execution_data.execution.price,
                                        last_updated: Utc::now(),
                                    })
                                    .await
                                {
                                    tracing::error!(
                                        "Error occured while inserting into CurrentOptionPositions: {e:?}"
                                    )
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Error occured while reading from CurrentOptionPositions: {e:?}"
                            )
                        }
                    }
                }
            } else {
                // Try reconcilliation by assumption of missed open order
                on_new_option_execution_no_open_order(
                    option_transactions_crud,
                    current_option_positions_crud,
                    specific_current_option_positions_crud,
                    execution_data,
                    default_strategy,
                );
                tracing::error!("OpenOptionOrders does not contain required row!");
            }
        }
        Err(e) => {
            tracing::error!("Error occurred when reading open option orders: {e:?}")
        }
    };
}

/// No open order -> Execution event comes in
/// Assumption: Our server measures everything properly
/// - Dumps the unknown execution event to "unknown" strategy
/// - "unknown" strategy should ideally be set up in the beginning and be subscribed to a timestep
/// set by the user (up to the max timestep the user wants before "unknown" should try to offload
/// the position via Market Orders)
pub fn on_new_stock_execution_no_open_order(
    stock_transactions_crud: CRUD<
        StockTransactionsFullKeys,
        StockTransactionsPrimaryKeys,
        StockTransactionsUpdateKeys,
    >,
    _current_stock_positions_crud: CRUD<
        CurrentStockPositionsFullKeys,
        CurrentStockPositionsPrimaryKeys,
        CurrentStockPositionsUpdateKeys,
    >,
    specific_current_stock_positions_crud: CurrentStockPositionsCRUD,
    execution_data: &ExecutionData,
    default_strategy: &str,
) {
    let naive_dt =
        NaiveDateTime::parse_from_str(&execution_data.execution.time, "%Y%m%d  %H:%M:%S").expect(
            &format!(
                "Failed to parse execution time: {}",
                &execution_data.execution.time
            ),
        );
    let symbol = get_local_symbol(&execution_data.contract);
    let transaction = StockTransactionsFullKeys {
        strategy: default_strategy.to_string(),
        execution_id: execution_data.execution.execution_id.to_string(),
        order_perm_id: execution_data.execution.perm_id,
        stock: symbol.clone(),
        primary_exchange: execution_data.contract.primary_exchange.to_string(),
        currency: execution_data.contract.currency.to_string(),
        time: Utc
            .from_local_datetime(&naive_dt)
            .single()
            .expect("Ambiguous or invalid datetime in New York timezone")
            .to_utc(),
        price: execution_data.execution.average_price,
        quantity: if execution_data.execution.side == "BOT" {
            execution_data.execution.shares
        } else {
            -execution_data.execution.shares
        },
        fees: dec!(0),
    };
    tokio::spawn(async move {
        if let Err(e) = stock_transactions_crud.create(&transaction).await {
            tracing::error!("Error inserting into StockTransactions for unknown strategy: {e:?}")
        };
    });

    let prim_exch = execution_data.contract.primary_exchange.to_string();
    let currency = execution_data.contract.currency.to_string();
    let exec_shares = execution_data.execution.shares;
    let exec_price = execution_data.execution.price;
    let default_strategy = default_strategy.to_string();
    tokio::spawn(async move {
        if let Err(e) = specific_current_stock_positions_crud
            .update_strat_positions(
                symbol.as_str(),
                &prim_exch,
                &currency,
                &default_strategy,
                &exec_shares,
                Some(exec_price),
            )
            .await
        {
            tracing::error!("Error updating CurrentStockPositions with unknown strategy: {e:?}")
        };
    });
}

/// No open order -> Execution event comes in
/// Assumption: Our server measures everything properly
/// - Dumps the unknown execution event to "unknown" strategy
/// - "unknown" strategy should ideally be set up in the beginning and be subscribed to a timestep
/// set by the user (up to the max timestep the user wants before "unknown" should try to offload
/// the position via Market Orders)
pub fn on_new_option_execution_no_open_order(
    option_transactions_crud: CRUD<
        OptionTransactionsFullKeys,
        OptionTransactionsPrimaryKeys,
        OptionTransactionsUpdateKeys,
    >,
    _current_option_positions_crud: CRUD<
        CurrentOptionPositionsFullKeys,
        CurrentOptionPositionsPrimaryKeys,
        CurrentOptionPositionsUpdateKeys,
    >,
    specific_current_option_positions_crud: CurrentOptionPositionsCRUD,
    execution_data: &ExecutionData,
    default_strategy: &str,
) {
    let naive_dt =
        NaiveDateTime::parse_from_str(&execution_data.execution.time, "%Y%m%d  %H:%M:%S").expect(
            &format!(
                "Failed to parse execution time: {}",
                &execution_data.execution.time
            ),
        );
    let execution_time = Utc
        .from_local_datetime(&naive_dt)
        .single()
        .expect("Ambiguous or invalid datetime in New York timezone");
    let transaction = OptionTransactionsFullKeys {
        strategy: default_strategy.to_string(),
        execution_id: execution_data.execution.execution_id.to_string(),
        order_perm_id: execution_data.execution.perm_id,
        stock: execution_data.contract.symbol.as_str().to_string(),
        primary_exchange: execution_data.contract.primary_exchange.to_string(),
        currency: execution_data.contract.currency.to_string(),
        expiry: execution_data
            .contract
            .last_trade_date_or_contract_month
            .clone(),
        strike: execution_data.contract.strike.clone(),
        multiplier: execution_data.contract.multiplier.clone(),
        option_type: OptionType::from_str(&execution_data.contract.right)
            .expect("Error parsing OptionType from contract right in update_option_execution"),
        time: execution_time.to_utc(),

        price: execution_data.execution.average_price,
        quantity: if execution_data.execution.side == "BOT" {
            execution_data.execution.shares.clone()
        } else {
            -execution_data.execution.shares.clone()
        },
        fees: dec!(0),
    };
    tokio::spawn(async move {
        if let Err(e) = option_transactions_crud.create(&transaction).await {
            tracing::error!("Error inserting into OptionTransactions for unknown strategy: {e:?}");
        };
    });
    let symbol = execution_data.contract.symbol.as_str().to_string();
    let prim_exch = execution_data.contract.primary_exchange.to_string();
    let currency = execution_data.contract.currency.to_string();
    let expiry = execution_data
        .contract
        .last_trade_date_or_contract_month
        .to_string();
    let strike = execution_data.contract.strike;
    let mult = execution_data.contract.multiplier.to_string();
    let opt_type = OptionType::from_str(&execution_data.contract.right)
        .expect("Error parsing OptionType from contract right in update_option_execution");
    let exec_shares = execution_data.execution.shares;
    let default_strategy = default_strategy.to_string();
    tokio::spawn(async move {
        if let Err(e) = specific_current_option_positions_crud
            .update_strat_positions(
                symbol.as_str(),
                prim_exch.as_str(),
                currency.as_str(),
                expiry.as_str(),
                &strike,
                mult.as_str(),
                &opt_type,
                &default_strategy,
                &exec_shares,
            )
            .await
        {
            tracing::error!(
                "Error inserting into CurrentOptionPositions for unknown strategy: {e:?}"
            )
        };
    });
}

// pub fn update_stock_execution(
//     open_stock_orders_crud: CRUD<
//         OpenStockOrdersFullKeys,
//         OpenStockOrdersPrimaryKeys,
//         OpenStockOrdersUpdateKeys,
//     >,
//     stock_transactions_crud: CRUD<
//         StockTransactionsFullKeys,
//         StockTransactionsPrimaryKeys,
//         StockTransactionsUpdateKeys,
//     >,
//     current_stock_positions_crud: CRUD<
//         CurrentStockPositionsFullKeys,
//         CurrentStockPositionsPrimaryKeys,
//         CurrentStockPositionsUpdateKeys,
//     >,
//     specific_current_stock_positions_crud: CurrentStockPositionsCRUD,
//     execution_data: ExecutionData,
//     execution_id: String,
// ) {
// }
//
// pub fn update_option_execution(
//     open_option_orders_crud: CRUD<
//         OpenOptionOrdersFullKeys,
//         OpenOptionOrdersPrimaryKeys,
//         OpenOptionOrdersUpdateKeys,
//     >,
//     option_transactions_crud: CRUD<
//         OptionTransactionsFullKeys,
//         OptionTransactionsPrimaryKeys,
//         OptionTransactionsUpdateKeys,
//     >,
//     current_option_positions_crud: CRUD<
//         CurrentOptionPositionsFullKeys,
//         CurrentOptionPositionsPrimaryKeys,
//         CurrentOptionPositionsUpdateKeys,
//     >,
//     specific_current_option_positions_crud: CurrentOptionPositionsCRUD,
//     execution_data: ExecutionData,
//     execution_id: String,
// ) {
// }
