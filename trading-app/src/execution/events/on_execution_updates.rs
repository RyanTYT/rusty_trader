// Issues: Need to ensure parsing of datetime from execution_data.execution.time is correct - may
// need to parse with timezone as i suspect
// Lines 108, 324: DateTime Parsing

use chrono::{NaiveDateTime, TimeZone, Utc};
use ibapi::orders::ExecutionData;
use rust_decimal::dec;
use tracing::info;

use crate::database::{
    crud::{CRUD, CRUDTrait},
    models::{
        CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys,
        CurrentOptionPositionsUpdateKeys, CurrentStockPositionsFullKeys,
        CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys, ExecutionSide,
        OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys,
        OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys,
        OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys, OptionTransactionsUpdateKeys,
        OptionType, StockTransactionsFullKeys, StockTransactionsPrimaryKeys,
        StockTransactionsUpdateKeys,
    },
    models_crud::{
        current_option_positions::CurrentOptionPositionsCRUD,
        current_stock_positions::CurrentStockPositionsCRUD,
    },
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
pub fn on_new_stock_execution(
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
    execution_data: ExecutionData,
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
    tokio::spawn(async move {
        info!(
            "Execution: Looking for order with order_id {}",
            &execution_data.execution.order_id
        );
        match open_stock_orders_crud
            .read(&OpenStockOrdersPrimaryKeys {
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
                            .push(execution_data.execution.execution_id.clone());

                        // ===== Update Open Orders =====
                        if open_order.filled
                            != execution_data.execution.cumulative_quantity
                                - execution_data.execution.shares
                        {
                            tracing::error!(
                                "New Execution: Cumulative Quantity does not coincide with locally tracked filled quantity (Cumulative: {}, Locally Tracked: {})",
                                execution_data.execution.cumulative_quantity
                                    - execution_data.execution.shares,
                                open_order.filled
                            );
                        }
                        let cloned_execution_data = execution_data.clone();
                        let cloned_open_order = open_order.clone();
                        tokio::spawn(async move {
                            if &cloned_execution_data.execution.cumulative_quantity
                                == &cloned_open_order.quantity.abs()
                            {
                                if let Err(e) = open_stock_orders_crud
                                    .delete(&OpenStockOrdersPrimaryKeys {
                                        order_perm_id: cloned_open_order.order_perm_id,
                                        order_id: cloned_open_order.order_id,
                                    })
                                    .await
                                {
                                    tracing::error!(
                                        "Error occurend while deleting open order in OpenStockOrders: {}",
                                        e
                                    )
                                }
                            } else {
                                if let Err(e) = open_stock_orders_crud
                                    .update(
                                        &OpenStockOrdersPrimaryKeys {
                                            order_perm_id: cloned_open_order.order_perm_id,
                                            order_id: cloned_open_order.order_id,
                                        },
                                        &OpenStockOrdersUpdateKeys {
                                            strategy: Some(cloned_open_order.strategy.clone()),
                                            stock: Some(cloned_open_order.stock.clone()),
                                            primary_exchange: Some(
                                                cloned_open_order.primary_exchange.clone(),
                                            ),
                                            time: Some(cloned_open_order.time.clone()),
                                            quantity: Some(cloned_open_order.quantity.clone()),
                                            executions: Some(cloned_open_order.executions.clone()),
                                            filled: Some(
                                                cloned_open_order.filled.clone()
                                                    + &cloned_execution_data.execution.shares,
                                            ),
                                        },
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        "Error occured while updating OpenStockOrders: {}",
                                        e
                                    )
                                };
                            }
                        });

                        // ===== Update Transactions =====
                        tracing::info!("execution time is {}", &execution_data.execution.time);
                        let naive_dt = NaiveDateTime::parse_from_str(
                            &execution_data.execution.time,
                            "%Y%m%d  %H:%M:%S",
                        )
                        .expect(&format!(
                            "Failed to parse execution time: {}",
                            &execution_data.execution.time
                        ));
                        let execution_time = Utc
                            .from_local_datetime(&naive_dt)
                            .single()
                            .expect("Ambiguous or invalid datetime in New York timezone");

                        let cloned_open_order = open_order.clone();
                        let cloned_execution_data = execution_data.clone();
                        tokio::spawn(async move {
                            if let Err(e) = stock_transactions_crud
                                .create(&StockTransactionsFullKeys {
                                    strategy: cloned_open_order.strategy.clone(),
                                    execution_id: cloned_execution_data.execution.execution_id,
                                    order_perm_id: cloned_execution_data.execution.perm_id,
                                    stock: cloned_open_order.stock.clone(),
                                    primary_exchange: cloned_open_order.primary_exchange.clone(),
                                    time: execution_time.with_timezone(&Utc),
                                    price: cloned_execution_data.execution.price.clone(),
                                    quantity: if cloned_execution_data.execution.side == "BOT" {
                                        cloned_execution_data.execution.shares.clone()
                                    } else {
                                        -cloned_execution_data.execution.shares.clone()
                                    },
                                    fees: dec!(0),
                                })
                                .await
                            {
                                tracing::error!(
                                    "Error occured while inserting into StockTransactions: {}",
                                    e
                                )
                            };
                        });

                        // ===== Update Positions =====
                        // Final CRUD operation in alr spawned thread so unnecessary to spawn
                        // another thread
                        match current_stock_positions_crud
                            .read(&CurrentStockPositionsPrimaryKeys {
                                stock: open_order.stock.clone(),
                                primary_exchange: open_order.primary_exchange.clone(),
                                strategy: open_order.strategy.clone(),
                            })
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
                                    } else {
                                        if &execution_data.execution.shares > &pos.quantity.abs() {
                                            new_qty = &execution_data.execution.shares
                                                - &pos.quantity.abs();
                                            new_avg_price = execution_data.execution.price.clone();
                                        } else {
                                            new_qty = &pos.quantity.abs()
                                                - &execution_data.execution.shares;
                                            new_avg_price = pos.avg_price.clone();
                                        }
                                    }

                                    if let Err(e) = current_stock_positions_crud
                                        .update(
                                            &CurrentStockPositionsPrimaryKeys {
                                                stock: open_order.stock,
                                                primary_exchange: open_order
                                                    .primary_exchange
                                                    .clone(),
                                                strategy: open_order.strategy,
                                            },
                                            &CurrentStockPositionsUpdateKeys {
                                                quantity: Some(new_qty),
                                                avg_price: Some(new_avg_price),
                                            },
                                        )
                                        .await
                                    {
                                        tracing::error!(
                                            "Error occured while updating CurrentStockPositions: {}",
                                            e
                                        )
                                    }
                                } else {
                                    if let Err(e) = current_stock_positions_crud
                                        .create(&CurrentStockPositionsFullKeys {
                                            stock: open_order.stock,
                                            primary_exchange: open_order.primary_exchange.clone(),
                                            strategy: open_order.strategy,
                                            quantity: execution_data.execution.shares,
                                            avg_price: execution_data.execution.price,
                                        })
                                        .await
                                    {
                                        tracing::error!(
                                            "Error occured while inserting into CurrentStockPositions: {}",
                                            e
                                        )
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Error occured while reading from CurrentStockPositions: {}",
                                    e
                                )
                            }
                        }
                    }
                } else {
                    // Try reconcilliation by assumption of missed open order
                    // this_self.try_reconcilliation_assume_missing_order();
                    on_new_stock_execution_no_open_order(
                        stock_transactions_crud,
                        current_stock_positions_crud,
                        specific_current_stock_positions_crud,
                        execution_data,
                    );
                    tracing::error!("OpenStockOrders does not contain required row!");
                }
            }
            Err(e) => {
                tracing::error!("Error occurred when reading open stock orders: {}", e)
            }
        };
    });
}

/// Called by on_new_execution event defined in order_events
/// - Performs ALL the necessary DB operations
/// - Updates OpenOrders, if OpenOrder is filled, the entry is deleted
/// - Inserts into Transactions
/// - Updates Position if alr exists, else Inserts Position
/// - NOTE: all crud operations are done asynchronously via tokio::spawn
pub fn on_new_option_execution(
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
    execution_data: ExecutionData,
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
    tokio::spawn(async move {
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
                            .push(execution_data.execution.execution_id.clone());

                        // ===== Update Open Orders =====
                        if open_order.filled
                            != execution_data.execution.cumulative_quantity
                                - execution_data.execution.shares
                        {
                            tracing::error!(
                                "New Execution: Cumulative Quantity does not coincide with locally tracked filled quantity (Cumulative: {}, Locally Tracked: {})",
                                execution_data.execution.cumulative_quantity,
                                open_order.filled
                            );
                        }

                        let cloned_execution_data = execution_data.clone();
                        let cloned_open_order = open_order.clone();
                        tokio::spawn(async move {
                            if &cloned_execution_data.execution.cumulative_quantity
                                == &cloned_open_order.quantity.abs()
                            {
                                if let Err(e) = open_option_orders_crud
                                    .delete(&OpenOptionOrdersPrimaryKeys {
                                        order_perm_id: cloned_open_order.order_perm_id,
                                        order_id: cloned_open_order.order_id,
                                    })
                                    .await
                                {
                                    tracing::error!(
                                        "Error occurred while deleting open option order in OpenOptionOrders: {}",
                                        e
                                    )
                                };
                            } else {
                                if let Err(e) = open_option_orders_crud
                                    .update(
                                        &OpenOptionOrdersPrimaryKeys {
                                            order_perm_id: cloned_open_order.order_perm_id,
                                            order_id: cloned_open_order.order_id,
                                        },
                                        &OpenOptionOrdersUpdateKeys {
                                            strategy: None,
                                            stock: None,
                                            primary_exchange: None,
                                            expiry: None,
                                            strike: None,
                                            multiplier: None,
                                            option_type: None,
                                            time: Some(cloned_open_order.time.clone()),
                                            quantity: Some(cloned_open_order.quantity.clone()),
                                            executions: Some(cloned_open_order.executions.clone()),
                                            filled: Some(
                                                cloned_open_order.filled.clone()
                                                    + &cloned_execution_data.execution.shares,
                                            ),
                                        },
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        "Error occured while updating OpenOptionOrders: {}",
                                        e
                                    )
                                };
                            }
                        });

                        // ===== Update Transactions =====
                        tracing::info!("execution time is {}", &execution_data.execution.time);
                        let naive_dt = NaiveDateTime::parse_from_str(
                            &execution_data.execution.time,
                            "%Y%m%d  %H:%M:%S",
                        )
                        .expect(&format!(
                            "Failed to parse execution time: {}",
                            &execution_data.execution.time
                        ));
                        let execution_time = Utc
                            .from_local_datetime(&naive_dt)
                            .single()
                            .expect("Ambiguous or invalid datetime in New York timezone");

                        let cloned_open_order = open_order.clone();
                        let cloned_execution_data = execution_data.clone();
                        tokio::spawn(async move {
                            if let Err(e) = option_transactions_crud
                                .create(&OptionTransactionsFullKeys {
                                    strategy: cloned_open_order.strategy.clone(),
                                    execution_id: cloned_execution_data.execution.execution_id,
                                    order_perm_id: cloned_execution_data.execution.perm_id,
                                    stock: cloned_open_order.stock.clone(),
                                    primary_exchange: cloned_open_order.primary_exchange.clone(),
                                    expiry: cloned_open_order.expiry.clone(),
                                    strike: cloned_open_order.strike.clone(),
                                    multiplier: cloned_open_order.multiplier.clone(),
                                    option_type: cloned_open_order.option_type.clone(),
                                    time: execution_time.with_timezone(&Utc),
                                    price: cloned_execution_data.execution.price.clone(),
                                    quantity: if cloned_execution_data.execution.side == "BOT" {
                                        cloned_execution_data.execution.shares.clone()
                                    } else {
                                        -cloned_execution_data.execution.shares.clone()
                                    },
                                    fees: dec!(0),
                                })
                                .await
                            {
                                tracing::error!(
                                    "Error occured while inserting into OptionTransactions: {}",
                                    e
                                )
                            };
                        });

                        // ===== Update Positions =====
                        match current_option_positions_crud
                            .read(&CurrentOptionPositionsPrimaryKeys {
                                stock: open_order.stock.clone(),
                                primary_exchange: open_order.primary_exchange.clone(),
                                strategy: open_order.strategy.clone(),
                                expiry: open_order.expiry.clone(),
                                strike: open_order.strike.clone(),
                                multiplier: open_order.multiplier.clone(),
                                option_type: open_order.option_type.clone(),
                            })
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
                                    } else {
                                        if &execution_data.execution.shares > &pos.quantity.abs() {
                                            new_qty = &execution_data.execution.shares
                                                - &pos.quantity.abs();
                                            new_avg_price = execution_data.execution.price.clone();
                                        } else {
                                            new_qty = &pos.quantity.abs()
                                                - &execution_data.execution.shares;
                                            new_avg_price = pos.avg_price.clone();
                                        }
                                    }

                                    if let Err(e) = current_option_positions_crud
                                        .update(
                                            &&CurrentOptionPositionsPrimaryKeys {
                                                stock: open_order.stock.clone(),
                                                primary_exchange: open_order
                                                    .primary_exchange
                                                    .clone(),
                                                strategy: open_order.strategy.clone(),
                                                expiry: open_order.expiry.clone(),
                                                strike: open_order.strike.clone(),
                                                multiplier: open_order.multiplier.clone(),
                                                option_type: open_order.option_type.clone(),
                                            },
                                            &CurrentOptionPositionsUpdateKeys {
                                                quantity: Some(new_qty),
                                                avg_price: Some(new_avg_price),
                                            },
                                        )
                                        .await
                                    {
                                        tracing::error!(
                                            "Error occured while updating CurrentOptionPositions: {}",
                                            e
                                        )
                                    }
                                } else {
                                    if let Err(e) = current_option_positions_crud
                                        .create(&&CurrentOptionPositionsFullKeys {
                                            stock: open_order.stock,
                                            primary_exchange: open_order.primary_exchange,
                                            strategy: open_order.strategy,
                                            expiry: open_order.expiry,
                                            strike: open_order.strike,
                                            multiplier: open_order.multiplier,
                                            option_type: open_order.option_type,
                                            quantity: if execution_data.execution.side == "BOT" {
                                                execution_data.execution.shares.clone()
                                            } else {
                                                -execution_data.execution.shares.clone()
                                            },
                                            avg_price: execution_data.execution.price,
                                        })
                                        .await
                                    {
                                        tracing::error!(
                                            "Error occured while inserting into CurrentOptionPositions: {}",
                                            e
                                        )
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Error occured while reading from CurrentOptionPositions: {}",
                                    e
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
                    );
                    tracing::error!("OpenOptionOrders does not contain required row!");
                }
            }
            Err(e) => {
                tracing::error!("Error occurred when reading open option orders: {}", e)
            }
        };
    });
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
    execution_data: ExecutionData,
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
    let cloned_execution_data = execution_data.clone();
    tokio::spawn(async move {
        if let Err(e) = stock_transactions_crud
            .create(&StockTransactionsFullKeys {
                strategy: "unknown".to_string(),
                execution_id: cloned_execution_data.execution.execution_id,
                order_perm_id: cloned_execution_data.execution.perm_id,
                stock: cloned_execution_data.contract.symbol.clone(),
                primary_exchange: cloned_execution_data.contract.primary_exchange,
                time: execution_time.to_utc(),

                price: cloned_execution_data.execution.average_price,
                quantity: if cloned_execution_data.execution.side == "BOT" {
                    cloned_execution_data.execution.shares.clone()
                } else {
                    -cloned_execution_data.execution.shares.clone()
                },
                fees: dec!(0),
            })
            .await
        {
            tracing::error!(
                "Error inserting into StockTransactions for unknown strategy: {}",
                e
            )
        };
    });
    let cloned_execution_data = execution_data.clone();
    tokio::spawn(async move {
        if let Err(e) = specific_current_stock_positions_crud
            .update_unknown_strat_positions(
                cloned_execution_data.contract.symbol,
                cloned_execution_data.execution.shares,
            )
            .await
        {
            tracing::error!(
                "Error updating CurrentStockPositions with unknown strategy: {}",
                e
            )
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
    execution_data: ExecutionData,
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
    let cloned_execution_data = execution_data.clone();
    tokio::spawn(async move {
        if let Err(e) = option_transactions_crud
            .create(&OptionTransactionsFullKeys {
                strategy: "unknown".to_string(),
                execution_id: cloned_execution_data.execution.execution_id,
                order_perm_id: cloned_execution_data.execution.perm_id,
                stock: cloned_execution_data.contract.symbol.clone(),
                primary_exchange: cloned_execution_data.contract.primary_exchange.clone(),
                expiry: cloned_execution_data
                    .contract
                    .last_trade_date_or_contract_month
                    .clone(),
                strike: cloned_execution_data.contract.strike.clone(),
                multiplier: cloned_execution_data.contract.multiplier.clone(),
                option_type: OptionType::from_str(&cloned_execution_data.contract.right).expect(
                    "Error parsing OptionType from contract right in update_option_execution",
                ),
                time: execution_time.to_utc(),

                price: cloned_execution_data.execution.average_price,
                quantity: if cloned_execution_data.execution.side == "BOT" {
                    cloned_execution_data.execution.shares.clone()
                } else {
                    -cloned_execution_data.execution.shares.clone()
                },
                fees: dec!(0),
            })
            .await
        {
            tracing::error!(
                "Error inserting into OptionTransactions for unknown strategy: {}",
                e
            );
        };
    });
    let cloned_execution_data = execution_data.clone();
    tokio::spawn(async move {
        if let Err(e) = specific_current_option_positions_crud
            .update_unknown_strat_positions(
                cloned_execution_data.contract.symbol,
                cloned_execution_data.contract.primary_exchange,
                cloned_execution_data
                    .contract
                    .last_trade_date_or_contract_month
                    .clone(),
                cloned_execution_data.contract.strike.clone(),
                cloned_execution_data.contract.multiplier.clone(),
                OptionType::from_str(&cloned_execution_data.contract.right).expect(
                    "Error parsing OptionType from contract right in update_option_execution",
                ),
                cloned_execution_data.execution.shares,
            )
            .await
        {
            tracing::error!(
                "Error inserting into CurrentOptionPositions for unknown strategy: {}",
                e
            )
        };
    });
}

pub fn update_stock_execution(
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
    execution_data: ExecutionData,
    execution_id: String,
) {
}

pub fn update_option_execution(
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
    execution_data: ExecutionData,
    execution_id: String,
) {
}
