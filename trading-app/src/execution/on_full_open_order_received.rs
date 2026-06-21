use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use chrono::Utc;
use ibapi::{
    orders::{Order, OrderStatus},
    prelude::Contract,
};
use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUDTrait,
        models::{
            AssetType, OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys,
            OpenOptionOrdersUpdateKeys, OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys,
            OpenStockOrdersUpdateKeys, OptionType,
        },
        models_crud::{
            open_option_orders::get_open_option_orders_crud,
            open_stock_orders::get_open_stock_orders_crud,
        },
    },
    helpers::contract::{HashContract, get_local_symbol},
    market_data::consolidator::Consolidator,
};

// ONLY called in conjunction with sync_open_orders
pub fn on_full_open_order_received(
    contract_to_strategy: HashMap<HashContract, String>,
    order_map: Arc<RwLock<HashMap<i32, (String, Contract, Order)>>>,
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

    // 3. Update the relevant associated strategies, contracts and orders in order_map so
    //    order_update_stream is not bamboozled
    {
        let mut unlocked_order_map = order_map
            .write()
            .expect("Expected to be able to acquire lock from order_map while syncing open orders");
        unlocked_order_map.insert(
            borrowed_order.order_id,
            (strategy.clone(), contract.clone(), borrowed_order.clone()),
        );
    }

    // 4. Begin a detached thread to update the DB accordingly
    let order = borrowed_order.clone();
    let order_status_filled = order_status.filled;
    tokio::spawn(async move {
        // update order_map first
        match AssetType::from_str(&contract.security_type) {
            AssetType::Stock | AssetType::Future | AssetType::ForexPair | AssetType::CFD => {
                let open_stock_orders_crud = get_open_stock_orders_crud(pool);

                match open_stock_orders_crud
                    .read(&OpenStockOrdersPrimaryKeys {
                        order_perm_id: order.perm_id,
                        order_id: order.order_id,
                    })
                    .await
                {
                    Ok(open_stock_orders_row_opt) => {
                        if let Some(open_stock_orders_row) = open_stock_orders_row_opt {
                            // Update open_order
                            if open_stock_orders_row.filled != order_status_filled {
                                if let Err(e) = open_stock_orders_crud
                                    .update(
                                        &OpenStockOrdersPrimaryKeys {
                                            order_perm_id: order.perm_id,
                                            order_id: order.order_id,
                                        },
                                        &OpenStockOrdersUpdateKeys {
                                            strategy: None,
                                            stock: None,
                                            primary_exchange: None,
                                            currency: None,
                                            time: None,
                                            quantity: None,
                                            executions: None,
                                            filled: Some(order_status_filled),
                                        },
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        "Error when trying to update OpenStockOrders for order_id {}: {}",
                                        order.perm_id,
                                        e
                                    );
                                }
                            }
                        } else {
                            if let Err(e) = open_stock_orders_crud
                                .create(&OpenStockOrdersFullKeys {
                                    order_perm_id: order.perm_id,
                                    order_id: order.order_id,
                                    strategy: strategy,
                                    stock: symbol,
                                    primary_exchange: contract.primary_exchange.to_string(),
                                    currency: contract.currency.to_string(),
                                    time: Utc::now(),
                                    quantity: order.total_quantity,
                                    executions: Vec::new(),
                                    filled: order.filled_quantity,
                                })
                                .await
                            {
                                tracing::error!(
                                        message=%format!(
                                            "Error when trying to insert unmatched OpenStockOrders for order_id {}: {}",
                                            order.perm_id,
                                            e
                                        )
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "Error when trying to read OpenStockOrders in on_full_open_order_received for sync_open_orders: {e:?}"
                        )
                    }
                }
            }
            AssetType::Option => {
                let open_option_orders_crud = get_open_option_orders_crud(pool);

                match open_option_orders_crud
                    .read(&OpenOptionOrdersPrimaryKeys {
                        order_perm_id: order.perm_id,
                        order_id: order.order_id,
                    })
                    .await
                {
                    Ok(open_option_order_opt) => {
                        if let Some(open_option_order_row) = open_option_order_opt {
                            // Update open_order
                            if open_option_order_row.filled != order_status_filled {
                                if let Err(e) = open_option_orders_crud
                                    .update(
                                        &OpenOptionOrdersPrimaryKeys {
                                            order_perm_id: order.perm_id,
                                            order_id: order.order_id,
                                        },
                                        &OpenOptionOrdersUpdateKeys {
                                            strategy: None,
                                            stock: None,
                                            primary_exchange: None,
                                            currency: None,
                                            expiry: None,
                                            strike: None,
                                            multiplier: None,
                                            option_type: None,
                                            time: None,
                                            quantity: None,
                                            executions: None,
                                            filled: Some(order_status_filled),
                                        },
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        message=%format!(
                                            "Error when trying to update OpenOptionOrders for order_id {}: {}",
                                            order.perm_id,
                                            e
                                        )
                                    );
                                }
                            }
                        } else {
                            if let Err(e) = open_option_orders_crud
                                    .create(&OpenOptionOrdersFullKeys {
                                        order_perm_id: order.perm_id,
                                        order_id: order.order_id,
                                        strategy: strategy,
                                        stock: symbol,
                                        primary_exchange: contract.primary_exchange.to_string(),
                                        currency: contract.currency.to_string(),
                                        expiry: contract.last_trade_date_or_contract_month,
                                        strike: contract.strike,
                                        multiplier: contract.multiplier,
                                        option_type: OptionType::from_str(&contract.right).expect("Expected valid contract right to be passed to OptionType for sync_open_orders"),
                                        time: Utc::now(),
                                        quantity: order.total_quantity,
                                        executions: Vec::new(),
                                        filled: order.filled_quantity,
                                    })
                                    .await
                                {
                                    tracing::error!(
                                        message=%format!(
                                            "Error when trying to insert unmatched OpenOptionOrders for order_id {}: {}",
                                            order.perm_id,
                                            e
                                        )
                                    );
                                }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "Error when trying to read OpenOptionOrders in on_full_open_order_received for sync_open_orders: {e:?}"
                        )
                    }
                }
            }
            AssetType::CASH => {
                tracing::warn!(
                    "CASH AssetType encountered while syncing open orders:\nAssumed not from any of the strategies - don't need to track!"
                )
            }
            AssetType::Unknown => {
                tracing::warn!(
                    "Unknown AssetType encountered while syncing open orders:\nAssumed not from any of the strategies - don't need to track!"
                )
            }
        }
        // } else {
        //     tracing::error!(
        //         "No associated strategy found for open order received: ({},{})",
        //         contract.security_type.to_string(),
        //         contract.symbol
        //     )
        // }
    });
}
