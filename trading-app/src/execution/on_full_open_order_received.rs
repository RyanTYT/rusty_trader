use std::collections::HashMap;

use chrono::Utc;
use ibapi::{
    orders::{Order, OrderStatus},
    prelude::Contract,
};
use sqlx::PgPool;

use crate::database::{
    crud::{CRUD, CRUDTrait},
    models::{
        AssetType, OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys,
        OpenOptionOrdersUpdateKeys, OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys,
        OpenStockOrdersUpdateKeys, OptionType,
    },
};

// In conjunction with sync_open_orders
pub fn on_full_open_order_received(
    contract_to_strategy: HashMap<(String, String), String>,
    pool: PgPool,
    contract: Contract,
    order: Order,
    order_status: OrderStatus,
) {
    tokio::spawn(async move {
        if let Some(strategy) = contract_to_strategy.get(&(
            contract.security_type.to_string().clone(),
            contract.symbol.clone(),
        )) {
            match AssetType::from_str(contract.security_type.clone()) {
                AssetType::Stock => {
                    let open_stock_orders_crud = CRUD::<
                        OpenStockOrdersFullKeys,
                        OpenStockOrdersPrimaryKeys,
                        OpenStockOrdersUpdateKeys,
                    >::new(
                        pool.clone(),
                        String::from("trading.open_stock_orders_view"),
                    );

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
                                if open_stock_orders_row.filled != order_status.filled {
                                    if let Err(e) = open_stock_orders_crud
                                        .update(
                                            &OpenStockOrdersPrimaryKeys {
                                                order_perm_id: order.perm_id.clone(),
                                                order_id: order.order_id.clone(),
                                            },
                                            &OpenStockOrdersUpdateKeys {
                                                strategy: None,
                                                stock: None,
                                                primary_exchange: None,
                                                time: None,
                                                quantity: None,
                                                executions: None,
                                                filled: Some(order_status.filled.clone()),
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
                                        order_perm_id: order.perm_id.clone(),
                                        order_id: order.order_id.clone(),
                                        strategy: strategy.clone(),
                                        stock: contract.symbol,
                                        primary_exchange: contract.primary_exchange.clone(),
                                        time: Utc::now(),
                                        quantity: order.total_quantity,
                                        executions: Vec::new(),
                                        filled: order.filled_quantity,
                                    })
                                    .await
                                {
                                    tracing::error!(
                                        "Error when trying to insert unmatched OpenStockOrders for order_id {}: {}",
                                        order.perm_id,
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Error when trying to read OpenStockOrders in on_full_open_order_received for sync_open_orders: {}",
                                e
                            )
                        }
                    }
                }
                AssetType::Option => {
                    let open_option_orders_crud = CRUD::<
                        OpenOptionOrdersFullKeys,
                        OpenOptionOrdersPrimaryKeys,
                        OpenOptionOrdersUpdateKeys,
                    >::new(
                        pool.clone(),
                        String::from("trading.open_option_orders_view"),
                    );

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
                                if open_option_order_row.filled != order_status.filled {
                                    if let Err(e) = open_option_orders_crud
                                        .update(
                                            &OpenOptionOrdersPrimaryKeys {
                                                order_perm_id: order.perm_id.clone(),
                                                order_id: order.order_id.clone(),
                                            },
                                            &OpenOptionOrdersUpdateKeys {
                                                strategy: None,
                                                stock: None,
                                                primary_exchange: None,
                                                expiry: None,
                                                strike: None,
                                                multiplier: None,
                                                option_type: None,
                                                time: None,
                                                quantity: None,
                                                executions: None,
                                                filled: Some(order_status.filled.clone()),
                                            },
                                        )
                                        .await
                                    {
                                        tracing::error!(
                                            "Error when trying to update OpenOptionOrders for order_id {}: {}",
                                            order.perm_id,
                                            e
                                        );
                                    }
                                }
                            } else {
                                if let Err(e) = open_option_orders_crud
                                    .create(&OpenOptionOrdersFullKeys {
                                        order_perm_id: order.perm_id.clone(),
                                        order_id: order.order_id.clone(),
                                        strategy: strategy.clone(),
                                        stock: contract.symbol,
                                        primary_exchange: contract.primary_exchange.clone(),
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
                                        "Error when trying to insert unmatched OpenOptionOrders for order_id {}: {}",
                                        order.perm_id,
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Error when trying to read OpenOptionOrders in on_full_open_order_received for sync_open_orders: {}",
                                e
                            )
                        }
                    }
                }
            }
        } else {
            tracing::error!(
                "No associated strategy found for open order received: ({},{})",
                contract.security_type.to_string(),
                contract.symbol
            )
        }
    });
}
