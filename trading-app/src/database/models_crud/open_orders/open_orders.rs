use chrono::Utc;
use ibapi::{
    contracts::Contract,
    orders::{Action, ExecutionData, Order},
};
use sqlx::PgPool;

use crate::{
    database::{
        models::{
            AssetType, OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys,
            OpenOptionOrdersUpdateKeys, OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys,
            OpenStockOrdersUpdateKeys, OptionType,
        },
        models_crud::open_orders::{
            open_option_orders::OpenOptionOrdersCRUD, open_stock_orders::OpenStockOrdersCRUD,
        },
    },
    helpers::contract::get_local_symbol,
    implement_crud_trait_for_interface,
};

#[derive(Debug, Clone)]
pub enum OpenOrdersCRUD {
    Stock(OpenStockOrdersCRUD),
    Options(OpenOptionOrdersCRUD),
}

#[derive(Debug, Clone)]
pub enum OpenOrdersFullKeys {
    Stock(OpenStockOrdersFullKeys),
    Options(OpenOptionOrdersFullKeys),
}

impl OpenOrdersFullKeys {
    pub fn from_contract_and_order(contract: &Contract, order: &Order, filled: f64) -> Self {
        let asset_type = AssetType::from_str(&contract.security_type);
        match asset_type {
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                let qty = {
                    if order.action == Action::Sell {
                        -1.0
                    } else {
                        1.0
                    }
                } * order.total_quantity;

                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                    order_perm_id: order.perm_id,
                    order_id: order.order_id,
                    strategy: order.order_ref.clone(),
                    stock: get_local_symbol(&contract),
                    primary_exchange: contract.primary_exchange.to_string(),
                    currency: contract.currency.to_string(),
                    time: Utc::now(),
                    quantity: qty,
                    filled: filled,
                    executions: Vec::new(),
                })
            }
            AssetType::Option => {
                let qty = {
                    if order.action == Action::Sell {
                        -1.0
                    } else {
                        1.0
                    }
                } * order.total_quantity;

                Self::Options(OpenOptionOrdersFullKeys {
                    order_id: order.order_id,
                    order_perm_id: order.perm_id,
                    strategy: order.order_ref.clone(),
                    stock: contract.symbol.as_str().to_string(),
                    primary_exchange: contract.primary_exchange.to_string(),
                    currency: contract.currency.to_string(),
                    expiry: contract.last_trade_date_or_contract_month.to_string(),
                    strike: contract.strike,
                    multiplier: contract.multiplier.to_string(),
                    option_type: crate::database::models::OptionType::from_str(&contract.right)
                        .unwrap_or_else(|e| panic!("{}", e)),
                    time: Utc::now(),
                    quantity: qty,
                    filled: filled,
                    executions: Vec::new(),
                })
            }
            AssetType::CASH => {
                tracing::error!(
                    "Tried to create OpenOrdersFullKeys for CASH Asset Type: ({}, {})",
                    contract.symbol,
                    contract.security_type
                );
                panic!("Tried to create OpenOrdersFullKeys from AssetType::CASH");
            }
            AssetType::Unknown => {
                tracing::error!(
                    message=%format!("Tried to create OpenOrdersFullKeys for Unknown Asset Type: ({}, {})",
                    contract.symbol,
                    contract.security_type
                    )
                );
                panic!("Tried to create OpenOrdersFullKeys from AssetType::Unknown");
            }
        }
    }
}

// #[macro_export]
// macro_rules! extract_common_attr {
//     ($var:expr, $, $($variant:ident),* $(,)?) => {
//
//     };
// }

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for OpenOrdersFullKeys {
    fn from_row(_: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        // This will never be executed because the inner CRUD<FK,PK,UK>
        // decodes the inner concrete struct before wrapping it into this enum.
        Err(sqlx::Error::Decode(
            "OpenOrdersFullKeys cannot be decoded directly from a raw SQL row".into(),
        ))
    }
}

#[derive(Debug, Clone)]
pub enum OpenOrdersPrimaryKeys {
    Stock(OpenStockOrdersPrimaryKeys),
    Options(OpenOptionOrdersPrimaryKeys),
}

impl OpenOrdersPrimaryKeys {
    pub fn new(asset_type: &AssetType, order_perm_id: i32, order_id: i32) -> Self {
        match asset_type {
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                Self::Stock(OpenStockOrdersPrimaryKeys {
                    order_perm_id: order_perm_id,
                    order_id: order_id,
                })
            }
            AssetType::Option => Self::Options(OpenOptionOrdersPrimaryKeys {
                order_perm_id: order_perm_id,
                order_id: order_id,
            }),
            AssetType::CASH => {
                panic!("Tried to construct OpenOrdersPrimaryKeys from CASH asset_type")
            }
            AssetType::Unknown => {
                panic!("Tried to construct OpenOrdersPrimaryKeys from unknown asset_type")
            }
        }
    }

    pub fn from_execution(execution_data: &ExecutionData) -> Self {
        match AssetType::from_str(&execution_data.contract.security_type) {
            AssetType::Option => Self::Options(OpenOptionOrdersPrimaryKeys {
                order_perm_id: execution_data.execution.perm_id,
                order_id: execution_data.execution.order_id,
            }),
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                Self::Stock(OpenStockOrdersPrimaryKeys {
                    order_perm_id: execution_data.execution.perm_id,
                    order_id: execution_data.execution.order_id,
                })
            }
            AssetType::Unknown => panic!(
                "Tried to construct OpenOrdersPrimaryKeys from unknown asset_type: {execution_data:?}"
            ),
            AssetType::CASH => panic!(
                "Tried to construct OpenOrdersPrimaryKeys from CASH asset_type: should not have been possible to construct from contract: {execution_data:?}"
            ),
        }
    }

    pub fn from_open_order(open_order: &OpenOrdersFullKeys) -> Self {
        match open_order {
            OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                order_perm_id,
                order_id,
                ..
            }) => OpenOrdersPrimaryKeys::Stock(OpenStockOrdersPrimaryKeys {
                order_perm_id: *order_perm_id,
                order_id: *order_id,
            }),
            OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                order_perm_id,
                order_id,
                ..
            }) => OpenOrdersPrimaryKeys::Options(OpenOptionOrdersPrimaryKeys {
                order_perm_id: *order_perm_id,
                order_id: *order_id,
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub enum OpenOrdersUpdateKeys {
    Stock(OpenStockOrdersUpdateKeys),
    Options(OpenOptionOrdersUpdateKeys),
}

impl OpenOrdersUpdateKeys {
    pub fn new(asset_type: &AssetType, contract: &Contract, order: &Order) -> Self {
        match asset_type {
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                Self::Stock(OpenStockOrdersUpdateKeys {
                    strategy: Some(order.order_ref.to_string()),
                    stock: Some(get_local_symbol(&contract)),
                    primary_exchange: Some(contract.primary_exchange.to_string()),
                    currency: Some(contract.currency.to_string()),
                    time: Some(Utc::now()),
                    quantity: Some(order.total_quantity),
                    executions: None,
                    filled: Some(0.0),
                })
            }
            AssetType::Option => Self::Options(OpenOptionOrdersUpdateKeys {
                strategy: Some(order.order_ref.to_string()),
                stock: Some(get_local_symbol(&contract)),
                primary_exchange: Some(contract.primary_exchange.to_string()),
                currency: Some(contract.currency.to_string()),
                expiry: Some(contract.last_trade_date_or_contract_month.to_string()),
                strike: Some(contract.strike),
                multiplier: Some(contract.multiplier.to_string()),
                option_type: Some(OptionType::from_str(&contract.right).unwrap()),
                time: Some(Utc::now()),
                quantity: Some(order.total_quantity),
                executions: None,
                filled: Some(0.0),
            }),
            AssetType::CASH => {
                panic!("Tried to construct OpenOrdersPrimaryKeys from CASH asset_type")
            }
            AssetType::Unknown => {
                panic!("Tried to construct OpenOrdersPrimaryKeys from unknown asset_type")
            }
        }
    }
}

impl OpenOrdersCRUD {
    fn get_pg_pool<'a>(&'a self) -> &'a PgPool {
        match self {
            Self::Stock(stk) => &stk.crud.pool,
            Self::Options(opt) => &opt.crud.pool,
        }
    }

    pub fn stock(pool: PgPool) -> Self {
        Self::Stock(OpenStockOrdersCRUD::new(pool))
    }

    pub fn option(pool: PgPool) -> Self {
        Self::Options(OpenOptionOrdersCRUD::new(pool))
    }

    pub fn from(asset_type: &AssetType, pool: PgPool) -> Self {
        match asset_type {
            AssetType::Stock
            | AssetType::Future
            | AssetType::CFD
            | AssetType::ForexPair
            | AssetType::CASH => Self::stock(pool),
            AssetType::Option => Self::option(pool),
            AssetType::Unknown => panic!("Tried to get CRUD instance from an Unknown Asset Type!"),
        }
    }
}

implement_crud_trait_for_interface!(
    OpenOrdersCRUD,
    OpenOrdersFullKeys,
    OpenOrdersPrimaryKeys,
    OpenOrdersUpdateKeys,
    [Stock, Options]
);

#[async_trait::async_trait]
pub trait OpenOrdersOps {
    async fn get_orders_for_strat(&self, strategy: &str)
    -> Result<Vec<OpenOrdersFullKeys>, String>;
}

#[async_trait::async_trait]
impl OpenOrdersOps for OpenOrdersCRUD {
    async fn get_orders_for_strat(
        &self,
        strategy: &str,
    ) -> Result<Vec<OpenOrdersFullKeys>, String> {
        let result = match self {
            Self::Stock(_) => sqlx::query_as!(
                OpenStockOrdersFullKeys,
                r#"
                    SELECT 
                        order_perm_id as "order_perm_id!",
                        order_id as "order_id!",
                        strategy as "strategy!",
                        stock as "stock!",
                        primary_exchange as "primary_exchange!",
                        currency as "currency!",
                        time as "time!",
                        quantity as "quantity!",
                        executions as "executions!",
                        filled as "filled!"
                    FROM trading.open_stock_orders
                    WHERE strategy = $1;
                    "#,
                strategy
            )
            .fetch_all(self.get_pg_pool())
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(OpenOrdersFullKeys::Stock)
                    .collect::<Vec<OpenOrdersFullKeys>>()
            }),
            Self::Options(_) => sqlx::query_as!(
                OpenOptionOrdersFullKeys,
                r#"
                    SELECT 
                        order_perm_id as "order_perm_id!",
                        order_id as "order_id!",
                        strategy as "strategy!",
                        stock as "stock!",
                        primary_exchange as "primary_exchange!",
                        currency as "currency!",
                        expiry as "expiry!",
                        strike as "strike!",
                        multiplier as "multiplier!",
                        option_type AS "option_type!:OptionType",
                        time as "time!",
                        quantity as "quantity!",
                        executions as "executions!",
                        filled as "filled!"
                    FROM trading.open_option_orders
                    WHERE strategy = $1;
                    "#,
                strategy
            )
            .fetch_all(self.get_pg_pool())
            .await
            .map(|rows| rows.into_iter().map(OpenOrdersFullKeys::Options).collect()),
        };

        result.map_err(|e| format!("Failed to get_orders_for_strat: {e:?}"))
    }
}
