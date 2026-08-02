use chrono::{NaiveDateTime, TimeZone, Utc};
use ibapi::orders::ExecutionData;
use rust_decimal::dec;
use sqlx::PgPool;

use crate::{
    database::{
        models::{
            AssetType, OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys,
            OptionTransactionsUpdateKeys, OptionType, StockTransactionsFullKeys,
            StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys,
        },
        models_crud::transactions::{
            option_transactions::{
                OptionTransactionsCRUD, OptionTransactionsUnderlyingPrimaryKeys,
            },
            stock_transactions::{StockTransactionsCRUD, StockTransactionsUnderlyingPrimaryKeys},
        },
    },
    helpers::contract::get_local_symbol,
    implement_crud_trait_for_interface,
};

#[derive(Debug, Clone)]
pub enum TransactionsCRUD {
    Stock(StockTransactionsCRUD),
    Options(OptionTransactionsCRUD),
}

#[derive(Debug, Clone)]
pub enum TransactionsFullKeys {
    Stock(StockTransactionsFullKeys),
    Options(OptionTransactionsFullKeys),
}

impl TransactionsFullKeys {
    pub fn from_strat_and_exec(strategy: &str, execution_data: &ExecutionData) -> Self {
        let naive_dt =
            NaiveDateTime::parse_from_str(&execution_data.execution.time, "%Y%m%d  %H:%M:%S")
                .expect(&format!(
                    "Failed to parse execution time: {}",
                    &execution_data.execution.time
                ));
        let execution_time = Utc
            .from_local_datetime(&naive_dt)
            .single()
            .expect("Ambiguous or invalid datetime in New York timezone");
        match AssetType::from_str(&execution_data.contract.security_type) {
            AssetType::Option => Self::Options(OptionTransactionsFullKeys {
                strategy: strategy.to_string(),
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
                option_type: OptionType::from_str(&execution_data.contract.right).expect(
                    "Error parsing OptionType from contract right in update_option_execution",
                ),
                time: execution_time.to_utc(),

                price: execution_data.execution.average_price,
                quantity: if execution_data.execution.side == "BOT" {
                    execution_data.execution.shares.clone()
                } else {
                    -execution_data.execution.shares.clone()
                },
                fees: dec!(0),
            }),
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                let stock = get_local_symbol(&execution_data.contract);
                Self::Stock(StockTransactionsFullKeys {
                    strategy: strategy.to_string(),
                    execution_id: execution_data.execution.execution_id.to_string(),
                    order_perm_id: execution_data.execution.perm_id,
                    stock: stock,
                    primary_exchange: execution_data.contract.primary_exchange.to_string(),
                    currency: execution_data.contract.currency.to_string(),
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
                })
            }
            AssetType::Unknown => panic!(
                "Tried to construct TransactionPrimaryKeys from unknown asset_type: {execution_data:?}"
            ),
            AssetType::CASH => panic!(
                "Tried to construct TransactionsPrimaryKeys from CASH asset_type: should not have been possible to construct from contract: {execution_data:?}"
            ),
        }
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for TransactionsFullKeys {
    fn from_row(_: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        // This will never be executed because the inner CRUD<FK,PK,UK>
        // decodes the inner concrete struct before wrapping it into this enum.
        Err(sqlx::Error::Decode(
            "TransactionsFullKeys cannot be decoded directly from a raw SQL row".into(),
        ))
    }
}

#[derive(Debug, Clone)]
pub enum TransactionsPrimaryKeys {
    Stock(StockTransactionsPrimaryKeys),
    Options(OptionTransactionsPrimaryKeys),
}

impl TransactionsPrimaryKeys {
    pub fn from(execution_data: &ExecutionData) -> Self {
        match AssetType::from_str(&execution_data.contract.security_type) {
            AssetType::Option => Self::Options(OptionTransactionsPrimaryKeys {
                execution_id: execution_data.execution.execution_id.to_string(),
            }),
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                Self::Stock(StockTransactionsPrimaryKeys {
                    execution_id: execution_data.execution.execution_id.to_string(),
                })
            }
            AssetType::Unknown => panic!(
                "Tried to construct TransactionPrimaryKeys from unknown asset_type: {execution_data:?}"
            ),
            AssetType::CASH => panic!(
                "Tried to construct TransactionsPrimaryKeys from CASH asset_type: should not have been possible to construct from contract: {execution_data:?}"
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransactionsUpdateKeys {
    Stock(StockTransactionsUpdateKeys),
    Options(OptionTransactionsUpdateKeys),
}

#[derive(Debug, Clone)]
pub enum TransactionsUnderlyingPrimaryKeys {
    Stock(StockTransactionsUnderlyingPrimaryKeys),
    Options(OptionTransactionsUnderlyingPrimaryKeys),
}

impl TransactionsCRUD {
    fn get_pg_pool<'a>(&'a self) -> &'a PgPool {
        match self {
            Self::Stock(stk) => &stk.crud.pool,
            Self::Options(opt) => &opt.crud.pool,
        }
    }

    pub fn stock(pool: PgPool) -> Self {
        Self::Stock(StockTransactionsCRUD::new(pool))
    }

    pub fn option(pool: PgPool) -> Self {
        Self::Options(OptionTransactionsCRUD::new(pool))
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
    TransactionsCRUD,
    TransactionsFullKeys,
    TransactionsPrimaryKeys,
    TransactionsUpdateKeys,
    [Stock, Options]
);

pub trait TransactionsOps {
    /// Returns an Err(...) when sql query fails
    /// - returns a None when query succeeds but no transactions recorded in DB
    async fn read_last_transaction(
        &self,
        pk: TransactionsUnderlyingPrimaryKeys,
    ) -> Result<Option<TransactionsFullKeys>, String>;
}

impl TransactionsOps for TransactionsCRUD {
    async fn read_last_transaction(
        &self,
        pk: TransactionsUnderlyingPrimaryKeys,
    ) -> Result<Option<TransactionsFullKeys>, String> {
        let result = match pk {
            TransactionsUnderlyingPrimaryKeys::Stock(StockTransactionsUnderlyingPrimaryKeys {
                stock,
                primary_exchange,
                currency,
            }) => sqlx::query_as!(
                StockTransactionsFullKeys,
                r#"
                    SELECT * 
                    FROM trading.stock_transactions
                    WHERE stock = $1
                        AND primary_exchange = $2
                        AND currency = $3
                    ORDER BY time DESC
                    LIMIT 1;
                    "#,
                stock,
                primary_exchange,
                currency
            )
            .fetch_optional(self.get_pg_pool())
            .await
            .map(|ok_res| ok_res.map(TransactionsFullKeys::Stock)),
            TransactionsUnderlyingPrimaryKeys::Options(
                OptionTransactionsUnderlyingPrimaryKeys {
                    stock,
                    primary_exchange,
                    currency,
                    expiry,
                    strike,
                    multiplier,
                    option_type,
                },
            ) => sqlx::query_as!(
                OptionTransactionsFullKeys,
                r#"
                    SELECT 
                        execution_id as "execution_id!",
                        strategy as "strategy!",
                        stock as "stock!",
                        primary_exchange as "primary_exchange!",
                        currency as "currency!",
                        expiry as "expiry!",
                        strike as "strike!",
                        multiplier as "multiplier!",
                        option_type as "option_type!:OptionType",
                        order_perm_id as "order_perm_id!",
                        time as "time!",
                        price as "price!",
                        quantity as "quantity!",
                        fees as "fees!"
                    FROM trading.option_transactions
                    WHERE stock = $1
                        AND primary_exchange = $2
                        AND currency = $3
                        AND expiry = $4
                        AND strike = $5
                        AND multiplier = $6
                        AND option_type = $7
                    ORDER BY time DESC
                    LIMIT 1;
                    "#,
                stock,
                primary_exchange,
                currency,
                expiry,
                strike,
                multiplier,
                option_type as OptionType
            )
            .fetch_optional(self.get_pg_pool())
            .await
            .map(|ok_res| ok_res.map(TransactionsFullKeys::Options)),
        };

        result.map_err(|e| {
            format!(
                "Error when updating unknown strategy in stock positions: {}",
                e
            )
        })
    }
}
