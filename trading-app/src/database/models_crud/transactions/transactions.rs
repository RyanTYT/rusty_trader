use sqlx::PgPool;

use crate::database::{
    models::{
        OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys, OptionTransactionsUpdateKeys,
        OptionType, StockTransactionsFullKeys, StockTransactionsPrimaryKeys,
        StockTransactionsUpdateKeys,
    },
    models_crud::transactions::{
        option_transactions::{OptionTransactionsCRUD, OptionTransactionsUnderlyingPrimaryKeys},
        stock_transactions::{StockTransactionsCRUD, StockTransactionsUnderlyingPrimaryKeys},
    },
};

pub enum TransactionsCRUD {
    Stock(StockTransactionsCRUD),
    Options(OptionTransactionsCRUD),
}

pub enum TransactionsFullKeys {
    Stock(StockTransactionsFullKeys),
    Options(OptionTransactionsFullKeys),
}

pub enum TransactionsPrimaryKeys {
    Stock(StockTransactionsPrimaryKeys),
    Options(OptionTransactionsPrimaryKeys),
}

pub enum TransactionsUpdateKeys {
    Stock(StockTransactionsUpdateKeys),
    Options(OptionTransactionsUpdateKeys),
}

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
}

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
