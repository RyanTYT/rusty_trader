use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{
            StockTransactionsFullKeys, StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

pub fn get_stock_transactions_crud(
    pool: PgPool,
) -> CRUD<StockTransactionsFullKeys, StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys> {
    CRUD::<
        StockTransactionsFullKeys,
        StockTransactionsPrimaryKeys,
        StockTransactionsUpdateKeys,
    >::new(pool, String::from("trading.stock_transactions"))
}

#[derive(Debug, Clone)]
pub struct StockTransactionsCRUD {
    pub(super) crud:
        CRUD<StockTransactionsFullKeys, StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys>,
}

#[derive(Debug, Clone)]
pub struct StockTransactionsUnderlyingPrimaryKeys {
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
}

implement_all_crud_methods!(
    crud,
    StockTransactionsFullKeys,
    StockTransactionsPrimaryKeys,
    StockTransactionsUpdateKeys,
    StockTransactionsCRUD
);

impl StockTransactionsCRUD {
    pub(super) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                StockTransactionsFullKeys,
                StockTransactionsPrimaryKeys,
                StockTransactionsUpdateKeys,
            >::new(pool, String::from("trading.stock_transactions")),
        }
    }
}
