use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{
            OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys,
            OptionTransactionsUpdateKeys, OptionType,
        },
    },
    implement_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct OptionTransactionsCRUD {
    pub(super) crud: CRUD<
        OptionTransactionsFullKeys,
        OptionTransactionsPrimaryKeys,
        OptionTransactionsUpdateKeys,
    >,
}

#[derive(Debug, Clone)]
pub struct OptionTransactionsUnderlyingPrimaryKeys {
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
    pub expiry: String,
    pub strike: f64,
    pub multiplier: String,
    pub option_type: OptionType,
}

implement_all_crud_methods!(
    crud,
    OptionTransactionsFullKeys,
    OptionTransactionsPrimaryKeys,
    OptionTransactionsUpdateKeys,
    OptionTransactionsCRUD
);

impl OptionTransactionsCRUD {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                OptionTransactionsFullKeys,
                OptionTransactionsPrimaryKeys,
                OptionTransactionsUpdateKeys,
            >::new(pool, String::from("trading.option_transactions")),
        }
    }
}
