use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys,
            OptionTransactionsUpdateKeys, OptionType,
        },
    },
    delegate_all_crud_methods,
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

impl
    CRUDTrait<
        OptionTransactionsFullKeys,
        OptionTransactionsPrimaryKeys,
        OptionTransactionsUpdateKeys,
    > for OptionTransactionsCRUD
{
    delegate_all_crud_methods!(
        crud,
        OptionTransactionsFullKeys,
        OptionTransactionsPrimaryKeys,
        OptionTransactionsUpdateKeys
    );
}

impl OptionTransactionsCRUD {
    pub(super) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                OptionTransactionsFullKeys,
                OptionTransactionsPrimaryKeys,
                OptionTransactionsUpdateKeys,
            >::new(pool, String::from("trading.stock_transactions")),
        }
    }
}
