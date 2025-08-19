use sqlx::PgPool;

use crate::database::{
    crud::{CRUD, CRUDTrait},
    models::{
        OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys, OptionTransactionsUpdateKeys,
    },
};

pub fn get_option_transactions_crud(
    pool: PgPool,
) -> CRUD<OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys, OptionTransactionsUpdateKeys> {
    CRUD::<
        OptionTransactionsFullKeys,
        OptionTransactionsPrimaryKeys,
        OptionTransactionsUpdateKeys,
    >::new(pool, String::from("trading.option_transactions"))
}
