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
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                OptionTransactionsFullKeys,
                OptionTransactionsPrimaryKeys,
                OptionTransactionsUpdateKeys,
            >::new(pool, String::from("trading.stock_transactions")),
        }
    }

    // pub async fn read_last_transaction_of(
    //     &self,
    //     stock: &str,
    //     primary_exchange: &str,
    //     currency: &str,
    // ) -> Result<Option<OptionTransactionsFullKeys>, String> {
    //     sqlx::query_as!(
    //         OptionTransactionsFullKeys,
    //         r#"
    //         SELECT *
    //         FROM trading.stock_transactions
    //         WHERE stock = $1
    //             AND primary_exchange = $2
    //             AND currency = $3
    //         ORDER BY time DESC
    //         LIMIT 1;
    //         "#,
    //         stock,
    //         primary_exchange,
    //         currency
    //     )
    //     .fetch_optional(&self.crud.pool)
    //     .await
    //     .map_err(|e| {
    //         format!(
    //             "Error when updating unknown strategy in stock positions: {}",
    //             e
    //         )
    //     })
    // }
}
