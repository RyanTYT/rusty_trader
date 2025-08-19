use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            StockTransactionsFullKeys, StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys,
        },
    },
    delegate_all_crud_methods,
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
    crud:
        CRUD<StockTransactionsFullKeys, StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys>,
}
impl StockTransactionsCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                StockTransactionsFullKeys,
                StockTransactionsPrimaryKeys,
                StockTransactionsUpdateKeys,
            >::new(pool, String::from("trading.stock_transactions")),
        }
    }

    delegate_all_crud_methods!(
        crud,
        StockTransactionsFullKeys,
        StockTransactionsPrimaryKeys,
        StockTransactionsUpdateKeys
    );

    pub async fn read_last_transaction_of(
        &self,
        stock: String,
        primary_exchange: String,
    ) -> Result<Option<StockTransactionsFullKeys>, String> {
        sqlx::query_as!(
            StockTransactionsFullKeys,
            r#"
            SELECT * 
            FROM trading.stock_transactions
            WHERE stock = $1
                AND primary_exchange = $2
            ORDER BY time DESC
            LIMIT 1;
            "#,
            stock,
            primary_exchange
        )
        .fetch_optional(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error when updating unknown strategy in stock positions: {}",
                e
            )
        })
    }
}

pub fn get_specific_stock_transactions_crud(pool: PgPool) -> StockTransactionsCRUD {
    StockTransactionsCRUD::new(pool)
}
