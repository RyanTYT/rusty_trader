use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys},
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct OpenStockOrdersCRUD {
    pub(super) crud:
        CRUD<OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys>,
}

impl CRUDTrait<OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys>
    for OpenStockOrdersCRUD
{
    delegate_all_crud_methods!(
        crud,
        OpenStockOrdersFullKeys,
        OpenStockOrdersPrimaryKeys,
        OpenStockOrdersUpdateKeys
    );
}

impl OpenStockOrdersCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                OpenStockOrdersFullKeys,
                OpenStockOrdersPrimaryKeys,
                OpenStockOrdersUpdateKeys,
            >::new(pool, String::from("trading.open_stock_orders")),
        }
    }

    // pub async fn get_orders_for_strat(
    //     &self,
    //     strategy: &str,
    // ) -> Result<Vec<OpenStockOrdersFullKeys>, String> {
    //     let res = sqlx::query_as!(
    //         OpenStockOrdersFullKeysRes,
    //         r#"
    //         SELECT
    //             order_perm_id,
    //             order_id,
    //             strategy,
    //             stock,
    //             primary_exchange,
    //             currency,
    //             time,
    //             quantity,
    //             executions,
    //             filled
    //         FROM trading.open_stock_orders
    //         WHERE strategy = $1;
    //         "#,
    //         strategy
    //     )
    //     .fetch_all(&self.crud.pool)
    //     .await
    //     .map_err(|e| {
    //         format!(
    //             "Error when updating unknown strategy in stock positions: {}",
    //             e
    //         )
    //     })?;
    //     Ok(res
    //         .iter()
    //         .map(|order| OpenStockOrdersFullKeys {
    //             order_perm_id: order
    //                 .order_perm_id
    //                 .expect("Expected to be able to parse order_perm_id"),
    //             order_id: order
    //                 .order_id
    //                 .expect("Expected to be able to parse order_id"),
    //             strategy: order
    //                 .strategy
    //                 .clone()
    //                 .expect("Expected to be able to parse strategy"),
    //             stock: order
    //                 .stock
    //                 .clone()
    //                 .expect("Expected to be able to parse stock"),
    //             primary_exchange: order
    //                 .primary_exchange
    //                 .clone()
    //                 .expect("Expected to be able to parse primary_exchange"),
    //             currency: order
    //                 .currency
    //                 .clone()
    //                 .expect("Expected to be able to parse currency"),
    //             time: order.time.expect("Expected to be able to parse time"),
    //             quantity: order
    //                 .quantity
    //                 .expect("Expected to be able to parse quantity"),
    //             executions: order
    //                 .executions
    //                 .clone()
    //                 .expect("Expected to be able to parse executions"),
    //             filled: order.filled.expect("Expected to be able to parse filled"),
    //         })
    //         .collect())
    // }
}
