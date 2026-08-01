use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct OpenOptionOrdersCRUD {
    pub(super) crud:
        CRUD<OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys>,
}

impl CRUDTrait<OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys>
    for OpenOptionOrdersCRUD
{
    delegate_all_crud_methods!(
        crud,
        OpenOptionOrdersFullKeys,
        OpenOptionOrdersPrimaryKeys,
        OpenOptionOrdersUpdateKeys
    );
}

impl OpenOptionOrdersCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                OpenOptionOrdersFullKeys,
                OpenOptionOrdersPrimaryKeys,
                OpenOptionOrdersUpdateKeys,
            >::new(pool, String::from("trading.open_option_orders")),
        }
    }

    // pub async fn get_orders_for_strat(
    //     &self,
    //     strategy: &str,
    // ) -> Result<Vec<OpenOptionOrdersFullKeys>, String> {
    //     let res = sqlx::query_as!(
    //         OpenOptionOrdersFullKeysRes,
    //         r#"
    //         SELECT
    //             order_perm_id,
    //             order_id,
    //             strategy,
    //             stock,
    //             primary_exchange,
    //             currency,
    //             expiry,
    //             strike,
    //             multiplier,
    //             option_type AS "option_type!:OptionType",
    //             time,
    //             quantity,
    //             executions,
    //             filled
    //         FROM trading.open_option_orders
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
    //         .map(|order| OpenOptionOrdersFullKeys {
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
    //             expiry: order
    //                 .expiry
    //                 .clone()
    //                 .expect("Expected to be able to parse expiry"),
    //             strike: order.strike.expect("Expected to be able to parse strike"),
    //             multiplier: order
    //                 .multiplier
    //                 .clone()
    //                 .expect("Expected to be able to parse multiplier"),
    //             option_type: order
    //                 .option_type
    //                 .clone()
    //                 .expect("Expected to be able to parse option_type"),
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
