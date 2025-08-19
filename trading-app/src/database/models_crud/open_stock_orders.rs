use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys},
    },
    delegate_all_crud_methods,
};

pub struct OpenStockOrdersFullKeysRes {
    pub order_perm_id: Option<i32>,
    pub order_id: Option<i32>,
    pub strategy: Option<String>,
    pub stock: Option<String>,
    pub primary_exchange: Option<String>,
    pub time: Option<DateTime<Utc>>,
    pub quantity: Option<f64>,

    pub executions: Option<Vec<String>>,
    pub filled: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct OpenStockOrdersCRUD {
    crud: CRUD<OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys>,
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

    delegate_all_crud_methods!(
        crud,
        OpenStockOrdersFullKeys,
        OpenStockOrdersPrimaryKeys,
        OpenStockOrdersUpdateKeys
    );

    pub async fn get_orders_for_strat(
        &self,
        strategy: &String,
    ) -> Result<Vec<OpenStockOrdersFullKeys>, String> {
        let res = sqlx::query_as!(
            OpenStockOrdersFullKeysRes,
            r#"
            SELECT 
                order_perm_id,
                order_id,
                strategy,
                stock,
                primary_exchange,
                time,
                quantity,
                executions,
                filled
            FROM trading.open_stock_orders
            WHERE strategy = $1;
            "#,
            strategy
        )
        .fetch_all(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error when updating unknown strategy in stock positions: {}",
                e
            )
        })?;
        Ok(res
            .iter()
            .map(|order| OpenStockOrdersFullKeys {
                order_perm_id: order
                    .order_perm_id
                    .expect("Expected to be able to parse order_perm_id"),
                order_id: order
                    .order_id
                    .expect("Expected to be able to parse order_id"),
                strategy: order
                    .strategy
                    .clone()
                    .expect("Expected to be able to parse strategy"),
                stock: order
                    .stock
                    .clone()
                    .expect("Expected to be able to parse stock"),
                primary_exchange: order
                    .primary_exchange
                    .clone()
                    .expect("Expected to be able to parse stock"),
                time: order.time.expect("Expected to be able to parse time"),
                quantity: order
                    .quantity
                    .expect("Expected to be able to parse quantity"),
                executions: order
                    .executions
                    .clone()
                    .expect("Expected to be able to parse executions"),
                filled: order.filled.expect("Expected to be able to parse filled"),
            })
            .collect())
    }
}

pub fn get_open_stock_orders_crud(
    pool: PgPool,
) -> CRUD<OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys> {
    CRUD::<OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys>::new(
        pool,
        String::from("trading.open_stock_orders"),
    )
}

pub fn get_specific_open_stock_orders_crud(pool: PgPool) -> OpenStockOrdersCRUD {
    OpenStockOrdersCRUD::new(pool)
}
