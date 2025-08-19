use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys,
            OptionType,
        },
    },
    delegate_all_crud_methods,
};

pub struct OpenOptionOrdersFullKeysRes {
    pub order_perm_id: Option<i32>,
    pub order_id: Option<i32>,
    pub strategy: Option<String>,
    pub stock: Option<String>,
    pub primary_exchange: Option<String>,
    pub expiry: Option<String>,
    pub strike: Option<f64>,
    pub multiplier: Option<String>,
    pub option_type: Option<OptionType>,
    pub time: Option<DateTime<Utc>>,
    pub quantity: Option<f64>,

    pub executions: Option<Vec<String>>,
    pub filled: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct OpenOptionOrdersCRUD {
    crud: CRUD<OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys>,
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

    delegate_all_crud_methods!(
        crud,
        OpenOptionOrdersFullKeys,
        OpenOptionOrdersPrimaryKeys,
        OpenOptionOrdersUpdateKeys
    );

    pub async fn get_orders_for_strat(
        &self,
        strategy: &String,
    ) -> Result<Vec<OpenOptionOrdersFullKeys>, String> {
        let res = sqlx::query_as!(
            OpenOptionOrdersFullKeysRes,
            r#"
            SELECT 
                order_perm_id,
                order_id,
                strategy,
                stock,
                primary_exchange,
                expiry,
                strike,
                multiplier,
                option_type AS "option_type!:OptionType",
                time,
                quantity,
                executions,
                filled
            FROM trading.open_option_orders
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
            .map(|order| OpenOptionOrdersFullKeys {
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
                expiry: order
                    .expiry
                    .clone()
                    .expect("Expected to be able to parse expiry"),
                strike: order.strike.expect("Expected to be able to parse strike"),
                multiplier: order
                    .multiplier
                    .clone()
                    .expect("Expected to be able to parse multiplier"),
                option_type: order
                    .option_type
                    .clone()
                    .expect("Expected to be able to parse option_type"),
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

pub fn get_open_option_orders_crud(
    pool: PgPool,
) -> CRUD<OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys> {
    CRUD::<OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys>::new(
        pool,
        String::from("trading.open_option_orders"),
    )
}

pub fn get_specific_option_orders_crud(pool: PgPool) -> OpenOptionOrdersCRUD {
    OpenOptionOrdersCRUD::new(pool)
}
