use sqlx::PgPool;

use crate::{
    database::{
        models::{
            OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys,
            OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys,
            OptionType,
        },
        models_crud::open_orders::{
            open_option_orders::OpenOptionOrdersCRUD, open_stock_orders::OpenStockOrdersCRUD,
        },
    },
    implement_crud_trait_for_interface,
};

#[derive(Debug, Clone)]
pub enum OpenOrdersCRUD {
    Stock(OpenStockOrdersCRUD),
    Options(OpenOptionOrdersCRUD),
}

#[derive(Debug, Clone)]
pub enum OpenOrdersFullKeys {
    Stock(OpenStockOrdersFullKeys),
    Options(OpenOptionOrdersFullKeys),
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for OpenOrdersFullKeys {
    fn from_row(_: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        // This will never be executed because the inner CRUD<FK,PK,UK>
        // decodes the inner concrete struct before wrapping it into this enum.
        Err(sqlx::Error::Decode(
            "OpenOrdersFullKeys cannot be decoded directly from a raw SQL row".into(),
        ))
    }
}

#[derive(Debug, Clone)]
pub enum OpenOrdersPrimaryKeys {
    Stock(OpenStockOrdersPrimaryKeys),
    Options(OpenOptionOrdersPrimaryKeys),
}

#[derive(Debug, Clone)]
pub enum OpenOrdersUpdateKeys {
    Stock(OpenStockOrdersUpdateKeys),
    Options(OpenOptionOrdersUpdateKeys),
}

impl OpenOrdersCRUD {
    fn get_pg_pool<'a>(&'a self) -> &'a PgPool {
        match self {
            Self::Stock(stk) => &stk.crud.pool,
            Self::Options(opt) => &opt.crud.pool,
        }
    }
}

implement_crud_trait_for_interface!(
    OpenOrdersCRUD,
    OpenOrdersFullKeys,
    OpenOrdersPrimaryKeys,
    OpenOrdersUpdateKeys,
    [Stock, Options]
);

pub trait OpenOrdersOps {
    async fn get_orders_for_strat(&self, strategy: &str)
    -> Result<Vec<OpenOrdersFullKeys>, String>;
}

impl OpenOrdersOps for OpenOrdersCRUD {
    async fn get_orders_for_strat(
        &self,
        strategy: &str,
    ) -> Result<Vec<OpenOrdersFullKeys>, String> {
        let result = match self {
            Self::Stock(_) => sqlx::query_as!(
                OpenStockOrdersFullKeys,
                r#"
                    SELECT 
                        order_perm_id as "order_perm_id!",
                        order_id as "order_id!",
                        strategy as "strategy!",
                        stock as "stock!",
                        primary_exchange as "primary_exchange!",
                        currency as "currency!",
                        time as "time!",
                        quantity as "quantity!",
                        executions as "executions!",
                        filled as "filled!"
                    FROM trading.open_stock_orders
                    WHERE strategy = $1;
                    "#,
                strategy
            )
            .fetch_all(self.get_pg_pool())
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(OpenOrdersFullKeys::Stock)
                    .collect::<Vec<OpenOrdersFullKeys>>()
            }),
            Self::Options(_) => sqlx::query_as!(
                OpenOptionOrdersFullKeys,
                r#"
                    SELECT 
                        order_perm_id as "order_perm_id!",
                        order_id as "order_id!",
                        strategy as "strategy!",
                        stock as "stock!",
                        primary_exchange as "primary_exchange!",
                        currency as "currency!",
                        expiry as "expiry!",
                        strike as "strike!",
                        multiplier as "multiplier!",
                        option_type AS "option_type!:OptionType",
                        time as "time!",
                        quantity as "quantity!",
                        executions as "executions!",
                        filled as "filled!"
                    FROM trading.open_option_orders
                    WHERE strategy = $1;
                    "#,
                strategy
            )
            .fetch_all(self.get_pg_pool())
            .await
            .map(|rows| rows.into_iter().map(OpenOrdersFullKeys::Options).collect()),
        };

        result.map_err(|e| format!("Failed to get_orders_for_strat: {e:?}"))
    }
}
