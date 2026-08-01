use sqlx::PgPool;

use crate::database::{
    models::{
        OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys,
        OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys, OptionType,
    },
    models_crud::open_orders::{
        open_option_orders::OpenOptionOrdersCRUD, open_stock_orders::OpenStockOrdersCRUD,
    },
};

pub enum OpenOrdersCRUD {
    Stock(OpenStockOrdersCRUD),
    Options(OpenOptionOrdersCRUD),
}

pub enum OpenOrdersFullKeys {
    Stock(OpenStockOrdersFullKeys),
    Options(OpenOptionOrdersFullKeys),
}

pub enum OpenOrdersPrimaryKeys {
    Stock(OpenStockOrdersPrimaryKeys),
    Options(OpenOptionOrdersPrimaryKeys),
}

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
