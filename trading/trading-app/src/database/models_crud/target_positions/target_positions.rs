use async_trait::async_trait;
use sqlx::PgPool;

use crate::{
    database::{
        models::{
            AssetType, OptionType, TargetOptionPositionsFullKeys, TargetOptionPositionsPrimaryKeys,
            TargetOptionPositionsUpdateKeys, TargetStockPositionsFullKeys,
            TargetStockPositionsPrimaryKeys, TargetStockPositionsUpdateKeys,
        },
        models_crud::target_positions::{
            target_option_positions::{TargetOptionPositionsCRUD, TargetOptionPositionsQtyDiff},
            target_stock_positions::{TargetStockPositionsCRUD, TargetStockPositionsQtyDiff},
        },
    },
    implement_crud_trait_for_interface,
};

#[derive(Debug, Clone)]
pub enum TargetPositionsCRUD {
    Stock(TargetStockPositionsCRUD),
    Options(TargetOptionPositionsCRUD),
}

#[derive(Debug, Clone)]
pub enum TargetPositionsFullKeys {
    Stock(TargetStockPositionsFullKeys),
    Options(TargetOptionPositionsFullKeys),
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for TargetPositionsFullKeys {
    fn from_row(_: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        // This will never be executed because the inner CRUD<FK,PK,UK>
        // decodes the inner concrete struct before wrapping it into this enum.
        Err(sqlx::Error::Decode(
            "TargetPositionsFullKeys cannot be decoded directly from a raw SQL row".into(),
        ))
    }
}

#[derive(Debug, Clone)]
pub enum TargetPositionsQtyDiff {
    Stock(TargetStockPositionsQtyDiff),
    Options(TargetOptionPositionsQtyDiff),
}

impl TargetPositionsQtyDiff {
    pub fn update_qty_diff(&mut self, qty_diff: f64) -> f64 {
        match self {
            Self::Stock(v) => { v.qty_diff += qty_diff; v.qty_diff }
            Self::Options(v) => { v.qty_diff += qty_diff; v.qty_diff }
        }
    }

    pub fn get_qty_diff(&self) -> f64 {
        match self {
            Self::Stock(TargetStockPositionsQtyDiff {qty_diff, ..}) => *qty_diff,
            Self::Options(TargetOptionPositionsQtyDiff {qty_diff, ..}) => *qty_diff
        }
    }
}

#[derive(Debug, Clone)]
pub enum TargetPositionsPrimaryKeys {
    Stock(TargetStockPositionsPrimaryKeys),
    Options(TargetOptionPositionsPrimaryKeys),
}

#[derive(Debug, Clone)]
pub enum TargetPositionsUpdateKeys {
    Stock(TargetStockPositionsUpdateKeys),
    Options(TargetOptionPositionsUpdateKeys),
}

impl TargetPositionsCRUD {
    fn get_pg_pool<'a>(&'a self) -> &'a PgPool {
        match self {
            Self::Stock(stk) => &stk.crud.pool,
            Self::Options(opt) => &opt.crud.pool,
        }
    }

    pub fn stock(pool: PgPool) -> Self {
        Self::Stock(TargetStockPositionsCRUD::new(pool))
    }

    pub fn option(pool: PgPool) -> Self {
        Self::Options(TargetOptionPositionsCRUD::new(pool))
    }

    pub fn from(asset_type: &AssetType, pool: PgPool) -> Self {
        match asset_type {
            AssetType::Stock
            | AssetType::Future
            | AssetType::CFD
            | AssetType::ForexPair
            | AssetType::CASH => Self::stock(pool),
            AssetType::Option => Self::option(pool),
            AssetType::Unknown => panic!("Tried to get CRUD instance from an Unknown Asset Type!"),
        }
    }
}

implement_crud_trait_for_interface!(
    TargetPositionsCRUD,
    TargetPositionsFullKeys,
    TargetPositionsPrimaryKeys,
    TargetPositionsUpdateKeys,
    [Stock, Options]
);

#[async_trait]
pub trait TargetPositionsOps {
    async fn get_target_pos_diff_by_pk(
        &self,
        pk: TargetPositionsPrimaryKeys,
    ) -> Result<Vec<TargetPositionsQtyDiff>, String>;
    async fn get_target_pos_diff_by_strat(
        &self,
        strategy: &str,
    ) -> Result<Vec<TargetPositionsQtyDiff>, String>;
    async fn clear_strat_pos(&self, strategy: &str) -> Result<(), String>;
}

#[async_trait]
impl TargetPositionsOps for TargetPositionsCRUD {
    async fn get_target_pos_diff_by_pk(
        &self,
        pk: TargetPositionsPrimaryKeys,
    ) -> Result<Vec<TargetPositionsQtyDiff>, String> {
        let result = match pk {
            TargetPositionsPrimaryKeys::Stock(TargetStockPositionsPrimaryKeys {
                strategy,
                primary_exchange,
                currency,
                stock,
            }) => sqlx::query_as!(
                TargetStockPositionsQtyDiff,
                r#"
                SELECT
                    COALESCE(t.stock, c.stock) AS "stock!",
                    COALESCE(t.primary_exchange, c.primary_exchange) AS "primary_exchange!",
                    COALESCE(t.currency, c.currency) AS "currency!",
                    COALESCE(t.strategy, c.strategy) AS "strategy!",
                    (COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0))::float8 AS "qty_diff!",
                    COALESCE(c.quantity, 0.0) AS "current_qty!",
                    COALESCE(t.avg_price, 0.0) AS "avg_price!"
                FROM trading.target_stock_positions t
                FULL OUTER JOIN trading.current_stock_positions  c
                    ON t.stock = c.stock 
                        AND t.primary_exchange = c.primary_exchange 
                        AND t.currency = c.currency  
                        AND t.strategy = c.strategy 
                WHERE COALESCE(t.strategy, c.strategy) = $1
                    AND COALESCE(t.stock, c.stock) = $2
                    AND COALESCE(t.primary_exchange, c.primary_exchange) = $3
                    AND COALESCE(t.currency, c.currency) = $4;
                "#,
                strategy,
                stock,
                primary_exchange,
                currency
            )
            .fetch_all(self.get_pg_pool())
            .await
            .map(|ok_res| {
                ok_res
                    .into_iter()
                    .map(TargetPositionsQtyDiff::Stock)
                    .collect()
            }),

            TargetPositionsPrimaryKeys::Options(TargetOptionPositionsPrimaryKeys {
                strategy,
                stock,
                primary_exchange,
                currency,
                expiry,
                strike,
                multiplier,
                option_type,
            }) => sqlx::query_as!(
                TargetOptionPositionsQtyDiff,
                r#"
                SELECT
                    COALESCE(t.stock, c.stock) AS "stock!",
                    COALESCE(t.primary_exchange, c.primary_exchange) AS "primary_exchange!",
                    COALESCE(t.currency, c.currency) AS "currency!",
                    COALESCE(t.expiry, c.expiry) AS "expiry!",
                    COALESCE(t.strike, c.strike) AS "strike!",
                    COALESCE(t.multiplier, c.multiplier) AS "multiplier!",
                    COALESCE(t.option_type, c.option_type) AS "option_type!:OptionType",
                    COALESCE(t.strategy, c.strategy) AS "strategy!",
                    (COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0))::float8 AS "qty_diff!",
                    COALESCE(c.quantity, 0.0) AS "current_qty!",
                    COALESCE(t.avg_price, 0.0) AS "avg_price!"
                FROM trading.target_option_positions t
                FULL OUTER JOIN trading.current_option_positions  c
                    ON t.stock = c.stock 
                    AND t.primary_exchange = c.primary_exchange
                    AND t.currency = c.currency
                    AND t.expiry = c.expiry 
                    AND t.strike = c.strike
                    AND t.multiplier = c.multiplier
                    AND t.option_type = c.option_type
                    AND t.strategy = c.strategy
                WHERE COALESCE(t.strategy, c.strategy) = $1
                    AND COALESCE(t.stock, c.stock) = $2
                    AND COALESCE(t.primary_exchange, c.primary_exchange) = $3
                    AND COALESCE(t.currency, c.currency) = $4
                    AND COALESCE(t.expiry, c.expiry) = $5
                    AND COALESCE(t.strike, c.strike) = $6
                    AND COALESCE(t.multiplier, c.multiplier) = $7
                    AND COALESCE(t.option_type, c.option_type) = $8::option_type;
                "#,
                strategy,
                stock,
                primary_exchange,
                currency,
                expiry,
                strike,
                multiplier,
                option_type as OptionType
            )
            .fetch_all(self.get_pg_pool())
            .await
            .map(|ok_res| {
                ok_res
                    .into_iter()
                    .map(TargetPositionsQtyDiff::Options)
                    .collect()
            }),
        };
        result.map_err(|e| {
            format!(
                "Error retrieving qty difference in stocks for strategy: {}",
                e
            )
        })
    }

    async fn get_target_pos_diff_by_strat(
        &self,
        strategy: &str,
    ) -> Result<Vec<TargetPositionsQtyDiff>, String> {
        let result = match self {
            Self::Stock(_) => sqlx::query_as!(
                TargetStockPositionsQtyDiff,
                r#"
                SELECT
                    COALESCE(t.stock, c.stock) AS "stock!",
                    COALESCE(t.primary_exchange, c.primary_exchange) AS "primary_exchange!",
                    COALESCE(t.currency, c.currency) AS "currency!",
                    COALESCE(t.strategy, c.strategy) AS "strategy!",
                    (COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0))::float8 AS "qty_diff!",
                    COALESCE(c.quantity, 0.0) AS "current_qty!",
                    COALESCE(t.avg_price, 0.0) AS "avg_price!"
                FROM trading.target_stock_positions t
                FULL OUTER JOIN trading.current_stock_positions  c
                    ON t.stock = c.stock 
                        AND t.primary_exchange = c.primary_exchange 
                        AND t.currency = c.currency  
                        AND t.strategy = c.strategy 
                WHERE COALESCE(t.strategy, c.strategy) = $1;
                "#,
                strategy,
            )
            .fetch_all(self.get_pg_pool())
            .await
            .map(|ok_res| {
                ok_res
                    .into_iter()
                    .map(TargetPositionsQtyDiff::Stock)
                    .collect()
            }),

            Self::Options(_) => sqlx::query_as!(
                TargetOptionPositionsQtyDiff,
                r#"
                SELECT
                    COALESCE(t.stock, c.stock) AS "stock!",
                    COALESCE(t.primary_exchange, c.primary_exchange) AS "primary_exchange!",
                    COALESCE(t.currency, c.currency) AS "currency!",
                    COALESCE(t.expiry, c.expiry) AS "expiry!",
                    COALESCE(t.strike, c.strike) AS "strike!",
                    COALESCE(t.multiplier, c.multiplier) AS "multiplier!",
                    COALESCE(t.option_type, c.option_type) AS "option_type!:OptionType",
                    COALESCE(t.strategy, c.strategy) AS "strategy!",
                    COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0) AS "qty_diff!",
                    COALESCE(c.quantity, 0.0) AS "current_qty!",
                    COALESCE(t.avg_price, 0.0) AS "avg_price!"
                FROM trading.target_option_positions t
                FULL OUTER JOIN trading.current_option_positions  c
                    ON t.stock = c.stock 
                    AND t.primary_exchange = c.primary_exchange
                    AND t.currency = c.currency
                    AND t.expiry = c.expiry 
                    AND t.strike = c.strike
                    AND t.multiplier = c.multiplier
                    AND t.option_type = c.option_type
                    AND t.strategy = c.strategy
                WHERE COALESCE(t.strategy, c.strategy) = $1;
                "#,
                strategy,
            )
            .fetch_all(self.get_pg_pool())
            .await
            .map(|ok_res| {
                ok_res
                    .into_iter()
                    .map(TargetPositionsQtyDiff::Options)
                    .collect()
            }),
        };
        result.map_err(|e| {
            format!(
                "Error retrieving qty difference in stocks for strategy: {}",
                e
            )
        })
    }

    async fn clear_strat_pos(&self, strategy: &str) -> Result<(), String> {
        let result = match self {
            Self::Stock(_) => {
                sqlx::query!(
                    r#"
                    DELETE
                    FROM trading.target_stock_positions t
                    WHERE strategy = $1;
                    "#,
                    strategy,
                )
                .execute(self.get_pg_pool())
                .await
            }
            Self::Options(_) => {
                sqlx::query!(
                    r#"
                    DELETE
                    FROM trading.target_option_positions t
                    WHERE strategy = $1;
                    "#,
                    strategy,
                )
                .execute(self.get_pg_pool())
                .await
            }
        };
        result.map_err(|e| {
            format!(
                "Error retrieving qty difference in stocks for strategy: {}",
                e
            )
        })?;
        Ok(())
    }
}
