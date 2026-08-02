use sqlx::{PgPool, postgres::PgRow, prelude::FromRow};

use crate::{
    database::{
        models::{
            CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys,
            CurrentOptionPositionsUpdateKeys, CurrentStockPositionsFullKeys,
            CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys, OptionType,
        },
        models_crud::current_positions::{
            current_option_positions::CurrentOptionPositionsCRUD,
            current_stock_positions::CurrentStockPositionsCRUD,
        },
    },
    implement_crud_trait_for_interface,
};

#[derive(Debug, Clone)]
pub enum CurrentPositionsCRUD {
    Options(CurrentOptionPositionsCRUD),
    Stock(CurrentStockPositionsCRUD),
}

#[derive(Debug, Clone)]
pub enum CurrentPositionsFullKeys {
    Options(CurrentOptionPositionsFullKeys),
    Stock(CurrentStockPositionsFullKeys),
}

impl<'r> FromRow<'r, PgRow> for CurrentPositionsFullKeys {
    fn from_row(_: &'r PgRow) -> Result<Self, sqlx::Error> {
        // This will never be executed because the inner CRUD<FK,PK,UK>
        // decodes the inner concrete struct before wrapping it into this enum.
        Err(sqlx::Error::Decode(
            "CurrentPositionsFullKeys cannot be decoded directly from a raw SQL row".into(),
        ))
    }
}

#[derive(Debug, Clone)]
pub enum CurrentPositionsPrimaryKeys {
    Options(CurrentOptionPositionsPrimaryKeys),
    Stock(CurrentStockPositionsPrimaryKeys),
}

#[derive(Debug, Clone)]
pub enum CurrentPositionsUpdateKeys {
    Options(CurrentOptionPositionsUpdateKeys),
    Stock(CurrentStockPositionsUpdateKeys),
}

impl CurrentPositionsCRUD {
    fn get_pg_pool<'a>(&'a self) -> &'a PgPool {
        match self {
            Self::Options(opt) => &opt.crud.pool,
            Self::Stock(stk) => &stk.crud.pool,
        }
    }
}

implement_crud_trait_for_interface!(
    CurrentPositionsCRUD,
    CurrentPositionsFullKeys,
    CurrentPositionsPrimaryKeys,
    CurrentPositionsUpdateKeys,
    [Stock, Options]
);

pub trait CurrentPositionsOps {
    async fn get_pos_by_strat(
        &self,
        strategy: &str,
    ) -> Result<Vec<CurrentPositionsFullKeys>, String>;
    /// Returns position if exists, else None
    /// - Note: Returns outer Result Err on sql fail, returns inner None when 404
    async fn get_pos_by_pk(
        &self,
        pk: CurrentPositionsPrimaryKeys,
    ) -> Result<Option<CurrentPositionsFullKeys>, String>;
    /// Returns all positions across the database grouped by Primary Keys
    async fn get_all_pos_grouped(&self) -> Result<Vec<CurrentPositionsFullKeys>, String>;
    /// Should insert into DB, and on conflict, add the position to the row
    /// - i.e. quantity is added + avg_price is updated accordingly
    async fn update_positions_additive(
        &self,
        pk: CurrentPositionsPrimaryKeys,
        uk: CurrentPositionsUpdateKeys,
    ) -> Result<(), String>;
}

impl CurrentPositionsOps for CurrentPositionsCRUD {
    async fn get_pos_by_strat(
        &self,
        strategy: &str,
    ) -> Result<Vec<CurrentPositionsFullKeys>, String> {
        let result = match self {
            Self::Stock(_) => sqlx::query_as!(
                CurrentStockPositionsFullKeys,
                r#"
                    SELECT 
                        stock as "stock!", 
                        primary_exchange as "primary_exchange!", 
                        currency as "currency!", 
                        strategy as "strategy!", 
                        quantity as "quantity!", 
                        avg_price as "avg_price!",
                        last_updated as "last_updated!"
                    FROM trading.current_stock_positions
                    WHERE strategy = $1;
                    "#,
                &strategy
            )
            .fetch_all(self.get_pg_pool())
            .await
            .map(|positions| {
                positions
                    .into_iter()
                    .map(CurrentPositionsFullKeys::Stock)
                    .collect()
            }),
            Self::Options(_) => sqlx::query_as!(
                CurrentOptionPositionsFullKeys,
                r#"
                    SELECT 
                        stock as "stock!", 
                        primary_exchange as "primary_exchange!", 
                        currency as "currency!", 
                        expiry as "expiry!",
                        strike as "strike!",
                        multiplier as "multiplier!",
                        option_type as "option_type!:OptionType",
                        strategy as "strategy!", 
                        quantity as "quantity!", 
                        avg_price as "avg_price!",
                        last_updated as "last_updated!"
                    FROM trading.current_option_positions
                    WHERE strategy = $1;
                    "#,
                &strategy
            )
            .fetch_all(self.get_pg_pool())
            .await
            .map(|positions| {
                positions
                    .into_iter()
                    .map(CurrentPositionsFullKeys::Options)
                    .collect()
            }),
        };

        result.map_err(|e| {
            format!(
                "Error occurred fetching local positions for strategy {}: {}",
                strategy, e
            )
        })
    }

    async fn get_pos_by_pk(
        &self,
        pk: CurrentPositionsPrimaryKeys,
    ) -> Result<Option<CurrentPositionsFullKeys>, String> {
        let result = match pk {
            CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
                stock,
                primary_exchange,
                currency,
                strategy,
            }) => sqlx::query_as!(
                CurrentStockPositionsFullKeys,
                r#"
                    SELECT 
                        stock as "stock!", 
                        primary_exchange as "primary_exchange!", 
                        currency as "currency!", 
                        strategy as "strategy!", 
                        quantity as "quantity!", 
                        avg_price as "avg_price!",
                        last_updated as "last_updated!"
                    FROM trading.current_stock_positions
                    WHERE strategy = $1
                        AND stock = $2
                        AND primary_exchange = $3
                        AND currency = $4;
                    "#,
                strategy,
                stock,
                primary_exchange,
                currency
            )
            .fetch_optional(self.get_pg_pool())
            .await
            .map(|ok_res| ok_res.map(CurrentPositionsFullKeys::Stock)),
            CurrentPositionsPrimaryKeys::Options(CurrentOptionPositionsPrimaryKeys {
                stock,
                primary_exchange,
                currency,
                strategy,
                expiry,
                strike,
                multiplier,
                option_type,
            }) => sqlx::query_as!(
                CurrentOptionPositionsFullKeys,
                r#"
                SELECT 
                    stock as "stock!", 
                    primary_exchange as "primary_exchange!", 
                    currency as "currency!", 
                    expiry as "expiry!",
                    strike as "strike!",
                    multiplier as "multiplier!",
                    option_type as "option_type!:OptionType",
                    strategy as "strategy!", 
                    quantity as "quantity!", 
                    avg_price as "avg_price!",
                    last_updated as "last_updated!"
                FROM trading.current_option_positions
                WHERE strategy = $1
                    AND stock = $2
                    AND primary_exchange = $3
                    AND currency = $4
                    AND expiry = $5
                    AND strike = $6
                    AND multiplier = $7
                    AND option_type = $8;
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
            .fetch_optional(self.get_pg_pool())
            .await
            .map(|ok_res| ok_res.map(CurrentPositionsFullKeys::Options)),
        };

        result.map_err(|e| {
            format!(
                "Error occurred fetching local positions by primary keys: {}",
                e
            )
        })
    }

    async fn get_all_pos_grouped(&self) -> Result<Vec<CurrentPositionsFullKeys>, String> {
        let result = match self {
            Self::Stock(_) => sqlx::query_as!(
                CurrentStockPositionsFullKeys,
                r#"
                    SELECT 
                        stock AS "stock!", 
                        primary_exchange AS "primary_exchange!", 
                        currency AS "currency!", 
                        COALESCE(SUM(quantity), 0) AS "quantity!", 
                        COALESCE(AVG(avg_price), 0) as "avg_price!",
                        COALESCE((ARRAY_AGG(strategy))[1], '') AS "strategy!",
                        (ARRAY_AGG(last_updated))[1]  AS "last_updated!"
                    FROM trading.current_stock_positions
                    GROUP BY stock, primary_exchange, currency;
                    "#,
            )
            .fetch_all(self.get_pg_pool())
            .await
            .map(|positions| {
                positions
                    .into_iter()
                    .map(CurrentPositionsFullKeys::Stock)
                    .collect()
            }),
            Self::Options(_) => sqlx::query_as!(
                CurrentOptionPositionsFullKeys,
                r#"
                    SELECT 
                        stock AS "stock!", 
                        primary_exchange AS "primary_exchange!", 
                        currency AS "currency!", 
                        expiry as "expiry!",
                        strike as "strike!",
                        multiplier as "multiplier!",
                        option_type as "option_type!:OptionType",
                        COALESCE(SUM(quantity), 0) AS "quantity!", 
                        COALESCE(AVG(avg_price), 0) as "avg_price!",
                        COALESCE((ARRAY_AGG(strategy))[1], '') AS "strategy!",
                        (ARRAY_AGG(last_updated))[1] AS "last_updated!"
                    FROM trading.current_option_positions
                    GROUP BY stock, primary_exchange, currency, expiry, strike, multiplier, option_type;
                    "#,
            )
            .fetch_all(self.get_pg_pool())
            .await
            .map(|positions| {
                positions
                    .into_iter()
                    .map(CurrentPositionsFullKeys::Options)
                    .collect()
            }),
        };

        result.map_err(|e| format!("Error occurred fetching all local positions grouped: {}", e))
    }

    async fn update_positions_additive(
        &self,
        pk: CurrentPositionsPrimaryKeys,
        uk: CurrentPositionsUpdateKeys,
    ) -> Result<(), String> {
        match (pk, uk) {
            (
                CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
                    stock,
                    primary_exchange,
                    currency,
                    strategy,
                }),
                CurrentPositionsUpdateKeys::Stock(CurrentStockPositionsUpdateKeys {
                    quantity,
                    avg_price,
                    last_updated: _,
                }),
            ) => {
                sqlx::query!(
                    r#"
                    INSERT INTO trading.current_stock_positions (
                        strategy, 
                        stock, 
                        primary_exchange,
                        currency,
                        quantity, 
                        avg_price
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (stock, primary_exchange, currency, strategy)
                    DO UPDATE SET 
                    avg_price = CASE 
                        -- Avoid division by zero if total quantity becomes 0
                        WHEN (current_stock_positions.quantity + EXCLUDED.quantity) = 0 THEN 0
                        ELSE (
                            (current_stock_positions.quantity * current_stock_positions.avg_price) + 
                            (EXCLUDED.quantity * EXCLUDED.avg_price)
                        ) / (current_stock_positions.quantity + EXCLUDED.quantity)
                    END,
                    quantity = current_stock_positions.quantity + EXCLUDED.quantity;
                    "#,
                    strategy,
                    stock,
                    primary_exchange,
                    currency,
                    quantity,
                    avg_price.unwrap_or(0.0)
                )
                .execute(self.get_pg_pool())
                .await
                .map_err(|e| {
                    format!(
                        "Error when updating strategy ({}) in stock positions: {}",
                        strategy, e
                    )
                })?;
                Ok(())
            }
            (
                CurrentPositionsPrimaryKeys::Options(CurrentOptionPositionsPrimaryKeys {
                    stock,
                    primary_exchange,
                    currency,
                    expiry,
                    strike,
                    multiplier,
                    option_type,
                    strategy,
                }),
                CurrentPositionsUpdateKeys::Options(CurrentOptionPositionsUpdateKeys {
                    quantity,
                    avg_price,
                    last_updated: _,
                }),
            ) => {
                sqlx::query!(
                    r#"
                    INSERT INTO trading.current_option_positions (
                        strategy, 
                        stock, 
                        primary_exchange,
                        currency,
                        expiry,
                        strike,
                        multiplier,
                        option_type,
                        quantity, 
                        avg_price
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (stock, primary_exchange, currency, expiry, strike, multiplier, option_type, strategy)
                    DO UPDATE SET 
                    avg_price = CASE 
                        -- Avoid division by zero if total quantity becomes 0
                        WHEN (current_option_positions.quantity + EXCLUDED.quantity) = 0 THEN 0
                        ELSE (
                            (current_option_positions.quantity * current_option_positions.avg_price) + 
                            (EXCLUDED.quantity * EXCLUDED.avg_price)
                        ) / (current_option_positions.quantity + EXCLUDED.quantity)
                    END,
                    quantity = current_option_positions.quantity + EXCLUDED.quantity;
                    "#,
                    strategy,
                    stock,
                    primary_exchange,
                    currency,
                    expiry,
                    strike,
                    multiplier,
                    option_type as OptionType,
                    quantity,
                    avg_price.unwrap_or(0.0)
                )
                .execute(self.get_pg_pool())
                .await
                .map_err(|e| {
                    format!(
                        "Error when updating strategy ({}) in stock positions: {}",
                        strategy, e
                    )
                })?;
                Ok(())
            }
            _ => {
                Err("pk enum and uk enum should be aligned when using update_positions".to_string())
            }
        }
    }
}
