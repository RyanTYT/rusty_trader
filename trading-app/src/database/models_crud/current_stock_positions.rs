use sqlx::{PgPool, prelude::FromRow};

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys,
            CurrentStockPositionsUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone, FromRow)]
pub struct GroupedByStockOptional {
    pub stock: Option<String>,
    pub primary_exchange: Option<String>,
    pub quantity: Option<f64>,
}

#[derive(Debug, Clone, FromRow)]
pub struct GroupedByStock {
    pub stock: String,
    pub primary_exchange: String,
    pub quantity: f64,
}

struct OptionCurrentStockPositionsFullKeys {
    stock: Option<String>,
    primary_exchange: Option<String>,
    strategy: Option<String>,
    quantity: Option<f64>,
    avg_price: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct CurrentStockPositionsCRUD {
    crud: CRUD<
        CurrentStockPositionsFullKeys,
        CurrentStockPositionsPrimaryKeys,
        CurrentStockPositionsUpdateKeys,
    >,
}
impl CurrentStockPositionsCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                CurrentStockPositionsFullKeys,
                CurrentStockPositionsPrimaryKeys,
                CurrentStockPositionsUpdateKeys,
            >::new(pool, String::from("trading.current_stock_positions")),
        }
    }

    delegate_all_crud_methods!(
        crud,
        CurrentStockPositionsFullKeys,
        CurrentStockPositionsPrimaryKeys,
        CurrentStockPositionsUpdateKeys
    );

    pub async fn get_pos_by_strat_and_stock(
        &self,
        strategy: &String,
        stock: &String,
        primary_exchange: &String,
    ) -> Result<Option<CurrentStockPositionsFullKeys>, String> {
        let pos = sqlx::query_as!(
            OptionCurrentStockPositionsFullKeys,
            r#"
            SELECT stock, primary_exchange, strategy, quantity, avg_price
            FROM trading.current_stock_positions
            WHERE strategy = $1
            AND stock = $2
            AND primary_exchange = $3;
            "#,
            strategy,
            stock,
            primary_exchange
        )
        .fetch_optional(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error occurred fetching local positions for strategy {}: {}",
                strategy, e
            )
        })?;

        Ok(pos.map(|current_pos| CurrentStockPositionsFullKeys {
            stock: current_pos
                .stock
                .clone()
                .expect("Expected stock from returned row in get_pos_by_strat"),
            primary_exchange: current_pos
                .primary_exchange
                .clone()
                .expect("Expected stock from returned row in get_pos_by_strat"),
            strategy: current_pos
                .strategy
                .clone()
                .expect("Expected strategy from returned row in get_pos_by_strat"),
            quantity: current_pos
                .quantity
                .clone()
                .expect("Expected quantity from returned row in get_pos_by_strat"),
            avg_price: current_pos
                .avg_price
                .clone()
                .expect("Expected avg_price from returned row in get_pos_by_strat"),
        }))
    }

    pub async fn get_pos_by_strat(
        &self,
        strategy: String,
    ) -> Result<Vec<CurrentStockPositionsFullKeys>, String> {
        let pos = sqlx::query_as!(
            OptionCurrentStockPositionsFullKeys,
            r#"
            SELECT stock, primary_exchange, strategy, quantity, avg_price
            FROM trading.current_stock_positions
            WHERE strategy = $1;
            "#,
            &strategy
        )
        .fetch_all(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error occurred fetching local positions for strategy {}: {}",
                strategy, e
            )
        })?;

        Ok(pos
            .iter()
            .map(|current_pos| CurrentStockPositionsFullKeys {
                stock: current_pos
                    .stock
                    .clone()
                    .expect("Expected stock from returned row in get_pos_by_strat"),
                primary_exchange: current_pos
                    .primary_exchange
                    .clone()
                    .expect("Expected stock from returned row in get_pos_by_strat"),
                strategy: current_pos
                    .strategy
                    .clone()
                    .expect("Expected strategy from returned row in get_pos_by_strat"),
                quantity: current_pos
                    .quantity
                    .clone()
                    .expect("Expected quantity from returned row in get_pos_by_strat"),
                avg_price: current_pos
                    .avg_price
                    .clone()
                    .expect("Expected avg_price from returned row in get_pos_by_strat"),
            })
            .collect())
    }

    pub async fn get_all_positions_by_stock(&self) -> Result<Vec<GroupedByStock>, String> {
        let rows = sqlx::query_as!(
            GroupedByStockOptional,
            r#"
            SELECT stock, primary_exchange, SUM(quantity) AS quantity
            FROM trading.current_stock_positions
            GROUP BY stock, primary_exchange;
            "#,
        )
        .fetch_all(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error when fetching rows for CurrentStockPositions in get_all_positions: {}",
                e
            )
        })?;

        Ok(rows
            .iter()
            .map(|v| GroupedByStock {
                stock: v
                    .stock
                    .clone()
                    .expect("Expected stock in group by clause in get_all_positions_by_stock"),
                primary_exchange: v.primary_exchange.clone().expect(
                    "Expected primary_exchange group by clause in get_all_positions_by_stock",
                ),
                quantity: v
                    .quantity
                    .expect("Expected quantity in group by clause in get_all_positions_by_stock"),
            })
            .collect())
    }

    pub async fn update_unknown_strat_positions(
        &self,
        stock: String,
        qty: f64,
    ) -> Result<(), String> {
        sqlx::query!(
            r#"
            INSERT INTO trading.current_stock_positions (
                strategy, 
                stock, 
                quantity, 
                avg_price
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (stock, strategy)
            DO UPDATE SET quantity = current_stock_positions.quantity + EXCLUDED.quantity;
            "#,
            "unknown",
            stock,
            qty,
            0.0
        )
        .execute(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error when updating unknown strategy in stock positions: {}",
                e
            )
        })?;

        Ok(())
    }
}

pub fn get_current_stock_positions_crud(
    pool: PgPool,
) -> CRUD<
    CurrentStockPositionsFullKeys,
    CurrentStockPositionsPrimaryKeys,
    CurrentStockPositionsUpdateKeys,
> {
    // impl CurrentStockPositionsCRUD {}
    CRUD::<
        CurrentStockPositionsFullKeys,
        CurrentStockPositionsPrimaryKeys,
        CurrentStockPositionsUpdateKeys,
    >::new(pool, String::from("trading.current_stock_positions"))
}

pub fn get_specific_current_stock_positions_crud(pool: PgPool) -> CurrentStockPositionsCRUD {
    CurrentStockPositionsCRUD::new(pool)
}
