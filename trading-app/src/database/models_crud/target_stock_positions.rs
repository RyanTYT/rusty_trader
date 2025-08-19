use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            TargetStockPositionsFullKeys, TargetStockPositionsPrimaryKeys,
            TargetStockPositionsUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct TargetStockPositionsCRUD {
    crud: CRUD<
        TargetStockPositionsFullKeys,
        TargetStockPositionsPrimaryKeys,
        TargetStockPositionsUpdateKeys,
    >,
}

struct OptionalQtyDiff {
    stock: Option<String>,
    primary_exchange: Option<String>,
    strategy: Option<String>,
    qty_diff: Option<f64>,
    avg_price: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct QtyDiff {
    pub stock: String,
    pub primary_exchange: String,
    pub strategy: String,
    pub qty_diff: f64,
    pub avg_price: f64,
}

impl TargetStockPositionsCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                TargetStockPositionsFullKeys,
                TargetStockPositionsPrimaryKeys,
                TargetStockPositionsUpdateKeys,
            >::new(pool, String::from("trading.target_stock_positions")),
        }
    }

    delegate_all_crud_methods!(
        crud,
        TargetStockPositionsFullKeys,
        TargetStockPositionsPrimaryKeys,
        TargetStockPositionsUpdateKeys
    );

    pub async fn get_target_pos_diff(
        &self,
        strategy: String,
        stock: String,
    ) -> Result<Vec<QtyDiff>, String> {
        let qty_diff = sqlx::query_as!(
            OptionalQtyDiff,
            r#"
            SELECT
                COALESCE(t.stock, c.stock) AS stock,
                COALESCE(t.primary_exchange, c.primary_exchange) AS primary_exchange,
                COALESCE(t.strategy, c.strategy) AS strategy,
                COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0) AS qty_diff,
                COALESCE(t.avg_price, 0.0) AS avg_price
            FROM trading.target_stock_positions t
            FULL OUTER JOIN trading.current_stock_positions  c
                ON t.stock = c.stock AND t.strategy = c.strategy
            WHERE COALESCE(t.strategy, c.strategy) = $1
                AND COALESCE(t.stock, c.stock) = $2;
            "#,
            strategy,
            stock
        )
        .fetch_all(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error retrieving qty difference in stocks for strategy: {}",
                e
            )
        })?;

        Ok(qty_diff
            .iter()
            .map(|v| QtyDiff {
                stock: v
                    .stock
                    .clone()
                    .expect("Expected stock for get_target_pos_diff"),
                primary_exchange: v
                    .primary_exchange
                    .clone()
                    .expect("Expected primary_exchange for get_target_pos_diff"),
                strategy: v
                    .strategy
                    .clone()
                    .expect("Expected strategy for get_target_pos_diff"),
                qty_diff: v
                    .qty_diff
                    .clone()
                    .expect("Expected qty_diff for get_target_pos_diff"),
                avg_price: v
                    .avg_price
                    .clone()
                    .expect("Expected avg_price for get_target_pos_diff"),
            })
            .collect())
    }

    pub async fn get_target_pos_diff_strat(
        &self,
        strategy: String,
    ) -> Result<Vec<QtyDiff>, String> {
        let qty_diff = sqlx::query_as!(
            OptionalQtyDiff,
            r#"
            SELECT
                COALESCE(t.stock, c.stock) AS stock,
                COALESCE(t.primary_exchange, c.primary_exchange) AS primary_exchange,
                COALESCE(t.strategy, c.strategy) AS strategy,
                COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0) AS qty_diff,
                COALESCE(t.avg_price, 0.0) AS avg_price
            FROM trading.target_stock_positions t
            FULL OUTER JOIN trading.current_stock_positions  c
                ON t.stock = c.stock AND t.strategy = c.strategy
            WHERE COALESCE(t.strategy, c.strategy) = $1;
            "#,
            strategy,
        )
        .fetch_all(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error retrieving qty difference in stocks for strategy: {}",
                e
            )
        })?;

        Ok(qty_diff
            .iter()
            .map(|v| QtyDiff {
                stock: v
                    .stock
                    .clone()
                    .expect("Expected stock for get_target_pos_diff"),
                primary_exchange: v
                    .primary_exchange
                    .clone()
                    .expect("Expected primary_exchange for get_target_pos_diff"),
                strategy: v
                    .strategy
                    .clone()
                    .expect("Expected strategy for get_target_pos_diff"),
                qty_diff: v
                    .qty_diff
                    .clone()
                    .expect("Expected qty_diff for get_target_pos_diff"),
                avg_price: v
                    .avg_price
                    .clone()
                    .expect("Expected avg_price for get_target_pos_diff"),
            })
            .collect())
    }
}

pub fn get_target_stock_positions_crud(
    pool: PgPool,
) -> CRUD<
    TargetStockPositionsFullKeys,
    TargetStockPositionsPrimaryKeys,
    TargetStockPositionsUpdateKeys,
> {
    // impl CurrentStockPositionsCRUD {}
    CRUD::<
        TargetStockPositionsFullKeys,
        TargetStockPositionsPrimaryKeys,
        TargetStockPositionsUpdateKeys,
    >::new(pool, String::from("trading.target_stock_positions"))
}

pub fn get_specific_target_stock_positions_crud(pool: PgPool) -> TargetStockPositionsCRUD {
    TargetStockPositionsCRUD::new(pool)
}
