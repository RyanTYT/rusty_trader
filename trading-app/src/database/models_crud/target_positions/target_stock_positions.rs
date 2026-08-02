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
    pub(super) crud: CRUD<
        TargetStockPositionsFullKeys,
        TargetStockPositionsPrimaryKeys,
        TargetStockPositionsUpdateKeys,
    >,
}

impl
    CRUDTrait<
        TargetStockPositionsFullKeys,
        TargetStockPositionsPrimaryKeys,
        TargetStockPositionsUpdateKeys,
    > for TargetStockPositionsCRUD
{
    delegate_all_crud_methods!(
        crud,
        TargetStockPositionsFullKeys,
        TargetStockPositionsPrimaryKeys,
        TargetStockPositionsUpdateKeys
    );
}

// struct OptionalQtyDiff {
//     stock: Option<String>,
//     primary_exchange: Option<String>,
//     currency: Option<String>,
//     strategy: Option<String>,
//     qty_diff: Option<f64>,
//     current_qty: Option<f64>,
//     avg_price: Option<f64>,
// }
//
// #[derive(Debug, Clone)]
// pub struct QtyDiff {
//     pub stock: String,
//     pub primary_exchange: String,
//     pub currency: String,
//     pub strategy: String,
//     pub qty_diff: f64,
//     pub current_qty: f64,
//     pub avg_price: f64,
// }

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

    // pub async fn get_target_pos_diff(
    //     &self,
    //     strategy: &str,
    //     stock: &str,
    //     primary_exchange: &str,
    //     currency: &str,
    // ) -> Result<Vec<QtyDiff>, String> {
    //     let qty_diff = sqlx::query_as!(
    //         OptionalQtyDiff,
    //         r#"
    //         SELECT
    //             COALESCE(t.stock, c.stock) AS stock,
    //             COALESCE(t.primary_exchange, c.primary_exchange) AS primary_exchange,
    //             COALESCE(t.currency, c.currency) AS currency,
    //             COALESCE(t.strategy, c.strategy) AS strategy,
    //             (COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0))::float8 AS "qty_diff!",
    //             COALESCE(c.quantity, 0) AS current_qty,
    //             COALESCE(t.avg_price, 0.0) AS avg_price
    //         FROM trading.target_stock_positions t
    //         FULL OUTER JOIN trading.current_stock_positions  c
    //             ON t.stock = c.stock AND t.primary_exchange = c.primary_exchange AND t.currency = c.currency  AND t.strategy = c.strategy 
    //         WHERE COALESCE(t.strategy, c.strategy) = $1
    //             AND COALESCE(t.stock, c.stock) = $2
    //             AND COALESCE(t.primary_exchange, c.primary_exchange) = $3
    //             AND COALESCE(t.currency, c.currency) = $4;
    //         "#,
    //         strategy,
    //         stock,
    //         primary_exchange,
    //         currency
    //     )
    //     .fetch_all(&self.crud.pool)
    //     .await
    //     .map_err(|e| {
    //         format!(
    //             "Error retrieving qty difference in stocks for strategy: {}",
    //             e
    //         )
    //     })?;
    //
    //     Ok(qty_diff
    //         .iter()
    //         .map(|v| QtyDiff {
    //             stock: v
    //                 .stock
    //                 .clone()
    //                 .expect("Expected stock for get_target_pos_diff"),
    //             primary_exchange: v
    //                 .primary_exchange
    //                 .clone()
    //                 .expect("Expected primary_exchange for get_target_pos_diff"),
    //             currency: v
    //                 .currency
    //                 .clone()
    //                 .expect("Expected currency for get_target_pos_diff"),
    //             strategy: v
    //                 .strategy
    //                 .clone()
    //                 .expect("Expected strategy for get_target_pos_diff"),
    //             qty_diff: v
    //                 .qty_diff
    //                 .clone()
    //                 .expect("Expected qty_diff for get_target_pos_diff"),
    //             current_qty: v
    //                 .current_qty
    //                 .clone()
    //                 .expect("Expected current_qty for get_target_pos_diff"),
    //             avg_price: v
    //                 .avg_price
    //                 .clone()
    //                 .expect("Expected avg_price for get_target_pos_diff"),
    //         })
    //         .filter(|pos_diff| pos_diff.qty_diff != 0.0)
    //         .collect())
    // }
    //
    // pub async fn get_target_pos_diff_strat(&self, strategy: &str) -> Result<Vec<QtyDiff>, String> {
    //     let qty_diff = sqlx::query_as!(
    //         OptionalQtyDiff,
    //         r#"
    //         SELECT
    //             COALESCE(t.stock, c.stock) AS stock,
    //             COALESCE(t.primary_exchange, c.primary_exchange) AS primary_exchange,
    //             COALESCE(t.currency, c.currency) AS currency,
    //             COALESCE(t.strategy, c.strategy) AS strategy,
    //             (COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0))::float8 AS "qty_diff!",
    //             COALESCE(c.quantity, 0) AS current_qty,
    //             COALESCE(t.avg_price, 0.0) AS avg_price
    //         FROM trading.target_stock_positions t
    //         FULL OUTER JOIN trading.current_stock_positions  c
    //             ON t.stock = c.stock AND t.primary_exchange = c.primary_exchange AND t.currency = c.currency AND t.strategy = c.strategy
    //         WHERE COALESCE(t.strategy, c.strategy) = $1;
    //         "#,
    //         strategy,
    //     )
    //     .fetch_all(&self.crud.pool)
    //     .await
    //     .map_err(|e| {
    //         format!(
    //             "Error retrieving qty difference in stocks for strategy: {}",
    //             e
    //         )
    //     })?;
    //
    //     Ok(qty_diff
    //         .iter()
    //         .map(|v| QtyDiff {
    //             stock: v
    //                 .stock
    //                 .clone()
    //                 .expect("Expected stock for get_target_pos_diff"),
    //             primary_exchange: v
    //                 .primary_exchange
    //                 .clone()
    //                 .expect("Expected primary_exchange for get_target_pos_diff"),
    //             currency: v
    //                 .currency
    //                 .clone()
    //                 .expect("Expected currency for get_target_pos_diff"),
    //             strategy: v
    //                 .strategy
    //                 .clone()
    //                 .expect("Expected strategy for get_target_pos_diff"),
    //             qty_diff: v
    //                 .qty_diff
    //                 .clone()
    //                 .expect("Expected qty_diff for get_target_pos_diff"),
    //             current_qty: v
    //                 .current_qty
    //                 .clone()
    //                 .expect("Expected current_qty for get_target_pos_diff"),
    //             avg_price: v
    //                 .avg_price
    //                 .clone()
    //                 .expect("Expected avg_price for get_target_pos_diff"),
    //         })
    //         .filter(|pos_diff| pos_diff.qty_diff != 0.0)
    //         .collect())
    // }
    //
    // pub async fn delete_strat_pos(&self, strategy: &str) -> Result<(), String> {
    //     sqlx::query!(
    //         r#"
    //         DELETE
    //         FROM trading.target_stock_positions t
    //         WHERE strategy = $1;
    //         "#,
    //         strategy,
    //     )
    //     .execute(&self.crud.pool)
    //     .await
    //     .map_err(|e| {
    //         format!(
    //             "Error retrieving qty difference in stocks for strategy: {}",
    //             e
    //         )
    //     })?;
    //     Ok(())
    // }
}
