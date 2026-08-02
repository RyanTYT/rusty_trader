use sqlx::PgPool;

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

// #[derive(Debug, Clone, FromRow)]
// pub struct GroupedByStockOptional {
//     pub stock: Option<String>,
//     pub primary_exchange: Option<String>,
//     pub currency: Option<String>,
//     pub quantity: Option<f64>,
//     pub fx_avg_price: Option<f64>,
// }
//
// #[derive(Debug, Clone, FromRow)]
// pub struct GroupedByStock {
//     pub stock: String,
//     pub primary_exchange: String,
//     pub currency: String,
//     pub quantity: f64,
//     pub fx_avg_price: f64,
// }
//
// struct OptionCurrentStockPositionsFullKeys {
//     stock: Option<String>,
//     primary_exchange: Option<String>,
//     currency: Option<String>,
//     strategy: Option<String>,
//     quantity: Option<f64>,
//     avg_price: Option<f64>,
//     last_updated: Option<chrono::DateTime<Utc>>,
// }

#[derive(Debug, Clone)]
pub struct CurrentStockPositionsCRUD {
    pub(super) crud: CRUD<
        CurrentStockPositionsFullKeys,
        CurrentStockPositionsPrimaryKeys,
        CurrentStockPositionsUpdateKeys,
    >,
}

impl
    CRUDTrait<
        CurrentStockPositionsFullKeys,
        CurrentStockPositionsPrimaryKeys,
        CurrentStockPositionsUpdateKeys,
    > for CurrentStockPositionsCRUD
{
    delegate_all_crud_methods!(
        crud,
        CurrentStockPositionsFullKeys,
        CurrentStockPositionsPrimaryKeys,
        CurrentStockPositionsUpdateKeys
    );
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

    // pub async fn get_pos_by_strat_and_stock(
    //     &self,
    //     strategy: &str,
    //     stock: &str,
    //     primary_exchange: &str,
    // ) -> Result<Option<CurrentStockPositionsFullKeys>, String> {
    //     let pos = sqlx::query_as!(
    //         OptionCurrentStockPositionsFullKeys,
    //         r#"
    //         SELECT stock, primary_exchange, currency, strategy, quantity, avg_price, last_updated
    //         FROM trading.current_stock_positions
    //         WHERE strategy = $1
    //         AND stock = $2
    //         AND primary_exchange = $3;
    //         "#,
    //         strategy,
    //         stock,
    //         primary_exchange
    //     )
    //     .fetch_optional(&self.crud.pool)
    //     .await
    //     .map_err(|e| {
    //         format!(
    //             "Error occurred fetching local positions for strategy {}: {}",
    //             strategy, e
    //         )
    //     })?;
    //
    //     Ok(pos.map(|current_pos| CurrentStockPositionsFullKeys {
    //         stock: current_pos
    //             .stock
    //             .clone()
    //             .expect("Expected stock from returned row in get_pos_by_strat"),
    //         primary_exchange: current_pos
    //             .primary_exchange
    //             .clone()
    //             .expect("Expected stock from returned row in get_pos_by_strat"),
    //         currency: current_pos
    //             .currency
    //             .clone()
    //             .expect("Expected stock from returned row in get_pos_by_strat"),
    //         strategy: current_pos
    //             .strategy
    //             .clone()
    //             .expect("Expected strategy from returned row in get_pos_by_strat"),
    //         quantity: current_pos
    //             .quantity
    //             .clone()
    //             .expect("Expected quantity from returned row in get_pos_by_strat"),
    //         avg_price: current_pos
    //             .avg_price
    //             .clone()
    //             .expect("Expected avg_price from returned row in get_pos_by_strat"),
    //         last_updated: current_pos
    //             .last_updated
    //             .clone()
    //             .expect("Expected last_updated from returned row in get_pos_by_strat"),
    //     }))
    // }

    // pub async fn get_pos_by_strat(
    //     &self,
    //     strategy: &str,
    // ) -> Result<Vec<CurrentStockPositionsFullKeys>, String> {
    //     let pos = sqlx::query_as!(
    //         OptionCurrentStockPositionsFullKeys,
    //         r#"
    //         SELECT stock, primary_exchange, currency, strategy, quantity, avg_price, last_updated
    //         FROM trading.current_stock_positions
    //         WHERE strategy = $1;
    //         "#,
    //         &strategy
    //     )
    //     .fetch_all(&self.crud.pool)
    //     .await
    //     .map_err(|e| {
    //         format!(
    //             "Error occurred fetching local positions for strategy {}: {}",
    //             strategy, e
    //         )
    //     })?;
    //
    //     Ok(pos
    //         .iter()
    //         .map(|current_pos| CurrentStockPositionsFullKeys {
    //             stock: current_pos
    //                 .stock
    //                 .clone()
    //                 .expect("Expected stock from returned row in get_pos_by_strat"),
    //             primary_exchange: current_pos
    //                 .primary_exchange
    //                 .clone()
    //                 .expect("Expected primary_exchange from returned row in get_pos_by_strat"),
    //             currency: current_pos
    //                 .currency
    //                 .clone()
    //                 .expect("Expected currency from returned row in get_pos_by_strat"),
    //             strategy: current_pos
    //                 .strategy
    //                 .clone()
    //                 .expect("Expected strategy from returned row in get_pos_by_strat"),
    //             quantity: current_pos
    //                 .quantity
    //                 .clone()
    //                 .expect("Expected quantity from returned row in get_pos_by_strat"),
    //             avg_price: current_pos
    //                 .avg_price
    //                 .clone()
    //                 .expect("Expected avg_price from returned row in get_pos_by_strat"),
    //             last_updated: current_pos
    //                 .last_updated
    //                 .clone()
    //                 .expect("Expected last_updated from returned row in get_pos_by_strat"),
    //         })
    //         .collect())
    // }

    // pub async fn get_all_positions_by_stock(&self) -> Result<Vec<GroupedByStock>, String> {
    //     let rows = sqlx::query_as!(
    //         GroupedByStockOptional,
    //         r#"
    //         SELECT stock, primary_exchange, currency, SUM(quantity) AS quantity, AVG(avg_price) as fx_avg_price
    //         FROM trading.current_stock_positions
    //         GROUP BY stock, primary_exchange, currency;
    //         "#,
    //     )
    //     .fetch_all(&self.crud.pool)
    //     .await
    //     .map_err(|e| {
    //         format!(
    //             "Error when fetching rows for CurrentStockPositions in get_all_positions: {}",
    //             e
    //         )
    //     })?;
    //
    //     Ok(rows
    //         .iter()
    //         .map(|v| GroupedByStock {
    //             stock: v
    //                 .stock
    //                 .clone()
    //                 .expect("Expected stock in group by clause in get_all_positions_by_stock"),
    //             currency: v
    //                 .currency
    //                 .clone()
    //                 .expect("Expected currency in group by clause in get_all_positions_by_stock"),
    //             primary_exchange: v.primary_exchange.clone().expect(
    //                 "Expected primary_exchange group by clause in get_all_positions_by_stock",
    //             ),
    //             quantity: v
    //                 .quantity
    //                 .expect("Expected quantity in group by clause in get_all_positions_by_stock"),
    //             fx_avg_price: v
    //                 .fx_avg_price
    //                 .expect("Expected quantity in group by clause in get_all_positions_by_stock"),
    //         })
    //         .collect())
    // }

    // pub async fn update_strat_positions(
    //     &self,
    //     stock: &str,
    //     primary_exchange: &str,
    //     strategy: &str,
    //     currency: &str,
    //     qty: &f64,
    //     avg_price: Option<f64>,
    // ) -> Result<(), String> {
    //     sqlx::query!(
    //         r#"
    //         INSERT INTO trading.current_stock_positions (
    //             strategy,
    //             stock,
    //             primary_exchange,
    //             currency,
    //             quantity,
    //             avg_price
    //         )
    //         VALUES ($1, $2, $3, $4, $5, $6)
    //         ON CONFLICT (stock, primary_exchange, currency, strategy)
    //         DO UPDATE SET
    //         avg_price = CASE
    //             -- Avoid division by zero if total quantity becomes 0
    //             WHEN (current_stock_positions.quantity + EXCLUDED.quantity) = 0 THEN 0
    //             ELSE (
    //                 (current_stock_positions.quantity * current_stock_positions.avg_price) +
    //                 (EXCLUDED.quantity * EXCLUDED.avg_price)
    //             ) / (current_stock_positions.quantity + EXCLUDED.quantity)
    //         END,
    //         quantity = current_stock_positions.quantity + EXCLUDED.quantity;
    //         "#,
    //         strategy,
    //         stock,
    //         primary_exchange,
    //         currency,
    //         qty,
    //         avg_price.unwrap_or(0.0)
    //     )
    //     .execute(&self.crud.pool)
    //     .await
    //     .map_err(|e| {
    //         format!(
    //             "Error when updating unknown strategy in stock positions: {}",
    //             e
    //         )
    //     })?;
    //
    //     Ok(())
    // }
}
