use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{
            TargetStockPositionsFullKeys, TargetStockPositionsPrimaryKeys,
            TargetStockPositionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct TargetStockPositionsCRUD {
    pub(super) crud: CRUD<
        TargetStockPositionsFullKeys,
        TargetStockPositionsPrimaryKeys,
        TargetStockPositionsUpdateKeys,
    >,
}

#[derive(Debug, Clone)]
pub struct TargetStockPositionsQtyDiff {
    pub strategy: String,
    pub primary_exchange: String,
    pub currency: String,
    pub stock: String,
    pub avg_price: f64,
    pub qty_diff: f64,
    pub current_qty: f64,
}

implement_all_crud_methods!(
    crud,
    TargetStockPositionsFullKeys,
    TargetStockPositionsPrimaryKeys,
    TargetStockPositionsUpdateKeys,
    TargetStockPositionsCRUD
);

impl TargetStockPositionsCRUD {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                TargetStockPositionsFullKeys,
                TargetStockPositionsPrimaryKeys,
                TargetStockPositionsUpdateKeys,
            >::new(pool, String::from("trading.target_stock_positions")),
        }
    }
}
