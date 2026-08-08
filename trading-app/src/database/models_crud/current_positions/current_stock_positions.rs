use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{
            CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys,
            CurrentStockPositionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct CurrentStockPositionsCRUD {
    pub(super) crud: CRUD<
        CurrentStockPositionsFullKeys,
        CurrentStockPositionsPrimaryKeys,
        CurrentStockPositionsUpdateKeys,
    >,
}

implement_all_crud_methods!(
    crud,
    CurrentStockPositionsFullKeys,
    CurrentStockPositionsPrimaryKeys,
    CurrentStockPositionsUpdateKeys,
    CurrentStockPositionsCRUD
);

impl CurrentStockPositionsCRUD {
    pub(super) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                CurrentStockPositionsFullKeys,
                CurrentStockPositionsPrimaryKeys,
                CurrentStockPositionsUpdateKeys,
            >::new(pool, String::from("trading.current_stock_positions")),
        }
    }
}
