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

impl TargetStockPositionsCRUD {
    pub(super) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                TargetStockPositionsFullKeys,
                TargetStockPositionsPrimaryKeys,
                TargetStockPositionsUpdateKeys,
            >::new(pool, String::from("trading.target_stock_positions")),
        }
    }
}
