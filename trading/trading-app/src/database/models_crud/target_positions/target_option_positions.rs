use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{
            OptionType, TargetOptionPositionsFullKeys, TargetOptionPositionsPrimaryKeys,
            TargetOptionPositionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct TargetOptionPositionsCRUD {
    pub(super) crud: CRUD<
        TargetOptionPositionsFullKeys,
        TargetOptionPositionsPrimaryKeys,
        TargetOptionPositionsUpdateKeys,
    >,
}

#[derive(Debug, Clone)]
pub struct TargetOptionPositionsQtyDiff {
    pub strategy: String,
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
    pub expiry: String,
    pub strike: f64,
    pub multiplier: String,
    pub option_type: OptionType,
    pub avg_price: f64,
    pub qty_diff: f64,
    pub current_qty: f64,
}

implement_all_crud_methods!(
    crud,
    TargetOptionPositionsFullKeys,
    TargetOptionPositionsPrimaryKeys,
    TargetOptionPositionsUpdateKeys,
    TargetOptionPositionsCRUD
);

impl TargetOptionPositionsCRUD {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                TargetOptionPositionsFullKeys,
                TargetOptionPositionsPrimaryKeys,
                TargetOptionPositionsUpdateKeys,
            >::new(pool, String::from("trading.target_option_positions")),
        }
    }
}
