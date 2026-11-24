use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            TargetStockPositionsFullKeys, TargetStockPositionsPrimaryKeys,
            TargetStockPositionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

pub struct TargetStockPositionsCRUD<'a> {
    pub(super) crud: CRUD<'a, 
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

#[hotpath::measure_all]
impl<'a> TargetStockPositionsCRUD<'a> {
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                TargetStockPositionsFullKeys,
                TargetStockPositionsPrimaryKeys,
                TargetStockPositionsUpdateKeys,
            >::new(exec, "trading.target_stock_positions"),
        }
    }
}
