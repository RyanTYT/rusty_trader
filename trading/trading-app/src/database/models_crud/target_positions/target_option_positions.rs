use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            OptionType, TargetOptionPositionsFullKeys, TargetOptionPositionsPrimaryKeys,
            TargetOptionPositionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

pub struct TargetOptionPositionsCRUD<'a> {
    pub(super) crud: CRUD<'a, 
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

#[hotpath::measure_all]
impl<'a> TargetOptionPositionsCRUD<'a> {
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                TargetOptionPositionsFullKeys,
                TargetOptionPositionsPrimaryKeys,
                TargetOptionPositionsUpdateKeys,
            >::new(exec, "trading.target_option_positions"),
        }
    }
}
