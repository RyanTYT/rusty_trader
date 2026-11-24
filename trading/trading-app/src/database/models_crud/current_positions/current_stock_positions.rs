use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys,
            CurrentStockPositionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

pub struct CurrentStockPositionsCRUD<'a> {
    pub(super) crud: CRUD<'a, 
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

#[hotpath::measure_all]
impl<'a> CurrentStockPositionsCRUD<'a> {
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                CurrentStockPositionsFullKeys,
                CurrentStockPositionsPrimaryKeys,
                CurrentStockPositionsUpdateKeys,
            >::new(exec, "trading.current_stock_positions"),
        }
    }
}
