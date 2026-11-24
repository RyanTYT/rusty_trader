use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys,
            HistoricalStockDataUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

pub struct HistoricalStockDataCRUD<'a> {
    pub(super) crud: CRUD<'a, 
        HistoricalStockDataFullKeys,
        HistoricalStockDataPrimaryKeys,
        HistoricalStockDataUpdateKeys,
    >,
}

implement_all_crud_methods!(
    crud,
    HistoricalStockDataFullKeys,
    HistoricalStockDataPrimaryKeys,
    HistoricalStockDataUpdateKeys,
    HistoricalStockDataCRUD
);

#[hotpath::measure_all]
impl<'a> HistoricalStockDataCRUD<'a> {
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                HistoricalStockDataFullKeys,
                HistoricalStockDataPrimaryKeys,
                HistoricalStockDataUpdateKeys,
            >::new(exec, "market_data.historical_data"),
        }
    }
}
