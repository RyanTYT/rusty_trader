use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeys,
            HistoricalForexDataUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

pub struct HistoricalForexDataCRUD<'a> {
    pub(super) crud: CRUD<'a, 
        HistoricalForexDataFullKeys,
        HistoricalForexDataPrimaryKeys,
        HistoricalForexDataUpdateKeys,
    >,
}

implement_all_crud_methods!(
    crud,
    HistoricalForexDataFullKeys,
    HistoricalForexDataPrimaryKeys,
    HistoricalForexDataUpdateKeys,
    HistoricalForexDataCRUD
);

#[hotpath::measure_all]
impl<'a> HistoricalForexDataCRUD<'a> {
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                HistoricalForexDataFullKeys,
                HistoricalForexDataPrimaryKeys,
                HistoricalForexDataUpdateKeys,
            >::new(exec, "market_data.historical_forex_data"),
        }
    }
}
