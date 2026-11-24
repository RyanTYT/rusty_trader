use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            HistoricalOptionsDataFullKeys, HistoricalOptionsDataPrimaryKeys,
            HistoricalOptionsDataUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

pub struct HistoricalOptionsDataCRUD<'a> {
    pub(super) crud: CRUD<'a, 
        HistoricalOptionsDataFullKeys,
        HistoricalOptionsDataPrimaryKeys,
        HistoricalOptionsDataUpdateKeys,
    >,
}

implement_all_crud_methods!(
    crud,
    HistoricalOptionsDataFullKeys,
    HistoricalOptionsDataPrimaryKeys,
    HistoricalOptionsDataUpdateKeys,
    HistoricalOptionsDataCRUD
);

#[hotpath::measure_all]
impl<'a> HistoricalOptionsDataCRUD<'a> {
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                HistoricalOptionsDataFullKeys,
                HistoricalOptionsDataPrimaryKeys,
                HistoricalOptionsDataUpdateKeys,
            >::new(exec, "market_data.historical_options_data"),
        }
    }
}
