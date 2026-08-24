use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{
            HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeys,
            HistoricalForexDataUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct HistoricalForexDataCRUD {
    pub(super) crud: CRUD<
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

impl HistoricalForexDataCRUD {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                HistoricalForexDataFullKeys,
                HistoricalForexDataPrimaryKeys,
                HistoricalForexDataUpdateKeys,
            >::new(pool, String::from("market_data.historical_forex_data")),
        }
    }
}
