use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{
            HistoricalOptionsDataFullKeys, HistoricalOptionsDataPrimaryKeys,
            HistoricalOptionsDataUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct HistoricalOptionsDataCRUD {
    pub(super) crud: CRUD<
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

impl HistoricalOptionsDataCRUD {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                HistoricalOptionsDataFullKeys,
                HistoricalOptionsDataPrimaryKeys,
                HistoricalOptionsDataUpdateKeys,
            >::new(pool, String::from("market_data.historical_options_data")),
        }
    }
}
