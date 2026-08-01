use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            HistoricalOptionsDataFullKeys, HistoricalOptionsDataPrimaryKeys,
            HistoricalOptionsDataUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct HistoricalOptionsDataCRUD {
    pub(super) crud: CRUD<
        HistoricalOptionsDataFullKeys,
        HistoricalOptionsDataPrimaryKeys,
        HistoricalOptionsDataUpdateKeys,
    >,
}

impl
    CRUDTrait<
        HistoricalOptionsDataFullKeys,
        HistoricalOptionsDataPrimaryKeys,
        HistoricalOptionsDataUpdateKeys,
    > for HistoricalOptionsDataCRUD
{
    delegate_all_crud_methods!(
        crud,
        HistoricalOptionsDataFullKeys,
        HistoricalOptionsDataPrimaryKeys,
        HistoricalOptionsDataUpdateKeys
    );
}

impl HistoricalOptionsDataCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                HistoricalOptionsDataFullKeys,
                HistoricalOptionsDataPrimaryKeys,
                HistoricalOptionsDataUpdateKeys,
            >::new(pool, String::from("market_data.historical_options_data")),
        }
    }
}
