use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeys,
            HistoricalForexDataUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct HistoricalForexDataCRUD {
    pub(super) crud: CRUD<
        HistoricalForexDataFullKeys,
        HistoricalForexDataPrimaryKeys,
        HistoricalForexDataUpdateKeys,
    >,
}

impl
    CRUDTrait<
        HistoricalForexDataFullKeys,
        HistoricalForexDataPrimaryKeys,
        HistoricalForexDataUpdateKeys,
    > for HistoricalForexDataCRUD
{
    delegate_all_crud_methods!(
        crud,
        HistoricalForexDataFullKeys,
        HistoricalForexDataPrimaryKeys,
        HistoricalForexDataUpdateKeys
    );
}

impl HistoricalForexDataCRUD {
    pub(super) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                HistoricalForexDataFullKeys,
                HistoricalForexDataPrimaryKeys,
                HistoricalForexDataUpdateKeys,
            >::new(pool, String::from("market_data.historical_forex_data")),
        }
    }
}
