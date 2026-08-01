use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys,
            HistoricalStockDataUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct HistoricalStockDataCRUD {
    pub(super) crud: CRUD<
        HistoricalStockDataFullKeys,
        HistoricalStockDataPrimaryKeys,
        HistoricalStockDataUpdateKeys,
    >,
}

impl
    CRUDTrait<
        HistoricalStockDataFullKeys,
        HistoricalStockDataPrimaryKeys,
        HistoricalStockDataUpdateKeys,
    > for HistoricalStockDataCRUD
{
    delegate_all_crud_methods!(
        crud,
        HistoricalStockDataFullKeys,
        HistoricalStockDataPrimaryKeys,
        HistoricalStockDataUpdateKeys
    );
}

impl HistoricalStockDataCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                HistoricalStockDataFullKeys,
                HistoricalStockDataPrimaryKeys,
                HistoricalStockDataUpdateKeys,
            >::new(pool, String::from("market_data.historical_data")),
        }
    }
}
