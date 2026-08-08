use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{
            HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys,
            HistoricalStockDataUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct HistoricalStockDataCRUD {
    pub(super) crud: CRUD<
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

impl HistoricalStockDataCRUD {
    pub(super) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                HistoricalStockDataFullKeys,
                HistoricalStockDataPrimaryKeys,
                HistoricalStockDataUpdateKeys,
            >::new(pool, String::from("market_data.historical_data")),
        }
    }
}
