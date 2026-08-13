use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{
            DailyHistoricalStockDataFullKeys, DailyHistoricalStockDataPrimaryKeys,
            DailyHistoricalStockDataUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct DailyHistoricalStockDataCRUD {
    pub(super) crud: CRUD<
        DailyHistoricalStockDataFullKeys,
        DailyHistoricalStockDataPrimaryKeys,
        DailyHistoricalStockDataUpdateKeys,
    >,
}

implement_all_crud_methods!(
    crud,
    DailyHistoricalStockDataFullKeys,
    DailyHistoricalStockDataPrimaryKeys,
    DailyHistoricalStockDataUpdateKeys,
    DailyHistoricalStockDataCRUD
);

impl DailyHistoricalStockDataCRUD {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                DailyHistoricalStockDataFullKeys,
                DailyHistoricalStockDataPrimaryKeys,
                DailyHistoricalStockDataUpdateKeys,
            >::new(pool, String::from("market_data.daily_ohlcv")),
        }
    }
}
