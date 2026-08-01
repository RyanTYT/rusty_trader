use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            DailyHistoricalStockDataFullKeys, DailyHistoricalStockDataPrimaryKeys,
            DailyHistoricalStockDataUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct DailyHistoricalStockDataCRUD {
    pub(super) crud: CRUD<
        DailyHistoricalStockDataFullKeys,
        DailyHistoricalStockDataPrimaryKeys,
        DailyHistoricalStockDataUpdateKeys,
    >,
}

impl
    CRUDTrait<
        DailyHistoricalStockDataFullKeys,
        DailyHistoricalStockDataPrimaryKeys,
        DailyHistoricalStockDataUpdateKeys,
    > for DailyHistoricalStockDataCRUD
{
    delegate_all_crud_methods!(
        crud,
        DailyHistoricalStockDataFullKeys,
        DailyHistoricalStockDataPrimaryKeys,
        DailyHistoricalStockDataUpdateKeys
    );
}

impl DailyHistoricalStockDataCRUD {
    async fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                DailyHistoricalStockDataFullKeys,
                DailyHistoricalStockDataPrimaryKeys,
                DailyHistoricalStockDataUpdateKeys,
            >::new(pool, String::from("market_data.daily_historical_data")),
        }
    }
}
