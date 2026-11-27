//! `market_data.historical_data` — stock OHLCV bars (TimescaleDB hypertable).
//!
//! Primary key `(stock, primary_exchange, currency, time)`; `time` is UTC.
//! Rows are normally ingested in batches. An upsert needs a complete insert
//! image: Postgres validates `NOT NULL` on the proposed tuple before
//! `ON CONFLICT DO UPDATE`, so a partial UpdateKeys cannot create a bar.

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys,
            HistoricalStockDataUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

/// CRUD handle for `market_data.historical_data`.
///
/// Full [`CRUDTrait`](crate::database::crud::CRUDTrait) surface.
pub struct HistoricalStockDataCRUD<'a> {
    /// Generic CRUD bound to `market_data.historical_data`.
    pub(super) crud: CRUD<'a, 
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

#[hotpath::measure_all]
impl<'a> HistoricalStockDataCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held connection)
    /// for its lifetime and performs no I/O until a method is awaited.
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                HistoricalStockDataFullKeys,
                HistoricalStockDataPrimaryKeys,
                HistoricalStockDataUpdateKeys,
            >::new(exec, "market_data.historical_data"),
        }
    }
}
