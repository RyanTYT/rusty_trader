//! `market_data.historical_options_data` — option OHLCV bars.
//!
//! Primary key adds the option identity to the stock bar keys; `time` is UTC.
//! Batch ingest is the normal path; partial upserts cannot satisfy the
//! `NOT NULL` pre-check (see
//! [`historical_stock_data`](super::historical_stock_data)).

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

/// CRUD handle for `market_data.historical_options_data`.
///
/// Full [`CRUDTrait`](crate::database::crud::CRUDTrait) surface.
pub struct HistoricalOptionsDataCRUD<'a> {
    /// Generic CRUD bound to `market_data.historical_options_data`.
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
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held connection)
    /// for its lifetime and performs no I/O until a method is awaited.
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
