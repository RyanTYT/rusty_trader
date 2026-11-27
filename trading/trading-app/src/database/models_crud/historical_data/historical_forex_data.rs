//! `market_data.historical_forex_data` — forex bid/ask bars.
//!
//! Primary key `(pair, time)`; `pair` is the IBKR pair symbol and `time` is
//! UTC. Bid and ask OHLC fields are nullable because a pair may carry only
//! one side.

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeys,
            HistoricalForexDataUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

/// CRUD handle for `market_data.historical_forex_data`.
///
/// Full [`CRUDTrait`](crate::database::crud::CRUDTrait) surface.
pub struct HistoricalForexDataCRUD<'a> {
    /// Generic CRUD bound to `market_data.historical_forex_data`.
    pub(super) crud: CRUD<'a, 
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

#[hotpath::measure_all]
impl<'a> HistoricalForexDataCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held connection)
    /// for its lifetime and performs no I/O until a method is awaited.
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                HistoricalForexDataFullKeys,
                HistoricalForexDataPrimaryKeys,
                HistoricalForexDataUpdateKeys,
            >::new(exec, "market_data.historical_forex_data"),
        }
    }
}
