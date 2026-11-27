//! `trading.current_stock_positions` — settled stock positions per strategy.
//!
//! Primary key `(stock, primary_exchange, currency, strategy)`. `quantity` is
//! signed (negative = short) and `avg_price` is the weighted average. The
//! additive upsert keeps the existing average when an opposite order reduces
//! the position, but takes the incoming price when the incoming order is
//! larger and flips the side. `last_updated` is maintained by the database.

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys,
            CurrentStockPositionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

/// CRUD handle for `trading.current_stock_positions`.
///
/// Full [`CRUDTrait`](crate::database::crud::CRUDTrait) surface.
pub struct CurrentStockPositionsCRUD<'a> {
    /// Generic CRUD bound to `trading.current_stock_positions`.
    pub(super) crud: CRUD<'a, 
        CurrentStockPositionsFullKeys,
        CurrentStockPositionsPrimaryKeys,
        CurrentStockPositionsUpdateKeys,
    >,
}

implement_all_crud_methods!(
    crud,
    CurrentStockPositionsFullKeys,
    CurrentStockPositionsPrimaryKeys,
    CurrentStockPositionsUpdateKeys,
    CurrentStockPositionsCRUD
);

#[hotpath::measure_all]
impl<'a> CurrentStockPositionsCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held connection)
    /// for its lifetime and performs no I/O until a method is awaited.
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                CurrentStockPositionsFullKeys,
                CurrentStockPositionsPrimaryKeys,
                CurrentStockPositionsUpdateKeys,
            >::new(exec, "trading.current_stock_positions"),
        }
    }
}
