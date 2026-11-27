//! `trading.target_stock_positions` — the strategy's desired stock exposure.
//!
//! Primary key `(strategy, stock, primary_exchange, currency)`. The engine
//! compares this with current positions and working orders to decide what to
//! place or cancel; `quantity` is the target, not a delta.

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            TargetStockPositionsFullKeys, TargetStockPositionsPrimaryKeys,
            TargetStockPositionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

/// CRUD handle for `trading.target_stock_positions`.
///
/// Full [`CRUDTrait`](crate::database::crud::CRUDTrait) surface.
pub struct TargetStockPositionsCRUD<'a> {
    /// Generic CRUD bound to `trading.target_stock_positions`.
    pub(super) crud: CRUD<'a, 
        TargetStockPositionsFullKeys,
        TargetStockPositionsPrimaryKeys,
        TargetStockPositionsUpdateKeys,
    >,
}

/// Target-minus-current quantity for one stock exposure.
///
/// Produced by the target-diff queries; `qty_diff` is a signed delta, not a
/// target quantity.
#[derive(Debug, Clone, PartialEq)]
pub struct TargetStockPositionsQtyDiff {
    /// Owning strategy.
    pub strategy: String,
    /// Listing exchange of the instrument.
    pub primary_exchange: String,
    /// Trading currency.
    pub currency: String,
    /// Local symbol.
    pub stock: String,
    /// Target average price (0.0 when there is no target row).
    pub avg_price: f64,
    /// Signed `target - current` quantity.
    pub qty_diff: f64,
    /// Settled current quantity.
    pub current_qty: f64,
}

implement_all_crud_methods!(
    crud,
    TargetStockPositionsFullKeys,
    TargetStockPositionsPrimaryKeys,
    TargetStockPositionsUpdateKeys,
    TargetStockPositionsCRUD
);

#[hotpath::measure_all]
impl<'a> TargetStockPositionsCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held connection)
    /// for its lifetime and performs no I/O until a method is awaited.
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                TargetStockPositionsFullKeys,
                TargetStockPositionsPrimaryKeys,
                TargetStockPositionsUpdateKeys,
            >::new(exec, "trading.target_stock_positions"),
        }
    }
}
