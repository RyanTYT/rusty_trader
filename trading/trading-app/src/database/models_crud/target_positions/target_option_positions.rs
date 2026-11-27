//! `trading.target_option_positions` — the strategy's desired option exposure.
//!
//! Primary key adds the option identity to the stock target keys. `quantity`
//! is the target, not a delta.

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            OptionType, TargetOptionPositionsFullKeys, TargetOptionPositionsPrimaryKeys,
            TargetOptionPositionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

/// CRUD handle for `trading.target_option_positions`.
///
/// Full [`CRUDTrait`](crate::database::crud::CRUDTrait) surface.
pub struct TargetOptionPositionsCRUD<'a> {
    /// Generic CRUD bound to `trading.target_option_positions`.
    pub(super) crud: CRUD<'a, 
        TargetOptionPositionsFullKeys,
        TargetOptionPositionsPrimaryKeys,
        TargetOptionPositionsUpdateKeys,
    >,
}

/// Target-minus-current quantity for one option exposure.
///
/// Same delta semantics as the stock variant, plus the option identity.
#[derive(Debug, Clone, PartialEq)]
pub struct TargetOptionPositionsQtyDiff {
    /// Owning strategy.
    pub strategy: String,
    /// Underlying symbol.
    pub stock: String,
    /// Listing exchange of the underlying.
    pub primary_exchange: String,
    /// Trading currency.
    pub currency: String,
    /// Expiry as `YYYYMMDD`.
    pub expiry: String,
    /// Strike price.
    pub strike: f64,
    /// Contract multiplier (usually `"100"`).
    pub multiplier: String,
    /// Call or put.
    pub option_type: OptionType,
    /// Target average price (0.0 when there is no target row).
    pub avg_price: f64,
    /// Signed `target - current` quantity.
    pub qty_diff: f64,
    /// Settled current quantity.
    pub current_qty: f64,
}

implement_all_crud_methods!(
    crud,
    TargetOptionPositionsFullKeys,
    TargetOptionPositionsPrimaryKeys,
    TargetOptionPositionsUpdateKeys,
    TargetOptionPositionsCRUD
);

#[hotpath::measure_all]
impl<'a> TargetOptionPositionsCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held connection)
    /// for its lifetime and performs no I/O until a method is awaited.
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                TargetOptionPositionsFullKeys,
                TargetOptionPositionsPrimaryKeys,
                TargetOptionPositionsUpdateKeys,
            >::new(exec, "trading.target_option_positions"),
        }
    }
}
