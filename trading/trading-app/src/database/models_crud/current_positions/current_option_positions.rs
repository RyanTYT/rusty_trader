//! `trading.current_option_positions` — settled option positions per strategy.
//!
//! Primary key adds the option identity (`expiry`, `strike`, `multiplier`,
//! `option_type`) to the stock keys. Quantities are signed and `avg_price` is
//! weighted; unlike the stock table, the option additive upsert has no
//! sign-flip special case.

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys,
            CurrentOptionPositionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

/// CRUD handle for `trading.current_option_positions`.
///
/// Full [`CRUDTrait`](crate::database::crud::CRUDTrait) surface.
pub struct CurrentOptionPositionsCRUD<'a> {
    /// Generic CRUD bound to `trading.current_option_positions`.
    pub(super) crud: CRUD<'a, 
        CurrentOptionPositionsFullKeys,
        CurrentOptionPositionsPrimaryKeys,
        CurrentOptionPositionsUpdateKeys,
    >,
}

implement_all_crud_methods!(
    crud,
    CurrentOptionPositionsFullKeys,
    CurrentOptionPositionsPrimaryKeys,
    CurrentOptionPositionsUpdateKeys,
    CurrentOptionPositionsCRUD
);

#[hotpath::measure_all]
impl<'a> CurrentOptionPositionsCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held connection)
    /// for its lifetime and performs no I/O until a method is awaited.
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                CurrentOptionPositionsFullKeys,
                CurrentOptionPositionsPrimaryKeys,
                CurrentOptionPositionsUpdateKeys,
            >::new(exec, "trading.current_option_positions"),
        }
    }
}
