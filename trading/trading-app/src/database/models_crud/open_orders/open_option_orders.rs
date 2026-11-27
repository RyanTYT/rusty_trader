//! `trading.open_option_orders` — option orders currently working at the broker.
//!
//! Same lifecycle and keys as
//! [`open_stock_orders`](super::open_stock_orders), plus the option identity
//! (`expiry`, `strike`, `multiplier`, `option_type`). `order_perm_id = -1`
//! marks the optimistic row.

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

/// CRUD handle for `trading.open_option_orders`.
///
/// Full [`CRUDTrait`](crate::database::crud::CRUDTrait) surface.
pub struct OpenOptionOrdersCRUD<'a> {
    /// Generic CRUD bound to `trading.open_option_orders`.
    pub(super) crud:
        CRUD<'a, OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    OpenOptionOrdersFullKeys,
    OpenOptionOrdersPrimaryKeys,
    OpenOptionOrdersUpdateKeys,
    OpenOptionOrdersCRUD
);

#[hotpath::measure_all]
impl<'a> OpenOptionOrdersCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held connection)
    /// for its lifetime and performs no I/O until a method is awaited.
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                OpenOptionOrdersFullKeys,
                OpenOptionOrdersPrimaryKeys,
                OpenOptionOrdersUpdateKeys,
            >::new(exec, "trading.open_option_orders"),
        }
    }
}
