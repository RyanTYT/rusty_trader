//! `trading.open_stock_orders` — stock orders currently working at the broker.
//!
//! Primary key `(order_perm_id, order_id)`. A row with `order_perm_id = -1`
//! is the optimistic row written immediately after submission; it is replaced
//! by the acknowledged row once IBKR assigns a permanent id. `strategy` is a
//! foreign key to `trading.strategy`; `filled` is cumulative and `executions`
//! holds the execution ids already applied.

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys},
    },
    implement_all_crud_methods,
};

/// CRUD handle for `trading.open_stock_orders`.
///
/// Full [`CRUDTrait`](crate::database::crud::CRUDTrait) surface.
pub struct OpenStockOrdersCRUD<'a> {
    /// Generic CRUD bound to `trading.open_stock_orders`.
    pub(super) crud:
        CRUD<'a, OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    OpenStockOrdersFullKeys,
    OpenStockOrdersPrimaryKeys,
    OpenStockOrdersUpdateKeys,
    OpenStockOrdersCRUD
);

#[hotpath::measure_all]
impl<'a> OpenStockOrdersCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held connection)
    /// for its lifetime and performs no I/O until a method is awaited.
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                OpenStockOrdersFullKeys,
                OpenStockOrdersPrimaryKeys,
                OpenStockOrdersUpdateKeys,
            >::new(exec, "trading.open_stock_orders"),
        }
    }
}
