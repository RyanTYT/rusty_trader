//! `trading.stock_transactions` — one row per stock execution.
//!
//! Primary key `execution_id` (IBKR's id, unique). Rows are effectively
//! append-only: the execution handler inserts them once and afterwards only
//! `fees` is updated by the commission triggers. `quantity` is signed from
//! the execution side (`BOT` positive, otherwise negative); `price` and
//! `quantity` are double precision while `fees` is numeric.

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            StockTransactionsFullKeys, StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

/// Build a raw [`CRUD`] for `trading.stock_transactions`.
///
/// Used where the table wrapper adds nothing, for example when the
/// transactions interface needs a `CRUD` to dispatch on directly.
pub fn get_stock_transactions_crud<'a>(
    exec: Exec<'a>,
) -> CRUD<'a, StockTransactionsFullKeys, StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys>
{
    CRUD::<
        StockTransactionsFullKeys,
        StockTransactionsPrimaryKeys,
        StockTransactionsUpdateKeys,
    >::new(exec, "trading.stock_transactions")
}

/// CRUD handle for `trading.stock_transactions`.
///
/// Full [`CRUDTrait`](crate::database::crud::CRUDTrait) surface.
pub struct StockTransactionsCRUD<'a> {
    /// Generic CRUD bound to `trading.stock_transactions`.
    pub(super) crud:
        CRUD<'a, StockTransactionsFullKeys, StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys>,
}

/// Instrument identity used by the latest-transaction query.
#[derive(Debug, Clone, PartialEq)]
pub struct StockTransactionsUnderlyingPrimaryKeys {
    /// Local symbol.
    pub stock: String,
    /// Listing exchange.
    pub primary_exchange: String,
    /// Trading currency.
    pub currency: String,
}

implement_all_crud_methods!(
    crud,
    StockTransactionsFullKeys,
    StockTransactionsPrimaryKeys,
    StockTransactionsUpdateKeys,
    StockTransactionsCRUD
);

#[hotpath::measure_all]
impl<'a> StockTransactionsCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held connection)
    /// for its lifetime and performs no I/O until a method is awaited.
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                StockTransactionsFullKeys,
                StockTransactionsPrimaryKeys,
                StockTransactionsUpdateKeys,
            >::new(exec, "trading.stock_transactions"),
        }
    }
}
