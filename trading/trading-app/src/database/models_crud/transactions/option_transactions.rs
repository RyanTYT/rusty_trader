//! `trading.option_transactions` — one row per option execution.
//!
//! Primary key `execution_id`, plus the option identity columns. Same
//! append-only/fee-trigger behaviour and side-sign convention as
//! [`stock_transactions`](super::stock_transactions).

use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys,
            OptionTransactionsUpdateKeys, OptionType,
        },
    },
    implement_all_crud_methods,
};

/// CRUD handle for `trading.option_transactions`.
///
/// Full [`CRUDTrait`](crate::database::crud::CRUDTrait) surface.
pub struct OptionTransactionsCRUD<'a> {
    /// Generic CRUD bound to `trading.option_transactions`.
    pub(super) crud: CRUD<'a, 
        OptionTransactionsFullKeys,
        OptionTransactionsPrimaryKeys,
        OptionTransactionsUpdateKeys,
    >,
}

/// Option identity used by the latest-transaction query.
#[derive(Debug, Clone, PartialEq)]
pub struct OptionTransactionsUnderlyingPrimaryKeys {
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
}

implement_all_crud_methods!(
    crud,
    OptionTransactionsFullKeys,
    OptionTransactionsPrimaryKeys,
    OptionTransactionsUpdateKeys,
    OptionTransactionsCRUD
);

#[hotpath::measure_all]
impl<'a> OptionTransactionsCRUD<'a> {
    /// Bind a handle to `exec`.
    ///
    /// The handle borrows the session (pool, transaction, or held connection)
    /// for its lifetime and performs no I/O until a method is awaited.
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                OptionTransactionsFullKeys,
                OptionTransactionsPrimaryKeys,
                OptionTransactionsUpdateKeys,
            >::new(exec, "trading.option_transactions"),
        }
    }
}
