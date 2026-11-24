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

pub struct StockTransactionsCRUD<'a> {
    pub(super) crud:
        CRUD<'a, StockTransactionsFullKeys, StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys>,
}

#[derive(Debug, Clone)]
pub struct StockTransactionsUnderlyingPrimaryKeys {
    pub stock: String,
    pub primary_exchange: String,
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
