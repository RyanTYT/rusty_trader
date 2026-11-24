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

pub struct OptionTransactionsCRUD<'a> {
    pub(super) crud: CRUD<'a, 
        OptionTransactionsFullKeys,
        OptionTransactionsPrimaryKeys,
        OptionTransactionsUpdateKeys,
    >,
}

#[derive(Debug, Clone)]
pub struct OptionTransactionsUnderlyingPrimaryKeys {
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
    pub expiry: String,
    pub strike: f64,
    pub multiplier: String,
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
