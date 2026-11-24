use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys},
    },
    implement_all_crud_methods,
};

pub struct OpenStockOrdersCRUD<'a> {
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
