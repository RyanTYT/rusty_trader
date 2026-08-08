use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys},
    },
    implement_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct OpenStockOrdersCRUD {
    pub(super) crud:
        CRUD<OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    OpenStockOrdersFullKeys,
    OpenStockOrdersPrimaryKeys,
    OpenStockOrdersUpdateKeys,
    OpenStockOrdersCRUD
);

impl OpenStockOrdersCRUD {
    pub(super) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                OpenStockOrdersFullKeys,
                OpenStockOrdersPrimaryKeys,
                OpenStockOrdersUpdateKeys,
            >::new(pool, String::from("trading.open_stock_orders")),
        }
    }
}
