use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys},
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct OpenStockOrdersCRUD {
    pub(super) crud:
        CRUD<OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys>,
}

impl CRUDTrait<OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, OpenStockOrdersUpdateKeys>
    for OpenStockOrdersCRUD
{
    delegate_all_crud_methods!(
        crud,
        OpenStockOrdersFullKeys,
        OpenStockOrdersPrimaryKeys,
        OpenStockOrdersUpdateKeys
    );
}

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
