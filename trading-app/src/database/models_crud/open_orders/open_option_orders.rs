use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct OpenOptionOrdersCRUD {
    pub(super) crud:
        CRUD<OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys>,
}

impl CRUDTrait<OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys>
    for OpenOptionOrdersCRUD
{
    delegate_all_crud_methods!(
        crud,
        OpenOptionOrdersFullKeys,
        OpenOptionOrdersPrimaryKeys,
        OpenOptionOrdersUpdateKeys
    );
}

impl OpenOptionOrdersCRUD {
    pub(super) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                OpenOptionOrdersFullKeys,
                OpenOptionOrdersPrimaryKeys,
                OpenOptionOrdersUpdateKeys,
            >::new(pool, String::from("trading.open_option_orders")),
        }
    }
}
