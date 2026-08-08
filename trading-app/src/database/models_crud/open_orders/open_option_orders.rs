use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{
            OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct OpenOptionOrdersCRUD {
    pub(super) crud:
        CRUD<OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    OpenOptionOrdersFullKeys,
    OpenOptionOrdersPrimaryKeys,
    OpenOptionOrdersUpdateKeys,
    OpenOptionOrdersCRUD
);

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
