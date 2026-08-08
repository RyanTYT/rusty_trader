use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys, CancelledOrdersUpdateKeys},
    },
    implement_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct CancelledOrdersCRUD {
    pub(super) crud:
        CRUD<CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys, CancelledOrdersUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    CancelledOrdersFullKeys,
    CancelledOrdersPrimaryKeys,
    CancelledOrdersUpdateKeys,
    CancelledOrdersCRUD
);

impl CancelledOrdersCRUD {
    pub fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                CancelledOrdersFullKeys,
                CancelledOrdersPrimaryKeys,
                CancelledOrdersUpdateKeys,
            >::new(pool, String::from("logs.cancelled_orders")),
        }
    }
}
