use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys, CancelledOrdersUpdateKeys},
    },
    delegate_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct CancelledOrdersCRUD {
    pub(super) crud:
        CRUD<CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys, CancelledOrdersUpdateKeys>,
}

impl CRUDTrait<CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys, CancelledOrdersUpdateKeys>
    for CancelledOrdersCRUD
{
    delegate_all_crud_methods!(
        crud,
        CancelledOrdersFullKeys,
        CancelledOrdersPrimaryKeys,
        CancelledOrdersUpdateKeys
    );
}

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
