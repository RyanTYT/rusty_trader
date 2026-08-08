use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys},
    },
    implement_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct NotificationCRUD {
    pub(super) crud: CRUD<NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    NotificationFullKeys,
    NotificationPrimaryKeys,
    NotificationUpdateKeys,
    NotificationCRUD
);

impl NotificationCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud:
                CRUD::<NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys>::new(
                    pool,
                    String::from("trading.notifications"),
                ),
        }
    }
}
