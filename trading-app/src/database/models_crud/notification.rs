use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys},
    },
    delegate_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct NotificationCRUD {
    pub(super) crud: CRUD<NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys>,
}

impl CRUDTrait<NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys>
    for NotificationCRUD
{
    delegate_all_crud_methods!(
        crud,
        NotificationFullKeys,
        NotificationPrimaryKeys,
        NotificationUpdateKeys
    );
}

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
