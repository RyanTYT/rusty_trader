use sqlx::PgPool;

use crate::database::{
    crud::{CRUD, CRUDTrait},
    models::{NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys},
};

pub fn get_notification_crud(
    pool: PgPool,
) -> CRUD<NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys> {
    CRUD::<NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys>::new(
        pool,
        String::from("trading.notifications"),
    )
}
