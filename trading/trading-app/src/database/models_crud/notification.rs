use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys},
    },
    implement_all_crud_methods,
};

pub struct NotificationCRUD<'a> {
    pub(super) crud: CRUD<'a, NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    NotificationFullKeys,
    NotificationPrimaryKeys,
    NotificationUpdateKeys,
    NotificationCRUD
);

#[hotpath::measure_all]
impl<'a> NotificationCRUD<'a> {
    pub fn new(exec: Exec<'a>) -> Self {
        Self {
            crud:
                CRUD::<NotificationFullKeys, NotificationPrimaryKeys, NotificationUpdateKeys>::new(exec, "trading.notifications"),
        }
    }
}
