use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys},
    },
    implement_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct LogsCRUD {
    pub(super) crud: CRUD<LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    LogsFullKeys,
    LogsPrimaryKeys,
    LogsUpdateKeys,
    LogsCRUD
);

impl LogsCRUD {
    pub fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys>::new(
                pool,
                String::from("logs.logs"),
            ),
        }
    }
}
