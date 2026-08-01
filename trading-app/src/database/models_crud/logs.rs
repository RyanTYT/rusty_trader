use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys},
    },
    delegate_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct LogsCRUD {
    pub(super) crud: CRUD<LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys>,
}

impl CRUDTrait<LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys> for LogsCRUD {
    delegate_all_crud_methods!(crud, LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys);
}

impl LogsCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys>::new(
                pool,
                String::from("logs.logs"),
            ),
        }
    }
}
