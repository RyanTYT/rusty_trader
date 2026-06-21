use sqlx::PgPool;

use crate::database::{
    crud::{CRUD, CRUDTrait},
    models::{LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys},
};

pub fn get_logs_crud(pool: PgPool) -> CRUD<LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys> {
    CRUD::<LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys>::new(pool, String::from("logs.logs"))
}
