use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys},
    },
    implement_all_crud_methods,
};

pub struct LogsCRUD<'a> {
    pub(super) crud: CRUD<'a, LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    LogsFullKeys,
    LogsPrimaryKeys,
    LogsUpdateKeys,
    LogsCRUD
);

#[hotpath::measure_all]
impl<'a> LogsCRUD<'a> {
    pub fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<LogsFullKeys, LogsPrimaryKeys, LogsUpdateKeys>::new(exec, "logs.logs"),
        }
    }
}
