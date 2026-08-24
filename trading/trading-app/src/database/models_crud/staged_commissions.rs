use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{
            StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct StagedCommissionsCRUD {
    pub(super) crud:
        CRUD<StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    StagedCommissionsFullKeys,
    StagedCommissionsPrimaryKeys,
    StagedCommissionsUpdateKeys,
    StagedCommissionsCRUD
);

impl StagedCommissionsCRUD {
    pub fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                StagedCommissionsFullKeys,
                StagedCommissionsPrimaryKeys,
                StagedCommissionsUpdateKeys,
            >::new(pool, String::from("trading.staged_commissions")),
        }
    }
}
