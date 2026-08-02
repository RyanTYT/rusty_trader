use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct StagedCommissionsCRUD {
    pub(super) crud:
        CRUD<StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys>,
}

impl CRUDTrait<StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys>
    for StagedCommissionsCRUD
{
    delegate_all_crud_methods!(
        crud,
        StagedCommissionsFullKeys,
        StagedCommissionsPrimaryKeys,
        StagedCommissionsUpdateKeys
    );
}

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
