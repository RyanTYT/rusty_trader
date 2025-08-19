use sqlx::PgPool;

use crate::database::{
    crud::{CRUD, CRUDTrait},
    models::{
        StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys,
    },
};

pub fn get_staged_commissions_crud(
    pool: PgPool,
) -> CRUD<StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys> {
    CRUD::<
        StagedCommissionsFullKeys,
        StagedCommissionsPrimaryKeys,
        StagedCommissionsUpdateKeys
    >::new(pool, String::from("trading.staged_commissions"))
}
