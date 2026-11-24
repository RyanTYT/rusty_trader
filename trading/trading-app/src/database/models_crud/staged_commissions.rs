use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

pub struct StagedCommissionsCRUD<'a> {
    pub(super) crud:
        CRUD<'a, StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    StagedCommissionsFullKeys,
    StagedCommissionsPrimaryKeys,
    StagedCommissionsUpdateKeys,
    StagedCommissionsCRUD
);

#[hotpath::measure_all]
impl<'a> StagedCommissionsCRUD<'a> {
    pub fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                StagedCommissionsFullKeys,
                StagedCommissionsPrimaryKeys,
                StagedCommissionsUpdateKeys,
            >::new(exec, "trading.staged_commissions"),
        }
    }
}
