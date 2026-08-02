use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            TargetOptionPositionsFullKeys, TargetOptionPositionsPrimaryKeys,
            TargetOptionPositionsUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct TargetOptionPositionsCRUD {
    pub(super) crud: CRUD<
        TargetOptionPositionsFullKeys,
        TargetOptionPositionsPrimaryKeys,
        TargetOptionPositionsUpdateKeys,
    >,
}

impl
    CRUDTrait<
        TargetOptionPositionsFullKeys,
        TargetOptionPositionsPrimaryKeys,
        TargetOptionPositionsUpdateKeys,
    > for TargetOptionPositionsCRUD
{
    delegate_all_crud_methods!(
        crud,
        TargetOptionPositionsFullKeys,
        TargetOptionPositionsPrimaryKeys,
        TargetOptionPositionsUpdateKeys
    );
}

impl TargetOptionPositionsCRUD {
    pub(super) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                TargetOptionPositionsFullKeys,
                TargetOptionPositionsPrimaryKeys,
                TargetOptionPositionsUpdateKeys,
            >::new(pool, String::from("trading.target_option_positions")),
        }
    }
}
