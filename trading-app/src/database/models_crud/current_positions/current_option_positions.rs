use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys,
            CurrentOptionPositionsUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct CurrentOptionPositionsCRUD {
    pub(super) crud: CRUD<
        CurrentOptionPositionsFullKeys,
        CurrentOptionPositionsPrimaryKeys,
        CurrentOptionPositionsUpdateKeys,
    >,
}

impl
    CRUDTrait<
        CurrentOptionPositionsFullKeys,
        CurrentOptionPositionsPrimaryKeys,
        CurrentOptionPositionsUpdateKeys,
    > for CurrentOptionPositionsCRUD
{
    delegate_all_crud_methods!(
        crud,
        CurrentOptionPositionsFullKeys,
        CurrentOptionPositionsPrimaryKeys,
        CurrentOptionPositionsUpdateKeys
    );
}

impl CurrentOptionPositionsCRUD {
    pub(super) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                CurrentOptionPositionsFullKeys,
                CurrentOptionPositionsPrimaryKeys,
                CurrentOptionPositionsUpdateKeys,
            >::new(pool, String::from("trading.current_option_positions")),
        }
    }
}
