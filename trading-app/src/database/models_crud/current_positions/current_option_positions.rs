use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{
            CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys,
            CurrentOptionPositionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct CurrentOptionPositionsCRUD {
    pub(super) crud: CRUD<
        CurrentOptionPositionsFullKeys,
        CurrentOptionPositionsPrimaryKeys,
        CurrentOptionPositionsUpdateKeys,
    >,
}

implement_all_crud_methods!(
    crud,
    CurrentOptionPositionsFullKeys,
    CurrentOptionPositionsPrimaryKeys,
    CurrentOptionPositionsUpdateKeys,
    CurrentOptionPositionsCRUD
);

impl CurrentOptionPositionsCRUD {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                CurrentOptionPositionsFullKeys,
                CurrentOptionPositionsPrimaryKeys,
                CurrentOptionPositionsUpdateKeys,
            >::new(pool, String::from("trading.current_option_positions")),
        }
    }
}
