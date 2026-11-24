use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys,
            CurrentOptionPositionsUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

pub struct CurrentOptionPositionsCRUD<'a> {
    pub(super) crud: CRUD<'a, 
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

#[hotpath::measure_all]
impl<'a> CurrentOptionPositionsCRUD<'a> {
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                CurrentOptionPositionsFullKeys,
                CurrentOptionPositionsPrimaryKeys,
                CurrentOptionPositionsUpdateKeys,
            >::new(exec, "trading.current_option_positions"),
        }
    }
}
