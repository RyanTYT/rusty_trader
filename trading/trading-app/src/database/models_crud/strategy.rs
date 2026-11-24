use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys},
    },
    implement_all_crud_methods,
};

pub struct StrategyCRUD<'a> {
    pub(super) crud: CRUD<'a, StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    StrategyFullKeys,
    StrategyPrimaryKeys,
    StrategyUpdateKeys,
    StrategyCRUD
);

#[hotpath::measure_all]
impl<'a> StrategyCRUD<'a> {
    pub fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys>::new(exec, "trading.strategy"),
        }
    }
}
