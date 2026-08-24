use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUD,
        models::{StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys},
    },
    implement_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct StrategyCRUD {
    pub(super) crud: CRUD<StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    StrategyFullKeys,
    StrategyPrimaryKeys,
    StrategyUpdateKeys,
    StrategyCRUD
);

impl StrategyCRUD {
    pub fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys>::new(
                pool,
                String::from("trading.strategy"),
            ),
        }
    }
}
