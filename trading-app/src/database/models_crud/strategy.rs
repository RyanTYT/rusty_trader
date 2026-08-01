use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys},
    },
    delegate_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct StrategyCRUD {
    pub(super) crud: CRUD<StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys>,
}

impl CRUDTrait<StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys> for StrategyCRUD {
    delegate_all_crud_methods!(
        crud,
        StrategyFullKeys,
        StrategyPrimaryKeys,
        StrategyUpdateKeys
    );
}

impl StrategyCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys>::new(
                pool,
                String::from("trading.strategy"),
            ),
        }
    }
}
