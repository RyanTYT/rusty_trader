use sqlx::PgPool;

use crate::database::{
    crud::{CRUD, CRUDTrait},
    models::{StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys},
};

pub fn get_strategy_crud(
    pool: PgPool,
) -> CRUD<StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys> {
    CRUD::<StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys>::new(
        pool,
        String::from("trading.strategy"),
    )
}
