use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            FractionalMomentumWeeklyPositionsFullKeys,
            FractionalMomentumWeeklyPositionsPrimaryKeys,
            FractionalMomentumWeeklyPositionsUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct FractionalMomentumWeeklyPositionsCRUD {
    crud: CRUD<
        FractionalMomentumWeeklyPositionsFullKeys,
        FractionalMomentumWeeklyPositionsPrimaryKeys,
        FractionalMomentumWeeklyPositionsUpdateKeys,
    >,
}

impl FractionalMomentumWeeklyPositionsCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                FractionalMomentumWeeklyPositionsFullKeys,
                FractionalMomentumWeeklyPositionsPrimaryKeys,
                FractionalMomentumWeeklyPositionsUpdateKeys,
            >::new(
                pool,
                String::from("trading.fractional_momentum_weekly_positions"),
            ),
        }
    }

    delegate_all_crud_methods!(
        crud,
        FractionalMomentumWeeklyPositionsFullKeys,
        FractionalMomentumWeeklyPositionsPrimaryKeys,
        FractionalMomentumWeeklyPositionsUpdateKeys
    );

    pub async fn clear_table(&self) -> Result<u64, String> {
        let delete_res = sqlx::query!(
            r#"
            DELETE FROM trading.fractional_momentum_weekly_positions;
            "#
        )
        .execute(&self.crud.pool)
        .await
        .map_err(|e| {
            format!("Error occurred trying to delete all rows from fractional_momentum_weekly_positions: {}", e)
        })?;
        Ok(delete_res.rows_affected())
    }
}

pub fn get_frac_mom_weekly_pos_crud(
    pool: PgPool,
) -> CRUD<
    FractionalMomentumWeeklyPositionsFullKeys,
    FractionalMomentumWeeklyPositionsPrimaryKeys,
    FractionalMomentumWeeklyPositionsUpdateKeys,
> {
    // impl CurrentStockPositionsCRUD {}
    CRUD::<
        FractionalMomentumWeeklyPositionsFullKeys,
        FractionalMomentumWeeklyPositionsPrimaryKeys,
        FractionalMomentumWeeklyPositionsUpdateKeys,
    >::new(
        pool,
        String::from("trading.fractional_momentum_weekly_positions"),
    )
}

pub fn get_specific_frac_mom_weekly_pos_crud(
    pool: PgPool,
) -> FractionalMomentumWeeklyPositionsCRUD {
    FractionalMomentumWeeklyPositionsCRUD::new(pool)
}
