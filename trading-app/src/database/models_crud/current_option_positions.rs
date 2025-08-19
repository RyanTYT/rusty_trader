use sqlx::{PgPool, prelude::FromRow};

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys,
            CurrentOptionPositionsUpdateKeys, OptionType,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone, FromRow)]
pub struct GroupedByContractOptional {
    pub stock: Option<String>,
    pub primary_exchange: Option<String>,
    pub expiry: Option<String>,
    pub strike: Option<f64>,
    pub multiplier: Option<String>,
    pub option_type: Option<OptionType>,
    pub quantity: Option<f64>,
}

#[derive(Debug, Clone, FromRow)]
pub struct GroupedByContract {
    pub stock: String,
    pub primary_exchange: String,
    pub expiry: String,
    pub strike: f64,
    pub multiplier: String,
    pub option_type: OptionType,
    pub quantity: f64,
}

pub struct CurrentOptionPositionsCRUD {
    crud: CRUD<
        CurrentOptionPositionsFullKeys,
        CurrentOptionPositionsPrimaryKeys,
        CurrentOptionPositionsUpdateKeys,
    >,
}
impl CurrentOptionPositionsCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                CurrentOptionPositionsFullKeys,
                CurrentOptionPositionsPrimaryKeys,
                CurrentOptionPositionsUpdateKeys,
            >::new(pool, String::from("trading.current_option_positions")),
        }
    }

    delegate_all_crud_methods!(
        crud,
        CurrentOptionPositionsFullKeys,
        CurrentOptionPositionsPrimaryKeys,
        CurrentOptionPositionsUpdateKeys
    );

    pub async fn get_all_positions_by_contract(&self) -> Result<Vec<GroupedByContract>, String> {
        let rows = sqlx::query_as!(
            GroupedByContractOptional,
            r#"
            SELECT stock, primary_exchange, expiry, strike, multiplier, option_type AS "option_type!:OptionType", SUM(quantity) AS quantity
            FROM trading.current_option_positions
            GROUP BY stock, primary_exchange, expiry, strike, multiplier, option_type;
            "#,
        )
        .fetch_all(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error when fetching rows for CurrentStockPositions in get_all_positions: {}",
                e
            )
        })?;
        Ok(rows
            .iter()
            .map(|v| GroupedByContract {
                stock: v
                    .stock
                    .clone()
                    .expect("Expected stock in group by clause in get_all_positions_by_contract"),
                primary_exchange: v
                    .primary_exchange
                    .clone()
                    .expect("Expected primary_exchange in group by clause in get_all_positions_by_contract"),
                quantity: v.quantity.clone().expect(
                    "Expected quantity in group by clause in get_all_positions_by_contract",
                ),
                expiry: v
                    .expiry
                    .clone()
                    .expect("Expected expiry in group by clause in get_all_positions_by_contract"),
                strike: v
                    .strike
                    .expect("Expected strike in group by clause in get_all_positions_by_contract"),
                multiplier: v.multiplier.clone().expect(
                    "Expected multiplier in group by clause in get_all_positions_by_contract",
                ),
                option_type: v.option_type.clone().expect(
                    "Expected option_type in group by clause in get_all_positions_by_contract",
                ),
            })
            .collect())
    }

    pub async fn update_unknown_strat_positions(
        &self,
        stock: String,
        primary_exchange: String,
        expiry: String,
        strike: f64,
        multiplier: String,
        option_type: OptionType,
        qty: f64,
    ) -> Result<(), String> {
        sqlx::query!(
            "
            INSERT INTO trading.current_option_positions (
                stock, 
                primary_exchange,
                strategy, 
                expiry, 
                strike, 
                multiplier, 
                option_type, 
                quantity, 
                avg_price
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (stock, primary_exchange, strategy, expiry, strike, multiplier, option_type)
            DO UPDATE SET quantity = current_option_positions.quantity + EXCLUDED.quantity;
            ",
            stock,
            primary_exchange,
            "unknown",
            expiry,
            strike,
            multiplier,
            option_type as OptionType,
            qty,
            0.0
        )
        .execute(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error when updating unknown strategy in option positions: {}",
                e
            )
        })?;
        Ok(())
    }
}

pub fn get_current_option_positions_crud(
    pool: PgPool,
) -> CRUD<
    CurrentOptionPositionsFullKeys,
    CurrentOptionPositionsPrimaryKeys,
    CurrentOptionPositionsUpdateKeys,
> {
    CRUD::<
        CurrentOptionPositionsFullKeys,
        CurrentOptionPositionsPrimaryKeys,
        CurrentOptionPositionsUpdateKeys,
    >::new(pool, String::from("trading.current_option_positions"))
}

pub fn get_specific_current_option_positions_crud(pool: PgPool) -> CurrentOptionPositionsCRUD {
    CurrentOptionPositionsCRUD::new(pool)
}
