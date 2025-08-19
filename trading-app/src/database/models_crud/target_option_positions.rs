use sqlx::PgPool;

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            OptionType, TargetOptionPositionsFullKeys, TargetOptionPositionsPrimaryKeys,
            TargetOptionPositionsUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct TargetOptionPositionsCRUD {
    crud: CRUD<
        TargetOptionPositionsFullKeys,
        TargetOptionPositionsPrimaryKeys,
        TargetOptionPositionsUpdateKeys,
    >,
}

struct OptionalQtyDiff {
    stock: Option<String>,
    primary_exchange: Option<String>,
    expiry: Option<String>,
    strike: Option<f64>,
    multiplier: Option<String>,
    option_type: Option<OptionType>,
    strategy: Option<String>,
    qty_diff: Option<f64>,
    avg_price: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct OptionQtyDiff {
    pub stock: String,
    pub primary_exchange: String,
    pub expiry: String,
    pub strike: f64,
    pub multiplier: String,
    pub option_type: OptionType,
    pub strategy: String,
    pub qty_diff: f64,
    pub avg_price: f64,
}

impl TargetOptionPositionsCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                TargetOptionPositionsFullKeys,
                TargetOptionPositionsPrimaryKeys,
                TargetOptionPositionsUpdateKeys,
            >::new(pool, String::from("trading.target_option_positions")),
        }
    }

    delegate_all_crud_methods!(
        crud,
        TargetOptionPositionsFullKeys,
        TargetOptionPositionsPrimaryKeys,
        TargetOptionPositionsUpdateKeys
    );

    pub async fn get_target_pos_diff(
        &self,
        strategy: String,
        stock: String,
        primary_exchange: String,
        expiry: String,
        strike: f64,
        multiplier: String,
        option_type: OptionType,
    ) -> Result<Vec<OptionQtyDiff>, String> {
        let qty_diff = sqlx::query_as!(
            OptionalQtyDiff,
            r#"
            SELECT
                COALESCE(t.stock, c.stock) AS stock,
                COALESCE(t.primary_exchange, c.primary_exchange) AS primary_exchange,
                COALESCE(t.expiry, c.expiry) AS expiry,
                COALESCE(t.strike, c.strike) AS strike,
                COALESCE(t.multiplier, c.multiplier) AS multiplier,
                COALESCE(t.option_type, c.option_type) AS "option_type!:OptionType",
                COALESCE(t.strategy, c.strategy) AS strategy,
                COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0) AS qty_diff,
                COALESCE(t.avg_price, 0.0) AS avg_price
            FROM trading.target_option_positions t
            FULL OUTER JOIN trading.current_option_positions  c
                ON t.stock = c.stock 
                AND t.primary_exchange = c.primary_exchange
                AND t.expiry = c.expiry 
                AND t.strike = c.strike
                AND t.multiplier = c.multiplier
                AND t.option_type = c.option_type
                AND t.strategy = c.strategy
            WHERE COALESCE(t.strategy, c.strategy) = $1
                AND COALESCE(t.stock, c.stock) = $2
                AND COALESCE(t.primary_exchange, c.primary_exchange) = $3
                AND COALESCE(t.expiry, c.expiry) = $4
                AND COALESCE(t.strike, c.strike) = $5
                AND COALESCE(t.multiplier, c.multiplier) = $6
                AND COALESCE(t.option_type, c.option_type) = $7::option_type;
            "#,
            strategy,
            stock,
            primary_exchange,
            expiry,
            strike,
            multiplier,
            option_type as OptionType
        )
        .fetch_all(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error retrieving qty difference in stocks for strategy: {}",
                e
            )
        })?;

        Ok(qty_diff
            .iter()
            .map(|v| OptionQtyDiff {
                stock: v
                    .stock
                    .clone()
                    .expect("Expected stock for get_target_pos_diff"),
                primary_exchange: v
                    .primary_exchange
                    .clone()
                    .expect("Expected primary_exchange for get_target_pos_diff"),
                expiry: v
                    .expiry
                    .clone()
                    .expect("Expected to be able to parse expiry"),
                strike: v.strike.expect("Expected to be able to parse strike"),
                multiplier: v
                    .multiplier
                    .clone()
                    .expect("Expected to be able to parse multiplier"),
                option_type: v
                    .option_type
                    .clone()
                    .expect("Expected to be able to parse option_type"),
                strategy: v
                    .strategy
                    .clone()
                    .expect("Expected strategy for get_target_pos_diff"),
                qty_diff: v
                    .qty_diff
                    .clone()
                    .expect("Expected qty_diff for get_target_pos_diff"),
                avg_price: v
                    .avg_price
                    .clone()
                    .expect("Expected avg_price for get_target_pos_diff"),
            })
            .collect())
    }
}

pub fn get_specific_target_option_positions_crud(pool: PgPool) -> TargetOptionPositionsCRUD {
    TargetOptionPositionsCRUD::new(pool)
}

pub fn get_target_option_positions_crud(
    pool: PgPool,
) -> CRUD<
    TargetOptionPositionsFullKeys,
    TargetOptionPositionsPrimaryKeys,
    TargetOptionPositionsUpdateKeys,
> {
    CRUD::<
        TargetOptionPositionsFullKeys,
        TargetOptionPositionsPrimaryKeys,
        TargetOptionPositionsUpdateKeys,
    >::new(pool, String::from("trading.target_option_positions"))
}
