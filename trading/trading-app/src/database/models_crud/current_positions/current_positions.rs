use ibapi::{contracts::Contract, orders::ExecutionData};
use sqlx::{postgres::PgRow, prelude::FromRow};

use crate::{
    database::{
        db::Exec,
        models::{
            AssetType, CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys,
            CurrentOptionPositionsUpdateKeys, CurrentStockPositionsFullKeys,
            CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys,
            OpenOptionOrdersFullKeys, OpenStockOrdersFullKeys, OptionType,
        },
        models_crud::{
            current_positions::{
                current_option_positions::CurrentOptionPositionsCRUD,
                current_stock_positions::CurrentStockPositionsCRUD,
            },
            open_orders::open_orders::OpenOrdersFullKeys,
        },
    },
    helpers::contract::get_local_symbol,
    implement_crud_trait_for_interface,
};

pub enum CurrentPositionsCRUD<'a> {
    Options(CurrentOptionPositionsCRUD<'a>),
    Stock(CurrentStockPositionsCRUD<'a>),
}

#[derive(Debug, Clone)]
pub enum CurrentPositionsFullKeys {
    Options(CurrentOptionPositionsFullKeys),
    Stock(CurrentStockPositionsFullKeys),
}

impl<'r> FromRow<'r, PgRow> for CurrentPositionsFullKeys {
    fn from_row(_: &'r PgRow) -> Result<Self, sqlx::Error> {
        // This will never be executed because the inner CRUD<FK,PK,UK>
        // decodes the inner concrete struct before wrapping it into this enum.
        Err(sqlx::Error::Decode(
            "CurrentPositionsFullKeys cannot be decoded directly from a raw SQL row".into(),
        ))
    }
}

impl CurrentPositionsFullKeys {
    pub fn get_qty(&self) -> f64 {
        match self {
            Self::Stock(stk) => stk.quantity,
            Self::Options(opt) => opt.quantity,
        }
    }

    pub fn get_currency(&self) -> String {
        match self {
            Self::Stock(fk) => fk.currency.clone(),
            Self::Options(fk) => fk.currency.clone(),
        }
    }

    fn get_avg_price(&self) -> f64 {
        match self {
            Self::Stock(stk) => stk.avg_price,
            Self::Options(opt) => opt.avg_price,
        }
    }

    fn set_avg_price(&mut self, avg_price: f64) {
        match self {
            Self::Stock(stk) => stk.avg_price = avg_price,
            Self::Options(opt) => opt.avg_price = avg_price,
        }
    }
}

#[derive(Debug, Clone)]
pub enum CurrentPositionsPrimaryKeys {
    Options(CurrentOptionPositionsPrimaryKeys),
    Stock(CurrentStockPositionsPrimaryKeys),
}

impl CurrentPositionsPrimaryKeys {
    //     pub fn from_strat_and_exec(strategy: &str, execution_data: &ExecutionData) -> Self {
    //         match AssetType::from_str(&execution_data.contract.security_type) {
    //             AssetType::Option => Self::Options(CurrentOptionPositionsPrimaryKeys {
    //                 strategy: strategy.to_string(),
    //                 stock: execution_data.contract.symbol.as_str().to_string(),
    //                 primary_exchange: execution_data.contract.primary_exchange.to_string(),
    //                 currency: execution_data.contract.currency.to_string(),
    //                 expiry: execution_data
    //                     .contract
    //                     .last_trade_date_or_contract_month
    //                     .clone(),
    //                 strike: execution_data.contract.strike.clone(),
    //                 multiplier: execution_data.contract.multiplier.clone(),
    //                 option_type: OptionType::from_str(&execution_data.contract.right).expect(
    //                     "Error parsing OptionType from contract right in update_option_execution",
    //                 ),
    //             }),
    //             AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
    //                 let stock = get_local_symbol(&execution_data.contract);
    //                 Self::Stock(CurrentStockPositionsPrimaryKeys {
    //                     strategy: strategy.to_string(),
    //                     stock: stock,
    //                     primary_exchange: execution_data.contract.primary_exchange.to_string(),
    //                     currency: execution_data.contract.currency.to_string(),
    //                 })
    //             }
    //             AssetType::Unknown => panic!(
    //                 "Tried to construct TransactionPrimaryKeys from unknown asset_type: {execution_data:?}"
    //             ),
    //             AssetType::CASH => panic!(
    //                 "Tried to construct TransactionsPrimaryKeys from CASH asset_type: should not have been possible to construct from contract: {execution_data:?}"
    //             ),
    //         }
    //     }
    pub fn get_stock(&self) -> String {
        match self {
            Self::Stock(pk) => pk.stock.clone(),
            Self::Options(pk) => pk.stock.clone(),
        }
    }

    /// Modifies the underlying struct
    pub fn with_stock(&mut self, stock: &str) {
        match self {
            Self::Stock(pk) => pk.stock = stock.to_string(),
            Self::Options(pk) => pk.stock = stock.to_string(),
        }
    }

    pub fn from_open_order(open_order: &OpenOrdersFullKeys) -> Self {
        match open_order {
            OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                stock,
                primary_exchange,
                strategy,
                currency,
                ..
            }) => Self::Stock(CurrentStockPositionsPrimaryKeys {
                stock: stock.clone(),
                primary_exchange: primary_exchange.clone(),
                currency: currency.clone(),
                strategy: strategy.clone(),
            }),
            OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                strategy,
                stock,
                primary_exchange,
                currency,
                expiry,
                strike,
                multiplier,
                option_type,
                ..
            }) => Self::Options(CurrentOptionPositionsPrimaryKeys {
                stock: stock.clone(),
                primary_exchange: primary_exchange.clone(),
                currency: currency.clone(),
                strategy: strategy.clone(),
                expiry: expiry.clone(),
                strike: *strike,
                multiplier: multiplier.clone(),
                option_type: option_type.clone(),
            }),
        }
    }

    pub fn from_strat_and_contract(strategy: &str, contract: &Contract) -> Self {
        match AssetType::from_str(&contract.security_type) {
            AssetType::Option => Self::Options(CurrentOptionPositionsPrimaryKeys {
                strategy: strategy.to_string(),
                stock: contract.symbol.as_str().to_string(),
                primary_exchange: contract.primary_exchange.to_string(),
                currency: contract.currency.to_string(),
                expiry: contract.last_trade_date_or_contract_month.to_string(),
                strike: contract.strike,
                multiplier: contract.multiplier.to_string(),
                option_type: OptionType::from_str(&contract.right).expect(
                    "Error parsing OptionType from contract right in from_strat_and_contract",
                ),
            }),
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                let stock = get_local_symbol(&contract);
                Self::Stock(CurrentStockPositionsPrimaryKeys {
                    strategy: strategy.to_string(),
                    stock: stock,
                    primary_exchange: contract.primary_exchange.to_string(),
                    currency: contract.currency.to_string(),
                })
            }
            AssetType::Unknown => panic!(
                "Tried to construct TransactionPrimaryKeys from unknown asset_type: {contract:?}"
            ),
            AssetType::CASH => panic!(
                "Tried to construct TransactionsPrimaryKeys from CASH asset_type: should not have been possible to construct from contract: {contract:?}"
            ),
        }
    }

    pub fn from_strat_and_contract_currency(strategy: &str, contract: &Contract) -> Self {
        Self::Stock(CurrentStockPositionsPrimaryKeys {
            strategy: strategy.to_string(),
            stock: format!("CASH:{}", contract.currency.to_string()),
            primary_exchange: "".to_string(),
            currency: "SGD".to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub enum CurrentPositionsUpdateKeys {
    Options(CurrentOptionPositionsUpdateKeys),
    Stock(CurrentStockPositionsUpdateKeys),
}

impl CurrentPositionsUpdateKeys {
    pub fn from_execution(execution_data: &ExecutionData) -> Self {
        match AssetType::from_str(&execution_data.contract.security_type) {
            AssetType::Option => Self::Options(CurrentOptionPositionsUpdateKeys {
                quantity: Some(
                    execution_data.execution.shares
                        * if execution_data.execution.side == "SLD" {
                            -1.0
                        } else {
                            1.0
                        },
                ),
                avg_price: Some(execution_data.execution.price),
                last_updated: None,
            }),
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                Self::Stock(CurrentStockPositionsUpdateKeys {
                    quantity: Some(
                        execution_data.execution.shares
                            * if execution_data.execution.side == "SLD" {
                                -1.0
                            } else {
                                1.0
                            },
                    ),
                    avg_price: Some(execution_data.execution.price),
                    last_updated: None,
                })
            }

            AssetType::Unknown => panic!(
                "Tried to construct TransactionPrimaryKeys from unknown asset_type: {execution_data:?}"
            ),
            AssetType::CASH => panic!(
                "Tried to construct TransactionsPrimaryKeys from CASH asset_type: should not have been possible to construct from contract: {execution_data:?}"
            ),
        }
    }

    pub fn from_execution_currency(execution_data: &ExecutionData) -> Self {
        Self::Stock(CurrentStockPositionsUpdateKeys {
            quantity: Some(
                execution_data.execution.price
                    * execution_data.execution.shares
                    * if execution_data.execution.side == "SLD" {
                        1.0
                    } else {
                        -1.0
                    },
            ),
            avg_price: Some(0.0),
            last_updated: None,
        })
    }

    pub fn from(asset_type: &AssetType, quantity: Option<f64>, avg_price: Option<f64>) -> Self {
        match asset_type {
            AssetType::Option => Self::Options(CurrentOptionPositionsUpdateKeys {
                quantity: quantity,
                avg_price: avg_price,
                last_updated: None,
            }),
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                Self::Stock(CurrentStockPositionsUpdateKeys {
                    quantity: quantity,
                    avg_price: avg_price,
                    last_updated: None,
                })
            }

            AssetType::Unknown => panic!(
                "Tried to construct TransactionPrimaryKeys from unknown asset_type in from()"
            ),
            AssetType::CASH => panic!(
                "Tried to construct TransactionsPrimaryKeys from CASH asset_type: should not have been possible to construct from contract in from()"
            ),
        }
    }
}

#[hotpath::measure_all]
impl<'a> CurrentPositionsCRUD<'a> {
    pub fn exec(&mut self) -> &mut Exec<'a> {
        match self {
            Self::Options(opt) => &mut opt.crud.exec,
            Self::Stock(stk) => &mut stk.crud.exec,
        }
    }

    pub fn stock(exec: Exec<'a>) -> Self {
        Self::Stock(CurrentStockPositionsCRUD::new(exec))
    }

    pub fn option(exec: Exec<'a>) -> Self {
        Self::Options(CurrentOptionPositionsCRUD::new(exec))
    }

    pub fn from_exec(asset_type: &AssetType, exec: Exec<'a>) -> Self {
        match asset_type {
            AssetType::Stock
            | AssetType::Future
            | AssetType::CFD
            | AssetType::ForexPair
            | AssetType::CASH => Self::stock(exec),
            AssetType::Option => Self::option(exec),
            AssetType::Unknown => panic!("Tried to get CRUD instance from an Unknown Asset Type!"),
        }
    }
}

implement_crud_trait_for_interface!(
    CurrentPositionsCRUD,
    CurrentPositionsFullKeys,
    CurrentPositionsPrimaryKeys,
    CurrentPositionsUpdateKeys,
    [Stock, Options]
);

#[cfg(feature = "backtest")]
use crate::backtester::setup::config::BacktestMode;

#[async_trait::async_trait]
pub trait CurrentPositionsOps {
    async fn get_pos_by_strat(
        &mut self,
        strategy: &str,
        #[cfg(feature = "backtest")] mode: BacktestMode,
    ) -> Result<Vec<CurrentPositionsFullKeys>, String>;
    /// Returns position if exists, else None
    /// - Note: Returns outer Result Err on sql fail, returns inner None when 404
    async fn get_pos_by_pk(
        &mut self,
        pk: CurrentPositionsPrimaryKeys,
    ) -> Result<Option<CurrentPositionsFullKeys>, String>;
    /// Returns all positions across the database grouped by Primary Keys
    async fn get_all_pos_grouped(&mut self) -> Result<Vec<CurrentPositionsFullKeys>, String>;
    async fn get_all_pos_grouped_by_strat(&mut self) -> Result<Vec<CurrentPositionsFullKeys>, String>;
    /// Should insert into DB, and on conflict, add the position to the row
    /// - i.e. quantity is added + avg_price is updated accordingly
    ///     - if quantity is in opp direction of current position
    ///       -> no change to avg_price
    async fn update_positions_additive(
        &mut self,
        pk: CurrentPositionsPrimaryKeys,
        uk: CurrentPositionsUpdateKeys,
        #[cfg(feature = "backtest")] mode: BacktestMode,
    ) -> Result<(), String>;
}

#[async_trait::async_trait]
#[hotpath::measure_all]
impl CurrentPositionsOps for CurrentPositionsCRUD<'_> {
    async fn get_pos_by_strat(
        &mut self,
        strategy: &str,
        #[cfg(feature = "backtest")] mode: BacktestMode,
    ) -> Result<Vec<CurrentPositionsFullKeys>, String> {
        #[cfg(feature = "backtest")]
        if matches!(mode, BacktestMode::InMemory) {
            // In-memory arm (explicit BacktestMode::InMemory): read the
            // strategy's InMemoryState snapshot instead of the DB. The
            // multi replay installs the fill router (name → state) so ANY
            // strategy's positions resolve; Db mode (router absent) falls
            // back to the CURRENT strategy's thread-local state, filtered
            // to the requested name; no state at all → fail CLOSED (a
            // missing state is a wiring bug, never an empty book).
            use crate::backtester::methods::in_memory::state as in_memory;
            let Some(st) = in_memory::routed_state(strategy).or_else(|| in_memory::current())
            else {
                return Err(format!(
                    "get_pos_by_strat(InMemory): no InMemoryState installed for '{strategy}'"
                ));
            };
            let rows = st
                .current_positions_snapshot()
                .into_iter()
                .filter(|(k, _)| k.strategy == strategy)
                .map(|(k, p)| {
                    CurrentPositionsFullKeys::Stock(CurrentStockPositionsFullKeys {
                        strategy: k.strategy,
                        stock: k.stock,
                        primary_exchange: k.primary_exchange,
                        currency: k.currency,
                        quantity: p.quantity,
                        avg_price: p.avg_price,
                        // Replay-time stamp; consumers read only
                        // stock/currency/quantity/avg_price.
                        last_updated: chrono::Utc::now(),
                    })
                })
                .collect();
            return Ok(rows);
        }
        // Db body — the live path, and BacktestMode::Db under backtest builds.
        {
            let result = match self {
                Self::Stock(_) => sqlx::query_as!(
                    CurrentStockPositionsFullKeys,
                    r#"
                        SELECT 
                            stock as "stock!", 
                            primary_exchange as "primary_exchange!", 
                            currency as "currency!", 
                            strategy as "strategy!", 
                            quantity as "quantity!", 
                            avg_price as "avg_price!",
                            last_updated as "last_updated!"
                        FROM trading.current_stock_positions
                        WHERE strategy = $1;
                        "#,
                    &strategy
                )
                .fetch_all(self.exec())
                .await
                .map(|positions| {
                    positions
                        .into_iter()
                        .map(CurrentPositionsFullKeys::Stock)
                        .collect()
                }),
                Self::Options(_) => sqlx::query_as!(
                    CurrentOptionPositionsFullKeys,
                    r#"
                        SELECT 
                            stock as "stock!", 
                            primary_exchange as "primary_exchange!", 
                            currency as "currency!", 
                            expiry as "expiry!",
                            strike as "strike!",
                            multiplier as "multiplier!",
                            option_type as "option_type!:OptionType",
                            strategy as "strategy!", 
                            quantity as "quantity!", 
                            avg_price as "avg_price!",
                            last_updated as "last_updated!"
                        FROM trading.current_option_positions
                        WHERE strategy = $1;
                        "#,
                    &strategy
                )
                .fetch_all(self.exec())
                .await
                .map(|positions| {
                    positions
                        .into_iter()
                        .map(CurrentPositionsFullKeys::Options)
                        .collect()
                }),
            };

            return result.map_err(|e| {
                format!(
                    "Error occurred fetching local positions for strategy {}: {}",
                    strategy, e
                )
            });
        }
    }

    async fn get_pos_by_pk(
        &mut self,
        pk: CurrentPositionsPrimaryKeys,
    ) -> Result<Option<CurrentPositionsFullKeys>, String> {
        let result = match pk {
            CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
                stock,
                primary_exchange,
                currency,
                strategy,
            }) => sqlx::query_as!(
                CurrentStockPositionsFullKeys,
                r#"
                    SELECT 
                        stock as "stock!", 
                        primary_exchange as "primary_exchange!", 
                        currency as "currency!", 
                        strategy as "strategy!", 
                        quantity as "quantity!", 
                        avg_price as "avg_price!",
                        last_updated as "last_updated!"
                    FROM trading.current_stock_positions
                    WHERE strategy = $1
                        AND stock = $2
                        AND primary_exchange = $3
                        AND currency = $4;
                    "#,
                strategy,
                stock,
                primary_exchange,
                currency
            )
            .fetch_optional(self.exec())
            .await
            .map(|ok_res| ok_res.map(CurrentPositionsFullKeys::Stock)),
            CurrentPositionsPrimaryKeys::Options(CurrentOptionPositionsPrimaryKeys {
                stock,
                primary_exchange,
                currency,
                strategy,
                expiry,
                strike,
                multiplier,
                option_type,
            }) => sqlx::query_as!(
                CurrentOptionPositionsFullKeys,
                r#"
                SELECT 
                    stock as "stock!", 
                    primary_exchange as "primary_exchange!", 
                    currency as "currency!", 
                    expiry as "expiry!",
                    strike as "strike!",
                    multiplier as "multiplier!",
                    option_type as "option_type!:OptionType",
                    strategy as "strategy!", 
                    quantity as "quantity!", 
                    avg_price as "avg_price!",
                    last_updated as "last_updated!"
                FROM trading.current_option_positions
                WHERE strategy = $1
                    AND stock = $2
                    AND primary_exchange = $3
                    AND currency = $4
                    AND expiry = $5
                    AND strike = $6
                    AND multiplier = $7
                    AND option_type = $8;
                "#,
                strategy,
                stock,
                primary_exchange,
                currency,
                expiry,
                strike,
                multiplier,
                option_type as OptionType
            )
            .fetch_optional(self.exec())
            .await
            .map(|ok_res| ok_res.map(CurrentPositionsFullKeys::Options)),
        };

        result.map_err(|e| {
            format!(
                "Error occurred fetching local positions by primary keys: {}",
                e
            )
        })
    }

    /// Currently wrong - need to update avg_price to return correct number
    async fn get_all_pos_grouped(&mut self) -> Result<Vec<CurrentPositionsFullKeys>, String> {
        let result = match self {
            Self::Stock(_) => sqlx::query_as!(
                CurrentStockPositionsFullKeys,
                r#"
                    SELECT 
                        stock AS "stock!", 
                        primary_exchange AS "primary_exchange!", 
                        currency AS "currency!", 
                        COALESCE(SUM(quantity), 0) AS "quantity!", 
                        COALESCE(SUM(quantity * avg_price), 0) as "avg_price!",
                        COALESCE((ARRAY_AGG(strategy))[1], '') AS "strategy!",
                        (ARRAY_AGG(last_updated))[1]  AS "last_updated!"
                    FROM trading.current_stock_positions
                    GROUP BY stock, primary_exchange, currency;
                    "#,
            )
            .fetch_all(self.exec())
            .await
            .map(|positions| {
                positions
                    .into_iter()
                    .map(CurrentPositionsFullKeys::Stock)
                    .map(|mut v| { 
                        if v.get_qty() != 0.0 {
                            v.set_avg_price(v.get_avg_price() / v.get_qty());
                        } 
                        v
                    })
                    .collect()
            }),
            Self::Options(_) => sqlx::query_as!(
                CurrentOptionPositionsFullKeys,
                r#"
                    SELECT 
                        stock AS "stock!", 
                        primary_exchange AS "primary_exchange!", 
                        currency AS "currency!", 
                        expiry as "expiry!",
                        strike as "strike!",
                        multiplier as "multiplier!",
                        option_type as "option_type!:OptionType",
                        COALESCE(SUM(quantity), 0) AS "quantity!", 
                        COALESCE(SUM(quantity * avg_price), 0) as "avg_price!",
                        COALESCE((ARRAY_AGG(strategy))[1], '') AS "strategy!",
                        (ARRAY_AGG(last_updated))[1] AS "last_updated!"
                    FROM trading.current_option_positions
                    GROUP BY stock, primary_exchange, currency, expiry, strike, multiplier, option_type;
                    "#,
            )
            .fetch_all(self.exec())
            .await
            .map(|positions| {
                positions
                    .into_iter()
                    .map(CurrentPositionsFullKeys::Options)
                    .map(|mut v| { 
                        if v.get_qty() != 0.0 {
                            v.set_avg_price(v.get_avg_price() / v.get_qty());
                        } 
                        v
                    })
                    .collect()
            }),
        };

        result.map_err(|e| format!("Error occurred fetching all local positions grouped: {}", e))
    }

    /// This implementation of avg_price should be fine since one strategy should only have
    /// a single position (i.e. row) for a single contract
    async fn get_all_pos_grouped_by_strat(&mut self) -> Result<Vec<CurrentPositionsFullKeys>, String> {
        let result = match self {
            Self::Stock(_) => sqlx::query_as!(
                CurrentStockPositionsFullKeys,
                r#"
                    SELECT 
                        stock AS "stock!", 
                        primary_exchange AS "primary_exchange!", 
                        currency AS "currency!", 
                        COALESCE(SUM(quantity), 0) AS "quantity!", 
                        COALESCE(AVG(avg_price), 0) as "avg_price!",
                        strategy AS "strategy!",
                        (ARRAY_AGG(last_updated))[1]  AS "last_updated!"
                    FROM trading.current_stock_positions
                    GROUP BY strategy, stock, primary_exchange, currency;
                    "#,
            )
            .fetch_all(self.exec())
            .await
            .map(|positions| {
                positions
                    .into_iter()
                    .map(CurrentPositionsFullKeys::Stock)
                    .collect()
            }),
            Self::Options(_) => sqlx::query_as!(
                CurrentOptionPositionsFullKeys,
                r#"
                    SELECT 
                        stock AS "stock!", 
                        primary_exchange AS "primary_exchange!", 
                        currency AS "currency!", 
                        expiry as "expiry!",
                        strike as "strike!",
                        multiplier as "multiplier!",
                        option_type as "option_type!:OptionType",
                        COALESCE(SUM(quantity), 0) AS "quantity!", 
                        COALESCE(AVG(avg_price), 0) as "avg_price!",
                        strategy AS "strategy!",
                        (ARRAY_AGG(last_updated))[1] AS "last_updated!"
                    FROM trading.current_option_positions
                    GROUP BY strategy, stock, primary_exchange, currency, expiry, strike, multiplier, option_type;
                    "#,
            )
            .fetch_all(self.exec())
            .await
            .map(|positions| {
                positions
                    .into_iter()
                    .map(CurrentPositionsFullKeys::Options)
                    .collect()
            }),
        };

        result.map_err(|e| format!("Error occurred fetching all local positions grouped: {}", e))
    }

    async fn update_positions_additive(
        &mut self,
        pk: CurrentPositionsPrimaryKeys,
        uk: CurrentPositionsUpdateKeys,
        #[cfg(feature = "backtest")] mode: BacktestMode,
    ) -> Result<(), String> {
        #[cfg(feature = "backtest")]
        if matches!(mode, BacktestMode::InMemory) {
            // In-memory write arm (explicit BacktestMode::InMemory): the
            // additive update lands in the strategy's InMemoryState — the
            // SAME weighted-avg semantics as the SQL upsert below. Missing
            // state or non-stock rows fail CLOSED (Err); no silent fall
            // through to the real DB.
            use crate::backtester::methods::in_memory::state as in_memory;
            if let (
                CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
                    stock,
                    primary_exchange,
                    currency,
                    strategy,
                }),
                CurrentPositionsUpdateKeys::Stock(CurrentStockPositionsUpdateKeys {
                    quantity: Some(quantity),
                    avg_price,
                    last_updated: _,
                }),
            ) = (&pk, &uk)
            {
                let Some(st) = in_memory::routed_state(strategy).or_else(|| in_memory::current())
                else {
                    return Err(format!(
                        "update_positions_additive(InMemory): no InMemoryState installed \
                         for '{strategy}'"
                    ));
                };
                st.update_current_additive(
                    in_memory::PositionKey {
                        strategy: strategy.clone(),
                        stock: stock.clone(),
                        primary_exchange: primary_exchange.clone(),
                        currency: currency.clone(),
                    },
                    *quantity,
                    avg_price.unwrap_or(0.0),
                );
                return Ok(());
            }
            return Err(
                "update_positions_additive(InMemory): only Stock rows with a quantity are \
                 supported in the in-memory lane"
                    .into(),
            );
        }
        match (pk, uk) {
            (
                CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
                    stock,
                    primary_exchange,
                    currency,
                    strategy,
                }),
                CurrentPositionsUpdateKeys::Stock(CurrentStockPositionsUpdateKeys {
                    quantity,
                    avg_price,
                    last_updated: _,
                }),
            ) => {
                sqlx::query!(
                    r#"
                    INSERT INTO trading.current_stock_positions (
                        strategy, 
                        stock, 
                        primary_exchange,
                        currency,
                        quantity, 
                        avg_price
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (stock, primary_exchange, currency, strategy)
                    DO UPDATE SET 
                    avg_price = CASE 
                        -- Avoid division by zero if total quantity becomes 0
                        WHEN (current_stock_positions.quantity + EXCLUDED.quantity) = 0 THEN 0
                        WHEN SIGN(current_stock_positions.quantity) != SIGN(EXCLUDED.quantity) THEN current_stock_positions.avg_price
                        ELSE (
                            (current_stock_positions.quantity * current_stock_positions.avg_price) + 
                            (EXCLUDED.quantity * EXCLUDED.avg_price)
                        ) / (current_stock_positions.quantity + EXCLUDED.quantity)
                    END,
                    quantity = current_stock_positions.quantity + EXCLUDED.quantity;
                    "#,
                    strategy,
                    stock,
                    primary_exchange,
                    currency,
                    quantity,
                    avg_price.unwrap_or(0.0)
                )
                .execute(self.exec())
                .await
                .map_err(|e| {
                    format!(
                        "Error when updating strategy ({}) in stock positions: {}",
                        strategy, e
                    )
                })?;
                Ok(())
            }
            (
                CurrentPositionsPrimaryKeys::Options(CurrentOptionPositionsPrimaryKeys {
                    stock,
                    primary_exchange,
                    currency,
                    expiry,
                    strike,
                    multiplier,
                    option_type,
                    strategy,
                }),
                CurrentPositionsUpdateKeys::Options(CurrentOptionPositionsUpdateKeys {
                    quantity,
                    avg_price,
                    last_updated: _,
                }),
            ) => {
                sqlx::query!(
                    r#"
                    INSERT INTO trading.current_option_positions (
                        strategy, 
                        stock, 
                        primary_exchange,
                        currency,
                        expiry,
                        strike,
                        multiplier,
                        option_type,
                        quantity, 
                        avg_price
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (stock, primary_exchange, currency, expiry, strike, multiplier, option_type, strategy)
                    DO UPDATE SET 
                    avg_price = CASE 
                        -- Avoid division by zero if total quantity becomes 0
                        WHEN (current_option_positions.quantity + EXCLUDED.quantity) = 0 THEN 0
                        ELSE (
                            (current_option_positions.quantity * current_option_positions.avg_price) + 
                            (EXCLUDED.quantity * EXCLUDED.avg_price)
                        ) / (current_option_positions.quantity + EXCLUDED.quantity)
                    END,
                    quantity = current_option_positions.quantity + EXCLUDED.quantity;
                    "#,
                    strategy,
                    stock,
                    primary_exchange,
                    currency,
                    expiry,
                    strike,
                    multiplier,
                    option_type as OptionType,
                    quantity,
                    avg_price.unwrap_or(0.0)
                )
                .execute(self.exec())
                .await
                .map_err(|e| {
                    format!(
                        "Error when updating strategy ({}) in stock positions: {}",
                        strategy, e
                    )
                })?;
                Ok(())
            }
            _ => {
                Err("Invalid key variant combination: pk enum and uk enum should be aligned when using update_positions".to_string())
            }
        }
    }
}
