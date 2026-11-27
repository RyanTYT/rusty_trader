//! Current positions: one row per (strategy, instrument[, option identity]).
//!
//! `trading.current_stock_positions` stores stocks, futures, CFDs, FX and cash;
//! `trading.current_option_positions` stores listed options. Quantities are
//! signed: positive long, negative short.
//!
//! Cash is a stock row with `stock = "CASH:<currency>"`, `primary_exchange = ""`,
//! `currency = "SGD"` (the account base) and `avg_price = 0`.
//!
//! Execution writes go through
//! [`CurrentPositionsOps::update_positions_additive`]: an upsert that adds the
//! incoming quantity and keeps a weighted average, re-basing the average when an
//! opposite order is larger than the current position.
//!
//! Under `feature = "backtest"`, `BacktestMode::InMemory` reads and writes the
//! replay `InMemoryState` and fails closed when none is installed; all other
//! modes use the executor below.
//!
//! # Example
//!
//! Additive updates accumulate quantity and re-average the price: 100 @ 10
//! followed by 50 @ 20 leaves 150 @ 13.3333. `get_all_pos_grouped` reports the
//! instrument once, across strategies. Against `TEST_DATABASE_URL` this runs
//! and asserts the values; without it, it is skipped.
//!
//! ```rust
//! use trading_app::database::crud::CRUDTrait;
//! use trading_app::database::db::DbDirect;
//! use trading_app::database::models::{
//!     AssetType, CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys, Status,
//!     StrategyFullKeys, StrategyPrimaryKeys,
//! };
//! use trading_app::database::models_crud::current_positions::current_positions::{
//!     CurrentPositionsFullKeys, CurrentPositionsOps, CurrentPositionsPrimaryKeys,
//!     CurrentPositionsUpdateKeys,
//! };
//! use trading_app::database::tables::DbTables;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
//! # let pool = sqlx::PgPool::connect(&url).await?;
//! let strategy = format!("doc_current_add_{}", std::process::id());
//! let stock = format!("DOC{}", std::process::id());
//! let mut db = DbDirect::connect(&pool);
//!
//! db.strategy()
//!     .create_or_ignore(&StrategyFullKeys {
//!         strategy: strategy.clone(),
//!         status: Status::Active,
//!     })
//!     .await?;
//!
//! let pk = CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
//!     stock: stock.clone(),
//!     primary_exchange: "NASDAQ".into(),
//!     currency: "USD".into(),
//!     strategy: strategy.clone(),
//! });
//!
//! // 100 @ 10, then another 50 @ 20.
//! for (quantity, avg_price) in [(100.0, 10.0), (50.0, 20.0)] {
//!     db.current_positions(&AssetType::Stock)
//!         .update_positions_additive(
//!             pk.clone(),
//!             CurrentPositionsUpdateKeys::Stock(CurrentStockPositionsUpdateKeys {
//!                 quantity: Some(quantity),
//!                 avg_price: Some(avg_price),
//!                 last_updated: None,
//!             }),
//!         )
//!         .await?;
//! }
//!
//! let row = db
//!     .current_positions(&AssetType::Stock)
//!     .get_pos_by_pk(pk.clone())
//!     .await?
//!     .expect("position row");
//! let CurrentPositionsFullKeys::Stock(row) = row else {
//!     panic!("expected a stock position");
//! };
//! assert_eq!(row.quantity, 150.0);
//! assert!((row.avg_price - 13.333_333).abs() < 1e-6);
//!
//! // The grouped view sums quantities and weights the average by quantity.
//! let grouped = db
//!     .current_positions(&AssetType::Stock)
//!     .get_all_pos_grouped()
//!     .await?;
//! let grouped_row = grouped
//!     .iter()
//!     .find_map(|position| match position {
//!         CurrentPositionsFullKeys::Stock(row) if row.stock == stock => Some(row),
//!         _ => None,
//!     })
//!     .expect("grouped position");
//! assert_eq!(grouped_row.quantity, 150.0);
//!
//! // Clean up so repeated doctest runs stay independent.
//! db.current_positions(&AssetType::Stock).delete(&pk).await?;
//! db.strategy()
//!     .delete(&StrategyPrimaryKeys { strategy })
//!     .await?;
//! # Ok(())
//! # }
//! ```

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

/// Interface over the stock and option current-position tables.
///
/// Construct with [`CurrentPositionsCRUD::from_exec`] (asset-type dispatch) or
/// directly with [`CurrentPositionsCRUD::stock`] / [`CurrentPositionsCRUD::option`].
/// The [`CRUDTrait`](crate::database::crud::CRUDTrait) impl dispatches on `(self, key)`; mismatched variants
/// return [`crate::database::db::DbError::KeyVariantMismatch`].
pub enum CurrentPositionsCRUD<'a> {
    /// Option positions (`trading.current_option_positions`).
    Options(CurrentOptionPositionsCRUD<'a>),
    /// Stock/futures/CFD/FX and cash positions (`trading.current_stock_positions`).
    Stock(CurrentStockPositionsCRUD<'a>),
}

/// A current-position row, tagged by asset class.
///
/// `quantity` is signed; `avg_price` is the weighted average cost in the
/// instrument currency (0 for cash rows).
#[derive(Debug, Clone, PartialEq)]
pub enum CurrentPositionsFullKeys {
    /// An option position.
    Options(CurrentOptionPositionsFullKeys),
    /// A stock/futures/CFD/FX or cash position.
    Stock(CurrentStockPositionsFullKeys),
}

/// Never decodes from a raw row: the generic CRUD decodes the concrete type
/// first and wraps it. Direct decoding is a programming error.
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
    /// Signed quantity held: positive long, negative short.
    pub fn get_qty(&self) -> f64 {
        match self {
            Self::Stock(stk) => stk.quantity,
            Self::Options(opt) => opt.quantity,
        }
    }

    /// Instrument currency (not the account base currency).
    pub fn get_currency(&self) -> String {
        match self {
            Self::Stock(fk) => fk.currency.clone(),
            Self::Options(fk) => fk.currency.clone(),
        }
    }

    /// Weighted average cost in the instrument currency.
    fn get_avg_price(&self) -> f64 {
        match self {
            Self::Stock(stk) => stk.avg_price,
            Self::Options(opt) => opt.avg_price,
        }
    }

    /// Replaces the average cost; used by the grouped reads after aggregation.
    fn set_avg_price(&mut self, avg_price: f64) {
        match self {
            Self::Stock(stk) => stk.avg_price = avg_price,
            Self::Options(opt) => opt.avg_price = avg_price,
        }
    }
}

/// Primary key of a current-position row.
///
/// Stock keys are (strategy, stock, primary_exchange, currency); option keys add
/// the full option identity (expiry, strike, multiplier, option_type).
#[derive(Debug, Clone, PartialEq)]
pub enum CurrentPositionsPrimaryKeys {
    /// Option identity key.
    Options(CurrentOptionPositionsPrimaryKeys),
    /// Stock identity key.
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
    /// Returns the instrument symbol (also for `CASH:<currency>` rows).
    pub fn get_stock(&self) -> String {
        match self {
            Self::Stock(pk) => pk.stock.clone(),
            Self::Options(pk) => pk.stock.clone(),
        }
    }

    /// Rewrites the instrument symbol in place (used when a cash row is
    /// re-keyed from `FX:<pair>` to `CASH:<currency>`).
    pub fn with_stock(&mut self, stock: &str) {
        match self {
            Self::Stock(pk) => pk.stock = stock.to_string(),
            Self::Options(pk) => pk.stock = stock.to_string(),
        }
    }

    /// Builds the position key for the instrument of an open order.
    ///
    /// The order's `strategy` is copied verbatim.
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

    /// Builds the position key from a strategy and an IBKR contract.
    ///
    /// Options use the full option identity; stocks/futures/CFDs/FX use the
    /// local symbol.
    ///
    /// # Panics
    /// Panics for `AssetType::Unknown` or `AssetType::CASH` and when an option
    /// contract's `right` is not a valid [`OptionType`].
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

    /// Builds the cash key for a contract currency.
    ///
    /// Always a stock row: `stock = "CASH:<currency>"`, empty exchange, and the
    /// account base currency `SGD`.
    pub fn from_strat_and_contract_currency(strategy: &str, contract: &Contract) -> Self {
        Self::Stock(CurrentStockPositionsPrimaryKeys {
            strategy: strategy.to_string(),
            stock: format!("CASH:{}", contract.currency.to_string()),
            primary_exchange: "".to_string(),
            currency: "SGD".to_string(),
        })
    }
}

/// Additive-update payload for a current position.
///
/// `quantity` is the signed delta; `avg_price` is the incoming order price.
/// Both are optional so the payload can be reused for partial updates.
#[derive(Debug, Clone, PartialEq)]
pub enum CurrentPositionsUpdateKeys {
    /// Option delta.
    Options(CurrentOptionPositionsUpdateKeys),
    /// Stock delta.
    Stock(CurrentStockPositionsUpdateKeys),
}

impl CurrentPositionsUpdateKeys {
    /// Signed instrument delta for a fill: `BOT` is positive, anything else
    /// negative, times `shares`, at the execution price.
    ///
    /// # Panics
    /// Panics for `AssetType::Unknown` or `AssetType::CASH`.
    pub fn from_execution(execution_data: &ExecutionData) -> Self {
        match AssetType::from_str(&execution_data.contract.security_type) {
            AssetType::Option => Self::Options(CurrentOptionPositionsUpdateKeys {
                quantity: Some(
                    execution_data.execution.shares
                        * if execution_data.execution.side == "BOT" {
                            1.0
                        } else {
                            -1.0
                        },
                ),
                avg_price: Some(execution_data.execution.price),
                last_updated: None,
            }),
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                Self::Stock(CurrentStockPositionsUpdateKeys {
                    quantity: Some(
                        execution_data.execution.shares
                            * if execution_data.execution.side == "BOT" {
                                1.0
                            } else {
                                -1.0
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

    /// Signed cash delta for a fill: buys cost cash (`-price*shares`), sells
    /// credit it (`+price*shares`). `avg_price` is fixed at 0.
    pub fn from_execution_currency(execution_data: &ExecutionData) -> Self {
        Self::Stock(CurrentStockPositionsUpdateKeys {
            quantity: Some(
                execution_data.execution.price
                    * execution_data.execution.shares
                    * if execution_data.execution.side == "BOT" {
                        -1.0
                    } else {
                        1.0
                    },
            ),
            avg_price: Some(0.0),
            last_updated: None,
        })
    }

    /// Builds an update payload from an asset type and optional values.
    ///
    /// # Panics
    /// Panics for `AssetType::Unknown` or `AssetType::CASH`.
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
    /// Returns the live executor borrow so raw SQL can reuse this session.
    pub fn exec(&mut self) -> &mut Exec<'a> {
        match self {
            Self::Options(opt) => &mut opt.crud.exec,
            Self::Stock(stk) => &mut stk.crud.exec,
        }
    }

    /// Builds the stock/cash variant.
    pub fn stock(exec: Exec<'a>) -> Self {
        Self::Stock(CurrentStockPositionsCRUD::new(exec))
    }

    /// Builds the option variant.
    pub fn option(exec: Exec<'a>) -> Self {
        Self::Options(CurrentOptionPositionsCRUD::new(exec))
    }

    /// Asset-type dispatch: options to [`CurrentPositionsCRUD::Options`]; every
    /// other known type (including `CASH`) to [`CurrentPositionsCRUD::Stock`].
    ///
    /// # Panics
    /// Panics for `AssetType::Unknown`.
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

// The interface impl dispatches per variant and reports KeyVariantMismatch on
// mismatched (self, key) pairs.
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
/// Position reads and the additive write used by the execution path.
///
/// All methods return `Result<_, String>` (the historical error style) and run
/// on the executor the interface was built with, so they participate in the
/// caller's transaction.
pub trait CurrentPositionsOps {
    /// All rows for exactly `strategy`, in both tables of this variant only.
    ///
    /// # Errors
    /// Returns a formatted error when the query fails (including a missing
    /// `InMemoryState` in the backtest lane).
    ///
    /// # Example
    ///
    /// Two rows for the requested strategy are returned; another strategy's
    /// row for the same stock is excluded.
    ///
    /// ```rust
    /// use chrono::Utc;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys, Status,
    ///     StrategyFullKeys, StrategyPrimaryKeys,
    /// };
    /// use trading_app::database::models_crud::current_positions::current_positions::{
    ///     CurrentPositionsFullKeys, CurrentPositionsOps,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let strategy = format!("doc_cps_{}", std::process::id());
    /// let other = format!("{strategy}_other");
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// for name in [strategy.clone(), other.clone()] {
    ///     db.strategy()
    ///         .create(&StrategyFullKeys { strategy: name, status: Status::Active })
    ///         .await?;
    /// }
    /// let updated = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
    ///     .expect("valid timestamp");
    /// let seeded = [
    ///     CurrentStockPositionsFullKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///         quantity: 10.0,
    ///         avg_price: 100.0,
    ///         last_updated: updated,
    ///     },
    ///     CurrentStockPositionsFullKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "MSFT".into(),
    ///         quantity: 5.0,
    ///         avg_price: 200.0,
    ///         last_updated: updated,
    ///     },
    ///     CurrentStockPositionsFullKeys {
    ///         strategy: other.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///         quantity: 7.0,
    ///         avg_price: 50.0,
    ///         last_updated: updated,
    ///     },
    /// ];
    /// for row in &seeded {
    ///     db.current_stock_positions().create(row).await?;
    /// }
    ///
    /// let rows = db
    ///     .current_positions(&AssetType::Stock)
    ///     .get_pos_by_strat(&strategy)
    ///     .await
    ///     .expect("positions by strategy");
    /// assert_eq!(rows.len(), 2);
    /// assert!(rows.contains(&CurrentPositionsFullKeys::Stock(seeded[0].clone())));
    /// assert!(rows.contains(&CurrentPositionsFullKeys::Stock(seeded[1].clone())));
    /// assert!(!rows.contains(&CurrentPositionsFullKeys::Stock(seeded[2].clone())));
    ///
    /// // Cleanup.
    /// for (name, stock) in [
    ///     (strategy.clone(), "AAPL"),
    ///     (strategy.clone(), "MSFT"),
    ///     (other.clone(), "AAPL"),
    /// ] {
    ///     db.current_stock_positions()
    ///         .delete(&CurrentStockPositionsPrimaryKeys {
    ///             strategy: name,
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             stock: stock.into(),
    ///         })
    ///         .await?;
    /// }
    /// for name in [strategy, other] {
    ///     db.strategy()
    ///         .delete(&StrategyPrimaryKeys { strategy: name })
    ///         .await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    async fn get_pos_by_strat(
        &mut self,
        strategy: &str,
        #[cfg(feature = "backtest")] mode: BacktestMode,
    ) -> Result<Vec<CurrentPositionsFullKeys>, String>;
    /// Returns position if exists, else None
    /// - Note: Returns outer Result Err on sql fail, returns inner None when 404
    ///
    /// # Example
    ///
    /// ```rust
    /// use chrono::Utc;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys, Status,
    ///     StrategyFullKeys, StrategyPrimaryKeys,
    /// };
    /// use trading_app::database::models_crud::current_positions::current_positions::{
    ///     CurrentPositionsFullKeys, CurrentPositionsOps, CurrentPositionsPrimaryKeys,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let strategy = format!("doc_cpp_{}", std::process::id());
    /// let mut db = DbDirect::connect(&pool);
    /// db.strategy()
    ///     .create(&StrategyFullKeys { strategy: strategy.clone(), status: Status::Active })
    ///     .await?;
    /// let last_updated = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
    ///     .expect("valid timestamp");
    /// let entry = CurrentStockPositionsFullKeys {
    ///     strategy: strategy.clone(),
    ///     primary_exchange: "NASDAQ".into(),
    ///     currency: "USD".into(),
    ///     stock: "AAPL".into(),
    ///     quantity: 10.0,
    ///     avg_price: 100.0,
    ///     last_updated,
    /// };
    /// let entry_pk = CurrentStockPositionsPrimaryKeys {
    ///     strategy: strategy.clone(),
    ///     primary_exchange: "NASDAQ".into(),
    ///     currency: "USD".into(),
    ///     stock: "AAPL".into(),
    /// };
    /// db.current_stock_positions()
    ///     .create(&entry)
    ///     .await?;
    ///
    /// let found = db
    ///     .current_positions(&AssetType::Stock)
    ///     .get_pos_by_pk(CurrentPositionsPrimaryKeys::Stock(entry_pk.clone()))
    ///     .await
    ///     .expect("position by pk");
    /// assert_eq!(found, Some(CurrentPositionsFullKeys::Stock(entry)));
    ///
    /// // A key that addresses no row yields None.
    /// let missing = db
    ///     .current_positions(&AssetType::Stock)
    ///     .get_pos_by_pk(CurrentPositionsPrimaryKeys::Stock(
    ///         CurrentStockPositionsPrimaryKeys {
    ///             strategy: strategy.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             stock: "NOPE".into(),
    ///         },
    ///     ))
    ///     .await
    ///     .expect("missing position");
    /// assert!(missing.is_none());
    ///
    /// // Cleanup.
    /// db.current_stock_positions()
    ///     .delete(&entry_pk)
    ///     .await?;
    /// db.strategy()
    ///     .delete(&StrategyPrimaryKeys { strategy })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn get_pos_by_pk(
        &mut self,
        pk: CurrentPositionsPrimaryKeys,
    ) -> Result<Option<CurrentPositionsFullKeys>, String>;
    /// Returns all positions across the database grouped by Primary Keys
    ///
    /// # Example
    ///
    /// Two strategies holding the same stock are summed into one instrument
    /// row: `100 @ 10` + `100 @ 20` → quantity `200`, weighted average `15`.
    ///
    /// ```rust
    /// use chrono::Utc;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys, Status,
    ///     StrategyFullKeys, StrategyPrimaryKeys,
    /// };
    /// use trading_app::database::models_crud::current_positions::current_positions::{
    ///     CurrentPositionsFullKeys, CurrentPositionsOps,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let pid = std::process::id();
    /// let stock = format!("CPG{pid}");
    /// let first = format!("doc_cpg1_{pid}");
    /// let second = format!("doc_cpg2_{pid}");
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// for name in [first.clone(), second.clone()] {
    ///     db.strategy()
    ///         .create(&StrategyFullKeys { strategy: name, status: Status::Active })
    ///         .await?;
    /// }
    /// for (name, quantity, avg_price) in [
    ///     (first.clone(), 100.0, 10.0),
    ///     (second.clone(), 100.0, 20.0),
    /// ] {
    ///     db.current_stock_positions()
    ///         .create(&CurrentStockPositionsFullKeys {
    ///             strategy: name,
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             stock: stock.clone(),
    ///             quantity,
    ///             avg_price,
    ///             last_updated: Utc::now(),
    ///         })
    ///         .await?;
    /// }
    ///
    /// let rows = db
    ///     .current_positions(&AssetType::Stock)
    ///     .get_all_pos_grouped()
    ///     .await
    ///     .expect("grouped positions");
    /// // The table may contain other tests' rows; filter to this instrument.
    /// let mine: Vec<_> = rows
    ///     .iter()
    ///     .filter_map(|row| match row {
    ///         CurrentPositionsFullKeys::Stock(r) if r.stock == stock => Some(r),
    ///         _ => None,
    ///     })
    ///     .collect();
    /// assert_eq!(mine.len(), 1);
    /// assert_eq!(mine[0].quantity, 200.0);
    /// assert!((mine[0].avg_price - 15.0).abs() < 1e-9);
    ///
    /// // Cleanup.
    /// for name in [first, second] {
    ///     db.current_stock_positions()
    ///         .delete(&CurrentStockPositionsPrimaryKeys {
    ///             strategy: name.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             stock: stock.clone(),
    ///         })
    ///         .await?;
    ///     db.strategy()
    ///         .delete(&StrategyPrimaryKeys { strategy: name })
    ///         .await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    async fn get_all_pos_grouped(&mut self) -> Result<Vec<CurrentPositionsFullKeys>, String>;
    /// Returns all positions grouped by strategy and instrument.
    ///
    /// # Example
    ///
    /// The same two rows as [`CurrentPositionsOps::get_all_pos_grouped`] stay
    /// separate: one row per strategy, each with its own average.
    ///
    /// ```rust
    /// use chrono::Utc;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys, Status,
    ///     StrategyFullKeys, StrategyPrimaryKeys,
    /// };
    /// use trading_app::database::models_crud::current_positions::current_positions::{
    ///     CurrentPositionsFullKeys, CurrentPositionsOps,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let pid = std::process::id();
    /// let stock = format!("CPS{pid}");
    /// let first = format!("doc_cps1_{pid}");
    /// let second = format!("doc_cps2_{pid}");
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// for name in [first.clone(), second.clone()] {
    ///     db.strategy()
    ///         .create(&StrategyFullKeys { strategy: name, status: Status::Active })
    ///         .await?;
    /// }
    /// for (name, quantity, avg_price) in [
    ///     (first.clone(), 100.0, 10.0),
    ///     (second.clone(), 50.0, 20.0),
    /// ] {
    ///     db.current_stock_positions()
    ///         .create(&CurrentStockPositionsFullKeys {
    ///             strategy: name,
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             stock: stock.clone(),
    ///             quantity,
    ///             avg_price,
    ///             last_updated: Utc::now(),
    ///         })
    ///         .await?;
    /// }
    ///
    /// let rows = db
    ///     .current_positions(&AssetType::Stock)
    ///     .get_all_pos_grouped_by_strat()
    ///     .await
    ///     .expect("grouped positions by strategy");
    /// let mine: Vec<_> = rows
    ///     .iter()
    ///     .filter_map(|row| match row {
    ///         CurrentPositionsFullKeys::Stock(r) if r.stock == stock => Some(r),
    ///         _ => None,
    ///     })
    ///     .collect();
    /// assert_eq!(mine.len(), 2);
    /// let a = mine.iter().find(|r| r.strategy == first).expect("first strategy");
    /// assert_eq!(a.quantity, 100.0);
    /// assert_eq!(a.avg_price, 10.0);
    /// let b = mine.iter().find(|r| r.strategy == second).expect("second strategy");
    /// assert_eq!(b.quantity, 50.0);
    /// assert_eq!(b.avg_price, 20.0);
    ///
    /// // Cleanup.
    /// for name in [first, second] {
    ///     db.current_stock_positions()
    ///         .delete(&CurrentStockPositionsPrimaryKeys {
    ///             strategy: name.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             stock: stock.clone(),
    ///         })
    ///         .await?;
    ///     db.strategy()
    ///         .delete(&StrategyPrimaryKeys { strategy: name })
    ///         .await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    async fn get_all_pos_grouped_by_strat(&mut self) -> Result<Vec<CurrentPositionsFullKeys>, String>;
    /// Upserts the row and adds `uk.quantity` to the stored quantity.
    ///
    /// Average-price rules (stock and option):
    /// - net quantity becomes 0: average becomes 0;
    /// - opposite order larger than the position: the direction flips and the
    ///   average becomes the incoming price;
    /// - opposite order smaller or equal: the surviving position keeps its
    ///   average (partial close);
    /// - same direction: weighted average of the two.
    ///
    /// `None` quantity/price bind as 0. A pk/uk variant mismatch returns an
    /// error without writing.
    ///
    /// # Example
    ///
    /// `100 @ 10`, then add `50 @ 20` → `150 @ 13.3333`. An opposite order
    /// larger than the position (`-300 @ 30`) flips the direction
    /// (`150 - 300 = -150`) and resets the average to the incoming price.
    ///
    /// ```rust
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys, Status,
    ///     StrategyFullKeys, StrategyPrimaryKeys,
    /// };
    /// use trading_app::database::models_crud::current_positions::current_positions::{
    ///     CurrentPositionsFullKeys, CurrentPositionsOps, CurrentPositionsPrimaryKeys,
    ///     CurrentPositionsUpdateKeys,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let pid = std::process::id();
    /// let stock = format!("CPU{pid}");
    /// let strategy = format!("doc_cpu_{pid}");
    /// let mut db = DbDirect::connect(&pool);
    /// db.strategy()
    ///     .create(&StrategyFullKeys { strategy: strategy.clone(), status: Status::Active })
    ///     .await?;
    ///
    /// let pk = || CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
    ///     strategy: strategy.clone(),
    ///     primary_exchange: "NASDAQ".into(),
    ///     currency: "USD".into(),
    ///     stock: stock.clone(),
    /// });
    /// let add = |quantity: f64, avg_price: f64| {
    ///     CurrentPositionsUpdateKeys::Stock(CurrentStockPositionsUpdateKeys {
    ///         quantity: Some(quantity),
    ///         avg_price: Some(avg_price),
    ///         last_updated: None,
    ///     })
    /// };
    ///
    /// db.current_positions(&AssetType::Stock)
    ///     .update_positions_additive(pk(), add(100.0, 10.0))
    ///     .await
    ///     .expect("seed position");
    /// db.current_positions(&AssetType::Stock)
    ///     .update_positions_additive(pk(), add(50.0, 20.0))
    ///     .await
    ///     .expect("add to position");
    ///
    /// let row = db
    ///     .current_positions(&AssetType::Stock)
    ///     .get_pos_by_pk(pk())
    ///     .await
    ///     .expect("read position")
    ///     .expect("position exists");
    /// let CurrentPositionsFullKeys::Stock(row) = row else {
    ///     panic!("expected a stock position");
    /// };
    /// assert_eq!(row.quantity, 150.0);
    /// assert!((row.avg_price - 40.0 / 3.0).abs() < 1e-9);
    ///
    /// // Opposite order larger than the position: flip and reset the average.
    /// db.current_positions(&AssetType::Stock)
    ///     .update_positions_additive(pk(), add(-300.0, 30.0))
    ///     .await
    ///     .expect("flip position");
    /// let row = db
    ///     .current_positions(&AssetType::Stock)
    ///     .get_pos_by_pk(pk())
    ///     .await
    ///     .expect("read flipped position")
    ///     .expect("position exists");
    /// let CurrentPositionsFullKeys::Stock(row) = row else {
    ///     panic!("expected a stock position");
    /// };
    /// assert_eq!(row.quantity, -150.0);
    /// assert_eq!(row.avg_price, 30.0);
    ///
    /// // Cleanup.
    /// db.current_stock_positions()
    ///     .delete(&CurrentStockPositionsPrimaryKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock,
    ///     })
    ///     .await?;
    /// db.strategy()
    ///     .delete(&StrategyPrimaryKeys { strategy })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn update_positions_additive(
        &mut self,
        pk: CurrentPositionsPrimaryKeys,
        uk: CurrentPositionsUpdateKeys,
        #[cfg(feature = "backtest")] mode: BacktestMode,
    ) -> Result<(), String>;
}

#[async_trait::async_trait]
#[hotpath::measure_all]
/// DB and InMemory implementations of [`CurrentPositionsOps`]. The trait
/// methods carry the contracts; the InMemory arms mirror the SQL semantics and
/// fail closed when no replay state is installed.
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

    /// One row per instrument across strategies.
    ///
    /// `quantity` is the sum; `avg_price` is `SUM(quantity*avg_price)/SUM(quantity)`
    /// (0 when the net quantity is 0). `strategy` is an arbitrary member of the
    /// group (`(ARRAY_AGG(strategy))[1]`) and `last_updated` is likewise
    /// arbitrary; callers must not rely on either.
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

    /// One row per (strategy, instrument).
    ///
    /// `quantity` is the sum for that strategy and `avg_price` is the SQL
    /// `AVG(avg_price)` (there is normally one row per strategy/instrument, so
    /// this equals that row's price).
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
                        -- Opposite order LARGER than the position: the direction
                        -- flips, so the residual position is carried at the new
                        -- order's price, not the old average.
                        WHEN SIGN(current_stock_positions.quantity) != SIGN(EXCLUDED.quantity)
                             AND ABS(EXCLUDED.quantity) > ABS(current_stock_positions.quantity)
                        THEN EXCLUDED.avg_price
                        -- Opposite order smaller (or exactly equal): a partial
                        -- close leaves the surviving position at its average.
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
                        -- Opposite order LARGER than the position: the direction
                        -- flips, so the residual position is carried at the new
                        -- order's price, not the old average.
                        WHEN SIGN(current_option_positions.quantity) != SIGN(EXCLUDED.quantity)
                             AND ABS(EXCLUDED.quantity) > ABS(current_option_positions.quantity)
                        THEN EXCLUDED.avg_price
                        -- Opposite order smaller (or exactly equal): a partial
                        -- close leaves the surviving position at its average.
                        WHEN SIGN(current_option_positions.quantity) != SIGN(EXCLUDED.quantity) THEN current_option_positions.avg_price
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
