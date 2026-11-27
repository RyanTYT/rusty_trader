//! Target-position interface: the intended book versus the live book.
//!
//! `trading.target_stock_positions` and `trading.target_option_positions` hold
//! what a strategy wants to own. The diff queries `FULL OUTER JOIN` the
//! matching current-position table and return `qty_diff = target - current`:
//! positive means buy, negative means sell. `COALESCE` keeps rows that exist on
//! only one side, so a missing target or a missing current row still yields a
//! difference.
//!
//! The table is selected by [`AssetType`]: stock, future, CFD, forex and cash
//! use the stock table; options use the option table. `clear_strat_pos` deletes
//! a strategy's target rows without touching current positions.
//!
//! # Example
//!
//! Target 100 AAPL against a current position of 40: the diff is `+60`
//! (buy 60). Against `TEST_DATABASE_URL` this runs and asserts the exact
//! values; without it, it is skipped.
//!
//! ```rust
//! use trading_app::database::crud::CRUDTrait;
//! use trading_app::database::db::DbDirect;
//! use trading_app::database::models::{
//!     AssetType, CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys, Status,
//!     StrategyFullKeys, StrategyPrimaryKeys, TargetStockPositionsFullKeys,
//!     TargetStockPositionsPrimaryKeys,
//! };
//! use trading_app::database::models_crud::target_positions::target_positions::{
//!     TargetPositionsOps, TargetPositionsQtyDiff,
//! };
//! use trading_app::database::tables::DbTables;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
//! # let pool = sqlx::PgPool::connect(&url).await?;
//! let strategy = format!("doc_target_diff_{}", std::process::id());
//! let mut db = DbDirect::connect(&pool);
//!
//! db.strategy()
//!     .create_or_ignore(&StrategyFullKeys {
//!         strategy: strategy.clone(),
//!         status: Status::Active,
//!     })
//!     .await?;
//! db.target_stock_positions()
//!     .create(&TargetStockPositionsFullKeys {
//!         strategy: strategy.clone(),
//!         primary_exchange: "NASDAQ".into(),
//!         currency: "USD".into(),
//!         stock: "AAPL".into(),
//!         avg_price: 150.0,
//!         quantity: 100.0,
//!     })
//!     .await?;
//! db.current_stock_positions()
//!     .create(&CurrentStockPositionsFullKeys {
//!         strategy: strategy.clone(),
//!         primary_exchange: "NASDAQ".into(),
//!         currency: "USD".into(),
//!         stock: "AAPL".into(),
//!         quantity: 40.0,
//!         avg_price: 140.0,
//!         last_updated: chrono::Utc::now(),
//!     })
//!     .await?;
//!
//! let diffs = db
//!     .target_positions(&AssetType::Stock)
//!     .get_target_pos_diff_by_strat(&strategy)
//!     .await?;
//!
//! assert_eq!(diffs.len(), 1);
//! let diff = match &diffs[0] {
//!     TargetPositionsQtyDiff::Stock(d) => d,
//!     TargetPositionsQtyDiff::Options(_) => panic!("expected a stock diff"),
//! };
//! assert_eq!(diff.stock, "AAPL");
//! assert_eq!(diff.qty_diff, 60.0); // target 100 - current 40
//! assert_eq!(diff.current_qty, 40.0);
//! assert_eq!(diff.avg_price, 150.0); // taken from the target row
//!
//! // Clean up so repeated doctest runs stay independent.
//! db.target_stock_positions()
//!     .delete(&TargetStockPositionsPrimaryKeys {
//!         strategy: strategy.clone(),
//!         primary_exchange: "NASDAQ".into(),
//!         currency: "USD".into(),
//!         stock: "AAPL".into(),
//!     })
//!     .await?;
//! db.current_stock_positions()
//!     .delete(&CurrentStockPositionsPrimaryKeys {
//!         stock: "AAPL".into(),
//!         primary_exchange: "NASDAQ".into(),
//!         currency: "USD".into(),
//!         strategy: strategy.clone(),
//!     })
//!     .await?;
//! db.strategy()
//!     .delete(&StrategyPrimaryKeys { strategy })
//!     .await?;
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;

use crate::{
    database::{
        db::Exec,
        models::{
            AssetType, OptionType, TargetOptionPositionsFullKeys, TargetOptionPositionsPrimaryKeys,
            TargetOptionPositionsUpdateKeys, TargetStockPositionsFullKeys,
            TargetStockPositionsPrimaryKeys, TargetStockPositionsUpdateKeys,
        },
        models_crud::target_positions::{
            target_option_positions::{TargetOptionPositionsCRUD, TargetOptionPositionsQtyDiff},
            target_stock_positions::{TargetStockPositionsCRUD, TargetStockPositionsQtyDiff},
        },
    },
    implement_crud_trait_for_interface,
};

/// Typed handle over one target-position table.
///
/// The asset class is fixed at construction; SQL and row decoding stay on the
/// concrete handle, while callers work with [`TargetPositionsFullKeys`].
pub enum TargetPositionsCRUD<'a> {
    /// `trading.target_stock_positions` (also used for futures, CFDs, forex and cash).
    Stock(TargetStockPositionsCRUD<'a>),
    /// `trading.target_option_positions`.
    Options(TargetOptionPositionsCRUD<'a>),
}

#[derive(Debug, Clone, PartialEq)]
/// One target-position row, tagged with its asset class.
pub enum TargetPositionsFullKeys {
    /// A stock, future, CFD, forex or cash target row.
    Stock(TargetStockPositionsFullKeys),
    /// An option target row.
    Options(TargetOptionPositionsFullKeys),
}

/// Enum rows are never decoded directly from SQL. The concrete
/// `CRUD<FK, PK, UK>` decodes the inner struct and wraps it in the matching
/// variant, so reaching this impl is a programming error.
impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for TargetPositionsFullKeys {
    fn from_row(_: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        // This will never be executed because the inner CRUD<FK,PK,UK>
        // decodes the inner concrete struct before wrapping it into this enum.
        Err(sqlx::Error::Decode(
            "TargetPositionsFullKeys cannot be decoded directly from a raw SQL row".into(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Target minus current quantity for one instrument.
///
/// `qty_diff > 0` means the strategy should buy; `qty_diff < 0` means it holds
/// more than intended and should sell. `current_qty` and `avg_price` come from
/// the current-position side and are `0.0` when that row is absent.
pub enum TargetPositionsQtyDiff {
    /// Difference for a stock, future, CFD, forex or cash instrument.
    Stock(TargetStockPositionsQtyDiff),
    /// Difference for an option contract.
    Options(TargetOptionPositionsQtyDiff),
}

impl TargetPositionsQtyDiff {
    /// Adds `qty_diff` to the stored difference and returns the new value.
    ///
    /// Used to net repeated intents for the same instrument; plain `f64`
    /// addition, so callers should treat the result as approximate.
    ///
    /// # Example
    ///
    /// ```
    /// use trading_app::database::models_crud::target_positions::target_positions::TargetPositionsQtyDiff;
    /// use trading_app::database::models_crud::target_positions::target_stock_positions::TargetStockPositionsQtyDiff;
    ///
    /// let mut diff = TargetPositionsQtyDiff::Stock(TargetStockPositionsQtyDiff {
    ///     strategy: "demo".into(),
    ///     primary_exchange: "NASDAQ".into(),
    ///     currency: "USD".into(),
    ///     stock: "AAPL".into(),
    ///     avg_price: 100.0,
    ///     qty_diff: 5.0,
    ///     current_qty: 0.0,
    /// });
    ///
    /// // A second intent for the same instrument nets against the first.
    /// assert_eq!(diff.update_qty_diff(-3.0), 2.0);
    /// assert_eq!(diff.get_qty_diff(), 2.0);
    /// assert_eq!(diff.get_stock(), "AAPL");
    /// ```
    pub fn update_qty_diff(&mut self, qty_diff: f64) -> f64 {
        match self {
            Self::Stock(v) => {
                v.qty_diff += qty_diff;
                v.qty_diff
            }
            Self::Options(v) => {
                v.qty_diff += qty_diff;
                v.qty_diff
            }
        }
    }

    /// Returns the instrument symbol, cloned.
    pub fn get_stock(&self) -> String {
        match self {
            Self::Stock(TargetStockPositionsQtyDiff { stock, .. }) => stock.clone(),
            Self::Options(TargetOptionPositionsQtyDiff { stock, .. }) => stock.clone(),
        }
    }

    /// Returns the signed target-minus-current difference.
    pub fn get_qty_diff(&self) -> f64 {
        match self {
            Self::Stock(TargetStockPositionsQtyDiff { qty_diff, .. }) => *qty_diff,
            Self::Options(TargetOptionPositionsQtyDiff { qty_diff, .. }) => *qty_diff,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Address of one target-position row. The variant must match the row's asset
/// class; a mismatch is reported as [`crate::database::db::DbError::KeyVariantMismatch`].
pub enum TargetPositionsPrimaryKeys {
    /// Key for the stock table.
    Stock(TargetStockPositionsPrimaryKeys),
    /// Key for the option table.
    Options(TargetOptionPositionsPrimaryKeys),
}

#[derive(Debug, Clone, PartialEq)]
/// Partial update for one target-position row.
///
/// `None` fields are omitted from the `SET` clause; the variant must match the
/// row's asset class.
pub enum TargetPositionsUpdateKeys {
    /// Update fields for the stock table.
    Stock(TargetStockPositionsUpdateKeys),
    /// Update fields for the option table.
    Options(TargetOptionPositionsUpdateKeys),
}

#[hotpath::measure_all]
impl<'a> TargetPositionsCRUD<'a> {
    /// Borrows the executor behind this handle for raw SQL.
    pub fn exec(&mut self) -> &mut Exec<'a> {
        match self {
            Self::Stock(stk) => &mut stk.crud.exec,
            Self::Options(opt) => &mut opt.crud.exec,
        }
    }

    /// Builds a handle over `trading.target_stock_positions`.
    pub fn stock(exec: Exec<'a>) -> Self {
        Self::Stock(TargetStockPositionsCRUD::new(exec))
    }

    /// Builds a handle over `trading.target_option_positions`.
    pub fn option(exec: Exec<'a>) -> Self {
        Self::Options(TargetOptionPositionsCRUD::new(exec))
    }

    /// Builds the handle for `asset_type`.
    ///
    /// Cash, futures, CFDs and forex map to the stock table; `Unknown` panics.
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
    TargetPositionsCRUD,
    TargetPositionsFullKeys,
    TargetPositionsPrimaryKeys,
    TargetPositionsUpdateKeys,
    [Stock, Options]
);

/// Reads the target-versus-current book and clears a strategy's targets.
#[async_trait]
pub trait TargetPositionsOps {
    /// Returns the difference for one exact instrument.
    ///
    /// Empty when neither table has the row; at most one row otherwise.
    /// # Errors
    /// Returns the SQL error formatted as a `String` (the historical message
    /// says "in stocks" on both the stock and option paths).
    ///
    /// # Example
    ///
    /// Target `AAPL 100 @ 150`, current `AAPL 40 @ 140` → one diff of `+60`
    /// (buy 60 more), carrying the current quantity and the target price.
    ///
    /// ```rust
    /// use chrono::Utc;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys, Status,
    ///     StrategyFullKeys, StrategyPrimaryKeys, TargetStockPositionsFullKeys,
    ///     TargetStockPositionsPrimaryKeys,
    /// };
    /// use trading_app::database::models_crud::target_positions::target_positions::{
    ///     TargetPositionsOps, TargetPositionsPrimaryKeys, TargetPositionsQtyDiff,
    /// };
    /// use trading_app::database::models_crud::target_positions::target_stock_positions::TargetStockPositionsQtyDiff;
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let strategy = format!("doc_tpk_{}", std::process::id());
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// db.strategy()
    ///     .create(&StrategyFullKeys { strategy: strategy.clone(), status: Status::Active })
    ///     .await?;
    /// db.target_stock_positions()
    ///     .create(&TargetStockPositionsFullKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///         avg_price: 150.0,
    ///         quantity: 100.0,
    ///     })
    ///     .await?;
    /// db.current_stock_positions()
    ///     .create(&CurrentStockPositionsFullKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///         quantity: 40.0,
    ///         avg_price: 140.0,
    ///         last_updated: Utc::now(),
    ///     })
    ///     .await?;
    ///
    /// let diffs = db
    ///     .target_positions(&AssetType::Stock)
    ///     .get_target_pos_diff_by_pk(TargetPositionsPrimaryKeys::Stock(
    ///         TargetStockPositionsPrimaryKeys {
    ///             strategy: strategy.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             stock: "AAPL".into(),
    ///         },
    ///     ))
    ///     .await
    ///     .expect("target diff query");
    /// assert_eq!(
    ///     diffs,
    ///     vec![TargetPositionsQtyDiff::Stock(TargetStockPositionsQtyDiff {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///         avg_price: 150.0,
    ///         qty_diff: 60.0,
    ///         current_qty: 40.0,
    ///     })]
    /// );
    ///
    /// // Cleanup.
    /// db.target_stock_positions()
    ///     .delete(&TargetStockPositionsPrimaryKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///     })
    ///     .await?;
    /// db.current_stock_positions()
    ///     .delete(&CurrentStockPositionsPrimaryKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///     })
    ///     .await?;
    /// db.strategy()
    ///     .delete(&StrategyPrimaryKeys { strategy })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn get_target_pos_diff_by_pk(
        &mut self,
        pk: TargetPositionsPrimaryKeys,
    ) -> Result<Vec<TargetPositionsQtyDiff>, String>;
    /// Returns one difference per instrument for `strategy`.
    ///
    /// Includes instruments that exist only in the target table or only in the
    /// current table; ordering is whatever the `FULL OUTER JOIN` yields.
    /// # Errors
    /// Returns the SQL error formatted as a `String`.
    ///
    /// # Example
    ///
    /// For one strategy: `AAPL` is target 100 / current 40 → `+60`; `MSFT`
    /// exists only in current (30) → `-30`. The `FULL OUTER JOIN` reports both
    /// sides, and a missing target has `avg_price = 0.0`.
    ///
    /// ```rust
    /// use chrono::Utc;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys, Status,
    ///     StrategyFullKeys, StrategyPrimaryKeys, TargetStockPositionsFullKeys,
    ///     TargetStockPositionsPrimaryKeys,
    /// };
    /// use trading_app::database::models_crud::target_positions::target_positions::{
    ///     TargetPositionsOps, TargetPositionsQtyDiff,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let strategy = format!("doc_tps_{}", std::process::id());
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// db.strategy()
    ///     .create(&StrategyFullKeys { strategy: strategy.clone(), status: Status::Active })
    ///     .await?;
    /// db.target_stock_positions()
    ///     .create(&TargetStockPositionsFullKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///         avg_price: 150.0,
    ///         quantity: 100.0,
    ///     })
    ///     .await?;
    /// db.current_stock_positions()
    ///     .create(&CurrentStockPositionsFullKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///         quantity: 40.0,
    ///         avg_price: 140.0,
    ///         last_updated: Utc::now(),
    ///     })
    ///     .await?;
    /// db.current_stock_positions()
    ///     .create(&CurrentStockPositionsFullKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "MSFT".into(),
    ///         quantity: 30.0,
    ///         avg_price: 50.0,
    ///         last_updated: Utc::now(),
    ///     })
    ///     .await?;
    ///
    /// let diffs = db
    ///     .target_positions(&AssetType::Stock)
    ///     .get_target_pos_diff_by_strat(&strategy)
    ///     .await
    ///     .expect("target diff query");
    /// assert_eq!(diffs.len(), 2);
    ///
    /// let aapl = diffs
    ///     .iter()
    ///     .find_map(|diff| match diff {
    ///         TargetPositionsQtyDiff::Stock(row) if row.stock == "AAPL" => Some(row),
    ///         _ => None,
    ///     })
    ///     .expect("AAPL diff");
    /// assert_eq!(aapl.qty_diff, 60.0);
    ///
    /// let msft = diffs
    ///     .iter()
    ///     .find_map(|diff| match diff {
    ///         TargetPositionsQtyDiff::Stock(row) if row.stock == "MSFT" => Some(row),
    ///         _ => None,
    ///     })
    ///     .expect("MSFT diff");
    /// assert_eq!(msft.qty_diff, -30.0);
    /// assert_eq!(msft.current_qty, 30.0);
    /// assert_eq!(msft.avg_price, 0.0);
    ///
    /// // Cleanup.
    /// for stock in ["AAPL", "MSFT"] {
    ///     db.current_stock_positions()
    ///         .delete(&CurrentStockPositionsPrimaryKeys {
    ///             strategy: strategy.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             stock: stock.into(),
    ///         })
    ///         .await?;
    /// }
    /// db.target_stock_positions()
    ///     .delete(&TargetStockPositionsPrimaryKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///     })
    ///     .await?;
    /// db.strategy()
    ///     .delete(&StrategyPrimaryKeys { strategy })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn get_target_pos_diff_by_strat(
        &mut self,
        strategy: &str,
    ) -> Result<Vec<TargetPositionsQtyDiff>, String>;
    /// Deletes every target row for `strategy`; current positions are untouched.
    ///
    /// Idempotent: deleting zero rows succeeds.
    /// # Errors
    /// Returns the SQL error formatted as a `String`.
    ///
    /// # Example
    ///
    /// Clears two target rows for one strategy and leaves its current position
    /// untouched.
    ///
    /// ```rust
    /// use chrono::Utc;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys, Status,
    ///     StrategyFullKeys, StrategyPrimaryKeys, TargetStockPositionsFullKeys,
    /// };
    /// use trading_app::database::models_crud::target_positions::target_positions::TargetPositionsOps;
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let strategy = format!("doc_tclear_{}", std::process::id());
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// db.strategy()
    ///     .create(&StrategyFullKeys { strategy: strategy.clone(), status: Status::Active })
    ///     .await?;
    /// for stock in ["AAPL", "MSFT"] {
    ///     db.target_stock_positions()
    ///         .create(&TargetStockPositionsFullKeys {
    ///             strategy: strategy.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             stock: stock.into(),
    ///             avg_price: 100.0,
    ///             quantity: 10.0,
    ///         })
    ///         .await?;
    /// }
    /// db.current_stock_positions()
    ///     .create(&CurrentStockPositionsFullKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///         quantity: 4.0,
    ///         avg_price: 90.0,
    ///         last_updated: Utc::now(),
    ///     })
    ///     .await?;
    ///
    /// db.target_positions(&AssetType::Stock)
    ///     .clear_strat_pos(&strategy)
    ///     .await
    ///     .expect("clear target positions");
    ///
    /// let targets = db.target_stock_positions().read_all().await?;
    /// assert!(targets.iter().all(|row| row.strategy != strategy));
    /// assert!(db
    ///     .current_stock_positions()
    ///     .read(&CurrentStockPositionsPrimaryKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///     })
    ///     .await?
    ///     .is_some());
    ///
    /// // Cleanup.
    /// db.current_stock_positions()
    ///     .delete(&CurrentStockPositionsPrimaryKeys {
    ///         strategy: strategy.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         stock: "AAPL".into(),
    ///     })
    ///     .await?;
    /// db.strategy()
    ///     .delete(&StrategyPrimaryKeys { strategy })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn clear_strat_pos(&mut self, strategy: &str) -> Result<(), String>;
}

#[async_trait]
#[hotpath::measure_all]
impl TargetPositionsOps for TargetPositionsCRUD<'_> {
    async fn get_target_pos_diff_by_pk(
        &mut self,
        pk: TargetPositionsPrimaryKeys,
    ) -> Result<Vec<TargetPositionsQtyDiff>, String> {
        let result = match pk {
            TargetPositionsPrimaryKeys::Stock(TargetStockPositionsPrimaryKeys {
                strategy,
                primary_exchange,
                currency,
                stock,
            }) => sqlx::query_as!(
                TargetStockPositionsQtyDiff,
                r#"
                SELECT
                    COALESCE(t.stock, c.stock) AS "stock!",
                    COALESCE(t.primary_exchange, c.primary_exchange) AS "primary_exchange!",
                    COALESCE(t.currency, c.currency) AS "currency!",
                    COALESCE(t.strategy, c.strategy) AS "strategy!",
                    (COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0))::float8 AS "qty_diff!",
                    COALESCE(c.quantity, 0.0) AS "current_qty!",
                    COALESCE(t.avg_price, 0.0) AS "avg_price!"
                FROM trading.target_stock_positions t
                FULL OUTER JOIN trading.current_stock_positions  c
                    ON t.stock = c.stock 
                        AND t.primary_exchange = c.primary_exchange 
                        AND t.currency = c.currency  
                        AND t.strategy = c.strategy 
                WHERE COALESCE(t.strategy, c.strategy) = $1
                    AND COALESCE(t.stock, c.stock) = $2
                    AND COALESCE(t.primary_exchange, c.primary_exchange) = $3
                    AND COALESCE(t.currency, c.currency) = $4;
                "#,
                strategy,
                stock,
                primary_exchange,
                currency
            )
            .fetch_all(self.exec())
            .await
            .map(|ok_res| {
                ok_res
                    .into_iter()
                    .map(TargetPositionsQtyDiff::Stock)
                    .collect()
            }),

            TargetPositionsPrimaryKeys::Options(TargetOptionPositionsPrimaryKeys {
                strategy,
                stock,
                primary_exchange,
                currency,
                expiry,
                strike,
                multiplier,
                option_type,
            }) => sqlx::query_as!(
                TargetOptionPositionsQtyDiff,
                r#"
                SELECT
                    COALESCE(t.stock, c.stock) AS "stock!",
                    COALESCE(t.primary_exchange, c.primary_exchange) AS "primary_exchange!",
                    COALESCE(t.currency, c.currency) AS "currency!",
                    COALESCE(t.expiry, c.expiry) AS "expiry!",
                    COALESCE(t.strike, c.strike) AS "strike!",
                    COALESCE(t.multiplier, c.multiplier) AS "multiplier!",
                    COALESCE(t.option_type, c.option_type) AS "option_type!:OptionType",
                    COALESCE(t.strategy, c.strategy) AS "strategy!",
                    (COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0))::float8 AS "qty_diff!",
                    COALESCE(c.quantity, 0.0) AS "current_qty!",
                    COALESCE(t.avg_price, 0.0) AS "avg_price!"
                FROM trading.target_option_positions t
                FULL OUTER JOIN trading.current_option_positions  c
                    ON t.stock = c.stock 
                    AND t.primary_exchange = c.primary_exchange
                    AND t.currency = c.currency
                    AND t.expiry = c.expiry 
                    AND t.strike = c.strike
                    AND t.multiplier = c.multiplier
                    AND t.option_type = c.option_type
                    AND t.strategy = c.strategy
                WHERE COALESCE(t.strategy, c.strategy) = $1
                    AND COALESCE(t.stock, c.stock) = $2
                    AND COALESCE(t.primary_exchange, c.primary_exchange) = $3
                    AND COALESCE(t.currency, c.currency) = $4
                    AND COALESCE(t.expiry, c.expiry) = $5
                    AND COALESCE(t.strike, c.strike) = $6
                    AND COALESCE(t.multiplier, c.multiplier) = $7
                    AND COALESCE(t.option_type, c.option_type) = $8::option_type;
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
            .fetch_all(self.exec())
            .await
            .map(|ok_res| {
                ok_res
                    .into_iter()
                    .map(TargetPositionsQtyDiff::Options)
                    .collect()
            }),
        };
        result.map_err(|e| {
            format!(
                "Error retrieving qty difference in stocks for strategy: {}",
                e
            )
        })
    }

    async fn get_target_pos_diff_by_strat(
        &mut self,
        strategy: &str,
    ) -> Result<Vec<TargetPositionsQtyDiff>, String> {
        let result = match self {
            Self::Stock(stk) => sqlx::query_as!(
                TargetStockPositionsQtyDiff,
                r#"
                SELECT
                    COALESCE(t.stock, c.stock) AS "stock!",
                    COALESCE(t.primary_exchange, c.primary_exchange) AS "primary_exchange!",
                    COALESCE(t.currency, c.currency) AS "currency!",
                    COALESCE(t.strategy, c.strategy) AS "strategy!",
                    (COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0))::float8 AS "qty_diff!",
                    COALESCE(c.quantity, 0.0) AS "current_qty!",
                    COALESCE(t.avg_price, 0.0) AS "avg_price!"
                FROM trading.target_stock_positions t
                FULL OUTER JOIN trading.current_stock_positions  c
                    ON t.stock = c.stock 
                        AND t.primary_exchange = c.primary_exchange 
                        AND t.currency = c.currency  
                        AND t.strategy = c.strategy 
                WHERE COALESCE(t.strategy, c.strategy) = $1;
                "#,
                strategy,
            )
            .fetch_all(&mut stk.crud.exec)
            .await
            .map(|ok_res| {
                ok_res
                    .into_iter()
                    .map(TargetPositionsQtyDiff::Stock)
                    .collect()
            }),

            Self::Options(opt) => sqlx::query_as!(
                TargetOptionPositionsQtyDiff,
                r#"
                SELECT
                    COALESCE(t.stock, c.stock) AS "stock!",
                    COALESCE(t.primary_exchange, c.primary_exchange) AS "primary_exchange!",
                    COALESCE(t.currency, c.currency) AS "currency!",
                    COALESCE(t.expiry, c.expiry) AS "expiry!",
                    COALESCE(t.strike, c.strike) AS "strike!",
                    COALESCE(t.multiplier, c.multiplier) AS "multiplier!",
                    COALESCE(t.option_type, c.option_type) AS "option_type!:OptionType",
                    COALESCE(t.strategy, c.strategy) AS "strategy!",
                    COALESCE(t.quantity, 0) - COALESCE(c.quantity, 0) AS "qty_diff!",
                    COALESCE(c.quantity, 0.0) AS "current_qty!",
                    COALESCE(t.avg_price, 0.0) AS "avg_price!"
                FROM trading.target_option_positions t
                FULL OUTER JOIN trading.current_option_positions  c
                    ON t.stock = c.stock 
                    AND t.primary_exchange = c.primary_exchange
                    AND t.currency = c.currency
                    AND t.expiry = c.expiry 
                    AND t.strike = c.strike
                    AND t.multiplier = c.multiplier
                    AND t.option_type = c.option_type
                    AND t.strategy = c.strategy
                WHERE COALESCE(t.strategy, c.strategy) = $1;
                "#,
                strategy,
            )
            .fetch_all(&mut opt.crud.exec)
            .await
            .map(|ok_res| {
                ok_res
                    .into_iter()
                    .map(TargetPositionsQtyDiff::Options)
                    .collect()
            }),
        };
        result.map_err(|e| {
            format!(
                "Error retrieving qty difference in stocks for strategy: {}",
                e
            )
        })
    }

    async fn clear_strat_pos(&mut self, strategy: &str) -> Result<(), String> {
        let result = match self {
            Self::Stock(stk) => {
                sqlx::query!(
                    r#"
                    DELETE
                    FROM trading.target_stock_positions t
                    WHERE strategy = $1;
                    "#,
                    strategy,
                )
                .execute(&mut stk.crud.exec)
                .await
            }
            Self::Options(opt) => {
                sqlx::query!(
                    r#"
                    DELETE
                    FROM trading.target_option_positions t
                    WHERE strategy = $1;
                    "#,
                    strategy,
                )
                .execute(&mut opt.crud.exec)
                .await
            }
        };
        result.map_err(|e| {
            format!(
                "Error retrieving qty difference in stocks for strategy: {}",
                e
            )
        })?;
        Ok(())
    }
}
