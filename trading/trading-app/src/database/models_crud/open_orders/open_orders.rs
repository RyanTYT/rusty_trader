//! Open orders: the working-order rows used for reconciliation and sizing.
//!
//! A working order lives in exactly one of two tables —
//! `trading.open_stock_orders` or `trading.open_option_orders` — selected by
//! [`AssetType`]. [`OpenOrdersCRUD`] is the interface: [`OpenOrdersCRUD::Stock`]
//! wraps the stock table, [`OpenOrdersCRUD::Options`] wraps the option table,
//! and every operation dispatches on the variant.
//!
//! # Row semantics
//!
//! - `quantity` is **signed**: positive for buys, negative for sells.
//! - `strategy` is `order.order_ref` copied verbatim. The engine writes both
//!   bare strategy names and `"<strategy>:<price>"` keys, so do not parse it
//!   unless you know which producer wrote the row.
//! - `executions` is the set of execution ids already applied to the row;
//!   `filled` is the cumulative filled quantity (the execution handler uses
//!   `max(cumulative_quantity)` so reconnects and gaps cannot regress it).
//!
//! # Errors and panics
//!
//! Constructors panic for `AssetType::CASH` and `AssetType::Unknown`: neither
//! can address a working-order table. Rows are built only from a valid
//! [`Contract`].
//!
//! # Backtesting
//!
//! With the `backtest` feature,
//! [`OpenOrdersOps::get_orders_for_strat`] can read the in-memory resting book
//! (`BacktestMode::InMemory`) instead of SQL. That arm returns only
//! resting+active rows, signs quantities, and fails closed when no state is
//! installed for the strategy.
//!
//! # Example
//!
//! Seed two working orders for one strategy and one for another, then assert
//! that the strategy query returns exactly its two rows and that the delete
//! removes only those. Runs against `TEST_DATABASE_URL`; skipped otherwise.
//!
//! ```rust
//! use trading_app::database::crud::CRUDTrait;
//! use trading_app::database::db::DbDirect;
//! use trading_app::database::models::{
//!     AssetType, OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, Status,
//!     StrategyFullKeys, StrategyPrimaryKeys,
//! };
//! use trading_app::database::models_crud::open_orders::open_orders::{
//!     OpenOrdersFullKeys, OpenOrdersOps,
//! };
//! use trading_app::database::tables::DbTables;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
//! # let pool = sqlx::PgPool::connect(&url).await?;
//! let strategy = format!("doc_open_orders_{}", std::process::id());
//! let other = format!("{strategy}_other");
//! let base = (std::process::id() % 10_000) as i32 * 10;
//!
//! let mut db = DbDirect::connect(&pool);
//! for name in [&strategy, &other] {
//!     db.strategy()
//!         .create(&StrategyFullKeys { strategy: name.to_string(), status: Status::Active })
//!         .await?;
//! }
//!
//! let order = |strategy: &str, perm_id: i32, order_id: i32, qty: f64| {
//!     OpenStockOrdersFullKeys {
//!         order_perm_id: perm_id,
//!         order_id,
//!         strategy: strategy.to_string(),
//!         stock: "AAPL".into(),
//!         primary_exchange: "NASDAQ".into(),
//!         currency: "USD".into(),
//!         time: chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros())
//!             .expect("valid timestamp"),
//!         quantity: qty,
//!         parent_order_id: 0,
//!         executions: Vec::new(),
//!         filled: 0.0,
//!     }
//! };
//! let first = order(&strategy, base + 1, base + 1, 10.0);
//! let second = order(&strategy, base + 2, base + 2, 25.0);
//! let third = order(&other, base + 3, base + 3, -5.0);
//! for row in [&first, &second, &third] {
//!     db.open_stock_orders().create(row).await?;
//! }
//!
//! let mut working = db
//!     .open_orders(&AssetType::Stock)
//!     .get_orders_for_strat(&strategy)
//!     .await
//!     .expect("read open orders");
//! working.sort_by_key(|row| match row {
//!     OpenOrdersFullKeys::Stock(order) => order.order_id,
//!     OpenOrdersFullKeys::Options(_) => unreachable!("stock strategy"),
//! });
//! assert_eq!(
//!     working,
//!     vec![
//!         OpenOrdersFullKeys::Stock(first.clone()),
//!         OpenOrdersFullKeys::Stock(second.clone()),
//!     ]
//! );
//!
//! let deleted = db
//!     .open_orders(&AssetType::Stock)
//!     .delete_orders_for_strat(&strategy)
//!     .await
//!     .expect("delete open orders");
//! assert_eq!(deleted, 2);
//! let remaining = db.open_orders(&AssetType::Stock).read_all().await?;
//! assert!(!remaining.iter().any(
//!     |row| matches!(row, OpenOrdersFullKeys::Stock(o) if o.strategy == strategy)
//! ));
//! let survivor = remaining
//!     .iter()
//!     .find(|row| matches!(row, OpenOrdersFullKeys::Stock(o) if o.order_perm_id == base + 3))
//!     .expect("the other strategy's order survives");
//! assert_eq!(survivor, &OpenOrdersFullKeys::Stock(third.clone()));
//!
//! // Clean up the surviving row and both strategy rows.
//! db.open_stock_orders()
//!     .delete(&OpenStockOrdersPrimaryKeys { order_perm_id: base + 3, order_id: base + 3 })
//!     .await?;
//! db.strategy()
//!     .delete(&StrategyPrimaryKeys { strategy: strategy.clone() })
//!     .await?;
//! db.strategy()
//!     .delete(&StrategyPrimaryKeys { strategy: other })
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::{
    database::{
        db::Exec,
        models::{
            AssetType, OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys,
            OpenOptionOrdersUpdateKeys, OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys,
            OpenStockOrdersUpdateKeys, OptionType,
        },
        models_crud::open_orders::{
            open_option_orders::OpenOptionOrdersCRUD, open_stock_orders::OpenStockOrdersCRUD,
        },
    },
    helpers::contract::get_local_symbol,
    implement_crud_trait_for_interface,
};
use chrono::Utc;
use ibapi::{
    contracts::Contract,
    orders::{Action, ExecutionData, Order},
};

/// Interface over the stock and option working-order tables.
///
/// Each variant owns a concrete table wrapper and therefore the same
/// `Exec` borrow. Construct one through [`OpenOrdersCRUD::from_exec`] (or
/// the concrete [`OpenOrdersCRUD::stock`] / [`OpenOrdersCRUD::option`]).
pub enum OpenOrdersCRUD<'a> {
    /// Rows in `trading.open_stock_orders` (also used for futures, CFDs and
    /// FX pairs).
    Stock(OpenStockOrdersCRUD<'a>),
    /// Rows in `trading.open_option_orders`.
    Options(OpenOptionOrdersCRUD<'a>),
}

#[derive(Debug, Clone, PartialEq)]
/// One working-order row, tagged by asset class.
///
/// `Stock` also covers futures, CFDs and FX pairs because they share the
/// stock table; `Options` is the option table.
pub enum OpenOrdersFullKeys {
    /// Stock-table row.
    Stock(OpenStockOrdersFullKeys),
    /// Option-table row.
    Options(OpenOptionOrdersFullKeys),
}

impl OpenOrdersFullKeys {
    /// The row's **signed** quantity: positive for a buy, negative for a
    /// sell.
    pub fn get_qty(&self) -> f64 {
        match self {
            OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys { quantity, .. }) => *quantity,
            OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys { quantity, .. }) => *quantity,
        }
    }

    /// Build a row from an IBKR [`Contract`] and [`Order`].
    ///
    /// `strategy` is copied verbatim from `order.order_ref`; `executions`
    /// starts empty; `filled` is the caller-supplied fill amount; `quantity`
    /// is signed from [`Action`] (`Sell` is negative).
    ///
    /// # Panics
    ///
    /// Panics for `AssetType::CASH` and `AssetType::Unknown`. Option rows also
    /// panic when `contract.right` is not a recognised call/put string.
    pub fn from_contract_and_order(contract: &Contract, order: &Order, filled: f64) -> Self {
        let asset_type = AssetType::from_str(&contract.security_type);
        match asset_type {
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                let qty = {
                    if order.action == Action::Sell {
                        -1.0
                    } else {
                        1.0
                    }
                } * order.total_quantity;

                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                    order_perm_id: order.perm_id,
                    order_id: order.order_id,
                    strategy: order.order_ref.clone(),
                    stock: get_local_symbol(&contract),
                    primary_exchange: contract.primary_exchange.to_string(),
                    currency: contract.currency.to_string(),
                    time: Utc::now(),
                    quantity: qty,
                    parent_order_id: order.parent_id,
                    filled: filled,
                    executions: Vec::new(),
                })
            }
            AssetType::Option => {
                let qty = {
                    if order.action == Action::Sell {
                        -1.0
                    } else {
                        1.0
                    }
                } * order.total_quantity;

                Self::Options(OpenOptionOrdersFullKeys {
                    order_id: order.order_id,
                    order_perm_id: order.perm_id,
                    strategy: order.order_ref.clone(),
                    stock: contract.symbol.as_str().to_string(),
                    primary_exchange: contract.primary_exchange.to_string(),
                    currency: contract.currency.to_string(),
                    expiry: contract.last_trade_date_or_contract_month.to_string(),
                    strike: contract.strike,
                    multiplier: contract.multiplier.to_string(),
                    option_type: crate::database::models::OptionType::from_str(&contract.right)
                        .unwrap_or_else(|e| panic!("{}", e)),
                    time: Utc::now(),
                    quantity: qty,
                    parent_order_id: order.parent_id,
                    filled: filled,
                    executions: Vec::new(),
                })
            }
            AssetType::CASH => {
                tracing::error!(
                    "Tried to create OpenOrdersFullKeys for CASH Asset Type: ({}, {})",
                    contract.symbol,
                    contract.security_type
                );
                panic!("Tried to create OpenOrdersFullKeys from AssetType::CASH");
            }
            AssetType::Unknown => {
                tracing::error!(
                    message=%format!("Tried to create OpenOrdersFullKeys for Unknown Asset Type: ({}, {})",
                    contract.symbol,
                    contract.security_type
                    )
                );
                panic!("Tried to create OpenOrdersFullKeys from AssetType::Unknown");
            }
        }
    }
}

// #[macro_export]
// macro_rules! extract_common_attr {
//     ($var:expr, $, $($variant:ident),* $(,)?) => {
//
//     };
// }

/// The interface enum is never decoded straight from a SQL row: the
/// concrete stock/option CRUD decodes its own struct and wraps it. This impl
/// exists only to satisfy generic bounds and always returns a decode error.
impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for OpenOrdersFullKeys {
    fn from_row(_: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        // This will never be executed because the inner CRUD<FK,PK,UK>
        // decodes the inner concrete struct before wrapping it into this enum.
        Err(sqlx::Error::Decode(
            "OpenOrdersFullKeys cannot be decoded directly from a raw SQL row".into(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Primary key of a working order, tagged by asset class.
///
/// Both tables key on `(order_perm_id, order_id)`; the tag selects which
/// table the key addresses.
pub enum OpenOrdersPrimaryKeys {
    /// Key in `trading.open_stock_orders`.
    Stock(OpenStockOrdersPrimaryKeys),
    /// Key in `trading.open_option_orders`.
    Options(OpenOptionOrdersPrimaryKeys),
}

impl OpenOrdersPrimaryKeys {
    /// Build a key for `asset_type`.
    ///
    /// # Panics
    ///
    /// Panics for `AssetType::CASH` and `AssetType::Unknown`; neither has a
    /// working-order table.
    pub fn new(asset_type: &AssetType, order_perm_id: i32, order_id: i32) -> Self {
        match asset_type {
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                Self::Stock(OpenStockOrdersPrimaryKeys {
                    order_perm_id: order_perm_id,
                    order_id: order_id,
                })
            }
            AssetType::Option => Self::Options(OpenOptionOrdersPrimaryKeys {
                order_perm_id: order_perm_id,
                order_id: order_id,
            }),
            AssetType::CASH => {
                panic!("Tried to construct OpenOrdersPrimaryKeys from CASH asset_type")
            }
            AssetType::Unknown => {
                panic!("Tried to construct OpenOrdersPrimaryKeys from unknown asset_type")
            }
        }
    }

    /// Build the key of the working order an execution belongs to.
    ///
    /// # Panics
    ///
    /// Panics for `AssetType::CASH` and `AssetType::Unknown`, matching
    /// [`OpenOrdersPrimaryKeys::new`].
    pub fn from_execution(execution_data: &ExecutionData) -> Self {
        match AssetType::from_str(&execution_data.contract.security_type) {
            AssetType::Option => Self::Options(OpenOptionOrdersPrimaryKeys {
                order_perm_id: execution_data.execution.perm_id,
                order_id: execution_data.execution.order_id,
            }),
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                Self::Stock(OpenStockOrdersPrimaryKeys {
                    order_perm_id: execution_data.execution.perm_id,
                    order_id: execution_data.execution.order_id,
                })
            }
            AssetType::Unknown => panic!(
                "Tried to construct OpenOrdersPrimaryKeys from unknown asset_type: {execution_data:?}"
            ),
            AssetType::CASH => panic!(
                "Tried to construct OpenOrdersPrimaryKeys from CASH asset_type: should not have been possible to construct from contract: {execution_data:?}"
            ),
        }
    }

    /// Re-key an existing row (the variant and both id columns are
    /// preserved).
    pub fn from_open_order(open_order: &OpenOrdersFullKeys) -> Self {
        match open_order {
            OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                order_perm_id,
                order_id,
                ..
            }) => OpenOrdersPrimaryKeys::Stock(OpenStockOrdersPrimaryKeys {
                order_perm_id: *order_perm_id,
                order_id: *order_id,
            }),
            OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                order_perm_id,
                order_id,
                ..
            }) => OpenOrdersPrimaryKeys::Options(OpenOptionOrdersPrimaryKeys {
                order_perm_id: *order_perm_id,
                order_id: *order_id,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Partial update for a working order, tagged by asset class.
///
/// Every field is `Some` for a full row refresh; `None` leaves that column
/// untouched because the generic update only emits `Some` columns.
pub enum OpenOrdersUpdateKeys {
    /// Stock-table update image.
    Stock(OpenStockOrdersUpdateKeys),
    /// Option-table update image.
    Options(OpenOptionOrdersUpdateKeys),
}

impl OpenOrdersUpdateKeys {
    /// Full update image with `filled = 0.0`.
    pub fn new(asset_type: &AssetType, contract: &Contract, order: &Order) -> Self {
        Self::from(asset_type, contract, order, 0.0)
    }

    /// Full update image recording `fill_amount` in `filled`.
    pub fn new_with_fill(
        asset_type: &AssetType,
        contract: &Contract,
        order: &Order,
        fill_amount: f64,
    ) -> Self {
        Self::from(asset_type, contract, order, fill_amount)
    }

    /// Shared constructor for [`OpenOrdersUpdateKeys::new`] and
    /// [`OpenOrdersUpdateKeys::new_with_fill`].
    ///
    /// # Panics
    ///
    /// Panics for `CASH`/`Unknown`, and for an unrecognised option right.
    fn from(
        asset_type: &AssetType,
        contract: &Contract,
        order: &Order,
        filled_amount: f64,
    ) -> Self {
        match asset_type {
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                Self::Stock(OpenStockOrdersUpdateKeys {
                    strategy: Some(order.order_ref.to_string()),
                    stock: Some(get_local_symbol(&contract)),
                    primary_exchange: Some(contract.primary_exchange.to_string()),
                    currency: Some(contract.currency.to_string()),
                    time: Some(Utc::now()),
                    quantity: Some(
                        order.total_quantity
                            * if order.action == Action::Buy {
                                1.0
                            } else {
                                -1.0
                            },
                    ),
                    parent_order_id: Some(order.parent_id),
                    executions: None,
                    filled: Some(filled_amount),
                })
            }
            AssetType::Option => Self::Options(OpenOptionOrdersUpdateKeys {
                strategy: Some(order.order_ref.to_string()),
                stock: Some(get_local_symbol(&contract)),
                primary_exchange: Some(contract.primary_exchange.to_string()),
                currency: Some(contract.currency.to_string()),
                expiry: Some(contract.last_trade_date_or_contract_month.to_string()),
                strike: Some(contract.strike),
                multiplier: Some(contract.multiplier.to_string()),
                option_type: Some(OptionType::from_str(&contract.right).unwrap()),
                time: Some(Utc::now()),
                quantity: Some(
                    order.total_quantity
                        * if order.action == Action::Buy {
                            1.0
                        } else {
                            -1.0
                        },
                ),
                parent_order_id: Some(order.parent_id),
                executions: None,
                filled: Some(filled_amount),
            }),
            AssetType::CASH => {
                panic!("Tried to construct OpenOrdersPrimaryKeys from CASH asset_type")
            }
            AssetType::Unknown => {
                panic!("Tried to construct OpenOrdersPrimaryKeys from unknown asset_type")
            }
        }
    }
}

#[hotpath::measure_all]
/// Construction and executor access for the interface.
impl<'a> OpenOrdersCRUD<'a> {
    /// Reborrow the inner executor for raw SQL that is not part of `CRUDTrait`.
    /// Reborrow the active table's executor for raw SQL that is not part of
    /// the generic CRUD surface.
    ///
    /// The borrow lasts as long as the returned reference; the caller cannot
    /// touch the CRUD again until it ends.
    pub fn exec(&mut self) -> &mut Exec<'a> {
        match self {
            Self::Stock(stk) => &mut stk.crud.exec,
            Self::Options(opt) => &mut opt.crud.exec,
        }
    }

    /// Bind the stock table to `exec`.
    pub fn stock(exec: Exec<'a>) -> Self {
        Self::Stock(OpenStockOrdersCRUD::new(exec))
    }

    /// Bind the option table to `exec`.
    pub fn option(exec: Exec<'a>) -> Self {
        Self::Options(OpenOptionOrdersCRUD::new(exec))
    }

    /// Build the interface for `asset_type`.
    ///
    /// `Stock`, `Future`, `CFD`, `ForexPair` and `CASH` map to the stock
    /// table; `Option` maps to the option table.
    ///
    /// # Panics
    ///
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

implement_crud_trait_for_interface!(
    OpenOrdersCRUD,
    OpenOrdersFullKeys,
    OpenOrdersPrimaryKeys,
    OpenOrdersUpdateKeys,
    [Stock, Options]
);

#[cfg(feature = "backtest")]
use crate::backtester::setup::config::BacktestMode;

#[async_trait::async_trait]
/// Strategy-scoped operations that do not fit the generic CRUD surface.
pub trait OpenOrdersOps {
    /// Every working order whose `strategy` column equals `strategy`.
    ///
    /// The match is exact: `"strat"` does not match rows stored under
    /// `"strat:1.25"`. Rows are returned in table order.
    ///
    /// # Backtesting
    ///
    /// With `backtest` and `BacktestMode::InMemory`, this reads the in-memory
    /// resting book instead of SQL. Only `Resting` + active rows are returned,
    /// quantities are signed, ids are `0`, and the call fails closed when no
    /// state is installed for the strategy.
    ///
    /// # Errors
    ///
    /// Returns the SQL error as a string, or the missing-state error in the
    /// in-memory arm.
    ///
    /// # Example
    ///
    /// Two working orders for the strategy and one for another strategy: only
    /// the first two are returned, with their signed quantities intact.
    ///
    /// ```rust
    /// use chrono::Utc;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, Status,
    ///     StrategyFullKeys, StrategyPrimaryKeys,
    /// };
    /// use trading_app::database::models_crud::open_orders::open_orders::{
    ///     OpenOrdersFullKeys, OpenOrdersOps,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let strategy = format!("doc_oo_get_{}", std::process::id());
    /// let other = format!("{strategy}_other");
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// for name in [strategy.as_str(), other.as_str()] {
    ///     db.strategy()
    ///         .create(&StrategyFullKeys {
    ///             strategy: name.to_string(),
    ///             status: Status::Active,
    ///         })
    ///         .await?;
    /// }
    /// let time = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
    ///     .expect("valid timestamp");
    /// let order = |id: i32, strategy: &str, quantity: f64| OpenStockOrdersFullKeys {
    ///     order_perm_id: id,
    ///     order_id: id,
    ///     strategy: strategy.to_string(),
    ///     stock: "AAPL".into(),
    ///     primary_exchange: "NASDAQ".into(),
    ///     currency: "USD".into(),
    ///     time,
    ///     quantity,
    ///     parent_order_id: 0,
    ///     executions: Vec::new(),
    ///     filled: 0.0,
    /// };
    /// let order_1 = order(11, &strategy, 10.0);
    /// let order_2 = order(12, &strategy, 25.0);
    /// let order_3 = order(13, &other, -5.0);
    /// db.open_stock_orders().create(&order_1).await?;
    /// db.open_stock_orders().create(&order_2).await?;
    /// db.open_stock_orders().create(&order_3).await?;
    ///
    /// let mut working = db
    ///     .open_orders(&AssetType::Stock)
    ///     .get_orders_for_strat(&strategy)
    ///     .await?;
    /// working.sort_by_key(|row| match row {
    ///     OpenOrdersFullKeys::Stock(row) => row.order_id,
    ///     OpenOrdersFullKeys::Options(row) => row.order_id,
    /// });
    /// assert_eq!(
    ///     working,
    ///     vec![
    ///         OpenOrdersFullKeys::Stock(order_1),
    ///         OpenOrdersFullKeys::Stock(order_2),
    ///     ]
    /// );
    ///
    /// // Cleanup.
    /// for id in [11, 12, 13] {
    ///     db.open_stock_orders()
    ///         .delete(&OpenStockOrdersPrimaryKeys {
    ///             order_perm_id: id,
    ///             order_id: id,
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
    async fn get_orders_for_strat(
        &mut self,
        strategy: &str,
        #[cfg(feature = "backtest")] mode: BacktestMode,
    ) -> Result<Vec<OpenOrdersFullKeys>, String>;

    /// Delete every working order for `strategy`.
    ///
    /// Returns the number of rows removed; `0` is not an error.
    ///
    /// # Errors
    ///
    /// Returns the SQL error as a string.
    ///
    /// # Example
    ///
    /// Deletes only the listed strategy's rows and reports the count; another
    /// strategy's order survives.
    ///
    /// ```rust
    /// use chrono::Utc;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, OpenStockOrdersFullKeys, OpenStockOrdersPrimaryKeys, Status,
    ///     StrategyFullKeys, StrategyPrimaryKeys,
    /// };
    /// use trading_app::database::models_crud::open_orders::open_orders::OpenOrdersOps;
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let strategy = format!("doc_oo_del_{}", std::process::id());
    /// let other = format!("{strategy}_other");
    /// // The primary key is only (order_perm_id, order_id), so derive ids from
    /// // the process id to keep concurrent/repeated doctest runs isolated.
    /// let base = (std::process::id() as i32 % 100_000) * 10;
    /// let (first, second, third) = (base + 1, base + 2, base + 3);
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// for name in [strategy.as_str(), other.as_str()] {
    ///     db.strategy()
    ///         .create(&StrategyFullKeys {
    ///             strategy: name.to_string(),
    ///             status: Status::Active,
    ///         })
    ///         .await?;
    /// }
    /// let order = |id: i32, strategy: &str, quantity: f64| OpenStockOrdersFullKeys {
    ///     order_perm_id: id,
    ///     order_id: id,
    ///     strategy: strategy.to_string(),
    ///     stock: "AAPL".into(),
    ///     primary_exchange: "NASDAQ".into(),
    ///     currency: "USD".into(),
    ///     time: chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
    ///         .expect("valid timestamp"),
    ///     quantity,
    ///     parent_order_id: 0,
    ///     executions: Vec::new(),
    ///     filled: 0.0,
    /// };
    /// let first_order = order(first, &strategy, 10.0);
    /// let second_order = order(second, &strategy, 25.0);
    /// let third_order = order(third, &other, -5.0);
    /// for row in [&first_order, &second_order, &third_order] {
    ///     db.open_stock_orders().create(row).await?;
    /// }
    ///
    /// let deleted = db
    ///     .open_orders(&AssetType::Stock)
    ///     .delete_orders_for_strat(&strategy)
    ///     .await?;
    /// assert_eq!(deleted, 2);
    ///
    /// let remaining = db.open_stock_orders().read_all().await?;
    /// assert!(remaining
    ///     .iter()
    ///     .all(|row| row.order_perm_id != first && row.order_perm_id != second));
    /// let remaining_order = remaining
    ///     .iter()
    ///     .find(|row| row.order_perm_id == third)
    ///     .expect("the other strategy's order survives");
    /// assert_eq!(remaining_order, &third_order);
    ///
    /// // Cleanup.
    /// db.open_stock_orders()
    ///     .delete(&OpenStockOrdersPrimaryKeys {
    ///         order_perm_id: third,
    ///         order_id: third,
    ///     })
    ///     .await?;
    /// for name in [strategy, other] {
    ///     db.strategy()
    ///         .delete(&StrategyPrimaryKeys { strategy: name })
    ///         .await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    async fn delete_orders_for_strat(&mut self, strategy: &str) -> Result<u64, String>;
}

#[async_trait::async_trait]
/// Dispatch [`OpenOrdersOps`] to the concrete table; the SQL and the
/// in-memory arm live in [`OpenOrdersOps`].
#[hotpath::measure_all]
impl<'a> OpenOrdersOps for OpenOrdersCRUD<'a> {
    async fn get_orders_for_strat(
        &mut self,
        strategy: &str,
        #[cfg(feature = "backtest")] mode: BacktestMode,
    ) -> Result<Vec<OpenOrdersFullKeys>, String> {
        #[cfg(feature = "backtest")]
        if matches!(mode, BacktestMode::InMemory) {
            // In-memory arm (explicit BacktestMode::InMemory): read the
            // strategy's InMemoryState resting book instead of the DB.
            // Resting+ACTIVE rows only — the in-memory book marks fills via
            // RestingStatus (Filled/Cancelled rows filtered out), so every
            // remaining row is wholly unfilled (`filled` = 0). order_id /
            // parent_order_id = 0: the dormant-children exclusion
            // (parent != 0 ∧ parent exists) never fires — the OCA-active
            // gate replaces it. Multi replay resolves any strategy via the
            // fill router; Db mode falls back to the CURRENT strategy's
            // state; no state → fail CLOSED (missing state = wiring bug).
            use crate::backtester::methods::in_memory::{
                bracket::RestingStatus, state as in_memory,
            };
            let Some(st) = in_memory::routed_state(strategy).or_else(|| in_memory::current())
            else {
                return Err(format!(
                    "get_orders_for_strat(InMemory): no InMemoryState installed for '{strategy}'"
                ));
            };
            let book = st.resting_orders.read().expect("resting_orders poisoned");
            let rows = book
                .iter()
                .filter(|ro| {
                    ro.status == RestingStatus::Resting && ro.active && ro.order_ref == strategy
                })
                .map(|ro| {
                    let signed = if matches!(ro.action, ibapi::orders::Action::Buy) {
                        ro.qty
                    } else {
                        -ro.qty
                    };
                    OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                        order_perm_id: 0,
                        order_id: 0,
                        strategy: strategy.to_string(),
                        stock: ro.contract.symbol.to_string(),
                        primary_exchange: ro.contract.primary_exchange.to_string(),
                        currency: ro.contract.currency.to_string(),
                        time: chrono::Utc::now(),
                        quantity: signed,
                        parent_order_id: 0,
                        executions: Vec::new(),
                        filled: 0.0,
                    })
                })
                .collect();
            return Ok(rows);
        }
        let result = match self {
            Self::Stock(stk) => {
                let query = sqlx::query_as!(
                    OpenStockOrdersFullKeys,
                    r#"
                        SELECT 
                            order_perm_id as "order_perm_id!",
                            order_id as "order_id!",
                            strategy as "strategy!",
                            stock as "stock!",
                            primary_exchange as "primary_exchange!",
                            currency as "currency!",
                            time as "time!",
                            quantity as "quantity!",
                            parent_order_id as "parent_order_id!",
                            executions as "executions!",
                            filled as "filled!"
                        FROM trading.open_stock_orders
                        WHERE strategy = $1;
                        "#,
                    strategy
                );
                query.fetch_all(&mut stk.crud.exec).await.map(|rows| {
                    rows.into_iter()
                        .map(OpenOrdersFullKeys::Stock)
                        .collect::<Vec<OpenOrdersFullKeys>>()
                })
            }
            Self::Options(opt) => {
                let query = sqlx::query_as!(
                    OpenOptionOrdersFullKeys,
                    r#"
                        SELECT 
                            order_perm_id as "order_perm_id!",
                            order_id as "order_id!",
                            strategy as "strategy!",
                            stock as "stock!",
                            primary_exchange as "primary_exchange!",
                            currency as "currency!",
                            expiry as "expiry!",
                            strike as "strike!",
                            multiplier as "multiplier!",
                            option_type AS "option_type!:OptionType",
                            time as "time!",
                            quantity as "quantity!",
                            parent_order_id as "parent_order_id!",
                            executions as "executions!",
                            filled as "filled!"
                        FROM trading.open_option_orders
                        WHERE strategy = $1;
                        "#,
                    strategy
                );
                query
                    .fetch_all(&mut opt.crud.exec)
                    .await
                    .map(|rows| rows.into_iter().map(OpenOrdersFullKeys::Options).collect())
            }
        };

        result.map_err(|e| format!("Failed to get_orders_for_strat: {e:?}"))
    }

    async fn delete_orders_for_strat(&mut self, strategy: &str) -> Result<u64, String> {
        let result = match self {
            Self::Stock(stk) => {
                let query = sqlx::query!(
                    r#"
                DELETE FROM trading.open_stock_orders
                WHERE strategy = $1;
                "#,
                    strategy
                );
                stk.crud.exec.execute(query).await
            }
            Self::Options(opt) => {
                let query = sqlx::query!(
                    r#"
                    DELETE FROM trading.open_option_orders
                    WHERE strategy = $1;
                    "#,
                    strategy
                );
                opt.crud.exec.execute(query).await
            }
        };

        result
            .map(|res| res.rows_affected())
            .map_err(|e| format!("Failed to delete_orders_for_strat: {e:?}"))
    }
}
