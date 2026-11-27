//! Transactions: one row per IBKR execution, split by asset class.
//!
//! [`TransactionsCRUD`] dispatches to `trading.stock_transactions` (stock,
//! future, CFD, forex and cash) or `trading.option_transactions` (options).
//! Each row is keyed by `execution_id` and records the strategy, a signed
//! `quantity` (positive for `BOT`, negative for every other side), the price,
//! and `fees` (seeded to zero and later filled by the staged-commission
//! trigger).
//!
//! `time` is the IBKR execution time in `"yyyyMMdd HH:mm:ss <IANA zone>"`
//! form: it is parsed with that zone and stored in UTC. Option rows record
//! `execution.average_price`; stock rows record `execution.price`.
//!
//! Reads come in two shapes: the newest row for one underlying
//! ([`TransactionsOps::read_last_transaction`]) and all rows for a strategy in
//! ascending time order ([`TransactionsOps::read_all_transactions_of_strat`]).
//! Both return [`TransactionsFullKeys`]; the enum is never decoded from SQL
//! directly - the concrete inner row is decoded first and then wrapped.
//!
//! # Example
//!
//! Seed two stock transactions for one strategy and one for another, then
//! assert the newest-by-underlying row and the strategy listing (ascending
//! time, optional inclusive `since`). Runs against `TEST_DATABASE_URL`;
//! skipped otherwise.
//!
//! ```rust
//! use chrono::{Duration, Utc};
//! use rust_decimal::Decimal;
//! use trading_app::database::crud::CRUDTrait;
//! use trading_app::database::db::DbDirect;
//! use trading_app::database::models::{
//!     Status, StockTransactionsFullKeys, StockTransactionsPrimaryKeys,
//!     StrategyFullKeys, StrategyPrimaryKeys,
//! };
//! use trading_app::database::models_crud::transactions::stock_transactions::StockTransactionsUnderlyingPrimaryKeys;
//! use trading_app::database::models_crud::transactions::transactions::{
//!     TransactionsFullKeys, TransactionsOps, TransactionsUnderlyingPrimaryKeys,
//! };
//! use trading_app::database::tables::DbTables;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
//! # let pool = sqlx::PgPool::connect(&url).await?;
//! let strategy = format!("doc_transactions_{}", std::process::id());
//! let other = format!("{strategy}_other");
//! let (id1, id2) = (format!("{strategy}_1"), format!("{strategy}_2"));
//! let symbol = format!("TXD{}", std::process::id());
//! let t2 = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
//!     .expect("valid timestamp");
//! let t1 = t2 - Duration::minutes(10);
//!
//! let mut db = DbDirect::connect(&pool);
//! for name in [&strategy, &other] {
//!     db.strategy()
//!         .create(&StrategyFullKeys { strategy: name.to_string(), status: Status::Active })
//!         .await?;
//! }
//!
//! let row = |id: &str, strategy: &str, stock: &str, time: chrono::DateTime<Utc>, qty: f64| {
//!     StockTransactionsFullKeys {
//!         execution_id: id.to_string(),
//!         strategy: strategy.to_string(),
//!         stock: stock.to_string(),
//!         primary_exchange: "NASDAQ".into(),
//!         currency: "USD".into(),
//!         order_perm_id: 1,
//!         time,
//!         price: 100.0,
//!         quantity: qty,
//!         fees: Decimal::ZERO,
//!     }
//! };
//! let first = row(&id1, &strategy, &symbol, t1, 10.0);
//! let second = row(&id2, &strategy, &symbol, t2, 20.0);
//! let unrelated = row(&format!("{other}_1"), &other, &format!("{symbol}_O"), t2, 5.0);
//! for row in [&first, &second, &unrelated] {
//!     db.stock_transactions().create(row).await?;
//! }
//!
//! let underlying = TransactionsUnderlyingPrimaryKeys::Stock(
//!     StockTransactionsUnderlyingPrimaryKeys {
//!         stock: symbol.clone(),
//!         primary_exchange: "NASDAQ".into(),
//!         currency: "USD".into(),
//!     },
//! );
//! let last = db
//!     .transactions(&trading_app::database::models::AssetType::Stock)
//!     .read_last_transaction(underlying)
//!     .await
//!     .expect("read last transaction")
//!     .expect("a transaction exists");
//! assert_eq!(last, TransactionsFullKeys::Stock(second.clone())); // newest by time
//!
//! let all = db
//!     .transactions(&trading_app::database::models::AssetType::Stock)
//!     .read_all_transactions_of_strat(&strategy, None)
//!     .await
//!     .expect("read strategy transactions");
//! assert_eq!(
//!     all,
//!     vec![
//!         TransactionsFullKeys::Stock(first.clone()),
//!         TransactionsFullKeys::Stock(second.clone()),
//!     ]
//! ); // ascending time
//!
//! // `since` is inclusive: only the row at exactly `t2` remains.
//! let since = db
//!     .transactions(&trading_app::database::models::AssetType::Stock)
//!     .read_all_transactions_of_strat(&strategy, Some(t2))
//!     .await
//!     .expect("read since t2");
//! assert_eq!(since, vec![TransactionsFullKeys::Stock(second.clone())]);
//!
//! // Clean up transactions, then the strategies they reference.
//! for id in [&id1, &id2, &format!("{other}_1")] {
//!     db.stock_transactions()
//!         .delete(&StockTransactionsPrimaryKeys { execution_id: id.to_string() })
//!         .await?;
//! }
//! db.strategy()
//!     .delete(&StrategyPrimaryKeys { strategy: strategy.clone() })
//!     .await?;
//! db.strategy()
//!     .delete(&StrategyPrimaryKeys { strategy: other })
//!     .await?;
//! # Ok(())
//! # }
//! ```

use chrono::{DateTime, NaiveDateTime, Utc};
use ibapi::orders::ExecutionData;
use rust_decimal::dec;
use crate::{
    database::{
        db::Exec,
        models::{
            AssetType, OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys,
            OptionTransactionsUpdateKeys, OptionType, StockTransactionsFullKeys,
            StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys,
        },
        models_crud::transactions::{
            option_transactions::{
                OptionTransactionsCRUD, OptionTransactionsUnderlyingPrimaryKeys,
            },
            stock_transactions::{StockTransactionsCRUD, StockTransactionsUnderlyingPrimaryKeys},
        },
    },
    helpers::contract::get_local_symbol,
    implement_crud_trait_for_interface,
};

/// Handle over the two transaction tables, selected by asset class.
///
/// Build it with [`TransactionsCRUD::stock`], [`TransactionsCRUD::option`] or
/// [`TransactionsCRUD::from_exec`]; the variant decides which table every
/// CRUD call and every [`TransactionsOps`] query uses.
pub enum TransactionsCRUD<'a> {
    /// Rows in `trading.stock_transactions` (stock, future, CFD, forex, cash).
    Stock(StockTransactionsCRUD<'a>),
    /// Rows in `trading.option_transactions`.
    Options(OptionTransactionsCRUD<'a>),
}

#[derive(Debug, Clone, PartialEq)]
/// A transaction row, tagged with the table it came from.
pub enum TransactionsFullKeys {
    /// A row from `trading.stock_transactions`.
    Stock(StockTransactionsFullKeys),
    /// A row from `trading.option_transactions`.
    Options(OptionTransactionsFullKeys),
}

impl TransactionsFullKeys {
    /// Builds the insert row for one execution under `strategy`.
    ///
    /// The execution time is parsed as `"yyyyMMdd HH:mm:ss <IANA zone>"` and
    /// converted to UTC. `quantity` is positive when `execution.side == "BOT"`
    /// and negative for every other side. Fees start at zero; the
    /// staged-commission trigger fills them later. Option rows use
    /// `execution.average_price`, stock rows use `execution.price`.
    ///
    /// # Panics
    ///
    /// Panics when the timestamp, date or timezone cannot be parsed, when the
    /// contract's right is not a valid [`OptionType`], or when the asset type
    /// is [`AssetType::Unknown`] or [`AssetType::CASH`] (the caller must reject
    /// those before inserting).
    pub fn from_strat_and_exec(strategy: &str, execution_data: &ExecutionData) -> Self {
        // E.g.: "20260101 15:55:00 US/Eastern"
        let (dt_str, tz_str) = execution_data
            .execution
            .time
            .rsplit_once(' ')
            .expect("Invalid timestamp");
        let execution_time = NaiveDateTime::parse_from_str(dt_str, "%Y%m%d %H:%M:%S")
            .expect("Invalid date format")
            .and_local_timezone(tz_str.parse::<chrono_tz::Tz>().expect("Invalid timezone"))
            .single()
            .expect("Ambiguous local time");

        match AssetType::from_str(&execution_data.contract.security_type) {
            AssetType::Option => Self::Options(OptionTransactionsFullKeys {
                strategy: strategy.to_string(),
                execution_id: execution_data.execution.execution_id.to_string(),
                order_perm_id: execution_data.execution.perm_id,
                stock: execution_data.contract.symbol.as_str().to_string(),
                primary_exchange: execution_data.contract.primary_exchange.to_string(),
                currency: execution_data.contract.currency.to_string(),
                expiry: execution_data
                    .contract
                    .last_trade_date_or_contract_month
                    .clone(),
                strike: execution_data.contract.strike.clone(),
                multiplier: execution_data.contract.multiplier.clone(),
                option_type: OptionType::from_str(&execution_data.contract.right).expect(
                    "Error parsing OptionType from contract right in update_option_execution",
                ),
                time: execution_time.to_utc(),

                price: execution_data.execution.average_price,
                quantity: if execution_data.execution.side == "BOT" {
                    execution_data.execution.shares
                } else {
                    -execution_data.execution.shares
                },
                fees: dec!(0),
            }),
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                let stock = get_local_symbol(&execution_data.contract);
                Self::Stock(StockTransactionsFullKeys {
                    strategy: strategy.to_string(),
                    execution_id: execution_data.execution.execution_id.to_string(),
                    order_perm_id: execution_data.execution.perm_id,
                    stock: stock,
                    primary_exchange: execution_data.contract.primary_exchange.to_string(),
                    currency: execution_data.contract.currency.to_string(),
                    time: execution_time.to_utc(),
                    price: execution_data.execution.price,
                    quantity: if execution_data.execution.side == "BOT" {
                        execution_data.execution.shares
                    } else {
                        -execution_data.execution.shares
                    },
                    fees: dec!(0),
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
}

/// Never decodes a raw row: the concrete inner type is decoded by its own
/// CRUD and then wrapped into this enum.
impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for TransactionsFullKeys {
    fn from_row(_: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        // This will never be executed because the inner CRUD<FK,PK,UK>
        // decodes the inner concrete struct before wrapping it into this enum.
        Err(sqlx::Error::Decode(
            "TransactionsFullKeys cannot be decoded directly from a raw SQL row".into(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Identifies one transaction row by its IBKR `execution_id`.
pub enum TransactionsPrimaryKeys {
    /// Key into `trading.stock_transactions`.
    Stock(StockTransactionsPrimaryKeys),
    /// Key into `trading.option_transactions`.
    Options(OptionTransactionsPrimaryKeys),
}

impl TransactionsPrimaryKeys {
    /// Builds the row key for the execution's asset class.
    ///
    /// # Panics
    ///
    /// Panics for [`AssetType::Unknown`] and [`AssetType::CASH`], which can
    /// never produce a stored transaction.
    pub fn from(execution_data: &ExecutionData) -> Self {
        match AssetType::from_str(&execution_data.contract.security_type) {
            AssetType::Option => Self::Options(OptionTransactionsPrimaryKeys {
                execution_id: execution_data.execution.execution_id.to_string(),
            }),
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                Self::Stock(StockTransactionsPrimaryKeys {
                    execution_id: execution_data.execution.execution_id.to_string(),
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
}

#[derive(Debug, Clone, PartialEq)]
/// Update payload for a transaction row.
///
/// Production writes do not currently use it (fees are applied by trigger),
/// but it completes the generic CRUD contract for the interface.
pub enum TransactionsUpdateKeys {
    /// Update fields for a stock transaction.
    Stock(StockTransactionsUpdateKeys),
    /// Update fields for an option transaction.
    Options(OptionTransactionsUpdateKeys),
}

#[derive(Debug, Clone, PartialEq)]
/// Identifies the instrument a transaction belongs to, not the row.
///
/// Stocks use stock/exchange/currency; options add expiry, strike, multiplier
/// and option type. Used by [`TransactionsOps::read_last_transaction`].
pub enum TransactionsUnderlyingPrimaryKeys {
    /// Instrument key for `trading.stock_transactions`.
    Stock(StockTransactionsUnderlyingPrimaryKeys),
    /// Instrument key for `trading.option_transactions`.
    Options(OptionTransactionsUnderlyingPrimaryKeys),
}

#[hotpath::measure_all]
impl<'a> TransactionsCRUD<'a> {
    /// Borrows the active table's executor.
    ///
    /// Used by the raw-SQL [`TransactionsOps`] reads, which need the same
    /// transaction or held connection as the CRUD methods.
    pub fn exec(&mut self) -> &mut Exec<'a> {
        match self {
            Self::Stock(stk) => &mut stk.crud.exec,
            Self::Options(opt) => &mut opt.crud.exec,
        }
    }

    /// Wraps a stock-transactions handle.
    pub fn stock(exec: Exec<'a>) -> Self {
        Self::Stock(StockTransactionsCRUD::new(exec))
    }

    /// Wraps an option-transactions handle.
    pub fn option(exec: Exec<'a>) -> Self {
        Self::Options(OptionTransactionsCRUD::new(exec))
    }

    /// Dispatches by asset class.
    ///
    /// Stock, future, CFD, forex and cash map to the stock table; options map
    /// to the option table.
    ///
    /// # Panics
    ///
    /// Panics for [`AssetType::Unknown`].
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
    TransactionsCRUD,
    TransactionsFullKeys,
    TransactionsPrimaryKeys,
    TransactionsUpdateKeys,
    [Stock, Options]
);

#[async_trait::async_trait]
/// Instrument-scoped transaction reads.
///
/// The implementation runs against the variant selected when the handle was
/// built, so the caller chooses the table by choosing the asset class.
pub trait TransactionsOps {
    /// Returns the newest transaction for one underlying, or `None` when the
    /// instrument has no rows.
    ///
    /// Stock rows match on stock/exchange/currency; option rows also match
    /// expiry, strike, multiplier and option type. The query is
    /// `ORDER BY time DESC LIMIT 1`.
    ///
    /// # Errors
    ///
    /// Returns the SQL error as a `String` (the message keeps the historical
    /// "Error when updating unknown strategy in stock positions" wording).
    ///
    /// # Example
    ///
    /// Two AAPL fills for one strategy: the newer one is returned; another
    /// instrument's fill is ignored.
    ///
    /// ```rust
    /// use chrono::{Duration, Utc};
    /// use rust_decimal::Decimal;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, Status, StockTransactionsFullKeys, StockTransactionsPrimaryKeys,
    ///     StrategyFullKeys, StrategyPrimaryKeys,
    /// };
    /// use trading_app::database::models_crud::transactions::stock_transactions::StockTransactionsUnderlyingPrimaryKeys;
    /// use trading_app::database::models_crud::transactions::transactions::{
    ///     TransactionsFullKeys, TransactionsOps, TransactionsUnderlyingPrimaryKeys,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let strategy = format!("doc_tx_last_{}", std::process::id());
    /// let other = format!("{strategy}_other");
    /// let symbol = format!("TXL{}", std::process::id());
    /// let mut db = DbDirect::connect(&pool);
    /// for name in [strategy.as_str(), other.as_str()] {
    ///     db.strategy()
    ///         .create(&StrategyFullKeys {
    ///             strategy: name.to_string(),
    ///             status: Status::Active,
    ///         })
    ///         .await?;
    /// }
    ///
    /// let t0 = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
    ///     .expect("valid timestamp");
    /// let fill = |id: String, strategy: &str, stock: &str, time, quantity| {
    ///     StockTransactionsFullKeys {
    ///         execution_id: id,
    ///         strategy: strategy.to_string(),
    ///         stock: stock.to_string(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         order_perm_id: 1,
    ///         time,
    ///         price: 100.0,
    ///         quantity,
    ///         fees: Decimal::ZERO,
    ///     }
    /// };
    /// let first = fill(format!("{strategy}_1"), &strategy, &symbol, t0, 10.0);
    /// let second = fill(
    ///     format!("{strategy}_2"),
    ///     &strategy,
    ///     &symbol,
    ///     t0 + Duration::seconds(1),
    ///     20.0,
    /// );
    /// let unrelated = fill(
    ///     format!("{other}_1"),
    ///     &other,
    ///     &format!("{symbol}_O"),
    ///     t0 + Duration::seconds(2),
    ///     5.0,
    /// );
    /// for row in [&first, &second, &unrelated] {
    ///     db.stock_transactions().create(row).await?;
    /// }
    ///
    /// let last = db
    ///     .transactions(&AssetType::Stock)
    ///     .read_last_transaction(TransactionsUnderlyingPrimaryKeys::Stock(
    ///         StockTransactionsUnderlyingPrimaryKeys {
    ///             stock: symbol.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///         },
    ///     ))
    ///     .await?;
    /// assert_eq!(last, Some(TransactionsFullKeys::Stock(second.clone())));
    ///
    /// // Cleanup.
    /// for row in [first, second, unrelated] {
    ///     db.stock_transactions()
    ///         .delete(&StockTransactionsPrimaryKeys {
    ///             execution_id: row.execution_id,
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
    async fn read_last_transaction(
        &mut self,
        pk: TransactionsUnderlyingPrimaryKeys,
    ) -> Result<Option<TransactionsFullKeys>, String>;

    /// Returns every transaction for `strategy` in ascending `time` order.
    ///
    /// When `since_opt` is `Some(t)`, only rows with `time >= t` are returned;
    /// the bound is inclusive. The handle's variant selects the table.
    ///
    /// # Errors
    ///
    /// Returns the SQL error as a `String`.
    ///
    /// # Example
    ///
    /// Two fills for the strategy are returned oldest-first; another
    /// strategy's fill is excluded. `Some(t)` is an inclusive lower bound.
    ///
    /// ```rust
    /// use chrono::{Duration, Utc};
    /// use rust_decimal::Decimal;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, Status, StockTransactionsFullKeys, StockTransactionsPrimaryKeys,
    ///     StrategyFullKeys, StrategyPrimaryKeys,
    /// };
    /// use trading_app::database::models_crud::transactions::transactions::TransactionsOps;
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// use trading_app::database::models_crud::transactions::transactions::TransactionsFullKeys;
    /// let strategy = format!("doc_tx_all_{}", std::process::id());
    /// let other = format!("{strategy}_other");
    /// let symbol = format!("TXA{}", std::process::id());
    /// let mut db = DbDirect::connect(&pool);
    /// for name in [strategy.as_str(), other.as_str()] {
    ///     db.strategy()
    ///         .create(&StrategyFullKeys {
    ///             strategy: name.to_string(),
    ///             status: Status::Active,
    ///         })
    ///         .await?;
    /// }
    ///
    /// let t0 = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
    ///     .expect("valid timestamp");
    /// let fill = |id: String, strategy: &str, time| StockTransactionsFullKeys {
    ///     execution_id: id,
    ///     strategy: strategy.to_string(),
    ///     stock: symbol.clone(),
    ///     primary_exchange: "NASDAQ".into(),
    ///     currency: "USD".into(),
    ///     order_perm_id: 1,
    ///     time,
    ///     price: 100.0,
    ///     quantity: 10.0,
    ///     fees: Decimal::ZERO,
    /// };
    /// let first = fill(format!("{strategy}_1"), &strategy, t0);
    /// let second = fill(
    ///     format!("{strategy}_2"),
    ///     &strategy,
    ///     t0 + Duration::seconds(1),
    /// );
    /// let unrelated = fill(
    ///     format!("{other}_1"),
    ///     &other,
    ///     t0 + Duration::seconds(2),
    /// );
    /// for row in [&first, &second, &unrelated] {
    ///     db.stock_transactions().create(row).await?;
    /// }
    ///
    /// let all = db
    ///     .transactions(&AssetType::Stock)
    ///     .read_all_transactions_of_strat(&strategy, None)
    ///     .await?;
    /// assert_eq!(
    ///     all,
    ///     vec![
    ///         TransactionsFullKeys::Stock(first.clone()),
    ///         TransactionsFullKeys::Stock(second.clone()),
    ///     ]
    /// );    ///
    /// // Inclusive lower bound: only the second row remains.
    /// let since = db
    ///     .transactions(&AssetType::Stock)
    ///     .read_all_transactions_of_strat(&strategy, Some(t0 + Duration::seconds(1)))
    ///     .await?;
    /// assert_eq!(
    ///     since,
    ///     vec![TransactionsFullKeys::Stock(second.clone())]
    /// );
    ///
    /// // Cleanup.
    /// for row in [first, second, unrelated] {
    ///     db.stock_transactions()
    ///         .delete(&StockTransactionsPrimaryKeys {
    ///             execution_id: row.execution_id,
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
    async fn read_all_transactions_of_strat(
        &mut self,
        strategy: &str,
        since_opt: Option<DateTime<Utc>>,
    ) -> Result<Vec<TransactionsFullKeys>, String>;
}

/// Dispatches each read to the stock or option SQL arm. The `since` arms are
/// duplicated only because `sqlx::query_as!` needs one literal query per
/// branch.
#[async_trait::async_trait]
#[hotpath::measure_all]
impl TransactionsOps for TransactionsCRUD<'_> {
    async fn read_last_transaction(
        &mut self,
        pk: TransactionsUnderlyingPrimaryKeys,
    ) -> Result<Option<TransactionsFullKeys>, String> {
        let result = match pk {
            TransactionsUnderlyingPrimaryKeys::Stock(StockTransactionsUnderlyingPrimaryKeys {
                stock,
                primary_exchange,
                currency,
            }) => sqlx::query_as!(
                StockTransactionsFullKeys,
                r#"
                    SELECT
                        execution_id as "execution_id!",
                        strategy as "strategy!",
                        stock as "stock!",
                        primary_exchange as "primary_exchange!",
                        currency as "currency!",
                        order_perm_id as "order_perm_id!",
                        time as "time!",
                        price as "price!",
                        quantity as "quantity!",
                        fees as "fees!"
                    FROM trading.stock_transactions
                    WHERE stock = $1
                        AND primary_exchange = $2
                        AND currency = $3
                    ORDER BY time DESC
                    LIMIT 1;
                    "#,
                stock,
                primary_exchange,
                currency
            )
            .fetch_optional(self.exec())
            .await
            .map(|ok_res| ok_res.map(TransactionsFullKeys::Stock)),
            TransactionsUnderlyingPrimaryKeys::Options(
                OptionTransactionsUnderlyingPrimaryKeys {
                    stock,
                    primary_exchange,
                    currency,
                    expiry,
                    strike,
                    multiplier,
                    option_type,
                },
            ) => sqlx::query_as!(
                OptionTransactionsFullKeys,
                r#"
                    SELECT 
                        execution_id as "execution_id!",
                        strategy as "strategy!",
                        stock as "stock!",
                        primary_exchange as "primary_exchange!",
                        currency as "currency!",
                        expiry as "expiry!",
                        strike as "strike!",
                        multiplier as "multiplier!",
                        option_type as "option_type!:OptionType",
                        order_perm_id as "order_perm_id!",
                        time as "time!",
                        price as "price!",
                        quantity as "quantity!",
                        fees as "fees!"
                    FROM trading.option_transactions
                    WHERE stock = $1
                        AND primary_exchange = $2
                        AND currency = $3
                        AND expiry = $4
                        AND strike = $5
                        AND multiplier = $6
                        AND option_type = $7
                    ORDER BY time DESC
                    LIMIT 1;
                    "#,
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
            .map(|ok_res| ok_res.map(TransactionsFullKeys::Options)),
        };

        result.map_err(|e| {
            format!(
                "Error when updating unknown strategy in stock positions: {}",
                e
            )
        })
    }

    async fn read_all_transactions_of_strat(
        &mut self,
        strategy: &str,
        since_opt: Option<DateTime<Utc>>,
    ) -> Result<Vec<TransactionsFullKeys>, String> {
        let result = match since_opt {
            Some(since) => match self {
                TransactionsCRUD::Stock(stk) => sqlx::query_as!(
                    StockTransactionsFullKeys,
                    r#"
                    SELECT
                        execution_id as "execution_id!",
                        strategy as "strategy!",
                        stock as "stock!",
                        primary_exchange as "primary_exchange!",
                        currency as "currency!",
                        order_perm_id as "order_perm_id!",
                        time as "time!",
                        price as "price!",
                        quantity as "quantity!",
                        fees as "fees!"
                    FROM trading.stock_transactions
                    WHERE strategy = $1
                        AND time >= $2
                    ORDER BY time ASC;
                    "#,
                    strategy,
                    since
                )
                .fetch_all(&mut stk.crud.exec)
                .await
                .map(|ok_res| {
                    ok_res
                        .into_iter()
                        .map(TransactionsFullKeys::Stock)
                        .collect()
                }),
                TransactionsCRUD::Options(opt) => sqlx::query_as!(
                    OptionTransactionsFullKeys,
                    r#"
                    SELECT 
                        execution_id as "execution_id!",
                        strategy as "strategy!",
                        stock as "stock!",
                        primary_exchange as "primary_exchange!",
                        currency as "currency!",
                        expiry as "expiry!",
                        strike as "strike!",
                        multiplier as "multiplier!",
                        option_type as "option_type!:OptionType",
                        order_perm_id as "order_perm_id!",
                        time as "time!",
                        price as "price!",
                        quantity as "quantity!",
                        fees as "fees!"
                    FROM trading.option_transactions
                    WHERE strategy = $1
                        AND time >= $2
                    ORDER BY time ASC;
                    "#,
                    strategy,
                    since
                )
                .fetch_all(&mut opt.crud.exec)
                .await
                .map(|ok_res| {
                    ok_res
                        .into_iter()
                        .map(TransactionsFullKeys::Options)
                        .collect()
                }),
            },
            None => match self {
                TransactionsCRUD::Stock(stk) => sqlx::query_as!(
                    StockTransactionsFullKeys,
                    r#"
                SELECT
                    execution_id as "execution_id!",
                    strategy as "strategy!",
                    stock as "stock!",
                    primary_exchange as "primary_exchange!",
                    currency as "currency!",
                    order_perm_id as "order_perm_id!",
                    time as "time!",
                    price as "price!",
                    quantity as "quantity!",
                    fees as "fees!"
                FROM trading.stock_transactions
                WHERE strategy = $1
                ORDER BY time ASC;
                "#,
                    strategy
                )
                .fetch_all(&mut stk.crud.exec)
                .await
                .map(|ok_res| {
                    ok_res
                        .into_iter()
                        .map(TransactionsFullKeys::Stock)
                        .collect()
                }),
                TransactionsCRUD::Options(opt) => sqlx::query_as!(
                    OptionTransactionsFullKeys,
                    r#"
                SELECT 
                    execution_id as "execution_id!",
                    strategy as "strategy!",
                    stock as "stock!",
                    primary_exchange as "primary_exchange!",
                    currency as "currency!",
                    expiry as "expiry!",
                    strike as "strike!",
                    multiplier as "multiplier!",
                    option_type as "option_type!:OptionType",
                    order_perm_id as "order_perm_id!",
                    time as "time!",
                    price as "price!",
                    quantity as "quantity!",
                    fees as "fees!"
                FROM trading.option_transactions
                WHERE strategy = $1
                ORDER BY time ASC;
                "#,
                    strategy
                )
                .fetch_all(&mut opt.crud.exec)
                .await
                .map(|ok_res| {
                    ok_res
                        .into_iter()
                        .map(TransactionsFullKeys::Options)
                        .collect()
                }),
            },
        };

        result.map_err(|e| {
            format!(
                "Error when updating unknown strategy in stock positions: {}",
                e
            )
        })
    }
}
