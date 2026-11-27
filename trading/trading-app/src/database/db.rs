//! Database sessions: the two entry points for running CRUD operations.
//!
//! `DbTxn` wraps one transaction; `DbDirect` is pool-backed or holds one
//! connection. Both expose `DbConn::exec`, and the blanket `DbTables` impl
//! (see `database::tables`) turns any session into a set of model accessors:
//!
//! ```no_run
//! use trading_app::database::crud::CRUDTrait;
//! use trading_app::database::db::{DbDirect, DbTxn};
//! use trading_app::database::models::{AssetType, Status, StrategyFullKeys};
//! use trading_app::database::tables::DbTables;
//!
//! # async fn example(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! // One statement: pool-backed, no transaction.
//! let mut db = DbDirect::connect(pool);
//! let open = db.open_orders(&AssetType::Stock).read_all().await?;
//!
//! // Several writes that must succeed together.
//! let mut txn = DbTxn::begin(pool).await?;
//! txn.strategy()
//!     .create(&StrategyFullKeys { strategy: "demo".into(), status: Status::Active })
//!     .await?;
//! txn.commit().await?;
//! # let _ = open;
//! # Ok(())
//! # }
//! ```

#![allow(dead_code)]

use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};

pub use crate::database::exec::Exec;

/// Errors surfaced by the database layer.
///
/// CRUD methods attach the *operation* (`create`, `read`, ...) when the
/// failure comes from the statement; plain [`DbError::Sqlx`] is used for
/// infrastructure failures such as `BEGIN`/`COMMIT`, pool acquisition, or
/// row decoding.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// Infrastructure or driver error with no CRUD operation context.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    /// A CRUD statement failed; `operation` names what was attempted.
    #[error("{operation} failed: {source}")]
    Query {
        /// The CRUD operation that failed (`create`, `read`, `update`, ...).
        operation: &'static str,
        /// Underlying driver error.
        #[source]
        source: sqlx::Error,
    },

    /// A key enum and an interface enum disagreed on the asset class (for
    /// example, a stock key passed to an option CRUD). Nothing was executed.
    #[error("key variant mismatch during {operation}")]
    KeyVariantMismatch {
        /// The CRUD operation that rejected the pair.
        operation: &'static str,
    },

    /// A dynamically built table name failed validation. The model wrappers
    /// use constants, so this is reserved for future dynamic callers.
    #[error("invalid table name: {0}")]
    InvalidTable(String),

    /// A row could not be decoded into its model type.
    #[error("row decode failed: {0}")]
    Decode(String),
}

/// Temporary bridge so existing `Result<_, String>` functions can keep using
/// `?` while call sites are migrated incrementally.
///
/// New code should return [`DbError`] directly; the conversion loses the
/// structured variant (only the `Display` text survives).
impl From<DbError> for String {
    fn from(err: DbError) -> Self {
        err.to_string()
    }
}

/// A session that can lend an executor to a model.
///
/// Implemented by [`DbTxn`] and [`DbDirect`]. The returned [`Exec`] borrows the
/// session mutably, so one session can run only one operation at a time;
/// issue operations sequentially.
pub trait DbConn {
    /// Borrows the session as a query executor for one operation.
    fn exec(&mut self) -> Exec<'_>;
}

/// Atomic session: every operation runs inside one transaction.
///
/// `BEGIN` happens in [`DbTxn::begin`]; nothing is durable until
/// [`DbTxn::commit`]. Dropping an uncommitted session rolls back (sqlx's
/// transaction drop guard), so failure paths can simply return early.
///
/// Keep transactions short: do not hold one across IBKR calls, sleeps, or
/// spawned tasks. A transaction is not a lock — concurrent transactions can
/// still race; use `read_for_update` or a serialization point when needed.
///
/// # Examples
///
/// Two tables, committed together. Against `TEST_DATABASE_URL` this runs and
/// asserts that both rows are visible after the commit; without it, it is
/// skipped.
///
/// ```rust
/// use trading_app::database::crud::CRUDTrait;
/// use trading_app::database::db::{DbDirect, DbTxn};
/// use trading_app::database::models::{
///     StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, Status, StrategyFullKeys,
///     StrategyPrimaryKeys,
/// };
/// use trading_app::database::tables::DbTables;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
/// # let pool = sqlx::PgPool::connect(&url).await?;
/// let strategy = format!("doc_txn_commit_{}", std::process::id());
/// let execution = format!("{strategy}_exec");
///
/// let mut txn = DbTxn::begin(&pool).await?;
/// txn.strategy()
///     .create(&StrategyFullKeys { strategy: strategy.clone(), status: Status::Active })
///     .await?;
/// txn.staged_commissions()
///     .create(&StagedCommissionsFullKeys {
///         execution_id: execution.clone(),
///         fees: rust_decimal::Decimal::new(25, 2),
///     })
///     .await?;
/// txn.commit().await?;
///
/// let mut db = DbDirect::connect(&pool);
/// assert!(db
///     .strategy()
///     .read(&StrategyPrimaryKeys { strategy: strategy.clone() })
///     .await?
///     .is_some());
/// assert!(db
///     .staged_commissions()
///     .read(&StagedCommissionsPrimaryKeys { execution_id: execution.clone() })
///     .await?
///     .is_some());
///
/// // Clean up so repeated doctest runs stay independent.
/// db.strategy()
///     .delete(&StrategyPrimaryKeys { strategy })
///     .await?;
/// db.staged_commissions()
///     .delete(&StagedCommissionsPrimaryKeys { execution_id: execution })
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// A failing second statement rolls the first back:
///
/// ```rust
/// use trading_app::database::crud::CRUDTrait;
/// use trading_app::database::db::{DbDirect, DbTxn};
/// use trading_app::database::models::{
///     StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, Status, StrategyFullKeys,
///     StrategyPrimaryKeys,
/// };
/// use trading_app::database::tables::DbTables;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
/// # let pool = sqlx::PgPool::connect(&url).await?;
/// let strategy = format!("doc_txn_rollback_{}", std::process::id());
/// let execution = format!("{strategy}_exec");
/// let fee = rust_decimal::Decimal::new(25, 2);
///
/// // Seed the row the transaction will collide with.
/// let mut db = DbDirect::connect(&pool);
/// db.staged_commissions()
///     .create(&StagedCommissionsFullKeys { execution_id: execution.clone(), fees: fee })
///     .await?;
///
/// let mut txn = DbTxn::begin(&pool).await?;
/// txn.strategy()
///     .create(&StrategyFullKeys { strategy: strategy.clone(), status: Status::Active })
///     .await?; // succeeds
/// let duplicate = txn
///     .staged_commissions()
///     .create(&StagedCommissionsFullKeys { execution_id: execution.clone(), fees: fee })
///     .await; // primary-key conflict
/// assert!(duplicate.is_err());
/// txn.rollback().await?; // dropping the session would do the same
///
/// assert!(db
///     .strategy()
///     .read(&StrategyPrimaryKeys { strategy: strategy.clone() })
///     .await?
///     .is_none());
/// db.staged_commissions()
///     .delete(&StagedCommissionsPrimaryKeys { execution_id: execution })
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Returning early before `commit` is safe: the drop guard rolls back.
pub struct DbTxn {
    /// The open transaction. Private: access it through [`DbConn::exec`].
    tx: sqlx::Transaction<'static, Postgres>,
}

impl DbTxn {
    /// Opens a transaction on a pooled connection. The connection stays
    /// checked out until commit or rollback.
    pub async fn begin(pool: &PgPool) -> Result<Self, DbError> {
        Ok(Self {
            tx: pool.begin().await?,
        })
    }

    /// Commits the transaction and returns the connection to the pool.
    /// Consumes the session so it cannot be used afterwards.
    pub async fn commit(self) -> Result<(), DbError> {
        Ok(self.tx.commit().await?)
    }

    /// Rolls the transaction back explicitly. Consumes the session; dropping
    /// an uncommitted session has the same effect.
    pub async fn rollback(self) -> Result<(), DbError> {
        Ok(self.tx.rollback().await?)
    }
}

impl DbConn for DbTxn {
    fn exec(&mut self) -> Exec<'_> {
        Exec::Conn(&mut *self.tx)
    }
}

/// Non-atomic session: either pool-backed (acquire per statement) or holding a
/// single connection for the session.
///
/// Neither mode provides isolation between statements. Use [`DbTxn`] when
/// several writes must succeed together; use `DbDirect` for single statements,
/// idempotent read-only work, or sequences of independent writes.
///
/// # Example
///
/// Sequential statements on one session; there is no rollback, so this mode is
/// for independent operations:
///
/// ```rust
/// use trading_app::database::crud::CRUDTrait;
/// use trading_app::database::db::DbDirect;
/// use trading_app::database::models::{Status, StrategyFullKeys, StrategyPrimaryKeys};
/// use trading_app::database::tables::DbTables;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
/// # let pool = sqlx::PgPool::connect(&url).await?;
/// let strategy = format!("doc_direct_{}", std::process::id());
/// let mut db = DbDirect::connect(&pool);
///
/// db.strategy()
///     .create(&StrategyFullKeys { strategy: strategy.clone(), status: Status::Active })
///     .await?;
/// assert!(db
///     .strategy()
///     .read(&StrategyPrimaryKeys { strategy: strategy.clone() })
///     .await?
///     .is_some());
///
/// db.strategy()
///     .delete(&StrategyPrimaryKeys { strategy: strategy.clone() })
///     .await?;
/// assert!(db
///     .strategy()
///     .read(&StrategyPrimaryKeys { strategy })
///     .await?
///     .is_none());
/// # Ok(())
/// # }
/// ```
///
/// Use [`DbDirect::hold`] when a long sequence of statements should reuse one
/// connection (still without atomicity).
pub struct DbDirect {
    /// Pool used to acquire a connection per statement in the `connect` mode.
    pool: PgPool,
    /// When present, the single connection reused for the whole session.
    held: Option<PoolConnection<Postgres>>,
}

impl DbDirect {
    /// Pool-backed: no connection is acquired until a statement runs.
    pub fn connect(pool: &PgPool) -> Self {
        Self {
            pool: pool.clone(),
            held: None,
        }
    }

    /// Hold one connection for the lifetime of the session.
    ///
    /// This amortizes acquisition and preserves session state; it does **not**
    /// provide isolation between statements. Use `DbTxn` for that.
    pub async fn hold(pool: &PgPool) -> Result<Self, DbError> {
        Ok(Self {
            pool: pool.clone(),
            held: Some(pool.acquire().await?),
        })
    }
}

impl DbConn for DbDirect {
    /// Lends the held connection when present; otherwise lends the pool,
    /// which acquires/releases a connection per statement.
    fn exec(&mut self) -> Exec<'_> {
        match &mut self.held {
            Some(conn) => Exec::Conn(&mut **conn),
            None => Exec::Pool(&self.pool),
        }
    }
}


