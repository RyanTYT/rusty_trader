//! Database sessions: the two entry points for running CRUD operations.
//!
//! `DbTxn` wraps one transaction; `DbDirect` is pool-backed or holds one
//! connection. Both expose `DbConn::exec`, and the blanket `DbTables` impl
//! (see `database::tables`) turns any session into a set of model accessors:
//!
//! ```ignore
//! let mut db = DbDirect::connect(&pool);
//! db.open_orders(&asset).read(&pk).await?;
//!
//! let mut txn = DbTxn::begin(&pool).await?;
//! txn.transactions(&asset).create(&fk).await?;
//! txn.commit().await?;
//! ```

#![allow(dead_code)]

use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};

pub use crate::database::exec::Exec;

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error("{operation} failed: {source}")]
    Query {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },

    #[error("key variant mismatch during {operation}")]
    KeyVariantMismatch { operation: &'static str },

    #[error("invalid table name: {0}")]
    InvalidTable(String),

    #[error("row decode failed: {0}")]
    Decode(String),
}

/// Temporary bridge so existing `Result<_, String>` functions can keep using
/// `?` while call sites are migrated incrementally.
impl From<DbError> for String {
    fn from(err: DbError) -> Self {
        err.to_string()
    }
}

/// The two things the table accessors need from a session.
pub trait DbConn {
    fn exec(&mut self) -> Exec<'_>;
}

/// Atomic session: everything executed through it runs in one transaction.
pub struct DbTxn {
    tx: sqlx::Transaction<'static, Postgres>,
}

impl DbTxn {
    pub async fn begin(pool: &PgPool) -> Result<Self, DbError> {
        Ok(Self {
            tx: pool.begin().await?,
        })
    }

    pub async fn commit(self) -> Result<(), DbError> {
        Ok(self.tx.commit().await?)
    }

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
pub struct DbDirect {
    pool: PgPool,
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
    fn exec(&mut self) -> Exec<'_> {
        match &mut self.held {
            Some(conn) => Exec::Conn(&mut **conn),
            None => Exec::Pool(&self.pool),
        }
    }
}


