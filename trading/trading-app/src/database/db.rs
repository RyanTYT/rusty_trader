//! Database connection layer.
//!
//! `Exec` is the single execution seam used by the CRUD core: it is either a
//! borrowed pool (acquire per statement) or a borrowed connection (reuse one).
//! `DbTxn` and `DbDirect` are the two session entry points; model handles are
//! constructed per operation from `DbConn::exec`.

#![allow(dead_code)] // TODO: remove once this layer is wired into call sites

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgArguments, PgQueryResult, PgRow, PgTypeInfo};
use sqlx::query::{Query, QueryAs};
use sqlx::{Describe, Either, Execute, Executor, FromRow, PgConnection, PgPool, Postgres};

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

/// Where a query runs.
///
/// `Pool` mirrors sqlx's `&PgPool` executor: a connection is acquired and
/// released per statement. `Conn` reuses the caller's connection (a transaction
/// or a held `DbDirect` session).
///
/// For e.g.
///
/// let pg_executor = DbDirect::connect(pool: PgPool).exec();
/// pg_executor.fetch_all(query).await?;
///
/// let pg_transaction = DbTxn::begin(pool: PgPool).exec();
/// pg_transaction.execute( ... (some CRUD operation) ... ).await?;
/// pg_transaction.commit();
pub enum Exec<'a> {
    Pool(&'a PgPool),
    Conn(&'a mut PgConnection),
}

impl Exec<'_> {
    pub async fn execute(
        &mut self,
        query: Query<'_, Postgres, PgArguments>,
    ) -> Result<PgQueryResult, sqlx::Error> {
        match self {
            Exec::Pool(pool) => query.execute(*pool).await,
            Exec::Conn(conn) => query.execute(&mut **conn).await,
        }
    }

    pub async fn fetch_optional<O>(
        &mut self,
        query: QueryAs<'_, Postgres, O, PgArguments>,
    ) -> Result<Option<O>, sqlx::Error>
    where
        O: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        match self {
            Exec::Pool(pool) => query.fetch_optional(*pool).await,
            Exec::Conn(conn) => query.fetch_optional(&mut **conn).await,
        }
    }

    pub async fn fetch_all<O>(
        &mut self,
        query: QueryAs<'_, Postgres, O, PgArguments>,
    ) -> Result<Vec<O>, sqlx::Error>
    where
        O: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        match self {
            Exec::Pool(pool) => query.fetch_all(*pool).await,
            Exec::Conn(conn) => query.fetch_all(&mut **conn).await,
        }
    }

}
impl std::fmt::Debug for Exec<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Exec::Pool(_) => f.write_str("Exec::Pool"),
            Exec::Conn(_) => f.write_str("Exec::Conn"),
        }
    }
}

/// `Exec` is a transparent executor: queries can be run either through the
/// convenience methods above (`exec.fetch_all(query)`) or through sqlx's
/// native shape (`query.fetch_all(exec)`).
impl<'c, 'a: 'c> Executor<'c> for Exec<'a> {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<PgQueryResult, PgRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Postgres>,
    {
        match self {
            Exec::Pool(pool) => pool.fetch_many(query),
            Exec::Conn(conn) => conn.fetch_many(query),
        }
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<PgRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Postgres>,
    {
        match self {
            Exec::Pool(pool) => pool.fetch_optional(query),
            Exec::Conn(conn) => conn.fetch_optional(query),
        }
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [PgTypeInfo],
    ) -> BoxFuture<'e, Result<<Postgres as sqlx::Database>::Statement<'q>, sqlx::Error>>
    where
        'c: 'e,
    {
        match self {
            Exec::Pool(pool) => pool.prepare_with(sql, parameters),
            Exec::Conn(conn) => conn.prepare_with(sql, parameters),
        }
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Postgres>, sqlx::Error>>
    where
        'c: 'e,
    {
        match self {
            Exec::Pool(pool) => pool.describe(sql),
            Exec::Conn(conn) => conn.describe(sql),
        }
    }
}

/// Borrowed form of the executor. Model enums return `&mut Exec` from their
/// accessors, so a query can take the executor by value without the enum
/// having to manufacture a new one.
impl<'c, 'a: 'c> Executor<'c> for &'c mut Exec<'a> {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<PgQueryResult, PgRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Postgres>,
    {
        match self {
            Exec::Pool(pool) => (*pool).fetch_many(query),
            Exec::Conn(conn) => (&mut **conn).fetch_many(query),
        }
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<PgRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Postgres>,
    {
        match self {
            Exec::Pool(pool) => (*pool).fetch_optional(query),
            Exec::Conn(conn) => (&mut **conn).fetch_optional(query),
        }
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [PgTypeInfo],
    ) -> BoxFuture<'e, Result<<Postgres as sqlx::Database>::Statement<'q>, sqlx::Error>>
    where
        'c: 'e,
    {
        match self {
            Exec::Pool(pool) => (*pool).prepare_with(sql, parameters),
            Exec::Conn(conn) => (&mut **conn).prepare_with(sql, parameters),
        }
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Postgres>, sqlx::Error>>
    where
        'c: 'e,
    {
        match self {
            Exec::Pool(pool) => (*pool).describe(sql),
            Exec::Conn(conn) => (&mut **conn).describe(sql),
        }
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


