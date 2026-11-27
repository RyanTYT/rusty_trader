//! Execution seam: where a query runs.
//!
//! `Exec` is either a borrowed pool (acquire per statement) or a borrowed
//! connection (reuse one). It implements `sqlx::Executor` both by value and
//! by `&mut`, so a model accessor can hand out a borrow without rebuilding
//! the enum.

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqlx::postgres::{PgArguments, PgQueryResult, PgRow, PgTypeInfo};
use sqlx::query::{Query, QueryAs};
use sqlx::{Describe, Either, Execute, Executor, FromRow, PgConnection, PgPool, Postgres};

/// Where a query runs.
///
/// `Pool` mirrors sqlx's `&PgPool` executor: a connection is acquired and
/// released per statement. `Conn` reuses the caller's connection (a transaction
/// or a held `DbDirect` session).
///
/// # Examples
///
/// Run a query directly against the pool:
///
/// ```no_run
/// use trading_app::database::exec::Exec;
///
/// # async fn example(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
/// let mut exec = Exec::Pool(pool);
/// let one = exec
///     .fetch_optional(sqlx::query_as::<_, (i64,)>("SELECT 1"))
///     .await?;
/// # let _ = one;
/// # Ok(())
/// # }
/// ```
///
/// Model accessors construct the right variant for you; see
/// [`crate::database::db::DbTxn`] and [`crate::database::db::DbDirect`].
///
/// # Invariants
///
/// `Exec` never owns a connection. The owning session ([`crate::database::db::DbTxn`]
/// or [`crate::database::db::DbDirect`]) must outlive it and is responsible for
/// commit/rollback or returning the connection to the pool.
pub enum Exec<'a> {
    /// Pool-backed: a connection is acquired and returned per statement.
    Pool(&'a PgPool),
    /// Borrowed connection: reused for the caller's session (transaction or
    /// held direct connection).
    Conn(&'a mut PgConnection),
}

impl Exec<'_> {
    /// Executes a statement and returns sqlx's [`PgQueryResult`] (row count and
    /// command tag). Used for INSERT/UPDATE/DELETE.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use sqlx::query;
    /// # use trading_app::database::exec::Exec;
    /// # async fn example(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    /// let mut exec = Exec::Pool(pool);
    /// let result = exec.execute(query("SELECT 1")).await?;
    /// # let _ = result;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(
        &mut self,
        query: Query<'_, Postgres, PgArguments>,
    ) -> Result<PgQueryResult, sqlx::Error> {
        match self {
            Exec::Pool(pool) => query.execute(*pool).await,
            Exec::Conn(conn) => query.execute(&mut **conn).await,
        }
    }

    /// Runs a typed query and returns at most one row (`None` when the result
    /// set is empty). Used for primary-key lookups.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use sqlx::query_as;
    /// # use trading_app::database::exec::Exec;
    /// # async fn example(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    /// let mut exec = Exec::Pool(pool);
    /// let row = exec.fetch_optional(query_as::<_, (i64,)>("SELECT 1")).await?;
    /// # let _ = row;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Runs a typed query and collects every row. Used for unbounded table
    /// reads and non-unique lookups.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use sqlx::query_as;
    /// # use trading_app::database::exec::Exec;
    /// # async fn example(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    /// let mut exec = Exec::Pool(pool);
    /// let rows = exec.fetch_all(query_as::<_, (i64,)>("SELECT 1")).await?;
    /// # let _ = rows;
    /// # Ok(())
    /// # }
    /// ```
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
///
/// This by-value impl consumes the short-lived `Exec` (it is cheap to build);
/// the `&mut Exec` impl below lets model enums lend their executor without
/// rebuilding it.
///
/// ```no_run
/// # use sqlx::query_as;
/// # use trading_app::database::exec::Exec;
/// # async fn example(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
/// let exec = Exec::Pool(pool);
/// let rows = query_as::<_, (i64,)>("SELECT 1").fetch_all(exec).await?;
/// # let _ = rows;
/// # Ok(())
/// # }
/// ```
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
