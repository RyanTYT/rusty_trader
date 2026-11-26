//! Shared harness for the database test tree.
//!
//! Role: one connection pool and one serialized test slot, plus a small
//! session abstraction so the same CRUD assertions run against both
//! `DbDirect` (pool-backed, non-atomic) and `DbTxn` (one transaction).

#![allow(dead_code)]

pub mod scenario;

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::{Mutex, MutexGuard};

use trading_app::database::db::{DbConn, DbDirect, DbTxn, Exec};

static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
static SEQ: AtomicU64 = AtomicU64::new(0);

/// A unique, human-readable suffix. Keeps rows from different tests and from
/// repeated runs from colliding, without requiring table truncation.
pub fn tag(prefix: &str) -> String {
    format!(
        "{prefix}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// A pool plus the exclusive test slot. Drop the value to release the slot.
pub struct TestDb {
    pub pool: PgPool,
    url: String,
    _guard: MutexGuard<'static, ()>,
}

impl TestDb {
    pub async fn setup() -> Self {
        let guard = TEST_MUTEX.get_or_init(|| Mutex::new(())).lock().await;
        let url = std::env::var("TEST_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .expect("set TEST_DATABASE_URL or DATABASE_URL to a live Postgres");

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .expect("connect test database");

        truncate_tables(&pool)
            .await
            .expect("reset test tables before the test");

        Self {
            pool,
            url,
            _guard: guard,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionKind {
    Direct,
    Txn,
}

/// One of the two supported execution targets, exposing the same `DbConn`
/// surface so a test can be written once and run under both.
pub struct TestSession {
    kind: SessionKind,
    direct: Option<DbDirect>,
    txn: Option<DbTxn>,
}

impl TestSession {
    pub async fn new(pool: &PgPool, kind: SessionKind) -> Result<Self, String> {
        Ok(match kind {
            SessionKind::Direct => Self {
                kind,
                direct: Some(DbDirect::connect(pool)),
                txn: None,
            },
            SessionKind::Txn => Self {
                kind,
                direct: None,
                txn: Some(DbTxn::begin(pool).await.map_err(|e| e.to_string())?),
            },
        })
    }

    /// Commit a transaction; a no-op for `Direct`.
    pub async fn finish(mut self) -> Result<(), String> {
        if self.kind == SessionKind::Txn {
            self.txn
                .take()
                .expect("txn session has a transaction")
                .commit()
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Roll back a transaction; a no-op for `Direct`.
    pub async fn rollback(mut self) -> Result<(), String> {
        if self.kind == SessionKind::Txn {
            self.txn
                .take()
                .expect("txn session has a transaction")
                .rollback()
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

impl DbConn for TestSession {
    fn exec(&mut self) -> Exec<'_> {
        match self.kind {
            SessionKind::Direct => self
                .direct
                .as_mut()
                .expect("direct session has a DbDirect")
                .exec(),
            SessionKind::Txn => self.txn.as_mut().expect("txn session has a DbTxn").exec(),
        }
    }
}

/// Tables owned by the database test suite, kept in sync with
/// `src/database/models_crud`. `CASCADE` follows the foreign keys.
const TEST_TABLES: &str = "\
    logs.cancelled_orders, \
    logs.logs, \
    market_data.historical_data, \
    market_data.historical_forex_data, \
    market_data.historical_options_data, \
    trading.current_option_positions, \
    trading.current_stock_positions, \
    trading.notifications, \
    trading.open_option_orders, \
    trading.open_stock_orders, \
    trading.option_transactions, \
    trading.staged_commissions, \
    trading.stock_transactions, \
    trading.strategy, \
    trading.target_option_positions, \
    trading.target_stock_positions";

async fn truncate_tables(pool: &PgPool) -> Result<(), sqlx::Error> {
    let sql = format!("TRUNCATE TABLE {TEST_TABLES} RESTART IDENTITY CASCADE");
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

impl Drop for TestDb {
    fn drop(&mut self) {
        // A blocking cleanup on a dedicated thread: test panics still unwind
        // through this Drop, so the next run starts from an empty database.
        let url = self.url.clone();
        let _ = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async move {
                let Ok(pool) = PgPoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(std::time::Duration::from_secs(5))
                    .connect(&url)
                    .await
                else {
                    return;
                };
                let _ = sqlx::query("SET statement_timeout = 5000")
                    .execute(&pool)
                    .await;
                let _ = truncate_tables(&pool).await;
                pool.close().await;
            });
        })
        .join();
    }
}
