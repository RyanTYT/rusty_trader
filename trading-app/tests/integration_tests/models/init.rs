//! Shared infrastructure for per-table DB integration tests.
//!
//! These tests require a live Postgres database (DATABASE_URL env var).
//! The `setup_test_db` helper connects, runs migrations, and returns a pool.
//! `TEST_MUTEX` serializes all DB tests to avoid concurrent-access issues.

use std::sync::LazyLock;

use sqlx::{PgPool, migrate::Migrator, postgres::PgPoolOptions};
use tokio::sync::{Mutex, OnceCell};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");
static POOL: OnceCell<PgPool> = OnceCell::const_new();
static MIGRATED: OnceCell<()> = OnceCell::const_new();
pub static TEST_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn setup_test_db() -> PgPool {
    let database_url = std::env::var("DATABASE_URL")
        .expect("Expected DATABASE_URL environment variable to be set!");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");

    // Run migrations once
    MIGRATED
        .get_or_init(|| async {
            MIGRATOR.run(&pool).await.expect("Migration failed");
        })
        .await;

    POOL.set(pool.clone()).ok();

    pool
}

/// Runs the test inside a rollbackable transaction.
/// This ensures changes are not persisted after the test.
pub async fn with_rollback<T, F, Fut>(pool: &PgPool, test: F)
where
    F: FnOnce(sqlx::Transaction<'_, sqlx::Postgres>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let tx = pool.begin().await.expect("Failed to begin transaction");
    test(tx).await;
}

#[macro_export]
macro_rules! init_strat {
    ($pool:expr) => {
        trading_app::database::models_crud::strategy::StrategyCRUD::new($pool.clone())
            .create_or_ignore(&trading_app::database::models::StrategyFullKeys {
                strategy: "noise".to_string(),
                status: trading_app::database::models::Status::Inactive,
            })
            .await
            .expect("expected to be able to create or update strategy");
    };
}

#[macro_export]
macro_rules! del_strat {
    ($pool:expr) => {
        trading_app::database::models_crud::strategy::StrategyCRUD::new($pool.clone())
            .delete(&trading_app::database::models::StrategyPrimaryKeys {
                strategy: "noise".to_string(),
            })
            .await
            .expect("expected to be able to delete strategy");
        assert!(
            trading_app::database::models_crud::strategy::StrategyCRUD::new($pool.clone())
                .read_all()
                .await
                .expect("expected to be able to read all strategies")
                .len()
                == 0
        )
    };
}
