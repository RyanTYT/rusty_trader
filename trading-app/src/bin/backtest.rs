//! Backtest binary entry point.
//!
//! Gated on the `backtest` cargo feature via `required-features` in Cargo.toml.
//! Build & run:
//!   cargo run --bin backtest --features backtest
//!
//! The binary does EVERYTHING:
//!   1. Connects to test-db (TEST_TRADING_DB_URL or DATABASE_URL).
//!   2. Runs SQLx migrations (so the schema exists — no sqlx-cli needed).
//!   3. Calls `run_backtest` which:
//!      a. Data loader (IBKR via with_gateway_retry, Alpaca fallback).
//!      b. Replayer (real on_bar_update + real handle_bar_update_outcome).
//!      c. Results (PnL, equity curve, max DD, Sharpe, etc.) → JSON.

use trading_app::backtester;

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    let database_url = std::env::var("TEST_TRADING_DB_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map_err(|_| "DATABASE_URL or TEST_TRADING_DB_URL must be set".to_string())?;

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .map_err(|e| format!("failed to connect to {database_url}: {e}"))?;

    // Run migrations so the schema exists (no sqlx-cli needed in the image).
    if let Err(e) = sqlx::migrate!("./migrations").run(&pool).await {
        return Err(format!("migration error: {e}"));
    }

    backtester::run_backtest(pool).await
}
