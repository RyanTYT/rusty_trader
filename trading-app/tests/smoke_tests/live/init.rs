//! Shared infrastructure for live IBKR smoke tests.
//!
//! Uses the new `with_gateway` / `with_gateway_retry` RAII API from `ibc.rs`:
//! the gateway is booted, a closure runs, and the gateway is shut down
//! automatically when the closure returns. This ensures every test properly
//! boots + cleanly shuts down the gateway — no port races, no leaked processes.
//!
//! `with_live_ibkr(account, log_file, |state| async { ... })` boots the gateway
//! + connects clients + runs the test body. When the test body returns, the
//! gateway is shut down automatically.
//!
//! # Environment variables
//! - `IBKR_ACCOUNT` — IBKR account number (default: `DU111111`)
//! - `IBKR_API_PORT_ADDR` — IB Gateway API address (default: `127.0.0.1:4002`)
//! - `SERVER_BASE_URL` — HTTP server base URL (default: `http://127.0.0.1:8000`)
//! - `DATABASE_URL` — Postgres connection string (required)
//!
//! Requires:
//! - IBC installed at `/IBCLinux-3.21.2/scripts/ibcstart.sh`
//! - IB Gateway credentials in env vars
//! - Postgres + DATABASE_URL
//! Run with: `DATABASE_URL=... cargo test --test smoke_tests -- --ignored`

use std::sync::Arc;
use std::time::Duration;

use ibapi::Client;
use sqlx::PgPool;
use trading_app::test_internals::{with_gateway_retry, IBGateway};

/// IBKR account number from env var (default: `DU111111`).
pub fn ibkr_account() -> String {
    std::env::var("IBKR_ACCOUNT").unwrap_or_else(|_| "DU111111".to_string())
}

/// IB Gateway API address from env var (default: `127.0.0.1:4002`).
pub fn api_port_addr() -> String {
    std::env::var("IBKR_API_PORT_ADDR").unwrap_or_else(|_| "127.0.0.1:4002".to_string())
}

/// HTTP server base URL from env var (default: `http://127.0.0.1:8000`).
pub fn server_base_url() -> String {
    std::env::var("SERVER_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:8000".to_string())
}

/// Live IBKR state — holds the connected clients + pool.
/// Does NOT hold a reference to `IBGateway`; the gateway is owned by
/// `with_gateway_retry` and kept alive for the closure's duration.
pub struct LiveIbkr {
    pub master_client: Arc<Client>,
    pub client_1: Arc<Client>,
    pub pool: PgPool,
}

/// Poll port until it's free (no longer accepting connections).
/// Returns true if the port was released within the timeout, false otherwise.
pub async fn wait_for_port_release(max_wait: Duration) -> bool {
    let poll_interval = Duration::from_millis(500);
    let start = std::time::Instant::now();
    loop {
        let still_bound = tokio::net::TcpStream::connect(api_port_addr()).await.is_ok();
        if !still_bound {
            return true;
        }
        if start.elapsed() >= max_wait {
            return false;
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// Wait for port to be bound (IB Gateway ready to accept connections).
#[allow(dead_code)]
pub async fn wait_for_port_bind(max_wait: Duration) -> bool {
    let poll_interval = Duration::from_millis(500);
    let start = std::time::Instant::now();
    loop {
        if tokio::net::TcpStream::connect(api_port_addr()).await.is_ok() {
            return true;
        }
        if start.elapsed() >= max_wait {
            return false;
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn connect_to_client_with_retry(
    client_id: i32,
    retry_times: u32,
) -> Result<Client, String> {
    // IB Gateway has a documented delay between "Login has completed" (UI ready)
    // and the API socket accepting connections. Use a short poll interval.
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);
    let addr = api_port_addr();
    let mut retry_time = 0;
    loop {
        match Client::connect(&addr, client_id) {
            Ok(c) => return Ok(c),
            Err(e) => {
                tracing::warn!(
                    "Connection to {} (client {}) failed (attempt {}): {}",
                    addr, client_id, retry_time + 1, e
                );
                retry_time += 1;
                if retry_time <= retry_times {
                    tokio::time::sleep(POLL_INTERVAL).await;
                    continue;
                }
                return Err(format!(
                    "Could not connect to client {} after {} attempts: {}",
                    client_id, retry_time, e
                ));
            }
        }
    }
}

pub async fn get_pool() -> Result<PgPool, String> {
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL env var not set".to_string())?;
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .map_err(|e| format!("Failed to connect to DB: {e}"))
}

/// Boot an IB Gateway + connect clients + run a closure with `LiveIbkr`.
///
/// Uses `with_gateway_retry` so the gateway is automatically shut down when
/// the closure returns — no manual cleanup needed, no port races.
///
/// The closure receives an owned `LiveIbkr` (clients + pool). The gateway is
/// kept alive by `with_gateway_retry` for the closure's duration — tests don't
/// need a reference to `IBGateway` since clients talk to it via TCP.
///
/// # Errors
/// Returns `Err` (not `None`) if:
/// - The pool can't be created
/// - The gateway can't be booted
/// - **Either client fails to connect** (this is NOT silently swallowed)
///
/// Tests should `.expect()` the result so failures are loud, not silent.
///
/// # Example
/// ```ignore
/// with_live_ibkr(&ibkr_account(), "ibc_test.log", |state| async move {
///     // state.client_1 is Arc<Client>
///     // state.pool is PgPool
///     // ... test body ...
/// })
/// .await
/// .expect("Failed to boot live IBKR");
/// ```
pub async fn with_live_ibkr<F, Fut, T>(
    _account: &str,
    ibc_log_file: &'static str,
    f: F,
) -> Result<T, String>
where
    F: FnOnce(LiveIbkr) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let pool = get_pool().await?;

    // with_gateway_retry owns the IBGateway and shuts it down when the closure
    // returns. The closure receives &IBGateway but ignores it (|_|) — the
    // gateway is alive for the closure's duration, and clients connect to it
    // via TCP on the API port.
    //
    // Both master_client (id=0) and client_1 (id=1) get 6 retries each —
    // the IB Gateway API can be slow to accept connections right after boot.
    with_gateway_retry(ibc_log_file, 2, |_| async {
        let master_client = connect_to_client_with_retry(0, 6)
            .await
            .map_err(|e| format!("master_client (id=0) failed: {e}"))?;
        let client_1 = connect_to_client_with_retry(1, 6)
            .await
            .map_err(|e| format!("client_1 (id=1) failed: {e}"))?;

        let live = LiveIbkr {
            master_client: Arc::new(master_client),
            client_1: Arc::new(client_1),
            pool: pool.clone(),
        };

        Ok(f(live).await)
    })
    .await?
}

// `with_gateway` / `with_gateway_retry` / `IBGateway` are re-exported via
// `trading_app::test_internals` — import them directly from there in tests.
