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
//! The `LiveIbkr` struct does NOT hold a reference to `IBGateway` — the gateway
//! is owned by `with_gateway_retry` and kept alive for the closure's duration.
//! Tests only need the clients (which talk to the gateway via TCP on port 4002).
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
use trading_app::test_internals::with_gateway_retry;

const API_PORT_ADDR: &str = "127.0.0.1:4002";

/// Live IBKR state — holds the connected clients + pool.
/// Does NOT hold a reference to `IBGateway`; the gateway is owned by
/// `with_gateway_retry` and kept alive for the closure's duration.
pub struct LiveIbkr {
    pub master_client: Arc<Client>,
    pub client_1: Arc<Client>,
    pub pool: PgPool,
}

async fn connect_to_client_with_retry(
    api_port_addr: &str,
    client_id: i32,
    retry_times: u32,
) -> Result<Client, String> {
    // IB Gateway has a documented delay between "Login has completed" (UI ready)
    // and the API socket accepting connections. Use a short poll interval.
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);
    let mut retry_time = 0;
    loop {
        match Client::connect(api_port_addr, client_id) {
            Ok(c) => return Ok(c),
            Err(e) => {
                tracing::warn!(
                    "Connection to {} (client {}) failed (attempt {}): {}",
                    api_port_addr,
                    client_id,
                    retry_time + 1,
                    e
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

pub async fn get_pool() -> Option<PgPool> {
    let database_url = std::env::var("DATABASE_URL").ok()?;
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .ok()
}

/// Poll port 4002 until it's free (no longer accepting connections).
/// Returns true if the port was released within the timeout, false otherwise.
pub async fn wait_for_port_release(max_wait: Duration) -> bool {
    let poll_interval = Duration::from_millis(500);
    let start = std::time::Instant::now();
    loop {
        let still_bound = tokio::net::TcpStream::connect(API_PORT_ADDR).await.is_ok();
        if !still_bound {
            return true;
        }
        if start.elapsed() >= max_wait {
            return false;
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// Wait for port 4002 to be bound (IB Gateway ready to accept connections).
#[allow(dead_code)]
pub async fn wait_for_port_bind(max_wait: Duration) -> bool {
    let poll_interval = Duration::from_millis(500);
    let start = std::time::Instant::now();
    loop {
        if tokio::net::TcpStream::connect(API_PORT_ADDR).await.is_ok() {
            return true;
        }
        if start.elapsed() >= max_wait {
            return false;
        }
        tokio::time::sleep(poll_interval).await;
    }
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
/// # Example
/// ```ignore
/// with_live_ibkr("DU111111", "ibc_test.log", |state| async {
///     // state.client_1 is Arc<Client>
///     // state.pool is PgPool
///     // ... test body ...
/// })
/// .await;
/// ```
pub async fn with_live_ibkr<F, Fut, T>(
    _account: &str,
    ibc_log_file: &'static str,
    f: F,
) -> Option<T>
where
    F: FnOnce(LiveIbkr) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let pool = get_pool().await?;

    // with_gateway_retry owns the IBGateway and shuts it down when the closure
    // returns. The closure receives &IBGateway but ignores it (|_|) — the
    // gateway is alive for the closure's duration, and clients connect to it
    // via TCP on port 4002.
    with_gateway_retry(ibc_log_file, 2, |_| async {
        let master_client = connect_to_client_with_retry(API_PORT_ADDR, 0, 6)
            .await
            .ok()?;
        let client_1 = connect_to_client_with_retry(API_PORT_ADDR, 1, 1)
            .await
            .ok()?;

        let live = LiveIbkr {
            master_client: Arc::new(master_client),
            client_1: Arc::new(client_1),
            pool: pool.clone(),
        };

        Some(f(live).await)
    })
    .await
    .ok()?
}
