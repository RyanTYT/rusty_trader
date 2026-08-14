//! Shared infrastructure for live IBKR smoke tests.
//!
//! Uses the new `with_gateway` / `with_gateway_retry` RAII API from `ibc.rs`:
//! the gateway is booted, a closure runs with `&IBGateway`, and the gateway
//! is shut down automatically when the closure returns.
//!
//! `live_ibkr()` uses `start_gateway()` (test-only) to get an owned `IBGateway`
//! so the gateway stays alive for the test's duration. The gateway is shut
//! down via `Drop` when the `LiveIbkr` struct is dropped.
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
use trading_app::test_internals::{start_gateway, IBGateway};

const API_PORT_ADDR: &str = "127.0.0.1:4002";

pub struct LiveIbkr {
    pub master_client: Arc<Client>,
    pub client_1: Arc<Client>,
    pub pool: PgPool,
    /// Owned `IBGateway` — shut down via `Drop` when `LiveIbkr` is dropped.
    pub _gateway: IBGateway,
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
                    api_port_addr, client_id, retry_time + 1, e
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

/// Boot an IB Gateway + connect clients, returning an owned `LiveIbkr`.
///
/// The gateway is shut down via `Drop` when the `LiveIbkr` is dropped.
/// After dropping, call `wait_for_port_release()` to ensure the port is
/// fully released before the next test boots.
pub async fn live_ibkr(_account: &str, ibc_log_file: &'static str) -> Option<LiveIbkr> {
    let pool = get_pool().await?;

    // Retry logic for gateway start
    let gateway = {
        let mut attempt = 0;
        loop {
            match start_gateway(ibc_log_file).await {
                Ok(gw) => break gw,
                Err(e) => {
                    attempt += 1;
                    if attempt > 2 {
                        tracing::error!("Failed to boot IB Gateway after {attempt} attempts: {e:?}");
                        return None;
                    }
                    tracing::warn!("Retrying gateway start (attempt {attempt}): {e:?}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    };

    let master_client = connect_to_client_with_retry(API_PORT_ADDR, 0, 6).await.ok()?;
    let client_1 = connect_to_client_with_retry(API_PORT_ADDR, 1, 1).await.ok()?;

    Some(LiveIbkr {
        master_client: Arc::new(master_client),
        client_1: Arc::new(client_1),
        pool,
        _gateway: gateway,
    })
}
