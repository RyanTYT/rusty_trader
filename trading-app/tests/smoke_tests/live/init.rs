//! Shared infrastructure for live IBKR smoke tests.
//!
//! Boots a persistent IB Gateway via IBC and connects master + client_1.
//! The `LiveIbkr` guard's `Drop` robustly waits for full teardown (port 4002
//! release) before returning, so consecutive tests don't race on the port.
//!
//! Requires:
//! - IBC installed at `/IBCLinux-3.21.2/scripts/ibcstart.sh`
//! - IB Gateway credentials in env vars
//! - Postgres + DATABASE_URL
//! Run with: `DATABASE_URL=... cargo test --test smoke_tests -- --ignored`

use std::sync::Arc;

use ibapi::Client;
use sqlx::PgPool;
use tokio::time::Duration;
use trading_app::test_internals::{init_ibc_with_retry, IBGateway};

const API_PORT_ADDR: &str = "127.0.0.1:4002";

pub struct LiveIbkr {
    pub master_client: Arc<Client>,
    pub client_1: Arc<Client>,
    pub pool: PgPool,
    pub _gateway: IBGateway,
}

async fn connect_to_client_with_retry(
    api_port_addr: &str,
    client_id: i32,
    retry_times: u32,
) -> Result<Client, String> {
    let mut retry_time = 0;
    let client_opt = loop {
        match Client::connect(api_port_addr, client_id) {
            Ok(c) => break Some(c),
            Err(e) => {
                tracing::error!("Connection failed: {e}");
                retry_time += 1;
                if retry_time <= retry_times {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue;
                }
                break None;
            }
        }
    };
    client_opt.ok_or("Could not connect to client".to_string())
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
/// Use this after dropping `LiveIbkr` to guarantee the next boot won't race.
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
/// Used during boot to poll until the gateway is ready.
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

pub async fn live_ibkr(_account: &str, ibc_log_file: &str) -> Option<LiveIbkr> {
    let pool = get_pool().await?;
    // init_ibc_with_retry requires &'static str; leak the String to get a 'static.
    let ibc_log_file_static: &'static str = Box::leak(ibc_log_file.to_string().into_boxed_str());
    let gateway = init_ibc_with_retry(ibc_log_file_static, 2).await.ok()?;
    let master_client =
        connect_to_client_with_retry(API_PORT_ADDR, 0, 6).await.ok()?;
    let client_1 =
        connect_to_client_with_retry(API_PORT_ADDR, 1, 1).await.ok()?;

    Some(LiveIbkr {
        master_client: Arc::new(master_client),
        client_1: Arc::new(client_1),
        pool,
        _gateway: gateway,
    })
}
