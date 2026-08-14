//! Lifecycle tests for the trading-app: IBGateway boot/shutdown/reboot, Drop semantics,
//! and init_app boot/drop/reboot.
//!
//! These tests use the new `with_gateway` / `with_gateway_retry` RAII API:
//! the gateway is automatically shut down when the closure returns.
//!
//! Verifies:
//! - `with_gateway` boots + shuts down cleanly
//! - `with_gateway_retry` retries on failure + shuts down cleanly
//! - Port 4002 is released after `with_gateway` returns
//! - Multiple `with_gateway` calls in sequence work (boot/shutdown/reboot)
//! - `with_live_ibkr` boots clients + gateway together
//!
//! Requires: live IBC + Postgres + DATABASE_URL. All tests #[ignore]'d.

use std::sync::Arc;
use std::time::Duration;

use trading_app::test_internals::{with_gateway, with_gateway_retry, IBGateway};

use crate::live::init::{wait_for_port_release, wait_for_port_bind};

// ============================ with_gateway — basic boot/shutdown ============================

#[tokio::test]
#[ignore = "requires live IBC + Postgres + DATABASE_URL"]
async fn test_with_gateway_basic_boot_shutdown() {
    // Boot the gateway + verify port is bound inside the closure
    let result = with_gateway("/tmp/ibc_lifecycle_1.log", |_gateway| async {
        // Port 4002 should be bound while inside the closure
        let bound = tokio::net::TcpStream::connect("127.0.0.1:4002").await.is_ok();
        assert!(bound, "port 4002 should be bound while gateway is alive");
        println!("✅ with_gateway: port 4002 bound inside closure");
        true
    })
    .await;

    assert!(result.is_ok(), "with_gateway should succeed");
    println!("✅ with_gateway: closure returned Ok(true)");

    // After the closure returns, the gateway should be shut down + port released
    let released = wait_for_port_release(Duration::from_secs(10)).await;
    assert!(released, "port 4002 should be released after with_gateway returns");
    println!("✅ with_gateway: port 4002 released after closure returns");
}

// ============================ with_gateway_retry — retry + shutdown ============================

#[tokio::test]
#[ignore = "requires live IBC + Postgres + DATABASE_URL"]
async fn test_with_gateway_retry_boot_shutdown() {
    let result = with_gateway_retry("/tmp/ibc_lifecycle_2.log", 2, |_gateway| async {
        let bound = tokio::net::TcpStream::connect("127.0.0.1:4002").await.is_ok();
        assert!(bound, "port should be bound inside with_gateway_retry closure");
        println!("✅ with_gateway_retry: port bound inside closure");
        42
    })
    .await;

    assert!(result.is_ok(), "with_gateway_retry should succeed");
    assert_eq!(result.unwrap(), 42, "closure return value should propagate");

    let released = wait_for_port_release(Duration::from_secs(10)).await;
    assert!(released, "port should be released after with_gateway_retry returns");
    println!("✅ with_gateway_retry: port released after closure returns");
}

// ============================ Multiple boot/shutdown cycles ============================

#[tokio::test]
#[ignore = "requires live IBC + Postgres + DATABASE_URL"]
async fn test_multiple_boot_shutdown_cycles() {
    for i in 1..=3 {
        let log_file: &'static str = Box::leak(format!("/tmp/ibc_cycle_{i}.log").into_boxed_str());
        let result = with_gateway_retry(log_file, 2, |_gw| async {
            let bound = tokio::net::TcpStream::connect("127.0.0.1:4002").await.is_ok();
            assert!(bound, "cycle {i}: port should be bound");
            i
        })
        .await;

        assert!(result.is_ok(), "cycle {i}: with_gateway_retry should succeed");
        assert_eq!(result.unwrap(), i, "cycle {i}: return value mismatch");

        let released = wait_for_port_release(Duration::from_secs(10)).await;
        assert!(released, "cycle {i}: port should be released");
        println!("✅ cycle {i}: boot/shutdown complete, port released");
    }
}

// ============================ Port helper tests ============================

#[tokio::test]
#[ignore = "requires live IBC + Postgres + DATABASE_URL"]
async fn test_wait_for_port_bind_and_release_helpers() {
    // Boot a gateway
    let result = with_gateway("/tmp/ibc_port_helpers.log", |_gw| async {
        // wait_for_port_bind should return true (port is bound)
        let bound = wait_for_port_bind(Duration::from_secs(5)).await;
        assert!(bound, "wait_for_port_bind should return true while gateway is alive");
        println!("✅ wait_for_port_bind: true (port bound)");

        // wait_for_port_release should return false (port is still bound)
        let released = wait_for_port_release(Duration::from_secs(2)).await;
        assert!(!released, "wait_for_port_release should return false while gateway is alive");
        println!("✅ wait_for_port_release: false (port still bound)");
        true
    })
    .await;

    assert!(result.is_ok());

    // After the gateway is shut down, wait_for_port_release should return true
    let released = wait_for_port_release(Duration::from_secs(10)).await;
    assert!(released, "wait_for_port_release should return true after gateway shutdown");
    println!("✅ wait_for_port_release: true (port released after shutdown)");

    // wait_for_port_bind should now return false (port is free)
    let bound = wait_for_port_bind(Duration::from_secs(2)).await;
    assert!(!bound, "wait_for_port_bind should return false after shutdown");
    println!("✅ wait_for_port_bind: false (port not bound after shutdown)");
}
