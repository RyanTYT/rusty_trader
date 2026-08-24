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
//!
//! Requires: live IBC + Postgres + DATABASE_URL. All tests #[ignore]'d.

use std::time::Duration;

use trading_app::test_internals::{with_gateway, with_gateway_retry};

use crate::live::init::{api_port_addr, wait_for_port_bind, wait_for_port_release};

// ============================ with_gateway — basic boot/shutdown ============================

#[tokio::test]
#[ignore = "requires live IBC + Postgres + DATABASE_URL"]
async fn test_with_gateway_basic_boot_shutdown() {
    let result = with_gateway("/tmp/ibc_lifecycle_1.log", |_gateway| async {
        let bound = tokio::net::TcpStream::connect(api_port_addr())
            .await
            .is_ok();
        assert!(bound, "port should be bound while gateway is alive");
        println!("✅ with_gateway: port bound inside closure");
        true
    })
    .await;

    assert!(
        result.is_ok(),
        "with_gateway should succeed: {:?}",
        result.err()
    );

    let released = wait_for_port_release(Duration::from_secs(10)).await;
    assert!(
        released,
        "port should be released after with_gateway returns"
    );
    println!("✅ with_gateway: port released after closure returns");
}

// ============================ with_gateway_retry — retry + shutdown ============================

#[tokio::test]
#[ignore = "requires live IBC + Postgres + DATABASE_URL"]
async fn test_with_gateway_retry_boot_shutdown() {
    let result = with_gateway_retry("/tmp/ibc_lifecycle_2.log", 2, |_gateway| async {
        let bound = tokio::net::TcpStream::connect(api_port_addr())
            .await
            .is_ok();
        assert!(
            bound,
            "port should be bound inside with_gateway_retry closure"
        );
        42
    })
    .await;

    assert!(
        result.is_ok(),
        "with_gateway_retry should succeed: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 42, "closure return value should propagate");

    let released = wait_for_port_release(Duration::from_secs(10)).await;
    assert!(
        released,
        "port should be released after with_gateway_retry returns"
    );
    println!("✅ with_gateway_retry: port released after closure returns");
}

// ============================ Multiple boot/shutdown cycles ============================

#[tokio::test]
#[ignore = "requires live IBC + Postgres + DATABASE_URL"]
async fn test_multiple_boot_shutdown_cycles() {
    for i in 1..=3 {
        let log_file: &'static str = Box::leak(format!("/tmp/ibc_cycle_{i}.log").into_boxed_str());
        let result = with_gateway_retry(log_file, 2, |_gw| async {
            let bound = tokio::net::TcpStream::connect(api_port_addr())
                .await
                .is_ok();
            assert!(bound, "cycle {i}: port should be bound");
            i
        })
        .await;

        assert!(
            result.is_ok(),
            "cycle {i}: with_gateway_retry should succeed: {:?}",
            result.err()
        );
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
    let result = with_gateway("/tmp/ibc_port_helpers.log", |_gw| async {
        let bound = wait_for_port_bind(Duration::from_secs(5)).await;
        assert!(
            bound,
            "wait_for_port_bind should return true while gateway is alive"
        );

        let released = wait_for_port_release(Duration::from_secs(2)).await;
        assert!(
            !released,
            "wait_for_port_release should return false while gateway is alive"
        );
        true
    })
    .await;

    assert!(result.is_ok(), "with_gateway failed: {:?}", result.err());

    let released = wait_for_port_release(Duration::from_secs(10)).await;
    assert!(
        released,
        "wait_for_port_release should return true after gateway shutdown"
    );

    let bound = wait_for_port_bind(Duration::from_secs(2)).await;
    assert!(
        !bound,
        "wait_for_port_bind should return false after shutdown"
    );
    println!("✅ Port helpers work correctly");
}
