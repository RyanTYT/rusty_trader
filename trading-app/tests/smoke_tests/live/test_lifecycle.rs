//! Lifecycle tests for the trading-app: IBGateway boot/stop/reboot, Drop semantics,
//! and init_app boot/drop/reboot.
//!
//! These tests verify:
//! - IBGateway can be booted, stopped, and rebooted without port conflicts
//! - The stop() function robustly waits for port 4002 to be released
//! - Drop impls clean up properly (OrderUpdateStreamController singleton resets)
//! - init_app can be booted, dropped, and rebooted (full app lifecycle)
//!
//! Requires: live IBC + Postgres + DATABASE_URL. All tests #[ignore]'d.

use std::sync::Arc;

use trading_app::test_internals::{init_ibc_with_retry, IBGateway};

use crate::live::init::{wait_for_port_release, wait_for_port_bind};
use tokio::time::Duration;

// ============================ IBGateway boot/stop/reboot ============================

#[tokio::test]
#[ignore = "requires live IBC + Postgres + DATABASE_URL"]
async fn test_ibgateway_boot_stop_reboot() {
    // 1. Boot the gateway
    let gateway = init_ibc_with_retry("/tmp/ibc_lifecycle_1.log", 2)
        .await
        .expect("first boot failed");

    // 2. Verify port 4002 is bound
    let bound = tokio::net::TcpStream::connect("127.0.0.1:4002").await.is_ok();
    assert!(bound, "port 4002 should be bound after boot");

    // 3. Stop the gateway
    let mut gateway = gateway;
    gateway
        .stop()
        .await
        .expect("stop failed");

    // 4. Verify port 4002 is released (stop() should wait for this)
    let released = wait_for_port_release(Duration::from_secs(5)).await;
    assert!(released, "port 4002 should be released after stop()");

    // 5. Reboot — this would fail if the port wasn't released
    let gateway2 = init_ibc_with_retry("/tmp/ibc_lifecycle_2.log", 2)
        .await
        .expect("reboot failed — port may not have been released");

    // 6. Verify port is bound again
    let bound2 = tokio::net::TcpStream::connect("127.0.0.1:4002").await.is_ok();
    assert!(bound2, "port 4002 should be bound after reboot");

    // 7. Cleanup
    let mut gateway2 = gateway2;
    let _ = gateway2.stop().await;
    wait_for_port_release(Duration::from_secs(5)).await;
}

#[tokio::test]
#[ignore = "requires live IBC + Postgres + DATABASE_URL"]
async fn test_ibgateway_drop_releases_port() {
    // Boot
    let gateway = init_ibc_with_retry("/tmp/ibc_drop_1.log", 2)
        .await
        .expect("boot failed");

    // Verify bound
    let bound = tokio::net::TcpStream::connect("127.0.0.1:4002").await.is_ok();
    assert!(bound);

    // Drop it — Drop should call stop() which waits for port release
    drop(gateway);

    // After drop returns, the port should be free
    let released = wait_for_port_release(Duration::from_secs(10)).await;
    assert!(released, "port 4002 should be released after drop()");

    // Reboot should work
    let gateway2 = init_ibc_with_retry("/tmp/ibc_drop_2.log", 2)
        .await
        .expect("reboot after drop failed");

    // Cleanup
    drop(gateway2);
    wait_for_port_release(Duration::from_secs(10)).await;
}

#[tokio::test]
#[ignore = "requires live IBC + Postgres + DATABASE_URL"]
async fn test_ibgateway_multiple_boot_stop_cycles() {
    // Run 3 boot/stop cycles to verify no state leaks between runs
    for i in 1..=3 {
        let log_file = Box::leak(format!("/tmp/ibc_cycle_{i}.log").into_boxed_str());
        let gateway = init_ibc_with_retry(log_file, 2)
            .await
            .expect("cycle {i} boot failed");

        let bound = tokio::net::TcpStream::connect("127.0.0.1:4002").await.is_ok();
        assert!(bound, "cycle {i}: port should be bound");

        let mut gateway = gateway;
        gateway.stop().await.expect("cycle {i} stop failed");

        let released = wait_for_port_release(Duration::from_secs(5)).await;
        assert!(released, "cycle {i}: port should be released");
    }
}

// ============================ init_app boot/drop/reboot ============================

#[tokio::test]
#[ignore = "requires live IBC + Postgres + DATABASE_URL"]
async fn test_init_app_boot_drop_reboot() {
    let pool = crate::live::init::get_pool()
        .await
        .expect("DATABASE_URL must be set");

    // 1. Boot init_app
    let app1 = trading_app::init_app::init_app(
        "127.0.0.1:4002",
        "DU1111111",
        pool.clone(),
        "/tmp/init_app_1.log",
        "noise".to_string(),
    )
    .await
    .expect("first init_app boot failed");

    // Verify port is bound
    let bound = tokio::net::TcpStream::connect("127.0.0.1:4002").await.is_ok();
    assert!(bound, "port should be bound after init_app");

    // 2. Drop the app — should tear down IB Gateway + all threads
    drop(app1);

    // 3. Wait for port release (Drop calls stop() which should wait)
    let released = wait_for_port_release(Duration::from_secs(15)).await;
    assert!(released, "port should be released after drop");

    // 4. Reboot — this verifies no leaked state (Arc cycles, leaked tasks,
    //    ORDER_UPDATE_STREAM_NO not reset, etc.)
    let app2 = trading_app::init_app::init_app(
        "127.0.0.1:4002",
        "DU1111111",
        pool,
        "/tmp/init_app_2.log",
        "noise".to_string(),
    )
    .await
    .expect("reboot after drop failed");

    // Verify port is bound again
    let bound2 = tokio::net::TcpStream::connect("127.0.0.1:4002").await.is_ok();
    assert!(bound2, "port should be bound after reboot");

    // Cleanup
    drop(app2);
    wait_for_port_release(Duration::from_secs(15)).await;
}

// ============================ Port helper tests ============================

#[tokio::test]
#[ignore = "requires live IBC + Postgres + DATABASE_URL"]
async fn test_wait_for_port_bind_and_release_helpers() {
    // Boot a gateway
    let gateway = init_ibc_with_retry("/tmp/ibc_port_helpers.log", 2)
        .await
        .expect("boot failed");

    // wait_for_port_bind should return true (port is bound)
    let bound = wait_for_port_bind(Duration::from_secs(5)).await;
    assert!(bound, "port should be bound");

    // Stop
    let mut gateway = gateway;
    gateway.stop().await.expect("stop failed");

    // wait_for_port_release should return true (port is released)
    let released = wait_for_port_release(Duration::from_secs(10)).await;
    assert!(released, "port should be released");

    // wait_for_port_bind should now return false (port is free)
    let bound_again = wait_for_port_bind(Duration::from_secs(2)).await;
    assert!(!bound_again, "port should not be bound after stop");
}
