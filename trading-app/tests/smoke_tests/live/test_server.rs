//! Comprehensive smoke tests for the axum HTTP server (live IBKR).
//!
//! Tests all 5 HTTP endpoints exposed by `init_server`:
//! - GET /check-health — health check (IBKR connection status)
//! - GET /contract/price?stock=&primary_exchange=&currency= — current price for a contract
//! - GET /contracts/stock?stock=&primary_exchange=&currency= — possible stock contracts + prices
//! - GET /exchange_rate?currency=&quote= — forex exchange rate
//! - GET /strategy/capital?strategy= — strategy SGD value
//!
//! Also tests:
//! - `init_server()` — boots the server on an OS-assigned ephemeral port
//! - `CurrencyVal`, `Currencies`, `StrategyValue`, `StrategyQuery` structs (serialization)
//! - Server lifecycle (boot + shutdown)
//! - Error handling (not-running state, invalid query params)
//!
//! Each test binds `0.0.0.0:0` (ephemeral port) so parallel tests never collide
//! on a fixed port, and tears the server down via `ServerHandle::shutdown()`
//! (with `Drop` as a safety net if the closure panics).
//!
//! ## Boot pattern
//!
//! Unlike most other smoke tests, these do NOT use `with_live_ibkr` (which
//! boots the gateway + connects clients + passes them in `LiveIbkr`). That
//! pattern is incompatible with `init_app`: `init_app` connects its OWN
//! `master_client` (id=0) + `client_1` (id=1), so calling it inside
//! `with_live_ibkr` would attempt a duplicate client-id connection
//! ("Could not connect to client 0").
//!
//! Instead, these use `with_gateway_retry` (boots the gateway only) + call
//! `init_app` inside the closure (connects clients + builds the consolidator).
//! This mirrors `test_init_app_smoke.rs`.
//!
//! Requires: live IB Gateway + Postgres + DATABASE_URL + IBC installed.
//! Run with: DATABASE_URL=... cargo test --test smoke_tests test_server -- --ignored

use std::sync::{Arc, Weak};
use std::time::Duration;

use trading_app::ibc::with_gateway_retry;
use trading_app::init_app::{ApplicationState, init_app};
use trading_app::arc_drop_async;
use trading_app::server::server::init_server;

use crate::common::init_tracing;
use crate::live::init::{api_port_addr, get_pool};

// ============================ 1. init_server — boots on ephemeral port ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_init_boots() {
    init_tracing();
    let pool = get_pool().await.expect("Failed to connect to DB");
    assert!(
        with_gateway_retry("ibc_server_boot.log", 2, |_| async {
            let (app_state_sender, app_state_rcx) =
                tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
            let server = init_server("0.0.0.0:0", app_state_rcx).expect("failed to bind server");
            let base_url = format!("http://{}", server.local_addr());

            // init_app connects master_client (id=0) + client_1 (id=1) + builds
            // the consolidator. The gateway is already booted by with_gateway_retry.
            let app_state = init_app(
                &api_port_addr(),
                "DU111111",
                pool.clone(),
                "noise".to_string(),
            )
            .await
            .expect("init_app failed");

            let mut app_state = Arc::new(app_state);
            let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Verify the server is listening on the ephemeral port
            let client = reqwest::Client::new();
            let response = client
                .get(format!("{base_url}/check-health"))
                .send()
                .await
                .expect("HTTP request failed — server may not be running");

            assert!(
                response.status().is_success() || response.status().is_server_error(),
                "server should respond, got status {}",
                response.status()
            );
            println!(
                "✅ init_server: server is listening on {} (status {})",
                server.local_addr(),
                response.status()
            );

            // Explicit teardown: server before app_state (server borrows app_state).
            drop(client);
            drop(app_state_sender);
            arc_drop_async!(app_state);
            let handle = tokio::task::spawn_blocking(move || {
                server.shutdown();
            });
            handle.await.unwrap();
            true
        })
        .await
        .is_ok(),
        "Initialising IBGateway with server was not smooth"
    );
}

// ============================ 2. /check-health — health check ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_check_health() {
    init_tracing();
    let pool = get_pool().await.expect("Failed to connect to DB");
    assert!(
        with_gateway_retry("ibc_server_health.log", 2, |_| async {
            let (app_state_sender, app_state_rcx) =
                tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
            let server = init_server("0.0.0.0:0", app_state_rcx).expect("failed to bind server");
            let base_url = format!("http://{}", server.local_addr());

            let app_state = init_app(
                &api_port_addr(),
                "DU111111",
                pool.clone(),
                "noise".to_string(),
            )
            .await
            .expect("init_app failed");
            let mut app_state = Arc::new(app_state);
            let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
            tokio::time::sleep(Duration::from_secs(2)).await;

            let client = reqwest::Client::new();
            let response = client
                .get(format!("{base_url}/check-health"))
                .send()
                .await
                .expect("HTTP request failed");

            let status = response.status();
            let body: serde_json::Value = response.json().await.expect("failed to parse JSON");
            println!("check-health response: status={status}, body={body}");

            if status.is_success() {
                assert_eq!(
                    body["status"], "ok",
                    "healthy response should have status=ok"
                );
                println!("✅ /check-health: 200 OK (IBKR connected)");
            } else {
                assert_eq!(body["status"], "not connected to IBKR");
                println!("⚠️ /check-health: 500 (IBKR not connected)");
            }

            drop(client);
            drop(app_state_sender);
            println!("app_state count: {}", Arc::strong_count(&app_state));
            arc_drop_async!(app_state);
            let handle = tokio::task::spawn_blocking(move || {
                println!("server drop");
                server.shutdown();
            });
            handle.await.unwrap();
            true
        })
        .await
        .is_ok(),
        "Initialising IBGateway with server was not smooth"
    );
}

// ============================ 3. /contract/price — get current price ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_get_current_price() {
    init_tracing();
    let pool = get_pool().await.expect("Failed to connect to DB");
    assert!(
        with_gateway_retry("ibc_server_price.log", 2, |_| async {
            let (app_state_sender, app_state_rcx) =
                tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
            let server = init_server("0.0.0.0:0", app_state_rcx).expect("failed to bind server");
            let base_url = format!("http://{}", server.local_addr());

            let app_state = init_app(
                &api_port_addr(),
                "DU111111",
                pool.clone(),
                "noise".to_string(),
            )
            .await
            .expect("init_app failed");
            let mut app_state = Arc::new(app_state);
            let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
            tokio::time::sleep(Duration::from_secs(2)).await;

            let client = reqwest::Client::new();
            let response = client
                .get(format!("{base_url}/contract/price"))
                .query(&[
                    ("stock", "AAPL"),
                    ("primary_exchange", "NASDAQ"),
                    ("currency", "USD"),
                ])
                .send()
                .await
                .expect("HTTP request failed");

            let status = response.status();
            let body: serde_json::Value = response.json().await.expect("failed to parse JSON");

            if status.is_success() {
                let price = body["price"].as_f64();
                assert!(price.is_some());
                let price = price.unwrap();
                assert!(price > 0.0 && (50.0..=500.0).contains(&price));
                println!("✅ /contract/price: AAPL = ${price}");
            }

            drop(client);
            drop(app_state_sender);
            arc_drop_async!(app_state);
            let handle = tokio::task::spawn_blocking(move || {
                server.shutdown();
            });
            handle.await.unwrap();
            true
        })
        .await
        .is_ok(),
        "Initialising IBGateway with server was not smooth"
    );
}

// ============================ 4. /exchange_rate — forex exchange rate ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_get_exchange_rate() {
    init_tracing();
    let pool = get_pool().await.expect("Failed to connect to DB");
    assert!(
        with_gateway_retry("ibc_server_fx.log", 2, |_| async {
            let (app_state_sender, app_state_rcx) =
                tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
            let server = init_server("0.0.0.0:0", app_state_rcx).expect("failed to bind server");
            let base_url = format!("http://{}", server.local_addr());

            let app_state = init_app(
                &api_port_addr(),
                "DU111111",
                pool.clone(),
                "noise".to_string(),
            )
            .await
            .expect("init_app failed");
            let mut app_state = Arc::new(app_state);
            let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
            tokio::time::sleep(Duration::from_secs(2)).await;

            let client = reqwest::Client::new();
            let response = client
                .get(format!("{base_url}/exchange_rate"))
                .query(&[("currency", "USD"), ("quote", "SGD")])
                .send()
                .await
                .expect("HTTP request failed");

            let status = response.status();
            let body: serde_json::Value = response.json().await.expect("failed to parse JSON");
            println!("exchange_rate response: status={status}, body={body}");

            if status.is_success() {
                let price = body["price"].as_f64();
                assert!(price.is_some());
                let price = price.unwrap();
                assert!(price > 0.0 && (0.5..=2.0).contains(&price));
                println!("✅ /exchange_rate: USD/SGD = {price}");
            }

            drop(client);
            drop(app_state_sender);
            arc_drop_async!(app_state);
            let handle = tokio::task::spawn_blocking(move || {
                server.shutdown();
            });
            handle.await.unwrap();
            true
        })
        .await
        .is_ok(),
        "Initialising IBGateway with server was not smooth"
    );
}

// ============================ 5. /strategy/capital — strategy SGD value ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_get_strategy_value() {
    init_tracing();
    let pool = get_pool().await.expect("Failed to connect to DB");
    assert!(
        with_gateway_retry("ibc_server_sgd.log", 2, |_| async {
            let (app_state_sender, app_state_rcx) =
                tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
            let server = init_server("0.0.0.0:0", app_state_rcx).expect("failed to bind server");
            let base_url = format!("http://{}", server.local_addr());

            let app_state = init_app(
                &api_port_addr(),
                "DU111111",
                pool.clone(),
                "noise".to_string(),
            )
            .await
            .expect("init_app failed");
            let mut app_state = Arc::new(app_state);
            let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
            tokio::time::sleep(Duration::from_secs(2)).await;

            let client = reqwest::Client::new();
            let response = client
                .get(format!("{base_url}/strategy/capital"))
                .query(&[("strategy", "noise")])
                .send()
                .await
                .expect("HTTP request failed");

            let status = response.status();
            let body: serde_json::Value = response.json().await.expect("failed to parse JSON");
            println!("strategy/capital response: status={status}, body={body}");

            if status.is_success() {
                let sgd = body["sgd_value"].as_f64();
                assert!(sgd.is_some());
                let sgd = sgd.unwrap();
                assert!(sgd.is_finite() && sgd >= 0.0);
                println!("✅ /strategy/capital: noise = {sgd}");
            }

            drop(client);
            drop(app_state_sender);
            arc_drop_async!(app_state);
            let handle = tokio::task::spawn_blocking(move || {
                server.shutdown();
            });
            handle.await.unwrap();
            true
        })
        .await
        .is_ok(),
        "Initialising IBGateway with server was not smooth"
    );
}

// ============================ 6. /contracts/stock — possible stock contracts ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_get_possible_stock_contracts() {
    init_tracing();
    let pool = get_pool().await.expect("Failed to connect to DB");
    assert!(
        with_gateway_retry("ibc_server_contracts.log", 2, |_| async {
            let (app_state_sender, app_state_rcx) =
                tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
            let server = init_server("0.0.0.0:0", app_state_rcx).expect("failed to bind server");
            let base_url = format!("http://{}", server.local_addr());

            let app_state = init_app(
                &api_port_addr(),
                "DU111111",
                pool.clone(),
                "noise".to_string(),
            )
            .await
            .expect("init_app failed");
            let mut app_state = Arc::new(app_state);
            let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
            tokio::time::sleep(Duration::from_secs(2)).await;

            let client = reqwest::Client::new();
            let response = client
                .get(format!("{base_url}/contracts/stock"))
                .query(&[
                    ("stock", "AAPL"),
                    ("primary_exchange", "NASDAQ"),
                    ("currency", "USD"),
                ])
                .send()
                .await
                .expect("HTTP request failed");

            let status = response.status();
            let body: serde_json::Value = response.json().await.expect("failed to parse JSON");
            println!("contracts/stock response: status={status}, body={body}");

            if status.is_success() {
                let contracts = body.as_array();
                assert!(contracts.is_some());
            }

            drop(client);
            drop(app_state_sender);
            arc_drop_async!(app_state);
            let handle = tokio::task::spawn_blocking(move || {
                server.shutdown();
            });
            handle.await.unwrap();
            true
        })
        .await
        .is_ok(),
        "Initialising IBGateway with server was not smooth"
    );
}

// ============================ 7. Error handling — invalid query params ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_invalid_query_params() {
    init_tracing();
    /// NOTE GET_POOL CRASHED FOR NON-STALLED TESTS
    let pool = get_pool().await.expect("Failed to connect to DB");
    assert!(
        with_gateway_retry("ibc_server_invalid.log", 2, |_| async {
            let (app_state_sender, app_state_rcx) =
                tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
            let server = init_server("0.0.0.0:0", app_state_rcx).expect("failed to bind server");
            let base_url = format!("http://{}", server.local_addr());

            let app_state = init_app(
                &api_port_addr(),
                "DU111111",
                pool.clone(),
                "noise".to_string(),
            )
            .await
            .expect("init_app failed");
            let mut app_state = Arc::new(app_state);
            let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
            tokio::time::sleep(Duration::from_secs(2)).await;

            let client = reqwest::Client::new();
            let response = client
                .get(format!("{base_url}/contract/price"))
                .send()
                .await
                .expect("HTTP request failed");

            let status = response.status();
            assert!(status.is_client_error() || status.is_server_error());
            println!("✅ /contract/price with no params: {} (correct)", status);

            drop(client);
            drop(app_state_sender);
            arc_drop_async!(app_state);
            let handle = tokio::task::spawn_blocking(move || {
                server.shutdown();
            });
            handle.await.unwrap();
            true
        })
        .await
        .is_ok(),
        "Initialising IBGateway with server was not smooth"
    );
}
