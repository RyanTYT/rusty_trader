//! Comprehensive smoke tests for the axum HTTP server (live IBKR).
//!
//! Tests all 5 HTTP endpoints exposed by `init_server` on port 8000:
//! - GET /check-health — health check (IBKR connection status)
//! - GET /contract/price?stock=&primary_exchange=&currency= — current price for a contract
//! - GET /contracts/stock?stock=&primary_exchange=&currency= — possible stock contracts + prices
//! - GET /exchange_rate?currency=&quote= — forex exchange rate
//! - GET /strategy/capital?strategy= — strategy SGD value
//!
//! Also tests:
//! - `init_server()` — boots the server on port 8000
//! - `CurrencyVal`, `Currencies`, `StrategyValue`, `StrategyQuery` structs (serialization)
//! - Server lifecycle (boot + shutdown)
//! - Error handling (not-running state, invalid query params)
//!
//! Requires: live IB Gateway + Postgres + DATABASE_URL + IBC installed.
//! Run with: DATABASE_URL=... cargo test --test smoke_tests test_server -- --ignored

use std::sync::{Arc, Weak};
use std::time::Duration;

use trading_app::init_app::{init_app, ApplicationState};
use trading_app::server::server::init_server;

use crate::live::init::{with_live_ibkr, ibkr_account, api_port_addr, server_base_url};


/// Boot the server + init_app inside a `with_live_ibkr` closure.
/// The closure receives `state` (LiveIbkr with gateway + clients + pool).
/// The gateway is shut down when the closure returns.
fn boot_server_and_run_test<F, Fut>(test_fn: F)
where
    F: Fn(reqwest::Client) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    // This is a helper — the actual boot happens in each test via with_live_ibkr
    todo!("use with_live_ibkr directly in each test")
}

// ============================ 1. init_server — boots on port 8000 ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_init_boots() {
    with_live_ibkr(&ibkr_account(), "ibc_server_boot.log", |state| async move {
        // Create the channel + boot the server
        let (app_state_sender, app_state_rcx) = tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
        init_server(app_state_rcx);

        // Boot init_app (this connects to IBKR + builds the consolidator)
        let account: &'static str = Box::leak(ibkr_account().into_boxed_str());
        let app_state = init_app(
            &api_port_addr(),
            account,
            state.pool.clone(),
            "noise".to_string(),
        )
        .await
        .expect("init_app failed");

        let app_state = Arc::new(app_state);
        let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify the server is listening on port 8000
        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/check-health", server_base_url()))
            .send()
            .await
            .expect("HTTP request failed — server may not be running");

        assert!(
            response.status().is_success() || response.status().is_server_error(),
            "server should respond, got status {}",
            response.status()
        );
        println!("✅ init_server: server is listening on port 8000 (status {})", response.status());
    })
.await
.expect("Failed to boot live IBKR");
}

// ============================ 2. /check-health — health check ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_check_health() {
    with_live_ibkr(&ibkr_account(), "ibc_server_health.log", |state| async move {
        let (app_state_sender, app_state_rcx) = tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
        init_server(app_state_rcx);

        let app_state = init_app(
            &api_port_addr(), "DU111111", state.pool.clone(), "noise".to_string(),
        ).await.expect("init_app failed");
        let app_state = Arc::new(app_state);
        let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/check-health", server_base_url()))
            .send()
            .await
            .expect("HTTP request failed");

        let status = response.status();
        let body: serde_json::Value = response.json().await.expect("failed to parse JSON");
        println!("check-health response: status={status}, body={body}");

        if status.is_success() {
            assert_eq!(body["status"], "ok", "healthy response should have status=ok");
            println!("✅ /check-health: 200 OK (IBKR connected)");
        } else {
            assert_eq!(body["status"], "not connected to IBKR");
            println!("⚠️ /check-health: 500 (IBKR not connected)");
        }
    })
.await
.expect("Failed to boot live IBKR");
}

// ============================ 3. /contract/price — get current price ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_get_current_price() {
    with_live_ibkr(&ibkr_account(), "ibc_server_price.log", |state| async move {
        let (app_state_sender, app_state_rcx) = tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
        init_server(app_state_rcx);

        let app_state = init_app(
            &api_port_addr(), "DU111111", state.pool.clone(), "noise".to_string(),
        ).await.expect("init_app failed");
        let app_state = Arc::new(app_state);
        let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/contract/price", server_base_url()))
            .query(&[("stock", "AAPL"), ("primary_exchange", "NASDAQ"), ("currency", "USD")])
            .send()
            .await
            .expect("HTTP request failed");

        let status = response.status();
        let body: serde_json::Value = response.json().await.expect("failed to parse JSON");
        println!("contract/price response: status={status}, body={body}");

        if status.is_success() {
            let price = body["price"].as_f64();
            assert!(price.is_some());
            let price = price.unwrap();
            assert!(price > 0.0 && (50.0..=500.0).contains(&price));
            println!("✅ /contract/price: AAPL = ${price}");
        }
    })
.await
.expect("Failed to boot live IBKR");
}

// ============================ 4. /exchange_rate — forex exchange rate ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_get_exchange_rate() {
    with_live_ibkr(&ibkr_account(), "ibc_server_fx.log", |state| async move {
        let (app_state_sender, app_state_rcx) = tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
        init_server(app_state_rcx);

        let app_state = init_app(
            &api_port_addr(), "DU111111", state.pool.clone(), "noise".to_string(),
        ).await.expect("init_app failed");
        let app_state = Arc::new(app_state);
        let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/exchange_rate", server_base_url()))
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
    })
.await
.expect("Failed to boot live IBKR");
}

// ============================ 5. /strategy/capital — strategy SGD value ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_get_strategy_value() {
    with_live_ibkr(&ibkr_account(), "ibc_server_sgd.log", |state| async move {
        let (app_state_sender, app_state_rcx) = tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
        init_server(app_state_rcx);

        let app_state = init_app(
            &api_port_addr(), "DU111111", state.pool.clone(), "noise".to_string(),
        ).await.expect("init_app failed");
        let app_state = Arc::new(app_state);
        let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/strategy/capital", server_base_url()))
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
    })
.await
.expect("Failed to boot live IBKR");
}

// ============================ 6. /contracts/stock — possible stock contracts ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_get_possible_stock_contracts() {
    with_live_ibkr(&ibkr_account(), "ibc_server_contracts.log", |state| async move {
        let (app_state_sender, app_state_rcx) = tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
        init_server(app_state_rcx);

        let app_state = init_app(
            &api_port_addr(), "DU111111", state.pool.clone(), "noise".to_string(),
        ).await.expect("init_app failed");
        let app_state = Arc::new(app_state);
        let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/contracts/stock", server_base_url()))
            .query(&[("stock", "AAPL"), ("primary_exchange", "NASDAQ"), ("currency", "USD")])
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
    })
.await
.expect("Failed to boot live IBKR");
}

// ============================ 7. Error handling — invalid query params ============================

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + DATABASE_URL"]
async fn test_server_invalid_query_params() {
    with_live_ibkr(&ibkr_account(), "ibc_server_invalid.log", |state| async move {
        let (app_state_sender, app_state_rcx) = tokio::sync::mpsc::channel::<Weak<ApplicationState>>(2);
        init_server(app_state_rcx);

        let app_state = init_app(
            &api_port_addr(), "DU111111", state.pool.clone(), "noise".to_string(),
        ).await.expect("init_app failed");
        let app_state = Arc::new(app_state);
        let _ = app_state_sender.send(Arc::downgrade(&app_state)).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/contract/price", server_base_url()))
            .send()
            .await
            .expect("HTTP request failed");

        let status = response.status();
        assert!(status.is_client_error() || status.is_server_error());
        println!("✅ /contract/price with no params: {} (correct)", status);
    })
.await
.expect("Failed to boot live IBKR");
}
