//! Smoke test: init_app (live IBKR).
//! Tests the full `init_app` bootstrap. End-to-end integration test.
//! Requires: live IB Gateway + Postgres + DATABASE_URL + IBC installed.
//! Run with: `cargo test --test smoke_tests test_init_app_smoke -- --ignored`

use std::time::Duration;

use trading_app::{ibc::with_gateway_retry, init_app::init_app};

#[tokio::test]
#[ignore = "requires live IB Gateway + Postgres + IBC installed — full bootstrap"]
async fn test_init_app_smoke_live() {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to DB");

    assert!(
        with_gateway_retry("/test_init_app_smoke_live.log", 2, |_| async {
            let state = init_app("127.0.0.1:4002", "DU111111", pool, "noise".to_string())
                .await
                .expect("init_app failed");
            println!("init_app bootstrapped successfully — full smoke test passed");

            tokio::time::sleep(Duration::from_secs(10));

            drop(state);
        })
        .await
        .is_ok(),
        "Initialising IBGateway with App was not smooth - launch IBGateway failed"
    );
}
