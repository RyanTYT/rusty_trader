//! Tests for the order-placement helpers.
//!
//! `place_order` / `place_order_checked` / `place_orders` are the boundary between
//! the strategy layer and IBKR: they allocate an order id on the wire and write an
//! optimistic `open_orders` row (`perm_id = -1`) so that reconciliation can see an
//! in-flight order before IBKR echoes it back.
//!
//! The DB-backed tests are `#[ignore]`d and need `TEST_DATABASE_URL`. The rest use a
//! fake [`OrderRouter`] and a lazy pool, so they run anywhere.

#![cfg(not(feature = "backtest"))]

use std::sync::Arc;
use std::time::Duration;

use ibapi::orders::Action;
use ibapi::orders::order_builder::market_order;
use ibapi::prelude::Contract;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use trading_app::database::crud::CRUDTrait;
use trading_app::database::db::DbDirect;
use trading_app::database::models::{AssetType, Status, StrategyFullKeys};
use trading_app::database::models_crud::open_orders::open_orders::OpenOrdersPrimaryKeys;
use trading_app::database::tables::DbTables;
use trading_app::execution::order_engine::{OrderEngine, OrderIBKR};

use crate::common::TestDb;
use crate::support::fakes::FakeOrderRouter;

fn lazy_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://placeholder:placeholder@127.0.0.1:1/placeholder")
        .expect("lazy pool")
}

async fn seed_strategy(pool: &PgPool) {
    let mut db = DbDirect::connect(pool);
    db.strategy()
        .create_or_ignore(&StrategyFullKeys {
            strategy: "strat".to_string(),
            status: Status::Active,
        })
        .await
        .expect("seed strategy");
}

fn stock_order(symbol: &str, quantity: f64, reference: &str, references_parent: i32) -> OrderIBKR {
    let contract = Contract::stock(symbol).build();
    let mut order = market_order(Action::Buy, quantity);
    order.order_ref = reference.to_string();
    OrderIBKR::new(contract, order, references_parent)
}

fn rejected_order(symbol: &str) -> OrderIBKR {
    stock_order(symbol, 1.0, "strat", -1)
}

async fn optimistic_row_exists(pool: &PgPool, asset: &AssetType, order_id: i32) -> bool {
    let mut db = DbDirect::connect(pool);
    let mut crud = db.open_orders(asset);
    let pk = OpenOrdersPrimaryKeys::new(asset, -1, order_id);
    matches!(crud.read(&pk).await, Ok(Some(_)))
}

/// The optimistic row is written by a spawned task, so poll briefly instead of
/// sleeping a fixed amount.
async fn wait_for_row(pool: &PgPool, asset: &AssetType, order_id: i32) -> bool {
    for _ in 0..50 {
        if optimistic_row_exists(pool, asset, order_id).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

#[tokio::test]
async fn place_order_returns_the_router_id() {
    let pool = lazy_pool();
    let router = Arc::new(FakeOrderRouter::new(&[]));
    let id = OrderEngine::place_order(
        tokio::runtime::Handle::current(),
        pool,
        &Arc::downgrade(&router),
        rejected_order("AAPL"),
    );

    assert_eq!(id, 1000);
    assert_eq!(router.placed().len(), 1);
}

#[tokio::test]
async fn place_order_returns_minus_one_when_the_router_rejects() {
    let pool = lazy_pool();
    let router = Arc::new(FakeOrderRouter::new(&[0]));
    let id = OrderEngine::place_order(
        tokio::runtime::Handle::current(),
        pool,
        &Arc::downgrade(&router),
        rejected_order("AAPL"),
    );

    assert_eq!(id, -1);
}

#[tokio::test]
async fn place_order_checked_surfaces_router_rejection() {
    let pool = lazy_pool();
    let router = Arc::new(FakeOrderRouter::new(&[0]));
    let result = OrderEngine::place_order_checked(
        tokio::runtime::Handle::current(),
        pool,
        &Arc::downgrade(&router),
        rejected_order("AAPL"),
    );

    assert!(result.is_err());
}

#[tokio::test]
async fn place_orders_links_a_child_to_its_parent() {
    let pool = lazy_pool();
    let router = Arc::new(FakeOrderRouter::new(&[]));
    let orders = vec![
        stock_order("AAPL", 10.0, "strat", -1),
        stock_order("AAPL", 10.0, "strat", 0),
    ];
    OrderEngine::place_orders(
        tokio::runtime::Handle::current(),
        pool,
        &Arc::downgrade(&router),
        orders,
    );

    let placed = router.placed();
    assert_eq!(placed.len(), 2);
    assert_eq!(placed[1].order.parent_id, 1000);
}

#[tokio::test]
async fn place_orders_stops_after_the_head_fails() {
    let pool = lazy_pool();
    let router = Arc::new(FakeOrderRouter::new(&[0]));
    let orders = vec![
        stock_order("AAPL", 10.0, "strat", -1),
        stock_order("AAPL", 10.0, "strat", -1),
        stock_order("AAPL", 10.0, "strat", -1),
    ];
    OrderEngine::place_orders(
        tokio::runtime::Handle::current(),
        pool,
        &Arc::downgrade(&router),
        orders,
    );

    // A failing head aborts the sequence: a bracket with no entry must not place its legs.
    assert_eq!(router.placed().len(), 1);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn place_order_persists_an_optimistic_open_order() {
    let db = TestDb::setup().await;
    seed_strategy(&db.pool).await;
    let router = Arc::new(FakeOrderRouter::new(&[]));

    let id = OrderEngine::place_order(
        tokio::runtime::Handle::current(),
        db.pool.clone(),
        &Arc::downgrade(&router),
        stock_order("AAPL", 10.0, "strat", -1),
    );

    assert_eq!(id, 1000);
    assert!(
        wait_for_row(&db.pool, &AssetType::Stock, id).await,
        "expected optimistic open order for id {id}"
    );
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn a_rejected_order_leaves_no_optimistic_row() {
    let db = TestDb::setup().await;
    let router = Arc::new(FakeOrderRouter::new(&[0]));

    let id = OrderEngine::place_order(
        tokio::runtime::Handle::current(),
        db.pool.clone(),
        &Arc::downgrade(&router),
        stock_order("AAPL", 10.0, "strat", -1),
    );
    assert_eq!(id, -1);

    assert!(!optimistic_row_exists(&db.pool, &AssetType::Stock, 0).await);
    assert!(!optimistic_row_exists(&db.pool, &AssetType::Stock, -1).await);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn place_orders_persists_every_accepted_order() {
    let db = TestDb::setup().await;
    seed_strategy(&db.pool).await;
    let router = Arc::new(FakeOrderRouter::new(&[]));

    let orders = vec![
        stock_order("AAPL", 10.0, "strat", -1),
        stock_order("MSFT", 20.0, "strat", -1),
        stock_order("NVDA", 30.0, "strat", -1),
    ];
    OrderEngine::place_orders(
        tokio::runtime::Handle::current(),
        db.pool.clone(),
        &Arc::downgrade(&router),
        orders,
    );

    for id in [1000, 1001, 1002] {
        assert!(
            wait_for_row(&db.pool, &AssetType::Stock, id).await,
            "expected optimistic open order for id {id}"
        );
    }
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn place_orders_writes_rows_only_for_accepted_orders() {
    let db = TestDb::setup().await;
    seed_strategy(&db.pool).await;
    // The second order is rejected; the first and third are accepted.
    let router = Arc::new(FakeOrderRouter::new(&[1]));

    let orders = vec![
        stock_order("AAPL", 10.0, "strat", -1),
        stock_order("MSFT", 20.0, "strat", -1),
        stock_order("NVDA", 30.0, "strat", -1),
    ];
    OrderEngine::place_orders(
        tokio::runtime::Handle::current(),
        db.pool.clone(),
        &Arc::downgrade(&router),
        orders,
    );

    assert!(wait_for_row(&db.pool, &AssetType::Stock, 1000).await);
    assert!(!optimistic_row_exists(&db.pool, &AssetType::Stock, 1001).await);
    assert!(wait_for_row(&db.pool, &AssetType::Stock, 1002).await);
}
