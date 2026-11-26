//! Ops tests for `OpenOrdersCRUD`
//! (`src/database/models_crud/open_orders/open_orders.rs`).
//!
//! Happy paths only: zero, one and many rows for the strategy, and the
//! strategy-scoped delete. Each test talks to a live database.

use crate::common::{SessionKind, TestDb, TestSession, tag};
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    AssetType, OpenOptionOrdersFullKeys, OpenStockOrdersFullKeys, Status, StrategyFullKeys,
};
use trading_app::database::models_crud::open_orders::open_orders::{
    OpenOrdersFullKeys, OpenOrdersOps,
};
use trading_app::database::tables::DbTables;

async fn seed_strategy(s: &mut TestSession, name: &str) {
    s.strategy()
        .create_or_ignore(&StrategyFullKeys {
            strategy: name.to_string(),
            status: Status::Active,
        })
        .await
        .expect("seed strategy");
}

fn stock_order(tag: &str, strategy: &str) -> OpenStockOrdersFullKeys {
    let mut fk = super::open_stock_orders::fk(tag);
    fk.strategy = strategy.to_string();
    fk
}

fn option_order(tag: &str, strategy: &str) -> OpenOptionOrdersFullKeys {
    let mut fk = super::open_option_orders::fk(tag);
    fk.strategy = strategy.to_string();
    fk
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn stock_orders_for_strategy_returns_zero_one_many() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct)
        .await
        .expect("session");

    let strategy = tag("oo_stock_a");
    let other = tag("oo_stock_b");
    seed_strategy(&mut s, &strategy).await;
    seed_strategy(&mut s, &other).await;

    let none = s
        .open_orders(&AssetType::Stock)
        .get_orders_for_strat(&strategy)
        .await
        .expect("empty read");
    assert!(none.is_empty(), "no rows yet for {strategy}");

    s.open_stock_orders()
        .create(&stock_order(&tag("oo_s1"), &strategy))
        .await
        .expect("create first");
    let one = s
        .open_orders(&AssetType::Stock)
        .get_orders_for_strat(&strategy)
        .await
        .expect("one row");
    assert_eq!(one.len(), 1);
    assert!(matches!(one[0], OpenOrdersFullKeys::Stock(_)));

    s.open_stock_orders()
        .create(&stock_order(&tag("oo_s2"), &strategy))
        .await
        .expect("create second");
    s.open_stock_orders()
        .create(&stock_order(&tag("oo_s3"), &other))
        .await
        .expect("create other strategy");

    let many = s
        .open_orders(&AssetType::Stock)
        .get_orders_for_strat(&strategy)
        .await
        .expect("many rows");
    assert_eq!(many.len(), 2, "only this strategy's rows");
    assert!(many.iter().all(|row| match row {
        OpenOrdersFullKeys::Stock(fk) => fk.strategy == strategy,
        OpenOrdersFullKeys::Options(_) => false,
    }));
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn option_orders_for_strategy_returns_zero_one_many() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct)
        .await
        .expect("session");

    let strategy = tag("oo_option_a");
    let other = tag("oo_option_b");
    seed_strategy(&mut s, &strategy).await;
    seed_strategy(&mut s, &other).await;

    let none = s
        .open_orders(&AssetType::Option)
        .get_orders_for_strat(&strategy)
        .await
        .expect("empty read");
    assert!(none.is_empty());

    s.open_option_orders()
        .create(&option_order(&tag("oo_o1"), &strategy))
        .await
        .expect("create first");
    let one = s
        .open_orders(&AssetType::Option)
        .get_orders_for_strat(&strategy)
        .await
        .expect("one row");
    assert_eq!(one.len(), 1);
    assert!(matches!(one[0], OpenOrdersFullKeys::Options(_)));

    s.open_option_orders()
        .create(&option_order(&tag("oo_o2"), &strategy))
        .await
        .expect("create second");
    s.open_option_orders()
        .create(&option_order(&tag("oo_o3"), &other))
        .await
        .expect("create other strategy");

    let many = s
        .open_orders(&AssetType::Option)
        .get_orders_for_strat(&strategy)
        .await
        .expect("many rows");
    assert_eq!(many.len(), 2);
    assert!(many.iter().all(|row| match row {
        OpenOrdersFullKeys::Options(fk) => fk.strategy == strategy,
        OpenOrdersFullKeys::Stock(_) => false,
    }));
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn delete_orders_for_strategy_removes_only_that_strategy() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct)
        .await
        .expect("session");

    let strategy = tag("oo_delete_a");
    let other = tag("oo_delete_b");
    seed_strategy(&mut s, &strategy).await;
    seed_strategy(&mut s, &other).await;

    s.open_stock_orders()
        .create(&stock_order(&tag("oo_d1"), &strategy))
        .await
        .expect("create a1");
    s.open_stock_orders()
        .create(&stock_order(&tag("oo_d2"), &strategy))
        .await
        .expect("create a2");
    s.open_stock_orders()
        .create(&stock_order(&tag("oo_d3"), &other))
        .await
        .expect("create b");

    let deleted = s
        .open_orders(&AssetType::Stock)
        .delete_orders_for_strat(&strategy)
        .await
        .expect("delete");
    assert_eq!(deleted, 2);

    let remaining = s
        .open_stock_orders()
        .read_all()
        .await
        .expect("read_all");
    assert!(
        !remaining.iter().any(|row| row.strategy == strategy),
        "all rows for {strategy} should be gone"
    );
    assert!(
        remaining.iter().any(|row| row.strategy == other),
        "the other strategy is untouched"
    );
}
