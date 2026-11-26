//! Ops tests for `CurrentPositionsCRUD`
//! (`src/database/models_crud/current_positions/current_positions.rs`).
//!
//! Happy paths for the strategy and primary-key reads, the two grouped
//! views, and the additive upsert. Each test talks to a live database.

use crate::common::{SessionKind, TestDb, TestSession, tag};
use chrono::Utc;
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    AssetType, CurrentStockPositionsFullKeys, CurrentStockPositionsUpdateKeys, Status,
    StrategyFullKeys,
};
use trading_app::database::models_crud::current_positions::current_positions::{
    CurrentPositionsFullKeys, CurrentPositionsOps, CurrentPositionsPrimaryKeys,
    CurrentPositionsUpdateKeys,
};
use trading_app::database::tables::DbTables;

async fn seed_strategy(s: &mut TestSession, name: &str) {
    s.strategy()
        .create_or_ignore(&StrategyFullKeys {
            strategy: name.to_string(),
            status: Status::Active,
        })
        .await
        .unwrap();
}

fn stock_pos(
    tag: &str,
    strategy: &str,
    quantity: f64,
    avg_price: f64,
) -> CurrentStockPositionsFullKeys {
    let mut fk = super::current_stock_positions::fk(tag);
    fk.strategy = strategy.to_string();
    fk.quantity = quantity;
    fk.avg_price = avg_price;
    fk.last_updated = Utc::now();
    fk
}

fn stock_pk(fk: &CurrentStockPositionsFullKeys) -> CurrentPositionsPrimaryKeys {
    CurrentPositionsPrimaryKeys::Stock(super::current_stock_positions::pk(fk))
}

fn additive_uk(quantity: f64, avg_price: f64) -> CurrentPositionsUpdateKeys {
    CurrentPositionsUpdateKeys::Stock(CurrentStockPositionsUpdateKeys {
        quantity: Some(quantity),
        avg_price: Some(avg_price),
        last_updated: Some(Utc::now()),
    })
}

fn is_stock_strategy(row: &CurrentPositionsFullKeys, strategy: &str) -> bool {
    match row {
        CurrentPositionsFullKeys::Stock(stock) => stock.strategy == strategy,
        CurrentPositionsFullKeys::Options(_) => false,
    }
}

fn into_stock(row: CurrentPositionsFullKeys) -> CurrentStockPositionsFullKeys {
    match row {
        CurrentPositionsFullKeys::Stock(stock) => stock,
        CurrentPositionsFullKeys::Options(_) => panic!("expected a stock position"),
    }
}

fn as_stock(row: &CurrentPositionsFullKeys) -> Option<&CurrentStockPositionsFullKeys> {
    match row {
        CurrentPositionsFullKeys::Stock(stock) => Some(stock),
        CurrentPositionsFullKeys::Options(_) => None,
    }
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn get_pos_by_strat_returns_zero_one_many() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let a = tag("cp_strat_a");
    let b = tag("cp_strat_b");
    seed_strategy(&mut s, &a).await;
    seed_strategy(&mut s, &b).await;

    let none = s
        .current_positions(&AssetType::Stock)
        .get_pos_by_strat(&a)
        .await
        .unwrap();
    assert!(none.is_empty());

    s.current_stock_positions()
        .create(&stock_pos(&tag("cp_s1"), &a, 100.0, 10.0))
        .await
        .unwrap();
    let one = s
        .current_positions(&AssetType::Stock)
        .get_pos_by_strat(&a)
        .await
        .unwrap();
    assert_eq!(one.len(), 1);

    s.current_stock_positions()
        .create(&stock_pos(&tag("cp_s2"), &a, 50.0, 20.0))
        .await
        .unwrap();
    s.current_stock_positions()
        .create(&stock_pos(&tag("cp_s3"), &b, 10.0, 30.0))
        .await
        .unwrap();
    let many = s
        .current_positions(&AssetType::Stock)
        .get_pos_by_strat(&a)
        .await
        .unwrap();
    assert_eq!(many.len(), 2);
    assert!(many.iter().all(|row| is_stock_strategy(row, &a)));
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn get_pos_by_pk_returns_none_and_some() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let strategy = tag("cp_pk");
    seed_strategy(&mut s, &strategy).await;

    let missing = stock_pos(&tag("cp_missing"), &strategy, 1.0, 1.0);
    assert!(
        s.current_positions(&AssetType::Stock)
            .get_pos_by_pk(stock_pk(&missing))
            .await
            .unwrap()
            .is_none()
    );

    let fk = stock_pos(&tag("cp_found"), &strategy, 100.0, 10.0);
    s.current_stock_positions().create(&fk).await.unwrap();
    let found = s
        .current_positions(&AssetType::Stock)
        .get_pos_by_pk(stock_pk(&fk))
        .await
        .unwrap()
        .expect("row");
    let found = into_stock(found);
    assert_eq!(found.quantity, 100.0);
    assert_eq!(found.avg_price, 10.0);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn get_all_pos_grouped_sums_quantity_and_weights_price() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let a = tag("cp_group_a");
    let b = tag("cp_group_b");
    seed_strategy(&mut s, &a).await;
    seed_strategy(&mut s, &b).await;

    let mut first = stock_pos(&tag("cp_group_1"), &a, 100.0, 10.0);
    let mut second = stock_pos(&tag("cp_group_2"), &b, 100.0, 20.0);
    second.stock = first.stock.clone();
    second.primary_exchange = first.primary_exchange.clone();
    second.currency = first.currency.clone();
    first.strategy = a.clone();
    s.current_stock_positions().create(&first).await.unwrap();
    s.current_stock_positions().create(&second).await.unwrap();

    let grouped = s
        .current_positions(&AssetType::Stock)
        .get_all_pos_grouped()
        .await
        .unwrap();
    let ours: Vec<_> = grouped
        .iter()
        .filter_map(as_stock)
        .filter(|row| row.stock == first.stock)
        .collect();
    assert_eq!(ours.len(), 1);
    assert_eq!(ours[0].quantity, 200.0);
    assert!((ours[0].avg_price - 15.0).abs() < 1e-9);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn get_all_pos_grouped_by_strat_keeps_strategies_separate() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let a = tag("cp_by_strat_a");
    let b = tag("cp_by_strat_b");
    seed_strategy(&mut s, &a).await;
    seed_strategy(&mut s, &b).await;

    let mut first = stock_pos(&tag("cp_by_strat_1"), &a, 100.0, 10.0);
    let mut second = stock_pos(&tag("cp_by_strat_2"), &b, 50.0, 20.0);
    second.stock = first.stock.clone();
    second.primary_exchange = first.primary_exchange.clone();
    second.currency = first.currency.clone();
    first.strategy = a.clone();
    s.current_stock_positions().create(&first).await.unwrap();
    s.current_stock_positions().create(&second).await.unwrap();

    let grouped = s
        .current_positions(&AssetType::Stock)
        .get_all_pos_grouped_by_strat()
        .await
        .unwrap();
    let for_a: Vec<_> = grouped
        .iter()
        .filter_map(as_stock)
        .filter(|row| row.stock == first.stock && row.strategy == a)
        .collect();
    let for_b: Vec<_> = grouped
        .iter()
        .filter_map(as_stock)
        .filter(|row| row.stock == first.stock && row.strategy == b)
        .collect();
    assert_eq!(for_a.len(), 1);
    assert_eq!(for_a[0].quantity, 100.0);
    assert_eq!(for_a[0].avg_price, 10.0);
    assert_eq!(for_b.len(), 1);
    assert_eq!(for_b[0].quantity, 50.0);
    assert_eq!(for_b[0].avg_price, 20.0);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn update_positions_additive_accumulates_and_averages() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let strategy = tag("cp_add");
    seed_strategy(&mut s, &strategy).await;
    let pk = stock_pos(&tag("cp_add_pos"), &strategy, 0.0, 0.0);

    s.current_positions(&AssetType::Stock)
        .update_positions_additive(stock_pk(&pk), additive_uk(100.0, 10.0))
        .await
        .unwrap();
    let one = s
        .current_positions(&AssetType::Stock)
        .get_pos_by_pk(stock_pk(&pk))
        .await
        .unwrap()
        .expect("row after first fill");
    let one = into_stock(one);
    assert_eq!(one.quantity, 100.0);
    assert_eq!(one.avg_price, 10.0);

    s.current_positions(&AssetType::Stock)
        .update_positions_additive(stock_pk(&pk), additive_uk(50.0, 20.0))
        .await
        .unwrap();
    let two = s
        .current_positions(&AssetType::Stock)
        .get_pos_by_pk(stock_pk(&pk))
        .await
        .unwrap()
        .expect("row after second fill");
    let two = into_stock(two);
    assert_eq!(two.quantity, 150.0);
    assert!((two.avg_price - 40.0 / 3.0).abs() < 1e-9);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn update_positions_additive_flips_average_when_opposite_order_is_larger() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let strategy = tag("cp_flip");
    seed_strategy(&mut s, &strategy).await;
    let pk = stock_pos(&tag("cp_flip_pos"), &strategy, 0.0, 0.0);

    s.current_positions(&AssetType::Stock)
        .update_positions_additive(stock_pk(&pk), additive_uk(100.0, 10.0))
        .await
        .unwrap();
    s.current_positions(&AssetType::Stock)
        .update_positions_additive(stock_pk(&pk), additive_uk(-300.0, 30.0))
        .await
        .unwrap();

    let flipped = s
        .current_positions(&AssetType::Stock)
        .get_pos_by_pk(stock_pk(&pk))
        .await
        .unwrap()
        .expect("row after flip");
    let flipped = into_stock(flipped);
    assert_eq!(flipped.quantity, -200.0);
    assert_eq!(flipped.avg_price, 30.0);
}
