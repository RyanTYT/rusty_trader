//! Ops tests for `TargetPositionsCRUD`
//! (`src/database/models_crud/target_positions/target_positions.rs`).
//!
//! Happy paths: the target-vs-current diff for one instrument and for a
//! strategy, and the strategy-scoped clear. Each test talks to a live database.

use crate::common::{SessionKind, TestDb, TestSession, tag};
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    AssetType, CurrentOptionPositionsFullKeys, CurrentStockPositionsFullKeys, Status,
    StrategyFullKeys, TargetOptionPositionsFullKeys, TargetOptionPositionsPrimaryKeys,
    TargetStockPositionsFullKeys, TargetStockPositionsPrimaryKeys,
};
use trading_app::database::models_crud::target_positions::target_option_positions::TargetOptionPositionsQtyDiff;
use trading_app::database::models_crud::target_positions::target_positions::{
    TargetPositionsOps, TargetPositionsPrimaryKeys, TargetPositionsQtyDiff,
};
use trading_app::database::models_crud::target_positions::target_stock_positions::TargetStockPositionsQtyDiff;
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

fn target_stock(tag: &str, strategy: &str) -> TargetStockPositionsFullKeys {
    let mut row = super::target_stock_positions::fk(tag);
    row.strategy = strategy.to_string();
    row
}

fn target_option(tag: &str, strategy: &str) -> TargetOptionPositionsFullKeys {
    let mut row = super::target_option_positions::fk(tag);
    row.strategy = strategy.to_string();
    row
}

fn current_stock(
    tag: &str,
    strategy: &str,
    stock: &str,
    quantity: f64,
) -> CurrentStockPositionsFullKeys {
    let mut row = super::current_stock_positions::fk(tag);
    row.strategy = strategy.to_string();
    row.stock = stock.to_string();
    row.quantity = quantity;
    row
}

fn current_option(
    tag: &str,
    strategy: &str,
    target: &TargetOptionPositionsFullKeys,
    quantity: f64,
) -> CurrentOptionPositionsFullKeys {
    let mut row = super::current_option_positions::fk(tag);
    row.strategy = strategy.to_string();
    row.stock = target.stock.clone();
    row.primary_exchange = target.primary_exchange.clone();
    row.currency = target.currency.clone();
    row.expiry = target.expiry.clone();
    row.strike = target.strike;
    row.multiplier = target.multiplier.clone();
    row.option_type = target.option_type.clone();
    row.quantity = quantity;
    row
}

fn stock_pk(row: &TargetStockPositionsFullKeys) -> TargetPositionsPrimaryKeys {
    TargetPositionsPrimaryKeys::Stock(TargetStockPositionsPrimaryKeys {
        strategy: row.strategy.clone(),
        primary_exchange: row.primary_exchange.clone(),
        currency: row.currency.clone(),
        stock: row.stock.clone(),
    })
}

fn option_pk(row: &TargetOptionPositionsFullKeys) -> TargetPositionsPrimaryKeys {
    TargetPositionsPrimaryKeys::Options(TargetOptionPositionsPrimaryKeys {
        strategy: row.strategy.clone(),
        primary_exchange: row.primary_exchange.clone(),
        currency: row.currency.clone(),
        stock: row.stock.clone(),
        expiry: row.expiry.clone(),
        strike: row.strike,
        multiplier: row.multiplier.clone(),
        option_type: row.option_type.clone(),
    })
}

fn as_stock(diff: &TargetPositionsQtyDiff) -> &TargetStockPositionsQtyDiff {
    match diff {
        TargetPositionsQtyDiff::Stock(d) => d,
        TargetPositionsQtyDiff::Options(_) => panic!("expected a stock diff"),
    }
}

fn as_option(diff: &TargetPositionsQtyDiff) -> &TargetOptionPositionsQtyDiff {
    match diff {
        TargetPositionsQtyDiff::Options(d) => d,
        TargetPositionsQtyDiff::Stock(_) => panic!("expected an option diff"),
    }
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn diff_by_pk_is_empty_then_target_then_net_of_current() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct)
        .await
        .unwrap();
    let strategy = tag("tp_pk_stock");
    seed_strategy(&mut s, &strategy).await;

    let target = target_stock(&tag("tp_target"), &strategy);
    let pk = stock_pk(&target);

    let empty = s
        .target_positions(&AssetType::Stock)
        .get_target_pos_diff_by_pk(pk.clone())
        .await
        .unwrap();
    assert!(empty.is_empty());

    s.target_stock_positions().create(&target).await.unwrap();
    let one = s
        .target_positions(&AssetType::Stock)
        .get_target_pos_diff_by_pk(pk.clone())
        .await
        .unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(as_stock(&one[0]).qty_diff, 100.0);
    assert_eq!(as_stock(&one[0]).current_qty, 0.0);

    s.current_stock_positions()
        .create(&current_stock(
            &tag("tp_current"),
            &strategy,
            &target.stock,
            40.0,
        ))
        .await
        .unwrap();

    let one = s
        .target_positions(&AssetType::Stock)
        .get_target_pos_diff_by_pk(pk)
        .await
        .unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(as_stock(&one[0]).qty_diff, 60.0);
    assert_eq!(as_stock(&one[0]).current_qty, 40.0);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn diff_by_strat_returns_zero_one_many_and_excludes_other_strategies() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct)
        .await
        .unwrap();
    let strategy = tag("tp_strat_a");
    let other = tag("tp_strat_b");
    seed_strategy(&mut s, &strategy).await;
    seed_strategy(&mut s, &other).await;

    let empty = s
        .target_positions(&AssetType::Stock)
        .get_target_pos_diff_by_strat(&strategy)
        .await
        .unwrap();
    assert!(empty.is_empty());

    let first = target_stock(&tag("tp_s1"), &strategy);
    let second = target_stock(&tag("tp_s2"), &strategy);
    let foreign = target_stock(&tag("tp_s3"), &other);
    s.target_stock_positions().create(&first).await.unwrap();
    s.target_stock_positions().create(&second).await.unwrap();
    s.target_stock_positions().create(&foreign).await.unwrap();
    s.current_stock_positions()
        .create(&current_stock(
            &tag("tp_c1"),
            &strategy,
            &first.stock,
            30.0,
        ))
        .await
        .unwrap();

    let rows = s
        .target_positions(&AssetType::Stock)
        .get_target_pos_diff_by_strat(&strategy)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| as_stock(r).strategy == strategy));

    let first_diff = rows
        .iter()
        .map(as_stock)
        .find(|d| d.stock == first.stock)
        .expect("first target");
    assert_eq!(first_diff.qty_diff, 70.0);
    assert_eq!(first_diff.current_qty, 30.0);

    let second_diff = rows
        .iter()
        .map(as_stock)
        .find(|d| d.stock == second.stock)
        .expect("second target");
    assert_eq!(second_diff.qty_diff, 100.0);
    assert_eq!(second_diff.current_qty, 0.0);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn clear_strat_pos_removes_only_that_strategy() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct)
        .await
        .unwrap();
    let strategy = tag("tp_clear_a");
    let other = tag("tp_clear_b");
    seed_strategy(&mut s, &strategy).await;
    seed_strategy(&mut s, &other).await;

    let first = target_stock(&tag("tp_c1"), &strategy);
    let second = target_stock(&tag("tp_c2"), &strategy);
    let foreign = target_stock(&tag("tp_c3"), &other);
    s.target_stock_positions().create(&first).await.unwrap();
    s.target_stock_positions().create(&second).await.unwrap();
    s.target_stock_positions().create(&foreign).await.unwrap();

    s.target_positions(&AssetType::Stock)
        .clear_strat_pos(&strategy)
        .await
        .unwrap();

    let remaining = s.target_stock_positions().read_all().await.unwrap();
    assert!(!remaining.iter().any(|r| r.strategy == strategy));
    assert!(remaining.iter().any(|r| r.strategy == other));
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn option_diff_by_pk_nets_the_current_option_position() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct)
        .await
        .unwrap();
    let strategy = tag("tp_pk_option");
    seed_strategy(&mut s, &strategy).await;

    let target = target_option(&tag("tp_opt"), &strategy);
    s.target_option_positions().create(&target).await.unwrap();
    s.current_option_positions()
        .create(&current_option(
            &tag("tp_opt_current"),
            &strategy,
            &target,
            40.0,
        ))
        .await
        .unwrap();

    let rows = s
        .target_positions(&AssetType::Option)
        .get_target_pos_diff_by_pk(option_pk(&target))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(as_option(&rows[0]).qty_diff, 60.0);
    assert_eq!(as_option(&rows[0]).current_qty, 40.0);
}
