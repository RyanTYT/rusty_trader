//! Ops tests for `TransactionsCRUD`
//! (`src/database/models_crud/transactions/transactions.rs`).
//!
//! Happy paths only: zero, one and many rows, ordering and the optional
//! `since` filter. Each test talks to a live database.

use crate::common::{SessionKind, TestDb, TestSession, tag};
use chrono::{Duration, Utc};
use trading_app::database::crud::CRUDTrait;
use trading_app::database::models::{
    AssetType, OptionTransactionsFullKeys, Status, StockTransactionsFullKeys, StrategyFullKeys,
};
use trading_app::database::models_crud::transactions::option_transactions::OptionTransactionsUnderlyingPrimaryKeys;
use trading_app::database::models_crud::transactions::stock_transactions::StockTransactionsUnderlyingPrimaryKeys;
use trading_app::database::models_crud::transactions::transactions::{
    TransactionsFullKeys, TransactionsOps, TransactionsUnderlyingPrimaryKeys,
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

fn stock_row(tag: &str, strategy: &str) -> StockTransactionsFullKeys {
    let mut row = super::stock_transactions::fk(tag);
    row.strategy = strategy.to_string();
    row
}

fn option_row(tag: &str, strategy: &str) -> OptionTransactionsFullKeys {
    let mut row = super::option_transactions::fk(tag);
    row.strategy = strategy.to_string();
    row
}

fn stock_underlying(row: &StockTransactionsFullKeys) -> TransactionsUnderlyingPrimaryKeys {
    TransactionsUnderlyingPrimaryKeys::Stock(StockTransactionsUnderlyingPrimaryKeys {
        stock: row.stock.clone(),
        primary_exchange: row.primary_exchange.clone(),
        currency: row.currency.clone(),
    })
}

fn option_underlying(row: &OptionTransactionsFullKeys) -> TransactionsUnderlyingPrimaryKeys {
    TransactionsUnderlyingPrimaryKeys::Options(OptionTransactionsUnderlyingPrimaryKeys {
        stock: row.stock.clone(),
        primary_exchange: row.primary_exchange.clone(),
        currency: row.currency.clone(),
        expiry: row.expiry.clone(),
        strike: row.strike,
        multiplier: row.multiplier.clone(),
        option_type: row.option_type.clone(),
    })
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn read_last_transaction_stock_returns_none_then_latest() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let strategy = tag("tx_last_stock");
    seed_strategy(&mut s, &strategy).await;

    let older = stock_row(&tag("tx_last_old"), &strategy);
    assert!(
        s.transactions(&AssetType::Stock)
            .read_last_transaction(stock_underlying(&older))
            .await
            .unwrap()
            .is_none()
    );

    s.stock_transactions().create(&older).await.unwrap();
    let last = s
        .transactions(&AssetType::Stock)
        .read_last_transaction(stock_underlying(&older))
        .await
        .unwrap()
        .expect("one row");
    match last {
        TransactionsFullKeys::Stock(row) => assert_eq!(row.execution_id, older.execution_id),
        TransactionsFullKeys::Options(_) => panic!("expected a stock transaction"),
    }

    let mut newer = stock_row(&tag("tx_last_new"), &strategy);
    newer.stock = older.stock.clone();
    newer.primary_exchange = older.primary_exchange.clone();
    newer.currency = older.currency.clone();
    newer.time = Utc::now() + Duration::seconds(5);
    s.stock_transactions().create(&newer).await.unwrap();

    let last = s
        .transactions(&AssetType::Stock)
        .read_last_transaction(stock_underlying(&older))
        .await
        .unwrap()
        .expect("one row");
    match last {
        TransactionsFullKeys::Stock(row) => assert_eq!(row.execution_id, newer.execution_id),
        TransactionsFullKeys::Options(_) => panic!("expected a stock transaction"),
    }
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn read_last_transaction_option_returns_none_then_row() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let strategy = tag("tx_last_option");
    seed_strategy(&mut s, &strategy).await;

    let row = option_row(&tag("tx_last_opt"), &strategy);
    assert!(
        s.transactions(&AssetType::Option)
            .read_last_transaction(option_underlying(&row))
            .await
            .unwrap()
            .is_none()
    );

    s.option_transactions().create(&row).await.unwrap();
    let last = s
        .transactions(&AssetType::Option)
        .read_last_transaction(option_underlying(&row))
        .await
        .unwrap()
        .expect("one row");
    match last {
        TransactionsFullKeys::Options(found) => assert_eq!(found.execution_id, row.execution_id),
        TransactionsFullKeys::Stock(_) => panic!("expected an option transaction"),
    }
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn read_all_transactions_of_strat_filters_and_orders() {
    let db = TestDb::setup().await;
    let mut s = TestSession::new(&db.pool, SessionKind::Direct).await.unwrap();
    let strategy = tag("tx_all_a");
    let other = tag("tx_all_b");
    seed_strategy(&mut s, &strategy).await;
    seed_strategy(&mut s, &other).await;

    let mut first = stock_row(&tag("tx_all_1"), &strategy);
    first.time = Utc::now() - Duration::seconds(10);
    let mut second = stock_row(&tag("tx_all_2"), &strategy);
    second.time = Utc::now() - Duration::seconds(5);
    let third = stock_row(&tag("tx_all_3"), &other);

    for row in [&first, &second, &third] {
        s.stock_transactions().create(row).await.unwrap();
    }

    let all = s
        .transactions(&AssetType::Stock)
        .read_all_transactions_of_strat(&strategy, None)
        .await
        .unwrap();
    let ids: Vec<String> = all
        .into_iter()
        .map(|row| match row {
            TransactionsFullKeys::Stock(row) => row.execution_id,
            TransactionsFullKeys::Options(_) => panic!("expected stock transactions"),
        })
        .collect();
    assert_eq!(ids, vec![first.execution_id.clone(), second.execution_id.clone()]);

    let since = Utc::now() - Duration::seconds(7);
    let recent = s
        .transactions(&AssetType::Stock)
        .read_all_transactions_of_strat(&strategy, Some(since))
        .await
        .unwrap();
    let ids: Vec<String> = recent
        .into_iter()
        .map(|row| match row {
            TransactionsFullKeys::Stock(row) => row.execution_id,
            TransactionsFullKeys::Options(_) => panic!("expected stock transactions"),
        })
        .collect();
    assert_eq!(ids, vec![second.execution_id.clone()]);
}
