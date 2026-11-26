use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::models::{
    Status, StockTransactionsFullKeys, StockTransactionsPrimaryKeys, StockTransactionsUpdateKeys,
    StrategyFullKeys,
};

const STRATEGY: &str = "test_stock_transactions";
const EXCHANGE: &str = "NASDAQ";
const CURRENCY: &str = "USD";

fn seq(tag: &str) -> i32 {
    tag.rsplit('_')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

pub fn fk(tag: &str) -> StockTransactionsFullKeys {
    StockTransactionsFullKeys {
        execution_id: format!("STX_{tag}"),
        strategy: STRATEGY.to_string(),
        stock: format!("STK_{tag}"),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        order_perm_id: seq(tag),
        time: Utc::now(),
        price: 150.0,
        quantity: 100.0,
        fees: Decimal::new(50, 2),
    }
}

pub fn pk(fk: &StockTransactionsFullKeys) -> StockTransactionsPrimaryKeys {
    StockTransactionsPrimaryKeys {
        execution_id: fk.execution_id.clone(),
    }
}

pub fn pk_other(tag: &str) -> StockTransactionsPrimaryKeys {
    StockTransactionsPrimaryKeys {
        execution_id: format!("STX_{tag}"),
    }
}

pub fn uk(_tag: &str) -> StockTransactionsUpdateKeys {
    StockTransactionsUpdateKeys {
        strategy: None,
        stock: None,
        primary_exchange: None,
        currency: None,
        order_perm_id: None,
        time: None,
        price: Some(175.0),
        quantity: Some(250.0),
        fees: Some(Decimal::new(75, 2)),
    }
}

/// `create_or_update` proposes a full INSERT tuple before the `ON CONFLICT`
/// branch, so every NOT NULL non-PK column has to carry a value.
pub fn uk_full(_tag: &str) -> StockTransactionsUpdateKeys {
    StockTransactionsUpdateKeys {
        strategy: Some(STRATEGY.to_string()),
        stock: Some("STK_upsert".to_string()),
        primary_exchange: Some(EXCHANGE.to_string()),
        currency: Some(CURRENCY.to_string()),
        order_perm_id: Some(0),
        time: Some(Utc::now()),
        price: Some(175.0),
        quantity: Some(250.0),
        fees: Some(Decimal::new(75, 2)),
    }
}

pub fn uk_none() -> StockTransactionsUpdateKeys {
    StockTransactionsUpdateKeys {
        strategy: None,
        stock: None,
        primary_exchange: None,
        currency: None,
        order_perm_id: None,
        time: None,
        price: None,
        quantity: None,
        fees: None,
    }
}

pub fn conflicting(fk: &StockTransactionsFullKeys) -> StockTransactionsFullKeys {
    StockTransactionsFullKeys {
        execution_id: fk.execution_id.clone(),
        strategy: fk.strategy.clone(),
        stock: fk.stock.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        order_perm_id: fk.order_perm_id,
        time: fk.time,
        price: fk.price,
        quantity: fk.quantity,
        fees: fk.fees,
    }
}

pub fn check(expected: &StockTransactionsFullKeys, got: &StockTransactionsFullKeys) {
    assert_eq!(expected.execution_id, got.execution_id);
    assert_eq!(expected.stock, got.stock);
    assert_eq!(expected.price, got.price);
    assert_eq!(expected.quantity, got.quantity);
    assert_eq!(expected.fees, got.fees);
}

pub fn check_updated(uk: &StockTransactionsUpdateKeys, got: &StockTransactionsFullKeys) {
    assert_eq!(got.price, uk.price.expect("uk.price"));
    assert_eq!(got.quantity, uk.quantity.expect("uk.quantity"));
}

pub fn matches(a: &StockTransactionsFullKeys, b: &StockTransactionsFullKeys) -> bool {
    a.execution_id == b.execution_id
}

pub async fn setup(s: &mut crate::common::TestSession) {
    s.strategy()
        .create_or_ignore(&StrategyFullKeys {
            strategy: STRATEGY.to_string(),
            status: Status::Active,
        })
        .await
        .expect("seed strategy");
}

crud_suite!(stock_transactions, self, uk_full);
