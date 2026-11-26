use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::models::{
    OptionTransactionsFullKeys, OptionTransactionsPrimaryKeys, OptionTransactionsUpdateKeys,
    OptionType, Status, StrategyFullKeys,
};

const STRATEGY: &str = "test_option_transactions";
const EXCHANGE: &str = "NASDAQ";
const CURRENCY: &str = "USD";
const EXPIRY: &str = "20261218";
const STRIKE: f64 = 150.0;
const MULTIPLIER: &str = "100";

fn seq(tag: &str) -> i32 {
    tag.rsplit('_')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

pub fn fk(tag: &str) -> OptionTransactionsFullKeys {
    OptionTransactionsFullKeys {
        execution_id: format!("OTX_{tag}"),
        strategy: STRATEGY.to_string(),
        stock: format!("OPT_{tag}"),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        expiry: EXPIRY.to_string(),
        strike: STRIKE,
        multiplier: MULTIPLIER.to_string(),
        option_type: OptionType::Call,
        order_perm_id: seq(tag),
        time: Utc::now(),
        price: 150.0,
        quantity: 100.0,
        fees: Decimal::new(25, 2),
    }
}

pub fn pk(fk: &OptionTransactionsFullKeys) -> OptionTransactionsPrimaryKeys {
    OptionTransactionsPrimaryKeys {
        execution_id: fk.execution_id.clone(),
    }
}

pub fn pk_other(tag: &str) -> OptionTransactionsPrimaryKeys {
    OptionTransactionsPrimaryKeys {
        execution_id: format!("OTX_{tag}"),
    }
}

pub fn uk(_tag: &str) -> OptionTransactionsUpdateKeys {
    OptionTransactionsUpdateKeys {
        strategy: None,
        stock: None,
        primary_exchange: None,
        currency: None,
        expiry: None,
        strike: None,
        multiplier: None,
        option_type: None,
        order_perm_id: None,
        time: None,
        price: Some(175.0),
        quantity: Some(250.0),
        fees: Some(Decimal::new(75, 2)),
    }
}

/// `create_or_update` proposes a full INSERT tuple before the `ON CONFLICT`
/// branch, so every NOT NULL non-PK column has to carry a value.
pub fn uk_full(_tag: &str) -> OptionTransactionsUpdateKeys {
    OptionTransactionsUpdateKeys {
        strategy: Some(STRATEGY.to_string()),
        stock: Some("OPT_upsert".to_string()),
        primary_exchange: Some(EXCHANGE.to_string()),
        currency: Some(CURRENCY.to_string()),
        expiry: Some(EXPIRY.to_string()),
        strike: Some(STRIKE),
        multiplier: Some(MULTIPLIER.to_string()),
        option_type: Some(OptionType::Call),
        order_perm_id: Some(0),
        time: Some(Utc::now()),
        price: Some(175.0),
        quantity: Some(250.0),
        fees: Some(Decimal::new(75, 2)),
    }
}

pub fn uk_none() -> OptionTransactionsUpdateKeys {
    OptionTransactionsUpdateKeys {
        strategy: None,
        stock: None,
        primary_exchange: None,
        currency: None,
        expiry: None,
        strike: None,
        multiplier: None,
        option_type: None,
        order_perm_id: None,
        time: None,
        price: None,
        quantity: None,
        fees: None,
    }
}

pub fn conflicting(fk: &OptionTransactionsFullKeys) -> OptionTransactionsFullKeys {
    OptionTransactionsFullKeys {
        execution_id: fk.execution_id.clone(),
        strategy: fk.strategy.clone(),
        stock: fk.stock.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        expiry: fk.expiry.clone(),
        strike: fk.strike,
        multiplier: fk.multiplier.clone(),
        option_type: fk.option_type.clone(),
        order_perm_id: fk.order_perm_id,
        time: fk.time,
        price: fk.price,
        quantity: fk.quantity,
        fees: fk.fees,
    }
}

pub fn check(expected: &OptionTransactionsFullKeys, got: &OptionTransactionsFullKeys) {
    assert_eq!(expected.execution_id, got.execution_id);
    assert_eq!(expected.stock, got.stock);
    assert_eq!(expected.expiry, got.expiry);
    assert_eq!(expected.strike, got.strike);
    assert_eq!(expected.option_type, got.option_type);
    assert_eq!(expected.price, got.price);
    assert_eq!(expected.quantity, got.quantity);
    assert_eq!(expected.fees, got.fees);
}

pub fn check_updated(uk: &OptionTransactionsUpdateKeys, got: &OptionTransactionsFullKeys) {
    assert_eq!(got.price, uk.price.expect("uk.price"));
    assert_eq!(got.quantity, uk.quantity.expect("uk.quantity"));
}

pub fn matches(a: &OptionTransactionsFullKeys, b: &OptionTransactionsFullKeys) -> bool {
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

crud_suite!(option_transactions, self, uk_full);
