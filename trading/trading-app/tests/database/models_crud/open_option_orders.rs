use chrono::Utc;
use trading_app::database::models::{
    OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys, OptionType,
    Status, StrategyFullKeys,
};

const STRATEGY: &str = "test_open_option_orders";
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

pub fn fk(tag: &str) -> OpenOptionOrdersFullKeys {
    let n = seq(tag);
    OpenOptionOrdersFullKeys {
        order_perm_id: n,
        order_id: n,
        strategy: STRATEGY.to_string(),
        stock: format!("OOO_{tag}"),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        time: Utc::now(),
        quantity: 100.0,
        parent_order_id: 0,
        expiry: EXPIRY.to_string(),
        strike: STRIKE,
        multiplier: MULTIPLIER.to_string(),
        option_type: OptionType::Call,
        executions: vec![],
        filled: 0.0,
    }
}

pub fn pk(fk: &OpenOptionOrdersFullKeys) -> OpenOptionOrdersPrimaryKeys {
    OpenOptionOrdersPrimaryKeys {
        order_perm_id: fk.order_perm_id,
        order_id: fk.order_id,
    }
}

pub fn pk_other(tag: &str) -> OpenOptionOrdersPrimaryKeys {
    let n = seq(tag);
    OpenOptionOrdersPrimaryKeys {
        order_perm_id: n,
        order_id: n,
    }
}

pub fn uk(_tag: &str) -> OpenOptionOrdersUpdateKeys {
    OpenOptionOrdersUpdateKeys {
        strategy: None,
        stock: None,
        primary_exchange: None,
        currency: None,
        time: None,
        quantity: Some(250.0),
        parent_order_id: None,
        expiry: None,
        strike: None,
        multiplier: None,
        option_type: None,
        executions: Some(vec!["exec-1".to_string()]),
        filled: Some(25.0),
    }
}

/// `create_or_update` proposes a full INSERT tuple before the `ON CONFLICT`
/// branch, so every NOT NULL non-PK column has to carry a value.
pub fn uk_full(_tag: &str) -> OpenOptionOrdersUpdateKeys {
    OpenOptionOrdersUpdateKeys {
        strategy: Some(STRATEGY.to_string()),
        stock: Some("OOO_upsert".to_string()),
        primary_exchange: Some(EXCHANGE.to_string()),
        currency: Some(CURRENCY.to_string()),
        time: Some(Utc::now()),
        quantity: Some(250.0),
        parent_order_id: Some(0),
        expiry: Some(EXPIRY.to_string()),
        strike: Some(STRIKE),
        multiplier: Some(MULTIPLIER.to_string()),
        option_type: Some(OptionType::Call),
        executions: Some(vec!["exec-1".to_string()]),
        filled: Some(25.0),
    }
}

pub fn uk_none() -> OpenOptionOrdersUpdateKeys {
    OpenOptionOrdersUpdateKeys {
        strategy: None,
        stock: None,
        primary_exchange: None,
        currency: None,
        time: None,
        quantity: None,
        parent_order_id: None,
        expiry: None,
        strike: None,
        multiplier: None,
        option_type: None,
        executions: None,
        filled: None,
    }
}

pub fn conflicting(fk: &OpenOptionOrdersFullKeys) -> OpenOptionOrdersFullKeys {
    OpenOptionOrdersFullKeys {
        order_perm_id: fk.order_perm_id,
        order_id: fk.order_id,
        strategy: fk.strategy.clone(),
        stock: fk.stock.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        time: fk.time,
        quantity: fk.quantity,
        parent_order_id: fk.parent_order_id,
        expiry: fk.expiry.clone(),
        strike: fk.strike,
        multiplier: fk.multiplier.clone(),
        option_type: fk.option_type.clone(),
        executions: fk.executions.clone(),
        filled: fk.filled,
    }
}

pub fn check(expected: &OpenOptionOrdersFullKeys, actual: &OpenOptionOrdersFullKeys) {
    assert_eq!(actual.order_perm_id, expected.order_perm_id);
    assert_eq!(actual.order_id, expected.order_id);
    assert_eq!(actual.strategy, expected.strategy);
    assert_eq!(actual.stock, expected.stock);
    assert_eq!(actual.expiry, expected.expiry);
    assert_eq!(actual.strike, expected.strike);
    assert_eq!(actual.quantity, expected.quantity);
    assert_eq!(actual.filled, expected.filled);
}

pub fn check_updated(uk: &OpenOptionOrdersUpdateKeys, actual: &OpenOptionOrdersFullKeys) {
    assert_eq!(Some(actual.quantity), uk.quantity);
    assert_eq!(Some(actual.filled), uk.filled);
    assert_eq!(Some(actual.executions.clone()), uk.executions);
}

pub fn matches(pk: &OpenOptionOrdersFullKeys, row: &OpenOptionOrdersFullKeys) -> bool {
    pk.order_perm_id == row.order_perm_id && pk.order_id == row.order_id
}

pub async fn setup(s: &mut crate::common::TestSession) {
    s.strategy()
        .create_or_ignore(&StrategyFullKeys {
            strategy: STRATEGY.to_string(),
            status: Status::Active,
        })
        .await
        .expect("seed open_option_orders strategy");
}

crud_suite!(open_option_orders, self, uk_full);
