use chrono::Utc;
use trading_app::database::models::{
    CurrentOptionPositionsFullKeys, CurrentOptionPositionsPrimaryKeys,
    CurrentOptionPositionsUpdateKeys, OptionType, Status, StrategyFullKeys,
};

const STRATEGY: &str = "test_current_option_positions";
const EXCHANGE: &str = "NASDAQ";
const CURRENCY: &str = "USD";
const EXPIRY: &str = "20261218";
const STRIKE: f64 = 150.0;
const MULTIPLIER: &str = "100";

pub fn fk(tag: &str) -> CurrentOptionPositionsFullKeys {
    CurrentOptionPositionsFullKeys {
        stock: format!("COP_{tag}"),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        strategy: STRATEGY.to_string(),
        expiry: EXPIRY.to_string(),
        strike: STRIKE,
        multiplier: MULTIPLIER.to_string(),
        option_type: OptionType::Call,
        quantity: 100.0,
        avg_price: 150.0,
        last_updated: Utc::now(),
    }
}

pub fn pk(fk: &CurrentOptionPositionsFullKeys) -> CurrentOptionPositionsPrimaryKeys {
    CurrentOptionPositionsPrimaryKeys {
        stock: fk.stock.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        strategy: fk.strategy.clone(),
        expiry: fk.expiry.clone(),
        strike: fk.strike,
        multiplier: fk.multiplier.clone(),
        option_type: fk.option_type.clone(),
    }
}

pub fn pk_other(tag: &str) -> CurrentOptionPositionsPrimaryKeys {
    CurrentOptionPositionsPrimaryKeys {
        stock: format!("COP_{tag}"),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        strategy: STRATEGY.to_string(),
        expiry: EXPIRY.to_string(),
        strike: STRIKE,
        multiplier: MULTIPLIER.to_string(),
        option_type: OptionType::Call,
    }
}

pub fn uk(_tag: &str) -> CurrentOptionPositionsUpdateKeys {
    CurrentOptionPositionsUpdateKeys {
        quantity: Some(250.0),
        avg_price: Some(175.0),
        last_updated: Some(Utc::now()),
    }
}

pub fn uk_none() -> CurrentOptionPositionsUpdateKeys {
    CurrentOptionPositionsUpdateKeys {
        quantity: None,
        avg_price: None,
        last_updated: None,
    }
}

pub fn conflicting(fk: &CurrentOptionPositionsFullKeys) -> CurrentOptionPositionsFullKeys {
    CurrentOptionPositionsFullKeys {
        stock: fk.stock.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        strategy: fk.strategy.clone(),
        expiry: fk.expiry.clone(),
        strike: fk.strike,
        multiplier: fk.multiplier.clone(),
        option_type: fk.option_type.clone(),
        quantity: fk.quantity,
        avg_price: fk.avg_price,
        last_updated: fk.last_updated,
    }
}

pub fn check(expected: &CurrentOptionPositionsFullKeys, actual: &CurrentOptionPositionsFullKeys) {
    assert_eq!(actual.stock, expected.stock);
    assert_eq!(actual.primary_exchange, expected.primary_exchange);
    assert_eq!(actual.currency, expected.currency);
    assert_eq!(actual.strategy, expected.strategy);
    assert_eq!(actual.expiry, expected.expiry);
    assert_eq!(actual.strike, expected.strike);
    assert_eq!(actual.quantity, expected.quantity);
    assert_eq!(actual.avg_price, expected.avg_price);
}

pub fn check_updated(uk: &CurrentOptionPositionsUpdateKeys, actual: &CurrentOptionPositionsFullKeys) {
    assert_eq!(Some(actual.quantity), uk.quantity);
    assert_eq!(Some(actual.avg_price), uk.avg_price);
}

pub fn matches(pk: &CurrentOptionPositionsFullKeys, row: &CurrentOptionPositionsFullKeys) -> bool {
    pk.stock == row.stock
        && pk.primary_exchange == row.primary_exchange
        && pk.currency == row.currency
        && pk.strategy == row.strategy
        && pk.expiry == row.expiry
        && pk.strike == row.strike
        && pk.multiplier == row.multiplier
}

pub async fn setup(s: &mut crate::common::TestSession) {
    s.strategy()
        .create_or_ignore(&StrategyFullKeys {
            strategy: STRATEGY.to_string(),
            status: Status::Active,
        })
        .await
        .expect("seed current_option_positions strategy");
}

crud_suite!(current_option_positions, self);
