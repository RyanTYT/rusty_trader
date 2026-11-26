use chrono::Utc;
use trading_app::database::models::{
    CurrentStockPositionsFullKeys, CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys,
    Status, StrategyFullKeys,
};

const STRATEGY: &str = "test_current_stock_positions";
const EXCHANGE: &str = "NASDAQ";
const CURRENCY: &str = "USD";

pub fn fk(tag: &str) -> CurrentStockPositionsFullKeys {
    CurrentStockPositionsFullKeys {
        stock: format!("CSP_{tag}"),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        strategy: STRATEGY.to_string(),
        quantity: 100.0,
        avg_price: 150.0,
        last_updated: Utc::now(),
    }
}

pub fn pk(fk: &CurrentStockPositionsFullKeys) -> CurrentStockPositionsPrimaryKeys {
    CurrentStockPositionsPrimaryKeys {
        stock: fk.stock.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        strategy: fk.strategy.clone(),
    }
}

pub fn pk_other(tag: &str) -> CurrentStockPositionsPrimaryKeys {
    CurrentStockPositionsPrimaryKeys {
        stock: format!("CSP_{tag}"),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        strategy: STRATEGY.to_string(),
    }
}

pub fn uk(_tag: &str) -> CurrentStockPositionsUpdateKeys {
    CurrentStockPositionsUpdateKeys {
        quantity: Some(250.0),
        avg_price: Some(175.0),
        last_updated: Some(Utc::now()),
    }
}

pub fn uk_none() -> CurrentStockPositionsUpdateKeys {
    CurrentStockPositionsUpdateKeys {
        quantity: None,
        avg_price: None,
        last_updated: None,
    }
}

pub fn conflicting(fk: &CurrentStockPositionsFullKeys) -> CurrentStockPositionsFullKeys {
    CurrentStockPositionsFullKeys {
        stock: fk.stock.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        strategy: fk.strategy.clone(),
        quantity: fk.quantity,
        avg_price: fk.avg_price,
        last_updated: fk.last_updated,
    }
}

pub fn check(expected: &CurrentStockPositionsFullKeys, actual: &CurrentStockPositionsFullKeys) {
    assert_eq!(actual.stock, expected.stock);
    assert_eq!(actual.primary_exchange, expected.primary_exchange);
    assert_eq!(actual.currency, expected.currency);
    assert_eq!(actual.strategy, expected.strategy);
    assert_eq!(actual.quantity, expected.quantity);
    assert_eq!(actual.avg_price, expected.avg_price);
}

pub fn check_updated(uk: &CurrentStockPositionsUpdateKeys, actual: &CurrentStockPositionsFullKeys) {
    assert_eq!(Some(actual.quantity), uk.quantity);
    assert_eq!(Some(actual.avg_price), uk.avg_price);
}

pub fn matches(pk: &CurrentStockPositionsFullKeys, row: &CurrentStockPositionsFullKeys) -> bool {
    pk.stock == row.stock
        && pk.primary_exchange == row.primary_exchange
        && pk.currency == row.currency
        && pk.strategy == row.strategy
}

pub async fn setup(s: &mut crate::common::TestSession) {
    s.strategy()
        .create_or_ignore(&StrategyFullKeys {
            strategy: STRATEGY.to_string(),
            status: Status::Active,
        })
        .await
        .expect("seed current_stock_positions strategy");
}

crud_suite!(current_stock_positions, self);
