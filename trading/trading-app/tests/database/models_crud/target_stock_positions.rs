use trading_app::database::models::{
    Status, StrategyFullKeys, TargetStockPositionsFullKeys, TargetStockPositionsPrimaryKeys,
    TargetStockPositionsUpdateKeys,
};

const STRATEGY: &str = "test_target_stock_positions";
const EXCHANGE: &str = "NASDAQ";
const CURRENCY: &str = "USD";

pub fn fk(tag: &str) -> TargetStockPositionsFullKeys {
    TargetStockPositionsFullKeys {
        strategy: STRATEGY.to_string(),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        stock: format!("TSP_{tag}"),
        avg_price: 150.0,
        quantity: 100.0,
    }
}

pub fn pk(fk: &TargetStockPositionsFullKeys) -> TargetStockPositionsPrimaryKeys {
    TargetStockPositionsPrimaryKeys {
        strategy: fk.strategy.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        stock: fk.stock.clone(),
    }
}

pub fn pk_other(tag: &str) -> TargetStockPositionsPrimaryKeys {
    TargetStockPositionsPrimaryKeys {
        strategy: STRATEGY.to_string(),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        stock: format!("TSP_{tag}"),
    }
}

pub fn uk(_tag: &str) -> TargetStockPositionsUpdateKeys {
    TargetStockPositionsUpdateKeys {
        avg_price: Some(175.0),
        quantity: Some(250.0),
    }
}

pub fn uk_none() -> TargetStockPositionsUpdateKeys {
    TargetStockPositionsUpdateKeys {
        avg_price: None,
        quantity: None,
    }
}

pub fn conflicting(fk: &TargetStockPositionsFullKeys) -> TargetStockPositionsFullKeys {
    TargetStockPositionsFullKeys {
        strategy: fk.strategy.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        stock: fk.stock.clone(),
        avg_price: fk.avg_price,
        quantity: fk.quantity,
    }
}

pub fn check(expected: &TargetStockPositionsFullKeys, got: &TargetStockPositionsFullKeys) {
    assert_eq!(expected.strategy, got.strategy);
    assert_eq!(expected.stock, got.stock);
    assert_eq!(expected.quantity, got.quantity);
    assert_eq!(expected.avg_price, got.avg_price);
}

pub fn check_updated(uk: &TargetStockPositionsUpdateKeys, got: &TargetStockPositionsFullKeys) {
    assert_eq!(got.quantity, uk.quantity.expect("uk.quantity"));
    assert_eq!(got.avg_price, uk.avg_price.expect("uk.avg_price"));
}

pub fn matches(a: &TargetStockPositionsFullKeys, b: &TargetStockPositionsFullKeys) -> bool {
    a.strategy == b.strategy
        && a.primary_exchange == b.primary_exchange
        && a.currency == b.currency
        && a.stock == b.stock
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

crud_suite!(target_stock_positions, self);
