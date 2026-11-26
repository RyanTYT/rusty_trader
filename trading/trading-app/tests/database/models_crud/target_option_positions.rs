use trading_app::database::models::{
    OptionType, Status, StrategyFullKeys, TargetOptionPositionsFullKeys,
    TargetOptionPositionsPrimaryKeys, TargetOptionPositionsUpdateKeys,
};

const STRATEGY: &str = "test_target_option_positions";
const EXCHANGE: &str = "NASDAQ";
const CURRENCY: &str = "USD";
const EXPIRY: &str = "20261218";
const STRIKE: f64 = 150.0;
const MULTIPLIER: &str = "100";

pub fn fk(tag: &str) -> TargetOptionPositionsFullKeys {
    TargetOptionPositionsFullKeys {
        strategy: STRATEGY.to_string(),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        stock: format!("TOP_{tag}"),
        expiry: EXPIRY.to_string(),
        strike: STRIKE,
        multiplier: MULTIPLIER.to_string(),
        option_type: OptionType::Call,
        avg_price: 150.0,
        quantity: 100.0,
    }
}

pub fn pk(fk: &TargetOptionPositionsFullKeys) -> TargetOptionPositionsPrimaryKeys {
    TargetOptionPositionsPrimaryKeys {
        strategy: fk.strategy.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        stock: fk.stock.clone(),
        expiry: fk.expiry.clone(),
        strike: fk.strike,
        multiplier: fk.multiplier.clone(),
        option_type: fk.option_type.clone(),
    }
}

pub fn pk_other(tag: &str) -> TargetOptionPositionsPrimaryKeys {
    TargetOptionPositionsPrimaryKeys {
        strategy: STRATEGY.to_string(),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        stock: format!("TOP_{tag}"),
        expiry: EXPIRY.to_string(),
        strike: STRIKE,
        multiplier: MULTIPLIER.to_string(),
        option_type: OptionType::Call,
    }
}

pub fn uk(_tag: &str) -> TargetOptionPositionsUpdateKeys {
    TargetOptionPositionsUpdateKeys {
        avg_price: Some(175.0),
        quantity: Some(250.0),
    }
}

pub fn uk_none() -> TargetOptionPositionsUpdateKeys {
    TargetOptionPositionsUpdateKeys {
        avg_price: None,
        quantity: None,
    }
}

pub fn conflicting(fk: &TargetOptionPositionsFullKeys) -> TargetOptionPositionsFullKeys {
    TargetOptionPositionsFullKeys {
        strategy: fk.strategy.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        stock: fk.stock.clone(),
        expiry: fk.expiry.clone(),
        strike: fk.strike,
        multiplier: fk.multiplier.clone(),
        option_type: fk.option_type.clone(),
        avg_price: fk.avg_price,
        quantity: fk.quantity,
    }
}

pub fn check(expected: &TargetOptionPositionsFullKeys, got: &TargetOptionPositionsFullKeys) {
    assert_eq!(expected.strategy, got.strategy);
    assert_eq!(expected.stock, got.stock);
    assert_eq!(expected.expiry, got.expiry);
    assert_eq!(expected.strike, got.strike);
    assert_eq!(expected.option_type, got.option_type);
    assert_eq!(expected.quantity, got.quantity);
    assert_eq!(expected.avg_price, got.avg_price);
}

pub fn check_updated(uk: &TargetOptionPositionsUpdateKeys, got: &TargetOptionPositionsFullKeys) {
    assert_eq!(got.quantity, uk.quantity.expect("uk.quantity"));
    assert_eq!(got.avg_price, uk.avg_price.expect("uk.avg_price"));
}

pub fn matches(a: &TargetOptionPositionsFullKeys, b: &TargetOptionPositionsFullKeys) -> bool {
    a.strategy == b.strategy
        && a.primary_exchange == b.primary_exchange
        && a.currency == b.currency
        && a.stock == b.stock
        && a.expiry == b.expiry
        && a.strike == b.strike
        && a.multiplier == b.multiplier
        && a.option_type == b.option_type
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

crud_suite!(target_option_positions, self);
