use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use trading_app::database::models::{
    HistoricalOptionsDataFullKeys, HistoricalOptionsDataPrimaryKeys, HistoricalOptionsDataUpdateKeys,
    OptionType,
};

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

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap()
}

pub fn fk(tag: &str) -> HistoricalOptionsDataFullKeys {
    HistoricalOptionsDataFullKeys {
        stock: format!("HO{}", seq(tag)),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        expiry: EXPIRY.to_string(),
        strike: STRIKE,
        multiplier: MULTIPLIER.to_string(),
        option_type: OptionType::Call,
        time: now(),
        open: 3.50,
        high: 4.00,
        low: 3.25,
        close: 3.75,
        volume: Decimal::new(500, 0),
    }
}

pub fn pk(fk: &HistoricalOptionsDataFullKeys) -> HistoricalOptionsDataPrimaryKeys {
    HistoricalOptionsDataPrimaryKeys {
        stock: fk.stock.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        expiry: fk.expiry.clone(),
        strike: fk.strike,
        multiplier: fk.multiplier.clone(),
        option_type: fk.option_type.clone(),
        time: fk.time,
    }
}

pub fn pk_other(tag: &str) -> HistoricalOptionsDataPrimaryKeys {
    HistoricalOptionsDataPrimaryKeys {
        stock: format!("HO{}", seq(tag)),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        expiry: EXPIRY.to_string(),
        strike: STRIKE,
        multiplier: MULTIPLIER.to_string(),
        option_type: OptionType::Call,
        time: now(),
    }
}

pub fn uk(_tag: &str) -> HistoricalOptionsDataUpdateKeys {
    HistoricalOptionsDataUpdateKeys {
        open: None,
        high: None,
        low: None,
        close: Some(4.25),
        volume: Some(Decimal::new(600, 0)),
    }
}

pub fn uk_full(_tag: &str) -> HistoricalOptionsDataUpdateKeys {
    HistoricalOptionsDataUpdateKeys {
        open: Some(3.6),
        high: Some(4.1),
        low: Some(3.3),
        close: Some(4.25),
        volume: Some(Decimal::new(600, 0)),
    }
}

pub fn uk_none() -> HistoricalOptionsDataUpdateKeys {
    HistoricalOptionsDataUpdateKeys {
        open: None,
        high: None,
        low: None,
        close: None,
        volume: None,
    }
}

pub fn conflicting(fk: &HistoricalOptionsDataFullKeys) -> HistoricalOptionsDataFullKeys {
    HistoricalOptionsDataFullKeys {
        stock: fk.stock.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        expiry: fk.expiry.clone(),
        strike: fk.strike,
        multiplier: fk.multiplier.clone(),
        option_type: fk.option_type.clone(),
        time: fk.time,
        open: fk.open,
        high: fk.high,
        low: fk.low,
        close: fk.close,
        volume: fk.volume,
    }
}

pub fn check(a: &HistoricalOptionsDataFullKeys, b: &HistoricalOptionsDataFullKeys) {
    assert_eq!(a.stock, b.stock);
    assert_eq!(a.expiry, b.expiry);
    assert_eq!(a.strike, b.strike);
    assert_eq!(a.multiplier, b.multiplier);
    assert_eq!(a.option_type, b.option_type);
    assert_eq!(a.time, b.time);
    assert_eq!(a.open, b.open);
    assert_eq!(a.close, b.close);
    assert_eq!(a.volume, b.volume);
}

pub fn check_updated(uk: &HistoricalOptionsDataUpdateKeys, got: &HistoricalOptionsDataFullKeys) {
    assert_eq!(got.close, uk.close.unwrap());
    assert_eq!(got.volume, uk.volume.unwrap());
}

pub fn matches(pk: &HistoricalOptionsDataFullKeys, row: &HistoricalOptionsDataFullKeys) -> bool {
    pk.stock == row.stock && pk.expiry == row.expiry && pk.strike == row.strike && pk.time == row.time
}

pub async fn setup(_s: &mut crate::common::TestSession) {}

crud_suite!(historical_options_data, self, uk_full);
