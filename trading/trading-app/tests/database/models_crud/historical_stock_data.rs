use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use trading_app::database::models::{
    HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys, HistoricalStockDataUpdateKeys,
};

const EXCHANGE: &str = "NASDAQ";
const CURRENCY: &str = "USD";

fn seq(tag: &str) -> i32 {
    tag.rsplit('_')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap()
}

pub fn fk(tag: &str) -> HistoricalStockDataFullKeys {
    HistoricalStockDataFullKeys {
        stock: format!("HS{}", seq(tag)),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        time: now(),
        open: 150.0,
        high: 155.0,
        low: 149.0,
        close: 152.0,
        volume: Decimal::new(100_000, 0),
    }
}

pub fn pk(fk: &HistoricalStockDataFullKeys) -> HistoricalStockDataPrimaryKeys {
    HistoricalStockDataPrimaryKeys {
        stock: fk.stock.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        time: fk.time,
    }
}

pub fn pk_other(tag: &str) -> HistoricalStockDataPrimaryKeys {
    HistoricalStockDataPrimaryKeys {
        stock: format!("HS{}", seq(tag)),
        primary_exchange: EXCHANGE.to_string(),
        currency: CURRENCY.to_string(),
        time: now(),
    }
}

pub fn uk(_tag: &str) -> HistoricalStockDataUpdateKeys {
    HistoricalStockDataUpdateKeys {
        open: None,
        high: None,
        low: None,
        close: Some(160.0),
        volume: Some(Decimal::new(200_000, 0)),
    }
}

pub fn uk_full(_tag: &str) -> HistoricalStockDataUpdateKeys {
    HistoricalStockDataUpdateKeys {
        open: Some(151.0),
        high: Some(156.0),
        low: Some(150.0),
        close: Some(160.0),
        volume: Some(Decimal::new(200_000, 0)),
    }
}

pub fn uk_none() -> HistoricalStockDataUpdateKeys {
    HistoricalStockDataUpdateKeys {
        open: None,
        high: None,
        low: None,
        close: None,
        volume: None,
    }
}

pub fn conflicting(fk: &HistoricalStockDataFullKeys) -> HistoricalStockDataFullKeys {
    HistoricalStockDataFullKeys {
        stock: fk.stock.clone(),
        primary_exchange: fk.primary_exchange.clone(),
        currency: fk.currency.clone(),
        time: fk.time,
        open: fk.open,
        high: fk.high,
        low: fk.low,
        close: fk.close,
        volume: fk.volume,
    }
}

pub fn check(a: &HistoricalStockDataFullKeys, b: &HistoricalStockDataFullKeys) {
    assert_eq!(a.stock, b.stock);
    assert_eq!(a.primary_exchange, b.primary_exchange);
    assert_eq!(a.currency, b.currency);
    assert_eq!(a.time, b.time);
    assert_eq!(a.open, b.open);
    assert_eq!(a.high, b.high);
    assert_eq!(a.low, b.low);
    assert_eq!(a.close, b.close);
    assert_eq!(a.volume, b.volume);
}

pub fn check_updated(uk: &HistoricalStockDataUpdateKeys, got: &HistoricalStockDataFullKeys) {
    assert_eq!(got.close, uk.close.unwrap());
    assert_eq!(got.volume, uk.volume.unwrap());
}

pub fn matches(pk: &HistoricalStockDataFullKeys, row: &HistoricalStockDataFullKeys) -> bool {
    pk.stock == row.stock
        && pk.primary_exchange == row.primary_exchange
        && pk.currency == row.currency
        && pk.time == row.time
}

pub async fn setup(_s: &mut crate::common::TestSession) {}

crud_suite!(historical_stock_data, self, uk_full);
