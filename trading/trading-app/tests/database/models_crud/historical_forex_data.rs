use chrono::{DateTime, Utc};
use trading_app::database::models::{
    HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeys, HistoricalForexDataUpdateKeys,
};

fn seq(tag: &str) -> i32 {
    tag.rsplit('_')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap()
}

pub fn fk(tag: &str) -> HistoricalForexDataFullKeys {
    HistoricalForexDataFullKeys {
        pair: format!("FX{}", seq(tag)),
        time: now(),
        bid_open: Some(1.0850),
        bid_high: Some(1.0870),
        bid_low: Some(1.0840),
        bid_close: Some(1.0860),
        ask_open: Some(1.0852),
        ask_high: Some(1.0872),
        ask_low: Some(1.0842),
        ask_close: Some(1.0862),
    }
}

pub fn pk(fk: &HistoricalForexDataFullKeys) -> HistoricalForexDataPrimaryKeys {
    HistoricalForexDataPrimaryKeys {
        pair: fk.pair.clone(),
        time: fk.time,
    }
}

pub fn pk_other(tag: &str) -> HistoricalForexDataPrimaryKeys {
    HistoricalForexDataPrimaryKeys {
        pair: format!("FX{}", seq(tag)),
        time: now(),
    }
}

pub fn uk(_tag: &str) -> HistoricalForexDataUpdateKeys {
    HistoricalForexDataUpdateKeys {
        bid_open: None,
        bid_high: None,
        bid_low: None,
        bid_close: Some(1.09),
        ask_open: None,
        ask_high: None,
        ask_low: None,
        ask_close: Some(1.0902),
    }
}

pub fn uk_full(_tag: &str) -> HistoricalForexDataUpdateKeys {
    HistoricalForexDataUpdateKeys {
        bid_open: Some(1.0849),
        bid_high: Some(1.0869),
        bid_low: Some(1.0839),
        bid_close: Some(1.09),
        ask_open: Some(1.0851),
        ask_high: Some(1.0871),
        ask_low: Some(1.0841),
        ask_close: Some(1.0902),
    }
}

pub fn uk_none() -> HistoricalForexDataUpdateKeys {
    HistoricalForexDataUpdateKeys {
        bid_open: None,
        bid_high: None,
        bid_low: None,
        bid_close: None,
        ask_open: None,
        ask_high: None,
        ask_low: None,
        ask_close: None,
    }
}

pub fn conflicting(fk: &HistoricalForexDataFullKeys) -> HistoricalForexDataFullKeys {
    HistoricalForexDataFullKeys {
        pair: fk.pair.clone(),
        time: fk.time,
        bid_open: fk.bid_open,
        bid_high: fk.bid_high,
        bid_low: fk.bid_low,
        bid_close: fk.bid_close,
        ask_open: fk.ask_open,
        ask_high: fk.ask_high,
        ask_low: fk.ask_low,
        ask_close: fk.ask_close,
    }
}

pub fn check(a: &HistoricalForexDataFullKeys, b: &HistoricalForexDataFullKeys) {
    assert_eq!(a.pair, b.pair);
    assert_eq!(a.time, b.time);
    assert_eq!(a.bid_close, b.bid_close);
    assert_eq!(a.ask_close, b.ask_close);
}

pub fn check_updated(uk: &HistoricalForexDataUpdateKeys, got: &HistoricalForexDataFullKeys) {
    assert_eq!(got.bid_close, uk.bid_close);
    assert_eq!(got.ask_close, uk.ask_close);
}

pub fn matches(pk: &HistoricalForexDataFullKeys, row: &HistoricalForexDataFullKeys) -> bool {
    pk.pair == row.pair && pk.time == row.time
}

pub async fn setup(_s: &mut crate::common::TestSession) {}

crud_suite!(historical_forex_data, self, uk_full);
