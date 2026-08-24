//! Unit tests for pure CRUD constructors and accessors.
//!
//! See `src/database/models_crud/historical_data/historical_data.rs`. Tests cover:
//! - `VwapBarValue::as_str` — enum→string mapping
//! - `HistoricalDataFullKeys::get_time` — accessor
//! - `HistoricalDataFullKeys::get_price` — accessor (forex bid/ask fallback)
//!
//! NOTE: The full `from_data`/`from_contract_and_order`/`from_strat_and_exec`
//! constructors require complex ibapi types (`ExecutionData`, `Bar`, `Order`)
//! and are covered by T3/T4 integration tests instead.

use chrono::Utc;
use rust_decimal::Decimal;
use trading_app::database::models::{HistoricalForexDataFullKeys, HistoricalStockData};
use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataFullKeys, VwapBarValue,
};

// ============================ VwapBarValue::as_str ============================

#[test]
fn vwap_bar_value_close_as_str() {
    assert_eq!(VwapBarValue::Close.as_str(), "close");
}

#[test]
fn vwap_bar_value_open_as_str() {
    assert_eq!(VwapBarValue::Open.as_str(), "open");
}

#[test]
fn vwap_bar_value_bid_open_as_str() {
    assert_eq!(VwapBarValue::BidOpen.as_str(), "bid_open");
}

#[test]
fn vwap_bar_value_bid_close_as_str() {
    assert_eq!(VwapBarValue::BidClose.as_str(), "bid_close");
}

#[test]
fn vwap_bar_value_ask_open_as_str() {
    assert_eq!(VwapBarValue::AskOpen.as_str(), "ask_open");
}

#[test]
fn vwap_bar_value_ask_close_as_str() {
    assert_eq!(VwapBarValue::AskClose.as_str(), "ask_close");
}

// ============================ HistoricalDataFullKeys::get_time ============================

#[test]
fn historical_data_get_time_forex() {
    let t = Utc::now();
    let fx = HistoricalDataFullKeys::Forex(HistoricalForexDataFullKeys {
        pair: "EUR/USD".to_string(),
        time: t,
        bid_open: Some(1.0),
        bid_high: Some(1.0),
        bid_low: Some(1.0),
        bid_close: Some(1.0),
        ask_open: Some(1.0),
        ask_high: Some(1.0),
        ask_low: Some(1.0),
        ask_close: Some(1.0),
    });
    assert_eq!(fx.get_time(), t);
}

// ============================ HistoricalDataFullKeys::get_price ============================

#[test]
fn historical_data_get_price_forex_ask_close_preferred() {
    // Forex get_price: ask_close.unwrap_or(bid_close.unwrap_or(-1.0))
    // When ask_close is Some, returns ask_close
    let fx = HistoricalDataFullKeys::Forex(HistoricalForexDataFullKeys {
        pair: "EUR/USD".to_string(),
        time: Utc::now(),
        bid_open: None,
        bid_high: None,
        bid_low: None,
        bid_close: Some(1.10),
        ask_open: None,
        ask_high: None,
        ask_low: None,
        ask_close: Some(1.11),
    });
    assert!((fx.get_price() - 1.11).abs() < 1e-9, "ask_close should be preferred");
}

#[test]
fn historical_data_get_price_forex_falls_back_to_bid_close() {
    // When ask_close is None, falls back to bid_close
    let fx = HistoricalDataFullKeys::Forex(HistoricalForexDataFullKeys {
        pair: "EUR/USD".to_string(),
        time: Utc::now(),
        bid_open: None,
        bid_high: None,
        bid_low: None,
        bid_close: Some(1.10),
        ask_open: None,
        ask_high: None,
        ask_low: None,
        ask_close: None,
    });
    assert!((fx.get_price() - 1.10).abs() < 1e-9, "should fall back to bid_close");
}

#[test]
fn historical_data_get_price_forex_returns_neg1_when_both_none() {
    let fx = HistoricalDataFullKeys::Forex(HistoricalForexDataFullKeys {
        pair: "EUR/USD".to_string(),
        time: Utc::now(),
        bid_open: None,
        bid_high: None,
        bid_low: None,
        bid_close: None,
        ask_open: None,
        ask_high: None,
        ask_low: None,
        ask_close: None,
    });
    assert_eq!(fx.get_price(), -1.0, "should return -1.0 sentinel when both None");
}

// ============================ HistoricalStockData construction (smoke) ============================

#[test]
fn historical_stock_data_constructs() {
    // Smoke test: just verify the struct can be constructed with expected fields
    let _data = HistoricalStockData {
        stock: "AAPL".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        time: Utc::now(),
        open: Some(150.0),
        high: Some(155.0),
        low: Some(149.0),
        close: Some(152.0),
        volume: Some(Decimal::from(1000)),
    };
    // If this compiles and runs, the struct shape is as expected
}
