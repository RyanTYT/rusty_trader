//! Shared fixtures for the backtester tests.
//!
//! All timestamps are fixed literals (never `Utc::now()`), all contracts are
//! built from explicit fields (no IB), and `FakePrices` implements the prod
//! `PriceSupplier` trait so equity / fill tests can run without a DB.

use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;
use rust_decimal::Decimal;

use trading_app::database::models::HistoricalStockDataFullKeys;
use trading_app::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use trading_app::market_data::traits::current_price::PriceSupplier;

/// Fixed UTC timestamp builder. Panics on an invalid date (test bug).
pub fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, mi, s)
        .single()
        .expect("valid fixed UTC timestamp")
}

/// A US stock contract (the shape `build_contract_from_stock` produces).
pub fn stock(symbol: &str) -> Contract {
    Contract {
        symbol: symbol.into(),
        security_type: SecurityType::Stock,
        exchange: "SMART".into(),
        primary_exchange: "NASDAQ".into(),
        currency: "USD".into(),
        ..Default::default()
    }
}

/// A stock contract with explicit primary exchange / currency.
pub fn stock_full(symbol: &str, primary_exchange: &str, currency: &str) -> Contract {
    Contract {
        symbol: symbol.into(),
        security_type: SecurityType::Stock,
        exchange: "SMART".into(),
        primary_exchange: primary_exchange.into(),
        currency: currency.into(),
        ..Default::default()
    }
}

/// An IDEALPRO forex pair contract.
pub fn forex(base: &str, quote: &str) -> Contract {
    Contract {
        symbol: base.into(),
        security_type: SecurityType::ForexPair,
        exchange: "IDEALPRO".into(),
        primary_exchange: "".into(),
        currency: quote.into(),
        ..Default::default()
    }
}

/// A stock bar with the default key `("QQQ", "NASDAQ", "USD")`.
pub fn bar(
    time: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
) -> HistoricalDataFullKeys {
    bar_full("QQQ", "NASDAQ", "USD", time, open, high, low, close)
}

/// A stock bar with an explicit (stock, primary_exchange, currency) key.
pub fn bar_full(
    stock: &str,
    primary_exchange: &str,
    currency: &str,
    time: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
) -> HistoricalDataFullKeys {
    HistoricalDataFullKeys::Stock(HistoricalStockDataFullKeys {
        stock: stock.to_string(),
        primary_exchange: primary_exchange.to_string(),
        currency: currency.to_string(),
        time,
        open,
        high,
        low,
        close,
        volume: Decimal::new(1_000, 0),
    })
}

/// A forex bar (bid/ask OHLC; `decide_fill` refuses these).
pub fn fx_bar(time: DateTime<Utc>) -> HistoricalDataFullKeys {
    use trading_app::database::models::HistoricalForexDataFullKeys;
    HistoricalDataFullKeys::Forex(HistoricalForexDataFullKeys {
        pair: "FX:EUR/USD".to_string(),
        time,
        bid_open: Some(1.10),
        bid_high: Some(1.11),
        bid_low: Some(1.09),
        bid_close: Some(1.105),
        ask_open: Some(1.1002),
        ask_high: Some(1.1102),
        ask_low: Some(1.0902),
        ask_close: Some(1.1052),
    })
}

/// A fake point-in-time price supplier: explicit stock quotes plus an FX map
/// (both directions, reverse resolved by inversion). Missing entries return an
/// error (so callers exercise their documented fallbacks).
#[derive(Default)]
pub struct FakePrices {
    quotes: HashMap<(String, String), f64>,
    fx: HashMap<(String, String), f64>,
}

impl FakePrices {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_quote(mut self, symbol: &str, currency: &str, price: f64) -> Self {
        self.quotes
            .insert((symbol.to_string(), currency.to_string()), price);
        self
    }

    pub fn with_fx(mut self, base: &str, quote: &str, rate: f64) -> Self {
        self.fx.insert((base.to_string(), quote.to_string()), rate);
        self
    }

    pub fn usd_sgd(rate: f64) -> Self {
        Self::new().with_fx("USD", "SGD", rate)
    }
}

impl PriceSupplier for FakePrices {
    fn get_current_price(
        &self,
        contract: Contract,
        _vwap: bool,
        _generic_ticks: &[&str],
    ) -> Result<f64, String> {
        let symbol = contract.symbol.to_string();
        let currency = contract.currency.to_string();
        if contract.security_type == SecurityType::ForexPair {
            if let Some(v) = self.fx.get(&(symbol.clone(), currency.clone())) {
                return Ok(*v);
            }
            if let Some(v) = self.fx.get(&(currency.clone(), symbol.clone())) {
                if *v != 0.0 {
                    return Ok(1.0 / *v);
                }
            }
            return Err(format!("fake FX rate missing for {symbol}/{currency}"));
        }
        self.quotes
            .get(&(symbol.clone(), currency.clone()))
            .copied()
            .ok_or_else(|| format!("fake quote missing for {symbol}/{currency}"))
    }
}

/// Approximate float equality with a clear failure message.
#[track_caller]
pub fn assert_close(actual: f64, expected: f64, eps: f64) {
    assert!(
        (actual - expected).abs() <= eps,
        "expected {expected} +/- {eps}, got {actual} (diff {})",
        (actual - expected).abs()
    );
}
