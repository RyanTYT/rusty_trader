//! Unit tests for `Consolidator::yahoo_ticker_from_contract` and `is_ibkr_market_data_error`.
//!
//! See `src/market_data/traits/current_price.rs`. These are pure associated functions
//! (don't use `self`) on `impl Consolidator`. Tests cover:
//! - `yahoo_ticker_from_contract` — ~40 exchange→suffix mappings, US bare symbol, unmappable→None
//! - `is_ibkr_market_data_error` — pattern matching on IBKR error strings

use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;
use trading_app::market_data::consolidator::Consolidator;

fn stock(symbol: &str, exchange: &str, primary: &str, currency: &str) -> Contract {
    Contract {
        symbol: symbol.into(),
        security_type: SecurityType::Stock,
        exchange: exchange.into(),
        primary_exchange: primary.into(),
        currency: currency.into(),
        ..Default::default()
    }
}

// ============================ yahoo_ticker_from_contract: US exchanges ============================

#[test]
fn yahoo_ticker_smart_exchange_returns_bare_symbol() {
    let c = stock("AAPL", "SMART", "NASDAQ", "USD");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("AAPL".to_string()));
}

#[test]
fn yahoo_ticker_nyse_returns_bare_symbol() {
    let c = stock("GE", "NYSE", "", "USD");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("GE".to_string()));
}

#[test]
fn yahoo_ticker_nasdaq_returns_bare_symbol() {
    let c = stock("MSFT", "NASDAQ", "", "USD");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("MSFT".to_string()));
}

#[test]
fn yahoo_ticker_usd_empty_exchange_returns_bare_symbol() {
    // currency == "USD" && exchange.is_empty() → US default
    let c = stock("SPY", "", "", "USD");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("SPY".to_string()));
}

// ============================ yahoo_ticker_from_contract: Asia-Pacific ============================

#[test]
fn yahoo_ticker_tokyo_t_suffix() {
    let c = stock("7203", "TSEJ", "", "JPY");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("7203.T".to_string()));
}

#[test]
fn yahoo_ticker_kospi_ks_suffix() {
    let c = stock("005930", "KSE", "", "KRW");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("005930.KS".to_string()));
}

#[test]
fn yahoo_ticker_hong_kong_hk_suffix() {
    let c = stock("0700", "SEHK", "", "HKD");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("0700.HK".to_string()));
}

#[test]
fn yahoo_ticker_singapore_si_suffix() {
    let c = stock("D05", "SGX", "", "SGD");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("D05.SI".to_string()));
}

#[test]
fn yahoo_ticker_india_nse_ns_suffix() {
    let c = stock("RELIANCE", "NSE", "", "INR");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("RELIANCE.NS".to_string()));
}

// ============================ yahoo_ticker_from_contract: Europe ============================

#[test]
fn yahoo_titter_london_l_suffix() {
    let c = stock("BP", "LSE", "", "GBP");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("BP.L".to_string()));
}

#[test]
fn yahoo_ticker_xetra_de_suffix() {
    let c = stock("SAP", "IBIS", "", "EUR");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("SAP.DE".to_string()));
}

#[test]
fn yahoo_ticker_paris_pa_suffix() {
    let c = stock("AIR", "SBF", "", "EUR");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("AIR.PA".to_string()));
}

// ============================ yahoo_ticker_from_contract: Canada ============================

#[test]
fn yahoo_ticker_toronto_to_suffix() {
    let c = stock("RY", "TSX", "", "CAD");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), Some("RY.TO".to_string()));
}

// ============================ yahoo_ticker_from_contract: unmappable ============================

#[test]
fn yahoo_ticker_unmapped_exchange_returns_none() {
    // Use an exchange code not in the mapping table
    let c = stock("XYZ", "UNKNOWN_EXCHANGE", "", "USD");
    assert_eq!(Consolidator::yahoo_ticker_from_contract(&c), None);
}

// ============================ is_ibkr_market_data_error ============================

#[test]
fn is_ibkr_error_not_subscribed_true() {
    assert!(Consolidator::is_ibkr_market_data_error(
        "Requested market data is not subscribed"
    ));
}

#[test]
fn is_ibkr_error_market_data_farm_true() {
    assert!(Consolidator::is_ibkr_market_data_error(
        "market data farm connection is broken"
    ));
}

#[test]
fn is_ibkr_error_no_security_definition_true() {
    assert!(Consolidator::is_ibkr_market_data_error(
        "No security definition has been found for the request"
    ));
}

#[test]
fn is_ibkr_error_354_true() {
    assert!(Consolidator::is_ibkr_market_data_error("Error 354: not subscribed"));
}

#[test]
fn is_ibkr_error_10090_true() {
    assert!(Consolidator::is_ibkr_market_data_error("Part of requested market data is not subscribed (10090)"));
}

#[test]
fn is_ibkr_error_case_insensitive() {
    assert!(Consolidator::is_ibkr_market_data_error("NOT SUBSCRIBED"));
    assert!(Consolidator::is_ibkr_market_data_error("Market Data Farm broken"));
}

#[test]
fn is_ibkr_error_connectivity_error_false() {
    // Connection refused / timeout errors should NOT be classified as market-data errors
    assert!(!Consolidator::is_ibkr_market_data_error("connection refused (os error 111)"));
    assert!(!Consolidator::is_ibkr_market_data_error("timed out waiting for next bar"));
    assert!(!Consolidator::is_ibkr_market_data_error("Broken pipe: (os error 32)"));
}

#[test]
fn is_ibkr_error_empty_string_false() {
    assert!(!Consolidator::is_ibkr_market_data_error(""));
}
