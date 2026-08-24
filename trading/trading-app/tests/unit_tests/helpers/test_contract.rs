//! Unit tests for `helpers/contract.rs` — pure contract-construction & hashing logic.
//!
//! See `src/helpers/contract.rs`. Tests cover:
//! - `HashContract` custom Hash impl (primary_exchange trim, Option fields, OrderedFloat strike)
//! - `get_local_symbol` (per AssetType variant)
//! - `build_contract_from_stock` (CFD/FX/FUT/CASH/none branches + malformed-prefix panics)
//!
//! NOTE: The CASH branch hardcodes `currency: "SGD"` — this is a DESIGN POINT
//! (not a bug), so the test locks in current behavior.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use ibapi::prelude::{Contract, SecurityType};

use trading_app::test_internals::{build_contract_from_stock, get_local_symbol, HashContract};

/// Helper: compute the hash of a HashContract.
fn hash_of(c: &Contract) -> u64 {
    let mut h = DefaultHasher::new();
    HashContract { contract: c.clone() }.hash(&mut h);
    h.finish()
}

// ============================ HashContract ============================

#[test]
fn hash_contract_identical_stock_contracts_hash_equal() {
    let a = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        exchange: "SMART".into(),
        currency: "USD".into(),
        primary_exchange: "NASDAQ".into(),
        ..Default::default()
    };
    let b = a.clone();
    assert_eq!(hash_of(&a), hash_of(&b));
}

#[test]
fn hash_contract_primary_exchange_whitespace_trimmed() {
    // The custom Hash trims primary_exchange whitespace; " NASDAQ " and "NASDAQ" hash equal.
    let a = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        primary_exchange: "NASDAQ".into(),
        ..Default::default()
    };
    let b = Contract {
        primary_exchange: "  NASDAQ  ".into(),
        ..a.clone()
    };
    assert_eq!(hash_of(&a), hash_of(&b));
}

#[test]
fn hash_contract_different_symbol_hashes_differ() {
    let a = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        ..Default::default()
    };
    let b = Contract { symbol: "MSFT".into(), ..a.clone() };
    assert_ne!(hash_of(&a), hash_of(&b));
}

#[test]
fn hash_contract_different_currency_hashes_differ() {
    let a = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        ..Default::default()
    };
    let b = Contract { currency: "SGD".into(), ..a.clone() };
    assert_ne!(hash_of(&a), hash_of(&b));
}

#[test]
fn hash_contract_option_strike_included() {
    // Options with different strike should hash differently
    let base = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Option,
        currency: "USD".into(),
        right: "C".into(),
        last_trade_date_or_contract_month: "20250119".into(),
        multiplier: "100".into(),
        ..Default::default()
    };
    let a = Contract { strike: 150.0, ..base.clone() };
    let b = Contract { strike: 160.0, ..base.clone() };
    assert_ne!(hash_of(&a), hash_of(&b));
}

#[test]
fn hash_contract_option_right_included() {
    let base = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Option,
        currency: "USD".into(),
        strike: 150.0,
        last_trade_date_or_contract_month: "20250119".into(),
        multiplier: "100".into(),
        ..Default::default()
    };
    let a = Contract { right: "C".into(), ..base.clone() };
    let b = Contract { right: "P".into(), ..base.clone() };
    assert_ne!(hash_of(&a), hash_of(&b));
}

#[test]
fn hash_contract_stock_vs_option_differ() {
    let stock = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        ..Default::default()
    };
    let option = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Option,
        currency: "USD".into(),
        right: "C".into(),
        strike: 150.0,
        last_trade_date_or_contract_month: "20250119".into(),
        multiplier: "100".into(),
        ..Default::default()
    };
    assert_ne!(hash_of(&stock), hash_of(&option));
}

// ============================ get_local_symbol ============================

#[test]
fn get_local_symbol_stock() {
    let c = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        ..Default::default()
    };
    assert_eq!(get_local_symbol(&c), "AAPL");
}

#[test]
fn get_local_symbol_future() {
    let c = Contract {
        symbol: "ES".into(),
        security_type: SecurityType::Future,
        ..Default::default()
    };
    assert_eq!(get_local_symbol(&c), "FUT:ES");
}

#[test]
fn get_local_symbol_forexpair() {
    let c = Contract {
        symbol: "EUR".into(),
        security_type: SecurityType::ForexPair,
        currency: "USD".into(),
        ..Default::default()
    };
    assert_eq!(get_local_symbol(&c), "FX:EUR/USD");
}

#[test]
fn get_local_symbol_cfd() {
    let c = Contract {
        symbol: "XAUUSD".into(),
        security_type: SecurityType::CFD,
        ..Default::default()
    };
    assert_eq!(get_local_symbol(&c), "CFD:XAUUSD");
}

#[test]
fn get_local_symbol_option() {
    // Option falls through to the bare symbol (like Stock)
    let c = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Option,
        ..Default::default()
    };
    assert_eq!(get_local_symbol(&c), "AAPL");
}

#[test]
fn get_local_symbol_unknown_falls_through() {
    // Bond is not in the 5 mapped SecurityTypes → AssetType::Unknown → bare symbol
    let c = Contract {
        symbol: "UNKNOWN".into(),
        security_type: SecurityType::Bond,
        ..Default::default()
    };
    assert_eq!(get_local_symbol(&c), "UNKNOWN");
}

// ============================ build_contract_from_stock ============================
// Note: Contract fields are newtypes (Symbol, Exchange, Currency) — use .to_string().

#[test]
fn build_contract_stock_no_prefix() {
    let c = build_contract_from_stock(&"AAPL".to_string(), &"NASDAQ".to_string(), &"USD".to_string());
    assert_eq!(c.symbol.to_string(), "AAPL");
    assert_eq!(c.security_type, SecurityType::Stock);
    assert_eq!(c.currency.to_string(), "USD");
    assert_eq!(c.primary_exchange.to_string(), "NASDAQ");
}

#[test]
fn build_contract_cfd_prefix() {
    let c = build_contract_from_stock(&"CFD:XAUUSD".to_string(), &"".to_string(), &"USD".to_string());
    assert_eq!(c.symbol.to_string(), "XAUUSD");
    assert_eq!(c.security_type, SecurityType::CFD);
    assert_eq!(c.exchange.to_string(), "SMART");
    assert_eq!(c.currency.to_string(), "USD");
}

#[test]
fn build_contract_fx_prefix() {
    let c = build_contract_from_stock(&"FX:EUR/USD".to_string(), &"".to_string(), &"USD".to_string());
    assert_eq!(c.security_type, SecurityType::ForexPair);
    assert_eq!(c.exchange.to_string(), "IDEALPRO");
    assert_eq!(c.symbol.to_string(), "EUR");
}

#[test]
fn build_contract_fut_prefix() {
    let c = build_contract_from_stock(&"FUT:ES".to_string(), &"".to_string(), &"USD".to_string());
    assert_eq!(c.security_type, SecurityType::Future);
    assert_eq!(c.exchange.to_string(), "SMART");
    assert_eq!(c.currency.to_string(), "USD");
}

#[test]
fn build_contract_cash_prefix_hardcodes_sgd_currency() {
    // DESIGN POINT: CASH branch hardcodes currency="SGD", ignoring the currency param.
    // This is intentional (not a bug) — locked in as current behavior.
    let c = build_contract_from_stock(&"CASH:USD".to_string(), &"".to_string(), &"JPY".to_string());
    assert_eq!(c.security_type, SecurityType::ForexPair);
    assert_eq!(c.exchange.to_string(), "IDEALPRO");
    assert_eq!(c.currency.to_string(), "SGD"); // hardcoded, ignores "JPY" param
}

#[test]
#[should_panic]
fn build_contract_cfd_prefix_without_colon_panics() {
    // "CFD" (no colon) → split(':').next() == "CFD" matches, then strip_prefix("CFD:") fails → panic
    let _ = build_contract_from_stock(&"CFD".to_string(), &"".to_string(), &"".to_string());
}

#[test]
#[should_panic]
fn build_contract_fx_prefix_without_colon_panics() {
    let _ = build_contract_from_stock(&"FX".to_string(), &"".to_string(), &"".to_string());
}

#[test]
#[should_panic]
fn build_contract_fut_prefix_without_colon_panics() {
    let _ = build_contract_from_stock(&"FUT".to_string(), &"".to_string(), &"".to_string());
}

#[test]
#[should_panic]
fn build_contract_cash_prefix_without_colon_panics() {
    let _ = build_contract_from_stock(&"CASH".to_string(), &"".to_string(), &"".to_string());
}
