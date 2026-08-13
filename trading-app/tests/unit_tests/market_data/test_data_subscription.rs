//! Unit tests for `DataSubscription` — custom Hash/Eq impls.
//!
//! See `src/market_data/handler.rs`. `DataSubscription` wraps a `Contract` +
//! `WhatToShow`, with a custom Hash that trims `primary_exchange` whitespace
//! and includes Option-specific fields (right, expiry, strike, multiplier)
//! plus the `what_to_show` variant. Tests cover the Hash/Eq invariants.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use ibapi::contracts::Contract;
use ibapi::market_data::realtime::WhatToShow;
use ibapi::prelude::SecurityType;

use trading_app::test_internals::DataSubscription;

/// Helper: compute the hash of a DataSubscription.
fn hash_of(d: &DataSubscription) -> u64 {
    let mut h = DefaultHasher::new();
    d.hash(&mut h);
    h.finish()
}

fn stock_sub(symbol: &str, what: WhatToShow) -> DataSubscription {
    DataSubscription::new(
        Contract {
            symbol: symbol.into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            primary_exchange: "NASDAQ".into(),
            ..Default::default()
        },
        what,
    )
}

// ============================ identical subscriptions ============================

#[test]
fn identical_stock_subscriptions_hash_equal_and_eq() {
    let a = stock_sub("AAPL", WhatToShow::Trades);
    let b = stock_sub("AAPL", WhatToShow::Trades);
    assert_eq!(hash_of(&a), hash_of(&b));
    assert_eq!(a, b);
}

// ============================ what_to_show differentiates ============================

#[test]
fn different_what_to_show_hashes_differ_and_neq() {
    let a = stock_sub("AAPL", WhatToShow::Bid);
    let b = stock_sub("AAPL", WhatToShow::Ask);
    assert_ne!(hash_of(&a), hash_of(&b));
    assert_ne!(a, b);
}

#[test]
fn trades_vs_midpoint_neq() {
    let a = stock_sub("AAPL", WhatToShow::Trades);
    let b = stock_sub("AAPL", WhatToShow::MidPoint);
    assert_ne!(a, b);
}

// ============================ primary_exchange trim ============================

#[test]
fn primary_exchange_whitespace_trimmed_in_hash() {
    let a = stock_sub("AAPL", WhatToShow::Trades);
    let mut b_contract = a.contract.clone();
    b_contract.primary_exchange = "  NASDAQ  ".into();
    let b = DataSubscription::new(b_contract, WhatToShow::Trades);
    assert_eq!(hash_of(&a), hash_of(&b));
    assert_eq!(a, b); // eq also trims
}

// ============================ different symbol/currency/security_type ============================

#[test]
fn different_symbol_neq() {
    let a = stock_sub("AAPL", WhatToShow::Trades);
    let b = stock_sub("MSFT", WhatToShow::Trades);
    assert_ne!(a, b);
    assert_ne!(hash_of(&a), hash_of(&b));
}

#[test]
fn different_currency_neq() {
    let a = stock_sub("AAPL", WhatToShow::Trades);
    let mut b_contract = a.contract.clone();
    b_contract.currency = "SGD".into();
    let b = DataSubscription::new(b_contract, WhatToShow::Trades);
    assert_ne!(a, b);
}

#[test]
fn stock_vs_option_same_symbol_neq() {
    let a = stock_sub("AAPL", WhatToShow::Trades);
    let option = DataSubscription::new(
        Contract {
            symbol: "AAPL".into(),
            security_type: SecurityType::Option,
            currency: "USD".into(),
            right: "C".into(),
            strike: 150.0,
            last_trade_date_or_contract_month: "20250119".into(),
            multiplier: "100".into(),
            ..Default::default()
        },
        WhatToShow::Trades,
    );
    assert_ne!(a, option);
    assert_ne!(hash_of(&a), hash_of(&option));
}

// ============================ option-specific fields ============================

#[test]
fn option_different_strike_neq() {
    let base = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Option,
        currency: "USD".into(),
        right: "C".into(),
        last_trade_date_or_contract_month: "20250119".into(),
        multiplier: "100".into(),
        ..Default::default()
    };
    let a = DataSubscription::new(Contract { strike: 150.0, ..base.clone() }, WhatToShow::Trades);
    let b = DataSubscription::new(Contract { strike: 160.0, ..base.clone() }, WhatToShow::Trades);
    assert_ne!(a, b);
    assert_ne!(hash_of(&a), hash_of(&b));
}

#[test]
fn option_different_right_neq() {
    let base = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Option,
        currency: "USD".into(),
        strike: 150.0,
        last_trade_date_or_contract_month: "20250119".into(),
        multiplier: "100".into(),
        ..Default::default()
    };
    let a = DataSubscription::new(Contract { right: "C".into(), ..base.clone() }, WhatToShow::Trades);
    let b = DataSubscription::new(Contract { right: "P".into(), ..base.clone() }, WhatToShow::Trades);
    assert_ne!(a, b);
    assert_ne!(hash_of(&a), hash_of(&b));
}

#[test]
fn option_identical_eq() {
    let a = DataSubscription::new(
        Contract {
            symbol: "AAPL".into(),
            security_type: SecurityType::Option,
            currency: "USD".into(),
            right: "C".into(),
            strike: 150.0,
            last_trade_date_or_contract_month: "20250119".into(),
            multiplier: "100".into(),
            ..Default::default()
        },
        WhatToShow::Trades,
    );
    let b = DataSubscription::new(a.contract.clone(), WhatToShow::Trades);
    assert_eq!(a, b);
    assert_eq!(hash_of(&a), hash_of(&b));
}
