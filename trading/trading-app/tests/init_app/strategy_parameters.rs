//! `StrategyParameters` / `DataSubscription` construction.
//!
//! The production constructor `init_strategies` needs a live client, so these
//! tests exercise the same struct/constructor plumbing with the two roster
//! strategies whose constructors are IB-free (`Manual::new` and
//! `Unknown::new` both ignore the pool) and with hand-built contracts.
//!
//! `DataSubscription` identity is included because every per-contract map in
//! `init_app`/`SyncerEngine` keys on it: duplicates must collapse, unknown
//! contracts must miss, and `what_to_show` must keep Bid/Ask/Trades streams
//! distinct.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use ibapi::contracts::{Contract, Currency, Exchange, SecurityType, Symbol};
use ibapi::prelude::RealtimeWhatToShow;
use trading_app::market_data::handler::DataSubscription;
use trading_app::strategy::manual::Manual;
use trading_app::strategy::strategy::{StrategyEnum, StrategyExecutor};
use trading_app::strategy::unknown::Unknown;
use trading_app::test_internals::strategy_parameters;

use super::lazy_pool;

// ──────────────────────────── helpers ────────────────────────────

fn stock(symbol: &str, primary_exchange: &str, currency: &str) -> Contract {
    Contract {
        symbol: Symbol::from(symbol),
        security_type: SecurityType::Stock,
        exchange: Exchange::from("SMART"),
        primary_exchange: Exchange::from(primary_exchange),
        currency: Currency::from(currency),
        ..Default::default()
    }
}

fn option(symbol: &str, right: &str, expiry: &str, strike: f64, multiplier: &str) -> Contract {
    Contract {
        symbol: Symbol::from(symbol),
        security_type: SecurityType::Option,
        exchange: Exchange::from("SMART"),
        primary_exchange: Exchange::from("NASDAQ"),
        currency: Currency::from("USD"),
        right: right.to_string(),
        last_trade_date_or_contract_month: expiry.to_string(),
        strike,
        multiplier: multiplier.to_string(),
        ..Default::default()
    }
}

fn sub(contract: Contract, what_to_show: RealtimeWhatToShow) -> DataSubscription {
    DataSubscription::new(contract, what_to_show)
}

fn hash_of<T: Hash + ?Sized>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// `WhatToShow` has no `PartialEq`, so compare its stream name — the same
/// `&'static str` mapping `DataSubscription::hash` uses
/// (`src/market_data/handler.rs:65-70`).
fn what_label(what: RealtimeWhatToShow) -> &'static str {
    match what {
        RealtimeWhatToShow::Bid => "bid",
        RealtimeWhatToShow::Ask => "ask",
        RealtimeWhatToShow::Trades => "trades",
        RealtimeWhatToShow::MidPoint => "midpoint",
    }
}

// ──────────────────────────── tests ────────────────────────────

#[test]
fn manual_parameters_fields() {
    let contract = stock("SPY", "ARCA", "USD");
    let params = strategy_parameters(
        StrategyEnum::Manual(Manual::new(lazy_pool())),
        vec![
            sub(contract.clone(), RealtimeWhatToShow::Bid),
            sub(contract.clone(), RealtimeWhatToShow::Ask),
        ],
    );

    assert_eq!(params.strategy.get_name(), "manual");
    assert_eq!(params.subscribed_contracts.len(), 2);

    // init_app.rs:193-199 — Manual subscribes Bid then Ask on one contract.
    assert_eq!(
        what_label(params.subscribed_contracts[0].what_to_show),
        "bid"
    );
    assert_eq!(
        what_label(params.subscribed_contracts[1].what_to_show),
        "ask"
    );
    for s in &params.subscribed_contracts {
        assert_eq!(s.contract.symbol.as_str(), "SPY");
        assert_eq!(s.contract.primary_exchange.as_str(), "ARCA");
        assert_eq!(s.contract.currency.as_str(), "USD");
        assert_eq!(s.contract.security_type, SecurityType::Stock);
    }
}

#[test]
fn unknown_parameters_fields() {
    let contract = stock("VIX", "CBOE", "USD");
    let params = strategy_parameters(
        StrategyEnum::Unknown(Unknown::new(lazy_pool())),
        vec![
            sub(contract.clone(), RealtimeWhatToShow::Bid),
            sub(contract.clone(), RealtimeWhatToShow::Ask),
        ],
    );

    assert_eq!(params.strategy.get_name(), "unknown");
    assert_eq!(params.subscribed_contracts.len(), 2);
    // init_app.rs:206-212 — same Bid/Ask shape as Manual.
    assert_eq!(
        what_label(params.subscribed_contracts[0].what_to_show),
        "bid"
    );
    assert_eq!(
        what_label(params.subscribed_contracts[1].what_to_show),
        "ask"
    );
}

#[test]
fn subscribed_contracts_preserve_order_and_duplicates() {
    let aapl = stock("AAPL", "NASDAQ", "USD");
    let msft = stock("MSFT", "NASDAQ", "USD");

    let params = strategy_parameters(
        StrategyEnum::Manual(Manual::new(lazy_pool())),
        vec![
            sub(aapl.clone(), RealtimeWhatToShow::Trades),
            sub(msft.clone(), RealtimeWhatToShow::Trades),
            sub(aapl.clone(), RealtimeWhatToShow::Trades),
            sub(msft.clone(), RealtimeWhatToShow::Trades),
        ],
    );

    let got: Vec<(String, &'static str)> = params
        .subscribed_contracts
        .iter()
        .map(|s| (s.contract.symbol.to_string(), what_label(s.what_to_show)))
        .collect();
    assert_eq!(
        got,
        vec![
            ("AAPL".to_string(), "trades"),
            ("MSFT".to_string(), "trades"),
            ("AAPL".to_string(), "trades"),
            ("MSFT".to_string(), "trades"),
        ],
        "StrategyParameters stores the Vec verbatim: order and duplicates intact"
    );
    // The duplicate pair is equal under DataSubscription's identity.
    assert_eq!(
        params.subscribed_contracts[0],
        params.subscribed_contracts[2]
    );
    assert_eq!(
        params.subscribed_contracts[1],
        params.subscribed_contracts[3]
    );
}

#[test]
fn data_subscription_identity_matrix() {
    // `what_to_show` distinguishes streams; the contract identity is
    // (primary_exchange trimmed, symbol, currency, security_type) plus the
    // option fields (src/market_data/handler.rs:51-117).
    let base = stock("AAPL", "NASDAQ", "USD");
    let cases: Vec<(&str, DataSubscription, DataSubscription, bool)> = vec![
        (
            "identical",
            sub(base.clone(), RealtimeWhatToShow::Bid),
            sub(base.clone(), RealtimeWhatToShow::Bid),
            true,
        ),
        (
            "different what_to_show",
            sub(base.clone(), RealtimeWhatToShow::Bid),
            sub(base.clone(), RealtimeWhatToShow::Ask),
            false,
        ),
        (
            "primary_exchange whitespace is trimmed",
            sub(stock("AAPL", "NASDAQ", "USD"), RealtimeWhatToShow::Trades),
            sub(
                stock("AAPL", "  NASDAQ ", "USD"),
                RealtimeWhatToShow::Trades,
            ),
            true,
        ),
        (
            "different symbol",
            sub(base.clone(), RealtimeWhatToShow::Trades),
            sub(stock("MSFT", "NASDAQ", "USD"), RealtimeWhatToShow::Trades),
            false,
        ),
        (
            "different currency",
            sub(base.clone(), RealtimeWhatToShow::Trades),
            sub(stock("AAPL", "NASDAQ", "HKD"), RealtimeWhatToShow::Trades),
            false,
        ),
        (
            "different security_type",
            sub(base.clone(), RealtimeWhatToShow::Trades),
            sub(
                stock("AAPL", "NASDAQ", "USD").with_security_type(SecurityType::ForexPair),
                RealtimeWhatToShow::Trades,
            ),
            false,
        ),
        (
            "exchange (routing venue) is not part of identity",
            sub(base.clone(), RealtimeWhatToShow::Trades),
            sub(
                Contract {
                    exchange: Exchange::from("NYSE"),
                    ..base.clone()
                },
                RealtimeWhatToShow::Trades,
            ),
            true,
        ),
        (
            "option: identical",
            sub(
                option("AAPL", "C", "20270115", 150.0, "100"),
                RealtimeWhatToShow::Trades,
            ),
            sub(
                option("AAPL", "C", "20270115", 150.0, "100"),
                RealtimeWhatToShow::Trades,
            ),
            true,
        ),
        (
            "option: different right",
            sub(
                option("AAPL", "C", "20270115", 150.0, "100"),
                RealtimeWhatToShow::Trades,
            ),
            sub(
                option("AAPL", "P", "20270115", 150.0, "100"),
                RealtimeWhatToShow::Trades,
            ),
            false,
        ),
        (
            "option: different expiry",
            sub(
                option("AAPL", "C", "20270115", 150.0, "100"),
                RealtimeWhatToShow::Trades,
            ),
            sub(
                option("AAPL", "C", "20270219", 150.0, "100"),
                RealtimeWhatToShow::Trades,
            ),
            false,
        ),
        (
            "option: different strike",
            sub(
                option("AAPL", "C", "20270115", 150.0, "100"),
                RealtimeWhatToShow::Trades,
            ),
            sub(
                option("AAPL", "C", "20270115", 155.0, "100"),
                RealtimeWhatToShow::Trades,
            ),
            false,
        ),
        (
            "option: different multiplier",
            sub(
                option("AAPL", "C", "20270115", 150.0, "100"),
                RealtimeWhatToShow::Trades,
            ),
            sub(
                option("AAPL", "C", "20270115", 150.0, "10"),
                RealtimeWhatToShow::Trades,
            ),
            false,
        ),
        (
            "stock: option fields are ignored",
            sub(
                Contract {
                    right: "C".to_string(),
                    last_trade_date_or_contract_month: "20270115".to_string(),
                    strike: 150.0,
                    multiplier: "100".to_string(),
                    ..base.clone()
                },
                RealtimeWhatToShow::Trades,
            ),
            sub(base.clone(), RealtimeWhatToShow::Trades),
            true,
        ),
    ];

    for (label, a, b, want_eq) in cases {
        assert_eq!(a == b, want_eq, "equality case: {label}");
        assert_eq!(
            hash_of(&a) == hash_of(&b),
            want_eq,
            "hash must agree with Eq: {label}"
        );
    }
}

#[test]
fn duplicate_subscriptions_collapse_in_a_hash_map() {
    let aapl = stock("AAPL", "NASDAQ", "USD");
    let mut map: HashMap<DataSubscription, usize> = HashMap::new();

    map.insert(sub(aapl.clone(), RealtimeWhatToShow::Trades), 1);
    let replaced = map.insert(sub(aapl.clone(), RealtimeWhatToShow::Trades), 2);
    assert_eq!(replaced, Some(1), "equal key must hit the existing entry");
    assert_eq!(map.len(), 1);

    // A second stream on the same contract is a distinct key.
    map.insert(sub(aapl.clone(), RealtimeWhatToShow::Bid), 3);
    assert_eq!(map.len(), 2);

    // Unknown contract => miss (the caller falls back, e.g. default strategy).
    assert!(!map.contains_key(&sub(
        stock("NVDA", "NASDAQ", "USD"),
        RealtimeWhatToShow::Trades
    )));
}

/// Small extension used only in the matrix above.
trait WithSecurityType {
    fn with_security_type(self, security_type: SecurityType) -> Contract;
}

impl WithSecurityType for Contract {
    fn with_security_type(mut self, security_type: SecurityType) -> Contract {
        self.security_type = security_type;
        self
    }
}
