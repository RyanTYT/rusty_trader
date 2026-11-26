//! Tests for `src/market_data/handler.rs`.
//!
//! Covered here (pure, deterministic — no IB, no network):
//!   - `DataSubscription::new` (src/market_data/handler.rs:43)
//!   - `impl Hash for DataSubscription` (src/market_data/handler.rs:51) and
//!     `impl PartialEq for DataSubscription` (src/market_data/handler.rs:75):
//!     key = trimmed `primary_exchange` + `symbol` + `currency` +
//!     `security_type` (+ option-only `right` / `last_trade_date_or_contract_month`
//!     / `strike` / `multiplier`) + `what_to_show` variant.
//!   - `MarketDataHandler::new` construction defaults
//!     (src/market_data/handler.rs:148), `try_get_price` (:231) and
//!     `get_subsription` (:235) on a fresh handler. The pool is
//!     `PgPool::connect_lazy` pointed at a closed port, so no connection is
//!     ever attempted.
//!
//! NOT covered — need a live `ibapi::Client`, Postgres and/or spawned threads:
//!   - `MarketDataHandler::load_all_subscription_producers`
//!     (src/market_data/handler.rs:171) — `begin_producer_thread_grouped`
//!     immediately upgrades the `Weak<Client>` (src/market_data/producer.rs:92).
//!   - `MarketDataHandler::async_drop` (:124) — needs producers/consumers that
//!     only `load_all_subscription_producers` can create.
//!   - `DbSubscriptionMethod` (:142) — only meaningfully consumed by the above.
//!   - `live_prices` *values* — the cache has no public mutator; only the empty
//!     state is observable through `try_get_price`.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use ibapi::contracts::{Contract, SecurityType};
use ibapi::market_data::realtime::WhatToShow;
use trading_app::market_data::handler::{DataSubscription, MarketDataHandler};

// ──────────────────────────── helpers ────────────────────────────

fn hash_of(s: &DataSubscription) -> u64 {
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

fn stock(symbol: &str, what: WhatToShow) -> DataSubscription {
    DataSubscription::new(
        Contract {
            symbol: symbol.into(),
            security_type: SecurityType::Stock,
            currency: "USD".into(),
            primary_exchange: "NASDAQ".into(),
            exchange: "SMART".into(),
            ..Default::default()
        },
        what,
    )
}

fn option(right: &str, expiry: &str, strike: f64, multiplier: &str, what: WhatToShow) -> DataSubscription {
    DataSubscription::new(
        Contract {
            symbol: "AAPL".into(),
            security_type: SecurityType::Option,
            currency: "USD".into(),
            primary_exchange: "NASDAQ".into(),
            right: right.into(),
            last_trade_date_or_contract_month: expiry.into(),
            strike,
            multiplier: multiplier.into(),
            ..Default::default()
        },
        what,
    )
}

fn lazy_pool() -> sqlx::PgPool {
    // Lazy: the URL is parsed but no socket is opened. Port 1 is closed, so any
    // accidental acquire would fail immediately rather than hang.
    sqlx::PgPool::connect_lazy("postgres://test:test@127.0.0.1:1/test_db")
        .expect("lazy pool from a syntactically valid URL")
}

// ──────────────── DataSubscription::new ────────────────

#[test]
fn new_stores_contract_and_what_to_show() {
    let contract = Contract {
        symbol: "AAPL".into(),
        security_type: SecurityType::Stock,
        currency: "USD".into(),
        primary_exchange: "NASDAQ".into(),
        ..Default::default()
    };
    let sub = DataSubscription::new(contract, WhatToShow::Trades);

    assert_eq!(sub.contract.symbol.as_str(), "AAPL");
    assert_eq!(sub.contract.currency.as_str(), "USD");
    assert_eq!(sub.contract.primary_exchange.as_str(), "NASDAQ");
    assert_eq!(sub.contract.security_type, SecurityType::Stock);
    assert!(matches!(sub.what_to_show, WhatToShow::Trades));
}

// ──────────────── Hash / Eq: equivalence ────────────────

#[test]
fn identical_subscriptions_are_equal_and_hash_equal() {
    for what in [WhatToShow::Bid, WhatToShow::Ask, WhatToShow::Trades, WhatToShow::MidPoint] {
        let a = stock("AAPL", match what {
            WhatToShow::Bid => WhatToShow::Bid,
            WhatToShow::Ask => WhatToShow::Ask,
            WhatToShow::Trades => WhatToShow::Trades,
            WhatToShow::MidPoint => WhatToShow::MidPoint,
        });
        let b = stock("AAPL", match what {
            WhatToShow::Bid => WhatToShow::Bid,
            WhatToShow::Ask => WhatToShow::Ask,
            WhatToShow::Trades => WhatToShow::Trades,
            WhatToShow::MidPoint => WhatToShow::MidPoint,
        });
        assert_eq!(a, b, "same fields must compare equal");
        assert_eq!(hash_of(&a), hash_of(&b), "equal => hash equal");
        assert_eq!(a == b, b == a, "eq must be symmetric");
    }
}

#[test]
fn primary_exchange_is_trimmed_in_both_hash_and_eq() {
    let base = stock("AAPL", WhatToShow::Trades);

    for padded in ["  NASDAQ", "NASDAQ  ", "  NASDAQ  ", "\tNASDAQ\n"] {
        let mut c = base.contract.clone();
        c.primary_exchange = padded.into();
        let other = DataSubscription::new(c, WhatToShow::Trades);

        assert_eq!(base, other, "primary_exchange={padded:?} must trim to NASDAQ");
        assert_eq!(hash_of(&base), hash_of(&other), "hash must trim too");
    }
}

#[test]
fn equality_ignores_non_primary_exchange_field() {
    // Only `primary_exchange` is part of the key; `exchange` is not.
    let a = stock("AAPL", WhatToShow::Trades);
    let mut c = a.contract.clone();
    c.exchange = "ARCA".into();
    let b = DataSubscription::new(c, WhatToShow::Trades);

    assert_eq!(a, b);
    assert_eq!(hash_of(&a), hash_of(&b));
}

#[test]
fn non_option_ignores_option_only_fields() {
    // security_type != Option => right/expiry/strike/multiplier are not keyed.
    let a = stock("AAPL", WhatToShow::Trades);
    let mut c = a.contract.clone();
    c.right = "P".into();
    c.last_trade_date_or_contract_month = "20261218".into();
    c.strike = 200.0;
    c.multiplier = "100".into();
    let b = DataSubscription::new(c, WhatToShow::Trades);

    assert_eq!(a, b);
    assert_eq!(hash_of(&a), hash_of(&b));
}

// ──────────────── Hash / Eq: what_to_show differentiates ────────────────

#[test]
fn all_four_what_to_show_variants_are_pairwise_distinct() {
    let variants = [
        WhatToShow::Bid,
        WhatToShow::Ask,
        WhatToShow::Trades,
        WhatToShow::MidPoint,
    ];
    for (i, a) in variants.iter().enumerate() {
        for (j, b) in variants.iter().enumerate() {
            let sa = stock("AAPL", match a {
                WhatToShow::Bid => WhatToShow::Bid,
                WhatToShow::Ask => WhatToShow::Ask,
                WhatToShow::Trades => WhatToShow::Trades,
                WhatToShow::MidPoint => WhatToShow::MidPoint,
            });
            let sb = stock("AAPL", match b {
                WhatToShow::Bid => WhatToShow::Bid,
                WhatToShow::Ask => WhatToShow::Ask,
                WhatToShow::Trades => WhatToShow::Trades,
                WhatToShow::MidPoint => WhatToShow::MidPoint,
            });
            if i == j {
                assert_eq!(sa, sb);
            } else {
                assert_ne!(sa, sb, "variant {i} vs {j} must differ");
                assert_ne!(hash_of(&sa), hash_of(&sb), "variant {i} vs {j} hash must differ");
            }
        }
    }
}

// ──────────────── Hash / Eq: contract identity fields ────────────────

#[test]
fn different_symbol_currency_or_security_type_is_not_equal() {
    let base = stock("AAPL", WhatToShow::Trades);

    let cases: Vec<(&str, DataSubscription)> = vec![
        ("symbol", stock("MSFT", WhatToShow::Trades)),
        ("currency", {
            let mut c = base.contract.clone();
            c.currency = "SGD".into();
            DataSubscription::new(c, WhatToShow::Trades)
        }),
        ("security_type", {
            let mut c = base.contract.clone();
            c.security_type = SecurityType::CFD;
            DataSubscription::new(c, WhatToShow::Trades)
        }),
    ];

    for (label, other) in cases {
        assert_ne!(base, other, "{label} must differentiate");
        assert_ne!(hash_of(&base), hash_of(&other), "{label} must change the hash");
    }
}

// ──────────────── Hash / Eq: option-only fields ────────────────

#[test]
fn option_identity_covers_right_expiry_strike_multiplier() {
    let base = option("C", "20261218", 150.0, "100", WhatToShow::Trades);
    let same = option("C", "20261218", 150.0, "100", WhatToShow::Trades);
    assert_eq!(base, same);
    assert_eq!(hash_of(&base), hash_of(&same));

    let variants = vec![
        ("right", option("P", "20261218", 150.0, "100", WhatToShow::Trades)),
        ("expiry", option("C", "20270115", 150.0, "100", WhatToShow::Trades)),
        ("strike", option("C", "20261218", 155.0, "100", WhatToShow::Trades)),
        ("multiplier", option("C", "20261218", 150.0, "10", WhatToShow::Trades)),
    ];

    for (label, other) in variants {
        assert_ne!(base, other, "option {label} must differentiate");
        assert_ne!(hash_of(&base), hash_of(&other), "option {label} must change the hash");
    }
}

#[test]
fn option_versus_stock_with_same_symbol_is_not_equal() {
    let o = option("C", "20261218", 150.0, "100", WhatToShow::Trades);
    let s = stock("AAPL", WhatToShow::Trades);
    assert_ne!(o, s);
    assert_ne!(s, o);
    assert_ne!(hash_of(&o), hash_of(&s));
}

// ──────────────── Hash / Eq: usable as a HashMap key ────────────────

#[test]
fn hashmap_lookup_hits_with_differently_spaced_primary_exchange() {
    let mut map: HashMap<DataSubscription, &str> = HashMap::new();
    map.insert(stock("AAPL", WhatToShow::Trades), "aapl-trades");
    map.insert(stock("AAPL", WhatToShow::Bid), "aapl-bid");
    map.insert(stock("MSFT", WhatToShow::Trades), "msft-trades");
    assert_eq!(map.len(), 3, "distinct keys must not collide");

    let mut c = stock("AAPL", WhatToShow::Trades).contract;
    c.primary_exchange = "  NASDAQ ".into();
    let lookup = DataSubscription::new(c, WhatToShow::Trades);

    assert_eq!(map.get(&lookup), Some(&"aapl-trades"));
    assert_eq!(map.get(&stock("AAPL", WhatToShow::Bid)), Some(&"aapl-bid"));
    assert_eq!(map.get(&stock("MSFT", WhatToShow::Trades)), Some(&"msft-trades"));
    assert_eq!(map.get(&stock("TSLA", WhatToShow::Trades)), None);
}

// ──────────────── MarketDataHandler construction defaults ────────────────

#[tokio::test]
async fn fresh_handler_has_empty_live_price_cache() {
    let handler = MarketDataHandler::new(lazy_pool());

    assert_eq!(handler.try_get_price(0), None);
    assert_eq!(handler.try_get_price(-1), None);
    assert_eq!(handler.try_get_price(265598), None);
    assert_eq!(handler.try_get_price(i32::MAX), None);
}

#[tokio::test]
async fn fresh_handler_has_no_subscriptions() {
    let handler = MarketDataHandler::new(lazy_pool());

    assert!(handler.get_subsription(&stock("AAPL", WhatToShow::Trades)).is_none());
    assert!(handler.get_subsription(&option("C", "20261218", 150.0, "100", WhatToShow::Bid)).is_none());
}

#[tokio::test]
async fn fresh_handlers_are_independent() {
    let a = MarketDataHandler::new(lazy_pool());
    let b = MarketDataHandler::new(lazy_pool());

    assert!(a.get_subsription(&stock("AAPL", WhatToShow::Trades)).is_none());
    assert!(b.get_subsription(&stock("AAPL", WhatToShow::Trades)).is_none());
    assert_eq!(a.try_get_price(1), None);
    assert_eq!(b.try_get_price(1), None);
}
