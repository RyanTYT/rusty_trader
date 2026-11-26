//! Tests for `src/helpers/sentinel.rs`.
//!
//! `Sentinel` provides a magic value per type:
//!   * `Contract` — `symbol == "__SENTINEL__"` (src/helpers/sentinel.rs:11-22);
//!   * `HistoricalDataFullKeys` — `time == UNIX_EPOCH` (src/helpers/sentinel.rs:24-43).
//!
//! Both predicates are single-field checks, so these tests pin exactly which
//! field decides identity and which fields are ignored.

use chrono::{DateTime, TimeDelta, Utc};
use ibapi::contracts::{Contract, SecurityType};
use ibapi::market_data::realtime::WhatToShow;
use trading_app::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use trading_app::helpers::sentinel::Sentinel;

fn epoch() -> DateTime<Utc> {
    DateTime::<Utc>::UNIX_EPOCH
}

fn bar(contract: &Contract, what_to_show: &WhatToShow, time: DateTime<Utc>) -> HistoricalDataFullKeys {
    HistoricalDataFullKeys::from_data(contract, what_to_show, time, 1.0, 2.0, 0.5, 1.5, 100.0)
}

fn stock(symbol: &str, exchange: &str, primary_exchange: &str, currency: &str) -> Contract {
    Contract {
        symbol: symbol.into(),
        security_type: SecurityType::Stock,
        exchange: exchange.into(),
        primary_exchange: primary_exchange.into(),
        currency: currency.into(),
        ..Default::default()
    }
}

// ───────────────────────────── Contract ─────────────────────────────

#[test]
fn contract_sentinel_is_marked_only_by_its_symbol() {
    let sentinel = Contract::sentinel();
    assert_eq!(sentinel.symbol.as_str(), "__SENTINEL__");
    // Everything else is `Contract::default()`.
    assert_eq!(sentinel.exchange.as_str(), "SMART");
    assert_eq!(sentinel.primary_exchange.as_str(), "");
    assert_eq!(sentinel.currency.as_str(), "USD");
    assert_eq!(sentinel.security_type, SecurityType::Stock);
    assert_eq!(sentinel.strike, 0.0);
    assert_eq!(sentinel.right, "");
    assert_eq!(sentinel.multiplier, "");
    assert!(Contract::is_sentinel(&sentinel));

    // A default contract is not a sentinel.
    assert!(!Contract::is_sentinel(&Contract::default()));
    assert!(!Contract::is_sentinel(&stock(
        "AAPL", "SMART", "NASDAQ", "USD"
    )));
}

/// `is_sentinel` compares the symbol byte-for-byte: no trimming, no
/// case-folding, no prefix matching, and every other field is ignored.
#[test]
fn contract_is_sentinel_is_an_exact_symbol_comparison() {
    let sentinel = Contract {
        symbol: "__SENTINEL__".into(),
        exchange: "SMART".into(),
        primary_exchange: "NASDAQ".into(),
        currency: "USD".into(),
        ..Default::default()
    };
    assert!(Contract::is_sentinel(&sentinel));

    let near_misses = [
        "__sentinel__",
        "__SENTINEL__ ",
        " __SENTINEL__",
        "__SENTINEL__X",
        "X__SENTINEL__",
        "__SENTINEL",
        "SENTINEL",
        "",
    ];
    for symbol in near_misses {
        let contract = Contract {
            symbol: symbol.into(),
            ..Default::default()
        };
        assert!(
            !Contract::is_sentinel(&contract),
            "symbol {symbol:?} must not be a sentinel"
        );
    }
}

// ────────────────── HistoricalDataFullKeys ──────────────────

#[test]
fn historical_sentinel_is_a_stock_bar_at_unix_epoch() {
    let sentinel = HistoricalDataFullKeys::sentinel();

    assert!(HistoricalDataFullKeys::is_sentinel(&sentinel));
    assert_eq!(sentinel.get_time(), epoch());

    let HistoricalDataFullKeys::Stock(key) = sentinel else {
        panic!("Contract::sentinel() is a stock, so the bar must be Stock");
    };
    assert_eq!(key.stock, "__SENTINEL__", "local symbol comes from get_local_symbol");
    assert_eq!(key.primary_exchange, "");
    assert_eq!(key.currency, "USD");
    assert_eq!(key.time, epoch());
}

/// Identity is `time == UNIX_EPOCH` only: the symbol / venue / prices do not
/// participate, and all three key variants can carry the sentinel.
#[test]
fn historical_is_sentinel_is_an_exact_epoch_comparison() {
    let stock_contract = stock("AAPL", "SMART", "NASDAQ", "USD");

    // A real bar at a non-epoch time is not a sentinel.
    let real = bar(&stock_contract, &WhatToShow::Trades, epoch() + TimeDelta::seconds(1));
    assert!(!HistoricalDataFullKeys::is_sentinel(&real));

    // The sentinel predicate ignores the symbol and the prices.
    let other_symbol_at_epoch = bar(
        &stock("MSFT", "SMART", "NASDAQ", "USD"),
        &WhatToShow::Trades,
        epoch(),
    );
    assert!(HistoricalDataFullKeys::is_sentinel(&other_symbol_at_epoch));

    // One nanosecond after the epoch is already not a sentinel.
    let one_ns_after = bar(
        &stock_contract,
        &WhatToShow::Trades,
        epoch() + TimeDelta::nanoseconds(1),
    );
    assert!(!HistoricalDataFullKeys::is_sentinel(&one_ns_after));

    // Option variant: any right/strike/expiry at the epoch is a sentinel.
    let option_contract = Contract::option("AAPL", "20261218", 200.0, "C");
    let option_bar = bar(&option_contract, &WhatToShow::Trades, epoch());
    let HistoricalDataFullKeys::Options(key) = &option_bar else {
        panic!("option contract must build HistoricalDataFullKeys::Options");
    };
    assert_eq!(key.stock, "AAPL");
    assert_eq!(key.expiry, "20261218");
    assert_eq!(key.strike, 200.0);
    assert!(HistoricalDataFullKeys::is_sentinel(&option_bar));

    // Forex variant: `from_data` requires Bid or Ask (Trades panics), and the
    // epoch still decides sentinel identity.
    let forex_contract = Contract {
        symbol: "EUR".into(),
        currency: "USD".into(),
        security_type: SecurityType::ForexPair,
        ..Default::default()
    };
    let forex_bar = bar(&forex_contract, &WhatToShow::Bid, epoch());
    assert!(matches!(&forex_bar, HistoricalDataFullKeys::Forex(_)));
    assert!(HistoricalDataFullKeys::is_sentinel(&forex_bar));

    let forex_bar_after = bar(
        &forex_contract,
        &WhatToShow::Ask,
        DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp"),
    );
    assert!(!HistoricalDataFullKeys::is_sentinel(&forex_bar_after));
}
