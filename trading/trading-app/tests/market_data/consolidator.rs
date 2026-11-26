//! Tests for `src/market_data/consolidator.rs`, plus the contract-identity
//! helpers (`src/helpers/contract.rs`) and memoizer (`src/market_data/memoise.rs`)
//! that the consolidator keys its caches with.
//!
//! Covered here (pure, deterministic — no IB, no DB, no network):
//!   - `MemoisedConsolidatorFns` (src/market_data/consolidator.rs:42)
//!   - `last_bar_available` (src/market_data/consolidator.rs:395)
//!   - `last_bar_available_time_forex` (src/market_data/consolidator.rs:382)
//!   - `is_fx_trading_datetime` (src/market_data/consolidator.rs:438)
//!   - `fx_trading_day_start` (src/market_data/consolidator.rs:524)
//!   - `HashContract` / `HashContractRef` (src/helpers/contract.rs:24/:87)
//!   - `build_contract_from_stock` (src/helpers/contract.rs:151)
//!   - `get_local_symbol` (src/helpers/contract.rs:125)
//!   - `get_contract_from` (src/helpers/contract.rs:200)
//!   - `Memoized` / `AnyMemoized` (src/market_data/memoise.rs:17/:60)
//!
//! NOT covered — require a live `ibapi::Client` and/or Postgres:
//!   - `Consolidator::new` (src/market_data/consolidator.rs:88) — needs
//!     `Arc<Client>`; `new_for_backtest` (:170) is `backtest`-feature only.
//!   - `Consolidator::refresh_if_stale` (:190) and
//!     `update_at_least_n_days_data` (:260) — Client + DB.
//!   - `validate_contract` (src/market_data/traits/contracts.rs:45) /
//!     `_get_contract_details` (:59) — contract-details round trip; the
//!     `memoised_contract_details` cache is only reachable through them.
//!   - `_get_current_price` / `_get_current_price_inner`
//!     (src/market_data/traits/current_price.rs:488/:513).
//!   - `_get_strategy_sgd_value` (src/market_data/traits/strategy_value.rs:52),
//!     `_get_positions_value` (:79, private) and `get_fund_value` (:363) — DB +
//!     Client/memoiser.
//!   - `AccountTracker` (src/market_data/traits/available_funds.rs) — the whole
//!     module is not compiled (`// pub mod available_funds;` at
//!     src/market_data/traits/mod.rs:1) and `Consolidator` has no
//!     `available_funds` field.
//!
//! Previously a known bug, now fixed: the session-close cap in
//! `last_bar_available` used a strict `>` (src/market_data/consolidator.rs:426),
//! so a bar timestamped exactly at the close was returned. It is now `>=` and
//! guarded by `last_bar_available_never_returns_bar_at_or_after_close`.
//! The function should not be used going forward.

use std::{
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Timelike, Utc};
use chrono_tz::{America::New_York, Tz, UTC};
use ibapi::contracts::{Contract, Currency, Exchange, SecurityType, Symbol};

use trading_app::{
    database::models::{
        AssetType, CurrentOptionPositionsFullKeys, CurrentStockPositionsFullKeys, OptionType,
    },
    database::models_crud::current_positions::current_positions::CurrentPositionsFullKeys,
    market_data::consolidator::MemoisedConsolidatorFns,
    test_internals::{
        AnyMemoized, HashContract, LocalContractTypes, Memoized, build_contract_from_stock,
        fx_trading_day_start, get_contract_from, get_local_symbol, is_fx_trading_datetime,
        last_bar_available, last_bar_available_time_forex,
    },
};

// ──────────────────────────── helpers ────────────────────────────

fn ny(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Tz> {
    New_York
        .with_ymd_and_hms(year, month, day, hour, minute, 0)
        .single()
        .expect("valid New York local time")
}

fn rfc(dt: DateTime<Tz>) -> String {
    dt.to_rfc3339()
}

fn rfc_opt(dt: Option<DateTime<Tz>>) -> Option<String> {
    dt.map(rfc)
}

fn epoch() -> DateTime<Utc> {
    DateTime::from_timestamp(0, 0).expect("epoch")
}

fn stock(symbol: &str, exchange: &str, primary_exchange: &str, currency: &str) -> Contract {
    Contract {
        symbol: Symbol::from(symbol),
        security_type: SecurityType::Stock,
        exchange: Exchange::from(exchange),
        primary_exchange: Exchange::from(primary_exchange),
        currency: Currency::from(currency),
        ..Default::default()
    }
}

fn forex(base: &str, quote: &str) -> Contract {
    Contract {
        symbol: Symbol::from(base),
        security_type: SecurityType::ForexPair,
        exchange: Exchange::from("IDEALPRO"),
        currency: Currency::from(quote),
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

fn hc(contract: Contract) -> HashContract<'static> {
    HashContract::new(contract)
}

fn hash_of<T: Hash + ?Sized>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn panic_message<F: FnOnce()>(f: F) -> String {
    let payload = catch_unwind(AssertUnwindSafe(f)).expect_err("expected a panic");
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        panic!("panic payload was not a string");
    }
}

// ─────────────── MemoisedConsolidatorFns (:42) ───────────────

#[test]
fn memoised_consolidator_fns_is_a_distinct_hash_map_key() {
    let get_price = MemoisedConsolidatorFns::GetPrice;
    let get_strategy_value = MemoisedConsolidatorFns::GetStrategyValue;
    let get_available_funds = MemoisedConsolidatorFns::GetAvailableFunds;

    assert_eq!(get_price, MemoisedConsolidatorFns::GetPrice);
    assert_eq!(get_price.clone(), get_price);
    assert_ne!(get_price, get_strategy_value);
    assert_ne!(get_strategy_value, get_available_funds);
    assert_ne!(get_price, get_available_funds);

    assert_eq!(format!("{:?}", get_price), "GetPrice");
    assert_eq!(format!("{:?}", get_strategy_value), "GetStrategyValue");
    assert_eq!(format!("{:?}", get_available_funds), "GetAvailableFunds");

    let mut memoisers: HashMap<MemoisedConsolidatorFns, &'static str> = HashMap::new();
    memoisers.insert(get_price, "price");
    memoisers.insert(get_strategy_value, "strategy");
    memoisers.insert(get_available_funds, "funds");

    assert_eq!(memoisers.len(), 3);
    assert_eq!(
        memoisers.get(&MemoisedConsolidatorFns::GetPrice),
        Some(&"price")
    );
    assert_eq!(
        memoisers.get(&MemoisedConsolidatorFns::GetStrategyValue),
        Some(&"strategy")
    );
    assert_eq!(
        memoisers.get(&MemoisedConsolidatorFns::GetAvailableFunds),
        Some(&"funds")
    );

    let distinct: HashSet<MemoisedConsolidatorFns> = [
        MemoisedConsolidatorFns::GetPrice,
        MemoisedConsolidatorFns::GetPrice,
        MemoisedConsolidatorFns::GetStrategyValue,
        MemoisedConsolidatorFns::GetAvailableFunds,
        MemoisedConsolidatorFns::GetAvailableFunds,
    ]
    .into_iter()
    .collect();
    assert_eq!(distinct.len(), 3);
}

// ──────────── HashContract identity (src/helpers/contract.rs:24) ────────────

/// The stock key is (trimmed primary_exchange, symbol, currency,
/// security_type). Everything else — exchange, contract_id, trading_class,
/// local_symbol, description — is deliberately not part of the key.
#[test]
fn hash_contract_stock_identity_table() {
    let cases: Vec<(&str, Contract, Contract, bool)> = vec![
        (
            "identical",
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            true,
        ),
        (
            "exchange is not part of the key",
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            stock("AAPL", "NYSE", "NASDAQ", "USD"),
            true,
        ),
        (
            "primary_exchange is trimmed",
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            stock("AAPL", "SMART", "  NASDAQ  ", "USD"),
            true,
        ),
        (
            "primary_exchange is case-sensitive",
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            stock("AAPL", "SMART", "nasdaq", "USD"),
            false,
        ),
        (
            "symbol differs",
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            stock("AAPL ", "SMART", "NASDAQ", "USD"),
            false,
        ),
        (
            "currency differs",
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            stock("AAPL", "SMART", "NASDAQ", "EUR"),
            false,
        ),
        (
            "security_type differs",
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            Contract {
                security_type: SecurityType::Future,
                ..stock("AAPL", "SMART", "NASDAQ", "USD")
            },
            false,
        ),
        (
            "contract_id is not part of the key",
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            Contract {
                contract_id: 265_598,
                ..stock("AAPL", "SMART", "NASDAQ", "USD")
            },
            true,
        ),
        (
            "trading_class/local_symbol/description are not part of the key",
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            Contract {
                trading_class: "NMS".to_string(),
                local_symbol: "AAPL".to_string(),
                description: "APPLE INC".to_string(),
                ..stock("AAPL", "SMART", "NASDAQ", "USD")
            },
            true,
        ),
    ];

    for (label, a, b, expected_eq) in cases {
        let ha = hc(a);
        let hb = hc(b);

        assert_eq!(ha == hb, expected_eq, "HashContract eq: {label}");
        assert_eq!(hb == ha, expected_eq, "HashContract eq symmetric: {label}");
        if expected_eq {
            assert_eq!(
                hash_of(&ha),
                hash_of(&hb),
                "equal HashContracts must hash equally: {label}"
            );
        }
    }
}

/// Options add right, expiry/contract-month, strike (exact, via OrderedFloat)
/// and multiplier to the key; exchange stays out of it.
#[test]
fn hash_contract_option_identity_table() {
    let base = || option("AAPL", "C", "20261218", 200.0, "100");

    let cases: Vec<(&str, Contract, Contract, bool)> = vec![
        ("identical", base(), base(), true),
        (
            "right differs",
            base(),
            option("AAPL", "P", "20261218", 200.0, "100"),
            false,
        ),
        (
            "expiry differs",
            base(),
            option("AAPL", "C", "20270115", 200.0, "100"),
            false,
        ),
        (
            "strike differs",
            base(),
            option("AAPL", "C", "20261218", 200.5, "100"),
            false,
        ),
        (
            "multiplier differs",
            base(),
            option("AAPL", "C", "20261218", 200.0, "10"),
            false,
        ),
        (
            "exchange is not part of the key",
            base(),
            Contract {
                exchange: Exchange::from("CBOE"),
                ..base()
            },
            true,
        ),
        (
            "primary_exchange is trimmed",
            base(),
            option("AAPL", "C", "20261218", 200.0, "100").tap_primary("  NASDAQ "),
            true,
        ),
        (
            "stock vs option",
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            base(),
            false,
        ),
        (
            "options with a different underlying",
            base(),
            option("MSFT", "C", "20261218", 200.0, "100"),
            false,
        ),
    ];

    for (label, a, b, expected_eq) in cases {
        let ha = hc(a);
        let hb = hc(b);

        assert_eq!(ha == hb, expected_eq, "HashContract eq: {label}");
        assert_eq!(hb == ha, expected_eq, "HashContract eq symmetric: {label}");
        if expected_eq {
            assert_eq!(
                hash_of(&ha),
                hash_of(&hb),
                "equal HashContracts must hash equally: {label}"
            );
        }
    }
}

/// Small extension trait so the table above can adjust primary_exchange
/// without repeating the whole literal.
trait TapPrimary {
    fn tap_primary(self, primary: &str) -> Contract;
}

impl TapPrimary for Contract {
    fn tap_primary(mut self, primary: &str) -> Contract {
        self.primary_exchange = Exchange::from(primary);
        self
    }
}

#[test]
fn hash_contract_forex_identity_table() {
    let eur_usd = || forex("EUR", "USD");

    let cases: Vec<(&str, Contract, Contract, bool)> = vec![
        ("identical", eur_usd(), eur_usd(), true),
        (
            "exchange is not part of the key",
            eur_usd(),
            Contract {
                exchange: Exchange::from("SMART"),
                ..eur_usd()
            },
            true,
        ),
        (
            "quote currency differs",
            eur_usd(),
            forex("EUR", "SGD"),
            false,
        ),
        (
            "base currency differs",
            eur_usd(),
            forex("GBP", "USD"),
            false,
        ),
        (
            "forex vs stock with the same symbol/currency",
            eur_usd(),
            stock("EUR", "IDEALPRO", "", "USD"),
            false,
        ),
        (
            "builder output matches the manual forex contract",
            build_contract_from_stock(&"FX:EUR/USD".to_string(), &String::new(), &String::new()),
            eur_usd(),
            true,
        ),
        (
            "CASH:xxx matches the strategy-value USD/SGD FX contract",
            build_contract_from_stock(&"CASH:USD".to_string(), &String::new(), &String::new()),
            Contract {
                symbol: Symbol::from("USD"),
                security_type: SecurityType::ForexPair,
                exchange: Exchange::from("IDEALPRO"),
                currency: Currency::from("SGD"),
                ..Default::default()
            },
            true,
        ),
    ];

    for (label, a, b, expected_eq) in cases {
        let ha = hc(a);
        let hb = hc(b);

        assert_eq!(ha == hb, expected_eq, "HashContract eq: {label}");
        assert_eq!(hb == ha, expected_eq, "HashContract eq symmetric: {label}");
        if expected_eq {
            assert_eq!(
                hash_of(&ha),
                hash_of(&hb),
                "equal HashContracts must hash equally: {label}"
            );
        }
    }
}

#[test]
fn hash_contract_float_edge_cases() {
    let base = || option("AAPL", "C", "20261218", 200.0, "100");

    // OrderedFloat treats all NaNs as equal and hashes them canonically.
    let nan_a = hc(Contract {
        strike: f64::NAN,
        ..base()
    });
    let nan_b = hc(Contract {
        strike: -f64::NAN,
        ..base()
    });
    assert_eq!(nan_a, nan_b);
    assert_eq!(hash_of(&nan_a), hash_of(&nan_b));
    assert_ne!(
        nan_a,
        hc(Contract {
            strike: 200.0,
            ..base()
        })
    );

    // ... and +0.0 == -0.0 with a canonical hash, so the cache key is stable.
    let zero = hc(Contract {
        strike: 0.0,
        ..base()
    });
    let neg_zero = hc(Contract {
        strike: -0.0,
        ..base()
    });
    assert_eq!(zero, neg_zero);
    assert_eq!(hash_of(&zero), hash_of(&neg_zero));

    // Strike comparison is exact — no epsilon tolerance — so a float that only
    // differs by rounding is a different cache key.
    assert_ne!(
        hc(Contract {
            strike: 0.1 + 0.2,
            ..base()
        }),
        hc(Contract {
            strike: 0.3,
            ..base()
        })
    );
}

#[test]
fn hash_contract_map_lookup_ignores_non_key_fields() {
    let key = hc(stock("AAPL", "SMART", "NASDAQ", "USD"));
    let mut cache: HashMap<HashContract<'static>, f64> = HashMap::new();
    cache.insert(key, 123.5);

    // Different exchange + padded primary_exchange still hits.
    assert_eq!(
        cache.get(&hc(stock("AAPL", "NYSE", "  NASDAQ ", "USD"))),
        Some(&123.5)
    );
    // Same symbol, but an option contract, does not.
    assert_eq!(
        cache.get(&hc(option("AAPL", "C", "20261218", 200.0, "100"))),
        None
    );
    assert_eq!(cache.len(), 1);

    // Re-inserting an equivalent key overwrites in place (no growth).
    cache.insert(hc(stock("AAPL", "ARCA", "NASDAQ", "USD")), 456.0);
    assert_eq!(cache.len(), 1);
    assert_eq!(
        cache.get(&hc(stock("AAPL", "IEX", "NASDAQ", "USD"))),
        Some(&456.0)
    );
}

#[test]
fn hash_contract_set_dedupes_equivalent_contracts() {
    let mut set: HashSet<HashContract<'static>> = HashSet::new();
    assert!(set.insert(hc(stock("AAPL", "SMART", "NASDAQ", "USD"))));
    assert!(!set.insert(hc(stock("AAPL", "NYSE", " NASDAQ", "USD"))));
    assert!(!set.insert(hc(stock("AAPL", "ARCA", "NASDAQ", "USD"))));
    assert_eq!(set.len(), 1);

    // Options with the same key fields but different venues/points of origin
    // are one entry; a different strike is a second entry.
    assert!(set.insert(hc(option("AAPL", "C", "20261218", 200.0, "100"))));
    assert!(!set.insert(hc(Contract {
        exchange: Exchange::from("CBOE"),
        ..option("AAPL", "C", "20261218", 200.0, "100")
    })));
    assert!(set.insert(hc(option("AAPL", "C", "20261218", 205.0, "100"))));
    assert_eq!(set.len(), 3);
}

// `HashContractRef` was removed when `HashContract` moved to `Cow`; owned and
// borrowed keys now go through `HashContract::new` / `HashContract::borrowed`.

// ─────────────── last_bar_available (:395) ───────────────

/// Stock/future sessions: 09:30 open, 16:30 close, 5-minute bars. The last
/// completed bar is the 5-minute grid point strictly after the open threshold.
#[test]
fn last_bar_available_stock_grid_table() {
    let cases: &[(&str, u32, u32, Option<&str>)] = &[
        ("08:59 before open", 8, 59, None),
        ("00:01 before open", 0, 1, None),
        ("09:35 exactly on the open bar boundary", 9, 35, None),
        ("09:36", 9, 36, None),
        ("09:39", 9, 39, None),
        ("09:40 first bar past the open", 9, 40, Some("09:35")),
        ("09:41", 9, 41, Some("09:35")),
        ("09:44", 9, 44, Some("09:35")),
        ("09:45", 9, 45, Some("09:40")),
        ("10:00", 10, 0, Some("09:55")),
        ("10:03", 10, 3, Some("09:55")),
        ("10:05", 10, 5, Some("10:00")),
        ("12:34", 12, 34, Some("12:25")),
        ("16:00", 16, 0, Some("15:55")),
        ("16:31 close cap active", 16, 31, Some("16:25")),
        ("16:35 close cap active", 16, 35, Some("16:25")),
        ("16:40 close cap active", 16, 40, Some("16:25")),
        ("23:59", 23, 59, Some("16:25")),
    ];

    for (label, hour, minute, expected) in cases {
        let got = last_bar_available(ny(2026, 11, 26, *hour, *minute), &AssetType::Stock);
        assert_eq!(
            rfc_opt(got),
            expected.map(|t| format!("2026-11-26T{t}:00-05:00")),
            "stock at {label}"
        );
    }
}

#[test]
fn last_bar_available_option_session_table() {
    let cases: &[(&str, u32, u32, Option<&str>)] = &[
        ("08:59 before open", 8, 59, None),
        ("00:30 before open", 0, 30, None),
        ("09:05 exactly on the open bar boundary", 9, 5, None),
        ("09:06", 9, 6, None),
        ("09:11 first bar past the open", 9, 11, Some("09:05")),
        ("09:15", 9, 15, Some("09:10")),
        ("15:59", 15, 59, Some("15:50")),
        ("16:04 close cap active", 16, 4, Some("15:55")),
        ("16:20 close cap active", 16, 20, Some("15:55")),
    ];

    for (label, hour, minute, expected) in cases {
        let got = last_bar_available(ny(2026, 11, 26, *hour, *minute), &AssetType::Option);
        assert_eq!(
            rfc_opt(got),
            expected.map(|t| format!("2026-11-26T{t}:00-05:00")),
            "option at {label}"
        );
    }
}

#[test]
fn last_bar_available_future_uses_the_stock_session() {
    let cases: &[(u32, u32, Option<&str>)] = &[
        (9, 36, None),
        (9, 40, Some("09:35")),
        (10, 3, Some("09:55")),
        (16, 31, Some("16:25")),
        (23, 59, Some("16:25")),
    ];

    for (hour, minute, expected) in cases {
        let now = ny(2026, 11, 26, *hour, *minute);
        let future = last_bar_available(now, &AssetType::Future);
        let stock = last_bar_available(now, &AssetType::Stock);
        assert_eq!(future, stock, "future must mirror stock at {hour:02}:{minute:02}");
        assert_eq!(
            rfc_opt(future),
            expected.map(|t| format!("2026-11-26T{t}:00-05:00")),
            "future at {hour:02}:{minute:02}"
        );
    }
}

#[test]
fn last_bar_available_forex_and_cfd_delegate_to_fx_walk_back() {
    let instants = [
        ny(2026, 11, 27, 17, 0),  // Friday close
        ny(2026, 11, 28, 12, 0),  // Saturday
        ny(2026, 11, 29, 16, 59), // Sunday, pre-open
        ny(2026, 11, 29, 17, 0),  // Sunday open
        ny(2026, 11, 30, 0, 0),   // Monday
        ny(2026, 12, 25, 12, 0),  // Christmas (FX holiday)
    ];

    for now in instants {
        let expected = rfc(last_bar_available_time_forex(now));
        assert_eq!(
            rfc_opt(last_bar_available(now, &AssetType::ForexPair)),
            Some(expected.clone()),
            "ForexPair at {now}"
        );
        assert_eq!(
            rfc_opt(last_bar_available(now, &AssetType::CFD)),
            Some(expected),
            "CFD at {now}"
        );
    }
}

#[test]
fn last_bar_available_panics_for_unsupported_asset_types() {
    const MESSAGE: &str = "Should not be using this function for AssetType not stock or option";

    for asset_type in [AssetType::Unknown, AssetType::CASH] {
        let message = panic_message(|| {
            let _ = last_bar_available(ny(2026, 11, 26, 10, 3), &asset_type);
        });
        assert_eq!(message, MESSAGE, "asset_type={asset_type:?}");
    }
}

#[test]
fn last_bar_available_uses_new_york_offsets_across_dst() {
    let cases: &[(i32, u32, u32, u32, u32, &str)] = &[
        // EST (UTC-5)
        (2026, 3, 6, 10, 3, "2026-03-06T09:55:00-05:00"),
        // EDT (UTC-4), the Monday after the spring-forward
        (2026, 3, 9, 10, 3, "2026-03-09T09:55:00-04:00"),
        // EDT, the Friday before the fall-back
        (2026, 10, 30, 10, 3, "2026-10-30T09:55:00-04:00"),
        // EST, the Monday after the fall-back
        (2026, 11, 2, 10, 3, "2026-11-02T09:55:00-05:00"),
    ];

    for (year, month, day, hour, minute, expected) in cases {
        let got = last_bar_available(ny(*year, *month, *day, *hour, *minute), &AssetType::Stock);
        assert_eq!(
            rfc_opt(got),
            Some((*expected).to_string()),
            "{year}-{month:02}-{day:02} at {hour:02}:{minute:02}"
        );
    }

    // Options share the same DST handling.
    let got = last_bar_available(ny(2026, 11, 2, 15, 59), &AssetType::Option);
    assert_eq!(rfc_opt(got), Some("2026-11-02T15:50:00-05:00".to_string()));
}

/// Regression guard for the session-close cap at
/// src/market_data/consolidator.rs:426. It previously used a strict `>`, so a
/// bar whose start time was exactly at the close survived; it is now `>=`.
/// The function itself should not be used going forward.
#[test]
fn last_bar_available_never_returns_bar_at_or_after_close() {
    let mut mismatches = Vec::new();

    for minute in 30..60 {
        let got = last_bar_available(ny(2026, 11, 26, 16, minute), &AssetType::Stock)
            .expect("after the open there is always a previous bar");
        let expected = "2026-11-26T16:25:00-05:00";
        if rfc(got) != expected {
            mismatches.push(format!(
                "stock 16:{minute:02} → {} (expected {expected})",
                rfc(got)
            ));
        }
    }

    for minute in 0..60 {
        let got = last_bar_available(ny(2026, 11, 26, 16, minute), &AssetType::Option)
            .expect("after the open there is always a previous bar");
        let expected = "2026-11-26T15:55:00-05:00";
        if rfc(got) != expected {
            mismatches.push(format!(
                "option 16:{minute:02} → {} (expected {expected})",
                rfc(got)
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "bars starting at/after the close were returned:\n{}",
        mismatches.join("\n")
    );
}

// ──────────── is_fx_trading_datetime (:438) ────────────

#[test]
fn is_fx_trading_datetime_weekend_sessions() {
    let cases: &[(i32, u32, u32, u32, u32, bool)] = &[
        // Friday: trading until 17:00 NY, exclusive
        (2026, 11, 27, 0, 0, true),
        (2026, 11, 27, 16, 59, true),
        (2026, 11, 27, 17, 0, false),
        (2026, 11, 27, 23, 59, false),
        // Saturday: closed all day
        (2026, 11, 28, 0, 0, false),
        (2026, 11, 28, 12, 0, false),
        (2026, 11, 28, 23, 59, false),
        // Sunday: closed until 17:00 NY, inclusive
        (2026, 11, 29, 0, 0, false),
        (2026, 11, 29, 16, 59, false),
        (2026, 11, 29, 17, 0, true),
        (2026, 11, 29, 23, 59, true),
        // Monday–Thursday: open
        (2026, 11, 30, 0, 0, true),
        (2026, 12, 3, 12, 0, true),
    ];

    for (year, month, day, hour, minute, expected) in cases {
        let dt = ny(*year, *month, *day, *hour, *minute);
        assert_eq!(
            is_fx_trading_datetime(&dt),
            *expected,
            "{dt} (expected {expected})"
        );
    }
}

#[test]
fn is_fx_trading_datetime_holidays() {
    let cases: &[(&str, i32, u32, u32, bool)] = &[
        // New Year's Day (observed)
        ("2026 New Year (Thu)", 2026, 1, 1, false),
        ("2026 Jan 2", 2026, 1, 2, true),
        ("2027 New Year (Fri)", 2027, 1, 1, false),
        // 2028-01-01 is a Saturday → observed on Friday 2027-12-31.
        ("2027 New Year observed (Fri)", 2027, 12, 31, false),
        ("2028 New Year (Sat)", 2028, 1, 1, false),
        ("2028 Jan 3", 2028, 1, 3, true),
        // Christmas Day (observed)
        ("2026 Christmas Eve (Thu)", 2026, 12, 24, true),
        ("2026 Christmas (Fri)", 2026, 12, 25, false),
        ("2027 Dec 23 (Thu)", 2027, 12, 23, true),
        // 2027-12-25 is a Saturday → observed on Friday 2027-12-24.
        ("2027 Christmas observed (Fri)", 2027, 12, 24, false),
        ("2028 Christmas (Mon)", 2028, 12, 25, false),
        // Good Friday (Computus)
        ("2025 Good Friday", 2025, 4, 18, false),
        ("2025 Maundy Thursday", 2025, 4, 17, true),
        ("2026 Good Friday", 2026, 4, 3, false),
        ("2026 Maundy Thursday", 2026, 4, 2, true),
        ("2027 Good Friday", 2027, 3, 26, false),
        ("2028 Good Friday", 2028, 4, 14, false),
    ];

    for (label, year, month, day, expected) in cases {
        let dt = ny(*year, *month, *day, 10, 0);
        assert_eq!(
            is_fx_trading_datetime(&dt),
            *expected,
            "{label}: {dt} (expected {expected})"
        );
    }
}

/// Equity-only holidays do not close the FX market.
#[test]
fn is_fx_trading_datetime_ignores_equity_holidays() {
    let cases: &[(&str, i32, u32, u32)] = &[
        ("Independence Day observed (Fri)", 2026, 7, 3),
        ("MLK Day (Mon)", 2026, 1, 19),
        ("Memorial Day (Mon)", 2026, 5, 25),
        ("Labor Day (Mon)", 2026, 9, 7),
        ("Thanksgiving (Thu)", 2026, 11, 26),
        ("Day after Thanksgiving (Fri)", 2026, 11, 27),
        ("New Year's Eve (Thu)", 2026, 12, 31),
    ];

    for (label, year, month, day) in cases {
        let dt = ny(*year, *month, *day, 10, 0);
        assert!(is_fx_trading_datetime(&dt), "{label}: {dt} should trade");
    }
}

// ──────────── fx_trading_day_start (:524) ────────────

#[test]
fn fx_trading_day_start_is_previous_day_1700_in_requested_zone() {
    let cases: &[(&str, NaiveDate, Tz, &str)] = &[
        (
            "regular EST day",
            NaiveDate::from_ymd_opt(2026, 11, 26).unwrap(),
            New_York,
            "2026-11-25T17:00:00-05:00",
        ),
        (
            "spring-forward day itself",
            NaiveDate::from_ymd_opt(2026, 3, 8).unwrap(),
            New_York,
            "2026-03-07T17:00:00-05:00",
        ),
        (
            "day after spring-forward",
            NaiveDate::from_ymd_opt(2026, 3, 9).unwrap(),
            New_York,
            "2026-03-08T17:00:00-04:00",
        ),
        (
            "fall-back day itself",
            NaiveDate::from_ymd_opt(2026, 11, 1).unwrap(),
            New_York,
            "2026-10-31T17:00:00-04:00",
        ),
        (
            "day after fall-back",
            NaiveDate::from_ymd_opt(2026, 11, 2).unwrap(),
            New_York,
            "2026-11-01T17:00:00-05:00",
        ),
        (
            "year boundary",
            NaiveDate::from_ymd_opt(2027, 1, 1).unwrap(),
            New_York,
            "2026-12-31T17:00:00-05:00",
        ),
        (
            "tz parameter is honoured",
            NaiveDate::from_ymd_opt(2026, 11, 26).unwrap(),
            UTC,
            "2026-11-25T17:00:00+00:00",
        ),
    ];

    for (label, date, tz, expected) in cases {
        let got = fx_trading_day_start(date, tz);
        assert_eq!(rfc(got), *expected, "{label}");

        // Always 17:00 on the calendar day before `date`.
        assert_eq!(got.hour(), 17, "{label}");
        assert_eq!(got.minute(), 0, "{label}");
        assert_eq!(got.date_naive(), date.pred_opt().unwrap(), "{label}");
        assert_eq!(
            Datelike::weekday(&got),
            Datelike::weekday(&date.pred_opt().unwrap()),
            "{label}"
        );
    }
}

// ──────── last_bar_available_time_forex (:382) ────────

#[test]
fn last_bar_available_time_forex_walks_back_to_last_open_minute() {
    let cases: &[(&str, DateTime<Tz>, &str)] =
        &[
            (
                "Wednesday midday is already a valid minute",
                ny(2026, 11, 25, 10, 30),
                "2026-11-25T10:30:00-05:00",
            ),
            (
                "Friday at the close steps back one minute",
                ny(2026, 11, 27, 17, 0),
                "2026-11-27T16:59:00-05:00",
            ),
            (
                "Saturday steps back to Friday 16:59",
                ny(2026, 11, 28, 12, 0),
                "2026-11-27T16:59:00-05:00",
            ),
            (
                "Sunday pre-open steps back to Friday 16:59",
                ny(2026, 11, 29, 16, 59),
                "2026-11-27T16:59:00-05:00",
            ),
            (
                "Sunday at 17:00 is already trading",
                ny(2026, 11, 29, 17, 0),
                "2026-11-29T17:00:00-05:00",
            ),
            (
                "Monday midnight is trading",
                ny(2026, 11, 30, 0, 0),
                "2026-11-30T00:00:00-05:00",
            ),
            (
                "Christmas noon steps back to Christmas Eve 23:59",
                ny(2026, 12, 25, 12, 0),
                "2026-12-24T23:59:00-05:00",
            ),
            (
                "New Year midnight steps back to the previous year",
                ny(2026, 1, 1, 0, 0),
                "2025-12-31T23:59:00-05:00",
            ),
        ];

    for (label, now, expected) in cases {
        let got = last_bar_available_time_forex(*now);
        assert_eq!(rfc(got), *expected, "{label}");
        assert_eq!(got.second(), 0, "{label}");
        assert!(is_fx_trading_datetime(&got), "{label}: {got} must be trading");
        assert!(got <= *now, "{label}: result must not be in the future");
    }
}

// ───── build_contract_from_stock (src/helpers/contract.rs:151) ─────

struct BuiltContract {
    security_type: SecurityType,
    symbol: &'static str,
    exchange: &'static str,
    currency: &'static str,
    primary_exchange: &'static str,
}

#[test]
fn build_contract_from_stock_prefix_table() {
    let cases: &[(&str, &str, &str, &str, BuiltContract)] = &[
        (
            "plain US stock",
            "AAPL",
            "NASDAQ",
            "USD",
            BuiltContract {
                security_type: SecurityType::Stock,
                symbol: "AAPL",
                exchange: "SMART",
                currency: "USD",
                primary_exchange: "NASDAQ",
            },
        ),
        (
            "symbol with a space",
            "BRK B",
            "NYSE",
            "USD",
            BuiltContract {
                security_type: SecurityType::Stock,
                symbol: "BRK B",
                exchange: "SMART",
                currency: "USD",
                primary_exchange: "NYSE",
            },
        ),
        (
            "FX prefix splits base/quote, ignoring the arguments",
            "FX:EUR/USD",
            "NASDAQ",
            "JPY",
            BuiltContract {
                security_type: SecurityType::ForexPair,
                symbol: "EUR",
                exchange: "IDEALPRO",
                currency: "USD",
                primary_exchange: "",
            },
        ),
        (
            "FX prefix USD/SGD",
            "FX:USD/SGD",
            "",
            "",
            BuiltContract {
                security_type: SecurityType::ForexPair,
                symbol: "USD",
                exchange: "IDEALPRO",
                currency: "SGD",
                primary_exchange: "",
            },
        ),
        (
            "CASH prefix is an FX pair quoted in SGD",
            "CASH:USD",
            "",
            "",
            BuiltContract {
                security_type: SecurityType::ForexPair,
                symbol: "USD",
                exchange: "IDEALPRO",
                currency: "SGD",
                primary_exchange: "",
            },
        ),
        (
            "CFD prefix",
            "CFD:USOIL",
            "",
            "USD",
            BuiltContract {
                security_type: SecurityType::CFD,
                symbol: "USOIL",
                exchange: "SMART",
                currency: "USD",
                primary_exchange: "",
            },
        ),
        (
            "FUT prefix",
            "FUT:ES",
            "",
            "USD",
            BuiltContract {
                security_type: SecurityType::Future,
                symbol: "ES",
                exchange: "SMART",
                currency: "USD",
                primary_exchange: "",
            },
        ),
    ];

    for (label, stock, primary_exchange, currency, expected) in cases {
        let contract = build_contract_from_stock(
            &stock.to_string(),
            &primary_exchange.to_string(),
            &currency.to_string(),
        );
        assert_eq!(contract.security_type, expected.security_type, "{label}");
        assert_eq!(contract.symbol.as_str(), expected.symbol, "{label}");
        assert_eq!(contract.exchange.as_str(), expected.exchange, "{label}");
        assert_eq!(contract.currency.as_str(), expected.currency, "{label}");
        assert_eq!(
            contract.primary_exchange.as_str(),
            expected.primary_exchange,
            "{label}"
        );
    }

    // Futures get the next quarterly month in `YYYYMM` form.
    let future = build_contract_from_stock(&"FUT:ES".to_string(), &String::new(), &"USD".to_string());
    let expiry = &future.last_trade_date_or_contract_month;
    assert_eq!(expiry.len(), 6, "futures expiry must be YYYYMM: {expiry}");
    assert!(
        expiry.chars().all(|c| c.is_ascii_digit()),
        "futures expiry must be numeric: {expiry}"
    );
}

// ───────── get_local_symbol (src/helpers/contract.rs:125) ─────────

#[test]
fn get_local_symbol_format_table() {
    let cases: &[(&str, Contract, &str)] = &[
        (
            "stock",
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            "AAPL",
        ),
        (
            "future",
            Contract {
                security_type: SecurityType::Future,
                ..stock("ES", "CME", "", "USD")
            },
            "FUT:ES",
        ),
        (
            "forex",
            forex("EUR.USD", "USD"),
            "FX:EUR.USD/USD",
        ),
        (
            "cfd",
            Contract {
                security_type: SecurityType::CFD,
                ..stock("USOIL", "SMART", "", "USD")
            },
            "CFD:USOIL",
        ),
        (
            "option: symbol only — expiry/strike/right are not encoded",
            option("AAPL", "C", "20261218", 200.0, "100"),
            "AAPL",
        ),
        (
            "option and stock share the same local symbol",
            stock("AAPL", "SMART", "NASDAQ", "USD"),
            "AAPL",
        ),
        (
            "index falls into the Unknown branch",
            Contract {
                security_type: SecurityType::Index,
                ..stock("SPX", "CBOE", "", "USD")
            },
            "SPX",
        ),
        (
            "Other(\"CASH\") is Unknown, so the CASH branch is unreachable",
            Contract {
                security_type: SecurityType::Other("CASH".to_string()),
                ..stock("USD", "", "", "SGD")
            },
            "USD",
        ),
    ];

    for (label, contract, expected) in cases {
        assert_eq!(&get_local_symbol(contract), expected, "{label}");
    }
}

// ──────── get_contract_from (src/helpers/contract.rs:200) ────────

fn stock_full_keys(stock: &str, primary_exchange: &str, currency: &str) -> CurrentStockPositionsFullKeys {
    CurrentStockPositionsFullKeys {
        stock: stock.to_string(),
        primary_exchange: primary_exchange.to_string(),
        currency: currency.to_string(),
        strategy: "test-strategy".to_string(),
        quantity: 10.0,
        avg_price: 123.45,
        last_updated: epoch(),
    }
}

fn option_full_keys(
    stock: &str,
    option_type: OptionType,
    strike: f64,
) -> CurrentOptionPositionsFullKeys {
    CurrentOptionPositionsFullKeys {
        stock: stock.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        strategy: "test-strategy".to_string(),
        expiry: "20261218".to_string(),
        strike,
        multiplier: "100".to_string(),
        option_type,
        quantity: 3.0,
        avg_price: 5.25,
        last_updated: epoch(),
    }
}

#[test]
fn get_contract_from_current_positions_table() {
    // Stock position → SMART stock contract.
    let stock_position = CurrentPositionsFullKeys::Stock(stock_full_keys("AAPL", "NASDAQ", "USD"));
    let contract = get_contract_from(&LocalContractTypes::CurrentPosFk(stock_position));
    assert_eq!(contract.symbol.as_str(), "AAPL");
    assert_eq!(contract.security_type, SecurityType::Stock);
    assert_eq!(contract.exchange.as_str(), "SMART");
    assert_eq!(contract.primary_exchange.as_str(), "NASDAQ");
    assert_eq!(contract.currency.as_str(), "USD");

    // FX-backed stock position → IDEALPRO forex contract.
    let fx_position = CurrentPositionsFullKeys::Stock(stock_full_keys("FX:EUR/USD", "", "USD"));
    let fx_contract = get_contract_from(&LocalContractTypes::CurrentPosFk(fx_position));
    assert_eq!(fx_contract.symbol.as_str(), "EUR");
    assert_eq!(fx_contract.security_type, SecurityType::ForexPair);
    assert_eq!(fx_contract.exchange.as_str(), "IDEALPRO");
    assert_eq!(fx_contract.currency.as_str(), "USD");

    // CASH position → USD/SGD forex contract, regardless of the model currency.
    let cash_position = CurrentPositionsFullKeys::Stock(stock_full_keys("CASH:USD", "", "EUR"));
    let cash_contract = get_contract_from(&LocalContractTypes::CurrentPosFk(cash_position));
    assert_eq!(cash_contract.symbol.as_str(), "USD");
    assert_eq!(cash_contract.security_type, SecurityType::ForexPair);
    assert_eq!(cash_contract.exchange.as_str(), "IDEALPRO");
    assert_eq!(cash_contract.currency.as_str(), "SGD");

    // Option position → OCC-style option contract.
    let call_position = CurrentPositionsFullKeys::Options(option_full_keys(
        "AAPL",
        OptionType::Call,
        200.0,
    ));
    let call_contract = get_contract_from(&LocalContractTypes::CurrentPosFk(call_position));
    assert_eq!(call_contract.symbol.as_str(), "AAPL");
    assert_eq!(call_contract.security_type, SecurityType::Option);
    assert_eq!(call_contract.exchange.as_str(), "SMART");
    assert_eq!(call_contract.primary_exchange.as_str(), "NASDAQ");
    assert_eq!(call_contract.currency.as_str(), "USD");
    assert_eq!(call_contract.right, "C");
    assert_eq!(call_contract.last_trade_date_or_contract_month, "20261218");
    assert_eq!(call_contract.strike, 200.0);
    assert_eq!(call_contract.multiplier, "100");

    // Put differs only in `right`, and therefore in the cache key.
    let put_position =
        CurrentPositionsFullKeys::Options(option_full_keys("AAPL", OptionType::Put, 200.0));
    let put_contract = get_contract_from(&LocalContractTypes::CurrentPosFk(put_position));
    assert_eq!(put_contract.right, "P");
    assert_eq!(put_contract.strike, 200.0);
    assert_eq!(put_contract.multiplier, "100");
    assert_ne!(hc(call_contract), hc(put_contract));
}

// ───────────── Memoized / AnyMemoized (memoise.rs:17/:60) ─────────────

#[test]
fn memoized_caches_by_derived_key_and_keeps_the_first_value() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();

    // Key = the String; the u8 is part of the input but not the key, so a
    // second call with the same key returns the first computed value.
    let memo: Memoized<(String, u8), String, f64, String> = Memoized::new(
        Duration::from_secs(60),
        |input: &(String, u8)| input.0.clone(),
        move |input: &(String, u8)| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(f64::from(input.1) * 10.0)
        },
    );

    assert_eq!(memo.call(("a".to_string(), 1)), Ok(10.0));
    assert_eq!(memo.call(("a".to_string(), 2)), Ok(10.0));
    assert_eq!(calls.load(Ordering::SeqCst), 1, "same key must be a cache hit");

    assert_eq!(memo.call(("b".to_string(), 1)), Ok(10.0));
    assert_eq!(calls.load(Ordering::SeqCst), 2, "different key computes");

    assert_eq!(memo.call(("b".to_string(), 9)), Ok(10.0));
    assert_eq!(calls.load(Ordering::SeqCst), 2, "still a hit for b");
}

#[test]
fn memoized_does_not_cache_errors() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();

    let memo: Memoized<String, String, f64, String> = Memoized::new(
        Duration::from_secs(60),
        |key: &String| key.clone(),
        move |_key: &String| match counter.fetch_add(1, Ordering::SeqCst) {
            0 => Err("boom".to_string()),
            _ => Ok(7.5),
        },
    );

    assert_eq!(memo.call("x".to_string()), Err("boom".to_string()));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the failing call must have run"
    );

    // `try_get_with` does not cache errors, so the next call retries.
    assert_eq!(memo.call("x".to_string()), Ok(7.5));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(memo.call("x".to_string()), Ok(7.5));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn memoized_ttl_expires_entry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();

    let memo: Memoized<String, String, f64, String> = Memoized::new(
        Duration::from_millis(60),
        |key: &String| key.clone(),
        move |_key: &String| Ok(counter.fetch_add(1, Ordering::SeqCst) as f64 + 1.0),
    );

    assert_eq!(memo.call("k".to_string()), Ok(1.0));
    assert_eq!(memo.call("k".to_string()), Ok(1.0));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    std::thread::sleep(Duration::from_millis(250));

    assert_eq!(memo.call("k".to_string()), Ok(2.0));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn call_any_downcasts_result_and_maps_errors_to_display() {
    let ok: Memoized<u32, u32, f64, String> = Memoized::new(
        Duration::from_secs(60),
        |key: &u32| *key,
        |key: &u32| Ok(f64::from(*key) * 0.5),
    );

    let boxed = ok.call_any(Box::new(7u32)).expect("call must succeed");
    assert_eq!(*boxed.downcast::<f64>().expect("value must be f64"), 3.5);

    let failing: Memoized<String, String, u8, String> = Memoized::new(
        Duration::from_secs(60),
        |key: &String| key.clone(),
        |_key: &String| Err("bad input".to_string()),
    );

    match failing.call_any(Box::new("x".to_string())) {
        Ok(_) => panic!("expected the memoizer to return an error"),
        Err(message) => assert_eq!(message, "bad input"),
    }
}

#[test]
fn call_any_panics_on_input_type_mismatch() {
    let memo: Memoized<String, String, f64, String> = Memoized::new(
        Duration::from_secs(60),
        |key: &String| key.clone(),
        |key: &String| Ok(key.len() as f64),
    );

    let message = panic_message(|| {
        let _ = memo.call_any(Box::new(42u32));
    });

    assert!(
        message.contains("AnyMemoized: input type mismatch"),
        "unexpected panic message: {message}"
    );
    assert!(
        message.contains("String"),
        "panic message must name the expected input type: {message}"
    );
}
