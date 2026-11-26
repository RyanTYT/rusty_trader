//! `src/backtester/oracle` tests — the point-in-time price cache + FX fallback.
//!
//! All tests build `BacktestPriceSupplier` with a **lazy** `PgPool` pointing at
//! the documented test database URL; no connection is opened and no query is
//! issued (only the branch that needs a live DB is the unimplemented slow path).
//!
//! BLOCKED (documented, not tested):
//! - `oracle/data_loader.rs:40` `load_market_data` — IBKR gateway session
//!   (`Client::connect("localhost:4002")`), Alpaca REST fallback, and DB gap
//!   queries. `determine_missing_ranges` (`:134`) is private and takes a live
//!   `HistoricalDataCRUD`, so its gap arithmetic cannot be reached directly.
//! - `oracle/data_loader.rs:508` `refresh_continuous_aggregate` — executes a
//!   TimescaleDB `CALL`.
//! - `oracle/price_supplier.rs:161` slow path — permanently returns Err
//!   ("slow path not yet implemented") and needs a point-in-time DB query.

use std::sync::Arc;

use trading_app::backtester::oracle::price_supplier::BacktestPriceSupplier;
use trading_app::backtester::setup::clock::BacktestClock;
use trading_app::market_data::traits::current_price::PriceSupplier;

use crate::support::{assert_close, forex, stock, stock_full};

fn lazy_pool() -> sqlx::PgPool {
    // sqlx requires a Tokio context even for a lazy pool (it registers pool
    // housekeeping). Keep one runtime alive for the whole test binary; no
    // connection is ever opened because nothing awaits a query.
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    let rt = RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test tokio runtime")
    });
    rt.block_on(async {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://ryantan@127.0.0.1:5432/test_trading_system")
            .expect("lazy pool config should parse")
    })
}

fn supplier(contracts: &[ibapi::contracts::Contract]) -> BacktestPriceSupplier {
    BacktestPriceSupplier::new(Arc::new(BacktestClock::new()), lazy_pool(), contracts)
}

fn price(s: &BacktestPriceSupplier, contract: ibapi::contracts::Contract) -> Result<f64, String> {
    s.get_current_price(contract, false, &[])
}

#[test]
fn published_close_is_returned_for_the_assigned_slot() {
    let qqq = stock("QQQ");
    let s = supplier(std::slice::from_ref(&qqq));

    s.publish_close(&qqq, 412.34);
    assert_eq!(price(&s, qqq).unwrap(), 412.34);
}

#[test]
fn publish_overwrites_the_slot_with_the_latest_close() {
    let qqq = stock("QQQ");
    let s = supplier(std::slice::from_ref(&qqq));

    s.publish_close(&qqq, 100.0);
    s.publish_close(&qqq, 101.5);
    s.publish_close(&qqq, 99.25);
    assert_eq!(price(&s, qqq).unwrap(), 99.25);
}

#[test]
fn cache_match_ignores_non_key_contract_fields_like_exchange() {
    // HashContract keys on (trimmed pe, symbol, currency, security_type); the
    // exchange is not part of the identity, so a differently-routed contract
    // still hits the same slot.
    let published = stock_full("QQQ", "NASDAQ", "USD");
    let s = supplier(std::slice::from_ref(&published));
    s.publish_close(&published, 400.0);

    let mut lookup = published.clone();
    lookup.exchange = "ARCA".into();
    assert_eq!(price(&s, lookup).unwrap(), 400.0);

    // ...but a different primary exchange/currency is a different slot.
    let mut other_pe = published.clone();
    other_pe.primary_exchange = "NYSE".into();
    assert!(price(&s, other_pe).is_err());
    let mut other_ccy = published.clone();
    other_ccy.currency = "SGD".into();
    assert!(price(&s, other_ccy).is_err());
}

#[test]
fn each_contract_has_an_independent_slot() {
    let aapl = stock("AAPL");
    let msft = stock("MSFT");
    let s = supplier(&[aapl.clone(), msft.clone()]);

    s.publish_close(&aapl, 190.0);
    assert_eq!(price(&s, aapl.clone()).unwrap(), 190.0);
    assert!(price(&s, msft.clone()).is_err());

    s.publish_close(&msft, 420.0);
    assert_eq!(price(&s, aapl).unwrap(), 190.0);
    assert_eq!(price(&s, msft).unwrap(), 420.0);
}

#[test]
fn publishing_a_contract_outside_the_fixed_set_is_ignored() {
    let qqq = stock("QQQ");
    let s = supplier(std::slice::from_ref(&qqq));
    // Must not panic; the unknown contract is logged + dropped.
    s.publish_close(&stock("TSLA"), 250.0);
    assert!(price(&s, stock("TSLA")).is_err());
    // And the known slot is untouched.
    assert!(price(&s, qqq).is_err());
}

#[test]
fn fx_fallback_returns_the_fixed_rate_for_known_pairs() {
    let s = supplier(&[]);
    // USD -> SGD.
    assert_close(price(&s, forex("USD", "SGD")).unwrap(), 1.2713, 1e-12);
    // EUR -> SGD.
    assert_close(price(&s, forex("EUR", "SGD")).unwrap(), 1.4819, 1e-12);
    // JPY -> SGD (small rate).
    assert_close(price(&s, forex("JPY", "SGD")).unwrap(), 0.007983, 1e-15);
}

#[test]
fn fx_fallback_inverts_reverse_pairs() {
    let s = supplier(&[]);
    let got = price(&s, forex("SGD", "USD")).unwrap();
    assert_close(got, 1.0 / 1.2713, 1e-12);
    let got = price(&s, forex("SGD", "EUR")).unwrap();
    assert_close(got, 1.0 / 1.4819, 1e-12);
}

#[test]
fn fx_fallback_rejects_unknown_crosses_and_non_fx_contracts() {
    // Use a non-empty cache set: the empty-set error path itself is covered by
    // a separate known-bug test below.
    let s = supplier(&[stock("QQQ")]);
    // A cross (no SGD leg) is not in the fixed map.
    assert!(price(&s, forex("EUR", "USD")).is_err());
    // A stock contract is never served by the FX map, even with matching fields.
    let usd_quoted_stock = stock_full("USD", "", "SGD");
    assert!(price(&s, usd_quoted_stock).is_err());
}

#[test]
fn cached_close_wins_over_the_fx_fallback() {
    // A forex pair that IS in the bar stream uses its published close, not the
    // fixed rate map.
    let pair = forex("USD", "SGD");
    let s = supplier(std::slice::from_ref(&pair));
    s.publish_close(&pair, 1.30);
    assert_eq!(price(&s, pair).unwrap(), 1.30);
}

#[test]
fn unpublished_contract_returns_a_descriptive_error() {
    let s = supplier(&[stock("QQQ")]);
    let err = price(&s, stock("QQQ")).unwrap_err();
    assert_eq!(
        err,
        "BacktestPriceSupplier: no published close for QQQ (slow path not yet implemented)"
    );
}

#[test]
fn published_close_is_exact_for_fractional_and_negative_prices() {
    let qqq = stock("QQQ");
    let s = supplier(std::slice::from_ref(&qqq));
    for v in [0.0001_f64, 1234.567_890_123, -5.5] {
        s.publish_close(&qqq, v);
        // Bit-exact round trip through f64::to_bits / from_bits.
        assert_eq!(price(&s, qqq.clone()).unwrap(), v);
    }
}

#[test]
#[ignore = "known bug: a published close of 0.0 stores bits 0, which is also the 'unpublished' sentinel, so the oracle returns an error instead of 0.0 (oracle/price_supplier.rs:79-91,128-130)"]
fn published_zero_close_is_a_valid_price() {
    let qqq = stock("QQQ");
    let s = supplier(std::slice::from_ref(&qqq));
    s.publish_close(&qqq, 0.0);
    // Intended: Ok(0.0). Observed: Err("... no published close ...").
    assert_eq!(price(&s, qqq).unwrap(), 0.0);
}

#[test]
#[ignore = "known bug: with an empty contract set, the error path unwraps slot_map.keys().next() and panics instead of returning Err (oracle/price_supplier.rs:144-165)"]
fn empty_contract_set_returns_error_not_panic() {
    let s = supplier(&[]);
    // Intended: a normal Err. Observed: panic on `unwrap()` of an empty map.
    assert!(price(&s, stock("QQQ")).is_err());
}

#[test]
fn empty_contract_set_still_serves_fx_fallback_without_panicking() {
    // The FX branch runs before the panicking diagnostic `unwrap`, so this is
    // the one supported empty-set lookup.
    let s = supplier(&[]);
    assert_close(price(&s, forex("USD", "SGD")).unwrap(), 1.2713, 1e-12);
}
