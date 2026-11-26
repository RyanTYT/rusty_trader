//! `src/backtester/setup` tests — config parsing/defaults, the deterministic
//! clock, and contract construction.
//!
//! BLOCKED (documented, not tested):
//! - `setup/seed.rs:24` `seed_initial_capital` — executes DELETEs against
//!   `trading.*`; requires a live Postgres.
//! - `setup/context.rs:43` `build_light_context` / `:83`
//!   `BacktestContext::build` — need `PgPool`, `MarketDataHandler` and (for the
//!   full context) `OrderStore::open()` (on-disk state) plus a tokio runtime.

use chrono::{DateTime, Duration, TimeZone, Utc};

use trading_app::backtester::execution::fill_model::CommissionModel;
use trading_app::backtester::setup::clock::BacktestClock;
use trading_app::backtester::setup::config::{BacktestConfig, BacktestMode, BacktestPeriod};
use trading_app::database::models::AssetType;
use trading_app::helpers::contract::{build_contract_from_stock, get_local_symbol};
use trading_app::strategy::StrategySpec;

use crate::support::{assert_close, utc};

#[test]
fn backtest_mode_parses_known_aliases_case_insensitively() {
    let cases = [
        ("db", BacktestMode::Db),
        ("DB", BacktestMode::Db),
        ("Db", BacktestMode::Db),
        ("in_memory", BacktestMode::InMemory),
        ("IN_MEMORY", BacktestMode::InMemory),
        ("inmemory", BacktestMode::InMemory),
        ("InMemory", BacktestMode::InMemory),
    ];
    for (input, expected) in cases {
        assert_eq!(
            BacktestMode::from_str(input).unwrap(),
            expected,
            "input {input}"
        );
    }
}

#[test]
fn backtest_mode_rejects_unknown_values_with_exact_message() {
    assert_eq!(
        BacktestMode::from_str("realtime").unwrap_err(),
        "unknown backtest mode 'realtime' (expected 'db' or 'in_memory')"
    );
    // No trimming: a padded value is rejected.
    assert!(BacktestMode::from_str(" db ").is_err());
    assert!(BacktestMode::from_str("").is_err());
}

#[test]
fn config_new_has_documented_defaults() {
    let cfg = BacktestConfig::new(12_345.0);

    assert_eq!(cfg.stock_bar_interval, Duration::minutes(5));
    assert_eq!(cfg.forex_bar_interval, Duration::minutes(1));
    match &cfg.period {
        BacktestPeriod::NumBars(n) => assert_eq!(*n, 1000),
        other => panic!("expected NumBars(1000), got {other:?}"),
    }
    assert_eq!(cfg.starting_capital_sgd, 12_345.0);
    assert_eq!(cfg.slippage_bps, 0.0);
    assert_eq!(cfg.commission_model, CommissionModel::Tiered);
    assert_eq!(cfg.mode, BacktestMode::Db);
    assert_eq!(cfg.output_path, "backtest_results.json");
    assert!(cfg.strategies.is_empty());
    assert!(!cfg.multi_run);
    assert_eq!(cfg.fill_lag_bars, 0);

    // Default subscribed contract: QQQ / NASDAQ / USD on SMART.
    assert_eq!(cfg.subscribed_contracts.len(), 1);
    let c = &cfg.subscribed_contracts[0];
    assert_eq!(c.symbol.to_string(), "QQQ");
    assert_eq!(c.primary_exchange.to_string(), "NASDAQ");
    assert_eq!(c.currency.to_string(), "USD");
    assert_eq!(c.exchange.to_string(), "SMART");
    assert_eq!(c.security_type, ibapi::prelude::SecurityType::Stock);
}

#[test]
fn config_builder_sets_every_scalar_field() {
    let start = utc(2024, 1, 15, 14, 30, 0);
    let end = utc(2024, 2, 15, 21, 0, 0);
    let cfg = BacktestConfig::new(50_000.0)
        .stock_bar_interval(Duration::minutes(15))
        .forex_bar_interval(Duration::minutes(5))
        .period(BacktestPeriod::TimeRange { start, end })
        .slippage_bps(7.5)
        .commission_model(CommissionModel::Fixed)
        .mode(BacktestMode::InMemory)
        .multi_run(true)
        .fill_lag_bars(3)
        .output_path("out.json");

    assert_eq!(cfg.stock_bar_interval, Duration::minutes(15));
    assert_eq!(cfg.forex_bar_interval, Duration::minutes(5));
    match cfg.period {
        BacktestPeriod::TimeRange { start: s, end: e } => {
            assert_eq!(s, start);
            assert_eq!(e, end);
        }
        other => panic!("expected TimeRange, got {other:?}"),
    }
    assert_eq!(cfg.slippage_bps, 7.5);
    assert_eq!(cfg.commission_model, CommissionModel::Fixed);
    assert_eq!(cfg.mode, BacktestMode::InMemory);
    assert!(cfg.multi_run);
    assert_eq!(cfg.fill_lag_bars, 3);
    assert_eq!(cfg.output_path, "out.json");
}

#[test]
fn config_stock_builder_replaces_the_contract_set_with_one_contract() {
    let cfg = BacktestConfig::new(1000.0).contracts(vec![
        crate::support::stock("AAPL"),
        crate::support::stock("MSFT"),
    ]);
    assert_eq!(cfg.subscribed_contracts.len(), 2);

    let cfg = cfg.stock("TSLA", "NASDAQ", "USD");
    assert_eq!(cfg.subscribed_contracts.len(), 1);
    let c = &cfg.subscribed_contracts[0];
    assert_eq!(c.symbol.to_string(), "TSLA");
    assert_eq!(c.primary_exchange.to_string(), "NASDAQ");
    assert_eq!(c.currency.to_string(), "USD");
    assert_eq!(c.exchange.to_string(), "SMART");
}

#[test]
fn config_strategies_setter_replaces_the_map() {
    let spec = StrategySpec {
        active: true,
        params: [("lookback".to_string(), 20.0)].into_iter().collect(),
        contracts: vec![trading_app::strategy::ContractEntry {
            stock: "QQQ".to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
        }],
        benchmark: None,
    };
    let mut map = std::collections::HashMap::new();
    map.insert("noise".to_string(), spec);

    let cfg = BacktestConfig::new(1000.0).strategies(map);
    assert_eq!(cfg.strategies.len(), 1);
    assert_eq!(cfg.strategies["noise"].params["lookback"], 20.0);
    assert!(cfg.strategies["noise"].active);
}

#[test]
fn strategy_spec_serde_defaults_active_true_and_empty_collections() {
    let spec: StrategySpec = serde_json::from_str("{}").unwrap();
    assert!(spec.active);
    assert!(spec.params.is_empty());
    assert!(spec.contracts.is_empty());
    assert!(spec.benchmark.is_none());

    let spec: StrategySpec = serde_json::from_str(
        r#"{"active": false, "params": {"a": 1.5, "b": 2}, "contracts": [{"stock":"SPY","primary_exchange":"ARCA","currency":"USD"}]}"#,
    )
    .unwrap();
    assert!(!spec.active);
    assert_eq!(spec.params["a"], 1.5);
    assert_eq!(spec.contracts[0].stock, "SPY");
}

#[test]
fn clock_reports_the_exact_time_it_was_set_to() {
    let clock = BacktestClock::new();
    let t = utc(2024, 6, 14, 14, 30, 0);
    clock.set(t);
    assert_eq!(clock.now(), t);
}

#[test]
fn clock_preserves_nanoseconds_and_the_last_set_wins() {
    let clock = BacktestClock::new();
    let t = DateTime::parse_from_rfc3339("2024-01-15T14:30:00.123456789Z")
        .unwrap()
        .with_timezone(&Utc);
    clock.set(t);
    assert_eq!(clock.now(), t);

    let t2 = utc(2025, 12, 31, 23, 59, 59);
    clock.set(t2);
    assert_eq!(clock.now(), t2);
}

#[test]
fn clock_unset_falls_back_to_wall_clock_without_panicking() {
    let clock = BacktestClock::default();
    let now = clock.now();
    // Deterministic bound: wall clock is well past 2020 in any sane test env.
    assert!(
        now.timestamp() > 1_577_836_800,
        "unexpected wall clock: {now}"
    );
}

#[test]
#[ignore = "known bug: epoch nanos == 0 is the 'unset' sentinel, so setting 1970-01-01T00:00:00Z silently falls back to Utc::now() (setup/clock.rs:37-51)"]
fn clock_epoch_is_a_valid_time() {
    // Intended: a backtest may legitimately start at the unix epoch, and
    // `set(t)` then `now()` must round-trip.
    let clock = BacktestClock::new();
    let epoch = Utc.timestamp_opt(0, 0).unwrap();
    clock.set(epoch);
    // Observed instead: Utc::now() (2026-ish), because 0 means "never set".
    assert_eq!(clock.now(), epoch);
}

#[test]
fn build_contract_from_stock_covers_stock_fx_cfd_and_cash() {
    // Stock: SMART, explicit primary exchange + currency.
    let c = build_contract_from_stock(
        &"AAPL".to_string(),
        &"NASDAQ".to_string(),
        &"USD".to_string(),
    );
    assert_eq!(c.symbol.to_string(), "AAPL");
    assert_eq!(c.primary_exchange.to_string(), "NASDAQ");
    assert_eq!(c.currency.to_string(), "USD");
    assert_eq!(c.exchange.to_string(), "SMART");
    assert_eq!(AssetType::from_str(&c.security_type), AssetType::Stock);

    // FX: "FX:EUR/USD" → IDEALPRO, symbol EUR, currency USD.
    let c = build_contract_from_stock(
        &"FX:EUR/USD".to_string(),
        &"".to_string(),
        &"USD".to_string(),
    );
    assert_eq!(c.symbol.to_string(), "EUR");
    assert_eq!(c.currency.to_string(), "USD");
    assert_eq!(c.exchange.to_string(), "IDEALPRO");
    assert_eq!(AssetType::from_str(&c.security_type), AssetType::ForexPair);

    // CFD: "CFD:X" → SMART, symbol X.
    let c = build_contract_from_stock(&"CFD:X".to_string(), &"".to_string(), &"USD".to_string());
    assert_eq!(c.symbol.to_string(), "X");
    assert_eq!(c.security_type, ibapi::prelude::SecurityType::CFD);
    assert_eq!(c.exchange.to_string(), "SMART");

    // CASH: "CASH:SGD" → represented as an IDEALPRO SGD pair.
    let c = build_contract_from_stock(&"CASH:SGD".to_string(), &"".to_string(), &"SGD".to_string());
    assert_eq!(c.symbol.to_string(), "SGD");
    assert_eq!(c.currency.to_string(), "SGD");
    assert_eq!(c.exchange.to_string(), "IDEALPRO");
    assert_eq!(c.security_type, ibapi::prelude::SecurityType::ForexPair);
}

#[test]
fn local_symbol_round_trips_asset_prefixes() {
    assert_eq!(get_local_symbol(&crate::support::stock("QQQ")), "QQQ");
    assert_eq!(
        get_local_symbol(&crate::support::forex("EUR", "USD")),
        "FX:EUR/USD"
    );
    let cfd = ibapi::contracts::Contract {
        symbol: "X".into(),
        security_type: ibapi::prelude::SecurityType::CFD,
        ..Default::default()
    };
    assert_eq!(get_local_symbol(&cfd), "CFD:X");
}

#[test]
fn backtest_period_clone_and_debug_are_stable() {
    let start = utc(2024, 1, 1, 0, 0, 0);
    let end = utc(2024, 1, 31, 0, 0, 0);
    let p = BacktestPeriod::TimeRange { start, end };
    match p.clone() {
        BacktestPeriod::TimeRange { start: s, end: e } => {
            assert_eq!(s, start);
            assert_eq!(e, end);
        }
        _ => panic!("clone changed the variant"),
    }
    assert!(format!("{p:?}").contains("TimeRange"));
    assert!(format!("{:?}", BacktestPeriod::NumBars(7)).contains("NumBars(7)"));
}

#[test]
fn config_slippage_accepts_fractional_bps() {
    let cfg = BacktestConfig::new(1.0).slippage_bps(0.25);
    assert_close(cfg.slippage_bps, 0.25, 0.0);
}
