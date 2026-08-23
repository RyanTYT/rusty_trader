//! Backtester — replay historical bars through the REAL prod strategy +
//! reconciliation against test-db, with a simulated broker + point-in-time
//! price oracle. Feature-gated on `backtest`.
//!
//! # What this reuses (unchanged prod code, cfg-gated signatures)
//! - `StrategyExecutor::on_bar_update` — the real strategy signal logic.
//! - `OrderEngine::handle_bar_update_outcome` — the real reconciliation
//!   (FX attachments, cancel/replace, delta loop).
//! - `update_positions_additive`, `get_target_pos_diff_by_strat`, all CRUD.
//! - `Consolidator::new_for_backtest` — backtest-constructible (no `Client`).
//!
//! # What this substitutes (the cfg-gated seams)
//! - `BacktestBroker: OrderSubmitter` — simulated fills instead of `client.submit_order`.
//! - `BacktestPriceSupplier: PriceSupplier` — point-in-time bar close instead of IBKR.
//!
//! Build & run:
//!   cargo run --bin backtest --features backtest

pub mod broker;
pub mod clock;
pub mod config;
pub mod data_loader;
pub mod equity;
pub mod fill_model;
pub mod price_supplier;
pub mod replayer;
pub mod results;

pub use broker::BacktestBroker;
pub use clock::BacktestClock;
pub use config::BacktestConfig;
pub use equity::EquityCurve;
pub use replayer::Replayer;
pub use results::BacktestResults;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::env;

use crate::helpers::contract::build_contract_from_stock;
use crate::strategy::noise::Noise;
use crate::strategy::strategy::{StrategyEnum, StrategyExecutor};

/// Entry point. Builds a default config (env-overridable) + the real Noise
/// strategy, runs the data loader (IBKR/Alpaca) + replayer + results.
pub async fn run_backtest(pool: sqlx::PgPool) -> Result<(), String> {
    let handle = tokio::runtime::Handle::current();

    let stock = env::var("BACKTEST_STOCK").unwrap_or_else(|_| "QQQ".to_string());
    let primary_exchange =
        env::var("BACKTEST_PRIMARY_EXCHANGE").unwrap_or_else(|_| "NASDAQ".to_string());
    let currency = env::var("BACKTEST_CURRENCY").unwrap_or_else(|_| "USD".to_string());
    let start = env::var("BACKTEST_START")
        .map_err(|_| "BACKTEST_START (RFC3339) required".to_string())
        .and_then(|s| DateTime::parse_from_rfc3339(&s).map_err(|e| format!("BACKTEST_START: {e}")))
        .map(|dt| dt.with_timezone(&Utc))?;
    let end = env::var("BACKTEST_END")
        .map_err(|_| "BACKTEST_END (RFC3339) required".to_string())
        .and_then(|s| DateTime::parse_from_rfc3339(&s).map_err(|e| format!("BACKTEST_END: {e}")))
        .map(|dt| dt.with_timezone(&Utc))?;
    let starting_capital_sgd = env::var("BACKTEST_CAPITAL")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(100_000.0);
    let slippage_bps = env::var("BACKTEST_SLIPPAGE_BPS")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let commission_per_share = env::var("BACKTEST_COMM_PER_SHARE")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.005);
    let commission_min_per_order = env::var("BACKTEST_COMM_MIN")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(1.0);
    let output = env::var("BACKTEST_OUTPUT").unwrap_or_else(|_| "backtest_results.json".to_string());

    let contract = build_contract_from_stock(&stock, &primary_exchange, &currency);
    let config = BacktestConfig {
        subscribed_contracts: vec![contract.clone()],
        start,
        end,
        starting_capital_sgd,
        slippage_bps,
        commission_per_share,
        commission_min_per_order,
    };

    // 1. Load market data (IBKR first via with_gateway_retry, Alpaca fallback).
    //    This populates market_data.historical_data for the backtest period.
    crate::backtester::data_loader::load_market_data(
        &config.subscribed_contracts,
        config.start,
        config.end,
        &pool,
        &handle,
    )
    .await?;

    // 2. Real prod strategy — the SAME Noise that runs in production.
    let strategy = StrategyEnum::Noise(Noise::new(pool.clone(), handle.clone()));

    let pool_for_results = pool.clone();
    let replayer = Replayer::new(config, pool.clone(), handle, strategy);
    let equity = tokio::task::spawn_blocking(move || replayer.run())
        .await
        .map_err(|e| format!("replayer join: {e:?}"))??;

    let results =
        BacktestResults::compute(&pool_for_results, &equity, starting_capital_sgd).await?;
    results.write_json(&output)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&results).map_err(|e| format!("serialize: {e}"))?
    );
    Ok(())
}
