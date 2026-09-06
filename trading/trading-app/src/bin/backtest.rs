//! Backtest binary entry point.
//!
//! Gated on the `backtest` cargo feature via `required-features` in Cargo.toml.
//! Build & run:
//!   cargo run --bin backtest --features backtest
//!
//! Reads `backtest.json` (working dir) for the full config — mirrors
//! `optimiser_params.json`:
//! ```json
//! {
//!   "config": {
//!     "start": "2022-01-01T00:00:00Z", "end": "2025-01-01T00:00:00Z",
//!     "starting_capital_sgd": 100000.0, "mode": "in_memory",
//!     "stock_bar_interval_secs": 300, "slippage_bps": 0.0,
//!     "commission_model": "Tiered"
//!   },
//!   "noise": {
//!     "active": true,
//!     "contracts": [{"stock":"QQQ","primary_exchange":"NASDAQ","currency":"USD"}],
//!     "params": {"daily_vol_threshold":0.04, ...}
//!   },
//!   "relative": {
//!     "active": true,
//!     "benchmark": {"stock":"SPY","primary_exchange":"ARCA","currency":"USD"},
//!     "contracts": [{"stock":"NVDA",...}, ...],
//!     "params": {"rs_pct_threshold":0.80, ...}
//!   }
//! }
//! ```
//! The `config` section holds the backtest-wide scalars; the per-strategy
//! sections (flattened) hold each strategy's `active` flag, contracts,
//! + directly-defined params (NOT distributions — the optimizer sweeps
//! distributions; the backtester runs one backtest per active strategy with
//! these exact values).
//!
//! The binary does EVERYTHING:
//!   1. Connects to test-db (TEST_TRADING_DB_URL or DATABASE_URL).
//!   2. Runs SQLx migrations (so the schema exists — no sqlx-cli needed).
//!   3. Calls `run_backtest` which:
//!      a. Data loader (IBKR via with_gateway_retry, Alpaca fallback) — for
//!         the union of all active strategies' contracts + benchmarks.
//!      b. Replayer (real on_bar_update + real handle_bar_update_outcome).
//!      c. Results (PnL, equity curve, max DD, Sharpe, etc.) →
//!         backtest_results_{name}.json per strategy.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ibapi::contracts::Contract;
use serde::Deserialize;

use trading_app::backtester::{BacktestConfig, BacktestMode, BacktestPeriod, CommissionModel};
use trading_app::helpers::contract::build_contract_from_stock;
use trading_app::strategy::{ContractEntry, StrategySpec};

/// The `config` section of `backtest.json`. `start` + `end` (RFC3339) OR
/// `num_bars` select the period; the rest have defaults.
#[derive(Deserialize)]
struct BacktestConfigSection {
    start: Option<String>,
    end: Option<String>,
    num_bars: Option<usize>,
    starting_capital_sgd: Option<f64>,
    mode: Option<String>,
    stock_bar_interval_secs: Option<i64>,
    forex_bar_interval_secs: Option<i64>,
    slippage_bps: Option<f64>,
    commission_model: Option<String>,
    output_path: Option<String>,
}

/// The top-level `backtest.json` — the `config` section + the per-strategy
/// sections (flattened: each strategy name → its spec).
#[derive(Deserialize)]
struct BacktestFile {
    config: BacktestConfigSection,
    #[serde(flatten)]
    strategies: HashMap<String, StrategySpec>,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    let database_url = std::env::var("TEST_TRADING_DB_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map_err(|_| "DATABASE_URL or TEST_TRADING_DB_URL must be set".to_string())?;

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&database_url)
        .await
        .map_err(|e| format!("failed to connect to {database_url}: {e}"))?;

    // Run migrations so the schema exists (no sqlx-cli needed in the image).
    if let Err(e) = sqlx::migrate!("./migrations").run(&pool).await {
        return Err(format!("migration error: {e}"));
    }

    // 1. Read backtest.json (working dir) — the config section + the
    //    per-strategy sections (active flag, contracts, directly-defined
    //    params).
    let params_file = std::path::Path::new("backtest.json");
    let content = std::fs::read_to_string(params_file)
        .map_err(|e| format!("read backtest.json: {e} — expected in the working dir"))?;
    let file: BacktestFile =
        serde_json::from_str(&content).map_err(|e| format!("parse backtest.json: {e}"))?;

    // 2. Build the period (TimeRange from start/end, or NumBars).
    let period = if let Some(n) = file.config.num_bars {
        BacktestPeriod::NumBars(n)
    } else {
        let start_str = file
            .config
            .start
            .as_ref()
            .ok_or("config.start (RFC3339) or config.num_bars required")?;
        let end_str = file
            .config
            .end
            .as_ref()
            .ok_or("config.end (RFC3339) or config.num_bars required")?;
        let start = DateTime::parse_from_rfc3339(start_str)
            .map_err(|e| format!("config.start: {e}"))?
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339(end_str)
            .map_err(|e| format!("config.end: {e}"))?
            .with_timezone(&Utc);
        BacktestPeriod::TimeRange { start, end }
    };

    let capital = file.config.starting_capital_sgd.unwrap_or(100_000.0);
    let mode = BacktestMode::from_str(file.config.mode.as_deref().unwrap_or("db"))?;
    let stock_bar_interval =
        chrono::Duration::seconds(file.config.stock_bar_interval_secs.unwrap_or(300));
    let forex_bar_interval =
        chrono::Duration::seconds(file.config.forex_bar_interval_secs.unwrap_or(60));
    let slippage_bps = file.config.slippage_bps.unwrap_or(0.0);
    let commission_model =
        CommissionModel::from_str(file.config.commission_model.as_deref().unwrap_or("Tiered"))?;
    let output_path = file
        .config
        .output_path
        .clone()
        .unwrap_or_else(|| "backtest_results.json".to_string());

    // 3. Build the subscribed contracts — the union of all ACTIVE strategies'
    //    contracts + their benchmark (the relative strategy's benchmark is
    //    fetched in warm_up_data under primary_exchange "ARCA", so it must be
    //    subscribed). Inactive strategies' contracts are NOT loaded.
    let mut subscribed: Vec<Contract> = Vec::new();
    for (_name, spec) in &file.strategies {
        if !spec.active {
            continue;
        }
        for c in &spec.contracts {
            push_unique_contract(&mut subscribed, c);
        }
        if let Some(bench) = &spec.benchmark {
            push_unique_contract(&mut subscribed, bench);
        }
    }
    if subscribed.is_empty() {
        return Err(
            "no active strategies define any contracts — backtest.json must define at least one contract"
                .into(),
        );
    }
    tracing::info!(
        "backtest.json: {} strategy/strategies ({} active), {} subscribed contract/s",
        file.strategies.len(),
        file.strategies.values().filter(|s| s.active).count(),
        subscribed.len(),
    );

    for (strategy_name, strategy_config) in file.strategies.iter() {
        if !strategy_config.active {
            continue;
        }
        let mut strategies = HashMap::new();
        strategies.insert(strategy_name.clone(), strategy_config.clone());
        // 4. Build the BacktestConfig + run.
        let config = BacktestConfig::new(capital)
            .stock_bar_interval(stock_bar_interval)
            .forex_bar_interval(forex_bar_interval)
            .period(period.clone())
            .slippage_bps(slippage_bps)
            .commission_model(commission_model)
            .mode(mode)
            .contracts(
                strategy_config
                    .contracts
                    .iter()
                    .map(|contract| {
                        build_contract_from_stock(
                            &contract.stock,
                            &contract.primary_exchange,
                            &contract.currency,
                        )
                    })
                    .collect(),
            )
            .output_path(&output_path)
            .strategies(strategies);

        trading_app::backtester::run_backtest(pool.clone(), config).await?;
    }

    Ok(())
}

/// Append `entry` to `out` as a built `Contract`, skipping duplicates
/// (same stock + primary_exchange + currency).
fn push_unique_contract(out: &mut Vec<Contract>, entry: &ContractEntry) {
    let already = out.iter().any(|c| {
        c.symbol.as_str() == entry.stock.as_str()
            && c.primary_exchange.as_str() == entry.primary_exchange.as_str()
            && c.currency.as_str() == entry.currency.as_str()
    });
    if !already {
        out.push(build_contract_from_stock(
            &entry.stock,
            &entry.primary_exchange,
            &entry.currency,
        ));
    }
}
