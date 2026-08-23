//! Backtest configuration — the user-facing interface layer. Holds the key
//! backtest parameters (bar granularity per asset type, lookback period,
//! initial capital, fees, commission model, mode, + a generic strategy-params
//! map) + a fluent builder.
//!
//! Strategy params are a generic `HashMap<String, HashMap<String, f64>>` keyed
//! by strategy name → param name → value. They're parsed from env vars named
//! `<STRATEGY>_<VAR>` (e.g. `NOISE_LOOKBACK_PERIOD=5` →
//! `strategy_params["noise"]["lookback_period"] = 5.0`). The strategy reads
//! its params from this map (cfg-gated; falls back to hardcoded if unset).
//!
//! The `backtest` bin builds a `BacktestConfig` (via [`BacktestConfig::from_env`]
//! or the builder) + passes it to [`crate::backtester::run_backtest`].

use std::collections::HashMap;
use std::env;

use chrono::{DateTime, Duration, Utc};
use ibapi::contracts::Contract;

use crate::backtester::execution::fill_model::CommissionModel;
use crate::helpers::contract::build_contract_from_stock;

/// The backtest execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestMode {
    /// DB-backed (realistic, ~5-10 DB ops/bar).
    Db,
    /// Fast in-memory (mocked CRUDs + in-memory reconcile, ~0 DB ops/bar).
    InMemory,
}

impl BacktestMode {
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "db" => Ok(Self::Db),
            "in_memory" | "inmemory" => Ok(Self::InMemory),
            _ => Err(format!(
                "unknown BACKTEST_MODE '{s}' (expected 'db' or 'in_memory')"
            )),
        }
    }
}

/// The lookback window — either a time range or a fixed bar count.
#[derive(Debug, Clone)]
pub enum BacktestPeriod {
    /// Replay bars with `start <= time <= end`.
    TimeRange { start: DateTime<Utc>, end: DateTime<Utc> },
    /// Replay the last `n` bars (most recent first in the DB, reversed to ASC).
    NumBars(usize),
}

/// Parameters for a single backtest run. Built via [`BacktestConfig::new`] +
/// the fluent setters, or [`BacktestConfig::from_env`].
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    /// Bar granularity for stocks (default 5 min).
    pub stock_bar_interval: Duration,
    /// Bar granularity for FOREX pairs (default 1 min).
    pub forex_bar_interval: Duration,
    /// The lookback window (time range or bar count).
    pub period: BacktestPeriod,
    /// Starting SGD cash balance for the simulated account.
    pub starting_capital_sgd: f64,
    /// Slippage applied to market fills, in basis points (1bp = 0.01%).
    pub slippage_bps: f64,
    /// IBKR Pro commission model (Fixed or Tiered).
    pub commission_model: CommissionModel,
    /// DB-backed or in-memory execution.
    pub mode: BacktestMode,
    /// Contracts whose historical bars the replayer will stream.
    pub subscribed_contracts: Vec<Contract>,
    /// Where to write the JSON results.
    pub output_path: String,
    /// Generic per-strategy params, keyed by strategy name → param name →
    /// value. Parsed from `<STRATEGY>_<VAR>` env vars (e.g.
    /// `NOISE_DAILY_VOL_THRESHOLD=0.05` → `["noise"]["daily_vol_threshold"] =
    /// 0.05`). Strategies read their params from here (cfg-gated; fall back to
    /// hardcoded values if unset).
    pub strategy_params: HashMap<String, HashMap<String, f64>>,
}

impl BacktestConfig {
    /// Start a builder with `starting_capital_sgd` + sensible defaults:
    /// 5-min stock bars, 1-min FOREX bars, 1000 bars, 0 slippage, Tiered
    /// commissions, DB mode, QQQ/NASDAQ/USD, `backtest_results.json`, no
    /// strategy params.
    pub fn new(starting_capital_sgd: f64) -> Self {
        Self {
            stock_bar_interval: Duration::minutes(5),
            forex_bar_interval: Duration::minutes(1),
            period: BacktestPeriod::NumBars(1000),
            starting_capital_sgd,
            slippage_bps: 0.0,
            commission_model: CommissionModel::Tiered,
            mode: BacktestMode::Db,
            subscribed_contracts: vec![build_contract_from_stock(
                &"QQQ".to_string(),
                &"NASDAQ".to_string(),
                &"USD".to_string(),
            )],
            output_path: "backtest_results.json".to_string(),
            strategy_params: HashMap::new(),
        }
    }

    /// Bar granularity for stocks (e.g., `Duration::minutes(5)`).
    pub fn stock_bar_interval(mut self, d: Duration) -> Self {
        self.stock_bar_interval = d;
        self
    }

    /// Bar granularity for FOREX pairs (e.g., `Duration::minutes(1)`).
    pub fn forex_bar_interval(mut self, d: Duration) -> Self {
        self.forex_bar_interval = d;
        self
    }

    /// The lookback window — a time range or a bar count.
    pub fn period(mut self, p: BacktestPeriod) -> Self {
        self.period = p;
        self
    }

    /// Slippage in basis points (1bp = 0.01%).
    pub fn slippage_bps(mut self, bps: f64) -> Self {
        self.slippage_bps = bps;
        self
    }

    /// IBKR Pro commission model (Fixed or Tiered).
    pub fn commission_model(mut self, m: CommissionModel) -> Self {
        self.commission_model = m;
        self
    }

    /// DB-backed or in-memory execution.
    pub fn mode(mut self, m: BacktestMode) -> Self {
        self.mode = m;
        self
    }

    /// Contracts whose bars to replay (default: QQQ/NASDAQ/USD).
    pub fn contracts(mut self, c: Vec<Contract>) -> Self {
        self.subscribed_contracts = c;
        self
    }

    /// Where to write the JSON results.
    pub fn output_path(mut self, path: impl Into<String>) -> Self {
        self.output_path = path.into();
        self
    }

    /// Generic per-strategy params (keyed by strategy name → param name →
    /// value). Replaces any existing params.
    pub fn strategy_params(mut self, params: HashMap<String, HashMap<String, f64>>) -> Self {
        self.strategy_params = params;
        self
    }

    /// Build from environment variables. `BACKTEST_NUM_BARS` (if set) takes
    /// precedence over `BACKTEST_START`/`BACKTEST_END` for the lookback window.
    /// Strategy params are parsed from `<STRATEGY>_<VAR>` env vars (currently
    /// `NOISE_<VAR>` → `strategy_params["noise"][<var>]`).
    pub fn from_env() -> Result<Self, String> {
        let stock = env::var("BACKTEST_STOCK").unwrap_or_else(|_| "QQQ".to_string());
        let primary_exchange =
            env::var("BACKTEST_PRIMARY_EXCHANGE").unwrap_or_else(|_| "NASDAQ".to_string());
        let currency = env::var("BACKTEST_CURRENCY").unwrap_or_else(|_| "USD".to_string());
        let contract = build_contract_from_stock(&stock, &primary_exchange, &currency);

        let period = if let Ok(n) = env::var("BACKTEST_NUM_BARS") {
            BacktestPeriod::NumBars(n.parse::<usize>().map_err(|e| format!("BACKTEST_NUM_BARS: {e}"))?)
        } else {
            let start = env::var("BACKTEST_START")
                .map_err(|_| "BACKTEST_START (RFC3339) or BACKTEST_NUM_BARS required".to_string())
                .and_then(|s| DateTime::parse_from_rfc3339(&s).map_err(|e| format!("BACKTEST_START: {e}")))
                .map(|dt| dt.with_timezone(&Utc))?;
            let end = env::var("BACKTEST_END")
                .map_err(|_| "BACKTEST_END (RFC3339) or BACKTEST_NUM_BARS required".to_string())
                .and_then(|s| DateTime::parse_from_rfc3339(&s).map_err(|e| format!("BACKTEST_END: {e}")))
                .map(|dt| dt.with_timezone(&Utc))?;
            BacktestPeriod::TimeRange { start, end }
        };

        let stock_bar_interval = env::var("BACKTEST_STOCK_BAR_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .map(Duration::seconds)
            .unwrap_or_else(|| Duration::minutes(5));
        let forex_bar_interval = env::var("BACKTEST_FOREX_BAR_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .map(Duration::seconds)
            .unwrap_or_else(|| Duration::minutes(1));
        let starting_capital_sgd = env::var("BACKTEST_CAPITAL")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(100_000.0);
        let slippage_bps = env::var("BACKTEST_SLIPPAGE_BPS")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let commission_model = env::var("BACKTEST_COMMISSION_MODEL")
            .ok()
            .map(|s| CommissionModel::from_str(&s))
            .transpose()?
            .unwrap_or(CommissionModel::Tiered);
        let mode = env::var("BACKTEST_MODE")
            .ok()
            .map(|s| BacktestMode::from_str(&s))
            .transpose()?
            .unwrap_or(BacktestMode::Db);
        let output_path = env::var("BACKTEST_OUTPUT")
            .unwrap_or_else(|_| "backtest_results.json".to_string());

        // Generic strategy params: scan for <STRATEGY>_<VAR> env vars.
        // Currently NOISE_<VAR> → strategy_params["noise"][<var>].
        // Case-insensitive prefix; param name lowercased.
        let mut strategy_params: HashMap<String, HashMap<String, f64>> = HashMap::new();
        for (key, value) in env::vars() {
            let upper = key.to_uppercase();
            if let Some(rest) = upper.strip_prefix("NOISE_") {
                let param_name = rest.to_lowercase();
                match value.parse::<f64>() {
                    Ok(v) => {
                        strategy_params
                            .entry("noise".to_string())
                            .or_default()
                            .insert(param_name, v);
                    }
                    Err(_) => tracing::warn!(
                        "backtest: could not parse {}={} as f64; skipping",
                        key,
                        value
                    ),
                }
            }
        }

        Ok(Self {
            stock_bar_interval,
            forex_bar_interval,
            period,
            starting_capital_sgd,
            slippage_bps,
            commission_model,
            mode,
            subscribed_contracts: vec![contract],
            output_path,
            strategy_params,
        })
    }
}
