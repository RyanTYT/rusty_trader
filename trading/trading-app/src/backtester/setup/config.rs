//! Backtest configuration — the user-facing interface layer. Holds the key
//! backtest parameters (bar granularity per asset type, lookback period,
//! initial capital, fees, commission model, mode, + the per-strategy specs
//! parsed from `backtest.json`) + a fluent builder.
//!
//! Strategy specs are a `HashMap<String, StrategySpec>` keyed by strategy
//! name → the per-strategy JSON section. The `backtest` bin parses
//! `backtest.json` into this config (the `config` section → the scalar
//! fields below; the per-strategy sections → `strategies`). Each strategy
//! reads its params from its spec (cfg-gated; falls back to hardcoded
//! defaults for unset keys).
//!
//! The `backtest` bin builds a `BacktestConfig` via the fluent builder + the
//! parsed JSON, then passes it to [`crate::backtester::run_backtest`].

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use ibapi::contracts::Contract;

use crate::backtester::execution::fill_model::CommissionModel;
use crate::helpers::contract::build_contract_from_stock;
use crate::strategy::StrategySpec;

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
                "unknown backtest mode '{s}' (expected 'db' or 'in_memory')"
            )),
        }
    }
}

/// The lookback window — either a time range or a fixed bar count.
#[derive(Debug, Clone)]
pub enum BacktestPeriod {
    /// Replay bars with `start <= time <= end`.
    TimeRange {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
    /// Replay the last `n` bars (most recent first in the DB, reversed to ASC).
    NumBars(usize),
}

/// Parameters for a single backtest run. Built via [`BacktestConfig::new`] +
/// the fluent setters. The `backtest` bin parses `backtest.json` and uses
/// the builder; the optimizer builds a base config the same way.
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
    /// Contracts whose historical bars the replayer will stream — the union
    /// of all active strategies' contracts + benchmarks, built by the
    /// `backtest` bin from `backtest.json`.
    pub subscribed_contracts: Vec<Contract>,
    /// Where to write the JSON results.
    pub output_path: String,
    /// Per-strategy specs, keyed by strategy name (the JSON key in
    /// `backtest.json`). The binary filters to `active` strategies and
    /// constructs each via `construct_strategy(name, &spec, …)`. Strategies
    /// read their params from `spec.params` (cfg-gated; fall back to
    /// hardcoded defaults for unset keys).
    pub strategies: HashMap<String, StrategySpec>,
}

impl BacktestConfig {
    /// Start a builder with `starting_capital_sgd` + sensible defaults:
    /// 5-min stock bars, 1-min FOREX bars, 1000 bars, 0 slippage, Tiered
    /// commissions, DB mode, QQQ/NASDAQ/USD, `backtest_results.json`, no
    /// strategies.
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
            strategies: HashMap::new(),
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

    /// Contracts whose bars to replay (default: QQQ/NASDAQ/USD). The
    /// `backtest` bin builds this as the union of all active strategies'
    /// contracts + benchmarks.
    pub fn contracts(mut self, c: Vec<Contract>) -> Self {
        self.subscribed_contracts = c;
        self
    }

    /// Set the stock contract (symbol, primary exchange, currency). A
    /// convenience over `.contracts(vec![...])` — builds the contract via
    /// `build_contract_from_stock` (pub(crate), so not directly accessible
    /// from the optimizer crate).
    pub fn stock(
        mut self,
        stock: impl Into<String>,
        primary_exchange: impl Into<String>,
        currency: impl Into<String>,
    ) -> Self {
        let contract = crate::helpers::contract::build_contract_from_stock(
            &stock.into(),
            &primary_exchange.into(),
            &currency.into(),
        );
        self.subscribed_contracts = vec![contract];
        self
    }

    /// Where to write the JSON results.
    pub fn output_path(mut self, path: impl Into<String>) -> Self {
        self.output_path = path.into();
        self
    }

    /// Per-strategy specs (keyed by strategy name → spec). Replaces any
    /// existing specs.
    pub fn strategies(mut self, strategies: HashMap<String, StrategySpec>) -> Self {
        self.strategies = strategies;
        self
    }
}
