//! Strategy implementations and the runtime strategy factory.
//!
//! Each submodule is one strategy. All of them implement
//! [`StrategyExecutor`](crate::strategy::strategy::StrategyExecutor) and are
//! wrapped into [`StrategyEnum`](crate::strategy::strategy::StrategyEnum)
//! before being handed to `init_app`, the order engine or the backtester.
//!
//! # Backtest specs
//!
//! With `feature = "backtest"`, `StrategySpec` is the per-strategy section of
//! `backtest.json` and `construct_strategy` turns it into a concrete
//! [`StrategyEnum`](crate::strategy::strategy::StrategyEnum).
//! The JSON key (the strategy name) is passed separately so one spec can be
//! reused for optimisation sweeps.
//!
//! # Registration
//!
//! Live registration lives in `init_app`; the backtester constructs strategies
//! from specs. A name that is neither registered nor known to
//! `construct_strategy` yields `None` with a warning, never a panic.

pub mod noise;
pub mod strategy;
pub mod manual;
mod portfolio_functions;
pub mod unknown;

pub mod helpers;

// ─── Backtest strategy spec ──────────────────────────────────────────────
// The JSON-defined per-strategy config (backtest.json). Lives in the
// strategy module (not the backtester) so the backtester can depend on it
// without a circular module dependency. The strategy NAME is the JSON key
// (flattened, like optimiser_params.json) — passed separately to
// `construct_strategy`.

/// One stock contract as written in `backtest.json` (symbol + primary
/// exchange + currency). Resolved to an ibapi `Contract` via
/// `build_contract_from_stock` for the subscribed set (bar loading) and to
/// `(symbol, primary_exchange)` for the relative strategy's instrument list.
/// One stock contract as written in `backtest.json` (symbol + primary
/// exchange + currency). Resolved to an ibapi `Contract` via
/// `build_contract_from_stock` for the subscribed set (bar loading) and to
/// `(symbol, primary_exchange)` for the relative strategy's instrument list.
#[cfg(feature = "backtest")]
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ContractEntry {
    /// Ticker symbol (e.g. `AAPL`).
    pub stock: String,
    /// Listing exchange (e.g. `NASDAQ`, `NYSE`, `ARCA`).
    pub primary_exchange: String,
    /// Trading currency (e.g. `USD`).
    pub currency: String,
}

/// The per-strategy section of `backtest.json`:
/// ```json
/// "noise": { "active": true, "contracts": [...], "params": { ... } }
/// ```
/// For the relative (RsRw) strategy, `contracts` is the traded instrument
/// list and `benchmark` is the benchmark contract (SPY by default).
/// `params` is a direct name→value map — NOT a distribution. The backtester
/// runs ONE backtest per active strategy with these exact values; the
/// optimizer is what sweeps distributions.
#[cfg(feature = "backtest")]
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct StrategySpec {
    /// Whether to run this strategy. Defaults to `true` if omitted.
    #[serde(default = "default_active")]
    pub active: bool,
    /// Directly-defined params (name → value). The strategy reads them via
    /// `with_backtest_params`, falling back to hardcoded defaults for any
    /// key not listed.
    #[serde(default)]
    pub params: std::collections::HashMap<String, f64>,
    /// Traded instruments. For `relative`, this is the strategy's instrument
    /// list (overrides the hardcoded default NVDA/TSLA/XOM/CAT). For
    /// `noise`/`orb` (single-contract strategies) this is the contract to
    /// replay; the binary also unions these into `subscribed_contracts` for
    /// bar loading. Empty → strategy uses its default contracts.
    #[serde(default)]
    pub contracts: Vec<ContractEntry>,
    /// Benchmark contract (relative strategy only). None → default SPY.
    /// NB: the relative strategy fetches the benchmark under primary_exchange
    /// "ARCA" (hardcoded in `warm_up_data`), so set this to "ARCA" to match
    /// the bars the binary loads.
    #[serde(default)]
    pub benchmark: Option<ContractEntry>,
}

#[cfg(feature = "backtest")]
fn default_active() -> bool {
    true
}


/// Construct a strategy from a `StrategySpec`. The strategy NAME (the JSON
/// key) is passed separately.
///
/// Returns `None` for unknown or retired strategies (a warning is logged).
/// For multi-instrument strategies, `spec.contracts` (when non-empty) sets
/// the traded list, `spec.benchmark` (default `SPY`) sets the benchmark, and
/// `spec.params` is applied through `with_backtest_params`.
#[cfg(feature = "backtest")]
pub fn construct_strategy(
    name: &str,
    spec: &StrategySpec,
    pool: sqlx::PgPool,
    handle: tokio::runtime::Handle,
) -> Option<strategy::StrategyEnum> {
    match name {
        "noise" => Some(strategy::StrategyEnum::Noise(
            noise::Noise::new(pool, handle).with_backtest_params(spec.params.clone()),
        )),
        other => {
            tracing::warn!("Unknown strategy '{other}' - skipping");
            None
        }
    }
}
