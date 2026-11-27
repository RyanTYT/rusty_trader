//! Strategy traits and the runtime strategy enum.
//!
//! Every strategy implements [`StrategyExecutor`]: a name, its contract set, a
//! [`StrategyDetails`] identity, a [`StrategyFund`] allocation, a per-bar
//! callback returning a [`BarUpdateOutcome`], a warm-up pass and an optional
//! fill hook. [`StrategyEnum`] is the generated runtime union used by
//! `init_app` and the backtester, so both hot paths can dispatch without
//! generics.
//!
//! # Bar contract
//!
//! `on_bar_update` is synchronous and must not perform database or network I/O.
//! It either emits orders directly (fast path), asks the engine to compare
//! targets with current positions (slow path), or does nothing. All three are
//! expressed by [`BarUpdateOutcome`].
//!
//! # Funding
//!
//! [`StrategyFund`] describes a strategy's claim on the shared cash pool:
//! weight, priority, minimum capital and accepted currencies. `Ord` is defined
//! by `priority` only, so equal weights tie-break deterministically on priority.

use std::{hash::Hash, sync::Arc};

use chrono::{DateTime, Utc};
use ibapi::{Client, orders::ExecutionData, prelude::Contract};

use crate::{
    database::{
        models::AssetType,
        models_crud::{
            historical_data::historical_data::HistoricalDataFullKeys,
            open_orders::open_orders::OpenOrdersFullKeys,
        },
    },
    execution::order_engine::OrderIBKR,
    market_data::consolidator::Consolidator,
    strategy::{
        manual::Manual,
        noise::Noise,
        unknown::Unknown,
    },
};

/// What a strategy wants the engine to do after seeing one bar.
///
/// The fast path carries fully-formed orders; the slow path asks the engine to
/// read the database (targets vs current positions) before deciding; `NoAction`
/// is the no-op tick.
#[derive(Debug, Clone)]
pub enum BarUpdateOutcome {
    /// Fast Path: In-memory evaluation ready to submit orders immediately.
    EmitOrders {
        place_orders: Vec<OrderIBKR>,
        cancel_orders: Vec<OpenOrdersFullKeys>,
    },
    /// Slow Path: Strategy requires target position V current position
    /// to determine market action.
    PendingDbQuery(Vec<AssetType>),
    /// No action required for this bar tick.
    NoAction,
}

/// Identity and behaviour flags of a strategy.
///
/// `Ord` is derived over `(priority, name, is_fx_strategy, hooks_execution)`,
/// but the syncer only relies on the full ordering to pick a deterministic
/// winner when several strategies claim the same contract (smallest wins).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StrategyDetails {
    /// Allocation priority; lower runs first. Private to keep ordering explicit.
    priority: i32,
    /// Unique strategy name; used as the DB key and the metric label prefix.
    pub name: String,
    /// Whether `CASH:` rows are meaningful targets for this strategy.
    pub is_fx_strategy: bool,
    /// Whether fills should be forwarded to [`StrategyExecutor::on_fill`].
    pub hooks_execution: bool,
}

impl StrategyDetails {
    /// Builds the details record. `priority` is intentionally not readable —
    /// ordering is the only thing it is for.
    pub fn new(priority: i32, name: String, is_fx_strategy: bool, hooks_execution: bool) -> Self {
        Self {
            priority,
            name,
            is_fx_strategy,
            hooks_execution,
        }
    }
}

/// A strategy's claim on the shared cash pool.
///
/// Equality and ordering are defined by `priority` alone; the other fields are
/// allocation inputs, not identity. `Default` is the "never allocated" value
/// (`priority = usize::MAX`, zero weight).
#[derive(Debug, Clone)]
pub struct StrategyFund {
    /// Tie-break among equal weights. Lower runs first.
    pub priority: usize,
    pub to_reallocate: bool,
    /// Absolute share of the whole fund, in `[0, 1]`.
    pub weight: f64,
    /// Rationed-target floor. `<= 0` never knocks the strategy out.
    pub min_capital: f64,
    /// Currencies accepted as funding. Empty = accept anything (hub only).
    pub cash_prefs: Vec<String>,
}

impl PartialEq for StrategyFund {
    fn eq(&self, other: &StrategyFund) -> bool {
        self.priority == other.priority
    }
}
impl Eq for StrategyFund {}

impl PartialOrd for StrategyFund {
    fn partial_cmp(&self, other: &StrategyFund) -> Option<std::cmp::Ordering> {
        Some(self.priority.cmp(&other.priority))
    }
}
impl Ord for StrategyFund {
    fn cmp(&self, other: &StrategyFund) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl Default for StrategyFund {
    fn default() -> StrategyFund {
        StrategyFund {
            priority: usize::MAX,
            to_reallocate: false,
            weight: 0.0,
            min_capital: 0.0,
            cash_prefs: vec![],
        }
    }
}

/// The behaviour every strategy must provide.
///
/// Contract:
/// - [`StrategyExecutor::on_bar_update`] is synchronous and must not await or
///   touch the database; expensive work belongs in warm-up or in the engine's
///   slow path.
/// - The name returned by [`StrategyExecutor::get_name`] is the DB key and the
///   metric-label prefix; it must be unique and stable.
/// - `warm_up_data` runs once before live trading / replay and returns the
///   timestamp the strategy is warm through.
#[async_trait::async_trait]
pub trait StrategyExecutor: Send + Sync {
    /// NEEDS TO BE DEFINED CORRECTLY
    /// Usually for initialisation and storing of the relevant contracts for each strategy
    // fn new(pool: PgPool) -> Self;
    /// Should return a unique name for the DB table for coordination and tracking - the main
    /// reason for this whole app
    /// Unique strategy name: the `trading.strategy` key and the metric prefix.
    fn get_name(&self) -> String;
    /// Returns the contracts this strategy subscribes to.
    ///
    /// The client is passed so implementations can resolve option chains or
    /// contract details; the returned set becomes the strategy's ring
    /// subscriptions.
    fn get_contracts(&mut self, client: Arc<Client>) -> Vec<Contract>;
    /// Should return the [`StrategyDetails`] for this strategy:
    ///
    /// ```text
    /// StrategyDetails {
    ///     name: String,           // unique strategy name
    ///     is_fx_strategy: bool,   // true if the strategy trades FX pairs
    /// }
    /// ```
    fn get_strategy_details(&self) -> StrategyDetails;
    /// Funding of the strategy
    /// - weight: weight to be allocated to the strategy from overall cash
    /// - min_capital: minimum capital to be allocated, else -> 0 allocation
    /// - priority
    /// - to_reallocate: if true - participates in reallocation
    ///
    /// Funding works as follows: all cash of the participating strategies is in
    /// one pool. For each priority level (0 highest), the weights are summed and
    /// the excess is split:
    ///
    /// ```text
    /// strategy_a -> 40%
    /// strategy_b -> 70%
    /// available pool -> 100%
    ///
    /// difference is 10% -> split across
    /// strategy_a -> 35%
    /// strategy_b -> 65%
    /// ```
    ///
    /// Note that if higher priority strategies weight allocations are very high,
    /// weight allocations might result in lower priority strategies not being
    /// allocated at all.
    fn fund_strategy(&mut self) -> StrategyFund;

    /// Handles one completed bar for one contract.
    ///
    /// Must be synchronous and free of I/O: read only from memory, return
    /// [`BarUpdateOutcome::PendingDbQuery`] when a DB comparison is needed.
    ///
    /// # Errors
    ///
    /// Return `Err` only for programming/configuration errors; the producer logs
    /// and continues with the next bar.
    fn on_bar_update(
        &mut self,
        contract: &Contract,
        bar: &HistoricalDataFullKeys,
        consolidator: &Arc<Consolidator>,
    ) -> Result<BarUpdateOutcome, String>;
    /// Loads the history the strategy needs before it can react.
    ///
    /// Returns the timestamp the strategy is warm through. In a backtest the
    /// `bar_time` is the replay cursor; live code ignores it.
    ///
    /// # Errors
    ///
    /// Returns `Err` when required history cannot be loaded; startup aborts
    /// rather than running a strategy on incomplete data.
    async fn warm_up_data(
        &mut self,
        consolidator: &Arc<Consolidator>,
        #[cfg(feature = "backtest")] bar_time: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, String>;

    /// Number of bars this strategy's warm-up consumes.
    ///
    /// The backtester loads this many extra prefix bars before the replay window
    /// so warm-up does not eat into the tested period.
    fn warmup_bars_required(&self) -> usize;

    /// Optional hook invoked once per applied execution.
    ///
    /// Only called when [`StrategyDetails::hooks_execution`] is set, and only
    /// after the fill's database transaction has committed. The default is a
    /// no-op. Implementations must be idempotent: duplicate executions are
    /// filtered upstream, but retries must not double-count.
    fn on_fill(&mut self, _execution_data: ExecutionData) -> Result<(), String> {
        Ok(())
    }
}

// Define the macro to generate the enum and impl
macro_rules! strategy_enum {
    ($($variant:ident($type:ty)),* $(,)?) => {
        #[derive(Debug)]
        pub enum StrategyEnum {
            $($variant($type)),*
        }

        impl Hash for StrategyEnum {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                match self {
                    $(StrategyEnum::$variant(s) => s.get_name().hash(state)),*
                }
            }
        }

        #[async_trait::async_trait]
        impl StrategyExecutor for StrategyEnum {
            fn get_name(&self) -> String {
                match self {
                    $(StrategyEnum::$variant(s) => s.get_name()),*
                }
            }

            fn fund_strategy(&mut self) -> StrategyFund {
                match self {
                    $(StrategyEnum::$variant(s) => s.fund_strategy()),*
                }
            }

            fn get_contracts(&mut self, client: Arc<Client>) -> Vec<Contract> {
                match self {
                    $(StrategyEnum::$variant(s) => s.get_contracts(client)),*
                }
            }

            fn get_strategy_details(&self) -> StrategyDetails {
                match self {
                    $(StrategyEnum::$variant(s) => s.get_strategy_details()),*
                }
            }

            fn on_bar_update(
                &mut self,
                contract: &Contract,
                bar: &HistoricalDataFullKeys,
                consolidator: &Arc<Consolidator>,
            ) -> Result<BarUpdateOutcome, String>
            {
                match self {
                    $(StrategyEnum::$variant(s) => s.on_bar_update(contract, bar, consolidator)),*
                }
            }

            async fn warm_up_data(
                &mut self,
                consolidator: &Arc<Consolidator>,
                #[cfg(feature = "backtest")] bar_time: DateTime<Utc>,
            ) -> Result<DateTime<Utc>, String>
            {
                match self {
                    $(StrategyEnum::$variant(s) => s.warm_up_data(
                            consolidator,
                            #[cfg(feature = "backtest")] bar_time,
                     ).await),*
                }
            }

            fn warmup_bars_required(&self) -> usize {
                match self {
                    $(StrategyEnum::$variant(s) => s.warmup_bars_required()),*
                }
            }

            fn on_fill(&mut self, execution_data: ExecutionData) -> Result<(), String> {
                match self {
                    $(StrategyEnum::$variant(s) => s.on_fill(execution_data)),*
                }
            }
        }
    };
}

#[cfg(feature = "backtest")]
strategy_enum! {
    Noise(Noise),
    Manual(Manual),
    Unknown(Unknown),
}

#[cfg(not(feature = "backtest"))]
strategy_enum! {
    Noise(Noise),
    Manual(Manual),
    Unknown(Unknown),
}

/// Generates a `match (strategy_name, label) { … }` expression mapping a
/// (strategy, metric) pair to its `&'static str` label, without `Box::leak`.
/// TT-muncher cartesian product of the strategy-name and label-name lists.
macro_rules! metric_label_match {
    // Entry: kick off the accumulator with empty arms.
    ($sn:expr, $lb:expr, [$($s:literal),* $(,)?], [$($l:literal),* $(,)?]) => {
        metric_label_match!(@step $sn, $lb, [$($s),*], [$($l),*], {})
    };
    // Base: strats exhausted — emit the match with accumulated arms inlined.
    (@step $sn:expr, $lb:expr, [], $l:tt, {$($acc:tt)*}) => {
        match ($sn, $lb) { $($acc)* _ => "other" }
    };
    // Recursive: peel one strat, broadcast across all labels (single-level — no
    // lockstep), append the arms to the accumulator, recurse with the tail.
    (@step $sn:expr, $lb:expr, [$s:literal $(, $t:literal)*], [$($l:literal),*], {$($acc:tt)*}) => {
        metric_label_match!(@step $sn, $lb, [$($t),*], [$($l),*], { $($acc)* $( ($s, $l) => concat!($s, "_", $l), )* })
    };
}

impl StrategyEnum {
    /// Returns the `&'static str` label for a per-strategy `measure_block!` metric.
    ///
    /// `strategy_name` MUST exactly match the strategy's `.get_name()` return value
    /// — one of the strategy names in the `metric_label_match!` invocation below
    /// (currently `"noise"` / `"manual"` / `"unknown"`, set in each strategy's
    /// `new()`). It comes from `order.order_ref`'s `:`-prefix (stock
    /// `order_ref == strategy`; FX `order_ref == "{strategy}:{price}"`, so
    /// `split(':').next()` covers both). **If a new strategy is added, add its
    /// exact `.get_name()` string to the strategy-name list in the
    /// `metric_label_match!` invocation below.**
    ///
    /// `label` is one of the label names in the `metric_label_match!` invocation
    /// (currently `"order_submit_total"`, `"order_submit_ibkr_wire"`,
    /// `"get_target_pos_diff_by_strat"`, `"compute_fx_attachments"`).
    ///
    /// Unknown `(strategy_name, label)` pairs (contract violation — caller passed
    /// a name that doesn't match `.get_name()`, or a typo'd `label`) return
    /// `"other"` — a visible bug signal (offending metrics aggregate).
    pub fn metric_label(strategy_name: &str, label: &str) -> &'static str {
        metric_label_match!(
            strategy_name,
            label,
            [
                "noise",
                "manual",
                "unknown",
            ],
            [
                "order_submit_total",
                "order_submit_ibkr_wire",
                "get_target_pos_diff_by_strat",
                "compute_fx_attachments"
            ]
        )
    }
}
