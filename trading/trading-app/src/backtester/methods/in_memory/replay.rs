//! In-memory backtest method — runs the SAME prod strategy + a NEW in-memory
//! reconcile, with all state in [`InMemoryState`] (no DB I/O per bar). The
//! strategy's cfg-gated CRUD branches + the Consolidator's cfg-gated read
//! branch detect the thread-local `InMemoryState` + operate on it instead of
//! the DB. The reconcile ([`handle_bar_update_outcome_in_memory`]) reads the
//! mocked targets + adjusts positions/transactions/cash in-memory.
//!
//! ~10-100x faster than `HistoricalReplay` (eliminates all per-bar DB
//! round-trips). The only DB I/O is the one-time `load_bars` query done by
//! the caller (the single route / sweep).
//!
//! Two entry points:
//! - [`InMemoryReplay::run_with_warm_up`] — the full flow: set the bar cache,
//!   `warm_up_data`, trim to post-warm-up, `run_with_bars`, clear the cache.
//!   Shared by the single route + the sweep (`run_one_backtest`).
//! - [`InMemoryReplay::run_with_bars`] — the bar loop only (called by
//!   `run_with_warm_up`).

use std::collections::HashMap;
use std::sync::Arc;

use ibapi::contracts::Contract;

use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::helpers::sentinel::Sentinel;
use crate::market_data::consolidator::Consolidator;
use crate::market_data::traits::current_price::PriceSupplier;
use crate::strategy::strategy::{StrategyDetails, StrategyEnum, StrategyExecutor};

use crate::backtester::oracle::price_supplier::BacktestPriceSupplier;
use crate::backtester::output::equity::EquityCurve;
use crate::backtester::setup::clock::BacktestClock;
use crate::backtester::setup::config::BacktestConfig;
#[cfg(feature = "backtest")]
use crate::backtester::setup::config::BacktestPeriod;
use crate::backtester::setup::context::LightContext;

use crate::backtester::methods::in_memory::bar_cache;
use crate::backtester::methods::in_memory::reconcile::{
    drain_resting_orders_fill_first, handle_bar_update_outcome_in_memory,
};
use crate::backtester::methods::in_memory::state;
use crate::backtester::methods::in_memory::state::InMemoryState;

/// Lighter context for the in-memory run — only the pieces the bar loop
/// needs (no broker/order_engine/order_store, which are unused in-memory +
/// whose construction would contend on `OrderStore::open()` across parallel
/// sweep backtests). Built per-backtest by the sweep runner / single route.
pub struct InMemoryRunContext<'a> {
    /// Backtest configuration (capital, contracts, period).
    pub config: &'a BacktestConfig,
    /// The warmed strategy to replay.
    pub strategy: StrategyEnum,
    /// Runtime used for the `block_on` wait on `warm_up_data`.
    pub handle: &'a tokio::runtime::Handle,
    /// Clock the strategy and reconcile read.
    pub clock: &'a BacktestClock,
    /// Per-contract close cache.
    pub prices: &'a BacktestPriceSupplier,
    /// Read facade used by the strategy.
    pub consolidator: &'a Arc<Consolidator>,
    /// The strategy's details (hooks flag, name, FX flag).
    pub strategy_details: &'a StrategyDetails,
}

/// In-memory backtest method. Unit struct — no state; the `InMemoryState` is
/// created + threaded (via the thread-local) inside `run_with_bars`.
pub struct InMemoryReplay;

impl InMemoryReplay {
    /// The full in-memory backtest flow (shared by the single route + the
    /// sweep): set the bar cache → `warm_up_data` (block_on) → trim the bars
    /// to post-warm-up (linear-scan the short prefix + take the suffix slice,
    /// no clone) → `run_with_bars` → clear the cache.
    ///
    /// `bars` is the full pre-loaded bar set (shared via `Arc`); the cache
    /// reads the lookback from it + tracks `max_end_time` for the trim.
    ///
    /// # Errors
    ///
    /// - the period is [`BacktestPeriod::NumBars`] (warm-up needs a window);
    /// - there are fewer than `warmup_bars_required()` bar slots;
    /// - `warm_up_data` fails.
    ///
    /// # Panics
    ///
    /// A replay timestamp row contains no bars (`this_time.unwrap()`).
    pub fn run_with_warm_up(
        &self,
        strategy: StrategyEnum,
        bars: Arc<Vec<Vec<Option<HistoricalDataFullKeys>>>>,
        config: &BacktestConfig,
        handle: &tokio::runtime::Handle,
        light: &LightContext,
    ) -> Result<(EquityCurve, Arc<InMemoryState>), String> {
        // Set the bar cache so warm_up_data's `read_last_n` reads from the
        // cache (no DB) + tracks `max_end_time` for the post-warm-up trim.
        let cache = Arc::new(bar_cache::BarCache::new(bars.clone()));
        bar_cache::set(cache.clone());

        // bar_time = the first TEST timestamp (config.start + warmup_bars_required
        // bars forward). warm_up_data reads its lookback FORWARD from the dataset
        // start (read_last_n now=Some(bar_time)), and the replay begins right after
        // bar_time. config.start is the dataset start, not the test start.
        #[cfg(feature = "backtest")]
        let bar_time = match &config.period {
            BacktestPeriod::TimeRange { .. } => {
                let n = strategy.warmup_bars_required();
                bars.get(n)
                    .and_then(|slot| slot.iter().find_map(|b| b.as_ref()))
                    .map(|b| b.get_time())
                    .ok_or_else(|| {
                        format!("insufficient bars for the warmup lookback (need {n})")
                    })?
            }
            BacktestPeriod::NumBars(_) => {
                return Err(
                    "run_with_warm_up: NumBars period is not supported with warmup_bars (use TimeRange)"
                        .to_string(),
                );
            }
        };

        let mut strategy = strategy;
        let strategy_details = strategy.get_strategy_details();
        let warmup_end = handle
            .block_on(strategy.warm_up_data(
                &light.consolidator,
                #[cfg(feature = "backtest")]
                bar_time,
            ))
            .map_err(|e| format!("warm_up_data: {e}"))?;

        // Trim the bars to post-warm-up. The bars are sorted ascending, so
        // the warm-up window (bars <= max_end_time) is a short prefix —
        // linear-scan from the front (cache-friendly) + take the suffix as a
        // slice (no clone, no data leakage — the rolling fns saw the prefix).
        let trimmed: &[Vec<Option<HistoricalDataFullKeys>>] = match cache.max_end_time() {
            Some(t) => {
                let split = bars
                    .iter()
                    .position(|bar| {
                        bar.iter()
                            .any(|b| b.as_ref().is_some_and(|bar| bar.get_time() > warmup_end))
                    })
                    .unwrap_or(0);
                &bars[split..]
            }
            None => &bars[..],
        };

        let in_mem_ctx = InMemoryRunContext {
            config,
            strategy,
            handle,
            clock: light.clock.as_ref(),
            prices: light.prices.as_ref(),
            consolidator: &light.consolidator,
            strategy_details: &strategy_details,
        };
        let result = self.run_with_bars(in_mem_ctx, trimmed);

        bar_cache::clear();
        result
    }

    /// Run the bar loop with pre-loaded shared bars. Returns the equity curve
    /// + the `InMemoryState` (the sweep uses the state for results).
    ///
    /// For each timestamp row: publish the clock and close to every contract,
    /// run the real `on_bar_update`, reconcile the outcome against the
    /// in-memory state (no DB, no broker), run the bucket-end sentinel, then
    /// snapshot equity from the state. The thread-local state is installed
    /// before the loop and cleared after it; the returned `Arc` remains valid.
    ///
    /// # Panics
    ///
    /// A bar row's contract index does not align with
    /// `config.subscribed_contracts` (the two `.expect`s in the loop), or a
    /// timestamp row contains no bars.
    pub fn run_with_bars(
        &self,
        ctx: InMemoryRunContext,
        bars: &[Vec<Option<HistoricalDataFullKeys>>],
    ) -> Result<(EquityCurve, Arc<InMemoryState>), String> {
        // 1. Create + seed the in-memory state (CASH:SGD = starting capital).
        let strat_name = ctx.strategy.get_name();
        let mut strategy = ctx.strategy;
        let state = Arc::new(InMemoryState::new(
            strat_name.clone(),
            ctx.config.starting_capital_sgd,
        ));

        // 2. Set the thread-local so the strategy's cfg-gated CRUD branches +
        //    the Consolidator's read branch use the InMemoryState.
        state::set(state.clone());

        // 3. Replay.
        // let contract: Contract = ctx
        //     .config
        //     .subscribed_contracts
        //     .first()
        //     .cloned()
        //     .expect("BacktestConfig.subscribed_contracts must be non-empty");
        let mut equity = EquityCurve::new();
        let mut order_id: i32 = 0;
        for contracts in bars {
            let mut this_time = None;
            for idx in 0..contracts.len() {
                let opt_bar = contracts
                    .get(idx)
                    .expect("Expected idx from contracts to be within bounds");
                let contract = ctx
                    .config
                    .subscribed_contracts
                    .get(idx)
                    .expect("Expected contract aligned with opt_bar in replay");
                // for opt_bar in contracts {
                let bar = match opt_bar {
                    Some(v) => v,
                    None => continue,
                };
                let time = bar.get_time();
                this_time = Some(time);
                ctx.clock.set(time);
                let close = bar.get_price();
                ctx.prices.publish_close(&contract, close);

                // --- REAL prod on_bar_update (cfg-gated branches write to
                //     InMemoryState instead of the DB) ---
                let outcome = strategy
                    .on_bar_update(&contract, bar, ctx.consolidator)
                    .unwrap_or_else(|e| {
                        tracing::error!("on_bar_update error: {e:?}");
                        crate::strategy::strategy::BarUpdateOutcome::NoAction
                    });

                // --- NEW in-memory reconcile (no DB, no broker) ---
                if let Err(e) = handle_bar_update_outcome_in_memory(
                    ctx.config,
                    ctx.strategy_details
                        .hooks_execution
                        .then_some(&mut strategy),
                    ctx.prices as &dyn PriceSupplier,
                    &state,
                    &outcome,
                    &contract,
                    bar,
                    contracts,
                    &mut order_id,
                ) {
                    tracing::error!("InMemory reconcile error: {e:?}");
                }
            }

            // Bucket-end sentinel (engine convention — helpers::sentinel): this
            // bucket's bars are done arriving. Blended5 sleeves flush the
            // pending row on it (emission at completion — user directive
            // 2026-11-13); every other strategy ignores the sentinel symbol.
            // The emitted outcome reconciles IMMEDIATELY, with this bucket's
            // bars as the fill context.
            let sentinel_contract = Contract::sentinel();
            let sentinel_bar = HistoricalDataFullKeys::sentinel();
            let outcome = strategy
                .on_bar_update(&sentinel_contract, &sentinel_bar, ctx.consolidator)
                .unwrap_or_else(|e| {
                    tracing::error!("on_bar_update error: {e:?}");
                    crate::strategy::strategy::BarUpdateOutcome::NoAction
                });
            if let Err(e) = handle_bar_update_outcome_in_memory(
                ctx.config,
                ctx.strategy_details
                    .hooks_execution
                    .then_some(&mut strategy),
                ctx.prices as &dyn PriceSupplier,
                &state,
                &outcome,
                &sentinel_contract,
                &sentinel_bar,
                contracts,
                &mut order_id,
            ) {
                tracing::error!("InMemory reconcile error: {e:?}");
            }

            // 4. Equity snapshot (read directly from the InMemoryState — no
            //    DB, no `block_on`).
            let positions = state.current_positions_snapshot();
            let snap = crate::backtester::output::equity::compute_snapshot_from_positions(
                positions,
                ctx.prices as &dyn PriceSupplier,
                this_time.unwrap(),
                0.0,
            );
            equity.push(snap);
        }

        // 5. Clear the thread-locals (the Arc<InMemoryState> is returned, so the
        //    sweep can still compute results from it).
        state::clear();
        Ok((equity, state))
    }
}

// ── Multi-strategy single pass (§8/P1b) ───────────────────────────────────
// Each strategy runs in its OWN [`InMemoryState`] (own account). The
// thread-local is switched to each strategy's state before its
// `on_bar_update`, so the cfg-gated CRUD reads (equity, positions) see the
// owning account. The reconcile is called per strategy with that strategy's
// state + strategy ref, so fills key/order_ref to the OWNING strategy
// (`PositionKey.strategy` = `state.strategy_name`) — attribution by
// construction (E3, no reconcile.rs change needed).
//
// Composition-of-solos invariant: with one strategy per account, no
// cross-strategy reads, and the clock/price publish happening ONCE per bar
// (before any strategy sees it), the loop reproduces each strategy's solo
// run byte-for-byte (same per-bar ordering, same per-strategy order_id
// counters starting at 0).

/// Context for [`InMemoryReplay::run_multi_with_bars`] — one shared market
/// surface (clock/prices/consolidator), N strategies, N seeds.
pub struct InMemoryMultiContext<'a> {
    /// Backtest configuration shared by all strategies.
    pub config: &'a BacktestConfig,
    /// Strategies in DETERMINISTIC order (sorted by name at the call site).
    pub strategies: Vec<StrategyEnum>,
    /// Per-strategy starting capital (transfer seeding: each sleeve account
    /// is seeded independently; the caller's residual = config capital − Σ).
    pub seeds: Vec<f64>,
    /// Runtime used for the `block_on` waits.
    pub handle: &'a tokio::runtime::Handle,
    /// Clock read by every strategy.
    pub clock: &'a BacktestClock,
    /// Shared per-contract close cache.
    pub prices: &'a BacktestPriceSupplier,
    /// Shared read facade.
    pub consolidator: &'a Arc<Consolidator>,
}

/// Output of the multi run: per-strategy curves + states (for results), the
/// constant residual cash, and the rollup curve (residual + Σ sleeve equity).
pub struct MultiRunOutput {
    /// Strategy names, in `InMemoryMultiContext::strategies` order.
    pub names: Vec<String>,
    /// One curve per strategy, same order.
    pub curves: Vec<EquityCurve>,
    /// Each strategy's final state (for post-run result computation).
    pub states: Vec<Arc<InMemoryState>>,
    /// `config.starting_capital_sgd − Σ seeds` (≥ 0, asserted at entry).
    pub residual: f64,
    /// Residual + Σ sleeve equity, one row per replay timestamp.
    pub rollup: EquityCurve,
}

impl InMemoryReplay {
    /// The full multi-strategy flow (DB-path entry, mirrors
    /// [`InMemoryReplay::run_with_warm_up`]): set the bar cache → warm each
    /// strategy at its OWN warmup count → trim to post-warm-up →
    /// [`InMemoryReplay::run_multi_with_bars`] → clear the cache.
    ///
    /// The trim uses the shared cache's `max_end_time`, so it starts after
    /// the deepest warm-up among all strategies.
    ///
    /// # Errors
    ///
    /// - the period is [`BacktestPeriod::NumBars`];
    /// - a strategy's warm-up count exceeds the available bar slots;
    /// - any strategy's `warm_up_data` fails.
    ///
    /// # Panics
    ///
    /// A replay timestamp row contains no bars.
    pub fn run_multi_with_warm_up(
        &self,
        strategies: Vec<StrategyEnum>,
        seeds: Vec<f64>,
        bars: Arc<Vec<Vec<Option<HistoricalDataFullKeys>>>>,
        config: &BacktestConfig,
        handle: &tokio::runtime::Handle,
        light: &LightContext,
    ) -> Result<MultiRunOutput, String> {
        let cache = Arc::new(bar_cache::BarCache::new(bars.clone()));
        bar_cache::set(cache.clone());

        // Per-strategy bar_time from each strategy's own warmup count (the
        // same hint the single-strategy path uses).
        #[cfg(feature = "backtest")]
        let bar_times: Vec<_> = match &config.period {
            BacktestPeriod::TimeRange { .. } => strategies
                .iter()
                .map(|s| {
                    let n = s.warmup_bars_required();
                    bars.get(n)
                        .and_then(|slot| slot.iter().find_map(|b| b.as_ref()))
                        .map(|b| b.get_time())
                        .ok_or_else(|| {
                            format!("insufficient bars for the warmup lookback (need {n})")
                        })
                })
                .collect::<Result<Vec<_>, String>>()?,
            BacktestPeriod::NumBars(_) => {
                return Err(
                    "run_multi_with_warm_up: NumBars period is not supported with warmup_bars"
                        .to_string(),
                );
            }
        };

        let mut strategies = strategies;
        for (i, s) in strategies.iter_mut().enumerate() {
            #[cfg(feature = "backtest")]
            let end = handle
                .block_on(s.warm_up_data(&light.consolidator, bar_times[i]))
                .map_err(|e| format!("warm_up_data: {e}"))?;
            #[cfg(not(feature = "backtest"))]
            let end = handle
                .block_on(s.warm_up_data(&light.consolidator))
                .map_err(|e| format!("warm_up_data: {e}"))?;
            let _ = end;
        }

        // Trim to post-warm-up (same linear scan as the single path; the
        // shared cache's max_end_time covers every strategy's warm reads).
        let trimmed: &[Vec<Option<HistoricalDataFullKeys>>] = match cache.max_end_time() {
            Some(t) => {
                let split = bars
                    .iter()
                    .position(|bar| {
                        bar.iter()
                            .any(|b| b.as_ref().is_some_and(|bar| bar.get_time() > t))
                    })
                    .unwrap_or(0);
                &bars[split..]
            }
            None => &bars[..],
        };

        let ctx = InMemoryMultiContext {
            config,
            strategies,
            seeds,
            handle,
            clock: light.clock.as_ref(),
            prices: light.prices.as_ref(),
            consolidator: &light.consolidator,
        };
        let result = self.run_multi_with_bars(ctx, trimmed);
        bar_cache::clear();
        result
    }

    /// Run the multi-strategy bar loop with pre-loaded shared bars. Each
    /// strategy gets its own seeded [`InMemoryState`]; the thread-local is
    /// switched per strategy call. Returns per-strategy curves + states, the
    /// residual cash, and the rollup curve.
    ///
    /// Composition-of-solos invariant: with one account per strategy and the
    /// clock/close published once per bar before any strategy sees it, each
    /// strategy's curve matches its solo run. Two deliberate differences from
    /// solo:
    ///
    /// - parked intents fill FIRST at each slot (before new bars are fed);
    /// - fills resolve their settlement state through the fill router by
    ///   `order_ref`, so a pooled fill lands in the owning strategy's account.
    ///
    /// Per-strategy `order_id` counters start at zero and advance
    /// independently. The rollup adds the residual to the summed curves.
    ///
    /// # Panics
    ///
    /// - `strategies.len() != seeds.len()`;
    /// - the seeds over-commit `starting_capital_sgd`;
    /// - a timestamp row contains no bars.
    pub fn run_multi_with_bars(
        &self,
        mut ctx: InMemoryMultiContext,
        bars: &[Vec<Option<HistoricalDataFullKeys>>],
    ) -> Result<MultiRunOutput, String> {
        assert_eq!(
            ctx.strategies.len(),
            ctx.seeds.len(),
            "run_multi_with_bars: strategies/seeds length mismatch"
        );

        let names: Vec<String> = ctx.strategies.iter().map(|s| s.get_name()).collect();
        let details: Vec<StrategyDetails> = ctx
            .strategies
            .iter()
            .map(|s| s.get_strategy_details())
            .collect();

        // 1. Per-strategy states (own accounts, transfer-seeded).
        let states: Vec<Arc<InMemoryState>> = names
            .iter()
            .zip(&ctx.seeds)
            .map(|(n, seed)| Arc::new(InMemoryState::new(n.clone(), *seed)))
            .collect();
        let residual = ctx.config.starting_capital_sgd - ctx.seeds.iter().sum::<f64>();
        assert!(
            residual >= -1e-6,
            "transfer seeding over-commits: capital {} < Σ seeds {}",
            ctx.config.starting_capital_sgd,
            ctx.seeds.iter().sum::<f64>()
        );
        let residual = residual.max(0.0);

        // Install state router?
        {
            let router: std::collections::HashMap<String, Arc<InMemoryState>> =
                names.iter().cloned().zip(states.iter().cloned()).collect();
            state::set_fill_router(router);
        }

        // 2. Replay. Per-strategy order_id counters (each starts at 0, exactly
        //    like a solo run — synthetic execution ids stay per-account).
        let mut order_ids = vec![0i32; names.len()];
        let mut curves: Vec<EquityCurve> = (0..names.len()).map(|_| EquityCurve::new()).collect();
        let mut rollup = EquityCurve::new();
        let mut cells_eq = vec![0.0f64; names.len()];

        for slot in bars {
            for i in 0..ctx.strategies.len() {
                state::set(states[i].clone());
                if let Err(e) = drain_resting_orders_fill_first(
                    ctx.config,
                    details[i].hooks_execution.then_some(&mut ctx.strategies[i]),
                    ctx.prices as &dyn PriceSupplier,
                    &states[i],
                    slot,
                    &ctx.config.subscribed_contracts,
                    &mut order_ids[i],
                ) {
                    tracing::error!(
                        "InMemory multi fill-first drain error [{}]: {e:?}",
                        names[i]
                    );
                }
            }
            let mut this_time = None;
            for (opt_bar, contract) in slot.iter().zip(ctx.config.subscribed_contracts.iter()) {
                let bar = match opt_bar {
                    Some(v) => v,
                    None => continue,
                };
                let time = bar.get_time();
                this_time = Some(time);
                ctx.clock.set(time);
                let close = bar.get_price();
                ctx.prices.publish_close(&contract, close);

                for i in 0..ctx.strategies.len() {
                    state::set(states[i].clone());
                    let outcome = ctx.strategies[i]
                        .on_bar_update(&contract, bar, ctx.consolidator)
                        .unwrap_or_else(|e| {
                            tracing::error!("on_bar_update error [{}]: {e:?}", names[i]);
                            crate::strategy::strategy::BarUpdateOutcome::NoAction
                        });
                    if let Err(e) = handle_bar_update_outcome_in_memory(
                        ctx.config,
                        details[i].hooks_execution.then_some(&mut ctx.strategies[i]),
                        ctx.prices as &dyn PriceSupplier,
                        &states[i],
                        &outcome,
                        &contract,
                        bar,
                        slot,
                        &mut order_ids[i],
                    ) {
                        tracing::error!("InMemory multi reconcile error [{}]: {e:?}", names[i]);
                    }
                }
            }

            // Handle Bucket-end sentinel
            if let Some(_t) = this_time {
                let sentinel_contract = Contract::sentinel();
                let sentinel_bar = HistoricalDataFullKeys::sentinel();
                for i in 0..ctx.strategies.len() {
                    state::set(states[i].clone());
                    let outcome = ctx.strategies[i]
                        .on_bar_update(&sentinel_contract, &sentinel_bar, ctx.consolidator)
                        .unwrap_or_else(|e| {
                            tracing::error!("on_bar_update error [{}]: {e:?}", names[i]);
                            crate::strategy::strategy::BarUpdateOutcome::NoAction
                        });
                    if let Err(e) = handle_bar_update_outcome_in_memory(
                        ctx.config,
                        details[i].hooks_execution.then_some(&mut ctx.strategies[i]),
                        ctx.prices as &dyn PriceSupplier,
                        &states[i],
                        &outcome,
                        &sentinel_contract,
                        &sentinel_bar,
                        slot,
                        &mut order_ids[i],
                    ) {
                        tracing::error!("InMemory multi reconcile error [{}]: {e:?}", names[i]);
                    }
                }
            }

            // 3. Equity snapshots: per-strategy + rollup (residual + Σ).
            let t = this_time.unwrap();
            let mut sum_cash = residual;
            let mut sum_pv = 0.0;
            let mut sum_eq = residual;
            for i in 0..ctx.strategies.len() {
                let positions = states[i].current_positions_snapshot();
                let snap = crate::backtester::output::equity::compute_snapshot_from_positions(
                    positions,
                    ctx.prices as &dyn PriceSupplier,
                    t,
                    0.0,
                );
                sum_cash += snap.cash;
                sum_pv += snap.positions_value;
                sum_eq += snap.equity;
                cells_eq[i] = snap.equity;
                curves[i].push(snap);
            }
            rollup.push(crate::backtester::output::equity::EquitySnapshot {
                time: t,
                cash: sum_cash,
                positions_value: sum_pv,
                equity: sum_eq,
            });
            // §8 diag seam: per-slot equity row (writer closed over env).
            super::diag::diag_eq_row(t.timestamp_millis(), &cells_eq, sum_eq);
        }

        // 4. Clear the thread-locals (the Arc states are returned in `output`).
        state::clear();
        state::clear_fill_router();
        super::diag::diag_close();
        Ok(MultiRunOutput {
            names,
            curves,
            states,
            residual,
            rollup,
        })
    }
}
