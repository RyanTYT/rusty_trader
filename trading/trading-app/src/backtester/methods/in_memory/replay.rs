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

use std::sync::Arc;

use ibapi::contracts::Contract;

use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::market_data::consolidator::Consolidator;
use crate::market_data::traits::current_price::PriceSupplier;
use crate::strategy::strategy::{StrategyEnum, StrategyExecutor};

use crate::backtester::setup::clock::BacktestClock;
use crate::backtester::setup::config::BacktestConfig;
use crate::backtester::setup::context::LightContext;
use crate::backtester::output::equity::EquityCurve;
use crate::backtester::oracle::price_supplier::BacktestPriceSupplier;

use crate::backtester::methods::in_memory::bar_cache;
use crate::backtester::methods::in_memory::reconcile::handle_bar_update_outcome_in_memory;
use crate::backtester::methods::in_memory::state::InMemoryState;
use crate::backtester::methods::in_memory::state;

/// Lighter context for the in-memory run — only the pieces the bar loop
/// needs (no broker/order_engine/order_store, which are unused in-memory +
/// whose construction would contend on `OrderStore::open()` across parallel
/// sweep backtests). Built per-backtest by the sweep runner / single route.
pub struct InMemoryRunContext<'a> {
    pub config: &'a BacktestConfig,
    pub strategy: StrategyEnum,
    pub handle: &'a tokio::runtime::Handle,
    pub clock: &'a BacktestClock,
    pub prices: &'a BacktestPriceSupplier,
    pub consolidator: &'a Arc<Consolidator>,
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
    pub fn run_with_warm_up(
        &self,
        strategy: StrategyEnum,
        bars: Arc<Vec<HistoricalDataFullKeys>>,
        config: &BacktestConfig,
        handle: &tokio::runtime::Handle,
        light: &LightContext,
    ) -> Result<(EquityCurve, Arc<InMemoryState>), String> {
        // Set the bar cache so warm_up_data's `read_last_n` reads from the
        // cache (no DB) + tracks `max_end_time` for the post-warm-up trim.
        let cache = Arc::new(bar_cache::BarCache::new(bars.clone()));
        bar_cache::set(cache.clone());

        // Warm up the strategy's data (pure — reads the lookback from the
        // cache, builds the rolling fns). block_on on the caller's thread.
        let mut strategy = strategy;
        if let Some(first_bar) = bars.first() {
            let bar_time = first_bar.get_time();
            handle
                .block_on(strategy.warm_up_data(&light.consolidator, bar_time))
                .map_err(|e| format!("warm_up_data: {e}"))?;
        }

        // Trim the bars to post-warm-up. The bars are sorted ascending, so
        // the warm-up window (bars <= max_end_time) is a short prefix —
        // linear-scan from the front (cache-friendly) + take the suffix as a
        // slice (no clone, no data leakage — the rolling fns saw the prefix).
        let trimmed: &[HistoricalDataFullKeys] = match cache.max_end_time() {
            Some(t) => {
                let split = bars.iter().position(|b| b.get_time() > t).unwrap_or(0);
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
        };
        let result = self.run_with_bars(in_mem_ctx, trimmed);

        bar_cache::clear();
        result
    }

    /// Run the bar loop with pre-loaded shared bars. Returns the equity curve
    /// + the `InMemoryState` (the sweep uses the state for results).
    pub fn run_with_bars(
        &self,
        ctx: InMemoryRunContext,
        bars: &[HistoricalDataFullKeys],
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
        let contract: Contract = ctx
            .config
            .subscribed_contracts
            .first()
            .cloned()
            .expect("BacktestConfig.subscribed_contracts must be non-empty");
        let mut equity = EquityCurve::new();
        let mut order_id: i32 = 0;
        for bar in bars {
            let time = bar.get_time();
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
                ctx.prices as &dyn PriceSupplier,
                &state,
                &outcome,
                &contract,
                bar,
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
                time,
                close,
            );
            equity.push(snap);
        }

        // 5. Clear the thread-local (the Arc<InMemoryState> is returned, so the
        //    sweep can still compute results from it).
        state::clear();
        Ok((equity, state))
    }
}
