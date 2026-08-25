//! In-memory backtest method — runs the SAME prod strategy + a NEW in-memory
//! reconcile, with all state in [`InMemoryState`] (no DB I/O per bar). The
//! strategy's cfg-gated CRUD branches + the Consolidator's cfg-gated read
//! branch detect the thread-local `InMemoryState` + operate on it instead of
//! the DB. The reconcile ([`handle_bar_update_outcome_in_memory`]) reads the
//! mocked targets + adjusts positions/transactions/cash in-memory.
//!
//! ~10-100x faster than `HistoricalReplay` (eliminates all per-bar DB
//! round-trips). The only DB I/O is the one-time `load_bars` query (or none,
//! if shared bars are passed via [`InMemoryReplay::run_with_bars`] — used by
//! the sweep runner).

use std::sync::Arc;

use ibapi::contracts::Contract;

use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::market_data::consolidator::Consolidator;
use crate::market_data::handler::MarketDataHandler;
use crate::market_data::traits::current_price::PriceSupplier;
use crate::strategy::strategy::{StrategyEnum, StrategyExecutor};

use crate::backtester::clock::BacktestClock;
use crate::backtester::config::BacktestConfig;
use crate::backtester::context::BacktestContext;
use crate::backtester::equity::EquityCurve;
use crate::backtester::methods::BacktestMethod;
use crate::backtester::methods::load_bars;
use crate::backtester::oracle::price_supplier::BacktestPriceSupplier;

use crate::backtester::methods::in_memory::historical_cache;
use crate::backtester::methods::in_memory::reconcile::handle_bar_update_outcome_in_memory;
use crate::backtester::methods::in_memory::state::InMemoryState;
use crate::backtester::methods::in_memory::thread_local;

/// Lighter context for the in-memory run — only the pieces `InMemoryReplay`
/// needs (no broker/order_engine/order_store, which are unused in-memory +
/// whose construction would contend on `OrderStore::open()` across parallel
/// sweep backtests). Built per-backtest by the sweep runner.
pub struct InMemoryRunContext<'a> {
    pub config: &'a BacktestConfig,
    pub strategy: StrategyEnum,
    pub handle: &'a tokio::runtime::Handle,
    pub clock: &'a BacktestClock,
    pub prices: &'a BacktestPriceSupplier,
    pub consolidator: &'a Arc<Consolidator>,
}

/// In-memory backtest method. Unit struct — no state; the `InMemoryState` is
/// created + threaded (via the thread-local) inside `run`/`run_with_bars`.
pub struct InMemoryReplay;

impl InMemoryReplay {
    /// Run with pre-loaded shared bars (the sweep runner loads bars once +
    /// passes them here, so N backtests don't re-query the DB). Returns the
    /// equity curve + the `InMemoryState` (the sweep uses the state for
    /// `BacktestResults::compute_in_memory`).
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
        thread_local::set(state.clone());

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
            let snap = crate::backtester::equity::compute_snapshot_from_positions(
                positions,
                ctx.prices as &dyn PriceSupplier,
                time,
                close,
            );
            equity.push(snap);
        }

        // 5. Clear the thread-local (the Arc<InMemoryState> is returned, so the
        //    sweep can still compute results from it).
        thread_local::clear();
        Ok((equity, state))
    }
}

impl BacktestMethod for InMemoryReplay {
    fn run(&self, ctx: BacktestContext) -> Result<EquityCurve, String> {
        // Load the bar stream (the only DB query — one-time, not per-bar).
        let bars = ctx.handle.block_on(load_bars(&ctx.config, &ctx.pool))?;
        tracing::info!(
            "InMemory backtest: {} bars (period: {:?})",
            bars.len(),
            ctx.config.period
        );

        // Set the bar cache (the bars, shared via Arc) so warm_up_data's
        // `read_last_n` reads from the cache (no DB) + tracks `max_end_time`
        // (the latest bar fetched) for the post-warm-up trim below.
        let bars_arc = Arc::new(bars);
        let cache = Arc::new(historical_cache::InMemoryHistoricalCache::new(bars_arc.clone()));
        historical_cache::set(cache.clone());

        // Warm up the strategy's data (pure — reads the lookback from the
        // cache, builds the rolling fns). `warm_up_data` is async + takes
        // `&mut self`; block_on it on this (spawn_blocking) thread.
        let mut strategy = ctx.strategy;
        if let Some(first_bar) = bars_arc.first() {
            let bar_time = first_bar.get_time();
            ctx.handle
                .block_on(strategy.warm_up_data(&ctx.consolidator, bar_time))
                .map_err(|e| format!("warm_up_data: {e}"))?;
        }

        // Trim the bars to post-warm-up. The bars are sorted ascending, so the
        // warm-up window (bars <= max_end_time) is a short prefix — linear-scan
        // from the front (cache-friendly) for the split + take the suffix as a
        // slice (no clone). No data leakage (the rolling fns already saw the
        // prefix).
        let trimmed: &[HistoricalDataFullKeys] = match cache.max_end_time() {
            Some(t) => {
                let split = bars_arc.iter().position(|b| b.get_time() > t).unwrap_or(0);
                &bars_arc[split..]
            }
            None => &bars_arc[..],
        };

        let in_mem_ctx = InMemoryRunContext {
            config: &ctx.config,
            strategy,
            handle: &ctx.handle,
            clock: &ctx.clock,
            prices: &ctx.prices,
            consolidator: &ctx.consolidator,
        };
        let (equity, _state) = self.run_with_bars(in_mem_ctx, trimmed)?;
        historical_cache::clear();
        Ok(equity)
    }
}
