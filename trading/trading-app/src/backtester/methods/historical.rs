//! Historical bar replay — the DB-backed backtest method. Replays chronological
//! bars from `market_data.historical_data`, calling the REAL prod
//! `on_bar_update` + `handle_bar_update_outcome` for each bar, + snapshotting
//! equity. Runs on `spawn_blocking` (via `run_backtest`) so `handle.block_on`
//! calls are legal.

use crate::database::crud::CRUDTrait;
use crate::database::models::{Status, StrategyFullKeys};
use crate::database::models_crud::strategy::StrategyCRUD;
use crate::strategy::strategy::StrategyExecutor;

use crate::backtester::setup::context::BacktestContext;
use crate::backtester::output::equity::EquityCurve;
use crate::backtester::methods::BacktestMethod;
use crate::backtester::methods::load_bars;

/// The DB-backed backtest method: replay historical bars through the real prod
/// strategy + reconciliation. Unit struct — no state; all state is in the
/// `BacktestContext` passed to `run`.
pub struct HistoricalReplay;

impl BacktestMethod for HistoricalReplay {
    fn run(&self, ctx: BacktestContext) -> Result<EquityCurve, String> {
        // 1. Ensure the strategy row exists (FK target for transactions/positions).
        let pool = ctx.pool.clone();
        let strat_name = ctx.strategy.get_name();
        let strat_name_clone = strat_name.clone();
        let mut strategy = ctx.strategy;
        ctx.handle.block_on(async move {
            let crud = StrategyCRUD::new(pool);
            if let Err(e) = crud
                .create_or_ignore(&StrategyFullKeys {
                    strategy: strat_name_clone,
                    status: Status::Active,
                })
                .await
            {
                tracing::error!("backtest: create strategy row failed: {e:?}");
            }
        });

        // 2. Load the bar stream (shared with InMemoryReplay).
        let bars = ctx.handle.block_on(load_bars(&ctx.config, &ctx.pool))?;
        tracing::info!(
            "Backtest: {} bars (period: {:?})",
            bars.len(),
            ctx.config.period
        );

        // 2.5. Warm up the strategy's data. The strategy is pure (uses `self.data`
        //      via the rolling fns) — `warm_up_data` builds it. In Db mode there's
        //      no bar cache, so `read_last_n` hits the DB (one-time, not per-bar).
        if let Some(first_bar) = bars.first() {
            let bar_time = first_bar.get_time();
            ctx.handle
                .block_on(strategy.warm_up_data(&ctx.consolidator, bar_time))
                .map_err(|e| format!("warm_up_data: {e}"))?;
        }

        // 3. Replay.
        let contract = ctx
            .config
            .subscribed_contracts
            .first()
            .cloned()
            .expect("BacktestConfig.subscribed_contracts must be non-empty");
        let mut equity = EquityCurve::new();
        for bar in bars {
            let time = bar.get_time();
            ctx.clock.set(time);
            let close = bar.get_price();
            ctx.prices.publish_close(&contract, close);
            ctx.broker.set_current_bar(bar.clone());

            // --- REAL prod on_bar_update (the strategy signal logic) ---
            let outcome = strategy
                .on_bar_update(&contract, &bar, &ctx.consolidator)
                .unwrap_or_else(|e| {
                    tracing::error!("on_bar_update error: {e:?}");
                    crate::strategy::strategy::BarUpdateOutcome::NoAction
                });

            // --- REAL prod handle_bar_update_outcome (the reconciliation) ---
            ctx.order_engine.handle_bar_update_outcome(
                &*ctx.broker,
                &*ctx.prices,
                outcome,
                &ctx.strategy_details,
                &ctx.order_store,
            );

            // 4. Equity snapshot.
            let snap = ctx
                .handle
                .block_on(crate::backtester::output::equity::compute_snapshot(
                    &ctx.pool,
                    &*ctx.prices,
                    &strat_name,
                    time,
                    &contract,
                    close,
                ));
            equity.push(snap);
        }
        Ok(equity)
    }
}
