//! Historical bar replay — the DB-backed backtest method. Replays chronological
//! bars from `market_data.historical_data`, calling the REAL prod
//! `on_bar_update` + `handle_bar_update_outcome` for each bar, + snapshotting
//! equity. Runs on `spawn_blocking` (via `run_backtest`) so `handle.block_on`
//! calls are legal.

use crate::database::crud::CRUDTrait;
use crate::database::models::{Status, StrategyFullKeys};
use crate::database::models_crud::strategy::StrategyCRUD;
use crate::strategy::strategy::StrategyExecutor;

use crate::backtester::methods::load_bars;
use crate::backtester::methods::{BacktestMethod, transpose};
use crate::backtester::output::equity::EquityCurve;
#[cfg(feature = "backtest")]
use crate::backtester::setup::config::BacktestPeriod;
use crate::backtester::setup::context::BacktestContext;

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

        // 2. Load the bar stream (shared with InMemoryReplay) — include the
        // strategy's warmup prefix so warm_up_data doesn't eat into the
        // backtest window.
        let bars_raw = ctx.handle.block_on(load_bars(
            &ctx.config,
            &ctx.pool,
            strategy.warmup_bars_required(),
        ))?;
        let mut bars = transpose(bars_raw);

        // bar_time = the backtest window start (TimeRange). The warmup reads
        // the N bars BEFORE this + returns warmup_end (the bar just before
        // the window).
        #[cfg(feature = "backtest")]
        let bar_time = match &ctx.config.period {
            BacktestPeriod::TimeRange { start, .. } => *start,
            BacktestPeriod::NumBars(_) => {
                return Err(
                    "HistoricalReplay: NumBars period is not supported with warmup_bars (use TimeRange)"
                        .to_string(),
                );
            }
        };

        // 2.5. Warm up the strategy's data. The strategy is pure (uses `self.data`
        //      via the rolling fns) — `warm_up_data` builds it. In Db mode there's
        //      no bar cache, so `read_last_n` hits the DB (one-time, not per-bar).
        let warmup_end = ctx
            .handle
            .block_on(strategy.warm_up_data(
                &ctx.consolidator,
                #[cfg(feature = "backtest")] bar_time,
            ))
            .map_err(|e| format!("warm_up_data: {e}"))?;

        // 3. Replay — trim to bars AFTER warmup_end (the backtest window).
        let split = bars
            .iter()
            .position(|row| {
                row.iter()
                    .any(|b| b.as_ref().is_some_and(|bar| bar.get_time() > warmup_end))
            })
            .unwrap_or(0);
        let trimmed = bars.split_off(split);
        let contract = ctx
            .config
            .subscribed_contracts
            .first()
            .cloned()
            .expect("BacktestConfig.subscribed_contracts must be non-empty");
        let mut equity = EquityCurve::new();
        for bars_in_time in trimmed {
            let mut this_time = None;
            for opt_bar in bars_in_time {
                let bar = match opt_bar {
                    Some(v) => v,
                    None => continue,
                };

                let time = bar.get_time();
                this_time = Some(time);
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
            }
            // 4. Equity snapshot.
            let snap = ctx
                .handle
                .block_on(crate::backtester::output::equity::compute_snapshot(
                    &ctx.pool,
                    &*ctx.prices,
                    &strat_name,
                    this_time.unwrap(),
                    &contract,
                    0.0,
                ));
            equity.push(snap);
        }

        Ok(equity)
    }
}
