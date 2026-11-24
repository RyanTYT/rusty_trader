//! Historical bar replay — the DB-backed backtest method. Replays chronological
//! bars from `market_data.historical_data`, calling the REAL prod
//! `on_bar_update` + `handle_bar_update_outcome` for each bar, + snapshotting
//! equity. Runs on `spawn_blocking` (via `run_backtest`) so `handle.block_on`
//! calls are legal.

use crate::database::crud::CRUDTrait;
use crate::database::db::DbDirect;
use crate::database::models::{Status, StrategyFullKeys};
use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::database::models_crud::strategy::StrategyCRUD;
use crate::database::tables::DbTables;
use crate::helpers::sentinel::Sentinel;
use crate::strategy::strategy::StrategyExecutor;

use std::sync::Arc;

use ibapi::contracts::Contract;
use sqlx::PgPool;

use crate::backtester::execution::broker::BacktestBroker;
use crate::backtester::oracle::price_supplier::BacktestPriceSupplier;
use crate::backtester::setup::clock::BacktestClock;
use crate::backtester::setup::config::BacktestConfig;
use crate::execution::fx_backed_up_order::OrderStore;
use crate::execution::order_engine::OrderEngine;
use crate::market_data::consolidator::Consolidator;
use crate::market_data::traits::current_price::PriceSupplier;
use crate::strategy::strategy::{StrategyDetails, StrategyEnum};

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
            let mut db = DbDirect::connect(&pool);
            let mut crud = db.strategy();
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

        // 2. Load the bar stream
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

        // 2.5. Warm up strategy
        let warmup_end = ctx
            .handle
            .block_on(strategy.warm_up_data(
                &ctx.consolidator,
                #[cfg(feature = "backtest")]
                bar_time,
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
        let mut equity = EquityCurve::new();
        for bars_in_time in trimmed {
            ctx.broker.begin_slot();
            let mut this_time = None;
            for (opt_bar, contract) in bars_in_time
                .iter()
                .zip(ctx.config.subscribed_contracts.iter())
            {
                let bar = match opt_bar {
                    Some(v) => v,
                    None => continue,
                };

                let time = bar.get_time();
                this_time = Some(time);
                ctx.clock.set(time);
                let close = bar.get_price();
                ctx.prices.publish_close(&contract, close);
                ctx.broker.publish_slot_bar(&contract, bar.clone());
                ctx.broker.drain_parked_orders();

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
                // --- Strategy fill hooks ---
                if ctx.strategy_details.hooks_execution {
                    for execution_data in ctx.broker.take_execution_backlog() {
                        if let Err(e) = strategy.on_fill(execution_data) {
                            tracing::error!("on_fill error: {e:?}");
                        }
                    }
                } else {
                    // drain - since push is ungated
                    ctx.broker.take_execution_backlog();
                }
            }
            let sentinel_contract = Contract::sentinel();
            let sentinel_bar = HistoricalDataFullKeys::sentinel();
            let outcome = strategy
                .on_bar_update(&sentinel_contract, &sentinel_bar, &ctx.consolidator)
                .unwrap_or_else(|e| {
                    tracing::error!("on_bar_update error: {e:?}");
                    crate::strategy::strategy::BarUpdateOutcome::NoAction
                });
            ctx.order_engine.handle_bar_update_outcome(
                &*ctx.broker,
                &*ctx.prices,
                outcome,
                &ctx.strategy_details,
                &ctx.order_store,
            );
            if ctx.strategy_details.hooks_execution {
                for execution_data in ctx.broker.take_execution_backlog() {
                    if let Err(e) = strategy.on_fill(execution_data) {
                        tracing::error!("on_fill error: {e:?}");
                    }
                }
            } else {
                ctx.broker.take_execution_backlog();
            }

            // 4. Equity snapshot.
            let snap = ctx
                .handle
                .block_on(crate::backtester::output::equity::compute_snapshot(
                    &ctx.pool,
                    &*ctx.prices,
                    &strat_name,
                    this_time.unwrap(),
                    0.0,
                ));
            equity.push(snap);
        }

        Ok(equity)
    }
}

/// Db-lane multi context: everything the shared replay loop needs to advance
/// several strategies against ONE bar matrix on the Db (DB-backed) lane.
/// Strategies arrive ALREADY warmed (`warm_up_data` ran in the async caller);
/// `run_multi` owns the sync replay loop. NO `InMemoryState` is installed
/// anywhere — equity snapshots read the DB (`BacktestMode::Db` reads) and
/// fills attribute to the owning strategy by construction (`order_ref` =
/// strategy name keys every broker write).
pub struct DbMultiContext {
    pub config: BacktestConfig,
    pub pool: PgPool,
    pub handle: tokio::runtime::Handle,
    pub clock: Arc<BacktestClock>,
    pub prices: Arc<BacktestPriceSupplier>,
    pub consolidator: Arc<Consolidator>,
    pub strategies: Vec<StrategyEnum>,
}

pub struct DbMultiOutput {
    pub names: Vec<String>,
    pub curves: Vec<EquityCurve>,
    pub residual: f64,
    pub rollup: EquityCurve,
}

impl HistoricalReplay {
    /// Run all strategies in ONE pass over the shared bar matrix, mirroring
    /// `run_multi_with_bars`' slot semantics (clock + close publish happen
    /// ONCE per bar before any strategy sees it) with the single-route
    /// execution machinery (real `BacktestBroker` fills + fill hooks).
    pub fn run_multi(
        &self,
        mut ctx: DbMultiContext,
        bars: &[Vec<
            Option<
                crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys,
            >,
        >],
    ) -> Result<DbMultiOutput, String> {
        let names: Vec<String> = ctx.strategies.iter().map(|s| s.get_name()).collect();
        let details: Vec<StrategyDetails> = ctx
            .strategies
            .iter()
            .map(|s| s.get_strategy_details())
            .collect();

        // Per-strategy brokers (own cash / order-id / execution-backlog);
        // ONE shared OrderStore + OrderEngine (matches live semantics).
        let brokers: Vec<Arc<BacktestBroker>> = names
            .iter()
            .map(|_| {
                Arc::new(BacktestBroker::new(
                    ctx.pool.clone(),
                    ctx.handle.clone(),
                    ctx.config.clone(),
                    ctx.prices.clone() as Arc<dyn PriceSupplier + Send + Sync>,
                ))
            })
            .collect();
        let order_engine = OrderEngine::new(ctx.pool.clone(), ctx.handle.clone());
        let order_store =
            Arc::new(OrderStore::open().expect("Expected to open OrderStore for backtest"));

        // Equal-split seeding residual
        let n_f = names.len() as f64;
        let seeds_total = n_f * (ctx.config.starting_capital_sgd / n_f);
        let residual = ctx.config.starting_capital_sgd - seeds_total;
        assert!(
            residual >= -1e-6,
            "equal-split seeding over-commits: capital {} < Σ seeds {}",
            ctx.config.starting_capital_sgd,
            seeds_total
        );
        let residual = if residual.abs() < 1e-6 {
            0.0
        } else {
            residual.max(0.0)
        };

        let mut curves: Vec<EquityCurve> = (0..names.len()).map(|_| EquityCurve::new()).collect();
        let mut rollup = EquityCurve::new();

        for bars_in_time in bars {
            let mut this_time = None;
            // Phase 1: feed every bar to the brokers
            for broker in &brokers {
                broker.begin_slot();
            }
            for (contract, opt_bar) in ctx
                .config
                .subscribed_contracts
                .iter()
                .zip(bars_in_time.iter())
            {
                let bar = match opt_bar {
                    Some(v) => v,
                    None => continue,
                };
                this_time = Some(bar.get_time());
                for broker in &brokers {
                    broker.publish_slot_bar(&contract, bar.clone());
                }
            }

            // Drain parked fill intents
            for broker in &brokers {
                broker.drain_parked_orders();
            }

            // Phase 2 — interleaved, mirroring the in-memory runner
            for (contract, opt_bar) in ctx
                .config
                .subscribed_contracts
                .iter()
                .zip(bars_in_time.iter())
            {
                let bar = match opt_bar {
                    Some(v) => v,
                    None => continue,
                };
                ctx.clock.set(bar.get_time());
                ctx.prices.publish_close(&contract, bar.get_price());

                for i in 0..ctx.strategies.len() {
                    let outcome = ctx.strategies[i]
                        .on_bar_update(&contract, bar, &ctx.consolidator)
                        .unwrap_or_else(|e| {
                            tracing::error!("on_bar_update error [{}]: {e:?}", names[i]);
                            crate::strategy::strategy::BarUpdateOutcome::NoAction
                        });
                    order_engine.handle_bar_update_outcome(
                        &*brokers[i],
                        &*ctx.prices,
                        outcome,
                        &details[i],
                        &order_store,
                    );
                    if details[i].hooks_execution {
                        for execution_data in brokers[i].take_execution_backlog() {
                            let owner = execution_data.execution.order_reference.as_str();
                            let owner_idx = match names.iter().position(|n| n.as_str() == owner) {
                                Some(idx) => idx,
                                None => {
                                    tracing::warn!(
                                        "execution order_reference '{owner}' matches no strategy — dispatching to '{}'",
                                        names[i]
                                    );
                                    i
                                }
                            };
                            if let Err(e) = ctx.strategies[owner_idx].on_fill(execution_data) {
                                tracing::error!("on_fill error: {e:?}");
                            }
                        }
                    } else {
                        brokers[i].take_execution_backlog();
                    }
                }
            }

            // Bucket-end sentinel (engine convention — helpers::sentinel)
            let sentinel_contract = Contract::sentinel();
            let sentinel_bar = HistoricalDataFullKeys::sentinel();
            for i in 0..ctx.strategies.len() {
                let outcome = ctx.strategies[i]
                    .on_bar_update(&sentinel_contract, &sentinel_bar, &ctx.consolidator)
                    .unwrap_or_else(|e| {
                        tracing::error!("on_bar_update error [{}]: {e:?}", names[i]);
                        crate::strategy::strategy::BarUpdateOutcome::NoAction
                    });
                order_engine.handle_bar_update_outcome(
                    &*brokers[i],
                    &*ctx.prices,
                    outcome,
                    &details[i],
                    &order_store,
                );
                if details[i].hooks_execution {
                    for execution_data in brokers[i].take_execution_backlog() {
                        // Same fill-attribution rule as the per-pair block
                        // above: route by order_reference, not positionally.
                        let owner = execution_data.execution.order_reference.as_str();
                        let owner_idx = match names.iter().position(|n| n.as_str() == owner) {
                            Some(idx) => idx,
                            None => {
                                tracing::warn!(
                                    "execution order_reference '{owner}' matches no strategy — dispatching to '{}'",
                                    names[i]
                                );
                                i
                            }
                        };
                        if let Err(e) = ctx.strategies[owner_idx].on_fill(execution_data) {
                            tracing::error!("on_fill error: {e:?}");
                        }
                    }
                } else {
                    brokers[i].take_execution_backlog();
                }
            }

            // Equity snapshots: per-strategy (DB-backed) + rollup (residual + Σ).
            let t = this_time.expect("bar row produced no bars");
            let snaps = ctx.handle.block_on(async {
                let mut snaps = Vec::with_capacity(names.len());
                for name in &names {
                    snaps.push(
                        crate::backtester::output::equity::compute_snapshot(
                            &ctx.pool,
                            &*ctx.prices,
                            name,
                            t,
                            0.0,
                        )
                        .await,
                    );
                }
                snaps
            });
            // // Rollup adds the FUND-LEVEL unallocated residual. Read the LIVE
            // // value each slot: the strategy-layer month-end realloc zeroes
            // // PoolState's residual and distributes it into the sleeves, so a
            // // frozen initial value would double-count from then on. Runs with
            // // no pool (non-fund strategies) keep the frozen initial value.
            // let live_residual =
            //     crate::strategy_wrappers::gates::impls::fund_gate::PoolState::current()
            //         .map(|p| p.residual())
            //         .unwrap_or(residual);
            // let mut sum_cash = live_residual;
            let mut sum_cash = 0.0;
            let mut sum_pv = 0.0;
            // let mut sum_eq = live_residual;
            let mut sum_eq = 0.0;
            for (i, snap) in snaps.into_iter().enumerate() {
                sum_cash += snap.cash;
                sum_pv += snap.positions_value;
                sum_eq += snap.equity;
                curves[i].push(snap);
            }
            rollup.push(crate::backtester::output::equity::EquitySnapshot {
                time: t,
                cash: sum_cash,
                positions_value: sum_pv,
                equity: sum_eq,
            });
        }

        Ok(DbMultiOutput {
            names,
            curves,
            residual: crate::strategy_wrappers::gates::impls::fund_gate::PoolState::current()
                .map(|p| p.residual())
                .unwrap_or(residual),
            rollup,
        })
    }
}
