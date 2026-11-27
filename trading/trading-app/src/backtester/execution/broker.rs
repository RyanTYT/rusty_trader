//! Simulated broker for the backtester — the order-placement surface.
//!
//! On `submit_order`: synchronously decides a fill against the current bar
//! (published by the replayer each tick), then does synchronously what prod's
//! 4-handler order-update stream does asynchronously:
//!   - writes a `transactions` row (mirror of `on_execution_update`), and
//!   - updates `current_positions` via the real `update_positions_additive`
//!     (weighted-avg cost; same path as prod).
//! No `-1` optimistic `open_orders` sentinel is written — the backtest has no
//! IBKR-confirmation race to bridge, so there's nothing to reconcile against.

use crate::database::tables::DbTables;
use std::cmp::max;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};

#[cfg(feature = "backtest")]
use crate::backtester::setup::config::BacktestMode;
use arc_swap::ArcSwapOption;
use atomic_float::AtomicF64;
use ibapi::contracts::Contract;
use ibapi::orders::{ExecutionData, Order};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::PgPool;

use crate::database::crud::CRUDTrait;
use crate::database::models::{
    AssetType, CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys, OptionType,
    StockTransactionsFullKeys,
};
use crate::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsOps, CurrentPositionsPrimaryKeys,
    CurrentPositionsUpdateKeys,
};
use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::database::models_crud::open_orders::open_orders::{
    OpenOrdersCRUD, OpenOrdersPrimaryKeys, OpenOrdersUpdateKeys,
};
use crate::database::models_crud::transactions::transactions::{
    TransactionsCRUD, TransactionsFullKeys,
};
use crate::helpers::contract::{HashContract, get_local_symbol};

use crate::backtester::execution::{
    execution_report::synthetic_execution,
    fill_model::{commission, decide_fill},
};
use crate::backtester::setup::config::BacktestConfig;

/// A fill intent parked for `fill_lag_bars` of the order's own bars.
///
/// Only slots where the contract printed a bar advance `bars_remaining`; while
/// parked the order is visible as an optimistic `(-1, order_id)` open-order
/// row, so strategies can see their working orders.
struct PendingFill {
    /// Broker-assigned order id (`BacktestBroker::next_order_id`).
    order_id: i32,
    /// Contract being traded; also the lookup key for the slot bar.
    contract: Contract,
    /// The submitted order (action, quantity, limit price, order_ref).
    order: Order,
    /// Own printed bars still to wait before the first fill decision.
    bars_remaining: u64,
}

/// Simulated broker: the order-placement surface of a Db-lane backtest.
///
/// `submit_order` decides a fill synchronously against the current slot's bar
/// (published per contract by the replayer), then settles it the way the
/// production execution handler would:
/// - write a `transactions` row;
/// - apply the instrument position through the real additive upsert;
/// - apply the SGD cash leg through `CASH:SGD`.
///
/// There is no IBKR confirmation race, so no optimistic `-1` row is written
/// except while an order is parked for a lagged or unfilled decision. Cash and
/// positions are updated through `handle.block_on`, because the submit surface
/// is synchronous.
///
/// Known limitation: option fills log a warning and record their transaction
/// row in the **stock** transactions table (the option transaction write is not
/// implemented in this lane).
pub struct BacktestBroker {
    pool: PgPool,
    handle: tokio::runtime::Handle,
    config: BacktestConfig,
    next_id: AtomicI32,
    /// Per-slot latest bar per contract, published by the replayer before any
    /// strategy acts (cleared each slot by [`begin_slot`]).
    /// - for broker purposes - fill price, etc
    latest_bars:
        std::sync::Mutex<std::collections::HashMap<HashContract<'static>, Arc<HistoricalDataFullKeys>>>,
    /// Parked fill intents (`fill_lag_bars > 0`), drained by [`advance_slot`].
    pending_fills: std::sync::Mutex<Vec<PendingFill>>,
    /// Running cash balance (SGD), updated on each fill
    cash: AtomicF64,
    /// Point-in-time price oracle — used to look up the FX rate
    /// (contract.currency → SGD) when settling non-SGD fills into CASH:SGD.
    prices:
        std::sync::Arc<dyn crate::market_data::traits::current_price::PriceSupplier + Send + Sync>,
    execution_backlog: std::sync::Mutex<Vec<ExecutionData>>,
}

impl BacktestBroker {
    /// Build a broker for one run: the starting cash comes from
    /// `config.starting_capital_sgd`, and order ids start at 1.
    pub fn new(
        pool: PgPool,
        handle: tokio::runtime::Handle,
        config: BacktestConfig,
        prices: std::sync::Arc<
            dyn crate::market_data::traits::current_price::PriceSupplier + Send + Sync,
        >,
    ) -> Self {
        let starting_cash = config.starting_capital_sgd;
        Self {
            pool,
            handle,
            config,
            next_id: AtomicI32::new(1),
            latest_bars: std::sync::Mutex::new(std::collections::HashMap::new()),
            pending_fills: std::sync::Mutex::new(Vec::new()),
            cash: AtomicF64::new(starting_cash),
            prices,
            execution_backlog: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// The database pool the broker settles against.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The runtime handle used for synchronous settlement writes.
    pub fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }

    /// Current cash balance (SGD) after settled fills. Lock-free atomic load.
    pub fn cash(&self) -> f64 {
        self.cash.load(Ordering::Relaxed)
    }

    /// Start a new replay slot: bump the slot counter and clear the per-slot
    /// latest-bar cache (a symbol with no bar this slot is a MISS, not a
    /// stale fill — mirroring the in-memory lane's no-fill-this-slot
    /// semantics). Call before publishing the slot's bars.
    pub fn begin_slot(&self) {
        self.latest_bars
            .lock()
            .expect("backtest latest_bars poisoned")
            .clear();
        // Multi slots resolve fills via `latest_bars`; a symbol with no bar
        // this slot must NOT fall back to a stale other-contract bar — clear
        // it so the fill skips (the in-memory lane's no-fill-this-slot rule).
        // self.current_bar.store(None);
    }

    /// Publish this slot's bar for `contract` (per-contract fill surface).
    ///
    /// Replaces any previous bar for the same contract; contracts that do not
    /// publish a bar in a slot cannot fill in that slot.
    pub fn publish_slot_bar(&self, contract: &Contract, bar: HistoricalDataFullKeys) {
        self.latest_bars
            .lock()
            .expect("backtest latest_bars poisoned")
            .insert(
                HashContract::new(contract.clone()),
                Arc::new(bar),
            );
    }

    /// Re-evaluate every parked fill intent against the slot's published bars.
    ///
    /// A contract without a bar this slot does not advance the lag and cannot
    /// fill. Once `bars_remaining` reaches zero the fill model is consulted:
    /// crossed market/limit orders settle, unfilled limit orders stay parked
    /// and are re-checked on the contract's next bar. A settled fill also
    /// deletes its optimistic `(-1, order_id)` open-order row.
    pub fn drain_parked_orders(&self) {
        let mut to_fill: Vec<(PendingFill, f64, f64, chrono::DateTime<chrono::Utc>)> = Vec::new();
        {
            let mut pending = self
                .pending_fills
                .lock()
                .expect("backtest pending_fills poisoned");
            let latest = self
                .latest_bars
                .lock()
                .expect("backtest latest_bars poisoned");
            pending.retain_mut(|pf| {
                let Some(bar) = latest
                    .get(&HashContract::borrowed(&pf.contract))
                    .cloned()
                else {
                    return true; // no own bar this slot — doesn't count
                };
                if pf.bars_remaining > 0 {
                    pf.bars_remaining -= 1;
                    if pf.bars_remaining > 0 {
                        return true; // still inside the lag
                    }
                }
                let outcome = decide_fill(&pf.order, &bar, self.config.slippage_bps);
                if outcome.filled {
                    to_fill.push((
                        PendingFill {
                            order_id: pf.order_id,
                            contract: pf.contract.clone(),
                            order: pf.order.clone(),
                            bars_remaining: 0,
                        },
                        outcome.fill_price,
                        outcome.fill_qty,
                        bar.get_time(),
                    ));
                    false // remove — filled
                } else {
                    true // limit not crossed — keeps resting, re-checks next own bar
                }
            });
        }
        for (pf, fill_price, fill_qty, time) in to_fill {
            let asset_type = AssetType::from_str(&pf.contract.security_type);
            self.settle_fill(
                pf.order_id,
                &pf.contract,
                &pf.order,
                fill_qty,
                fill_price,
                time,
            );
            // The pending open-order row is consumed by the fill.
            let pk = OpenOrdersPrimaryKeys::new(&asset_type, -1, pf.order_id);
            let mut db = crate::database::db::DbDirect::connect(&self.pool);
            let mut oo_crud = db.open_orders(&asset_type);
            self.handle.block_on(async move {
                if let Err(e) = oo_crud.delete(&pk).await {
                    tracing::error!("BacktestBroker: delete pending open-order row failed: {e:?}");
                }
            });
        }
    }

    /// Drain-and-clear the synthesized execution reports. Mirrors the
    /// consumer contract of the prod ring's `try_pop` drain loop.
    ///
    /// The caller gets every report accumulated since the previous take; the
    /// backlog is left empty.
    pub fn take_execution_backlog(&self) -> Vec<ExecutionData> {
        std::mem::take(
            &mut *self
                .execution_backlog
                .lock()
                .expect("backtest execution backlog poisoned"),
        )
    }
}

/// `BacktestBroker` implements `OrderSubmitter` so it can be passed as
/// `&dyn OrderSubmitter` to the cfg-gated `handle_bar_update_outcome`.
impl crate::backtester::execution::OrderSubmitter for BacktestBroker {
    /// Allocate the next broker order id (monotonic from 1).
    fn next_order_id(&self) -> i32 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Submit an order: decide and settle immediately when possible, otherwise
    /// park it.
    ///
    /// Parking happens when the contract printed no bar in this slot, when
    /// `fill_lag_bars > 0`, or when the fill model reports no fill. A parked
    /// order is recorded as an optimistic `(-1, order_id)` row and re-evaluated
    /// by [`BacktestBroker::drain_parked_orders`]. Settlement failures are
    /// logged; submission never fails.
    fn submit_order(&self, order_id: i32, contract: &Contract, order: &Order) {
        let asset_type = AssetType::from_str(&contract.security_type);
        let bar: Option<Arc<HistoricalDataFullKeys>> = {
            let latest = self
                .latest_bars
                .lock()
                .expect("backtest latest_bars poisoned");
            latest
                .get(&HashContract::borrowed(&contract))
                .cloned()
        };
        let Some(bar) = bar else {
            let parked_num_bars = max(self.config.fill_lag_bars, 0);
            let pk = OpenOrdersPrimaryKeys::new(&asset_type, -1, order_id);
            let uk = OpenOrdersUpdateKeys::new(&asset_type, contract, order);
            let mut db = crate::database::db::DbDirect::connect(&self.pool);
            let mut oo_crud = db.open_orders(&asset_type);
            self.handle.block_on(async move {
                if let Err(e) = oo_crud.create_or_update(&pk, &uk).await {
                    tracing::error!("BacktestBroker: create pending open-order row failed: {e:?}");
                }
            });
            self.pending_fills
                .lock()
                .expect("backtest pending_fills poisoned")
                .push(PendingFill {
                    order_id,
                    contract: contract.clone(),
                    order: order.clone(),
                    bars_remaining: parked_num_bars,
                });
            return;
        };

        let fill = decide_fill(order, &bar, self.config.slippage_bps);
        if self.config.fill_lag_bars > 0 || !fill.filled {
            let parked_num_bars = max(self.config.fill_lag_bars, 0);

            let pk = OpenOrdersPrimaryKeys::new(&asset_type, -1, order_id);
            let uk = OpenOrdersUpdateKeys::new(&asset_type, contract, order);
            let mut db = crate::database::db::DbDirect::connect(&self.pool);
            let mut oo_crud = db.open_orders(&asset_type);
            self.handle.block_on(async move {
                if let Err(e) = oo_crud.create_or_update(&pk, &uk).await {
                    tracing::error!("BacktestBroker: create pending open-order row failed: {e:?}");
                }
            });
            self.pending_fills
                .lock()
                .expect("backtest pending_fills poisoned")
                .push(PendingFill {
                    order_id,
                    contract: contract.clone(),
                    order: order.clone(),
                    bars_remaining: parked_num_bars,
                });
        } else {
            self.settle_fill(
                order_id,
                contract,
                order,
                fill.fill_qty,
                fill.fill_price,
                bar.get_time(),
            );
        }
    }

    /// Remove a parked intent and both stock/option `(-1, order_id)` rows.
    ///
    /// Cancelling an id that is not parked (already filled or unknown) is a
    /// successful no-op and touches no rows.
    ///
    /// # Errors
    /// Never returns `Err`: every database failure is logged.
    fn cancel_order(&self, order_id: i32) -> Result<(), String> {
        // delete from parked intents first
        {
            let mut pending = self
                .pending_fills
                .lock()
                .expect("backtest pending_fills poisoned");
            let before = pending.len();
            pending.retain(|pf| pf.order_id != order_id);
            if pending.len() == before {
                tracing::debug!(
                    "BacktestBroker cancel_order({order_id}): no parked intent (immediate fill or unknown id)"
                );
                return Ok(());
            }
        }
        // delete from db
        for asset_type in [AssetType::Stock, AssetType::Option] {
            let pk = OpenOrdersPrimaryKeys::new(&asset_type, -1, order_id);
            let mut db = crate::database::db::DbDirect::connect(&self.pool);
            let mut oo_crud = db.open_orders(&asset_type);
            self.handle.block_on(async move {
                if let Err(e) = oo_crud.delete(&pk).await {
                    tracing::error!(
                        "BacktestBroker: delete cancelled open-order row failed: {e:?}"
                    );
                }
            });
        }
        Ok(())
    }
}

impl BacktestBroker {
    /// Settle a decided fill.
    ///
    /// Steps, all executed under `handle.block_on`:
    /// 1. commission from the configured model;
    /// 2. cash leg in SGD: `-(qty * price + fees) * fx`, where `fx` converts
    ///    the contract currency to SGD (1.0 for SGD, otherwise the IDEALPRO
    ///    pair via the price oracle, falling back to 1.0 with a warning);
    /// 3. a `transactions` row for the fill;
    /// 4. an instrument position update through `update_positions_additive`;
    /// 5. a `CASH:SGD` update for the cash leg;
    /// 6. a synthetic execution report appended to the backlog.
    ///
    /// The cash delta is negative for a positive (buy) quantity and positive
    /// for a sell, because the fill quantity is signed. Fees are charged on
    /// both sides.
    ///
    /// Known limitation: option fills warn and write a stock transaction row;
    /// the option transaction path is not implemented in this lane.
    fn settle_fill(
        &self,
        order_id: i32,
        contract: &Contract,
        order: &Order,
        fill_qty: f64,
        fill_price: f64,
        time: chrono::DateTime<chrono::Utc>,
    ) {
        let asset_type = AssetType::from_str(&contract.security_type);
        let strat = order.order_ref.clone();
        let fees_f64 = commission(fill_qty, fill_price, self.config.commission_model);
        let fees_decimal = Decimal::from_f64(fees_f64).unwrap_or(Decimal::ZERO);
        // FX rate: contract.currency → SGD. The fill value + commission are in
        // the contract's currency (e.g., USD for QQQ); convert to SGD to
        // settle CASH:SGD. For SGD contracts, the rate is 1.0. For others,
        // look up the FX pair (base=contract.currency, quote=SGD) via the
        // price supplier (which falls back to the fixed FX map).
        let cash_sgd_delta = {
            let fx_rate = if contract.currency.to_string() == "SGD" {
                1.0
            } else {
                let fx_contract = ibapi::contracts::Contract {
                    symbol: contract.currency.to_string().into(),
                    security_type: ibapi::prelude::SecurityType::ForexPair,
                    exchange: "IDEALPRO".into(),
                    currency: "SGD".into(),
                    ..Default::default()
                };
                self.prices
                    .get_current_price(fx_contract, false, &[])
                    .unwrap_or_else(|e| {
                        tracing::warn!(
                            "BacktestBroker: FX rate for {} unavailable ({}); defaulting to 1.0",
                            contract.currency,
                            e
                        );
                        1.0
                    })
            };
            -(fill_qty * fill_price + fees_f64) * fx_rate
        };
        // Settle the SGD cash balance (lock-free atomic update).
        if let Err(e) = self
            .cash
            .fetch_update(Ordering::Release, Ordering::Acquire, |c| {
                Some(c + cash_sgd_delta)
            })
        {
            tracing::error!("Failed to update cash in BacktestBroker: {e:?}");
        };
        let execution_id = format!("bt-{order_id}");

        tracing::info!(
            "BacktestBroker FILL {execution_id}: {} {} {} @ {} (fees {}), strat {}",
            contract.symbol,
            order.action,
            fill_qty,
            fill_price,
            fees_decimal,
            strat,
        );

        let pool = self.pool.clone();
        let handle = self.handle.clone();
        let contract_owned = contract.clone();
        handle.block_on(async move {
            // 1. Write the transaction row (mirror prod on_execution_update).
            let mut db = crate::database::db::DbDirect::connect(&pool);
            let mut tx_crud = db.transactions(&asset_type);
            let tx_fk = match asset_type {
                AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::ForexPair => {
                    TransactionsFullKeys::Stock(StockTransactionsFullKeys {
                        strategy: strat.clone(),
                        execution_id,
                        order_perm_id: order_id,
                        stock: get_local_symbol(&contract_owned),
                        primary_exchange: contract_owned.primary_exchange.to_string(),
                        currency: contract_owned.currency.to_string(),
                        time,
                        price: fill_price,
                        quantity: fill_qty,
                        fees: fees_decimal,
                    })
                }
                AssetType::Option => {
                    tracing::warn!(
                        "BacktestBroker: option transaction write not yet implemented; skipping transaction row for {}",
                        contract_owned.symbol
                    );
                    TransactionsFullKeys::Stock(StockTransactionsFullKeys {
                        strategy: strat.clone(),
                        execution_id,
                        order_perm_id: order_id,
                        stock: get_local_symbol(&contract_owned),
                        primary_exchange: contract_owned.primary_exchange.to_string(),
                        currency: contract_owned.currency.to_string(),
                        time,
                        price: fill_price,
                        quantity: fill_qty,
                        fees: fees_decimal,
                    })
                }
                _ => return,
            };
            if let Err(e) = tx_crud.create(&tx_fk).await {
                tracing::error!("BacktestBroker: write transaction failed: {e:?}");
            }
            self.execution_backlog
                .lock()
                .expect("backtest execution backlog poisoned")
                .push(synthetic_execution(
                    order_id, &contract_owned, &order.action,
                    fill_qty, fill_price, time, &strat,
                ));

            // 2. Update current_pos for asset
            let mut db = crate::database::db::DbDirect::connect(&pool);
            let mut cp_crud = db.current_positions(&asset_type);
            let cp_pk =
                CurrentPositionsPrimaryKeys::from_strat_and_contract(&strat, &contract_owned);
            let cp_uk =
                CurrentPositionsUpdateKeys::from(&asset_type, Some(fill_qty), Some(fill_price));
            if let Err(e) = cp_crud
                .update_positions_additive(
                    cp_pk, 
                    cp_uk, 
                    #[cfg(feature = "backtest")]
                    BacktestMode::Db
                )
                .await {
                tracing::error!("BacktestBroker: update_positions_additive failed: {e:?}");
            }

            // 3. Update current_pos for CASH used
            let mut db = crate::database::db::DbDirect::connect(&pool);
            let mut cash_cp_crud = db.current_positions(&AssetType::Stock);
            let cash_pk = CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
                strategy: strat.clone(),
                stock: "CASH:SGD".to_string(),
                primary_exchange: "".to_string(),
                currency: "SGD".to_string(),
            });
            let cash_uk = CurrentPositionsUpdateKeys::Stock(CurrentStockPositionsUpdateKeys {
                quantity: Some(cash_sgd_delta),
                avg_price: Some(1.0),
                last_updated: None,
            });
            if let Err(e) = cash_cp_crud
                .update_positions_additive(
                    cash_pk, 
                    cash_uk, 
                    #[cfg(feature = "backtest")]
                    BacktestMode::Db
                )
                .await {
                tracing::error!("BacktestBroker: CASH:SGD update failed: {e:?}");
            }
        });
    }
}

// // Silence unused-field warnings for the Option arm's OptionType import path
// // (option transaction write is a TODO; the import is retained for parity).
// #[allow(unused_imports)]
// use crate::database::models::OptionTransactionsFullKeys as _OptionTxFullKeys;
// #[allow(dead_code)]
// fn _option_type_marker() -> OptionType {
//     OptionType::Call
// }
