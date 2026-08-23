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
//!
//! Runs on a `spawn_blocking` thread (the replayer), so `handle.block_on` is
//! legal — same precondition as prod's `on_bar_update`.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;

use ibapi::contracts::Contract;
use ibapi::orders::Order;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::PgPool;

use crate::database::crud::CRUDTrait;
use crate::database::models::{
    AssetType, StockTransactionsFullKeys, OptionType,
};
use crate::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsOps, CurrentPositionsPrimaryKeys,
    CurrentPositionsUpdateKeys,
};
use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::database::models_crud::transactions::transactions::{
    TransactionsCRUD, TransactionsFullKeys,
};
use crate::helpers::contract::get_local_symbol;

use crate::backtester::config::BacktestConfig;
use crate::backtester::fill_model::{commission, decide_fill};

pub struct BacktestBroker {
    pool: PgPool,
    handle: tokio::runtime::Handle,
    config: BacktestConfig,
    next_id: AtomicI32,
    /// The bar `submit_order` should fill against. Set by the replayer before
    /// each tick via [`set_current_bar`].
    current_bar: Mutex<Option<HistoricalDataFullKeys>>,
    /// Running cash balance (base currency), updated on each fill.
    cash: Mutex<f64>,
}

impl BacktestBroker {
    pub fn new(pool: PgPool, handle: tokio::runtime::Handle, config: BacktestConfig) -> Self {
        let starting_cash = config.starting_capital_sgd;
        Self {
            pool,
            handle,
            config,
            next_id: AtomicI32::new(1),
            current_bar: Mutex::new(None),
            cash: Mutex::new(starting_cash),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }

    /// Current cash balance (base currency) after settled fills.
    pub fn cash(&self) -> f64 {
        *self.cash.lock().expect("BacktestBroker cash poisoned")
    }

    /// Publish the bar `submit_order` should fill against for this tick.
    pub fn set_current_bar(&self, bar: HistoricalDataFullKeys) {
        *self.current_bar.lock().expect("BacktestBroker current_bar poisoned") = Some(bar);
    }

    fn current_bar_clone(&self) -> Option<HistoricalDataFullKeys> {
        self.current_bar
            .lock()
            .expect("BacktestBroker current_bar poisoned")
            .clone()
    }
}

/// `BacktestBroker` implements `OrderSubmitter` so it can be passed as
/// `&dyn OrderSubmitter` to the cfg-gated `handle_bar_update_outcome`.
impl crate::execution::order_submitter::OrderSubmitter for BacktestBroker {
    fn next_order_id(&self) -> i32 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn submit_order(&self, order_id: i32, contract: &Contract, order: &Order) {
        let asset_type = AssetType::from_str(&contract.security_type);
        let bar = match self.current_bar_clone() {
            Some(b) => b,
            None => {
                tracing::warn!(
                    "BacktestBroker submit_order({order_id}): no current bar; skipping fill"
                );
                return;
            }
        };

        let outcome = decide_fill(order, &bar, self.config.slippage_bps);
        if !outcome.filled {
            tracing::debug!(
                "BacktestBroker submit_order({order_id}): no fill (limit not crossed or unsupported asset)"
            );
            return;
        }

        let strat = order.order_ref.clone();
        let fill_price = outcome.fill_price;
        let fill_qty = outcome.fill_qty; // signed: +buy / -sell
        let fees_f64 = commission(
            fill_qty,
            fill_price,
            self.config.commission_per_share,
            self.config.commission_min_per_order,
        );
        let fees_decimal = Decimal::from_f64(fees_f64).unwrap_or(Decimal::ZERO);
        // Settle cash synchronously: buy (fill_qty>0) reduces cash by qty*price;
        // sell (fill_qty<0) increases it; fees always reduce. Uniformly:
        //   cash -= fill_qty * fill_price + fees_f64
        {
            let mut c = self.cash.lock().expect("BacktestBroker cash poisoned");
            *c -= fill_qty * fill_price + fees_f64;
        }
        let time = bar.get_time();
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
            let tx_crud = TransactionsCRUD::from(&asset_type, pool.clone());
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

            // 2. Update current_positions additively (weighted-avg; same path
            //    as the prod execution handler). Note: this reuses prod's
            //    `update_positions_additive` verbatim — no prod code changed.
            let cp_crud = CurrentPositionsCRUD::from(&asset_type, pool.clone());
            let cp_pk =
                CurrentPositionsPrimaryKeys::from_strat_and_contract(&strat, &contract_owned);
            let cp_uk =
                CurrentPositionsUpdateKeys::from(&asset_type, Some(fill_qty), Some(fill_price));
            if let Err(e) = cp_crud.update_positions_additive(cp_pk, cp_uk).await {
                tracing::error!("BacktestBroker: update_positions_additive failed: {e:?}");
            }
        });
    }

    /// No-op in the backtest (fills are synchronous; no persistent open orders
    /// to cancel). Present for surface parity with prod's cancel path.
    fn cancel_order(&self, order_id: i32) -> Result<(), String> {
        tracing::debug!("BacktestBroker cancel_order({order_id}): no-op (synchronous fills)");
        let _ = order_id;
        Ok(())
    }
}

// Silence unused-field warnings for the Option arm's OptionType import path
// (option transaction write is a TODO; the import is retained for parity).
#[allow(unused_imports)]
use crate::database::models::OptionTransactionsFullKeys as _OptionTxFullKeys;
#[allow(dead_code)]
fn _option_type_marker() -> OptionType {
    OptionType::Call
}
