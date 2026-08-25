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
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use atomic_float::AtomicF64;
use ibapi::contracts::Contract;
use ibapi::orders::Order;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::PgPool;

use crate::database::crud::CRUDTrait;
use crate::database::models::{
    AssetType, CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys,
    StockTransactionsFullKeys, OptionType,
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

use crate::backtester::setup::config::BacktestConfig;
use crate::backtester::execution::fill_model::{commission, decide_fill};

pub struct BacktestBroker {
    pool: PgPool,
    handle: tokio::runtime::Handle,
    config: BacktestConfig,
    next_id: AtomicI32,
    /// The bar `submit_order` should fill against. Set by the replayer before
    /// each tick via [`set_current_bar`]. Lock-free (`ArcSwapOption`).
    current_bar: ArcSwapOption<HistoricalDataFullKeys>,
    /// Running cash balance (SGD), updated on each fill. Lock-free
    /// (`AtomicF64`) — the broker is single-threaded (only the replayer's
    /// `spawn_blocking` thread), but it's shared via `Arc` + moved into
    /// `spawn_blocking`, so it must be `Send + Sync`.
    cash: AtomicF64,
    /// Point-in-time price oracle — used to look up the FX rate
    /// (contract.currency → SGD) when settling non-SGD fills into CASH:SGD.
    prices: std::sync::Arc<dyn crate::market_data::traits::current_price::PriceSupplier + Send + Sync>,
}

impl BacktestBroker {
    pub fn new(
        pool: PgPool,
        handle: tokio::runtime::Handle,
        config: BacktestConfig,
        prices: std::sync::Arc<dyn crate::market_data::traits::current_price::PriceSupplier + Send + Sync>,
    ) -> Self {
        let starting_cash = config.starting_capital_sgd;
        Self {
            pool,
            handle,
            config,
            next_id: AtomicI32::new(1),
            current_bar: ArcSwapOption::new(None),
            cash: AtomicF64::new(starting_cash),
            prices,
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }

    /// Current cash balance (SGD) after settled fills. Lock-free atomic load.
    pub fn cash(&self) -> f64 {
        self.cash.load(Ordering::Relaxed)
    }

    /// Publish the bar `submit_order` should fill against for this tick.
    /// Lock-free atomic store (`ArcSwapOption`).
    pub fn set_current_bar(&self, bar: HistoricalDataFullKeys) {
        self.current_bar.store(Some(Arc::new(bar)));
    }
}

/// `BacktestBroker` implements `OrderSubmitter` so it can be passed as
/// `&dyn OrderSubmitter` to the cfg-gated `handle_bar_update_outcome`.
impl crate::backtester::execution::OrderSubmitter for BacktestBroker {
    fn next_order_id(&self) -> i32 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn submit_order(&self, order_id: i32, contract: &Contract, order: &Order) {
        let asset_type = AssetType::from_str(&contract.security_type);
        // Lock-free load of the current bar (ArcSwapOption guard kept alive
        // for the whole fill — `bar` borrows from it).
        let bar_guard = self.current_bar.load();
        let bar: &HistoricalDataFullKeys = match &*bar_guard {
            Some(arc) => arc,
            None => {
                tracing::warn!(
                    "BacktestBroker submit_order({order_id}): no current bar; skipping fill"
                );
                return;
            }
        };

        let outcome = decide_fill(order, bar, self.config.slippage_bps);
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
            self.config.commission_model,
        );
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
            // -(fill_value + fees) * fx_rate: negative for buys (cash
            // decreases), positive for sells (cash increases, minus fees).
            -(fill_qty * fill_price + fees_f64) * fx_rate
        };
        // Settle the SGD cash balance (lock-free atomic update).
        let _ = self.cash.fetch_update(Ordering::Release, Ordering::Acquire, |c| {
            Some(c + cash_sgd_delta)
        });
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

            // 3. Update CASH:SGD: decrease on buy (fill_qty>0), increase on sell
            //    (fill_qty<0), minus fees. Keeps the strategy's
            //    `get_strategy_sgd_value` correct (the cash position reflects
            //    actual cash after fills, not the stale seed).
            let cash_cp_crud = CurrentPositionsCRUD::from(&AssetType::Stock, pool.clone());
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
            if let Err(e) = cash_cp_crud.update_positions_additive(cash_pk, cash_uk).await {
                tracing::error!("BacktestBroker: CASH:SGD update failed: {e:?}");
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
