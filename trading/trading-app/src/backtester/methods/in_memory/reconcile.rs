//! In-memory `handle_bar_update_outcome` — the fast-path reconciliation. Reads
//! the mocked target positions from `InMemoryState`, computes the delta vs the
//! in-memory current positions, "fills" via `decide_fill` (the same pure fill
//! logic the broker uses), + updates `InMemoryState` (current positions,
//! transactions, CASH:SGD) entirely in-memory. No DB, no `block_on`, no broker.

use ibapi::contracts::Contract;
use ibapi::orders::Order;
use ibapi::prelude::SecurityType;

use crate::backtester::setup::config::BacktestConfig;
use crate::backtester::execution::fill_model::{commission, decide_fill};
use super::state::{InMemoryPosition, InMemoryState, InMemoryTransaction, PositionKey};
use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::market_data::traits::current_price::PriceSupplier;
use crate::strategy::strategy::BarUpdateOutcome;

/// In-memory reconcile. Mirrors the prod `handle_bar_update_outcome`'s
/// `EmitOrders` + `PendingDbQuery` arms but operates on `InMemoryState` with
/// no DB I/O.
///
/// - `EmitOrders(orders)` — the strategy pre-built the orders; fill each one
///   directly via [`fill_order_in_memory`].
/// - `PendingDbQuery(asset_types)` — read the mocked targets, compute the
///   delta vs current, build an order per delta, fill via
///   [`fill_order_in_memory`].
/// - `NoAction` — no-op.
pub fn handle_bar_update_outcome_in_memory(
    config: &BacktestConfig,
    prices: &dyn PriceSupplier,
    state: &InMemoryState,
    outcome: &BarUpdateOutcome,
    _contract: &Contract,
    bar: &HistoricalDataFullKeys,
    order_id: &mut i32,
) -> Result<(), String> {
    match outcome {
        BarUpdateOutcome::EmitOrders(orders) => {
            // Fast path: the strategy pre-built the orders. Fill each one.
            for order_ibkr in orders {
                fill_order_in_memory(config, prices, state, &order_ibkr.contract, &order_ibkr.order, bar, order_id)?;
            }
            Ok(())
        }
        BarUpdateOutcome::PendingDbQuery(asset_types) => {
            // Slow path: read the mocked targets, compute deltas, build orders.
            // Only Stock is supported (mirrors the Noise strategy's QQQ scope).
            if !asset_types.iter().any(|at| matches!(at, crate::database::models::AssetType::Stock)) {
                return Ok(());
            }
            // Snapshot the targets (avoid holding the write lock across the fill).
            let targets: Vec<(PositionKey, InMemoryPosition)> = {
                let guard = state
                    .target_positions
                    .read()
                    .expect("InMemoryState target_positions poisoned");
                guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
            };
            for (key, target_pos) in targets {
                let target_qty = target_pos.quantity;
                let current_qty = state.current_qty(&key);
                let delta = target_qty - current_qty;
                if delta.abs() < 1e-9 {
                    continue;
                }
                // Build the order for this delta.
                let action = if delta > 0.0 {
                    ibapi::orders::Action::Buy
                } else {
                    ibapi::orders::Action::Sell
                };
                let order = Order {
                    action,
                    total_quantity: delta.abs(),
                    order_ref: state.strategy_name.clone(),
                    ..Default::default()
                };
                // The order's contract mirrors the target's key.
                let contract = Contract {
                    symbol: key.stock.clone().into(),
                    security_type: if key.stock.starts_with("CASH:") {
                        SecurityType::ForexPair
                    } else {
                        SecurityType::Stock
                    },
                    exchange: if key.stock.starts_with("CASH:") {
                        "IDEALPRO".into()
                    } else {
                        key.primary_exchange.clone().into()
                    },
                    currency: key.currency.clone().into(),
                    ..Default::default()
                };
                fill_order_in_memory(config, prices, state, &contract, &order, bar, order_id)?;
            }
            Ok(())
        }
        BarUpdateOutcome::NoAction => Ok(()),
    }
}

/// Fill a single order in-memory: decide the fill (via `decide_fill`, the same
/// pure logic the broker uses), compute the commission, + update
/// `InMemoryState` (current positions, transactions, CASH:SGD with FX). No DB,
/// no broker. Shared by both the `EmitOrders` + `PendingDbQuery` arms.
fn fill_order_in_memory(
    config: &BacktestConfig,
    prices: &dyn PriceSupplier,
    state: &InMemoryState,
    contract: &Contract,
    order: &Order,
    bar: &HistoricalDataFullKeys,
    order_id: &mut i32,
) -> Result<(), String> {
    // FX rate: contract.currency → SGD (for settling CASH:SGD). Same logic as
    // the broker's `submit_order`.
    let fx_rate = if contract.currency.to_string() == "SGD" {
        1.0
    } else {
        let fx_contract = Contract {
            symbol: contract.currency.to_string().into(),
            security_type: SecurityType::ForexPair,
            exchange: "IDEALPRO".into(),
            currency: "SGD".into(),
            ..Default::default()
        };
        prices
            .get_current_price(fx_contract, false, &[])
            .unwrap_or(1.0)
    };

    // Decide the fill (same pure logic as the broker).
    let fill = decide_fill(order, bar, config.slippage_bps);
    if !fill.filled {
        tracing::debug!(
            "InMemory fill_order: no fill for {} (limit not crossed or unsupported asset)",
            contract.symbol
        );
        return Ok(());
    }
    let fees = commission(fill.fill_qty, fill.fill_price, config.commission_model);

    // Update the filled contract's position (weighted-avg, mirrors
    // `update_positions_additive`).
    let key = PositionKey {
        strategy: state.strategy_name.clone(),
        stock: contract.symbol.to_string(),
        primary_exchange: contract.primary_exchange.to_string(),
        currency: contract.currency.to_string(),
    };
    state.update_current_additive(key.clone(), fill.fill_qty, fill.fill_price);

    // Record the transaction.
    *order_id += 1;
    state.record_transaction(InMemoryTransaction {
        strategy: key.strategy.clone(),
        stock: key.stock.clone(),
        primary_exchange: key.primary_exchange.clone(),
        currency: key.currency.clone(),
        time: bar.get_time(),
        price: fill.fill_price,
        quantity: fill.fill_qty,
        fees,
        action: format!("{:?}", order.action),
    });

    // Settle CASH:SGD (with FX, same as the broker).
    let cash_sgd_delta = -(fill.fill_qty * fill.fill_price + fees) * fx_rate;
    let cash_key = PositionKey {
        strategy: state.strategy_name.clone(),
        stock: "CASH:SGD".to_string(),
        primary_exchange: "".to_string(),
        currency: "SGD".to_string(),
    };
    state.update_current_additive(cash_key, cash_sgd_delta, 1.0);

    tracing::info!(
        "InMemory FILL bt-{order_id}: {} {:?} {} @ {} (fees {fees}), strat {}",
        contract.symbol,
        order.action,
        fill.fill_qty,
        fill.fill_price,
        state.strategy_name,
    );
    Ok(())
}
