//! In-memory `handle_bar_update_outcome` — the fast-path reconciliation. Reads
//! the mocked target positions from `InMemoryState`, computes the delta vs the
//! in-memory current positions, "fills" via `decide_fill` (the same pure fill
//! logic the broker uses), + updates `InMemoryState` (current positions,
//! transactions, CASH:SGD) entirely in-memory. No DB, no `block_on`, no broker.

use std::collections::HashMap;

use chrono::{DateTime, Timelike, Utc};
use chrono_tz::America::New_York;

use ibapi::contracts::Contract;
use ibapi::orders::Order;
use ibapi::prelude::SecurityType;

use std::collections::HashSet;

use super::bracket::{
    build_resting_orders, CloseReason, OrderRole, RestingOrder, RestingStatus,
};
use super::state::{InMemoryPosition, InMemoryState, InMemoryTransaction, PositionKey};
use crate::backtester::execution::fill_model::{commission, decide_fill, BarOhlc, FillEval};
use crate::backtester::setup::config::BacktestConfig;
use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::helpers::contract::get_local_symbol;
use crate::market_data::traits::current_price::PriceSupplier;
use crate::strategy::strategy::BarUpdateOutcome;

/// Resolve the bar for `target_contract` at the current timestamp. The
/// `contracts` slice is the timestamp-row (all subscribed instruments' bars
/// at the current time, aligned with `config.subscribed_contracts`).
/// Falls back to `fallback` (the current instrument's bar) if the target
/// isn't a subscribed contract or its bar is a gap (None) — preserves the
/// old behavior in the edge case.
///
/// WHY: `fill_order_in_memory` derives `fill_price` from the bar's close
/// (via `decide_fill`). Without this lookup, the reconcile would use the
/// *current* instrument's bar for ALL target fills — so a multi-instrument
/// ranking (e.g. the RS-pair entering long TSLA + short NVDA from CAT's
/// `on_bar_update`) would settle BOTH legs at CAT's close, not their own,
/// inflating `cash_sgd_delta` (the CASH:SGD position) catastrophically.
fn lookup_bar<'a>(
    config: &BacktestConfig,
    contracts: &'a [Option<HistoricalDataFullKeys>],
    target_contract: &Contract,
    fallback: &'a HistoricalDataFullKeys,
) -> &'a HistoricalDataFullKeys {
    let target_hash = crate::helpers::contract::HashContract {
        contract: target_contract.clone(),
    };
    for (i, c) in config.subscribed_contracts.iter().enumerate() {
        let hash = crate::helpers::contract::HashContract {
            contract: c.clone(),
        };
        if hash == target_hash {
            if let Some(Some(bar)) = contracts.get(i) {
                return bar;
            }
            break;
        }
    }
    fallback
}

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
    contract: &Contract,
    bar: &HistoricalDataFullKeys,
    contracts: &[Option<HistoricalDataFullKeys>],
    order_id: &mut i32,
) -> Result<(), String> {
    // ── STEP 1: Check resting brackets against this bar ──
    // (before the strategy runs — stop/TP/midpoint-close from prior bars)
    let mso = {
        let ny_time = bar.get_time().with_timezone(&chrono_tz::America::New_York);
        (ny_time.time().num_seconds_from_midnight() as i32 / 60 - 570)
    };
    process_resting_orders(config, prices, state, contract, bar, mso, order_id)?;

    match outcome {
        BarUpdateOutcome::EmitOrders(orders) => {
            // Build the resting-order book from the emitted bracket orders: each
            // order carries a FillModel (market/midprice fill now; limit/stop-limit
            // rest). Market entries fill immediately on THIS bar; limit entries rest
            // and fill on a later crossing bar.
            let slippage_bps = config.slippage_bps;
            let stop_pct_of_bar = 0.5; // orb_general legacy market-stop slippage
            let mut resting = build_resting_orders(orders, slippage_bps, stop_pct_of_bar);

            // Immediate (market/midprice) entries fill NOW, against their own bar.
            for r in resting.iter_mut() {
                if !matches!(r.role, OrderRole::Entry) {
                    continue;
                }
                if !r.fill_model.as_ref().map(|m| m.is_immediate()).unwrap_or(false) {
                    continue; // limit entry — rests
                }
                let target_bar = lookup_bar(config, contracts, &r.contract, bar);
                if let (Some(ohlc), Some(model)) = (BarOhlc::from_bar(target_bar), r.fill_model.as_mut()) {
                    if let FillEval::Fill { price } = model.evaluate(&ohlc) {
                        fill_order_at_price(
                            config, prices, state, &r.contract, r.action, r.qty, price,
                            target_bar.get_time(), order_id,
                        )?;
                        r.status = RestingStatus::Filled;
                    }
                }
            }

            // Activate exits whose entry has already filled (immediate entries).
            let filled_parents: HashSet<usize> = resting
                .iter()
                .filter(|r| matches!(r.role, OrderRole::Entry) && r.status == RestingStatus::Filled)
                .map(|r| r.bracket_id)
                .collect();
            for r in resting.iter_mut() {
                if matches!(r.role, OrderRole::Exit(_)) && filled_parents.contains(&r.bracket_id) {
                    r.active = true;
                }
            }

            {
                let mut guard = state
                    .resting_orders
                    .write()
                    .expect("InMemoryState resting_orders poisoned");
                for r in resting {
                    if r.status == RestingStatus::Resting {
                        guard.push(r);
                    }
                }
            }
            Ok(())
        }
        BarUpdateOutcome::PendingDbQuery(asset_types) => {
            // Slow path: read the mocked targets, compute deltas, build orders.
            // Only Stock is supported (mirrors the Noise strategy's QQQ scope).
            if !asset_types
                .iter()
                .any(|at| matches!(at, crate::database::models::AssetType::Stock))
            {
                return Ok(());
            }
            // Snapshot the targets (avoid holding the write lock across the fill).
            let mut targets_map: HashMap<PositionKey, InMemoryPosition> = {
                let guard = state
                    .target_positions
                    .read()
                    .expect("InMemoryState target_positions poisoned");
                guard.clone()
                // guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
            };
            {
                let guard = state
                    .current_positions
                    .read()
                    .expect("InMemoryState current_positions poisoned");
                guard.iter().for_each(|(k, _v)| {
                    if !targets_map.contains_key(k) {
                        targets_map.insert(
                            k.clone(),
                            InMemoryPosition {
                                quantity: 0.0,
                                avg_price: 0.0,
                            },
                        );
                    }
                });
            };

            for (key, target_pos) in targets_map.iter() {
                // skip CASH positions
                if key.stock.strip_prefix("CASH:").is_some() {
                    continue;
                }

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
                let contract = crate::helpers::contract::build_contract_from_stock(
                    &key.stock,
                    &key.primary_exchange,
                    &key.currency,
                );
                let target_bar = lookup_bar(config, contracts, &contract, bar);
                fill_order_in_memory(
                    config, prices, state, &contract, &order, target_bar, order_id,
                )?;
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
        stock: get_local_symbol(&contract),
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

    // tracing::info!(
    //     "InMemory FILL bt-{order_id}: {} {:?} {} @ {} (fees {fees}), strat {}",
    //     contract.symbol,
    //     order.action,
    //     fill.fill_qty,
    //     fill.fill_price,
    //     state.strategy_name,
    // );
    Ok(())
}

/// Check all resting brackets against the current bar. If any child
/// (stop / TP / midpoint-close) triggers, fill the close order, cancel
/// the OCA siblings, and remove the bracket.
///
/// At EOD (mso >= 385), force-close any still-open brackets at the bar's
/// close.
fn process_resting_orders(
    config: &BacktestConfig,
    prices: &dyn PriceSupplier,
    state: &InMemoryState,
    contract: &Contract,
    bar: &HistoricalDataFullKeys,
    mso: i32,
    order_id: &mut i32,
) -> Result<(), String> {
    let ohlc = BarOhlc::from_bar(bar);
    let orders: Vec<RestingOrder> = {
        let guard = state
            .resting_orders
            .read()
            .expect("InMemoryState resting_orders poisoned");
        guard.clone()
    };

    let mut remaining: Vec<RestingOrder> = Vec::new();
    let mut filled_entry_brackets: HashSet<usize> = HashSet::new();
    let mut filled_exit_brackets: HashSet<usize> = HashSet::new();

    for mut order in orders {
        // Keep other instruments' orders untouched.
        if order.contract.symbol != contract.symbol {
            remaining.push(order);
            continue;
        }
        if order.status != RestingStatus::Resting {
            continue; // already filled/cancelled — drop
        }

        // ── EOD (mso >= 385): force-close active exits at close/mid, cancel the rest.
        if mso >= 385 {
            if order.active && matches!(order.role, OrderRole::Exit(_)) {
                if let Some(o) = ohlc {
                    let price = if order.close_is_mid {
                        (o.high + o.low) / 2.0
                    } else {
                        o.close
                    };
                    fill_order_at_price(
                        config, prices, state, &order.contract, order.action, order.qty,
                        price, bar.get_time(), order_id,
                    )?;
                    filled_exit_brackets.insert(order.bracket_id);
                }
            }
            continue; // everything for this contract is dropped at EOD
        }

        // Inactive exits wait for their entry to fill.
        if !order.active {
            remaining.push(order);
            continue;
        }

        // ── Time-gated close ──
        if order.close_mso.is_some() {
            let close_mso = order.close_mso.unwrap();
            if mso >= close_mso {
                if let Some(o) = ohlc {
                    let price = if order.close_is_mid {
                        (o.high + o.low) / 2.0
                    } else {
                        o.close
                    };
                    fill_order_at_price(
                        config, prices, state, &order.contract, order.action, order.qty,
                        price, bar.get_time(), order_id,
                    )?;
                    order.status = RestingStatus::Filled;
                    filled_exit_brackets.insert(order.bracket_id);
                }
                continue; // drop (filled)
            }
            remaining.push(order);
            continue;
        }

        // ── Price-gated (limit / stop-limit / market-stop) ──
        let Some(o) = ohlc else {
            remaining.push(order);
            continue;
        };
        let mut did_fill = false;
        if let Some(model) = order.fill_model.as_mut() {
            if let FillEval::Fill { price } = model.evaluate(&o) {
                fill_order_at_price(
                    config, prices, state, &order.contract, order.action, order.qty,
                    price, bar.get_time(), order_id,
                )?;
                order.status = RestingStatus::Filled;
                did_fill = true;
                match order.role {
                    OrderRole::Entry => {
                        filled_entry_brackets.insert(order.bracket_id);
                    }
                    OrderRole::Exit(_) => {
                        filled_exit_brackets.insert(order.bracket_id);
                    }
                }
            }
        }
        if !did_fill {
            remaining.push(order);
        }
    }

    // Cross-order side effects.
    for order in remaining.iter_mut() {
        if matches!(order.role, OrderRole::Exit(_)) {
            if filled_entry_brackets.contains(&order.bracket_id) {
                order.active = true; // entry filled this bar → arm the exits
            }
            if filled_exit_brackets.contains(&order.bracket_id) {
                order.status = RestingStatus::Cancelled; // an exit filled → cancel siblings
            }
        }
    }

    {
        let mut guard = state
            .resting_orders
            .write()
            .expect("InMemoryState resting_orders poisoned");
        *guard = remaining
            .into_iter()
            .filter(|o| o.status == RestingStatus::Resting)
            .collect();
    }

    Ok(())
}

/// Fill an order at a known price (market/limit/stop/mid — the fill model already
/// resolved the exact price). Updates position + transaction + CASH:SGD, charges
/// commission, but applies NO extra slippage.
fn fill_order_at_price(
    config: &BacktestConfig,
    prices: &dyn PriceSupplier,
    state: &InMemoryState,
    contract: &Contract,
    action: ibapi::orders::Action,
    qty: f64,
    fill_price: f64,
    bar_time: DateTime<Utc>,
    order_id: &mut i32,
) -> Result<(), String> {
    // FX rate: contract.currency → SGD.
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

    let is_buy = matches!(action, ibapi::orders::Action::Buy);
    let signed_qty = if is_buy { qty } else { -qty };
    let fees = commission(signed_qty.abs(), fill_price, config.commission_model);

    let key = PositionKey {
        strategy: state.strategy_name.clone(),
        stock: get_local_symbol(contract),
        primary_exchange: contract.primary_exchange.to_string(),
        currency: contract.currency.to_string(),
    };
    state.update_current_additive(key.clone(), signed_qty, fill_price);

    state.record_transaction(InMemoryTransaction {
        strategy: key.strategy.clone(),
        stock: key.stock.clone(),
        primary_exchange: key.primary_exchange.clone(),
        currency: key.currency.clone(),
        time: bar_time,
        price: fill_price,
        quantity: signed_qty,
        fees,
        action: format!("{:?}", action),
    });

    let sgd_value = signed_qty * fill_price * fx_rate;
    let cash_key = PositionKey {
        strategy: state.strategy_name.clone(),
        stock: "CASH:SGD".to_string(),
        primary_exchange: "".to_string(),
        currency: "SGD".to_string(),
    };
    {
        let mut guard = state
            .current_positions
            .write()
            .expect("InMemoryState current_positions poisoned");
        let cash = guard.entry(cash_key.clone()).or_insert(InMemoryPosition {
            quantity: 0.0,
            avg_price: 1.0,
        });
        cash.quantity -= sgd_value + fees;
    }

    *order_id += 1;
    Ok(())
}

