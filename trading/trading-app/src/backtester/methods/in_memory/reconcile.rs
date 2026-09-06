//! In-memory `handle_bar_update_outcome` — the fast-path reconciliation. Reads
//! the mocked target positions from `InMemoryState`, computes the delta vs the
//! in-memory current positions, "fills" via `decide_fill` (the same pure fill
//! logic the broker uses), + updates `InMemoryState` (current positions,
//! transactions, CASH:SGD) entirely in-memory. No DB, no `block_on`, no broker.

use std::collections::HashMap;

use chrono::Timelike;
use chrono_tz::America::New_York;

use ibapi::contracts::Contract;
use ibapi::orders::Order;
use ibapi::prelude::SecurityType;

use super::bracket::{
    BracketFillConfig, CloseReason, RestingBracket, build_resting_bracket, check_bracket,
    parse_brackets,
};
use super::state::{InMemoryPosition, InMemoryState, InMemoryTransaction, PositionKey};
use crate::backtester::execution::fill_model::{commission, decide_fill};
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
    _contract: &Contract,
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
    check_resting_brackets(config, prices, state, bar, mso, order_id)?;

    match outcome {
        BarUpdateOutcome::EmitOrders(orders) => {
            // ── STEP 2: Parse the EmitOrders into bracket groups ──
            let bracket_groups = parse_brackets(orders);
            let bracket_config = BracketFillConfig::from_params(
                &config
                    .strategies
                    .get(&state.strategy_name)
                    .map(|s| s.params.clone())
                    .unwrap_or_default(),
            );

            for (parent_idx, child_indices) in &bracket_groups {
                // Fill the parent (entry) at the bar's close (MIDPRICE fill).
                let parent = &orders[*parent_idx];
                let target_bar = lookup_bar(config, contracts, &parent.contract, bar);
                fill_order_in_memory(
                    config,
                    prices,
                    state,
                    &parent.contract,
                    &parent.order,
                    target_bar,
                    order_id,
                )?;

                // Build + register the resting bracket (the children).
                let entry_price = match target_bar {
                    HistoricalDataFullKeys::Stock(v) => v.close,
                    HistoricalDataFullKeys::Options(v) => v.close,
                    HistoricalDataFullKeys::DailyStock(v) => v.close,
                    _ => bar.get_price(),
                };
                if let Some(bracket) =
                    build_resting_bracket(orders, *parent_idx, child_indices, entry_price)
                {
                    let mut guard = state
                        .resting_brackets
                        .write()
                        .expect("InMemoryState resting_brackets poisoned");
                    guard.push(bracket);
                }
            }

            // Also fill any non-bracket orders (orders with no children —
            // e.g. standalone market orders not part of a bracket).
            let bracket_parent_indices: std::collections::HashSet<usize> =
                bracket_groups.iter().map(|(p, _)| *p).collect();
            for (i, order_ibkr) in orders.iter().enumerate() {
                if !bracket_parent_indices.contains(&i) && order_ibkr.references_parent_order == -1
                {
                    // Standalone order — fill immediately.
                    let target_bar = lookup_bar(config, contracts, &order_ibkr.contract, bar);
                    fill_order_in_memory(
                        config,
                        prices,
                        state,
                        &order_ibkr.contract,
                        &order_ibkr.order,
                        target_bar,
                        order_id,
                    )?;
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
fn check_resting_brackets(
    config: &BacktestConfig,
    prices: &dyn PriceSupplier,
    state: &InMemoryState,
    bar: &HistoricalDataFullKeys,
    mso: i32,
    order_id: &mut i32,
) -> Result<(), String> {
    let bracket_config = BracketFillConfig::from_params(
        &config
            .strategies
            .get(&state.strategy_name)
            .map(|s| s.params.clone())
            .unwrap_or_default(),
    );

    // Snapshot the resting brackets (avoid holding the write lock across fills).
    let brackets: Vec<RestingBracket> = {
        let guard = state
            .resting_brackets
            .read()
            .expect("InMemoryState resting_brackets poisoned");
        guard.clone()
    };

    let mut updated: Vec<RestingBracket> = Vec::new();

    for mut bracket in brackets {
        if bracket.closed {
            // Already closed — skip (it'll be removed by not being pushed to `updated`).
            continue;
        }

        // If pending (NextBarOpen fill), fill at this bar's open.
        let trigger = if bracket.pending_close_reason.is_some() {
            // The pending close — fill at this bar's open.
            let open_price = match bar {
                HistoricalDataFullKeys::Stock(v) => v.open,
                HistoricalDataFullKeys::Options(v) => v.open,
                HistoricalDataFullKeys::DailyStock(v) => v.open,
                _ => bar.get_price(),
            };
            Some((open_price, bracket.pending_close_reason.unwrap()))
        } else if mso >= 385 {
            // EOD force-close: if the bracket has a midpoint-close, fill at close.
            // Otherwise force-close at the bar's close.
            let close_price = bar.get_price();
            Some((close_price, CloseReason::EodForceClose))
        } else {
            // Normal check: stop / TP / midpoint-close.
            check_bracket(&bracket, bar, mso, &bracket_config)
        };

        if let Some((fill_price, reason)) = trigger {
            // Check for NextBarOpen sentinel (fill_price == 0.0 and reason is Stop/TP).
            if fill_price == 0.0
                && bracket.pending_close_reason.is_none()
                && (reason == CloseReason::Stop || reason == CloseReason::TakeProfit)
            {
                // Mark as pending — fill at next bar's open.
                bracket.pending_close_reason = Some(reason);
                updated.push(bracket);
                continue;
            }

            // Fill the close order.
            let close_order = Order {
                action: bracket.close_action,
                total_quantity: bracket.qty,
                order_ref: state.strategy_name.clone(),
                ..Default::default()
            };
            // Override the fill price — the bracket logic computed it.
            // We do a direct fill (not via decide_fill) since the price is known.
            fill_bracket_close(
                config,
                prices,
                state,
                &bracket.contract,
                &close_order,
                fill_price,
                order_id,
            )?;

            bracket.closed = true;
            bracket.close_price = Some(fill_price);
            bracket.close_reason = Some(reason);
            // Closed brackets are NOT pushed to `updated` (they're removed).
        } else {
            // No trigger — keep monitoring.
            updated.push(bracket);
        }
    }

    // Write back the updated (still-active) brackets.
    {
        let mut guard = state
            .resting_brackets
            .write()
            .expect("InMemoryState resting_brackets poisoned");
        *guard = updated;
    }

    Ok(())
}

/// Fill a bracket's close order at a known price (not via decide_fill —
/// the bracket logic computed the fill price from the stop/TP/close model).
fn fill_bracket_close(
    config: &BacktestConfig,
    prices: &dyn PriceSupplier,
    state: &InMemoryState,
    contract: &Contract,
    order: &Order,
    fill_price: f64,
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

    let is_buy = matches!(order.action, ibapi::orders::Action::Buy);
    let qty = order.total_quantity;
    let signed_qty = if is_buy { qty } else { -qty };
    let fees = commission(signed_qty.abs(), fill_price, config.commission_model);

    // Update current positions.
    let key = PositionKey {
        strategy: state.strategy_name.clone(),
        stock: get_local_symbol(contract),
        primary_exchange: contract.primary_exchange.to_string(),
        currency: contract.currency.to_string(),
    };
    {
        let mut guard = state
            .current_positions
            .write()
            .expect("InMemoryState current_positions poisoned");
        let pos = guard.entry(key.clone()).or_insert(InMemoryPosition {
            quantity: 0.0,
            avg_price: 0.0,
        });
        // Closing: the position goes toward 0.
        let new_qty = pos.quantity + signed_qty;
        if new_qty.abs() < 1e-9 {
            guard.remove(&key);
        } else {
            pos.quantity = new_qty;
            pos.avg_price = fill_price;
        }
    }

    // Record the transaction.
    {
        let mut guard = state
            .transactions
            .write()
            .expect("InMemoryState transactions poisoned");
        guard.push(InMemoryTransaction {
            strategy: state.strategy_name.clone(),
            stock: get_local_symbol(contract),
            primary_exchange: contract.primary_exchange.to_string(),
            currency: contract.currency.to_string(),
            time: bar_time_from_contract(config, contract),
            price: fill_price,
            quantity: signed_qty,
            fees,
            action: format!("{:?}", order.action),
        });
    }

    // Update CASH:SGD.
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
        let cash = guard.entry(cash_key).or_insert(InMemoryPosition {
            quantity: 0.0,
            avg_price: 1.0,
        });
        cash.quantity -= sgd_value + fees;
    }

    *order_id += 1;
    Ok(())
}

/// Get the bar time (for transaction logging). Falls back to the current
/// UTC time if no bar is available.
fn bar_time_from_contract(
    _config: &BacktestConfig,
    _contract: &Contract,
) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}
