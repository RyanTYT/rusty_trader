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

use std::sync::atomic::Ordering;

use super::bracket::{
    NEXT_BRACKET_ID, OrderRole, RestingOrder, RestingStatus, build_resting_orders,
};
use super::state::{
    mailbox_push, routed_state, InMemoryPosition, InMemoryState, InMemoryTransaction, MailFill,
    PositionKey,
};
use crate::backtester::execution::{
    execution_report::synthetic_execution,
    fill_model::{BarOhlc, FillEval, MarketFillModel, commission, decide_fill},
};
use crate::backtester::setup::config::BacktestConfig;
use crate::database::models::OpenStockOrdersFullKeys;
use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::database::models_crud::open_orders::open_orders::OpenOrdersFullKeys;
use crate::helpers::contract::get_local_symbol;
use crate::market_data::traits::current_price::PriceSupplier;
use crate::strategy::strategy::StrategyExecutor;
use crate::strategy::strategy::{BarUpdateOutcome, StrategyEnum};

/// Resolve the bar for `target_contract` at the current timestamp, from the
/// target's OWN subscribed bar only. The `contracts` slice is the
/// timestamp-row (all subscribed instruments' bars at the current time,
/// aligned with `config.subscribed_contracts`).
///
/// Returns `None` when the target symbol has no bar at this slot (gap or not
/// subscribed). Callers MUST treat `None` as "defer" (rest a market order and
/// fill it at the symbol's next own bar) — NEVER fill from another symbol's
/// ambient bar: the old `fallback` behavior settled cross-symbol fills at
/// whatever instrument was currently being processed (e.g. a thin symbol's
/// 09:30 trim-to-0 exit filled at AAPL's 09:30 close → phantom P&L), which is
/// why this fn replaced the old `lookup_bar` (ambient-bar leak, fixed 2026-10-30).
///
/// WHY the own-bar requirement exists at all: `fill_order_in_memory` derives
/// `fill_price` from the bar's close (via `decide_fill`). Without the lookup,
/// the reconcile would use the *current* instrument's bar for ALL target fills
/// — so a multi-instrument ranking (e.g. the RS-pair entering long TSLA +
/// short NVDA from CAT's `on_bar_update`) would settle BOTH legs at CAT's
/// close, not their own, inflating `cash_sgd_delta` (the CASH:SGD position)
/// catastrophically.
fn resolve_own_bar<'a>(
    config: &BacktestConfig,
    contracts: &'a [Option<HistoricalDataFullKeys>],
    target_contract: &Contract,
) -> Option<&'a HistoricalDataFullKeys> {
    let target_hash = crate::helpers::contract::HashContract {
        contract: target_contract.clone(),
    };
    for (i, c) in config.subscribed_contracts.iter().enumerate() {
        let hash = crate::helpers::contract::HashContract {
            contract: c.clone(),
        };
        if hash == target_hash {
            if let Some(Some(bar)) = contracts.get(i) {
                return Some(bar);
            }
            return None;
        }
    }
    None
}

/// Cancel stale resting deferrals (resting market-class entries) for
/// `contract`, so a re-flushed delta for a symbol whose previous deferral is
/// still parked can never double-fill (a second Sell with no position would
/// open an unintended short). A resting *market* entry is by construction a
/// deferral — the fill-any-bar model fills on the symbol's first own bar.
fn cancel_stale_deferrals(state: &InMemoryState, contract: &Contract) {
    let mut guard = state
        .resting_orders
        .write()
        .expect("InMemoryState resting_orders poisoned");
    for o in guard.iter_mut() {
        if o.contract.symbol == contract.symbol
            && o.status == RestingStatus::Resting
            && matches!(o.role, OrderRole::Entry)
            && o.fill_model
                .as_ref()
                .map(|m| m.is_immediate())
                .unwrap_or(false)
        {
            o.status = RestingStatus::Cancelled;
        }
    }
}

/// Build the resting deferral that replaces an immediate market fill whose own
/// bar is absent at the trigger slot: it fills at the symbol's OWN next bar
/// (close ± slippage, via `process_resting_orders`' price-gated market eval).
fn defer_market_entry(
    contract: &Contract,
    action: ibapi::orders::Action,
    qty: f64,
    slippage_bps: f64,
    order_ref: &str,
) -> RestingOrder {
    RestingOrder {
        contract: contract.clone(),
        action,
        qty,
        order_ref: order_ref.to_string(),
        oca_group: String::new(),
        bracket_id: NEXT_BRACKET_ID.fetch_add(1, Ordering::SeqCst),
        role: OrderRole::Entry,
        active: true,
        fill_model: Some(Box::new(MarketFillModel {
            slippage_bps,
            is_buy: matches!(action, ibapi::orders::Action::Buy),
        })),
        close_mso: None,
        close_is_mid: false,
        status: RestingStatus::Resting,
    }
}

/// Buy = +1, Sell = −1 — mirrors `OpenOrdersFullKeys::from_contract_and_order`'s
/// signed-quantity convention (Sell → negative), used by the cancel seam to
/// match an emitted cancel key's side against a resting order's action.
fn action_signum(action: &ibapi::orders::Action) -> f64 {
    if matches!(action, ibapi::orders::Action::Buy) {
        1.0
    } else {
        -1.0
    }
}

/// In-memory reconcile. Mirrors the prod `handle_bar_update_outcome`'s
/// `EmitOrders` + `PendingDbQuery` arms but operates on `InMemoryState` with
/// no DB I/O.
///
/// - `EmitOrders { place_orders, cancel_orders }` — the strategy pre-built the
///   orders to place AND the open-order keys to cancel; place each one
///   directly via [`fill_order_in_memory`].
/// - `PendingDbQuery(asset_types)` — read the mocked targets, compute the
///   delta vs current, build an order per delta, fill via
///   [`fill_order_in_memory`].
/// - `NoAction` — no-op.
pub fn handle_bar_update_outcome_in_memory(
    config: &BacktestConfig,
    strategy: Option<&mut StrategyEnum>,
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
    let strategy = process_resting_orders(
        config, strategy, prices, state, contract, bar, mso, order_id,
    )?;

    match outcome {
        BarUpdateOutcome::EmitOrders {
            place_orders: orders,
            cancel_orders,
        } => {
            // ── Cancel seam: the strategy's cancel_orders carry the OPEN-order
            // keys it read BEFORE this emission (live: exact DB pks + IBKR ids;
            // here: the resting book's coarse identity — there is no perm_id in
            // the backtest book). Swept FIRST: a cancel + same-key re-place in
            // ONE emission must not kill the fresh placement (live is immune
            // because it cancels by exact order_id; the backtest book matches
            // by symbol + order_ref + side, so a place-then-cancel sweep would
            // hit the just-placed rest).
            // Resting status only (same convention as the ogcancel- seam below):
            // filled/cancelled entries can't be cancelled, and this also sweeps
            // parked deferrals — a cancelled deferred entry must never fill later.
            {
                let mut guard = state
                    .resting_orders
                    .write()
                    .expect("InMemoryState resting_orders poisoned");
                for cancel_key in cancel_orders {
                    match cancel_key {
                        OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                            strategy: ref_,
                            stock,
                            quantity,
                            ..
                        }) => {
                            // Buy = +1 / Sell = −1 — the signed-quantity
                            // convention of `OpenOrdersFullKeys::from_contract_and_order`.
                            // Side filter: rule-(b) cancels the OPPOSING group
                            // only, never a same-key rest on the other side.
                            let side = quantity.signum();
                            guard.retain(|r| {
                                !(r.contract.symbol.as_str() == stock.as_str()
                                    && r.order_ref == *ref_
                                    && r.status == RestingStatus::Resting
                                    && action_signum(&r.action) == side)
                            });
                        }
                        OpenOrdersFullKeys::Options(_) => {
                            tracing::warn!(
                                "backtest EmitOrders cancel_orders: Options key unsupported — ignored"
                            );
                        }
                    }
                }
            }

            // Build the resting-order book from the emitted bracket orders: each
            // order carries a FillModel (market/midprice fill now; limit/stop-limit
            // rest). Market entries fill immediately on THIS bar; limit entries rest
            // and fill on a later crossing bar.
            let slippage_bps = config.slippage_bps;
            let stop_pct_of_bar = 0.5; // orb_general legacy market-stop slippage
            let mut resting = build_resting_orders(orders, slippage_bps, stop_pct_of_bar);

            // ── OrderGate cancel seam: an "ogcancel-"-prefixed emitted order
            // cancels the engine's working resting entries for (the marker
            // order's OWN symbol, this order_ref). §4 refinement: match the
            // marker order's contract (a sheet carries many syms — the BAR's
            // contract is unrelated) and drop the action condition (a
            // flat-cancel of a resting SELL emits a same-action sell-to-flat;
            // the gate only stamps the marker when cancelling its own entry
            // for that exact key, so over-cancellation is impossible).
            // Same-slot semantics match python: the pending-limit fill checks
            // already ran in STEP 1 (before the strategy), so the cancel
            // prevents FUTURE fills only. This is the only engine-side sweep
            // now that the EOD force-close block is removed — every cancel
            // path is load-bearing.
            {
                let mut guard = state
                    .resting_orders
                    .write()
                    .expect("InMemoryState resting_orders poisoned");
                for o in orders
                    .iter()
                    .filter(|o| o.order.oca_group.starts_with(crate::strategy_wrappers::payload::OG_CANCEL_PREFIX))
                {
                    guard.retain(|r| {
                        !(r.contract.symbol == o.contract.symbol
                            && r.order_ref == o.order.order_ref
                            && r.status == RestingStatus::Resting)
                    });
                }
            }

            // Immediate (market/midprice) entries fill NOW, against their own bar.
            // Entries whose OWN bar is absent at this slot defer: they rest as a
            // market-class order and fill at the symbol's own next bar (never at
            // another symbol's ambient close — see `resolve_own_bar`).
            let mut new_strategy = strategy;
            let mut deferred: Vec<RestingOrder> = Vec::new();
            for r in resting.iter_mut() {
                if !matches!(r.role, OrderRole::Entry) {
                    continue;
                }
                if !r
                    .fill_model
                    .as_ref()
                    .map(|m| m.is_immediate())
                    .unwrap_or(false)
                {
                    continue; // limit entry — rests
                }
                let Some(target_bar) = resolve_own_bar(config, contracts, &r.contract) else {
                    // Defer: park a market-class deferral (fills at the
                    // symbol's own next bar) and drop the original resting
                    // entry so it can't double-fill.
                    cancel_stale_deferrals(state, &r.contract);
                    deferred.push(defer_market_entry(
                        &r.contract,
                        r.action,
                        r.qty,
                        slippage_bps,
                        &r.order_ref,
                    ));
                    r.status = RestingStatus::Cancelled;
                    continue;
                };
                if let (Some(ohlc), Some(model)) =
                    (BarOhlc::from_bar(target_bar), r.fill_model.as_mut())
                {
                    if let FillEval::Fill { price } = model.evaluate(&ohlc) {
                        new_strategy = fill_order_at_price(
                            config,
                            new_strategy,
                            prices,
                            state,
                            &r.contract,
                            r.action,
                            r.qty,
                            price,
                            target_bar.get_time(),
                            order_id,
                            &r.order_ref,
                            &r.oca_group,
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
                // Deferrals: resting market entries whose own bar was absent —
                // they fill later at the symbol's own next bar.
                guard.extend(deferred);
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

            let mut new_strategy = strategy;
            // Deferred fills: target deltas whose OWN symbol has no bar at this
            // slot are parked as resting market orders (fill at the symbol's own
            // next bar), never priced off another symbol's ambient bar
            // (AAPL-close leak, fixed 2026-10-30).
            let mut deferred: Vec<RestingOrder> = Vec::new();
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
                let Some(target_bar) = resolve_own_bar(config, contracts, &contract) else {
                    cancel_stale_deferrals(state, &contract);
                    deferred.push(defer_market_entry(
                        &contract,
                        action,
                        delta.abs(),
                        config.slippage_bps,
                        &order.order_ref,
                    ));
                    continue;
                };
                new_strategy = fill_order_in_memory(
                    config,
                    new_strategy,
                    prices,
                    state,
                    &contract,
                    &order,
                    target_bar,
                    order_id,
                )?;
            }
            if !deferred.is_empty() {
                let mut guard = state
                    .resting_orders
                    .write()
                    .expect("InMemoryState resting_orders poisoned");
                guard.extend(deferred);
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
fn fill_order_in_memory<'a>(
    config: &BacktestConfig,
    strategy: Option<&'a mut StrategyEnum>,
    prices: &dyn PriceSupplier,
    state: &InMemoryState,
    contract: &Contract,
    order: &Order,
    bar: &HistoricalDataFullKeys,
    order_id: &mut i32,
) -> Result<Option<&'a mut StrategyEnum>, String> {
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
        return Ok(strategy);
    }
    let fees = commission(fill.fill_qty, fill.fill_price, config.commission_model);

    // ── §8 ROUTED SETTLEMENT (same seam as `fill_order_at_price`): a
    // routed order_ref ≠ the passing reconciler's state settles into the
    // OWNING account + mailbox; pooled fills bypass every strategy hook.
    let routed = routed_state(&order.order_ref)
        .filter(|t| t.strategy_name != state.strategy_name);
    if let Some(tgt) = routed {
        let key = PositionKey {
            strategy: tgt.strategy_name.clone(),
            stock: get_local_symbol(contract),
            primary_exchange: contract.primary_exchange.to_string(),
            currency: contract.currency.to_string(),
        };
        tgt.update_current_additive(key.clone(), fill.fill_qty, fill.fill_price);
        tgt.record_transaction(InMemoryTransaction {
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
        // order-id BEFORE the mailbox push: the stamped id == the value the
        // own path's synthetic execution carries (post-increment convention).
        *order_id += 1;
        mailbox_push(
            &tgt.strategy_name,
            MailFill {
                sym: key.stock.clone(),
                qty: fill.fill_qty as i64,
                px: fill.fill_price,
                date: bar
                    .get_time()
                    .with_timezone(&New_York)
                    .date_naive(),
                order_id: *order_id as i64,
                oca_group: order.oca_group.clone(),
            },
        );
        let cash_sgd_delta = -(fill.fill_qty * fill.fill_price + fees) * fx_rate;
        let cash_key = PositionKey {
            strategy: tgt.strategy_name.clone(),
            stock: "CASH:SGD".to_string(),
            primary_exchange: "".to_string(),
            currency: "SGD".to_string(),
        };
        tgt.update_current_additive(cash_key, cash_sgd_delta, 1.0);
        super::diag::diag_fill(
            bar.get_time().timestamp_millis(),
            bar.get_time().with_timezone(&New_York).date_naive(),
            &state.strategy_name,
            &order.order_ref,
            Some(&tgt.strategy_name),
            &key.stock,
            &format!("{:?}", order.action),
            fill.fill_qty,
            fill.fill_price,
            fees,
            fx_rate,
        );
        return Ok(strategy);
    }

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
    let new_strategy = if let Some(strat) = strategy {
        let execution_data = synthetic_execution(
            *order_id,
            contract,
            &order.action,
            fill.fill_qty, // +buy / -sell, as decided by decide_fill
            fill.fill_price,
            bar.get_time(),
            &key.strategy,
        );
        if let Err(e) = strat.on_fill(execution_data) {
            tracing::warn!("backtest on_fill failed: {e:?}");
        }
        Some(strat)
    } else {
        None
    };

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
    Ok(new_strategy)
}

/// Check all resting brackets against the current bar. If any child
/// (stop / TP / midpoint-close) triggers, fill the close order, cancel
/// the OCA siblings, and remove the bracket.
///
/// At EOD (mso >= 385), force-close any still-open brackets at the bar's
/// close.
fn process_resting_orders<'a>(
    config: &BacktestConfig,
    strategy_opt: Option<&'a mut StrategyEnum>,
    prices: &dyn PriceSupplier,
    state: &InMemoryState,
    contract: &Contract,
    bar: &HistoricalDataFullKeys,
    mso: i32,
    order_id: &mut i32,
) -> Result<Option<&'a mut StrategyEnum>, String> {
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
    // OrderGate entry-sibling cancel: oca_groups of gate-placed entries
    // ("og-…") that filled THIS bar — one sibling filling cancels the
    // rest of the group (mirrors filled_exit_brackets' sibling-cancel for
    // gate-placed entries, which carry no bracket exits).
    let mut filled_entry_groups: HashSet<String> = HashSet::new();

    let mut strategy = strategy_opt;
    for mut order in orders {
        // Keep other instruments' orders untouched.
        if order.contract.symbol != contract.symbol {
            remaining.push(order);
            continue;
        }
        if order.status != RestingStatus::Resting {
            continue; // already filled/cancelled — drop
        }

        // ── EOD (mso >= 385): force-close active exits at close/mid, cancel
        // the rest. REMOVED 2026-11-05 (user ruling): the only strategies
        // that relied on it (vp_touch brackets, orb_general market-stop
        // closes) are DEPRECATED. Resting entries now persist overnight
        // exactly like python pending limits; lifecycle is owned by the
        // OrderGate (TTL expiry / contradicting-intent cancels via the
        // ogcancel- marker seam in the EmitOrders arm) and by the OCA
        // sibling cancels below. NOTE: with this block gone there is NO
        // unconditional sweep — every cancel path is now load-bearing.
        // if mso >= 385 {
        //     if order.active && matches!(order.role, OrderRole::Exit(_)) {
        //         if let Some(o) = ohlc {
        //             let price = if order.close_is_mid {
        //                 (o.high + o.low) / 2.0
        //             } else {
        //                 o.close
        //             };
        //             strategy = fill_order_at_price(
        //                 config,
        //                 strategy,
        //                 prices,
        //                 state,
        //                 &order.contract,
        //                 order.action,
        //                 order.qty,
        //                 price,
        //                 bar.get_time(),
        //                 order_id,
        //                 &order.order_ref,
        //                 &order.oca_group,
        //             )?;
        //             filled_exit_brackets.insert(order.bracket_id);
        //         }
        //     }
        //     continue; // everything for this contract is dropped at EOD
        // }

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
                    strategy = fill_order_at_price(
                        config,
                        strategy,
                        prices,
                        state,
                        &order.contract,
                        order.action,
                        order.qty,
                        price,
                        bar.get_time(),
                        order_id,
                        &order.order_ref,
                        &order.oca_group,
                    )?;
                    order.status = RestingStatus::Filled;
                    match order.role {
                        OrderRole::Entry => {
                            filled_entry_brackets.insert(order.bracket_id);
                            if order.oca_group.starts_with(crate::strategy_wrappers::payload::OG_PREFIX) {
                                filled_entry_groups.insert(order.oca_group.clone());
                            }
                        }
                        OrderRole::Exit(_) => {
                            filled_exit_brackets.insert(order.bracket_id);
                        }
                    }
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
                strategy = fill_order_at_price(
                    config,
                    strategy,
                    prices,
                    state,
                    &order.contract,
                    order.action,
                    order.qty,
                    price,
                    bar.get_time(),
                    order_id,
                    &order.order_ref,
                    &order.oca_group,
                )?;
                order.status = RestingStatus::Filled;
                did_fill = true;
                match order.role {
                    OrderRole::Entry => {
                        filled_entry_brackets.insert(order.bracket_id);
                        if order.oca_group.starts_with(crate::strategy_wrappers::payload::OG_PREFIX) {
                            filled_entry_groups.insert(order.oca_group.clone());
                        }
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
        // OrderGate entry-sibling mirror: one "og-" entry of a group filled
        // this bar → cancel sibling ENTRIES sharing the group (non-empty
        // groups only — engine-placed orders carry "").
        else if order.oca_group.starts_with(crate::strategy_wrappers::payload::OG_PREFIX)
            && filled_entry_groups.contains(&order.oca_group)
        {
            order.status = RestingStatus::Cancelled;
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

    Ok(strategy)
}

/// Fill an order at a known price (market/limit/stop/mid — the fill model already
/// resolved the exact price). Updates position + transaction + CASH:SGD, charges
/// commission, but applies NO extra slippage.
fn fill_order_at_price<'a>(
    config: &BacktestConfig,
    strategy: Option<&'a mut StrategyEnum>,
    prices: &dyn PriceSupplier,
    state: &InMemoryState,
    contract: &Contract,
    action: ibapi::orders::Action,
    qty: f64,
    fill_price: f64,
    bar_time: DateTime<Utc>,
    order_id: &mut i32,
    // §8 pooled emission: the completing sleeve emits on behalf of ALL
    // sleeves — every order's `order_ref` names its OWNING strategy.
    order_ref: &str,
    // OCA group of the order being filled ("" = none) — threaded into the
    // mailbox so the OrderGate can join fills to its entries (§4).
    oca_group: &str,
) -> Result<Option<&'a mut StrategyEnum>, String> {
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

    // ── §8 ROUTED SETTLEMENT: when the fill router resolves `order_ref` to
    // a DIFFERENT strategy's account than the passing reconciler's, the
    // fill settles there (positions, transactions, CASH) and the realized
    // fill is mailboxed for the owning sleeve's next flush — pooled fills
    // bypass every strategy hook (the completing strategy must NOT ingest
    // peer fills into its severity streams). The router is only installed
    // by the multi replay runner; absence (or a self-named order) falls
    // through to the byte-exact legacy path below.
    let routed = routed_state(order_ref)
        .filter(|t| t.strategy_name != state.strategy_name);
    if let Some(tgt) = routed {
        let key = PositionKey {
            strategy: tgt.strategy_name.clone(),
            stock: get_local_symbol(contract),
            primary_exchange: contract.primary_exchange.to_string(),
            currency: contract.currency.to_string(),
        };
        tgt.update_current_additive(key.clone(), signed_qty, fill_price);
        tgt.record_transaction(InMemoryTransaction {
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
        // order-id BEFORE the mailbox push (post-increment convention).
        *order_id += 1;
        mailbox_push(
            &tgt.strategy_name,
            MailFill {
                sym: key.stock.clone(),
                qty: signed_qty as i64,
                px: fill_price,
                date: bar_time.with_timezone(&New_York).date_naive(),
                order_id: *order_id as i64,
                oca_group: oca_group.to_string(),
            },
        );
        let sgd_value = signed_qty * fill_price * fx_rate;
        let cash_key = PositionKey {
            strategy: tgt.strategy_name.clone(),
            stock: "CASH:SGD".to_string(),
            primary_exchange: "".to_string(),
            currency: "SGD".to_string(),
        };
        {
            let mut guard = tgt
                .current_positions
                .write()
                .expect("InMemoryState current_positions poisoned");
            let cash = guard.entry(cash_key.clone()).or_insert(InMemoryPosition {
                quantity: 0.0,
                avg_price: 1.0,
            });
            cash.quantity -= sgd_value + fees;
        }
        super::diag::diag_fill(
            bar_time.timestamp_millis(),
            bar_time.with_timezone(&New_York).date_naive(),
            &state.strategy_name,
            order_ref,
            Some(&tgt.strategy_name),
            &key.stock,
            &format!("{:?}", action),
            signed_qty,
            fill_price,
            fees,
            fx_rate,
        );
        return Ok(strategy); // peer fills reach their own chain via the mailbox
    }

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
    let new_strategy = if let Some(strategy_inner) = strategy {
        let execution_data = synthetic_execution(
            *order_id,
            contract,
            &action,
            signed_qty, // +buy / -sell, as decided by decide_fill
            fill_price,
            bar_time,
            &key.strategy,
        );
        if let Err(e) = strategy_inner.on_fill(execution_data) {
            tracing::warn!("backtest on_fill failed: {e:?}");
        }
        super::diag::diag_fill(
            bar_time.timestamp_millis(),
            bar_time.with_timezone(&New_York).date_naive(),
            &key.strategy,
            order_ref,
            None,
            &key.stock,
            &format!("{:?}", action),
            signed_qty,
            fill_price,
            fees,
            fx_rate,
        );
        Some(strategy_inner)
    } else {
        None
    };

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
    Ok(new_strategy)
}

// ── Tests (2026-10-30 ambient-bar leak fix) ─────────────────────────────────
// The old `lookup_bar(config, contracts, target, fallback)` fell back to the
// AMBIENT bar (whatever symbol's bar is being processed) when the target
// symbol had no bar at the current slot — a thin symbol's 09:30 flush delta
// filled at AAPL's 09:30 close (u173c: ARIS sold 4,492 sh @ 170.22 vs the
// real ~2.96) → phantom P&L. These pins: own-bar-or-nothing; the deferral
// fills at the symbol's OWN next bar's own close.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::models::AssetType;
    use crate::execution::order_engine::OrderIBKR;
    use crate::helpers::contract::build_contract_from_stock;
    use chrono::TimeZone;
    use ibapi::market_data::realtime::WhatToShow;

    struct NoFx;
    impl PriceSupplier for NoFx {
        fn get_current_price(&self, _c: Contract, _vwap: bool, _t: &[&str]) -> Result<f64, String> {
            Err("no fx in tests".into())
        }
    }

    fn apple() -> Contract {
        build_contract_from_stock(&"AAPL".to_string(), &"NASDAQ".to_string(), &"USD".to_string())
    }

    fn aris() -> Contract {
        build_contract_from_stock(&"ARIS".to_string(), &"NASDAQ".to_string(), &"USD".to_string())
    }

    fn cfg() -> BacktestConfig {
        BacktestConfig::new(10_000.0)
            .slippage_bps(0.0)
            .contracts(vec![apple(), aris()])
    }

    fn stock_bar(c: &Contract, time: DateTime<Utc>, close: f64) -> HistoricalDataFullKeys {
        HistoricalDataFullKeys::from_data(c, &WhatToShow::Trades, time, close, close, close, close, 1_000.0)
    }

    /// 09:30 / 09:35 ET on 2024-03-05 (weekly session, no EOD edge).
    fn slot(minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 3, 5, 14, minute, 0).unwrap()
    }

    fn mk_order(c: &Contract, qty: f64, buy: bool) -> OrderIBKR {
        let action = if buy { ibapi::orders::Action::Buy } else { ibapi::orders::Action::Sell };
        let order = Order {
            action,
            total_quantity: qty,
            order_ref: "test".to_string(),
            ..Default::default()
        };
        OrderIBKR::new(c.clone(), order, -1)
    }

    /// A cancel key as a strategy would read it from the open-orders DB:
    /// `from_contract_and_order` applies the signed-quantity convention
    /// (Sell → negative), which the cancel seam's side filter relies on.
    fn mk_cancel_key(c: &Contract, qty: f64, buy: bool, order_ref: &str) -> OpenOrdersFullKeys {
        let action = if buy { ibapi::orders::Action::Buy } else { ibapi::orders::Action::Sell };
        let order = Order {
            action,
            total_quantity: qty,
            order_ref: order_ref.to_string(),
            ..Default::default()
        };
        OpenOrdersFullKeys::from_contract_and_order(c, &order, 0.0)
    }

    /// A resting LMT entry — limit entries rest (the immediate-fill loop skips
    /// non-immediate models), and `cancel_stale_deferrals` does NOT touch
    /// market-class-only deferrals, so a cancel+replace of the SAME key is
    /// discriminated by the cancel seam's ordering, not by the re-flush dedup.
    fn mk_limit_order(c: &Contract, qty: f64, px: f64, buy: bool) -> OrderIBKR {
        let action = if buy { ibapi::orders::Action::Buy } else { ibapi::orders::Action::Sell };
        let order = Order {
            action,
            order_type: "LMT".to_string(),
            limit_price: Some(px),
            total_quantity: qty,
            order_ref: "test".to_string(),
            ..Default::default()
        };
        OrderIBKR::new(c.clone(), order, -1)
    }

    fn key(state: &InMemoryState, sym: &str) -> PositionKey {
        PositionKey {
            strategy: state.strategy_name.clone(),
            stock: sym.to_string(),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
        }
    }

    fn resting_count(state: &InMemoryState, sym: &str) -> usize {
        state
            .resting_orders
            .read()
            .expect("resting poisoned")
            .iter()
            .filter(|o| o.contract.symbol.to_string() == sym && o.status == RestingStatus::Resting)
            .count()
    }

    fn txn_prices(state: &InMemoryState, sym: &str) -> Vec<f64> {
        state
            .transactions
            .read()
            .expect("transactions poisoned")
            .iter()
            .filter(|t| t.stock == sym)
            .map(|t| t.price)
            .collect()
    }

    #[test]
    fn emit_defers_missing_own_bar_then_fills_at_own_close() {
        let config = cfg();
        let state = InMemoryState::new("t".to_string(), 10_000.0);
        let mut order_id = 0;
        let prices = NoFx;

        // Slot 1 (09:30): AAPL bar present, ARIS absent. An ARIS MKT buy of 10
        // shares must NOT fill — and certainly not at AAPL's 200.00 close.
        let aapl_bar = stock_bar(&apple(), slot(30), 200.0);
        let contracts = vec![Some(aapl_bar.clone()), None];
        let outcome = BarUpdateOutcome::EmitOrders {
            place_orders: vec![mk_order(&aris(), 10.0, true)],
            cancel_orders: vec![],
        };
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &outcome, &apple(), &aapl_bar, &contracts, &mut order_id,
        )
        .expect("reconcile slot1");

        assert_eq!(state.current_qty(&key(&state, "ARIS")), 0.0, "no phantom fill on deferral");
        assert_eq!(txn_prices(&state, "ARIS").len(), 0, "no ARIS transaction from an ambient bar");
        assert_eq!(resting_count(&state, "ARIS"), 1, "exactly one parked deferral");

        // Slot 2 (09:35): ARIS's OWN bar arrives at close 10.00. The deferral
        // fills there — NOT at AAPL's close.
        let aapl_bar2 = stock_bar(&apple(), slot(35), 205.0);
        let aris_bar2 = stock_bar(&aris(), slot(35), 10.0);
        let contracts2 = vec![Some(aapl_bar2), Some(aris_bar2.clone())];
        let outcome2 = BarUpdateOutcome::NoAction;
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &outcome2, &aris(), &aris_bar2, &contracts2, &mut order_id,
        )
        .expect("reconcile slot2");

        assert_eq!(state.current_qty(&key(&state, "ARIS")), 10.0, "fills once at own bar");
        assert_eq!(txn_prices(&state, "ARIS"), vec![10.0], "fill price is the OWN close");
        assert_eq!(txn_prices(&state, "AAPL").len(), 0, "AAPL untouched");
        assert_eq!(resting_count(&state, "ARIS"), 0, "deferral consumed");
    }

    #[test]
    fn reflushed_delta_cancels_stale_deferral_never_double_fills() {
        let config = cfg();
        let state = InMemoryState::new("t".to_string(), 10_000.0);
        let mut order_id = 0;
        let prices = NoFx;

        let aapl_bar = stock_bar(&apple(), slot(30), 200.0);
        let contracts = vec![Some(aapl_bar.clone()), None];
        let outcome = BarUpdateOutcome::EmitOrders {
            place_orders: vec![mk_order(&aris(), 10.0, true)],
            cancel_orders: vec![],
        };
        for _ in 0..2 {
            handle_bar_update_outcome_in_memory(
                &config, None, &prices, &state, &outcome, &apple(), &aapl_bar, &contracts, &mut order_id,
            )
            .expect("reconcile reflush");
        }

        assert_eq!(resting_count(&state, "ARIS"), 1, "second flush cancelled the stale deferral");
        assert_eq!(state.current_qty(&key(&state, "ARIS")), 0.0);

        // The single surviving deferral fills exactly once.
        let aris_bar = stock_bar(&aris(), slot(35), 10.0);
        let contracts2 = vec![Some(stock_bar(&apple(), slot(35), 205.0)), Some(aris_bar.clone())];
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &BarUpdateOutcome::NoAction, &aris(), &aris_bar, &contracts2,
            &mut order_id,
        )
        .expect("reconcile fill");
        assert_eq!(txn_prices(&state, "ARIS"), vec![10.0], "exactly ONE fill at own close");
    }

    // ── Cancel seam (EmitOrders.cancel_orders, 2026-11-11) ────────────────────
    // A strategy's cancel_orders carry the open-order keys it read BEFORE the
    // emission (live: DB pks; here: coarse identity — symbol + order_ref +
    // side). The seam sweeps Resting entries FIRST so a cancel + same-key
    // re-place in one emission never kills the fresh placement.

    #[test]
    fn cancel_orders_sweeps_matching_resting_entry_and_blocks_later_fill() {
        let config = cfg();
        let state = InMemoryState::new("t".to_string(), 10_000.0);
        let mut order_id = 0;
        let prices = NoFx;

        // Slot 1 (09:30): ARIS bar absent → MKT buy 10 parks as a deferral.
        let aapl_bar = stock_bar(&apple(), slot(30), 200.0);
        let contracts = vec![Some(aapl_bar.clone()), None];
        let outcome = BarUpdateOutcome::EmitOrders {
            place_orders: vec![mk_order(&aris(), 10.0, true)],
            cancel_orders: vec![],
        };
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &outcome, &apple(), &aapl_bar, &contracts, &mut order_id,
        )
        .expect("reconcile slot1");
        assert_eq!(resting_count(&state, "ARIS"), 1, "deferral parked");

        // Slot 2: emit a cancel key for the parked deferral (same identity the
        // sleeve reads off its open-order book: ARIS, order_ref "test", BUY side).
        let outcome2 = BarUpdateOutcome::EmitOrders {
            place_orders: vec![],
            cancel_orders: vec![mk_cancel_key(&aris(), 10.0, true, "test")],
        };
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &outcome2, &apple(), &aapl_bar, &contracts, &mut order_id,
        )
        .expect("reconcile cancel");
        assert_eq!(resting_count(&state, "ARIS"), 0, "matching key swept the deferral");
        assert_eq!(state.current_qty(&key(&state, "ARIS")), 0.0, "cancel is not a fill");

        // Slot 3: ARIS's own bar arrives — the cancelled deferral must NOT fill.
        let aris_bar3 = stock_bar(&aris(), slot(35), 10.0);
        let contracts3 = vec![Some(stock_bar(&apple(), slot(35), 205.0)), Some(aris_bar3.clone())];
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &BarUpdateOutcome::NoAction, &aris(), &aris_bar3,
            &contracts3, &mut order_id,
        )
        .expect("reconcile slot3");
        assert_eq!(txn_prices(&state, "ARIS").len(), 0, "cancelled deferral never fills");
        assert_eq!(state.current_qty(&key(&state, "ARIS")), 0.0);
    }

    #[test]
    fn cancel_orders_never_cancels_a_non_matching_key() {
        let config = cfg();
        let state = InMemoryState::new("t".to_string(), 10_000.0);
        let mut order_id = 0;
        let prices = NoFx;

        let aapl_bar = stock_bar(&apple(), slot(30), 200.0);
        let contracts = vec![Some(aapl_bar.clone()), None];
        let outcome = BarUpdateOutcome::EmitOrders {
            place_orders: vec![mk_order(&aris(), 10.0, true)],
            cancel_orders: vec![],
        };
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &outcome, &apple(), &aapl_bar, &contracts, &mut order_id,
        )
        .expect("reconcile slot1");
        assert_eq!(resting_count(&state, "ARIS"), 1);

        // Three wrong keys in ONE emission: wrong symbol, wrong order_ref,
        // wrong side (SELL vs the parked BUY). None may sweep the deferral.
        let outcome2 = BarUpdateOutcome::EmitOrders {
            place_orders: vec![],
            cancel_orders: vec![
                mk_cancel_key(&apple(), 10.0, true, "test"),  // wrong symbol
                mk_cancel_key(&aris(), 10.0, true, "other"),  // wrong order_ref
                mk_cancel_key(&aris(), 10.0, false, "test"),  // wrong side
            ],
        };
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &outcome2, &apple(), &aapl_bar, &contracts, &mut order_id,
        )
        .expect("reconcile non-matching");
        assert_eq!(resting_count(&state, "ARIS"), 1, "only an exact (symbol, order_ref, side) match cancels");
        assert_eq!(state.current_qty(&key(&state, "ARIS")), 0.0);
    }

    #[test]
    fn cancel_then_replace_same_key_in_one_emission_keeps_new_rest() {
        let config = cfg();
        let state = InMemoryState::new("t".to_string(), 10_000.0);
        let mut order_id = 0;
        let prices = NoFx;

        // Slot 1: a resting ARIS LMT buy 10 @ 10.00 (limit entries rest).
        let aapl_bar = stock_bar(&apple(), slot(30), 200.0);
        let contracts = vec![Some(aapl_bar.clone()), None];
        let outcome = BarUpdateOutcome::EmitOrders {
            place_orders: vec![mk_limit_order(&aris(), 10.0, 10.0, true)],
            cancel_orders: vec![],
        };
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &outcome, &apple(), &aapl_bar, &contracts, &mut order_id,
        )
        .expect("reconcile slot1");
        assert_eq!(resting_count(&state, "ARIS"), 1, "LMT entry rests");

        // Slot 2: cancel the SAME key + re-place an identical LMT in ONE
        // emission. The sweep runs FIRST: old rest swept, new rest parks.
        // (A place-then-cancel sweep would kill BOTH — the ordering pin.)
        let outcome2 = BarUpdateOutcome::EmitOrders {
            place_orders: vec![mk_limit_order(&aris(), 10.0, 10.0, true)],
            cancel_orders: vec![mk_cancel_key(&aris(), 10.0, true, "test")],
        };
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &outcome2, &apple(), &aapl_bar, &contracts, &mut order_id,
        )
        .expect("reconcile cancel+replace");
        assert_eq!(resting_count(&state, "ARIS"), 1, "old swept, fresh placement survives");
        assert_eq!(state.current_qty(&key(&state, "ARIS")), 0.0);

        // Slot 3: ARIS's own bar (9.00) crosses the 10.00 buy limit — the
        // replacement fills exactly once.
        let aris_bar3 = stock_bar(&aris(), slot(35), 9.0);
        let contracts3 = vec![Some(stock_bar(&apple(), slot(35), 205.0)), Some(aris_bar3.clone())];
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &BarUpdateOutcome::NoAction, &aris(), &aris_bar3,
            &contracts3, &mut order_id,
        )
        .expect("reconcile slot3");
        assert_eq!(txn_prices(&state, "ARIS").len(), 1, "replacement fills exactly once");
        assert_eq!(state.current_qty(&key(&state, "ARIS")), 10.0);
    }

    #[test]
    fn db_arm_defers_missing_own_bar_then_fills_at_own_close() {
        let config = cfg();
        let state = InMemoryState::new("t".to_string(), 10_000.0);
        let mut order_id = 0;
        let prices = NoFx;

        // Seed a full-fund target book in the state (the /openDb mode's intent):
        // AAPL flat, ARIS 10 shares. Slot 1's flush runs under AAPL's bar;
        // ARIS has no bar of its own at this slot.
        let tkey = key(&state, "ARIS");
        state
            .target_positions
            .write()
            .expect("targets poisoned")
            .insert(tkey, InMemoryPosition { quantity: 10.0, avg_price: 0.0 });

        let aapl_bar = stock_bar(&apple(), slot(30), 200.0);
        let contracts = vec![Some(aapl_bar.clone()), None];
        let outcome = BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]);
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &outcome, &apple(), &aapl_bar, &contracts, &mut order_id,
        )
        .expect("reconcile db slot1");

        assert_eq!(state.current_qty(&key(&state, "ARIS")), 0.0, "Db-arm: no phantom ARIS fill at AAPL's close");
        assert_eq!(txn_prices(&state, "ARIS").len(), 0, "Db-arm: no ARIS transaction from an ambient bar");
        assert_eq!(resting_count(&state, "ARIS"), 1, "Db-arm deferral parked");
        assert_eq!(txn_prices(&state, "AAPL").len(), 0, "AAPL target flat → no fill");

        // Slot 2: ARIS's own bar → the deferral fills at the OWN close 10.00;
        // the Db flush (driven by ARIS's bar) tops up 10 − 0 = +10 at own price.
        let aris_bar2 = stock_bar(&aris(), slot(35), 10.0);
        let contracts2 = vec![Some(stock_bar(&apple(), slot(35), 205.0)), Some(aris_bar2.clone())];
        handle_bar_update_outcome_in_memory(
            &config, None, &prices, &state, &BarUpdateOutcome::NoAction, &aris(), &aris_bar2, &contracts2,
            &mut order_id,
        )
        .expect("reconcile db slot2");

        assert_eq!(state.current_qty(&key(&state, "ARIS")), 10.0, "Db-arm: single fill at own bar");
        assert_eq!(txn_prices(&state, "ARIS"), vec![10.0], "Db-arm fill price is the OWN close");
    }
}
