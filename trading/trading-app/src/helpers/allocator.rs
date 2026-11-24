//! Cross-strategy funding: absolute-weight allocation with FX-routed cash placement.
//!
//! Weights are absolute claims on the whole fund (`0.7` = "ideally 70% of the
//! fund"). A strategy that exists and does not ask to reallocate is frozen at
//! its actual equity; one with no rows yet is always seeded. `__unallocated__`
//! is the only slack actor: it absorbs leftovers, pays for conversions, and is
//! the `order_ref` of every funding FX order.
//!
//! Invariants: a strategy may only be **credited** in a currency listed in its
//! `cash_prefs` — an empty list means it accepts any currency and is paid in
//! kind from whatever the hub holds (decreases are unrestricted),
//! `sum(delta) == 0`, and nothing is booked for a conversion that could not be
//! planned.
//!
//! Needs `Consolidator::get_fund_value(handle, pool) -> Result<f64, String>`
//! and `OrderEngine::place_order_checked(handle, pool, weak_client, order)
//! -> Result<i32, String>`.

use crate::database::tables::DbTables;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use ibapi::Client;
use ibapi::orders::{Action, order_builder::market_order};
use sqlx::PgPool;
use tokio::runtime::Handle;

use crate::{
    database::{
        models::{AssetType, CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys},
        models_crud::current_positions::current_positions::{
            CurrentPositionsFullKeys, CurrentPositionsOps, CurrentPositionsPrimaryKeys,
            CurrentPositionsUpdateKeys,
        },
    },
    execution::order_engine::{OrderEngine, OrderIBKR},
    helpers::contract::build_contract_from_stock,
    market_data::{
        consolidator::Consolidator,
        traits::{current_price::PriceSupplier, strategy_value::GetStrategyValue},
    },
    strategy::strategy::StrategyFund,
};

/// The unallocated bucket: never in the roster, accepts any currency.
pub const UNALLOCATED: &str = "__unallocated__";

const EPS: f64 = 1e-9;

/// Async→sync bridge. Funding runs on a startup thread, outside the runtime.
macro_rules! on_rt {
    ($handle:expr, $fut:expr) => {
        futures::executor::block_on($handle.spawn($fut))
            .map_err(|e| format!("funding: task join error: {e}"))??
    };
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FundingSummary {
    fund_value: f64,
    allocations: Vec<(String, f64)>,
    unallocated: f64,
}

struct Book {
    name: String,
    fund: StrategyFund,
    equity: f64,
    cash: BTreeMap<String, f64>,
    #[allow(dead_code)]
    exists: bool,
    participating: bool,
    target: f64,
}

impl Book {
    fn delta(&self) -> f64 {
        self.target - self.equity
    }
}

/// One CASH row adjustment, in units of that currency.
struct Move {
    strategy: String,
    currency: String,
    quantity: f64,
}

pub fn fund_strategies(
    handle: &Handle,
    client: &Arc<Client>,
    consolidator: &Arc<Consolidator>,
    strategies: &[(String, StrategyFund)],
    pool: PgPool,
) -> Result<FundingSummary, String> {
    // ── Snapshot ────────────────────────────────────────────────────────
    let fund_value = consolidator.get_fund_value(&client, &handle, &pool)?;
    let rows = on_rt!(handle, {
        let pool = pool.clone();
        async move {
            let mut db = crate::database::db::DbDirect::connect(&pool);
            db.current_positions(&AssetType::Stock)
                .get_all_pos_grouped_by_strat()
                .await
        }
    });

    let mut names = std::collections::HashSet::new();
    let mut has_dup = "";
    let mut books: Vec<Book> = strategies
        .into_iter()
        .map(|(name, fund_strat)| {
            if !names.insert(name.as_str()) {
                has_dup = name.as_str();
            }
            let exists = rows.iter().any(
                |r| matches!(r, CurrentPositionsFullKeys::Stock(k) if k.strategy.as_str() == name),
            );
            let participating = fund_strat.to_reallocate || !exists;
            Book {
                name: name.to_string(),
                fund: fund_strat.clone(),
                equity: consolidator.get_strategy_sgd_value(&name).unwrap_or(0.0),
                cash: BTreeMap::new(),
                exists,
                participating,
                target: 0.0,
            }
        })
        .collect();
    if has_dup != "" {
        return Err(format!("funding: duplicate strategy '{}'", has_dup));
    }
    books.push(Book {
        name: UNALLOCATED.to_string(),
        fund: StrategyFund::default(),
        equity: 0.0,
        cash: BTreeMap::new(),
        exists: true,
        participating: false,
        target: 0.0,
    });
    let hub = books.len() - 1;

    for row in &rows {
        let CurrentPositionsFullKeys::Stock(k) = row else {
            continue;
        };
        let Some(ccy) = k.stock.strip_prefix("CASH:") else {
            continue;
        };
        if let Some(book) = books.iter_mut().find(|b| b.name == k.strategy) {
            *book.cash.entry(ccy.to_string()).or_default() += k.quantity;
        }
    }

    // ── Validate ────────────────────────────────────────────────────────
    let total_weight: f64 = books.iter().map(|b| b.fund.weight).sum();
    if total_weight > 1.0 + EPS {
        return Err(format!(
            "funding: weights sum to {total_weight}, must be <= 1"
        ));
    }
    for b in &books {
        if b.name == UNALLOCATED {
            continue;
        }
        if !b.fund.weight.is_finite() || b.fund.weight < 0.0 {
            return Err(format!("funding: '{}' has negative weight", b.name));
        }
    }

    // ── Solve: ration absolute wants across A, then knock out floor misses ──
    // Solve exactly which strategies are allocated and how much weight to give
    if fund_value <= 0.0 {
        return Err(format!("funding: non-positive fund value {fund_value}"));
    }
    let mut live: Vec<usize> = (0..hub).filter(|&i| books[i].participating).collect();
    let available = books[hub].equity + live.iter().map(|&i| books[i].equity).sum::<f64>();
    if available <= 0.0 {
        return Err(format!(
            "funding: non-positive available capital {available}"
        ));
    }
    loop {
        let sum_w: f64 = live.iter().map(|&i| books[i].fund.weight).sum();
        let scale = if sum_w > 0.0 {
            (available / (sum_w * fund_value)).min(1.0)
        } else {
            0.0
        };
        if scale < 1.0 - EPS {
            tracing::warn!(
                "funding: over-subscribed (wants {:.2} > available {available:.2}), scale {scale:.4}",
                sum_w * fund_value
            );
        }
        for &i in &live {
            books[i].target = scale * books[i].fund.weight * fund_value;
        }
        // Lowest rank first: smaller weight, higher priority number, name desc.
        let violator = live
            .iter()
            .copied()
            .filter(|&i| books[i].target < books[i].fund.min_capital)
            .min_by(|&a, &b| {
                books[a]
                    .fund
                    .weight
                    .total_cmp(&books[b].fund.weight)
                    .then(books[b].fund.priority.cmp(&books[a].fund.priority))
                    .then(books[b].name.cmp(&books[a].name))
            });
        let Some(i) = violator else { break };
        tracing::warn!(
            "funding: '{}' rationed target {:.2} < min_capital {:.2}, knocked out",
            books[i].name,
            books[i].target,
            books[i].fund.min_capital
        );
        books[i].target = 0.0;
        live.retain(|&x| x != i);
    }

    // ── FX rates to SGD ────────────────────────────────────────────────
    let mut rates: BTreeMap<String, f64> = BTreeMap::new();
    let mut currencies: Vec<String> = books.iter().flat_map(|b| b.cash.keys().cloned()).collect();
    currencies.sort();
    currencies.dedup();
    for ccy in currencies {
        let rate = if ccy == "SGD" {
            1.0
        } else {
            consolidator.get_current_price(
                build_contract_from_stock(
                    &format!("CASH:{ccy}"),
                    &String::new(),
                    &"SGD".to_string(),
                ),
                false,
                &[],
            )?
        };
        rates.insert(ccy, rate);
    }

    // ── Leg 1: payers drain into the hub (cheapest currency first) ──────
    let mut leg1: Vec<Move> = Vec::new();
    let mut projected = books[hub].cash.clone();
    for payer in books.iter().filter(|b| b.participating && b.delta() < -EPS) {
        let mut need = -payer.delta();
        let mut candidates: Vec<(&String, f64)> = payer
            .cash
            .iter()
            .filter(|(_, qty)| **qty > 0.0)
            .map(|(ccy, qty)| (ccy, qty * *rates.get(ccy).unwrap_or(&1.0)))
            .collect();
        // Non-preferred positives first, then preferred in reverse order.
        candidates.sort_by(|a, b| {
            let rank = |ccy: &str| {
                payer
                    .fund
                    .cash_prefs
                    .iter()
                    .position(|p| p == ccy)
                    .map_or((0, 0), |i| (1, payer.fund.cash_prefs.len() - i))
            };
            rank(a.0).cmp(&rank(b.0)).then(b.1.total_cmp(&a.1))
        });
        for (ccy, value) in candidates {
            if need <= EPS {
                break;
            }
            let take = need.min(value);
            let units = take / rates.get(ccy).copied().unwrap_or(1.0);
            leg1.push(Move {
                strategy: payer.name.clone(),
                currency: ccy.clone(),
                quantity: -units,
            });
            leg1.push(Move {
                strategy: UNALLOCATED.to_string(),
                currency: ccy.clone(),
                quantity: units,
            });
            *projected.entry(ccy.clone()).or_default() += units;
            need -= take;
        }
        if need > EPS {
            // All cash is in positions: real leverage, clears on the next sale.
            let ccy = payer
                .fund
                .cash_prefs
                .last()
                .cloned()
                .unwrap_or_else(|| "SGD".to_string());
            let units = need / rates.get(&ccy).copied().unwrap_or(1.0);
            tracing::error!(
                "funding: '{}' is cash-short, pushing {ccy} negative by {need:.2} SGD",
                payer.name
            );
            leg1.push(Move {
                strategy: payer.name.clone(),
                currency: ccy.clone(),
                quantity: -units,
            });
            leg1.push(Move {
                strategy: UNALLOCATED.to_string(),
                currency: ccy.clone(),
                quantity: units,
            });
            *projected.entry(ccy).or_default() += units;
        }
    }
    // Book leg 1 first so the poll baseline is exact.
    for m in &leg1 {
        write_cash(
            handle,
            consolidator,
            &pool,
            &m.strategy,
            &m.currency,
            m.quantity,
        )?;
    }

    // ── Conversions the projected hub balance cannot cover in kind ──────
    let mut fx: Vec<(String, String, f64)> = Vec::new();
    let mut pending = 0usize;
    for receiver in books.iter().filter(|b| b.participating && b.delta() > EPS) {
        // Empty `cash_prefs` = accept anything: leg 2 pays this receiver in
        // kind from whatever the hub holds, so there is nothing to convert
        // and no preferred buy currency to convert into.
        if receiver.fund.cash_prefs.is_empty() {
            continue;
        }
        let mut need = receiver.delta();
        for ccy in &receiver.fund.cash_prefs {
            if need <= EPS {
                break;
            }
            let take = (projected.get(ccy).copied().unwrap_or(0.0).max(0.0)
                * rates.get(ccy).copied().unwrap_or(1.0))
            .min(need);
            *projected.entry(ccy.clone()).or_default() -=
                take / rates.get(ccy).copied().unwrap_or(1.0);
            need -= take;
        }
        if need <= EPS {
            continue;
        }
        // Sell the hub's largest surplus, buy the receiver's top preference.
        let source = projected
            .iter()
            .filter(|(ccy, qty)| **qty > EPS && !receiver.fund.cash_prefs.contains(ccy))
            .max_by(|a, b| {
                (a.1 * rates.get(a.0).copied().unwrap_or(1.0))
                    .total_cmp(&(b.1 * rates.get(b.0).copied().unwrap_or(1.0)))
            })
            .map(|(ccy, _)| ccy.clone());
        let Some(sell) = source else {
            tracing::error!(
                "funding: no positive balance to convert for '{}' (short {need:.2} SGD)",
                receiver.name
            );
            pending += 1;
            continue;
        };
        let buy = receiver.fund.cash_prefs[0].clone();
        let buy_qty = need / rates.get(&buy).copied().unwrap_or(1.0);
        *projected.entry(sell.clone()).or_default() -=
            need / rates.get(&sell).copied().unwrap_or(1.0);
        *projected.entry(buy.clone()).or_default() += buy_qty;
        fx.push((sell, buy, buy_qty));
    }

    // ── Execute conversions, waiting for the hub's bought leg ───────────
    for (sell, buy, buy_qty) in &fx {
        let before = hub_cash(handle, &pool)?.get(buy).copied().unwrap_or(0.0);

        #[cfg(feature = "backtest")]
        return Err(format!(
            "funding: FX {sell} -> {buy} is not available under the backtest feature"
        ));
        #[cfg(not(feature = "backtest"))]
        {
            let mut order = market_order(Action::Buy, *buy_qty);
            order.order_ref = UNALLOCATED.to_string();
            OrderEngine::place_order_checked(
                handle.clone(),
                pool.clone(),
                &Arc::downgrade(&consolidator.client),
                OrderIBKR::new(
                    build_contract_from_stock(
                        &format!("FX:{sell}/{buy}"),
                        &String::new(),
                        &String::new(),
                    ),
                    order,
                    -1,
                ),
            )
            .map_err(|e| format!("funding: FX {sell} -> {buy} rejected: {e}"))?;
        }

        if buy_qty * rates.get(buy).copied().unwrap_or(1.0) < 25_000.0 {
            tracing::warn!("funding: {sell} -> {buy} below IDEALPRO minimum, routed odd-lot");
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while hub_cash(handle, &pool)?.get(buy).copied().unwrap_or(0.0) + EPS < before + buy_qty {
            if std::time::Instant::now() >= deadline {
                return Err(format!(
                    "funding: FX fill for CASH:{buy} did not arrive in 60s"
                ));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    // ── Leg 2: pay receivers from actual hub balances, accepted ccy only ──
    let mut hub_balances = hub_cash(handle, &pool)?;
    for receiver in books.iter().filter(|b| b.participating && b.delta() > EPS) {
        let mut need = receiver.delta();
        let accepted = acceptable_currencies(&receiver.fund.cash_prefs, &hub_balances, &rates);
        for ccy in &accepted {
            if need <= EPS {
                break;
            }
            let available = hub_balances.get(ccy).copied().unwrap_or(0.0).max(0.0);
            let take = (available * rates.get(ccy).copied().unwrap_or(1.0)).min(need);
            if take <= EPS {
                continue;
            }
            let units = take / rates.get(ccy).copied().unwrap_or(1.0);
            write_cash(handle, consolidator, &pool, &receiver.name, ccy, units)?;
            write_cash(handle, consolidator, &pool, UNALLOCATED, ccy, -units)?;
            *hub_balances.entry(ccy.clone()).or_default() -= units;
            need -= take;
        }
        if need > EPS {
            tracing::error!(
                "funding: '{}' under-funded by {need:.2} SGD (left in hub)",
                receiver.name
            );
            pending += 1;
        }
    }

    // ── Result ─────────────────────────────────────────────────────────
    let allocations: Vec<(String, f64)> = books
        .iter()
        .filter(|b| b.participating)
        .map(|b| (b.name.clone(), b.target))
        .collect();
    let unallocated = available - allocations.iter().map(|(_, t)| t).sum::<f64>();

    tracing::info!(
        "funding: F={fund_value:.2} A={available:.2} unallocated={unallocated:.2} \
         fx={} pending={pending}",
        fx.len()
    );
    for (name, target) in &allocations {
        tracing::info!("funding: {name} -> {target:.2}");
    }

    Ok(FundingSummary {
        fund_value,
        allocations,
        unallocated,
    })
}

/// Currencies a book accepts as credit, most-preferred first.
///
/// An empty `cash_prefs` means "accept anything": use the hub's positive
/// balances, largest SGD value first, so funding finishes with as few
/// currencies as possible and at most one balance is left partially drained.
fn acceptable_currencies(
    cash_prefs: &[String],
    available: &BTreeMap<String, f64>,
    rates: &BTreeMap<String, f64>,
) -> Vec<String> {
    if !cash_prefs.is_empty() {
        return cash_prefs.to_vec();
    }
    let mut ranked: Vec<(String, f64)> = available
        .iter()
        .filter(|(_, qty)| **qty > EPS)
        .map(|(ccy, qty)| (ccy.clone(), qty * rates.get(ccy).copied().unwrap_or(1.0)))
        .collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
    ranked.into_iter().map(|(ccy, _)| ccy).collect()
}

/// One additive CASH row write. `avg_price` is the currency's SGD rate, matching
/// what the syncer stores for cash rows.
fn write_cash(
    handle: &Handle,
    consolidator: &Arc<Consolidator>,
    pool: &PgPool,
    strategy: &str,
    currency: &str,
    quantity: f64,
) -> Result<(), String> {
    if quantity.abs() <= EPS {
        return Ok(());
    }
    let avg_price = if currency == "SGD" {
        1.0
    } else {
        consolidator.get_current_price(
            build_contract_from_stock(
                &format!("CASH:{currency}"),
                &String::new(),
                &"SGD".to_string(),
            ),
            false,
            &[],
        )?
    };
    let (strategy, stock) = (strategy.to_string(), format!("CASH:{currency}"));
    let pool = pool.clone();
    on_rt!(handle, async move {
        let mut db = crate::database::db::DbDirect::connect(&pool);
        db.current_positions(&AssetType::Stock)
            .update_positions_additive(
                CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
                    strategy,
                    stock,
                    primary_exchange: String::new(),
                    currency: "SGD".to_string(),
                }),
                CurrentPositionsUpdateKeys::Stock(CurrentStockPositionsUpdateKeys {
                    quantity: Some(quantity),
                    avg_price: Some(avg_price),
                    last_updated: None,
                }),
                #[cfg(feature = "backtest")]
                crate::backtester::setup::config::BacktestMode::Db,
            )
            .await
    });
    Ok(())
}

/// Every `CASH:*` balance the hub holds.
fn hub_cash(handle: &Handle, pool: &PgPool) -> Result<BTreeMap<String, f64>, String> {
    let pool = pool.clone();
    let rows = on_rt!(handle, async move {
        let mut db = crate::database::db::DbDirect::connect(&pool);
        db.current_positions(&AssetType::Stock)
            .get_pos_by_strat(
                UNALLOCATED,
                #[cfg(feature = "backtest")]
                crate::backtester::setup::config::BacktestMode::Db,
            )
            .await
    });
    let mut cash = BTreeMap::new();
    for row in &rows {
        let CurrentPositionsFullKeys::Stock(k) = row else {
            continue;
        };
        if let Some(ccy) = k.stock.strip_prefix("CASH:") {
            *cash.entry(ccy.to_string()).or_default() += k.quantity;
        }
    }
    Ok(cash)
}
