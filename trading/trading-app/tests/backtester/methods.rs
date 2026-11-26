//! `src/backtester/methods` tests — the pure helpers shared by both replay
//! lanes: the bar-matrix transposer, the in-memory bar cache, the in-memory
//! account state (+ fill router / mailbox), bracket parsing, and the
//! in-memory order matching engine (resting-order lifecycle, target deltas,
//! deferred fills, pooled-fill routing).
//!
//! BLOCKED (documented, not tested):
//! - `methods/mod.rs:42` `load_bars` — SQL against `market_data.historical_data`.
//! - `methods/historical.rs:43` `HistoricalReplay` + `:221` `run_multi` — need a
//!   `BacktestContext` (DB, `OrderStore::open()` file state, live strategies).
//! - `methods/in_memory/replay.rs:71/152/322/405` — `InMemoryReplay` needs a
//!   constructed `StrategyEnum`, a tokio `Handle`, a `Consolidator`, and the
//!   real market-data wiring.
//! - `methods/in_memory/diag.rs` — process-wide `OnceLock` env-gated file
//!   writers; toggling env in parallel tests is unsound, and the payload is
//!   diagnostic CSV only.
//! - `sweep.rs:45` `run_one_backtest` — constructs a real strategy and the
//!   light context (pool + handle).

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};
use ibapi::orders::{Action, Order};
use trading_app::backtester::execution::fill_model::{
    CommissionModel, FillModel, LimitFillModel, MarketFillModel, commission,
};
use trading_app::backtester::methods::in_memory::bar_cache::BarCache;
use trading_app::backtester::methods::in_memory::bracket::{
    CloseReason, OrderRole, RestingOrder, RestingStatus, build_resting_orders, parse_brackets,
};
use trading_app::backtester::methods::in_memory::reconcile::{
    drain_resting_orders_fill_first, handle_bar_update_outcome_in_memory,
};
use trading_app::backtester::methods::in_memory::state::{
    self, InMemoryPosition, InMemoryState, MailFill, PositionKey,
};
use trading_app::backtester::methods::transpose;
use trading_app::backtester::setup::config::BacktestConfig;
use trading_app::database::models::{AssetType, HistoricalStockDataPrimaryKeysWoTime};
use trading_app::database::models_crud::historical_data::historical_data::{
    HistoricalDataFullKeys, HistoricalDataPrimaryKeysWoTime,
};
use trading_app::execution::order_engine::OrderIBKR;
use trading_app::strategy::strategy::BarUpdateOutcome;

use crate::support::{FakePrices, assert_close, bar, bar_full, stock, utc};

fn t(min: i64) -> chrono::DateTime<Utc> {
    utc(2024, 6, 14, 14, 0, 0) + Duration::minutes(min)
}

fn qqq_pk() -> HistoricalDataPrimaryKeysWoTime {
    HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
        stock: "QQQ".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    })
}

fn pk_for(stock: &str) -> HistoricalDataPrimaryKeysWoTime {
    HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
        stock: stock.to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    })
}

// ── transpose ─────────────────────────────────────────────────────────────

#[test]
fn transpose_empty_inputs_are_empty() {
    assert!(transpose(vec![]).is_empty());
    assert!(transpose(vec![vec![]]).is_empty());
}

#[test]
fn transpose_merges_two_contracts_on_a_shared_time_axis() {
    let a = vec![
        bar_full("A", "NASDAQ", "USD", t(0), 1.0, 1.0, 1.0, 1.0),
        bar_full("A", "NASDAQ", "USD", t(1), 2.0, 2.0, 2.0, 2.0),
        bar_full("A", "NASDAQ", "USD", t(2), 3.0, 3.0, 3.0, 3.0),
    ];
    let b = vec![
        bar_full("B", "NASDAQ", "USD", t(0), 10.0, 10.0, 10.0, 10.0),
        bar_full("B", "NASDAQ", "USD", t(2), 30.0, 30.0, 30.0, 30.0),
    ];
    let rows = transpose(vec![a, b]);
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|r| r.len() == 2));

    assert_eq!(rows[0][0].as_ref().unwrap().get_time(), t(0));
    assert_eq!(rows[0][1].as_ref().unwrap().get_time(), t(0));
    assert_eq!(rows[1][0].as_ref().unwrap().get_time(), t(1));
    assert!(rows[1][1].is_none(), "B has no bar at t1");
    assert_eq!(rows[2][0].as_ref().unwrap().get_price(), 3.0);
    assert_eq!(rows[2][1].as_ref().unwrap().get_price(), 30.0);
}

#[test]
fn transpose_keeps_extra_times_from_a_longer_contract() {
    let a = vec![
        bar_full("A", "NASDAQ", "USD", t(0), 1.0, 1.0, 1.0, 1.0),
        bar_full("A", "NASDAQ", "USD", t(1), 2.0, 2.0, 2.0, 2.0),
    ];
    let b = vec![
        bar_full("B", "NASDAQ", "USD", t(0), 10.0, 10.0, 10.0, 10.0),
        bar_full("B", "NASDAQ", "USD", t(1), 20.0, 20.0, 20.0, 20.0),
        bar_full("B", "NASDAQ", "USD", t(2), 30.0, 30.0, 30.0, 30.0),
    ];
    let rows = transpose(vec![a, b]);
    assert_eq!(rows.len(), 3);
    assert!(rows[2][0].is_none());
    assert_eq!(rows[2][1].as_ref().unwrap().get_price(), 30.0);
}

#[test]
fn transpose_of_one_contract_is_its_bars_one_per_row() {
    let a = vec![
        bar_full("A", "NASDAQ", "USD", t(0), 1.0, 1.0, 1.0, 1.0),
        bar_full("A", "NASDAQ", "USD", t(1), 2.0, 2.0, 2.0, 2.0),
    ];
    let rows = transpose(vec![a]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_ref().unwrap().get_price(), 1.0);
    assert_eq!(rows[1][0].as_ref().unwrap().get_price(), 2.0);
}

#[test]
fn transpose_deduplicates_repeated_times_across_contracts() {
    let time = t(0);
    let a = vec![bar_full("A", "NASDAQ", "USD", time, 1.0, 1.0, 1.0, 1.0)];
    // Two bars from the same contract at the same time still occupy one row.
    let b = vec![
        bar_full("B", "NASDAQ", "USD", time, 2.0, 2.0, 2.0, 2.0),
        bar_full("B", "NASDAQ", "USD", time, 3.0, 3.0, 3.0, 3.0),
    ];
    let rows = transpose(vec![a, b]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_ref().unwrap().get_price(), 1.0);
    // The merge takes the first B bar at this time and advances its index.
    assert_eq!(rows[0][1].as_ref().unwrap().get_price(), 2.0);
}

#[test]
#[ignore = "known bug: transpose returns an empty matrix when the FIRST contract has zero bars, silently dropping every other contract's data (methods/mod.rs:165-167)"]
fn transpose_empty_first_contract_keeps_later_contracts() {
    let b = vec![
        bar_full("B", "NASDAQ", "USD", t(0), 10.0, 10.0, 10.0, 10.0),
        bar_full("B", "NASDAQ", "USD", t(1), 20.0, 20.0, 20.0, 20.0),
    ];
    let rows = transpose(vec![vec![], b]);
    // Intended: 2 rows with column 1 populated. Observed: vec![].
    assert_eq!(rows.len(), 2);
    assert!(rows[0][0].is_none());
    assert_eq!(rows[0][1].as_ref().unwrap().get_price(), 10.0);
}

#[test]
#[ignore = "known bug: transpose panics with an integer underflow (len()-1 on an empty vector) when a later contract has zero bars (methods/mod.rs:189)"]
fn transpose_empty_later_contract_does_not_panic() {
    let a = vec![bar_full("A", "NASDAQ", "USD", t(0), 1.0, 1.0, 1.0, 1.0)];
    let rows = transpose(vec![a, vec![]]);
    // Intended: one row, [Some(A0), None]. Observed: panic (debug overflow).
    assert_eq!(rows.len(), 1);
    assert!(rows[0][0].is_some());
    assert!(rows[0][1].is_none());
}

// ── BarCache ──────────────────────────────────────────────────────────────

fn cache_with_n_bars(n: usize) -> BarCache {
    let rows: Vec<Vec<Option<HistoricalDataFullKeys>>> = (0..n)
        .map(|i| {
            vec![Some(bar_full(
                "QQQ",
                "NASDAQ",
                "USD",
                t(i as i64),
                1.0,
                1.0,
                1.0,
                i as f64,
            ))]
        })
        .collect();
    BarCache::new(Arc::new(rows))
}

#[test]
fn bar_cache_none_returns_the_oldest_bars_up_to_the_limit() {
    let cache = cache_with_n_bars(5);
    let got = cache.get_bars_before(&qqq_pk(), None, 2).unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].get_time(), t(0));
    assert_eq!(got[1].get_time(), t(1));
    assert_eq!(cache.max_end_time(), Some(t(1)));
}

#[test]
fn bar_cache_some_time_returns_the_most_recent_bars_strictly_before_it() {
    let cache = cache_with_n_bars(5);
    // Bars before t(3): t0..t2; the LAST two are t1,t2 (oldest-first).
    let got = cache.get_bars_before(&qqq_pk(), Some(t(3)), 2).unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].get_time(), t(1));
    assert_eq!(got[1].get_time(), t(2));
    assert_eq!(cache.max_end_time(), Some(t(2)));
}

#[test]
fn bar_cache_limit_larger_than_available_returns_everything() {
    let cache = cache_with_n_bars(3);
    let got = cache.get_bars_before(&qqq_pk(), None, 99).unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].get_time(), t(0));
    assert_eq!(got[2].get_time(), t(2));
}

#[test]
fn bar_cache_miss_returns_none_and_does_not_advance_max_end_time() {
    let cache = cache_with_n_bars(2);
    assert!(cache.get_bars_before(&pk_for("AAPL"), None, 10).is_none());
    assert!(cache.get_bars_before(&qqq_pk(), Some(t(0)), 10).is_none());
    assert_eq!(cache.max_end_time(), None);
}

#[test]
fn bar_cache_max_end_time_only_moves_forward() {
    let cache = cache_with_n_bars(5);
    let _ = cache.get_bars_before(&qqq_pk(), Some(t(4)), 2).unwrap();
    assert_eq!(cache.max_end_time(), Some(t(3)));
    // An older window must not move the trim boundary backwards.
    let _ = cache.get_bars_before(&qqq_pk(), Some(t(2)), 5).unwrap();
    assert_eq!(cache.max_end_time(), Some(t(3)));
}

#[test]
fn bar_cache_picks_the_matching_contract_out_of_a_multi_contract_row() {
    let rows = vec![
        vec![
            Some(bar_full("QQQ", "NASDAQ", "USD", t(0), 1.0, 1.0, 1.0, 100.0)),
            Some(bar_full(
                "AAPL",
                "NASDAQ",
                "USD",
                t(0),
                1.0,
                1.0,
                1.0,
                200.0,
            )),
        ],
        vec![
            Some(bar_full("QQQ", "NASDAQ", "USD", t(1), 1.0, 1.0, 1.0, 101.0)),
            Some(bar_full(
                "AAPL",
                "NASDAQ",
                "USD",
                t(1),
                1.0,
                1.0,
                1.0,
                201.0,
            )),
        ],
    ];
    let cache = BarCache::new(Arc::new(rows));
    let qqq = cache.get_bars_before(&qqq_pk(), None, 10).unwrap();
    assert_eq!(qqq.len(), 2);
    assert_eq!(qqq[0].get_price(), 100.0);
    assert_eq!(qqq[1].get_price(), 101.0);

    let aapl = cache.get_bars_before(&pk_for("AAPL"), None, 10).unwrap();
    assert_eq!(aapl[0].get_price(), 200.0);
    assert_eq!(aapl[1].get_price(), 201.0);
}

#[test]
fn bar_cache_forex_pk_matches_by_pair_string() {
    use trading_app::database::models::HistoricalForexDataPrimaryKeysWoTime;
    let rows = vec![vec![Some(crate::support::fx_bar(t(0)))]];
    let cache = BarCache::new(Arc::new(rows));
    let pk = HistoricalDataPrimaryKeysWoTime::Forex(HistoricalForexDataPrimaryKeysWoTime {
        pair: "FX:EUR/USD".to_string(),
    });
    let got = cache.get_bars_before(&pk, None, 1).unwrap();
    assert_eq!(got.len(), 1);

    let wrong = HistoricalDataPrimaryKeysWoTime::Forex(HistoricalForexDataPrimaryKeysWoTime {
        pair: "FX:GBP/USD".to_string(),
    });
    assert!(cache.get_bars_before(&wrong, None, 1).is_none());
}

#[test]
fn bar_cache_thread_local_set_current_clear() {
    state::clear();
    let cache = Arc::new(cache_with_n_bars(1));
    assert!(trading_app::backtester::methods::in_memory::bar_cache::current().is_none());
    trading_app::backtester::methods::in_memory::bar_cache::set(cache.clone());
    let cur = trading_app::backtester::methods::in_memory::bar_cache::current().unwrap();
    assert!(Arc::ptr_eq(&cur, &cache));
    trading_app::backtester::methods::in_memory::bar_cache::clear();
    assert!(trading_app::backtester::methods::in_memory::bar_cache::current().is_none());
}

// ── InMemoryState ─────────────────────────────────────────────────────────

fn key(stock: &str, pe: &str, ccy: &str) -> PositionKey {
    PositionKey {
        strategy: "test".to_string(),
        stock: stock.to_string(),
        primary_exchange: pe.to_string(),
        currency: ccy.to_string(),
    }
}

fn cash_key() -> PositionKey {
    key("CASH:SGD", "", "SGD")
}

#[test]
fn state_seeds_cash_sgd_with_the_starting_capital_at_rate_one() {
    let s = InMemoryState::new("test".to_string(), 12_345.0);
    assert_eq!(s.strategy_name, "test");
    assert_eq!(s.current_qty(&cash_key()), 12_345.0);
    let snap = s.current_positions_snapshot();
    assert_eq!(snap.len(), 1);
    let (k, p) = &snap[0];
    assert_eq!(k, &cash_key());
    assert_eq!(p.avg_price, 1.0);
    assert_eq!(s.target_qty(&cash_key()), 0.0, "targets start empty");
    assert_eq!(s.current_qty(&key("QQQ", "NASDAQ", "USD")), 0.0);
    assert!(s.transactions.read().unwrap().is_empty());
    assert!(s.resting_orders.read().unwrap().is_empty());
}

#[test]
fn state_additive_updates_blend_buys_then_keep_avg_on_partial_sells() {
    let s = InMemoryState::new("test".to_string(), 100_000.0);
    let qqq = key("QQQ", "NASDAQ", "USD");

    s.update_current_additive(qqq.clone(), 10.0, 100.0);
    assert_eq!(s.current_qty(&qqq), 10.0);

    s.update_current_additive(qqq.clone(), 10.0, 120.0);
    assert_eq!(s.current_qty(&qqq), 20.0);
    let avg = s.current_positions.read().unwrap()[&qqq].avg_price;
    assert_eq!(avg, 110.0);

    // Partial sell: quantity drops, average cost is preserved.
    s.update_current_additive(qqq.clone(), -5.0, 130.0);
    assert_eq!(s.current_qty(&qqq), 15.0);
    assert_eq!(s.current_positions.read().unwrap()[&qqq].avg_price, 110.0);
}

#[test]
fn state_zeroing_a_position_resets_quantity_and_average() {
    let s = InMemoryState::new("test".to_string(), 10_000.0);
    let qqq = key("QQQ", "NASDAQ", "USD");
    s.update_current_additive(qqq.clone(), 10.0, 100.0);
    s.update_current_additive(qqq.clone(), -10.0, 120.0);
    assert_eq!(s.current_qty(&qqq), 0.0);
    let p = &s.current_positions.read().unwrap()[&qqq];
    assert_eq!(p.avg_price, 0.0);
    assert_eq!(p.quantity, 0.0);
}

#[test]
fn state_flip_through_zero_keeps_the_old_average_cost() {
    // Documented prod-mirroring behaviour: a cross-direction update only moves
    // the quantity and keeps the existing average.
    let s = InMemoryState::new("test".to_string(), 10_000.0);
    let qqq = key("QQQ", "NASDAQ", "USD");
    s.update_current_additive(qqq.clone(), 10.0, 100.0);
    s.update_current_additive(qqq.clone(), -15.0, 120.0);
    assert_eq!(s.current_qty(&qqq), -5.0);
    assert_eq!(s.current_positions.read().unwrap()[&qqq].avg_price, 100.0);
}

#[test]
fn state_targets_set_replace_and_delete() {
    let s = InMemoryState::new("test".to_string(), 1_000.0);
    let qqq = key("QQQ", "NASDAQ", "USD");
    assert_eq!(s.target_qty(&qqq), 0.0);
    s.set_target(qqq.clone(), 5.0, 99.0);
    assert_eq!(s.target_qty(&qqq), 5.0);
    s.set_target(qqq.clone(), -2.0, 101.0);
    assert_eq!(s.target_qty(&qqq), -2.0);
    s.delete_target(&qqq);
    assert_eq!(s.target_qty(&qqq), 0.0);
}

#[test]
fn state_snapshot_includes_cash_plus_every_position() {
    let s = InMemoryState::new("test".to_string(), 1_000.0);
    s.update_current_additive(key("QQQ", "NASDAQ", "USD"), 2.0, 400.0);
    let snap = s.current_positions_snapshot();
    assert_eq!(snap.len(), 2);
    let qqq = snap.iter().find(|(k, _)| k.stock == "QQQ").unwrap();
    assert_eq!(qqq.1.quantity, 2.0);
    assert_eq!(qqq.1.avg_price, 400.0);
}

#[test]
fn fill_router_resolves_known_owners_and_clears() {
    state::clear_fill_router();
    let a = Arc::new(InMemoryState::new("a".to_string(), 1.0));
    let b = Arc::new(InMemoryState::new("b".to_string(), 1.0));
    state::set_fill_router(HashMap::from([
        ("a".to_string(), a.clone()),
        ("b".to_string(), b.clone()),
    ]));

    assert!(Arc::ptr_eq(&state::routed_state("a").unwrap(), &a));
    assert!(Arc::ptr_eq(&state::routed_state("b").unwrap(), &b));
    assert!(state::routed_state("nobody").is_none());

    state::clear_fill_router();
    assert!(state::routed_state("a").is_none());
}

#[test]
fn mailbox_pushes_per_owner_and_drains_exactly_once() {
    let date = chrono::NaiveDate::from_ymd_opt(2024, 6, 14).unwrap();
    let _ = state::mailbox_drain("owner");
    let _ = state::mailbox_drain("other");

    state::mailbox_push(
        "owner",
        MailFill {
            sym: "QQQ".to_string(),
            qty: 3,
            px: 100.0,
            date,
            order_id: 7,
            oca_group: "".to_string(),
        },
    );
    state::mailbox_push(
        "owner",
        MailFill {
            sym: "AAPL".to_string(),
            qty: -2,
            px: 200.0,
            date,
            order_id: 8,
            oca_group: "oca".to_string(),
        },
    );
    state::mailbox_push(
        "other",
        MailFill {
            sym: "MSFT".to_string(),
            qty: 1,
            px: 400.0,
            date,
            order_id: 9,
            oca_group: "".to_string(),
        },
    );

    let got = state::mailbox_drain("owner");
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].sym, "QQQ");
    assert_eq!(got[0].qty, 3);
    assert_eq!(got[0].px, 100.0);
    assert_eq!(got[0].order_id, 7);
    assert_eq!(got[1].qty, -2);
    assert_eq!(got[1].oca_group, "oca");

    assert!(state::mailbox_drain("owner").is_empty(), "drained once");
    assert_eq!(
        state::mailbox_drain("other").len(),
        1,
        "other owner untouched"
    );
    assert!(state::mailbox_drain("other").is_empty());
}

// ── Bracket parsing ───────────────────────────────────────────────────────

fn order_ibkr(
    contract: &ibapi::contracts::Contract,
    action: Action,
    qty: f64,
    order_type: &str,
    limit: Option<f64>,
    aux: Option<f64>,
    refs_parent: i32,
    oca: &str,
    good_after: &str,
) -> OrderIBKR {
    OrderIBKR {
        contract: contract.clone(),
        order: Order {
            action,
            total_quantity: qty,
            order_type: order_type.to_string(),
            limit_price: limit,
            aux_price: aux,
            order_ref: "strat".to_string(),
            oca_group: oca.to_string(),
            good_after_time: good_after.to_string(),
            ..Default::default()
        },
        references_parent_order: refs_parent,
    }
}

#[test]
fn parse_brackets_groups_parents_with_their_following_children() {
    let c = stock("QQQ");
    let orders = vec![
        order_ibkr(&c, Action::Buy, 1.0, "LMT", Some(100.0), None, -1, "", ""),
        order_ibkr(&c, Action::Sell, 1.0, "STP", None, Some(90.0), 0, "oca", ""),
        order_ibkr(
            &c,
            Action::Sell,
            1.0,
            "LMT",
            Some(110.0),
            None,
            0,
            "oca",
            "",
        ),
    ];
    assert_eq!(parse_brackets(&orders), vec![(0, vec![1, 2])]);
}

#[test]
fn parse_brackets_supports_multiple_brackets_in_one_batch() {
    let c = stock("QQQ");
    let orders = vec![
        order_ibkr(&c, Action::Buy, 1.0, "LMT", Some(100.0), None, -1, "", ""),
        order_ibkr(
            &c,
            Action::Sell,
            1.0,
            "STP",
            None,
            Some(90.0),
            0,
            "oca1",
            "",
        ),
        order_ibkr(&c, Action::Buy, 2.0, "LMT", Some(50.0), None, -1, "", ""),
        order_ibkr(
            &c,
            Action::Sell,
            2.0,
            "STP",
            None,
            Some(45.0),
            2,
            "oca2",
            "",
        ),
    ];
    assert_eq!(parse_brackets(&orders), vec![(0, vec![1]), (2, vec![3])]);
}

#[test]
fn parse_brackets_without_a_parent_yields_no_brackets() {
    let c = stock("QQQ");
    let orders = vec![order_ibkr(
        &c,
        Action::Sell,
        1.0,
        "STP",
        None,
        Some(90.0),
        0,
        "oca",
        "",
    )];
    assert_eq!(parse_brackets(&orders), Vec::<(usize, Vec<usize>)>::new());
}

#[test]
fn build_resting_orders_maps_entry_and_exit_types_to_fill_models() {
    let c = stock("QQQ");
    let orders = vec![
        order_ibkr(&c, Action::Buy, 10.0, "LMT", Some(90.0), None, -1, "", ""),
        order_ibkr(
            &c,
            Action::Sell,
            10.0,
            "STP",
            None,
            Some(80.0),
            0,
            "oca",
            "",
        ),
        order_ibkr(
            &c,
            Action::Sell,
            10.0,
            "LMT",
            Some(120.0),
            None,
            0,
            "oca",
            "",
        ),
        order_ibkr(
            &c,
            Action::Sell,
            10.0,
            "MIDCLOSE",
            None,
            None,
            0,
            "oca",
            "20240115-15:45:00",
        ),
    ];
    let resting = build_resting_orders(&orders, 5.0, 0.5);
    assert_eq!(resting.len(), 4);

    let entry = &resting[0];
    assert_eq!(entry.role, OrderRole::Entry);
    assert!(entry.active, "entries are live immediately");
    assert_eq!(entry.qty, 10.0);
    assert_eq!(entry.order_ref, "strat");
    assert_eq!(entry.status, RestingStatus::Resting);
    let mut m = entry.fill_model.as_ref().unwrap().clone();
    assert!(!m.is_immediate(), "a limit entry rests");

    let stop = &resting[1];
    assert_eq!(stop.role, OrderRole::Exit(CloseReason::Stop));
    assert!(!stop.active, "exits arm only after the entry fills");
    let mut m = stop.fill_model.as_ref().unwrap().clone();
    assert!(matches!(
        m.evaluate(&bar(t(0), 100.0, 85.0, 79.0, 80.0)),
        trading_app::backtester::execution::fill_model::FillEval::Fill { .. }
    ));

    let tp = &resting[2];
    assert_eq!(tp.role, OrderRole::Exit(CloseReason::TakeProfit));
    assert!(!tp.active);
    let mut m = tp.fill_model.as_ref().unwrap().clone();
    let ev = m.evaluate(&bar(t(0), 118.0, 125.0, 115.0, 124.0));
    match ev {
        trading_app::backtester::execution::fill_model::FillEval::Fill { price } => {
            assert_eq!(price, 120.0, "limit family fills exactly at the limit")
        }
        other => panic!("expected TP fill, got {other:?}"),
    }

    let close = &resting[3];
    assert_eq!(close.role, OrderRole::Exit(CloseReason::MidpointClose));
    assert!(!close.active);
    assert!(
        close.fill_model.is_none(),
        "time-gated close has no price model"
    );
    assert_eq!(close.close_mso, Some(375)); // 15:45 ET = 375 minutes after 09:30
    assert!(close.close_is_mid, "MIDCLOSE uses the bar midpoint");

    // Entry + every exit share one bracket id.
    let id = entry.bracket_id;
    assert!(resting[1..].iter().all(|r| r.bracket_id == id));
}

#[test]
fn build_resting_orders_uses_distinct_ids_for_distinct_brackets() {
    let c = stock("QQQ");
    let orders = vec![
        order_ibkr(&c, Action::Buy, 1.0, "LMT", Some(100.0), None, -1, "", ""),
        order_ibkr(
            &c,
            Action::Sell,
            1.0,
            "LMT",
            Some(110.0),
            None,
            0,
            "oca1",
            "",
        ),
        order_ibkr(&c, Action::Buy, 2.0, "LMT", Some(50.0), None, -1, "", ""),
        order_ibkr(
            &c,
            Action::Sell,
            2.0,
            "LMT",
            Some(60.0),
            None,
            2,
            "oca2",
            "",
        ),
    ];
    let resting = build_resting_orders(&orders, 0.0, 0.5);
    assert_eq!(resting.len(), 4);
    assert_ne!(resting[0].bracket_id, resting[2].bracket_id);
    assert_eq!(resting[0].bracket_id, resting[1].bracket_id);
    assert_eq!(resting[2].bracket_id, resting[3].bracket_id);
}

#[test]
fn build_resting_orders_falls_back_to_market_for_lmt_without_a_limit() {
    let c = stock("QQQ");
    let orders = vec![order_ibkr(
        &c,
        Action::Buy,
        5.0,
        "LMT",
        None,
        None,
        -1,
        "",
        "",
    )];
    let resting = build_resting_orders(&orders, 10.0, 0.5);
    assert_eq!(resting.len(), 1);
    assert!(resting[0].fill_model.as_ref().unwrap().is_immediate());
}

#[test]
fn build_resting_orders_drops_unknown_exit_order_types() {
    let c = stock("QQQ");
    let orders = vec![
        order_ibkr(&c, Action::Buy, 1.0, "LMT", Some(100.0), None, -1, "", ""),
        order_ibkr(&c, Action::Sell, 1.0, "TRAIL", None, None, 0, "oca", ""),
    ];
    let resting = build_resting_orders(&orders, 0.0, 0.5);
    assert_eq!(resting.len(), 1, "entry only");
}

#[test]
fn build_resting_orders_marks_midprice_close_variant() {
    let c = stock("QQQ");
    let orders = vec![
        order_ibkr(&c, Action::Buy, 1.0, "LMT", Some(100.0), None, -1, "", ""),
        order_ibkr(
            &c,
            Action::Sell,
            1.0,
            "MIDPRICE",
            None,
            None,
            0,
            "oca",
            "20240115-15:30:00",
        ),
    ];
    let resting = build_resting_orders(&orders, 0.0, 0.5);
    let close = &resting[1];
    assert_eq!(close.close_mso, Some(360)); // 15:30 = 360 min after 09:30
    assert!(
        !close.close_is_mid,
        "MIDPRICE is not the midpoint-close variant"
    );
}

// ── In-memory reconcile: target deltas ────────────────────────────────────

fn cfg() -> BacktestConfig {
    BacktestConfig::new(10_000.0)
        .stock("QQQ", "NASDAQ", "USD")
        .slippage_bps(0.0)
}

fn slot(bar: &HistoricalDataFullKeys) -> Vec<Option<HistoricalDataFullKeys>> {
    vec![Some(bar.clone())]
}

fn trade_key() -> PositionKey {
    key("QQQ", "NASDAQ", "USD")
}

#[test]
fn no_action_leaves_state_untouched() {
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    let b = bar(t(0), 100.0, 100.0, 100.0, 100.0);
    let mut oid = 0;
    handle_bar_update_outcome_in_memory(
        &cfg(),
        None,
        &prices,
        &state,
        &BarUpdateOutcome::NoAction,
        &contract,
        &b,
        &slot(&b),
        &mut oid,
    )
    .unwrap();
    assert_eq!(state.current_qty(&trade_key()), 0.0);
    assert_eq!(state.current_qty(&cash_key()), 10_000.0);
    assert!(state.transactions.read().unwrap().is_empty());
    assert_eq!(oid, 0);
}

#[test]
fn pending_db_query_fills_the_target_delta_at_the_close_plus_fees() {
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    state.set_target(trade_key(), 10.0, 0.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    let b = bar(t(0), 100.0, 101.0, 99.0, 100.0);
    let mut oid = 0;

    handle_bar_update_outcome_in_memory(
        &cfg(),
        None,
        &prices,
        &state,
        &BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]),
        &contract,
        &b,
        &slot(&b),
        &mut oid,
    )
    .unwrap();

    assert_eq!(state.current_qty(&trade_key()), 10.0);
    assert_eq!(
        state.current_positions.read().unwrap()[&trade_key()].avg_price,
        100.0
    );
    let fees = commission(10.0, 100.0, CommissionModel::Tiered);
    let expected_cash = 10_000.0 - (10.0 * 100.0 + fees) * 1.35;
    assert_close(state.current_qty(&cash_key()), expected_cash, 1e-9);

    let txns = state.transactions.read().unwrap();
    assert_eq!(txns.len(), 1);
    assert_eq!(txns[0].stock, "QQQ");
    assert_eq!(txns[0].quantity, 10.0);
    assert_eq!(txns[0].price, 100.0);
    assert_close(txns[0].fees, fees, 1e-12);
    assert_eq!(txns[0].action, "Buy");
    assert_eq!(txns[0].time, t(0));
    assert_eq!(oid, 1);
}

#[test]
fn pending_db_query_with_a_met_target_places_no_order() {
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    state.update_current_additive(trade_key(), 10.0, 100.0);
    state.set_target(trade_key(), 10.0, 0.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    let b = bar(t(0), 100.0, 100.0, 100.0, 100.0);
    let mut oid = 0;
    handle_bar_update_outcome_in_memory(
        &cfg(),
        None,
        &prices,
        &state,
        &BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]),
        &contract,
        &b,
        &slot(&b),
        &mut oid,
    )
    .unwrap();
    assert!(state.transactions.read().unwrap().is_empty());
    assert_eq!(oid, 0);
}

#[test]
fn pending_db_query_sells_a_negative_target_and_credits_cash() {
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    state.set_target(trade_key(), -5.0, 0.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    let b = bar(t(0), 100.0, 100.0, 100.0, 100.0);
    let mut oid = 0;
    handle_bar_update_outcome_in_memory(
        &cfg(),
        None,
        &prices,
        &state,
        &BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]),
        &contract,
        &b,
        &slot(&b),
        &mut oid,
    )
    .unwrap();

    assert_eq!(state.current_qty(&trade_key()), -5.0);
    let fees = commission(-5.0, 100.0, CommissionModel::Tiered);
    let expected_cash = 10_000.0 - (-5.0 * 100.0 + fees) * 1.35;
    assert_close(state.current_qty(&cash_key()), expected_cash, 1e-9);
    let txns = state.transactions.read().unwrap();
    assert_eq!(txns[0].quantity, -5.0);
    assert_eq!(txns[0].action, "Sell");
}

#[test]
fn pending_db_query_with_a_missing_own_bar_defers_the_fill() {
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    state.set_target(trade_key(), 10.0, 0.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    let b = bar(t(0), 100.0, 100.0, 100.0, 100.0);
    let no_bar: Vec<Option<HistoricalDataFullKeys>> = vec![None];
    let mut oid = 0;

    handle_bar_update_outcome_in_memory(
        &cfg(),
        None,
        &prices,
        &state,
        &BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]),
        &contract,
        &b,
        &no_bar,
        &mut oid,
    )
    .unwrap();

    assert_eq!(state.current_qty(&trade_key()), 0.0);
    assert!(state.transactions.read().unwrap().is_empty());
    let resting = state.resting_orders.read().unwrap();
    assert_eq!(resting.len(), 1, "one deferred market entry");
    assert_eq!(resting[0].role, OrderRole::Entry);
    assert!(resting[0].active);
    assert_eq!(resting[0].qty, 10.0);
    assert!(resting[0].fill_model.as_ref().unwrap().is_immediate());
    assert_eq!(oid, 0);
}

#[test]
fn pending_db_query_ignores_a_non_stock_asset_request() {
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    state.set_target(trade_key(), 10.0, 0.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    let b = bar(t(0), 100.0, 100.0, 100.0, 100.0);
    let mut oid = 0;
    handle_bar_update_outcome_in_memory(
        &cfg(),
        None,
        &prices,
        &state,
        &BarUpdateOutcome::PendingDbQuery(vec![AssetType::Option]),
        &contract,
        &b,
        &slot(&b),
        &mut oid,
    )
    .unwrap();
    assert!(state.transactions.read().unwrap().is_empty());
    assert_eq!(state.current_qty(&trade_key()), 0.0);
}

// ── In-memory reconcile: bracket lifecycle ───────────────────────────────

fn bracket_orders(contract: &ibapi::contracts::Contract) -> Vec<OrderIBKR> {
    vec![
        order_ibkr(
            contract,
            Action::Buy,
            10.0,
            "LMT",
            Some(90.0),
            None,
            -1,
            "",
            "",
        ),
        order_ibkr(
            contract,
            Action::Sell,
            10.0,
            "STP",
            None,
            Some(80.0),
            0,
            "oca",
            "",
        ),
        order_ibkr(
            contract,
            Action::Sell,
            10.0,
            "LMT",
            Some(120.0),
            None,
            0,
            "oca",
            "",
        ),
    ]
}

fn emit(orders: Vec<OrderIBKR>) -> BarUpdateOutcome {
    BarUpdateOutcome::EmitOrders {
        place_orders: orders,
        cancel_orders: vec![],
    }
}

#[test]
fn limit_entry_rests_then_fills_and_arms_the_oca_exits() {
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    let mut oid = 0;

    // Bar 1: limit 90 not crossed (low 95) -> entry + 2 exits rest.
    let b1 = bar(t(0), 100.0, 105.0, 95.0, 100.0);
    handle_bar_update_outcome_in_memory(
        &cfg(),
        None,
        &prices,
        &state,
        &emit(bracket_orders(&contract)),
        &contract,
        &b1,
        &slot(&b1),
        &mut oid,
    )
    .unwrap();
    {
        let resting = state.resting_orders.read().unwrap();
        assert_eq!(resting.len(), 3);
        assert!(
            resting
                .iter()
                .any(|r| r.role == OrderRole::Entry && r.active)
        );
        assert!(
            resting
                .iter()
                .filter(|r| matches!(r.role, OrderRole::Exit(_)))
                .all(|r| !r.active)
        );
    }
    assert!(state.transactions.read().unwrap().is_empty());

    // Bar 2: low 85 crosses the limit -> entry fills at 90, exits arm.
    let b2 = bar(t(1), 92.0, 96.0, 85.0, 88.0);
    handle_bar_update_outcome_in_memory(
        &cfg(),
        None,
        &prices,
        &state,
        &BarUpdateOutcome::NoAction,
        &contract,
        &b2,
        &slot(&b2),
        &mut oid,
    )
    .unwrap();
    assert_eq!(state.current_qty(&trade_key()), 10.0);
    assert_eq!(
        state.current_positions.read().unwrap()[&trade_key()].avg_price,
        90.0
    );
    {
        let resting = state.resting_orders.read().unwrap();
        assert_eq!(resting.len(), 2, "entry consumed, both exits remain");
        assert!(
            resting.iter().all(|r| r.active),
            "exits armed after entry fill"
        );
    }

    // Bar 3: low 79 breaches the stop -> stop fills at 77, TP sibling cancelled.
    let b3 = bar(t(2), 82.0, 85.0, 79.0, 80.0);
    handle_bar_update_outcome_in_memory(
        &cfg(),
        None,
        &prices,
        &state,
        &BarUpdateOutcome::NoAction,
        &contract,
        &b3,
        &slot(&b3),
        &mut oid,
    )
    .unwrap();

    assert_eq!(state.current_qty(&trade_key()), 0.0);
    assert!(state.resting_orders.read().unwrap().is_empty());
    let txns = state.transactions.read().unwrap();
    assert_eq!(txns.len(), 2);
    assert_eq!(txns[0].quantity, 10.0);
    assert_eq!(txns[0].price, 90.0);
    assert_eq!(txns[1].quantity, -10.0);
    assert_eq!(txns[1].price, 77.0, "stop market: 80 - 0.5*(85-79)");
    assert_eq!(oid, 2);

    let buy_fees = commission(10.0, 90.0, CommissionModel::Tiered);
    let sell_fees = commission(-10.0, 77.0, CommissionModel::Tiered);
    let expected_cash =
        10_000.0 - (10.0 * 90.0 + buy_fees) * 1.35 + (10.0 * 77.0 - sell_fees) * 1.35;
    assert_close(state.current_qty(&cash_key()), expected_cash, 1e-9);
}

#[test]
fn cancel_orders_sweep_same_symbol_side_and_ref() {
    use trading_app::database::models::OpenStockOrdersFullKeys;
    use trading_app::database::models_crud::open_orders::open_orders::OpenOrdersFullKeys;

    let state = InMemoryState::new("test".to_string(), 10_000.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    let mut oid = 0;

    // Place a resting buy limit via an emission.
    let b1 = bar(t(0), 100.0, 105.0, 95.0, 100.0);
    handle_bar_update_outcome_in_memory(
        &cfg(),
        None,
        &prices,
        &state,
        &emit(vec![order_ibkr(
            &contract,
            Action::Buy,
            1.0,
            "LMT",
            Some(90.0),
            None,
            -1,
            "",
            "",
        )]),
        &contract,
        &b1,
        &slot(&b1),
        &mut oid,
    )
    .unwrap();
    assert_eq!(state.resting_orders.read().unwrap().len(), 1);

    // Cancel key: same symbol/ref and buy side (quantity > 0).
    let cancel = OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
        order_perm_id: -1,
        order_id: 1,
        strategy: "strat".to_string(),
        stock: "QQQ".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
        time: t(0),
        quantity: 1.0,
        parent_order_id: 0,
        executions: vec![],
        filled: 0.0,
    });
    let outcome = BarUpdateOutcome::EmitOrders {
        place_orders: vec![],
        cancel_orders: vec![cancel],
    };
    handle_bar_update_outcome_in_memory(
        &cfg(),
        None,
        &prices,
        &state,
        &outcome,
        &contract,
        &b1,
        &slot(&b1),
        &mut oid,
    )
    .unwrap();
    assert!(
        state.resting_orders.read().unwrap().is_empty(),
        "resting entry cancelled"
    );
}

// ── In-memory reconcile: pooled-fill routing ─────────────────────────────

#[test]
fn pooled_order_fills_settle_into_the_owning_account_and_mailbox() {
    state::clear_fill_router();
    let _ = state::mailbox_drain("owner");
    let emitter = InMemoryState::new("emitter".to_string(), 10_000.0);
    let owner = Arc::new(InMemoryState::new("owner".to_string(), 5_000.0));
    state::set_fill_router(HashMap::from([("owner".to_string(), owner.clone())]));

    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    let b = bar(t(0), 100.0, 101.0, 99.0, 100.0);
    let mut oid = 0;

    // Market entry emitted by `emitter`, owned by `owner`.
    let mut o = order_ibkr(&contract, Action::Buy, 3.0, "LMT", None, None, -1, "", "");
    o.order.order_ref = "owner".to_string();
    handle_bar_update_outcome_in_memory(
        &cfg(),
        None,
        &prices,
        &emitter,
        &emit(vec![o]),
        &contract,
        &b,
        &slot(&b),
        &mut oid,
    )
    .unwrap();

    // Owning account got the position + cash movement.
    let owner_key = PositionKey {
        strategy: "owner".to_string(),
        stock: "QQQ".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    };
    assert_eq!(owner.current_qty(&owner_key), 3.0);
    let fees = commission(3.0, 100.0, CommissionModel::Tiered);
    let expected_owner_cash = 5_000.0 - (3.0 * 100.0 + fees) * 1.35;
    let owner_cash = PositionKey {
        strategy: "owner".to_string(),
        stock: "CASH:SGD".to_string(),
        primary_exchange: "".to_string(),
        currency: "SGD".to_string(),
    };
    assert_close(owner.current_qty(&owner_cash), expected_owner_cash, 1e-9);

    // Emitter stayed flat and paid nothing (keys must carry its own strategy).
    let emitter_key = PositionKey {
        strategy: "emitter".to_string(),
        stock: "QQQ".to_string(),
        primary_exchange: "NASDAQ".to_string(),
        currency: "USD".to_string(),
    };
    let emitter_cash = PositionKey {
        strategy: "emitter".to_string(),
        stock: "CASH:SGD".to_string(),
        primary_exchange: "".to_string(),
        currency: "SGD".to_string(),
    };
    assert_eq!(emitter.current_qty(&emitter_key), 0.0);
    assert_eq!(emitter.current_qty(&emitter_cash), 10_000.0);
    assert!(emitter.transactions.read().unwrap().is_empty());

    // Realized fill was mailboxed for the owning sleeve.
    let mail = state::mailbox_drain("owner");
    assert_eq!(mail.len(), 1);
    assert_eq!(mail[0].sym, "QQQ");
    assert_eq!(mail[0].qty, 3);
    assert_eq!(mail[0].px, 100.0);
    assert_eq!(mail[0].order_id, 1);
    assert_eq!(
        mail[0].date,
        chrono::NaiveDate::from_ymd_opt(2024, 6, 14).unwrap()
    );
    assert_eq!(oid, 1);

    state::clear_fill_router();
}

// ── drain_resting_orders_fill_first ──────────────────────────────────────

fn market_rest(
    name: &str,
    contract: &ibapi::contracts::Contract,
    action: Action,
    qty: f64,
) -> RestingOrder {
    RestingOrder {
        contract: contract.clone(),
        action,
        qty,
        order_ref: name.to_string(),
        oca_group: String::new(),
        bracket_id: 1,
        role: OrderRole::Entry,
        active: true,
        fill_model: Some(Box::new(MarketFillModel {
            slippage_bps: 0.0,
            is_buy: matches!(action, Action::Buy),
        })),
        close_mso: None,
        close_is_mid: false,
        status: RestingStatus::Resting,
    }
}

#[test]
fn drain_fills_a_market_entry_at_its_own_bar() {
    state::clear_fill_router();
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    state
        .resting_orders
        .write()
        .unwrap()
        .push(market_rest("test", &contract, Action::Buy, 2.0));

    let b = bar(t(0), 100.0, 101.0, 99.0, 100.0);
    let mut oid = 0;
    drain_resting_orders_fill_first(
        &cfg(),
        None,
        &prices,
        &state,
        &slot(&b),
        std::slice::from_ref(&contract),
        &mut oid,
    )
    .unwrap();

    assert_eq!(state.current_qty(&trade_key()), 2.0);
    assert!(state.resting_orders.read().unwrap().is_empty());
    assert_eq!(oid, 1);
    let txns = state.transactions.read().unwrap();
    assert_eq!(txns.len(), 1);
    assert_eq!(txns[0].price, 100.0);
}

#[test]
fn drain_leaves_orders_without_an_own_bar_resting() {
    state::clear_fill_router();
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    state
        .resting_orders
        .write()
        .unwrap()
        .push(market_rest("test", &contract, Action::Buy, 2.0));

    let mut oid = 0;
    drain_resting_orders_fill_first(
        &cfg(),
        None,
        &prices,
        &state,
        &[None],
        std::slice::from_ref(&contract),
        &mut oid,
    )
    .unwrap();
    assert_eq!(state.resting_orders.read().unwrap().len(), 1);
    assert!(state.transactions.read().unwrap().is_empty());
}

#[test]
fn drain_keeps_an_uncrossed_limit_and_fills_it_on_a_later_slot() {
    state::clear_fill_router();
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    {
        let mut r = market_rest("test", &contract, Action::Buy, 5.0);
        r.fill_model = Some(Box::new(LimitFillModel {
            limit: 90.0,
            is_buy: true,
        }));
        state.resting_orders.write().unwrap().push(r);
    }

    let b1 = bar(t(0), 100.0, 105.0, 95.0, 100.0);
    let mut oid = 0;
    drain_resting_orders_fill_first(
        &cfg(),
        None,
        &prices,
        &state,
        &slot(&b1),
        std::slice::from_ref(&contract),
        &mut oid,
    )
    .unwrap();
    assert_eq!(state.resting_orders.read().unwrap().len(), 1);
    assert_eq!(oid, 0);

    let b2 = bar(t(1), 92.0, 96.0, 85.0, 88.0);
    drain_resting_orders_fill_first(
        &cfg(),
        None,
        &prices,
        &state,
        &slot(&b2),
        std::slice::from_ref(&contract),
        &mut oid,
    )
    .unwrap();
    assert!(state.resting_orders.read().unwrap().is_empty());
    assert_eq!(state.current_qty(&trade_key()), 5.0);
    assert_eq!(state.transactions.read().unwrap()[0].price, 90.0);
}

#[test]
fn drain_time_gated_close_fires_only_after_its_mso() {
    state::clear_fill_router();
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    {
        let mut r = market_rest("test", &contract, Action::Sell, 4.0);
        r.role = OrderRole::Exit(CloseReason::MidpointClose);
        r.fill_model = None;
        r.close_mso = Some(375); // 15:45 ET
        r.close_is_mid = true;
        state.resting_orders.write().unwrap().push(r);
    }

    // 15:44 ET = 19:44Z (EDT) -> 374 mso, not yet.
    let before = bar(utc(2024, 6, 14, 19, 44, 0), 100.0, 102.0, 98.0, 101.0);
    let mut oid = 0;
    drain_resting_orders_fill_first(
        &cfg(),
        None,
        &prices,
        &state,
        &slot(&before),
        std::slice::from_ref(&contract),
        &mut oid,
    )
    .unwrap();
    assert_eq!(state.resting_orders.read().unwrap().len(), 1);
    assert!(state.transactions.read().unwrap().is_empty());

    // 15:45 ET exactly -> fills at (high+low)/2.
    let at = bar(utc(2024, 6, 14, 19, 45, 0), 100.0, 102.0, 98.0, 101.0);
    drain_resting_orders_fill_first(
        &cfg(),
        None,
        &prices,
        &state,
        &slot(&at),
        std::slice::from_ref(&contract),
        &mut oid,
    )
    .unwrap();
    assert!(state.resting_orders.read().unwrap().is_empty());
    let txns = state.transactions.read().unwrap();
    assert_eq!(txns.len(), 1);
    assert_eq!(txns[0].quantity, -4.0);
    assert_eq!(txns[0].price, 100.0); // (102 + 98) / 2
}

#[test]
fn drain_with_no_orders_is_a_noop() {
    state::clear_fill_router();
    let state = InMemoryState::new("test".to_string(), 10_000.0);
    let prices = FakePrices::usd_sgd(1.35);
    let contract = stock("QQQ");
    let b = bar(t(0), 100.0, 100.0, 100.0, 100.0);
    let mut oid = 0;
    drain_resting_orders_fill_first(
        &cfg(),
        None,
        &prices,
        &state,
        &slot(&b),
        std::slice::from_ref(&contract),
        &mut oid,
    )
    .unwrap();
    assert_eq!(oid, 0);
    assert!(state.transactions.read().unwrap().is_empty());
}
