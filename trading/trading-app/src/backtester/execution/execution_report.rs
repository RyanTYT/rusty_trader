//! Synthesized execution reports for backtest fills.
//!
//! The Db lane writes its own transaction/position rows, but post-commit
//! consumers (the fund hooks, order store release, scenario dumps) still expect
//! an IBKR-shaped `ExecutionData`. This module fabricates one that is
//! indistinguishable from a live fill to those consumers.

use chrono::DateTime;
use chrono::Utc;
use chrono_tz::America::New_York;
use ibapi::orders::{Execution, ExecutionData, Liquidity};
use ibapi::prelude::{Action, Contract};

/// Build the `ExecutionData` for one backtest fill, mirroring prod's
/// consumer-facing conventions exactly:
/// - `execution.side` = "BOT" | "SLD" (IB never emits "SELL"/"BUY" — matches
///   `fund.rs::realised_fill_from`)
/// - `execution.shares` = abs per-fill quantity; the side carries the sign
/// - `execution.time` = US/Eastern wall time "yyyymmdd hh:mm:ss" (matches the
///   only parse format any prod consumer knows)
/// - `execution.execution_id` = "bt-{order_id}" — joins the transaction rows
///   the engines already write, and is unique/dedup-safe like prod's ids
/// - `execution.order_reference` = strategy key (clean `{strategy}`, per the
///   prod convention that all submitted orders carry clean refs)
/// - `cumulative_quantity`/`average_price` = this fill (backtest fills whole
///   orders in one shot, so per-order cumulative == per-fill quantity)
///
/// `signed_qty` carries the direction (+buy / −sell) and is converted to
/// `shares = abs`; `bar_time` is the bar at which the fill was decided and is
/// rendered in New York time; `strategy` becomes `order_reference` verbatim.
/// Broker metadata (`perm_id`, `client_id`, `submitter`, …) is deterministic
/// zero/empty, and `request_id` keeps its default because a backtest has no
/// historical stream.
pub fn synthetic_execution(
    order_id: i32,
    contract: &Contract,
    action: &Action,
    signed_qty: f64, // +buy / -sell, as decided by decide_fill
    fill_price: f64,
    bar_time: DateTime<Utc>,
    strategy: &str,
) -> ExecutionData {
    // IB's execution side is strictly BOT/SLD regardless of the order action:
    // SellShort/SellLong executions also come back as "SLD".
    let side = match action {
        Action::Buy => "BOT",
        Action::Sell | Action::SellShort | Action::SellLong => "SLD",
    };

    ExecutionData {
        contract: contract.clone(),
        execution: Execution {
            order_id,
            execution_id: format!("bt-{order_id}"),
            time: bar_time
                .with_timezone(&New_York)
                .format("%Y%m%d %H:%M:%S")
                .to_string(),
            exchange: contract.exchange.as_str().to_string(),
            side: side.to_string(),
            shares: signed_qty.abs(),
            price: fill_price,
            order_reference: strategy.to_string(),
            cumulative_quantity: signed_qty.abs(),
            average_price: fill_price,
            account_number: "backtest".to_string(),
            // Deterministically-zero remaining metadata; real producers send
            // these too for external/other-origin trades.
            client_id: 0,
            perm_id: 0,
            liquidation: 0,
            last_liquidity: Liquidity::None,
            ev_rule: String::new(),
            ev_multiplier: None,
            model_code: String::new(),
            pending_price_revision: false,
            submitter: String::new(),
        },
        // request_id: no historical stream in backtest.
        ..Default::default()
    }
}
