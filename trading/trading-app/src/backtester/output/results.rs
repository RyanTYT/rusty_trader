//! Backtest results & metrics, computed from the transactions + the in-memory
//! equity curve, serialized to JSON.
//!
//! v1 metrics: total PnL / return, max drawdown, per-bar Sharpe & Sortino
//! (not annualized — annualization depends on bar frequency, which varies),
//! trade count, and per-trade realized P&L / win rate via avg-cost tracking.
//!
//! [`BacktestResults::compute`] reads transactions from the DB (the realistic
//! mode). [`BacktestResults::compute_in_memory`] reads them from the
//! [`InMemoryState`] (the fast in-memory mode — no DB I/O, used by the sweep).

use crate::database::tables::DbTables;
use std::collections::HashMap;
use std::path::Path;

use chrono::Duration;
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;

use crate::database::crud::CRUDTrait;
use crate::database::models::AssetType;
use crate::database::models_crud::transactions::transactions::{
    TransactionsCRUD, TransactionsFullKeys,
};

use crate::backtester::methods::in_memory::state::InMemoryState;
use crate::backtester::output::equity::{EquityCurve, EquitySnapshot};

/// One completed backtest, serialized to `backtest_results_{name}.json`.
///
/// All percentages are in percent units (e.g. `12.5` = +12.5%), not fractions.
#[derive(Debug, Clone, Serialize)]
pub struct BacktestResults {
    /// Capital assigned to this run before the first bar.
    pub starting_capital: f64,
    /// Equity at the last snapshot (or `starting_capital` for an empty curve).
    pub final_equity: f64,
    /// `final_equity - starting_capital`.
    pub total_pnl: f64,
    /// `total_pnl / starting_capital * 100`; `0.0` when capital is zero.
    pub total_return_pct: f64,
    /// Largest peak-to-trough decline of the equity curve, in percent.
    pub max_drawdown_pct: f64,
    /// Transaction rows read from the run (fills, including partials).
    pub num_trades: usize,
    /// Positions fully or partially closed by an opposite fill.
    pub num_closed_trades: usize,
    /// Closed trades with strictly positive realized P&L.
    pub num_winning_trades: usize,
    /// `num_winning_trades / num_closed_trades * 100`; `0.0` with no closes.
    pub win_rate_pct: f64,
    /// Per-bar Sharpe ratio, annualized — see `BacktestResults::build`.
    pub sharpe: f64,
    /// Per-bar Sortino ratio, annualized — see `BacktestResults::build`.
    pub sortino: f64,
    /// Bar width used for the run, in minutes (minimum 1).
    pub bar_interval_minutes: i64,
    /// Annualization base: `390*252/minute_interval`, or `252` for daily+ bars.
    pub bars_per_year: f64,
    /// The full equity history, oldest first.
    pub equity_curve: Vec<EquityPoint>,
}

/// A serialized [`EquitySnapshot`]: `time` is a Unix timestamp (seconds).
#[derive(Debug, Clone, Serialize)]
pub struct EquityPoint {
    /// Snapshot time, Unix seconds.
    pub time: i64,
    /// Cash leg at the snapshot.
    pub cash: f64,
    /// Mark-to-market value of open positions.
    pub positions_value: f64,
    /// `cash + positions_value`.
    pub equity: f64,
}

impl BacktestResults {
    /// Compute from the DB (the realistic mode — reads all stock transactions
    /// the backtest wrote to the test-db).
    ///
    /// Reads the whole `trading.stock_transactions` table, not just this
    /// strategy's rows, so it assumes the test DB is clean (or that only one
    /// run is being reported). Option transactions are ignored. `bar_interval`
    /// is the run's actual bar width and drives Sharpe/Sortino annualization.
    ///
    /// # Errors
    ///
    /// Returns the read failure as a string; an empty table is not an error.
    pub async fn compute(
        pool: &sqlx::PgPool,
        equity: &EquityCurve,
        starting_capital: f64,
        bar_interval: Duration,
    ) -> Result<Self, String> {
        let mut db = crate::database::db::DbDirect::connect(&pool);
        let mut tx_crud = db.transactions(&AssetType::Stock);
        let transactions = tx_crud
            .read_all()
            .await
            .map_err(|e| format!("read_all transactions: {e:?}"))?;
        let tuples: Vec<(&str, &str, &str, f64, f64, f64)> = transactions
            .iter()
            .filter_map(|t| match t {
                TransactionsFullKeys::Stock(s) => Some((
                    s.stock.as_str(),
                    s.primary_exchange.as_str(),
                    s.currency.as_str(),
                    s.quantity,
                    s.price,
                    s.fees.to_f64().unwrap_or(0.0),
                )),
                _ => None,
            })
            .collect();
        let num_trades = tuples.len();
        let per_trade_pnl = realized_per_trade_pnl(&tuples);
        Ok(Self::build(
            equity,
            starting_capital,
            num_trades,
            per_trade_pnl,
            bar_interval,
        ))
    }

    /// Compute from the in-memory state (the fast mode — no DB I/O; reads
    /// transactions from the [`InMemoryState`]). Used by the sweep runner.
    ///
    /// # Panics
    ///
    /// Panics if the state's transaction lock was poisoned by a panicking
    /// writer.
    pub fn compute_in_memory(
        equity: &EquityCurve,
        state: &InMemoryState,
        starting_capital: f64,
        bar_interval: Duration,
    ) -> Self {
        let txns = state
            .transactions
            .read()
            .expect("InMemoryState transactions poisoned");
        let tuples: Vec<(&str, &str, &str, f64, f64, f64)> = txns
            .iter()
            .map(|t| {
                (
                    t.stock.as_str(),
                    t.primary_exchange.as_str(),
                    t.currency.as_str(),
                    t.quantity,
                    t.price,
                    t.fees,
                )
            })
            .collect();
        let num_trades = tuples.len();
        let per_trade_pnl = realized_per_trade_pnl(&tuples);
        Self::build(
            equity,
            starting_capital,
            num_trades,
            per_trade_pnl,
            bar_interval,
        )
    }

    /// Common metrics builder — shared by `compute` (DB) + `compute_in_memory`.
    ///
    /// `bar_interval` is the actual bar frequency used in the backtest (from
    /// `BacktestConfig::stock_bar_interval`) — drives the Sharpe/Sortino
    /// annualization factor. Per-bar ratios are computed on simple equity
    /// returns between consecutive snapshots, then multiplied by
    /// `sqrt(bars_per_year)`, where `bars_per_year = 390*252 / interval_minutes`
    /// (390 RTH minutes per day, 252 trading days) or `252` for daily+ bars.
    ///
    /// `num_winning_trades` counts closed trades with strictly positive
    /// realized P&L; `win_rate_pct` is `0.0` when nothing closed.
    fn build(
        equity: &EquityCurve,
        starting_capital: f64,
        num_trades: usize,
        per_trade_pnl: Vec<f64>,
        bar_interval: Duration,
    ) -> Self {
        let num_closed = per_trade_pnl.len();
        let num_winning = per_trade_pnl.iter().filter(|p| **p > 0.0).count();
        let win_rate_pct = if num_closed > 0 {
            num_winning as f64 / num_closed as f64 * 100.0
        } else {
            0.0
        };

        let final_equity = equity.last_equity().unwrap_or(starting_capital);
        let total_pnl = final_equity - starting_capital;
        let total_return_pct = if starting_capital != 0.0 {
            total_pnl / starting_capital * 100.0
        } else {
            0.0
        };
        let max_drawdown_pct = max_drawdown_pct(&equity.snapshots);
        let (sharpe_per_bar, sortino_per_bar) = sharpe_sortino(&equity.snapshots);

        // Annualize: bars_per_year = trading_minutes_per_year / bar_interval
        // where trading_minutes_per_year = 390 min/day (9:30–16:00 ET) × 252 days.
        // For daily+ bars (interval >= 1440 min), use 252 bars/year directly.
        let bar_interval_minutes = bar_interval.num_minutes().max(1);
        let bars_per_year = if bar_interval_minutes >= 1440 {
            252.0
        } else {
            (390.0 * 252.0) / bar_interval_minutes as f64
        };
        let annualization_factor = bars_per_year.sqrt();
        let sharpe = sharpe_per_bar * annualization_factor;
        let sortino = sortino_per_bar * annualization_factor;

        let curve = equity
            .snapshots
            .iter()
            .map(|s| EquityPoint {
                time: s.time.timestamp(),
                cash: s.cash,
                positions_value: s.positions_value,
                equity: s.equity,
            })
            .collect();

        Self {
            starting_capital,
            final_equity,
            total_pnl,
            total_return_pct,
            max_drawdown_pct,
            num_trades,
            num_closed_trades: num_closed,
            num_winning_trades: num_winning,
            win_rate_pct,
            sharpe,
            sortino,
            bar_interval_minutes,
            bars_per_year,
            equity_curve: curve,
        }
    }

    /// Serialize pretty-printed JSON to `path`, overwriting an existing file,
    /// and log the destination.
    ///
    /// # Errors
    ///
    /// Serialization failure or an unwritable path.
    pub fn write_json(&self, path: &str) -> Result<(), String> {
        let json = serde_json::to_string_pretty(self).map_err(|e| format!("serialize: {e}"))?;
        std::fs::write(Path::new(path), json).map_err(|e| format!("write {path}: {e}"))?;
        tracing::info!("Backtest results written to {path}");
        Ok(())
    }
}

/// Largest peak-to-trough decline of the equity curve, in percent of the peak.
///
/// Returns `0.0` for an empty curve or one that never declines. Peaks at or
/// below zero are skipped (a drawdown is undefined without positive equity).
fn max_drawdown_pct(snapshots: &[EquitySnapshot]) -> f64 {
    let mut peak = f64::NEG_INFINITY;
    let mut max_dd = 0.0_f64;
    for s in snapshots {
        if s.equity > peak {
            peak = s.equity;
        }
        if peak > 0.0 {
            let dd = (peak - s.equity) / peak * 100.0;
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }
    max_dd
}

/// Per-bar Sharpe and Sortino, not annualized.
///
/// Returns (mean/std of simple equity returns, mean/downside-deviation), where
/// the downside deviation is the RMS of the negative returns over the total
/// number of bars (i.e. positives contribute zero, not excluded). Both are
/// `0.0` with fewer than two snapshots or a zero denominator.
fn sharpe_sortino(snapshots: &[EquitySnapshot]) -> (f64, f64) {
    if snapshots.len() < 2 {
        return (0.0, 0.0);
    }
    let returns: Vec<f64> = snapshots
        .windows(2)
        .map(|w| {
            let prev = w[0].equity;
            let cur = w[1].equity;
            if prev > 0.0 { (cur - prev) / prev } else { 0.0 }
        })
        .collect();
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
    let std = var.sqrt();
    let downside_var: f64 = returns
        .iter()
        .filter(|r| **r < 0.0)
        .map(|r| r.powi(2))
        .sum::<f64>()
        / n;
    let downside_std = downside_var.sqrt();
    let sharpe = if std > 0.0 { mean / std } else { 0.0 };
    let sortino = if downside_std > 0.0 {
        mean / downside_std
    } else {
        0.0
    };
    (sharpe, sortino)
}

/// Per-trade realized P&L via running avg-cost (FIFO-ish: avg cost blends
/// buys; a sell realizes `(sell_price - avg_cost) * sell_qty - fees`).
///
/// Takes a tuple slice `(stock, primary_exchange, currency, quantity, price,
/// fees)` so both the DB (`TransactionsFullKeys`) + the in-memory
/// (`InMemoryTransaction`) paths can adapt to it.
///
/// Positions are keyed by `stock:primary_exchange:currency`; a fill that flips
/// the sign realizes only the closed part and re-bases the remaining cost at
/// the fill price. The returned vector holds one entry per closing fill, in
/// transaction order; a still-open position contributes nothing.
fn realized_per_trade_pnl(transactions: &[(&str, &str, &str, f64, f64, f64)]) -> Vec<f64> {
    // (signed quantity, average cost) per instrument.
    let mut state: HashMap<String, (f64, f64)> = HashMap::new();
    // Sell-side fees are charged when the sell realizes. A sell that OPENS a
    // short has nothing to realize yet, so its fee is carried until the buy
    // that closes the position. Buy-side fees remain ignored (the avg-cost
    // pairing has always charged only the sell leg).
    let mut pending_sell_fees: HashMap<String, f64> = HashMap::new();
    let mut pnls = Vec::new();
    for (stock, pe, currency, quantity, price, fees) in transactions {
        let key = format!("{}:{}:{}", stock, pe, currency);
        let (qty, cost) = state.entry(key.clone()).or_insert((0.0, 0.0));
        let signed = *quantity;
        let closes = *qty != 0.0 && signed != 0.0 && signed.signum() != qty.signum();
        if closes {
            let closed_qty = signed.abs().min(qty.abs());
            let realized = if *qty > 0.0 {
                // Closing a long: sell proceeds above average cost.
                (*price - *cost) * closed_qty
            } else {
                // Closing a short: buy back below average cost.
                (*cost - *price) * closed_qty
            };
            let carry = pending_sell_fees.remove(&key).unwrap_or(0.0);
            let sell_fee = if signed < 0.0 { *fees } else { 0.0 };
            pnls.push(realized - sell_fee - carry);
            *qty += signed;
            if qty.abs() < 1e-9 {
                *qty = 0.0;
                *cost = 0.0;
            } else if signed.abs() > closed_qty + 1e-9 {
                // Flipped through flat: the excess opens at this trade's price.
                *cost = *price;
            }
        } else {
            if signed < 0.0 {
                *pending_sell_fees.entry(key).or_insert(0.0) += *fees;
            }
            let new_qty = *qty + signed;
            if new_qty.abs() > 1e-9 {
                *cost = (*qty * *cost + signed * *price) / new_qty;
            }
            *qty = new_qty;
        }
    }
    pnls
}
