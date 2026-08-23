//! Backtest results & metrics, computed from the real `TransactionsCRUD` +
//! the in-memory equity curve, serialized to JSON.
//!
//! v1 metrics: total PnL / return, max drawdown, per-bar Sharpe & Sortino
//! (not annualized — annualization depends on bar frequency, which varies),
//! trade count, and per-trade realized P&L / win rate via avg-cost tracking.

use std::collections::HashMap;
use std::path::Path;

use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;

use crate::database::crud::CRUDTrait;
use crate::database::models::AssetType;
use crate::database::models_crud::transactions::transactions::{
    TransactionsCRUD, TransactionsFullKeys,
};

use crate::backtester::equity::{EquityCurve, EquitySnapshot};

#[derive(Debug, Serialize)]
pub struct BacktestResults {
    pub starting_capital: f64,
    pub final_equity: f64,
    pub total_pnl: f64,
    pub total_return_pct: f64,
    pub max_drawdown_pct: f64,
    pub num_trades: usize,
    pub num_closed_trades: usize,
    pub num_winning_trades: usize,
    pub win_rate_pct: f64,
    pub sharpe_per_bar: f64,
    pub sortino_per_bar: f64,
    pub equity_curve: Vec<EquityPoint>,
}

#[derive(Debug, Serialize)]
pub struct EquityPoint {
    pub time: String,
    pub cash: f64,
    pub positions_value: f64,
    pub equity: f64,
}

impl BacktestResults {
    pub async fn compute(
        pool: &sqlx::PgPool,
        equity: &EquityCurve,
        starting_capital: f64,
    ) -> Result<Self, String> {
        // Read all stock transactions the backtest wrote (test-db is fresh per run).
        let tx_crud = TransactionsCRUD::from(&AssetType::Stock, pool.clone());
        let transactions = tx_crud
            .read_all()
            .await
            .map_err(|e| format!("read_all transactions: {e:?}"))?;

        let num_trades = transactions.len();
        let per_trade_pnl = realized_per_trade_pnl(&transactions);
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
        let (sharpe, sortino) = sharpe_sortino(&equity.snapshots);
        let curve = equity
            .snapshots
            .iter()
            .map(|s| EquityPoint {
                time: s.time.to_rfc3339(),
                cash: s.cash,
                positions_value: s.positions_value,
                equity: s.equity,
            })
            .collect();

        Ok(Self {
            starting_capital,
            final_equity,
            total_pnl,
            total_return_pct,
            max_drawdown_pct,
            num_trades,
            num_closed_trades: num_closed,
            num_winning_trades: num_winning,
            win_rate_pct,
            sharpe_per_bar: sharpe,
            sortino_per_bar: sortino,
            equity_curve: curve,
        })
    }

    pub fn write_json(&self, path: &str) -> Result<(), String> {
        let json =
            serde_json::to_string_pretty(self).map_err(|e| format!("serialize: {e}"))?;
        std::fs::write(Path::new(path), json).map_err(|e| format!("write {path}: {e}"))?;
        tracing::info!("Backtest results written to {path}");
        Ok(())
    }
}

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

fn sharpe_sortino(snapshots: &[EquitySnapshot]) -> (f64, f64) {
    if snapshots.len() < 2 {
        return (0.0, 0.0);
    }
    let returns: Vec<f64> = snapshots
        .windows(2)
        .map(|w| {
            let prev = w[0].equity;
            let cur = w[1].equity;
            if prev > 0.0 {
                (cur - prev) / prev
            } else {
                0.0
            }
        })
        .collect();
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
    let std = var.sqrt();
    let downside_var: f64 = returns.iter().filter(|r| **r < 0.0).map(|r| r.powi(2)).sum::<f64>() / n;
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
/// buys; a sell realizes `(sell_price - avg_cost) * sell_qty`).
fn realized_per_trade_pnl(transactions: &[TransactionsFullKeys]) -> Vec<f64> {
    // stock -> (qty, avg_cost)
    let mut state: HashMap<String, (f64, f64)> = HashMap::new();
    let mut pnls = Vec::new();
    for tx in transactions {
        let s = match tx {
            TransactionsFullKeys::Stock(s) => s,
            _ => continue,
        };
        let key = format!("{}:{}:{}", s.stock, s.primary_exchange, s.currency);
        let (qty, cost) = state.entry(key).or_insert((0.0, 0.0));
        if s.quantity >= 0.0 {
            // buy — blend avg cost
            let new_qty = *qty + s.quantity;
            if new_qty.abs() > 1e-9 {
                *cost = (*qty * *cost + s.quantity * s.price) / new_qty;
            }
            *qty = new_qty;
        } else {
            // sell — realize
            let sell_qty = s.quantity.abs();
            let realized_qty = sell_qty.min(qty.abs());
            let realized = (s.price - *cost) * realized_qty
                - s.fees.to_f64().unwrap_or(0.0);
            pnls.push(realized);
            *qty -= sell_qty;
            if qty.abs() < 1e-9 {
                *cost = 0.0;
            }
        }
    }
    pnls
}
