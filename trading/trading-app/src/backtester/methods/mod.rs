//! Backtest methods — the [`BacktestMethod`] trait + impls. Each method takes
//! a `&BacktestContext` + produces an [`EquityCurve`].
//!
//! - [`HistoricalReplay`] — DB-backed (realistic, ~5-10 DB ops/bar).
//! - [`InMemoryReplay`] — fast in-memory (mocked CRUDs + in-memory reconcile,
//!   ~0 DB ops/bar after the one-time [`load_bars`]).
//!
//! Shared helpers (e.g., [`load_bars`]) live here so both methods can reuse
//! them without duplication.

pub mod historical;
pub mod in_memory;

use std::collections::{BTreeSet, HashMap};

pub use historical::HistoricalReplay;
pub use in_memory::replay::InMemoryReplay;

use chrono::{DateTime, Utc};
use sqlx::FromRow;

use crate::database::models::HistoricalStockDataFullKeys;
use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::helpers::contract::get_local_symbol;

use crate::backtester::output::equity::EquityCurve;
use crate::backtester::setup::context::BacktestContext;

/// Trait for backtest methods — each method implements a different replay
/// strategy (historical bar replay, walk-forward, parameter sweep, etc.).
/// The shared execution surface is in [`BacktestContext`]; each method's
/// `run` produces an [`EquityCurve`].
pub trait BacktestMethod {
    /// Run the backtest method against the shared `ctx`. Returns the per-bar
    /// equity curve.
    fn run(&self, ctx: BacktestContext) -> Result<EquityCurve, String>;
}

/// Load the chronological bar stream for the configured contract + period.
/// The ONLY DB query in the in-memory method (one-time, not per-bar). Shared
/// by both `HistoricalReplay` + `InMemoryReplay`.
///
/// One `market_data.historical_data` query per subscribed contract, run
/// concurrently and returned in `config.subscribed_contracts` order:
///
/// - [`BacktestPeriod::TimeRange`] loads the inclusive window
///   `start ..= end`, then prepends up to `warmup_bars` bars from just before
///   `start` (DESC query, reversed to ASC) so warm-up does not consume the
///   replay window.
/// - [`BacktestPeriod::NumBars`] loads the newest `n` bars (DESC) and reverses
///   them to ascending order.
///
/// Options are flattened to their underlying local symbol (see
/// [`get_local_symbol`]), so an option backtest replays the stock bars.
///
/// # Errors
///
/// Returns `Err` with the failing query and period kind when any contract's
/// read fails; a partial result is not returned.
pub async fn load_bars(
    config: &crate::backtester::setup::config::BacktestConfig,
    pool: &sqlx::PgPool,
    warmup_bars: usize,
) -> Result<Vec<Vec<HistoricalDataFullKeys>>, String> {
    let data_fut = config.subscribed_contracts.iter().map(|c| {
        let pool_clone = pool.clone();
        let warmup_bars = warmup_bars;
        async move {
            let stock = get_local_symbol(c);
            let pe = c.primary_exchange.to_string();
            let currency = c.currency.to_string();

            #[derive(FromRow)]
            struct BarRow {
                stock: String,
                primary_exchange: String,
                currency: String,
                time: DateTime<Utc>,
                open: f64,
                high: f64,
                low: f64,
                close: f64,
                volume: rust_decimal::Decimal,
            }

            use crate::backtester::setup::config::BacktestPeriod;
            let rows: Vec<BarRow> = match &config.period {
                BacktestPeriod::TimeRange { start, end } => {
                    // Window bars (the backtest period).
                    let mut rows: Vec<BarRow> = sqlx::query_as(
                        r#"SELECT stock, primary_exchange, currency, time, open, high, low, close, volume
                           FROM market_data.historical_data
                           WHERE stock = $1 AND primary_exchange = $2 AND currency = $3
                             AND time >= $4 AND time <= $5
                           ORDER BY time ASC"#,
                    )
                    .bind(stock.clone())
                    .bind(pe.clone())
                    .bind(currency.clone())
                    .bind(*start)
                    .bind(*end)
                    .fetch_all(&pool_clone)
                    .await
                    .map_err(|e| format!("load_bars (TimeRange window): {e:?}"))?;

                    // Warmup prefix: the `warmup_bars` bars BEFORE `start` —
                    // loaded so the strategy's warm_up_data can read them from
                    // the bar cache (now=Some(bar_time)) without eating into
                    // the backtest window.
                    if warmup_bars > 0 {
                        let mut prefix: Vec<BarRow> = sqlx::query_as(
                            r#"SELECT stock, primary_exchange, currency, time, open, high, low, close, volume
                               FROM market_data.historical_data
                               WHERE stock = $1 AND primary_exchange = $2 AND currency = $3
                                 AND time < $4
                               ORDER BY time DESC
                               LIMIT $5"#,
                        )
                        .bind(stock.clone())
                        .bind(pe.clone())
                        .bind(currency.clone())
                        .bind(*start)
                        .bind(warmup_bars as i64)
                        .fetch_all(&pool_clone)
                        .await
                        .map_err(|e| format!("load_bars (TimeRange prefix): {e:?}"))?;
                        prefix.reverse(); // DESC -> ASC (oldest-first, contiguous with the window)
                        prefix.append(&mut rows); // prefix + window
                        prefix
                    } else {
                        rows
                    }
                }
                BacktestPeriod::NumBars(n) => {
                    // Last N bars in the DB (DESC) — reverse to chronological (ASC).
                    let mut rows: Vec<BarRow> = sqlx::query_as(
                    r#"SELECT stock, primary_exchange, currency, time, open, high, low, close, volume
                       FROM market_data.historical_data
                       WHERE stock = $1 AND primary_exchange = $2 AND currency = $3
                       ORDER BY time DESC
                       LIMIT $4"#,
                )
                .bind(stock.clone())
                .bind(pe.clone())
                .bind(currency.clone())
                .bind(*n as i64)
                .fetch_all(&pool_clone)
                .await
                .map_err(|e| format!("load_bars (NumBars): {e:?}"))?;
                    rows.reverse();
                    rows
                }
            };

            Ok(rows
                .into_iter()
                .map(|r| {
                    HistoricalDataFullKeys::Stock(HistoricalStockDataFullKeys {
                        stock: r.stock,
                        primary_exchange: r.primary_exchange,
                        currency: r.currency,
                        time: r.time,
                        open: r.open,
                        high: r.high,
                        low: r.low,
                        close: r.close,
                        volume: r.volume,
                    })
                })
                .collect::<Vec<HistoricalDataFullKeys>>())
        }
    });

    let results = futures::future::join_all(data_fut).await;
    let data = results.into_iter().collect::<Result<Vec<_>, String>>()?;

    Ok(data)
}

/// Transpose per-contract bar vectors into one row per unique timestamp.
///
/// The output has one row per timestamp in the union of all contracts'
/// `time` values (ascending) and one column per contract, in input order.
/// Missing bars are `None`, so the replay loop can advance time even when a
/// contract has no bar at that instant.
///
/// # Known limitations
///
/// - If the first contract has no bars, the whole result is empty even when
///   later contracts do have data.
/// - A contract with an empty vector panics (integer underflow at
///   `len() - 1`) when the loop reaches it.
#[must_use]
pub fn transpose(
    matrix: Vec<Vec<HistoricalDataFullKeys>>,
) -> Vec<Vec<Option<HistoricalDataFullKeys>>> {
    if matrix.is_empty() || matrix[0].is_empty() {
        return vec![];
    }

    let rows = matrix.len();
    let cols = matrix[0].len();

    let mut unique_times: BTreeSet<DateTime<Utc>> = BTreeSet::new();
    for bars in &matrix {
        for bar in bars {
            unique_times.insert(bar.get_time());
        }
    }

    // Create a new matrix with swapped dimensions (cols x rows)
    let mut result: Vec<Vec<Option<HistoricalDataFullKeys>>> =
        vec![vec![None; rows]; unique_times.len()];

    let mut indices = vec![0; rows];
    for (res_idx, time) in unique_times.into_iter().enumerate() {
        indices = indices
            .into_iter()
            .enumerate()
            .map(|(stock_idx, idx)| {
                if idx as usize > matrix[stock_idx].len() - 1 {
                    return idx;
                }
                let bar = matrix[stock_idx][idx as usize].clone();
                if bar.get_time() == time {
                    result[res_idx][stock_idx] = Some(bar);
                    idx + 1
                } else {
                    idx
                }
            })
            .collect()
    }

    result
}
