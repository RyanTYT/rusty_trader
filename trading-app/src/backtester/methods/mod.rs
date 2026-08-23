//! Backtest methods — the [`BacktestMethod`] trait + impls. Each method takes
//! a `&BacktestContext` + produces an [`EquityCurve`].
//!
//! - [`HistoricalReplay`] — DB-backed (realistic, ~5-10 DB ops/bar).
//! - [`InMemoryReplay`] — fast in-memory (mocked CRUDs + in-memory reconcile,
//!   ~0 DB ops/bar after the one-time `load_bars`).
//!
//! Shared helpers (e.g., [`load_bars`]) live here so both methods can reuse
//! them without duplication.

pub mod historical;
pub mod in_memory;
pub mod method;

pub use historical::HistoricalReplay;
pub use in_memory::replay::InMemoryReplay;
pub use method::BacktestMethod;

use chrono::{DateTime, Utc};
use sqlx::FromRow;

use crate::database::models::{HistoricalStockDataFullKeys};
use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
use crate::helpers::contract::get_local_symbol;

use crate::backtester::context::BacktestContext;

/// Load the chronological bar stream for the configured contract + period.
/// The ONLY DB query in the in-memory method (one-time, not per-bar). Shared
/// by both `HistoricalReplay` + `InMemoryReplay`.
pub async fn load_bars(config: &crate::backtester::config::BacktestConfig, pool: &sqlx::PgPool) -> Result<Vec<HistoricalDataFullKeys>, String> {
    let c = config
        .subscribed_contracts
        .first()
        .expect("subscribed_contracts non-empty");
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

    use crate::backtester::config::BacktestPeriod;
    let rows: Vec<BarRow> = match &config.period {
        BacktestPeriod::TimeRange { start, end } => {
            sqlx::query_as(
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
            .fetch_all(pool)
            .await
            .map_err(|e| format!("load_bars (TimeRange): {e:?}"))?
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
            .fetch_all(pool)
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
        .collect())
}
