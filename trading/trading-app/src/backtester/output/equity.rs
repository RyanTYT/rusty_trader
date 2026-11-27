//! Per-bar equity snapshot, held in-memory by the replayer (no DB schema
//! change — avoids a new migration that would also touch the prod DB).

use crate::database::tables::DbTables;
#[cfg(feature = "backtest")]
use crate::backtester::setup::config::BacktestMode;
use chrono::{DateTime, Utc};
use ibapi::contracts::Contract;
use ibapi::contracts::SecurityType::ForexPair;
use ibapi::prelude::SecurityType;
use sqlx::PgPool;

use crate::database::crud::CRUDTrait;
use crate::database::models::AssetType;
use crate::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsFullKeys, CurrentPositionsOps,
};
use crate::helpers::contract::{LocalContractTypes, get_contract_from};
use crate::market_data::traits::current_price::PriceSupplier;

/// One mark-to-market snapshot, taken after the bar's fills settle.
#[derive(Debug, Clone)]
pub struct EquitySnapshot {
    /// Bar time of the snapshot.
    pub time: DateTime<Utc>,
    /// Cash balance (CASH:SGD position value) after settling this bar's fills.
    pub cash: f64,
    /// Mark-to-market value of open positions (base currency, excl. CASH).
    pub positions_value: f64,
    /// `cash + positions_value`.
    pub equity: f64,
}

/// The run's equity history, in replay order.
#[derive(Debug, Default)]
pub struct EquityCurve {
    /// One snapshot per replay bar.
    pub snapshots: Vec<EquitySnapshot>,
}

impl EquityCurve {
    /// Empty curve.
    pub fn new() -> Self {
        Self {
            snapshots: Vec::new(),
        }
    }

    /// Append a snapshot.
    pub fn push(&mut self, snap: EquitySnapshot) {
        self.snapshots.push(snap);
    }

    /// Equity of the latest snapshot, or `None` for an empty curve.
    pub fn last_equity(&self) -> Option<f64> {
        self.snapshots.last().map(|s| s.equity)
    }
}

/// Compute an equity snapshot at `time` from the strategy's current positions:
/// `CASH:*` rows add to `cash`, everything else is marked to market into
/// `positions_value`. Uses the DB (not `broker.cash()`) to avoid
/// double-counting — the broker updates the `CASH:SGD` position on fills, so
/// that row *is* the cash.
///
/// `fallback_close` prices a position whose contract the supplier cannot price
/// (e.g. a contract not subscribed in this run). A foreign-currency position is
/// converted to SGD at the IDEALPRO pair rate, falling back to the constant
/// `1.2713` when the pair cannot be priced.
///
/// When the `backtest` feature is on and the replay thread has installed its
/// [`InMemoryState`](crate::backtester::methods::in_memory::state::InMemoryState),
/// the positions come from that state instead of the DB.
///
/// # Limitations
///
/// Only `CurrentPositionsFullKeys::Stock` rows are valued — option positions
/// are skipped by the `_ => continue` arm, so option PnL does not appear in the
/// curve. A position whose absolute quantity is below `1e-9` is ignored.
pub async fn compute_snapshot(
    pool: &PgPool,
    prices: &dyn PriceSupplier,
    strategy_name: &str,
    time: DateTime<Utc>,
    fallback_close: f64,
) -> EquitySnapshot {
    // cfg-gated in-memory branch: if the thread-local InMemoryState is set
    // (InMemoryReplay mode), read positions from it instead of the DB.
    #[cfg(feature = "backtest")]
    {
        if let Some(state) = crate::backtester::methods::in_memory::state::current() {
            return compute_snapshot_from_positions(
                state.current_positions_snapshot(),
                prices,
                time,
                fallback_close,
            );
        }
    }
    // DB-backed path.
    let mut db = crate::database::db::DbDirect::connect(&pool);
    let mut cp_crud = db.current_positions(&AssetType::Stock);
    #[cfg(feature = "backtest")]
    let positions = match cp_crud
        .get_pos_by_strat(strategy_name, BacktestMode::Db)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("snapshot: get_pos_by_strat failed: {e:?}");
            Vec::new()
        }
    };
    #[cfg(not(feature = "backtest"))]
    let positions = match cp_crud.get_pos_by_strat(strategy_name).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("snapshot: get_pos_by_strat failed: {e:?}");
            Vec::new()
        }
    };
    let mut cash = 0.0_f64;
    let mut positions_value = 0.0_f64;
    for pos in positions {
        let (stock, qty) = match &pos {
            CurrentPositionsFullKeys::Stock(v) => (v.stock.clone(), v.quantity),
            _ => continue,
        };
        if qty.abs() < 1e-9 {
            continue;
        }
        let currency = pos.get_currency().into();
        let pcontract = get_contract_from(&LocalContractTypes::CurrentPosFk(pos));
        let price = prices
            .get_current_price(pcontract, false, &[])
            .unwrap_or(fallback_close);
        if stock.starts_with("CASH:") {
            cash += qty * price;
        } else {
            let fx_exchange = prices
                .get_current_price(
                    Contract {
                        symbol: currency,
                        exchange: "IDEALPRO".into(),
                        primary_exchange: "".into(),
                        currency: "SGD".into(),
                        security_type: ForexPair,
                        ..Default::default()
                    },
                    false,
                    &[],
                )
                .unwrap_or(1.2713);
            positions_value += qty * price * fx_exchange;
        }
    }
    let equity = cash + positions_value;
    EquitySnapshot {
        time,
        cash,
        positions_value,
        equity,
    }
}

/// Value a list of `(PositionKey, InMemoryPosition)` — the shared valuation
/// logic used by the in-memory `compute_snapshot` branch. `CASH:*` rows add to
/// `cash`, everything else is marked to market into `positions_value`; a
/// position is priced by the supplier and converted to SGD at the IDEALPRO pair
/// rate (constant `1.2713` when the pair cannot be priced).
///
/// Differences from the DB path: cash in the base currency is priced at exactly
/// `1.0` (`CASH:SGD`), the position contract is rebuilt from the key's
/// stock/exchange/currency, and no DB access happens at all, so this is safe to
/// call from the replay thread.
pub fn compute_snapshot_from_positions(
    positions: Vec<(
        crate::backtester::methods::in_memory::state::PositionKey,
        crate::backtester::methods::in_memory::state::InMemoryPosition,
    )>,
    prices: &dyn PriceSupplier,
    time: DateTime<Utc>,
    fallback_close: f64,
) -> EquitySnapshot {
    let mut cash = 0.0_f64;
    let mut positions_value = 0.0_f64;
    for (key, pos) in positions {
        if pos.quantity.abs() < 1e-9 {
            continue;
        }
        let pcontract = crate::helpers::contract::build_contract_from_stock(
            &key.stock,
            &key.primary_exchange,
            &key.currency,
        );
        let price = if key.stock == "CASH:SGD" {
            1.0
        } else {
            prices
                .get_current_price(pcontract, false, &[])
                .unwrap_or(fallback_close)
        };
        if key.stock.starts_with("CASH:") {
            cash += pos.quantity * price;
        } else {
            let fx_exchange = prices
                .get_current_price(
                    Contract {
                        symbol: key.currency.into(),
                        exchange: "IDEALPRO".into(),
                        primary_exchange: "".into(),
                        currency: "SGD".into(),
                        security_type: ForexPair,
                        ..Default::default()
                    },
                    false,
                    &[],
                )
                .unwrap_or(1.2713);
            positions_value += pos.quantity * price * fx_exchange;
        }
    }
    let equity = cash + positions_value;
    EquitySnapshot {
        time,
        cash,
        positions_value,
        equity,
    }
}
