//! Per-bar equity snapshot, held in-memory by the replayer (no DB schema
//! change — avoids a new migration that would also touch the prod DB).

use chrono::{DateTime, Utc};
use ibapi::contracts::Contract;
use ibapi::prelude::SecurityType;
use sqlx::PgPool;

use crate::database::crud::CRUDTrait;
use crate::database::models::AssetType;
use crate::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsFullKeys, CurrentPositionsOps,
};
use crate::helpers::contract::{get_contract_from, LocalContractTypes};
use crate::market_data::traits::current_price::PriceSupplier;

#[derive(Debug, Clone)]
pub struct EquitySnapshot {
    pub time: DateTime<Utc>,
    /// Cash balance (CASH:SGD position value) after settling this bar's fills.
    pub cash: f64,
    /// Mark-to-market value of open positions (base currency, excl. CASH).
    pub positions_value: f64,
    /// `cash + positions_value`.
    pub equity: f64,
}

#[derive(Debug, Default)]
pub struct EquityCurve {
    pub snapshots: Vec<EquitySnapshot>,
}

impl EquityCurve {
    pub fn new() -> Self {
        Self {
            snapshots: Vec::new(),
        }
    }

    pub fn push(&mut self, snap: EquitySnapshot) {
        self.snapshots.push(snap);
    }

    pub fn last_equity(&self) -> Option<f64> {
        self.snapshots.last().map(|s| s.equity)
    }
}

/// Compute an equity snapshot at `time` by iterating the strategy's current
/// positions: `CASH:*` → `cash`, the rest → `positions_value` (valued via the
/// price supplier). `fallback_close` is used if the supplier can't price a
/// position contract.
///
/// Uses the DB (not `broker.cash()`) to avoid double-counting — the broker
/// updates the `CASH:SGD` position on fills, so it IS the cash.
pub async fn compute_snapshot(
    pool: &PgPool,
    prices: &dyn PriceSupplier,
    strategy_name: &str,
    time: DateTime<Utc>,
    _contract: &Contract,
    fallback_close: f64,
) -> EquitySnapshot {
    // cfg-gated in-memory branch: if the thread-local InMemoryState is set
    // (InMemoryReplay mode), read positions from it instead of the DB.
    #[cfg(feature = "backtest")]
    {
        if let Some(state) = crate::backtester::methods::in_memory::thread_local::current() {
            return compute_snapshot_from_positions(
                state.current_positions_snapshot(),
                prices,
                time,
                fallback_close,
            );
        }
    }
    // DB-backed path.
    let cp_crud = CurrentPositionsCRUD::from(&AssetType::Stock, pool.clone());
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
        let pcontract = get_contract_from(&LocalContractTypes::CurrentPosFk(pos));
        let price = prices
            .get_current_price(pcontract, false, &[])
            .unwrap_or(fallback_close);
        if stock.starts_with("CASH:") {
            cash += qty * price;
        } else {
            positions_value += qty * price;
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
/// logic used by the in-memory `compute_snapshot` branch. `CASH:*` → `cash`,
/// the rest → `positions_value` (valued via the price supplier).
pub fn compute_snapshot_from_positions(
    positions: Vec<(crate::backtester::methods::in_memory::state::PositionKey, crate::backtester::methods::in_memory::state::InMemoryPosition)>,
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
        let pcontract = Contract {
            symbol: key.stock.clone().into(),
            security_type: if key.stock.starts_with("CASH:") {
                SecurityType::ForexPair
            } else {
                SecurityType::Stock
            },
            exchange: if key.stock.starts_with("CASH:") {
                "IDEALPRO".into()
            } else {
                key.primary_exchange.clone().into()
            },
            currency: key.currency.clone().into(),
            ..Default::default()
        };
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
            positions_value += pos.quantity * price;
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
