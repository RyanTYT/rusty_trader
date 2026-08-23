//! Per-bar equity snapshot, held in-memory by the replayer (no DB schema
//! change — avoids a new migration that would also touch the prod DB).

use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct EquitySnapshot {
    pub time: DateTime<Utc>,
    /// Cash balance (base currency) after settling this bar's fills.
    pub cash: f64,
    /// Mark-to-market value of open positions (base currency).
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
