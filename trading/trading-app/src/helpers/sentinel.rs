//! Placeholder values used to mark empty slots in fixed-size buffers and
//! streams.
//!
//! A sentinel is never valid data: callers test `is_sentinel` to skip it. The
//! two implementations are deliberately cheap — the contract checks the symbol
//! string, the bar checks the timestamp — because sentinels can appear in hot
//! loops.

use chrono::{DateTime, Utc};
use ibapi::{contracts::Contract, market_data::realtime::WhatToShow};

use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;

/// A type with a canonical "empty" value.
pub trait Sentinel {
    /// The placeholder value.
    fn sentinel() -> Self;
    /// Whether `value` is the placeholder.
    fn is_sentinel(value: &Self) -> bool;
}

impl Sentinel for Contract {
    fn sentinel() -> Self {
        Contract {
            symbol: "__SENTINEL__".into(),
            ..Default::default()
        }
    }

    fn is_sentinel(value: &Self) -> bool {
        return value.symbol.as_str() == "__SENTINEL__";
    }
}

impl Sentinel for HistoricalDataFullKeys {
    /// A bar at the Unix epoch with NaN OHLC and volume.
    fn sentinel() -> Self {
        Self::from_data(
            &Contract::sentinel(),
            &WhatToShow::Trades,
            DateTime::<Utc>::UNIX_EPOCH,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
        )
    }

    /// Sentinel by timestamp only — any bar stamped at `UNIX_EPOCH` counts.
    fn is_sentinel(value: &Self) -> bool {
        return value.get_time() == DateTime::<Utc>::UNIX_EPOCH;
    }
}
