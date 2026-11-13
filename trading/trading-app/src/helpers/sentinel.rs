use chrono::{DateTime, Utc};
use ibapi::{contracts::Contract, market_data::realtime::WhatToShow};

use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;

pub trait Sentinel {
    fn sentinel() -> Self;
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

    fn is_sentinel(value: &Self) -> bool {
        return value.get_time() == DateTime::<Utc>::UNIX_EPOCH;
    }
}
