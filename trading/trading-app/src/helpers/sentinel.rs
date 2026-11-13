use chrono::{DateTime, Utc};
use ibapi::{contracts::Contract, market_data::realtime::WhatToShow};

use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;

pub trait Sentinel {
    fn sentinel() -> Self;
}

impl Sentinel for Contract {
    fn sentinel() -> Self {
        Contract {
            symbol: "__SENTINEL__".into(),
            ..Default::default()
        }
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
}
