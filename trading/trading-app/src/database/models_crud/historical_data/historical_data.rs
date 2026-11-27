//! Historical bars and the reads built on them.
//!
//! One interface ([`HistoricalDataCRUD`]) over three physical tables:
//! `market_data.historical_data` (stock/future/CFD/cash),
//! `market_data.historical_options_data`, and
//! `market_data.historical_forex_data`. Bar times are UTC; bucketing and
//! "today" filters run in `America/New_York` (the market's civil day).
//!
//! The ops here are read-side analytics:
//! [`HistoricalDataOps::read_last_n`] (time-bucketed OHLCV aggregation with a
//! completeness split), [`HistoricalDataOps::read_last_bar`],
//! [`HistoricalDataOps::read_last_vwap`], and
//! [`HistoricalDataOps::get_rows_since`]; [`NoiseOps`] computes the average
//! absolute move since the open. Writes go through the CRUD interface and the
//! batch COPY writer in [`super::batch_operations`].

//! Historical-data interface: stock, option and forex bars, selected by
//! [`AssetType`].
//!
//! The concrete table is chosen from the contract's security type. The methods
//! in [`HistoricalDataOps`] aggregate and read bars (latest buckets, last bar,
//! VWAP, row counts); the inner handles expose the generic CRUD operations.
//! Timestamps are stored in UTC; bucket boundaries are built in the requested
//! timezone (default `US/Eastern`).
//!
//! # Example
//!
//! Three 5-minute AAPL bars; the last two buckets are read and asserted.
//! Against `TEST_DATABASE_URL` this runs; without it, it is skipped.
//!
//! ```rust
//! use chrono::{Duration, TimeZone, Utc};
//! use rust_decimal::Decimal;
//! use trading_app::database::crud::CRUDTrait;
//! use trading_app::database::db::DbDirect;
//! use trading_app::database::models::{
//!     AssetType, HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys,
//!     HistoricalStockDataPrimaryKeysWoTime,
//! };
//! use trading_app::database::models_crud::historical_data::historical_data::{
//!     HistoricalDataOps, HistoricalDataPrimaryKeysWoTime,
//! };
//! use trading_app::database::tables::DbTables;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
//! # let pool = sqlx::PgPool::connect(&url).await?;
//! // Near ET midnight the "today" filter can drop the oldest bar; skip that window.
//! # if Utc::now().with_timezone(&chrono_tz::America::New_York).time()
//! #     < chrono::NaiveTime::from_hms_opt(0, 20, 0).unwrap() { return Ok(()); }
//! let stock = format!("DOC{}", std::process::id());
//! let now = chrono::DateTime::<Utc>::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
//! let mut db = DbDirect::connect(&pool);
//!
//! // Three bars, fifteen, ten and five minutes old, closes 100/101/102.
//! for (minutes, close) in [(15, 100.0), (10, 101.0), (5, 102.0)] {
//!     db.historical_stock_data()
//!         .create(&HistoricalStockDataFullKeys {
//!             stock: stock.clone(),
//!             primary_exchange: "NASDAQ".into(),
//!             currency: "USD".into(),
//!             time: now - Duration::minutes(minutes),
//!             open: close,
//!             high: close,
//!             low: close,
//!             close,
//!             volume: Decimal::new(100, 0),
//!         })
//!         .await?;
//! }
//!
//! let pk_n = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
//!     stock: stock.clone(),
//!     primary_exchange: "NASDAQ".into(),
//!     currency: "USD".into(),
//! });
//! let bars = db.historical_data(&AssetType::Stock).read_last_n(pk_n, 5, 2).await?;
//! assert_eq!(
//!     bars.full.iter().map(|row| row.get_price()).collect::<Vec<_>>(),
//!     vec![101.0, 102.0] // oldest first
//! );
//!
//! // The newest complete bucket is the last bar.
//! let pk_bar = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
//!     stock: stock.clone(),
//!     primary_exchange: "NASDAQ".into(),
//!     currency: "USD".into(),
//! });
//! let last = db.historical_data(&AssetType::Stock).read_last_bar(pk_bar, 5).await?;
//! assert_eq!(last.get_price(), 102.0);
//!
//! // Clean up.
//! for minutes in [15, 10, 5] {
//!     db.historical_stock_data()
//!         .delete(&HistoricalStockDataPrimaryKeys {
//!             stock: stock.clone(),
//!             primary_exchange: "NASDAQ".into(),
//!             currency: "USD".into(),
//!             time: now - Duration::minutes(minutes),
//!         })
//!         .await?;
//! }
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
#[cfg(feature = "backtest")]
use chrono::TimeZone;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use ibapi::{contracts::Contract, market_data::realtime::WhatToShow};
use rust_decimal::{Decimal, dec, prelude::FromPrimitive};
use sqlx::prelude::FromRow;

use crate::{
    database::{
        db::Exec,
        models::{
            AssetType, DailyHistoricalStockDataPrimaryKeys, HistoricalForexDataFullKeys,
            HistoricalForexDataPrimaryKeys, HistoricalForexDataPrimaryKeysWoTime,
            HistoricalForexDataUpdateKeys, HistoricalOptionsDataFullKeys,
            HistoricalOptionsDataPrimaryKeys, HistoricalOptionsDataPrimaryKeysWoTime,
            HistoricalOptionsDataUpdateKeys, HistoricalStockDataFullKeys,
            HistoricalStockDataPrimaryKeys, HistoricalStockDataPrimaryKeysWoTime,
            HistoricalStockDataUpdateKeys, OptionType,
        },
        models_crud::historical_data::{
            historical_forex_data::HistoricalForexDataCRUD,
            historical_options_data::HistoricalOptionsDataCRUD,
            historical_stock_data::HistoricalStockDataCRUD,
        },
    },
    helpers::contract::get_local_symbol,
    implement_crud_trait_for_interface,
};

/// Asset-dispatching handle over the three historical tables.
///
/// Built by [`HistoricalDataCRUD::from_exec`]; the variant selects the table
/// and therefore the key shapes. Stock, future, CFD and cash contracts all map
/// to [`HistoricalDataCRUD::Stock`]; [`AssetType::Unknown`] panics.
pub enum HistoricalDataCRUD<'a> {
    /// `market_data.historical_data` (stock, future, CFD and cash bars).
    Stock(HistoricalStockDataCRUD<'a>),
    /// `market_data.historical_options_data`.
    Options(HistoricalOptionsDataCRUD<'a>),
    /// `market_data.historical_forex_data` (separate bid/ask columns).
    Forex(HistoricalForexDataCRUD<'a>),
}

#[derive(Debug, Clone, PartialEq)]
/// One bar of any asset class.
///
/// The variant mirrors the physical table; prefer the class-agnostic getters
/// ([`HistoricalDataFullKeys::get_price`] and friends) when the caller does not
/// care which table produced the bar.
pub enum HistoricalDataFullKeys {
    /// A stock/future/CFD/cash OHLCV bar.
    Stock(HistoricalStockDataFullKeys),
    /// An option OHLCV bar (includes the option identity).
    Options(HistoricalOptionsDataFullKeys),
    /// A forex bid/ask bar; either side may be absent.
    Forex(HistoricalForexDataFullKeys),
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for HistoricalDataFullKeys {
    /// Always errors: the enum is never decoded from a raw row. The inner
    /// `CRUD<FK, PK, UK>` decodes the concrete struct first and wraps it.
    fn from_row(_: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        // This will never be executed because the inner CRUD<FK,PK,UK>
        // decodes the inner concrete struct before wrapping it into this enum.
        Err(sqlx::Error::Decode(
            "HistoricalDataFullKeys cannot be decoded directly from a raw SQL row".into(),
        ))
    }
}

/// Generates the four class-agnostic OHLC getters on
/// [`HistoricalDataFullKeys`]; forex falls back to the ask column and then the
/// bid column.
macro_rules! delegate_price {
    ($( [$fn_name:ident, $field:ident, $ask_field:ident, $bid_field:ident, $doc:literal ]),+ $(,)?) => {
        $(
            #[doc = $doc]
            pub fn $fn_name(&self) -> f64 {
                match self {
                    Self::Stock(v) => v.$field,
                    Self::Options(v) => v.$field,
                    Self::Forex(v) => v.$ask_field.unwrap_or(v.$bid_field.unwrap_or(-1.0)),
                }
            }
        )+
    };
}
impl HistoricalDataFullKeys {
    delegate_price!(
        [
            get_price,
            close,
            ask_close,
            bid_close,
            "Latest close. Forex falls back to the ask close, then the bid close; returns -1.0 when both are absent."
        ],
        [
            get_open_price,
            open,
            ask_open,
            bid_open,
            "Opening price. Forex falls back to the ask open, then the bid open; returns -1.0 when both are absent."
        ],
        [
            get_high_price,
            high,
            ask_high,
            bid_high,
            "Session high. Forex falls back to the ask high, then the bid high; returns -1.0 when both are absent."
        ],
        [
            get_low_price,
            low,
            ask_low,
            bid_low,
            "Session low. Forex falls back to the ask low, then the bid low; returns -1.0 when both are absent."
        ],
    );
}

impl HistoricalDataFullKeys {
    /// Bar timestamp (UTC).
    pub fn get_time(&self) -> DateTime<Utc> {
        match self {
            Self::Stock(v) => v.time,
            Self::Forex(v) => v.time,
            Self::Options(v) => v.time,
        }
    }

    /// Forex bid open.
    ///
    /// # Panics
    /// Panics for a stock or option bar (they carry a single price series).
    pub fn get_bid_open(&self) -> Option<f64> {
        match self {
            Self::Forex(v) => v.bid_open,
            _ => panic!("Tried to get bid open of non-stock asset"),
        }
    }
    /// Forex ask open.
    ///
    /// # Panics
    /// Panics for a stock or option bar.
    pub fn get_ask_open(&self) -> Option<f64> {
        match self {
            Self::Forex(v) => v.ask_open,
            _ => panic!("Tried to get ask open of non-stock asset"),
        }
    }

    /// Bar volume.
    ///
    /// # Panics
    /// Panics for a forex bar: forex tables carry no volume column.
    pub fn get_volume(&self) -> Decimal {
        match self {
            Self::Stock(v) => v.volume,
            Self::Forex(_) => panic!("Tried to get volume from forex bar"),
            Self::Options(v) => v.volume,
        }
    }

    /// Combines separately stored bid and ask forex bars into one bar.
    ///
    /// The pair and timestamp come from `bid_data_wrapped`; the OHLC fields are
    /// copied from their respective side.
    ///
    /// # Panics
    /// Panics unless both inputs are [`HistoricalDataFullKeys::Forex`].
    pub fn from_inter_repr(
        contract: &Contract,
        bid_data_wrapped: &HistoricalDataFullKeys,
        ask_data_wrapped: &HistoricalDataFullKeys,
    ) -> Self {
        let bid_data = match bid_data_wrapped {
            HistoricalDataFullKeys::Forex(v) => v,
            _ => panic!("Tried to call from_inter_repr for non-FX contract"),
        };
        let ask_data = match ask_data_wrapped {
            HistoricalDataFullKeys::Forex(v) => v,
            _ => panic!("Tried to call from_inter_repr for non-FX contract"),
        };
        Self::Forex(HistoricalForexDataFullKeys {
            pair: get_local_symbol(contract),
            time: bid_data.time,
            bid_open: bid_data.bid_open,
            bid_high: bid_data.bid_high,
            bid_low: bid_data.bid_low,
            bid_close: bid_data.bid_close,
            ask_open: ask_data.ask_open,
            ask_high: ask_data.ask_high,
            ask_low: ask_data.ask_low,
            ask_close: ask_data.ask_close,
        })
    }

    /// Builds a bar from one IBKR data point for `contract`.
    ///
    /// Stock/future/CFD/cash map to the stock table, options to the option
    /// table (identity taken from the contract), and forex to the bid/ask table.
    ///
    /// # Panics
    /// - forex with a `what_to_show` other than Bid/Ask;
    /// - an unknown asset type, or an option contract whose `right` cannot be
    ///   decoded.
    ///
    /// Volume that cannot be represented as a [`Decimal`] is stored as `-1`.
    pub fn from_data(
        contract: &Contract,
        what_to_show: &WhatToShow,
        time: DateTime<Utc>,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
    ) -> Self {
        match AssetType::from_str(&contract.security_type) {
            AssetType::Stock | AssetType::CFD | AssetType::Future => {
                HistoricalDataFullKeys::Stock(HistoricalStockDataFullKeys {
                    stock: get_local_symbol(contract),
                    primary_exchange: contract.primary_exchange.to_string(),
                    currency: contract.currency.to_string(),
                    time,
                    open,
                    high,
                    low,
                    close,
                    volume: Decimal::from_f64(volume).unwrap_or(dec!(-1.0)),
                })
            }
            AssetType::Option => HistoricalDataFullKeys::Options(HistoricalOptionsDataFullKeys {
                stock: get_local_symbol(contract),
                primary_exchange: contract.primary_exchange.to_string(),
                currency: contract.currency.to_string(),
                expiry: contract.last_trade_date_or_contract_month.clone(),
                strike: contract.strike,
                multiplier: contract.multiplier.clone(),
                option_type: OptionType::from_str(&contract.right)
                    .expect("Expected to be able to decode contract right to OptionType"),
                time,
                open,
                high,
                low,
                close,
                volume: Decimal::from_f64(volume).unwrap_or(dec!(-1.0)),
            }),
            AssetType::ForexPair => match what_to_show {
                WhatToShow::Bid => Self::Forex(HistoricalForexDataFullKeys {
                    pair: get_local_symbol(contract),
                    time: time,
                    bid_open: Some(open),
                    bid_high: Some(high),
                    bid_low: Some(low),
                    bid_close: Some(close),
                    ask_open: None,
                    ask_high: None,
                    ask_low: None,
                    ask_close: None,
                }),
                WhatToShow::Ask => Self::Forex(HistoricalForexDataFullKeys {
                    pair: get_local_symbol(contract),
                    time: time,
                    bid_open: None,
                    bid_high: None,
                    bid_low: None,
                    bid_close: None,
                    ask_open: Some(open),
                    ask_high: Some(high),
                    ask_low: Some(low),
                    ask_close: Some(close),
                }),
                _ => panic!("Tried to construct Forex from trades/midpoint"),
            },
            AssetType::Unknown => {
                panic!("Unknown asset type when trying to create from_data")
            }
            AssetType::CASH => {
                panic!("Should not have been able to get AssetType CASH from contract sec_type")
            }
        }
    }

    /// Converts an IBKR historical [`ibapi::market_data::historical::Bar`] into
    /// a storage bar, preserving its series (`Bid`, `Ask` or `Trades`).
    ///
    /// # Panics
    /// Panics on an unsupported historical series, or if the bar timestamp
    /// cannot be converted to UTC.
    pub fn from_contract_and_bar(
        contract: &Contract,
        what_to_show: &ibapi::market_data::historical::WhatToShow,
        bar: ibapi::market_data::historical::Bar,
    ) -> Self {
        Self::from_data(
            contract,
            match what_to_show {
                ibapi::market_data::historical::WhatToShow::Bid => {
                    &ibapi::market_data::realtime::WhatToShow::Bid
                }
                ibapi::market_data::historical::WhatToShow::Ask => {
                    &ibapi::market_data::realtime::WhatToShow::Ask
                }
                ibapi::market_data::historical::WhatToShow::Trades => {
                    &ibapi::market_data::realtime::WhatToShow::Trades
                }
                _ => panic!("Tried to get WhatToShow that is currently unsupported"),
            },
            DateTime::from_timestamp(bar.date.unix_timestamp(), bar.date.nanosecond() as u32)
                .expect("Expected to be able to convert bar time to DateTime<Utc>"),
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Time-qualified key for one bar; the variant selects the table.
pub enum HistoricalDataPrimaryKeys {
    /// A stock/future/CFD/cash bar.
    Stock(HistoricalStockDataPrimaryKeys),
    /// Legacy daily-bar key. Current code never produces it; retained for the
    /// historical enum surface.
    DailyStock(DailyHistoricalStockDataPrimaryKeys),
    /// An option bar (includes the option identity).
    Options(HistoricalOptionsDataPrimaryKeys),
    /// A forex bar.
    Forex(HistoricalForexDataPrimaryKeys),
}

impl HistoricalDataPrimaryKeys {
    /// Builds the key for `contract` at `time`.
    ///
    /// Stock/future/CFD/cash map to the stock table, options to the option
    /// table and forex pairs to the forex table.
    ///
    /// # Panics
    /// Panics for [`AssetType::Unknown`] or an option contract whose `right`
    /// cannot be decoded.
    pub fn from_contract(contract: &Contract, time: DateTime<Utc>) -> Self {
        match AssetType::from_str(&contract.security_type) {
            AssetType::CASH | AssetType::Stock | AssetType::CFD | AssetType::Future => {
                HistoricalDataPrimaryKeys::Stock(HistoricalStockDataPrimaryKeys {
                    stock: get_local_symbol(contract),
                    primary_exchange: contract.primary_exchange.to_string(),
                    currency: contract.currency.to_string(),
                    time: time,
                })
            }
            AssetType::Option => {
                HistoricalDataPrimaryKeys::Options(HistoricalOptionsDataPrimaryKeys {
                    stock: get_local_symbol(contract),
                    primary_exchange: contract.primary_exchange.to_string(),
                    currency: contract.currency.to_string(),

                    expiry: contract.last_trade_date_or_contract_month.clone(),
                    strike: contract.strike,
                    multiplier: contract.multiplier.clone(),
                    option_type: OptionType::from_str(&contract.right)
                        .expect("Expected option_type to be derivable from contract right"),

                    time: time,
                })
            }
            AssetType::ForexPair => {
                HistoricalDataPrimaryKeys::Forex(HistoricalForexDataPrimaryKeys {
                    pair: get_local_symbol(contract),
                    time: time,
                })
            }
            AssetType::Unknown => {
                panic!("Tried to construct HistoricalDataPrimaryKeys for unknown asset type")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Bar identity without the timestamp: the instrument columns of the table.
pub enum HistoricalDataPrimaryKeysWoTime {
    /// Stock/future/CFD/cash instrument identity.
    Stock(HistoricalStockDataPrimaryKeysWoTime),
    /// Option identity (underlying plus expiry/strike/multiplier/type).
    Options(HistoricalOptionsDataPrimaryKeysWoTime),
    /// Forex pair.
    Forex(HistoricalForexDataPrimaryKeysWoTime),
}

impl HistoricalDataPrimaryKeysWoTime {
    /// Builds the time-less key for `contract`.
    ///
    /// # Panics
    /// Same asset-class mapping and panic conditions as
    /// [`HistoricalDataPrimaryKeys::from_contract`].
    pub fn from_contract(contract: &Contract) -> Self {
        match AssetType::from_str(&contract.security_type) {
            AssetType::CASH | AssetType::Stock | AssetType::CFD | AssetType::Future => {
                Self::Stock(HistoricalStockDataPrimaryKeysWoTime {
                    stock: get_local_symbol(contract),
                    primary_exchange: contract.primary_exchange.to_string(),
                    currency: contract.currency.to_string(),
                })
            }
            AssetType::Option => Self::Options(HistoricalOptionsDataPrimaryKeysWoTime {
                stock: get_local_symbol(contract),
                primary_exchange: contract.primary_exchange.to_string(),
                currency: contract.currency.to_string(),

                expiry: contract.last_trade_date_or_contract_month.clone(),
                strike: contract.strike,
                multiplier: contract.multiplier.clone(),
                option_type: OptionType::from_str(&contract.right)
                    .expect("Expected option_type to be derivable from contract right"),
            }),
            AssetType::ForexPair => Self::Forex(HistoricalForexDataPrimaryKeysWoTime {
                pair: get_local_symbol(contract),
            }),
            AssetType::Unknown => {
                panic!("Tried to construct HistoricalDataPrimaryKeys for unknown asset type")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Partial update of a bar's OHLCV columns.
pub enum HistoricalDataUpdateKeys {
    /// Stock/future/CFD/cash OHLCV.
    Stock(HistoricalStockDataUpdateKeys),
    /// Option OHLCV.
    Options(HistoricalOptionsDataUpdateKeys),
    /// Forex bid/ask columns; only the selected side is set by [`Self::from_bar`].
    Forex(HistoricalForexDataUpdateKeys),
}

impl HistoricalDataUpdateKeys {
    /// Fills OHLCV from an already-built `bar` for `contract`'s asset class.
    ///
    /// Forex takes bid columns first (falling back to ask) and writes only the
    /// side named by `what_to_show`; volume is `None` for forex.
    ///
    /// # Panics
    /// - forex with a series other than Bid/Ask;
    /// - an unknown asset class.
    pub fn from_bar(
        contract: &Contract,
        what_to_show: &WhatToShow,
        bar: &HistoricalDataFullKeys,
    ) -> Self {
        // Step 1: Extract OHLCV fields by destructuring `bar`
        let (open, high, low, close, volume) = match bar {
            HistoricalDataFullKeys::Stock(s) => (s.open, s.high, s.low, s.close, Some(s.volume)),
            HistoricalDataFullKeys::Options(o) => (o.open, o.high, o.low, o.close, Some(o.volume)),
            HistoricalDataFullKeys::Forex(f) => {
                // If Forex holds bid/ask fields internally, handle fallback values appropriately:
                let open = f.bid_open.or(f.ask_open).unwrap_or_default();
                let high = f.bid_high.or(f.ask_high).unwrap_or_default();
                let low = f.bid_low.or(f.ask_low).unwrap_or_default();
                let close = f.bid_close.or(f.ask_close).unwrap_or_default();
                (open, high, low, close, None)
            }
        };

        // Step 2: Construct the update keys based on asset type
        match AssetType::from_str(&contract.security_type) {
            AssetType::CASH | AssetType::Stock | AssetType::CFD | AssetType::Future => {
                HistoricalDataUpdateKeys::Stock(HistoricalStockDataUpdateKeys {
                    open: Some(open),
                    high: Some(high),
                    low: Some(low),
                    close: Some(close),
                    volume: volume,
                })
            }
            AssetType::Option => {
                HistoricalDataUpdateKeys::Options(HistoricalOptionsDataUpdateKeys {
                    open: Some(open),
                    high: Some(high),
                    low: Some(low),
                    close: Some(close),
                    volume: volume,
                })
            }
            AssetType::ForexPair => match what_to_show {
                WhatToShow::Bid => HistoricalDataUpdateKeys::Forex(HistoricalForexDataUpdateKeys {
                    bid_open: Some(open),
                    bid_high: Some(high),
                    bid_low: Some(low),
                    bid_close: Some(close),
                    ask_open: None,
                    ask_high: None,
                    ask_low: None,
                    ask_close: None,
                }),
                WhatToShow::Ask => HistoricalDataUpdateKeys::Forex(HistoricalForexDataUpdateKeys {
                    bid_open: None,
                    bid_high: None,
                    bid_low: None,
                    bid_close: None,
                    ask_open: Some(open),
                    ask_high: Some(high),
                    ask_low: Some(low),
                    ask_close: Some(close),
                }),
                _ => panic!("Requested WhatToShow that is not Bid/Ask for Forex Contract"),
            },
            AssetType::Unknown => {
                panic!("Tried to construct HistoricalDataPrimaryKeys for unknown asset type")
            }
        }
    }

    /// Historical-series analogue of [`Self::from_bar`].
    ///
    /// # Panics
    /// Panics for an unsupported historical series.
    pub fn from_historical_bar(
        contract: &Contract,
        what_to_show: &ibapi::market_data::historical::WhatToShow,
        bar: &HistoricalDataFullKeys,
    ) -> Self {
        let what_to_show = match what_to_show {
            ibapi::market_data::historical::WhatToShow::Bid => {
                &ibapi::market_data::realtime::WhatToShow::Bid
            }
            ibapi::market_data::historical::WhatToShow::Ask => {
                &ibapi::market_data::realtime::WhatToShow::Ask
            }
            ibapi::market_data::historical::WhatToShow::Trades => {
                &ibapi::market_data::realtime::WhatToShow::Trades
            }
            _ => panic!("Tried to get WhatToShow that is currently unsupported"),
        };
        Self::from_bar(contract, what_to_show, bar)
    }
}

#[hotpath::measure_all]
impl<'a> HistoricalDataCRUD<'a> {
    /// Borrows the executor of the selected table.
    pub fn exec(&mut self) -> &mut Exec<'a> {
        match self {
            Self::Stock(stk) => &mut stk.crud.exec,
            Self::Forex(fx) => &mut fx.crud.exec,
            Self::Options(opt) => &mut opt.crud.exec,
        }
    }

    /// Handle for the stock/future/CFD/cash table.
    pub fn stock(exec: Exec<'a>) -> Self {
        Self::Stock(HistoricalStockDataCRUD::new(exec))
    }

    /// Handle for the options table.
    pub fn option(exec: Exec<'a>) -> Self {
        Self::Options(HistoricalOptionsDataCRUD::new(exec))
    }

    /// Handle for the forex table.
    pub fn forex(exec: Exec<'a>) -> Self {
        Self::Forex(HistoricalForexDataCRUD::new(exec))
    }

    /// Selects the table for `asset_type`.
    ///
    /// Stock, future, CFD and cash use the stock table; forex pairs the forex
    /// table; options the option table.
    ///
    /// # Panics
    /// Panics for [`AssetType::Unknown`].
    pub fn from_exec(asset_type: &AssetType, exec: Exec<'a>) -> Self {
        match asset_type {
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::CASH => {
                Self::stock(exec)
            }
            AssetType::ForexPair => Self::forex(exec),
            AssetType::Option => Self::option(exec),
            AssetType::Unknown => panic!("Tried to get CRUD instance from an Unknown Asset Type!"),
        }
    }
}

implement_crud_trait_for_interface!(
    HistoricalDataCRUD,
    HistoricalDataFullKeys,
    HistoricalDataPrimaryKeys,
    HistoricalDataUpdateKeys,
    [Stock, Options, Forex]
);

/// Which stored column the VWAP read weights by volume.
pub enum VwapBarValue {
    /// `close`.
    Close,
    /// `open`.
    Open,
    /// `bid_open`.
    BidOpen,
    /// `bid_close`.
    BidClose,
    /// `ask_open`.
    AskOpen,
    /// `ask_close`.
    AskClose,
}

impl VwapBarValue {
    /// SQL column name for this choice.
    pub fn as_str(&self) -> String {
        match self {
            Self::Open => "open".to_string(),
            Self::Close => "close".to_string(),
            Self::BidOpen => "bid_open".to_string(),
            Self::BidClose => "bid_close".to_string(),
            Self::AskOpen => "ask_open".to_string(),
            Self::AskClose => "ask_close".to_string(),
        }
    }
}

/// Read-side analytics over a historical table.
#[async_trait]
pub trait HistoricalDataOps {
    /// Reads up to `limit` time buckets of `timestep_minutes` ending at `now`
    /// (UTC; `Utc::now()` outside backtests).
    ///
    /// Buckets are built in `America/New_York` with `first(open)`,
    /// `max(high)`, `min(low)`, `last(close)` and `sum(volume)`. SQL returns
    /// newest-first; the result is reversed to oldest-first. The newest bucket
    /// must be complete (at least `timestep_minutes / 5` five-minute stock bars,
    /// or `timestep_minutes` forex bars); older buckets must reach 93% of that.
    /// Complete buckets go to [`AggregatedBars::full`], the rest to
    /// [`AggregatedBars::incomplete`].
    ///
    /// Under `backtest`, an in-memory bar cache may answer without a DB read.
    ///
    /// # Errors
    /// Returns the SQL error text prefixed with the table kind.
    ///
    /// # Example
    ///
    /// Three 5-minute AAPL bars (closes 100/101/102) read as two buckets,
    /// oldest first.
    ///
    /// ```rust
    /// use chrono::{Duration, TimeZone, Utc};
    /// use rust_decimal::Decimal;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys,
    ///     HistoricalStockDataPrimaryKeysWoTime,
    /// };
    /// use trading_app::database::models_crud::historical_data::historical_data::{
    ///     HistoricalDataOps, HistoricalDataPrimaryKeysWoTime,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// // Near ET midnight the "today" filter can drop the oldest bar.
    /// # if Utc::now().with_timezone(&chrono_tz::America::New_York).time()
    /// #     < chrono::NaiveTime::from_hms_opt(0, 20, 0).unwrap() { return Ok(()); }
    /// let stock = format!("HDN{}", std::process::id());
    /// let now = chrono::DateTime::<Utc>::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// for (minutes, close) in [(15, 100.0), (10, 101.0), (5, 102.0)] {
    ///     db.historical_stock_data()
    ///         .create(&HistoricalStockDataFullKeys {
    ///             stock: stock.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             time: now - Duration::minutes(minutes),
    ///             open: close,
    ///             high: close,
    ///             low: close,
    ///             close,
    ///             volume: Decimal::new(100, 0),
    ///         })
    ///         .await?;
    /// }
    ///
    /// let pk = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
    ///     stock: stock.clone(),
    ///     primary_exchange: "NASDAQ".into(),
    ///     currency: "USD".into(),
    /// });
    /// let bars = db.historical_data(&AssetType::Stock).read_last_n(pk, 5, 2).await?;
    /// assert_eq!(
    ///     bars.full.iter().map(|row| row.get_price()).collect::<Vec<_>>(),
    ///     vec![101.0, 102.0] // oldest first
    /// );
    ///
    /// for minutes in [15, 10, 5] {
    ///     db.historical_stock_data()
    ///         .delete(&HistoricalStockDataPrimaryKeys {
    ///             stock: stock.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             time: now - Duration::minutes(minutes),
    ///         })
    ///         .await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    async fn read_last_n(
        &mut self,
        pk: HistoricalDataPrimaryKeysWoTime,
        timestep_minutes: u32,
        limit: u32,
        #[cfg(feature = "backtest")] now: Option<DateTime<Utc>>,
    ) -> Result<AggregatedBars, String>;
    /// Returns the most recent complete bucket; falls back to a warned
    /// incomplete bucket when that is all there is.
    ///
    /// # Errors
    /// Errors when both lists are empty, or when both are non-empty (the
    /// completeness rule should have made that impossible).
    ///
    /// # Example
    ///
    /// The newest complete 5-minute bucket of three AAPL bars is the
    /// five-minute-old one (`close == 102`).
    ///
    /// ```rust
    /// use chrono::{Duration, TimeZone, Utc};
    /// use rust_decimal::Decimal;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys,
    ///     HistoricalStockDataPrimaryKeysWoTime,
    /// };
    /// use trading_app::database::models_crud::historical_data::historical_data::{
    ///     HistoricalDataOps, HistoricalDataPrimaryKeysWoTime,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// # if Utc::now().with_timezone(&chrono_tz::America::New_York).time()
    /// #     < chrono::NaiveTime::from_hms_opt(0, 20, 0).unwrap() { return Ok(()); }
    /// let stock = format!("HDB{}", std::process::id());
    /// let now = chrono::DateTime::<Utc>::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// for (minutes, close) in [(15, 100.0), (10, 101.0), (5, 102.0)] {
    ///     db.historical_stock_data()
    ///         .create(&HistoricalStockDataFullKeys {
    ///             stock: stock.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             time: now - Duration::minutes(minutes),
    ///             open: close,
    ///             high: close,
    ///             low: close,
    ///             close,
    ///             volume: Decimal::new(100, 0),
    ///         })
    ///         .await?;
    /// }
    ///
    /// let pk = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
    ///     stock: stock.clone(),
    ///     primary_exchange: "NASDAQ".into(),
    ///     currency: "USD".into(),
    /// });
    /// let last = db.historical_data(&AssetType::Stock).read_last_bar(pk, 5).await?;
    /// assert_eq!(last.get_price(), 102.0);
    ///
    /// for minutes in [15, 10, 5] {
    ///     db.historical_stock_data()
    ///         .delete(&HistoricalStockDataPrimaryKeys {
    ///             stock: stock.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             time: now - Duration::minutes(minutes),
    ///         })
    ///         .await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    async fn read_last_bar(
        &mut self,
        pk: HistoricalDataPrimaryKeysWoTime,
        timestep_minutes: u32,
        #[cfg(feature = "backtest")] now: DateTime<Utc>,
    ) -> Result<HistoricalDataFullKeys, String>;
    /// Volume-weighted average of `vwap_bar_value` over bars since the start of
    /// the current civil day in `timezone` (default `US/Eastern`).
    ///
    /// Returns `None` when no bars exist for today. Backtests bind `now`
    /// instead of SQL `now()`.
    ///
    /// # Errors
    /// Returns the SQL error text when the aggregate query fails.
    ///
    /// # Example
    ///
    /// Two bars today: `100` volume at `100`, `300` volume at `102` →
    /// `(100*100 + 300*102) / 400 = 101.5`.
    ///
    /// ```rust
    /// use chrono::{Duration, TimeZone, Utc};
    /// use rust_decimal::Decimal;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys,
    ///     HistoricalStockDataPrimaryKeysWoTime,
    /// };
    /// use trading_app::database::models_crud::historical_data::historical_data::{
    ///     HistoricalDataOps, HistoricalDataPrimaryKeysWoTime, VwapBarValue,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// # if Utc::now().with_timezone(&chrono_tz::America::New_York).time()
    /// #     < chrono::NaiveTime::from_hms_opt(0, 30, 0).unwrap() { return Ok(()); }
    /// let stock = format!("HDV{}", std::process::id());
    /// let now = chrono::DateTime::<Utc>::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// for (minutes, close, volume) in [(20, 100.0, 100), (10, 102.0, 300)] {
    ///     db.historical_stock_data()
    ///         .create(&HistoricalStockDataFullKeys {
    ///             stock: stock.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             time: now - Duration::minutes(minutes),
    ///             open: close,
    ///             high: close,
    ///             low: close,
    ///             close,
    ///             volume: Decimal::new(volume, 0),
    ///         })
    ///         .await?;
    /// }
    ///
    /// let pk = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
    ///     stock: stock.clone(),
    ///     primary_exchange: "NASDAQ".into(),
    ///     currency: "USD".into(),
    /// });
    /// let vwap = db
    ///     .historical_data(&AssetType::Stock)
    ///     .read_last_vwap(pk, None, VwapBarValue::Close)
    ///     .await
    ///     .expect("vwap query");
    /// assert_eq!(vwap, Some(101.5));
    ///
    /// for minutes in [20, 10] {
    ///     db.historical_stock_data()
    ///         .delete(&HistoricalStockDataPrimaryKeys {
    ///             stock: stock.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             time: now - Duration::minutes(minutes),
    ///         })
    ///         .await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    async fn read_last_vwap(
        &mut self,
        pk: HistoricalDataPrimaryKeysWoTime,
        timezone: Option<String>,
        vwap_bar_value: VwapBarValue,
        #[cfg(feature = "backtest")] now: DateTime<Utc>,
    ) -> Result<Option<f64>, String>;
    /// Counts up to `n` rows at or after `datetime` (inclusive).
    ///
    /// The result is in `0..=n`: callers use it to decide whether enough
    /// history exists before a read.
    ///
    /// # Errors
    /// Returns the SQL error text when the count query fails.
    ///
    /// # Example
    ///
    /// Bars 20, 10 and 5 minutes old; counting at or after `now - 10m`
    /// includes the 10- and 5-minute-old rows (2), not the 20-minute-old one.
    ///
    /// ```rust
    /// use chrono::{Duration, TimeZone, Utc};
    /// use rust_decimal::Decimal;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys,
    ///     HistoricalStockDataPrimaryKeysWoTime,
    /// };
    /// use trading_app::database::models_crud::historical_data::historical_data::{
    ///     HistoricalDataOps, HistoricalDataPrimaryKeysWoTime,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let stock = format!("HDR{}", std::process::id());
    /// let now = chrono::DateTime::<Utc>::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// for minutes in [20, 10, 5] {
    ///     db.historical_stock_data()
    ///         .create(&HistoricalStockDataFullKeys {
    ///             stock: stock.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             time: now - Duration::minutes(minutes),
    ///             open: 100.0,
    ///             high: 100.0,
    ///             low: 100.0,
    ///             close: 100.0,
    ///             volume: Decimal::new(100, 0),
    ///         })
    ///         .await?;
    /// }
    ///
    /// let pk = HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
    ///     stock: stock.clone(),
    ///     primary_exchange: "NASDAQ".into(),
    ///     currency: "USD".into(),
    /// });
    /// let since = (now - Duration::minutes(10)).with_timezone(&chrono_tz::America::New_York);
    /// let rows = db
    ///     .historical_data(&AssetType::Stock)
    ///     .get_rows_since(pk, 10, &since)
    ///     .await
    ///     .expect("row count");
    /// assert_eq!(rows, 2);
    ///
    /// for minutes in [20, 10, 5] {
    ///     db.historical_stock_data()
    ///         .delete(&HistoricalStockDataPrimaryKeys {
    ///             stock: stock.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             time: now - Duration::minutes(minutes),
    ///         })
    ///         .await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    async fn get_rows_since(
        &mut self,
        pk: HistoricalDataPrimaryKeysWoTime,
        n: u64,
        datetime: &DateTime<Tz>,
    ) -> Result<i64, String>;
}

/// Average absolute move since the open, used by noise/volatility sizing.
#[async_trait]
pub trait NoiseOps {
    /// Mean over the last `avg_move_lookback` sessions of
    /// `abs(close/open - 1)` at the latest session's time of day.
    ///
    /// Today's session is excluded.
    ///
    /// # Panics
    /// Panics if the query returns no rows or a row without a computed move.
    ///
    /// # Example
    ///
    /// Three previous sessions at `10:00` ET: closes `110`, `90`, `105` over
    /// opens of `100` → mean absolute move `(0.10 + 0.10 + 0.05) / 3`.
    ///
    /// ```rust
    /// use chrono::{Duration, TimeZone, Utc};
    /// use rust_decimal::Decimal;
    /// use trading_app::database::crud::CRUDTrait;
    /// use trading_app::database::db::DbDirect;
    /// use trading_app::database::models::{
    ///     AssetType, HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys,
    ///     HistoricalStockDataPrimaryKeysWoTime,
    /// };
    /// use trading_app::database::models_crud::historical_data::historical_data::{
    ///     HistoricalDataOps, NoiseOps,
    /// };
    /// use trading_app::database::tables::DbTables;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let Ok(url) = std::env::var("TEST_DATABASE_URL") else { return Ok(()); };
    /// # let pool = sqlx::PgPool::connect(&url).await?;
    /// let stock = format!("HDA{}", std::process::id());
    /// let tz = chrono_tz::America::New_York;
    /// let today = Utc::now().with_timezone(&tz).date_naive();
    /// let bar_time = |days_ago: i64| {
    ///     tz.from_local_datetime(&(today - Duration::days(days_ago)).and_hms_opt(10, 0, 0).unwrap())
    ///         .unwrap()
    ///         .with_timezone(&Utc)
    /// };
    /// let mut db = DbDirect::connect(&pool);
    ///
    /// // Today's 10:00 row defines the time-of-day; its date is excluded.
    /// let mut times = vec![bar_time(0)];
    /// for (days_ago, close) in [(3, 110.0), (2, 90.0), (1, 105.0)] {
    ///     let time = bar_time(days_ago);
    ///     times.push(time);
    ///     db.historical_stock_data()
    ///         .create(&HistoricalStockDataFullKeys {
    ///             stock: stock.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             time,
    ///             open: 100.0,
    ///             high: close,
    ///             low: close,
    ///             close,
    ///             volume: Decimal::new(100, 0),
    ///         })
    ///         .await?;
    /// }
    /// db.historical_stock_data()
    ///     .create(&HistoricalStockDataFullKeys {
    ///         stock: stock.clone(),
    ///         primary_exchange: "NASDAQ".into(),
    ///         currency: "USD".into(),
    ///         time: bar_time(0),
    ///         open: 100.0,
    ///         high: 100.0,
    ///         low: 100.0,
    ///         close: 100.0,
    ///         volume: Decimal::new(100, 0),
    ///     })
    ///     .await?;
    ///
    /// let pk = HistoricalStockDataPrimaryKeysWoTime {
    ///     stock: stock.clone(),
    ///     primary_exchange: "NASDAQ".into(),
    ///     currency: "USD".into(),
    /// };
    /// let avg = db
    ///     .historical_data(&AssetType::Stock)
    ///     .get_avg_move_since_open(pk, 3)
    ///     .await
    ///     .expect("average move");
    /// assert!((avg - 0.25 / 3.0).abs() < 1e-9);
    ///
    /// for time in times {
    ///     db.historical_stock_data()
    ///         .delete(&HistoricalStockDataPrimaryKeys {
    ///             stock: stock.clone(),
    ///             primary_exchange: "NASDAQ".into(),
    ///             currency: "USD".into(),
    ///             time,
    ///         })
    ///         .await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(not(feature = "backtest"))]
    async fn get_avg_move_since_open(
        &mut self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
        avg_move_lookback: i64,
    ) -> Result<f64, String>;
    /// Backtest variant: the latest bar is restricted to `time <= now`, so the
    /// calculation cannot look ahead.
    ///
    /// # Panics
    /// Same as the production variant.
    #[cfg(feature = "backtest")]
    async fn get_avg_move_since_open(
        &mut self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
        now: DateTime<Utc>,
        avg_move_lookback: i64,
    ) -> Result<f64, String>;
}

#[derive(Debug, Clone, PartialEq)]
/// Result of [`HistoricalDataOps::read_last_n`], oldest bar first within each
/// list.
pub struct AggregatedBars {
    /// Buckets that met the completeness threshold.
    pub full: Vec<HistoricalDataFullKeys>,
    /// Buckets below the threshold (the newest bucket is the usual reason).
    pub incomplete: Vec<HistoricalDataFullKeys>,
}

#[async_trait]
#[hotpath::measure_all]
impl HistoricalDataOps for HistoricalDataCRUD<'_> {
    async fn read_last_n(
        &mut self,
        pk: HistoricalDataPrimaryKeysWoTime,
        timestep_minutes: u32,
        limit: u32,
        #[cfg(feature = "backtest")] now: Option<DateTime<Utc>>,
    ) -> Result<AggregatedBars, String> {
        #[cfg(feature = "backtest")]
        {
            if let Some(cache) = crate::backtester::methods::in_memory::bar_cache::current() {
                if let Some(bars) = cache.get_bars_before(&pk, now, limit as usize) {
                    return Ok(AggregatedBars {
                        full: bars,
                        incomplete: vec![],
                    });
                }
            }
        }

        #[cfg(feature = "backtest")]
        let now = now.unwrap_or(Utc::now());
        #[cfg(not(feature = "backtest"))]
        let now = Utc::now();
        let mut full = Vec::new();
        let mut incomplete = Vec::new();

        match pk {
            HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
                stock,
                primary_exchange,
                currency,
            }) => {
                let rows = sqlx::query!(
                    r#"
                    SELECT
                        time_bucket(make_interval(mins => $4), time, timezone:= 'America/New_York') AS bucket,
                        stock,
                        primary_exchange,
                        currency,
                        first(open, time)       AS "open!",
                        max(high)               AS "high!",
                        min(low)                AS "low!",
                        last(close, time)       AS "close!",
                        sum(volume)             AS "volume!",
                        count(*)                AS bar_count
                    FROM market_data.historical_data
                    WHERE stock = $1
                      AND primary_exchange = $2
                      AND currency = $3
                      AND time <= $6
                    GROUP BY bucket, stock, primary_exchange, currency
                    ORDER BY bucket DESC
                    LIMIT $5
                    "#,
                    stock,
                    primary_exchange,
                    currency,
                    timestep_minutes as i32,
                    limit as i32,
                    now
                )
                .fetch_all(self.exec())
                .await
                .map_err(|e| format!("Error reading stock bars in read_last_n: {}", e))?;

                for (idx, row) in rows.iter().enumerate() {
                    let bar = HistoricalDataFullKeys::Stock(HistoricalStockDataFullKeys {
                        time: row.bucket.unwrap(),
                        stock: row.stock.clone(),
                        primary_exchange: row.primary_exchange.clone(),
                        currency: row.currency.clone(),
                        open: row.open,
                        high: row.high,
                        low: row.low,
                        close: row.close,
                        volume: row.volume,
                    });

                    let bar_count = row.bar_count.unwrap() as f64;
                    if (idx == 0 && bar_count >= timestep_minutes as f64 / 5.0) // MUST HAVE most recent time bar
                || (idx >= 1 && bar_count >= (timestep_minutes as f64 * 0.93 / 5.0))
                    {
                        full.push(bar);
                    } else {
                        incomplete.push(bar);
                    }
                }
            }

            HistoricalDataPrimaryKeysWoTime::Forex(HistoricalForexDataPrimaryKeysWoTime {
                pair,
            }) => {
                let rows = sqlx::query!(
                    r#"
                    SELECT
                        time_bucket(make_interval(mins => $2), time, timezone:= 'America/New_York') AS bucket,
                        pair,
                        first(bid_open, time) AS bid_open,
                        max(bid_high)         AS bid_high,
                        min(bid_low)          AS bid_low,
                        last(bid_close, time) AS bid_close,
                        first(ask_open, time) AS ask_open,
                        max(ask_high)         AS ask_high,
                        min(ask_low)          AS ask_low,
                        last(ask_close, time) AS ask_close,
                        count(*)              AS bar_count
                    FROM market_data.historical_forex_data
                    WHERE pair = $1
                      AND bid_open IS NOT NULL
                      AND ask_open IS NOT NULL
                      AND time <= $4
                    GROUP BY bucket, pair
                    ORDER BY bucket DESC
                    LIMIT $3;
                    "#,
                    pair,
                    timestep_minutes as i32,
                    limit as i32,
                    now
                )
                .fetch_all(self.exec())
                .await
                .map_err(|e| format!("Error reading forex bars in read_last_n: {}", e))?;

                for (idx, row) in rows.iter().enumerate() {
                    let bar = HistoricalDataFullKeys::Forex(HistoricalForexDataFullKeys {
                        time: row.bucket.unwrap(),
                        pair: row.pair.clone(),
                        bid_open: row.bid_open,
                        bid_high: row.bid_high,
                        bid_low: row.bid_low,
                        bid_close: row.bid_close,
                        ask_open: row.ask_open,
                        ask_high: row.ask_high,
                        ask_low: row.ask_low,
                        ask_close: row.ask_close,
                    });

                    let bar_count = row.bar_count.unwrap() as f64;
                    if (idx == 0 && bar_count >= timestep_minutes as f64) // MUST HAVE most recent time bar
                || (idx >= 1 && bar_count >= (timestep_minutes as f64 * 0.93))
                    {
                        full.push(bar);
                    } else {
                        incomplete.push(bar);
                    }
                }
            }

            HistoricalDataPrimaryKeysWoTime::Options(HistoricalOptionsDataPrimaryKeysWoTime {
                stock,
                primary_exchange,
                currency,
                expiry,
                strike,
                multiplier,
                option_type,
            }) => {
                let rows = sqlx::query!(
                    r#"
                    SELECT
                        time_bucket(make_interval(mins => $8), time, timezone:= 'America/New_York') AS bucket,
                        stock,
                        primary_exchange,
                        currency,
                        expiry,
                        multiplier,
                        strike,
                        option_type             AS "option_type!:OptionType",
                        first(open, time)       AS "open!",
                        max(high)               AS "high!",
                        min(low)                AS "low!",
                        last(close, time)       AS "close!",
                        sum(volume)             AS "volume!",
                        count(*)                AS bar_count
                    FROM market_data.historical_options_data
                    WHERE stock = $1
                      AND primary_exchange = $2
                      AND currency = $3
                      AND expiry = $4
                      AND multiplier = $5
                      AND strike = $6
                      AND option_type = $7::option_type
                      AND time <= $10
                    GROUP BY
                        bucket,
                        stock,
                        primary_exchange,
                        currency,
                        expiry,
                        multiplier,
                        strike,
                        option_type
                    ORDER BY bucket DESC
                    LIMIT $9;
                    "#,
                    stock,
                    primary_exchange,
                    currency,
                    expiry,
                    multiplier,
                    strike,
                    option_type as OptionType,
                    timestep_minutes as i32,
                    limit as i32,
                    now
                )
                .fetch_all(self.exec())
                .await
                .map_err(|e| format!("Error reading option bars in read_last_n: {}", e))?;

                for (idx, row) in rows.iter().enumerate() {
                    let bar = HistoricalDataFullKeys::Options(HistoricalOptionsDataFullKeys {
                        time: row.bucket.unwrap(),
                        stock: row.stock.clone(),
                        primary_exchange: row.primary_exchange.clone(),
                        currency: row.currency.clone(),
                        expiry: row.expiry.clone(),
                        strike: row.strike,
                        multiplier: row.multiplier.clone(),
                        option_type: row.option_type.clone(),
                        open: row.open,
                        high: row.high,
                        low: row.low,
                        close: row.close,
                        volume: row.volume,
                    });

                    let bar_count = row.bar_count.unwrap() as f64;
                    if (idx == 0 && bar_count >= timestep_minutes as f64) // MUST HAVE most recent time bar
                || (idx >= 1 && bar_count >= (timestep_minutes as f64 * 0.93))
                    {
                        full.push(bar);
                    } else {
                        incomplete.push(bar);
                    }
                }
            }
        };

        full.reverse(); // chronological
        incomplete.reverse();

        Ok(AggregatedBars { full, incomplete })
    }

    async fn read_last_bar(
        &mut self,
        pk: HistoricalDataPrimaryKeysWoTime,
        timestep_minutes: u32,
        #[cfg(feature = "backtest")] now: DateTime<Utc>,
    ) -> Result<HistoricalDataFullKeys, String> {
        let mut agg_bars = self
            .read_last_n(
                pk,
                timestep_minutes,
                1,
                #[cfg(feature = "backtest")]
                Some(now),
            )
            .await
            .map_err(|e| format!("Failed to read last bar: {e:?}"))?;
        match (agg_bars.full.is_empty(), agg_bars.incomplete.is_empty()) {
            (false, true) => Ok(agg_bars.full.pop().unwrap()),
            (true, false) => {
                tracing::warn!("read_last_bar returning an incomplete bar");
                Ok(agg_bars.incomplete.pop().unwrap())
            }
            (true, true) => Err("Both AggBars empty from read_last_bar".to_string()),
            (false, false) => Err("Both Bars have data from read_last_bar!".to_string()),
        }
    }

    /// Returns the latest VWAP value from the stored bars in the DB
    /// - Uses 'close' value of the bars stored
    /// - Use 'timezone' to determine which bars to include for 'today'
    /// - For ForexBars ->
    async fn read_last_vwap(
        &mut self,
        pk: HistoricalDataPrimaryKeysWoTime,
        timezone: Option<String>,
        vwap_bar_value: VwapBarValue,
        #[cfg(feature = "backtest")] now: DateTime<Utc>,
    ) -> Result<Option<f64>, String> {
        /// Row shape of the VWAP aggregate.
        #[derive(FromRow)]
        struct Vwap {
            /// `SUM(value * volume) / NULLIF(SUM(volume), 0)`.
            vwap: f64,
        }

        let vwap_opt: Option<Vwap> = match pk {
            HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
                stock,
                primary_exchange,
                currency,
            }) => {
                let now_sql = if cfg!(feature = "backtest") {
                    "$4"
                } else {
                    "now()"
                };
                let sql_str = format!(
                    r#"
                        SELECT
                            SUM({} * volume) / NULLIF(SUM(volume), 0) AS vwap
                        FROM market_data.historical_data
                        WHERE stock = $1
                          AND primary_exchange = $2
                          AND currency = $3
                          AND time >= (({now_sql}) AT TIME ZONE '{}')::date
                        GROUP BY stock;
                        "#,
                    vwap_bar_value.as_str(),
                    timezone.unwrap_or("US/Eastern".to_string())
                );
                let q = sqlx::query_as(&sql_str)
                    .bind(stock)
                    .bind(primary_exchange)
                    .bind(currency);
                #[cfg(feature = "backtest")]
                let q = q.bind(now);
                q.fetch_optional(self.exec()).await.map_err(|e| {
                    format!(
                        "Error when fetching most recent bar from HistoricalData \
                                    for in read_vwap: {e:?}",
                    )
                })?
            }

            HistoricalDataPrimaryKeysWoTime::Forex(HistoricalForexDataPrimaryKeysWoTime {
                pair,
            }) => {
                let now_sql = if cfg!(feature = "backtest") {
                    "$2"
                } else {
                    "now()"
                };
                let sql_query_str = format!(
                    r#"
                        SELECT
                            SUM({} * volume) / NULLIF(SUM(volume), 0) AS vwap
                        FROM market_data.historical_forex_data
                        WHERE pair = $1
                          -- Convert now() to Eastern, truncate to the day,
                          -- then cast back to timestamptz
                          AND time >= ({now_sql} AT TIME ZONE '{}')::date
                        GROUP BY stock;
                        "#,
                    vwap_bar_value.as_str(),
                    timezone.unwrap_or("US/Eastern".to_string())
                );
                let query = sqlx::query_as(&sql_query_str).bind(pair);
                #[cfg(feature = "backtest")]
                let query = query.bind(now);
                query.fetch_optional(self.exec()).await.map_err(|e| {
                    format!(
                        "Error when fetching most recent bar from HistoricalData \
                            for in read_vwap: {e:?}",
                    )
                })?
            }

            HistoricalDataPrimaryKeysWoTime::Options(HistoricalOptionsDataPrimaryKeysWoTime {
                stock,
                primary_exchange,
                currency,
                expiry,
                strike,
                multiplier,
                option_type,
            }) => {
                let now_sql = if cfg!(feature = "backtest") {
                    "$8"
                } else {
                    "now()"
                };
                let sql_query_str = format!(
                    r#"
                        SELECT
                            SUM({} * volume) / NULLIF(SUM(volume), 0) AS vwap
                        FROM market_data.historical_options_data
                        WHERE stock = $1
                            AND primary_exchange = $2
                            AND currency = $3
                            AND expiry = $4
                            AND strike = $5
                            AND multiplier = $6
                            AND option_type = $7
                          -- Convert now() to Eastern, truncate to the day,
                          -- then cast back to timestamptz
                          AND time >= ({now_sql} AT TIME ZONE '{}')::date
                        GROUP BY stock;
                        "#,
                    vwap_bar_value.as_str(),
                    timezone.unwrap_or("US/Eastern".to_string())
                );
                let query = sqlx::query_as(&sql_query_str)
                    .bind(stock)
                    .bind(primary_exchange)
                    .bind(currency)
                    .bind(expiry)
                    .bind(strike)
                    .bind(multiplier)
                    .bind(option_type);
                #[cfg(feature = "backtest")]
                let query = query.bind(now);

                query.fetch_optional(self.exec()).await.map_err(|e| {
                    format!(
                        "Error when fetching most recent bar from HistoricalData\
                            for in read_vwap: {e:?}",
                    )
                })?
            }
        };

        Ok(vwap_opt.map(|v| v.vwap))
    }

    async fn get_rows_since(
        &mut self,
        pk: HistoricalDataPrimaryKeysWoTime,
        n: u64,
        datetime: &DateTime<Tz>,
    ) -> Result<i64, String> {
        let enough_rows = match pk {
            HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
                stock,
                primary_exchange,
                currency,
            }) => {
                sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*)
                    FROM (
                        SELECT 1
                        FROM market_data.historical_data
                        WHERE stock = $2 AND primary_exchange = $3 AND currency = $4 AND time >= $5
                        LIMIT $1
                    ) sub;
                    "#,
                    n as i32,
                    stock,
                    primary_exchange,
                    currency,
                    datetime
                )
                .fetch_one(self.exec())
                .await
            }
            HistoricalDataPrimaryKeysWoTime::Options(HistoricalOptionsDataPrimaryKeysWoTime {
                stock,
                primary_exchange,
                currency,
                expiry,
                strike,
                multiplier,
                option_type,
            }) => {
                sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*)
                    FROM (
                        SELECT 1
                        FROM market_data.historical_options_data
                        WHERE stock = $2
                            AND primary_exchange = $3
                            AND currency = $4
                            AND expiry = $5
                            AND strike = $6
                            AND multiplier = $7
                            AND option_type = $8
                            AND time >= $9
                        LIMIT $1
                    ) sub;
                    "#,
                    n as i32,
                    stock,
                    primary_exchange,
                    currency,
                    expiry,
                    strike,
                    multiplier,
                    option_type as OptionType,
                    datetime
                )
                .fetch_one(self.exec())
                .await
            }
            HistoricalDataPrimaryKeysWoTime::Forex(HistoricalForexDataPrimaryKeysWoTime {
                pair,
            }) => {
                sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*)
                    FROM (
                        SELECT 1
                        FROM market_data.historical_forex_data
                        WHERE pair = $2
                            AND bid_open IS NOT NULL
                            AND ask_open IS NOT NULL
                            AND time >= $3
                        LIMIT $1
                    ) sub;
                    "#,
                    n as i32,
                    pair,
                    datetime
                )
                .fetch_one(self.exec())
                .await
            }
        };
        match enough_rows {
            Ok(n_rows) => {
                Ok(n_rows
                    .expect("Expected sql query to return a boolean at least in get_rows_since"))
            }
            Err(e) => Err(format!(
                "Error when fetching most recent rows from HistoricalData \
                in get_rows_since: {}",
                e
            )),
        }
    }
}

#[async_trait]
#[hotpath::measure_all]
impl NoiseOps for HistoricalDataCRUD<'_> {
    /// Today's move will NOT be included in the calculation
    /// avg move is of the last 15 days
    #[cfg(not(feature = "backtest"))]
    async fn get_avg_move_since_open(
        &mut self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
        avg_move_lookback: i64,
    ) -> Result<f64, String> {
        match sqlx::query_scalar!(
            r#"
            WITH latest AS (
                SELECT time AT TIME ZONE 'America/New_York' AS ny
                FROM market_data.historical_data
                WHERE stock = $1 AND primary_exchange = $2 AND currency = $3
                ORDER BY time DESC
                LIMIT 1
            )
            SELECT m.close / o.open AS movement_since_open
            FROM (
                SELECT h.time, h.close,
                       (h.time AT TIME ZONE 'America/New_York')::date AS day
                FROM market_data.historical_data h, latest l
                WHERE h.stock = $1 AND h.primary_exchange = $2 AND h.currency = $3
                  AND (h.time AT TIME ZONE 'America/New_York')::time = l.ny::time
                  AND (h.time AT TIME ZONE 'America/New_York')::date < l.ny::date
                ORDER BY h.time DESC
                LIMIT $4
            ) m
            CROSS JOIN LATERAL (
                SELECT open
                FROM market_data.historical_data
                WHERE stock = $1 AND primary_exchange = $2 AND currency = $3
                  AND time >= m.day::timestamp AT TIME ZONE 'America/New_York'
                  AND time <  (m.day + 1)::timestamp AT TIME ZONE 'America/New_York'
                ORDER BY time
                LIMIT 1
            ) o
            ORDER BY m.time DESC
            "#,
            pk.stock,
            pk.primary_exchange,
            pk.currency,
            avg_move_lookback
        )
        .fetch_all(self.exec())
        .await
        {
            Ok(moves_since_open) => {
                let abs_move_since_open = moves_since_open.iter().map(|move_since_open| {
                    (move_since_open
                        .expect("Expected avg_move_since_open to return at least 1 entry")
                        - 1.0)
                        .abs()
                });
                Ok(abs_move_since_open.sum::<f64>() / moves_since_open.len() as f64)
            }
            Err(e) => Err(format!(
                "Error when fetching most recent rows from \
                HistoricalData in read_last_n_of_stock: {}",
                e
            )),
        }
    }

    /// Backtest variant: the `latest` CTE filters `time <= now` (the bar time)
    /// to prevent look-ahead bias. Under prod, `get_avg_move_since_open`
    /// (above) uses the real wall-clock latest.
    #[cfg(feature = "backtest")]
    async fn get_avg_move_since_open(
        &mut self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
        now: DateTime<Utc>,
        avg_move_lookback: i64,
    ) -> Result<f64, String> {
        match sqlx::query_scalar!(
            r#"
            WITH latest AS (
                SELECT time AT TIME ZONE 'America/New_York' AS ny
                FROM market_data.historical_data
                WHERE stock = $1 AND primary_exchange = $2 AND currency = $3 AND time <= $4
                ORDER BY time DESC
                LIMIT 1
            )
            SELECT m.close / o.open AS movement_since_open
            FROM (
                SELECT h.time, h.close,
                       (h.time AT TIME ZONE 'America/New_York')::date AS day
                FROM market_data.historical_data h, latest l
                WHERE h.stock = $1 AND h.primary_exchange = $2 AND h.currency = $3
                  AND (h.time AT TIME ZONE 'America/New_York')::time = l.ny::time
                  AND (h.time AT TIME ZONE 'America/New_York')::date < l.ny::date
                ORDER BY h.time DESC
                LIMIT $5
            ) m
            CROSS JOIN LATERAL (
                SELECT open
                FROM market_data.historical_data
                WHERE stock = $1 AND primary_exchange = $2 AND currency = $3
                  AND time >= m.day::timestamp AT TIME ZONE 'America/New_York'
                  AND time <  (m.day + 1)::timestamp AT TIME ZONE 'America/New_York'
                ORDER BY time
                LIMIT 1
            ) o
            ORDER BY m.time DESC
            "#,
            pk.stock,
            pk.primary_exchange,
            pk.currency,
            now,
            avg_move_lookback
        )
        .fetch_all(self.exec())
        .await
        {
            Ok(moves_since_open) => {
                let abs_move_since_open = moves_since_open.iter().map(|move_since_open| {
                    (move_since_open
                        .expect("Expected avg_move_since_open to return at least 1 entry")
                        - 1.0)
                        .abs()
                });
                Ok(abs_move_since_open.sum::<f64>() / moves_since_open.len() as f64)
            }
            Err(e) => Err(format!(
                "Error when fetching most recent rows from \
                HistoricalData in read_last_n_of_stock: {}",
                e
            )),
        }
    }
}
