use async_trait::async_trait;
use chrono::{DateTime, Timelike, Utc};
use chrono_tz::{America::New_York, Tz};
use ibapi::{contracts::Contract, market_data::realtime::WhatToShow};
use ordered_float::OrderedFloat;
use rust_decimal::{
    Decimal, dec,
    prelude::{FromPrimitive, ToPrimitive},
};
use sqlx::{PgPool, prelude::FromRow};

use crate::{
    database::{
        models::{
            AssetType, DailyHistoricalStockDataFullKeys, DailyHistoricalStockDataPrimaryKeys,
            DailyHistoricalStockDataPrimaryKeysWoTime, DailyHistoricalStockDataUpdateKeys,
            HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeys,
            HistoricalForexDataPrimaryKeysWoTime, HistoricalForexDataUpdateKeys,
            HistoricalOptionsDataFullKeys, HistoricalOptionsDataPrimaryKeys,
            HistoricalOptionsDataPrimaryKeysWoTime, HistoricalOptionsDataUpdateKeys,
            HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys,
            HistoricalStockDataPrimaryKeysWoTime, HistoricalStockDataUpdateKeys, OptionType,
        },
        models_crud::historical_data::{
            daily_historical_data::DailyHistoricalStockDataCRUD,
            historical_forex_data::HistoricalForexDataCRUD,
            historical_options_data::HistoricalOptionsDataCRUD,
            historical_stock_data::HistoricalStockDataCRUD,
        },
    },
    helpers::contract::get_local_symbol,
    implement_crud_trait_for_interface,
};

#[derive(Debug, Clone)]
pub enum HistoricalDataCRUD {
    Stock(HistoricalStockDataCRUD),
    DailyStock(DailyHistoricalStockDataCRUD),
    Options(HistoricalOptionsDataCRUD),
    Forex(HistoricalForexDataCRUD),
}

#[derive(Debug, Clone)]
pub enum HistoricalDataFullKeys {
    Stock(HistoricalStockDataFullKeys),
    DailyStock(DailyHistoricalStockDataFullKeys),
    Options(HistoricalOptionsDataFullKeys),
    Forex(HistoricalForexDataFullKeys),
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for HistoricalDataFullKeys {
    fn from_row(_: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        // This will never be executed because the inner CRUD<FK,PK,UK>
        // decodes the inner concrete struct before wrapping it into this enum.
        Err(sqlx::Error::Decode(
            "HistoricalDataFullKeys cannot be decoded directly from a raw SQL row".into(),
        ))
    }
}

impl HistoricalDataFullKeys {
    pub fn get_time(&self) -> DateTime<Utc> {
        match self {
            Self::Stock(v) => v.time,
            Self::Forex(v) => v.time,
            Self::Options(v) => v.time,
            Self::DailyStock(v) => v.day,
        }
    }

    pub fn get_price(&self) -> f64 {
        match self {
            Self::Stock(v) => v.close,
            Self::Forex(v) => v.ask_close.unwrap_or(v.bid_close.unwrap_or(-1.0)),
            Self::Options(v) => v.close,
            Self::DailyStock(v) => v.close.to_f64().expect("Expected conversion to f64"),
        }
    }

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

#[derive(Debug, Clone)]
pub enum HistoricalDataPrimaryKeys {
    Stock(HistoricalStockDataPrimaryKeys),
    DailyStock(DailyHistoricalStockDataPrimaryKeys),
    Options(HistoricalOptionsDataPrimaryKeys),
    Forex(HistoricalForexDataPrimaryKeys),
}

impl HistoricalDataPrimaryKeys {
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

#[derive(Debug, Clone)]
pub enum HistoricalDataPrimaryKeysWoTime {
    Stock(HistoricalStockDataPrimaryKeysWoTime),
    DailyStock(DailyHistoricalStockDataPrimaryKeysWoTime),
    Options(HistoricalOptionsDataPrimaryKeysWoTime),
    Forex(HistoricalForexDataPrimaryKeysWoTime),
}

impl HistoricalDataPrimaryKeysWoTime {
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

#[derive(Debug, Clone)]
pub enum HistoricalDataUpdateKeys {
    Stock(HistoricalStockDataUpdateKeys),
    DailyStock(DailyHistoricalStockDataUpdateKeys),
    Options(HistoricalOptionsDataUpdateKeys),
    Forex(HistoricalForexDataUpdateKeys),
}

impl HistoricalDataUpdateKeys {
    pub fn from_bar(
        contract: &Contract,
        what_to_show: &WhatToShow,
        bar: &HistoricalDataFullKeys,
    ) -> Self {
        // Step 1: Extract OHLCV fields by destructuring `bar`
        let (open, high, low, close, volume) = match bar {
            HistoricalDataFullKeys::Stock(s) => (s.open, s.high, s.low, s.close, Some(s.volume)),
            HistoricalDataFullKeys::DailyStock(s) => (
                s.open.to_f64().unwrap(),
                s.high.to_f64().unwrap(),
                s.low.to_f64().unwrap(),
                s.close.to_f64().unwrap(),
                Some(s.volume),
            ),
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

impl HistoricalDataCRUD {
    fn get_pg_pool<'a>(&'a self) -> &'a PgPool {
        match self {
            Self::Stock(stk) => &stk.crud.pool,
            Self::Forex(fx) => &fx.crud.pool,
            Self::Options(opt) => &opt.crud.pool,
            Self::DailyStock(daily_stk) => &daily_stk.crud.pool,
        }
    }

    pub fn stock(pool: PgPool) -> Self {
        Self::Stock(HistoricalStockDataCRUD::new(pool))
    }

    pub fn daily_stock(pool: PgPool) -> Self {
        Self::DailyStock(DailyHistoricalStockDataCRUD::new(pool))
    }

    pub fn option(pool: PgPool) -> Self {
        Self::Options(HistoricalOptionsDataCRUD::new(pool))
    }

    pub fn forex(pool: PgPool) -> Self {
        Self::Forex(HistoricalForexDataCRUD::new(pool))
    }

    pub fn from(asset_type: &AssetType, pool: PgPool) -> Self {
        match asset_type {
            AssetType::Stock | AssetType::Future | AssetType::CFD | AssetType::CASH => {
                Self::stock(pool)
            }
            AssetType::ForexPair => Self::forex(pool),
            AssetType::Option => Self::option(pool),
            AssetType::Unknown => panic!("Tried to get CRUD instance from an Unknown Asset Type!"),
        }
    }
}

implement_crud_trait_for_interface!(
    HistoricalDataCRUD,
    HistoricalDataFullKeys,
    HistoricalDataPrimaryKeys,
    HistoricalDataUpdateKeys,
    [Stock, DailyStock, Options, Forex]
);

pub enum VwapBarValue {
    Close,
    Open,
    BidOpen,
    BidClose,
    AskOpen,
    AskClose,
}

impl VwapBarValue {
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

#[async_trait]
pub trait HistoricalDataOps {
    async fn read_last_n(
        &self,
        pk: HistoricalDataPrimaryKeysWoTime,
        timestep_minutes: u32,
        limit: u32,
    ) -> Result<AggregatedBars, String>;
    async fn read_last_bar(
        &self,
        pk: HistoricalDataPrimaryKeysWoTime,
        timestep_minutes: u32,
    ) -> Result<HistoricalDataFullKeys, String>;
    async fn read_last_vwap(
        &self,
        pk: HistoricalDataPrimaryKeysWoTime,
        timezone: Option<String>,
        vwap_bar_value: VwapBarValue,
        #[cfg(feature = "backtest")]
        now: DateTime<Utc>,
    ) -> Result<Option<f64>, String>;
    async fn has_at_least_n_rows_since(
        &self,
        pk: HistoricalDataPrimaryKeysWoTime,
        n: u64,
        datetime: &DateTime<Tz>,
    ) -> Result<bool, String>;
}

#[async_trait]
pub trait NoiseOps {
    #[cfg(not(feature = "backtest"))]
    async fn get_avg_move_since_open(
        &self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
    ) -> Result<f64, String>;
    #[cfg(feature = "backtest")]
    async fn get_avg_move_since_open(
        &self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
        now: DateTime<Utc>,
    ) -> Result<f64, String>;

    async fn get_most_recent_daily_open(
        &self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
        #[cfg(feature = "backtest")]
        now: DateTime<Utc>,
    ) -> Result<f64, String>;

    #[cfg(not(feature = "backtest"))]
    async fn get_daily_vol(&self, pk: HistoricalStockDataPrimaryKeysWoTime) -> Result<f64, String>;
    #[cfg(feature = "backtest")]
    async fn get_daily_vol(
        &self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
        now: DateTime<Utc>,
    ) -> Result<f64, String>;
}

#[async_trait::async_trait]
pub trait TimescaleDbOps {
    async fn refresh_daily_data(&self) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub struct AggregatedBars {
    pub full: Vec<HistoricalDataFullKeys>,
    pub incomplete: Vec<HistoricalDataFullKeys>,
}

#[async_trait]
impl HistoricalDataOps for HistoricalDataCRUD {
    async fn read_last_n(
        &self,
        pk: HistoricalDataPrimaryKeysWoTime,
        timestep_minutes: u32,
        limit: u32,
    ) -> Result<AggregatedBars, String> {
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
                        time_bucket(make_interval(mins => $4), time) AS bucket,
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
                    GROUP BY bucket, stock, primary_exchange, currency
                    ORDER BY bucket DESC
                    LIMIT $5
                    "#,
                    stock,
                    primary_exchange,
                    currency,
                    timestep_minutes as i32,
                    limit as i32,
                )
                .fetch_all(self.get_pg_pool())
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
                        time_bucket(make_interval(mins => $2), time) AS bucket,
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
                    GROUP BY bucket, pair
                    ORDER BY bucket DESC
                    LIMIT $3;
                    "#,
                    pair,
                    timestep_minutes as i32,
                    limit as i32,
                )
                .fetch_all(self.get_pg_pool())
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
                        time_bucket(make_interval(mins => $8), time) AS bucket,
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
                )
                .fetch_all(self.get_pg_pool())
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

            HistoricalDataPrimaryKeysWoTime::DailyStock(
                DailyHistoricalStockDataPrimaryKeysWoTime {
                    stock,
                    primary_exchange,
                    currency,
                },
            ) => {
                let rows = sqlx::query!(
                    r#"
                    SELECT
                        day as "day!",
                        stock as "stock!",
                        primary_exchange as "primary_exchange!",
                        currency as "currency!",
                        open as "open!",
                        high as "high!",
                        low as "low!",
                        close as "close!",
                        volume as "volume!"
                    FROM market_data.daily_ohlcv
                    WHERE stock = $1
                      AND primary_exchange = $2
                      AND currency = $3
                    ORDER BY day DESC
                    LIMIT $4
                    "#,
                    stock,
                    primary_exchange,
                    currency,
                    limit as i32,
                )
                .fetch_all(self.get_pg_pool())
                .await
                .map_err(|e| {
                    format!("Error reading daily historical bars in read_last_n: {}", e)
                })?;

                for row in rows.iter() {
                    let bar =
                        HistoricalDataFullKeys::DailyStock(DailyHistoricalStockDataFullKeys {
                            day: row.day,
                            stock: row.stock.clone(),
                            primary_exchange: row.primary_exchange.clone(),
                            currency: row.currency.clone(),
                            open: row.open,
                            high: row.high,
                            low: row.low,
                            close: row.close,
                            volume: row.volume,
                        });
                    full.push(bar);
                }
            }
        };

        full.reverse(); // chronological
        incomplete.reverse();

        Ok(AggregatedBars { full, incomplete })
    }

    async fn read_last_bar(
        &self,
        pk: HistoricalDataPrimaryKeysWoTime,
        timestep_minutes: u32,
    ) -> Result<HistoricalDataFullKeys, String> {
        let mut agg_bars = self
            .read_last_n(pk, timestep_minutes, 1)
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
        &self,
        pk: HistoricalDataPrimaryKeysWoTime,
        timezone: Option<String>,
        vwap_bar_value: VwapBarValue,
        #[cfg(feature = "backtest")] now: DateTime<Utc>,
    ) -> Result<Option<f64>, String> {
        #[derive(FromRow)]
        struct Vwap {
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
                          AND time >= ({now_sql}) AT TIME ZONE '{}'::date
                        GROUP BY stock;
                        "#,
                    vwap_bar_value.as_str(),
                    timezone.unwrap_or("US/Eastern".to_string())
                );
                let q = sqlx::query_as(sql_str.as_str())
                    .bind(stock)
                    .bind(primary_exchange)
                    .bind(currency);
                #[cfg(feature = "backtest")]
                let q = q.bind(now);
                q.fetch_optional(self.get_pg_pool()).await.map_err(|e| {
                    format!(
                        "Error when fetching most recent bar from HistoricalData \
                                    for in read_vwap: {e:?}",
                    )
                })?
            }

            HistoricalDataPrimaryKeysWoTime::Forex(HistoricalForexDataPrimaryKeysWoTime {
                pair,
            }) => sqlx::query_as(
                format!(
                    r#"
                        SELECT
                            SUM({} * volume) / NULLIF(SUM(volume), 0) AS vwap
                        FROM market_data.historical_forex_data
                        WHERE pair = $1
                          -- Convert now() to Eastern, truncate to the day,
                          -- then cast back to timestamptz
                          AND time >= (now() AT TIME ZONE '{}')::date
                        GROUP BY stock;
                        "#,
                    vwap_bar_value.as_str(),
                    timezone.unwrap_or("US/Eastern".to_string())
                )
                .as_str(),
            )
            .bind(pair)
            .fetch_optional(self.get_pg_pool())
            .await
            .map_err(|e| {
                format!(
                    "Error when fetching most recent bar from HistoricalData \
                            for in read_vwap: {e:?}",
                )
            })?,

            HistoricalDataPrimaryKeysWoTime::Options(HistoricalOptionsDataPrimaryKeysWoTime {
                stock,
                primary_exchange,
                currency,
                expiry,
                strike,
                multiplier,
                option_type,
            }) => sqlx::query_as(
                format!(
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
                          AND time >= (now() AT TIME ZONE '{}')::date
                        GROUP BY stock;
                        "#,
                    vwap_bar_value.as_str(),
                    timezone.unwrap_or("US/Eastern".to_string())
                )
                .as_str(),
            )
            .bind(stock)
            .bind(primary_exchange)
            .bind(currency)
            .bind(expiry)
            .bind(strike)
            .bind(multiplier)
            .bind(option_type)
            .fetch_optional(self.get_pg_pool())
            .await
            .map_err(|e| {
                format!(
                    "Error when fetching most recent bar from HistoricalData\
                            for in read_vwap: {e:?}",
                )
            })?,

            HistoricalDataPrimaryKeysWoTime::DailyStock(_) => {
                return Err("Tried to get vwap price using daily stock data: currently only works for daily".to_string());
            }
        };

        Ok(vwap_opt.map(|v| v.vwap))
    }

    async fn has_at_least_n_rows_since(
        &self,
        pk: HistoricalDataPrimaryKeysWoTime,
        n: u64,
        datetime: &DateTime<Tz>,
    ) -> Result<bool, String> {
        let enough_rows = match pk {
            HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
                stock,
                primary_exchange,
                currency,
            }) => {
                sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*) > $1
                    FROM market_data.historical_data
                    WHERE stock = $2 AND primary_exchange = $3 AND currency = $4 AND time > $5;
            "#,
                    (n - 1) as i32,
                    stock,
                    primary_exchange,
                    currency,
                    datetime
                )
                .fetch_one(self.get_pg_pool())
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
                    SELECT COUNT(*) > $1
                    FROM market_data.historical_options_data
                    WHERE stock = $2
                        AND primary_exchange = $3
                        AND currency = $4
                        AND expiry = $5
                        AND strike = $6
                        AND multiplier = $7
                        AND option_type = $8
                        AND time > $9;
                    "#,
                    (n - 1) as i32,
                    stock,
                    primary_exchange,
                    currency,
                    expiry,
                    strike,
                    multiplier,
                    option_type as OptionType,
                    datetime
                )
                .fetch_one(self.get_pg_pool())
                .await
            }
            HistoricalDataPrimaryKeysWoTime::Forex(HistoricalForexDataPrimaryKeysWoTime {
                pair,
            }) => {
                sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*) > $1
                    FROM market_data.historical_forex_data
                    WHERE pair = $2
                        AND bid_open IS NOT NULL
                        AND ask_open IS NOT NULL
                        AND time > $3;
                    "#,
                    (n - 1) as i32,
                    pair,
                    datetime
                )
                .fetch_one(self.get_pg_pool())
                .await
            }
            HistoricalDataPrimaryKeysWoTime::DailyStock(
                DailyHistoricalStockDataPrimaryKeysWoTime {
                    stock,
                    primary_exchange,
                    currency,
                },
            ) => {
                sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*) > $1
                    FROM market_data.daily_ohlcv
                    WHERE stock = $2
                        AND primary_exchange = $3
                        AND currency = $4
                        AND day > $5;
            "#,
                    (n - 1) as i32,
                    stock,
                    primary_exchange,
                    currency,
                    datetime
                )
                .fetch_one(self.get_pg_pool())
                .await
            }
        };
        match enough_rows {
            Ok(has_at_least_n_rows) => Ok(has_at_least_n_rows.expect(
                "Expected sql query to return a boolean at least in has_at_least_n_rows_since",
            )),
            Err(e) => Err(format!(
                "Error when fetching most recent rows from HistoricalData \
                in has_at_least_n_rows_since: {}",
                e
            )),
        }
    }
}

#[async_trait]
impl NoiseOps for HistoricalDataCRUD {
    /// Today's move will NOT be included in the calculation
    /// avg move is of the last 15 days
    #[cfg(not(feature = "backtest"))]
    async fn get_avg_move_since_open(
        &self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
    ) -> Result<f64, String> {
        match sqlx::query_scalar!(
            r#"
            WITH latest AS (
                SELECT
                    time::time AS latest_close_time,
                    time::date AS latest_date
                FROM market_data.historical_data
                WHERE stock = $1
                  AND primary_exchange = $2
                  AND currency = $3
                ORDER BY time DESC
                LIMIT 1
            ),
            historical_matches AS (
                SELECT
                    h.stock,
                    h.primary_exchange,
                    h.currency,
                    h.time::date AS trading_day,
                    h.time,
                    h.close
                FROM market_data.historical_data h
                CROSS JOIN latest
                WHERE h.stock = $1
                  AND h.primary_exchange = $2
                  AND h.currency= $3
                  AND h.time::time = latest.latest_close_time
                  AND h.time::date <> latest.latest_date  -- exclude today
                ORDER BY h.time DESC
                LIMIT 15
            ),
            opens AS (
                SELECT stock, primary_exchange, day AS trading_day, open AS open_at_0930
                FROM market_data.daily_ohlcv
                WHERE stock = $1
                    AND primary_exchange = $2
            )
            SELECT
                hm.close / o.open_at_0930 AS movement_since_open
            FROM historical_matches hm
            JOIN
                opens o ON hm.stock = o.stock
                AND hm.primary_exchange = o.primary_exchange
                AND hm.trading_day = o.trading_day
            ORDER BY hm.time DESC;
            "#,
            pk.stock,
            pk.primary_exchange,
            pk.currency
        )
        .fetch_all(self.get_pg_pool())
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
        &self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
        now: DateTime<Utc>,
    ) -> Result<f64, String> {
        match sqlx::query_scalar!(
            r#"
            WITH latest AS (
                SELECT
                    time::time AS latest_close_time,
                    time::date AS latest_date
                FROM market_data.historical_data
                WHERE stock = $1
                  AND primary_exchange = $2
                  AND currency = $3
                  AND time <= $4
                ORDER BY time DESC
                LIMIT 1
            ),
            historical_matches AS (
                SELECT
                    h.stock,
                    h.primary_exchange,
                    h.currency,
                    h.time::date AS trading_day,
                    h.time,
                    h.close
                FROM market_data.historical_data h
                CROSS JOIN latest
                WHERE h.stock = $1
                  AND h.primary_exchange = $2
                  AND h.currency= $3
                  AND h.time::time = latest.latest_close_time
                  AND h.time::date <> latest.latest_date
                ORDER BY h.time DESC
                LIMIT 15
            ),
            opens AS (
                SELECT stock, primary_exchange, day AS trading_day, open AS open_at_0930
                FROM market_data.daily_ohlcv
                WHERE stock = $1
                    AND primary_exchange = $2
            )
            SELECT
                hm.close / o.open_at_0930 AS movement_since_open
            FROM historical_matches hm
            JOIN
                opens o ON hm.stock = o.stock
                AND hm.primary_exchange = o.primary_exchange
                AND hm.trading_day = o.trading_day
            ORDER BY hm.time DESC;
            "#,
            pk.stock,
            pk.primary_exchange,
            pk.currency,
            now
        )
        .fetch_all(self.get_pg_pool())
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

    async fn get_most_recent_daily_open(
        &self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
        #[cfg(feature = "backtest")]
        now: DateTime<Utc>,
    ) -> Result<f64, String> {
        // Under prod, `now` = Utc::now() (real wall clock); under backtest,
        // `now` = bar time (the fn param) — prevents look-ahead bias.
        #[cfg(not(feature = "backtest"))]
        let now = Utc::now();

        #[derive(FromRow)]
        struct DailyOpenClose {
            day: DateTime<Utc>,
            open: f64,
            close: f64,
        }
        let most_recent_daily_close = sqlx::query_as!(
            DailyOpenClose,
            r#"
            SELECT day as "day!", open as "open!", close as "close!"
            FROM market_data.daily_ohlcv
            WHERE stock = $1
                AND primary_exchange = $2
                AND currency = $3
                AND day < $4
            ORDER BY day DESC
            LIMIT 1;
            "#,
            pk.stock,
            pk.primary_exchange,
            pk.currency,
            now
        )
        .fetch_one(self.get_pg_pool())
        .await
        .map(|most_recent_daily_open_option| DailyOpenClose {
            day: most_recent_daily_open_option.day,
            open: most_recent_daily_open_option.open,
            close: most_recent_daily_open_option.close,
        })
        .map_err(|e| format!("Error when getting most recent daily close of stock: {}", e))?;

        let most_recent_daily_open_option = sqlx::query_scalar!(
            r#"
            SELECT open
            FROM market_data.historical_data
            WHERE stock = $1 AND time > $2 AND time < $3;
            "#,
            pk.stock,
            now.with_timezone(&New_York)
                .with_hour(9)
                .unwrap()
                .with_minute(29)
                .unwrap()
                .with_second(0)
                .unwrap()
                .with_nanosecond(0)
                .unwrap(),
            now.with_timezone(&New_York)
                .with_hour(9)
                .unwrap()
                .with_minute(31)
                .unwrap()
                .with_second(0)
                .unwrap()
                .with_nanosecond(0)
                .unwrap(),
        )
        .fetch_one(self.get_pg_pool())
        .await
        .map_err(|e| format!("Error when getting most recent daily open of stock: {}", e))?;

        Ok(std::cmp::max::<OrderedFloat<f64>>(
            OrderedFloat::from(most_recent_daily_close.close),
            OrderedFloat::from(most_recent_daily_open_option),
        )
        .to_f64()
        .expect(
            "Expected close and open of the daily opens/close \
                to be valid in get_most_recent_daily_open",
        ))
    }

    #[cfg(not(feature = "backtest"))]
    async fn get_daily_vol(&self, pk: HistoricalStockDataPrimaryKeysWoTime) -> Result<f64, String> {
        let daily_vol = sqlx::query_scalar!(
            r#"
            SELECT rolling_volatility
            FROM market_data.daily_volatility
            WHERE stock = $1
                AND primary_exchange = $2
                AND currency = $3
            ORDER BY day DESC
            LIMIT 1;
        "#,
            pk.stock,
            pk.primary_exchange,
            pk.currency
        )
        .fetch_one(self.get_pg_pool())
        .await
        .map_err(|e| {
            format!(
                "Error getting most recent daily volatility of {}: {}",
                pk.stock, e
            )
        })?;
        Ok(daily_vol.expect(&format!(
            "Expected to have enough data to get volatility of stock: {}",
            pk.stock
        )))
    }

    /// Backtest variant: filter "latest" to `day <= now` (the bar time) to
    /// prevent look-ahead bias. Under prod, `get_daily_vol` (above) uses the
    /// real wall-clock latest.
    #[cfg(feature = "backtest")]
    async fn get_daily_vol(
        &self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
        now: DateTime<Utc>,
    ) -> Result<f64, String> {
        let daily_vol = sqlx::query_scalar!(
            r#"
            SELECT rolling_volatility
            FROM market_data.daily_volatility
            WHERE stock = $1
                AND primary_exchange = $2
                AND currency = $3
                AND day <= $4
            ORDER BY day DESC
            LIMIT 1;
        "#,
            pk.stock,
            pk.primary_exchange,
            pk.currency,
            now
        )
        .fetch_one(self.get_pg_pool())
        .await
        .map_err(|e| {
            format!(
                "Error getting most recent daily volatility of {}: {}",
                pk.stock, e
            )
        })?;
        Ok(daily_vol.expect(&format!(
            "Expected to have enough data to get volatility of stock: {}",
            pk.stock
        )))
    }
}

#[async_trait::async_trait]
impl TimescaleDbOps for HistoricalDataCRUD {
    async fn refresh_daily_data(&self) -> Result<(), String> {
        sqlx::query!(
            r#"
            CALL refresh_continuous_aggregate(
                'market_data.daily_ohlcv',
                NOW() - INTERVAL '30 days',
                NOW()
            );
            "#,
        )
        .execute(self.get_pg_pool())
        .await
        .map_err(|e| format!("Failed to refresh_continuous_aggregate for daily_ohlcv: {e:?}"))?;
        Ok(())
    }
}
