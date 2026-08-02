use chrono::{DateTime, Timelike, Utc};
use chrono_tz::{America::New_York, Tz};
use ordered_float::OrderedFloat;
use rust_decimal::{Decimal, prelude::{FromPrimitive, ToPrimitive}};
use sqlx::{PgPool, prelude::FromRow};

use crate::{database::{
    models::{
        AssetType, DailyHistoricalStockDataFullKeys, DailyHistoricalStockDataPrimaryKeys, DailyHistoricalStockDataPrimaryKeysWoTime, DailyHistoricalStockDataUpdateKeys, HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeys, HistoricalForexDataPrimaryKeysWoTime, HistoricalForexDataUpdateKeys, HistoricalOptionsDataFullKeys, HistoricalOptionsDataPrimaryKeys, HistoricalOptionsDataPrimaryKeysWoTime, HistoricalOptionsDataUpdateKeys, HistoricalStockDataFullKeys, HistoricalStockDataPrimaryKeys, HistoricalStockDataPrimaryKeysWoTime, HistoricalStockDataUpdateKeys, OptionType,
    }, models_crud::historical_data::{
        daily_historical_data::DailyHistoricalStockDataCRUD,
        historical_forex_data::HistoricalForexDataCRUD,
        historical_options_data::HistoricalOptionsDataCRUD,
        historical_stock_data::HistoricalStockDataCRUD,
    },
}, implement_crud_trait_for_interface};

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

#[derive(Debug, Clone)]
pub enum HistoricalDataPrimaryKeys {
    Stock(HistoricalStockDataPrimaryKeys),
    DailyStock(DailyHistoricalStockDataPrimaryKeys),
    Options(HistoricalOptionsDataPrimaryKeys),
    Forex(HistoricalForexDataPrimaryKeys),
}

#[derive(Debug, Clone)]
pub enum HistoricalDataPrimaryKeysWoTime {
    Stock(HistoricalStockDataPrimaryKeysWoTime),
    DailyStock(DailyHistoricalStockDataPrimaryKeysWoTime),
    Options(HistoricalOptionsDataPrimaryKeysWoTime),
    Forex(HistoricalForexDataPrimaryKeysWoTime),
}

#[derive(Debug, Clone)]
pub enum HistoricalDataUpdateKeys {
    Stock(HistoricalStockDataUpdateKeys),
    DailyStock(DailyHistoricalStockDataUpdateKeys),
    Options(HistoricalOptionsDataUpdateKeys),
    Forex(HistoricalForexDataUpdateKeys),
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

    pub fn stock(pool:PgPool) -> Self {
        Self::Stock(HistoricalStockDataCRUD::new(pool))
    }

    pub fn daily_stock(pool:PgPool) -> Self {
        Self::DailyStock(DailyHistoricalStockDataCRUD::new(pool))
    }

    pub fn option(pool:PgPool) -> Self {
        Self::Options(HistoricalOptionsDataCRUD::new(pool))
    }

    pub fn forex(pool:PgPool) -> Self {
        Self::Forex(HistoricalForexDataCRUD::new(pool))
    }

    pub fn from(asset_type: &AssetType, pool: PgPool) -> Self {
        match asset_type {
            AssetType::Stock 
            | AssetType::Future
            | AssetType::CFD
            | AssetType::CASH => Self::stock(pool),
            AssetType::ForexPair => Self::forex(pool),
            AssetType::Option => Self::option(pool),
            AssetType::Unknown => panic!("Tried to get CRUD instance from an Unknown Asset Type!")
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
    fn as_str(&self) -> String {
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
    ) -> Result<f64, String>;
    async fn has_at_least_n_rows_since(
        &self,
        pk: HistoricalDataPrimaryKeysWoTime,
        n: u64,
        datetime: &DateTime<Tz>,
    ) -> Result<bool, String>;
}

pub trait NoiseOps {
    async fn get_avg_move_since_open(
        &self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
    ) -> Result<f64, String>;
    async fn get_most_recent_daily_open(
        &self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
    ) -> Result<f64, String>;
    async fn get_daily_vol(
        &self,
        pk: HistoricalStockDataPrimaryKeysWoTime,
    ) -> Result<f64, String>;
}

pub trait TimescaleDbOps {
    async fn refresh_daily_data(&self) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub struct AggregatedBars {
    pub full: Vec<HistoricalDataFullKeys>,
    pub incomplete: Vec<HistoricalDataFullKeys>,
}

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
                    if (idx == 0 && bar_count >= timestep_minutes as f64) // MUST HAVE most recent time bar
                || (idx >= 1 && bar_count >= (timestep_minutes as f64 * 0.93))
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
                    GROUP BY bucket, stock, primary_exchange, currency, expiry, multiplier, strike, option_type
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
                            time: row.day,
                            stock: row.stock.clone(),
                            primary_exchange: row.primary_exchange.clone(),
                            currency: row.currency.clone(),
                            open: Decimal::from_f64(row.open).unwrap(),
                            high: Decimal::from_f64(row.high).unwrap(),
                            low: Decimal::from_f64(row.low).unwrap(),
                            close: Decimal::from_f64(row.close).unwrap(),
                            volume: row.volume
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
    ) -> Result<f64, String> {
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
                sqlx::query_as(format!(
                    r#"
                        SELECT
                            SUM({} * volume) / NULLIF(SUM(volume), 0) AS vwap
                        FROM market_data.historical_data
                        WHERE stock = $1
                          AND primary_exchange = $2
                          AND currency = $3
                          -- Convert now() to Eastern, truncate to the day, then cast back to timestamptz
                          AND time >= (now() AT TIME ZONE '{}')::date
                        GROUP BY stock;
                        "#,
                    vwap_bar_value.as_str(),
                    timezone.unwrap_or("US/Eastern".to_string())
                ).as_str())
                    .bind(stock)
                    .bind(primary_exchange)
                    .bind(currency)
                    .fetch_optional(self.get_pg_pool())
                    .await
                    .map_err(|e| {
                        format!(
                            "Error when fetching most recent bar from HistoricalData for in read_vwap: {e:?}",
                        )
                    })?
            }

            HistoricalDataPrimaryKeysWoTime::Forex(HistoricalForexDataPrimaryKeysWoTime { pair }) => {
                sqlx::query_as(format!(
                    r#"
                        SELECT
                            SUM({} * volume) / NULLIF(SUM(volume), 0) AS vwap
                        FROM market_data.historical_forex_data
                        WHERE pair = $1
                          -- Convert now() to Eastern, truncate to the day, then cast back to timestamptz
                          AND time >= (now() AT TIME ZONE '{}')::date
                        GROUP BY stock;
                        "#,
                    vwap_bar_value.as_str(),
                    timezone.unwrap_or("US/Eastern".to_string())
                ).as_str())
                    .bind(pair)
                    .fetch_optional(self.get_pg_pool())
                    .await
                    .map_err(|e| {
                        format!(
                            "Error when fetching most recent bar from HistoricalData for in read_vwap: {e:?}",
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
                option_type 
            }) => {
                sqlx::query_as(format!(
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
                          -- Convert now() to Eastern, truncate to the day, then cast back to timestamptz
                          AND time >= (now() AT TIME ZONE '{}')::date
                        GROUP BY stock;
                        "#,
                    vwap_bar_value.as_str(),
                    timezone.unwrap_or("US/Eastern".to_string())
                ).as_str())
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
                            "Error when fetching most recent bar from HistoricalData for in read_vwap: {e:?}",
                        )
                    })?
            }

            HistoricalDataPrimaryKeysWoTime::DailyStock(_) => {
                return Err("Tried to get vwap price using daily stock data: currently only works for daily".to_string());
            }
        };

        vwap_opt.ok_or_else(|| "Failed to get vwap value".to_string()).map(|v| v.vwap)
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
                    WHERE pair = $2 AND bid_open IS NOT NULL AND ask_open IS NOT NULL AND time > $3;
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
                    WHERE stock = $2 AND primary_exchange = $3 AND currency = $4 AND day > $5;
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
                "Error when fetching most recent rows from HistoricalData in has_at_least_n_rows_since: {}",
                e
            )),
        }
    }
} 


impl NoiseOps for HistoricalDataCRUD {
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
            JOIN opens o ON hm.stock = o.stock AND hm.primary_exchange = o.primary_exchange AND hm.trading_day = o.trading_day
            ORDER BY hm.time DESC;
            "#,
            pk.stock,
            pk.primary_exchange,
            pk.currency
        )
        .fetch_all(self.get_pg_pool())
        .await
        {
            Ok(moves_since_open) =>  {
                let abs_move_since_open = moves_since_open.iter().map(|move_since_open| (
                    move_since_open.expect(
                        "Expected avg_move_since_open to return at least 1 entry"
                    ) - 1.0).abs()
                );
                Ok(abs_move_since_open.sum::<f64>() / moves_since_open.len() as f64)
            }
            ,
            Err(e) => Err(format!(
                "Error when fetching most recent rows from HistoricalData in read_last_n_of_stock: {}",
                e
            )),
        }
    }

    async fn get_most_recent_daily_open(
        &self,
        pk: HistoricalStockDataPrimaryKeysWoTime
    ) -> Result<f64, String> {
        #[derive(FromRow)]
        struct DailyOpenClose {
            day: DateTime<Utc>,
            open: f64,
            close: f64
        }
        let most_recent_daily_close = sqlx::query_as!(
            DailyOpenClose,
            r#"
            SELECT day as "day!", open as "open!", close as "close!"
            FROM market_data.daily_ohlcv
            WHERE stock = $1 AND primary_exchange = $2 AND currency = $3 AND day < $4
            ORDER BY day DESC
            LIMIT 1;
            "#,
            pk.stock,
            pk.primary_exchange,
            pk.currency,
            Utc::now()
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
            Utc::now()
                .with_timezone(&New_York)
                .with_hour(9)
                .unwrap()
                .with_minute(29)
                .unwrap()
                .with_second(0)
                .unwrap()
                .with_nanosecond(0)
                .unwrap(),
            Utc::now()
                .with_timezone(&New_York)
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
            .expect("Expected close and open of the daily opens/close to be valid in get_most_recent_daily_open"))
    }

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
}

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

