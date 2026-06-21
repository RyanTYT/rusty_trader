use std::{
    cmp::max,
    collections::HashSet,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Timelike, Utc};
use chrono_tz::{America::New_York, Tz};
use ordered_float::OrderedFloat;
use rand::{Rng, distr::Alphanumeric};
use rust_decimal::prelude::ToPrimitive;
use sqlx::PgPool;
use tokio::{
    sync::{
        Notify,
        mpsc::{Receiver, Sender, channel},
    },
    time::Instant,
};
use tokio_postgres::{NoTls, binary_copy::BinaryCopyInWriter};

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{HistoricalDataFullKeys, HistoricalDataPrimaryKeys, HistoricalDataUpdateKeys},
    },
    delegate_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct HistoricalDataCRUD {
    crud: CRUD<HistoricalDataFullKeys, HistoricalDataPrimaryKeys, HistoricalDataUpdateKeys>,
    sender: Arc<Mutex<Option<Arc<Sender<HistoricalDataFullKeys>>>>>,
    shutdown_sender: Arc<Mutex<Option<Arc<Sender<bool>>>>>,
    shutdown_resp_rcx: Arc<Mutex<Option<Receiver<bool>>>>,

    num_channel_connections: Arc<Mutex<u32>>,
    notify_initialised: Arc<Notify>,
}

async fn init_channel() -> (
    Arc<Sender<HistoricalDataFullKeys>>,
    Arc<Sender<bool>>,
    Receiver<bool>,
) {
    const BATCH_SIZE: usize = 200_000;
    const MAX_BATCH_WAIT_MS: u64 = 1000;

    let host = std::env::var("DATABASE_HOST")
        .expect("Expected DATABASE_HOST environment variable to be set!");

    let (mut client, connection) = tokio_postgres::connect(
        &format!(
            "host={} user=ryantan password=admin dbname=trading_system",
            host
        ),
        NoTls,
    )
    .await
    .expect("Expected to be able to make tokio_postgres connection");
    tracing::info!("INIT CHANNEL");

    // spawn connection task so client works
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    let (sender, mut rx) = channel::<HistoricalDataFullKeys>(10_000);
    let (shutdown_sender, mut shutdown_rx) = channel::<bool>(2);
    let (shutdown_resp_sender, shutdown_resp_rx) = channel::<bool>(2);

    tokio::spawn(async move {
        let mut buffer = Vec::with_capacity(BATCH_SIZE);
        let mut last_flush = Instant::now();
        tracing::info!("Entered loop to receive goods");

        loop {
            tokio::select! {
                maybe_row = rx.recv() => {
                    match maybe_row {
                        Some(row) => {
                            buffer.push(row);
                            if buffer.len() >= BATCH_SIZE {
                                if let Err(e) = HistoricalDataCRUD::flush_batch(&mut client, &buffer).await {
                                    tracing::error!("Expected to be able to flush batch: {e:?}");
                                }
                                buffer.clear();
                                last_flush = Instant::now();
                            }
                        }
                        None => {
                            if !buffer.is_empty() {
                                if let Err(e) = HistoricalDataCRUD::flush_batch(&mut client, &buffer).await {
                                    tracing::error!("Expected to be able to flush batch: {e:?}");
                                }
                            }
                            if let Err(e) = shutdown_resp_sender.send(true).await {
                                tracing::warn!("Could not send update to shutdown_resp_sender in historical_data: {e:?}")
                            };
                            break;
                        }
                    }
                }
                maybe_shutdown = shutdown_rx.recv() => {
                    if let Some(to_shutdown) = maybe_shutdown {
                        if to_shutdown {
                            if !buffer.is_empty() {
                                if let Err(e) = HistoricalDataCRUD::flush_batch(&mut client, &buffer).await {
                                    tracing::error!("Expected to be able to flush batch: \n{e:?}");
                                }
                            }
                            drop(client);
                            if let Err(e) = shutdown_resp_sender.send(true).await {
                                tracing::warn!("Could not send update to shutdown_resp_sender in historical_data: {e:?}")
                            };
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(MAX_BATCH_WAIT_MS)) => {
                    if !buffer.is_empty() && last_flush.elapsed().as_millis() as u64 >= MAX_BATCH_WAIT_MS {
                        if let Err(e) = HistoricalDataCRUD::flush_batch(&mut client, &buffer).await {
                            tracing::error!("Expected to be able to flush batch: \n{e:?}");
                        }
                        buffer.clear();
                        last_flush = Instant::now();
                    }
                }
            }
        }
        tracing::info!("loop to receive goods ended");
    });

    (
        Arc::new(sender),
        Arc::new(shutdown_sender),
        shutdown_resp_rx,
    )
}

struct OptionDailyOC {
    day: Option<DateTime<Utc>>,
    open: Option<f64>,
    close: Option<f64>,
}

struct DailyOC {
    _day: DateTime<Utc>,
    _open: f64,
    close: f64,
}

struct OptionVWAP {
    vwap: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct AggregatedBars {
    pub full: Vec<HistoricalDataFullKeys>,
    pub incomplete: Vec<HistoricalDataFullKeys>,
}

impl HistoricalDataCRUD {
    fn new(pool: PgPool) -> Self {
        // let sender = GLOBAL_SENDER
        //     .get_or_init(|| async { init_channel() })
        //     .await
        //     .clone();
        Self {
            crud: CRUD::<HistoricalDataFullKeys, HistoricalDataPrimaryKeys, HistoricalDataUpdateKeys>::new(pool, String::from("market_data.historical_data")),
            sender: Arc::new(Mutex::new(None)),
            shutdown_sender: Arc::new(Mutex::new(None)),
            shutdown_resp_rcx: Arc::new(Mutex::new(None)),

            num_channel_connections: Arc::new(Mutex::new(0)),
            notify_initialised: Arc::new(Notify::new()),
        }
    }

    async fn flush_batch(
        client: &mut tokio_postgres::Client,
        batch: &[HistoricalDataFullKeys],
    ) -> Result<(), anyhow::Error> {
        let suffix: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();
        let staging_table = format!("staging_{}", suffix);

        let tx = client.transaction().await?;

        let create_sql = format!(
            "CREATE TEMP TABLE {st} (
                stock VARCHAR(50), 
                primary_exchange VARCHAR(50), 
                currency VARCHAR(10), 
                time TIMESTAMPTZ,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume NUMERIC(30, 6)
            ) ON COMMIT DROP;",
            st = &staging_table,
        );
        tx.batch_execute(&create_sql).await?;

        let copy_sql = format!(
            "COPY {st} (stock, primary_exchange, currency, time, open, high, low, close, volume) FROM STDIN WITH (FORMAT binary)",
            st = &staging_table,
        );

        let sink = tx.copy_in(&copy_sql).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                tokio_postgres::types::Type::VARCHAR,
                tokio_postgres::types::Type::VARCHAR,
                tokio_postgres::types::Type::VARCHAR,
                tokio_postgres::types::Type::TIMESTAMPTZ,
                tokio_postgres::types::Type::FLOAT8,
                tokio_postgres::types::Type::FLOAT8,
                tokio_postgres::types::Type::FLOAT8,
                tokio_postgres::types::Type::FLOAT8,
                tokio_postgres::types::Type::NUMERIC,
            ],
        );
        tokio::pin!(writer);

        let mut unique_keys = HashSet::new();
        for row in batch {
            if unique_keys.contains(&(&row.stock, &row.primary_exchange, &row.time)) {
                continue;
            };
            unique_keys.insert((&row.stock, &row.primary_exchange, &row.time));
            writer
                .as_mut()
                .write(&[
                    &row.stock,
                    &row.primary_exchange,
                    &row.currency,
                    &row.time,
                    &row.open,
                    &row.high,
                    &row.low,
                    &row.close,
                    &row.volume,
                ])
                .await
                .map_err(|e| anyhow::Error::msg(format!("{}", e)))?;
        }
        if let Err(e) = writer.finish().await {
            tracing::warn!(
                "Error trying to wait for writer to finish writing batch in historical data: {e:?}"
            );
        };

        let merge_sql = format!(
            r#"
            INSERT INTO market_data.historical_data (stock, primary_exchange, currency, time, open, high, low, close, volume)
            SELECT stock, primary_exchange, currency, time, open, high, low, close, volume FROM {st}
            ON CONFLICT (stock, primary_exchange, currency, time)
            DO UPDATE 
            SET 
                open = EXCLUDED.open, 
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        "#,
            st = &staging_table,
        );

        tx.batch_execute(&merge_sql).await?;

        tx.commit().await?;
        println!("Flushed batch of {} rows", batch.len());
        Ok(())
    }

    delegate_all_crud_methods!(
        crud,
        HistoricalDataFullKeys,
        HistoricalDataPrimaryKeys,
        HistoricalDataUpdateKeys
    );

    /// MUST BE VV CAREFUL
    /// init_channel MUST ALWAYS be called in conjunction with close_channel
    pub async fn init_channel(&self) {
        let is_first = {
            let mut conn_count = self
                .num_channel_connections
                .lock()
                .expect("Expected num_channel_connections lock to not be poisoned");
            *conn_count += 1;
            *conn_count == 1
        };

        if !is_first {
            // Another init is in progress or already done; wait until first init completes
            loop {
                if self
                    .sender
                    .lock()
                    .expect("Expected sender lock not to be poisoned")
                    .is_some()
                {
                    break;
                }
                self.notify_initialised.notified().await;
            }
            return;
        }

        // Only first caller initialises
        let (sender, shutdown_sender, shutdown_resp_rcx) = init_channel().await;
        self.sender
            .lock()
            .expect("Expected to be able to acquire sender lock")
            .replace(sender);
        self.shutdown_sender
            .lock()
            .expect("Expected to be able to acquire shutdown_sender lock")
            .replace(shutdown_sender);
        self.shutdown_resp_rcx
            .lock()
            .expect("Expected to be able to acquire shutdown_resp_rcx lock")
            .replace(shutdown_resp_rcx);
    }

    pub async fn close_channel(&self) {
        // Take receiver out of mutex BEFORE awaiting
        {
            let mut conn_count = self
                .num_channel_connections
                .lock()
                .expect("Expected num_channel_connections lock to not be poisoned");
            if *conn_count == 0 {
                return; // Already closed
            }

            *conn_count -= 1;
            if *conn_count > 0 {
                return; // Still active users
            }
        }

        let mut shutdown_resp_rcx = {
            let opt = self
                .shutdown_resp_rcx
                .lock()
                .expect("Failed to acquire lock shutdown_resp_rcx")
                .take();
            if opt.is_none() {
                tracing::warn!("historical_data: Channel already closed!");
                return;
            }
            opt.unwrap()
        };

        let sender = {
            let opt = self
                .shutdown_sender
                .lock()
                .expect("Failed to acquire lock shutdown_sender")
                .take();
            if opt.is_none() {
                tracing::warn!("historical_data: Channel already closed!");
                return;
            }
            opt.unwrap()
        };

        if let Err(e) = sender.send(true).await {
            tracing::error!("error sending shutdown command: {e:?}");
        }

        // Wait for acknowledgement / channel close
        let _ = shutdown_resp_rcx.recv().await;
    }

    pub async fn batch_create_or_update(&self, fk: &HistoricalDataFullKeys) -> Result<(), String> {
        let sender = self
            .sender
            .lock()
            .expect("Expected to be able to acquire sender lock")
            .clone()
            .expect("Expected channel to be initialised before batch_create_or_update");
        if let Err(e) = sender.send(fk.clone()).await {
            tracing::error!("error sending data via batch_create_or_update: {e:?}");
        };
        Ok(())
    }

    pub async fn read_last_n_of_stock(
        &self,
        stock: &str,
        primary_exchange: &str,
        currency: &str,
        timestep_minutes: &u32,
        limit: &u32,
    ) -> Result<AggregatedBars, String> {
        let rows = sqlx::query!(
            r#"
            SELECT
                time_bucket(make_interval(mins => $4), time) AS bucket,
                stock,
                primary_exchange,
                currency,
                first(open, time)       AS open,
                max(high)               AS high,
                min(low)                AS low,
                last(close, time)       AS close,
                sum(volume)             AS volume,
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
            *timestep_minutes as i32,
            *limit as i32,
        )
        .fetch_all(&self.crud.pool)
        .await
        .map_err(|e| format!("Error aggregating bars in read_last_n_bars: {}", e))?;

        let mut full = Vec::new();
        let mut incomplete = Vec::new();

        for (idx, row) in rows.iter().enumerate() {
            let bar = HistoricalDataFullKeys {
                time: row.bucket.unwrap(),
                stock: row.stock.clone(),
                primary_exchange: row.primary_exchange.clone(),
                currency: row.currency.clone(),
                open: row.open.unwrap(),
                high: row.high.unwrap(),
                low: row.low.unwrap(),
                close: row.close.unwrap(),
                volume: row.volume.unwrap(),
            };

            let bar_count = row.bar_count.unwrap() as f64;
            if (idx == 0 && bar_count >= (*timestep_minutes as f64 / 5.0)) // MUST HAVE most recent time bar
                || (idx >= 1 && bar_count >= (*timestep_minutes as f64 * 0.93 / 5.9))
            {
                full.push(bar);
            } else {
                incomplete.push(bar);
            }

            // if row.bar_count.unwrap() == (*timestep_minutes / 5) as i64 {
            //     full.push(bar);
            // } else {
            //     incomplete.push(bar);
            // }
        }

        full.reverse(); // chronological
        incomplete.reverse();

        Ok(AggregatedBars { full, incomplete })
    }

    pub async fn read_last_bar_of_stock(
        &self,
        stock: &str,
        primary_exchange: &str,
        currency: &str,
        timestep_minutes: &u32,
    ) -> Result<Option<HistoricalDataFullKeys>, String> {
        let last_bars = self
            .read_last_n_of_stock(stock, primary_exchange, currency, timestep_minutes, &1)
            .await?;
        if last_bars.incomplete.is_empty() {
            if let Some(bar) = last_bars.full.first() {
                Ok(Some(bar.clone()))
            } else {
                tracing::warn!(
                    "Error: read_last_n_bars from read_last_bar returned empty full list with empty incomplete list"
                );
                Ok(None)
            }
        } else {
            tracing::warn!("Last bar returned from read_last_n_bars is incomplete");
            if !last_bars.full.is_empty() {
                tracing::warn!(
                    "read_last_n_bars returned bars in full AND incomplete in read_last_bar!"
                );
                Ok(last_bars.full.last().cloned())
            } else {
                Ok(None)
            }
        }
    }

    // pub async fn read_last_n_of_stock(
    //     &self,
    //     stock: String,
    //     primary_exchange: String,
    //     limit: u32,
    // ) -> Result<Vec<HistoricalDataFullKeys>, String> {
    //     sqlx::query_as!(
    //         HistoricalDataFullKeys,
    //         r#"
    //         SELECT * FROM market_data.historical_data
    //         WHERE stock = $1
    //             AND primary_exchange = $2
    //         ORDER BY time DESC
    //         LIMIT $3;
    //         "#,
    //         stock,
    //         primary_exchange,
    //         limit as i32
    //     )
    //     .fetch_all(&self.crud.pool)
    //     .await.map_err(|e| {
    //         format!("Error when fetching most recent rows from HistoricalData in read_last_n_of_stock: {}", e)
    //     })
    // }
    //
    // pub async fn read_last_bar_of_stock(
    //     &self,
    //     stock: String,
    //     primary_exchange: String,
    // ) -> Result<Option<HistoricalDataFullKeys>, String> {
    //     sqlx::query_as!(
    //         HistoricalDataFullKeys,
    //         r#"
    //         SELECT * FROM market_data.historical_data
    //         WHERE stock = $1
    //             AND primary_exchange = $2
    //         ORDER BY time DESC
    //         LIMIT 1;
    //         "#,
    //         stock,
    //         primary_exchange
    //     )
    //     .fetch_optional(&self.crud.pool)
    //     .await
    //     .map_err(|e| {
    //         format!("Error when fetching most recent bar from HistoricalData for {} in read_last_bar_of_stock: {}", stock, e)
    //     })
    // }

    pub async fn read_vwap(
        &self,
        stock: &str,
        primary_exchange: &str,
        currency: &str,
    ) -> Result<Option<f64>, String> {
        let opt_vwap = sqlx::query_as!(
            OptionVWAP,
            r#"
            SELECT
                SUM(close * volume) / NULLIF(SUM(volume), 0) AS vwap
            FROM market_data.historical_data
            WHERE stock = $1
              AND primary_exchange = $2
              AND currency = $3
              -- Convert now() to Eastern, truncate to the day, then cast back to timestamptz
              AND time >= (now() AT TIME ZONE 'US/Eastern')::date
            GROUP BY stock;
            "#,
            stock,
            primary_exchange,
            currency
        )
        .fetch_optional(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error when fetching most recent bar from HistoricalData for {} in read_vwap: {}",
                stock, e
            )
        })?;
        Ok(opt_vwap.map(|row| row.vwap.unwrap()))
    }

    pub async fn has_at_least_n_rows_since(
        &self,
        stock: &str,
        primary_exchange: &str,
        currency: &str,
        datetime: &DateTime<Tz>,
        n: &u32,
    ) -> Result<bool, String> {
        match sqlx::query_scalar!(
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
        .fetch_one(&self.crud.pool)
        .await
        {
            Ok(has_at_least_n_rows) => Ok(has_at_least_n_rows.expect(
                "Expected sql query to return a boolean at least in has_at_least_n_rows_since",
            )),
            Err(e) => Err(format!(
                "Error when fetching most recent rows from HistoricalData in has_at_least_n_rows_since: {}",
                e
            )),
        }
    }

    pub async fn get_avg_move_since_open(
        &self,
        stock: &str,
        primary_exchange: &str,
        currency: &str,
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
            stock,
            primary_exchange,
            currency
        )
        .fetch_all(&self.crud.pool)
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

    pub async fn get_most_recent_daily_open(
        &self,
        stock: &str,
        primary_exchange: &str,
    ) -> Result<f64, String> {
        let most_recent_daily_close = sqlx::query_as!(
            OptionDailyOC,
            r#"
            SELECT day, open, close
            FROM market_data.daily_ohlcv
            WHERE stock = $1 AND primary_exchange = $2 AND day < $3
            ORDER BY day DESC
            LIMIT 1;
            "#,
            stock,
            primary_exchange,
            Utc::now()
        )
        .fetch_one(&self.crud.pool)
        .await
        .map(|most_recent_daily_open_option| DailyOC {
            _day: most_recent_daily_open_option.day.expect(&format!(
                "Expected at least 1 past entry for stock: {}",
                stock
            )),
            _open: most_recent_daily_open_option.open.expect(&format!(
                "Expected at least 1 past entry for stock: {}",
                stock
            )),
            close: most_recent_daily_open_option.close.expect(&format!(
                "Expected at least 1 past entry for stock: {}",
                stock
            )),
        })
        .map_err(|e| format!("Error when getting most recent daily close of stock: {}", e))?;

        let most_recent_daily_open_option = sqlx::query_scalar!(
            r#"
            SELECT open
            FROM market_data.historical_data
            WHERE stock = $1 AND time > $2 AND time < $3;
            "#,
            stock,
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
        .fetch_one(&self.crud.pool)
        .await
        .map_err(|e| format!("Error when getting most recent daily open of stock: {}", e))?;

        Ok(max::<OrderedFloat<f64>>(
                OrderedFloat::from(most_recent_daily_close.close),
                OrderedFloat::from(most_recent_daily_open_option),
        )
        .to_f64().expect("Expected close and open of the daily opens/close to be valid in get_most_recent_daily_open"))
    }

    pub async fn get_daily_vol(&self, stock: &str, primary_exchange: &str) -> Result<f64, String> {
        let daily_vol = sqlx::query_scalar!(
            r#"
            SELECT rolling_volatility
            FROM market_data.daily_volatility
            WHERE stock = $1
                AND primary_exchange = $2
            ORDER BY day DESC
            LIMIT 1;
        "#,
            stock,
            primary_exchange
        )
        .fetch_one(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error getting most recent daily volatility of {}: {}",
                stock, e
            )
        })?;
        Ok(daily_vol.expect(&format!(
            "Expected to have enough data to get volatility of stock: {}",
            stock
        )))
    }

    // refreshes daily ohlcv for all stocks for the past 30 days
    pub async fn refresh_daily_data(&self) -> Result<(), String> {
        sqlx::query!(
            r#"
            CALL refresh_continuous_aggregate(
                'market_data.daily_ohlcv',
                NOW() - INTERVAL '30 days',
                NOW()
            );
            "#,
        )
        .execute(&self.crud.pool)
        .await
        .map_err(|e| format!("Failed to refresh_continuous_aggregate for daily_ohlcv: {e:?}"))?;
        Ok(())
    }
}

pub fn get_historical_data_crud(
    pool: PgPool,
) -> CRUD<HistoricalDataFullKeys, HistoricalDataPrimaryKeys, HistoricalDataUpdateKeys> {
    CRUD::<HistoricalDataFullKeys, HistoricalDataPrimaryKeys, HistoricalDataUpdateKeys>::new(
        pool,
        String::from("market_data.historical_data"),
    )
}

pub fn get_specific_historical_data_crud(pool: PgPool) -> HistoricalDataCRUD {
    HistoricalDataCRUD::new(pool)
}
