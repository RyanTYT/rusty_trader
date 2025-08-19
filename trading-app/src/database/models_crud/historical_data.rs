use std::{
    cmp::max,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Timelike, Utc};
use chrono_tz::{America::New_York, Tz};
use ordered_float::OrderedFloat;
use rand::{Rng, distr::Alphanumeric};
use rust_decimal::prelude::ToPrimitive;
use sqlx::PgPool;
use tokio::{
    sync::mpsc::{Sender, channel},
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
}

async fn init_channel() -> (Arc<Sender<HistoricalDataFullKeys>>, Arc<Sender<bool>>) {
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
                                    tracing::error!("Expected to be able to flush batch: \n{}", e);
                                }
                                buffer.clear();
                                last_flush = Instant::now();
                            }
                        }
                        None => {
                            if !buffer.is_empty() {
                                if let Err(e) = HistoricalDataCRUD::flush_batch(&mut client, &buffer).await {
                                    tracing::error!("Expected to be able to flush batch: \n{}", e);
                                }
                            }
                            break;
                        }
                    }
                }
                maybe_shutdown = shutdown_rx.recv() => {
                    if let Some(to_shutdown) = maybe_shutdown {
                        if to_shutdown {
                            if !buffer.is_empty() {
                                if let Err(e) = HistoricalDataCRUD::flush_batch(&mut client, &buffer).await {
                                    tracing::error!("Expected to be able to flush batch: \n{}", e);
                                }
                            }
                            drop(client);
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(MAX_BATCH_WAIT_MS)) => {
                    if !buffer.is_empty() && last_flush.elapsed().as_millis() as u64 >= MAX_BATCH_WAIT_MS {
                        if let Err(e) = HistoricalDataCRUD::flush_batch(&mut client, &buffer).await {
                            tracing::error!("Expected to be able to flush batch: \n{}", e);
                        }
                        buffer.clear();
                        last_flush = Instant::now();
                    }
                }
            }
        }
        tracing::info!("loop to receive goods ended");
    });

    (Arc::new(sender), Arc::new(shutdown_sender))
}

struct OptionDailyOC {
    day: Option<DateTime<Utc>>,
    open: Option<f64>,
    close: Option<f64>,
}

struct DailyOC {
    day: DateTime<Utc>,
    open: f64,
    close: f64,
}

struct OptionVWAP {
    vwap: Option<f64>,
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
            "COPY {st} (stock, primary_exchange, time, open, high, low, close, volume) FROM STDIN WITH (FORMAT binary)",
            st = &staging_table,
        );

        let sink = tx.copy_in(&copy_sql).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
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

        for row in batch {
            writer
                .as_mut()
                .write(&[
                    &row.stock,
                    &row.primary_exchange,
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
        writer.finish().await;

        let merge_sql = format!(
            r#"
            INSERT INTO market_data.historical_data (stock, primary_exchange, time, open, high, low, close, volume)
            SELECT stock, primary_exchange, time, open, high, low, close, volume FROM {st}
            ON CONFLICT (stock, primary_exchange, time)
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

    pub async fn init_channel(&self) {
        let (sender, shutdown_sender) = init_channel().await;
        self.sender
            .lock()
            .expect("Expected to be able to acquire sender lock")
            .replace(sender);
        self.shutdown_sender
            .lock()
            .expect("Expected to be able to acquire shutdown_sender lock")
            .replace(shutdown_sender);
    }

    pub async fn close_channel(&self) {
        let sender_guard = self
            .shutdown_sender
            .lock()
            .expect("Expected to be able to acquire lock for shutdown_sender")
            .take();
        if let Some(sender) = sender_guard {
            sender.send(true).await;
        }
    }

    pub async fn batch_create_or_update(&self, fk: &HistoricalDataFullKeys) -> Result<(), String> {
        let sender = self
            .sender
            .lock()
            .expect("Expected to be able to acquire sender lock")
            .clone()
            .expect("Expected channel to be initialised before batch_create_or_update");
        sender.send(fk.clone()).await;
        Ok(())
    }

    pub async fn read_last_n_of_stock(
        &self,
        stock: String,
        primary_exchange: String,
        limit: u32,
    ) -> Result<Vec<HistoricalDataFullKeys>, String> {
        sqlx::query_as!(
            HistoricalDataFullKeys,
            r#"
            SELECT * FROM market_data.historical_data
            WHERE stock = $1
                AND primary_exchange = $2
            ORDER BY time DESC
            LIMIT $3;
            "#,
            stock,
            primary_exchange,
            limit as i32
        )
        .fetch_all(&self.crud.pool)
        .await.map_err(|e| {
            format!("Error when fetching most recent rows from HistoricalData in read_last_n_of_stock: {}", e)
        })
    }

    pub async fn read_last_bar_of_stock(
        &self,
        stock: String,
        primary_exchange: String,
    ) -> Result<Option<HistoricalDataFullKeys>, String> {
        sqlx::query_as!(
            HistoricalDataFullKeys,
            r#"
            SELECT * FROM market_data.historical_data
            WHERE stock = $1
                AND primary_exchange = $2
            ORDER BY time DESC
            LIMIT 1;
            "#,
            stock,
            primary_exchange
        )
        .fetch_optional(&self.crud.pool)
        .await
        .map_err(|e| {
            format!("Error when fetching most recent bar from HistoricalData for {} in read_last_bar_of_stock: {}", stock, e)
        })
    }

    pub async fn read_vwap(&self, stock: String, primary_exchange: String) -> Result<f64, String> {
        let opt_vwap = sqlx::query_as!(
            OptionVWAP,
            r#"
            SELECT
                SUM(close * volume) / NULLIF(SUM(volume), 0) AS vwap
            FROM market_data.historical_data
            WHERE stock = $1
              AND primary_exchange = $2
              AND time >= date_trunc('day', now())
            GROUP BY stock;
            "#,
            stock,
            primary_exchange
        )
        .fetch_optional(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error when fetching most recent bar from HistoricalData for {} in read_vwap: {}",
                stock, e
            )
        })?;

        Ok(opt_vwap
            .expect(&format!(
                "Expected enough data to calculate VWAP price of stock: {}",
                stock
            ))
            .vwap
            .expect(&format!(
                "Expected row to contain VWAP for stock: {}",
                stock
            )))
    }

    pub async fn has_at_least_n_rows_since(
        &self,
        stock: String,
        primary_exchange: String,
        datetime: DateTime<Tz>,
        n: u32,
    ) -> Result<bool, String> {
        match sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) > $1 
            FROM market_data.historical_data
            WHERE stock = $2 AND primary_exchange = $3 AND time > $4;
            "#,
            (n - 1) as i32,
            stock,
            primary_exchange,
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
        stock: String,
        primary_exchange: String,
    ) -> Result<f64, String> {
        match sqlx::query_scalar!(
            r#"
            WITH latest_bar_time AS (
                SELECT
                    time::time AS latest_close
                FROM
                    market_data.historical_data
                WHERE
                    stock = $1
                AND 
                    primary_exchange = $2
                ORDER BY
                    time DESC
                LIMIT 1
            ),
            historical_matches AS (
                SELECT
                    h.stock,
                    h.primary_exchange,
                    h.time::date AS trading_day,
                    h.time,
                    h.close
                FROM market_data.historical_data h
                JOIN latest_bar_time lb ON h.time::time = lb.latest_close
                WHERE h.stock = $1
                    AND primary_exchange = $2
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
            primary_exchange
        )
        .fetch_one(&self.crud.pool)
        .await
        {
            Ok(has_at_least_n_rows) => Ok(has_at_least_n_rows.expect(
                "Expected sql query to return a boolean at least in has_at_least_n_rows_since",
            )),
            Err(e) => Err(format!(
                "Error when fetching most recent rows from HistoricalData in read_last_n_of_stock: {}",
                e
            )),
        }
    }

    pub async fn get_most_recent_daily_open(
        &self,
        stock: String,
        primary_exchange: String,
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
            day: most_recent_daily_open_option.day.expect(&format!(
                "Expected at least 1 past entry for stock: {}",
                stock
            )),
            open: most_recent_daily_open_option.open.expect(&format!(
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

    pub async fn get_daily_vol(&self, stock: String, primary_exchange: String) -> Result<f64, String> {
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
        .map_err(|e| format!("Failed to refresh_continuous_aggregate for daily_ohlcv"))?;
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
