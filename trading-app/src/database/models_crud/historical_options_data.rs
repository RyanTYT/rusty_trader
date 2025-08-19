use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use rand::{Rng, distr::Alphanumeric};
use rust_decimal::Decimal;
use sqlx::{PgPool, prelude::FromRow};
use tokio::{
    sync::mpsc::{Sender, channel},
    time::Instant,
};
use tokio_postgres::{NoTls, binary_copy::BinaryCopyInWriter};

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            HistoricalOptionsDataFullKeys, HistoricalOptionsDataPrimaryKeys,
            HistoricalOptionsDataUpdateKeys, OptionType,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Debug, Clone)]
pub struct HistoricalOptionsDataCRUD {
    crud: CRUD<
        HistoricalOptionsDataFullKeys,
        HistoricalOptionsDataPrimaryKeys,
        HistoricalOptionsDataUpdateKeys,
    >,
    sender: Arc<Mutex<Option<Arc<Sender<HistoricalOptionsDataFullKeys>>>>>,
    shutdown_sender: Arc<Mutex<Option<Arc<Sender<bool>>>>>,
}

async fn init_channel() -> (
    Arc<Sender<HistoricalOptionsDataFullKeys>>,
    Arc<Sender<bool>>,
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

    let (sender, mut rx) = channel::<HistoricalOptionsDataFullKeys>(10_000);
    let (shutdown_sender, mut shutdown_rx) = channel::<bool>(2);

    tokio::spawn(async move {
        let mut buffer = Vec::with_capacity(BATCH_SIZE);
        let mut last_flush = Instant::now();

        loop {
            tokio::select! {
                maybe_row = rx.recv() => {
                    match maybe_row {
                        Some(row) => {
                            buffer.push(row);
                            if buffer.len() >= BATCH_SIZE {
                                if let Err(e) = HistoricalOptionsDataCRUD::flush_batch(&mut client, &buffer).await {
                                    tracing::error!("Expected to be able to flush batch: \n{}", e);
                                }
                                buffer.clear();
                                last_flush = Instant::now();
                            }
                        }
                        None => {
                            if !buffer.is_empty() {
                                if let Err(e) = HistoricalOptionsDataCRUD::flush_batch(&mut client, &buffer).await {
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
                            drop(client);
                        }
                        break;
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(MAX_BATCH_WAIT_MS)) => {
                    if !buffer.is_empty() && last_flush.elapsed().as_millis() as u64 >= MAX_BATCH_WAIT_MS {
                        if let Err(e) = HistoricalOptionsDataCRUD::flush_batch(&mut client, &buffer).await {
                            tracing::error!("Expected to be able to flush batch: \n{}", e);
                        }
                        buffer.clear();
                        last_flush = Instant::now();
                    }
                }
            }
        }
    });

    (Arc::new(sender), Arc::new(shutdown_sender))
}

#[derive(Debug, Clone, FromRow)]
pub struct OptionalHistoricalOptionsData {
    pub stock: Option<String>,
    pub primary_exchange: Option<String>,
    pub expiry: Option<String>,
    pub strike: Option<f64>,
    pub multiplier: Option<String>,
    pub option_type: Option<OptionType>,
    pub time: Option<DateTime<Utc>>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume: Option<Decimal>,
}

impl HistoricalOptionsDataCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                HistoricalOptionsDataFullKeys,
                HistoricalOptionsDataPrimaryKeys,
                HistoricalOptionsDataUpdateKeys,
            >::new(pool, String::from("market_data.historical_options_data")),
            sender: Arc::new(Mutex::new(None)),
            shutdown_sender: Arc::new(Mutex::new(None)),
        }
    }

    async fn flush_batch(
        client: &mut tokio_postgres::Client,
        batch: &[HistoricalOptionsDataFullKeys],
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
                expiry VARCHAR(20),
                strike DOUBLE PRECISION,
                multiplier VARCHAR(50),
                option_type option_type,
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
            "COPY {st} (stock, primary_exchange, expiry, strike, multiplier, option_type, time, open, high, low, close, volume) FROM STDIN WITH (FORMAT binary)",
            st = &staging_table,
        );

        let sink = tx.copy_in(&copy_sql).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                tokio_postgres::types::Type::VARCHAR,
                tokio_postgres::types::Type::VARCHAR,
                tokio_postgres::types::Type::VARCHAR,
                tokio_postgres::types::Type::NUMERIC,
                tokio_postgres::types::Type::VARCHAR,
                tokio_postgres::types::Type::VARCHAR,
                tokio_postgres::types::Type::TIMESTAMPTZ,
                tokio_postgres::types::Type::NUMERIC,
                tokio_postgres::types::Type::NUMERIC,
                tokio_postgres::types::Type::NUMERIC,
                tokio_postgres::types::Type::NUMERIC,
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
                    &row.expiry,
                    &row.strike,
                    &row.multiplier,
                    match &row.option_type {
                        OptionType::Put => &"P",
                        OptionType::Call => &"C",
                    },
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
            INSERT INTO market_data.historical_options_data (stock, primary_exchange, expiry, strike, multiplier, option_type, time, open, high, low, close, volume)
            SELECT stock, primary_exchange, expiry, strike, multiplier, option_type, time, open, high, low, close, volume FROM {st}
            ON CONFLICT (stock, primary_exchange, expiry, strike, multiplier, option_type, time)
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
        HistoricalOptionsDataFullKeys,
        HistoricalOptionsDataPrimaryKeys,
        HistoricalOptionsDataUpdateKeys
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

    pub async fn batch_create_or_update(
        &self,
        fk: &HistoricalOptionsDataFullKeys,
    ) -> Result<(), String> {
        let sender = self
            .sender
            .lock()
            .expect("Expected to be able to acquire sender lock")
            .clone()
            .expect("Expected channel to be initialised before batch_create_or_update");
        sender.send(fk.clone()).await;
        Ok(())
    }

    pub async fn read_last_bar_of_contract(
        &self,
        stock: String,
        primary_exchange: String,
        expiry: String,
        strike: f64,
        multiplier: String,
        option_type: OptionType,
    ) -> Result<Option<HistoricalOptionsDataFullKeys>, String> {
        // sqlx::query_as!(
        //     OptionalHistoricalOptionsData,
        //     r#"
        //     SELECT *
        //     FROM market_data.historical_options_data
        //     WHERE stock = $1
        //         AND expiry = $2
        //         AND strike = $3
        //         AND multiplier = $4
        //     ORDER BY time
        //     LIMIT 1;
        //     "#,
        //     stock,
        //     expiry,
        //     strike,
        //     multiplier,
        // )
        // .fetch_optional(&self.crud.pool)
        // .await
        // .map_err(|e| {
        //     format!("Error when fetching most recent bar from HistoricalOptionsData for {} in read_last_n_of_stock: {}", stock, e)
        // })
        let row = sqlx::query_as::<_, HistoricalOptionsDataFullKeys>(
            r#"
            SELECT * FROM market_data.historical_options_data
            WHERE stock = $1
              AND primary_exchange = $2
              AND expiry = $3
              AND multiplier = $4
              AND strike = $5
              AND option_type = $6::option_type
            ORDER BY time
            LIMIT 1;
            "#,
        )
        .bind(stock)
        .bind(primary_exchange)
        .bind(expiry)
        .bind(multiplier)
        .bind(strike)
        .bind(option_type)
        .fetch_optional(&self.crud.pool)
        .await
        .map_err(|e| {
            format!(
                "Error trying to get last bar of historical options contract: {}",
                e
            )
        })?;
        Ok(row)
    }

    pub async fn has_at_least_n_rows_since(
        &self,
        stock: String,
        primary_exchange: String,
        expiry: String,
        strike: f64,
        multiplier: String,
        option_type: OptionType,
        datetime: DateTime<Tz>,
        n: u32,
    ) -> Result<bool, String> {
        match sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) > $1 
            FROM market_data.historical_options_data
            WHERE stock = $2 
                AND primary_exchange = $3
                AND expiry = $4 
                AND strike = $5
                AND multiplier = $6
                AND option_type = $7
                AND time > $8;
            "#,
            (n - 1) as i32,
            stock,
            primary_exchange,
            expiry,
            strike,
            multiplier,
            option_type as OptionType,
            datetime
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
}

pub fn get_historical_options_data_crud(
    pool: PgPool,
) -> CRUD<
    HistoricalOptionsDataFullKeys,
    HistoricalOptionsDataPrimaryKeys,
    HistoricalOptionsDataUpdateKeys,
> {
    CRUD::<
        HistoricalOptionsDataFullKeys,
        HistoricalOptionsDataPrimaryKeys,
        HistoricalOptionsDataUpdateKeys,
    >::new(pool, String::from("market_data.historical_options_data"))
}

pub fn get_specific_historical_options_data_crud(pool: PgPool) -> HistoricalOptionsDataCRUD {
    HistoricalOptionsDataCRUD::new(pool)
}
