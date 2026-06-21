use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use chrono::DateTime;
use chrono_tz::Tz;
use rand::{Rng, distr::Alphanumeric};
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
        models::{
            DailyHistoricalDataFullKeys, DailyHistoricalDataPrimaryKeys,
            DailyHistoricalDataUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

#[derive(Clone, Debug)]
pub struct DailyHistoricalDataCRUD {
    crud: CRUD<
        DailyHistoricalDataFullKeys,
        DailyHistoricalDataPrimaryKeys,
        DailyHistoricalDataUpdateKeys,
    >,
    sender: Arc<Mutex<Option<Arc<Sender<DailyHistoricalDataFullKeys>>>>>,
    shutdown_sender: Arc<Mutex<Option<Arc<Sender<bool>>>>>,
    shutdown_resp_rcx: Arc<Mutex<Option<Receiver<bool>>>>,

    num_channel_connections: Arc<Mutex<u32>>,
    notify_initialised: Arc<Notify>,
}

// struct OptionDailyOC {
//     day: Option<DateTime<Utc>>,
//     open: Option<f64>,
//     close: Option<f64>,
// }
//
// struct DailyOC {
//     day: DateTime<Utc>,
//     open: f64,
//     close: f64,
// }
//
// struct OptionVWAP {
//     vwap: Option<f64>,
// }

async fn init_channel() -> (
    Arc<Sender<DailyHistoricalDataFullKeys>>,
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

    let (sender, mut rx) = channel::<DailyHistoricalDataFullKeys>(100_000);
    let (shutdown_sender, mut shutdown_rx) = channel::<bool>(2);
    let (shutdown_resp_sender, shutdown_resp_rx) = channel::<bool>(2);

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
                                if let Err(e) = DailyHistoricalDataCRUD::flush_batch(&mut client, &buffer).await {
                                    tracing::error!("Expected to be able to flush batch: {e:?}", );
                                }
                                buffer.clear();
                                last_flush = Instant::now();
                            }
                        }
                        None => {
                            if !buffer.is_empty() {
                                if let Err(e) = DailyHistoricalDataCRUD::flush_batch(&mut client, &buffer).await {
                                    tracing::error!("Expected to be able to flush batch: {e:?}");
                                }
                            }
                            if let Err(e) = shutdown_resp_sender.send(true).await {
                                tracing::warn!("Could not send update to shutdown_resp_sender in historical_options_data: {e:?}")
                            };
                            break;
                        }
                    }
                }
                maybe_shutdown = shutdown_rx.recv() => {
                    if let Some(to_shutdown) = maybe_shutdown {
                        if to_shutdown {
                            if !buffer.is_empty() {
                                if let Err(e) = DailyHistoricalDataCRUD::flush_batch(&mut client, &buffer).await {
                                    tracing::error!("Expected to be able to flush batch: {e:?}");
                                }
                            }
                            drop(client);
                            if let Err(e) = shutdown_resp_sender.send(true).await {
                                tracing::warn!("Could not send update to shutdown_resp_sender in historical_options_data: {e:?}")
                            };
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(MAX_BATCH_WAIT_MS)) => {
                    if !buffer.is_empty() && last_flush.elapsed().as_millis() as u64 >= MAX_BATCH_WAIT_MS {
                        if let Err(e) = DailyHistoricalDataCRUD::flush_batch(&mut client, &buffer).await {
                            tracing::error!("Expected to be able to flush batch: \n{}", e);
                        }
                        buffer.clear();
                        last_flush = Instant::now();
                    }
                }
            }
        }
    });

    (
        Arc::new(sender),
        Arc::new(shutdown_sender),
        shutdown_resp_rx,
    )
}

impl DailyHistoricalDataCRUD {
    async fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                DailyHistoricalDataFullKeys,
                DailyHistoricalDataPrimaryKeys,
                DailyHistoricalDataUpdateKeys,
            >::new(pool, String::from("market_data.daily_historical_data")),
            sender: Arc::new(Mutex::new(None)),
            shutdown_sender: Arc::new(Mutex::new(None)),
            shutdown_resp_rcx: Arc::new(Mutex::new(None)),

            num_channel_connections: Arc::new(Mutex::new(0)),
            notify_initialised: Arc::new(Notify::new()),
        }
    }

    /// Flush one batch with COPY + staging + merge (like the function I showed you before)
    async fn flush_batch(
        client: &mut tokio_postgres::Client,
        batch: &[DailyHistoricalDataFullKeys],
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
                time TIMESTAMPTZ,
                open NUMERIC(30, 15),
                high NUMERIC(30, 15),
                low NUMERIC(30, 15), 
                close NUMERIC(30, 15), 
                volume NUMERIC(30, 6)
            ) ON COMMIT DROP;",
            st = &staging_table,
        );
        tx.batch_execute(&create_sql).await?;

        let copy_sql = format!(
            "COPY {st} (stock, time, open, high, low, close, volume) FROM STDIN WITH (FORMAT binary)",
            st = &staging_table,
        );

        let sink = tx.copy_in(&copy_sql).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
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

        let mut unique_keys = HashSet::new();
        for row in batch {
            if unique_keys.contains(&(&row.stock, &row.time)) {
                continue;
            };
            unique_keys.insert((&row.stock, &row.time));
            writer
                .as_mut()
                .write(&[
                    &row.stock,
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
                "Error trying to wait for writer to finish writing batch in daily historical data: {e:?}"
            );
        };

        let merge_sql = format!(
            r#"
            INSERT INTO market_data.daily_historical_data (stock, time, open, high, low, close, volume)
            SELECT stock, time, open, high, low, close, volume FROM {st}
            ON CONFLICT (stock, time)
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
        DailyHistoricalDataFullKeys,
        DailyHistoricalDataPrimaryKeys,
        DailyHistoricalDataUpdateKeys
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

        // Shutdown process for final user
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

    pub async fn batch_create_or_update(
        &self,
        fk: &DailyHistoricalDataFullKeys,
    ) -> Result<(), String> {
        let sender = self
            .sender
            .lock()
            .expect("Expected to be able to acquire sender lock")
            .clone()
            .expect("Expected channel to be initialised before batch_create_or_update");
        if let Err(e) = sender
            .send_timeout(fk.clone(), tokio::time::Duration::from_millis(200))
            .await
        {
            tracing::error!("error sending data via batch_create_or_update: {e:?}");
        };
        Ok(())
    }

    pub async fn read_last_n_of_stock(
        &self,
        stock: &str,
        limit: &u32,
    ) -> Result<Vec<DailyHistoricalDataFullKeys>, String> {
        sqlx::query_as!(
            DailyHistoricalDataFullKeys,
            r#"
            SELECT * FROM market_data.daily_historical_data
            WHERE stock = $1
            ORDER BY time DESC
            LIMIT $2;
            "#,
            stock,
            *limit as i32
        )
        .fetch_all(&self.crud.pool)
        .await.map_err(|e| {
            format!("Error when fetching most recent rows from DailyHistoricalData in read_last_n_of_stock: {}", e)
        })
    }

    pub async fn read_last_bar_of_stock(
        &self,
        stock: &str,
    ) -> Result<Option<DailyHistoricalDataFullKeys>, String> {
        sqlx::query_as!(
            DailyHistoricalDataFullKeys,
            r#"
            SELECT * FROM market_data.daily_historical_data
            WHERE stock = $1
            ORDER BY time DESC
            LIMIT 1;
            "#,
            stock
        )
        .fetch_optional(&self.crud.pool)
        .await
        .map_err(|e| {
            format!("Error when fetching most recent bar from DailyHistoricalData for {} in read_last_bar_of_stock: {}", stock, e)
        })
    }

    pub async fn has_at_least_n_rows_since(
        &self,
        stock: String,
        datetime: DateTime<Tz>,
        n: u32,
    ) -> Result<bool, String> {
        match sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) > $1 
            FROM market_data.daily_historical_data
            WHERE stock = $2 AND time > $3;
            "#,
            (n - 1) as i32,
            stock,
            datetime
        )
        .fetch_one(&self.crud.pool)
        .await
        {
            Ok(has_at_least_n_rows) => Ok(has_at_least_n_rows.expect(
                "Expected sql query to return a boolean at least in has_at_least_n_rows_since",
            )),
            Err(e) => Err(format!(
                "Error when fetching most recent rows from DailyHistoricalData in has_at_least_n_rows_since: {}",
                e
            )),
        }
    }

    pub async fn has_at_least_1_entry(&self) -> Result<bool, String> {
        match sqlx::query_scalar!(
            r#"SELECT EXISTS (SELECT 1 FROM market_data.daily_historical_data LIMIT 1);"#
        )
        .fetch_one(&self.crud.pool)
        .await
        {
            Ok(has_entry) => {
                Ok(has_entry.expect("Expected a bool return value for existence query"))
            }
            Err(e) => Err(format!(
                "Error checking for existence of rows in daily_historical_data: {}",
                e
            )),
        }
    }

    pub async fn read_all_bars_of_stock(
        &self,
        stock: &str,
    ) -> Result<Vec<DailyHistoricalDataFullKeys>, String> {
        match sqlx::query_as!(
            DailyHistoricalDataFullKeys,
            r#"
            SELECT *
            FROM market_data.daily_historical_data
            WHERE stock = $1
            ORDER BY time ASC;
            "#,
            stock
        )
        .fetch_all(&self.crud.pool)
        .await
        {
            Ok(has_entry) => Ok(has_entry),
            Err(e) => Err(format!(
                "Error checking for existence of rows in daily_historical_data: {}",
                e
            )),
        }
    }
}

pub fn get_daily_historical_data_crud(
    pool: PgPool,
) -> CRUD<DailyHistoricalDataFullKeys, DailyHistoricalDataPrimaryKeys, DailyHistoricalDataUpdateKeys>
{
    CRUD::<DailyHistoricalDataFullKeys, DailyHistoricalDataPrimaryKeys, DailyHistoricalDataUpdateKeys>::new(
        pool,
        String::from("market_data.daily_historical_data"),
    )
}

pub async fn get_specific_daily_historical_data_crud(pool: PgPool) -> DailyHistoricalDataCRUD {
    DailyHistoricalDataCRUD::new(pool).await
}
