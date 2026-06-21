use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use chrono::DateTime;
use chrono_tz::Tz;
use rand::{Rng, distr::Alphanumeric};
use sqlx::PgPool;
use tokio::sync::Notify;
use tokio::sync::mpsc::Receiver;
use tokio::{
    sync::mpsc::{Sender, channel},
    time::Instant,
};
use tokio_postgres::{NoTls, binary_copy::BinaryCopyInWriter};

use crate::{
    database::{
        crud::{CRUD, CRUDTrait},
        models::{
            HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeys,
            HistoricalForexDataUpdateKeys,
        },
    },
    delegate_all_crud_methods,
};

// fn map_to_placeholder(key: usize, column_name: &str) -> String {
//     match column_name {
//         "asset_type" => format!("${}::asset_type", key),
//         "status" => format!("${}::status", key),
//         "option_type" => format!("${}::option_type", key),
//         _ => format!("${}", key),
//     }
// }
//
// #[derive(Debug, Clone)]
// struct BarCompleteResult {
//     is_complete: bool,
// }

#[derive(Clone, Debug)]
pub struct HistoricalForexDataCRUD {
    crud: CRUD<
        HistoricalForexDataFullKeys,
        HistoricalForexDataPrimaryKeys,
        HistoricalForexDataUpdateKeys,
    >,
    sender: Arc<Mutex<Option<Arc<Sender<HistoricalForexDataFullKeys>>>>>,
    shutdown_sender: Arc<Mutex<Option<Arc<Sender<bool>>>>>,
    shutdown_resp_rcx: Arc<Mutex<Option<Receiver<bool>>>>,

    num_channel_connections: Arc<Mutex<u32>>,
    notify_initialised: Arc<Notify>,
}

async fn init_channel() -> (
    Arc<Sender<HistoricalForexDataFullKeys>>,
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

    let (sender, mut rx) = channel::<HistoricalForexDataFullKeys>(10_000);
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
                                if let Err(e) = HistoricalForexDataCRUD::flush_batch(&mut client, &buffer).await {
                                    tracing::error!("Expected to be able to flush batch: \n{e:?}");
                                }
                                buffer.clear();
                                last_flush = Instant::now();
                            }
                        }
                        None => {
                            if !buffer.is_empty() {
                                if let Err(e) = HistoricalForexDataCRUD::flush_batch(&mut client, &buffer).await {
                                    tracing::error!("Expected to be able to flush batch: \n{e:?}");
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
                                if let Err(e) = HistoricalForexDataCRUD::flush_batch(&mut client, &buffer).await {
                                    tracing::error!("Expected to be able to flush batch: \n{e:?}");
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
                        if let Err(e) = HistoricalForexDataCRUD::flush_batch(&mut client, &buffer).await {
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

#[derive(Debug, Clone)]
pub struct AggregatedBars {
    pub full: Vec<HistoricalForexDataFullKeys>,
    pub incomplete: Vec<HistoricalForexDataFullKeys>,
}

impl HistoricalForexDataCRUD {
    fn new(pool: PgPool) -> Self {
        Self {
            crud: CRUD::<
                HistoricalForexDataFullKeys,
                HistoricalForexDataPrimaryKeys,
                HistoricalForexDataUpdateKeys,
            >::new(pool, String::from("market_data.historical_forex_data")),
            sender: Arc::new(Mutex::new(None)),
            shutdown_sender: Arc::new(Mutex::new(None)),
            shutdown_resp_rcx: Arc::new(Mutex::new(None)),

            num_channel_connections: Arc::new(Mutex::new(0)),
            notify_initialised: Arc::new(Notify::new()),
        }
    }

    async fn flush_batch(
        client: &mut tokio_postgres::Client,
        batch: &[HistoricalForexDataFullKeys],
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
                pair VARCHAR(30) NOT NULL,
                time TIMESTAMPTZ NOT NULL,

                bid_open DOUBLE PRECISION,
                bid_high DOUBLE PRECISION,
                bid_low DOUBLE PRECISION,
                bid_close DOUBLE PRECISION,
                ask_open DOUBLE PRECISION,
                ask_high DOUBLE PRECISION,
                ask_low DOUBLE PRECISION,
                ask_close DOUBLE PRECISION

            ) ON COMMIT DROP;",
            st = &staging_table,
        );
        tx.batch_execute(&create_sql).await?;

        let copy_sql = format!(
            "COPY {st} (pair, time, bid_open, bid_high, bid_low, bid_close, ask_open, ask_high, ask_low, ask_close) FROM STDIN WITH (FORMAT binary)",
            st = &staging_table,
        );

        let sink = tx.copy_in(&copy_sql).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                tokio_postgres::types::Type::VARCHAR,
                tokio_postgres::types::Type::TIMESTAMPTZ,
                tokio_postgres::types::Type::FLOAT8,
                tokio_postgres::types::Type::FLOAT8,
                tokio_postgres::types::Type::FLOAT8,
                tokio_postgres::types::Type::FLOAT8,
                tokio_postgres::types::Type::FLOAT8,
                tokio_postgres::types::Type::FLOAT8,
                tokio_postgres::types::Type::FLOAT8,
                tokio_postgres::types::Type::FLOAT8,
            ],
        );
        tokio::pin!(writer);

        let mut unique_keys = HashSet::new();
        for row in batch {
            if unique_keys.contains(&(&row.pair, &row.time)) {
                continue;
            };
            unique_keys.insert((&row.pair, &row.time));
            writer
                .as_mut()
                .write(&[
                    &row.pair,
                    &row.time,
                    &row.bid_open,
                    &row.bid_high,
                    &row.bid_low,
                    &row.bid_close,
                    &row.ask_open,
                    &row.ask_high,
                    &row.ask_low,
                    &row.ask_close,
                ])
                .await
                .map_err(|e| anyhow::Error::msg(format!("{}", e)))?;
        }
        if let Err(e) = writer.finish().await {
            tracing::warn!(
                "Error trying to wait for writer to finish writing batch in historical forex data: {e:?}"
            );
        };

        let merge_sql = format!(
            r#"
            INSERT INTO market_data.historical_forex_data (pair, time, bid_open, bid_high, bid_low, bid_close, ask_open, ask_high, ask_low, ask_close)
            SELECT pair, time, bid_open, bid_high, bid_low, bid_close, ask_open, ask_high, ask_low, ask_close FROM {st}
            ON CONFLICT (pair, time)
            DO UPDATE 
            SET 
                bid_open = EXCLUDED.bid_open, 
                bid_high = EXCLUDED.bid_high,
                bid_low = EXCLUDED.bid_low,
                bid_close = EXCLUDED.bid_close,
                ask_open = EXCLUDED.ask_open, 
                ask_high = EXCLUDED.ask_high,
                ask_low = EXCLUDED.ask_low,
                ask_close = EXCLUDED.ask_close;
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
        HistoricalForexDataFullKeys,
        HistoricalForexDataPrimaryKeys,
        HistoricalForexDataUpdateKeys
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

        // Notify all waiting tasks
        self.notify_initialised.notify_waiters();
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
        {
            self.sender
                .lock()
                .expect("Expected sender lock to not be poisoned")
                .take();
        }

        if let Err(e) = sender.send(true).await {
            tracing::error!("error sending shutdown command: {e:?}");
        }
        // Wait for acknowledgement / channel close
        let _ = shutdown_resp_rcx.recv().await;
    }

    pub async fn batch_create_or_update(
        &self,
        fk: &HistoricalForexDataFullKeys,
    ) -> Result<(), String> {
        let sender = self
            .sender
            .lock()
            .expect("Expected to be able to acquire sender lock")
            .clone()
            .expect("Expected channel to be initialised before batch_create_or_update");
        if let Err(e) = sender.send(fk.clone()).await {
            tracing::error!(message=%format!("error sending batch_create_or_update data: {e:?}"));
        };
        Ok(())
    }

    // pub async fn read_last_n_bars(
    //     &self,
    //     pair: String,
    //     timestep: u32,
    //     limit: u32,
    // ) -> Result<Vec<HistoricalForexDataFullKeys>, String> {
    //     sqlx::query_as!(
    //         HistoricalForexDataFullKeys,
    //         r#"
    //         SELECT * FROM market_data.historical_forex_data
    //         WHERE pair = $1 AND bid_open IS NOT NULL AND ask_open IS NOT NULL
    //         ORDER BY time DESC
    //         LIMIT $2;
    //         "#,
    //         pair,
    //         limit as i32
    //     )
    //     .fetch_all(&self.crud.pool)
    //     .await
    //     .map_err(|e| {
    //         format!("Error when fetching most recent rows from HistoricalForexData in read_last_n_of_stock: {}", e)
    //     })
    // }

    pub async fn read_last_n_bars(
        &self,
        pair: &str,
        timestep_minutes: &u32,
        limit: &u32,
    ) -> Result<AggregatedBars, String> {
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
            *timestep_minutes as i32,
            *limit as i32,
        )
        .fetch_all(&self.crud.pool)
        .await
        .map_err(|e| format!("Error aggregating bars in read_last_n_bars: {}", e))?;

        let mut full = Vec::new();
        let mut incomplete = Vec::new();

        for (idx, row) in rows.iter().enumerate() {
            let bar = HistoricalForexDataFullKeys {
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
            };

            let bar_count = row.bar_count.unwrap() as f64;
            // tracing::info!(
            //     "bar_count: {bar_count:?}, timestep: {:?}, bool: {:?}",
            //     timestep_minutes,
            //     bar_count >= *timestep_minutes as f64
            // );
            if (idx == 0 && bar_count >= *timestep_minutes as f64) // MUST HAVE most recent time bar
                || (idx >= 1 && bar_count >= (*timestep_minutes as f64 * 0.93))
            {
                full.push(bar);
            } else {
                incomplete.push(bar);
            }
        }

        full.reverse(); // chronological
        incomplete.reverse();

        Ok(AggregatedBars { full, incomplete })
    }

    pub async fn read_last_bar(
        &self,
        pair: &str,
        timestep_minutes: &u32,
    ) -> Result<Option<HistoricalForexDataFullKeys>, String> {
        let last_bars = self.read_last_n_bars(pair, timestep_minutes, &1).await?;
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

    pub async fn has_at_least_n_rows_since(
        &self,
        pair: &str,
        datetime: &DateTime<Tz>,
        n: &u32,
    ) -> Result<bool, String> {
        match sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) > $1 
            FROM market_data.historical_forex_data
            WHERE pair = $2 AND bid_open IS NOT NULL AND ask_open IS NOT NULL AND time > $3;
            "#,
            (n - 1) as i32,
            pair,
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
}

pub fn get_historical_forex_data_crud(
    pool: PgPool,
) -> CRUD<HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeys, HistoricalForexDataUpdateKeys>
{
    CRUD::<HistoricalForexDataFullKeys, HistoricalForexDataPrimaryKeys, HistoricalForexDataUpdateKeys>::new(
        pool,
        String::from("market_data.historical_forex_data"),
    )
}

pub fn get_specific_historical_forex_data_crud(pool: PgPool) -> HistoricalForexDataCRUD {
    HistoricalForexDataCRUD::new(pool)
}
