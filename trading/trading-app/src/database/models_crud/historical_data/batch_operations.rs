//! COPY-based bulk writes into the historical tables.
//!
//! [`BatchDbCreator`] wraps a bounded channel (10 000 rows) and one long-lived
//! `tokio_postgres` connection. A worker buffers rows, flushes at 200 000 rows
//! or after one second of inactivity, and flushes whatever is queued when
//! shutdown is signalled. Each flush runs in its own transaction: a random
//! staging table is created, rows are binary-COPYed (deduplicated by primary
//! key, later rows winning), and a SQL merge upserts staging into the target.
//!
//! This path is deliberately separate from the sqlx CRUD layer: it talks to
//! `tokio_postgres` directly so it can stream `COPY`. Use it for market-data
//! ingestion, not for transactional business writes.
//!
//! # Example
//!
//! ```no_run
//! use trading_app::database::models::HistoricalStockDataFullKeys;
//! use trading_app::database::models_crud::historical_data::batch_operations::init_channel;
//!
//! # async fn example(fk: HistoricalStockDataFullKeys) -> Result<(), Box<dyn std::error::Error>> {
//! let batch = init_channel::<HistoricalStockDataFullKeys>().await;
//! batch.batch_create_or_update(&fk).await?;
//!
//! // Rows become visible on the size/interval flush, or immediately here.
//! batch.flush().await?;
//! # Ok(())
//! # }
//! ```

use std::hash::Hash;
use std::sync::Mutex;
use std::{collections::HashSet, sync::Arc};

use chrono::{DateTime, Utc};
use rand::{Rng, distr::Alphanumeric};
use tokio::sync::mpsc::Sender;
use tokio::{sync::mpsc::channel, time::Instant};
use tokio_postgres::NoTls;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;

use crate::database::models::{
    DailyHistoricalStockDataFullKeys, HistoricalForexDataFullKeys, HistoricalOptionsDataFullKeys,
    HistoricalStockDataFullKeys, OptionType,
};
use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;

/// A row model that can be streamed into a table with `COPY`.
///
/// Implementors describe the target table once — the staging DDL, the COPY
/// statement, the column types and how to write a row — plus how to merge the
/// staging table back into the target.
pub trait BulkInsertable {
    /// Identity used to deduplicate a batch; later rows with the same key win.
    type PrimaryKey: Hash + Eq + Send + Sync;

    /// Fully qualified target table, e.g. `market_data.historical_data`.
    fn table_name() -> &'static str;
    /// Primary key of `self`, used for in-batch deduplication.
    fn primary_keys(&self) -> Self::PrimaryKey;
    /// `CREATE TEMP TABLE <staging_table> (...)` matching the target columns.
    fn create_temp_table_sql(staging_table: &str) -> String;
    /// `COPY <staging_table> FROM STDIN (FORMAT binary)`.
    fn copy_in_sql(staging_table: &str) -> String;
    /// Binary COPY column types, in column order.
    fn types() -> &'static [Type];
    /// Appends `self`'s columns to `row`, in [`Self::types`] order.
    fn write_to_row<'a>(&'a self, row: &mut Vec<&'a (dyn tokio_postgres::types::ToSql + Sync)>);
    /// Upsert from staging into the target table.
    fn merge_sql(staging_table: &str) -> String;
}

/// Borrowed rows implement the trait by delegating to the owned type, so a
/// `&T` slice can be flushed without cloning.
impl<'b, T: BulkInsertable> BulkInsertable for &'b T {
    type PrimaryKey = T::PrimaryKey;

    fn table_name() -> &'static str {
        T::table_name()
    }

    fn primary_keys(&self) -> Self::PrimaryKey {
        (*self).primary_keys()
    }

    fn create_temp_table_sql(staging_table: &str) -> String {
        T::create_temp_table_sql(staging_table)
    }

    fn copy_in_sql(staging_table: &str) -> String {
        T::copy_in_sql(staging_table)
    }

    fn types() -> &'static [Type] {
        T::types()
    }

    fn write_to_row<'a>(&'a self, row: &mut Vec<&'a (dyn tokio_postgres::types::ToSql + Sync)>) {
        (*self).write_to_row(row);
    }

    fn merge_sql(staging_table: &str) -> String {
        T::merge_sql(staging_table)
    }
}

/// Implements [`BulkInsertable`] from a table/column specification.
///
/// `pk_fields` computes the dedupe key, `[pk]` marks the columns used by the
/// generated `ON CONFLICT` merge, and `columns` supplies the staging DDL
/// fragment, the Postgres type and the accessor for every column. The target
/// table must have a unique constraint on the `[pk]` columns.
macro_rules! impl_bulk_insertable {
    (
        $struct_name:ident {
            table: $table_name:expr,
            pk_type: $pk_type:ty,
            pk_fields: ($self_ident:ident) -> [$($pk_field:expr),+ $(,)?],
            pk_field_names: [$($pk_field_name:expr),+ $(,)?],
            columns: [
                $(
                    $field:ident : $pg_type_str:expr => $pg_type:path $([$is_pk:ident])?
                ),+ $(,)?
            ]
        }
    ) => {
        impl BulkInsertable for $struct_name {
            type PrimaryKey = $pk_type;

            fn table_name() -> &'static str {
                $table_name
            }

            fn primary_keys(&$self_ident) -> Self::PrimaryKey {
                ( $( $pk_field ),+ )
            }

            fn create_temp_table_sql(staging_table: &str) -> String {
                let cols = vec![
                    $( format!("{} {}", stringify!($field), $pg_type_str) ),+
                ];
                format!("CREATE TEMP TABLE {} ({}) ON COMMIT DROP;", staging_table, cols.join(", "))
            }

            fn copy_in_sql(staging_table: &str) -> String {
                let cols = vec![ $( stringify!($field) ),+ ].join(", ");
                format!("COPY {} ({}) FROM STDIN WITH (FORMAT binary)", staging_table, cols)
            }

            fn types() -> &'static [Type] {
                &[ $( $pg_type ),+ ]
            }

            fn write_to_row<'a>(&'a self, row: &mut Vec<&'a (dyn tokio_postgres::types::ToSql + Sync)>) {
                $(
                    row.push(&self.$field);
                )+
            }

            fn merge_sql(staging_table: &str) -> String {
                let all_cols = vec![ $( stringify!($field) ),+ ];
                let pk_cols = vec![ $( stringify!($pk_field_name) ),+ ];

                // Filter non-primary key columns for DO UPDATE clause
                let non_pk_cols: Vec<&str> = all_cols
                    .iter()
                    .copied()
                    .filter(|col| !pk_cols.contains(col))
                    .collect();

                let update_assignments: Vec<String> = non_pk_cols
                    .iter()
                    .map(|col| format!("{col} = EXCLUDED.{col}"))
                    .collect();

                format!(
                    "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO UPDATE SET {};",
                    $table_name,
                    all_cols.join(", "),
                    all_cols.join(", "),
                    staging_table,
                    pk_cols.join(", "),
                    update_assignments.join(", ")
                )
            }
        }
    };
}

/// Writes one batch into `T`'s table through a staging table.
///
/// Empty batches are a no-op. Rows are deduplicated by
/// [`BulkInsertable::primary_keys`], iterating the batch in reverse so the
/// latest row wins. Staging creation, COPY and merge run in one transaction.
/// A COPY-finish error is logged and the merge is still attempted, so only rows
/// that reached the staging table are committed.
///
/// # Errors
/// Any connection, transaction, COPY or merge failure, naming the step.
pub async fn flush_batch_generic<T>(
    client: &mut tokio_postgres::Client,
    batch: &[T],
) -> Result<(), String>
where
    T: BulkInsertable + Clone,
{
    if batch.is_empty() {
        return Ok(());
    }

    let suffix: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    let staging_table = format!("staging_{}", suffix);

    let tx = client
        .transaction()
        .await
        .map_err(|e| format!("Failed to create client transaction: {e:?}"))?;

    // 1. Create Staging Table
    tx.batch_execute(&T::create_temp_table_sql(&staging_table))
        .await
        .map_err(|e| format!("Failed to batch_execute creation of staging table: {e:?}"))?;

    // 2. Prepare Copy In
    let sink = tx
        .copy_in(&T::copy_in_sql(&staging_table))
        .await
        .map_err(|e| format!("Failed to perform copy_in transaction: {e:?}"))?;
    let writer = BinaryCopyInWriter::new(sink, T::types());
    tokio::pin!(writer);

    // 3. Deduplicate and Write Binary Payload
    let mut unique_keys = HashSet::new();
    let mut row_buffer = Vec::with_capacity(T::types().len());

    // Prioritise later updates
    for row in batch.iter().rev() {
        let pk = row.primary_keys();
        if !unique_keys.insert(pk) {
            continue;
        }

        row_buffer.clear();
        row.write_to_row(&mut row_buffer);
        writer
            .as_mut()
            .write(&row_buffer[..])
            .await
            .map_err(|e| format!("Failed to write to buffer: {e:?}"))?;
    }

    if let Err(e) = writer.finish().await {
        tracing::warn!(
            "Error trying to wait for writer to finish writing batch in historical forex data: {e:?}"
        );
    };

    // 4. Upsert from Staging into Target Table
    tx.batch_execute(&T::merge_sql(&staging_table))
        .await
        .map_err(|e| format!("Failed to batch execut merge sql: {e:?}"))?;
    tx.commit()
        .await
        .map_err(|e| format!("Failed to commit transaction for merge sql: {e:?}"))?;

    println!(
        "Flushed batch of {} items to {}",
        batch.len(),
        T::table_name()
    );
    Ok(())
}

impl_bulk_insertable! {
    HistoricalStockDataFullKeys {
        table: "market_data.historical_data",
        pk_type: (String, String, String, DateTime<Utc>),
        pk_fields: (self) -> [
            self.stock.clone(),
            self.primary_exchange.clone(),
            self.currency.clone(),
            self.time.clone()
        ],
        pk_field_names: [stock, primary_exchange, currency, time],
        columns: [
            stock: "VARCHAR(50) NOT NULL" => Type::VARCHAR [pk],
            primary_exchange: "VARCHAR(50) NOT NULL" => Type::VARCHAR [pk],
            currency: "VARCHAR(10) NOT NULL" => Type::VARCHAR [pk],
            time: "TIMESTAMPTZ NOT NULL" => Type::TIMESTAMPTZ [pk],
            open: "DOUBLE PRECISION" => Type::FLOAT8,
            high: "DOUBLE PRECISION" => Type::FLOAT8,
            low: "DOUBLE PRECISION" => Type::FLOAT8,
            close: "DOUBLE PRECISION" => Type::FLOAT8,
            volume: "NUMERIC(30, 6)" => Type::NUMERIC,
        ]
    }
}

impl_bulk_insertable! {
    DailyHistoricalStockDataFullKeys {
        table: "market_data.daily_ohlcv",
        pk_type: (String, String, String, DateTime<Utc>),
        pk_fields: (self) -> [self.stock.clone(), self.primary_exchange.clone(), self.currency.clone(), self.day.clone()],
        pk_field_names: [stock, primary_exchange, currency, day],
        columns: [
            stock: "VARCHAR(50) NOT NULL" => Type::VARCHAR [pk],
            primary_exchange: "VARCHAR(50) NOT NULL" => Type::VARCHAR [pk],
            currency: "VARCHAR(10) NOT NULL" => Type::VARCHAR [pk],
            day: "TIMESTAMPTZ NOT NULL" => Type::TIMESTAMPTZ [pk],
            open: "DOUBLE PRECISION" => Type::FLOAT8,
            high: "DOUBLE PRECISION" => Type::FLOAT8,
            low: "DOUBLE PRECISION" => Type::FLOAT8,
            close: "DOUBLE PRECISION" => Type::FLOAT8,
            volume: "NUMERIC(30, 6)" => Type::NUMERIC,
        ]
    }
}

impl_bulk_insertable! {
    HistoricalOptionsDataFullKeys {
        table: "market_data.historical_options_data",
        pk_type: (String, String, String, String, u64, String, OptionType, DateTime<Utc>),
        pk_fields: (self) -> [
            self.stock.clone(),
            self.primary_exchange.clone(),
            self.currency.clone(),
            self.expiry.clone(),
            self.strike.to_bits().clone(),
            self.multiplier.clone(),
            self.option_type.clone(),
            self.time.clone()
        ],
        pk_field_names: [stock, primary_exchange, currency, expiry, strike, multiplier, option_type, time],
        columns: [
            stock: "VARCHAR(50) NOT NULL" => Type::VARCHAR [pk],
            primary_exchange: "VARCHAR(50) NOT NULL" => Type::VARCHAR [pk],
            currency: "VARCHAR(10) NOT NULL" => Type::VARCHAR [pk],
            expiry: "VARCHAR(20) NOT NULL" => Type::VARCHAR [pk],
            strike: "DOUBLE PRECISION" => Type::FLOAT8 [pk],
            multiplier: "VARCHAR(50) NOT NULL" => Type::VARCHAR [pk],
            option_type: "option_type" => Type::VARCHAR [pk],
            time: "TIMESTAMPTZ NOT NULL" => Type::TIMESTAMPTZ [pk],
            open: "DOUBLE PRECISION" => Type::FLOAT8,
            high: "DOUBLE PRECISION" => Type::FLOAT8,
            low: "DOUBLE PRECISION" => Type::FLOAT8,
            close: "DOUBLE PRECISION" => Type::FLOAT8,
            volume: "NUMERIC(30, 6)" => Type::NUMERIC,
        ]
    }
}

impl_bulk_insertable! {
    HistoricalForexDataFullKeys {
        table: "market_data.historical_forex_data",
        pk_type: (String, DateTime<Utc>),
        pk_fields: (self) -> [self.pair.clone(), self.time.clone()],
        pk_field_names: [pair, time],
        columns: [
            pair: "VARCHAR(30) NOT NULL" => Type::VARCHAR [pk],
            time: "TIMESTAMPTZ NOT NULL" => Type::TIMESTAMPTZ [pk],
            bid_open: "DOUBLE PRECISION" => Type::FLOAT8,
            bid_high: "DOUBLE PRECISION" => Type::FLOAT8,
            bid_low: "DOUBLE PRECISION" => Type::FLOAT8,
            bid_close: "DOUBLE PRECISION" => Type::FLOAT8,
            ask_open: "DOUBLE PRECISION" => Type::FLOAT8,
            ask_high: "DOUBLE PRECISION" => Type::FLOAT8,
            ask_low: "DOUBLE PRECISION" => Type::FLOAT8,
            ask_close: "DOUBLE PRECISION" => Type::FLOAT8,
        ]
    }
}

/// Handle to the background historical-data writer.
///
/// Cloning shares the row channel and the shutdown signal. The worker is
/// stopped by [`Self::flush`] or, best-effort, when the last clone drops.
#[derive(Debug, Clone)]
pub struct BatchDbCreator<T>
where
    T: BulkInsertable + Clone,
{
    /// Bounded channel to the worker (capacity 10 000).
    sender: Arc<Sender<T>>,
    /// One-shot shutdown signal shared by all clones.
    shutdown_sender: Arc<Mutex<Option<tokio::sync::oneshot::Sender<bool>>>>,
    /// Worker handle, taken by [`Self::flush`].
    join: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Set after [`Self::flush`] awaited the worker; suppresses the Drop signal.
    joined: bool,
}

impl<T> BatchDbCreator<T>
where
    T: BulkInsertable + Clone,
{
    /// Builds a handle around an already-spawned worker.
    fn new(
        sender: Arc<Sender<T>>,
        shutdown_sender: Arc<Mutex<Option<tokio::sync::oneshot::Sender<bool>>>>,
        join: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            sender,
            shutdown_sender,
            join: Arc::new(Mutex::new(Some(join))),
            joined: false,
        }
    }

    /// Signals shutdown, lets the worker drain and flush the queue, and awaits
    /// it.
    ///
    /// After this, [`Drop`] does nothing for this handle. Errors when the
    /// worker task panicked or was cancelled.
    pub async fn flush(mut self) -> Result<(), String> {
        if let Some(sender) = self
            .shutdown_sender
            .lock()
            .expect("Failed to acquire lock shutdown_sender")
            .take()
        {
            let _ = sender.send(true);
        }

        if let Some(handle) = self
            .join
            .lock()
            .expect("Failed to acquire lock join")
            .take()
        {
            handle
                .await
                .map_err(|e| format!("batch worker failed: {e}"))?;
        }

        self.joined = true;
        Ok(())
    }

    /// Queues one row (cloned) for the worker.
    ///
    /// Best-effort: a send failure is logged and swallowed, so this always
    /// returns `Ok`.
    pub async fn batch_create_or_update(&self, fk: &T) -> Result<(), String> {
        if let Err(e) = self.sender.send(fk.clone()).await {
            tracing::error!("error sending data via batch_create_or_update: {e:?}");
        };
        Ok(())
    }

    // /// Drop BatchDbCreator, waiting for all to flush fully
    // async fn blocking_drop(&mut self) {
    //     let sender = {
    //         let opt = self
    //             .shutdown_sender
    //             .lock()
    //             .expect("Failed to acquire lock shutdown_sender")
    //             .take();
    //         if opt.is_none() {
    //             tracing::warn!("historical_data: Channel already closed!");
    //             return;
    //         }
    //         opt.unwrap()
    //     };
    //
    //     if let Err(e) = sender.send(true) {
    //         tracing::error!("Error sending shutdown command: {e:?}");
    //     }
    //
    //     // Wait for acknowledgement / channel close
    //     let rcx = {
    //         let rcx_opt = self
    //             .shutdown_success_rcx
    //             .lock()
    //             .expect("Failed to acquire shutdown_success_rcx lock")
    //             .take();
    //         if rcx_opt.is_none() {
    //             tracing::warn!("Shutdown signal already sent and success already received!");
    //             return;
    //         }
    //         rcx_opt.unwrap()
    //     };
    //     if let Err(e) = rcx.await {
    //         tracing::error!("Shutdown responder dropped prematurely without sending: {e:?}")
    //     };
    // }
}

/// Best-effort shutdown.
///
/// Only the last remaining clone signals the worker; the worker flushes on its
/// way out. Dropping does not wait for the flush — call [`BatchDbCreator::flush`]
/// when the final write must be guaranteed.
impl<T> Drop for BatchDbCreator<T>
where
    T: BulkInsertable + Clone,
{
    /// Sends the shutdown signal unless [`BatchDbCreator::flush`] already joined.
    fn drop(&mut self) {
        if self.joined {
            return;
        }
        let sender = {
            if Arc::strong_count(&self.shutdown_sender) > 1 {
                // there is still another one out there still not done
                return;
            }
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

        if let Err(e) = sender.send(true) {
            tracing::error!("Error sending shutdown command: {e:?}");
        }
    }
}

/// Spawns the background writer and returns its handle.
///
/// Reads `DATABASE_HOST`, `DB_USERNAME`, `DB_PW` and `DB_DB`; the host string is
/// interpolated as libpq keywords, so a non-default port must be carried as
/// `host port=5432`.
///
/// # Panics
/// Panics when an environment variable is missing or the initial
/// `tokio_postgres` connection fails.
///
/// The worker flushes at 200 000 rows or after one second of inactivity and
/// drains anything already sent before its final flush, so
/// [`BatchDbCreator::flush`] cannot lose queued rows.
pub async fn init_channel<T>() -> BatchDbCreator<T>
where
    T: BulkInsertable + Clone + Send + Sync + 'static,
{
    const BATCH_SIZE: usize = 200_000;
    const MAX_BATCH_WAIT_MS: u64 = 1000;

    let host = std::env::var("DATABASE_HOST")
        .expect("Expected DATABASE_HOST environment variable to be set!");

    let user = std::env::var("DB_USERNAME").expect("Expected DB_USERNAME to be defined properly");
    let pw = std::env::var("DB_PW").expect("Expected DB_PW to be defined properly");
    let db_name = std::env::var("DB_DB").expect("Expected DB_DB to be defined properly");
    let (mut client, connection) = tokio_postgres::connect(
        &format!(
            "host={} user={} password={} dbname={}",
            host, user, pw, db_name
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

    let (sender, mut rx) = channel::<T>(10_000);
    let (shutdown_sender, mut shutdown_rx) = tokio::sync::oneshot::channel::<bool>();
    let (shutdown_resp_sender, _shutdown_resp_rx) = tokio::sync::oneshot::channel::<bool>();

    let task = tokio::spawn(async move {
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
                                if let Err(e) = flush_batch_generic(&mut client, &buffer)
                                    .await
                                    .map_err(|e| e.to_string()) {

                                        tracing::error!("Expected to be able to flush batch: {e:?}");
                                }
                                // if let Err(e) = flush_historical_data_batch(&mut client, &buffer).await {
                                // }
                                buffer.clear();
                                last_flush = Instant::now();
                            }
                        }
                        None => {
                            if !buffer.is_empty() {
                                if let Err(e) = flush_batch_generic(&mut client, &buffer)
                                    .await
                                    .map_err(|e| e.to_string()) {

                                        tracing::error!("Expected to be able to flush batch: {e:?}");
                                }
                            }
                            if let Err(e) = shutdown_resp_sender.send(true) {
                                tracing::warn!("Could not send update to shutdown_resp_sender in historical_data: {e:?}")
                            };
                            break;
                        }
                    }
                }
                maybe_shutdown = &mut shutdown_rx => {
                    if let Err(e) = maybe_shutdown {
                        tracing::error!("Sender of shutdown_sender for batch_operations dropped before sending: {e:?}");
                    }

                    // A shutdown signal can overtake rows that were already sent but
                    // not yet received; drain the channel so `flush()` cannot lose
                    // queued rows.
                    while let Ok(row) = rx.try_recv() {
                        buffer.push(row);
                    }

                    if !buffer.is_empty() {
                        if let Err(e) = flush_batch_generic(&mut client, &buffer)
                            .await
                            .map_err(|e| e.to_string()) {

                                tracing::error!("Expected to be able to flush batch: {e:?}");
                        }
                    }
                    drop(client);
                    if let Err(e) = shutdown_resp_sender.send(true) {
                        tracing::warn!("Could not send update to shutdown_resp_sender in historical_data: {e:?}")
                    };
                    break;
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(MAX_BATCH_WAIT_MS)) => {
                    if !buffer.is_empty() && last_flush.elapsed().as_millis() as u64 >= MAX_BATCH_WAIT_MS {
                        if let Err(e) = flush_batch_generic(&mut client, &buffer)
                            .await
                            .map_err(|e| e.to_string())
                        {
                            tracing::error!("Expected to be able to flush batch: {e:?}");
                        }
                        buffer.clear();
                        last_flush = Instant::now();
                    }
                }
            }
        }
        tracing::info!("loop to receive goods ended");
    });

    BatchDbCreator::new(
        Arc::new(sender),
        Arc::new(Mutex::new(Some(shutdown_sender))),
        task,
        // Arc::new(Mutex::new(Some(shutdown_resp_rx))),
    )
}

#[derive(Debug, Clone)]
/// Asset-dispatching batch writer for [`HistoricalDataFullKeys`].
pub enum BatchDbCreatorEnum {
    /// Writer for `market_data.historical_data`.
    Stock(BatchDbCreator<HistoricalStockDataFullKeys>),
    /// Writer for `market_data.historical_options_data`.
    Options(BatchDbCreator<HistoricalOptionsDataFullKeys>),
    /// Writer for `market_data.historical_forex_data`.
    Forex(BatchDbCreator<HistoricalForexDataFullKeys>),
}

#[hotpath::measure_all]
impl BatchDbCreatorEnum {
    /// Queues `fk` on the creator matching its variant.
    ///
    /// # Errors
    /// Returns an error when the key variant and the creator variant differ.
    pub async fn batch_create_or_update(&self, fk: &HistoricalDataFullKeys) -> Result<(), String> {
        match (self, fk) {
            (Self::Stock(creator), HistoricalDataFullKeys::Stock(data)) => {
                creator.batch_create_or_update(data).await
            }
            (Self::Options(creator), HistoricalDataFullKeys::Options(data)) => {
                creator.batch_create_or_update(data).await
            }
            (Self::Forex(creator), HistoricalDataFullKeys::Forex(data)) => {
                creator.batch_create_or_update(data).await
            }
            _ => Err("BatchDbCreator Type doesn't match Historical Data Full Keys".to_string()),
        }
    }
}
