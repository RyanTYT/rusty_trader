use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tracing::level_filters::LevelFilter;
use std::fmt::{Debug, Write};
use tokio::sync::mpsc::{self, Sender};
use tokio::task;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{
    fmt::{self},
    layer::{Context, Layer},
    util::SubscriberInitExt,
};

struct FieldVisitor {
    pub output: String,
}

impl FieldVisitor {
    fn new() -> Self {
        FieldVisitor {
            output: String::new(),
        }
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let _ = write!(self.output, "{}={:?} ", field.name(), value);
    }
}

#[derive(Debug)]
struct LogRecord {
    timestamp: DateTime<Utc>,
    level: String,
    target: String,
    message: String,
}

/// The channel writer that receives formatted logs
#[derive(Clone)]
struct ChannelLayer {
    sender: Sender<LogRecord>,
}

/// Custom Layer that extracts metadata and sends a LogRecord through the channel
impl<S> Layer<S> for ChannelLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();

        // Format the fields using the default formatter
        let mut visitor = FieldVisitor::new();
        event.record(&mut visitor);

        // Don't log insertions into logs.logs
        let msg = visitor.output.to_lowercase();
        if msg.contains("insert into logs.logs") {
            return;
        }

        let now = chrono::Utc::now();

        let record = LogRecord {
            timestamp: now,
            level: meta.level().to_string(),
            target: meta.target().to_string(),
            message: visitor.output.trim().to_string(),
        };

        let _ = self.sender.try_send(record);
    }
}

pub fn init_logger() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel::<LogRecord>(1024);

    // Spawn background task to write logs to DB
    task::spawn(async move {
        while let Some(record) = rx.recv().await {
            // let _ = sqlx::query(
            //     "INSERT INTO logs.logs (time, level, name, message) VALUES ($1, $2, $3, $4)",
            // )
            // .bind(record.timestamp)
            // .bind(record.level)
            // .bind(record.target)
            // .bind(record.message)
            // .execute(&pool)
            // .await;
            println!(
                "===========\nTime: {}\nLevel: {}\nTarget: {}\nMsg: {}\n==========",
                record.timestamp, record.level, record.target, record.message
            );
        }
    });

    let stdout_layer = fmt::layer().pretty().with_target(true); // show function/module name
    // let db_layer = ChannelLayer { sender: tx };

    tracing_subscriber::registry()
        .with(stdout_layer)
        // .with(db_layer)
        .init();

    Ok(())
}

pub async fn init_logger_with_db(pool: PgPool) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel::<LogRecord>(1024);

    task::spawn(async move {
        while let Some(record) = rx.recv().await {
            let _ = sqlx::query(
                "INSERT INTO logs.logs (time, level, name, message) VALUES ($1, $2, $3, $4)",
            )
            .bind(record.timestamp)
            .bind(record.level)
            .bind(record.target)
            .bind(record.message)
            .execute(&pool)
            .await;
        }
    });

    let stdout_layer = fmt::layer().pretty().with_target(true);
    //.with_filter(LevelFilter::INFO); // show function/module name
    let db_layer = ChannelLayer { sender: tx }.with_filter(LevelFilter::INFO);

    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(db_layer)
        .try_init()
        .ok();

    Ok(())
}
