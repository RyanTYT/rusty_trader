//! `src/database/models_crud/logs.rs` — append-only table.
//!
//! `logs.logs` has a `GENERATED ALWAYS AS IDENTITY` id and is append-only:
//! `LogsCRUD` exposes only `create` and `read_all` (no update/delete/upsert),
//! so misuse is a compile error. These tests pin the append shape and the
//! generated-id round-trip.

use chrono::Utc;

use crate::common::{TestDb, tag};
use trading_app::database::db::DbDirect;
use trading_app::database::models::LogsFullKeys;
use trading_app::database::tables::DbTables;

fn row(tag: &str) -> LogsFullKeys {
    LogsFullKeys {
        id: None,
        time: Utc::now(),
        level: "INFO".to_string(),
        name: tag.to_string(),
        message: "test message".to_string(),
    }
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn append_then_read_all_returns_the_row_with_a_generated_id() {
    let db = TestDb::setup().await;
    let expected = row(&tag("logs_append"));

    let mut conn = DbDirect::connect(&db.pool);
    conn.logs().create(&expected).await.unwrap();

    let rows = conn.logs().read_all().await.unwrap();
    assert_eq!(rows.len(), 1);
    let got = &rows[0];
    assert!(got.id.is_some(), "the identity id is read back");
    assert_eq!(got.time, expected.time);
    assert_eq!(got.level, expected.level);
    assert_eq!(got.name, expected.name);
    assert_eq!(got.message, expected.message);
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn appending_twice_keeps_both_rows_with_distinct_ids() {
    let db = TestDb::setup().await;
    let first = row(&tag("logs_twice"));
    // Identical business columns: append-only means both rows survive.
    let second = first.clone();

    let mut conn = DbDirect::connect(&db.pool);
    conn.logs().create(&first).await.unwrap();
    conn.logs().create(&second).await.unwrap();

    let rows = conn.logs().read_all().await.unwrap();
    assert_eq!(rows.len(), 2, "duplicates are kept");
    assert_ne!(rows[0].id, rows[1].id, "the identity ids differ");
}
