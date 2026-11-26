//! Tests for `order_update_stream/event_handlers/commission_report.rs`.
//!
//! A commission report is staged into `staged_commissions`; a DB trigger applies it
//! to the matching transaction. The handler is fire-and-forget.

use std::time::Duration;

use ibapi::orders::CommissionReport;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::PgPool;

use trading_app::database::crud::CRUDTrait;
use trading_app::database::db::DbDirect;
use trading_app::database::tables::DbTables;
use trading_app::execution::order_update_stream::event_handlers::commission_report;

use crate::common::TestDb;

async fn staged_fees(pool: &PgPool, execution_id: &str) -> Option<Decimal> {
    let mut db = DbDirect::connect(pool);
    db.staged_commissions()
        .read_all()
        .await
        .expect("read staged commissions")
        .into_iter()
        .find(|row| row.execution_id == execution_id)
        .map(|row| row.fees)
}

fn commission(execution_id: &str, amount: f64) -> CommissionReport {
    CommissionReport {
        execution_id: execution_id.to_string(),
        commission: amount,
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn a_commission_report_is_staged() {
    let db = TestDb::setup().await;

    commission_report::on_commission_update(db.pool.clone(), &commission("E1", 1.25))
        .expect("handler accepts the report");

    let mut staged = None;
    for _ in 0..100 {
        if let Some(fees) = staged_fees(&db.pool, "E1").await {
            staged = Some(fees);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let fees = staged.expect("expected staged commission");
    assert_eq!(fees, Decimal::from_f64(1.25).unwrap());
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn a_second_report_for_the_same_execution_upserts_the_fee() {
    let db = TestDb::setup().await;

    commission_report::on_commission_update(db.pool.clone(), &commission("E1", 1.25)).unwrap();
    let mut first = false;
    for _ in 0..100 {
        if staged_fees(&db.pool, "E1").await == Decimal::from_f64(1.25) {
            first = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(first);

    commission_report::on_commission_update(db.pool.clone(), &commission("E1", 2.50)).unwrap();

    let mut updated = false;
    for _ in 0..100 {
        if staged_fees(&db.pool, "E1").await == Decimal::from_f64(2.50) {
            updated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(updated, "expected the fee to be replaced");
}
