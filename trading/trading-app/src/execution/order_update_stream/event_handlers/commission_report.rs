//! Commission staging.
//!
//! IBKR reports commissions separately and sometimes after the fill. The handler
//! stores the fee in `trading.staged_commissions`; database triggers then apply
//! it to the already-inserted transaction (last report wins) or keep it staged
//! until the fill arrives. See
//! [`super::execution`](crate::execution::order_update_stream::event_handlers::execution)
//! for the fill side of that handshake.

use crate::database::tables::DbTables;
use ibapi::orders::CommissionReport;
use rust_decimal::prelude::FromPrimitive;
use sqlx::PgPool;

use crate::database::{
    crud::CRUDTrait,
    models::{StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys},
};

/// Stages the commission for the execution so the database triggers can merge
/// it into the transaction.
///
/// Upserts `trading.staged_commissions` keyed by `execution_id` with the report's
/// commission. The write happens in a spawned task, so the function returns
/// before the row exists; staging failures are only logged. The database's
/// `trg_try_apply_commission_to_transaction` decides whether the fee is applied
/// immediately (transaction already present) or left staged (fill not seen yet);
/// a later report overwrites an earlier fee.
///
/// # Panics
///
/// Panics if the commission cannot be represented as a
/// [`rust_decimal::Decimal`].
pub fn on_commission_update(
    pool: PgPool,
    commission_report: &CommissionReport,
) -> Result<(), String> {
    let staged_commissions_pk = StagedCommissionsPrimaryKeys {
        execution_id: commission_report.execution_id.to_string(),
    };
    let staged_commissions_uk = StagedCommissionsUpdateKeys {
        fees: Some(
            rust_decimal::Decimal::from_f64(commission_report.commission)
                .expect("Expected commission from commission_report to be valid for Decimal"),
        ),
    };
    tokio::spawn(async move {
        let mut db = crate::database::db::DbDirect::connect(&pool);
        let mut staged_commissions_crud = db.staged_commissions();
        if let Err(e) = staged_commissions_crud
            .create_or_update(&staged_commissions_pk, &staged_commissions_uk)
            .await
        {
            tracing::error!("Error trying to insert into StagedCommissions table: {e:?}");
        }
    });
    Ok(())
}
