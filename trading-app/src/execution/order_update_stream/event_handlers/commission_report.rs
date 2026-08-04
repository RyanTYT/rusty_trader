use ibapi::orders::CommissionReport;
use rust_decimal::prelude::FromPrimitive;
use sqlx::PgPool;

use crate::database::{
    crud::CRUDTrait,
    models::{StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys},
    models_crud::staged_commissions::StagedCommissionsCRUD,
};

/// Should be triggered by CommissionUpdate(CommissionReport) events
/// Simply create_or_update the row in StagedCommissions
/// - StagedCommissions should have triggers attached to update the associated transactions
/// automatically on inserts
pub fn on_commission_update(
    pool: PgPool,
    commission_report: &CommissionReport,
) -> Result<(), String> {
    let staged_commissions_crud = StagedCommissionsCRUD::new(pool);
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
        if let Err(e) = staged_commissions_crud
            .create_or_update(&staged_commissions_pk, &staged_commissions_uk)
            .await
        {
            tracing::error!("Error trying to insert into StagedCommissions table: {e:?}");
        }
    });
    Ok(())
}
