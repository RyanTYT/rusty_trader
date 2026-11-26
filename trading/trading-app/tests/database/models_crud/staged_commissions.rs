//! `src/database/models_crud/staged_commissions.rs`

use std::str::FromStr;

use rust_decimal::Decimal;
use trading_app::database::models::{
    StagedCommissionsFullKeys, StagedCommissionsPrimaryKeys, StagedCommissionsUpdateKeys,
};

pub mod fx {
    use super::*;

    pub async fn setup(_s: &mut crate::common::TestSession) {}

    pub fn fk(tag: &str) -> StagedCommissionsFullKeys {
        StagedCommissionsFullKeys {
            execution_id: tag.to_string(),
            fees: Decimal::from_str("1.25").expect("decimal"),
        }
    }

    pub fn pk(fk: &StagedCommissionsFullKeys) -> StagedCommissionsPrimaryKeys {
        StagedCommissionsPrimaryKeys {
            execution_id: fk.execution_id.clone(),
        }
    }

    pub fn pk_other(tag: &str) -> StagedCommissionsPrimaryKeys {
        StagedCommissionsPrimaryKeys {
            execution_id: format!("{tag}_missing"),
        }
    }

    pub fn uk(tag: &str) -> StagedCommissionsUpdateKeys {
        StagedCommissionsUpdateKeys {
            fees: Some(Decimal::from_str(if tag.is_empty() { "2.50" } else { "2.50" }).expect("decimal")),
        }
    }

    pub fn uk_none() -> StagedCommissionsUpdateKeys {
        StagedCommissionsUpdateKeys { fees: None }
    }

    pub fn conflicting(fk: &StagedCommissionsFullKeys) -> StagedCommissionsFullKeys {
        StagedCommissionsFullKeys {
            execution_id: fk.execution_id.clone(),
            fees: Decimal::from_str("9.99").expect("decimal"),
        }
    }

    pub fn check(expected: &StagedCommissionsFullKeys, got: &StagedCommissionsFullKeys) {
        assert_eq!(got.execution_id, expected.execution_id);
        assert_eq!(got.fees, expected.fees);
    }

    pub fn check_updated(uk: &StagedCommissionsUpdateKeys, got: &StagedCommissionsFullKeys) {
        assert_eq!(got.fees, uk.fees.expect("update sets fees"));
    }

    pub fn matches(row: &StagedCommissionsFullKeys, fk: &StagedCommissionsFullKeys) -> bool {
        row.execution_id == fk.execution_id
    }
}

crud_suite!(staged_commissions, fx);
