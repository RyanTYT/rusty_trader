//! `src/database/models_crud/strategy.rs`

use trading_app::database::models::{
    Status, StrategyFullKeys, StrategyPrimaryKeys, StrategyUpdateKeys,
};

mod fx {
    use super::*;

    pub fn fk(tag: &str) -> StrategyFullKeys {
        StrategyFullKeys {
            strategy: tag.to_string(),
            status: Status::Active,
        }
    }

    pub fn pk(fk: &StrategyFullKeys) -> StrategyPrimaryKeys {
        StrategyPrimaryKeys {
            strategy: fk.strategy.clone(),
        }
    }

    pub fn pk_other(tag: &str) -> StrategyPrimaryKeys {
        StrategyPrimaryKeys {
            strategy: format!("{tag}_missing"),
        }
    }

    pub fn uk(_tag: &str) -> StrategyUpdateKeys {
        StrategyUpdateKeys {
            status: Some(Status::Inactive),
        }
    }

    pub fn uk_none() -> StrategyUpdateKeys {
        StrategyUpdateKeys { status: None }
    }

    pub fn conflicting(fk: &StrategyFullKeys) -> StrategyFullKeys {
        StrategyFullKeys {
            strategy: fk.strategy.clone(),
            status: Status::Stopping,
        }
    }

    pub fn check(expected: &StrategyFullKeys, got: &StrategyFullKeys) {
        assert_eq!(got.strategy, expected.strategy);
        assert_eq!(format!("{:?}", got.status), format!("{:?}", expected.status));
    }

    pub fn check_updated(uk: &StrategyUpdateKeys, got: &StrategyFullKeys) {
        let expected = uk.status.as_ref().expect("update sets a status");
        assert_eq!(format!("{:?}", got.status), format!("{:?}", expected));
    }

    pub fn matches(row: &StrategyFullKeys, fk: &StrategyFullKeys) -> bool {
        row.strategy == fk.strategy
    }
    pub async fn setup(_s: &mut crate::common::TestSession) {}
}

crud_suite!(strategy, fx);
