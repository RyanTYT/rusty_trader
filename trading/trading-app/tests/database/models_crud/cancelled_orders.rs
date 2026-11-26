//! `src/database/models_crud/cancelled_orders.rs`

use chrono::Utc;
use trading_app::database::models::{
    CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys, CancelledOrdersUpdateKeys, Status,
    StrategyFullKeys,
};

const STRATEGY: &str = "test_cancelled_orders";

mod fx {
    use super::*;
    use trading_app::database::tables::DbTables;

    fn seq(tag: &str) -> i32 {
        tag.rsplit('_')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    pub async fn setup(s: &mut crate::common::TestSession) {
        s.strategy()
            .create_or_ignore(&StrategyFullKeys {
                strategy: STRATEGY.to_string(),
                status: Status::Active,
            })
            .await
            .expect("seed strategy row for cancelled_orders FK");
    }

    pub fn fk(tag: &str) -> CancelledOrdersFullKeys {
        CancelledOrdersFullKeys {
            time: Utc::now(),
            order_perm_id: seq(tag),
            order_id: seq(tag),
            strategy: STRATEGY.to_string(),
            stock: format!("AAPL_{tag}"),
            primary_exchange: "NASDAQ".to_string(),
            currency: "USD".to_string(),
            quantity: 100.0,
            executions: vec![format!("exec_{tag}")],
            filled: 0.0,
            reason: "test".to_string(),
        }
    }

    pub fn pk(fk: &CancelledOrdersFullKeys) -> CancelledOrdersPrimaryKeys {
        CancelledOrdersPrimaryKeys {
            time: fk.time,
            order_perm_id: fk.order_perm_id,
            order_id: fk.order_id,
        }
    }

    pub fn pk_other(tag: &str) -> CancelledOrdersPrimaryKeys {
        CancelledOrdersPrimaryKeys {
            time: Utc::now(),
            order_perm_id: 999_999,
            order_id: tag.len() as i32,
        }
    }

    pub fn uk(tag: &str) -> CancelledOrdersUpdateKeys {
        CancelledOrdersUpdateKeys {
            strategy: Some(STRATEGY.to_string()),
            stock: Some(format!("MSFT_{tag}")),
            primary_exchange: Some("NASDAQ".to_string()),
            currency: Some("USD".to_string()),
            quantity: Some(50.0),
            executions: Some(vec![format!("updated_{tag}")]),
            filled: Some(25.0),
            reason: Some("updated reason".to_string()),
        }
    }

    pub fn uk_none() -> CancelledOrdersUpdateKeys {
        CancelledOrdersUpdateKeys {
            strategy: None,
            stock: None,
            primary_exchange: None,
            currency: None,
            quantity: None,
            executions: None,
            filled: None,
            reason: None,
        }
    }

    pub fn conflicting(fk: &CancelledOrdersFullKeys) -> CancelledOrdersFullKeys {
        CancelledOrdersFullKeys {
            reason: "conflicting reason".to_string(),
            ..fk.clone()
        }
    }

    pub fn check(expected: &CancelledOrdersFullKeys, got: &CancelledOrdersFullKeys) {
        assert_eq!(got.time, expected.time);
        assert_eq!(got.order_perm_id, expected.order_perm_id);
        assert_eq!(got.order_id, expected.order_id);
        assert_eq!(got.strategy, expected.strategy);
        assert_eq!(got.stock, expected.stock);
        assert_eq!(got.primary_exchange, expected.primary_exchange);
        assert_eq!(got.currency, expected.currency);
        assert_eq!(got.quantity, expected.quantity);
        assert_eq!(got.executions, expected.executions);
        assert_eq!(got.filled, expected.filled);
        assert_eq!(got.reason, expected.reason);
    }

    pub fn check_updated(uk: &CancelledOrdersUpdateKeys, got: &CancelledOrdersFullKeys) {
        assert_eq!(Some(got.stock.clone()), uk.stock);
        assert_eq!(Some(got.quantity), uk.quantity);
        assert_eq!(Some(got.executions.clone()), uk.executions);
        assert_eq!(Some(got.filled), uk.filled);
        assert_eq!(Some(got.reason.clone()), uk.reason);
    }

    pub fn matches(row: &CancelledOrdersFullKeys, fk: &CancelledOrdersFullKeys) -> bool {
        row.time == fk.time && row.order_perm_id == fk.order_perm_id && row.order_id == fk.order_id
    }
}

crud_suite!(cancelled_orders, fx);
