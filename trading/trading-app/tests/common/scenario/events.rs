//! Execution and commission event decoding.
//!
//! Event rows carry the scripted fill (`id`, `order_id`, `perm_id`, `shares`,
//! `price`, `cumulative`, `reference`, `side`, optional `symbol`/`option`) or
//! the commission report (`execution_id`, `fees`). Defaults mirror the ibapi
//! `Default` values the old per-runner builders used: stock `AAPL`, execution
//! time `20261125 10:00:00 US/Eastern`, side empty (the syncer case files
//! always spell the side out).

use ibapi::contracts::Contract;
use ibapi::orders::{CommissionReport, Execution, ExecutionData};
use serde_json::Value;

use super::traits::{FromJson, JsonError, integer, number, opt_integer, opt_string, string};

pub const EXECUTION_TIME: &str = "20261125 10:00:00 US/Eastern";

impl FromJson for ExecutionData {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        let shares = number(value, "shares")?;
        let price = number(value, "price")?;
        let cumulative_quantity = value
            .get("cumulative")
            .and_then(Value::as_f64)
            .unwrap_or(shares);
        let contract = if value.get("symbol").is_some() {
            Contract::from_json(value)?
        } else {
            Contract::stock("AAPL").build()
        };

        Ok(ExecutionData {
            request_id: 1,
            contract,
            execution: Execution {
                order_id: integer(value, "order_id")? as i32,
                perm_id: opt_integer(value, "perm_id").unwrap_or(0) as i32,
                execution_id: string(value, "id")?,
                time: opt_string(value, "time").unwrap_or_else(|| EXECUTION_TIME.to_string()),
                shares,
                price,
                cumulative_quantity,
                average_price: price,
                order_reference: opt_string(value, "reference").unwrap_or_default(),
                side: opt_string(value, "side").unwrap_or_default(),
                ..Default::default()
            },
        })
    }
}

impl FromJson for CommissionReport {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        Ok(CommissionReport {
            execution_id: string(value, "execution_id")?,
            commission: number(value, "fees")?,
            ..Default::default()
        })
    }
}
