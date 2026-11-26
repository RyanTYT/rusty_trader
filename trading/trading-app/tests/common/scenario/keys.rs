//! Decoders for the database row types used as case seeds.
//!
//! The case files describe rows the way an operator would read them, so these
//! impls accept the short spellings used there: `qty` or `quantity`, `price` or
//! `avg_price`, and `exchange` for a row's `primary_exchange`. `strategy`
//! defaults to `strat`, which is what the `handle_bar` suite names its strategy;
//! every other suite spells the field out explicitly. The database-managed
//! `time` / `last_updated` columns are stamped with `Utc::now()` — dumps omit
//! them.

use chrono::Utc;
use serde_json::Value;

use trading_app::database::models::{
    CurrentOptionPositionsFullKeys, CurrentStockPositionsFullKeys, OpenOptionOrdersFullKeys,
    OpenStockOrdersFullKeys, OptionType, Status, StrategyFullKeys, TargetOptionPositionsFullKeys,
    TargetStockPositionsFullKeys,
};
use trading_app::database::models_crud::open_orders::open_orders::OpenOrdersFullKeys;

use super::traits::{FromJson, JsonError, number, opt_integer, opt_string, string};

/// Default strategy name for a row that omits one.
pub const DEFAULT_STRATEGY: &str = "strat";

fn quantity(value: &Value) -> Result<f64, JsonError> {
    if value.get("qty").is_some() {
        number(value, "qty")
    } else {
        number(value, "quantity")
    }
}

fn avg_price(value: &Value) -> Result<f64, JsonError> {
    if value.get("price").is_some() {
        number(value, "price")
    } else {
        number(value, "avg_price")
    }
}

fn primary_exchange(value: &Value) -> String {
    opt_string(value, "primary_exchange")
        .or_else(|| opt_string(value, "exchange"))
        .unwrap_or_default()
}

fn strategy(value: &Value) -> String {
    opt_string(value, "strategy").unwrap_or_else(|| DEFAULT_STRATEGY.to_string())
}

fn option_type(value: &Value) -> Result<OptionType, JsonError> {
    let Some(raw) = opt_string(value, "option_type") else {
        return Ok(OptionType::Call);
    };
    match raw.chars().next().map(|c| c.to_ascii_uppercase()) {
        Some('C') => Ok(OptionType::Call),
        Some('P') => Ok(OptionType::Put),
        _ => Err(JsonError::new(
            "option_type",
            format!("unknown option_type `{raw}`"),
        )),
    }
}

impl FromJson for StrategyFullKeys {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        if let Some(name) = value.as_str() {
            return Ok(StrategyFullKeys {
                strategy: name.to_string(),
                status: Status::Active,
            });
        }
        Ok(StrategyFullKeys {
            strategy: string(value, "strategy")?,
            status: Status::Active,
        })
    }
}

impl FromJson for OpenStockOrdersFullKeys {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        Ok(OpenStockOrdersFullKeys {
            order_perm_id: opt_integer(value, "perm_id").unwrap_or(0) as i32,
            order_id: opt_integer(value, "order_id").unwrap_or(0) as i32,
            strategy: strategy(value),
            stock: opt_string(value, "stock").unwrap_or_else(|| "AAPL".to_string()),
            primary_exchange: primary_exchange(value),
            currency: opt_string(value, "currency").unwrap_or_else(|| "USD".to_string()),
            time: Utc::now(),
            quantity: quantity(value)?,
            parent_order_id: opt_integer(value, "parent_order_id").unwrap_or(0) as i32,
            executions: value
                .get("executions")
                .and_then(Value::as_array)
                .map(|rows| {
                    rows.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
            filled: value.get("filled").and_then(Value::as_f64).unwrap_or(0.0),
        })
    }
}

impl FromJson for OpenOptionOrdersFullKeys {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        Ok(OpenOptionOrdersFullKeys {
            order_perm_id: opt_integer(value, "perm_id").unwrap_or(0) as i32,
            order_id: opt_integer(value, "order_id").unwrap_or(0) as i32,
            strategy: strategy(value),
            stock: opt_string(value, "stock").unwrap_or_else(|| "AAPL".to_string()),
            primary_exchange: primary_exchange(value),
            currency: opt_string(value, "currency").unwrap_or_else(|| "USD".to_string()),
            time: Utc::now(),
            quantity: quantity(value)?,
            parent_order_id: opt_integer(value, "parent_order_id").unwrap_or(0) as i32,
            expiry: string(value, "expiry")?,
            strike: number(value, "strike")?,
            multiplier: opt_string(value, "multiplier").unwrap_or_else(|| "100".to_string()),
            option_type: option_type(value)?,
            executions: value
                .get("executions")
                .and_then(Value::as_array)
                .map(|rows| {
                    rows.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
            filled: value.get("filled").and_then(Value::as_f64).unwrap_or(0.0),
        })
    }
}

impl FromJson for OpenOrdersFullKeys {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        match opt_string(value, "asset").as_deref() {
            Some("option") => Ok(OpenOrdersFullKeys::Options(
                OpenOptionOrdersFullKeys::from_json(value)?,
            )),
            _ => Ok(OpenOrdersFullKeys::Stock(
                OpenStockOrdersFullKeys::from_json(value)?,
            )),
        }
    }
}

impl FromJson for CurrentStockPositionsFullKeys {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        Ok(CurrentStockPositionsFullKeys {
            stock: string(value, "stock")?,
            primary_exchange: primary_exchange(value),
            currency: opt_string(value, "currency").unwrap_or_else(|| "USD".to_string()),
            strategy: strategy(value),
            quantity: quantity(value)?,
            avg_price: avg_price(value)?,
            last_updated: Utc::now(),
        })
    }
}

impl FromJson for CurrentOptionPositionsFullKeys {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        Ok(CurrentOptionPositionsFullKeys {
            stock: string(value, "stock")?,
            primary_exchange: primary_exchange(value),
            currency: opt_string(value, "currency").unwrap_or_else(|| "USD".to_string()),
            strategy: strategy(value),
            expiry: string(value, "expiry")?,
            strike: number(value, "strike")?,
            multiplier: opt_string(value, "multiplier").unwrap_or_else(|| "100".to_string()),
            option_type: option_type(value)?,
            quantity: quantity(value)?,
            avg_price: avg_price(value)?,
            last_updated: Utc::now(),
        })
    }
}

impl FromJson for TargetStockPositionsFullKeys {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        Ok(TargetStockPositionsFullKeys {
            strategy: strategy(value),
            primary_exchange: primary_exchange(value),
            currency: opt_string(value, "currency").unwrap_or_else(|| "USD".to_string()),
            stock: string(value, "stock")?,
            avg_price: avg_price(value)?,
            quantity: quantity(value)?,
        })
    }
}

impl FromJson for TargetOptionPositionsFullKeys {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        Ok(TargetOptionPositionsFullKeys {
            strategy: strategy(value),
            stock: string(value, "stock")?,
            primary_exchange: primary_exchange(value),
            currency: opt_string(value, "currency").unwrap_or_else(|| "USD".to_string()),
            expiry: string(value, "expiry")?,
            strike: number(value, "strike")?,
            multiplier: opt_string(value, "multiplier").unwrap_or_else(|| "100".to_string()),
            option_type: option_type(value)?,
            avg_price: avg_price(value)?,
            quantity: quantity(value)?,
        })
    }
}
