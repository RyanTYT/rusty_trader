//! Order-side decoding and encoding.
//!
//! `Order` covers the ibapi fields a case file scripts; `OrderIBKR` pairs it
//! with a `Contract` and the parent link. `OrderIBKR::to_json` is the compact
//! scenario view (the inverse of its `FromJson`), while the FX plan encoder in
//! [`super::plan`] uses the full serialized order.

use std::collections::HashMap;

use ibapi::contracts::Contract;
use ibapi::orders::{
    Action, Order, OrderStatus,
    order_builder::{limit_order, market_order},
};
use serde_json::{Map, Value, json};

use trading_app::database::models::AssetType;
use trading_app::database::models_crud::open_orders::open_orders::OpenOrdersFullKeys;
use trading_app::execution::order_engine::OrderIBKR;
use trading_app::strategy::strategy::BarUpdateOutcome;

use super::traits::{
    FromJson, JsonError, ToJson, array, integer, number, opt_boolean, opt_integer, opt_string,
    string,
};

fn action(value: &str) -> Result<Action, JsonError> {
    match value {
        "Buy" => Ok(Action::Buy),
        "Sell" => Ok(Action::Sell),
        other => Err(JsonError::new(
            "action",
            format!("unknown action `{other}`"),
        )),
    }
}

impl FromJson for Order {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        let action = action(&string(value, "action")?)?;
        let quantity = number(value, "quantity")?;
        let order_type = opt_string(value, "order_type").unwrap_or_else(|| "Market".to_string());
        let mut order = match order_type.as_str() {
            "Market" => market_order(action, quantity),
            "Limit" => limit_order(action, quantity, number(value, "limit_price")?),
            other => {
                return Err(JsonError::new(
                    "order_type",
                    format!("unknown order_type `{other}`"),
                ));
            }
        };

        order.order_ref = opt_string(value, "order_ref").unwrap_or_default();
        order.perm_id = opt_integer(value, "perm_id").unwrap_or(0) as i32;
        order.order_id = opt_integer(value, "order_id").unwrap_or(0) as i32;
        order.parent_id = opt_integer(value, "parent_id").unwrap_or(0) as i32;
        order.transmit = opt_boolean(value, "transmit").unwrap_or(true);
        Ok(order)
    }
}

impl FromJson for OrderStatus {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        Ok(OrderStatus {
            order_id: integer(value, "order_id")? as i32,
            perm_id: opt_integer(value, "perm_id").unwrap_or(0) as i32,
            status: opt_string(value, "status").unwrap_or_else(|| "Submitted".to_string()),
            filled: value.get("filled").and_then(Value::as_f64).unwrap_or(0.0),
            remaining: value.get("remaining").and_then(Value::as_f64).unwrap_or(0.0),
            ..Default::default()
        })
    }
}

impl FromJson for OrderIBKR {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        let contract = <Contract as FromJson>::from_json(value)?;
        let order = Order::from_json(value)?;
        let references_parent_order =
            opt_integer(value, "references_parent_order").unwrap_or(-1) as i32;
        Ok(OrderIBKR::new(contract, order, references_parent_order))
    }
}

impl ToJson for OrderIBKR {
    fn to_json(&self) -> Value {
        let contract = &self.contract;
        let order = &self.order;
        json!({
            "symbol": contract.symbol.as_str(),
            "security_type": format!("{:?}", contract.security_type),
            "action": format!("{:?}", order.action),
            "quantity": order.total_quantity,
            "order_ref": order.order_ref,
            "parent_id": order.parent_id,
            "transmit": order.transmit,
            "order_type": match order.order_type.as_str() {
                "MKT" => "Market".to_string(),
                "LMT" => "Limit".to_string(),
                other => other.to_string(),
            },
            "limit_price": order.limit_price,
        })
    }
}

/// Encodes the parked-order store (`strategy -> orders`) the way the
/// `handle_bar` case files describe it: each strategy's orders as a sorted
/// array of compact order rows.
impl ToJson for HashMap<String, Vec<OrderIBKR>> {
    fn to_json(&self) -> Value {
        let mut out = Map::new();
        for (key, orders) in self {
            let mut rows: Vec<Value> = orders.iter().map(ToJson::to_json).collect();
            rows.sort_by_key(|value| serde_json::to_string(value).expect("serialize order"));
            out.insert(key.clone(), Value::Array(rows));
        }
        Value::Object(out)
    }
}

impl FromJson for BarUpdateOutcome {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        match string(value, "type")?.as_str() {
            "no_action" => Ok(BarUpdateOutcome::NoAction),
            "emit" => Ok(BarUpdateOutcome::EmitOrders {
                place_orders: array(value, "place")
                    .iter()
                    .enumerate()
                    .map(|(index, item)| {
                        OrderIBKR::from_json(item).map_err(|e| e.prefix(format!("place[{index}]")))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                cancel_orders: array(value, "cancel")
                    .iter()
                    .enumerate()
                    .map(|(index, item)| {
                        OpenOrdersFullKeys::from_json(item)
                            .map_err(|e| e.prefix(format!("cancel[{index}]")))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            "pending" => Ok(BarUpdateOutcome::PendingDbQuery(
                array(value, "asset_types")
                    .iter()
                    .map(|item| {
                        let name = item
                            .as_str()
                            .ok_or_else(|| JsonError::expected("asset_types", "string"))?;
                        Ok(AssetType::from_string(name))
                    })
                    .collect::<Result<Vec<_>, JsonError>>()?,
            )),
            other => Err(JsonError::new(
                "type",
                format!("unknown outcome type `{other}`"),
            )),
        }
    }
}
