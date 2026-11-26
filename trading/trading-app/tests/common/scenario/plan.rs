//! FX plan decoding/encoding.
//!
//! The case files describe a target diff row and the expected plan as compact
//! order specs. Decoding rebuilds the plan with [`OrderIBKR::from_json`]; the
//! encoder then serializes each order fully, which is the view the planner is
//! compared against.

use serde_json::{Value, json};

use trading_app::database::models::OptionType;
use trading_app::database::models_crud::target_positions::target_option_positions::TargetOptionPositionsQtyDiff;
use trading_app::database::models_crud::target_positions::target_positions::TargetPositionsQtyDiff;
use trading_app::database::models_crud::target_positions::target_stock_positions::TargetStockPositionsQtyDiff;
use trading_app::execution::fx_organiser::FxFundingPlan;
use trading_app::execution::order_engine::OrderIBKR;

use super::traits::{FromJson, JsonError, ToJson, array, number, opt_string, string};

impl FromJson for TargetPositionsQtyDiff {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        let asset = string(value, "asset")?;
        let strategy = string(value, "strategy")?;
        let currency = string(value, "currency")?;
        let stock = string(value, "stock")?;
        let primary_exchange = string(value, "exchange")?;
        let avg_price = number(value, "avg_price")?;
        let qty_diff = number(value, "qty_diff")?;
        let current_qty = number(value, "current_qty")?;

        if asset == "option" {
            Ok(TargetPositionsQtyDiff::Options(
                TargetOptionPositionsQtyDiff {
                    strategy,
                    stock,
                    primary_exchange,
                    currency,
                    expiry: string(value, "expiry")?,
                    strike: number(value, "strike")?,
                    multiplier: string(value, "multiplier")?,
                    option_type: option_type(value)?,
                    avg_price,
                    qty_diff,
                    current_qty,
                },
            ))
        } else {
            Ok(TargetPositionsQtyDiff::Stock(TargetStockPositionsQtyDiff {
                strategy,
                primary_exchange,
                currency,
                stock,
                avg_price,
                qty_diff,
                current_qty,
            }))
        }
    }
}

fn option_type(value: &Value) -> Result<OptionType, JsonError> {
    match opt_string(value, "option_type").as_deref() {
        Some("Call") | Some("C") => Ok(OptionType::Call),
        Some("Put") | Some("P") => Ok(OptionType::Put),
        Some(other) => Err(JsonError::new(
            "option_type",
            format!("unknown option_type `{other}`"),
        )),
        None => Err(JsonError::missing("option_type")),
    }
}

fn decode_orders(value: &Value, key: &str) -> Result<Vec<OrderIBKR>, JsonError> {
    array(value, key)
        .iter()
        .enumerate()
        .map(|(index, item)| {
            OrderIBKR::from_json(item).map_err(|e| e.prefix(format!("{key}[{index}]")))
        })
        .collect()
}

impl FromJson for FxFundingPlan {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        let order_batches = array(value, "order_batches")
            .iter()
            .enumerate()
            .map(|(index, batch)| {
                let batch = batch.as_array().ok_or_else(|| {
                    JsonError::expected(format!("order_batches[{index}]"), "array")
                })?;
                batch
                    .iter()
                    .enumerate()
                    .map(|(order_index, item)| {
                        OrderIBKR::from_json(item)
                            .map_err(|e| e.prefix(format!("order_batches[{index}][{order_index}]")))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(FxFundingPlan {
            order_batches,
            root_orders: decode_orders(value, "root_orders")?,
            parked_orders: decode_orders(value, "parked_orders")?,
        })
    }
}

fn order_values(orders: &[OrderIBKR]) -> Vec<Value> {
    orders
        .iter()
        .map(|order| serde_json::to_value(order).expect("serialize OrderIBKR"))
        .collect()
}

impl ToJson for FxFundingPlan {
    fn to_json(&self) -> Value {
        json!({
            "order_batches": self
                .order_batches
                .iter()
                .map(|batch| order_values(batch))
                .collect::<Vec<_>>(),
            "root_orders": order_values(&self.root_orders),
            "parked_orders": order_values(&self.parked_orders),
        })
    }
}
