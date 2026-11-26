//! Whole-table dumps used by the JSON scenario suites.
//!
//! Columns that change on every run (`last_updated`, `created_at`) are omitted
//! so a case file can describe the durable state exactly. Every other column is
//! included, and rows are sorted by primary key.
//!
//! `option_type` is rendered with `Display` (`C` / `P`), matching the case
//! files for every table.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

use trading_app::database::models::OptionType;

use super::assert::sorted_rows;

fn time(value: Option<DateTime<Utc>>) -> Option<String> {
    value.map(|t| t.to_rfc3339())
}

fn decimal(value: Option<Decimal>) -> Option<f64> {
    value.map(|d| d.to_string().parse::<f64>().expect("decimal -> f64"))
}

pub async fn strategy(pool: &PgPool) -> Value {
    let rows = sqlx::query("SELECT strategy, status FROM trading.strategy")
        .fetch_all(pool)
        .await
        .expect("dump strategy");
    sorted_rows(
        rows.iter()
            .map(|r| {
                json!({
                    "strategy": r.get::<String, _>("strategy"),
                    "status": r.get::<String, _>("status"),
                })
            })
            .collect(),
        &["strategy"],
    )
}

pub async fn current_positions(pool: &PgPool) -> Value {
    let stock = sqlx::query(
        "SELECT stock, primary_exchange, currency, strategy, quantity, avg_price
         FROM trading.current_stock_positions",
    )
    .fetch_all(pool)
    .await
    .expect("dump current_stock_positions");
    let option = sqlx::query(
        "SELECT stock, primary_exchange, currency, strategy, expiry, strike, multiplier,
                option_type, quantity, avg_price
         FROM trading.current_option_positions",
    )
    .fetch_all(pool)
    .await
    .expect("dump current_option_positions");

    let mut out: Vec<Value> = stock
        .iter()
        .map(|r| {
            json!({
                "asset": "stock",
                "stock": r.get::<String, _>("stock"),
                "primary_exchange": r.get::<String, _>("primary_exchange"),
                "currency": r.get::<String, _>("currency"),
                "strategy": r.get::<String, _>("strategy"),
                "quantity": r.get::<f64, _>("quantity"),
                "avg_price": r.get::<f64, _>("avg_price"),
            })
        })
        .collect();
    out.extend(option.iter().map(|r| {
        json!({
            "asset": "option",
            "stock": r.get::<String, _>("stock"),
            "primary_exchange": r.get::<String, _>("primary_exchange"),
            "currency": r.get::<String, _>("currency"),
            "strategy": r.get::<String, _>("strategy"),
            "expiry": r.get::<String, _>("expiry"),
            "strike": r.get::<f64, _>("strike"),
            "multiplier": r.get::<String, _>("multiplier"),
            "option_type": r.get::<OptionType, _>("option_type").to_string(),
            "quantity": r.get::<f64, _>("quantity"),
            "avg_price": r.get::<f64, _>("avg_price"),
        })
    }));
    sorted_rows(out, &["strategy", "stock", "expiry", "strike"])
}

pub async fn open_orders(pool: &PgPool) -> Value {
    let stock = sqlx::query(
        "SELECT order_perm_id, order_id, strategy, stock, primary_exchange, currency,
                quantity, parent_order_id, filled, executions
         FROM trading.open_stock_orders",
    )
    .fetch_all(pool)
    .await
    .expect("dump open_stock_orders");
    let option = sqlx::query(
        "SELECT order_perm_id, order_id, strategy, stock, primary_exchange, currency,
                expiry, strike, multiplier, option_type, quantity, parent_order_id,
                filled, executions
         FROM trading.open_option_orders",
    )
    .fetch_all(pool)
    .await
    .expect("dump open_option_orders");

    let mut out: Vec<Value> = stock
        .iter()
        .map(|r| {
            json!({
                "asset": "stock",
                "order_perm_id": r.get::<i32, _>("order_perm_id"),
                "order_id": r.get::<i32, _>("order_id"),
                "strategy": r.get::<String, _>("strategy"),
                "stock": r.get::<String, _>("stock"),
                "primary_exchange": r.get::<String, _>("primary_exchange"),
                "currency": r.get::<String, _>("currency"),
                "quantity": r.get::<f64, _>("quantity"),
                "parent_order_id": r.get::<Option<i32>, _>("parent_order_id"),
                "filled": r.get::<f64, _>("filled"),
                "executions": r.get::<Option<Vec<String>>, _>("executions"),
            })
        })
        .collect();
    out.extend(option.iter().map(|r| {
        json!({
            "asset": "option",
            "order_perm_id": r.get::<i32, _>("order_perm_id"),
            "order_id": r.get::<i32, _>("order_id"),
            "strategy": r.get::<String, _>("strategy"),
            "stock": r.get::<String, _>("stock"),
            "primary_exchange": r.get::<String, _>("primary_exchange"),
            "currency": r.get::<String, _>("currency"),
            "expiry": r.get::<String, _>("expiry"),
            "strike": r.get::<f64, _>("strike"),
            "multiplier": r.get::<String, _>("multiplier"),
            "option_type": r.get::<OptionType, _>("option_type").to_string(),
            "quantity": r.get::<f64, _>("quantity"),
            "parent_order_id": r.get::<Option<i32>, _>("parent_order_id"),
            "filled": r.get::<f64, _>("filled"),
            "executions": r.get::<Option<Vec<String>>, _>("executions"),
        })
    }));
    sorted_rows(out, &["order_perm_id", "order_id"])
}

pub async fn transactions(pool: &PgPool) -> Value {
    let stock = sqlx::query(
        "SELECT execution_id, strategy, stock, primary_exchange, currency,
                order_perm_id, time, price, quantity, fees
         FROM trading.stock_transactions",
    )
    .fetch_all(pool)
    .await
    .expect("dump stock_transactions");
    let option = sqlx::query(
        "SELECT execution_id, strategy, stock, primary_exchange, currency,
                expiry, strike, multiplier, option_type, order_perm_id, time,
                price, quantity, fees
         FROM trading.option_transactions",
    )
    .fetch_all(pool)
    .await
    .expect("dump option_transactions");

    let mut out: Vec<Value> = stock
        .iter()
        .map(|r| {
            json!({
                "asset": "stock",
                "execution_id": r.get::<String, _>("execution_id"),
                "strategy": r.get::<String, _>("strategy"),
                "stock": r.get::<String, _>("stock"),
                "primary_exchange": r.get::<String, _>("primary_exchange"),
                "currency": r.get::<String, _>("currency"),
                "order_perm_id": r.get::<i32, _>("order_perm_id"),
                "time": time(r.get::<Option<DateTime<Utc>>, _>("time")),
                "price": r.get::<f64, _>("price"),
                "quantity": r.get::<f64, _>("quantity"),
                "fees": decimal(r.get::<Option<Decimal>, _>("fees")),
            })
        })
        .collect();
    out.extend(option.iter().map(|r| {
        json!({
            "asset": "option",
            "execution_id": r.get::<String, _>("execution_id"),
            "strategy": r.get::<String, _>("strategy"),
            "stock": r.get::<String, _>("stock"),
            "primary_exchange": r.get::<String, _>("primary_exchange"),
            "currency": r.get::<String, _>("currency"),
            "expiry": r.get::<String, _>("expiry"),
            "strike": r.get::<f64, _>("strike"),
            "multiplier": r.get::<String, _>("multiplier"),
            "option_type": r.get::<OptionType, _>("option_type").to_string(),
            "order_perm_id": r.get::<i32, _>("order_perm_id"),
            "time": time(r.get::<Option<DateTime<Utc>>, _>("time")),
            "price": r.get::<f64, _>("price"),
            "quantity": r.get::<f64, _>("quantity"),
            "fees": decimal(r.get::<Option<Decimal>, _>("fees")),
        })
    }));
    sorted_rows(out, &["execution_id"])
}

pub async fn staged_commissions(pool: &PgPool) -> Value {
    let rows = sqlx::query("SELECT execution_id, fees FROM trading.staged_commissions")
        .fetch_all(pool)
        .await
        .expect("dump staged_commissions");
    sorted_rows(
        rows.iter()
            .map(|r| {
                json!({
                    "execution_id": r.get::<String, _>("execution_id"),
                    "fees": decimal(Some(r.get::<Decimal, _>("fees"))),
                })
            })
            .collect(),
        &["execution_id"],
    )
}

pub async fn targets(pool: &PgPool) -> Value {
    let stock = sqlx::query(
        "SELECT strategy, primary_exchange, currency, stock, avg_price, quantity
         FROM trading.target_stock_positions",
    )
    .fetch_all(pool)
    .await
    .expect("dump target_stock_positions");
    let option = sqlx::query(
        "SELECT strategy, primary_exchange, currency, stock, expiry, strike, multiplier,
                option_type, avg_price, quantity
         FROM trading.target_option_positions",
    )
    .fetch_all(pool)
    .await
    .expect("dump target_option_positions");

    let mut out: Vec<Value> = stock
        .iter()
        .map(|r| {
            json!({
                "asset": "stock",
                "strategy": r.get::<String, _>("strategy"),
                "primary_exchange": r.get::<String, _>("primary_exchange"),
                "currency": r.get::<String, _>("currency"),
                "stock": r.get::<String, _>("stock"),
                "avg_price": r.get::<f64, _>("avg_price"),
                "quantity": r.get::<f64, _>("quantity"),
            })
        })
        .collect();
    out.extend(option.iter().map(|r| {
        json!({
            "asset": "option",
            "strategy": r.get::<String, _>("strategy"),
            "primary_exchange": r.get::<String, _>("primary_exchange"),
            "currency": r.get::<String, _>("currency"),
            "stock": r.get::<String, _>("stock"),
            "expiry": r.get::<String, _>("expiry"),
            "strike": r.get::<f64, _>("strike"),
            "multiplier": r.get::<String, _>("multiplier"),
            "option_type": r.get::<OptionType, _>("option_type").to_string(),
            "avg_price": r.get::<f64, _>("avg_price"),
            "quantity": r.get::<f64, _>("quantity"),
        })
    }));
    sorted_rows(out, &["strategy", "stock", "expiry", "strike"])
}
