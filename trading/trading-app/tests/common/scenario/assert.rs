//! Exact JSON comparison used by every scenario runner.

use std::cmp::Ordering;

use serde_json::{Value, json};

/// Rounds floats to 6 decimal places and rounds nested arrays/objects.
pub fn normalize(value: Value) -> Value {
    match value {
        Value::Number(n) => match n.as_f64() {
            Some(x) => json!(round6(x)),
            None => Value::Number(n),
        },
        Value::Array(items) => Value::Array(items.into_iter().map(normalize).collect()),
        Value::Object(map) => {
            Value::Object(map.into_iter().map(|(k, v)| (k, normalize(v))).collect())
        }
        other => other,
    }
}

pub fn round6(x: f64) -> f64 {
    (x * 1_000_000.0).round() / 1_000_000.0
}

pub fn sorted_rows(mut rows: Vec<Value>, keys: &[&str]) -> Value {
    rows.sort_by(|a, b| {
        for key in keys {
            let av = &a[*key];
            let bv = &b[*key];
            let ordering = match (av, bv) {
                (Value::Number(x), Value::Number(y)) => x
                    .as_f64()
                    .partial_cmp(&y.as_f64())
                    .unwrap_or(Ordering::Equal),
                _ => av.to_string().cmp(&bv.to_string()),
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
    Value::Array(rows)
}

/// Compares complete rows, field by field. The panic message is the assertion
/// diff, so a mismatch names the table and both row sets.
pub fn assert_exact(label: &str, actual: Value, expected: &Value) {
    let actual = normalize(actual);
    let expected = normalize(expected.clone());
    assert_eq!(actual, expected, "`{label}` does not match");
}
