//! The decode/encode traits and their path-carrying error.

use std::fmt;

use serde_json::Value;

/// Decode a domain value from its JSON scenario representation.
///
/// Implemented directly on the type, so a runner writes
/// `BarUpdateOutcome::from_json(&case["outcome"])?` instead of a free
/// `bar_update_outcome_from_json` function.
pub trait FromJson: Sized {
    fn from_json(value: &Value) -> Result<Self, JsonError>;
}

/// Encode a domain value into its JSON scenario representation.
pub trait ToJson {
    fn to_json(&self) -> Value;
}

/// A decode error with a JSON-ish location, for example
/// `cases[2].events[0].id: expected string`.
///
/// Paths are built relative to the decoded value; callers prefix them with the
/// case/index they are decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonError {
    path: String,
    message: String,
}

impl JsonError {
    pub fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }

    /// `path: expected <kind>`.
    pub fn expected(path: impl Into<String>, kind: &str) -> Self {
        Self::new(path, format!("expected {kind}"))
    }

    /// `path: missing required field`.
    pub fn missing(path: impl Into<String>) -> Self {
        Self::new(path, "missing required field")
    }

    /// Append a field/index segment: `events` `.at("[0]")` -> `events[0]`.
    pub fn at(mut self, segment: impl AsRef<str>) -> Self {
        let segment = segment.as_ref();
        if segment.starts_with('[') {
            self.path.push_str(segment);
        } else {
            if !self.path.is_empty() {
                self.path.push('.');
            }
            self.path.push_str(segment);
        }
        self
    }

    /// Prepend a location: `id` `.prefix("cases[2].events[0]")`.
    pub fn prefix(mut self, prefix: impl AsRef<str>) -> Self {
        let prefix = prefix.as_ref();
        if prefix.is_empty() {
            return self;
        }
        self.path = if self.path.is_empty() {
            prefix.to_string()
        } else {
            format!("{prefix}.{}", self.path)
        };
        self
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for JsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.path.is_empty() {
            f.write_str(&self.message)
        } else {
            write!(f, "{}: {}", self.path, self.message)
        }
    }
}

impl std::error::Error for JsonError {}

fn field<'a>(value: &'a Value, key: &str) -> Result<&'a Value, JsonError> {
    value.get(key).ok_or_else(|| JsonError::missing(key))
}

pub fn string(value: &Value, key: &str) -> Result<String, JsonError> {
    field(value, key)?
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| JsonError::expected(key, "string"))
}

pub fn number(value: &Value, key: &str) -> Result<f64, JsonError> {
    field(value, key)?
        .as_f64()
        .ok_or_else(|| JsonError::expected(key, "number"))
}

pub fn integer(value: &Value, key: &str) -> Result<i64, JsonError> {
    field(value, key)?
        .as_i64()
        .ok_or_else(|| JsonError::expected(key, "integer"))
}

pub fn boolean(value: &Value, key: &str) -> Result<bool, JsonError> {
    field(value, key)?
        .as_bool()
        .ok_or_else(|| JsonError::expected(key, "boolean"))
}

pub fn opt_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_string)
}

pub fn opt_number(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(Value::as_f64)
}

pub fn opt_integer(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(Value::as_i64)
}

pub fn opt_boolean(value: &Value, key: &str) -> Option<bool> {
    value.get(key).and_then(Value::as_bool)
}

/// The array at `key`, or an empty slice when it is absent.
pub fn array<'a>(value: &'a Value, key: &str) -> &'a [Value] {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

// ---------------------------------------------------------------------------
// Panicking accessors for runner-side scripting (seeding, stream construction)
// ---------------------------------------------------------------------------

/// Panicking string accessor for test scripting.
pub fn s(value: &Value, key: &str) -> String {
    string(value, key).unwrap_or_else(|e| panic!("{e}"))
}

/// Panicking optional-string accessor for test scripting.
pub fn opt_s(value: &Value, key: &str) -> Option<String> {
    opt_string(value, key)
}

/// Panicking number accessor for test scripting.
pub fn f(value: &Value, key: &str) -> f64 {
    number(value, key).unwrap_or_else(|e| panic!("{e}"))
}

/// Panicking integer accessor for test scripting.
pub fn i(value: &Value, key: &str) -> i64 {
    integer(value, key).unwrap_or_else(|e| panic!("{e}"))
}

/// Panicking boolean accessor for test scripting.
pub fn b(value: &Value, key: &str) -> bool {
    boolean(value, key).unwrap_or_else(|e| panic!("{e}"))
}

/// Panicking array accessor for test scripting.
pub fn arr<'a>(value: &'a Value, key: &str) -> &'a [Value] {
    array(value, key)
}
