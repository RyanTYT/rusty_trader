//! Shared JSON scenario layer.
//!
//! Scenario suites are data-driven tests: a JSON file holds named cases with
//! inputs and whole-table expectations, and a `scenarios()` runner next to the
//! module under test replays them. This module owns everything the suites have
//! in common:
//!
//! - [`traits`] — `FromJson` / `ToJson` and the path-carrying [`JsonError`].
//! - [`contract`], [`order`], [`events`], [`keys`], [`plan`] — trait impls on
//!   the domain and row types, one file per domain.
//! - [`read`] — whole-table DB dumps (DB -> JSON encoders).
//! - [`assert`] — float normalisation and exact whole-table comparison.
//!
//! The case loader stays here too, but runners pass their own file path:
//!
//! ```ignore
//! for (name, case) in scenario::cases("tests/execution/syncer.json") { ... }
//! ```

pub mod assert;
pub mod contract;
pub mod events;
pub mod keys;
pub mod order;
pub mod plan;
pub mod read;
pub mod traits;

#[allow(unused_imports)]
pub use assert::{assert_exact, normalize, round6, sorted_rows};
#[allow(unused_imports)]
pub use traits::{FromJson, JsonError, ToJson};
#[allow(unused_imports)]
pub use traits::{arr, b, f, i, opt_s, s};

use std::fs;

use serde_json::Value;

/// Reads a named-case JSON file.
///
/// `path` is relative to the crate root (for example
/// `tests/execution/syncer.json`); the returned vector keeps the case name and
/// the whole case value.
pub fn cases(path: &str) -> Vec<(String, Value)> {
    let path = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), path);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let root: Value = serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path}: {e}"));
    root["cases"]
        .as_array()
        .unwrap_or_else(|| panic!("{path}: missing `cases` array"))
        .iter()
        .map(|case| {
            let name = case["name"]
                .as_str()
                .unwrap_or_else(|| panic!("{path}: case without a name"))
                .to_string();
            (name, case.clone())
        })
        .collect()
}
