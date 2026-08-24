//! Unit tests for `map_to_placeholder` — SQL placeholder formatting.
//!
//! See `src/database/crud.rs`. The function maps column names to typed Postgres
//! placeholders: typed columns get `$N::type_name`, others get plain `$N`.

use trading_app::test_internals::map_to_placeholder;

#[test]
fn asset_type_column_gets_typed_placeholder() {
    assert_eq!(map_to_placeholder(1, "asset_type"), "$1::asset_type");
}

#[test]
fn status_column_gets_typed_placeholder() {
    assert_eq!(map_to_placeholder(5, "status"), "$5::status");
}

#[test]
fn option_type_column_gets_typed_placeholder() {
    assert_eq!(map_to_placeholder(3, "option_type"), "$3::option_type");
}

#[test]
fn default_column_gets_plain_placeholder() {
    assert_eq!(map_to_placeholder(2, "stock"), "$2");
}

#[test]
fn default_column_strategy_gets_plain_placeholder() {
    assert_eq!(map_to_placeholder(1, "strategy"), "$1");
}

#[test]
fn default_column_quantity_gets_plain_placeholder() {
    assert_eq!(map_to_placeholder(4, "quantity"), "$4");
}

#[test]
fn placeholder_index_preserved() {
    // Verify the index N is correctly threaded through
    assert_eq!(map_to_placeholder(10, "asset_type"), "$10::asset_type");
    assert_eq!(map_to_placeholder(10, "stock"), "$10");
}
