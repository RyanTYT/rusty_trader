//! Serde/JSON contract of the public request/response structs in
//! `src/server/server.rs`.
//!
//! All four structs have private fields, so they are exercised through their
//! (public) `Serialize`/`Deserialize` impls exactly as the HTTP layer does.
//! Assertions are on the exact serialized JSON so field names, ordering
//! (declaration order) and numeric formatting are pinned.

use serde::Serialize;
use serde::de::DeserializeOwned;
use trading_app::server::server::{Currencies, CurrencyVal, StrategyQuery, StrategyValue};

fn round_trip<T>(input: &str) -> String
where
    T: DeserializeOwned + Serialize,
{
    let value: T = serde_json::from_str(input).unwrap();
    serde_json::to_string(&value).unwrap()
}

fn parse_err<T>(input: &str) -> String
where
    T: DeserializeOwned + std::fmt::Debug,
{
    serde_json::from_str::<T>(input)
        .expect_err("expected deserialization to fail")
        .to_string()
}

// ───────────────────────────── CurrencyVal ─────────────────────────────
// `src/server/server.rs:241-244`: `{ price: f64 }` — the body of
// `/exchange_rate` (`:276`).

#[test]
fn currency_val_serializes_as_object_with_price_field() {
    assert_eq!(
        round_trip::<CurrencyVal>(r#"{"price":101.25}"#),
        r#"{"price":101.25}"#
    );
}

#[test]
fn currency_val_is_an_object_not_a_bare_number() {
    let json = round_trip::<CurrencyVal>(r#"{"price":1.5}"#);
    assert!(json.starts_with('{') && json.ends_with('}'), "{json}");
    assert_eq!(json, r#"{"price":1.5}"#);
}

#[test]
fn currency_val_integer_input_serializes_as_float() {
    // The field is f64, so a JSON integer comes back out as `1.0`.
    assert_eq!(round_trip::<CurrencyVal>(r#"{"price":1}"#), r#"{"price":1.0}"#);
}

#[test]
fn currency_val_rejects_missing_price() {
    let err = parse_err::<CurrencyVal>(r#"{}"#);
    assert!(err.contains("missing field `price`"), "{err}");
}

#[test]
fn currency_val_field_name_is_exact_and_case_sensitive() {
    // `Price` is not accepted: there is no serde rename/alias on the field.
    let err = parse_err::<CurrencyVal>(r#"{"Price":1.0}"#);
    assert!(err.contains("missing field `price`"), "{err}");
}

#[test]
fn currency_val_ignores_unknown_fields_and_drops_them_on_serialize() {
    // No `deny_unknown_fields`, so extra keys in a response/request the server
    // produced are tolerated, but they never survive a round-trip.
    assert_eq!(
        round_trip::<CurrencyVal>(r#"{"price":1.5,"volume":10,"nested":{"a":1}}"#),
        r#"{"price":1.5}"#
    );
}

#[test]
fn currency_val_rejects_duplicate_price_field() {
    let err = parse_err::<CurrencyVal>(r#"{"price":1.0,"price":2.0}"#);
    assert!(err.contains("duplicate field `price`"), "{err}");
}

#[test]
fn currency_val_rejects_null_and_string_prices() {
    let err = parse_err::<CurrencyVal>(r#"{"price":null}"#);
    assert!(err.contains("invalid type: null"), "{err}");
    assert!(err.contains("expected f64"), "{err}");

    let err = parse_err::<CurrencyVal>(r#"{"price":"1.5"}"#);
    assert!(err.contains("invalid type: string"), "{err}");
    assert!(err.contains("expected f64"), "{err}");
}

#[test]
fn currency_val_rejects_out_of_range_number() {
    let err = parse_err::<CurrencyVal>(r#"{"price":1e400}"#);
    assert!(err.contains("number out of range"), "{err}");
}

#[test]
fn currency_val_float_formatting_is_canonical_json() {
    assert_eq!(
        round_trip::<CurrencyVal>(r#"{"price":-0.0}"#),
        r#"{"price":-0.0}"#
    );
    assert_eq!(
        round_trip::<CurrencyVal>(r#"{"price":0.1}"#),
        r#"{"price":0.1}"#
    );
    assert_eq!(
        round_trip::<CurrencyVal>(r#"{"price":1e-7}"#),
        r#"{"price":1e-7}"#
    );
    // serde_json 1.0.150 formats floats through `zmij`, which writes an
    // explicit '+' on positive exponents (unlike the old ryu output).
    assert_eq!(
        round_trip::<CurrencyVal>(r#"{"price":1e300}"#),
        r#"{"price":1e+300}"#
    );
    assert_eq!(
        round_trip::<CurrencyVal>(r#"{"price":1.7976931348623157e308}"#),
        r#"{"price":1.7976931348623157e+308}"#
    );
    assert_eq!(
        round_trip::<CurrencyVal>(r#"{"price":5e-324}"#),
        r#"{"price":5e-324}"#
    );
}

// ───────────────────────────── Currencies ──────────────────────────────
// `src/server/server.rs:246-250`: `{ currency: String, quote: String }` — the
// query of `/exchange_rate` (`:251-254`).

#[test]
fn currencies_serializes_in_declaration_order() {
    assert_eq!(
        round_trip::<Currencies>(r#"{"currency":"SGD","quote":"USD"}"#),
        r#"{"currency":"SGD","quote":"USD"}"#
    );
}

#[test]
fn currencies_serialization_order_is_independent_of_input_order() {
    assert_eq!(
        round_trip::<Currencies>(r#"{"quote":"USD","currency":"SGD"}"#),
        r#"{"currency":"SGD","quote":"USD"}"#
    );
}

#[test]
fn currencies_round_trip_is_stable() {
    let once = round_trip::<Currencies>(r#"{"currency":"SGD","quote":"USD"}"#);
    let twice = round_trip::<Currencies>(&once);
    assert_eq!(once, twice);
}

#[test]
fn currencies_rejects_missing_either_field() {
    let err = parse_err::<Currencies>(r#"{"currency":"SGD"}"#);
    assert!(err.contains("missing field `quote`"), "{err}");

    let err = parse_err::<Currencies>(r#"{"quote":"USD"}"#);
    assert!(err.contains("missing field `currency`"), "{err}");

    let err = parse_err::<Currencies>("{}");
    assert!(err.contains("missing field `currency`"), "{err}");
}

#[test]
fn currencies_rejects_non_string_values() {
    let err = parse_err::<Currencies>(r#"{"currency":840,"quote":"USD"}"#);
    assert!(err.contains("invalid type: integer"), "{err}");
    assert!(err.contains("expected a string"), "{err}");
}

#[test]
fn currencies_accepts_empty_strings_and_unicode() {
    // The handler treats `""` as "not provided" when it builds the contract
    // (`src/server/server.rs:158-167`), so empty strings must deserialize.
    assert_eq!(
        round_trip::<Currencies>(r#"{"currency":"","quote":""}"#),
        r#"{"currency":"","quote":""}"#
    );
    // Non-ASCII stays literal (serde_json only escapes control chars/quotes).
    assert_eq!(
        round_trip::<Currencies>(r#"{"currency":"¥","quote":"€"}"#),
        r#"{"currency":"¥","quote":"€"}"#
    );
}

#[test]
fn currencies_ignores_unknown_fields() {
    assert_eq!(
        round_trip::<Currencies>(r#"{"currency":"SGD","quote":"USD","extra":true}"#),
        r#"{"currency":"SGD","quote":"USD"}"#
    );
}

// ──────────────────────────── StrategyValue ────────────────────────────
// `src/server/server.rs:285-288`: `{ sgd_value: f64 }` — the body of
// `/strategy/capital` (`:310`).

#[test]
fn strategy_value_uses_snake_case_field_name() {
    assert_eq!(
        round_trip::<StrategyValue>(r#"{"sgd_value":1234.56}"#),
        r#"{"sgd_value":1234.56}"#
    );
}

#[test]
fn strategy_value_rejects_camel_case_field_name() {
    let err = parse_err::<StrategyValue>(r#"{"sgdValue":1.0}"#);
    assert!(err.contains("missing field `sgd_value`"), "{err}");
}

#[test]
fn strategy_value_integer_input_serializes_as_float() {
    assert_eq!(
        round_trip::<StrategyValue>(r#"{"sgd_value":250}"#),
        r#"{"sgd_value":250.0}"#
    );
}

#[test]
fn strategy_value_rejects_missing_field_and_wrong_type() {
    let err = parse_err::<StrategyValue>("{}");
    assert!(err.contains("missing field `sgd_value`"), "{err}");

    let err = parse_err::<StrategyValue>(r#"{"sgd_value":"1.0"}"#);
    assert!(err.contains("invalid type: string"), "{err}");
}

// ──────────────────────────── StrategyQuery ────────────────────────────
// `src/server/server.rs:290-293`: `{ strategy: String }` — the query of
// `/strategy/capital` (`:296`).

#[test]
fn strategy_query_serializes_single_strategy_field() {
    assert_eq!(
        round_trip::<StrategyQuery>(r#"{"strategy":"manual"}"#),
        r#"{"strategy":"manual"}"#
    );
}

#[test]
fn strategy_query_accepts_empty_string() {
    assert_eq!(
        round_trip::<StrategyQuery>(r#"{"strategy":""}"#),
        r#"{"strategy":""}"#
    );
}

#[test]
fn strategy_query_rejects_missing_strategy() {
    let err = parse_err::<StrategyQuery>("{}");
    assert!(err.contains("missing field `strategy`"), "{err}");
}

#[test]
fn strategy_query_rejects_non_string_strategy() {
    let err = parse_err::<StrategyQuery>(r#"{"strategy":7}"#);
    assert!(err.contains("invalid type: integer"), "{err}");
    assert!(err.contains("expected a string"), "{err}");
}

#[test]
fn strategy_query_ignores_unknown_fields() {
    // A stray `strategy_id` param must not break extraction; only `strategy`
    // is kept.
    assert_eq!(
        round_trip::<StrategyQuery>(r#"{"strategy":"manual","strategy_id":4}"#),
        r#"{"strategy":"manual"}"#
    );
}

#[test]
fn strategy_query_preserves_arbitrary_string_content() {
    let parsed = round_trip::<StrategyQuery>(r#"{"strategy":"my strat/中文 +1%"}"#);
    assert_eq!(parsed, r#"{"strategy":"my strat/中文 +1%"}"#);
}
