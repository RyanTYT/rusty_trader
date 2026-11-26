//! Query-string parsing for the server routes.
//!
//! The handlers take `Query<StrategyQuery>` (`src/server/server.rs:296`) and
//! `Query<Currencies>` (`:253`), which use axum's
//! `Query::try_from_uri` → `serde_urlencoded` path. Driving that directly gives
//! the real extractor behaviour (and rejection body/status) with no socket.
//!
//! `ContractDetails` (`:104-109`) is private, so its query parsing is covered
//! at the route level in `tests/server/server.rs`
//! (`missing_query_params_are_rejected_with_400`).

use axum::extract::Query;
use axum::extract::rejection::QueryRejection;
use axum::http::{StatusCode, Uri};
use serde::Serialize;
use serde::de::DeserializeOwned;
use trading_app::server::server::{Currencies, StrategyQuery};

fn parse<T>(url: &str) -> Result<Query<T>, QueryRejection>
where
    T: DeserializeOwned,
{
    let uri: Uri = url.parse().expect("test URL must be a valid Uri");
    Query::try_from_uri(&uri)
}

fn parse_strategy(query: &str) -> Result<Query<StrategyQuery>, QueryRejection> {
    parse(&format!("http://test.local/strategy/capital?{query}"))
}

fn parse_strategy_url(url: &str) -> Result<Query<StrategyQuery>, QueryRejection> {
    parse(url)
}

fn parse_currencies(query: &str) -> Result<Query<Currencies>, QueryRejection> {
    parse(&format!("http://test.local/exchange_rate?{query}"))
}

fn json_of<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap()
}

fn assert_rejected(
    result: Result<Query<impl std::fmt::Debug>, QueryRejection>,
    expected_fragment: &str,
) -> String {
    let rejection = result.expect_err("expected the query extractor to reject");
    assert_eq!(rejection.status(), StatusCode::BAD_REQUEST);
    let body = rejection.body_text();
    assert!(
        body.contains(expected_fragment),
        "body {body:?} does not contain {expected_fragment:?}"
    );
    body
}

// ─────────────────────────── StrategyQuery ────────────────────────────

#[test]
fn strategy_query_plain_value() {
    let q = parse_strategy("strategy=manual").unwrap();
    assert_eq!(json_of(&q.0), r#"{"strategy":"manual"}"#);
}

#[test]
fn strategy_query_percent_decoding() {
    // `%20` → space, `%2B` → '+', `%2F` → '/', `%25` → '%'.
    let q = parse_strategy("strategy=my%20strat%2Bx%2Fy%25").unwrap();
    assert_eq!(json_of(&q.0), r#"{"strategy":"my strat+x/y%"}"#);
}

#[test]
fn strategy_query_plus_is_decoded_to_space() {
    // form-urlencoded, not RFC 3986: bare '+' means space.
    let q = parse_strategy("strategy=a+b+c").unwrap();
    assert_eq!(json_of(&q.0), r#"{"strategy":"a b c"}"#);
}

#[test]
fn strategy_query_utf8_percent_decoding() {
    let q = parse_strategy("strategy=%E4%B8%AD%E6%96%87").unwrap();
    assert_eq!(json_of(&q.0), r#"{"strategy":"中文"}"#);
}

#[test]
fn strategy_query_empty_value_is_an_empty_string() {
    // `/strategy/capital?strategy=` passes an empty strategy to the
    // consolidator rather than being rejected as missing.
    let q = parse_strategy("strategy=").unwrap();
    assert_eq!(json_of(&q.0), r#"{"strategy":""}"#);
}

#[test]
fn strategy_query_key_without_equals_is_an_empty_string() {
    let q = parse_strategy("strategy").unwrap();
    assert_eq!(json_of(&q.0), r#"{"strategy":""}"#);
}

#[test]
fn strategy_query_invalid_utf8_percent_escape_is_lossily_replaced() {
    // `form_urlencoded::parse` decodes lossily, so `%FF` does not reject the
    // request; it becomes U+FFFD. Pin that behaviour so a stricter decoder
    // later is a deliberate choice, not an accident.
    let q = parse_strategy("strategy=%FF").unwrap();
    let value: serde_json::Value = serde_json::from_str(&json_of(&q.0)).unwrap();
    assert_eq!(value, serde_json::json!({ "strategy": "\u{FFFD}" }));
}

#[test]
fn strategy_query_missing_param_is_rejected_with_400() {
    let rejection = parse_strategy_url("http://test.local/strategy/capital").unwrap_err();
    assert_eq!(rejection.status(), StatusCode::BAD_REQUEST);
    let body = rejection.body_text();
    assert!(
        body.starts_with("Failed to deserialize query string:"),
        "{body}"
    );
    assert!(body.contains("missing field `strategy`"), "{body}");
}

#[test]
fn strategy_query_repeated_param_is_rejected_as_duplicate() {
    let body = assert_rejected(parse_strategy("strategy=a&strategy=b"), "duplicate field `strategy`");
    assert!(
        body.starts_with("Failed to deserialize query string:"),
        "{body}"
    );
}

#[test]
fn strategy_query_unknown_params_are_ignored() {
    let q = parse_strategy("strategy=manual&strategy_id=7&debug=true").unwrap();
    assert_eq!(json_of(&q.0), r#"{"strategy":"manual"}"#);
}

#[test]
fn strategy_query_tolerates_empty_pairs_and_trailing_ampersands() {
    let q = parse_strategy("&&strategy=manual&&").unwrap();
    assert_eq!(json_of(&q.0), r#"{"strategy":"manual"}"#);
}

// ───────────────────────────── Currencies ─────────────────────────────

#[test]
fn currencies_plain_pair() {
    let q = parse_currencies("currency=SGD&quote=USD").unwrap();
    assert_eq!(
        json_of(&q.0),
        r#"{"currency":"SGD","quote":"USD"}"#
    );
}

#[test]
fn currencies_param_order_does_not_matter() {
    let q = parse_currencies("quote=USD&currency=SGD").unwrap();
    assert_eq!(
        json_of(&q.0),
        r#"{"currency":"SGD","quote":"USD"}"#
    );
}

#[test]
fn currencies_percent_decoding() {
    let q = parse_currencies("currency=S%47D&quote=U%53D").unwrap();
    assert_eq!(
        json_of(&q.0),
        r#"{"currency":"SGD","quote":"USD"}"#
    );
}

#[test]
fn currencies_empty_values_are_allowed() {
    // The handler only needs `stock`; empty exchange/currency means "any"
    // when building the contract (`src/server/server.rs:158-167`), so the
    // exchange-rate struct likewise accepts empty strings.
    let q = parse_currencies("currency=&quote=").unwrap();
    assert_eq!(json_of(&q.0), r#"{"currency":"","quote":""}"#);
}

#[test]
fn currencies_missing_quote_is_rejected_with_400() {
    let body = assert_rejected(parse_currencies("currency=SGD"), "missing field `quote`");
    assert!(
        body.starts_with("Failed to deserialize query string:"),
        "{body}"
    );
}

#[test]
fn currencies_missing_currency_is_rejected_with_400() {
    assert_rejected(parse_currencies("quote=USD"), "missing field `currency`");
}

#[test]
fn currencies_repeated_param_is_rejected_as_duplicate() {
    assert_rejected(
        parse_currencies("currency=SGD&currency=USD&quote=USD"),
        "duplicate field `currency`",
    );
}
