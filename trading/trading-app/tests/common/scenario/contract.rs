//! `Contract` decoding.
//!
//! Two shapes appear in the case files:
//!
//! - a compact contract spec: `symbol`, `security_type`, `exchange`,
//!   `currency`, `primary_exchange`, and for options `expiry`, `strike`,
//!   `multiplier`, `option_type`;
//! - an event/position row whose option details are nested under `option`.
//!
//! Both decode to the same `Contract`, with the defaults the ibapi builders
//! use: exchange `SMART` (or `IDEALPRO` for FX), currency `USD`, primary
//! exchange empty, option multiplier `100`, call when the right is omitted.

use ibapi::contracts::{Contract, Currency, Exchange, SecurityType, Symbol};
use serde_json::Value;

use super::traits::{array, number, opt_string, string, FromJson, JsonError};

impl FromJson for Contract {
    fn from_json(value: &Value) -> Result<Self, JsonError> {
        let symbol = string(value, "symbol")?;
        let security_type = match opt_string(value, "security_type") {
            Some(raw) => parse_security_type(&raw)?,
            None if value.get("option").is_some() => SecurityType::Option,
            None => SecurityType::Stock,
        };

        let exchange = opt_string(value, "exchange")
            .unwrap_or_else(|| default_exchange(&security_type).to_string());
        let currency = opt_string(value, "currency").unwrap_or_else(|| "USD".to_string());
        let primary_exchange = opt_string(value, "primary_exchange").unwrap_or_default();

        match security_type {
            SecurityType::Stock => Ok(Contract::stock(symbol)
                .on_exchange(exchange)
                .in_currency(currency)
                .primary(primary_exchange)
                .build()),
            SecurityType::Option => {
                let nested = value.get("option");
                let option = nested.unwrap_or(value);
                let prefix = if nested.is_some() { "option" } else { "" };
                let expiry = string(option, "expiry").map_err(|e| e.prefix(prefix))?;
                let strike = number(option, "strike").map_err(|e| e.prefix(prefix))?;
                let multiplier =
                    opt_string(option, "multiplier").unwrap_or_else(|| "100".to_string());
                let right = option_right(option).map_err(|e| e.prefix(prefix))?;

                let mut contract = Contract::option(&symbol, &expiry, strike, right);
                contract.exchange = Exchange::from(exchange);
                contract.primary_exchange = Exchange::from(primary_exchange);
                contract.currency = Currency::from(currency);
                contract.multiplier = multiplier;
                Ok(contract)
            }
            other => Ok(Contract {
                symbol: Symbol::new(symbol),
                security_type: other,
                exchange: Exchange::from(exchange),
                currency: Currency::from(currency),
                ..Default::default()
            }),
        }
    }
}

fn default_exchange(security_type: &SecurityType) -> &'static str {
    match security_type {
        SecurityType::Stock | SecurityType::Option => "SMART",
        _ => "IDEALPRO",
    }
}

fn parse_security_type(raw: &str) -> Result<SecurityType, JsonError> {
    match raw {
        "Stock" => Ok(SecurityType::Stock),
        "Option" => Ok(SecurityType::Option),
        "ForexPair" => Ok(SecurityType::ForexPair),
        "Future" => Ok(SecurityType::Future),
        "CFD" => Ok(SecurityType::CFD),
        // `SecurityType` has no `Cash` variant; ibapi carries it as `Other`.
        // `AssetType::from_str` maps it (and every other exotic type) to
        // `Unknown`, which the execution handler rejects.
        "Cash" => Ok(SecurityType::Other("CASH".to_string())),
        // `Index` is the canonical supported-but-not-traded security type; it
        // also maps to `AssetType::Unknown`.
        "Index" => Ok(SecurityType::Index),
        other => Err(JsonError::new(
            "security_type",
            format!("unknown security_type `{other}`"),
        )),
    }
}

/// `option_type` accepts `Call`/`C` and `Put`/`P`; the first character is
/// matched case-insensitively, and an omitted right means a call.
fn option_right(option: &Value) -> Result<&'static str, JsonError> {
    let Some(raw) = opt_string(option, "option_type") else {
        return Ok("C");
    };
    match raw.chars().next().map(|c| c.to_ascii_uppercase()) {
        Some('C') => Ok("C"),
        Some('P') => Ok("P"),
        _ => Err(JsonError::new(
            "option_type",
            format!("unknown option_type `{raw}`"),
        )),
    }
}

/// The option fields shared by row decoders; returns `(expiry, strike,
/// multiplier, option_type)` with the same defaults as [`FromJson for
/// Contract`].
pub fn option_fields(value: &Value) -> Result<(String, f64, String, String), JsonError> {
    Ok((
        string(value, "expiry")?,
        number(value, "strike")?,
        opt_string(value, "multiplier").unwrap_or_else(|| "100".to_string()),
        option_right(value)?.to_string(),
    ))
}

/// All elements of an array field, decoded with `T::from_json`.
pub fn decode_array<T: FromJson>(value: &Value, key: &str) -> Result<Vec<T>, JsonError> {
    array(value, key)
        .iter()
        .enumerate()
        .map(|(index, item)| T::from_json(item).map_err(|e| e.prefix(format!("{key}[{index}]"))))
        .collect()
}
