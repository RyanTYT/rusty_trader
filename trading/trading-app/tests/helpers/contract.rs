//! Extra coverage for `src/helpers/contract.rs` beyond
//! `tests/market_data/consolidator.rs:944–1240`.
//!
//! Added here (not covered there):
//!   * `get_contract_from` — the `TargetPosQtyDiff` and `OpenOrders` arms.
//!     The existing suite only exercises `CurrentPosFk`.
//!   * `get_local_symbol` — blank symbol/currency and the remaining
//!     `AssetType::Unknown` security types.
//!   * `build_contract_from_stock` — case-sensitive prefix matching, empty
//!     payloads, extra `FX:` segments, and the missing-quote panic.

use chrono::{DateTime, Utc};
use ibapi::contracts::{Contract, SecurityType};
use trading_app::database::models::{
    OpenOptionOrdersFullKeys, OpenStockOrdersFullKeys, OptionType,
};
use trading_app::database::models_crud::open_orders::open_orders::OpenOrdersFullKeys;
use trading_app::database::models_crud::target_positions::target_option_positions::TargetOptionPositionsQtyDiff;
use trading_app::database::models_crud::target_positions::target_positions::TargetPositionsQtyDiff;
use trading_app::database::models_crud::target_positions::target_stock_positions::TargetStockPositionsQtyDiff;
use trading_app::test_internals::{
    LocalContractTypes, build_contract_from_stock, get_contract_from, get_local_symbol,
};

fn epoch() -> DateTime<Utc> {
    DateTime::from_timestamp(0, 0).expect("epoch")
}

fn stock_contract(symbol: &str, primary_exchange: &str, currency: &str) -> Contract {
    Contract {
        symbol: symbol.into(),
        security_type: SecurityType::Stock,
        exchange: "SMART".into(),
        primary_exchange: primary_exchange.into(),
        currency: currency.into(),
        ..Default::default()
    }
}

// ───────────── get_contract_from: TargetPosQtyDiff arm ─────────────

fn target_stock(
    stock: &str,
    primary_exchange: &str,
    currency: &str,
) -> TargetPositionsQtyDiff {
    TargetPositionsQtyDiff::Stock(TargetStockPositionsQtyDiff {
        strategy: "s".to_string(),
        primary_exchange: primary_exchange.to_string(),
        currency: currency.to_string(),
        stock: stock.to_string(),
        avg_price: 1.5,
        qty_diff: 10.0,
        current_qty: 2.0,
    })
}

fn target_option(
    stock: &str,
    option_type: OptionType,
    strike: f64,
    primary_exchange: &str,
    currency: &str,
    multiplier: &str,
) -> TargetPositionsQtyDiff {
    TargetPositionsQtyDiff::Options(TargetOptionPositionsQtyDiff {
        strategy: "s".to_string(),
        stock: stock.to_string(),
        primary_exchange: primary_exchange.to_string(),
        currency: currency.to_string(),
        expiry: "20261218".to_string(),
        strike,
        multiplier: multiplier.to_string(),
        option_type,
        avg_price: 5.0,
        qty_diff: -3.0,
        current_qty: 1.0,
    })
}

/// `TargetPositionsQtyDiff::Stock` delegates to `build_contract_from_stock`, so
/// all three prefix forms (`plain`, `CASH:`, `CFD:`) must look identical to the
/// `CurrentPosFk` arm already tested in `tests/market_data`.
#[test]
fn get_contract_from_target_stock_table() {
    struct Case {
        label: &'static str,
        stock: &'static str,
        primary_exchange: &'static str,
        currency: &'static str,
        expected_security_type: SecurityType,
        expected_symbol: &'static str,
        expected_exchange: &'static str,
        expected_primary: &'static str,
        expected_currency: &'static str,
    }

    let cases = [
        Case {
            label: "plain US stock",
            stock: "AAPL",
            primary_exchange: "NASDAQ",
            currency: "USD",
            expected_security_type: SecurityType::Stock,
            expected_symbol: "AAPL",
            expected_exchange: "SMART",
            expected_primary: "NASDAQ",
            expected_currency: "USD",
        },
        Case {
            label: "CASH: is an FX pair quoted in SGD",
            stock: "CASH:USD",
            primary_exchange: "",
            currency: "EUR",
            expected_security_type: SecurityType::ForexPair,
            expected_symbol: "USD",
            expected_exchange: "IDEALPRO",
            expected_primary: "",
            expected_currency: "SGD",
        },
        Case {
            label: "CFD: prefix",
            stock: "CFD:USOIL",
            primary_exchange: "ignored",
            currency: "USD",
            expected_security_type: SecurityType::CFD,
            expected_symbol: "USOIL",
            expected_exchange: "SMART",
            expected_primary: "",
            expected_currency: "USD",
        },
    ];

    for case in &cases {
        let contract = get_contract_from(&LocalContractTypes::TargetPosQtyDiff(target_stock(
            case.stock,
            case.primary_exchange,
            case.currency,
        )));
        assert_eq!(contract.security_type, case.expected_security_type, "{}", case.label);
        assert_eq!(contract.symbol.as_str(), case.expected_symbol, "{}", case.label);
        assert_eq!(contract.exchange.as_str(), case.expected_exchange, "{}", case.label);
        assert_eq!(
            contract.primary_exchange.as_str(),
            case.expected_primary,
            "{}",
            case.label
        );
        assert_eq!(contract.currency.as_str(), case.expected_currency, "{}", case.label);
    }
}

/// The target-option arm maps `OptionType` to IBKR right (`C`/`P`) and copies
/// expiry, strike, multiplier, venue and currency verbatim.
#[test]
fn get_contract_from_target_option_table() {
    let cases = [
        (OptionType::Call, "C", 200.0),
        (OptionType::Put, "P", 150.5),
    ];

    for (option_type, right, strike) in cases {
        let contract = get_contract_from(&LocalContractTypes::TargetPosQtyDiff(target_option(
            "AAPL",
            option_type.clone(),
            strike,
            "NASDAQ",
            "USD",
            "100",
        )));

        assert_eq!(contract.security_type, SecurityType::Option, "{option_type:?}");
        assert_eq!(contract.symbol.as_str(), "AAPL", "{option_type:?}");
        assert_eq!(contract.exchange.as_str(), "SMART", "{option_type:?}");
        assert_eq!(contract.primary_exchange.as_str(), "NASDAQ", "{option_type:?}");
        assert_eq!(contract.currency.as_str(), "USD", "{option_type:?}");
        assert_eq!(contract.right, right, "{option_type:?}");
        assert_eq!(
            contract.last_trade_date_or_contract_month, "20261218",
            "{option_type:?}"
        );
        assert_eq!(contract.strike, strike, "{option_type:?}");
        assert_eq!(contract.multiplier, "100", "{option_type:?}");
    }
}

// ───────────── get_contract_from: OpenOrders arm ─────────────

fn open_stock(stock: &str, primary_exchange: &str, currency: &str) -> OpenOrdersFullKeys {
    OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
        order_perm_id: 1,
        order_id: 2,
        strategy: "s".to_string(),
        stock: stock.to_string(),
        primary_exchange: primary_exchange.to_string(),
        currency: currency.to_string(),
        time: epoch(),
        quantity: 10.0,
        parent_order_id: 0,
        executions: Vec::new(),
        filled: 0.0,
    })
}

fn open_option(
    stock: &str,
    option_type: OptionType,
    strike: f64,
    primary_exchange: &str,
    currency: &str,
    multiplier: &str,
) -> OpenOrdersFullKeys {
    OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
        order_perm_id: 1,
        order_id: 2,
        strategy: "s".to_string(),
        stock: stock.to_string(),
        primary_exchange: primary_exchange.to_string(),
        currency: currency.to_string(),
        expiry: "20261218".to_string(),
        strike,
        multiplier: multiplier.to_string(),
        option_type,
        time: epoch(),
        quantity: 10.0,
        parent_order_id: 0,
        executions: Vec::new(),
        filled: 0.0,
    })
}

/// `OpenOrdersFullKeys::Stock`: plain stocks and `FX:` pairs. For `FX:` the
/// stored primary/currency columns are ignored in favour of base/quote.
#[test]
fn get_contract_from_open_orders_stock_table() {
    struct Case {
        label: &'static str,
        stock: &'static str,
        primary_exchange: &'static str,
        currency: &'static str,
        expected_security_type: SecurityType,
        expected_symbol: &'static str,
        expected_exchange: &'static str,
        expected_primary: &'static str,
        expected_currency: &'static str,
    }

    let cases = [
        Case {
            label: "plain stock keeps its primary exchange",
            stock: "MSFT",
            primary_exchange: "NASDAQ",
            currency: "USD",
            expected_security_type: SecurityType::Stock,
            expected_symbol: "MSFT",
            expected_exchange: "SMART",
            expected_primary: "NASDAQ",
            expected_currency: "USD",
        },
        Case {
            label: "FX: pair ignores the stored currency",
            stock: "FX:USD/SGD",
            primary_exchange: "ignored",
            currency: "EUR",
            expected_security_type: SecurityType::ForexPair,
            expected_symbol: "USD",
            expected_exchange: "IDEALPRO",
            expected_primary: "",
            expected_currency: "SGD",
        },
    ];

    for case in &cases {
        let contract = get_contract_from(&LocalContractTypes::OpenOrders(open_stock(
            case.stock,
            case.primary_exchange,
            case.currency,
        )));
        assert_eq!(contract.security_type, case.expected_security_type, "{}", case.label);
        assert_eq!(contract.symbol.as_str(), case.expected_symbol, "{}", case.label);
        assert_eq!(contract.exchange.as_str(), case.expected_exchange, "{}", case.label);
        assert_eq!(
            contract.primary_exchange.as_str(),
            case.expected_primary,
            "{}",
            case.label
        );
        assert_eq!(contract.currency.as_str(), case.expected_currency, "{}", case.label);
    }
}

/// `OpenOrdersFullKeys::Options`: same right/strike/expiry/multiplier mapping as
/// the other two arms.
#[test]
fn get_contract_from_open_orders_option_table() {
    let cases = [
        (OptionType::Call, "C", 200.0),
        (OptionType::Put, "P", 150.5),
    ];

    for (option_type, right, strike) in cases {
        let contract = get_contract_from(&LocalContractTypes::OpenOrders(open_option(
            "AAPL",
            option_type.clone(),
            strike,
            "NYSE",
            "USD",
            "10",
        )));

        assert_eq!(contract.security_type, SecurityType::Option, "{option_type:?}");
        assert_eq!(contract.symbol.as_str(), "AAPL", "{option_type:?}");
        assert_eq!(contract.exchange.as_str(), "SMART", "{option_type:?}");
        assert_eq!(contract.primary_exchange.as_str(), "NYSE", "{option_type:?}");
        assert_eq!(contract.currency.as_str(), "USD", "{option_type:?}");
        assert_eq!(contract.right, right, "{option_type:?}");
        assert_eq!(
            contract.last_trade_date_or_contract_month, "20261218",
            "{option_type:?}"
        );
        assert_eq!(contract.strike, strike, "{option_type:?}");
        assert_eq!(contract.multiplier, "10", "{option_type:?}");
    }
}

// ───────────── get_local_symbol: edge cases ─────────────

/// Complements `get_local_symbol_format_table` in
/// `tests/market_data/consolidator.rs:1080`: blank symbol/currency, symbols
/// that contain `:`, and the remaining `Unknown` security types.
#[test]
fn get_local_symbol_edge_cases() {
    let cases: &[(&str, Contract, &str)] = &[
        (
            "blank stock symbol stays blank",
            stock_contract("", "NASDAQ", "USD"),
            "",
        ),
        (
            "future prefix is kept even with a blank symbol",
            Contract {
                security_type: SecurityType::Future,
                ..stock_contract("", "", "USD")
            },
            "FUT:",
        ),
        (
            "forex formats missing parts verbatim",
            Contract {
                security_type: SecurityType::ForexPair,
                ..stock_contract("", "", "")
            },
            "FX:/",
        ),
        (
            "CFD prefix is kept even with a blank symbol",
            Contract {
                security_type: SecurityType::CFD,
                ..stock_contract("", "", "USD")
            },
            "CFD:",
        ),
        (
            "stock symbols containing ':' are not re-parsed",
            stock_contract("BRK:B", "NYSE", "USD"),
            "BRK:B",
        ),
        (
            "ContinuousFuture is Unknown -> symbol only",
            Contract {
                security_type: SecurityType::ContinuousFuture,
                ..stock_contract("ES", "CME", "USD")
            },
            "ES",
        ),
        (
            "FuturesOption is Unknown -> symbol only",
            Contract {
                security_type: SecurityType::FuturesOption,
                ..stock_contract("ES", "CME", "USD")
            },
            "ES",
        ),
        (
            "Warrant is Unknown -> symbol only",
            Contract {
                security_type: SecurityType::Warrant,
                ..stock_contract("ABC.WS", "NYSE", "USD")
            },
            "ABC.WS",
        ),
    ];

    for (label, contract, expected) in cases {
        assert_eq!(&get_local_symbol(contract), expected, "{label}");
    }
}

// ───────── build_contract_from_stock: edge cases ─────────

/// Prefix matching is exact and case-sensitive; unmatched prefixes fall through
/// to the stock branch and the whole string becomes the symbol. Blank payloads
/// are accepted (no validation).
#[test]
fn build_contract_from_stock_prefix_edge_cases() {
    struct Case {
        label: &'static str,
        stock: &'static str,
        primary_exchange: &'static str,
        currency: &'static str,
        expected_security_type: SecurityType,
        expected_symbol: &'static str,
        expected_exchange: &'static str,
        expected_primary: &'static str,
        expected_currency: &'static str,
    }

    let cases = [
        Case {
            label: "lowercase fx: is not the FX prefix",
            stock: "fx:EUR/USD",
            primary_exchange: "NASDAQ",
            currency: "USD",
            expected_security_type: SecurityType::Stock,
            expected_symbol: "fx:EUR/USD",
            expected_exchange: "SMART",
            expected_primary: "NASDAQ",
            expected_currency: "USD",
        },
        Case {
            label: "unknown prefix is part of the stock symbol",
            stock: "FUTX:ES",
            primary_exchange: "",
            currency: "USD",
            expected_security_type: SecurityType::Stock,
            expected_symbol: "FUTX:ES",
            expected_exchange: "SMART",
            expected_primary: "",
            expected_currency: "USD",
        },
        Case {
            label: "blank stock symbol",
            stock: "",
            primary_exchange: "NYSE",
            currency: "USD",
            expected_security_type: SecurityType::Stock,
            expected_symbol: "",
            expected_exchange: "SMART",
            expected_primary: "NYSE",
            expected_currency: "USD",
        },
        Case {
            label: "CFD: with a blank payload",
            stock: "CFD:",
            primary_exchange: "ignored",
            currency: "USD",
            expected_security_type: SecurityType::CFD,
            expected_symbol: "",
            expected_exchange: "SMART",
            expected_primary: "",
            expected_currency: "USD",
        },
        Case {
            label: "CASH: with a blank payload",
            stock: "CASH:",
            primary_exchange: "",
            currency: "",
            expected_security_type: SecurityType::ForexPair,
            expected_symbol: "",
            expected_exchange: "IDEALPRO",
            expected_primary: "",
            expected_currency: "SGD",
        },
        Case {
            label: "FX: keeps only the first two segments",
            stock: "FX:EUR/USD/JPY",
            primary_exchange: "",
            currency: "",
            expected_security_type: SecurityType::ForexPair,
            expected_symbol: "EUR",
            expected_exchange: "IDEALPRO",
            expected_primary: "",
            expected_currency: "USD",
        },
    ];

    for case in &cases {
        let contract = build_contract_from_stock(
            &case.stock.to_string(),
            &case.primary_exchange.to_string(),
            &case.currency.to_string(),
        );
        assert_eq!(contract.security_type, case.expected_security_type, "{}", case.label);
        assert_eq!(contract.symbol.as_str(), case.expected_symbol, "{}", case.label);
        assert_eq!(contract.exchange.as_str(), case.expected_exchange, "{}", case.label);
        assert_eq!(
            contract.primary_exchange.as_str(),
            case.expected_primary,
            "{}",
            case.label
        );
        assert_eq!(contract.currency.as_str(), case.expected_currency, "{}", case.label);
    }
}

/// `FX:` without `/` panics: `build_contract_from_stock` unwraps the quote leg
/// of `split('/')` (src/helpers/contract.rs:200-206). Documents the input
/// contract — callers must pass `FX:BASE/QUOTE`.
#[test]
fn build_contract_from_stock_fx_without_quote_currency_panics() {
    let result = std::panic::catch_unwind(|| {
        build_contract_from_stock(
            &"FX:USD".to_string(),
            &String::new(),
            &String::new(),
        )
    });
    assert!(
        result.is_err(),
        "FX:USD has no quote currency, so the second split('/').unwrap() panics"
    );
}
