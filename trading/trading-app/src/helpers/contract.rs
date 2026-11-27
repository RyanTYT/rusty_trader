//! Contract vocabulary and row ⇄ IBKR contract conversions.
//!
//! Local symbols encode the instrument in the `stock` column:
//! `FUT:<sym>`, `FX:<base>/<quote>`, `CFD:<sym>`, `CASH:<ccy>`; a bare symbol
//! is a stock (or the underlying of an option, whose identity lives in the
//! separate option columns).
//!
//! [`HashContract`](crate::helpers::contract::HashContract) is the map key used
//! across the crate. Its equality and hash deliberately ignore most of
//! `Contract`: they compare trimmed `primary_exchange`, `symbol`, `currency`,
//! `security_type` and — only for options — right, expiry/month, strike
//! (ordered float) and multiplier. Two contracts built from different sources
//! therefore match when they denote the same instrument, and the key hits a
//! map without a fallback scan.

use ibapi::{
    contracts::{Currency, Exchange},
    prelude::{Contract, ContractMonth, SecurityType, Symbol},
};
use std::{borrow::Cow, hash::Hash};

use crate::database::{
    models::{
        AssetType, CurrentOptionPositionsFullKeys, CurrentStockPositionsFullKeys,
        OpenOptionOrdersFullKeys, OpenStockOrdersFullKeys,
    },
    models_crud::{
        current_positions::current_positions::CurrentPositionsFullKeys,
        open_orders::open_orders::OpenOrdersFullKeys,
        target_positions::{
            target_option_positions::TargetOptionPositionsQtyDiff,
            target_positions::TargetPositionsQtyDiff,
            target_stock_positions::TargetStockPositionsQtyDiff,
        },
    },
};

// #[derive(Debug, Clone)]
// pub struct HashContract {
//     pub contract: Contract,
// }

/// Borrowed-or-owned contract used as a `HashMap` key.
///
/// Hash/equality use the instrument identity described in the module docs, so
/// keys built from DB rows match keys built from IBKR contracts.
#[derive(Debug, Clone)]
pub struct HashContract<'a> {
    /// The contract, borrowed when possible to avoid a clone on lookups.
    pub contract: Cow<'a, Contract>,
}

impl<'a> HashContract<'a> {
    /// Own an already-built contract.
    pub fn new(contract: Contract) -> Self {
        Self {
            contract: Cow::Owned(contract),
        }
    }

    /// Borrow a contract for a short-lived key (no clone).
    pub fn borrowed(contract: &'a Contract) -> Self {
        Self {
            contract: Cow::Borrowed(contract),
        }
    }

    /// Detach from the borrowed lifetime, cloning only if needed.
    pub fn into_owned(self) -> HashContract<'static> {
        HashContract {
            contract: Cow::Owned(self.contract.into_owned()),
        }
    }

    /// Clone the underlying contract into a `'static` key.
    pub fn static_clone(&self) -> HashContract<'static> {
        HashContract {
            contract: Cow::Owned(Contract::clone(&self.contract)),
        }
    }
}

/// The fields a [`HashContract`]/[`HashContractRef`] is hashed + eq'd on
/// (primary_exchange trimmed, symbol, currency, security_type + option
/// fields) — NOT the full `Contract`. Two contracts with the same
/// (pe, symbol, currency, security_type) [+ option fields] are equal even
/// if other `Contract` fields (exchange, trading_class, etc.) differ. This
/// makes the HashMap key match a caller's contract (e.g. constructed from a
/// `PositionKey`) to the published contract — so the `publish_close` cache
/// hits O(1) instead of falling through to a search.
fn contracts_eq(a: &Contract, b: &Contract) -> bool {
    let option_fields_match = if a.security_type == SecurityType::Option {
        a.right == b.right
            && a.last_trade_date_or_contract_month == b.last_trade_date_or_contract_month
            && ordered_float::OrderedFloat(a.strike) == ordered_float::OrderedFloat(b.strike)
            && a.multiplier == b.multiplier
    } else {
        true
    };
    a.primary_exchange.as_str().trim() == b.primary_exchange.as_str().trim()
        && a.symbol == b.symbol
        && a.currency == b.currency
        && a.security_type == b.security_type
        && option_fields_match
}

fn hash_contract<H: std::hash::Hasher>(contract: &Contract, state: &mut H) {
    contract.primary_exchange.as_str().trim().hash(state);
    contract.symbol.as_str().hash(state);
    contract.currency.as_str().hash(state);
    contract.security_type.to_string().hash(state);
    if contract.security_type == SecurityType::Option {
        contract.right.hash(state);
        contract.last_trade_date_or_contract_month.hash(state);
        ordered_float::OrderedFloat(contract.strike).hash(state);
        contract.multiplier.hash(state);
    }
}

impl<'a> PartialEq for HashContract<'a> {
    fn eq(&self, other: &Self) -> bool {
        contracts_eq(&self.contract, &other.contract)
    }
}

impl<'a> Eq for HashContract<'a> {}

impl<'a> Hash for HashContract<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_contract(&self.contract, state)
    }
}

// /// A borrowed [`HashContract`] — wraps a `&Contract` (no clone). Hash + Eq on
// /// the SAME fields as `HashContract`. Use to look up a `HashMap<HashContract, _>`
// /// without cloning the `Contract` into a `HashContract`: build a
// /// `HashContractRef` from the `&Contract` + scan the map (the std `Borrow`
// /// path doesn't work because `Contract` isn't `Hash+Eq` + the borrowed form
// /// has a lifetime). Cross-`PartialEq` with `HashContract` so
// /// `slot_map.iter().find(|(hc, _)| ref_key == *hc)` works.
// #[derive(Debug, Clone, Copy)]
// pub struct HashContractRef<'a> {
//     pub contract: &'a Contract,
// }
//
// impl<'a> HashContractRef<'a> {
//     pub fn new(contract: &'a Contract) -> Self {
//         Self { contract }
//     }
// }
//
// impl<'a> PartialEq for HashContractRef<'a> {
//     fn eq(&self, other: &Self) -> bool {
//         contracts_eq(self.contract, other.contract)
//     }
// }
//
// impl<'a> Eq for HashContractRef<'a> {}
//
// impl<'a> Hash for HashContractRef<'a> {
//     fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
//         hash_contract(self.contract, state)
//     }
// }
//
// /// `HashContractRef == HashContract` (cross-comparison — same fields).
// impl<'a> PartialEq<HashContract<'a>> for HashContractRef<'a> {
//     fn eq(&self, other: &HashContract) -> bool {
//         contracts_eq(self.contract, &other.contract)
//     }
// }
//
// /// `HashContract == HashContractRef` (cross-comparison — same fields).
// impl<'a> PartialEq<HashContractRef<'a>> for HashContract<'a> {
//     fn eq(&self, other: &HashContractRef<'a>) -> bool {
//         contracts_eq(&self.contract, other.contract)
//     }
// }

/// The local-symbol form of a contract's instrument identity.
///
/// Mirrors the prefixes accepted by [`build_contract_from_stock`]; options and
/// unknown security types return the bare symbol (the option identity is stored
/// separately). A `CASH` security type is not expected here and is logged.
pub fn get_local_symbol(contract: &Contract) -> String {
    match AssetType::from_str(&contract.security_type) {
        AssetType::Stock => contract.symbol.as_str().to_string(),
        AssetType::Future => {
            format!("FUT:{}", contract.symbol.as_str())
        }
        AssetType::ForexPair => format!(
            "FX:{}/{}",
            contract.symbol.as_str(),
            contract.currency.as_str()
        ),
        AssetType::CFD => format!("CFD:{}", contract.symbol.as_str()),
        AssetType::Option | AssetType::Unknown => contract.symbol.as_str().to_string(),
        AssetType::CASH => {
            tracing::error!("Should not be getting a local symbol from AssetType cash");
            format!("CASH:{}", contract.symbol.to_string())
        }
    }
}

/// A DB row or strategy diff that a contract can be built from.
pub enum LocalContractTypes {
    /// A target-vs-current diff row.
    TargetPosQtyDiff(TargetPositionsQtyDiff),
    /// A current-position row.
    CurrentPosFk(CurrentPositionsFullKeys),
    /// An open-order row.
    OpenOrders(OpenOrdersFullKeys),
}

/// Build the IBKR contract for a local symbol plus its row columns.
///
/// Prefix dispatch: `CFD:` → CFD on SMART; `FX:<a>/<b>` → IDEALPRO pair;
/// `FUT:` → next-quarter future on SMART in the row currency; `CASH:<ccy>` →
/// the `<ccy>/SGD` IDEALPRO pair (cash rows are always SGD-valued); anything
/// else → stock on SMART with the row exchange/currency.
pub fn build_contract_from_stock(
    stock: &String,
    primary_exchange: &String,
    currency: &String,
) -> Contract {
    let prior = stock.split(":").next();
    if prior == Some("CFD") {
        let symbol = stock.strip_prefix("CFD:").unwrap();
        Contract {
            symbol: symbol.into(),
            security_type: SecurityType::CFD,
            exchange: "SMART".into(),
            currency: currency.into(),
            ..Default::default()
        }
    } else if prior == Some("FX") {
        let mut currencies = stock.strip_prefix("FX:").unwrap().split("/");
        // Contract::forex(currencies.next().unwrap(), currencies.next().unwrap()).build()
        Contract {
            symbol: Symbol::new(currencies.next().unwrap()),
            security_type: ibapi::prelude::SecurityType::ForexPair,
            exchange: "IDEALPRO".into(),
            currency: ibapi::prelude::Currency(currencies.next().unwrap().to_string()),
            ..Default::default()
        }
    } else if prior == Some("FUT") {
        Contract::futures(stock.strip_prefix("FUT:").unwrap())
            .in_currency(currency)
            .on_exchange("SMART")
            .expires_in(ContractMonth::next_quarter())
            .build()
    } else if prior == Some("CASH") {
        Contract {
            symbol: Symbol::new(stock.strip_prefix("CASH:").unwrap()),
            security_type: ibapi::prelude::SecurityType::ForexPair,
            exchange: "IDEALPRO".into(),
            currency: "SGD".into(),
            ..Default::default()
        }
    } else {
        Contract::stock(stock.to_string())
            .primary(primary_exchange)
            .on_exchange("SMART")
            .in_currency(currency)
            .build()
    }
}

/// Build the contract for a target diff, position row or open-order row.
///
/// Option variants are matched first and rebuilt from the option columns
/// (right, expiry, strike, multiplier, exchange, currency); the stock variants
/// go through [`build_contract_from_stock`] and its local-symbol prefixes.
pub(crate) fn get_contract_from(pos_diff: &LocalContractTypes) -> Contract {
    match pos_diff {
        LocalContractTypes::TargetPosQtyDiff(v) => match v {
            TargetPositionsQtyDiff::Stock(TargetStockPositionsQtyDiff {
                primary_exchange,
                currency,
                stock,
                ..
            }) => build_contract_from_stock(stock, primary_exchange, currency),
            TargetPositionsQtyDiff::Options(TargetOptionPositionsQtyDiff {
                stock,
                expiry,
                strike,
                option_type,
                multiplier,
                currency,
                primary_exchange,
                ..
            }) => {
                let mut contract = Contract::option(
                    &stock,
                    &expiry,
                    *strike,
                    match option_type {
                        crate::database::models::OptionType::Put => "P",
                        crate::database::models::OptionType::Call => "C",
                    },
                );
                contract.primary_exchange = Exchange::from(primary_exchange);
                contract.currency = Currency::from(currency);
                contract.multiplier = multiplier.clone();
                contract
            }
        },
        LocalContractTypes::CurrentPosFk(v) => match v {
            CurrentPositionsFullKeys::Stock(CurrentStockPositionsFullKeys {
                stock,
                primary_exchange,
                currency,
                ..
            }) => build_contract_from_stock(stock, primary_exchange, currency),
            CurrentPositionsFullKeys::Options(CurrentOptionPositionsFullKeys {
                stock,
                expiry,
                strike,
                option_type,
                primary_exchange,
                currency,
                multiplier,
                ..
            }) => {
                let mut contract = Contract::option(
                    &stock,
                    &expiry,
                    *strike,
                    match option_type {
                        crate::database::models::OptionType::Put => "P",
                        crate::database::models::OptionType::Call => "C",
                    },
                );
                contract.primary_exchange = Exchange::from(primary_exchange);
                contract.currency = Currency::from(currency);
                contract.multiplier = multiplier.clone();
                contract
            }
        },
        LocalContractTypes::OpenOrders(v) => match v {
            OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                stock,
                primary_exchange,
                currency,
                ..
            }) => build_contract_from_stock(stock, primary_exchange, currency),
            OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                stock,
                expiry,
                strike,
                option_type,
                primary_exchange,
                currency,
                multiplier,
                ..
            }) => {
                let mut contract = Contract::option(
                    &stock,
                    &expiry,
                    *strike,
                    match option_type {
                        crate::database::models::OptionType::Put => "P",
                        crate::database::models::OptionType::Call => "C",
                    },
                );
                contract.primary_exchange = Exchange::from(primary_exchange);
                contract.currency = Currency::from(currency);
                contract.multiplier = multiplier.clone();
                contract
            }
        },
    }
}
