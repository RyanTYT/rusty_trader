use ibapi::prelude::{Contract, ContractMonth, SecurityType, Symbol};
use std::hash::Hash;

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

#[derive(Debug, Clone)]
pub struct HashContract {
    pub contract: Contract,
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

impl PartialEq for HashContract {
    fn eq(&self, other: &Self) -> bool {
        contracts_eq(&self.contract, &other.contract)
    }
}

impl Eq for HashContract {}

impl Hash for HashContract {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_contract(&self.contract, state)
    }
}

/// A borrowed [`HashContract`] — wraps a `&Contract` (no clone). Hash + Eq on
/// the SAME fields as `HashContract`. Use to look up a `HashMap<HashContract, _>`
/// without cloning the `Contract` into a `HashContract`: build a
/// `HashContractRef` from the `&Contract` + scan the map (the std `Borrow`
/// path doesn't work because `Contract` isn't `Hash+Eq` + the borrowed form
/// has a lifetime). Cross-`PartialEq` with `HashContract` so
/// `slot_map.iter().find(|(hc, _)| ref_key == *hc)` works.
#[derive(Debug, Clone, Copy)]
pub struct HashContractRef<'a> {
    pub contract: &'a Contract,
}

impl<'a> HashContractRef<'a> {
    pub fn new(contract: &'a Contract) -> Self {
        Self { contract }
    }
}

impl<'a> PartialEq for HashContractRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        contracts_eq(self.contract, other.contract)
    }
}

impl<'a> Eq for HashContractRef<'a> {}

impl<'a> Hash for HashContractRef<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_contract(self.contract, state)
    }
}

/// `HashContractRef == HashContract` (cross-comparison — same fields).
impl<'a> PartialEq<HashContract> for HashContractRef<'a> {
    fn eq(&self, other: &HashContract) -> bool {
        contracts_eq(self.contract, &other.contract)
    }
}

/// `HashContract == HashContractRef` (cross-comparison — same fields).
impl<'a> PartialEq<HashContractRef<'a>> for HashContract {
    fn eq(&self, other: &HashContractRef<'a>) -> bool {
        contracts_eq(&self.contract, other.contract)
    }
}

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

pub enum LocalContractTypes {
    TargetPosQtyDiff(TargetPositionsQtyDiff),
    CurrentPosFk(CurrentPositionsFullKeys),
    OpenOrders(OpenOrdersFullKeys),
}

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

/// Function to get contract from symbol - but NOT for option contracts!
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
                ..
            }) => Contract::option(
                &stock,
                &expiry,
                *strike,
                match option_type {
                    crate::database::models::OptionType::Put => "P",
                    crate::database::models::OptionType::Call => "C",
                },
            ),
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
                ..
            }) => Contract::option(
                &stock,
                &expiry,
                *strike,
                match option_type {
                    crate::database::models::OptionType::Put => "P",
                    crate::database::models::OptionType::Call => "C",
                },
            ),
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
                ..
            }) => Contract::option(
                &stock,
                &expiry,
                *strike,
                match option_type {
                    crate::database::models::OptionType::Put => "P",
                    crate::database::models::OptionType::Call => "C",
                },
            ),
        },
    }
}
