use ibapi::prelude::{Contract, ContractMonth, SecurityType, Symbol};
use std::hash::Hash;

use crate::database::{
    models::{AssetType, CurrentOptionPositionsFullKeys, CurrentStockPositionsFullKeys},
    models_crud::{
        current_positions::current_positions::CurrentPositionsFullKeys,
        target_positions::{
            target_option_positions::TargetOptionPositionsQtyDiff,
            target_positions::TargetPositionsQtyDiff,
            target_stock_positions::TargetStockPositionsQtyDiff,
        },
    },
};

#[derive(Debug, Clone, PartialEq)]
pub struct HashContract {
    pub contract: Contract,
}

impl Hash for HashContract {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.contract.primary_exchange.as_str().trim().hash(state);
        self.contract.symbol.as_str().hash(state);
        self.contract.currency.as_str().hash(state);
        self.contract.security_type.to_string().hash(state);

        if self.contract.security_type == SecurityType::Option {
            self.contract.right.hash(state);
            self.contract.last_trade_date_or_contract_month.hash(state);
            ordered_float::OrderedFloat(self.contract.strike).hash(state);
            self.contract.multiplier.hash(state);
        }
    }
}

impl Eq for HashContract {}

pub(crate) fn get_local_symbol(contract: &Contract) -> String {
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
}

fn build_contract_from_stock(
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
    }
}
