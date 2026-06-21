use std::hash::Hash;

use ibapi::prelude::{Contract, ContractMonth, SecurityType, Symbol};

use crate::database::models::AssetType;

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

/// Function to get contract from symbol - but NOT for option contracts!
pub(crate) fn get_contract_from_local_symbol(
    symbol: &str,
    primary_exchange: &str,
    currency: &str,
) -> Contract {
    if currency == "" {
        tracing::warn!(
            "Currency field is empty for some reason: {symbol:?} ({primary_exchange:?}, {currency:?})"
        )
    }

    let prior = symbol.split(":").next();
    if prior == Some("CFD") {
        let symbol = symbol.strip_prefix("CFD:").unwrap();
        Contract {
            symbol: symbol.into(),
            security_type: SecurityType::CFD,
            exchange: "SMART".into(),
            currency: currency.into(),
            ..Default::default()
        }
    } else if prior == Some("FX") {
        let mut currencies = symbol.strip_prefix("FX:").unwrap().split("/");
        // Contract::forex(currencies.next().unwrap(), currencies.next().unwrap()).build()
        Contract {
            symbol: Symbol::new(currencies.next().unwrap()),
            security_type: ibapi::prelude::SecurityType::ForexPair,
            exchange: "IDEALPRO".into(),
            currency: ibapi::prelude::Currency(currencies.next().unwrap().to_string()),
            ..Default::default()
        }
    } else if prior == Some("FUT") {
        Contract::futures(symbol.strip_prefix("FUT:").unwrap())
            .in_currency(currency)
            .on_exchange("SMART")
            .expires_in(ContractMonth::next_quarter())
            .build()
    } else if prior == Some("CASH") {
        Contract {
            symbol: Symbol::new(symbol.strip_prefix("CASH:").unwrap()),
            security_type: ibapi::prelude::SecurityType::ForexPair,
            exchange: "IDEALPRO".into(),
            currency: "SGD".into(),
            ..Default::default()
        }
    } else {
        Contract::stock(symbol.to_string())
            .primary(primary_exchange)
            .on_exchange("SMART")
            .in_currency(currency)
            .build()
    }
}
