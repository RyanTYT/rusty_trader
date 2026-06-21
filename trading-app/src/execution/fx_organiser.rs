use std::collections::HashMap;

use ibapi::orders::{Order, order_builder::market_order};

use crate::{
    execution::order_engine::OrderEngine,
    helpers::contract::{HashContract, get_contract_from_local_symbol},
};

impl OrderEngine {
    /// Returns two maps that together describe the SELL → FX → BUY attachment chains:
    ///   - sell_to_fx:  HashContract(sell equity) → Vec<HashContract(FX contract)>
    ///   - fx_to_buys:  HashContract(FX contract) → Vec<HashContract(buy equity)>
    ///
    /// Matching is greedy: shortfalls are iterated in order, and each is satisfied
    /// by consuming sell proceeds until the shortfall is covered or no more sells remain.
    pub fn get_required_fx_attachments(
        funds: HashMap<String, f64>,
        funds_from_selling: HashMap<HashContract, Vec<f64>>,
        insufficient_funds: HashMap<HashContract, f64>,
    ) -> (
        HashMap<HashContract, Vec<(HashContract, Order)>>, // sell -> [(fx_contract, fx_order)]
        HashMap<HashContract, Vec<(HashContract, Order)>>, // fx -> [(buy_contract, buy_order)]
    ) {
        let mut remaining_proceeds: HashMap<HashContract, f64> = funds_from_selling
            .into_iter()
            .map(|(contract, amounts)| (contract, amounts.iter().sum()))
            .collect();

        let mut sell_to_fx: HashMap<HashContract, Vec<(HashContract, Order)>> = HashMap::new();
        let mut fx_to_buys: HashMap<HashContract, Vec<(HashContract, Order)>> = HashMap::new();
        let mut available_funds = funds.clone();

        for (buy_contract, shortfall) in &insufficient_funds {
            let buy_currency = &buy_contract.contract.currency.0;
            let available = available_funds.get(buy_currency).copied().unwrap_or(0.0);
            let mut remaining_shortfall = (shortfall - available).max(0.0);
            if remaining_shortfall <= 0.0 {
                continue;
            }

            for (sell_contract, proceeds) in remaining_proceeds.iter_mut() {
                if *proceeds <= 0.0 {
                    continue;
                }

                let sell_currency = &sell_contract.contract.currency.0;
                let fx_symbol = format!("FX:{}/{}", sell_currency, buy_currency);
                let fx_contract = HashContract {
                    contract: get_contract_from_local_symbol(&fx_symbol, "", buy_currency),
                };

                let consumed = proceeds.min(remaining_shortfall);
                *proceeds -= consumed;
                remaining_shortfall -= consumed;

                let entry = available_funds.entry(buy_currency.clone()).or_insert(0.0);
                *entry = (*entry - consumed).max(0.0);

                // FX order: buy `consumed` units of buy_currency (e.g. buy USD)
                let fx_order = market_order(ibapi::orders::Action::Buy, consumed);

                sell_to_fx
                    .entry(sell_contract.clone())
                    .or_insert_with(Vec::new)
                    .push((fx_contract.clone(), fx_order));

                // Buy order: the actual equity buy, full shortfall qty
                // We carry the buy_contract's order here so place_order can submit it as a child
                let buy_order = market_order(ibapi::orders::Action::Buy, *shortfall);
                fx_to_buys
                    .entry(fx_contract)
                    .or_insert_with(Vec::new)
                    .push((buy_contract.clone(), buy_order));

                if remaining_shortfall <= 0.0 {
                    break;
                }
            }
        }

        (sell_to_fx, fx_to_buys)
    }
}

