use std::collections::HashMap;

use ibapi::{contracts::Contract, orders::order_builder::market_order};

use crate::{
    execution::order_engine::{OrderEngine, OrderIBKR},
    helpers::contract::HashContract,
};

pub struct FxAttachments {
    // Contract to be sold -> FX orders:
    // - FX to be attached as child orders directly to Contract
    pub contracts_sold_to_fx_orders: HashMap<HashContract, Vec<OrderIBKR>>,
    // Backed up orders that are blocked by the FX shortfall
    pub backed_up_orders: Vec<OrderIBKR>,
}

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
        strategy: String,
    ) -> FxAttachments {
        let mut remaining_proceeds: HashMap<HashContract, f64> = funds_from_selling
            .into_iter()
            .map(|(contract, amounts)| (contract, amounts.iter().sum()))
            .collect();

        let mut sell_to_fx: HashMap<HashContract, Vec<OrderIBKR>> = HashMap::new();
        let mut backed_up_orders: Vec<OrderIBKR> = Vec::new();
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
                // let fx_symbol = format!("FX:{}/{}", sell_currency, buy_currency);
                let fx_contract = HashContract {
                    contract: Contract {
                        symbol: ibapi::contracts::Symbol::new(sell_currency),
                        security_type: ibapi::prelude::SecurityType::ForexPair,
                        exchange: "IDEALPRO".into(),
                        currency: ibapi::prelude::Currency(buy_currency.clone()),
                        ..Default::default()
                    },
                };

                let consumed = proceeds.min(remaining_shortfall);
                *proceeds -= consumed;
                remaining_shortfall -= consumed;

                let entry = available_funds.entry(buy_currency.clone()).or_insert(0.0);
                *entry = (*entry - consumed).max(0.0);

                // FX order: buy `consumed` units of buy_currency (e.g. buy USD)
                let mut fx_order = market_order(ibapi::orders::Action::Buy, consumed);
                fx_order.order_ref = strategy.clone();

                sell_to_fx
                    .entry(sell_contract.clone())
                    .or_insert_with(Vec::new)
                    .push(OrderIBKR::new(fx_contract.contract.clone(), fx_order, -1));

                if remaining_shortfall <= 0.0 {
                    break;
                }
            }

            if remaining_shortfall > 0.0 {
                tracing::error!(
                    "Order for ({}, {}) cannot be fulfilled for Strategy ({strategy})",
                    buy_contract.contract.symbol,
                    buy_contract.contract.security_type
                );
                continue;
            }
            // Buy order: the actual equity buy, full shortfall qty
            // We carry the buy_contract's order here so place_order can submit it as a child
            let mut buy_order = market_order(ibapi::orders::Action::Buy, *shortfall);
            buy_order.order_ref = strategy.clone();
            backed_up_orders.push(OrderIBKR::new(buy_contract.contract.clone(), buy_order, -1));
        }

        FxAttachments {
            contracts_sold_to_fx_orders: sell_to_fx,
            backed_up_orders,
        }
    }
}
