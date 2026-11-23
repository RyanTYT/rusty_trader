use std::collections::{HashMap, HashSet};
#[cfg(not(feature = "backtest"))]
use std::sync::Weak;

use ibapi::{
    contracts::{Contract, SecurityType, Symbol},
    orders::{
        Action,
        order_builder::{limit_order, market_order},
    },
    prelude::Currency,
};

#[cfg(not(feature = "backtest"))]
use crate::market_data::consolidator::Consolidator;
use crate::{
    database::models_crud::target_positions::target_positions::TargetPositionsQtyDiff,
    execution::{
        fx_backed_up_order::OrderStore,
        order_engine::{OrderEngine, OrderIBKR},
    },
    helpers::contract::{HashContract, LocalContractTypes, get_contract_from},
    market_data::traits::current_price::PriceSupplier,
};

/// Account base currency. Everything that has to be compared across currencies
/// is first converted into this one.
const BASE_CURRENCY: &str = "SGD";

pub struct FxAttachments {
    // Contract to be sold -> FX orders:
    // - FX to be attached as child orders directly to Contract
    pub contracts_sold_to_fx_orders: HashMap<HashContract, Vec<OrderIBKR>>,
    // Backed up orders that are blocked by the FX shortfall
    pub backed_up_orders: Vec<OrderIBKR>,
}

/// Result of planning how every order in a target-position update is paid for.
#[derive(Debug, Default)]
pub struct FxFundingPlan {
    /// Ready-to-place batches: `[head]` or `[closing head, fx legs..]`.
    /// One batch per contract; the first order is the head that
    /// `on_new_qty_diff_for_strat` uses as the canonical contract.
    pub order_batches: Vec<Vec<OrderIBKR>>,
    /// Buys that have to wait for a currency credit before they can be placed.
    pub parked_orders: Vec<OrderIBKR>,
    /// Buys that could not be funded at all this bar. They must not be placed.
    pub deferred_buys: HashSet<HashContract>,
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
        insufficient_funds: HashMap<HashContract, ((f64, f64), f64)>,
        strategy: &str,
        #[cfg(not(feature = "backtest"))] consolidator: &Weak<Consolidator>,
        #[cfg(feature = "backtest")]
        price_supplier: &dyn crate::market_data::traits::current_price::PriceSupplier,
    ) -> FxAttachments {
        let mut remaining_proceeds: HashMap<HashContract, f64> = funds_from_selling
            .into_iter()
            .map(|(contract, amounts)| (contract, amounts.iter().sum()))
            .collect();

        let mut sell_to_fx: HashMap<HashContract, Vec<OrderIBKR>> = HashMap::new();
        let mut backed_up_orders: Vec<OrderIBKR> = Vec::new();
        let mut available_funds = funds.clone();

        for (buy_contract, ((qty, current_price), shortfall)) in &insufficient_funds {
            let buy_currency = &buy_contract.contract.currency.0;
            let available = available_funds
                .get(buy_currency)
                .copied()
                .unwrap_or(0.0)
                .max(0.0);
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
                let ib_fx_contract = Contract {
                    symbol: ibapi::contracts::Symbol::new(sell_currency),
                    security_type: ibapi::prelude::SecurityType::ForexPair,
                    exchange: "IDEALPRO".into(),
                    currency: ibapi::prelude::Currency(buy_currency.clone()),
                    ..Default::default()
                };
                let fx_contract = HashContract {
                    contract: ib_fx_contract.clone(),
                };
                let is_same_currency =
                    ib_fx_contract.symbol.as_str() == ib_fx_contract.currency.as_str();

                let conversion_price = consolidator
                    .upgrade()
                    .expect("Expected consolidator to still be alive when in fx_organiser")
                    .get_current_price(ib_fx_contract, false, &[])
                    .expect("Expected to be able to get FX conversion rate from consolidator in fx_organiser");
                let consumed = proceeds.min(remaining_shortfall / conversion_price);
                *proceeds -= consumed;
                remaining_shortfall -= consumed * conversion_price;

                if remaining_shortfall <= 0.0 {
                    break;
                }

                let entry = available_funds.entry(buy_currency.clone()).or_insert(0.0);
                *entry = (*entry - consumed).max(0.0);

                // FX order: buy `consumed` units of buy_currency (e.g. buy USD)
                let mut fx_order = market_order(ibapi::orders::Action::Sell, consumed);
                fx_order.order_ref = strategy.to_string();

                if !is_same_currency {
                    sell_to_fx
                        .entry(sell_contract.clone())
                        .or_insert_with(Vec::new)
                        .push(OrderIBKR::new(fx_contract.contract.clone(), fx_order, -1));
                }
            }

            // Early return if insufficient
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
            let mut buy_order = market_order(ibapi::orders::Action::Buy, *qty);
            // buy_order.order_ref = strategy.clone();
            buy_order.order_ref = format!("{}:{}", strategy, current_price.to_string());
            buy_order.transmit = true;
            backed_up_orders.push(OrderIBKR::new(buy_contract.contract.clone(), buy_order, -1));
        }

        FxAttachments {
            contracts_sold_to_fx_orders: sell_to_fx,
            backed_up_orders,
        }
    }

    /// Plans how every order in one target-position update is paid for.
    ///
    /// # Sections
    /// 1. Classify: `CASH:<ccy>` rows become spending budgets; every other diff is split
    ///    into the quantity being closed and the quantity being opened
    ///    (`closing = current_qty != 0 && current_qty * qty_diff < 0`).
    /// 2. Value: each closing is valued once in its own currency, then ranked by its
    ///    base-currency value so `smallest first` is comparable across currencies.
    /// 3. Fund: each opening draws from its currency budget, then from a closing in the
    ///    same currency, then from one closing in another currency through a single FX leg.
    ///    Anything left unfunded is deferred and never placed.
    /// 4. Assemble: one batch per contract, `[head]` or `[head, fx legs..]`, with the head
    ///    transmitting only after the last leg.
    ///
    /// # Assumptions
    /// - Only a negative `CASH:` diff is spendable; a cash increase is not an instruction.
    /// - A conversion is one hop (`sell_ccy -> buy_ccy`). Deeper chains cannot be expressed
    ///   as an IBKR bracket (a child cannot have children), so they are deferred.
    /// - A buy funded by a closing is parked, not placed: the `CASH:<buy_ccy>` credit from
    ///   the FX fill is what places it.
    /// - An identical order already in `order_store` is in flight and is not funded again.
    /// - One closing can fund several buys (its cash is decremented); one buy is funded by
    ///   at most one closing.
    /// - A flip (current 100, `qty_diff` -150) is a 100 close plus a 50 open, and the caller
    ///   places both.
    /// - Prices fall back to `avg_price`; a zero price defers the opening.
    #[allow(dead_code)]
    pub fn get_fx_attachments(
        target_pos_diffs: &[TargetPositionsQtyDiff],
        strategy_name: &str,
        order_store: &OrderStore,
        #[cfg(not(feature = "backtest"))] consolidator: &Weak<Consolidator>,
        #[cfg(feature = "backtest")] price_supplier: &dyn PriceSupplier,
    ) -> Result<FxFundingPlan, String> {
        #[cfg(not(feature = "backtest"))]
        let fetch_price = |contract: Contract| -> Result<f64, String> {
            consolidator
                .upgrade()
                .ok_or_else(|| "Consolidator reference dropped in get_fx_attachments".to_string())?
                .get_current_price(contract, false, &[])
        };
        #[cfg(feature = "backtest")]
        let fetch_price =
            |contract: Contract| -> Result<f64, String> {
                price_supplier.get_current_price(contract, false, &[])
            };

        let stored_orders = order_store
            .load_orders(strategy_name)
            .map_err(|e| format!("Could not load parked orders for Strategy ({strategy_name}): {e}"))?
            .unwrap_or_default();

        let mut plan = FxFundingPlan::default();
        let mut budgets: HashMap<String, f64> = HashMap::new();
        let mut ranked_closings: Vec<(f64, ClosingPosition)> = Vec::new();
        let mut openings: Vec<OpeningPosition> = Vec::new();

        // 1. Classify: budgets, closings and openings.
        for pos_diff in target_pos_diffs {
            let (current_qty, qty_diff, currency, avg_price) = match pos_diff {
                TargetPositionsQtyDiff::Stock(v) => {
                    (v.current_qty, v.qty_diff, v.currency.clone(), v.avg_price)
                }
                TargetPositionsQtyDiff::Options(v) => {
                    (v.current_qty, v.qty_diff, v.currency.clone(), v.avg_price)
                }
            };

            // A cash row is a spending budget only when the target decreases.
            if let TargetPositionsQtyDiff::Stock(v) = pos_diff {
                if v.stock.starts_with("CASH:") {
                    let budget = (-qty_diff).max(0.0);
                    if budget > 0.0 {
                        *budgets.entry(currency).or_insert(0.0) += budget;
                    }
                    continue;
                }
            }

            let contract = get_contract_from(&LocalContractTypes::TargetPosQtyDiff(pos_diff.clone()));
            let label = format!("{}, {}", contract.symbol, contract.security_type);
            let multiplier = if contract.security_type == SecurityType::Option {
                contract.multiplier.parse::<f64>().unwrap_or(1.0)
            } else {
                1.0
            };

            let is_closing = current_qty != 0.0 && current_qty * qty_diff < 0.0;
            let qty_closed = if is_closing {
                qty_diff.abs().min(current_qty.abs())
            } else {
                0.0
            };
            let qty_opened = qty_diff.abs() - qty_closed;

            if qty_closed <= 0.0 && qty_opened <= 0.0 {
                // Reconcile-only batch: lets on_new_qty_diff_for_strat cancel stale
                // working orders for this contract.
                let mut head = market_order(Action::Buy, 0.0);
                head.order_ref = strategy_name.to_string();
                head.transmit = false;
                plan.order_batches.push(vec![OrderIBKR::new(contract, head, -1)]);
                continue;
            }

            let price = fetch_price(contract.clone()).unwrap_or_else(|e| {
                tracing::warn!("Could not fetch price for {label} in get_fx_attachments: {e}");
                avg_price
            });

            if qty_closed > 0.0 {
                let cash = qty_closed * price * multiplier;
                if cash > 0.0 {
                    let rank = fx_conversion_rate(&fetch_price, &currency, BASE_CURRENCY)
                        .map(|rate| cash * rate)
                        .unwrap_or(cash);
                    let close_action = if qty_diff < 0.0 { Action::Sell } else { Action::Buy };
                    let mut close_order = if avg_price == 0.0 {
                        market_order(close_action, qty_closed)
                    } else {
                        limit_order(close_action, qty_closed, avg_price)
                    };
                    close_order.order_ref = strategy_name.to_string();
                    close_order.transmit = true;
                    ranked_closings.push((
                        rank,
                        ClosingPosition {
                            hash_contract: HashContract {
                                contract: contract.clone(),
                            },
                            currency: currency.clone(),
                            cash_remaining: cash,
                            order_ibkr: OrderIBKR::new(contract.clone(), close_order, -1),
                            legs: Vec::new(),
                        },
                    ));
                }
            }

            if qty_opened > 0.0 {
                let required = qty_opened * price * multiplier;
                let action = if qty_diff < 0.0 {
                    Action::Sell
                } else {
                    Action::Buy
                };
                let mut order = market_order(action, qty_opened);
                // The execution handler parses the price out of this suffix to size the
                // currency requirement of the parked buy, so it has to stay.
                order.order_ref = format!("{}:{}", strategy_name, price);
                order.transmit = true;
                openings.push(OpeningPosition {
                    hash_contract: HashContract {
                        contract: contract.clone(),
                    },
                    label,
                    currency,
                    required,
                    order_ibkr: OrderIBKR::new(contract, order, -1),
                });
            }
        }

        // 2. Value: rank closings by base-currency value, smallest first.
        ranked_closings.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut closings: Vec<ClosingPosition> = ranked_closings
            .into_iter()
            .map(|(_, closing)| closing)
            .collect();

        // 3. Fund each opening, or defer it.
        for opening in openings {
            if opening.required <= 0.0 {
                tracing::error!(
                    "Order for ({}) cannot be fulfilled for Strategy ({strategy_name}): no price available",
                    opening.label
                );
                plan.deferred_buys.insert(opening.hash_contract);
                continue;
            }

            // Never fund an order that is already in flight.
            if stored_orders.iter().any(|parked| parked == &opening.order_ibkr) {
                continue;
            }

            let available = budgets.get(&opening.currency).copied().unwrap_or(0.0).max(0.0);
            let draw = available.min(opening.required);
            if draw > 0.0 {
                if let Some(budget) = budgets.get_mut(&opening.currency) {
                    *budget = (*budget - draw).max(0.0);
                }
            }
            let residual = opening.required - draw;
            if residual <= 1e-9 {
                plan.order_batches.push(vec![opening.order_ibkr]);
                continue;
            }

            let same_currency = closings.iter().position(|closing| {
                closing.currency == opening.currency && closing.cash_remaining + 1e-9 >= residual
            });
            if let Some(index) = same_currency {
                closings[index].cash_remaining -= residual;
                plan.parked_orders.push(opening.order_ibkr);
                continue;
            }

            let mut one_hop: Option<(usize, f64)> = None;
            for (index, closing) in closings.iter().enumerate() {
                if closing.currency == opening.currency || closing.cash_remaining <= 1e-9 {
                    continue;
                }
                let rate = match fx_conversion_rate(&fetch_price, &closing.currency, &opening.currency) {
                    Ok(rate) if rate > 0.0 => rate,
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!(
                            "No FX rate {} -> {} for Strategy ({strategy_name}): {e}",
                            closing.currency,
                            opening.currency
                        );
                        continue;
                    }
                };
                let consumed = residual / rate;
                if consumed <= closing.cash_remaining + 1e-9 {
                    one_hop = Some((index, consumed));
                    break;
                }
            }

            if let Some((index, consumed)) = one_hop {
                let sell_currency = closings[index].currency.clone();
                closings[index].cash_remaining -= consumed;

                let fx_contract = Contract {
                    symbol: Symbol::new(sell_currency.as_str()),
                    security_type: SecurityType::ForexPair,
                    exchange: "IDEALPRO".into(),
                    currency: Currency(opening.currency.clone()),
                    ..Default::default()
                };
                let mut fx_order = market_order(Action::Sell, consumed);
                fx_order.order_ref = strategy_name.to_string();
                fx_order.transmit = false;

                closings[index]
                    .legs
                    .push(OrderIBKR::new(fx_contract, fx_order, -1));
                plan.parked_orders.push(opening.order_ibkr);
                continue;
            }

            tracing::error!(
                "Order for ({}) cannot be fulfilled for Strategy ({strategy_name})",
                opening.label
            );
            plan.deferred_buys.insert(opening.hash_contract);
        }

        // 4. Assemble: head, then legs; only the last leg transmits, otherwise the
        // bracket never reaches IBKR.
        for closing in closings {
            let mut batch = vec![closing.order_ibkr];
            if !closing.legs.is_empty() {
                batch[0].order.transmit = false;
                let mut legs = closing.legs;
                if let Some(last) = legs.last_mut() {
                    last.order.transmit = true;
                }
                batch.extend(legs);
            }
            plan.order_batches.push(batch);
        }

        Ok(plan)
    }
}

struct ClosingPosition {
    hash_contract: HashContract,
    currency: String,
    cash_remaining: f64,
    order_ibkr: OrderIBKR,
    legs: Vec<OrderIBKR>,
}

struct OpeningPosition {
    hash_contract: HashContract,
    label: String,
    currency: String,
    required: f64,
    order_ibkr: OrderIBKR,
}

fn fx_conversion_rate(
    fetch_price: &impl Fn(Contract) -> Result<f64, String>,
    from: &str,
    to: &str,
) -> Result<f64, String> {
    if from == to {
        return Ok(1.0);
    }
    let contract = Contract {
        symbol: Symbol::new(from),
        security_type: SecurityType::ForexPair,
        exchange: "IDEALPRO".into(),
        currency: Currency(to.to_string()),
        ..Default::default()
    };
    fetch_price(contract)
}
