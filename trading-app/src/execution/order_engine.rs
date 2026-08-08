// Issues: Line 619
// - NEED to update place_order to function properly and efficiently
// await for order updates
// - once async version is released and stable - can use tokio threads instead of new os kernel
// thread
// - More issues:
// -- Need to deconflict potential revisions to executions - .02 is a revision to an execution with
// .01 and so on
// -- Finish writing the place_orders_for_strategy for options
// -- Update the place_orders_for_strategy functionality to update according to current open
// orders, ...
// -- place_orders_for_strategy fn will update open orders but if it updates it too late, an
// execution may come in with no open order and execution (NOTE: from my understanding this should
// be fine as the new execution will be filed under "unknown" for which target positions will
// always be 0 && when we delete the open order it is only because the open order is in the
// opposite direction we want to go in - i.e. under "unknown", position to go to 0 is also fine -
// just maybe different order types but that is fine - should be minimal impact)
use ibapi::{
    Client,
    orders::{
        Order,
        order_builder::{limit_order, market_order},
    },
    prelude::Contract,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{
    collections::{HashMap, VecDeque},
    sync::Weak,
};

use crate::{
    database::{
        crud::CRUDTrait,
        models::{AssetType, OpenOptionOrdersFullKeys, OpenStockOrdersFullKeys},
        models_crud::{
            open_orders::open_orders::{
                OpenOrdersCRUD, OpenOrdersFullKeys, OpenOrdersOps, OpenOrdersPrimaryKeys,
            },
            target_positions::{
                target_positions::{
                    TargetPositionsCRUD, TargetPositionsOps, TargetPositionsQtyDiff,
                },
                target_stock_positions::TargetStockPositionsQtyDiff,
            },
        },
    },
    execution::{fx_backed_up_order::OrderStore, fx_organiser::FxAttachments},
    helpers::contract::{HashContract, LocalContractTypes, get_contract_from},
    market_data::{consolidator::Consolidator, traits::current_price::PriceSupplier},
    strategy::strategy::{BarUpdateOutcome, StrategyEnum, StrategyExecutor},
};

// pub struct StaleOrderEngine {
//     pub pool: PgPool,
//     // pub(super) strategy_map: Arc<HashMap<String, StrategyEnum>>,
//     // pub(super) account: String,
// }

#[derive(Debug, Clone)]
pub struct OrderEngine {
    pool: PgPool,
    tokio_handle: tokio::runtime::Handle,
    // pub(super) strategy_map: Arc<HashMap<String, StrategyEnum>>,
    // pub(super) account: String,
    // order_update_stream: Arc<OrderUpdateStreamController>,
}

// // Dummy implementations since in the app, only 1 should live at any point in time
// impl PartialEq for OrderEngine {
//     fn eq(&self, _other: &Self) -> bool {
//         true
//     }
// }
//
// impl Eq for OrderEngine {}
//
// impl PartialOrd for OrderEngine {
//     fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
//         Some(1.cmp(&2))
//     }
// }
//
// impl Ord for OrderEngine {
//     fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
//         1.cmp(&2)
//     }
// }

// impl StaleOrderEngine {
//     pub fn with_order_update_stream(
//         &self,
//         strategy_map: Arc<HashMap<String, StrategyEnum>>,
//         weak_client: Weak<Client>,
//         default_strategy: Option<String>,
//     ) -> Option<OrderEngine> {
//         match OrderUpdateStreamController::new(
//             self.pool.clone(),
//             weak_client,
//             strategy_map,
//             default_strategy,
//         ) {
//             Some(order_update_stream) => Some(OrderEngine {
//                 // strategy_map: self.strategy_map.clone(),
//                 // account: self.account.clone(),
//                 pool: self.pool.clone(),
//                 order_update_stream: Arc::new(order_update_stream),
//             }),
//             None => None,
//         }
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderIBKR {
    pub contract: Contract,
    pub order: Order,
    // For efficiency purposes
    // -> we directly link via the index of the parent in the Vec this is passed in
    references_parent_order: i32,
}

impl OrderIBKR {
    pub fn new(contract: Contract, order: Order, references_parent_order: i32) -> Self {
        Self {
            contract,
            order,
            references_parent_order,
        }
    }
}

impl OrderEngine {
    // Active Strategies passed for deconflicting of executions in cases where it occurs
    pub fn new(
        pool: PgPool,
        tokio_handle: tokio::runtime::Handle,
        // active_strategies: &Vec<StrategyParameters>,
    ) -> Self {
        // create strategy map
        // let mut strategy_map = HashMap::new();
        // for strategy in active_strategies {
        //     strategy_map.insert(strategy.strategy.get_name(), strategy.strategy.clone());
        // }
        Self { pool, tokio_handle }
    }

    /// Note: it is on onus of client to pass with the correct .transmit field
    pub fn place_order(weak_client: &Weak<Client>, order_ibkr: OrderIBKR) -> i32 {
        let client_opt = weak_client.upgrade();
        if client_opt.is_none() {
            tracing::error!("Client is dead when trying to place order");
        }
        let client = client_opt.unwrap();
        let order_id = client.next_order_id();
        std::thread::spawn(move || {
            let order_id = client.next_order_id();

            tracing::info!(
                "Order submitted to IBKR: {:?} for {:?}",
                order_ibkr.order.action,
                order_ibkr.contract.symbol
            );
            if let Err(e) = client.submit_order(order_id, &order_ibkr.contract, &order_ibkr.order) {
                tracing::error!(
                    message=%format!(
                        "Failed to place order for {}, order: {}, Error: {}",
                        order_ibkr.contract.symbol,
                        order_ibkr.order.action,
                        e
                    )
                );
            }
        });

        order_id
    }

    /// Note: it is on onus of client to pass with the correct .transmit field
    pub fn place_orders(weak_client: &Weak<Client>, orders: impl IntoIterator<Item = OrderIBKR>) {
        let orders_iter = orders.into_iter();
        let (lower_bound, _) = orders_iter.size_hint();
        let mut order_ids = Vec::with_capacity(lower_bound);
        for (idx, mut order_ibkr) in orders_iter.enumerate() {
            // let order = order_ibkr.order;
            if order_ibkr.references_parent_order >= 0 {
                order_ibkr.order.parent_id = order_ids[order_ibkr.references_parent_order as usize];
            }
            let order_id = Self::place_order(&weak_client, order_ibkr);
            order_ids[idx] = order_id;
        }
    }

    pub fn handle_bar_update_outcome(
        &self,
        weak_client: &Weak<Client>,
        consolidator: &Weak<Consolidator>,
        bar_update_outcome: BarUpdateOutcome,
        strategy: &StrategyEnum,
        order_store: &OrderStore,
    ) {
        match bar_update_outcome {
            BarUpdateOutcome::EmitOrders(orders_ibkr) => {
                Self::place_orders(weak_client, orders_ibkr)
            }
            BarUpdateOutcome::PendingDbQuery(asset_types) => {
                for asset_type in asset_types.iter() {
                    let target_positions_crud =
                        TargetPositionsCRUD::from(&asset_type, self.pool.clone());
                    let target_positions_crud_clone = target_positions_crud.clone();
                    let target_pos_diffs = match self.tokio_handle.block_on(async move {
                        target_positions_crud_clone
                            .get_target_pos_diff_by_strat(&strategy.get_name())
                            .await
                    }) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::error!("target_qty_diff error: {}", e);
                            continue;
                        }
                    };
                    // tracing::info!(
                    //     message = %format!("Detected pos_diffs: \n{}",
                    //         &target_pos_diffs
                    //             .iter()
                    //             .map(|pos_diff| format!(
                    //                 "    {}, {}, {} ({})",
                    //                 pos_diff.strategy,
                    //                 pos_diff.stock,
                    //                 pos_diff.primary_exchange,
                    //                 pos_diff.qty_diff
                    //             ))
                    //             .collect::<Vec<String>>()
                    //             .join(",\n")
                    //     )
                    // );
                    let mut fx_attachments = if !strategy.is_fx_strategy() {
                        tracing::info!("Strat is not FX Strat");
                        let mut funds = HashMap::new();
                        let mut funds_from_selling = HashMap::<HashContract, Vec<f64>>::new();
                        let mut insufficient_funds = HashMap::new();

                        let mut pos_to_open = Vec::new();
                        for pos_diff in target_pos_diffs.clone() {
                            if let TargetPositionsQtyDiff::Stock(TargetStockPositionsQtyDiff {
                                ref stock,
                                qty_diff,
                                ..
                            }) = pos_diff
                            {
                                if let Some(quote) = stock.strip_prefix("CASH:") {
                                    funds.insert(quote.to_string(), -qty_diff);
                                    continue;
                                }
                            }
                            let (current_qty, qty_diff) = match pos_diff {
                                TargetPositionsQtyDiff::Stock(ref v) => (v.current_qty, v.qty_diff),
                                TargetPositionsQtyDiff::Options(ref v) => {
                                    (v.current_qty, v.qty_diff)
                                }
                            };
                            if current_qty != 0.0 && qty_diff.signum() != current_qty.signum() {
                                let hash_contract = HashContract {
                                    contract: get_contract_from(
                                        &LocalContractTypes::TargetPosQtyDiff(pos_diff),
                                    ),
                                };
                                if !funds_from_selling.contains_key(&hash_contract) {
                                    funds_from_selling.insert(hash_contract.clone(), Vec::new());
                                };
                                let funds_from_selling_currency =
                                    funds_from_selling.get_mut(&hash_contract).unwrap();
                                funds_from_selling_currency.push(qty_diff.abs());
                            } else {
                                pos_to_open.push(pos_diff);
                            }
                        }

                        tracing::info!("Constructed hashmaps for FX strat");
                        {
                            let strong_consolidator = {
                                let upgraded_consolidator_opt = consolidator.upgrade();
                                if upgraded_consolidator_opt.is_none() {
                                    tracing::error!(
                                        "Consolidator reference dropped before orders for positional differences could be submitted!"
                                    );
                                    return;
                                }
                                upgraded_consolidator_opt.unwrap()
                            };
                            for pos_diff in pos_to_open {
                                let (qty_diff, currency) = match &pos_diff {
                                    TargetPositionsQtyDiff::Stock(v) => {
                                        (v.qty_diff, v.currency.clone())
                                    }
                                    TargetPositionsQtyDiff::Options(v) => {
                                        (v.qty_diff, v.currency.clone())
                                    }
                                };

                                let contract = get_contract_from(
                                    &LocalContractTypes::TargetPosQtyDiff(pos_diff),
                                );
                                let hash_contract = HashContract {
                                    contract: contract.clone(),
                                };

                                let symbol = contract.symbol.clone();
                                tracing::info!("Fetching price for {}", symbol);
                                let required_currency = qty_diff
                                        * strong_consolidator.get_current_price(
                                            contract,
                                            false,
                                            &[],
                                        ).unwrap_or_else(|_| {
                                            tracing::warn!("Could not get current price of contract in order_engine!");
                                            0.0
                                        });
                                tracing::info!("Fetched price for {}", symbol);

                                let available_funds = funds.get(&currency).unwrap_or(&0.0);
                                if available_funds >= &required_currency {
                                    funds.insert(
                                        currency.clone(),
                                        available_funds - required_currency,
                                    );
                                    continue;
                                }

                                insufficient_funds
                                    .insert(hash_contract, required_currency - available_funds);
                                funds.insert(currency.clone(), 0.0);
                            }
                        }

                        // Collect all buy contracts that are handled via FX attachment chains
                        tracing::info!("getting required attached FX");
                        // so we can skip them in the main placement loop.
                        OrderEngine::get_required_fx_attachments(
                            funds,
                            funds_from_selling,
                            insufficient_funds,
                            strategy.get_name(),
                        )
                    } else {
                        FxAttachments {
                            contracts_sold_to_fx_orders: HashMap::new(),
                            backed_up_orders: Vec::new(),
                        }
                    };

                    target_pos_diffs.into_iter().for_each(|pos_diff| {
                        let (stock, qty_diff, avg_price) = match &pos_diff {
                            TargetPositionsQtyDiff::Stock(v) => {
                                (v.stock.clone(), v.qty_diff, v.avg_price)
                            }
                            TargetPositionsQtyDiff::Options(v) => {
                                (v.stock.clone(), v.qty_diff, v.avg_price)
                            }
                        };
                        if stock.split(":").next() == Some("CASH") {
                            return;
                        }

                        let contract =
                            get_contract_from(&LocalContractTypes::TargetPosQtyDiff(pos_diff));
                        let hash_contract = HashContract {
                            contract: contract.clone(),
                        };

                        let pool = self.pool.clone();
                        let weak_client_cloned = weak_client.clone();
                        let strat = strategy.get_name();

                        // Determine attachments for this contract:
                        //   - If it's a sell with an FX chain:    attach the FX contract(s)
                        //   - If it's an FX contract with buys:   attach the buy contract(s)
                        //   - Otherwise:                           no attachment
                        let mut orders = VecDeque::new();
                        let mut order = if avg_price == 0.0 {
                            market_order(
                                if qty_diff > 0.0 {
                                    ibapi::orders::Action::Buy
                                } else {
                                    ibapi::orders::Action::Sell
                                },
                                qty_diff,
                            )
                        } else {
                            limit_order(
                                if qty_diff > 0.0 {
                                    ibapi::orders::Action::Buy
                                } else {
                                    ibapi::orders::Action::Sell
                                },
                                qty_diff,
                                avg_price,
                            )
                        };
                        order.order_ref = strat;
                        let order_ibkr = OrderIBKR::new(contract, order, -1);
                        orders.push_back(order_ibkr);

                        // Remove from Hashmap to take ownership
                        if let Some(mut fx_orders) = fx_attachments
                            .contracts_sold_to_fx_orders
                            .remove(&hash_contract)
                        {
                            for mut fx_order in fx_orders {
                                fx_order.references_parent_order = 0;
                                orders.push_back(fx_order);
                            }
                        }

                        self.on_new_qty_diff_for_strat(pool, weak_client_cloned, orders);
                    });

                    if !fx_attachments.backed_up_orders.is_empty() {
                        order_store
                            .store_orders(&strategy.get_name(), &fx_attachments.backed_up_orders);
                    }
                }
            }
            BarUpdateOutcome::NoAction => {}
        }
    }

    /// Provides the logic to handle open order
    /// - i.e. cancelling and placing orders efficiently
    /// - 1st order MUST be correct strategy (i.e. order_ref == strategy)
    fn on_new_qty_diff_for_strat(
        &self,
        pool: PgPool,
        weak_client: Weak<Client>,
        mut orders: VecDeque<OrderIBKR>,
    ) -> Result<(), String> {
        let order_ibkr_opt = orders.pop_front();
        if order_ibkr_opt.is_none() {
            return Err("Expected orders not to be empty!".to_string());
        }
        let order_ibkr = order_ibkr_opt.unwrap();
        let contract = order_ibkr.contract;
        let qty_diff = order_ibkr.order.total_quantity;
        let asset_type = AssetType::from_str(&contract.security_type);
        let mut order = order_ibkr.order;

        let open_orders_crud = OpenOrdersCRUD::from(&asset_type, pool);
        let open_orders = match self.tokio_handle.block_on(async {
            open_orders_crud
                .get_orders_for_strat(&order.order_ref)
                .await
        }) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("Failed to get orders for strategy: {e:?}");
                return Err("Failed to get orders for strategy".to_string());
            }
        };

        let tot_qty_dir = open_orders
            .iter()
            .map(|open_order| match open_order {
                OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys { quantity, .. }) => {
                    quantity.signum()
                }
                OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys { quantity, .. }) => {
                    quantity.signum()
                }
            })
            .sum::<f64>()
            .abs() as u64;
        if tot_qty_dir != open_orders.len() as u64 {
            tracing::error!(
                message=%format!(
                    "Error: Open orders placed for {} for stock {} are not all in the same direction!",
                    &order.order_ref, &contract.symbol
                )
            );
        };

        let (curr_open_orders_filled, curr_open_orders_quantity): (f64, f64) = (
            open_orders
                .iter()
                .map(|open_order| match open_order {
                    OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys { filled, .. }) => filled,
                    OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys { filled, .. }) => filled,
                })
                .sum(),
            open_orders
                .iter()
                .map(|open_order| match open_order {
                    OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys { quantity, .. }) => quantity,
                    OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys { quantity, .. }) => {
                        quantity
                    }
                })
                .sum(),
        );

        // return 1 entry
        let current_qty_diff = (curr_open_orders_quantity - curr_open_orders_filled)
            * (curr_open_orders_quantity.signum());

        // Alr correct
        if qty_diff == 0.0 {
            // Cancel Open Orders
            open_orders.iter().for_each(|open_order| {
                let (order_perm_id, order_id) = match open_order {
                    OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                        order_perm_id,
                        order_id,
                        ..
                    }) => (order_perm_id, order_id),
                    OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                        order_perm_id,
                        order_id,
                        ..
                    }) => (order_perm_id, order_id),
                };

                // Cancel on IBKR side
                let cloned_weak_client = weak_client.clone();
                let owned_order_id = *order_id;
                std::thread::spawn(move || {
                    let client_opt = cloned_weak_client.upgrade();
                    if client_opt.is_none() {
                        tracing::warn!("client died while cancelling order!");
                        return;
                    }
                    let client = client_opt.unwrap();
                    if let Err(e) = client.cancel_order(owned_order_id, "") {
                        tracing::warn!("Could not cancel order: {e:?}");
                    };
                });

                // Cancel on Local Side (i.e. DB)
                let open_orders_crud_cloned = open_orders_crud.clone();
                let open_order_pk =
                    OpenOrdersPrimaryKeys::new(&asset_type, *order_perm_id, *order_id);
                self.tokio_handle.spawn(async move {
                    if let Err(e) = open_orders_crud_cloned.delete(&open_order_pk).await {
                        tracing::error!("Error trying to delete OpenOptionOrder entry: {e:?}")
                    };
                });
            });
            return Ok(());
        }

        // Cancel the order if qty_diff is in wrong direction / open order qty too high
        if current_qty_diff.signum() != qty_diff.signum()
            || (current_qty_diff.signum() == qty_diff.signum()
                && current_qty_diff.abs() > qty_diff.abs())
        {
            // Cancel all open orders first
            open_orders.iter().for_each(|open_order| {
                let (order_perm_id, order_id) = match open_order {
                    OpenOrdersFullKeys::Stock(OpenStockOrdersFullKeys {
                        order_perm_id,
                        order_id,
                        ..
                    }) => (order_perm_id, order_id),
                    OpenOrdersFullKeys::Options(OpenOptionOrdersFullKeys {
                        order_perm_id,
                        order_id,
                        ..
                    }) => (order_perm_id, order_id),
                };
                let weak_client_cloned = weak_client.clone();
                let owned_order_id = *order_id;
                std::thread::spawn(move || {
                    let client_opt = weak_client_cloned.upgrade();
                    if client_opt.is_none() {
                        tracing::warn!("client died while cancelling multiple orders!");
                        return;
                    }
                    let client = client_opt.unwrap();
                    if let Err(e) = client.cancel_order(owned_order_id, "") {
                        tracing::warn!("Could not cancel order: {e:?}");
                    };
                });
                let open_order_pk =
                    OpenOrdersPrimaryKeys::new(&asset_type, *order_perm_id, *order_id);
                let open_orders_crud_cloned = open_orders_crud.clone();
                self.tokio_handle.spawn(async move {
                    if let Err(e) = open_orders_crud_cloned.delete(&open_order_pk).await {
                        tracing::error!("Error trying to delete entry in OpenOrders: {e:?}")
                    }
                });
            });

            let weak_client_cloned = weak_client;
            orders.push_front(OrderIBKR::new(contract, order, -1));
            std::thread::spawn(move || Self::place_orders(&weak_client_cloned, orders.into_iter()));

            return Ok(());
        }

        // If it's here: Order is in same dirction of qty_diff
        if current_qty_diff.abs() < qty_diff.abs() {
            let weak_client_cloned = weak_client.clone();
            order.total_quantity = (qty_diff - current_qty_diff).abs();
            orders.push_front(OrderIBKR::new(contract, order, -1));
            std::thread::spawn(move || Self::place_orders(&weak_client_cloned, orders.into_iter()));
        }

        Ok(())
    }
}
