use crate::models;
use axum::Json;
use futures::future::join_all;
use rust_decimal::{dec, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use chrono::{DateTime, Utc};
use std::f64;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PositionInfo {
    pub avg_price: f64,
    pub quantity: f64,
    pub last_pnl: f64,
    pub contract_type: String,                 // "stock" or "option"
    pub option_details: Option<OptionDetails>, // Only for options
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OptionDetails {
    pub expiry: String,
    pub strike: f64,
    pub multiplier: String,
    pub option_type: String, // "Call" or "Put"
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PortfolioMetrics {
    pub cagr: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub calmar_ratio: f64,
    pub profit_factor: f64,
    pub win_rate: f64,
    pub avg_trade_return: f64,
    pub positions: HashMap<String, PositionInfo>,
}

// pub fn compute_portfolio_metrics(
//     portfolio_values: &Vec<(DateTime<Utc>, f64)>,
//     transactions: &Vec<crate::models::StockTransactionsFullKeys>,
// ) -> PortfolioMetrics {
//     // ===== Portfolio Value Metrics =====
//     let first = portfolio_values.first().unwrap();
//     let last = portfolio_values.last().unwrap();
//
//     let duration = last.0.signed_duration_since(first.0);
//     let years = duration.num_seconds() as f64 / (365.25 * 24.0 * 3600.0);
//
//     let cagr = (last.1 / first.1).powf(1.0 / years) - 1.0;
//
//     // Log returns for Sharpe Ratio
//     let mut returns = vec![];
//     for w in portfolio_values.windows(2) {
//         let r = (w[1].1 / w[0].1).ln();
//         if r.is_nan(){
//             returns.push(0.0);
//             continue;
//         }
//         returns.push(r);
//     }
//
//     let mean_return = returns.iter().copied().sum::<f64>() / returns.len() as f64;
//     let std_return = (returns
//         .iter()
//         .map(|r| (r - mean_return).powi(2))
//         .sum::<f64>()
//         / returns.len() as f64)
//         .sqrt();
//     let sharpe_ratio = if std_return != 0.0 {
//         mean_return / std_return * ((252.0 * 24.0 * 12.0) as f64).sqrt() // annualizing 5min returns (12 per hour * 24 * 365.25)
//     } else {
//         0.0
//     };
//
//     // Max Drawdown
//     let mut peak = first.1;
//     let mut max_drawdown = 0.0;
//     for &(_, value) in portfolio_values.iter() {
//         if value > peak {
//             peak = value;
//         }
//         let drawdown = (peak - value) / peak;
//         if drawdown > max_drawdown {
//             max_drawdown = drawdown;
//         }
//     }
//
//     let calmar_ratio = if max_drawdown != 0.0 {
//         cagr / max_drawdown
//     } else {
//         0.0
//     };
//
//     // ===== Transaction Metrics =====
//
//     // Pair buy and sell trades
//     let mut open_positions = HashMap::<String, (f64, f64)>::new();
//     let mut open_positions_last_pnl = HashMap::<String, f64>::new();
//     let mut profits: Vec<f64> = vec![];
//
//     for txn in transactions {
//         let (price, qty) = (txn.price_transacted, txn.quantity);
//         if qty > 0.0 {
//             // Buy
//             let curr_position = open_positions
//                 .get(&txn.stock)
//                 .unwrap_or_else(|| &(0.0 as f64, 0.0 as f64));
//             let new_avg_price =
//                 ((curr_position.0 * curr_position.1) + (price * qty)) / (curr_position.1 + qty);
//             open_positions.insert(txn.stock.clone(), (new_avg_price, curr_position.1 + qty));
//         } else {
//             // Sell
//             if let Some(curr_position) = open_positions.get(&txn.stock) {
//                 let profit = qty * (price - curr_position.0);
//                 profits.push(profit);
//
//                 open_positions.insert(txn.stock.clone(), (curr_position.0, curr_position.1 - qty));
//                 open_positions_last_pnl.insert(txn.stock.clone(), profit);
//             } else {
//                 println!("ERROR OCCURRED!");
//             }
//         }
//     }
//     let mut positions_latest_pnl = HashMap::<String, (f64, f64, f64)>::new();
//     for (stock, position) in open_positions.iter() {
//         if position.1 != 0.0 {
//             positions_latest_pnl.insert(
//                 stock.clone(),
//                 (
//                     position.0,
//                     position.1,
//                     *open_positions_last_pnl.get(stock).unwrap_or_else(|| &0.0),
//                 ),
//             );
//         }
//     }
//
//     let gross_profit: f64 = profits.iter().filter(|&&p| p > 0.0).sum();
//     let gross_loss: f64 = profits.iter().filter(|&&p| p < 0.0).map(|p| p.abs()).sum();
//     let profit_factor = if gross_loss != 0.0 {
//         gross_profit / gross_loss
//     } else {
//         f64::INFINITY
//     };
//
//     let wins = profits.iter().filter(|&&p| p > 0.0).count();
//     let total = profits.len();
//     let win_rate = if total > 0 {
//         wins as f64 / total as f64
//     } else {
//         0.0
//     };
//
//     let avg_trade_return = if total > 0 {
//         profits.iter().sum::<f64>() / total as f64
//     } else {
//         0.0
//     };
//
//     PortfolioMetrics {
//         cagr,
//         sharpe_ratio,
//         max_drawdown,
//         calmar_ratio,
//         profit_factor,
//         win_rate,
//         avg_trade_return,
//         positions: positions_latest_pnl,
//     }
// }

pub fn compute_portfolio_metrics(
    portfolio_values: &Vec<(DateTime<Utc>, f64)>,
    stock_transactions: &Vec<crate::models::StockTransactions>,
    option_transactions: &Vec<crate::models::OptionTransactions>,
) -> PortfolioMetrics {
    // ===== Portfolio Value Metrics =====
    if portfolio_values.is_empty() {
        return PortfolioMetrics {
            cagr: 0.0,
            sharpe_ratio: 0.0,
            max_drawdown: 0.0,
            calmar_ratio: 0.0,
            profit_factor: 0.0,
            win_rate: 0.0,
            avg_trade_return: 0.0,
            positions: HashMap::new(),
        };
    }

    let first = portfolio_values.first().unwrap();
    let last = portfolio_values.last().unwrap();

    let duration = last.0.signed_duration_since(first.0);
    let years = duration.num_seconds() as f64 / (365.25 * 24.0 * 3600.0);

    let cagr = if years > 0.0 && first.1 > 0.0 {
        (last.1 / first.1).powf(1.0 / years) - 1.0
    } else {
        0.0
    };

    // Log returns for Sharpe Ratio
    let mut returns = vec![];
    for w in portfolio_values.windows(2) {
        if w[0].1 > 0.0 {
            let r = (w[1].1 / w[0].1).ln();
            if !r.is_nan() {
                returns.push(r);
            } else {
                returns.push(0.0);
            }
        } else {
            returns.push(0.0);
        }
    }

    let mean_return = if !returns.is_empty() {
        returns.iter().copied().sum::<f64>() / returns.len() as f64
    } else {
        0.0
    };

    let std_return = if !returns.is_empty() {
        (returns
            .iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>()
            / returns.len() as f64)
            .sqrt()
    } else {
        0.0
    };

    let sharpe_ratio = if std_return != 0.0 {
        mean_return / std_return * ((252.0 * 24.0 * 12.0) as f64).sqrt() // annualizing 5min returns (12 per hour * 24 * 365.25)
    } else {
        0.0
    };

    // Max Drawdown
    let mut peak = first.1;
    let mut max_drawdown = 0.0;
    for &(_, value) in portfolio_values.iter() {
        if value > peak {
            peak = value;
        }
        let drawdown = if peak > 0.0 {
            (peak - value) / peak
        } else {
            0.0
        };
        if drawdown > max_drawdown {
            max_drawdown = drawdown;
        }
    }

    let calmar_ratio = if max_drawdown != 0.0 {
        cagr / max_drawdown
    } else {
        0.0
    };

    // ===== Transaction Metrics =====
    let mut combined_profits: Vec<f64> = vec![];

    // Process stock transactions
    let mut open_stock_positions = HashMap::<String, (f64, f64)>::new(); // (avg_price, quantity)
    let mut stock_last_pnl = HashMap::<String, f64>::new();

    for txn in stock_transactions {
        let price = txn.price.unwrap_or(0.0);
        let qty = txn.quantity.unwrap_or(0.0);

        if qty > 0.0 {
            // Buy
            let curr_position = open_stock_positions
                .get(&txn.stock.clone().unwrap())
                .unwrap_or(&(0.0, 0.0));
            let new_avg_price = if curr_position.1 + qty > 0.0 {
                ((curr_position.0 * curr_position.1) + (price * qty)) / (curr_position.1 + qty)
            } else {
                0.0
            };
            open_stock_positions.insert(
                txn.stock.clone().unwrap().clone(),
                (new_avg_price, curr_position.1 + qty),
            );
        } else if qty < 0.0 {
            // Sell
            if let Some(curr_position) = open_stock_positions.get(&txn.stock.clone().unwrap()) {
                let profit = -qty * (price - curr_position.0);
                combined_profits.push(profit);
                stock_last_pnl.insert(txn.stock.clone().unwrap(), profit);

                open_stock_positions.insert(
                    txn.stock.clone().unwrap(),
                    (curr_position.0, curr_position.1 + qty),
                );
            }
        }
    }

    // Process option transactions
    let mut open_option_positions =
        HashMap::<String, (f64, f64, String, String, f64, String)>::new(); // (avg_price, quantity, expiry, option_type, strike, multiplier)
    let mut option_last_pnl = HashMap::<String, f64>::new();

    for txn in option_transactions {
        let price = txn.price.unwrap_or(0.0);
        let qty = txn.quantity.unwrap_or(0.0);
        let option_key = format!(
            "{}_{}_{}_{}_{}",
            txn.stock.clone().unwrap(),
            txn.expiry.clone().unwrap(),
            txn.strike.clone().unwrap(),
            txn.option_type.clone().unwrap().to_string(),
            txn.multiplier.clone().unwrap()
        );

        if qty > 0.0 {
            // Buy
            let fallback_value = (
                0.0,
                0.0,
                txn.expiry.clone().unwrap(),
                txn.option_type.clone().unwrap().to_string(),
                txn.strike.clone().unwrap(),
                txn.multiplier.clone().unwrap(),
            );
            let curr_position = open_option_positions
                .get(&option_key)
                .unwrap_or(&fallback_value);
            let new_avg_price = if curr_position.1 + qty > 0.0 {
                ((curr_position.0 * curr_position.1) + (price * qty)) / (curr_position.1 + qty)
            } else {
                0.0
            };
            open_option_positions.insert(
                option_key.clone(),
                (
                    new_avg_price,
                    curr_position.1 + qty,
                    txn.expiry.clone().unwrap(),
                    txn.option_type.clone().unwrap().to_string(),
                    txn.strike.unwrap(),
                    txn.multiplier.clone().unwrap(),
                ),
            );
        } else if qty < 0.0 {
            // Sell
            if let Some(curr_position) = open_option_positions.get(&option_key) {
                let multiplier: f64 = txn
                    .multiplier
                    .clone()
                    .unwrap()
                    .parse()
                    .expect("Expected multiplier to be easily convertible to f64");
                let profit = -qty * (price - curr_position.0) * multiplier;
                combined_profits.push(profit);
                option_last_pnl.insert(option_key.clone(), profit);

                open_option_positions.insert(
                    option_key.clone(),
                    (
                        curr_position.0,
                        curr_position.1 + qty,
                        curr_position.2.clone(),
                        curr_position.3.clone(),
                        curr_position.4,
                        curr_position.5.clone(),
                    ),
                );
            }
        }
    }

    // Combine positions into final result format
    let mut positions_latest_pnl = HashMap::<String, PositionInfo>::new();

    // Add stock positions
    for (stock, position) in open_stock_positions.iter() {
        if position.1 != 0.0 {
            positions_latest_pnl.insert(
                stock.clone(),
                PositionInfo {
                    avg_price: position.0,
                    quantity: position.1,
                    last_pnl: *stock_last_pnl.get(stock).unwrap_or(&0.0),
                    contract_type: "stock".to_string(),
                    option_details: None,
                },
            );
        }
    }

    // Add option positions
    for (option_key, position) in open_option_positions.iter() {
        if position.1 != 0.0 {
            let parts: Vec<&str> = option_key.split('_').collect();
            if parts.len() >= 5 {
                // let stock = parts[0].to_string();
                positions_latest_pnl.insert(
                    option_key.clone(),
                    PositionInfo {
                        avg_price: position.0,
                        quantity: position.1,
                        last_pnl: *option_last_pnl.get(option_key).unwrap_or(&0.0),
                        contract_type: "option".to_string(),
                        option_details: Some(OptionDetails {
                            expiry: position.2.clone(),
                            strike: position.4,
                            multiplier: position.5.clone(),
                            option_type: position.3.clone(),
                        }),
                    },
                );
            }
        }
    }

    // Calculate profit metrics
    combined_profits.iter().for_each(|&p| print!("{}", p));
    let gross_profit: f64 = combined_profits.iter().filter(|&&p| p > 0.0).sum();
    let gross_loss: f64 = combined_profits
        .iter()
        .filter(|&&p| p < 0.0)
        .map(|p| p.abs())
        .sum();
    let profit_factor = if gross_loss != 0.0 {
        gross_profit / gross_loss
    } else if combined_profits.len() == 0 {
        -1.0
    } else {
        f64::INFINITY
    };

    let wins = combined_profits.iter().filter(|&&p| p > 0.0).count();
    let total = combined_profits.len();
    let win_rate = if total > 0 {
        wins as f64 / total as f64
    } else {
        0.0
    };

    let avg_trade_return = if total > 0 {
        combined_profits.iter().sum::<f64>() / total as f64
    } else {
        0.0
    };

    PortfolioMetrics {
        cagr,
        sharpe_ratio,
        max_drawdown,
        calmar_ratio,
        profit_factor,
        win_rate,
        avg_trade_return,
        positions: positions_latest_pnl,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Strategy {
    pub strategy: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioValueStrategy {
    pub strategy: String,
    pub status: models::Status,
    pub portfolio: Vec<(chrono::DateTime<chrono::Utc>, f64)>,
    pub metrics: PortfolioMetrics,
}

// pub async fn compute_portfolio_value_for_strategy(
//     state: crate::AppState,
//     strategy: Strategy,
// ) -> Result<Json<PortfolioValueStrategy>, String> {
//     let sql_strategy = format!(
//         "SELECT * FROM trading.strategy WHERE strategy = '{}'",
//         strategy.strategy
//     );
//     let sql_transactions = format!(
//         "SELECT *, time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Eastern' AS time_est FROM trading.transactions WHERE strategy = '{}' ORDER BY time ASC",
//         strategy.strategy
//     );
//     let sql_historical_data = format!(
//         "SELECT *, time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Eastern' AS time_est FROM market_data.historical_data WHERE stock IN (SELECT DISTINCT stock FROM trading.transactions WHERE strategy = '{}') ORDER BY time ASC",
//         strategy.strategy
//     );
//
//     let query_strategy = sqlx::query_as::<_, crate::models::StrategyFullKeys>(&sql_strategy);
//     let strategy = query_strategy
//         .fetch_one(&state.db)
//         .await
//         .map_err(|err| format!("Failed to find strategy in Database: {}", err))?;
//     let query_transactions =
//         sqlx::query_as::<_, crate::models::StockTransactionsFullKeys>(&sql_transactions);
//     let transactions = query_transactions
//         .fetch_all(&state.db)
//         .await
//         .map_err(|err| {
//             format!(
//                 "Failed to find transactions for strategy in Database: {}",
//                 err
//             )
//         })?;
//     let query_historical_data =
//         sqlx::query_as::<_, crate::models::HistoricalDataFullKeys>(&sql_historical_data);
//     let historical_data = query_historical_data
//         .fetch_all(&state.db)
//         .await
//         .map_err(|err| {
//             format!(
//                 "Failed to find historical_data for strategy in Database: {}",
//                 err
//             )
//         })?;
//
//     let mut portfolio_value: Vec<(chrono::DateTime<chrono::Utc>, f64)> = Vec::new();
//
//     fn update_until_next_transaction(
//         curr_transaction: &crate::models::StockTransactionsFullKeys,
//         next_transaction: &crate::models::StockTransactionsFullKeys,
//         historical_data: &Vec<crate::models::HistoricalDataFullKeys>,
//         capital: &f64,
//         portfolio_value: &mut Vec<(chrono::DateTime<chrono::Utc>, f64)>,
//         position: &f64,
//         price_idx: &mut usize,
//     ) {
//         while true {
//             if let Some(historical_data_specific) = historical_data.get(*price_idx) {
//                 if curr_transaction.time >= historical_data_specific.time {
//                     *price_idx += 1;
//                     continue;
//                 }
//             }
//             break;
//         }
//         if *price_idx > historical_data.len() - 1 {
//             return;
//         }
//
//         while true {
//             if let Some(historical_data_specific) = historical_data.get(*price_idx) {
//                 if historical_data_specific.time >= next_transaction.time {
//                     break;
//                 }
//                 let avg_price = (historical_data_specific.open
//                     + historical_data_specific.high
//                     + historical_data_specific.low
//                     + historical_data_specific.close)
//                     / 4.0;
//                 portfolio_value.push((
//                     historical_data_specific.time,
//                     capital + position * avg_price,
//                 ));
//                 *price_idx += 1;
//                 continue;
//             }
//             break;
//         }
//     }
//
//     // Only works for long only positions currently
//     if let Some(mut prev_transaction) = transactions.get(0) {
//         let mut capital = strategy.initial_capital;
//         // let mut stock_value = 0.0;
//         let mut position = 0.0;
//         let mut price_idx: usize = 0;
//         for curr_transaction in transactions.iter() {
//             if prev_transaction.time == curr_transaction.time
//                 && prev_transaction.stock == curr_transaction.stock
//                 && prev_transaction.strategy == curr_transaction.strategy
//             {
//                 prev_transaction = &curr_transaction;
//                 continue;
//             }
//             if prev_transaction.quantity > 0.0 {
//                 capital -= prev_transaction.quantity * prev_transaction.price_transacted
//                     + prev_transaction.fees;
//                 capital = capital.max(0.0);
//                 // stock_value += prev_transaction.quantity * prev_transaction.price_transacted;
//                 position += prev_transaction.quantity;
//             } else if prev_transaction.quantity < 0.0 {
//                 capital += -prev_transaction.quantity * prev_transaction.price_transacted
//                     - prev_transaction.fees;
//                 // stock_value -= -prev_transaction.quantity * prev_transaction.price_transacted;
//                 position -= -prev_transaction.quantity;
//             }
//             portfolio_value.push((
//                 prev_transaction.time,
//                 capital + position * prev_transaction.price_transacted,
//             ));
//             update_until_next_transaction(
//                 &prev_transaction,
//                 &curr_transaction,
//                 &historical_data,
//                 &capital,
//                 &mut portfolio_value,
//                 &position,
//                 &mut price_idx,
//             );
//             prev_transaction = &curr_transaction;
//         }
//
//         if prev_transaction.quantity > 0.0 {
//             capital -= prev_transaction.quantity * prev_transaction.price_transacted
//                 - prev_transaction.fees;
//             capital = capital.max(0.0);
//             // stock_value += prev_transaction.quantity * prev_transaction.price_transacted;
//             position += prev_transaction.quantity;
//         } else if prev_transaction.quantity < 0.0 {
//             capital += -prev_transaction.quantity * prev_transaction.price_transacted
//                 - prev_transaction.fees;
//             // stock_value -= -prev_transaction.quantity * prev_transaction.price_transacted;
//             position -= -prev_transaction.quantity;
//         }
//         portfolio_value.push((
//             prev_transaction.time,
//             capital + position * prev_transaction.price_transacted,
//         ));
//     }
//
//     let metrics = compute_portfolio_metrics(&portfolio_value, &transactions);
//
//     Ok(Json(PortfolioValueStrategy {
//         strategy: strategy.strategy,
//         portfolio: portfolio_value,
//         metrics,
//     }))
// }

pub async fn compute_portfolio_value_for_strategy(
    state: crate::AppState,
    strategy: Strategy,
) -> Result<Json<PortfolioValueStrategy>, String> {
    // Get strategy information
    let sql_strategy = format!(
        "SELECT * FROM trading.strategy WHERE strategy = '{}'",
        strategy.strategy
    );

    // Get stock transactions
    let sql_stock_transactions = format!(
        "SELECT *, time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Eastern' AS time_est FROM trading.stock_transactions WHERE strategy = '{}' ORDER BY time ASC",
        strategy.strategy
    );

    // Get option transactions
    let sql_option_transactions = format!(
        "SELECT *, time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Eastern' AS time_est FROM trading.option_transactions WHERE strategy = '{}' ORDER BY time ASC",
        strategy.strategy
    );

    // Get historical stock data
    let sql_historical_stock_data = format!(
        "SELECT *, time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Eastern' AS time_est FROM market_data.historical_data WHERE stock IN (SELECT DISTINCT stock FROM trading.stock_transactions WHERE strategy = '{}') ORDER BY time ASC",
        strategy.strategy
    );

    // Get historical options data
    let sql_historical_options_data = format!(
        "SELECT *, time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Eastern' AS time_est FROM phantom_trading.historical_options_data WHERE stock IN (SELECT DISTINCT stock FROM trading.option_transactions WHERE strategy = '{}') ORDER BY time ASC",
        strategy.strategy
    );

    // Execute queries
    let query_strategy = sqlx::query_as::<_, crate::models::Strategy>(&sql_strategy);
    let strategy_info = query_strategy
        .fetch_one(&state.db)
        .await
        .map_err(|err| format!("Failed to find strategy in Database: {}", err))?;

    let query_stock_transactions =
        sqlx::query_as::<_, crate::models::StockTransactions>(&sql_stock_transactions);
    let stock_transactions = query_stock_transactions
        .fetch_all(&state.db)
        .await
        .map_err(|err| {
            format!(
                "Failed to find stock transactions for strategy in Database: {}",
                err
            )
        })?;

    let query_option_transactions =
        sqlx::query_as::<_, crate::models::OptionTransactions>(&sql_option_transactions);
    let option_transactions = query_option_transactions
        .fetch_all(&state.db)
        .await
        .map_err(|err| {
            format!(
                "Failed to find option transactions for strategy in Database: {}",
                err
            )
        })?;

    let query_historical_stock_data =
        sqlx::query_as::<_, crate::models::HistoricalData>(&sql_historical_stock_data);
    let historical_stock_data = query_historical_stock_data
        .fetch_all(&state.db)
        .await
        .map_err(|err| {
            format!(
                "Failed to find historical stock data for strategy in Database: {}",
                err
            )
        })?;

    let query_historical_options_data =
        sqlx::query_as::<_, crate::models::HistoricalOptionsData>(&sql_historical_options_data);
    let historical_options_data = query_historical_options_data
        .fetch_all(&state.db)
        .await
        .map_err(|err| {
            format!(
                "Failed to find historical options data for strategy in Database: {}",
                err
            )
        })?;

    // Create a combined timeline of all transactions (both stocks and options)
    let mut all_transactions: Vec<(
        DateTime<Utc>,
        String,
        f64,
        f64,
        f64,
        bool,
        Option<(String, f64, String, String)>,
    )> = Vec::new();

    // Add stock transactions to the timeline
    for txn in &stock_transactions {
        all_transactions.push((
            txn.time.clone().unwrap(),
            txn.stock.clone().unwrap(),
            txn.price.clone().unwrap_or(0.0),
            txn.quantity.clone().unwrap_or(0.0),
            txn.fees.clone().unwrap_or(dec!(0.0)).to_f64().unwrap(),
            true, // is_stock
            None, // no option details
        ));
    }

    // Add option transactions to the timeline
    for txn in &option_transactions {
        all_transactions.push((
            txn.time.clone().unwrap(),
            txn.stock.clone().unwrap(),
            txn.price.clone().unwrap_or(0.0),
            txn.quantity.clone().unwrap_or(0.0),
            txn.fees.clone().unwrap_or(dec!(0.0)).to_f64().unwrap(),
            false, // is_option
            Some((
                txn.expiry.clone().unwrap(),
                txn.strike.unwrap(),
                txn.multiplier.clone().unwrap(),
                txn.option_type.as_ref().unwrap().to_string(),
            )),
        ));
    }

    // Sort all transactions by time
    all_transactions.sort_by(|a, b| a.0.cmp(&b.0));

    // Calculate portfolio value over time
    let mut portfolio_value: Vec<(chrono::DateTime<chrono::Utc>, f64)> = Vec::new();

    // Initialize portfolio state
    let initial_capital = strategy_info.initial_capital.unwrap_or(0.0);
    let mut capital = initial_capital;
    let mut stock_positions: HashMap<String, (f64, f64)> = HashMap::new(); // (avg_price, quantity)
    let mut option_positions: HashMap<String, (f64, f64, f64)> = HashMap::new(); // (avg_price, quantity, multiplier)

    for (time, symbol, price, quantity, fees, is_stock, option_details) in all_transactions {
        // Update positions and capital
        if is_stock {
            // Process stock transaction
            if quantity > 0.0 {
                // Buy stock
                capital -= quantity * price + fees;
                capital = capital.max(0.0);

                // Update position
                let curr_position = stock_positions.get(&symbol).unwrap_or(&(0.0, 0.0));
                let new_avg_price = if curr_position.1 + quantity > 0.0 {
                    ((curr_position.0 * curr_position.1) + (price * quantity))
                        / (curr_position.1 + quantity)
                } else {
                    0.0
                };
                stock_positions.insert(symbol.clone(), (new_avg_price, curr_position.1 + quantity));
            } else if quantity < 0.0 {
                // Sell stock
                capital += -quantity * price - fees;

                // Update position
                if let Some(curr_position) = stock_positions.get(&symbol) {
                    stock_positions.insert(
                        symbol.clone(),
                        (curr_position.0, curr_position.1 + quantity),
                    );
                }
            }
        } else {
            // Process option transaction
            if let Some((expiry, strike, multiplier_str, option_type)) = option_details {
                let option_key = format!(
                    "{}_{}_{}_{}_{}",
                    symbol, expiry, strike, option_type, multiplier_str
                );
                let multiplier = multiplier_str
                    .parse()
                    .expect("Expected multiplier to be parsable");

                if quantity > 0.0 {
                    // Buy option
                    capital -= quantity * price * multiplier + fees;
                    capital = capital.max(0.0);

                    // Update position
                    let fallback_value = (0.0, 0.0, multiplier);
                    let curr_position =
                        option_positions.get(&option_key).unwrap_or(&fallback_value);
                    let new_avg_price = if curr_position.1 + quantity > 0.0 {
                        ((curr_position.0 * curr_position.1) + (price * quantity))
                            / (curr_position.1 + quantity)
                    } else {
                        0.0
                    };
                    option_positions.insert(
                        option_key.clone(),
                        (new_avg_price, curr_position.1 + quantity, multiplier),
                    );
                } else if quantity < 0.0 {
                    // Sell option
                    capital += -quantity * price * multiplier - fees;

                    // Update position
                    if let Some(curr_position) = option_positions.get(&option_key) {
                        option_positions.insert(
                            option_key.clone(),
                            (curr_position.0, curr_position.1 + quantity, curr_position.2),
                        );
                    }
                }
            }
        }

        // Calculate current portfolio value
        let mut stock_value = 0.0;
        for (symbol, (avg_price, quantity)) in &stock_positions {
            if *quantity > 0.0 {
                // Use latest price or average price if no data available
                let latest_price = historical_stock_data
                    .iter()
                    .filter(|data| &data.stock == symbol && data.time <= time)
                    .last()
                    .map(|data| {
                        (data.open.unwrap_or(0.0)
                            + data.high.unwrap_or(0.0)
                            + data.low.unwrap_or(0.0)
                            + data.close.unwrap_or(0.0))
                            / 4.0
                    })
                    .unwrap_or(*avg_price);

                stock_value += quantity * latest_price;
            }
        }

        let mut option_value = 0.0;
        for (option_key, (avg_price, quantity, multiplier)) in &option_positions {
            if *quantity > 0.0 {
                let parts: Vec<&str> = option_key.split('_').collect();
                if parts.len() >= 5 {
                    let symbol = parts[0];
                    let expiry = parts[1];
                    let strike = parts[2].parse::<f64>().unwrap_or(0.0);
                    let option_type = parts[3];

                    // Find latest option price
                    let latest_price = historical_options_data
                        .iter()
                        .filter(|data| {
                            &data.stock == symbol
                                && &data.expiry == expiry
                                && data.strike == strike
                                && data.option_type.to_string() == option_type
                                && data.time <= time
                        })
                        .last()
                        .map(|data| data.close.unwrap_or(*avg_price))
                        .unwrap_or(*avg_price);

                    option_value += quantity * latest_price * multiplier;
                }
            }
        }

        // Add entry to portfolio value timeline
        let total_value = capital + stock_value + option_value;
        portfolio_value.push((time, total_value));
    }

    // If there are no transactions, just return the initial capital
    if portfolio_value.is_empty() && initial_capital > 0.0 {
        portfolio_value.push((chrono::offset::Utc::now(), initial_capital));
    }

    // Calculate portfolio metrics
    let metrics =
        compute_portfolio_metrics(&portfolio_value, &stock_transactions, &option_transactions);

    Ok(Json(PortfolioValueStrategy {
        strategy: strategy.strategy,
        status: strategy_info.status.unwrap(),
        portfolio: portfolio_value,
        metrics,
    }))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioEntryWithStrategy {
    pub strategy: String,
    pub value: (chrono::DateTime<chrono::Utc>, f64),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioEntryReturn {
    pub value: (chrono::DateTime<chrono::Utc>, f64),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioValue {
    pub strategies: Vec<PortfolioValueStrategy>,
    pub portfolio: Vec<(chrono::DateTime<chrono::Utc>, f64)>,
}

// pub async fn compute_overall_portfolio_value(
//     state: crate::AppState,
// ) -> Result<Json<PortfolioValue>, String> {
//     let sql_strategy = "SELECT DISTINCT strategy FROM trading.strategy";
//     let query_strategy = sqlx::query_as::<_, crate::models::StrategyPrimaryKeys>(&sql_strategy);
//     let strategies = query_strategy
//         .fetch_all(&state.db)
//         .await
//         .map_err(|err| format!("Failed to find strategies in Database: {}", err))?;
//
//     let tasks = strategies.iter().map(|strat| {
//         let state = state.clone();
//         let strategy_name = strat.strategy.clone();
//
//         // if let Some(portfolio_value_for_strat) =
//         async move {
//             match compute_portfolio_value_for_strategy(
//                 state,
//                 Strategy {
//                     strategy: strategy_name.clone(),
//                 },
//             )
//             .await
//             {
//                 Ok(portfolio_value_for_strat) => portfolio_value_for_strat,
//                 Err(_) => Json(PortfolioValueStrategy {
//                     strategy: strategy_name.clone(),
//                     portfolio: vec![],
//                     metrics: PortfolioMetrics {
//                         cagr: 0.0,
//                         sharpe_ratio: 0.0,
//                         max_drawdown: 0.0,
//                         calmar_ratio: 0.0,
//                         profit_factor: 0.0,
//                         win_rate: 0.0,
//                         avg_trade_return: 0.0,
//                         positions: HashMap::<String, (f64, f64, f64)>::new(),
//                     },
//                 }),
//             }
//         }
//     });
//     let portfolio_value_over_time_unmapped: Vec<Json<PortfolioValueStrategy>> =
//         join_all(tasks).await;
//
//     let mut portfolio_value_over_time: Vec<PortfolioEntryWithStrategy> =
//         portfolio_value_over_time_unmapped
//             .iter()
//             .flat_map(|portfolio_val| {
//                 if portfolio_val.portfolio.len() == 0 {
//                     return Vec::<PortfolioEntryWithStrategy>::new();
//                 }
//                 portfolio_val
//                     .portfolio
//                     .iter()
//                     .map(|val| PortfolioEntryWithStrategy {
//                         strategy: portfolio_val.strategy.clone(),
//                         value: val.clone(),
//                     })
//                     .collect()
//             })
//             .collect();
//
//     portfolio_value_over_time.sort_by(|a, b| a.value.0.cmp(&b.value.0));
//
//     let mut portfolio_value_overall = Vec::<PortfolioEntryReturn>::new();
//     let mut strategies = HashMap::<String, f64>::new();
//     for portfolio_value_at_t in portfolio_value_over_time {
//         let change = portfolio_value_at_t.value.1
//             - strategies
//                 .get(&portfolio_value_at_t.strategy)
//                 .unwrap_or(&0.0);
//         portfolio_value_overall.push(PortfolioEntryReturn {
//             value: (
//                 portfolio_value_at_t.value.0,
//                 portfolio_value_overall
//                     .last()
//                     .unwrap_or(&PortfolioEntryReturn {
//                         value: (chrono::offset::Utc::now(), 0.0),
//                     })
//                     // .unwrap()
//                     .value
//                     .1
//                     + change,
//             ),
//         });
//         strategies.insert(
//             portfolio_value_at_t.strategy.clone(),
//             portfolio_value_at_t.value.1.clone(),
//         );
//     }
//
//     Ok(Json(PortfolioValue {
//         portfolio: portfolio_value_overall
//             .iter()
//             .map(|val| val.value)
//             .collect(),
//         strategies: portfolio_value_over_time_unmapped
//             .iter()
//             .map(|json_data| PortfolioValueStrategy {
//                 strategy: json_data.strategy.clone(),
//                 portfolio: json_data.portfolio.clone(),
//                 metrics: json_data.metrics.clone(),
//             })
//             .collect(),
//     }))
// }

pub async fn compute_overall_portfolio_value(
    state: crate::AppState,
) -> Result<Json<PortfolioValue>, String> {
    let sql_strategy = "SELECT DISTINCT strategy FROM trading.strategy";
    let query_strategy = sqlx::query_as::<_, crate::models::StrategyPrimaryKeys>(&sql_strategy);
    let strategies = query_strategy
        .fetch_all(&state.db)
        .await
        .map_err(|err| format!("Failed to find strategies in Database: {}", err))?;

    let tasks = strategies.iter().map(|strat| {
        let state = state.clone();
        let strategy_name = strat.strategy.clone();

        async move {
            match compute_portfolio_value_for_strategy(
                state,
                Strategy {
                    strategy: strategy_name.clone(),
                },
            )
            .await
            {
                Ok(portfolio_value_for_strat) => portfolio_value_for_strat,
                Err(_) => Json(PortfolioValueStrategy {
                    strategy: strategy_name.clone(),
                    status: models::Status::Inactive,
                    portfolio: vec![],
                    metrics: PortfolioMetrics {
                        cagr: 0.0,
                        sharpe_ratio: 0.0,
                        max_drawdown: 0.0,
                        calmar_ratio: 0.0,
                        profit_factor: 0.0,
                        win_rate: 0.0,
                        avg_trade_return: 0.0,
                        positions: HashMap::new(),
                    },
                }),
            }
        }
    });

    let portfolio_value_over_time_unmapped: Vec<Json<PortfolioValueStrategy>> =
        join_all(tasks).await;

    let mut portfolio_value_over_time: Vec<PortfolioEntryWithStrategy> =
        portfolio_value_over_time_unmapped
            .iter()
            .flat_map(|portfolio_val| {
                if portfolio_val.portfolio.is_empty() {
                    return Vec::<PortfolioEntryWithStrategy>::new();
                }
                portfolio_val
                    .portfolio
                    .iter()
                    .map(|val| PortfolioEntryWithStrategy {
                        strategy: portfolio_val.strategy.clone(),
                        value: val.clone(),
                    })
                    .collect()
            })
            .collect();

    portfolio_value_over_time.sort_by(|a, b| a.value.0.cmp(&b.value.0));

    let mut portfolio_value_overall = Vec::<PortfolioEntryReturn>::new();
    let mut strategies = HashMap::<String, f64>::new();

    for portfolio_value_at_t in portfolio_value_over_time {
        let change = portfolio_value_at_t.value.1
            - strategies
                .get(&portfolio_value_at_t.strategy)
                .unwrap_or(&0.0);

        let last_value = if portfolio_value_overall.is_empty() {
            0.0
        } else {
            portfolio_value_overall.last().unwrap().value.1
        };

        portfolio_value_overall.push(PortfolioEntryReturn {
            value: (portfolio_value_at_t.value.0, last_value + change),
        });

        strategies.insert(
            portfolio_value_at_t.strategy.clone(),
            portfolio_value_at_t.value.1,
        );
    }

    Ok(Json(PortfolioValue {
        portfolio: portfolio_value_overall
            .iter()
            .map(|val| val.value)
            .collect(),
        strategies: portfolio_value_over_time_unmapped
            .iter()
            .map(|json_data| PortfolioValueStrategy {
                strategy: json_data.strategy.clone(),
                status: json_data.status.clone(),
                portfolio: json_data.portfolio.clone(),
                metrics: json_data.metrics.clone(),
            })
            .collect(),
    }))
}
