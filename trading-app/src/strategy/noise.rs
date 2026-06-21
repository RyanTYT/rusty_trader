use std::{
    cmp::Ordering,
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{Datelike, TimeZone, Timelike, Utc};
use chrono_tz::America::New_York;
use ibapi::prelude::Contract;
use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUDTrait,
        models::{AssetType, TargetStockPositionsUpdateKeys},
        models_crud::{
            current_stock_positions::get_specific_current_stock_positions_crud,
            historical_data::get_specific_historical_data_crud,
            target_stock_positions::get_target_stock_positions_crud,
        },
    },
    market_data::{account_tracker::AccountTracker, consolidator::Consolidator},
    strategy::{portfolio_functions::proportional_integer_reduce, strategy::StrategyExecutor},
};

#[derive(Debug, Clone)]
pub struct Noise {
    priority: u32,
    name: String,
    pool: PgPool,
    contract: Contract,
}

impl PartialEq for Noise {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.name == other.name
    }
}

impl Eq for Noise {}

impl PartialOrd for Noise {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Noise {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => self.name.cmp(&other.name),
            other => other,
        }
    }
}

impl Noise {
    pub fn new(pool: PgPool, weak_consolidator: Weak<Consolidator>) -> Self {
        let contract = weak_consolidator
            .upgrade()
            .expect("Expected consolidator not to be dead while init noise")
            .validate_contract(
                &Contract::stock("QQQ")
                    .on_exchange("SMART")
                    .primary("NASDAQ")
                    .in_currency("USD")
                    .build(),
                Duration::from_secs(10),
            )
            .expect("Expected noise contract not to be none");
        Self {
            priority: 1,
            name: "noise".to_string(),
            pool,
            contract,
        }
    }
}

#[async_trait]
impl StrategyExecutor for Noise {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn uses_data_type(&self) -> AssetType {
        AssetType::Stock
    }

    fn is_fx_strategy(&self) -> bool {
        return false;
    }

    async fn on_bar_update(
        &self,
        _contract: &Contract,
        consolidator: &Arc<Consolidator>,
    ) -> Result<(bool, bool), String> {
        let cloned_pool = self.pool.clone();
        let avg_move_since_open_thread = tokio::spawn(async move {
            let historical_data_crud = get_specific_historical_data_crud(cloned_pool);
            historical_data_crud
                .get_avg_move_since_open("QQQ", "NASDAQ", "USD")
                .await
                .map_err(|e| format!("{}", e))
        });
        let cloned_pool = self.pool.clone();
        let most_recent_open_thread = tokio::spawn(async move {
            let historical_data_crud = get_specific_historical_data_crud(cloned_pool);
            historical_data_crud
                .get_most_recent_daily_open("QQQ", "NASDAQ")
                .await
                .map_err(|e| format!("{}", e))
        });
        let cloned_pool = self.pool.clone();
        let most_recent_daily_vol_thread = tokio::spawn(async {
            let historical_data_crud = get_specific_historical_data_crud(cloned_pool);
            historical_data_crud
                .get_daily_vol("QQQ", "NASDAQ")
                .await
                .map_err(|e| format!("{}", e))
        });
        let cloned_pool = self.pool.clone();
        let vwap_thread = tokio::spawn(async {
            let historical_data_crud = get_specific_historical_data_crud(cloned_pool);
            historical_data_crud
                .read_vwap("QQQ", "NASDAQ", "USD")
                .await
                .map_err(|e| format!("{}", e))
        });
        let cloned_pool = self.pool.clone();
        let current_price_thread = tokio::spawn(async {
            let historical_data_crud = get_specific_historical_data_crud(cloned_pool);
            historical_data_crud
                .read_last_bar_of_stock("QQQ", "NASDAQ", "USD", &5)
                .await
                .map_err(|e| format!("{}", e))
                .expect("Expected at least one bar of QQQ in historical_data table")
        });
        let strat_name = self.name.to_string();
        let cloned_pool = self.pool.clone();
        let current_pos_thread = tokio::spawn(async move {
            let current_stock_positions = get_specific_current_stock_positions_crud(cloned_pool);
            current_stock_positions
                .get_pos_by_strat(strat_name.as_str())
                .await
                .map_err(|e| format!("{}", e))
        });

        let (
            avg_move_since_open_joined,
            most_recent_open_joined,
            most_recent_daily_vol_joined,
            vwap_joined,
            current_price_joined,
            current_pos_joined,
        ) = tokio::join!(
            avg_move_since_open_thread,
            most_recent_open_thread,
            most_recent_daily_vol_thread,
            vwap_thread,
            current_price_thread,
            current_pos_thread
        );

        let (
            avg_move_since_open_res,
            most_recent_open_res,
            most_recent_daily_vol_res,
            vwap_res,
            current_price_res,
            current_pos_res,
        ) = (
            avg_move_since_open_joined.map_err(|_e| "avg_move_since_open_thread panicked")?,
            most_recent_open_joined.map_err(|_e| "most_recent_open_thread panicked")?,
            most_recent_daily_vol_joined.map_err(|_e| "most_recent_daily_vol_thread panicked")?,
            vwap_joined.map_err(|_e| "vwap_thread panicked")?,
            current_price_joined.map_err(|_e| "current_price_thread panicked")?,
            current_pos_joined.map_err(|_e| "current_pos_thread panicked")?,
        );

        let (
            avg_move_since_open,
            most_recent_open,
            most_recent_daily_vol,
            vwap_opt,
            current_price,
            current_poses,
        ) = (
            avg_move_since_open_res.map_err(|e| format!("{}", e))?,
            most_recent_open_res.map_err(|e| format!("{}", e))?,
            most_recent_daily_vol_res.map_err(|e| format!("{}", e))?,
            vwap_res.map_err(|e| format!("{}", e))?,
            current_price_res.expect(&format!("Expected to be able to get current price of QQQ")),
            current_pos_res.map_err(|e| format!("{}", e))?,
        );
        if let None = vwap_opt {
            tracing::warn!("Not enough data from today (in ET) to calculate VWAP for QQQ");
            return Ok((false, false));
        }
        let vwap = vwap_opt.unwrap();

        // Minimum required qty for decent stats is 5.0
        // 50/100 gives a decent reward-return of 5% roughly annualised returns
        // *4 is max leverage intraday
        let curr_available_funds_wrapped = consolidator.get_current_available_funds();
        if let Err(e) = curr_available_funds_wrapped {
            tracing::error!("Error trying to get current available funds in noise: {e:?}");
            return Ok((false, false));
        }
        let curr_available_funds = curr_available_funds_wrapped.unwrap();

        let ideal_qty = if most_recent_daily_vol >= 0.04 {
            70 as i64
        } else {
            100 as i64
        };
        let allowable_positions_tuple = proportional_integer_reduce(
            &vec![ideal_qty],
            &vec![current_price.high],
            curr_available_funds,
        );
        let allowable_positions = allowable_positions_tuple.0.first().unwrap();
        let qty = {
            if *allowable_positions != ideal_qty {
                tracing::warn!(
                    "Maximum allowable position for QQQ for Noise is: {allowable_positions:?}"
                );
                *allowable_positions
            } else {
                ideal_qty
            }
        } as f64;

        let (upper_noise, _lower_noise) = (
            (1.0 + avg_move_since_open) * most_recent_open,
            (1.0 - avg_move_since_open) * most_recent_open,
        );

        tracing::info!(
            message=%format!(
                "QQQ price is {}, upper noise is {}",
                &current_price.close,
                &upper_noise
            )
        );

        for current_pos in current_poses {
            if current_pos.stock.as_str() != "QQQ" && current_pos.quantity != 0.0 {
                let target_stock_positions = get_target_stock_positions_crud(self.pool.clone());
                target_stock_positions
                    .delete(&crate::database::models::TargetStockPositionsPrimaryKeys {
                        strategy: self.name.clone(),
                        stock: current_pos.stock,
                        primary_exchange: current_pos.primary_exchange,
                        currency: current_pos.currency,
                    })
                    .await
                    .map_err(|e| format!("{}", e))?;
                continue;
            }
            if current_pos.quantity != 0.0 {
                let current_time = Utc::now().with_timezone(&New_York);
                let last_time = New_York
                    .with_ymd_and_hms(
                        current_time.year(),
                        current_time.month(),
                        current_time.day(),
                        15,
                        45,
                        0,
                    )
                    .unwrap();
                if ((current_price.close < upper_noise || current_price.close <= vwap)
                    && (current_price.time.minute() == 0 || current_price.time.minute() == 30))
                    || current_time >= last_time
                {
                    let target_stock_positions = get_target_stock_positions_crud(self.pool.clone());
                    target_stock_positions
                        .delete(&crate::database::models::TargetStockPositionsPrimaryKeys {
                            strategy: self.name.clone(),
                            stock: "QQQ".to_string(),
                            primary_exchange: "".to_string(),
                            currency: "USD".to_string(),
                        })
                        .await
                        .map_err(|e| format!("{}", e))?;
                    return Ok((true, false));
                    // target_stock_positions
                    //     .update(
                    //         &crate::database::models::TargetStockPositionsPrimaryKeys {
                    //             strategy: self.name.clone(),
                    //             asset_type: crate::database::models::AssetType::Stock,
                    //             stock: "QQQ".to_string(),
                    //         },
                    //         &crate::database::models::TargetStockPositionsUpdateKeys {
                    //             avg_price: Some(current_price.close),
                    //             quantity: Some(0.0),
                    //         },
                    //     )
                    //     .await
                    //     .map_err(|e| format!("{}", e))?;
                }
                return Ok((false, false));
            }

            if current_price.close > upper_noise
                && (current_price.time.minute() == 0 || current_price.time.minute() == 30)
            {
                let target_stock_positions = get_target_stock_positions_crud(self.pool.clone());
                if let Ok(target_stock_pos) = target_stock_positions
                    .read(&crate::database::models::TargetStockPositionsPrimaryKeys {
                        strategy: self.name.clone(),
                        stock: "QQQ".to_string(),
                        primary_exchange: "".to_string(),
                        currency: "USD".to_string(),
                    })
                    .await
                    .map_err(|e| {
                        format!(
                            "Couldn't read TargetStockPositions for QQQ for noise: {}",
                            e
                        )
                    })
                {
                    if target_stock_pos.is_some() {
                        target_stock_positions
                            .update(
                                &crate::database::models::TargetStockPositionsPrimaryKeys {
                                    strategy: self.name.clone(),
                                    stock: "QQQ".to_string(),
                                    primary_exchange: "".to_string(),
                                    currency: "USD".to_string(),
                                },
                                &TargetStockPositionsUpdateKeys {
                                    avg_price: Some(0.0),
                                    quantity: Some(qty),
                                },
                            )
                            .await
                            .map_err(|e| format!("{}", e))?;
                        return Ok((true, false));
                    }
                }
                target_stock_positions
                    .create(&crate::database::models::TargetStockPositionsFullKeys {
                        strategy: self.name.clone(),
                        stock: "QQQ".to_string(),
                        primary_exchange: "".to_string(),
                        currency: "USD".to_string(),
                        avg_price: 0.0,
                        quantity: qty,
                    })
                    .await
                    .map_err(|e| format!("{}", e))?;
                return Ok((true, false));
            }
        }

        Ok((false, false))
    }

    fn get_contracts(&self) -> Vec<Contract> {
        let mut contracts = Vec::new();
        contracts.push(self.contract.clone());
        // contracts.push(
        //     ContractBuilder::new()
        //         .symbol("USD")
        //         .security_type(ibapi::prelude::SecurityType::ForexPair)
        //         .currency("SGD")
        //         .exchange("IDEALPRO")
        //         .build()
        //         .expect("Expected contract builder to return valid contract"),
        // );
        contracts
    }

    fn get_contract(
        &self,
        stock: &str,
        _primary_exchange: &str,
        _currency: &str,
        _consolidator: &Arc<Consolidator>,
    ) -> Option<Contract> {
        if stock == "QQQ" {
            return Some(self.contract.clone());
        }
        // return Some(
        //     ContractBuilder::new()
        //         .symbol("USD")
        //         .security_type(ibapi::prelude::SecurityType::ForexPair)
        //         .currency("SGD")
        //         .exchange("IDEALPRO")
        //         .build()
        //         .expect("Expected contract builder to return valid contract"),
        // );
        return None;
    }

    async fn warm_up_data(&self, consolidator: &Arc<Consolidator>) -> Result<(), String> {
        consolidator
            .update_at_least_n_days_data(
                &self.contract,
                ibapi::prelude::HistoricalWhatToShow::Trades,
                &20,
                &true,
            )
            .await
            .map_err(|e| format!("Error in update_at_least_n_days_data: {}", e))?;

        Ok(())
    }
}
