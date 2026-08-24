use std::{cmp::Ordering, sync::Arc};

use async_trait::async_trait;
use chrono::Timelike;
use ibapi::{contracts::ContractBuilder, prelude::Contract};
use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUDTrait,
        models::TargetStockPositionsUpdateKeys,
        models_crud::{
            current_stock_positions::get_specific_current_stock_positions_crud,
            historical_data::get_specific_historical_data_crud,
            target_stock_positions::get_target_stock_positions_crud,
        },
    },
    market_data::consolidator::Consolidator,
    strategy::strategy::StrategyExecutor,
};

#[derive(Clone)]
pub struct ThresholdRebalancer {
    priority: u32,
    name: String,
    pool: PgPool,
    es_contract: Contract,
    zn_contract: Contract,
}

impl PartialEq for ThresholdRebalancer {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.name == other.name
    }
}

impl Eq for ThresholdRebalancer {}

impl PartialOrd for ThresholdRebalancer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ThresholdRebalancer {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => self.name.cmp(&other.name),
            other => other,
        }
    }
}

impl ThresholdRebalancer {
    pub fn new(pool: PgPool) -> Self {
        let es_contract = ContractBuilder::new()
            .symbol("ES")
            .security_type(ibapi::prelude::SecurityType::Future)
            .exchange("CME")
            .currency("USD")
            .build()
            .expect("Expected to be able to build ES contract for ThresholdRebalancer strategy");
        let zn_contract = ContractBuilder::new()
            .symbol("ZN")
            .security_type(ibapi::prelude::SecurityType::Future)
            .exchange("CBOT")
            .currency("USD")
            .build()
            .expect("Expected to be able to build ZN contract for ThresholdRebalancer strategy");
        Self {
            priority: 1,
            name: "threshold_rebalancer".to_string(),
            pool,
            es_contract,
            zn_contract,
        }
    }
}

#[async_trait]
impl StrategyExecutor for ThresholdRebalancer {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    async fn on_bar_update<T>(
        &self,
        _contract: &Contract,
        consolidator: Arc<Consolidator<T>>,
    ) -> Result<(bool, bool), String>
    where
        T: StrategyExecutor + 'static,
    {
        let cloned_pool = self.pool.clone();
        let avg_move_since_open_thread = tokio::spawn(async move {
            let historical_data_crud = get_specific_historical_data_crud(cloned_pool);
            historical_data_crud
                .get_avg_move_since_open("QQQ".to_string(), "".to_string())
                .await
                .map_err(|e| format!("{}", e))
        });
        let cloned_pool = self.pool.clone();
        let most_recent_open_thread = tokio::spawn(async move {
            let historical_data_crud = get_specific_historical_data_crud(cloned_pool);
            historical_data_crud
                .get_most_recent_daily_open("QQQ".to_string(), "".to_string())
                .await
                .map_err(|e| format!("{}", e))
        });
        let cloned_pool = self.pool.clone();
        let most_recent_daily_vol_thread = tokio::spawn(async {
            let historical_data_crud = get_specific_historical_data_crud(cloned_pool);
            historical_data_crud
                .get_daily_vol("QQQ".to_string(), "".to_string())
                .await
                .map_err(|e| format!("{}", e))
        });
        let cloned_pool = self.pool.clone();
        let vwap_thread = tokio::spawn(async {
            let historical_data_crud = get_specific_historical_data_crud(cloned_pool);
            historical_data_crud
                .read_vwap("QQQ".to_string(), "".to_string())
                .await
                .map_err(|e| format!("{}", e))
        });
        let cloned_pool = self.pool.clone();
        let current_price_thread = tokio::spawn(async {
            let historical_data_crud = get_specific_historical_data_crud(cloned_pool);
            historical_data_crud
                .read_last_bar_of_stock("QQQ".to_string(), "".to_string())
                .await
                .map_err(|e| format!("{}", e))
                .expect("Expected at least one bar of QQQ in historical_data table")
        });
        let strat_name = self.name.clone();
        let cloned_pool = self.pool.clone();
        let current_pos_thread = tokio::spawn(async move {
            let current_stock_positions = get_specific_current_stock_positions_crud(cloned_pool);
            current_stock_positions
                .get_pos_by_strat(strat_name)
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
            vwap,
            current_price,
            current_pos,
        ) = (
            avg_move_since_open_res.map_err(|e| format!("{}", e))?,
            most_recent_open_res.map_err(|e| format!("{}", e))?,
            most_recent_daily_vol_res.map_err(|e| format!("{}", e))?,
            vwap_res.map_err(|e| format!("{}", e))?,
            current_price_res.expect(&format!("Expected to be able to get current price of QQQ")),
            current_pos_res.map_err(|e| format!("{}", e))?,
        );

        let qty = if most_recent_daily_vol > 0.04 {
            5.0
        } else {
            20.0
        };

        let (upper_noise, lower_noise) = (
            (1.0 + avg_move_since_open) * most_recent_open,
            (1.0 - avg_move_since_open) * most_recent_open,
        );

        assert!(current_pos.len() <= 1);

        if current_pos.len() == 1 {
            if current_price.close < upper_noise || current_price.close <= vwap {
                let target_stock_positions = get_target_stock_positions_crud(self.pool.clone());
                target_stock_positions
                    .delete(&crate::database::models::TargetStockPositionsPrimaryKeys {
                        strategy: self.name.clone(),
                        stock: "QQQ".to_string(),
                        primary_exchange: "".to_string(),
                    })
                    .await
                    .map_err(|e| format!("{}", e))?;
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
            return Ok((true, false));
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
                            },
                            &TargetStockPositionsUpdateKeys {
                                // avg_price: Some(current_price.close),
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
                    // avg_price: current_price.close,
                    avg_price: 0.0,
                    quantity: qty,
                })
                .await
                .map_err(|e| format!("{}", e))?;
            return Ok((true, false));
        }

        Ok((false, false))
    }

    fn get_contracts(&self) -> Vec<Contract> {
        let mut contracts = Vec::new();
        contracts.push(self.es_contract.clone());
        contracts.push(self.zn_contract.clone());
        contracts
    }

    fn get_contract(&self, stock: String, primary_exchange: String) -> Option<Contract> {
        if stock == "ES" {
            return Some(self.es_contract.clone());
        } else if stock == "ZN" {
            return Some(self.zn_contract.clone());
        }
        return None;
    }

    async fn warm_up_data<T>(&self, consolidator: Arc<Consolidator<T>>) -> Result<(), String>
    where
        T: StrategyExecutor + 'static,
    {
        consolidator
            .update_at_least_n_days_data(
                &self.es_contract,
                ibapi::prelude::HistoricalWhatToShow::Trades,
                20,
                false,
            )
            .await
            .map_err(|e| format!("Error in update_at_least_n_days_data: {}", e))?;

        consolidator
            .update_at_least_n_days_data(
                &self.zn_contract,
                ibapi::prelude::HistoricalWhatToShow::Trades,
                20,
                false,
            )
            .await
            .map_err(|e| format!("Error in update_at_least_n_days_data: {}", e))?;

        Ok(())
    }
}
