use std::{cmp::Ordering, sync::Arc, time::Duration};

use chrono::{Datelike, TimeZone, Timelike, Utc};
use chrono_tz::America::New_York;
use ibapi::{Client, prelude::Contract};
use sqlx::PgPool;

use crate::{
    database::{
        crud::CRUDTrait,
        models::{
            AssetType, HistoricalStockDataPrimaryKeysWoTime, TargetStockPositionsPrimaryKeys,
            TargetStockPositionsUpdateKeys,
        },
        models_crud::{
            historical_data::historical_data::{
                HistoricalDataCRUD, HistoricalDataFullKeys, HistoricalDataOps,
                HistoricalDataPrimaryKeysWoTime, NoiseOps, VwapBarValue,
            },
            target_positions::target_positions::{
                TargetPositionsCRUD, TargetPositionsPrimaryKeys, TargetPositionsUpdateKeys,
            },
        },
    },
    market_data::{consolidator::Consolidator, traits::strategy_value::GetStrategyValue},
    strategy::{
        portfolio_functions::proportional_integer_reduce,
        strategy::{BarUpdateOutcome, StrategyExecutor},
    },
};

#[derive(Debug, Clone)]
pub struct Noise {
    priority: u32,
    name: String,
    pool: PgPool,
    tokio_handle: tokio::runtime::Handle,
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
    pub fn new(pool: PgPool, tokio_handle: tokio::runtime::Handle) -> Self {
        Self {
            priority: 1,
            name: "noise".to_string(),
            pool,
            tokio_handle,
        }
    }
}

impl StrategyExecutor for Noise {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn is_fx_strategy(&self) -> bool {
        return false;
    }

    fn on_bar_update(
        &self,
        contract: &Contract,
        bar: &HistoricalDataFullKeys,
        consolidator: &Arc<Consolidator>,
    ) -> Result<BarUpdateOutcome, String> {
        match self._on_bar_update(contract, bar, consolidator) {
            Ok(v) => Ok(v),
            Err(v) => Ok(v),
        }
    }

    fn get_contracts(&self, client: Arc<Client>) -> Vec<Contract> {
        vec![
            Consolidator::_validate_contract(
                client,
                Contract::stock("QQQ")
                    .on_exchange("SMART")
                    .primary("NASDAQ")
                    .in_currency("USD")
                    .build(),
                Duration::from_secs(10),
            )
            .expect("Expected to be able to get_contracts when init_app"),
        ]
    }

    fn warm_up_data(&self, consolidator: &Arc<Consolidator>) -> Result<(), String> {
        let consolidator = consolidator.clone();
        self.tokio_handle.block_on(async move {
            consolidator
                .update_at_least_n_days_data(
                    &Contract::stock("QQQ")
                        .on_exchange("SMART")
                        .primary("NASDAQ")
                        .in_currency("USD")
                        .build(),
                    20,
                    true,
                )
                .await
                .map_err(|e| format!("Error in update_at_least_n_days_data: {}", e))
        })?;

        Ok(())
    }
}

impl Noise {
    fn _on_bar_update(
        &self,
        _contract: &Contract,
        raw_bar: &HistoricalDataFullKeys,
        consolidator: &Arc<Consolidator>,
    ) -> Result<BarUpdateOutcome, BarUpdateOutcome> {
        let bar = match raw_bar {
            HistoricalDataFullKeys::Stock(v) => v,
            _ => panic!("Should not be receiving any other type of bar other than Stock"),
        };
        // tracing::info!("bar")
        let historical_data_crud_orig =
            HistoricalDataCRUD::from(&AssetType::Stock, self.pool.clone());
        let historical_data_crud = historical_data_crud_orig.clone();
        let avg_move_since_open_thread = self.tokio_handle.spawn(async move {
            historical_data_crud
                .get_avg_move_since_open(HistoricalStockDataPrimaryKeysWoTime {
                    stock: "QQQ".to_string(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                })
                // .get_avg_move_since_open("QQQ", "NASDAQ", "USD")
                .await
                .map_err(|e| format!("{}", e))
        });
        let historical_data_crud = historical_data_crud_orig.clone();
        let most_recent_open_thread = self.tokio_handle.spawn(async move {
            historical_data_crud
                .get_most_recent_daily_open(HistoricalStockDataPrimaryKeysWoTime {
                    stock: "QQQ".to_string(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                })
                .await
                .map_err(|e| format!("{}", e))
        });
        let historical_data_crud = historical_data_crud_orig.clone();
        let most_recent_daily_vol_thread = self.tokio_handle.spawn(async move {
            historical_data_crud
                .get_daily_vol(HistoricalStockDataPrimaryKeysWoTime {
                    stock: "QQQ".to_string(),
                    primary_exchange: "NASDAQ".to_string(),
                    currency: "USD".to_string(),
                })
                .await
                .map_err(|e| format!("{}", e))
        });
        let historical_data_crud = historical_data_crud_orig.clone();
        let vwap_thread = self.tokio_handle.spawn(async move {
            historical_data_crud
                .read_last_vwap(
                    HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
                        stock: "QQQ".to_string(),
                        primary_exchange: "NASDAQ".to_string(),
                        currency: "USD".to_string(),
                    }),
                    Some("US/Eastern".to_string()),
                    VwapBarValue::Close,
                )
                .await
                .map_err(|e| format!("{}", e))
        });
        // let historical_data_crud = historical_data_crud_orig.clone();
        // let current_price_thread = self.tokio_handle.spawn(async move {
        //     historical_data_crud
        //         .read_last_bar(
        //             HistoricalDataPrimaryKeysWoTime::Stock(HistoricalStockDataPrimaryKeysWoTime {
        //                 stock: "QQQ".to_string(),
        //                 primary_exchange: "NASDAQ".to_string(),
        //                 currency: "USD".to_string(),
        //             }),
        //             5,
        //         )
        //         .await
        //         .map_err(|e| format!("{}", e))
        //         .expect("Expected at least one bar of QQQ in historical_data table")
        // });
        // let strat_name = self.name.to_string();
        // let current_stock_positions_crud =
        //     CurrentPositionsCRUD::from(&AssetType::Stock, self.pool.clone());
        // let current_pos_thread = self.tokio_handle.spawn(async move {
        //     current_stock_positions_crud
        //         .get_pos_by_strat(strat_name.as_str())
        //         .await
        //         .map_err(|e| format!("{}", e))
        // });

        let (
            avg_move_since_open_joined,
            most_recent_open_joined,
            most_recent_daily_vol_joined,
            vwap_joined,
            // current_price_joined,
            // current_pos_joined,
        ) = self.tokio_handle.block_on(async {
            tokio::join!(
                avg_move_since_open_thread,
                most_recent_open_thread,
                most_recent_daily_vol_thread,
                vwap_thread,
                // current_price_thread,
                // current_pos_thread
            )
        });

        let (
            avg_move_since_open_res,
            most_recent_open_res,
            most_recent_daily_vol_res,
            vwap_res,
            // current_price_res,
            // current_pos_res,
        ) = (
            avg_move_since_open_joined.map_err(|e| {
                tracing::error!("avg_move_since_open_joined failed to resolve: {e:?}");
                BarUpdateOutcome::NoAction
            })?,
            most_recent_open_joined.map_err(|e| {
                tracing::error!("most_recent_open_joined failed to resolve: {e:?}");
                BarUpdateOutcome::NoAction
            })?,
            most_recent_daily_vol_joined.map_err(|e| {
                tracing::error!("most_recent_daily_vol_joined joined failed to resolve: {e:?}");
                BarUpdateOutcome::NoAction
            })?,
            vwap_joined.map_err(|e| {
                tracing::error!("vwap_joined joined failed to resolve: {e:?}");
                BarUpdateOutcome::NoAction
            })?,
            // current_price_joined.map_err(|e| {
            //     tracing::error!("current_price_joined joined failed to resolve: {e:?}");
            //     BarUpdateOutcome::NoAction
            // })?,
            // current_pos_joined.map_err(|e| {
            //     tracing::error!("current_pos_joined joined failed to resolve: {e:?}");
            //     BarUpdateOutcome::NoAction
            // })?,
        );

        let (
            avg_move_since_open,
            most_recent_open,
            most_recent_daily_vol,
            vwap_opt,
            // (high, low, close, time),
            // mut current_positions,
        ) = (
            avg_move_since_open_res.map_err(|e| {
                tracing::error!("avg_move_since_open_res failed to resolve: {e:?}");
                BarUpdateOutcome::NoAction
            })?,
            most_recent_open_res.map_err(|e| {
                tracing::error!("most_recent_open_res failed to resolve: {e:?}");
                BarUpdateOutcome::NoAction
            })?,
            most_recent_daily_vol_res.map_err(|e| {
                tracing::error!("most_recent_daily_vol_res failed to resolve: {e:?}");
                BarUpdateOutcome::NoAction
            })?,
            vwap_res.map_err(|e| {
                tracing::error!("vwap_res failed to resolve: {e:?}");
                BarUpdateOutcome::NoAction
            })?,
            // match current_price_res {
            //     HistoricalDataFullKeys::Stock(v) => (v.high, v.low, v.close, v.time),
            //     _ => panic!("Fetch current price for noise returned Non-stock"),
            // },
            // current_pos_res
            //     .map_err(|e| {
            //         tracing::error!("Failed to fetch current_pos: {e:?}");
            //         BarUpdateOutcome::NoAction
            //     })?
            //     .into_iter()
            //     .map(|pos| match pos {
            //         CurrentPositionsFullKeys::Stock(v) => (v.stock.clone(), v.quantity),
            //         _ => panic!("Fetch current positions for noise returned Non-stock"),
            //     })
            // .collect::<Vec<(String, f64)>>(),
        );
        // if current_positions.len() > 1 {
        //     tracing::error!(
        //         "Got excessive number of positions for noise: {}",
        //         current_positions
        //             .into_iter()
        //             .map(|(stock, qty)| format!("{stock}: {qty}"))
        //             .collect::<Vec<String>>()
        //             .join("\n")
        //     );
        //     return Err(BarUpdateOutcome::NoAction);
        // }
        if let None = vwap_opt {
            tracing::warn!("Not enough data from today (in ET) to calculate VWAP for QQQ");
            return Err(BarUpdateOutcome::NoAction);
        }
        let vwap = vwap_opt.unwrap();

        // Minimum required qty for decent stats is 5.0
        // 50/100 gives a decent reward-return of 5% roughly annualised returns
        // *4 is max leverage intraday
        // let curr_available_funds_wrapped = consolidator.get_current_available_funds();
        // if let Err(e) = curr_available_funds_wrapped {
        //     tracing::error!("Error trying to get current available funds in noise: {e:?}");
        //     return Ok((false, false));
        // }
        // let curr_available_funds = curr_available_funds_wrapped.unwrap();
        let curr_available_funds = consolidator
            .get_strategy_sgd_value(&self.get_name())
            .map_err(|e| {
                tracing::error!("Failed to fetch strategy SGD value for noise: {e:?}");
                BarUpdateOutcome::NoAction
            })?;

        let ideal_qty = if most_recent_daily_vol >= 0.04 {
            70 as i64
        } else {
            100 as i64
        };
        let allowable_positions_tuple =
            proportional_integer_reduce(&vec![ideal_qty], &vec![bar.high], curr_available_funds);
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
                &bar.close,
                &upper_noise
            )
        );

        // let current_pos = current_positions.pop().unwrap_or(("QQQ".to_string(), 0.0));
        // if current_pos.0 != "QQQ".to_string() {
        //     tracing::error!("Encountered foreign asset in Noise: {}", current_pos.0);
        //     return Err(BarUpdateOutcome::NoAction);
        // }
        // let qty = current_pos.1;

        if qty != 0.0 {
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
            if ((bar.close < upper_noise || bar.close <= vwap)
                && (bar.time.minute() == 0 || bar.time.minute() == 30))
                || current_time >= last_time
            {
                let target_stock_positions_crud =
                    TargetPositionsCRUD::from(&AssetType::Stock, self.pool.clone());
                let name = self.get_name();
                self.tokio_handle.block_on(async move {
                    target_stock_positions_crud
                        .delete(&TargetPositionsPrimaryKeys::Stock(
                            TargetStockPositionsPrimaryKeys {
                                strategy: name,
                                stock: "QQQ".to_string(),
                                primary_exchange: "NASDAQ".to_string(),
                                currency: "USD".to_string(),
                            },
                        ))
                        .await
                        .map_err(|e| {
                            tracing::error!("Failed to delete QQQ: {e:?}");
                            BarUpdateOutcome::NoAction
                        })
                })?;
                return Ok(BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]));
            }
            return Ok(BarUpdateOutcome::NoAction);
        }

        if bar.close > upper_noise && (bar.time.minute() == 0 || bar.time.minute() == 30) {
            let target_stock_positions_crud =
                TargetPositionsCRUD::from(&AssetType::Stock, self.pool.clone());
            let name = self.get_name();
            self.tokio_handle.block_on(async move {
                target_stock_positions_crud
                    .create_or_update(
                        &TargetPositionsPrimaryKeys::Stock(TargetStockPositionsPrimaryKeys {
                            strategy: name,
                            primary_exchange: "NASDAQ".to_string(),
                            currency: "USD".to_string(),
                            stock: "QQQ".to_string(),
                        }),
                        &TargetPositionsUpdateKeys::Stock(TargetStockPositionsUpdateKeys {
                            avg_price: Some(0.0),
                            quantity: Some(qty),
                        }),
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!("Failed to delete QQQ: {e:?}");
                        BarUpdateOutcome::NoAction
                    })
            })?;
            return Ok(BarUpdateOutcome::PendingDbQuery(vec![AssetType::Stock]));
        }

        return Ok(BarUpdateOutcome::NoAction);
    }
}

