use crate::market_data::consolidator::Consolidator;
use ibapi::prelude::{AccountSummaryResult, AccountSummaryTags, Contract};
use std::{
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::sync::mpsc::channel;

pub trait AccountTracker {
    fn begin_receiving_available_funds(
        &self,
        account_raw: &str,
        weak_consolidator: Weak<Consolidator>,
    );
    fn close_available_funds_channel(&self) -> Result<(), String>;
    fn get_current_available_funds(&self) -> Result<f64, String>;
}

impl AccountTracker for Consolidator {
    fn begin_receiving_available_funds(
        &self,
        account_raw: &str,
        weak_consolidator: Weak<Consolidator>,
    ) {
        let alive_checker = Arc::new(());
        {
            self.available_funds_channel_killer
                .lock()
                .expect("lock fore available_funds_channel_killer poisoned for some reason")
                .replace(alive_checker.clone());
        }
        let (sender, mut rcv) = channel::<f64>(10);
        let weak_client = Arc::downgrade(&self.client);
        let account = account_raw.to_string();
        std::thread::spawn(move || {
            let account_summary_subscription = {
                let client_opt = weak_client.upgrade();
                if client_opt.is_none() {
                    tracing::warn!("client dead before listening for available funds");
                }
                let client = client_opt.unwrap();
                let account_group = ibapi::accounts::types::AccountGroup("All".to_string());
                client
                    .account_summary(&account_group, &[AccountSummaryTags::AVAILABLE_FUNDS, "$LEDGER:USD"])
                    .expect("Expected to be able to request account_updates when new consolidator instance was initialised!")
            };
            loop {
                // for account_summary in account_summary_subscription {

                match account_summary_subscription.next_timeout(Duration::from_secs(5)) {
                    Some(account_summary) => match account_summary {
                        AccountSummaryResult::Summary(summary) => {
                            if summary.account != account {
                                continue;
                            }
                            if summary.tag != "AvailableFunds" {
                                continue;
                            }

                            if summary.currency != "USD" {
                                tracing::warn!(
                                    message=%format!(
                                        "Currency not configured correctly for AccountSummaries: {}",
                                        summary.currency
                                    )
                                );
                                // let usd_contract = Contract::forex("SGD", "USD").build();
                                let sgd_contract = Contract {
                                    symbol: ibapi::prelude::Symbol::new("USD"),
                                    security_type: ibapi::prelude::SecurityType::ForexPair,
                                    exchange: "IDEALPRO".into(),
                                    currency: "SGD".into(),
                                    ..Default::default()
                                };
                                let sgd_price = {
                                    let consolidator_opt = weak_consolidator.upgrade();
                                    if consolidator_opt.is_none() {
                                        tracing::warn!("Consolidator is dead");
                                        return;
                                    }
                                    let consolidator = consolidator_opt.unwrap();
                                    consolidator.get_current_price(&sgd_contract, &false, &["2"])
                                };
                                if let Err(e) = sgd_price {
                                    tracing::error!(
                                        message=%format!(
                                            "Error trying to request USD price from {}: {e:?}",
                                            summary.currency
                                        )
                                    );
                                    continue;
                                }
                                if let Err(e) = sender.blocking_send(
                                    summary.value.parse::<f64>().expect(
                                        "Expected to be able to parse available_funds_value",
                                    ) * sgd_price.unwrap(),
                                ) {
                                    tracing::error!(
                                        "Error occured while sending account summary value to tokio runtime: {e:?}"
                                    )
                                };
                                continue;
                            }
                            if let Err(e) = sender.blocking_send(
                                summary
                                    .value
                                    .parse::<f64>()
                                    .expect("Expected to be able to parse available_funds_value"),
                            ) {
                                tracing::error!(
                                    "Error occured while sending account summary value to tokio runtime: {e:?}"
                                )
                            };
                        }
                        AccountSummaryResult::End => {
                            continue;
                        }
                    },
                    None => {
                        {
                            let alive_rc = Arc::strong_count(&alive_checker);
                            tracing::warn!("alive_checker has {alive_rc:?} strong references");
                            if alive_rc == 1 {
                                account_summary_subscription.cancel();
                                return;
                            }
                        }
                        return;
                    }
                }
            }
        });
        let cloned_available_funds = self.available_funds.clone();
        tokio::spawn(async move {
            while let Some(available_funds_value) = rcv.recv().await {
                tracing::info!(
                    "AccountSummaries Update received! Still Alive! AvailableFunds value: {available_funds_value:?}",
                );
                cloned_available_funds
                    .lock()
                    .expect("Expected to be able to acquire available_funds lock!")
                    .replace(available_funds_value);
            }
        });
    }

    fn close_available_funds_channel(&self) -> Result<(), String> {
        self.available_funds_channel_killer
            .lock()
            .map_err(|e| {
                format!("error trying to acquire lock for available_funds_channel_killer: {e:?}")
            })?
            .take();
        Ok(())
    }

    fn get_current_available_funds(&self) -> Result<f64, String> {
        let curr_available_funds = self.available_funds.lock().expect(
            "Expected to be able to acquire available_funds lock for get_current_available_funds",
        );
        if curr_available_funds.is_some() {
            return Ok(curr_available_funds.unwrap());
        }
        Err("No available funds value retrieved yet!".to_string())
    }
}
