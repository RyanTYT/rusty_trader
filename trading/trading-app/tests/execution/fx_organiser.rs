//! Funding-plan scenarios for `OrderEngine::get_fx_attachments`, driven by a
//! fake `PriceSupplier` and a temp `OrderStore` instead of an IB Gateway.

use std::collections::HashMap;
use std::sync::Arc;

use ibapi::prelude::Contract;
use serde_json::Value;
use trading_app::database::models_crud::target_positions::target_positions::TargetPositionsQtyDiff;
use trading_app::execution::fx_backed_up_order::OrderStore;
use trading_app::execution::fx_organiser::FxFundingPlan;
use trading_app::execution::order_engine::{OrderEngine, OrderIBKR};
use trading_app::market_data::traits::contracts::ContractBroker;
use trading_app::market_data::traits::current_price::{HistoricalDataConfig, PriceSupplier};

use crate::common::TestDb;
use crate::common::scenario::{self, FromJson, ToJson, arr, assert_exact};
use crate::support::open_store;

// ---------------------------------------------------------------------------
// JSON scenario runner (`tests/execution/fx_organiser.json`)
// ---------------------------------------------------------------------------

/// Price per symbol, 1.0 when the case does not name the symbol. Validation is
/// always on so the market also satisfies `ContractBroker`.
struct ScenarioMarket {
    prices: HashMap<String, f64>,
    validates: bool,
}

#[async_trait::async_trait]
impl PriceSupplier for ScenarioMarket {
    fn get_current_price(
        &self,
        contract: Contract,
        _vwap: bool,
        _generic_ticks: &[&str],
    ) -> Result<f64, String> {
        Ok(self
            .prices
            .get(contract.symbol.as_str())
            .copied()
            .unwrap_or(1.0))
    }

    async fn populate_historical_data(
        &self,
        _contract: &Contract,
        _config: &HistoricalDataConfig,
    ) -> Result<(), String> {
        Ok(())
    }
}

impl ContractBroker for ScenarioMarket {
    fn get_validated_contract(&self, contract: Contract) -> Option<Contract> {
        self.validates.then_some(contract)
    }
}

fn scenario_prices(case: &Value) -> HashMap<String, f64> {
    case.get("prices")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .map(|(symbol, price)| {
                    (
                        symbol.clone(),
                        price
                            .as_f64()
                            .unwrap_or_else(|| panic!("price for {symbol} is not a number")),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn seed_scenario_store(store: &OrderStore, case: &Value) {
    let Some(seeded) = case.get("store").and_then(Value::as_object) else {
        return;
    };
    for (strategy, orders) in seeded {
        let orders = orders
            .as_array()
            .unwrap_or_else(|| panic!("store[{strategy}] must be an array in {case}"))
            .iter()
            .enumerate()
            .map(|(index, order)| {
                OrderIBKR::from_json(order)
                    .unwrap_or_else(|e| panic!("store[{strategy}][{index}]: {e}"))
            })
            .collect::<Vec<_>>();
        store
            .store_orders(strategy, &orders)
            .unwrap_or_else(|e| panic!("seed store for {strategy}: {e}"));
    }
}

/// Each case supplies target-position diffs (including the `CASH:<ccy>` budget
/// rows), an optional parked-order store, and a price per symbol. The
/// expectation is the complete `FxFundingPlan`.
#[tokio::test]
#[ignore = "requires a live database"]
async fn scenarios() {
    let db = TestDb::setup().await;
    let _engine = OrderEngine::new(db.pool.clone(), tokio::runtime::Handle::current());

    for (name, case) in scenario::cases("tests/execution/fx_organiser.json") {
        if case
            .get("known_bug")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            eprintln!(
                "SKIP known_bug `{name}`: {}",
                case.get("bug")
                    .and_then(Value::as_str)
                    .unwrap_or("no description")
            );
            continue;
        }

        let (_dir, _guard, store) = open_store();
        seed_scenario_store(&store, &case);

        let targets = arr(&case, "targets")
            .iter()
            .enumerate()
            .map(|(index, target)| {
                TargetPositionsQtyDiff::from_json(target)
                    .unwrap_or_else(|e| panic!("{name}: targets[{index}]: {e}"))
            })
            .collect::<Vec<_>>();
        let market = Arc::new(ScenarioMarket {
            prices: scenario_prices(&case),
            validates: true,
        });

        let plan =
            OrderEngine::get_fx_attachments(&targets, "strat", &store, &Arc::downgrade(&market))
                .unwrap_or_else(|e| panic!("`{name}`: {e}"));

        let expected = FxFundingPlan::from_json(&case["expected"])
            .unwrap_or_else(|e| panic!("{name}: expected: {e}"));

        assert_exact(
            &format!("fx_organiser/{name}"),
            plan.to_json(),
            &expected.to_json(),
        );
    }
}
