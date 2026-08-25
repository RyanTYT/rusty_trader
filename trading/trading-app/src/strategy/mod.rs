pub mod noise;
pub mod strategy;
pub mod forex_momentum;
pub mod manual;
pub mod portfolio_functions;
pub mod unknown;

pub mod helpers;

#[cfg(feature = "backtest")]
pub fn construct_strategy(
    name: &str,
    pool: sqlx::PgPool,
    handle: tokio::runtime::Handle,
    params: Option<std::collections::HashMap<String, f64>>,
) -> Option<strategy::StrategyEnum> {
    if params.is_none() {
        return None;
    }

    match name {
        "noise" => Some(strategy::StrategyEnum::Noise(
            noise::Noise::new(pool, handle).with_backtest_params(params.unwrap()),
        )),
        other => {
            tracing::warn!("Unknown strategy '{other}' - skipping");
            None
        }
    }
}

#[cfg(feature = "backtest")]
pub fn construct_strategies(
    names: Vec<String>,
    pool: sqlx::PgPool,
    handle: tokio::runtime::Handle,
) -> Vec<strategy::StrategyEnum> {
    names
        .iter()
        .filter_map(|name| construct_strategy(name, pool.clone(), handle.clone(), None))
        .collect()
}
