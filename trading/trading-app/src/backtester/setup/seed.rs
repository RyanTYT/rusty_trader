//! Seed the initial capital as a `CASH:SGD` position before the backtest.
//!
//! The strategy's `get_strategy_sgd_value` reads positions from the DB — so
//! the starting cash must be a `CASH:SGD` position (rate 1.0) for the strategy
//! to "see" its available capital. This also clears leftover positions/
//! transactions from a previous run (idempotent re-runs).

use crate::database::tables::DbTables;
use sqlx::PgPool;
#[cfg(feature = "backtest")]
use crate::backtester::setup::config::BacktestMode;

use crate::database::crud::CRUDTrait;
use crate::database::models::{
    AssetType, CurrentStockPositionsPrimaryKeys, CurrentStockPositionsUpdateKeys,
};
use crate::database::models_crud::current_positions::current_positions::{
    CurrentPositionsCRUD, CurrentPositionsOps, CurrentPositionsPrimaryKeys,
    CurrentPositionsUpdateKeys,
};

/// Clear leftover positions/transactions for `strat_name` + seed a
/// `CASH:SGD` position with `capital` SGD (rate 1.0). Idempotent.
///
/// Only `trading.current_stock_positions` and `trading.stock_transactions`
/// for this strategy are cleared; option positions/transactions and open
/// orders are left untouched. The `CASH:SGD` row is what funding and
/// sizing reads through `get_strategy_sgd_value`, so it is inserted with
/// `avg_price = 1.0` (SGD is the base currency) and `last_updated = None`
/// (the trigger stamps it).
///
/// # Errors
/// Returns a string for any SQL failure, including a missing
/// `trading.strategy` row for `strat_name` (the position FK rejects the
/// insert) and any failure of the cumulative position upsert.
pub async fn seed_initial_capital(
    pool: &PgPool,
    strat_name: &str,
    capital: f64,
) -> Result<(), String> {
    // Clear leftovers from a previous run.
    sqlx::query("DELETE FROM trading.current_stock_positions WHERE strategy = $1")
        .bind(strat_name)
        .execute(pool)
        .await
        .map_err(|e| format!("clear current_stock_positions: {e}"))?;
    sqlx::query("DELETE FROM trading.stock_transactions WHERE strategy = $1")
        .bind(strat_name)
        .execute(pool)
        .await
        .map_err(|e| format!("clear stock_transactions: {e}"))?;

    // Seed CASH:SGD = capital.
    let mut db = crate::database::db::DbDirect::connect(&pool);
    let mut cp_crud = db.current_positions(&AssetType::Stock);
    let cash_pk = CurrentPositionsPrimaryKeys::Stock(CurrentStockPositionsPrimaryKeys {
        strategy: strat_name.to_string(),
        stock: "CASH:SGD".to_string(),
        primary_exchange: "".to_string(),
        currency: "SGD".to_string(),
    });
    let cash_uk = CurrentPositionsUpdateKeys::Stock(CurrentStockPositionsUpdateKeys {
        quantity: Some(capital),
        avg_price: Some(1.0),
        last_updated: None,
    });
    #[cfg(feature = "backtest")]
    let res = cp_crud
        .update_positions_additive(cash_pk, cash_uk, BacktestMode::Db)
        .await
        .map_err(|e| format!("seed CASH:SGD: {e}"));
    #[cfg(not(feature = "backtest"))]
    let res = cp_crud
        .update_positions_additive(cash_pk, cash_uk)
        .await
        .map_err(|e| format!("seed CASH:SGD: {e}"));
    res?;
    Ok(())
}
