//! Model accessors shared by every database session.
//!
//! Import `DbTables` and any `DbConn` session (`DbDirect` or `DbTxn`) gets one
//! method per table, each returning the model CRUD bound to that session:
//!
//! ```ignore
//! let mut db = DbDirect::connect(&pool);
//! db.open_orders(&asset).read(&pk).await?;
//! db.current_positions(&asset).read_all().await?;
//! ```
//!
//! Interface models (`open_orders`, `current_positions`, `target_positions`,
//! `transactions`, `historical_data`) take an `AssetType` and return the
//! stock/option/forex enum. The concrete tables are exposed too, for when the
//! asset class is known statically.

use crate::database::db::DbConn;
use crate::database::models::AssetType;
use crate::database::models_crud::cancelled_orders::CancelledOrdersCRUD;
use crate::database::models_crud::current_positions::current_option_positions::CurrentOptionPositionsCRUD;
use crate::database::models_crud::current_positions::current_positions::CurrentPositionsCRUD;
use crate::database::models_crud::current_positions::current_stock_positions::CurrentStockPositionsCRUD;
use crate::database::models_crud::historical_data::historical_data::HistoricalDataCRUD;
use crate::database::models_crud::historical_data::historical_forex_data::HistoricalForexDataCRUD;
use crate::database::models_crud::historical_data::historical_options_data::HistoricalOptionsDataCRUD;
use crate::database::models_crud::historical_data::historical_stock_data::HistoricalStockDataCRUD;
use crate::database::models_crud::logs::LogsCRUD;
use crate::database::models_crud::notification::NotificationCRUD;
use crate::database::models_crud::open_orders::open_orders::OpenOrdersCRUD;
use crate::database::models_crud::open_orders::open_option_orders::OpenOptionOrdersCRUD;
use crate::database::models_crud::open_orders::open_stock_orders::OpenStockOrdersCRUD;
use crate::database::models_crud::staged_commissions::StagedCommissionsCRUD;
use crate::database::models_crud::strategy::StrategyCRUD;
use crate::database::models_crud::target_positions::target_option_positions::TargetOptionPositionsCRUD;
use crate::database::models_crud::target_positions::target_positions::TargetPositionsCRUD;
use crate::database::models_crud::target_positions::target_stock_positions::TargetStockPositionsCRUD;
use crate::database::models_crud::transactions::option_transactions::OptionTransactionsCRUD;
use crate::database::models_crud::transactions::stock_transactions::StockTransactionsCRUD;
use crate::database::models_crud::transactions::transactions::TransactionsCRUD;

/// One accessor per table, implemented for every `DbConn`.
pub trait DbTables: DbConn {
    // ── standalone tables ──────────────────────────────────────────────
    fn logs(&mut self) -> LogsCRUD<'_> {
        LogsCRUD::new(self.exec())
    }

    fn strategy(&mut self) -> StrategyCRUD<'_> {
        StrategyCRUD::new(self.exec())
    }

    fn notification(&mut self) -> NotificationCRUD<'_> {
        NotificationCRUD::new(self.exec())
    }

    fn cancelled_orders(&mut self) -> CancelledOrdersCRUD<'_> {
        CancelledOrdersCRUD::new(self.exec())
    }

    fn staged_commissions(&mut self) -> StagedCommissionsCRUD<'_> {
        StagedCommissionsCRUD::new(self.exec())
    }

    // ── open orders ────────────────────────────────────────────────────
    fn open_orders(&mut self, asset: &AssetType) -> OpenOrdersCRUD<'_> {
        OpenOrdersCRUD::from_exec(asset, self.exec())
    }

    fn open_stock_orders(&mut self) -> OpenStockOrdersCRUD<'_> {
        OpenStockOrdersCRUD::new(self.exec())
    }

    fn open_option_orders(&mut self) -> OpenOptionOrdersCRUD<'_> {
        OpenOptionOrdersCRUD::new(self.exec())
    }

    // ── current positions ──────────────────────────────────────────────
    fn current_positions(&mut self, asset: &AssetType) -> CurrentPositionsCRUD<'_> {
        CurrentPositionsCRUD::from_exec(asset, self.exec())
    }

    fn current_stock_positions(&mut self) -> CurrentStockPositionsCRUD<'_> {
        CurrentStockPositionsCRUD::new(self.exec())
    }

    fn current_option_positions(&mut self) -> CurrentOptionPositionsCRUD<'_> {
        CurrentOptionPositionsCRUD::new(self.exec())
    }

    // ── target positions ───────────────────────────────────────────────
    fn target_positions(&mut self, asset: &AssetType) -> TargetPositionsCRUD<'_> {
        TargetPositionsCRUD::from_exec(asset, self.exec())
    }

    fn target_stock_positions(&mut self) -> TargetStockPositionsCRUD<'_> {
        TargetStockPositionsCRUD::new(self.exec())
    }

    fn target_option_positions(&mut self) -> TargetOptionPositionsCRUD<'_> {
        TargetOptionPositionsCRUD::new(self.exec())
    }

    // ── transactions ───────────────────────────────────────────────────
    fn transactions(&mut self, asset: &AssetType) -> TransactionsCRUD<'_> {
        TransactionsCRUD::from_exec(asset, self.exec())
    }

    fn stock_transactions(&mut self) -> StockTransactionsCRUD<'_> {
        StockTransactionsCRUD::new(self.exec())
    }

    fn option_transactions(&mut self) -> OptionTransactionsCRUD<'_> {
        OptionTransactionsCRUD::new(self.exec())
    }

    // ── historical data ────────────────────────────────────────────────
    fn historical_data(&mut self, asset: &AssetType) -> HistoricalDataCRUD<'_> {
        HistoricalDataCRUD::from_exec(asset, self.exec())
    }

    fn historical_stock_data(&mut self) -> HistoricalStockDataCRUD<'_> {
        HistoricalStockDataCRUD::new(self.exec())
    }

    fn historical_options_data(&mut self) -> HistoricalOptionsDataCRUD<'_> {
        HistoricalOptionsDataCRUD::new(self.exec())
    }

    fn historical_forex_data(&mut self) -> HistoricalForexDataCRUD<'_> {
        HistoricalForexDataCRUD::new(self.exec())
    }
}

impl<T: DbConn + ?Sized> DbTables for T {}
