use chrono::{DateTime, Utc};
// required for ExtractPrimaryKeys, ExtractUpdateKeys, ExtractFullKeys
use crud_insertable::Insertable;
use crud_insertable_macro::DeriveInsertable;
use crud_models::{
    ExtractFullKeys, ExtractPrimaryKeys, ExtractPrimaryKeysWoTime, ExtractUpdateKeys,
};
use ibapi::prelude::SecurityType;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use sqlx::query::Query;
use sqlx::{Postgres, postgres::PgArguments, query::QueryAs};
use std::fmt::{self, Display};
use std::hash::Hash;
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};

// Enums
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "status", rename_all = "lowercase")]
pub enum Status {
    Active,
    Stopping,
    Inactive,
}

#[derive(Eq, Hash, PartialEq, Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "asset_type", rename_all = "lowercase")]
pub enum AssetType {
    Stock,
    Future,
    Option,
    ForexPair,
    CFD,
    CASH,
    Unknown,
}

#[derive(Eq, Hash, PartialEq, Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "option_type")]
pub enum OptionType {
    #[sqlx(rename = "C")]
    Call,
    #[sqlx(rename = "P")]
    Put,
}

impl ToSql for OptionType {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let val = match self {
            OptionType::Call => "C",
            OptionType::Put => "P",
        };
        // Delegate binary encoding to standard &str implementation
        val.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        // Accept standard text/varchar/char Postgres types, or custom enum type if defined
        matches!(ty.kind(), tokio_postgres::types::Kind::Simple)
    }

    to_sql_checked!();
}

#[derive(Debug, Clone)]
pub enum ExecutionSide {
    Bought,
    Sold,
}

impl ExecutionSide {
    pub fn from_str(side: &str) -> ExecutionSide {
        match side {
            "BOT" => ExecutionSide::Bought,
            "SLD" => ExecutionSide::Sold,
            _ => panic!(
                "ExecutionSide from_str called with string that is not BOT/SLD: {}",
                side
            ),
        }
    }
}

impl AssetType {
    /// NOTE: this is a different from_str from typical fmt::from_str
    /// Accepts ibapi's SecurityType and converts it to the local AssetType
    pub fn from_str(security_type: &SecurityType) -> AssetType {
        match security_type {
            SecurityType::Stock => AssetType::Stock,
            SecurityType::Future => AssetType::Future,
            SecurityType::ForexPair => AssetType::ForexPair,
            SecurityType::CFD => AssetType::CFD,
            SecurityType::Option => AssetType::Option,
            _ => {
                tracing::warn!(
                    "Unknown Security Type being parsed for AssetType: {security_type:?}",
                );
                AssetType::Unknown
            }
        }
    }
}

impl Display for AssetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            AssetType::Stock => write!(f, "stock"),
            AssetType::Future => write!(f, "future"),
            AssetType::CFD => write!(f, "cfd"),
            AssetType::Option => write!(f, "option"),
            AssetType::ForexPair => write!(f, "forex_pair"),
            AssetType::CASH => write!(f, "cash_asset"),
            AssetType::Unknown => write!(f, "unknown"),
        }
    }
}

impl OptionType {
    pub fn from_str(right: &str) -> Result<OptionType, String> {
        match right
            .chars()
            .next()
            .expect("Expected Option Right passed to OptionType to have String of len > 0")
        {
            'P' => Ok(OptionType::Put),
            'C' => Ok(OptionType::Call),
            _ => Err(format!("Unknown Option Right passed: {}", right)),
        }
    }
}

impl fmt::Display for OptionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            OptionType::Call => "C",
            OptionType::Put => "P",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MismatchedPosition {
    pub strategy: String,
    pub broker: f64,
    pub local: f64,
    pub fix: f64,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct Notification {
    pub title: String,
    pub body: Option<String>,
    pub alert_type: Option<String>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct Strategy {
    pub strategy: String,
    pub status: Option<Status>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct CurrentStockPositions {
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
    pub strategy: String,
    pub quantity: Option<f64>,
    pub avg_price: Option<f64>,
    pub last_updated: Option<chrono::DateTime<Utc>>, // pub stop_limit: Option<f64>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct CurrentOptionPositions {
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
    pub strategy: String,
    pub expiry: String,
    pub strike: f64,
    pub multiplier: String,
    pub option_type: OptionType,
    pub quantity: Option<f64>,
    pub avg_price: Option<f64>,
    pub last_updated: Option<chrono::DateTime<Utc>>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct TargetStockPositions {
    pub strategy: String,
    pub primary_exchange: String,
    pub currency: String,
    pub stock: String,
    pub avg_price: Option<f64>,
    pub quantity: Option<f64>,
    // pub stop_limit: Option<f64>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct TargetOptionPositions {
    pub strategy: String,
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
    pub expiry: String,
    pub strike: f64,
    pub multiplier: String,
    pub option_type: OptionType,
    pub avg_price: Option<f64>,
    pub quantity: Option<f64>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct OpenStockOrders {
    pub order_perm_id: i32,
    pub order_id: i32,
    pub strategy: Option<String>,
    pub stock: Option<String>,
    pub primary_exchange: Option<String>,
    pub currency: Option<String>,
    pub time: Option<DateTime<Utc>>,
    pub quantity: Option<f64>,

    pub executions: Option<Vec<String>>,
    pub filled: Option<f64>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct OpenOptionOrders {
    pub order_perm_id: i32,
    pub order_id: i32,
    pub strategy: Option<String>,
    pub stock: Option<String>,
    pub primary_exchange: Option<String>,
    pub currency: Option<String>,
    pub expiry: Option<String>,
    pub strike: Option<f64>,
    pub multiplier: Option<String>,
    pub option_type: Option<OptionType>,
    pub time: Option<DateTime<Utc>>,
    pub quantity: Option<f64>,

    pub executions: Option<Vec<String>>,
    pub filled: Option<f64>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct StockTransactions {
    pub execution_id: String,
    pub strategy: Option<String>,
    pub stock: Option<String>,
    pub primary_exchange: Option<String>,
    pub currency: Option<String>,
    pub order_perm_id: Option<i32>,
    pub time: Option<DateTime<Utc>>,
    pub price: Option<f64>,
    pub quantity: Option<f64>,
    pub fees: Option<Decimal>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct OptionTransactions {
    pub execution_id: String,
    pub strategy: Option<String>,
    pub stock: Option<String>,
    pub primary_exchange: Option<String>,
    pub currency: Option<String>,
    pub expiry: Option<String>,
    pub strike: Option<f64>,
    pub multiplier: Option<String>,
    pub option_type: Option<OptionType>,
    pub order_perm_id: Option<i32>,
    pub time: Option<DateTime<Utc>>,
    pub price: Option<f64>,
    pub quantity: Option<f64>,
    pub fees: Option<rust_decimal::Decimal>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct StagedCommissions {
    pub execution_id: String,
    pub fees: Option<Decimal>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractPrimaryKeysWoTime,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct HistoricalStockData {
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
    pub time: DateTime<Utc>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume: Option<Decimal>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractPrimaryKeysWoTime,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct DailyHistoricalStockData {
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
    pub time: DateTime<Utc>,
    pub open: Option<Decimal>,
    pub high: Option<Decimal>,
    pub low: Option<Decimal>,
    pub close: Option<Decimal>,
    pub volume: Option<Decimal>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractPrimaryKeys,
    ExtractPrimaryKeysWoTime,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct HistoricalForexData {
    pub pair: String, // e.g., "EUR/USD"
    pub time: DateTime<Utc>,
    pub bid_open: Option<f64>,
    pub bid_high: Option<f64>,
    pub bid_low: Option<f64>,
    pub bid_close: Option<f64>,
    pub ask_open: Option<f64>,
    pub ask_high: Option<f64>,
    pub ask_low: Option<f64>,
    pub ask_close: Option<f64>,
    // pub is_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, DeriveInsertable, FromRow)]
pub struct HistoricalForexDataFullKeys {
    pub pair: String, // e.g., "EUR/USD"
    pub time: DateTime<Utc>,
    pub bid_open: Option<f64>,
    pub bid_high: Option<f64>,
    pub bid_low: Option<f64>,
    pub bid_close: Option<f64>,
    pub ask_open: Option<f64>,
    pub ask_high: Option<f64>,
    pub ask_low: Option<f64>,
    pub ask_close: Option<f64>,
    // pub is_complete: bool,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct HistoricalVolatilityData {
    pub stock: String,
    pub time: DateTime<Utc>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractPrimaryKeysWoTime,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct HistoricalOptionsData {
    pub stock: String,
    pub primary_exchange: String,
    pub currency: String,
    pub expiry: String,
    pub strike: f64,
    pub multiplier: String,
    pub option_type: OptionType,
    pub time: DateTime<Utc>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume: Option<Decimal>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct Logs {
    pub time: DateTime<Utc>,
    pub level: String,
    pub name: String,
    pub message: Option<String>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
pub struct FractionalMomentumWeeklyPositions {
    pub stock: String,
    pub primary_exchange: String,
    pub quantity: Option<f64>,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    FromRow,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
)]
pub struct PhantomPortfolioValue {
    pub time: DateTime<Utc>,
    pub cash_portfolio_value: Option<f64>,
    pub option_portfolio_value: Option<f64>,
    pub bought_price: Option<f64>,
    pub strike: Option<f64>,
    pub peak: Option<f64>,
    pub paused: Option<bool>,
    pub resume_trades: Option<i32>,
}
