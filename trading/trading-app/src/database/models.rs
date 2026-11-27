//! Row and key types for the `trading`, `logs` and `market_data` schemas.
//!
//! These structs are the SQL shapes used by `models_crud` and the generic
//! CRUD layer. The derive macros generate the companion `*FullKeys`,
//! `*PrimaryKeys` and `*UpdateKeys` types: non-`Option` source fields become
//! required insert columns and primary-key fields, `Option` fields become
//! nullable columns and partial-update fields. `logs` and `notifications`
//! are hand-written because their identity `id` must be omitted on insert.

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "status", rename_all = "lowercase")]
pub enum Status {
    /// Strategy is trading.
    Active,
    /// Strategy is winding down and should not open new positions.
    Stopping,
    /// Strategy is not trading.
    Inactive,
}

#[derive(Eq, Hash, PartialEq, Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "asset_type", rename_all = "lowercase")]
pub enum AssetType {
    /// US equity.
    Stock,
    /// Futures contract.
    Future,
    /// Listed option.
    Option,
    /// FX pair (IDEALPRO).
    ForexPair,
    /// Contract for difference.
    CFD,
    /// Cash pseudo-asset used for currency rows.
    CASH,
    /// Unrecognized or unsupported security type.
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

/// Binary encoding: writes the right as a single `C`/`P` text value.
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

#[derive(Debug, PartialEq, Clone)]
pub enum ExecutionSide {
    /// Buy-side execution (`BOT`).
    Bought,
    /// Sell-side execution (`SLD`).
    Sold,
}

/// Conversion from IBKR's side strings.
impl ExecutionSide {
    /// Parses IBKR's `BOT`/`SLD`; panics on any other value.
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

/// Conversions between IBKR/string forms and the local asset class.
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

    /// Parses the lowercase wire form used by the API; panics on an unknown value.
    ///
    /// # Example
    ///
    /// ```
    /// use trading_app::database::models::AssetType;
    ///
    /// assert_eq!(AssetType::from_string("stock"), AssetType::Stock);
    /// assert_eq!(AssetType::from_string("option"), AssetType::Option);
    /// assert_eq!(AssetType::from_string("cash"), AssetType::CASH);
    /// ```
    pub fn from_string(asset_type: &str) -> Self {
        match asset_type {
            "stock" => Self::Stock,
            "future" => Self::Future,
            "option" => Self::Option,
            "forexPair" => Self::ForexPair,
            "cfd" => Self::CFD,
            "cash" => Self::CASH,
            "unknown" => Self::Unknown,
            _ => panic!("Can't parse AssetType from string"),
        }
    }
}

/// Lowercase wire spelling (`forex_pair`, `cash_asset`).
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

/// Construction from option-right strings.
impl OptionType {
    /// Parses the IBKR string form; an unknown right is a descriptive `Err`.
    ///
    /// # Example
    ///
    /// ```
    /// use trading_app::database::models::OptionType;
    ///
    /// assert_eq!(OptionType::from_str("C").unwrap(), OptionType::Call);
    /// assert_eq!(OptionType::from_str("P").unwrap(), OptionType::Put);
    /// assert!(OptionType::from_str("X").is_err());
    /// ```
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

/// Single-letter `C`/`P` spelling.
impl fmt::Display for OptionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            OptionType::Call => "C",
            OptionType::Put => "P",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct MismatchedPosition {
    /// Owning strategy name; FK to `trading.strategy` (open orders store the verbatim `order_ref`).
    pub strategy: String,
    /// Quantity held at the broker.
    pub broker: f64,
    /// Quantity recorded locally.
    pub local: f64,
    /// Additive quantity that reconciles local to broker.
    pub fix: f64,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    DeriveInsertable,
    FromRow,
)]
/// A user-facing notification row (append-only; the table owns the identity id).
pub struct Notification {
    /// Notification title.
    pub title: String,
    /// Notification body.
    pub body: Option<String>,
    /// Notification category used by the front end.
    pub alert_type: Option<String>,
}

/// Hand-written (not derived) because `trading.notifications.id` is a
/// `GENERATED ALWAYS AS IDENTITY` column: the insert must omit it, so the
/// full key carries `Option<i64>` (`None` on insert, `Some` on read).
/// `body`/`alert_type` are NOT NULL in the table, so they are required here.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, FromRow, DeriveInsertable)]
pub struct NotificationFullKeys {
    /// Database identity value; `None` when constructing a row for insert.
    pub id: Option<i64>,
    /// Notification title.
    pub title: String,
    /// Notification body.
    pub body: String,
    /// Notification category used by the front end.
    pub alert_type: String,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, FromRow, DeriveInsertable)]
pub struct NotificationPrimaryKeys {
    /// Database identity value; `None` when constructing a row for insert.
    pub id: i64,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, FromRow, DeriveInsertable)]
pub struct NotificationUpdateKeys {
    /// Notification title.
    pub title: Option<String>,
    /// Notification body.
    pub body: Option<String>,
    /// Notification category used by the front end.
    pub alert_type: Option<String>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// A registered strategy; the FK target for positions, orders and transactions.
pub struct Strategy {
    /// Owning strategy name; FK to `trading.strategy` (open orders store the verbatim `order_ref`).
    pub strategy: String,
    /// Strategy lifecycle state.
    pub status: Option<Status>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// Current stock position per (stock, exchange, currency, strategy).
pub struct CurrentStockPositions {
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: String,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: String,
    /// Trading currency; cash rows use the account currency (SGD).
    pub currency: String,
    /// Owning strategy name; FK to `trading.strategy` (open orders store the verbatim `order_ref`).
    pub strategy: String,
    /// Signed quantity: positive long, negative short.
    pub quantity: Option<f64>,
    /// Average entry price in `currency`.
    pub avg_price: Option<f64>,
    /// Timestamp of the last write to this position row.
    pub last_updated: Option<chrono::DateTime<Utc>>, // pub stop_limit: Option<f64>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// Current option position per (contract, strategy).
pub struct CurrentOptionPositions {
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: String,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: String,
    /// Trading currency; cash rows use the account currency (SGD).
    pub currency: String,
    /// Owning strategy name; FK to `trading.strategy` (open orders store the verbatim `order_ref`).
    pub strategy: String,
    /// Option expiry as `YYYYMMDD`.
    pub expiry: String,
    /// Option strike price.
    pub strike: f64,
    /// Contract multiplier (for example `"100"` for equity options).
    pub multiplier: String,
    /// Option right (`C`/`P` in the database).
    pub option_type: OptionType,
    /// Signed quantity: positive long, negative short.
    pub quantity: Option<f64>,
    /// Average entry price in `currency`.
    pub avg_price: Option<f64>,
    /// Timestamp of the last write to this position row.
    pub last_updated: Option<chrono::DateTime<Utc>>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// Desired stock position per (stock, exchange, currency, strategy).
pub struct TargetStockPositions {
    /// Owning strategy name; FK to `trading.strategy` (open orders store the verbatim `order_ref`).
    pub strategy: String,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: String,
    /// Trading currency; cash rows use the account currency (SGD).
    pub currency: String,
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: String,
    /// Average entry price in `currency`.
    pub avg_price: Option<f64>,
    /// Signed quantity: positive long, negative short.
    pub quantity: Option<f64>,
    // pub stop_limit: Option<f64>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// Desired option position per (contract, strategy).
pub struct TargetOptionPositions {
    /// Owning strategy name; FK to `trading.strategy` (open orders store the verbatim `order_ref`).
    pub strategy: String,
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: String,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: String,
    /// Trading currency; cash rows use the account currency (SGD).
    pub currency: String,
    /// Option expiry as `YYYYMMDD`.
    pub expiry: String,
    /// Option strike price.
    pub strike: f64,
    /// Contract multiplier (for example `"100"` for equity options).
    pub multiplier: String,
    /// Option right (`C`/`P` in the database).
    pub option_type: OptionType,
    /// Average entry price in `currency`.
    pub avg_price: Option<f64>,
    /// Signed quantity: positive long, negative short.
    pub quantity: Option<f64>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// Working stock order mirrored from IBKR (includes the optimistic `perm_id = -1` row).
pub struct OpenStockOrders {
    /// IBKR permanent order id; `-1` on the optimistic row before acknowledgement.
    pub order_perm_id: i32,
    /// IBKR order id assigned when the order was placed.
    pub order_id: i32,
    /// Owning strategy name; FK to `trading.strategy` (open orders store the verbatim `order_ref`).
    pub strategy: Option<String>,
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: Option<String>,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: Option<String>,
    /// Trading currency; cash rows use the account currency (SGD).
    pub currency: Option<String>,
    /// Event/bar timestamp in UTC.
    pub time: Option<DateTime<Utc>>,
    /// Signed quantity: positive long, negative short.
    pub quantity: Option<f64>,

    /// IBKR parent order id for bracket/attached orders; `0` when none.
    pub parent_order_id: Option<i32>,
    /// Execution ids already applied to this open order (deduplication set).
    pub executions: Option<Vec<String>>,
    /// Cumulative filled quantity reported by IBKR.
    pub filled: Option<f64>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// Working option order mirrored from IBKR (includes the optimistic `perm_id = -1` row).
pub struct OpenOptionOrders {
    /// IBKR permanent order id; `-1` on the optimistic row before acknowledgement.
    pub order_perm_id: i32,
    /// IBKR order id assigned when the order was placed.
    pub order_id: i32,
    /// Owning strategy name; FK to `trading.strategy` (open orders store the verbatim `order_ref`).
    pub strategy: Option<String>,
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: Option<String>,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: Option<String>,
    /// Trading currency; cash rows use the account currency (SGD).
    pub currency: Option<String>,
    /// Option expiry as `YYYYMMDD`.
    pub expiry: Option<String>,
    /// Option strike price.
    pub strike: Option<f64>,
    /// Contract multiplier (for example `"100"` for equity options).
    pub multiplier: Option<String>,
    /// Option right (`C`/`P` in the database).
    pub option_type: Option<OptionType>,
    /// Event/bar timestamp in UTC.
    pub time: Option<DateTime<Utc>>,
    /// Signed quantity: positive long, negative short.
    pub quantity: Option<f64>,

    /// IBKR parent order id for bracket/attached orders; `0` when none.
    pub parent_order_id: Option<i32>,
    /// Execution ids already applied to this open order (deduplication set).
    pub executions: Option<Vec<String>>,
    /// Cumulative filled quantity reported by IBKR.
    pub filled: Option<f64>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// A realized stock fill.
pub struct StockTransactions {
    /// IBKR execution id; the idempotency key for a fill.
    pub execution_id: String,
    /// Owning strategy name; FK to `trading.strategy` (open orders store the verbatim `order_ref`).
    pub strategy: Option<String>,
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: Option<String>,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: Option<String>,
    /// Trading currency; cash rows use the account currency (SGD).
    pub currency: Option<String>,
    /// IBKR permanent order id; `-1` on the optimistic row before acknowledgement.
    pub order_perm_id: Option<i32>,
    /// Event/bar timestamp in UTC.
    pub time: Option<DateTime<Utc>>,
    /// Execution or bar price in `currency`.
    pub price: Option<f64>,
    /// Signed quantity: positive long, negative short.
    pub quantity: Option<f64>,
    /// Commission and fees charged on the execution (exact decimal).
    pub fees: Option<Decimal>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// A realized option fill.
pub struct OptionTransactions {
    /// IBKR execution id; the idempotency key for a fill.
    pub execution_id: String,
    /// Owning strategy name; FK to `trading.strategy` (open orders store the verbatim `order_ref`).
    pub strategy: Option<String>,
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: Option<String>,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: Option<String>,
    /// Trading currency; cash rows use the account currency (SGD).
    pub currency: Option<String>,
    /// Option expiry as `YYYYMMDD`.
    pub expiry: Option<String>,
    /// Option strike price.
    pub strike: Option<f64>,
    /// Contract multiplier (for example `"100"` for equity options).
    pub multiplier: Option<String>,
    /// Option right (`C`/`P` in the database).
    pub option_type: Option<OptionType>,
    /// IBKR permanent order id; `-1` on the optimistic row before acknowledgement.
    pub order_perm_id: Option<i32>,
    /// Event/bar timestamp in UTC.
    pub time: Option<DateTime<Utc>>,
    /// Execution or bar price in `currency`.
    pub price: Option<f64>,
    /// Signed quantity: positive long, negative short.
    pub quantity: Option<f64>,
    /// Commission and fees charged on the execution (exact decimal).
    pub fees: Option<rust_decimal::Decimal>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// Commission staged until the matching transaction exists (merged by DB triggers).
pub struct StagedCommissions {
    /// IBKR execution id; the idempotency key for a fill.
    pub execution_id: String,
    /// Commission and fees charged on the execution (exact decimal).
    pub fees: Option<Decimal>,
}

#[derive(
    Debug, PartialEq,
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
/// A stock OHLCV bar.
pub struct HistoricalStockData {
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: String,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: String,
    /// Trading currency; cash rows use the account currency (SGD).
    pub currency: String,
    /// Event/bar timestamp in UTC.
    pub time: DateTime<Utc>,
    /// Bar open.
    pub open: Option<f64>,
    /// Bar high.
    pub high: Option<f64>,
    /// Bar low.
    pub low: Option<f64>,
    /// Bar close.
    pub close: Option<f64>,
    /// Bar volume (exact decimal; may exceed f64 precision).
    pub volume: Option<Decimal>,
}

#[derive(
    Debug, PartialEq,
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
/// A daily stock OHLCV bar (legacy daily path).
pub struct DailyHistoricalStockData {
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: String,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: String,
    /// Trading currency; cash rows use the account currency (SGD).
    pub currency: String,
    /// Trading day for a daily bar.
    pub day: DateTime<Utc>,
    /// Bar open.
    pub open: Option<f64>,
    /// Bar high.
    pub high: Option<f64>,
    /// Bar low.
    pub low: Option<f64>,
    /// Bar close.
    pub close: Option<f64>,
    /// Bar volume (exact decimal; may exceed f64 precision).
    pub volume: Option<Decimal>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractPrimaryKeys,
    ExtractPrimaryKeysWoTime,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// A forex bid/ask OHLC bar.
pub struct HistoricalForexData {
    /// FX pair symbol (for example `EUR/USD`).
    pub pair: String, // e.g., "EUR/USD"
    /// Event/bar timestamp in UTC.
    pub time: DateTime<Utc>,
    /// Bid open.
    pub bid_open: Option<f64>,
    /// Bid high.
    pub bid_high: Option<f64>,
    /// Bid low.
    pub bid_low: Option<f64>,
    /// Bid close.
    pub bid_close: Option<f64>,
    /// Ask open.
    pub ask_open: Option<f64>,
    /// Ask high.
    pub ask_high: Option<f64>,
    /// Ask low.
    pub ask_low: Option<f64>,
    /// Ask close.
    pub ask_close: Option<f64>,
    // pub is_complete: bool,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, DeriveInsertable, FromRow)]
pub struct HistoricalForexDataFullKeys {
    /// FX pair symbol (for example `EUR/USD`).
    pub pair: String, // e.g., "EUR/USD"
    /// Event/bar timestamp in UTC.
    pub time: DateTime<Utc>,
    /// Bid open.
    pub bid_open: Option<f64>,
    /// Bid high.
    pub bid_high: Option<f64>,
    /// Bid low.
    pub bid_low: Option<f64>,
    /// Bid close.
    pub bid_close: Option<f64>,
    /// Ask open.
    pub ask_open: Option<f64>,
    /// Ask high.
    pub ask_high: Option<f64>,
    /// Ask low.
    pub ask_low: Option<f64>,
    /// Ask close.
    pub ask_close: Option<f64>,
    // pub is_complete: bool,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// A historical volatility bar.
pub struct HistoricalVolatilityData {
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: String,
    /// Event/bar timestamp in UTC.
    pub time: DateTime<Utc>,
    /// Bar open.
    pub open: Option<f64>,
    /// Bar high.
    pub high: Option<f64>,
    /// Bar low.
    pub low: Option<f64>,
    /// Bar close.
    pub close: Option<f64>,
}

#[derive(
    Debug, PartialEq,
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
/// An option OHLCV bar.
pub struct HistoricalOptionsData {
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: String,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: String,
    /// Trading currency; cash rows use the account currency (SGD).
    pub currency: String,
    /// Option expiry as `YYYYMMDD`.
    pub expiry: String,
    /// Option strike price.
    pub strike: f64,
    /// Contract multiplier (for example `"100"` for equity options).
    pub multiplier: String,
    /// Option right (`C`/`P` in the database).
    pub option_type: OptionType,
    /// Event/bar timestamp in UTC.
    pub time: DateTime<Utc>,
    /// Bar open.
    pub open: Option<f64>,
    /// Bar high.
    pub high: Option<f64>,
    /// Bar low.
    pub low: Option<f64>,
    /// Bar close.
    pub close: Option<f64>,
    /// Bar volume (exact decimal; may exceed f64 precision).
    pub volume: Option<Decimal>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    DeriveInsertable,
    FromRow,
)]
/// An application log row (append-only; the table owns the identity id).
pub struct Logs {
    /// Event/bar timestamp in UTC.
    pub time: DateTime<Utc>,
    /// Log severity (`INFO`, `WARN`, `ERROR`, ...).
    pub level: String,
    /// Component or strategy that emitted the log line.
    pub name: String,
    /// Log message body.
    pub message: Option<String>,
}

/// Hand-written (not derived) because `logs.logs.id` is a
/// `GENERATED ALWAYS AS IDENTITY` column: the insert must omit it, so the
/// full key carries `Option<i64>` (`None` on insert, `Some` on read).
/// `id` is the only addressing key; the table's PK is `(time, id)`.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, FromRow, DeriveInsertable)]
pub struct LogsFullKeys {
    /// Database identity value; `None` when constructing a row for insert.
    pub id: Option<i64>,
    /// Event/bar timestamp in UTC.
    pub time: DateTime<Utc>,
    /// Log severity (`INFO`, `WARN`, `ERROR`, ...).
    pub level: String,
    /// Component or strategy that emitted the log line.
    pub name: String,
    /// Log message body.
    pub message: String,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, FromRow, DeriveInsertable)]
pub struct LogsPrimaryKeys {
    /// Database identity value; `None` when constructing a row for insert.
    pub id: i64,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, FromRow, DeriveInsertable)]
pub struct LogsUpdateKeys {
    /// Event/bar timestamp in UTC.
    pub time: Option<DateTime<Utc>>,
    /// Log severity (`INFO`, `WARN`, `ERROR`, ...).
    pub level: Option<String>,
    /// Component or strategy that emitted the log line.
    pub name: Option<String>,
    /// Log message body.
    pub message: Option<String>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// Audit row for an order cancelled by the engine.
pub struct CancelledOrders {
    /// Event/bar timestamp in UTC.
    pub time: DateTime<Utc>,
    /// IBKR permanent order id; `-1` on the optimistic row before acknowledgement.
    pub order_perm_id: i32,
    /// IBKR order id assigned when the order was placed.
    pub order_id: i32,

    /// Owning strategy name; FK to `trading.strategy` (open orders store the verbatim `order_ref`).
    pub strategy: Option<String>,
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: Option<String>,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: Option<String>,
    /// Trading currency; cash rows use the account currency (SGD).
    pub currency: Option<String>,

    /// Signed quantity: positive long, negative short.
    pub quantity: Option<f64>,
    /// Execution ids already applied to this open order (deduplication set).
    pub executions: Option<Vec<String>>,
    /// Cumulative filled quantity reported by IBKR.
    pub filled: Option<f64>,

    /// Cancellation reason reported by IBKR/the engine.
    pub reason: Option<String>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
    FromRow,
)]
/// Weekly per-stock position used by the fractional-momentum strategy.
pub struct FractionalMomentumWeeklyPositions {
    /// Underlying symbol; `CASH:<currency>` marks a cash row.
    pub stock: String,
    /// Listing exchange (for example `NASDAQ`); empty for FX and cash rows.
    pub primary_exchange: String,
    /// Signed quantity: positive long, negative short.
    pub quantity: Option<f64>,
}

#[derive(
    Debug, PartialEq,
    Clone,
    Serialize,
    Deserialize,
    FromRow,
    ExtractFullKeys,
    ExtractPrimaryKeys,
    ExtractUpdateKeys,
    DeriveInsertable,
)]
/// Synthetic portfolio-value tracking for the phantom/paper overlay.
pub struct PhantomPortfolioValue {
    /// Event/bar timestamp in UTC.
    pub time: DateTime<Utc>,
    /// Cash component of the synthetic portfolio value.
    pub cash_portfolio_value: Option<f64>,
    /// Option component of the synthetic portfolio value.
    pub option_portfolio_value: Option<f64>,
    /// Reference purchase price of the tracked position.
    pub bought_price: Option<f64>,
    /// Option strike price.
    pub strike: Option<f64>,
    /// Peak value observed for the strategy.
    pub peak: Option<f64>,
    /// Whether trading is currently paused.
    pub paused: Option<bool>,
    /// Counter/threshold used to resume trading.
    pub resume_trades: Option<i32>,
}
