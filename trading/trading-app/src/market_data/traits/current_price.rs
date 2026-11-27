//! Price reads and historical-bar population.
//!
//! [`PriceSupplier`] is the port used everywhere a price is needed (fund gates,
//! strategy values, the HTTP server, backtests). The live implementation on
//! [`Consolidator`] resolves a price in this order:
//!
//! 1. the live cache from an active subscription (`try_get_price`, 60 s TTL),
//!    skipped for VWAP requests;
//! 2. `symbol == currency` → `1.0` (a currency pricing itself);
//! 3. a memoised IBKR snapshot (`GetPrice`, 15-minute TTL).
//!
//! Inside a snapshot request it retries up to 10 times with a 5 s wait, treats
//! error 10167 (delayed data confirmed) as informational, falls back to delayed
//! data once, inverts an FX pair once if IBKR has no access, and finally uses
//! Yahoo Finance when the error indicates a permission/subscription problem.
//! The client is always switched back to real-time data before returning.
//!
//! [`Consolidator::populate_historical_data`] is the write side used by the
//! freshness checks: it requests bars from IBKR, drops the still-forming newest
//! bucket, and upserts the rest (optionally through the batch channel), fetching
//! both bid and ask for FX.

use crate::database::tables::DbTables;
use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Datelike, Utc};
use ibapi::{
    Client,
    client::Subscription,
    market_data::MarketDataType,
    prelude::{Contract, SecurityType, TickTypes},
};
use rust_decimal::str;
use yfinance_rs::{Ticker, YfClient};

use crate::{
    database::{
        crud::CRUDTrait,
        models::{
            AssetType, HistoricalForexDataFullKeys, HistoricalOptionsDataFullKeys,
            HistoricalStockDataFullKeys,
        },
        models_crud::historical_data::{
            batch_operations::{BatchDbCreatorEnum, init_channel},
            historical_data::{
                HistoricalDataFullKeys, HistoricalDataPrimaryKeys, HistoricalDataUpdateKeys,
            },
        },
    },
    market_data::consolidator::{Consolidator, MemoisedConsolidatorFns},
};

#[allow(dead_code)]
const DUKASCOPY_BASE_URL: &str = "https://jetta.dukascopy.com/v1/candles/minute";

/// Request shape for [`PriceSupplier::populate_historical_data`].
///
/// `use_batching` selects the batch channel instead of per-row upserts; the
/// caller must have opened the channels for that asset type first.
pub struct HistoricalDataConfig {
    duration: ibapi::market_data::historical::Duration,
    bar_size: ibapi::market_data::historical::BarSize,
    what_to_show: ibapi::market_data::historical::WhatToShow,
    use_batching: bool,
}

impl HistoricalDataConfig {
    /// Builds a config from IBKR duration/bar-size/what-to-show plus the batching
    /// flag.
    pub fn new(
        duration: ibapi::market_data::historical::Duration,
        bar_size: ibapi::market_data::historical::BarSize,
        what_to_show: ibapi::market_data::historical::WhatToShow,
        use_batching: bool,
    ) -> Self {
        Self {
            duration,
            bar_size,
            what_to_show,
            use_batching,
        }
    }
}

/// Port for current and historical prices.
///
/// Implementations must be safe to call from synchronous strategy code;
/// `get_current_price` may block while it waits for a tick.
#[async_trait::async_trait]
pub trait PriceSupplier {
    /// Returns the current price for `contract`.
    ///
    /// `vwap = true` requests the VWAP tick; when set, `generic_ticks` is
    /// ignored (VWAP is requested as tick 233). `generic_ticks` may otherwise
    /// request specific tick types.
    ///
    /// # Errors
    ///
    /// Fails when no price can be obtained from any source (IBKR, delayed data,
    /// Yahoo fallback).
    fn get_current_price(
        &self,
        contract: Contract,
        vwap: bool,
        generic_ticks: &[&str],
    ) -> Result<f64, String>;

    /// Fetches historical bars for `contract` and upserts them.
    ///
    /// The newest, still-forming bucket is skipped. For FX pairs only the bid leg
    /// is requested here; the implementation then recurses for the ask leg.
    ///
    /// # Errors
    ///
    /// Fails when the IBKR request fails. Per-bar insert failures are logged and
    /// do not abort the remaining bars.
    #[cfg(not(feature = "backtest"))]
    async fn populate_historical_data(
        &self,
        contract: &Contract,
        config: &HistoricalDataConfig,
    ) -> Result<(), String>;
}

#[async_trait::async_trait]
impl PriceSupplier for Consolidator {
    /// Live: cache → self-priced currency → memoised snapshot; backtest: delegate
    /// to the replay supplier.
    ///
    /// The live shortcut `try_get_price` is only used for non-VWAP reads because
    /// the cache stores trade prices, not VWAP.
    fn get_current_price(
        &self,
        contract: Contract,
        vwap: bool,
        generic_ticks: &[&str],
    ) -> Result<f64, String> {
        #[cfg(not(feature = "backtest"))]
        {
            if !vwap {
                if let Some(price) = self.market_data_handler.try_get_price(contract.contract_id) {
                    return Ok(price);
                }
            }

            if contract.symbol.as_str() == contract.currency.as_str() {
                return Ok(1.0);
            }

            let entry = self
                .memoisers
                .get(&MemoisedConsolidatorFns::GetPrice)
                .expect("all MemoisedConsolidatorFns variants must be registered");

            let client = self.client.clone();
            let ticks_owned: Vec<String> = generic_ticks.iter().map(|s| s.to_string()).collect();

            // `contract` is already owned by this fn — no need to clone it,
            // just move it straight into the tuple.
            let result = entry.call_any(Box::new((client, contract, vwap, ticks_owned, false)))?;

            result
                .downcast::<f64>()
                .map(|v| *v)
                .map_err(|e| format!("AnyMemoized: return type mismatch for GetPrice: {e:?}"))
        }

        #[cfg(feature = "backtest")]
        self.price_supplier
            .get_current_price(contract, vwap, generic_ticks)
    }

    // If is_forex is true, what_to_show is ignored - takes both bid and ask
    /// Requests bars from IBKR and upserts every complete bucket.
    ///
    /// The bar interval is 60 s for FX and 300 s otherwise; the bucket containing
    /// the request time is skipped because it is still forming. With
    /// `use_batching`, rows go through the asset-specific batch channel.
    ///
    /// # Panics
    ///
    /// Panics when the contract resolves to `AssetType::CASH` or
    /// `AssetType::Unknown` (neither can have a historical table).
    ///
    /// # Errors
    ///
    /// Returns the IBKR/`async_call` error; insert failures are logged per bar.
    #[cfg(not(feature = "backtest"))]
    async fn populate_historical_data(
        &self,
        contract: &Contract,
        config: &HistoricalDataConfig,
    ) -> Result<(), String> {
        use crate::helpers::sync_timeout::async_call;

        let pool = self.pool.clone();
        let (cloned_client, cloned_contract, duration, bar_size, what_to_show) = (
            self.client.clone(),
            contract.clone(),
            config.duration.clone(),
            config.bar_size.clone(),
            config.what_to_show.clone(),
        );
        let historical_bars = async_call(move || {
            cloned_client
                .historical_data(
                    &cloned_contract,
                    None,
                    duration,
                    bar_size.clone(),
                    what_to_show.clone(),
                    ibapi::market_data::TradingHours::Regular,
                )
                .map_err(|e| {
                    format!(
                        "Expected Historical Data Request to TWS to succeed for {}: {}",
                        &cloned_contract.symbol.clone(),
                        e
                    )
                })
        })
        .await
        .map_err(|e| format!("Failed to fetch historical data: {e:?}"))?;

        let bar_interval = if contract.security_type == SecurityType::ForexPair {
            60
        } else {
            300
        };
        let latest_request_timestamp = Utc::now().timestamp();
        let latest_request_time_bar =
            latest_request_timestamp - (latest_request_timestamp % bar_interval);

        let batch_creator_opt = if config.use_batching {
            Some(match AssetType::from_str(&contract.security_type) {
                AssetType::Stock | AssetType::CFD | AssetType::Future => {
                    BatchDbCreatorEnum::Stock(init_channel::<HistoricalStockDataFullKeys>().await)
                }
                AssetType::Option => BatchDbCreatorEnum::Options(
                    init_channel::<HistoricalOptionsDataFullKeys>().await,
                ),
                AssetType::ForexPair => {
                    BatchDbCreatorEnum::Forex(init_channel::<HistoricalForexDataFullKeys>().await)
                }
                AssetType::CASH => {
                    panic!("Shouldn't be possible to get CASH")
                }
                AssetType::Unknown => {
                    panic!("Tried to init batch channel for Unknown Asset Type")
                }
            })
        } else {
            None
        };

        let asset_type = AssetType::from_str(&contract.security_type);
        let mut join_handles = Vec::new();
        for bar in historical_bars.bars {
            if bar.date.unix_timestamp() == latest_request_time_bar {
                continue;
            }
            let cloned_pool = pool.clone();
            let historical_data_fk =
                HistoricalDataFullKeys::from_contract_and_bar(&contract, &config.what_to_show, bar);
            let historical_data_pk = HistoricalDataPrimaryKeys::from_contract(
                &contract,
                DateTime::from_timestamp(bar.date.unix_timestamp(), bar.date.nanosecond() as u32)
                    .expect("Expected to be able to convert bar time to DateTime<Utc>"),
            );
            let historical_data_uk = HistoricalDataUpdateKeys::from_historical_bar(
                &contract,
                &config.what_to_show,
                &historical_data_fk,
            );

            let cloned_batch_creator_opt = batch_creator_opt.clone();
            let cloned_asset_type = asset_type.clone();
            let insert_thread = tokio::spawn(async move {
                match cloned_batch_creator_opt {
                    Some(batch_creator) => {
                        if let Err(e) = batch_creator
                            .batch_create_or_update(&historical_data_fk)
                            .await
                        {
                            tracing::error!("Failed to batch create or update: {e:?}");
                        };
                    }
                    None => {
                        let mut db = crate::database::db::DbDirect::connect(&cloned_pool);
                        if let Err(e) = db
                            .historical_data(&cloned_asset_type)
                            .create_or_update(&historical_data_pk, &historical_data_uk)
                            .await
                        {
                            tracing::error!("Failed to batch create or update: {e:?}");
                        };
                    }
                }
            });
            join_handles.push(insert_thread);
        }

        futures::future::join_all(join_handles).await;

        if asset_type == AssetType::ForexPair
            && what_to_show == ibapi::market_data::historical::WhatToShow::Bid
        {
            self.populate_historical_data(
                contract,
                &HistoricalDataConfig {
                    duration: config.duration,
                    bar_size: config.bar_size,
                    what_to_show: ibapi::market_data::historical::WhatToShow::Ask,
                    use_batching: config.use_batching,
                },
            )
            .await?;
        }

        Ok(())
    }
}

/// Outcome of attempting to extract a price from a single tick.
pub enum PriceExtraction {
    /// A usable price. Live and delayed ticks both arrive as `TickTypes::Price`
    /// — IBKR distinguishes them only via `tick_price.tick_type`
    /// (Bid/Ask/Last vs DelayedBid/DelayedAsk/DelayedLast); the wrapper and
    /// `price` field are identical either way, so no special-casing is needed.
    Price(f64),
    /// Non-fatal: TWS error code 10167, confirming the client is now
    /// receiving delayed data. The actual delayed price tick follows
    /// separately on this same subscription — caller should keep listening,
    /// not treat this as a failure or retry-budget consumer.
    DelayedDataConfirmed,
    /// Anything else: SnapshotEnd, RequestParameters, unrelated notices, etc.
    Err(String),
}

// ---------------------------------------------------------------------------
// Price helpers
// ---------------------------------------------------------------------------

impl Consolidator {
    /// Derives the Yahoo Finance ticker symbol from an IBKR Contract.
    ///
    /// Yahoo Finance uses suffixed tickers to identify non-US exchanges:
    ///   - No suffix   → US (NYSE/NASDAQ/SMART in USD)
    ///   - .T          → Tokyo Stock Exchange
    ///   - .KS / .KQ   → Korea Stock Exchange / KOSDAQ
    ///   - .L          → London Stock Exchange
    ///   - .HK         → Hong Kong Stock Exchange
    ///   - .SI         → Singapore Exchange
    ///   - .AX         → Australian Securities Exchange
    ///   - .PA / .DE / .MI / .MC / .AS → European exchanges
    ///
    /// Resolution order:
    ///   1. currency == USD and exchange is a US venue  → bare symbol, no suffix
    ///   2. exchange/primary_exchange mapped to known Yahoo suffix  → symbol.SUFFIX
    ///   3. Unmappable → None (disables fallback)
    ///
    ///   pub visibility ONLY for testing purposes
    pub fn yahoo_ticker_from_contract(contract: &Contract) -> Option<String> {
        let symbol = contract.symbol.as_str();
        let exchange = contract.exchange.as_str();
        let primary = contract.primary_exchange.as_str();
        let currency = contract.currency.as_str();

        // An option's Yahoo ticker is an OCC symbol, not the underlying's ticker.
        // Never fall back to the stock quote for an option.
        if contract.security_type == SecurityType::Option {
            tracing::warn!(
                "Cannot derive a Yahoo Finance ticker for option {}: \
                 options must be priced from their own contract.",
                symbol
            );
            return None;
        }

        // US exchanges: IBKR routes through SMART or names the actual venue.
        // Yahoo expects no suffix for US-listed stocks.
        const US_EXCHANGES: &[&str] = &[
            "SMART", "NYSE", "NASDAQ", "ARCA", "BATS", "CBOE", "AMEX", "ISLAND", "DRCTEDGE", "BEX",
            "NSX", "PSX", "IEX",
        ];

        let effective_exchange = if exchange == "SMART" {
            primary
        } else {
            exchange
        };

        if US_EXCHANGES.contains(&effective_exchange)
            || currency == "USD" && effective_exchange.is_empty()
        {
            return Some(symbol.to_string());
        }

        // Map IBKR exchange code → Yahoo Finance suffix
        let suffix = match effective_exchange {
            // Asia-Pacific
            "TSEJ" | "XTKS" => "T",  // Tokyo Stock Exchange
            "KSE" | "XKRX" => "KS",  // Korea Stock Exchange (KOSPI)
            "KOSDAQ" => "KQ",        // Korea KOSDAQ
            "SEHK" | "XHKG" => "HK", // Hong Kong
            "SGX" | "XSES" => "SI",  // Singapore
            "ASX" | "XASX" => "AX",  // Australia
            "NSE" | "XNSE" => "NS",  // India NSE
            "BSE" | "XBOM" => "BO",  // India BSE
            "TWSE" | "XTAI" => "TW", // Taiwan
            "XSHG" => "SS",          // Shanghai
            "XSHE" => "SZ",          // Shenzhen

            // Europe
            "LSE" | "XLON" => "L",            // London
            "IBIS" | "XETR" | "XFRA" => "DE", // Germany (XETRA / Frankfurt)
            "SBF" | "XPAR" => "PA",           // Paris (Euronext)
            "BVL" | "XLIS" => "LS",           // Lisbon
            "AEB" | "XAMS" => "AS",           // Amsterdam (Euronext)
            "BVME" | "XMIL" => "MI",          // Milan
            "BME" | "XMAD" => "MC",           // Madrid
            "SWX" | "XVTX" => "SW",           // Swiss Exchange
            "OMXS" | "XSTO" => "ST",          // Stockholm
            "OMXH" | "XHEL" => "HE",          // Helsinki
            "OMXC" | "XCSE" => "CO",          // Copenhagen
            "OSE" | "XOSL" => "OL",           // Oslo
            "VSE" | "XWBO" => "VI",           // Vienna
            "XBRU" => "BR",                   // Brussels (Euronext)
            "XPRA" => "PR",                   // Prague
            "XWAR" => "WA",                   // Warsaw
            "XBUD" => "BD",                   // Budapest

            // Canada
            "TSX" | "XTSE" => "TO", // Toronto
            "TSXV" | "XTSX" => "V", // TSX Venture
            "NEO" => "NE",          // NEO Exchange

            // Mexico / Brazil
            "MEXI" | "XMEX" => "MX",    // Mexico
            "BOVESPA" | "BVMF" => "SA", // Brazil B3

            _ => {
                tracing::warn!(
                    "Cannot derive Yahoo Finance ticker for contract {} on exchange '{}': \
                     no suffix mapping. yfinance fallback disabled.",
                    symbol,
                    effective_exchange
                );
                return None;
            }
        };

        Some(format!("{}.{}", symbol, suffix))
    }

    /// Fetches price from Yahoo Finance (yfinance-rs) as a synchronous fallback.
    /// Uses `fast_info().last_price` which is the simplest current-price path.
    /// The Yahoo ticker must already be in the correct format (e.g. "7203.T", "AAPL").
    /// Fetches `fast_info().last_price` from Yahoo Finance as a blocking fallback.
    ///
    /// Returns `Ok(None)` when Yahoo yields no last price. The caller must pass a
    /// ticker already in Yahoo form (see
    /// [`Consolidator::yahoo_ticker_from_contract`]).
    ///
    /// # Errors
    ///
    /// Propagates the yfinance error or a join failure.
    fn get_price_from_yfinance(
        yahoo_ticker: &str,
        handle: &tokio::runtime::Handle,
    ) -> Result<Option<f64>, String> {
        let yahoo_ticker = yahoo_ticker.to_string();

        let join_handle = handle.spawn(async move {
            let client = YfClient::default();
            let ticker = Ticker::new(&client, &yahoo_ticker);
            let fast_info = ticker
                .fast_info()
                .await
                .map_err(|e| format!("yfinance-rs fast_info failed for {yahoo_ticker}: {e:?}"))?;
            Ok(fast_info.last.map(|v| v.amount().as_f64()))
        });

        futures::executor::block_on(join_handle).map_err(|e| e.to_string())?
    }

    /// Returns true if the IBKR error string indicates a market data permission/access issue,
    /// as opposed to a connectivity or timeout problem.
    ///
    /// Detection is deliberately conservative: any of the text patterns
    /// "not subscribed", "market data farm" or "no security definition"
    /// matches, and numeric error codes are compared as whole tokens so `3540`
    /// and `100901` are not mistaken for `354` / `10090`.
    ///
    /// pub visibility ONLY for testing purposes
    pub fn is_ibkr_market_data_error(err: &str) -> bool {
        // IBKR error 354: "Requested market data is not subscribed"
        // IBKR error 10090: "Part of requested market data is not subscribed"
        // IBKR error 200: "No security definition has been found"
        let lower = err.to_lowercase();
        let text_patterns = [
            "not subscribed",
            "market data farm",
            "no security definition",
        ];
        if text_patterns.iter().any(|pattern| lower.contains(pattern)) {
            return true;
        }

        // Match whole numeric tokens, not substrings: "3540" and "100901" must
        // not be mistaken for 354 / 10090.
        err.split(|c: char| !c.is_ascii_digit())
            .filter(|token| !token.is_empty())
            .filter_map(|token| token.parse::<u32>().ok())
            .any(|code| matches!(code, 200 | 354 | 10090))
    }

    /// Helper function to extract the price of contract from the ticker received
    /// NOTE: this extracts the first price returned which may NOT be the actual correct data point
    /// - i.e. smth like requesting for ask -> may result in ticktype of high, open, ... to come
    /// first and is returned first
    ///
    /// Both `Price` and `PriceSize` ticks are accepted (IBKR sends either for a
    /// snapshot). `SnapshotEnd` cancels the subscription before returning the
    /// error. Notice `10167` maps to [`PriceExtraction::DelayedDataConfirmed`];
    /// every other notice is an error.
    ///
    /// pub visibility ONLY for testing purposes
    pub fn _extract_price(
        tick: TickTypes,
        contract: &Contract,
        subscription: &Subscription<TickTypes>,
    ) -> PriceExtraction {
        match tick {
            TickTypes::PriceSize(tick_price) => PriceExtraction::Price(tick_price.price),
            TickTypes::Price(tick_price) => PriceExtraction::Price(tick_price.price),
            TickTypes::SnapshotEnd => {
                subscription.cancel();
                PriceExtraction::Err(format!(
                    "Got SnapshotEnd from request for market data: {}",
                    contract.symbol
                ))
            }
            TickTypes::RequestParameters(_) => PriceExtraction::Err(format!(
                "Got RequestParameters ticker from request for mkt data: {}:",
                contract.symbol
            )),
            TickTypes::String(msg) => PriceExtraction::Err(format!(
                "Got string from request for market data: {}",
                msg.value
            )),
            // ── notices: disambiguate by numeric code, not message text ──
            // Message text overlaps between "you have no access" (fatal, e.g.
            // 10089) and "confirming delayed data is now streaming" (10167,
            // informational), so string-matching alone can't tell them apart.
            TickTypes::Notice(notice) if notice.code == 10167 => {
                PriceExtraction::DelayedDataConfirmed
            }
            TickTypes::Notice(notice) => PriceExtraction::Err(format!(
                "Got notice {} from request for market data: {}: {}",
                notice.code, contract.symbol, notice.message
            )),
            other => PriceExtraction::Err(format!(
                "Got unknown ticker from request for market data: {}: {:?}",
                contract.symbol, other
            )),
        }
    }

    /// Gets the current price of the contract from IBKR
    /// - if currently subscribed to their live data - unlocks and returns it
    ///     - Note: Each live_data subscription is wrapped behind a std::sync::Mutex so this
    ///     function could be potentially blocking for a longer period of time than expected
    /// - if requested the data in the last 20s, returns that
    /// - else, requests from IBKR, falling back to delayed data (if live is unavailable) and
    ///   then yfinance (if IBKR access is denied outright)
    ///
    /// Always switches the client's market data type back to Live before returning, regardless
    /// of whether the delayed-data fallback was triggered or the request succeeded/failed.
    ///
    /// - if you want to pass generic_ticks, vwap MUST be false
    ///
    /// VWAP requests are translated to generic tick `233` (the explicit
    /// `generic_ticks` list is then ignored). The reset to
    /// [`MarketDataType::Realtime`] happens even when the price lookup fails; a
    /// reset failure is only warned.
    pub(crate) fn _get_current_price(
        client: Arc<Client>,
        contract: &Contract,
        handle: &tokio::runtime::Handle,
        vwap: bool,
        generic_ticks: &[&str],
        is_second_try: bool,
    ) -> Result<f64, String> {
        let result = Self::_get_current_price_inner(
            client.clone(),
            contract,
            handle,
            vwap,
            generic_ticks,
            is_second_try,
            false,
        );

        if let Err(e) = client.switch_market_data_type(MarketDataType::Realtime) {
            tracing::warn!("Failed to switch market data type back to live: {e:?}");
        }

        result
    }

    /// Snapshot request loop: up to 10 ticks with a 5 s timeout each.
    ///
    /// Fallback rules:
    /// - one delayed-data retry, only when IBKR reports delayed data is available;
    /// - one FX inversion (swap symbol/currency, take the reciprocal), only when
    ///   access is denied for an FX pair and this is not already the second try;
    /// - otherwise, on an access error, Yahoo Finance (or `Err` when no ticker can
    ///   be derived).
    ///
    /// All failures across the attempts are accumulated into one newline-joined
    /// error.
    fn _get_current_price_inner(
        client: Arc<Client>,
        contract: &Contract,
        handle: &tokio::runtime::Handle,
        vwap: bool,
        generic_ticks: &[&str],
        is_second_try: bool,
        is_delayed_retry: bool,
    ) -> Result<f64, String> {
        let generic_ticks = if vwap { &["233"] } else { generic_ticks };

        let subscription = client
            .market_data(contract)
            .generic_ticks(generic_ticks)
            .snapshot()
            .subscribe()
            .map_err(|e| {
                tracing::error!("Failed to request current price from IBKR: {e:?}");
                format!("Failed to request current price from IBKR: {e:?}")
            })?;

        let mut cum_err = Vec::new();
        for attempt in 1..=10 {
            let Some(tick) = subscription.next_timeout(Duration::from_secs(5)) else {
                cum_err.push("Timed out!".to_string());
                continue;
            };

            let err = match Self::_extract_price(tick, contract, &subscription) {
                PriceExtraction::Price(price) => return Ok(price),
                PriceExtraction::DelayedDataConfirmed => {
                    tracing::debug!(
                        "Delayed market data confirmed for {}, awaiting tick",
                        contract.symbol
                    );
                    continue; // doesn't consume a retry, doesn't trip fallback logic
                }
                PriceExtraction::Err(e) => e,
            };

            // ── delayed-data fallback: only once, and only when live access is denied ──
            if !is_delayed_retry && err.contains("Delayed market data is available") {
                tracing::warn!(
                    "Live market data unavailable for {}, switching to delayed data: {err}",
                    contract.symbol
                );
                client
                    .switch_market_data_type(MarketDataType::Delayed)
                    .map_err(|e| format!("Failed to switch to delayed market data: {e:?}"))?;
                return Self::_get_current_price_inner(
                    client,
                    contract,
                    handle,
                    vwap,
                    generic_ticks,
                    is_second_try,
                    true,
                );
            }

            // ── yfinance fallback: only on access/subscription errors ──
            if Self::is_ibkr_market_data_error(&err) {
                if contract.security_type == SecurityType::ForexPair && !is_second_try {
                    return Self::_get_current_price_inner(
                        client,
                        &Contract {
                            symbol: contract.currency.to_string().into(),
                            currency: contract.symbol.to_string().into(),
                            ..contract.clone()
                        },
                        handle,
                        vwap,
                        generic_ticks,
                        true,
                        is_delayed_retry,
                    )
                    .map(|v| 1.0 / v);
                }

                return match Self::yahoo_ticker_from_contract(contract) {
                    Some(yt) => {
                        tracing::warn!(
                            "IBKR market data access denied for {} ({err}), falling back to yfinance-rs with ticker '{yt}'",
                            contract.symbol,
                        );
                        Self::get_price_from_yfinance(&yt, handle)
                            .map_err(|e| format!("yfinance error for {yt}: {e}"))?
                            .ok_or_else(|| format!("yfinance returned no price data for {yt}"))
                    }
                    None => Err("Failed to fetch data from yfinance".to_string()),
                };
            }

            cum_err.push(format!(
                "{attempt}th try for price data for {} failed due to {err}",
                contract.symbol
            ));
        }

        cum_err.push(format!(
            "Final try for price data for {} failed. Function has failed and is returning Error!",
            contract.symbol
        ));
        Err(cum_err.join("\n"))
    }

    /// Unused Dukascopy minute-candle fetcher kept for reference.
    ///
    /// Builds `.../<PAIR>/<SIDE>/<year>/<month>/<day>` from the pair name
    /// (stripping any `FX:` prefix and replacing `/` with `-`) and returns
    /// `(time, open, high, low, close)` rows.
    ///
    /// # Panics
    ///
    /// Panics on malformed payloads (missing `time`/OHLC fields) and on pairs
    /// without the expected `FX:` prefix.
    #[allow(unused)]
    async fn fetch_dukascopy_day(
        pair: &str, // "GBP/USD"
        side: &str, // "BID" or "ASK"
        date: &chrono::NaiveDate,
    ) -> anyhow::Result<Vec<(DateTime<Utc>, f64, f64, f64, f64)>> {
        let pair_dash = pair
            .replace("/", "-")
            .strip_prefix("FX:")
            .unwrap()
            .to_string();
        let url = format!(
            "{}/{}/{}/{}/{}/{}",
            DUKASCOPY_BASE_URL,
            pair_dash,
            side,
            date.year(),
            date.month(),
            date.day()
        );

        let client = reqwest::Client::new();
        let resp = client
            .get(url)
            .header("Accept", "application/json, text/plain, */*")
            .header("Origin", "https://widgets.dukascopy.com")
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<serde_json::Value>>()
            .await?;

        let day_start = date.and_hms_opt(0, 0, 0).unwrap().and_utc();

        let mut out = Vec::with_capacity(resp.len());

        for c in resp {
            let minutes = c["time"].as_i64().unwrap();
            let ts = day_start + chrono::Duration::minutes(minutes);

            out.push((
                ts,
                c["open"].as_f64().unwrap(),
                c["high"].as_f64().unwrap(),
                c["low"].as_f64().unwrap(),
                c["close"].as_f64().unwrap(),
            ));
        }

        Ok(out)
    }
}

/// Forwards both price calls to the inner supplier.
///
/// Lets `Arc<dyn PriceSupplier>` and `Arc<Consolidator>` be used wherever a
/// `PriceSupplier` is expected, including in spawned tasks.
impl<T> PriceSupplier for Arc<T>
where
    T: PriceSupplier,
{
    fn get_current_price(
        &self,
        contract: Contract,
        vwap: bool,
        generic_ticks: &[&str],
    ) -> Result<f64, String> {
        (**self).get_current_price(contract, vwap, generic_ticks)
    }

    #[cfg(not(feature = "backtest"))]
    fn populate_historical_data<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        contract: &'life1 Contract,
        config: &'life2 HistoricalDataConfig,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<(), String>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        (**self).populate_historical_data(contract, config)
    }
}
