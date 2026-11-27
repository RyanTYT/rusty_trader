//! Data loader — ensures `market_data.historical_data` covers the backtest
//! period before the replay starts.
//!
//! Only the `TimeRange` lane calls this, and only when at least one contract
//! has no bars (`run_backtest`'s offline-mode check). The loader:
//! 1. reads the existing bars per contract and derives the missing leading and
//!    trailing ranges — interior holes are not detected;
//! 2. boots the IBKR gateway once and pages 30-day windows backwards over
//!    regular trading hours (FX fetches bid and ask; everything else trades);
//! 3. falls back to Alpaca REST for stock ranges IBKR returned nothing for
//!    (requires `ALPACA_API_KEY` / `ALPACA_API_SECRET`);
//! 4. refreshes the continuous aggregate — currently a no-op because the
//!    `daily_ohlcv` aggregate was removed — and warns about contracts that
//!    still have no bars.

use crate::database::tables::DbTables;
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use ibapi::Client;
use ibapi::contracts::Contract;
use ibapi::market_data::TradingHours;
use ibapi::market_data::historical::{BarSize, ToDuration, WhatToShow};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::PgPool;
use std::collections::HashMap;

use crate::database::crud::CRUDTrait;
use crate::database::models::AssetType;
use crate::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, HistoricalDataFullKeys, HistoricalDataOps, HistoricalDataPrimaryKeys,
    HistoricalDataPrimaryKeysWoTime, HistoricalDataUpdateKeys,
};
use crate::ibc::with_gateway_retry;

/// Defines a range of missing data that needs to be fetched.
#[derive(Debug, Clone, Copy)]
struct MissingRange {
    /// First instant of the gap (inclusive).
    start: DateTime<Utc>,
    /// Last instant of the gap (inclusive).
    end: DateTime<Utc>,
}

/// Entry point: checks existing database bars and fetches only the data that
/// is missing at the edges of `[start, end]`.
///
/// Existing bars are counted with `read_last_n`; a contract with no bars gets
/// the whole range. IBKR is attempted first for every gap in one gateway
/// session, then Alpaca fills stock gaps that IBKR returned nothing for.
/// Finally the loader re-checks each contract and warns when it still has no
/// bars — it never fails the run for missing data.
///
/// # Errors
/// Returns a string when the initial database read fails or when an Alpaca
/// fallback fails (missing credentials, HTTP error, malformed payload).
/// IBKR failures are logged and degraded to an empty result.
pub async fn load_market_data(
    contracts: &[Contract],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    pool: &PgPool,
    _handle: &tokio::runtime::Handle,
) -> Result<(), String> {
    tracing::info!(
        "Data loader: {} contracts, requested period [{}, {}]",
        contracts.len(),
        start,
        end
    );

    // 1. Identify missing ranges for all contracts upfront
    let mut contract_missing_ranges: HashMap<usize, Vec<MissingRange>> = HashMap::new();
    for (idx, contract) in contracts.iter().enumerate() {
        let asset_type = AssetType::from_str(&contract.security_type);
        let mut db = crate::database::db::DbDirect::connect(&pool);
        let mut crud = db.historical_data(&asset_type);
        let pk_wo_time = HistoricalDataPrimaryKeysWoTime::from_contract(contract);

        let missing_ranges = determine_missing_ranges(&mut crud, &pk_wo_time, start, end).await?;
        if missing_ranges.is_empty() {
            tracing::info!(
                "All market data for {} is already present in DB. Skipping fetch.",
                contract.symbol
            );
        } else {
            contract_missing_ranges.insert(idx, missing_ranges);
        }
    }

    if contract_missing_ranges.is_empty() {
        tracing::info!("All requested data is already loaded in the database.");
        refresh_continuous_aggregate(pool, start, end).await;
        return Ok(());
    }

    // 2. Try IBKR first for all contracts within a single gateway session
    let ibkr_loaded_counts = try_ibkr(contracts, &contract_missing_ranges, start, end, pool).await;

    // 3. Fall back to Alpaca for any missing ranges that IBKR failed to fill
    for (idx, contract) in contracts.iter().enumerate() {
        let missing_ranges = match contract_missing_ranges.get(&idx) {
            Some(ranges) => ranges,
            None => continue,
        };

        for range in missing_ranges {
            let ibkr_fetched = ibkr_loaded_counts
                .get(&(idx, range.start))
                .copied()
                .unwrap_or(0);

            if ibkr_fetched == 0 {
                tracing::warn!(
                    "IBKR produced 0 bars for {} [{}, {}] — falling back to Alpaca",
                    contract.symbol,
                    range.start,
                    range.end
                );
                try_alpaca_range(contract, range.start, range.end, pool).await?;
            }
        }
    }

    // 4. Refresh continuous aggregate for the requested period.
    refresh_continuous_aggregate(pool, start, end).await;

    // 5. Final verification
    for contract in contracts {
        let asset_type = AssetType::from_str(&contract.security_type);
        let mut db = crate::database::db::DbDirect::connect(&pool);
        let mut crud = db.historical_data(&asset_type);
        let pk = HistoricalDataPrimaryKeysWoTime::from_contract(contract);
        let start_tz = start.with_timezone(&New_York);
        let has_data = crud
            .get_rows_since(pk, 1, &start_tz)
            .await
            .map(|v| v >= 1)
            .unwrap_or(false);
        if !has_data {
            tracing::warn!(
                "No data found for {} after load — backtest may produce no results",
                contract.symbol
            );
        }
    }

    Ok(())
}

/// Queries the database to identify gap ranges in `[start, end]`.
///
/// Only the leading gap (before the earliest stored bar) and the trailing gap
/// (after the latest stored bar) are returned; an interior hole between two
/// stored bars is invisible to this check. With no stored bars the whole range
/// is returned.
///
/// # Errors
/// Returns a string when the underlying `read_last_n` query fails.
async fn determine_missing_ranges(
    crud: &mut HistoricalDataCRUD<'_>,
    pk_wo_time: &HistoricalDataPrimaryKeysWoTime,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<MissingRange>, String> {
    let bars = crud
        .read_last_n(
            pk_wo_time.clone(),
            5,
            99999999,
            #[cfg(feature = "backtest")]
            None,
        )
        .await
        .map_err(|e| format!("Failed to query DB via read_last_n: {e:?}"))?;

    if bars.full.is_empty() {
        return Ok(vec![MissingRange { start, end }]);
    }

    let mut times: Vec<DateTime<Utc>> = bars.full.iter().map(|b| b.get_time()).collect();
    times.sort_unstable();

    let earliest_db_time = times.first().copied().unwrap();
    let latest_db_time = times.last().copied().unwrap();

    let mut ranges = Vec::new();

    if start < earliest_db_time {
        ranges.push(MissingRange {
            start,
            end: end.min(earliest_db_time - chrono::Duration::seconds(1)),
        });
    }

    if end > latest_db_time {
        ranges.push(MissingRange {
            start: start.max(latest_db_time + chrono::Duration::seconds(1)),
            end,
        });
    }

    Ok(ranges)
}

// ─── IBKR ──────────────────────────────────────────────────────────────────

/// Boots the IBKR Gateway once and iterates through all missing ranges for
/// all contracts.
///
/// Returns `(contract index, range start) -> chunks fetched`; a range that
/// errored records `0`, and a failed gateway session returns an empty map (the
/// caller then falls back to Alpaca where applicable). The two startup
/// attempts are made by `with_gateway_retry`.
async fn try_ibkr(
    contracts: &[Contract],
    missing_map: &HashMap<usize, Vec<MissingRange>>,
    _start: DateTime<Utc>,
    _end: DateTime<Utc>,
    pool: &PgPool,
) -> HashMap<(usize, DateTime<Utc>), usize> {
    let pool = pool.clone();
    let contracts = contracts.to_vec();
    let missing_map = missing_map.clone();

    // with_gateway_retry is wrapped outside the entire population loop
    let result = with_gateway_retry("/tmp/ibc.log", 2, |_gateway| async move {
        let client =
            Client::connect("localhost:4002", 0).map_err(|e| format!("connect to IBKR: {e}"))?;

        let mut loaded_counts = HashMap::new();

        for (contract_idx, contract) in contracts.iter().enumerate() {
            if let Some(ranges) = missing_map.get(&contract_idx) {
                for range in ranges {
                    tracing::info!(
                        "Fetching missing range [{}, {}] via IBKR for {}",
                        range.start,
                        range.end,
                        contract.symbol
                    );

                    match paginate_historical_data(&client, contract, range.start, range.end, &pool)
                        .await
                    {
                        Ok(n) => {
                            tracing::info!(
                                "✅ IBKR fetched {n} chunk(s) for {} [{}, {}]",
                                contract.symbol,
                                range.start,
                                range.end
                            );
                            loaded_counts.insert((contract_idx, range.start), n);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "IBKR fetch error for {} [{}, {}]: {e}",
                                contract.symbol,
                                range.start,
                                range.end
                            );
                            loaded_counts.insert((contract_idx, range.start), 0);
                        }
                    }
                }
            }
        }

        Ok::<HashMap<(usize, DateTime<Utc>), usize>, String>(loaded_counts)
    })
    .await;

    match result {
        Ok(map) => map.expect("Expected result to be fine"),
        Err(e) => {
            tracing::warn!("IBKR gateway session failed: {e}");
            HashMap::new()
        }
    }
}

/// Fetch one gap for one contract: FX requests bid *and* ask, everything else
/// requests trades. Bar size is 1 minute for FX and 5 minutes otherwise.
///
/// Returns the number of 30-day chunks fetched (summed over both directions
/// for FX).
///
/// # Errors
/// Propagates the first pagination failure.
async fn paginate_historical_data(
    client: &Client,
    contract: &Contract,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    pool: &PgPool,
) -> Result<usize, String> {
    let asset_type = AssetType::from_str(&contract.security_type);
    let is_forex = asset_type == AssetType::ForexPair;

    let bar_size = if is_forex {
        BarSize::Min
    } else {
        BarSize::Min5
    };
    let bar_interval_secs: i64 = if is_forex { 60 } else { 300 };

    let what_to_shows = if is_forex {
        vec![WhatToShow::Bid, WhatToShow::Ask]
    } else {
        vec![WhatToShow::Trades]
    };

    let mut total = 0;
    for what_to_show in &what_to_shows {
        total += paginate_single_direction(
            client,
            contract,
            start,
            end,
            pool,
            *what_to_show,
            bar_size.clone(),
            bar_interval_secs,
        )
        .await?;
    }
    Ok(total)
}

/// Pages `client.historical_data` backwards in ~30-day windows from `end`
/// towards `start` for a single `what_to_show`.
///
/// The still-forming current bar (the bar whose timestamp equals the
/// wall-clock-aligned interval) is skipped. Each bar is upserted with
/// `create_or_update`; an upsert failure is logged and does not abort the
/// page. A one-second sleep separates windows.
///
/// Returns the number of chunks fetched.
///
/// # Errors
/// Returns a string when the request itself fails (IBKR error or cursor
/// conversion).
async fn paginate_single_direction(
    client: &Client,
    contract: &Contract,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    pool: &PgPool,
    what_to_show: WhatToShow,
    bar_size: BarSize,
    bar_interval_secs: i64,
) -> Result<usize, String> {
    let asset_type = AssetType::from_str(&contract.security_type);
    let mut db = crate::database::db::DbDirect::connect(&pool);
    let mut crud = db.historical_data(&asset_type);
    let mut total = 0;
    let mut end_cursor = end;

    loop {
        if end_cursor <= start {
            break;
        }

        tracing::info!(
            "Fetching {} historical data for {} ending at {}",
            what_to_show_str(&what_to_show),
            contract.symbol,
            end_cursor
        );

        let end_odt = time::OffsetDateTime::from_unix_timestamp(end_cursor.timestamp())
            .map_err(|e| format!("convert end_cursor: {e}"))?;

        let result = client
            .historical_data(
                contract,
                Some(end_odt),
                30.days(), // ~1 month per call
                bar_size.clone(),
                what_to_show.clone(),
                TradingHours::Regular,
            )
            .map_err(|e| format!("historical_data error: {e}"))?;

        let bars = result.bars;
        if bars.is_empty() {
            tracing::warn!("No bars returned for {}", contract.symbol);
            break;
        }

        let now_ts = Utc::now().timestamp();
        let latest_request_time_bar = now_ts - (now_ts % bar_interval_secs);

        let earliest = bars
            .first()
            .map(|b| {
                DateTime::from_timestamp(b.date.unix_timestamp(), b.date.nanosecond() as u32)
                    .unwrap_or(end_cursor)
            })
            .unwrap_or(end_cursor);

        for bar in bars {
            let bar_ts = bar.date.unix_timestamp();
            if bar_ts == latest_request_time_bar {
                continue; // skip incomplete current bar
            }

            let bar_time = DateTime::from_timestamp(bar_ts, bar.date.nanosecond() as u32)
                .unwrap_or(end_cursor);

            let fk = HistoricalDataFullKeys::from_contract_and_bar(contract, &what_to_show, bar);
            let pk = HistoricalDataPrimaryKeys::from_contract(contract, bar_time);
            let uk = HistoricalDataUpdateKeys::from_historical_bar(contract, &what_to_show, &fk);

            if let Err(e) = crud.create_or_update(&pk, &uk).await {
                tracing::error!(
                    "Failed to upsert historical bar for {}: {e:?}",
                    contract.symbol
                );
            }
        }

        total += 1;
        tracing::info!("  → chunk fetched, moving cursor backwards");

        end_cursor = earliest - chrono::Duration::seconds(1);
        if end_cursor <= start {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(total)
}

/// Human-readable name for the IBKR price direction, used only in logs.
fn what_to_show_str(w: &WhatToShow) -> &'static str {
    match w {
        WhatToShow::Bid => "Bid",
        WhatToShow::Ask => "Ask",
        WhatToShow::Trades => "Trades",
        _ => "?",
    }
}

// ─── Alpaca fallback ───────────────────────────────────────────────────────

/// Alpaca REST fallback for a single stock range (5-minute raw bars).
///
/// Reads `ALPACA_API_KEY` and `ALPACA_API_SECRET` from the environment, follows
/// `next_page_token`, skips timestamps already present, and upserts the rest.
/// Only stock contracts are meaningful here: every bar is written as a
/// `HistoricalDataFullKeys::Stock` row.
///
/// # Errors
/// Returns a string when the credentials are missing, the HTTP request fails,
/// the payload cannot be parsed, or the response omits `bars`.
async fn try_alpaca_range(
    contract: &Contract,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    pool: &PgPool,
) -> Result<(), String> {
    let api_key = std::env::var("ALPACA_API_KEY")
        .map_err(|_| "ALPACA_API_KEY not set — cannot fall back to Alpaca".to_string())?;
    let api_secret =
        std::env::var("ALPACA_API_SECRET").map_err(|_| "ALPACA_API_SECRET not set".to_string())?;

    tracing::info!(
        "Fetching historical data from Alpaca for range [{}, {}]...",
        start,
        end
    );

    let client = reqwest::Client::new();
    let mut total = 0;
    let symbol = contract.symbol.to_string();

    let mut page_token: Option<String> = None;
    loop {
        let mut url = format!(
            "https://data.alpaca.markets/v2/stocks/{symbol}/bars?timeframe=5Min&start={}&end={}&limit=10000&adjustment=raw",
            start.format("%Y-%m-%dT%H:%M:%SZ"),
            end.format("%Y-%m-%dT%H:%M:%SZ"),
        );
        if let Some(token) = &page_token {
            url.push_str(&format!("&page_token={token}"));
        }

        let resp = client
            .get(&url)
            .header("APCA-API-KEY-ID", &api_key)
            .header("APCA-API-SECRET-KEY", &api_secret)
            .send()
            .await
            .map_err(|e| format!("Alpaca request failed: {e}"))?;

        if !resp.status().is_success() {
            return Err(format!(
                "Alpaca API error: {} {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            ));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("Alpaca parse error: {e}"))?;

        let bars = body["bars"]
            .as_array()
            .ok_or("Alpaca: no bars in response")?;
        if bars.is_empty() {
            break;
        }

        let asset_type = AssetType::from_str(&contract.security_type);
        let mut db = crate::database::db::DbDirect::connect(&pool);
        let mut crud = db.historical_data(&asset_type);

        for bar in bars {
            let t = bar["t"].as_str().ok_or("Alpaca: missing 't'")?;
            let time = chrono::DateTime::parse_from_rfc3339(t)
                .map_err(|e| format!("Alpaca: bad timestamp {t}: {e}"))?
                .with_timezone(&Utc);

            let pk = HistoricalDataPrimaryKeys::from_contract(contract, time);

            // Skip only rows that actually exist: `read` returns `Ok(None)` for
            // a missing key, so `is_ok()` would have skipped every bar.
            if matches!(crud.read(&pk).await, Ok(Some(_))) {
                continue;
            }

            let fk = HistoricalDataFullKeys::Stock(
                crate::database::models::HistoricalStockDataFullKeys {
                    stock: crate::helpers::contract::get_local_symbol(contract),
                    primary_exchange: contract.primary_exchange.to_string(),
                    currency: contract.currency.to_string(),
                    time,
                    open: bar["o"].as_f64().unwrap_or(0.0),
                    high: bar["h"].as_f64().unwrap_or(0.0),
                    low: bar["l"].as_f64().unwrap_or(0.0),
                    close: bar["c"].as_f64().unwrap_or(0.0),
                    volume: Decimal::from_f64(bar["v"].as_f64().unwrap_or(0.0))
                        .unwrap_or(Decimal::ZERO),
                },
            );

            let uk =
                HistoricalDataUpdateKeys::from_historical_bar(contract, &WhatToShow::Trades, &fk);

            if let Err(e) = crud.create_or_update(&pk, &uk).await {
                tracing::error!("Alpaca upsert failed for {}: {e:?}", contract.symbol);
            }
            total += 1;
        }

        page_token = body["next_page_token"].as_str().map(|s| s.to_string());
        if page_token.is_none() {
            break;
        }
    }

    tracing::info!("✅ Alpaca loaded {total} bars");
    Ok(())
}

// ─── Continuous aggregate refresh ─────────────────────────────────────────

/// Refreshes the continuous aggregate covering the requested period.
///
/// The baseline schema no longer defines `market_data.daily_ohlcv` (the
/// aggregate was removed with the deprecated daily table), so there is nothing
/// to refresh. The function is kept as the stable hook for the TimeRange entry
/// points and is deliberately a no-op: issuing the old `CALL` would fail and
/// log an error on every run.
pub async fn refresh_continuous_aggregate(
    _pool: &PgPool,
    _start: chrono::DateTime<chrono::Utc>,
    _end: chrono::DateTime<chrono::Utc>,
) {
    tracing::debug!("refresh_continuous_aggregate: no aggregate is configured; skipping");
}
