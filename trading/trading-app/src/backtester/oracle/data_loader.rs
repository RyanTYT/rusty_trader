//! Data loader — populates `market_data.*` for the backtest period.
//!
//! Strategy:
//! 1. Try IBKR: `with_gateway_retry` → `Client::connect` →
//!    `client.historical_data` (paginated, 1-month chunks, 1s sleep between
//!    calls for IBKR rate limits) → upsert via `HistoricalDataCRUD`.
//!    For Forex: fetch BOTH Bid + Ask (two passes — mirrors prod).
//! 2. If IBKR fails (no gateway / error / empty): Alpaca fallback via REST.
//! 3. Refresh `daily_ohlcv` continuous aggregate.
//!
//! This mirrors the prod `Consolidator::populate_historical_data` as closely
//! as possible — same bar construction, same upsert, same forex recursion,
//! same "skip latest incomplete bar" logic — but calls `client.historical_data`
//! directly (the prod version is cfg-gated out under `backtest`).

use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use ibapi::Client;
use ibapi::contracts::Contract;
use ibapi::market_data::TradingHours;
use ibapi::market_data::historical::{BarSize, ToDuration, WhatToShow};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use sqlx::PgPool;

use crate::database::crud::CRUDTrait;
use crate::database::models::AssetType;
use crate::database::models_crud::historical_data::historical_data::{
    HistoricalDataCRUD, HistoricalDataFullKeys, HistoricalDataOps,
    HistoricalDataPrimaryKeys, HistoricalDataPrimaryKeysWoTime,
    HistoricalDataUpdateKeys,
};
use crate::ibc::with_gateway_retry;

/// Entry point. Tries IBKR first, Alpaca fallback. Refreshes caggs after.
pub async fn load_market_data(
    contracts: &[Contract],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    pool: &PgPool,
    _handle: &tokio::runtime::Handle,
) -> Result<(), String> {
    tracing::info!(
        "Data loader: {} contracts, period [{}, {}]",
        contracts.len(),
        start,
        end
    );

    // 1. Try IBKR
    let ibkr_result = try_ibkr(contracts, start, end, pool).await;
    let loaded = match ibkr_result {
        Ok(n) if n > 0 => {
            tracing::info!("✅ Data loaded via IBKR ({n} bars)");
            n
        }
        Ok(_) => {
            tracing::warn!("IBKR returned 0 bars — falling back to Alpaca");
            0
        }
        Err(e) => {
            tracing::warn!("IBKR data load failed: {e} — falling back to Alpaca");
            0
        }
    };

    // 2. Alpaca fallback if IBKR yielded nothing
    if loaded == 0 {
        try_alpaca(contracts, start, end, pool).await?;
    }

    // 3. Refresh continuous aggregate for the backtest period.
    refresh_continuous_aggregate(pool, start, end).await;

    // 4. Verify data exists
    for contract in contracts {
        let asset_type = AssetType::from_str(&contract.security_type);
        let crud = HistoricalDataCRUD::from(&asset_type, pool.clone());
        let pk = HistoricalDataPrimaryKeysWoTime::from_contract(contract);
        let start_tz = start.with_timezone(&New_York);
        let has_data = crud
            .has_at_least_n_rows_since(pk, 1, &start_tz)
            .await
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

// ─── IBKR ──────────────────────────────────────────────────────────────────

async fn try_ibkr(
    contracts: &[Contract],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    pool: &PgPool,
) -> Result<usize, String> {
    let pool = pool.clone();
    let contracts = contracts.to_vec();
    with_gateway_retry("/tmp/ibc.log", 2, |_gateway| async move {
        let client = Client::connect("localhost:4002", 0)
            .map_err(|e| format!("connect to IBKR: {e}"))?;
        let mut total = 0;
        for contract in &contracts {
            total += paginate_historical_data(&client, contract, start, end, &pool).await?;
        }
        Ok::<usize, String>(total)
    })
    .await?
}

/// Paginate `client.historical_data` backwards from `end` to `start`.
/// IBKR returns max ~2000 5-min bars per call (~1 month for stocks).
/// Each call fetches 1 month; the cursor moves backwards; 1s sleep
/// between calls (IBKR rate limit: ~1 req/sec).
async fn paginate_historical_data(
    client: &Client,
    contract: &Contract,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    pool: &PgPool,
) -> Result<usize, String> {
    let asset_type = AssetType::from_str(&contract.security_type);
    let is_forex = asset_type == AssetType::ForexPair;

    let bar_size = if is_forex { BarSize::Min } else { BarSize::Min5 };
    let bar_interval_secs: i64 = if is_forex { 60 } else { 300 };

    // For forex, fetch Bid first, then Ask (mirrors prod recursion).
    let what_to_shows = if is_forex {
        vec![WhatToShow::Bid, WhatToShow::Ask]
    } else {
        vec![WhatToShow::Trades]
    };

    let mut total = 0;
    for what_to_show in &what_to_shows {
        total += paginate_single_direction(
            client, contract, start, end, pool, *what_to_show, bar_size.clone(), bar_interval_secs,
        )
        .await?;
    }
    Ok(total)
}

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
    let crud = HistoricalDataCRUD::from(&asset_type, pool.clone());
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
                30.days(), // ~1 month per call (ToDuration trait)
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

        // Compute the "latest incomplete bar" timestamp to skip (mirrors prod).
        let now_ts = Utc::now().timestamp();
        let latest_request_time_bar = now_ts - (now_ts % bar_interval_secs);

        // Determine the earliest bar's time (to move the cursor backwards).
        // bars are DESC order (most recent first); last() is the earliest.
        let earliest = bars
            .last()
            .map(|b| {
                DateTime::from_timestamp(b.date.unix_timestamp(), b.date.nanosecond() as u32)
                    .unwrap_or(end_cursor)
            })
            .unwrap_or(end_cursor);

        for bar in bars {
            let bar_ts = bar.date.unix_timestamp();
            if bar_ts == latest_request_time_bar {
                continue; // skip the latest (possibly incomplete) bar
            }

            let bar_time =
                DateTime::from_timestamp(bar_ts, bar.date.nanosecond() as u32).unwrap_or(end_cursor);

            let fk = HistoricalDataFullKeys::from_contract_and_bar(contract, &what_to_show, bar);
            let pk = HistoricalDataPrimaryKeys::from_contract(contract, bar_time);
            let uk = HistoricalDataUpdateKeys::from_historical_bar(contract, &what_to_show, &fk);

            if let Err(e) = crud.create_or_update(&pk, &uk).await {
                tracing::error!("Failed to upsert historical bar for {}: {e:?}", contract.symbol);
            }
        }

        total += 1; // count chunks; actual bar count is harder since we consumed bars
        tracing::info!("  → chunk fetched, moving cursor backwards");

        end_cursor = earliest - chrono::Duration::seconds(1);
        if end_cursor <= start {
            break;
        }

        // IBKR rate limit: ~1 req/sec.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(total)
}

fn what_to_show_str(w: &WhatToShow) -> &'static str {
    match w {
        WhatToShow::Bid => "Bid",
        WhatToShow::Ask => "Ask",
        WhatToShow::Trades => "Trades",
        _ => "?",
    }
}

// ─── Alpaca fallback ───────────────────────────────────────────────────────

async fn try_alpaca(
    contracts: &[Contract],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    pool: &PgPool,
) -> Result<(), String> {
    let api_key = std::env::var("ALPACA_API_KEY")
        .map_err(|_| "ALPACA_API_KEY not set — cannot fall back to Alpaca".to_string())?;
    let api_secret = std::env::var("ALPACA_API_SECRET")
        .map_err(|_| "ALPACA_API_SECRET not set".to_string())?;

    tracing::info!("Fetching historical data from Alpaca...");

    let client = reqwest::Client::new();
    let mut total = 0;

    for contract in contracts {
        let symbol = contract.symbol.to_string();
        tracing::info!("Fetching Alpaca bars for {symbol}");

        // Alpaca REST: GET /v2/stocks/{symbol}/bars?timeframe=5Min&start=...&end=...&limit=10000
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
            let crud = HistoricalDataCRUD::from(&asset_type, pool.clone());

            for bar in bars {
                let t = bar["t"].as_str().ok_or("Alpaca: missing 't'")?;
                let time = chrono::DateTime::parse_from_rfc3339(t)
                    .map_err(|e| format!("Alpaca: bad timestamp {t}: {e}"))?
                    .with_timezone(&Utc);

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
                let pk = HistoricalDataPrimaryKeys::from_contract(contract, time);
                let uk = HistoricalDataUpdateKeys::from_historical_bar(
                    contract,
                    &WhatToShow::Trades,
                    &fk,
                );

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
    }

    tracing::info!("✅ Alpaca loaded {total} bars");
    Ok(())
}

// ─── Continuous aggregate refresh ─────────────────────────────────────────

/// Refresh the `daily_ohlcv` continuous aggregate for `[start, end]`. This
/// also updates the `daily_volatility` VIEW (it reads from `daily_ohlcv`).
/// `pub` so the optimizer can call it directly (when the data is already
/// loaded but the aggregate needs refreshing).
pub async fn refresh_continuous_aggregate(
    pool: &PgPool,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
) {
    tracing::info!("Refreshing daily_ohlcv continuous aggregate for [{start}, {end}]...");
    // Runtime query (not sqlx::query! macro) to avoid needing .sqlx/ cache.
    if let Err(e) = sqlx::query(
        r#"CALL refresh_continuous_aggregate(
            'market_data.daily_ohlcv',
            $1,
            $2
        );"#,
    )
    .bind(start)
    .bind(end)
    .execute(pool)
    .await
    {
        tracing::error!("Failed to refresh daily_ohlcv: {e:?}");
    }
    tracing::info!("Continuous aggregate refresh done.");
}
