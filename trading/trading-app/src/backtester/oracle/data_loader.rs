//! Data loader — populates `market_data.*` for the backtest period.
//!
//! Strategy:
//! 1. Check existing DB data using `HistoricalDataCRUD` to determine missing ranges per contract.
//! 2. For missing ranges:
//!    a. Try IBKR: `with_gateway_retry` → `Client::connect` → `client.historical_data`
//!       (paginated backwards, 1-month chunks, 1s sleep between calls).
//!       For Forex: fetch BOTH Bid + Ask.
//!    b. Fall back to Alpaca REST if IBKR yields no data.
//! 3. Refresh `daily_ohlcv` continuous aggregate.

use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use ibapi::Client;
use ibapi::contracts::Contract;
use ibapi::market_data::TradingHours;
use ibapi::market_data::historical::{BarSize, ToDuration, WhatToShow};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::PgPool;

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
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

/// Entry point. Dynamically checks existing database bars and fetches only missing data.
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

    for contract in contracts {
        let asset_type = AssetType::from_str(&contract.security_type);
        let crud = HistoricalDataCRUD::from(&asset_type, pool.clone());
        let pk_wo_time = HistoricalDataPrimaryKeysWoTime::from_contract(contract);

        // Calculate missing ranges using DB queries via HistoricalDataCRUD
        let missing_ranges = determine_missing_ranges(&crud, &pk_wo_time, start, end).await?;

        if missing_ranges.is_empty() {
            tracing::info!(
                "All market data for {} is already present in DB. Skipping fetch.",
                contract.symbol
            );
            continue;
        }

        for range in missing_ranges {
            tracing::info!(
                "Fetching missing range [{}, {}] for {}",
                range.start,
                range.end,
                contract.symbol
            );

            // 1. Try IBKR first for missing range
            let ibkr_result = try_ibkr_range(contract, range.start, range.end, pool).await;
            let loaded = match ibkr_result {
                Ok(n) if n > 0 => {
                    tracing::info!("✅ Data loaded via IBKR ({n} chunks/passes)");
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
                try_alpaca_range(contract, range.start, range.end, pool).await?;
            }
        }
    }

    // 3. Refresh continuous aggregate for the requested period.
    refresh_continuous_aggregate(pool, start, end).await;

    // 4. Final verification
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

/// Queries the database to identify gap ranges in `[start, end]`.
async fn determine_missing_ranges(
    crud: &HistoricalDataCRUD,
    pk_wo_time: &HistoricalDataPrimaryKeysWoTime,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<MissingRange>, String> {
    // Read existing bars to inspect cached range boundaries
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

    // Sort times to easily evaluate min/max boundaries available locally
    let mut times: Vec<DateTime<Utc>> = bars.full.iter().map(|b| b.get_time()).collect();
    times.sort_unstable();

    let earliest_db_time = times.first().copied().unwrap();
    let latest_db_time = times.last().copied().unwrap();

    let mut ranges = Vec::new();

    // Gap before existing DB window
    if start < earliest_db_time {
        ranges.push(MissingRange {
            start,
            end: end.min(earliest_db_time - chrono::Duration::seconds(1)),
        });
    }

    // Gap after existing DB window
    if end > latest_db_time {
        ranges.push(MissingRange {
            start: start.max(latest_db_time + chrono::Duration::seconds(1)),
            end,
        });
    }

    Ok(ranges)
}

// ─── IBKR ──────────────────────────────────────────────────────────────────

async fn try_ibkr_range(
    contract: &Contract,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    pool: &PgPool,
) -> Result<usize, String> {
    let pool = pool.clone();
    let contract = contract.clone();
    with_gateway_retry("/tmp/ibc.log", 2, |_gateway| async move {
        let client =
            Client::connect("localhost:4002", 0).map_err(|e| format!("connect to IBKR: {e}"))?;
        paginate_historical_data(&client, &contract, start, end, &pool).await
    })
    .await?
}

/// Paginate `client.historical_data` backwards from `end` to `start`.
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

            // Double check individual bar presence via `read` prior to upserting if needed
            if crud.read(&pk).await.is_ok() {
                continue;
            }

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

fn what_to_show_str(w: &WhatToShow) -> &'static str {
    match w {
        WhatToShow::Bid => "Bid",
        WhatToShow::Ask => "Ask",
        WhatToShow::Trades => "Trades",
        _ => "?",
    }
}

// ─── Alpaca fallback ───────────────────────────────────────────────────────

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
        let crud = HistoricalDataCRUD::from(&asset_type, pool.clone());

        for bar in bars {
            let t = bar["t"].as_str().ok_or("Alpaca: missing 't'")?;
            let time = chrono::DateTime::parse_from_rfc3339(t)
                .map_err(|e| format!("Alpaca: bad timestamp {t}: {e}"))?
                .with_timezone(&Utc);

            let pk = HistoricalDataPrimaryKeys::from_contract(contract, time);

            // Skip DB write if already stored
            if crud.read(&pk).await.is_ok() {
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

pub async fn refresh_continuous_aggregate(
    pool: &PgPool,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
) {
    tracing::info!("Refreshing daily_ohlcv continuous aggregate for [{start}, {end}]...");
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

