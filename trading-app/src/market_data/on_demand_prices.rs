use std::{iter::zip, sync::Arc, time::Duration};

use chrono::{DateTime, Datelike, Utc};
use ibapi::{
    Client,
    client::Subscription,
    prelude::{Contract, HistoricalWhatToShow, SecurityType, TickTypes},
};
use rust_decimal::{Decimal, prelude::FromPrimitive, str};
use yfinance_rs::{Ticker, YfClient};

use crate::{
    database::{
        models::{
            HistoricalDataPrimaryKeys, HistoricalDataUpdateKeys, HistoricalForexDataUpdateKeys,
            HistoricalOptionsDataPrimaryKeys, OptionType,
        },
        models_crud::{
            historical_data::HistoricalDataCRUD, historical_forex_data::HistoricalForexDataCRUD,
            historical_options_data::HistoricalOptionsDataCRUD,
        },
    },
    helpers::{
        contract::{HashContract, get_local_symbol},
        sync_timeout::timeout,
    },
    market_data::consolidator::Consolidator,
};

#[allow(dead_code)]
const DUKASCOPY_BASE_URL: &str = "https://jetta.dukascopy.com/v1/candles/minute";

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
    fn yahoo_ticker_from_contract(contract: &Contract) -> Option<String> {
        let symbol = contract.symbol.as_str();
        let exchange = contract.exchange.as_str();
        let primary = contract.primary_exchange.as_str();
        let currency = contract.currency.as_str();

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
    fn get_price_from_yfinance(yahoo_ticker: &str) -> Result<Option<f64>, String> {
        // We're in a sync context; spin up a one-shot runtime for the async call.
        // This is acceptable in a fallback path — not on a hot loop.
        let yahoo_ticker = yahoo_ticker.to_string();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let client = YfClient::default();
                let ticker = Ticker::new(&client, &yahoo_ticker);
                let fast_info = ticker.fast_info().await.map_err(|e| {
                    format!("yfinance-rs fast_info failed for {yahoo_ticker}: {e:?}")
                })?;
                Ok(fast_info.last.map(|v| v.amount().as_f64()))
            })
        })
    }

    /// Returns true if the IBKR error string indicates a market data permission/access issue,
    /// as opposed to a connectivity or timeout problem.
    fn is_ibkr_market_data_error(err: &str) -> bool {
        // IBKR error 354: "Requested market data is not subscribed"
        // IBKR error 10090: "Part of requested market data is not subscribed"
        // IBKR error 200: "No security definition has been found"
        let access_denied_patterns = [
            "not subscribed",
            "market data farm",
            "No security definition",
            "354",
            "10090",
        ];
        access_denied_patterns
            .iter()
            .any(|pat| err.to_lowercase().contains(&pat.to_lowercase()))
    }

    /// Helper function to extract the price of contract from the ticker received
    /// NOTE: this extracts the first price returned which may NOT be the actual correct data point
    /// - i.e. smth like requesting for ask -> may result in ticktype of high, open, ... to come
    /// first and is returned first
    pub fn _extract_price(
        &self,
        tick: TickTypes,
        contract: &Contract,
        subscription: &Subscription<TickTypes>,
    ) -> Result<f64, String> {
        match tick {
            ibapi::prelude::TickTypes::Price(tick_price) => return Ok(tick_price.price),
            ibapi::prelude::TickTypes::SnapshotEnd => {
                subscription.cancel();

                return Err(format!(
                    "Got SnapshotEnd from request for market data: {}",
                    contract.symbol
                ));
            }
            ibapi::prelude::TickTypes::RequestParameters(_) => {
                return Err(format!(
                    "Got RequestParameters ticker from request for mkt data: {}:",
                    contract.symbol
                ));
            }
            ibapi::prelude::TickTypes::String(msg) => {
                return Err(format!(
                    "Got string from request for market data: {}",
                    msg.value
                ));
            }
            _ => {
                return Err(format!(
                    "Got unknown ticker from request for market data: {}: {:?}",
                    contract.symbol, tick
                ));
            }
        }
    }

    pub fn get_current_price(
        &self,
        contract: &Contract,
        vwap: &bool,
        generic_ticks: &[&str],
    ) -> Result<f64, String> {
        return self._get_current_price(contract, vwap, generic_ticks, &false);
    }

    /// Gets the current price of the contract from IBKR
    /// - if currently subscribed to their live data - unlocks and returns it
    ///     - Note: Each live_data subscription is wrapped behind a std::sync::Mutex so this
    ///     function could be potentially blocking for a longer period of time than expected
    /// - if requested the data in the last 20s, returns that
    /// - else, requests from IBKR
    ///
    /// - if you want to pass generic_ticks, vwap MUST be false
    fn _get_current_price(
        &self,
        contract: &Contract,
        vwap: &bool,
        generic_ticks: &[&str],
        is_second_try: &bool,
    ) -> Result<f64, String> {
        let hash_contract = HashContract {
            contract: contract.clone(),
        };

        {
            // If currently tracking, then j return latest data
            let live_data = self
                .live_data
                .read()
                .expect("Expected live data read lock not to be poisoned");
            if !vwap && live_data.contains_key(&hash_contract) {
                let live_data_for_contract_arc = live_data.get(&hash_contract).unwrap();
                let live_data_for_contract_opt = live_data_for_contract_arc.upgrade();
                if let Some(live_data_for_contract) = live_data_for_contract_opt {
                    if let Some(latest_bar) = live_data_for_contract
                        .read()
                        .expect("Expected read lock for live_data_for_contract to not be poisoned")
                        .as_ref()
                    {
                        return Ok(*latest_bar);
                    }
                }
            }
        }

        // If recently requested
        if *vwap {
            if self.past_data_vwap.contains_key(&hash_contract) {
                return Ok(self.past_data_vwap.get(&hash_contract).expect(
                    format!("past_data_vwap lost value for {}", contract.symbol).as_str(),
                ));
            }
        } else {
            if self.past_data.contains_key(&hash_contract) {
                return Ok(self
                    .past_data
                    .get(&hash_contract)
                    .expect(format!("past_data lost value for {}", contract.symbol).as_str()));
            }
        }

        // Request data as last resort
        let generic_ticks = if *vwap {
            &["233"]
        } else if contract.security_type == SecurityType::ForexPair && generic_ticks.len() == 0 {
            // &["2"] // ask price by default
            generic_ticks
        } else {
            generic_ticks
        };
        let subscription = self
            .client
            .market_data(&contract)
            .generic_ticks(generic_ticks)
            .snapshot()
            .subscribe()
            .map_err(|e| {
                tracing::error!("Failed to request current price from IBKR: {e:?}");
                format!("Failed to request current price from IBKR: {e:?}")
            })?;

        let mut num_iter = 0;
        let mut cum_err = Vec::new();
        loop {
            if let Some(latest_tick) = subscription.next_timeout(Duration::from_secs(5)) {
                match self._extract_price(latest_tick, &contract, &subscription) {
                    Ok(price) => {
                        if *vwap {
                            self.past_data_vwap.insert(hash_contract.clone(), price);
                        } else {
                            self.past_data.insert(hash_contract.clone(), price);
                        }

                        return Ok(price);
                    }
                    Err(e) => {
                        // ── yfinance fallback: only on access/subscription errors ──
                        if Self::is_ibkr_market_data_error(&e) {
                            if contract.security_type == SecurityType::ForexPair && !is_second_try {
                                return self
                                    ._get_current_price(
                                        &Contract {
                                            symbol: contract.currency.to_string().into(),
                                            currency: contract.symbol.to_string().into(),
                                            ..contract.clone()
                                        },
                                        vwap,
                                        generic_ticks,
                                        &true,
                                    )
                                    .map(|v| 1.0 / v);
                            }
                            match Self::yahoo_ticker_from_contract(contract) {
                                Some(yt) => {
                                    tracing::warn!(
                                        "IBKR market data access denied for {} ({}), \
                                 falling back to yfinance-rs with ticker '{}'",
                                        contract.symbol,
                                        e,
                                        yt
                                    );
                                    let yfinance_price = Self::get_price_from_yfinance(&yt)?;
                                    return yfinance_price
                                        .ok_or("Failed to fetch data from yfinance".to_string());
                                }
                                None => {
                                    // No Yahoo mapping — fall through to normal retry/error path
                                    return Err("Failed to fetch data from yfinance".to_string());
                                }
                            }
                        }

                        num_iter += 1;
                        if num_iter >= 15 {
                            cum_err.push(format!("Final try for price data for {} failed due to {e:?}.\nFunction has failed and is returning Error!", contract.symbol.clone()));
                            return Err(cum_err.join("\n"));
                        }
                        cum_err.push(format!("{num_iter:?}th try for price data for {} failed due to {e:?}\ntrying again!", contract.symbol.clone()));
                    }
                }
            } else {
                num_iter += 1;
                cum_err.push("Timed out!".to_string());
            }
        }
    }

    pub async fn fetch_dukascopy_day(
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

    // If is_forex is true, what_to_show is ignored - takes both bid and ask
    pub fn get_historical_data(
        client: Arc<Client>,
        historical_data_crud: &HistoricalDataCRUD,
        historical_options_data_crud: &HistoricalOptionsDataCRUD,
        historical_forex_data_crud: &HistoricalForexDataCRUD,

        contract: &Contract,
        duration: ibapi::market_data::historical::Duration,
        bar_size: ibapi::prelude::HistoricalBarSize,
        what_to_show: HistoricalWhatToShow,
        is_bid_ask: &bool,
        apply_batching_raw: &bool,
    ) -> Result<(), String> {
        let apply_batching = apply_batching_raw.clone();
        let symbol = get_local_symbol(&contract);

        enum BarView {
            Single(ibapi::market_data::historical::Bar),
            BidAsk {
                bid: ibapi::market_data::historical::Bar,
                ask: ibapi::market_data::historical::Bar,
            },
        }
        let bar_interval = if contract.security_type == SecurityType::ForexPair {
            60
        } else {
            300
        };
        let last_request_timestamp = Utc::now().timestamp();
        let last_request_time_bar =
            last_request_timestamp - (last_request_timestamp % bar_interval);

        // let historical_data_bars: Box<dyn Iterator<Item = BarView>>;

        let cloned_contract = contract.clone();
        let cloned_is_bid_ask = is_bid_ask.clone();
        let historical_data_bars = timeout(Duration::from_secs(5*60), move || {
            let historical_data_bars: Box<dyn Iterator<Item = BarView> + Send> = if !cloned_is_bid_ask {
                let data = client
                    .historical_data(
                        &cloned_contract,
                        None,
                        duration,
                        bar_size,
                        what_to_show,
                        ibapi::prelude::TradingHours::Regular,
                    )
                    .map_err(|e| {
                        format!(
                            "Expected Historical Data Request to TWS to succeed for {}: {}",
                            symbol.clone(),
                            e
                        )
                    })?;
                Box::new(data.bars.into_iter().map(BarView::Single))
            } else {
                let mut historical_bid_data = client
                    .historical_data(
                        &cloned_contract,
                        None,
                        duration,
                        bar_size,
                        HistoricalWhatToShow::Bid,
                        ibapi::prelude::TradingHours::Regular,
                    )
                    .map_err(|e| {
                        format!(
                            "Expected Historical Data Request to TWS to succeed for {}: {}",
                            symbol.clone(),
                            e
                        )
                    })?;
                let mut historical_ask_data = client
                    .historical_data(
                        &cloned_contract,
                        None,
                        duration,
                        bar_size,
                        HistoricalWhatToShow::Ask,
                        ibapi::prelude::TradingHours::Regular,
                    )
                    .map_err(|e| {
                        format!(
                            "Expected Historical Data Request to TWS to succeed for {}: {}",
                            symbol.clone(),
                            e
                        )
                    })?;

                let first_bid_bar = historical_bid_data
                    .bars
                    .first()
                    .clone()
                    .expect("Expected at least one bar in historical_bid_data!");
                let first_ask_bar = historical_ask_data
                    .bars
                    .first()
                    .clone()
                    .expect("Expected at least one bar in historical_ask_data!");
                let last_bid_bar = historical_bid_data
                    .bars
                    .last()
                    .clone()
                    .expect("Expected at least one bar in historical_bid_data!");
                let last_ask_bar = historical_ask_data
                    .bars
                    .last()
                    .clone()
                    .expect("Expected at least one bar in historical_ask_data!");
                if first_bid_bar.date != first_ask_bar.date {
                    let later_date = first_bid_bar.date.max(first_ask_bar.date);

                    let historical_bid_data_bars: Vec<ibapi::market_data::historical::Bar> =
                        historical_bid_data
                            .bars
                            .iter()
                            .cloned()
                            .filter(|bar| bar.date >= later_date)
                            .collect();
                    let historical_ask_data_bars: Vec<ibapi::market_data::historical::Bar> =
                        historical_ask_data
                            .bars
                            .iter()
                            .cloned()
                            .filter(|bar| bar.date >= later_date)
                            .collect();
                    if historical_ask_data_bars.len() != historical_bid_data_bars.len()
                        || historical_ask_data_bars.is_empty()
                    {
                        return Err("Completely unaligned historical_bid_data and historical_ask_data, pruned length not equal".to_string());
                    }
                    if !zip(&historical_bid_data_bars, &historical_ask_data_bars)
                        .all(|(bid_bar, ask_bar)| bid_bar.date == ask_bar.date)
                    {
                        return Err(
                            "misaligned historical_bid_data and historical_ask_data".to_string()
                        );
                    }
                    Box::new(
                        zip(historical_bid_data_bars, historical_ask_data_bars)
                            .map(|(bid, ask)| BarView::BidAsk { bid, ask }),
                    )
                } else if last_bid_bar.date != last_ask_bar.date {
                    if historical_ask_data.bars.len() < historical_bid_data.bars.len() {
                        let num_bars_diff =
                            historical_bid_data.bars.len() - historical_ask_data.bars.len();
                        let historical_ask_data_new = client
                            .historical_data(
                                &cloned_contract,
                                None,
                                ibapi::market_data::historical::Duration::seconds(
                                    (num_bars_diff * 60) as i32 + 60,
                                ),
                                bar_size,
                                HistoricalWhatToShow::Ask,
                                ibapi::prelude::TradingHours::Regular,
                            )
                            .map_err(|e| {
                                format!(
                                    "Expected Historical Data Request to TWS to succeed for {}: {}",
                                    symbol.clone(),
                                    e
                                )
                            })?;
                        let missing_date_early = last_ask_bar.date;
                        let missing_date_late = last_bid_bar.date;
                        historical_ask_data_new.bars.iter().for_each(|bar| {
                            if bar.date > missing_date_early && bar.date <= missing_date_late {
                                historical_ask_data.bars.push(*bar);
                            }
                        });

                        Box::new(
                            zip(historical_bid_data.bars, historical_ask_data.bars)
                                .map(|(bid, ask)| BarView::BidAsk { bid, ask }),
                        )
                    } else {
                        let num_bars_diff =
                            historical_ask_data.bars.len() - historical_bid_data.bars.len();
                        let historical_bid_data_new = client
                            .historical_data(
                                &cloned_contract,
                                None,
                                ibapi::market_data::historical::Duration::seconds(
                                    (num_bars_diff * 60) as i32 + 60,
                                ),
                                bar_size,
                                HistoricalWhatToShow::Bid,
                                ibapi::prelude::TradingHours::Regular,
                            )
                            .map_err(|e| {
                                format!(
                                    "Expected Historical Data Request to TWS to succeed for {}: {}",
                                    symbol.clone(),
                                    e
                                )
                            })?;
                        let missing_date_early = last_bid_bar.date;
                        let missing_date_late = last_ask_bar.date;
                        historical_bid_data_new.bars.iter().for_each(|bar| {
                            if bar.date > missing_date_early && bar.date <= missing_date_late {
                                historical_bid_data.bars.push(*bar);
                            }
                        });

                        Box::new(
                            zip(historical_bid_data.bars, historical_ask_data.bars)
                                .map(|(bid, ask)| BarView::BidAsk { bid, ask }),
                        )
                    }
                } else if historical_bid_data.bars.len() != historical_ask_data.bars.len() {
                    tracing::error!("Something is very fcking wrong!");
                    tracing::info!(
                        message=%format!(
                            "Historical Bid bars: \n{}",
                            historical_bid_data
                                .bars
                                .iter()
                                .take(10)
                                .map(|bar| {
                                    format!(
                                        "    Bar: {},{},{},{},{}",
                                        bar.open, bar.high, bar.low, bar.close, bar.date
                                    )
                                })
                                .collect::<Vec<String>>()
                                .join("\n")
                        )
                    );
                    tracing::info!(
                        message=%format!(
                            "...{}",
                            historical_bid_data
                                .bars
                                .iter()
                                .rev()
                                .take(10)
                                .rev()
                                .map(|bar| {
                                    format!(
                                        "    Bar: {},{},{},{},{}",
                                        bar.open, bar.high, bar.low, bar.close, bar.date
                                    )
                                })
                                .collect::<Vec<String>>()
                                .join("\n")
                        )
                    );
                    tracing::info!(
                        message=%format!(
                            "Historical Ask bars: \n{}",
                            historical_ask_data
                                .bars
                                .iter()
                                .take(10)
                                .map(|bar| {
                                    format!(
                                        "    Bar: {},{},{},{},{}",
                                        bar.open, bar.high, bar.low, bar.close, bar.date
                                    )
                                })
                                .collect::<Vec<String>>()
                                .join("\n")
                        )
                    );
                    tracing::info!(
                        message=%format!(
                            "...{}",
                            historical_ask_data
                                .bars
                                .iter()
                                .rev()
                                .take(10)
                                .rev()
                                .map(|bar| {
                                    format!(
                                        "    Bar: {},{},{},{},{}",
                                        bar.open, bar.high, bar.low, bar.close, bar.date
                                    )
                                })
                                .collect::<Vec<String>>()
                                .join("\n")
                        )
                    );
                    panic!("bid and ask are very misaligned - too tough to fix");
                } else {
                    Box::new(
                        zip(historical_bid_data.bars, historical_ask_data.bars)
                            .map(|(bid, ask)| BarView::BidAsk { bid, ask }),
                    )
                }
            };
            Ok(historical_data_bars)
        }).map_err(|e| format!("Error trying to get historical data: {e:?}"))?;

        let symbol = get_local_symbol(&contract);

        if apply_batching {
            if contract.security_type == SecurityType::Option {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(historical_options_data_crud.init_channel())
                });
            } else if !is_bid_ask {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(historical_data_crud.init_channel())
                });
            } else {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(historical_forex_data_crud.init_channel())
                });
            }
        }

        let mut join_handles = Vec::new();
        for bar_data in historical_data_bars {
            match bar_data {
                BarView::Single(bar) => {
                    if bar.date.unix_timestamp() == last_request_time_bar {
                        continue;
                    }
                    if contract.security_type == SecurityType::Option {
                        let historical_options_data_crud = historical_options_data_crud.clone();
                        let cloned_symbol = symbol.clone();
                        let hist_data_prim_keys = HistoricalOptionsDataPrimaryKeys {
                            stock: symbol.clone(),
                            primary_exchange: contract.primary_exchange.to_string(),
                            currency: contract.currency.to_string(),
                            expiry: contract.last_trade_date_or_contract_month.to_string(),
                            strike: contract.strike,
                            multiplier: contract.multiplier.to_string(),
                            option_type: OptionType::from_str(&contract.right)
                                .expect("Expected to be able to parse\
                                    contract right in update_at_least_n_days_data for option contract"),
                            time: DateTime::from_timestamp(
                                bar.date.unix_timestamp(),
                                bar.date.nanosecond() as u32,
                            )
                            .expect("Expected to be able to convert bar time to DateTime<Utc>"),
                        };
                        let insert_thread = tokio::spawn(async move {
                            if apply_batching {
                                if let Err(e) = historical_options_data_crud
                                    .batch_create_or_update(
                                        &crate::database::models::HistoricalOptionsDataFullKeys {
                                        stock: hist_data_prim_keys.stock,
                                        primary_exchange: hist_data_prim_keys.primary_exchange,
                                        currency: hist_data_prim_keys.currency,
                                        expiry: hist_data_prim_keys.expiry,
                                        strike: hist_data_prim_keys.strike,
                                        multiplier: hist_data_prim_keys.multiplier,
                                        option_type: hist_data_prim_keys.option_type,
                                        time: DateTime::from_timestamp(
                                            bar.date.unix_timestamp(),
                                            bar.date.nanosecond() as u32,
                                        )
                                        .expect("Expected to be able to convert bar time to DateTime<Utc>"),
                                            open: bar.open,
                                            high: bar.high,
                                            low: bar.low,
                                            close: bar.close,
                                            volume: Decimal::from_f64(bar.volume * 100.0).expect(
                                                "Expected to be able to parse f64 to Decimal",
                                            ),
                                        },
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        message=%format!(
                                            "Error occurred while upserting bars into historical data for {}: {}",
                                            cloned_symbol,
                                            e
                                        )
                                    )
                                }
                            } else {
                                if let Err(e) = historical_options_data_crud
                                    .create_or_update(
                                        &hist_data_prim_keys,
                                        &crate::database::models::HistoricalOptionsDataUpdateKeys {
                                            open: Some(bar.open),
                                            high: Some(bar.high),
                                            low: Some(bar.low),
                                            close: Some(bar.close),
                                            volume: Some(
                                                Decimal::from_f64(bar.volume * 100.0).expect(
                                                    "Expected to be able to parse f64 to Decimal",
                                                ),
                                            ),
                                        },
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        message=%format!(
                                            "Error occurred while upserting bars into historical data for {}: {}",
                                            cloned_symbol,
                                            e
                                        )
                                    )
                                }
                            }
                        });
                        join_handles.push(insert_thread);
                        continue;
                    }
                    let historical_data_crud = historical_data_crud.clone();
                    let cloned_symbol = symbol.clone();
                    let hist_data_prim_keys = HistoricalDataPrimaryKeys {
                        stock: symbol.clone(),
                        primary_exchange: contract.primary_exchange.to_string(),
                        currency: contract.currency.to_string(),
                        time: DateTime::from_timestamp(
                            bar.date.unix_timestamp(),
                            bar.date.nanosecond() as u32,
                        )
                        .expect("Expected to be able to convert bar time to DateTime<Utc>"),
                    };
                    let insert_thread = tokio::spawn(async move {
                        if apply_batching {
                            if let Err(e) = historical_data_crud
                                .batch_create_or_update(
                                    &crate::database::models::HistoricalDataFullKeys {
                                        stock: hist_data_prim_keys.stock,
                                        primary_exchange: hist_data_prim_keys.primary_exchange,
                                        currency: hist_data_prim_keys.currency,
                                        time: hist_data_prim_keys.time,
                                        open: bar.open,
                                        high: bar.high,
                                        low: bar.low,
                                        close: bar.close,
                                        volume: Decimal::from_f64(bar.volume * 100.0)
                                            .expect("Expected to be able to parse f64 to Decimal"),
                                    },
                                )
                                .await
                            {
                                tracing::error!(
                                    message=%format!(
                                        "Error occurred while upserting bars into historical data for {}: {}",
                                        cloned_symbol,
                                        e
                                    )
                                )
                            }
                        } else {
                            if let Err(e) = historical_data_crud
                                .create_or_update(
                                    &hist_data_prim_keys,
                                    &HistoricalDataUpdateKeys {
                                        open: Some(bar.open),
                                        high: Some(bar.high),
                                        low: Some(bar.low),
                                        close: Some(bar.close),
                                        volume: Some(
                                            Decimal::from_f64(bar.volume * 100.0).expect(
                                                "Expected to be able to parse f64 to Decimal",
                                            ),
                                        ),
                                    },
                                )
                                .await
                            {
                                tracing::error!(
                                    message=%format!(
                                        "Error occurred while upserting bars into historical data for {}: {}",
                                        cloned_symbol,
                                        e
                                    )
                                )
                            }
                        }
                    });
                    join_handles.push(insert_thread);
                }
                BarView::BidAsk {
                    bid: bid_bar,
                    ask: ask_bar,
                } => {
                    if bid_bar.date.unix_timestamp() == last_request_time_bar {
                        continue;
                    }
                    let historical_forex_data_crud = historical_forex_data_crud.clone();
                    let cloned_symbol = symbol.clone();
                    let insert_thread = tokio::spawn(async move {
                        if apply_batching {
                            if let Err(e) = historical_forex_data_crud
                                .batch_create_or_update(
                                    &crate::database::models::HistoricalForexDataFullKeys {
                                        pair: cloned_symbol.clone(),
                                        time: DateTime::from_timestamp(
                                            bid_bar.date.unix_timestamp(),
                                            bid_bar.date.nanosecond() as u32,
                                        )
                                        .expect(
                                            "Expected to be able to convert bar time to DateTime<Utc>",
                                        ),
                                        bid_open: Some(bid_bar.open),
                                        bid_high: Some(bid_bar.high),
                                        bid_low: Some(bid_bar.low),
                                        bid_close: Some(bid_bar.close),
                                        ask_open: Some(ask_bar.open),
                                        ask_high: Some(ask_bar.high),
                                        ask_low: Some(ask_bar.low),
                                        ask_close: Some(ask_bar.close),
                                    },
                                )
                                .await
                            {
                                tracing::error!(
                                    message=%format!(
                                        "Error occurred while upserting bars into historical data for {}: {}",
                                        cloned_symbol,
                                        e
                                    )
                                )
                            }
                        } else {
                            if let Err(e) = historical_forex_data_crud
                                .create_or_update(
                                    &crate::database::models::HistoricalForexDataPrimaryKeys {
                                        pair: cloned_symbol.clone(),
                                        time: DateTime::from_timestamp(
                                            bid_bar.date.unix_timestamp(),
                                            bid_bar.date.nanosecond() as u32,
                                        )
                                        .expect(
                                            "Expected to be able to convert bar time to DateTime<Utc>",
                                        ),
                                    },
                                    &HistoricalForexDataUpdateKeys {
                                        bid_open: Some(bid_bar.open),
                                        bid_high: Some(bid_bar.high),
                                        bid_low: Some(bid_bar.low),
                                        bid_close: Some(bid_bar.close),
                                        ask_open: Some(ask_bar.open),
                                        ask_high: Some(ask_bar.high),
                                        ask_low: Some(ask_bar.low),
                                        ask_close: Some(ask_bar.close),
                                    },
                                )
                                .await
                            {
                                tracing::error!(
                                    message=%format!(
                                        "Error occurred while upserting bars into historical data for {}: {}",
                                        cloned_symbol,
                                        e
                                    )
                                )
                            }
                        }
                    });
                    join_handles.push(insert_thread);
                }
            }
        }

        // tokio::runtime::Handle::current().block_on(async {
        //     for h in join_handles {
        //         h.await.expect("some insertion thread panicked!");
        //     }
        // });
        // futures::join_all!(join_handles);
        // futures::future::join_all(iter)

        for h in join_handles {
            if let Err(e) =
                tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(h))
            {
                tracing::error!("Some stupid thread panicked while getting historical_data: {e:?}",);
            };
        }

        if apply_batching {
            if contract.security_type == SecurityType::Option {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(historical_options_data_crud.close_channel())
                });
            } else if !is_bid_ask {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(historical_data_crud.close_channel())
                });
            } else {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(historical_forex_data_crud.close_channel())
                });
            }
        }

        Ok(())
    }
}
