//! Tests for `src/market_data/traits/current_price.rs`.
//!
//! Covered here (pure logic, no IB/DB/network):
//!   - `Consolidator::yahoo_ticker_from_contract` (:288)
//!   - `Consolidator::is_ibkr_market_data_error` (:396)
//!   - `HistoricalDataConfig::new` (:42) — construction only; its fields are
//!     private with no accessors, so field *values* are not observable here.
//!   - `impl<T> PriceSupplier for Arc<T>` (:655)
//!
//! NOT covered: `Consolidator::_extract_price` (:418). Its signature requires a
//! `&Subscription<TickTypes>`, and the ibapi fork offers no way to construct one
//! from outside that crate:
//!   - `Subscription::new` is `pub(crate)` (rust-ibapi-fork src/subscriptions/sync.rs:38)
//!   - `StreamDecoder` (the generic bound) is `pub(crate)` (src/subscriptions/mod.rs:4)
//!   - the only public producers are `Client::market_data(..).subscribe()` (needs a
//!     live connection) and `MessageBusStub`, which is `pub(crate)`.
//! Covering it externally would need a src-level helper that fabricates a
//! subscription; no src edits were made.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use ibapi::contracts::{Contract, Currency, Exchange, SecurityType, Symbol};
use ibapi::market_data::historical::{BarSize, Duration, WhatToShow};
use trading_app::market_data::consolidator::Consolidator;
use trading_app::market_data::traits::current_price::{HistoricalDataConfig, PriceSupplier};

// ──────────────────────────── helpers ────────────────────────────

fn contract(
    symbol: &str,
    security_type: SecurityType,
    exchange: &str,
    primary_exchange: &str,
    currency: &str,
) -> Contract {
    Contract {
        symbol: Symbol::from(symbol),
        security_type,
        exchange: Exchange::from(exchange),
        primary_exchange: Exchange::from(primary_exchange),
        currency: Currency::from(currency),
        ..Default::default()
    }
}

fn stock(symbol: &str, exchange: &str, primary_exchange: &str, currency: &str) -> Contract {
    contract(
        symbol,
        SecurityType::Stock,
        exchange,
        primary_exchange,
        currency,
    )
}

// ──────────────── yahoo_ticker_from_contract (:288) ────────────────

struct YahooCase {
    symbol: &'static str,
    security_type: SecurityType,
    exchange: &'static str,
    primary_exchange: &'static str,
    currency: &'static str,
    expected: Option<&'static str>,
}

fn yc(
    symbol: &'static str,
    security_type: SecurityType,
    exchange: &'static str,
    primary_exchange: &'static str,
    currency: &'static str,
    expected: Option<&'static str>,
) -> YahooCase {
    YahooCase {
        symbol,
        security_type,
        exchange,
        primary_exchange,
        currency,
        expected,
    }
}

/// Every branch of the exchange → Yahoo-suffix table, plus the US shortcut and
/// the `SMART` → `primary_exchange` routing.
#[test]
fn yahoo_ticker_from_contract_exchange_table() {
    use SecurityType::{Crypto, ForexPair, Future, Stock};

    let cases: &[YahooCase] = &[
        // ── US venues: bare symbol, no suffix ──
        yc("AAPL", Stock, "SMART", "NASDAQ", "USD", Some("AAPL")),
        yc("AAPL", Stock, "SMART", "NYSE", "USD", Some("AAPL")),
        yc("AAPL", Stock, "SMART", "", "USD", Some("AAPL")),
        yc("AAPL", Stock, "NASDAQ", "", "USD", Some("AAPL")),
        yc("AAPL", Stock, "NYSE", "", "USD", Some("AAPL")),
        yc("AAPL", Stock, "ARCA", "", "USD", Some("AAPL")),
        yc("AAPL", Stock, "BATS", "", "USD", Some("AAPL")),
        yc("AAPL", Stock, "CBOE", "", "USD", Some("AAPL")),
        yc("AAPL", Stock, "AMEX", "", "USD", Some("AAPL")),
        yc("AAPL", Stock, "ISLAND", "", "USD", Some("AAPL")),
        yc("AAPL", Stock, "DRCTEDGE", "", "USD", Some("AAPL")),
        yc("AAPL", Stock, "BEX", "", "USD", Some("AAPL")),
        yc("AAPL", Stock, "NSX", "", "USD", Some("AAPL")),
        yc("AAPL", Stock, "PSX", "", "USD", Some("AAPL")),
        yc("AAPL", Stock, "IEX", "", "USD", Some("AAPL")),
        // Empty exchange is treated as US only when currency == USD.
        yc("AAPL", Stock, "", "", "USD", Some("AAPL")),
        // Symbols are passed through verbatim (spaces included).
        yc("BRK B", Stock, "SMART", "NYSE", "USD", Some("BRK B")),
        // SMART routes through primary_exchange, not the SMART literal.
        yc("VOD", Stock, "SMART", "LSE", "GBP", Some("VOD.L")),
        yc("7203", Stock, "SMART", "TSEJ", "JPY", Some("7203.T")),
        // ── Asia-Pacific ──
        yc("7203", Stock, "TSEJ", "", "JPY", Some("7203.T")),
        yc("7203", Stock, "XTKS", "", "JPY", Some("7203.T")),
        yc("005930", Stock, "KSE", "", "KRW", Some("005930.KS")),
        yc("005930", Stock, "XKRX", "", "KRW", Some("005930.KS")),
        yc("247540", Stock, "KOSDAQ", "", "KRW", Some("247540.KQ")),
        yc("0700", Stock, "SEHK", "", "HKD", Some("0700.HK")),
        yc("0700", Stock, "XHKG", "", "HKD", Some("0700.HK")),
        yc("D05", Stock, "SGX", "", "SGD", Some("D05.SI")),
        yc("D05", Stock, "XSES", "", "SGD", Some("D05.SI")),
        yc("BHP", Stock, "ASX", "", "AUD", Some("BHP.AX")),
        yc("BHP", Stock, "XASX", "", "AUD", Some("BHP.AX")),
        yc("RELIANCE", Stock, "NSE", "", "INR", Some("RELIANCE.NS")),
        yc("RELIANCE", Stock, "XNSE", "", "INR", Some("RELIANCE.NS")),
        yc("500325", Stock, "BSE", "", "INR", Some("500325.BO")),
        yc("500325", Stock, "XBOM", "", "INR", Some("500325.BO")),
        yc("2330", Stock, "TWSE", "", "TWD", Some("2330.TW")),
        yc("2330", Stock, "XTAI", "", "TWD", Some("2330.TW")),
        yc("600519", Stock, "XSHG", "", "CNY", Some("600519.SS")),
        yc("000001", Stock, "XSHE", "", "CNY", Some("000001.SZ")),
        // ── Europe ──
        yc("VOD", Stock, "LSE", "", "GBP", Some("VOD.L")),
        yc("VOD", Stock, "XLON", "", "GBP", Some("VOD.L")),
        yc("SAP", Stock, "IBIS", "", "EUR", Some("SAP.DE")),
        yc("SAP", Stock, "XETR", "", "EUR", Some("SAP.DE")),
        yc("SAP", Stock, "XFRA", "", "EUR", Some("SAP.DE")),
        yc("MC", Stock, "SBF", "", "EUR", Some("MC.PA")),
        yc("MC", Stock, "XPAR", "", "EUR", Some("MC.PA")),
        yc("EDP", Stock, "BVL", "", "EUR", Some("EDP.LS")),
        yc("EDP", Stock, "XLIS", "", "EUR", Some("EDP.LS")),
        yc("ASML", Stock, "AEB", "", "EUR", Some("ASML.AS")),
        yc("ASML", Stock, "XAMS", "", "EUR", Some("ASML.AS")),
        yc("ENI", Stock, "BVME", "", "EUR", Some("ENI.MI")),
        yc("ENI", Stock, "XMIL", "", "EUR", Some("ENI.MI")),
        yc("SAN", Stock, "BME", "", "EUR", Some("SAN.MC")),
        yc("SAN", Stock, "XMAD", "", "EUR", Some("SAN.MC")),
        yc("NESN", Stock, "SWX", "", "CHF", Some("NESN.SW")),
        yc("NESN", Stock, "XVTX", "", "CHF", Some("NESN.SW")),
        yc("VOLVB", Stock, "OMXS", "", "SEK", Some("VOLVB.ST")),
        yc("VOLVB", Stock, "XSTO", "", "SEK", Some("VOLVB.ST")),
        yc("NOKIA", Stock, "OMXH", "", "EUR", Some("NOKIA.HE")),
        yc("NOKIA", Stock, "XHEL", "", "EUR", Some("NOKIA.HE")),
        yc("NOVO B", Stock, "OMXC", "", "DKK", Some("NOVO B.CO")),
        yc("NOVO B", Stock, "XCSE", "", "DKK", Some("NOVO B.CO")),
        yc("EQNR", Stock, "OSE", "", "NOK", Some("EQNR.OL")),
        yc("EQNR", Stock, "XOSL", "", "NOK", Some("EQNR.OL")),
        yc("OMV", Stock, "VSE", "", "EUR", Some("OMV.VI")),
        yc("OMV", Stock, "XWBO", "", "EUR", Some("OMV.VI")),
        yc("UCB", Stock, "XBRU", "", "EUR", Some("UCB.BR")),
        yc("CEZ", Stock, "XPRA", "", "CZK", Some("CEZ.PR")),
        yc("PKN", Stock, "XWAR", "", "PLN", Some("PKN.WA")),
        yc("OTP", Stock, "XBUD", "", "HUF", Some("OTP.BD")),
        // ── Canada / LatAm ──
        yc("SHOP", Stock, "TSX", "", "CAD", Some("SHOP.TO")),
        yc("SHOP", Stock, "XTSE", "", "CAD", Some("SHOP.TO")),
        yc("ABC", Stock, "TSXV", "", "CAD", Some("ABC.V")),
        yc("ABC", Stock, "XTSX", "", "CAD", Some("ABC.V")),
        yc("ABC", Stock, "NEO", "", "CAD", Some("ABC.NE")),
        yc("WALMEX", Stock, "MEXI", "", "MXN", Some("WALMEX.MX")),
        yc("WALMEX", Stock, "XMEX", "", "MXN", Some("WALMEX.MX")),
        yc("PETR4", Stock, "BOVESPA", "", "BRL", Some("PETR4.SA")),
        yc("PETR4", Stock, "BVMF", "", "BRL", Some("PETR4.SA")),
        // ── Unsupported / unmappable → None ──
        yc("EUR.USD", ForexPair, "IDEALPRO", "", "USD", None),
        yc("USD.SGD", ForexPair, "IDEALPRO", "", "SGD", None),
        yc("EUR.USD", ForexPair, "SMART", "IDEALPRO", "USD", None),
        // "TSE" is not one of the two Tokyo codes the table maps.
        yc("7203", Stock, "TSE", "", "JPY", None),
        // Matching is exact and case-sensitive.
        yc("AAPL", Stock, "smart", "", "USD", None),
        yc("AAPL", Stock, "SMART", "NASDAQ.US", "USD", None),
        // Empty exchange is not a free pass unless the currency is USD.
        yc("AAPL", Stock, "", "", "EUR", None),
        yc("ES", Future, "GLOBEX", "", "USD", None),
        yc("ES", Future, "CME", "", "USD", None),
        yc("AAPL", Stock, "PINK", "", "USD", None),
        yc("BTC", Crypto, "PAXOS", "", "USD", None),
    ];

    for case in cases {
        let c = contract(
            case.symbol,
            case.security_type.clone(),
            case.exchange,
            case.primary_exchange,
            case.currency,
        );
        assert_eq!(
            Consolidator::yahoo_ticker_from_contract(&c),
            case.expected.map(str::to_string),
            "symbol={} sec_type={} exchange={} primary={} currency={}",
            case.symbol,
            case.security_type,
            case.exchange,
            case.primary_exchange,
            case.currency,
        );
    }
}

/// `security_type` is checked: an option must never resolve to the underlying's
/// ticker, because the IBKR-access-denied fallback would price it with the
/// stock's quote.
#[test]
fn yahoo_ticker_from_contract_rejects_options() {
    let call_option = Contract {
        right: "C".to_string(),
        strike: 200.0,
        last_trade_date_or_contract_month: "20261218".to_string(),
        ..stock("AAPL", "SMART", "NASDAQ", "USD")
    };
    let put_option = Contract {
        right: "P".to_string(),
        strike: 150.0,
        last_trade_date_or_contract_month: "20270115".to_string(),
        ..stock("AAPL", "SMART", "NASDAQ", "USD")
    };

    // BUG-1 fix: an option resolves to no Yahoo ticker at all. Falling back to
    // the underlying's quote would price the option wrongly.
    assert_eq!(
        Consolidator::yahoo_ticker_from_contract(&Contract {
            security_type: SecurityType::Option,
            ..call_option
        }),
        None
    );
    assert_eq!(
        Consolidator::yahoo_ticker_from_contract(&Contract {
            security_type: SecurityType::Option,
            ..put_option
        }),
        None
    );

    // Same for bonds / indices on a US venue: symbol only, no Yahoo-specific
    // encoding (Yahoo indices need a leading '^', which is never added).
    assert_eq!(
        Consolidator::yahoo_ticker_from_contract(&Contract {
            security_type: SecurityType::Bond,
            ..stock("US912828", "SMART", "NYSE", "USD")
        }),
        Some("US912828".to_string())
    );
    assert_eq!(
        Consolidator::yahoo_ticker_from_contract(&Contract {
            security_type: SecurityType::Index,
            ..stock("SPX", "SMART", "CBOE", "USD")
        }),
        Some("SPX".to_string())
    );
}

/// The US-venue branch does not check currency; only the empty-exchange branch
/// does. Documents the exact precedence of the two conditions.
#[test]
fn yahoo_ticker_from_contract_us_branch_ignores_currency() {
    assert_eq!(
        Consolidator::yahoo_ticker_from_contract(&stock("7203", "NYSE", "", "JPY")),
        Some("7203".to_string())
    );
    assert_eq!(
        Consolidator::yahoo_ticker_from_contract(&stock("7203", "NYSE", "TSEJ", "JPY")),
        Some("7203".to_string())
    );
    assert_eq!(
        Consolidator::yahoo_ticker_from_contract(&stock("7203", "", "", "JPY")),
        None
    );
}

// ───────────── is_ibkr_market_data_error (:396) ─────────────

/// Case-insensitive text patterns plus exact numeric tokens for
/// 200 / 354 / 10090.
#[test]
fn is_ibkr_market_data_error_table() {
    let cases: &[(&str, bool)] = &[
        // ── true: subscription / permission problems ──
        ("Requested market data is not subscribed", true),
        (
            "Part of requested market data is not subscribed. Delayed market data is available",
            true,
        ),
        ("NOT SUBSCRIBED", true),
        ("error: Not Subscribed to market data", true),
        ("No security definition has been found for the request", true),
        ("ERROR 200: NO SECURITY DEFINITION has been found", true),
        ("Error 354: Requested market data is not subscribed", true),
        ("Error 10090: Part of requested market data is not subscribed", true),
        ("Market data farm connection is OK:usfarm", true),
        ("Market Data Farm connection is broken:usfarm", true),
        ("market data farm is connecting:usfarm", true),
        // ── true via exact numeric tokens ──
        ("354", true),
        ("10090", true),
        ("error 200", true),
        ("Error 1354 while requesting", false),
        ("code=100901", false),
        ("Error 3540 while requesting", false),
        // ── false: connectivity / unrelated / near misses ──
        ("", false),
        ("1009", false),
        ("1016", false),
        ("Error 10167", false),
        ("Timed out!", false),
        ("Failed to request current price from IBKR: Io(ConnectionRefused)", false),
        ("Connection reset by peer", false),
        ("not-subscribed", false),
        ("subscribed", false),
        ("market data", false),
        ("farm connection is OK", false),
        ("No security", false),
        ("security definition", false),
        ("Requested data is unavailable", false),
    ];

    for (input, expected) in cases {
        assert_eq!(
            Consolidator::is_ibkr_market_data_error(input),
            *expected,
            "input={input:?}"
        );
    }
}

// ─────────────── HistoricalDataConfig::new (:42) ───────────────

/// `new` is a field-for-field constructor (no defaults, no validation), but its
/// fields are private with no accessors, so all that can be asserted from an
/// external test is construction over every supported enum form plus the
/// `Send + 'static` property the callers rely on.
#[test]
fn historical_data_config_new_constructs_every_supported_form() {
    fn assert_send_static<T: Send + 'static>() {}
    assert_send_static::<HistoricalDataConfig>();

    let cases = [
        (Duration::from("1 D"), BarSize::Min, WhatToShow::Trades, true),
        (Duration::from("2 W"), BarSize::Min5, WhatToShow::MidPoint, false),
        (Duration::from("6 M"), BarSize::Hour, WhatToShow::Bid, true),
        (Duration::from("1 Y"), BarSize::Day, WhatToShow::Ask, false),
        (Duration::from("3600 S"), BarSize::Sec, WhatToShow::BidAsk, true),
    ];

    let cfgs: Vec<HistoricalDataConfig> = cases
        .into_iter()
        .map(|(duration, bar_size, what_to_show, use_batching)| {
            HistoricalDataConfig::new(duration, bar_size, what_to_show, use_batching)
        })
        .collect();

    // Five separately-constructed values, none aliased.
    assert_eq!(cfgs.len(), 5);
    for i in 0..cfgs.len() {
        for j in 0..cfgs.len() {
            assert_eq!(std::ptr::eq(&cfgs[i], &cfgs[j]), i == j);
        }
    }
}

// ───────── impl<T> PriceSupplier for Arc<T> (:655) ─────────

#[derive(Debug, PartialEq)]
struct ObservedPriceCall {
    contract: Contract,
    vwap: bool,
    generic_ticks: Vec<String>,
}

struct FakePriceSupplier {
    price: f64,
    error: Option<String>,
    price_calls: AtomicUsize,
    observed_price_calls: Mutex<Vec<ObservedPriceCall>>,
    hist_calls: AtomicUsize,
    /// `(contract, address of the `&HistoricalDataConfig` it was handed)`
    observed_hist_calls: Mutex<Vec<(Contract, usize)>>,
}

impl FakePriceSupplier {
    fn ok(price: f64) -> Self {
        Self {
            price,
            error: None,
            price_calls: AtomicUsize::new(0),
            observed_price_calls: Mutex::new(Vec::new()),
            hist_calls: AtomicUsize::new(0),
            observed_hist_calls: Mutex::new(Vec::new()),
        }
    }

    fn failing(message: &str) -> Self {
        Self {
            error: Some(message.to_string()),
            ..Self::ok(0.0)
        }
    }
}

#[async_trait::async_trait]
impl PriceSupplier for FakePriceSupplier {
    fn get_current_price(
        &self,
        contract: Contract,
        vwap: bool,
        generic_ticks: &[&str],
    ) -> Result<f64, String> {
        self.price_calls.fetch_add(1, Ordering::SeqCst);
        self.observed_price_calls
            .lock()
            .unwrap()
            .push(ObservedPriceCall {
                contract,
                vwap,
                generic_ticks: generic_ticks.iter().map(|s| (*s).to_string()).collect(),
            });

        match &self.error {
            Some(message) => Err(message.clone()),
            None => Ok(self.price),
        }
    }

    #[cfg(not(feature = "backtest"))]
    async fn populate_historical_data(
        &self,
        contract: &Contract,
        config: &HistoricalDataConfig,
    ) -> Result<(), String> {
        self.hist_calls.fetch_add(1, Ordering::SeqCst);
        self.observed_hist_calls
            .lock()
            .unwrap()
            .push((contract.clone(), config as *const HistoricalDataConfig as usize));
        Ok(())
    }
}

#[test]
fn arc_price_supplier_forwards_get_current_price_to_inner() {
    let inner = Arc::new(FakePriceSupplier::ok(42.25));
    let contract = stock("AAPL", "SMART", "NASDAQ", "USD");

    let out = <Arc<FakePriceSupplier> as PriceSupplier>::get_current_price(
        &inner,
        contract.clone(),
        true,
        &["233", "2"],
    );

    assert_eq!(out, Ok(42.25));
    assert_eq!(inner.price_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *inner.observed_price_calls.lock().unwrap(),
        vec![ObservedPriceCall {
            contract,
            vwap: true,
            generic_ticks: vec!["233".to_string(), "2".to_string()],
        }]
    );
}

#[test]
fn arc_price_supplier_forwards_zero_price_verbatim() {
    let inner = Arc::new(FakePriceSupplier::ok(0.0));

    let out = <Arc<FakePriceSupplier> as PriceSupplier>::get_current_price(
        &inner,
        stock("AAPL", "SMART", "NASDAQ", "USD"),
        false,
        &[],
    );

    assert_eq!(out, Ok(0.0));
    assert_eq!(inner.price_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        inner.observed_price_calls.lock().unwrap()[0].generic_ticks,
        Vec::<String>::new()
    );
}

#[test]
fn arc_price_supplier_forwards_error_verbatim() {
    let inner = Arc::new(FakePriceSupplier::failing("IBKR unavailable"));

    let out = <Arc<FakePriceSupplier> as PriceSupplier>::get_current_price(
        &inner,
        stock("AAPL", "SMART", "NASDAQ", "USD"),
        false,
        &["2"],
    );

    assert_eq!(out, Err("IBKR unavailable".to_string()));
    assert_eq!(inner.price_calls.load(Ordering::SeqCst), 1);
}

#[cfg(not(feature = "backtest"))]
#[tokio::test]
async fn arc_price_supplier_forwards_populate_historical_data_to_inner() {
    let inner = Arc::new(FakePriceSupplier::ok(1.0));
    let contract = stock("7203", "SMART", "TSEJ", "JPY");
    let config = HistoricalDataConfig::new(
        Duration::from("5 D"),
        BarSize::Min5,
        WhatToShow::Trades,
        false,
    );
    let config_addr = &config as *const HistoricalDataConfig as usize;

    let out = <Arc<FakePriceSupplier> as PriceSupplier>::populate_historical_data(
        &inner,
        &contract,
        &config,
    )
    .await;

    assert_eq!(out, Ok(()));
    assert_eq!(inner.hist_calls.load(Ordering::SeqCst), 1);
    let observed = inner.observed_hist_calls.lock().unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].0, contract);
    // The Arc impl passes the caller's config straight through — same address.
    assert_eq!(observed[0].1, config_addr);

    // The config is still owned by this test after the call.
    assert_eq!(&config as *const HistoricalDataConfig as usize, config_addr);
}
