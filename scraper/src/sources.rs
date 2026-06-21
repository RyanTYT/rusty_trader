// scraper/src/sources.rs
//
// Central registry of all news sources.
// Rss  = fast path (reqwest + feed-rs)
// Playwright = JS-heavy sites that require browser rendering

#[derive(Clone, Debug)]
pub enum FetchMethod {
    Rss,
    Playwright,
}

#[derive(Clone, Debug)]
pub struct Source {
    pub name: &'static str,
    pub url: &'static str,
    pub method: FetchMethod,
    /// CSS selectors used by the Playwright scraper to find article cards
    /// Format: (container_selector, title_selector, link_selector, body_selector)
    pub selectors: Option<(&'static str, &'static str, &'static str, &'static str)>,
    /// Max articles to collect per run (prevents token explosion downstream)
    pub max_articles: usize,
    pub market: Market,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Market {
    Us,
    Uk,
    Japan,
    Korea,
    Global,
}

pub fn us_sources() -> Vec<Source> {
    vec![
        // ── RSS fast path ────────────────────────────────────────────────
        Source {
            name: "CNBC Markets RSS",
            url: "https://www.cnbc.com/id/100003114/device/rss/rss.html",
            method: FetchMethod::Rss,
            selectors: None,
            max_articles: 30,
            market: Market::Us,
        },
        // Source {
        //     name: "Reuters Business RSS",
        //     url: "https://feeds.reuters.com/reuters/businessNews",
        //     method: FetchMethod::Rss,
        //     selectors: None,
        //     max_articles: 30,
        //     market: Market::Global,
        // },
        Source {
            name: "Bloomberg Markets RSS",
            // Bloomberg public RSS - headlines + first paragraph available
            url: "https://feeds.bloomberg.com/markets/news.rss",
            method: FetchMethod::Rss,
            selectors: None,
            max_articles: 30,
            market: Market::Global,
        },
        Source {
            name: "WSJ Markets RSS",
            url: "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
            method: FetchMethod::Rss,
            selectors: None,
            max_articles: 25,
            market: Market::Us,
        },
        Source {
            name: "FreightWaves RSS",
            // Supply chain / logistics — catches bullwhip signals early
            url: "https://www.freightwaves.com/news/feed",
            method: FetchMethod::Rss,
            selectors: None,
            max_articles: 20,
            market: Market::Global,
        },
        Source {
            name: "Supply Chain Brain RSS",
            url: "https://www.supplychainbrain.com/rss/articles",
            method: FetchMethod::Rss,
            selectors: None,
            max_articles: 15,
            market: Market::Global,
        },
        // ── SEC EDGAR filings (8-K, 13-F, 10-Q) ─────────────────────────
        // Current reports (8-K) filed today — look for material events
        Source {
            name: "SEC EDGAR 8-K RSS",
            url: "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&dateb=&owner=include&count=40&search_text=&output=atom",
            method: FetchMethod::Rss,
            selectors: None,
            max_articles: 40,
            market: Market::Us,
        },
        // 13-F fund filings — new institutional positions
        Source {
            name: "SEC EDGAR 13-F RSS",
            url: "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=13-F&dateb=&owner=include&count=20&search_text=&output=atom",
            method: FetchMethod::Rss,
            selectors: None,
            max_articles: 20,
            market: Market::Us,
        },
        // ── Playwright JS-heavy ──────────────────────────────────────────
        Source {
            name: "SeekingAlpha Markets",
            url: "https://seekingalpha.com/market-news",
            method: FetchMethod::Playwright,
            selectors: Some((
                "div[data-test-id='post-list'] > article",
                // "section[data-test-id='cards-container'] ",
                "h3",
                "h3 > a",
                "p[data-test-id='post-list-item-summary']",
            )),
            max_articles: 30,
            market: Market::Us,
        },
        // Source {
        //     name: "Barrons",
        //     url: "https://www.barrons.com/market-data",
        //     method: FetchMethod::Playwright,
        //     selectors: Some((
        //         "article.WSJTheme--story--XB4V2mwX",
        //         "h3.WSJTheme--headline--7VCzo7Ay",
        //         "a",
        //         "p.WSJTheme--summary--lmOSe5qd",
        //     )),
        //     max_articles: 20,
        //     market: Market::Us,
        // },
    ]
}

pub fn uk_sources() -> Vec<Source> {
    vec![
        Source {
            name: "FT Markets RSS",
            url: "https://www.ft.com/rss/home/uk",
            method: FetchMethod::Rss,
            selectors: None,
            max_articles: 25,
            market: Market::Uk,
        },
        // Source {
        //     name: "Reuters UK RSS",
        //     url: "https://feeds.reuters.com/reuters/UKbusinessNews",
        //     method: FetchMethod::Rss,
        //     selectors: None,
        //     max_articles: 25,
        //     market: Market::Uk,
        // },
        Source {
            name: "The Information",
            // Tech industry deep dives — RSS available for headlines
            url: "https://www.theinformation.com/feed",
            method: FetchMethod::Rss,
            selectors: None,
            max_articles: 15,
            market: Market::Global,
        },
    ]
}

pub fn asia_sources() -> Vec<Source> {
    vec![
        Source {
            name: "Nikkei Asia RSS",
            url: "https://asia.nikkei.com/rss/feed/nar",
            method: FetchMethod::Rss,
            selectors: None,
            max_articles: 25,
            market: Market::Japan,
        },
        // Source {
        //     name: "Korea JoongAng Daily Business",
        //     url: "https://koreajoongangdaily.joins.com/rss/feeds/business.xml",
        //     method: FetchMethod::Rss,
        //     selectors: None,
        //     max_articles: 20,
        //     market: Market::Korea,
        // },
        // Source {
        //     name: "Reuters Asia RSS",
        //     url: "https://feeds.reuters.com/reuters/AsianMarkets",
        //     method: FetchMethod::Rss,
        //     selectors: None,
        //     max_articles: 20,
        //     market: Market::Japan,
        // },
    ]
}
