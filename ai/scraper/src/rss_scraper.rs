// scraper/src/rss_scraper.rs
use chrono::Utc;
use tracing::debug;

use crate::dedup::DedupStore;
use crate::sources::Source;
use crate::types::Article;

pub fn create_random_ua_client(i: usize) -> Result<reqwest::Client, String> {
    // Pick a random User-Agent from the slice
    let ua_list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Mobile/15E148 Safari/604.1",
    ];

    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .user_agent(ua_list[i])
        .build()
        .map_err(|e| format!("Failed to build client for reqwest: {e:?}"))
}

pub async fn fetch(source: &Source) -> Result<Vec<Article>, String> {
    let mut client = if source.name.contains("SEC") {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .user_agent("Mozilla/5.0 (compatible; AutoTrader-Scraper/1.0)")
            // .user_agent("TheDeep/1.0 (contact@yourdomain.com)")
            .build()
            .map_err(|e| format!("Failed to build client for reqwest: {e:?}"))?
    } else {
        // reqwest::Client::builder()
        //     .timeout(std::time::Duration::from_secs(15))
        //     // .user_agent("Mozilla/5.0 (compatible; AutoTrader-Scraper/1.0)")
        //     .user_agent(
        //         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\
        //                 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        //     )
        //     .build()
        //     .map_err(|e| format!("Failed to build client for reqwest: {e:?}"))?
        create_random_ua_client(0)
            .map_err(|e| format!("Failed to build client for reqwest: {e:?}"))?
    };

    let url = source.url;
    let mut feed_opt = None;
    for i in 0..5 {
        if i == 3 {
            client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .user_agent("Mozilla/5.0 (compatible; AutoTrader-Scraper/1.0)")
                .build()
                .map_err(|e| format!("Failed to build client for reqwest: {e:?}"))?;
        } else if i == 4 {
            client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .user_agent("TheDeep/1.0 (contact@yourdomain.com)")
                .build()
                .map_err(|e| format!("Failed to build client for reqwest: {e:?}"))?;
        } else if !source.name.contains("SEC") {
            client = create_random_ua_client(i)
                .map_err(|e| format!("Failed to build client for reqwest: {e:?}"))?
        }

        let response = client
            .get(source.url)
            .send()
            .await
            .map_err(|e| format!("Failed to make GET reqwest to {url:?}: {e:?}"))?;
        let bytes = response
            .bytes()
            .await
            .map_err(|e| format!("Failed to receive response bytes: {e:?}"))?;

        match feed_rs::parser::parse(bytes.as_ref()) {
            Ok(res) => {
                feed_opt = Some(res);
                break;
            }
            Err(e) => {
                tracing::warn!("Failed to parse rss result as bytes: {e:?}");
                continue;
            }
        };
    }
    if feed_opt.is_none() {
        tracing::error!("Failed to parse rss result as bytes for {url:?}");
        return Err(format!("Failed to parse rss result as bytes for {url:?}"));
    }

    let feed = feed_opt.unwrap();
    let mut articles: Vec<Article> = Vec::new();

    for entry in feed.entries.iter().take(source.max_articles) {
        let title = entry
            .title
            .as_ref()
            .map(|t| t.content.clone())
            .unwrap_or_default();

        let url = entry
            .links
            .first()
            .map(|l| l.href.clone())
            .unwrap_or_default()
            .to_string();

        if title.is_empty() || url.is_empty() {
            continue;
        }

        let summary = entry
            .summary
            .as_ref()
            .and_then(|s| {
                // Strip HTML tags from RSS summaries
                match html2text::from_read(s.content.as_bytes(), 500) {
                    Ok(text) => Some(text.chars().take(600).collect::<String>()),
                    Err(e) => {
                        tracing::error!("Failed to convert HTML to text: {e:?}");
                        None
                    }
                }
            })
            .or_else(|| {
                entry.content.as_ref().and_then(|c| {
                    c.body
                        .as_ref()
                        .and_then(|b| match html2text::from_read(b.as_bytes(), 500) {
                            Ok(text) => Some(text.chars().take(600).collect::<String>()),
                            Err(e) => {
                                tracing::error!("Failed to read bytes via html2text: {e:?}");
                                None
                            }
                        })
                })
            })
            .unwrap_or_default();

        let published_at = entry.published.or(entry.updated);

        // Skip articles older than 48 hours — keeps knowledge base fresh
        if let Some(pub_time) = published_at {
            let age = Utc::now().signed_duration_since(pub_time);
            if age.num_hours() > 48 {
                debug!("Skipping old article: {} ({}h old)", title, age.num_hours());
                continue;
            }
        }

        // Detect SEC filing type from title
        let filing_type = detect_filing_type(&title, &url);
        let ticker = extract_ticker_from_sec_url(&url);

        let id = DedupStore::hash(&url, &title);

        articles.push(Article {
            id,
            source: source.name.to_string(),
            market: format!("{:?}", source.market).to_lowercase(),
            url,
            title,
            summary,
            full_text: None, // RSS doesn't give us full text — LLM will fetch if needed
            published_at,
            scraped_at: Utc::now(),
            filing_type,
            ticker,
        });
    }

    debug!("RSS {}: fetched {} articles", source.name, articles.len());
    Ok(articles)
}

fn detect_filing_type(title: &str, url: &str) -> Option<String> {
    let combined = format!("{} {}", title, url).to_uppercase();
    for t in &["13-F", "8-K", "10-Q", "10-K", "S-1", "DEF 14A"] {
        if combined.contains(t) {
            return Some(t.to_string());
        }
    }
    None
}

fn extract_ticker_from_sec_url(url: &str) -> Option<String> {
    // SEC EDGAR URLs sometimes contain CIK but not ticker — best effort
    // e.g. https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=AAPL
    let re = regex::Regex::new(r"CIK=([A-Z]{1,5})\b").ok()?;
    re.captures(url)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
}
