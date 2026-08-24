// scraper/src/playwright_scraper.rs
//
// Handles JS-heavy sites (SeekingAlpha, Barrons) that block simple HTTP scrapers.
// Uses a shared Playwright browser instance (launched once in main.rs).
// Each scrape gets a fresh BrowserContext with stealth headers.

use std::collections::HashMap;

use chrono::Utc;
// use playwright::api::Browser;
use playwright_rs::{Browser, BrowserContextOptions};
use tracing::debug;

use crate::dedup::DedupStore;
use crate::sources::Source;
use crate::types::Article;

struct BrowserGuard {
    ctx: playwright_rs::BrowserContext,
    page: playwright_rs::Page,
    rt: tokio::runtime::Handle,
}

impl BrowserGuard {
    async fn new(browser: &Browser) -> Result<Self, String> {
        let ctx = browser
            .new_context_with_options(playwright_rs::BrowserContextOptions {
                user_agent: Some(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\
                        (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                        .to_string(),
                ),
                extra_http_headers: Some(HashMap::from([
                    ("Accept-Language".to_string(), "en-US,en;q=0.9".to_string()),
                    (
                        "Accept".to_string(),
                        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                            .to_string(),
                    ),
                ])),
                ..BrowserContextOptions::default()
            })
            .await
            .map_err(|e| format!("Failed to initialise browser with context: {e:?}"))?;
        let page = ctx
            .new_page()
            .await
            .map_err(|e| format!("Failed to open new page with playwright: {e:?}"))?;

        Ok(Self {
            ctx,
            page,
            rt: tokio::runtime::Handle::current(),
        })
    }
}

impl Drop for BrowserGuard {
    fn drop(&mut self) {
        // block_on is needed because Drop is sync
        let page = self.page.clone();
        let ctx = self.ctx.clone();
        self.rt.spawn(async move {
            let _ = page.close().await;
            let _ = ctx.close().await;
        });
    }
}

pub async fn fetch(browser: &Browser, source: &Source) -> Result<Vec<Article>, String> {
    let (container_sel, title_sel, link_sel, body_sel) = source
        .selectors
        .ok_or_else(|| format!("No selectors defined for Playwright source {}", source.name))?;

    let guard = BrowserGuard::new(browser).await?;

    // Navigate with generous timeout for JS-heavy pages
    guard
        .page
        .goto(source.url, None)
        .await
        .map_err(|e| format!("Failed to navigate to url: {e:?}"))?;

    // Wait for article container to render
    let url = source.url;
    guard
        .page
        .locator(container_sel)
        .await
        .scroll_into_view_if_needed()
        .await
        .map_err(|e| format!("Failed to find container_sel for {url:?}: {e:?}"))?;

    // Scroll to trigger lazy loading
    for _ in 0..3 {
        if let Err(e) = guard
            .page
            .evaluate::<(), ()>("() => window.scrollBy(0, 800)", None)
            .await
        {
            tracing::error!("Failed to expand article cards with JS evaluation: {e:?}");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;
    }

    // Extract article cards via JS evaluation — more reliable than element handles
    let raw: serde_json::Value = guard
        .page
        .evaluate::<serde_json::Value, serde_json::Value>(
            &format!(
                r#"() => {{
                    const cards = Array.from(document.querySelectorAll('{container}'));
                    return cards.slice(0, {max}).map(card => {{
                        const titleEl = card.querySelector('{title}');
                        const linkEl  = card.querySelector('{link}');
                        const bodyEl  = card.querySelector('{body}');
                        return {{
                            title: titleEl ? titleEl.innerText.trim() : '',
                            url:   linkEl  ? (linkEl.href || '') : '',
                            body:  bodyEl  ? bodyEl.innerText.trim().slice(0, 600) : '',
                        }};
                    }});
                }}"#,
                container = container_sel,
                max = source.max_articles,
                title = title_sel,
                link = link_sel,
                body = body_sel,
            ),
            None,
        )
        .await
        .map_err(|e| format!("Failed to expand article cards with JS evaluation: {e:?}"))?;

    drop(guard);

    let mut articles = Vec::new();

    if let Some(arr) = raw.as_array() {
        for item in arr {
            let title = item["title"].as_str().unwrap_or("").to_string();
            let url = item["url"].as_str().unwrap_or("").to_string();
            let body = item["body"].as_str().unwrap_or("").to_string();

            if title.is_empty() || url.is_empty() {
                continue;
            }

            // Resolve relative URLs
            let full_url = if url.starts_with("http") {
                url.clone()
            } else {
                format!("{}{}", base_url(source.url), url)
            };

            let id = DedupStore::hash(&full_url, &title);

            articles.push(Article {
                id,
                source: source.name.to_string(),
                market: format!("{:?}", source.market).to_lowercase(),
                url: full_url,
                title,
                summary: body,
                full_text: None,
                published_at: None, // Playwright path rarely gives structured dates
                scraped_at: Utc::now(),
                filing_type: None,
                ticker: None,
            });
        }
    }

    debug!(
        "Playwright {}: fetched {} articles",
        source.name,
        articles.len()
    );
    Ok(articles)
}

fn base_url(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(u) => format!("{}://{}", u.scheme(), u.host_str().unwrap_or("")),
        Err(_) => url.to_string(),
    }
}
