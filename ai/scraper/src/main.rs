// scraper/src/main.rs
//
// Responsibilities:
//   1. Schedule scrape jobs per exchange market open (UTC)
//   2. For each source: fetch via RSS (fast path) or Playwright (JS-heavy sites)
//   3. Deduplicate via SHA-256(url + title)
//   4. Write raw articles as JSON to /data/scraped_articles/YYYY-MM-DD/
//   5. Notify LLM service that new articles are ready

mod dedup;
mod playwright_scraper;
mod rss_scraper;
mod sources;
mod types;

use chrono::Utc;
use chrono_tz::America::New_York;
use chrono_tz::Europe::London;
use playwright_rs::Browser;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), String> {
    eprintln!("Entering scraper");
    if let Err(e) = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "news_scraper=debug".into()),
        )
        .try_init()
    {
        eprintln!("Failed to initialise tracing_subscriber: {e:?}");
    };

    eprintln!("Finished initialising tracing_subscriber");
    info!("Scraper service starting");

    // Install chromium browser first
    // playwright_rs::install_browsers(Some(&["chromium"]))
    //     .await
    //     .map_err(|e| format!("Failed to install chromium browser: {e:?}"))?;

    // Initialise playwright once — shared across jobs via Arc
    let playwright = playwright_rs::Playwright::launch()
        .await
        .map_err(|e| format!("Failed to launch playwright: {e:?}"))?;
    let browser = std::sync::Arc::new(
        playwright
            .chromium()
            .launch()
            .await
            .map_err(|e| format!("Failed to launch chromium: {e:?}"))?,
    );

    // let pw = playwright_rs::Playwright::initialize().await?;
    let sched = JobScheduler::new()
        .await
        .map_err(|e| format!("Failed to initialise JobScheduler instance: {e:?}"))?;

    // ── NYSE/NASDAQ: scrape at 13:00 UTC (90 min before 14:30 open) ────────
    let b1 = browser.clone();
    sched
        .add(
            Job::new_async_tz("0 0 8 * * Mon-Fri", New_York, move |_, _| {
                let b = b1.clone();
                Box::pin(async move {
                    info!("Running US market scrape");
                    if let Err(e) = run_scrape_cycle(&b, sources::us_sources()).await {
                        error!("US scrape failed: {e}");
                    }
                })
            })
            .map_err(|e| {
                format!("Error trying to initialise new daily cron job at 13:00: {e:?}")
            })?,
        )
        .await
        .map_err(|e| format!("Error trying ot add 13:00 cron job to schedule: {e:?}"))?;

    // ── LSE: scrape at 06:30 UTC (90 min before 08:00 open) ───────────────
    let b2 = browser.clone();
    sched
        .add(
            Job::new_async_tz("0 30 6 * * Mon-Fri", London, move |_, _| {
                let b = b2.clone();
                Box::pin(async move {
                    info!("Running UK market scrape");
                    if let Err(e) = run_scrape_cycle(&b, sources::uk_sources()).await {
                        error!("UK scrape failed: {e}");
                    }
                })
            })
            .map_err(|e| {
                format!("Error trying to initialise new daily cron job at 13:00: {e:?}")
            })?,
        )
        .await
        .map_err(|e| format!("Error trying ot add 13:00 cron job to schedule: {e:?}"))?;

    // ── TSE/KRX: scrape at 22:00 UTC previous day ─────────────────────────
    // ── TSE/KRX: opens 00:00 UTC ────────────────────────
    let b3 = browser.clone();
    sched
        .add(
            Job::new_async("0 0 23 * * *", move |_, _| {
                let b = b3.clone();
                Box::pin(async move {
                    info!("Running Asia market scrape");
                    if let Err(e) = run_scrape_cycle(&b, sources::asia_sources()).await {
                        error!("Asia scrape failed: {e}");
                    }
                })
            })
            .map_err(|e| {
                format!("Error trying to initialise new daily cron job at 13:00: {e:?}")
            })?,
        )
        .await
        .map_err(|e| format!("Error trying ot add 13:00 cron job to schedule: {e:?}"))?;

    // ── Run immediately on startup (catches up if just deployed) ──────────
    {
        let b = browser.clone();
        let all = [
            sources::us_sources(),
            sources::uk_sources(),
            sources::asia_sources(),
        ]
        .concat();
        tokio::spawn(async move {
            info!("Startup scrape running");
            if let Err(e) = run_scrape_cycle(&b, all).await {
                error!("Startup scrape failed: {e}");
            }
        });
    }

    sched
        .start()
        .await
        .map_err(|e| format!("Error trying to start master cron scheduler: {e:?}"))?;

    // Keep alive
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}

async fn run_scrape_cycle(browser: &Browser, sources: Vec<sources::Source>) -> Result<(), String> {
    let date_str = Utc::now().format("%Y-%m-%d").to_string();
    let out_dir = format!("{}/{}", articles_path(), date_str);
    tokio::fs::create_dir_all(&out_dir)
        .await
        .map_err(|e| format!("Error trying to create article directories: {e:?}"))?;

    let dedup = dedup::DedupStore::load(&out_dir).await?;
    let mut articles: Vec<types::Article> = Vec::new();

    for source in sources {
        let fetched = match source.method {
            sources::FetchMethod::Rss => rss_scraper::fetch(&source).await.unwrap_or_else(|e| {
                error!("RSS fetch failed for {}: {e}", source.name);
                vec![]
            }),
            sources::FetchMethod::Playwright => playwright_scraper::fetch(browser, &source)
                .await
                .unwrap_or_else(|e| {
                    error!("Playwright fetch failed for {}: {e}", source.name);
                    vec![]
                }),
        };

        for article in fetched {
            if dedup.is_new(&article) {
                articles.push(article);
            }
        }
    }

    // Write deduplicated batch
    let batch_path = format!("{}/batch_{}.json", out_dir, Utc::now().timestamp());
    let json = serde_json::to_string_pretty(&articles)
        .map_err(|e| format!("Failed to convert articles to pretty strings: {e:?}"))?;
    tokio::fs::write(&batch_path, json)
        .await
        .map_err(|e| format!("Failed to write to batch json: {e:?}"))?;

    // Persist updated dedup hashes
    dedup.save_new(&articles, &out_dir).await?;

    info!("Scraped {} new articles → {}", articles.len(), batch_path);

    // Notify LLM service
    notify_llm_service(&date_str).await?;

    Ok(())
}

async fn notify_llm_service(date_str: &str) -> Result<(), String> {
    let url = format!(
        "{}/internal/articles_ready",
        std::env::var("LLM_SERVICE_URL").unwrap_or_else(|_| "http://llm_service:8001".into())
    );
    let client = reqwest::Client::new();
    client
        .post(&url)
        .json(&serde_json::json!({ "date": date_str }))
        .send()
        .await
        .map_err(|e| {
            format!("Failed to send POST request to llm_service on articles ready: {e:?}")
        })?;
    Ok(())
}

fn articles_path() -> String {
    std::env::var("ARTICLES_PATH").unwrap_or_else(|_| "/data/scraped_articles".into())
}
