use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Article {
    pub id: String,
    pub source: String,
    pub market: String,
    pub url: String,
    pub title: String,
    pub summary: String,
    pub full_text: Option<String>,
    pub published_at: Option<chrono::DateTime<chrono::Utc>>,
    pub scraped_at: chrono::DateTime<chrono::Utc>,
    pub filing_type: Option<String>,
    pub ticker: Option<String>,
}
