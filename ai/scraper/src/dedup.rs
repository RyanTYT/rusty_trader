// scraper/src/dedup.rs
//
// Persists SHA-256(url + title) hashes to a JSON file in the daily article directory.
// On each scrape cycle we load today's hashes, filter new articles, then append.
//
// NOTE: there are some other options for hashing that are less compute heavy, but this doesn't
// seem too impt -> still using 3rd party libs so idrc that much
// - inbuilt hash CANNOT be used (apparently since it uses a seed on program startup that may be
// diff on each run)

use sha2::{Digest, Sha256};
use std::collections::HashSet;
use tracing::debug;

use crate::types::Article;

pub struct DedupStore {
    seen: HashSet<String>,
    path: String,
}

impl DedupStore {
    pub async fn load(dir: &str) -> Result<Self, String> {
        let path = format!("{}/dedup_hashes.json", dir);
        let seen: HashSet<String> = if tokio::fs::try_exists(&path)
            .await
            .map_err(|e| format!("dedup_hashes.json file doesn't exist: {e:?}"))?
        {
            let raw = tokio::fs::read_to_string(&path)
                .await
                .map_err(|e| format!("could not read dedup_hashes.json file to string: {e:?}"))?;
            serde_json::from_str(&raw).unwrap_or_default()
        } else {
            HashSet::new()
        };
        debug!("Loaded {} dedup hashes from {}", seen.len(), path);
        Ok(Self { seen, path })
    }

    pub fn hash(url: &str, title: &str) -> String {
        let mut h = Sha256::new();
        h.update(url.as_bytes());
        h.update(b"|");
        h.update(title.as_bytes());
        hex::encode(h.finalize())
    }

    pub fn is_new(&self, article: &Article) -> bool {
        !self.seen.contains(&article.id)
    }

    /// Persist new hashes after a scrape cycle
    pub async fn save_new(&self, new_articles: &[Article], dir: &str) -> Result<(), String> {
        let mut all = self.seen.clone();
        for a in new_articles {
            all.insert(a.id.clone());
        }
        let json = serde_json::to_string_pretty(&all)
            .map_err(|e| format!("Failed to convert to pretty string: {e:?}"))?;
        let path = self.path.clone();
        tokio::fs::write(&self.path, json)
            .await
            .map_err(|e| format!("Failed to write to {path:?}: {e:?}"))?;
        debug!("Saved {} total dedup hashes to {}", all.len(), dir);
        Ok(())
    }
}
