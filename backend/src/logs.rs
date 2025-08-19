use axum::{
    Json,
    extract::{Path, Query},
    response::IntoResponse,
};
use regex::Regex;
use std::{collections::HashMap, fs, path::PathBuf};

#[derive(Debug, serde::Deserialize)]
pub struct LogFilter {
    level: Option<String>,
    name: Option<String>,
    exclude_name: Option<String>,
    limit: Option<usize>,
    start: Option<usize>,
}

fn parse_log_line(line: &str) -> Option<HashMap<String, String>> {
    // Adjust this regex to match your format exactly
    let pattern = Regex::new(
        // r"(?x)
        // ^(?P<asctime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) -
        // (?P<levelname>\w+) -
        // (?P<name>[\w\.-]+) -
        // (?P<module>[\w\-\.]+)\.(?P<funcName>\w+):(?P<lineno>\d+) -
        // (?P<message>.+)$",
        r"(?sx) # 's' for singleline (dotall), 'x' for free-spacing comments
        ^(?P<asctime>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3})\s-\s
        (?P<levelname>\w+)\s-\s
        (?P<name>[\w\.-]+)\s-\s
        (?P<module>[\w\-\.]+)\.(?P<funcName>\w+):(?P<lineno>\d+)\s-\s
        (?P<message>.*?)$",
    )
    .unwrap();

    // Naming of keys aligns with the names for the logging in python
    pattern.captures(line).map(|caps| {
        let keys = [
            "asctime",
            "levelname",
            "name",
            "module",
            "funcName",
            "lineno",
            "message",
        ];
        keys.iter()
            .map(|&k| {
                (
                    k.to_string(),
                    caps.name(k).map_or("", |m| m.as_str()).trim().to_string(),
                )
            })
            .collect()
    })
}

pub async fn list_logs() -> impl IntoResponse {
    let log_dir = PathBuf::from("logs");
    let Ok(entries) = fs::read_dir(log_dir) else {
        return Json(serde_json::json!({ "error": "Log directory not found" }));
    };

    let filenames: Vec<String> = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|e| e.path().file_name()?.to_str().map(String::from))
        .collect();

    Json(serde_json::json!(filenames))
}

pub async fn read_log(
    Path(filename): Path<String>,
    Query(filter): Query<LogFilter>,
) -> impl IntoResponse {
    let path = PathBuf::from("logs").join(&filename);
    if !path.exists() {
        return Json(serde_json::json!({ "error": "File not found" }));
    }

    let Ok(content) = fs::read_to_string(path) else {
        return Json(serde_json::json!({ "error": "Failed to read file" }));
    };

    let mut results = vec![];
    let start_offset = filter.start.unwrap_or(0); // Entries to skip from the end (after reverse)
    let limit = filter.limit.unwrap_or(100); // Max entries to collect

    let LOG_START_REGEX: Regex = Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap();

    let mut current_log_lines: Vec<&str> = Vec::new();
    let mut entries_processed_count = 0; // Tracks how many *valid* log entries we've processed (from the end)

    // Iterate through lines in reverse order
    for line in content.lines().rev() {
        current_log_lines.push(line);

        if LOG_START_REGEX.is_match(line) && !current_log_lines.is_empty() {
            // Reverse the lines to get the original order, then join them
            current_log_lines.reverse();
            let full_log_entry_text = current_log_lines.join("\n");

            // Now, parse this full log entry
            if let Some(parsed) = parse_log_line(&full_log_entry_text) {
                // Apply filtering logic here
                if let Some(level) = &filter.level {
                    if parsed.get("levelname").map(|v| v != level).unwrap_or(true) {
                        current_log_lines.clear(); // Clear for the next entry
                        continue;
                    }
                }

                if let Some(name) = &filter.name {
                    if parsed.get("name").map(|v| v != name).unwrap_or(true) {
                        current_log_lines.clear(); // Clear for the next entry
                        continue;
                    }
                }

                if let Some(exclude_name) = &filter.exclude_name {
                    if parsed
                        .get("name")
                        .map(|v| v == exclude_name)
                        .unwrap_or(false)
                    {
                        current_log_lines.clear(); // Clear for the next entry
                        continue;
                    }
                }

                // If filters pass, consider this a valid entry
                entries_processed_count += 1;

                if entries_processed_count > start_offset {
                    results.push(parsed);
                    if results.len() >= limit {
                        break; // We have enough results
                    }
                }
            }
            // Clear the buffer for the next log entry, only if it was successfully processed
            current_log_lines.clear();
        }
    }

    // After the loop, there might be one last log entry left in current_log_lines
    // (if the file doesn't end exactly at the start of a log entry, or for the very first entry)
    if !current_log_lines.is_empty() {
        current_log_lines.reverse(); // Reverse for correct order
        let full_log_entry_text = current_log_lines.join("\n");
        if let Some(parsed) = parse_log_line(&full_log_entry_text) {
            // Apply filtering logic for the last entry
            if let Some(level) = &filter.level {
                if parsed.get("levelname").map(|v| v != level).unwrap_or(true) {
                    // skip
                } else {
                    entries_processed_count += 1;
                    if entries_processed_count > start_offset && results.len() < limit {
                        results.push(parsed);
                    }
                }
            } else {
                // No level filter
                entries_processed_count += 1;
                if entries_processed_count > start_offset && results.len() < limit {
                    results.push(parsed);
                }
            }
            // Add name and exclude_name filters here as well for the last entry
            // ... (similar logic as above)
        }
    }

    // The `results` are collected in reverse order (from newest to oldest).
    // If you want them from oldest to newest (chronological within the filtered set), reverse them again.
    // results.reverse();

    Json(serde_json::json!(results))
}
