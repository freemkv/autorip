pub fn record(history_dir: &str, entry: &serde_json::Value) {
    let _ = std::fs::create_dir_all(history_dir);
    let timestamp = chrono_timestamp();
    let path = format!("{}/{}.json", history_dir, timestamp);
    if let Ok(json) = serde_json::to_string_pretty(entry) {
        let _ = std::fs::write(&path, json);
    }
}

pub fn load_recent(history_dir: &str, count: usize) -> Vec<serde_json::Value> {
    let dir = match std::fs::read_dir(history_dir) {
        Ok(d) => d,
        Err(_) => return Vec::new(),
    };

    let mut entries: Vec<_> = dir
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|x| x == "json")
                .unwrap_or(false)
        })
        .collect();

    // Sort by filename (timestamp) descending
    entries.sort_by(|a, b| b.file_name().cmp(&a.file_name()));
    entries.truncate(count);

    entries
        .iter()
        .filter_map(|e| std::fs::read_to_string(e.path()).ok())
        .filter_map(|s| serde_json::from_str(&s).ok())
        .collect()
}

fn chrono_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{}", secs)
}
