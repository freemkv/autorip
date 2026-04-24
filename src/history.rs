pub fn record(history_dir: &str, entry: &serde_json::Value) {
    if let Err(e) = std::fs::create_dir_all(history_dir) {
        tracing::warn!(path = %history_dir, error = %e, "history: cannot create dir");
        return;
    }
    let timestamp = unix_timestamp_nanos();
    // Include the device suffix when present so two devices recording in the
    // same nanosecond (rare, but possible — clock granularity varies) can't
    // collide. Falls through to plain timestamp when the entry has no device.
    let device = entry
        .get("device")
        .and_then(|v| v.as_str())
        .unwrap_or("system");
    let path = format!("{}/{}_{}.json", history_dir, timestamp, device);
    match serde_json::to_string_pretty(entry) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&path, json) {
                tracing::warn!(path = %path, error = %e, "history: write failed");
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "history: serialize failed");
        }
    }
}

pub fn load_recent(history_dir: &str, count: usize) -> Vec<serde_json::Value> {
    let dir = match std::fs::read_dir(history_dir) {
        Ok(d) => d,
        Err(_) => return Vec::new(),
    };

    let mut entries: Vec<_> = dir
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "json").unwrap_or(false))
        .collect();

    // Sort by filename (timestamp) descending
    entries.sort_by_key(|b| std::cmp::Reverse(b.file_name()));
    entries.truncate(count);

    entries
        .iter()
        .filter_map(|e| std::fs::read_to_string(e.path()).ok())
        .filter_map(|s| serde_json::from_str(&s).ok())
        .collect()
}

/// Unix nanoseconds since epoch. Nanosecond precision so two rapid-fire
/// rips (or rips on two drives finishing in the same wall-clock second)
/// don't collide on the same filename. Misnamed `chrono_timestamp` pre-0.13
/// despite never using the `chrono` crate.
fn unix_timestamp_nanos() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(0);
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!(
            "autorip-history-test-{}-{}-{}",
            std::process::id(),
            tag,
            n
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn record_writes_json_file() {
        let d = tmpdir("record");
        let e = serde_json::json!({"title":"Test","year":2024});
        record(d.to_str().unwrap(), &e);
        let files: Vec<_> = std::fs::read_dir(&d)
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(files.len(), 1);
        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["title"], "Test");
        assert_eq!(parsed["year"], 2024);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn load_recent_returns_newest_first() {
        let d = tmpdir("load_recent");
        // Write files with known Unix-timestamp filenames — load_recent sorts by filename desc.
        for (ts, title) in &[("1000", "oldest"), ("2000", "middle"), ("3000", "newest")] {
            let p = d.join(format!("{ts}.json"));
            std::fs::write(&p, format!(r#"{{"title":"{title}"}}"#)).unwrap();
        }
        let recent = load_recent(d.to_str().unwrap(), 5);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0]["title"], "newest");
        assert_eq!(recent[2]["title"], "oldest");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn load_recent_respects_count_limit() {
        let d = tmpdir("count_limit");
        for i in 0..10 {
            let p = d.join(format!("{}.json", 1000 + i));
            std::fs::write(&p, format!(r#"{{"n":{i}}}"#)).unwrap();
        }
        let recent = load_recent(d.to_str().unwrap(), 3);
        assert_eq!(recent.len(), 3);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn load_recent_ignores_non_json_files() {
        let d = tmpdir("filter");
        std::fs::write(d.join("1.json"), r#"{"a":1}"#).unwrap();
        std::fs::write(d.join("2.txt"), "not json").unwrap();
        std::fs::write(d.join("3.json"), r#"{"a":3}"#).unwrap();
        assert_eq!(load_recent(d.to_str().unwrap(), 10).len(), 2);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn load_recent_missing_dir_returns_empty() {
        assert!(load_recent("/nonexistent/autorip-history-missing", 5).is_empty());
    }
}
