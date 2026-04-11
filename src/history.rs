pub fn record(_entry: &serde_json::Value, _history_dir: &str) {}

pub fn load_recent(_history_dir: &str, _count: usize) -> Vec<serde_json::Value> {
    Vec::new()
}
