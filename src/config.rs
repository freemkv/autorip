use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub port: u16,
    pub staging_dir: String,
    pub output_dir: String,
    pub movie_dir: String,
    pub tv_dir: String,
    pub min_length_secs: u64,
    pub main_feature: bool,
    pub auto_eject: bool,
    pub on_insert: String,      // "nothing", "identify", "rip"
    pub output_format: String,  // "mkv", "m2ts", "iso"
    pub network_target: String, // e.g. "192.168.1.100:9000" for network output
    pub on_read_error: String,  // "stop", "skip"
    /// Number of retry passes after the initial disc→ISO pass. 0 = single pass
    /// (direct disc→MKV, no ISO intermediate). 1..=10 = multi-pass (disc→ISO,
    /// retry bad ranges N times, then mux to MKV).
    pub max_retries: u8,
    /// Keep the intermediate ISO after mux completes. Defaults to false — the
    /// ISO is pruned once the MKV is successfully finalized.
    pub keep_iso: bool,
    /// Abort rip if main movie loss exceeds N seconds. 0 = never abort (continue anyway).
    pub abort_on_lost_secs: u64,
    /// Maximum total time for entire rip across all passes (seconds). Prevents infinite hangs.
    pub max_rip_duration_secs: u64,
    /// Minimum per-pass wallclock budget (seconds), used when disc runtime is unknown.
    pub min_pass_budget_secs: u64,
    /// Transport failure recovery: delay after USB re-enumeration before retrying open (seconds).
    pub transport_recovery_delay_secs: u64,
    pub tmdb_api_key: String,
    pub keydb_path: Option<String>,
    pub keydb_url: String,
    pub webhook_urls: Vec<String>,
    pub autorip_dir: String,
}

impl Config {
    pub fn staging_device_dir(&self, device: &str) -> String {
        format!("{}/{}", self.staging_dir, device)
    }
    pub fn history_dir(&self) -> String {
        format!("{}/history", self.autorip_dir)
    }
    pub fn log_dir(&self) -> String {
        format!("{}/logs", self.autorip_dir)
    }
    pub fn settings_file(&self) -> String {
        format!("{}/settings.json", self.autorip_dir)
    }
}

pub fn load() -> Arc<RwLock<Config>> {
    let autorip_dir = std::env::var("AUTORIP_DIR").unwrap_or_else(|_| "/config".to_string());
    let cfg = Config {
        port: env_or("PORT", "8080").parse().unwrap_or(8080),
        staging_dir: env_or("STAGING_DIR", "/staging"),
        output_dir: env_or("OUTPUT_DIR", "/output"),
        movie_dir: env_or("MOVIE_DIR", ""),
        tv_dir: env_or("TV_DIR", ""),
        min_length_secs: env_or("MIN_LENGTH", "600").parse().unwrap_or(600),
        main_feature: env_or("MAIN_FEATURE", "true") == "true",
        auto_eject: env_or("AUTO_EJECT", "true") == "true",
        on_insert: env_or("ON_INSERT", "scan"),
        output_format: env_or("OUTPUT_FORMAT", "mkv"),
        network_target: env_or("NETWORK_TARGET", ""),
        on_read_error: env_or("ON_READ_ERROR", "stop"),
        max_retries: env_or("MAX_RETRIES", "1")
            .parse::<u8>()
            .unwrap_or(1)
            .min(10),
        keep_iso: env_or("KEEP_ISO", "false") == "true",
        abort_on_lost_secs: env_or("ABORT_ON_LOST_SECS", "0")
            .parse::<u64>()
            .unwrap_or(0),
        max_rip_duration_secs: env_or("MAX_RIP_DURATION_SECS", "28800")
            .parse::<u64>()
            .unwrap_or(7200), // 2 hours default for UHD with damage recovery
        min_pass_budget_secs: env_or("MIN_PASS_BUDGET_SECS", "5400")
            .parse::<u64>()
            .unwrap_or(3600), // 1 hour per pass default
        transport_recovery_delay_secs: env_or("TRANSPORT_RECOVERY_DELAY_SECS", "5")
            .parse::<u64>()
            .unwrap_or(5), // 5 seconds delay after USB re-enumeration
        tmdb_api_key: env_or("TMDB_API_KEY", ""),
        keydb_path: std::env::var("KEYDB_PATH").ok(),
        keydb_url: env_or("KEYDB_URL", ""),
        webhook_urls: Vec::new(),
        autorip_dir,
    };
    // Try loading saved settings
    let cfg = load_saved(cfg);
    Arc::new(RwLock::new(cfg))
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn load_saved(mut cfg: Config) -> Config {
    let path = cfg.settings_file();
    if let Ok(data) = std::fs::read_to_string(&path) {
        if let Ok(saved) = serde_json::from_str::<serde_json::Value>(&data) {
            // Overlay saved settings onto defaults
            if let Some(v) = saved.get("output_dir").and_then(|v| v.as_str()) {
                cfg.output_dir = v.to_string();
            }
            if let Some(v) = saved.get("movie_dir").and_then(|v| v.as_str()) {
                cfg.movie_dir = v.to_string();
            }
            if let Some(v) = saved.get("tv_dir").and_then(|v| v.as_str()) {
                cfg.tv_dir = v.to_string();
            }
            if let Some(v) = saved.get("tmdb_api_key").and_then(|v| v.as_str()) {
                cfg.tmdb_api_key = v.to_string();
            }
            if let Some(v) = saved.get("keydb_url").and_then(|v| v.as_str()) {
                cfg.keydb_url = v.to_string();
            }
            if let Some(v) = saved.get("main_feature").and_then(|v| v.as_bool()) {
                cfg.main_feature = v;
            }
            if let Some(v) = saved.get("auto_eject").and_then(|v| v.as_bool()) {
                cfg.auto_eject = v;
            }
            if let Some(v) = saved.get("on_insert").and_then(|v| v.as_str()) {
                cfg.on_insert = v.to_string();
            }
            if let Some(v) = saved.get("output_format").and_then(|v| v.as_str()) {
                cfg.output_format = v.to_string();
            }
            if let Some(v) = saved.get("network_target").and_then(|v| v.as_str()) {
                cfg.network_target = v.to_string();
            }
            if let Some(v) = saved.get("on_read_error").and_then(|v| v.as_str()) {
                cfg.on_read_error = v.to_string();
            }
            if let Some(v) = saved.get("max_retries").and_then(|v| v.as_u64()) {
                cfg.max_retries = (v.min(10)) as u8;
            }
            if let Some(v) = saved.get("keep_iso").and_then(|v| v.as_bool()) {
                cfg.keep_iso = v;
            }
            if let Some(v) = saved.get("abort_on_lost_secs").and_then(|v| v.as_u64()) {
                cfg.abort_on_lost_secs = v;
            }
            if let Some(v) = saved.get("max_rip_duration_secs").and_then(|v| v.as_u64()) {
                cfg.max_rip_duration_secs = v;
            }
            if let Some(v) = saved.get("min_pass_budget_secs").and_then(|v| v.as_u64()) {
                cfg.min_pass_budget_secs = v;
            }
            if let Some(v) = saved
                .get("transport_recovery_delay_secs")
                .and_then(|v| v.as_u64())
            {
                cfg.transport_recovery_delay_secs = v;
            }
            // Migrate old setting
            if let Some(true) = saved.get("abort_on_error").and_then(|v| v.as_bool()) {
                cfg.on_read_error = "stop".to_string();
            }
            if let Some(arr) = saved.get("webhook_urls").and_then(|v| v.as_array()) {
                cfg.webhook_urls = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .filter(|s| !s.is_empty())
                    .collect();
            }
        }
    }
    cfg
}

pub fn save(cfg: &Config) {
    let path = cfg.settings_file();
    let Ok(json) = serde_json::to_string_pretty(cfg) else {
        return;
    };
    // Write atomically: a SIGKILL or container OOM mid-`fs::write` would
    // truncate settings.json to zero or partial bytes; on next start
    // `load_saved` would silently reset every persisted field
    // (TMDB key, output dirs, max_retries, abort_on_lost_secs, …) to
    // env-var defaults. Watchtower restarts the container on every
    // release, so this isn't theoretical. Write to a sibling temp file,
    // fsync, then rename — POSIX rename is atomic within a filesystem.
    let tmp = format!("{path}.tmp");
    if std::fs::write(&tmp, json.as_bytes()).is_err() {
        return;
    }
    if let Ok(f) = std::fs::File::open(&tmp) {
        let _ = f.sync_all();
    }
    if let Err(e) = std::fs::rename(&tmp, &path) {
        let _ = std::fs::remove_file(&tmp);
        tracing::warn!(error = %e, "settings rename failed; settings.json unchanged");
    }
}
