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
    pub abort_on_error: bool,
    pub tmdb_api_key: String,
    pub keydb_path: Option<String>,
    pub keydb_url: String,
    pub webhooks: Vec<WebhookConfig>,
    pub autorip_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub url: String,
    pub events: Vec<String>,
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
        on_insert: env_or("ON_INSERT", "rip"),
        output_format: env_or("OUTPUT_FORMAT", "mkv"),
        network_target: env_or("NETWORK_TARGET", ""),
        abort_on_error: env_or("ABORT_ON_ERROR", "true") == "true",
        tmdb_api_key: env_or("TMDB_API_KEY", ""),
        keydb_path: std::env::var("KEYDB_PATH").ok(),
        keydb_url: env_or("KEYDB_URL", ""),
        webhooks: Vec::new(),
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
            if let Some(v) = saved.get("abort_on_error").and_then(|v| v.as_bool()) {
                cfg.abort_on_error = v;
            }
        }
    }
    cfg
}

pub fn save(cfg: &Config) {
    let path = cfg.settings_file();
    if let Ok(json) = serde_json::to_string_pretty(cfg) {
        let _ = std::fs::write(&path, json);
    }
}
