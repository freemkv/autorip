use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

/// Runtime config. Single source of truth is `settings.json` on disk;
/// the UI POSTs updates to it via `/api/settings`.
///
/// **Bootstrap-only env vars** (v0.25.7 cleanup, "no dupes" rule):
/// only these env vars influence Config — everything else is read
/// from `settings.json` (or, on first boot, the hardcoded
/// [`Config::default`] values):
/// - `PORT` — web bind port. Can't change after the server is
///   listening, so it must be set before the daemon starts.
/// - `AUTORIP_DIR` — where `settings.json` itself lives. Chicken-
///   and-egg with everything else.
/// - `AUTORIP_LOG_LEVEL` (read inside `observe::init`, not here) —
///   tracing filter is built before web is up.
/// - `RIP_USER`, `NFS_*` (read inside `autorip --bootstrap` in
///   `main.rs`, not here) — mount/user setup runs before the
///   daemon starts.
///
/// Operator-facing knobs (AUTO_EJECT, MAX_RETRIES, KEEP_ISO,
/// MIN_LENGTH, MAIN_FEATURE, OUTPUT_FORMAT, NETWORK_TARGET,
/// ON_READ_ERROR, ABORT_ON_LOST_SECS, MOVIE_DIR / TV_DIR /
/// STAGING_DIR / OUTPUT_DIR, TMDB_API_KEY, KEYDB_*, the new
/// FREEMKV_THREADS / LOG_RETENTION_DAYS): all UI now, no env-var
/// reads, no duplication. Pre-0.25.7 deployments that set these in
/// docker-compose.yml will see the env values silently ignored —
/// operators must set them via the Settings page.
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
    /// When a disc has no usable keys: if true, capture it to an ISO anyway and
    /// defer the mux until keys are available; if false (default), abort the rip
    /// with an explicit message. The operator's "proceed vs abort" decision.
    pub capture_without_keys: bool,
    /// Maximum total time for entire rip across all passes (seconds). Prevents infinite hangs.
    pub max_rip_duration_secs: u64,
    /// Minimum per-pass wallclock budget (seconds), used when disc runtime is unknown.
    pub min_pass_budget_secs: u64,
    /// Transport failure recovery: delay after USB re-enumeration before retrying open (seconds).
    pub transport_recovery_delay_secs: u64,
    pub tmdb_api_key: String,
    pub keydb_path: Option<String>,
    pub keydb_url: String,
    /// Where AACS keys come from: "local" (a key database on disk) or "online"
    /// (an external key service). Mutually exclusive.
    pub key_source: String,
    /// Base URL of the external key service used when `key_source = "online"`.
    pub keyserver_url: String,
    /// Optional bearer token for the key service. Empty = none.
    pub keyserver_secret: String,
    pub webhook_urls: Vec<String>,
    pub autorip_dir: String,

    /// v0.25.7: number of threads for AACS decryption. 0 = auto (all
    /// available cores, capped at libfreemkv's MAX_THREADS). Was the
    /// `FREEMKV_THREADS` env var pre-0.25.7. Applied at startup and
    /// whenever the UI POSTs a change via
    /// `libfreemkv::decrypt::set_decrypt_threads`.
    pub decrypt_threads: usize,

    /// v0.25.7: how long to keep per-device `.log` files in
    /// `$AUTORIP_DIR/logs` before the in-process prune thread
    /// deletes them. Was the `LOG_RETENTION_DAYS` env var pre-0.25.7.
    pub log_retention_days: u64,
}

impl Default for Config {
    /// Hardcoded first-boot defaults. UI changes overlay these via
    /// `settings.json`. No env-var sourcing here — `PORT` and
    /// `AUTORIP_DIR` are spliced in by [`load`] because they're the
    /// only two knobs that genuinely need to come from env (see the
    /// `Config` doc comment).
    fn default() -> Self {
        Self {
            port: 8080,
            staging_dir: "/staging".into(),
            output_dir: "/output".into(),
            movie_dir: String::new(),
            tv_dir: String::new(),
            min_length_secs: 600,
            main_feature: true,
            auto_eject: true,
            on_insert: "scan".into(),
            output_format: "mkv".into(),
            network_target: String::new(),
            on_read_error: "stop".into(),
            max_retries: 1,
            keep_iso: false,
            abort_on_lost_secs: 0,
            capture_without_keys: false,
            max_rip_duration_secs: 28_800, // 8h cap for UHD with heavy recovery
            min_pass_budget_secs: 5_400,   // 90 min per pass
            transport_recovery_delay_secs: 5,
            tmdb_api_key: String::new(),
            keydb_path: None,
            keydb_url: String::new(),
            key_source: "local".into(),
            keyserver_url: String::new(),
            keyserver_secret: String::new(),
            webhook_urls: Vec::new(),
            autorip_dir: "/config".into(),
            decrypt_threads: 0, // 0 = auto-detect cores
            log_retention_days: 30,
        }
    }
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
    // Only the two bootstrap-only env vars are read here. Everything
    // else comes from settings.json (or Config::default if it's a
    // first boot with no settings file).
    let autorip_dir = std::env::var("AUTORIP_DIR").unwrap_or_else(|_| "/config".to_string());
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);

    let mut cfg = Config {
        port,
        autorip_dir,
        ..Config::default()
    };
    cfg = load_saved(cfg);

    // Apply the persisted decrypt thread count to libfreemkv's
    // global pool. Subsequent UI POSTs re-apply via the same fn.
    apply_decrypt_threads(cfg.decrypt_threads);

    Arc::new(RwLock::new(cfg))
}

/// Apply the configured decrypt thread count to libfreemkv's global
/// rayon pool. 0 means "auto" — let libfreemkv fall back to its own
/// default (all cores, capped). UI invokes this on settings POST so
/// changes take effect without restarting the container.
pub fn apply_decrypt_threads(n: usize) {
    if n > 0 {
        libfreemkv::decrypt::set_decrypt_threads(n);
    }
    // If n == 0 we leave the existing setting in place. There's no
    // libfreemkv "reset to default" hook today; first-call lazy init
    // already picked up the right default. Setting it to a specific
    // value here and never back is fine — the operator either tunes
    // it or doesn't.
}

fn load_saved(mut cfg: Config) -> Config {
    let path = cfg.settings_file();
    let Ok(data) = std::fs::read_to_string(&path) else {
        return cfg;
    };
    let Ok(saved) = serde_json::from_str::<serde_json::Value>(&data) else {
        return cfg;
    };
    // Overlay saved settings onto defaults. Each field is independently
    // gated so a settings.json missing a field (or with a wrong type)
    // doesn't wipe the rest.
    if let Some(v) = saved.get("staging_dir").and_then(|v| v.as_str()) {
        cfg.staging_dir = v.to_string();
    }
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
    if let Some(v) = saved.get("keydb_path").and_then(|v| v.as_str()) {
        cfg.keydb_path = Some(v.to_string());
    }
    if let Some(v) = saved.get("key_source").and_then(|v| v.as_str()) {
        cfg.key_source = v.to_string();
    }
    if let Some(v) = saved.get("keyserver_url").and_then(|v| v.as_str()) {
        cfg.keyserver_url = v.to_string();
    }
    if let Some(v) = saved.get("keyserver_secret").and_then(|v| v.as_str()) {
        cfg.keyserver_secret = v.to_string();
    }
    if let Some(v) = saved.get("min_length_secs").and_then(|v| v.as_u64()) {
        cfg.min_length_secs = v;
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
    if let Some(v) = saved.get("capture_without_keys").and_then(|v| v.as_bool()) {
        cfg.capture_without_keys = v;
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
    if let Some(v) = saved.get("decrypt_threads").and_then(|v| v.as_u64()) {
        cfg.decrypt_threads = v as usize;
    }
    if let Some(v) = saved.get("log_retention_days").and_then(|v| v.as_u64()) {
        cfg.log_retention_days = v;
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
