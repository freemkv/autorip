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
#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    /// Bootstrap-only (env `PORT`, set before the server binds). Never
    /// persisted: `load_saved` never reads it back, so writing it into
    /// settings.json would only create misleading on-disk state that an
    /// operator could edit to no effect. `#[serde(default)]` keeps any
    /// stale value from an older settings.json deserializing cleanly.
    #[serde(skip_serializing, default = "default_port")]
    pub port: u16,
    pub staging_dir: String,
    pub output_dir: String,
    pub movie_dir: String,
    pub tv_dir: String,
    pub min_length_secs: u64,
    pub main_feature: bool,
    pub auto_eject: bool,
    pub on_insert: String,      // "nothing", "scan", "rip"
    pub output_format: String,  // "mkv", "m2ts", "iso"
    pub network_target: String, // e.g. "nas.example.com:9000" for network output
    pub on_read_error: String,  // "stop", "skip"
    /// Number of retry passes over the disc. 0 = single pass (read the disc
    /// once, no retries). 1..=10 = multi-pass: an initial sweep plus N retry
    /// passes over the bad ranges. This controls pass count only; the output
    /// container (MKV / M2TS / ISO) is selected by `output_format`.
    pub max_retries: u8,
    /// Promote the intermediate ISO into the output library alongside the muxed
    /// title (MKV/M2TS) after mux completes. Defaults to false — the ISO is
    /// pruned once the title is finalized. The disc mapfile is staging-only and
    /// never promoted.
    pub keep_iso: bool,
    /// Abort rip if main-movie loss exceeds N seconds after retries.
    /// 0 = perfect rip required (abort on any remaining main-movie loss).
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

    /// Number of threads for AACS decryption. 0 = auto (all available
    /// cores, capped at libfreemkv's MAX_THREADS). Applied at startup and
    /// whenever the UI POSTs a change via
    /// `libfreemkv::decrypt::set_decrypt_threads`.
    pub decrypt_threads: usize,

    /// How long to keep per-device `.log` files in `$AUTORIP_DIR/logs`
    /// before the in-process prune thread deletes them.
    pub log_retention_days: u64,
}

/// Manual `Debug` that redacts the secret-bearing fields. The derived
/// `Debug` would print `tmdb_api_key`, `keyserver_secret`, and the
/// `webhook_urls` (Discord/Slack/Jellyfin URLs embed bearer tokens in
/// their path/query) verbatim — a `tracing::debug!(?cfg)` anywhere would
/// then spill them into logs. Non-secret fields are printed normally so
/// the Debug output stays useful for diagnostics.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn redact(s: &str) -> &'static str {
            if s.is_empty() {
                "<unset>"
            } else {
                "<redacted>"
            }
        }
        f.debug_struct("Config")
            .field("port", &self.port)
            .field("staging_dir", &self.staging_dir)
            .field("output_dir", &self.output_dir)
            .field("movie_dir", &self.movie_dir)
            .field("tv_dir", &self.tv_dir)
            .field("min_length_secs", &self.min_length_secs)
            .field("main_feature", &self.main_feature)
            .field("auto_eject", &self.auto_eject)
            .field("on_insert", &self.on_insert)
            .field("output_format", &self.output_format)
            .field("network_target", &self.network_target)
            .field("on_read_error", &self.on_read_error)
            .field("max_retries", &self.max_retries)
            .field("keep_iso", &self.keep_iso)
            .field("abort_on_lost_secs", &self.abort_on_lost_secs)
            .field("capture_without_keys", &self.capture_without_keys)
            .field("max_rip_duration_secs", &self.max_rip_duration_secs)
            .field("min_pass_budget_secs", &self.min_pass_budget_secs)
            .field(
                "transport_recovery_delay_secs",
                &self.transport_recovery_delay_secs,
            )
            .field("tmdb_api_key", &redact(&self.tmdb_api_key))
            .field("keydb_path", &self.keydb_path)
            .field("keydb_url", &redact(&self.keydb_url))
            .field("key_source", &self.key_source)
            .field("keyserver_url", &redact(&self.keyserver_url))
            .field("keyserver_secret", &redact(&self.keyserver_secret))
            .field(
                "webhook_urls",
                &format!("[{} redacted]", self.webhook_urls.len()),
            )
            .field("autorip_dir", &self.autorip_dir)
            .field("decrypt_threads", &self.decrypt_threads)
            .field("log_retention_days", &self.log_retention_days)
            .finish()
    }
}

/// Default web bind port — used by `#[serde(default)]` on `port` when an
/// older settings.json carried the (now non-serialized) field. The live
/// value always comes from the `PORT` env var via [`load`].
fn default_port() -> u16 {
    8080
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
    pub fn log_dir(&self) -> String {
        format!("{}/logs", self.autorip_dir)
    }
    pub fn settings_file(&self) -> String {
        format!("{}/settings.json", self.autorip_dir)
    }
}

/// True if `p` is an existing directory we can create a file in.
fn dir_is_writable(p: &str) -> bool {
    let probe = std::path::Path::new(p).join(".autorip-write-probe");
    match std::fs::File::create(&probe) {
        Ok(_) => {
            let _ = std::fs::remove_file(&probe);
            true
        }
        Err(_) => false,
    }
}

/// Resolve the base config directory. `AUTORIP_DIR` always wins. Otherwise
/// prefer `/config` when it exists and is writable — the Docker image creates
/// it, so the container path is unchanged. On a bare install (downloadable
/// binary, no container mounts) fall back to `$XDG_CONFIG_HOME/autorip` or
/// `~/.config/autorip` so `./autorip` just works without root.
/// Resolve where autorip keeps all its state (settings.json, logs, keys,
/// staging, output). Identical logic on EVERY OS — no per-platform branches —
/// and always returns a REAL ABSOLUTE path the UI/logs can show verbatim.
///
/// Order:
///   1. `AUTORIP_DIR` — explicit override. The Docker image sets this to
///      `/config` (its bind mount), so the container is handled here.
///   2. A writable `/config` — the container bind mount, for older Docker
///      deployments that didn't set `AUTORIP_DIR`. On a fresh native Windows /
///      macOS box this directory does not exist, so it is skipped — autorip
///      never creates `C:\config` at the drive root.
///   3. A `config` folder NEXT TO the executable — the self-contained default
///      for a downloaded binary. `current_exe()` is absolute on every OS, so
///      this is a real absolute path (the download folder + `config`), never a
///      relative `.\config`. Move the folder, the app's state moves with it.
///   4. Last resort: the absolute working directory + `config`.
pub fn default_autorip_dir() -> String {
    if let Ok(d) = std::env::var("AUTORIP_DIR") {
        if !d.is_empty() {
            return d;
        }
    }
    if std::path::Path::new("/config").is_dir() && dir_is_writable("/config") {
        return "/config".to_string();
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            return parent.join("config").to_string_lossy().into_owned();
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        return cwd.join("config").to_string_lossy().into_owned();
    }
    "config".to_string()
}

pub fn load() -> Arc<RwLock<Config>> {
    // Only the two bootstrap-only env vars are read here. Everything
    // else comes from settings.json (or Config::default if it's a
    // first boot with no settings file).
    let autorip_dir = default_autorip_dir();
    // PORT is bootstrap-only and can't be changed after the server binds,
    // so a typo'd or out-of-range value must not be silently swallowed —
    // that would bind 8080 while the operator believes their value took
    // effect. Warn and fall back so the misconfiguration is diagnosable.
    let port: u16 = match std::env::var("PORT") {
        Ok(s) => match s.trim().parse::<u16>() {
            Ok(p) if p != 0 => p,
            _ => {
                tracing::warn!(
                    value = %s,
                    "PORT env var is not a valid 1-65535 port; falling back to 8080"
                );
                8080
            }
        },
        Err(_) => 8080,
    };

    let mut cfg = Config {
        port,
        autorip_dir,
        ..Config::default()
    };
    cfg = load_saved(cfg);

    // Bare-run (no container): when staging/output are still the container
    // defaults and those root paths aren't writable, relocate them under the
    // resolved config dir so the downloadable binary runs without /staging and
    // /output mounts. In Docker (where both exist and are writable) this is a
    // no-op. Then ensure every dir we use exists (bootstrap normally does this
    // in the container; bare run has no bootstrap).
    // Use existence, NOT writability, to detect "no container mount". In Docker
    // both dirs exist (mounted); a *transient* NFS unwritability at container
    // start must not relocate staging/output to the config dir — that would
    // orphan an in-progress ISO and split data across two directories. Bare run
    // (downloadable binary, no mounts) is the only case where they don't exist.
    if cfg.staging_dir == "/staging" && !std::path::Path::new("/staging").exists() {
        // Native join so the derived path uses the platform separator (clean
        // `...\config\staging` on Windows, not a mixed-slash `config/staging`).
        cfg.staging_dir = std::path::Path::new(&cfg.autorip_dir)
            .join("staging")
            .to_string_lossy()
            .into_owned();
    }
    if cfg.output_dir == "/output" && !std::path::Path::new("/output").exists() {
        cfg.output_dir = std::path::Path::new(&cfg.autorip_dir)
            .join("output")
            .to_string_lossy()
            .into_owned();
    }
    for d in [
        cfg.log_dir(),
        format!("{}/freemkv", cfg.autorip_dir),
        cfg.staging_dir.clone(),
        cfg.output_dir.clone(),
    ] {
        if let Err(e) = std::fs::create_dir_all(&d) {
            tracing::warn!(path = %d, error = %e, "could not create required directory");
        }
    }

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
    let data = match std::fs::read_to_string(&path) {
        Ok(data) => data,
        Err(e) => {
            // ENOENT is the legitimate first-boot case — stay silent. Any
            // other io error (perms, NFS stale handle) means we're falling
            // back to defaults for a non-obvious reason; surface it.
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %path, error = %e, "settings.json unreadable - using defaults");
            }
            return cfg;
        }
    };
    let saved = match serde_json::from_str::<serde_json::Value>(&data) {
        Ok(saved) => saved,
        Err(e) => {
            // A parse failure (e.g. a partial write from a SIGKILL mid-save,
            // not theoretical given Watchtower restarts) silently reverts
            // every persisted field to defaults. Warn so the operator can
            // see why their settings vanished.
            tracing::warn!(path = %path, error = %e, "settings.json failed to parse - all settings reverting to defaults");
            return cfg;
        }
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
        if matches!(v, "local" | "online") {
            cfg.key_source = v.to_string();
        } else {
            tracing::warn!(value = %v, "settings.json key_source has unknown value - using default");
        }
    }
    if let Some(v) = saved.get("keyserver_url").and_then(|v| v.as_str()) {
        cfg.keyserver_url = v.to_string();
    }
    if let Some(v) = saved.get("keyserver_secret").and_then(|v| v.as_str()) {
        cfg.keyserver_secret = v.to_string();
    }
    // Operator-edited numeric knobs are clamped to sane ceilings at the
    // settings.json trust boundary, matching max_retries (.min(10)) and
    // decrypt_threads (.min(256)). A pathological value (e.g.
    // max_rip_duration_secs near u64::MAX) would otherwise defeat the
    // "prevents infinite hangs" purpose these knobs exist for. The
    // ceilings are deliberately generous — well past any real disc.
    const MAX_DURATION_SECS: u64 = 30 * 24 * 3600; // 30 days
    const MAX_RETENTION_DAYS: u64 = 3650; // 10 years
    if let Some(v) = saved.get("min_length_secs").and_then(|v| v.as_u64()) {
        cfg.min_length_secs = v.min(MAX_DURATION_SECS);
    }
    if let Some(v) = saved.get("main_feature").and_then(|v| v.as_bool()) {
        cfg.main_feature = v;
    }
    if let Some(v) = saved.get("auto_eject").and_then(|v| v.as_bool()) {
        cfg.auto_eject = v;
    }
    // String-enum fields are validated against their documented allowed
    // values at this same trust boundary as the numeric clamps below: a
    // corrupt value (e.g. output_format="garbage") would otherwise load
    // cleanly and only misbehave downstream depending on each consumer's
    // match/else. On an unknown value we keep the field's default and warn.
    if let Some(v) = saved.get("on_insert").and_then(|v| v.as_str()) {
        if matches!(v, "nothing" | "scan" | "rip") {
            cfg.on_insert = v.to_string();
        } else {
            tracing::warn!(value = %v, "settings.json on_insert has unknown value - using default");
        }
    }
    if let Some(v) = saved.get("output_format").and_then(|v| v.as_str()) {
        if matches!(v, "mkv" | "m2ts" | "iso" | "network") {
            cfg.output_format = v.to_string();
        } else {
            tracing::warn!(value = %v, "settings.json output_format has unknown value - using default");
        }
    }
    if let Some(v) = saved.get("network_target").and_then(|v| v.as_str()) {
        cfg.network_target = v.to_string();
    }
    // Collapsed double-lookup: read on_read_error once, validate it, and
    // record presence in the same pass (the flag still gates the legacy
    // abort_on_error migration below).
    let on_read_error_present = if let Some(v) = saved.get("on_read_error").and_then(|v| v.as_str())
    {
        if matches!(v, "stop" | "skip") {
            cfg.on_read_error = v.to_string();
        } else {
            tracing::warn!(value = %v, "settings.json on_read_error has unknown value - using default");
        }
        true
    } else {
        false
    };
    if let Some(v) = saved.get("max_retries").and_then(|v| v.as_u64()) {
        cfg.max_retries = (v.min(10)) as u8;
    }
    if let Some(v) = saved.get("keep_iso").and_then(|v| v.as_bool()) {
        cfg.keep_iso = v;
    }
    if let Some(v) = saved.get("abort_on_lost_secs").and_then(|v| v.as_u64()) {
        cfg.abort_on_lost_secs = v.min(MAX_DURATION_SECS);
    }
    if let Some(v) = saved.get("capture_without_keys").and_then(|v| v.as_bool()) {
        cfg.capture_without_keys = v;
    }
    if let Some(v) = saved.get("max_rip_duration_secs").and_then(|v| v.as_u64()) {
        cfg.max_rip_duration_secs = v.min(MAX_DURATION_SECS);
    }
    if let Some(v) = saved.get("min_pass_budget_secs").and_then(|v| v.as_u64()) {
        cfg.min_pass_budget_secs = v.min(MAX_DURATION_SECS);
    }
    if let Some(v) = saved
        .get("transport_recovery_delay_secs")
        .and_then(|v| v.as_u64())
    {
        cfg.transport_recovery_delay_secs = v.min(MAX_DURATION_SECS);
    }
    if let Some(v) = saved.get("decrypt_threads").and_then(|v| v.as_u64()) {
        // Clamp on load for parity with max_retries (line above) and as an
        // explicit trust-boundary bound on operator-edited settings.json.
        // libfreemkv caps internally, but the bound is implicit there; make
        // it explicit so a bogus value can't request an absurd pool size.
        cfg.decrypt_threads = (v as usize).min(256);
    }
    if let Some(v) = saved.get("log_retention_days").and_then(|v| v.as_u64()) {
        cfg.log_retention_days = v.min(MAX_RETENTION_DAYS);
    }
    // Migrate the legacy `abort_on_error` bool to the `on_read_error`
    // string, mirroring web.rs's POST-time migration. Only applied when
    // the modern `on_read_error` field is absent — an explicit
    // `on_read_error` always wins so re-saving a migrated settings.json
    // (which keeps the stale `abort_on_error` key) doesn't flip the
    // policy back. Both branches are present so `false` migrates to the
    // looser `skip` instead of silently reverting to the default `stop`.
    if !on_read_error_present {
        match saved.get("abort_on_error").and_then(|v| v.as_bool()) {
            Some(true) => cfg.on_read_error = "stop".to_string(),
            Some(false) => cfg.on_read_error = "skip".to_string(),
            None => {}
        }
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

/// Persist `cfg` to `settings.json` atomically (temp file + fsync + rename).
///
/// Returns `Ok(())` only when the on-disk file was successfully replaced.
/// On serialize failure, temp-write failure, or rename failure the change
/// did NOT land on disk and an `Err` is returned (and logged) so the caller
/// can surface the failure instead of falsely reporting success.
pub fn save(cfg: &Config) -> std::io::Result<()> {
    let path = cfg.settings_file();
    let json = match serde_json::to_string_pretty(cfg) {
        Ok(json) => json,
        Err(e) => {
            tracing::warn!(error = %e, "settings serialize failed; settings.json unchanged");
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
        }
    };
    // Write atomically: a SIGKILL or container OOM mid-`fs::write` would
    // truncate settings.json to zero or partial bytes; on next start
    // `load_saved` would silently reset every persisted field
    // (TMDB key, output dirs, max_retries, abort_on_lost_secs, …) to
    // env-var defaults. Watchtower restarts the container on every
    // release, so this isn't theoretical. Write to a sibling temp file,
    // fsync, then rename — POSIX rename is atomic within a filesystem.
    //
    // The fsync MUST succeed before the rename: a `sync_all` failure
    // (NFS ENOSPC/ESTALE, disk full) means the kernel never guaranteed
    // the bytes reached stable storage, so publishing them via rename
    // would defeat the whole crash-safety contract. Keep the writable
    // fd from the write step (rather than re-opening read-only) so the
    // sync covers what we just wrote, and bail before the rename on any
    // error — leaving the prior settings.json untouched.
    // Unique temp name per call. The web server spawns one thread per
    // request, so two concurrent settings saves could otherwise both open
    // the same fixed `{path}.tmp`, interleave their writes, and rename a
    // mangled file over settings.json. Disambiguate with pid + a process-
    // local monotonic counter so each save writes to its own sibling temp
    // and the rename publishes exactly one writer's complete bytes.
    static TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp = format!(
        "{path}.tmp.{}.{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let write_result = (|| -> std::io::Result<()> {
        use std::io::Write as _;
        // settings.json holds secrets (tmdb_api_key, keyserver_secret, webhook
        // tokens). Create the temp file 0600 so those bytes are never briefly
        // world-readable before the rename publishes them.
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            opts.mode(0o600);
        }
        let mut f = opts.open(&tmp)?;
        f.write_all(json.as_bytes())?;
        f.sync_all()?;
        Ok(())
    })();
    if let Err(e) = write_result {
        let _ = std::fs::remove_file(&tmp);
        tracing::warn!(error = %e, "settings write/fsync failed; settings.json unchanged");
        return Err(e);
    }
    if let Err(e) = std::fs::rename(&tmp, &path) {
        let _ = std::fs::remove_file(&tmp);
        tracing::warn!(error = %e, "settings rename failed; settings.json unchanged");
        return Err(e);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Per project convention, tests never touch /tmp (wiped on reboot).
    // Anchor scratch under the workspace's target/ (gitignored), not /tmp.
    fn scratch(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(0);
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        let d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target/test-scratch")
            .join(format!(
                "autorip-config-test-{}-{}-{}",
                std::process::id(),
                tag,
                n
            ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn cfg_in(dir: &std::path::Path) -> Config {
        Config {
            autorip_dir: dir.to_string_lossy().to_string(),
            ..Config::default()
        }
    }

    #[test]
    fn save_writes_atomically_and_leaves_no_temp() {
        let d = scratch("save_ok");
        let mut cfg = cfg_in(&d);
        cfg.tmdb_api_key = "abc123".into();
        save(&cfg).expect("save must succeed to a writable dir");

        let path = cfg.settings_file();
        let data = std::fs::read_to_string(&path).expect("settings.json written");
        let parsed: serde_json::Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["tmdb_api_key"].as_str(), Some("abc123"));
        // The sibling temp file must be cleaned up (renamed away).
        assert!(
            !std::path::Path::new(&format!("{path}.tmp")).exists(),
            "temp file should not linger after a successful save"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn concurrent_saves_never_corrupt_settings_json() {
        // The web server spawns one thread per request, so two settings
        // saves can run concurrently. With a fixed `{path}.tmp` they would
        // interleave writes into the same temp and rename a mangled file
        // over settings.json. The unique-per-call temp name (pid + counter)
        // gives each save its own sibling temp, so every rename publishes
        // one writer's COMPLETE bytes — the final file always parses and the
        // tmdb_api_key is one of the values that was actually written.
        let d = scratch("concurrent");
        const N: usize = 16;
        let handles: Vec<_> = (0..N)
            .map(|i| {
                let dir = d.clone();
                std::thread::spawn(move || {
                    let mut cfg = cfg_in(&dir);
                    // Distinct, generously-sized payload per thread so an
                    // interleave would produce invalid JSON, not a value
                    // that happens to parse.
                    cfg.tmdb_api_key = format!("key-{i}-{}", "x".repeat(4096));
                    save(&cfg)
                })
            })
            .collect();
        for h in handles {
            h.join().expect("save thread panicked").expect("save Err");
        }

        // Final file is valid JSON (no interleave corruption).
        let path = cfg_in(&d).settings_file();
        let data = std::fs::read_to_string(&path).expect("settings.json written");
        let parsed: serde_json::Value = serde_json::from_str(&data)
            .expect("settings.json must be valid JSON after concurrency");
        let key = parsed["tmdb_api_key"].as_str().unwrap_or("");
        assert!(
            key.starts_with("key-") && key.ends_with(&"x".repeat(4096)),
            "final key must be one writer's COMPLETE value, got {} chars",
            key.len()
        );

        // No temp turds linger (every save renamed its own unique temp away).
        let leftovers: Vec<_> = std::fs::read_dir(&d)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no .tmp files should remain, found {}",
            leftovers.len()
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn save_leaves_prior_settings_untouched_when_write_fails() {
        // Failure-mode contract: if the temp write/fsync fails, the rename
        // must never run, so any pre-existing settings.json is preserved
        // verbatim. We force the write to fail by pointing autorip_dir at
        // a path whose parent does not exist (open(2) ENOENT), after
        // seeding a known-good settings.json one level up.
        let d = scratch("save_fail");
        let good = cfg_in(&d);
        // Seed a valid prior file.
        let mut prior = good.clone();
        prior.tmdb_api_key = "PRIOR".into();
        save(&prior).expect("seeding the prior settings.json must succeed");
        let good_path = good.settings_file();
        let before = std::fs::read_to_string(&good_path).unwrap();

        // Now attempt a save whose temp open will fail: settings_file()
        // lives under a non-existent subdirectory, so OpenOptions::open
        // returns ENOENT and save() must bail before any rename.
        let bad = cfg_in(&d.join("does-not-exist"));
        let mut changed = bad.clone();
        changed.tmdb_api_key = "SHOULD_NOT_LAND".into();
        // This save is EXPECTED to fail (ENOENT) — the point of the test.
        assert!(save(&changed).is_err(), "save into a missing dir must Err");

        // The good file is byte-for-byte intact.
        let after = std::fs::read_to_string(&good_path).unwrap();
        assert_eq!(before, after, "prior settings.json must be untouched");
        // No temp turd left behind in the bad location either.
        assert!(!std::path::Path::new(&format!("{}.tmp", bad.settings_file())).exists());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn load_saved_clamps_pathological_durations() {
        let d = scratch("clamp");
        let path = cfg_in(&d).settings_file();
        std::fs::write(
            &path,
            serde_json::json!({
                "max_rip_duration_secs": u64::MAX,
                "min_pass_budget_secs": u64::MAX,
                "log_retention_days": u64::MAX,
            })
            .to_string(),
        )
        .unwrap();
        let cfg = load_saved(cfg_in(&d));
        assert!(cfg.max_rip_duration_secs <= 30 * 24 * 3600);
        assert!(cfg.min_pass_budget_secs <= 30 * 24 * 3600);
        assert!(cfg.log_retention_days <= 3650);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn port_not_serialized_into_settings_json() {
        let d = scratch("port");
        let cfg = cfg_in(&d);
        save(&cfg).expect("save must succeed to a writable dir");
        let data = std::fs::read_to_string(cfg.settings_file()).unwrap();
        assert!(
            !data.contains("\"port\""),
            "port is bootstrap-only and must not be persisted: {data}"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn decrypt_threads_clamped_on_load() {
        let d = scratch("decrypt_clamp");
        let base = cfg_in(&d);
        std::fs::write(base.settings_file(), r#"{"decrypt_threads": 100000}"#).unwrap();
        let loaded = load_saved(base);
        assert_eq!(loaded.decrypt_threads, 256, "huge value must clamp to 256");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn decrypt_threads_small_value_preserved() {
        let d = scratch("decrypt_small");
        let base = cfg_in(&d);
        std::fs::write(base.settings_file(), r#"{"decrypt_threads": 8}"#).unwrap();
        let loaded = load_saved(base);
        assert_eq!(loaded.decrypt_threads, 8);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn corrupt_settings_reverts_to_defaults_without_panicking() {
        let d = scratch("corrupt");
        let base = cfg_in(&d);
        // Partial write — invalid JSON. Must not panic and must keep defaults.
        std::fs::write(base.settings_file(), r#"{"max_retries": 5, "abort_on_l"#).unwrap();
        let loaded = load_saved(cfg_in(&d));
        assert_eq!(loaded.max_retries, Config::default().max_retries);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn missing_settings_file_uses_defaults() {
        let d = scratch("missing");
        // No settings.json written.
        let loaded = load_saved(cfg_in(&d));
        assert_eq!(loaded.max_retries, Config::default().max_retries);
        assert_eq!(
            loaded.abort_on_lost_secs,
            Config::default().abort_on_lost_secs
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn save_then_load_roundtrips_and_returns_ok() {
        let d = scratch("roundtrip");
        let mut base = cfg_in(&d);
        base.abort_on_lost_secs = 30;
        base.max_retries = 3;
        base.decrypt_threads = 4;
        save(&base).expect("save must succeed to a writable dir");
        let loaded = load_saved(cfg_in(&d));
        assert_eq!(loaded.abort_on_lost_secs, 30);
        assert_eq!(loaded.max_retries, 3);
        assert_eq!(loaded.decrypt_threads, 4);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn save_to_unwritable_dir_returns_err() {
        // settings_file() under a path whose parent does not exist -> open
        // of the .tmp fails -> Err, not a false success.
        let base = Config {
            autorip_dir: "/nonexistent-autorip-dir-xyz/sub".into(),
            ..Config::default()
        };
        assert!(save(&base).is_err());
    }

    /// Write a settings.json containing `json` under `dir`, then run it
    /// through `load_saved`. Mirrors `cfg_in` but seeds the file first.
    fn load_with(dir: &std::path::Path, json: &str) -> Config {
        std::fs::write(cfg_in(dir).settings_file(), json).unwrap();
        load_saved(cfg_in(dir))
    }

    #[test]
    fn legacy_abort_on_error_false_migrates_to_skip() {
        let d = scratch("abort_false");
        // Pre-migration settings.json: only the legacy bool, no
        // on_read_error field. False must become the looser "skip",
        // not silently fall through to the default "stop".
        let cfg = load_with(&d, r#"{"abort_on_error": false}"#);
        assert_eq!(cfg.on_read_error, "skip");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn legacy_abort_on_error_true_migrates_to_stop() {
        let d = scratch("abort_true");
        let cfg = load_with(&d, r#"{"abort_on_error": true}"#);
        assert_eq!(cfg.on_read_error, "stop");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn explicit_on_read_error_wins_over_legacy_key() {
        let d = scratch("explicit_wins");
        // A migrated settings.json keeps the stale abort_on_error key
        // alongside the modern field; the explicit field must win so
        // re-loading doesn't flip the policy back.
        let cfg = load_with(&d, r#"{"on_read_error": "skip", "abort_on_error": true}"#);
        assert_eq!(cfg.on_read_error, "skip");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn no_legacy_key_uses_default() {
        let d = scratch("no_legacy");
        let cfg = load_with(&d, r#"{}"#);
        assert_eq!(cfg.on_read_error, "stop");
        let _ = std::fs::remove_dir_all(&d);
    }
}
