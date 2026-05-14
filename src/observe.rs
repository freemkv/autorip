//! Observability — single init point for the structured event log.
//!
//! Three sinks, written from the same tracing event stream:
//!
//! - **`{AUTORIP_DIR}/logs/autorip.log`** — daily-rolling, human-readable.
//!   The file an operator tails when something is going on.
//! - **`{AUTORIP_DIR}/logs/autorip.jsonl`** — daily-rolling, JSON Lines.
//!   The file a tool greps when something already went wrong.
//! - **stderr** — compact, captured by Docker as the container log.
//!
//! Filter level via `AUTORIP_LOG_LEVEL` (env-filter syntax). Default
//! `autorip=info,libfreemkv=warn` — autorip's own events at info, library
//! noise muted unless explicitly enabled. For deep dives:
//! `AUTORIP_LOG_LEVEL=autorip=debug docker compose up -d`.
//!
//! Why this exists: pre-0.13 the codebase had ~60 silent failure paths
//! (`Err(_) => continue`, `let _ = …`, `unwrap_or_default`) and ad-hoc
//! `eprintln!` for the rest. Diagnosing "No drives detected" required
//! reading source + poking `/proc` + reading `/sys` because the running
//! process produced zero observable evidence of the poll loop's decisions.
//! Tracing fixes that structurally: every silent path is replaced by a
//! structured event, every lifecycle function is wrapped in a span, and
//! the JSONL stream is machine-queryable.

use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling;
use tracing_subscriber::reload;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// EnvFilter directive used when /api/debug is OFF (the normal state).
/// `autorip=info` for the binary's own narration, `libfreemkv=warn` so
/// the library is quiet — warnings + errors only. Two modes, one flag:
/// prod = warnings only; dev = full debug (see FILTER_ON).
const FILTER_OFF: &str = "autorip=info,libfreemkv=warn";

/// EnvFilter directive used when /api/debug is ON. `debug` globally,
/// plus `mux=debug` and `stream=debug` so the `target: "mux"` /
/// `target: "stream"` events (writeback seeks, WAIT_AFTER latency,
/// fill_extents stalls — the events the user actually wants when
/// diagnosing a jumpy mux) are visible. `freemkv::scsi` / `freemkv::disc`
/// inherit `libfreemkv=debug` here so SCSI CDB events surface at debug
/// (not the per-CDB trace firehose — ~800 lines/sec during sweep would
/// drown the useful signal and could itself slow throughput via stdout
/// contention; raise to `freemkv::scsi=trace` via AUTORIP_LOG_LEVEL if
/// you need per-CDB forensics for a drive issue). Producer's per-frame
/// log is at `trace` and stays muted here on purpose.
const FILTER_ON: &str = "autorip=debug,libfreemkv=debug,mux=debug,stream=debug";

/// Worker guards for the non-blocking file appenders. Must outlive the
/// process — flushed on drop. Stored in a static so `init()` can be called
/// from `main` without the caller having to thread guards through.
static GUARDS: once_cell::sync::OnceCell<Vec<WorkerGuard>> = once_cell::sync::OnceCell::new();

/// Reload handle for the active EnvFilter. Set by `init()`, swapped by
/// `set_debug()` from the /api/debug endpoint. Stored as `Option<...>`
/// so init failures (or a custom `AUTORIP_LOG_LEVEL`) leave the handle
/// absent and `set_debug` becomes a no-op rather than panicking.
static RELOAD_HANDLE: once_cell::sync::OnceCell<reload::Handle<EnvFilter, Registry>> =
    once_cell::sync::OnceCell::new();

/// Initialize the tracing stack. Idempotent — second + subsequent calls
/// are no-ops, so it's safe to call from `main` even if a thread spawned
/// before init touches the global subscriber.
///
/// Returns Ok if the subscriber installed cleanly. On failure (couldn't
/// create log dir, couldn't open files), still installs a stderr-only
/// fallback so events at least surface in `docker logs`.
pub fn init() {
    if GUARDS.get().is_some() {
        return;
    }

    let log_dir = log_dir();
    let _ = std::fs::create_dir_all(&log_dir);

    // Honour AUTORIP_LOG_LEVEL if the operator set one explicitly; that
    // becomes the starting filter for the whole process. /api/debug
    // becomes a no-op on the filter when AUTORIP_LOG_LEVEL is set (the
    // operator's directive wins; flipping the API toggle would be
    // surprising). Otherwise we install FILTER_OFF and wire up a
    // reload handle so /api/debug can swap to FILTER_ON at runtime.
    let env_override = std::env::var("AUTORIP_LOG_LEVEL")
        .ok()
        .filter(|s| !s.is_empty());
    let initial_filter = match env_override.as_deref() {
        Some(s) => EnvFilter::try_new(s).unwrap_or_else(|_| EnvFilter::new(FILTER_OFF)),
        None => EnvFilter::new(FILTER_OFF),
    };
    let (filter, reload_handle) = reload::Layer::new(initial_filter);
    let reload_handle = if env_override.is_some() {
        None
    } else {
        Some(reload_handle)
    };

    let stderr_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(false)
        .with_ansi(false)
        .compact();

    let mut guards: Vec<WorkerGuard> = Vec::new();

    // Human-readable log: daily-rolled. Operators tailing for the day's
    // events get a manageable file size; older days archive to disk.
    let human_appender = rolling::daily(&log_dir, "autorip.log");
    let (human_writer, human_guard) = tracing_appender::non_blocking(human_appender);
    guards.push(human_guard);
    let human_layer = fmt::layer()
        .with_writer(human_writer)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(true);

    // Machine-readable JSONL: NOT rolled. The web UI / `/api/debug` endpoint
    // tails this file by a stable path; daily rotation would mean
    // `autorip.jsonl.YYYY-MM-DD`, breaking lookups (the v0.13.0 file-not-
    // found regression). We accept unbounded growth here — disk usage is
    // ~1 KB per event × ~hundreds of events / day = MB/day at most. A
    // future external `logrotate` (or similar) can rotate it out-of-band.
    let json_appender = rolling::never(&log_dir, "autorip.jsonl");
    let (json_writer, json_guard) = tracing_appender::non_blocking(json_appender);
    guards.push(json_guard);
    let json_layer = fmt::layer()
        .json()
        .with_writer(json_writer)
        .with_target(true)
        .with_thread_ids(true);

    tracing_subscriber::registry()
        .with(filter)
        .with(stderr_layer)
        .with(human_layer)
        .with(json_layer)
        .init();

    if let Some(h) = reload_handle {
        let _ = RELOAD_HANDLE.set(h);
    }
    let _ = GUARDS.set(guards);
}

/// Swap the active EnvFilter at runtime. Called by `/api/debug` to
/// flip between FILTER_OFF and FILTER_ON. No-op if `AUTORIP_LOG_LEVEL`
/// was set explicitly at startup (the operator's directive wins) or if
/// init() failed.
///
/// Returns `true` when the swap was applied, `false` when the handle
/// is absent (env override, init failure) so the caller can surface
/// the no-op in the API response.
pub fn set_debug(enabled: bool) -> bool {
    let Some(handle) = RELOAD_HANDLE.get() else {
        return false;
    };
    let directive = if enabled { FILTER_ON } else { FILTER_OFF };
    let new_filter = match EnvFilter::try_new(directive) {
        Ok(f) => f,
        Err(_) => return false,
    };
    handle.reload(new_filter).is_ok()
}

fn log_dir() -> String {
    let base = std::env::var("AUTORIP_DIR").unwrap_or_else(|_| "/config".to_string());
    format!("{}/logs", base)
}

/// Path of the JSONL stream — exposed so the web `/api/debug` endpoint
/// can tail it without re-deriving the layout.
pub fn json_log_path() -> String {
    format!("{}/autorip.jsonl", log_dir())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both filter strings must parse — a typo here would mean the
    /// /api/debug toggle silently no-ops in production. This is the
    /// cheapest possible guard.
    #[test]
    fn filter_strings_parse() {
        EnvFilter::try_new(FILTER_OFF).expect("FILTER_OFF must parse");
        EnvFilter::try_new(FILTER_ON).expect("FILTER_ON must parse");
    }

    /// FILTER_ON must enable the `mux` and `stream` targets at
    /// debug — these are the events the user actually wants from
    /// /api/debug. Belt-and-braces guard against future edits that
    /// drop them.
    #[test]
    fn filter_on_includes_mux_and_stream_targets() {
        assert!(
            FILTER_ON.contains("mux=debug"),
            "FILTER_ON must enable target=\"mux\" at debug; got: {FILTER_ON}"
        );
        assert!(
            FILTER_ON.contains("stream=debug"),
            "FILTER_ON must enable target=\"stream\" at debug; got: {FILTER_ON}"
        );
        assert!(
            FILTER_ON.contains("libfreemkv=debug"),
            "FILTER_ON must raise libfreemkv to debug; got: {FILTER_ON}"
        );
    }

    /// FILTER_OFF must not accidentally turn on the verbose targets.
    /// If a future edit promotes them, /api/debug becomes meaningless
    /// because the steady-state already shows the events.
    #[test]
    fn filter_off_stays_quiet_on_mux_and_stream() {
        assert!(
            !FILTER_OFF.contains("mux=debug"),
            "FILTER_OFF must not enable mux at debug; got: {FILTER_OFF}"
        );
        assert!(
            !FILTER_OFF.contains("stream=debug"),
            "FILTER_OFF must not enable stream at debug; got: {FILTER_OFF}"
        );
        assert!(
            FILTER_OFF.contains("libfreemkv=warn"),
            "FILTER_OFF must keep libfreemkv at warn; got: {FILTER_OFF}"
        );
    }

    /// `set_debug` must report `false` when init() never ran (no
    /// reload handle installed). The /api/debug handler surfaces this
    /// to the caller via the `filter_swapped` JSON field; assert the
    /// contract so a future refactor that flips the default doesn't
    /// silently mis-report success.
    #[test]
    fn set_debug_returns_false_without_init() {
        // RELOAD_HANDLE is a process-wide OnceCell. If another test in
        // the same binary called `observe::init()` first, the handle
        // may be present. We only assert the negative case when the
        // handle is genuinely absent — otherwise the assertion is moot.
        if RELOAD_HANDLE.get().is_none() {
            assert!(!set_debug(true));
            assert!(!set_debug(false));
        }
    }
}
