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
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Worker guards for the non-blocking file appenders. Must outlive the
/// process — flushed on drop. Stored in a static so `init()` can be called
/// from `main` without the caller having to thread guards through.
static GUARDS: once_cell::sync::OnceCell<Vec<WorkerGuard>> = once_cell::sync::OnceCell::new();

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

    let filter = EnvFilter::try_from_env("AUTORIP_LOG_LEVEL")
        .unwrap_or_else(|_| EnvFilter::new("autorip=info,libfreemkv=warn"));

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

    let _ = GUARDS.set(guards);
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
