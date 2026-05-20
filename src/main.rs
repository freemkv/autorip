mod config;
mod history;
mod log;
mod mover;
mod muxer;
mod observe;
mod ripper;
mod tmdb;
mod util;
mod verify;
mod web;
mod webhook;

use std::io::Read as _;
use std::sync::atomic::{AtomicBool, Ordering};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

fn main() {
    // Tracing FIRST — before any log call, panic hook, or thread spawn.
    // Sets up stderr + autorip.log + autorip.jsonl sinks. Filter via
    // AUTORIP_LOG_LEVEL env (default `autorip=info,libfreemkv=warn`).
    observe::init();

    // Signal handler for graceful shutdown
    #[cfg(unix)]
    unsafe {
        libc::signal(
            libc::SIGTERM,
            handle_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGINT,
            handle_signal as *const () as libc::sighandler_t,
        );
    }

    // Panic hook — log any thread panic to the system log so we can debug
    // post-mortem without a live stderr. Without this the user just sees "UI
    // crashed" with no clue which thread or path blew up.
    std::panic::set_hook(Box::new(|info| {
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let msg = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
            .unwrap_or("<non-string panic>");
        let thread = std::thread::current()
            .name()
            .unwrap_or("<unnamed>")
            .to_string();
        // Both: structured event for the JSONL stream (greppable post-mortem)
        // AND the legacy syslog line so the per-device file + UI keep working.
        tracing::error!(thread = %thread, location = %loc, message = %msg, "panic");
        log::syslog(&format!("PANIC in thread '{thread}' at {loc}: {msg}"));
    }));

    log::syslog(&format!(
        "autorip starting (v{}, edition 2024)",
        env!("CARGO_PKG_VERSION")
    ));
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        target = std::env::consts::OS,
        arch = std::env::consts::ARCH,
        "autorip starting"
    );

    // Load config
    let cfg = config::load();

    // Ensure KEYDB exists — download on first boot if URL is configured
    if libfreemkv::keydb::default_path()
        .ok()
        .map(|p| p.exists())
        .unwrap_or(false)
    {
        log::syslog("KEYDB found");
    } else {
        let url = cfg
            .read()
            .ok()
            .map(|c| c.keydb_url.clone())
            .unwrap_or_default();
        if !url.is_empty() {
            log::syslog("KEYDB not found, downloading...");
            match ureq::get(&url).call() {
                Ok(resp) => {
                    let mut buf = Vec::new();
                    if resp
                        .into_reader()
                        .take(100 * 1024 * 1024)
                        .read_to_end(&mut buf)
                        .is_ok()
                    {
                        match libfreemkv::keydb::save(&buf) {
                            Ok(r) => {
                                log::syslog(&format!("KEYDB downloaded: {} entries", r.entries))
                            }
                            Err(e) => log::syslog(&format!("KEYDB save failed: {e}")),
                        }
                    }
                }
                Err(e) => log::syslog(&format!("KEYDB download failed: {e}")),
            }
        }
    }

    // Start mover thread.
    let _mover_handle = std::thread::spawn({
        let cfg = cfg.clone();
        move || mover::run(&cfg)
    });

    // Start mux worker thread — pipelines mux behind the drive so a
    // disc can be ripped on one device while a prior title muxes in
    // the background. v0.25.3 scaffold; phase 3 wires the actual mux
    // dispatch. Today the loop scans staging for `.ripped` markers and
    // logs only — no behavioural change until the drive thread starts
    // writing those markers.
    let _muxer_handle = std::thread::spawn({
        let cfg = cfg.clone();
        move || muxer::run(&cfg)
    });

    // Start web server thread
    let _web_handle = std::thread::spawn({
        let cfg = cfg.clone();
        move || web::run(&cfg)
    });

    // Start KEYDB auto-update thread. Single source of truth for periodic
    // KEYDB refresh — pre-0.13 there was also a cron entry that spawned a
    // second `autorip` binary, which raced this thread for /dev/sg* and
    // port 8080. Cron path was removed; this is now the only daily updater.
    let _keydb_handle = std::thread::spawn({
        let cfg2 = cfg.clone();
        move || {
            tracing::info!("keydb update thread starting (24h interval)");
            'outer: loop {
                // 24h sleep in 1s chunks so SHUTDOWN is observed within ~1s.
                for _ in 0..(24 * 3600) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    if SHUTDOWN.load(Ordering::Relaxed) {
                        break 'outer;
                    }
                }
                let url = cfg2
                    .read()
                    .ok()
                    .map(|c| c.keydb_url.clone())
                    .unwrap_or_default();
                if url.is_empty() {
                    continue;
                }
                tracing::info!(url = %url, "keydb: starting daily update");
                match ureq::get(&url).call() {
                    Ok(resp) => {
                        let mut buf = Vec::new();
                        if resp
                            .into_reader()
                            .take(100 * 1024 * 1024)
                            .read_to_end(&mut buf)
                            .is_ok()
                        {
                            match libfreemkv::keydb::save(&buf) {
                                Ok(r) => {
                                    log::syslog(&format!("KEYDB updated: {} entries", r.entries))
                                }
                                Err(e) => log::syslog(&format!("KEYDB update failed: {e}")),
                            }
                        } else {
                            tracing::warn!("keydb: response read failed");
                        }
                    }
                    Err(e) => log::syslog(&format!("KEYDB update failed: {e}")),
                }
            }
            tracing::info!("keydb update thread stopping");
        }
    });

    // Log prune thread — replaces the v0.25.5 cron-based daily cleanup
    // (./entrypoint.sh used to drop a line in /etc/cron.d). Moving this
    // in-process let us drop the cron package + the cron service from
    // the image (alpine swap in v0.25.6), shrinking the deployed
    // container by ~5 MB and eliminating a runtime dependency.
    let _log_prune_handle = std::thread::spawn({
        let cfg = cfg.clone();
        move || {
            tracing::info!("log prune thread starting (24h interval)");
            let retention_days: u64 = std::env::var("LOG_RETENTION_DAYS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30);
            'outer: loop {
                // Wait first; on a fresh container the logs dir has only
                // a few minutes of data and pruning is a no-op anyway.
                for _ in 0..(24 * 3600) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    if SHUTDOWN.load(Ordering::Relaxed) {
                        break 'outer;
                    }
                }
                let log_dir = cfg.read().ok().map(|c| c.log_dir()).unwrap_or_default();
                if log_dir.is_empty() {
                    continue;
                }
                prune_old_logs(&log_dir, retention_days);
            }
            tracing::info!("log prune thread stopping");
        }
    });

    // Main loop: poll drives (checks SHUTDOWN flag internally)
    ripper::drive_poll_loop(&cfg);

    // Drain any rip threads that are still mid-flight so we don't
    // exit the process while libfreemkv is holding a SCSI session
    // and writing into staging. Bounded so a stuck drive can't
    // pin shutdown indefinitely.
    ripper::join_all_rip_threads(std::time::Duration::from_secs(60));

    log::syslog("autorip stopped");
}

#[cfg(unix)]
extern "C" fn handle_signal(_sig: libc::c_int) {
    if SHUTDOWN.load(Ordering::Relaxed) {
        // Second signal — force exit
        unsafe { libc::_exit(1) };
    }
    SHUTDOWN.store(true, Ordering::Relaxed);
}

/// Delete `.log` files under `log_dir` older than `retention_days`.
/// Replaces the v0.25.5 cron-based cleanup so the deployed image
/// doesn't need a cron daemon. Single-shot; the caller drives the
/// daily cadence.
fn prune_old_logs(log_dir: &str, retention_days: u64) {
    let cutoff = std::time::SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(retention_days * 86_400));
    let Some(cutoff) = cutoff else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return;
    };
    let mut pruned = 0u32;
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("log") {
            continue;
        }
        let Ok(meta) = entry.metadata() else { continue };
        let Ok(mtime) = meta.modified() else { continue };
        if mtime < cutoff && std::fs::remove_file(&path).is_ok() {
            pruned += 1;
        }
    }
    if pruned > 0 {
        log::syslog(&format!(
            "log prune: removed {pruned} files older than {retention_days}d from {log_dir}"
        ));
    }
}
