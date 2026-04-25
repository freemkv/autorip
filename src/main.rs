mod config;
mod history;
mod log;
mod mover;
mod observe;
mod ripper;
mod tmdb;
mod util;
mod verify;
mod web;
mod webhook;

use std::io::Read as _;
use std::sync::atomic::{AtomicBool, Ordering};

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

    // Start mover thread
    let _mover_handle = std::thread::spawn({
        let cfg = cfg.clone();
        move || mover::run(&cfg)
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

    // Main loop: poll drives (checks SHUTDOWN flag internally)
    ripper::drive_poll_loop(&cfg);

    // Drain any rip threads that are still mid-flight so we don't
    // exit the process while libfreemkv is holding a SCSI session
    // and writing into staging. Bounded so a stuck drive can't
    // pin shutdown indefinitely.
    ripper::join_all_rip_threads(std::time::Duration::from_secs(35));

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
