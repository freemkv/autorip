mod config;
mod history;
mod log;
mod mover;
mod ripper;
mod tmdb;
mod util;
mod web;
mod webhook;

use std::io::Read as _;
use std::sync::atomic::{AtomicBool, Ordering};

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

fn main() {
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

    log::syslog("autorip starting");

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

    // Start KEYDB auto-update thread
    let _keydb_handle = std::thread::spawn({
        let cfg2 = cfg.clone();
        move || loop {
            // Check every hour, update once daily
            for _ in 0..24 {
                std::thread::sleep(std::time::Duration::from_secs(3600));
                if SHUTDOWN.load(Ordering::Relaxed) {
                    return;
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
                            Ok(r) => log::syslog(&format!("KEYDB updated: {} entries", r.entries)),
                            Err(e) => log::syslog(&format!("KEYDB update failed: {e}")),
                        }
                    }
                }
                Err(e) => log::syslog(&format!("KEYDB update failed: {e}")),
            }
        }
    });

    // Main loop: poll drives (checks SHUTDOWN flag internally)
    ripper::drive_poll_loop(&cfg);

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
