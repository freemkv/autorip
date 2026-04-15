mod config;
mod history;
mod log;
mod mover;
mod ripper;
mod tmdb;
mod web;
mod webhook;

use std::io::Read as _;

fn main() {
    // Load config
    let cfg = config::load();

    // Start mover thread (watches staging -> moves to output)
    let _mover_handle = std::thread::spawn({
        let cfg = cfg.clone();
        move || mover::run(&cfg)
    });

    // Start web server thread
    let _web_handle = std::thread::spawn({
        let cfg = cfg.clone();
        move || web::run(&cfg)
    });

    // Start KEYDB auto-update thread (updates once daily)
    let _keydb_handle = std::thread::spawn({
        let cfg2 = cfg.clone();
        move || loop {
            std::thread::sleep(std::time::Duration::from_secs(24 * 3600));
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

    // Main loop: poll drives
    ripper::drive_poll_loop(&cfg);
}
