mod config;
mod history;
mod log;
mod mover;
mod ripper;
mod tmdb;
mod web;
mod webhook;

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
        let _cfg = cfg.clone();
        move || loop {
            std::thread::sleep(std::time::Duration::from_secs(24 * 3600));
            if let Some(ref url) = std::env::var("KEYDB_URL").ok() {
                match libfreemkv::keydb::update(url) {
                    Ok(r) => log::syslog(&format!("KEYDB updated: {} entries", r.entries)),
                    Err(e) => log::syslog(&format!("KEYDB update failed: {e}")),
                }
            }
        }
    });

    // Main loop: poll drives
    ripper::drive_poll_loop(&cfg);
}
