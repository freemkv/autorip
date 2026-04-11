mod config;
mod history;
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

    // Main loop: poll drives
    ripper::drive_poll_loop(&cfg);
}
