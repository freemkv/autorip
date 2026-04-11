use crate::config::Config;
use std::sync::{Arc, RwLock};

pub fn run(cfg: &Arc<RwLock<Config>>) {
    let port = cfg.read().unwrap().port;
    eprintln!("Web UI: http://0.0.0.0:{}", port);
    // TODO: implement HTTP server with SSE
    loop {
        std::thread::sleep(std::time::Duration::from_secs(60));
    }
}
