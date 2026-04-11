use crate::config::Config;
use std::sync::{Arc, RwLock};

pub fn run(cfg: &Arc<RwLock<Config>>) {
    let _cfg = cfg;
    // TODO: watch staging dir, move completed rips to output
    loop {
        std::thread::sleep(std::time::Duration::from_secs(10));
    }
}
