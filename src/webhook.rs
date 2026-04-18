use crate::config::Config;

/// Notify that a file was moved to its final destination.
pub fn send_move(cfg: &Config, title: &str, dest_path: &str) {
    let payload = serde_json::json!({
        "event": "move_complete",
        "title": title,
        "output_path": dest_path,
    });
    fire(cfg, &payload);
}

/// Rich payload with full metadata — used for rip_complete.
pub fn send_rich(
    cfg: &Config,
    event: &str,
    title: &str,
    year: u16,
    format: &str,
    poster_url: &str,
    duration: &str,
    codecs: &str,
    size_gb: f64,
    speed_mbs: f64,
    elapsed_secs: f64,
    output_path: &str,
) {
    let payload = serde_json::json!({
        "event": event,
        "title": title,
        "year": year,
        "format": format,
        "poster_url": poster_url,
        "duration": duration,
        "codecs": codecs,
        "size_gb": (size_gb * 10.0).round() / 10.0,
        "speed_mbs": (speed_mbs * 10.0).round() / 10.0,
        "elapsed_secs": elapsed_secs.round() as u64,
        "output_path": output_path,
    });
    fire(cfg, &payload);
}

fn fire(cfg: &Config, payload: &serde_json::Value) {
    let urls: Vec<String> = cfg.webhook_urls.iter().filter(|u| !u.is_empty()).cloned().collect();
    if urls.is_empty() {
        return;
    }
    let body = payload.to_string();
    std::thread::spawn(move || {
        for url in &urls {
            match ureq::post(url)
                .set("Content-Type", "application/json")
                .send_string(&body)
            {
                Ok(_) => {
                    crate::log::syslog(&format!("Webhook sent to {}", url));
                }
                Err(e) => {
                    crate::log::syslog(&format!("Webhook failed {}: {}", url, e));
                }
            }
        }
    });
}
