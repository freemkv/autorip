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

pub struct RipEvent<'a> {
    pub event: &'a str,
    pub title: &'a str,
    pub year: u16,
    pub format: &'a str,
    pub poster_url: &'a str,
    pub duration: &'a str,
    pub codecs: &'a str,
    pub size_gb: f64,
    pub speed_mbs: f64,
    pub elapsed_secs: f64,
    pub output_path: &'a str,
}

/// Rich payload with full metadata — used for rip_complete.
pub fn send_rich(cfg: &Config, ev: &RipEvent) {
    let payload = serde_json::json!({
        "event": ev.event,
        "title": ev.title,
        "year": ev.year,
        "format": ev.format,
        "poster_url": ev.poster_url,
        "duration": ev.duration,
        "codecs": ev.codecs,
        "size_gb": (ev.size_gb * 10.0).round() / 10.0,
        "speed_mbs": (ev.speed_mbs * 10.0).round() / 10.0,
        "elapsed_secs": ev.elapsed_secs.round() as u64,
        "output_path": ev.output_path,
    });
    fire(cfg, &payload);
}

fn fire(cfg: &Config, payload: &serde_json::Value) {
    let urls: Vec<String> = cfg
        .webhook_urls
        .iter()
        .filter(|u| !u.is_empty())
        .cloned()
        .collect();
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
