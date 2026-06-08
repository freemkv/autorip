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

/// Payload for a `rip_complete` webhook notification. String fields are
/// pre-formatted for display; numeric fields are rounded by [`send_rich`]
/// before serialization.
pub struct RipEvent<'a> {
    /// Event name (e.g. `"rip_complete"`).
    pub event: &'a str,
    /// Resolved movie/show title.
    pub title: &'a str,
    /// Release year (0 = unknown).
    pub year: u16,
    /// Disc format label (e.g. `"UHD"`, `"BluRay"`, `"DVD"`).
    pub format: &'a str,
    /// TMDB poster URL, or empty if none.
    pub poster_url: &'a str,
    /// Human-readable runtime string (preformatted, e.g. `"2h 14m"`).
    pub duration: &'a str,
    /// Human-readable codec summary (preformatted).
    pub codecs: &'a str,
    /// Output file size in gigabytes (rounded to 0.1 GB on send).
    pub size_gb: f64,
    /// Average rip throughput in MB/s (rounded to 0.1 on send).
    pub speed_mbs: f64,
    /// Total wall-clock time for the rip, in seconds (rounded to whole seconds on send).
    pub elapsed_secs: f64,
    /// Final destination path of the muxed output.
    pub output_path: &'a str,
    /// Raw count of SCSI read errors encountered.
    pub errors: u32,
    /// Estimated unrecoverable main-feature video loss, in seconds (rounded to ms on send).
    pub lost_video_secs: f64,
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
        "errors": ev.errors,
        "lost_video_secs": (ev.lost_video_secs * 1000.0).round() / 1000.0,
    });
    fire(cfg, &payload);
}

fn fire(cfg: &Config, payload: &serde_json::Value) {
    let urls: Vec<String> = cfg
        .webhook_urls
        .iter()
        .filter(|u| !u.trim().is_empty())
        .cloned()
        .collect();
    if urls.is_empty() {
        return;
    }
    let body = payload.to_string();
    std::thread::spawn(move || {
        for url in &urls {
            // SSRF guard at fire time (defence-in-depth; the URL is also
            // validated at store time in handle_settings_post). Resolve +
            // validate once and pin the connection to those IPs so DNS
            // rebinding can't redirect this POST to an internal/metadata
            // host, and so a permitted public URL can't 30x-redirect there
            // either (guarded_agent uses redirects(0)).
            let pinned = match crate::web::validate_fetch_url(url) {
                Ok(addrs) => addrs,
                Err(e) => {
                    crate::log::syslog(&format!("Webhook blocked {}: {}", url, e));
                    continue;
                }
            };
            let agent = crate::web::guarded_agent(pinned);
            match agent
                .post(url)
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
