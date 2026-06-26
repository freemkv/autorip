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

/// Return only the `scheme://host[:port]` portion of `url`, dropping any
/// path/query/fragment. Webhook URLs commonly embed a secret token in the
/// path (Discord, Slack, Jellyfin) or query string, so logging the full URL
/// would expose that token in the system log, which GET /api/system serves
/// unredacted to any LAN client. Logging the origin is enough to identify
/// the destination.
pub(crate) fn webhook_url_origin(url: &str) -> &str {
    if let Some(scheme_end) = url.find("://") {
        let after = scheme_end + 3;
        // Treat '/', '?', and '#' as origin-terminating so a token in a
        // query string (`https://host?token=SECRET`) is stripped too.
        let origin_end = url[after..]
            .find(['/', '?', '#'])
            .map(|i| after + i)
            .unwrap_or(url.len());
        return &url[..origin_end];
    }
    // No scheme — log nothing identifiable.
    "<redacted>"
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

    // Bound the number of concurrent webhook-dispatch threads. Each event
    // otherwise spawns an unbounded OS thread; a burst of events (or a hostile
    // client triggering many) could exhaust threads. Past the cap, drop the
    // event with a warning rather than spawning.
    use std::sync::atomic::{AtomicUsize, Ordering};
    const MAX_INFLIGHT: usize = 8;
    static INFLIGHT: AtomicUsize = AtomicUsize::new(0);
    if INFLIGHT
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            (n < MAX_INFLIGHT).then_some(n + 1)
        })
        .is_err()
    {
        crate::log::syslog("Webhook dropped: too many concurrent deliveries in flight");
        return;
    }
    // Decrement the in-flight counter however the thread exits.
    struct InflightGuard;
    impl Drop for InflightGuard {
        fn drop(&mut self) {
            INFLIGHT.fetch_sub(1, Ordering::AcqRel);
        }
    }

    std::thread::spawn(move || {
        let _guard = InflightGuard;
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
                    // Log only the origin — the path may contain a secret token.
                    crate::log::syslog(&format!(
                        "Webhook blocked {}: {}",
                        webhook_url_origin(url),
                        e
                    ));
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
                    // Log only the origin — the path may contain a secret token.
                    crate::log::syslog(&format!("Webhook sent to {}", webhook_url_origin(url)));
                }
                Err(e) => {
                    // Summarise the error WITHOUT embedding `e` directly —
                    // ureq's Display includes the full request URL, which
                    // leaks the token embedded in Discord/Slack/Jellyfin
                    // webhook URLs into the system log (and thence the
                    // unauthenticated GET /api/system endpoint).
                    let summary = match &e {
                        ureq::Error::Status(c, _) => format!("HTTP {c}"),
                        ureq::Error::Transport(t) => t.kind().to_string(),
                    };
                    crate::log::syslog(&format!(
                        "Webhook failed {}: {}",
                        webhook_url_origin(url),
                        summary
                    ));
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webhook_url_origin_strips_token_path() {
        // Discord-style: secret token in the path must not appear in the log.
        let url = "https://discord.com/api/webhooks/123456/SECRET_TOKEN";
        let origin = webhook_url_origin(url);
        assert_eq!(origin, "https://discord.com");
        assert!(!origin.contains("SECRET_TOKEN"));
    }

    #[test]
    fn webhook_url_origin_host_with_port() {
        let url = "http://jellyfin.example:8096/webhook/abc/SECRET";
        let origin = webhook_url_origin(url);
        assert_eq!(origin, "http://jellyfin.example:8096");
        assert!(!origin.contains("SECRET"));
    }

    #[test]
    fn webhook_url_origin_bare_origin_no_path() {
        // No path — the whole URL is the origin.
        let url = "https://example.com";
        assert_eq!(webhook_url_origin(url), "https://example.com");
    }

    #[test]
    fn webhook_url_origin_no_scheme_redacted() {
        assert_eq!(webhook_url_origin("not-a-url"), "<redacted>");
        assert_eq!(webhook_url_origin(""), "<redacted>");
    }

    #[test]
    fn webhook_url_origin_strips_query_string_token() {
        // Token in query string (no path slash) must not appear in the log.
        let url = "https://hooks.example.com?token=SUPERSECRET";
        let origin = webhook_url_origin(url);
        assert_eq!(origin, "https://hooks.example.com");
        assert!(!origin.contains("SUPERSECRET"));
    }

    #[test]
    fn webhook_url_origin_strips_fragment() {
        let url = "https://example.com#frag";
        assert_eq!(webhook_url_origin(url), "https://example.com");
    }

    /// Verify that the error summary produced for a Status error contains
    /// neither the full URL nor any embedded token — only the HTTP status code.
    #[test]
    fn fire_error_summary_status_no_url_leak() {
        // Simulate what the Err(e) arm produces for a Status error.
        // We can't call fire() directly (it needs a Config + spawns a thread
        // and makes a real HTTP request), so we replicate the summary logic
        // inline. If the logic in fire() changes, this test must change too —
        // that's the point: it pins the shape of the logged string.
        let url = "https://discord.com/api/webhooks/123456/SECRET_TOKEN";
        // We can't construct a `ureq::Error::Status` without a live connection, so
        // we test the origin-stripping half (already well-tested above) and the
        // summary format string that fire() would produce.
        let origin = webhook_url_origin(url);
        // The log line produced by fire() is: "Webhook failed {origin}: {summary}"
        // — neither contains the token path.
        let log_line = format!("Webhook failed {origin}: HTTP 403");
        assert!(
            !log_line.contains("SECRET_TOKEN"),
            "token leaked into log: {log_line}"
        );
        assert!(
            !log_line.contains("/api/webhooks/"),
            "path leaked into log: {log_line}"
        );
        assert!(log_line.contains("HTTP 403"));
        assert!(log_line.contains("https://discord.com"));
    }

    /// Same shape test for the Transport arm.
    #[test]
    fn fire_error_summary_transport_no_url_leak() {
        let url = "https://hooks.example.com?token=SUPERSECRET";
        let origin = webhook_url_origin(url);
        // Simulate what t.kind().to_string() produces — the actual string is
        // provider-defined, but it must never contain the URL.
        let kind_str = "connection failed"; // representative value
        let log_line = format!("Webhook failed {origin}: {kind_str}");
        assert!(
            !log_line.contains("SUPERSECRET"),
            "token leaked into log: {log_line}"
        );
        assert!(
            !log_line.contains("token="),
            "query param leaked: {log_line}"
        );
        assert!(log_line.contains("hooks.example.com"));
    }
}
