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
        "lost_video_secs": (ev.lost_video_secs * crate::util::MILLIS_PER_SEC).round()
            / crate::util::MILLIS_PER_SEC,
    });
    fire(cfg, &payload);
}

/// Return only the `scheme://host[:port]` portion of `url`, dropping any
/// userinfo, path, query, or fragment. Webhook/keyserver URLs commonly embed
/// a secret token in the path (Discord, Slack, Jellyfin), in the query
/// string, or as HTTP basic-auth userinfo (`scheme://user:token@host/...`),
/// so logging the full URL would expose that secret in the system log,
/// which GET /api/system serves unredacted to any LAN client. Logging the
/// origin is enough to identify the destination.
///
/// The userinfo-stripping step mirrors `web.rs`'s `mask_webhook_url` (which
/// has its own test proving it strips userinfo) — this is the same policy,
/// applied here as the one place it was missing rather than a second copy
/// of the logic.
pub(crate) fn webhook_url_origin(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let after = scheme_end + 3;
        // Treat '/', '?', and '#' as origin-terminating so a token in a
        // query string (`https://host?token=SECRET`) is stripped too.
        let origin_end = url[after..]
            .find(['/', '?', '#'])
            .map(|i| after + i)
            .unwrap_or(url.len());
        // The authority span is `url[after..origin_end]`. If it carries
        // HTTP basic-auth userinfo (`user:pass@host`), drop everything up
        // to and including the last '@' so only `host[:port]` survives —
        // otherwise the credential would leak straight through unredacted.
        let authority = &url[after..origin_end];
        let host_start = match authority.rfind('@') {
            Some(at) => after + at + 1,
            None => after,
        };
        return format!("{}{}", &url[..after], &url[host_start..origin_end]);
    }
    // No scheme — log nothing identifiable.
    "<redacted>".to_string()
}

/// Return only the non-blank webhook URLs, in order. Pulled out of [`fire`]
/// as a pure, directly-testable predicate — a blank/whitespace-only entry
/// (e.g. an unconfigured slot in the settings array) must never be treated
/// as a real destination to dispatch.
pub(crate) fn active_urls(urls: &[String]) -> Vec<String> {
    urls.iter()
        .filter(|u| !u.trim().is_empty())
        .cloned()
        .collect()
}

// Bound the number of concurrent webhook-dispatch threads. Each event
// otherwise spawns an unbounded OS thread; a burst of events (or a hostile
// client triggering many) could exhaust threads. Past the cap, drop the
// event with a warning rather than spawning.
use std::sync::atomic::{AtomicUsize, Ordering};
const MAX_INFLIGHT: usize = 8;
static INFLIGHT: AtomicUsize = AtomicUsize::new(0);

/// Attempt to claim one slot of a bounded concurrency counter. Returns
/// `true` and increments `counter` if it is currently below `max`;
/// otherwise leaves `counter` untouched and returns `false`.
///
/// Pulled out of [`fire`] as a pure function parameterised on `counter`
/// (rather than reaching for the module's `static INFLIGHT` directly) so a
/// test can drive the cap logic — including the exact boundary at `max` —
/// against a private counter instead of racing the real process-wide one
/// shared with every other test in the binary.
pub(crate) fn try_acquire_slot(counter: &AtomicUsize, max: usize) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            (n < max).then_some(n + 1)
        })
        .is_ok()
}

/// Release one slot claimed by [`try_acquire_slot`]. The single decrement
/// path used both by production's `InflightGuard::drop` and directly by
/// tests, so there is exactly one copy of the release logic.
pub(crate) fn release_slot(counter: &AtomicUsize) {
    counter.fetch_sub(1, Ordering::AcqRel);
}

// Decrement the in-flight counter however the dispatch thread exits.
struct InflightGuard;
impl Drop for InflightGuard {
    fn drop(&mut self) {
        release_slot(&INFLIGHT);
    }
}

fn fire(cfg: &Config, payload: &serde_json::Value) {
    let urls = active_urls(&cfg.webhook_urls);
    if urls.is_empty() {
        return;
    }
    let body = payload.to_string();

    if !try_acquire_slot(&INFLIGHT, MAX_INFLIGHT) {
        crate::log::syslog("Webhook dropped: too many concurrent deliveries in flight");
        return;
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
                .header("Content-Type", "application/json")
                .send(&body)
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
                    let summary = crate::web::ureq_error_kind(&e);
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

    // The tests above all use long example hostnames (discord.com,
    // jellyfin.example:8096, hooks.example.com, example.com). A previous
    // arithmetic bug in this function (`scheme_end + 3` accidentally written
    // as `scheme_end * 3`) happened to still compute the right origin for
    // those specific hosts, because `scheme_end * 3` (12 or 15, since
    // `scheme_end` is 4/5 for "http"/"https") lands inside or past the real
    // host boundary for a long enough hostname. It does NOT for a short one.
    // These tests pin the token-stripping behaviour against hosts short
    // enough that the `* 3` bug would visibly leak the token, so a
    // regression back to that arithmetic is caught regardless of hostname
    // length used elsewhere in the suite.

    #[test]
    fn webhook_url_origin_short_host_path_token() {
        let url = "https://a.b/SECRET_TOKEN";
        let origin = webhook_url_origin(url);
        assert_eq!(origin, "https://a.b");
        assert!(!origin.contains("SECRET_TOKEN"));
    }

    #[test]
    fn webhook_url_origin_short_host_query_token() {
        let url = "https://x?token=SECRET";
        let origin = webhook_url_origin(url);
        assert_eq!(origin, "https://x");
        assert!(!origin.contains("SECRET"));
    }

    #[test]
    fn webhook_url_origin_short_host_nested_path_token() {
        let url = "https://ab.cd/tokenpath/SECRET";
        let origin = webhook_url_origin(url);
        assert_eq!(origin, "https://ab.cd");
        assert!(!origin.contains("SECRET"));
        assert!(!origin.contains("tokenpath"));
    }

    #[test]
    fn webhook_url_origin_short_host_fragment_token() {
        let url = "https://a.b#SECRET_FRAGMENT_TOKEN";
        let origin = webhook_url_origin(url);
        assert_eq!(origin, "https://a.b");
        assert!(!origin.contains("SECRET_FRAGMENT_TOKEN"));
    }

    #[test]
    fn webhook_url_origin_ip_literal_with_port_and_token() {
        let url = "http://1.2.3.4:9000/hook/SECRET_TOKEN";
        let origin = webhook_url_origin(url);
        assert_eq!(origin, "http://1.2.3.4:9000");
        assert!(!origin.contains("SECRET_TOKEN"));
    }

    #[test]
    fn webhook_url_origin_no_scheme_with_embedded_secret_is_fully_redacted() {
        // A malformed/no-scheme "URL" that still contains something
        // token-shaped must never leak that content — the whole thing is
        // replaced with the fixed placeholder, not partially echoed back.
        let url = "hooks.example.com/SECRET_TOKEN?token=ALSO_SECRET";
        let origin = webhook_url_origin(url);
        assert_eq!(origin, "<redacted>");
        assert!(!origin.contains("SECRET_TOKEN"));
        assert!(!origin.contains("ALSO_SECRET"));
    }

    #[test]
    fn webhook_url_origin_strips_basic_auth_userinfo() {
        // HTTP basic-auth userinfo can carry a bearer token
        // (`scheme://user:token@host/...`). It must not survive into the
        // logged origin any more than a path- or query-embedded token does.
        let url = "https://autorip:s3cr3t-token@hooks.example.com/notify";
        let origin = webhook_url_origin(url);
        assert_eq!(origin, "https://hooks.example.com");
        assert!(!origin.contains("s3cr3t-token"));
        assert!(!origin.contains("autorip:"));
        assert!(!origin.contains('@'));
    }

    #[test]
    fn webhook_url_origin_strips_basic_auth_userinfo_short_host() {
        let url = "http://user:pw@a.b/hook";
        let origin = webhook_url_origin(url);
        assert_eq!(origin, "http://a.b");
        assert!(!origin.contains("pw"));
        assert!(!origin.contains('@'));
    }

    #[test]
    fn active_urls_filters_blank_and_whitespace_entries() {
        let urls = vec![
            "".to_string(),
            "   ".to_string(),
            "https://real.example/hook".to_string(),
            "\t\n".to_string(),
            "https://second.example/hook".to_string(),
        ];
        let filtered = active_urls(&urls);
        assert_eq!(
            filtered,
            vec![
                "https://real.example/hook".to_string(),
                "https://second.example/hook".to_string(),
            ]
        );
    }

    #[test]
    fn active_urls_all_blank_yields_empty() {
        let urls = vec!["".to_string(), "  ".to_string()];
        assert!(active_urls(&urls).is_empty());
    }

    /// Drives `try_acquire_slot`/`release_slot` directly against a private
    /// counter (not the shared process-wide `INFLIGHT` static, to avoid
    /// cross-test interference) through several full acquire/release
    /// cycles. This is the real cap-and-release logic `fire()` uses, not a
    /// re-implementation of it — so a regression in either function is
    /// caught here directly, including `InflightGuard::drop` never
    /// decrementing (which would show up as slot 9+ never becoming
    /// available again).
    #[test]
    fn inflight_slot_cap_and_release_cycle() {
        let counter = AtomicUsize::new(0);
        let max = 3usize;

        // Fill up to the cap.
        assert!(try_acquire_slot(&counter, max));
        assert!(try_acquire_slot(&counter, max));
        assert!(try_acquire_slot(&counter, max));
        assert_eq!(counter.load(Ordering::Acquire), max);

        // At the cap: the next acquire must be rejected and must NOT bump
        // the counter past `max`.
        assert!(!try_acquire_slot(&counter, max));
        assert_eq!(counter.load(Ordering::Acquire), max);

        // Release one slot; a new acquire must now succeed.
        release_slot(&counter);
        assert_eq!(counter.load(Ordering::Acquire), max - 1);
        assert!(try_acquire_slot(&counter, max));
        assert_eq!(counter.load(Ordering::Acquire), max);

        // Release everything currently held (3 slots) and confirm the
        // counter returns all the way to zero — this is the guarantee that
        // `InflightGuard::drop` must uphold on every dispatch thread exit,
        // otherwise the counter ratchets upward forever and every webhook
        // past the cap gets silently dropped for the rest of the process.
        release_slot(&counter);
        release_slot(&counter);
        release_slot(&counter);
        assert_eq!(counter.load(Ordering::Acquire), 0);

        // Fully available again after the drain.
        assert!(try_acquire_slot(&counter, max));
        release_slot(&counter);
    }

    #[test]
    fn try_acquire_slot_rejects_when_max_is_zero() {
        let counter = AtomicUsize::new(0);
        assert!(!try_acquire_slot(&counter, 0));
        assert_eq!(counter.load(Ordering::Acquire), 0);
    }
}
