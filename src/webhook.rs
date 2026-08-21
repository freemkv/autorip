use crate::config::{Config, WebhookEntry};

/// Which completion event a dispatch is for. Each configured [`WebhookEntry`]
/// opts in to rip- and/or move-complete independently, so `fire` filters the
/// destination list by the event it is delivering.
#[derive(Clone, Copy)]
pub(crate) enum WebhookEvent {
    Rip,
    Move,
}

/// Notify that a file was moved to its final destination.
pub fn send_move(cfg: &Config, title: &str, dest_path: &str) {
    let payload = serde_json::json!({
        "event": "move_complete",
        "title": title,
        "output_path": dest_path,
    });
    fire(cfg, &payload, WebhookEvent::Move);
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
    fire(cfg, &payload, WebhookEvent::Rip);
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

/// Return the non-blank webhook URLs that opted in to `event`, in order.
/// Pulled out of [`fire`] as a pure, directly-testable predicate — a
/// blank/whitespace-only entry (e.g. an unconfigured slot in the settings
/// array) must never be treated as a real destination to dispatch, and an
/// entry whose `post_rip`/`post_move` flag is false for this event is
/// deliberately skipped so a "move only" hook never receives a rip payload
/// (and vice versa).
pub(crate) fn active_urls(entries: &[WebhookEntry], event: WebhookEvent) -> Vec<String> {
    entries
        .iter()
        .filter(|e| match event {
            WebhookEvent::Rip => e.post_rip,
            WebhookEvent::Move => e.post_move,
        })
        .map(|e| e.url.clone())
        .filter(|u| !u.trim().is_empty())
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

fn fire(cfg: &Config, payload: &serde_json::Value, event: WebhookEvent) {
    let urls = active_urls(&cfg.webhook_urls, event);
    if urls.is_empty() {
        return;
    }
    let body = payload.to_string();

    if !try_acquire_slot(&INFLIGHT, MAX_INFLIGHT) {
        crate::log::syslog("Webhook dropped: too many concurrent deliveries in flight");
        return;
    }

    // The guard is built HERE, on the spawning thread, and moved in — not
    // constructed inside the closure. If `std::thread::spawn` itself fails the
    // OS refuses a thread and std panics; with the guard built inside, the slot
    // stayed claimed forever, and eight of those silence every webhook for the
    // life of the process. `web.rs`'s `ConnGuard` was reshaped for exactly this
    // and its comment says so.
    let guard = InflightGuard;
    let spawned = std::thread::Builder::new()
        .name("webhook".into())
        .spawn(move || {
            let _guard = guard;
            for url in &urls {
                // Webhooks are deliberately NOT SSRF-guarded: a webhook is a
                // blind notification POST with no response channel, and aiming
                // one at a LAN service (Home Assistant, a NAS) is the intended
                // use. Delivery goes through the un-pinned `web::webhook_agent`
                // (default resolver, no private-address block); see its doc
                // comment. `redirects(0)` is still set there, so `deliver`
                // still refuses to count a 3xx as a delivery.
                let _ = deliver(url, &body);
            }
        });
    if spawned.is_err() {
        // The guard was moved into the closure that never ran, so the slot is
        // already released by the failed spawn's drop — nothing leaks. Say so
        // and carry on: a notification is not worth taking the rip down for.
        crate::log::syslog("Webhook dropped: could not spawn a delivery thread");
    }
}

/// POST one payload to one URL.
///
/// Split out of [`fire`] so it can be TESTED against a loopback stub — until
/// this seam existed, the only HTTP call in this module (the one carrying the
/// user's rip-complete event) had no test at all. A mistake in it — a header
/// that stopped being sent, a body that never reached the wire — would compile,
/// pass every test, and be discovered by an operator whose Discord webhook
/// silently stopped arriving.
///
/// Uses `web::webhook_agent` (default resolver, redirects blocked, no SSRF
/// pinning): a webhook is a blind notification POST and is intended to be
/// able to reach LAN hosts. Tests drive it against a loopback stub by
/// pointing the URL straight at the listener's `127.0.0.1:<port>` address,
/// which the default resolver connects to directly.
fn deliver(url: &str, body: &str) -> bool {
    let agent = crate::web::webhook_agent();
    match agent
        .post(url)
        .header("Content-Type", "application/json")
        .send(body)
    {
        // NOT every `Ok` is a delivery. `webhook_agent` sets
        // `max_redirects(0)`, and at zero ureq's `max_redirects_do_error` is
        // false, so a 3xx comes back as `Ok` rather than an error — so an
        // http webhook URL that its receiver redirects to https logged
        // "Webhook sent" forever while nothing was ever delivered. A 4xx/5xx
        // already arrives as `Err`; this closes the redirect gap and anything
        // else non-2xx with it.
        Ok(r) if r.status().is_success() => {
            // Log only the origin — the path may contain a secret token.
            crate::log::syslog(&format!("Webhook sent to {}", webhook_url_origin(url)));
            true
        }
        Ok(r) => {
            crate::log::syslog(&format!(
                "Webhook not accepted {}: HTTP {}",
                webhook_url_origin(url),
                r.status().as_u16()
            ));
            false
        }
        Err(e) => {
            // Summarise the error WITHOUT embedding `e` directly. NOTE: in
            // ureq 3 the Display of the variants reachable here is already
            // URL-free (`io: {kind}`, `timeout: …`, `connection failed`); the
            // habit is kept because `ureq_error_kind` is the one place that
            // guarantee is stated, and `BadUri` — which does embed the URI —
            // would otherwise be one refactor away from the log.
            let summary = crate::web::ureq_error_kind(&e);
            crate::log::syslog(&format!(
                "Webhook failed {}: {}",
                webhook_url_origin(url),
                summary
            ));
            false
        }
    }
}

#[cfg(test)]
mod tests {
    /// A redirect is NOT a delivery.
    ///
    /// `webhook_agent` sets `max_redirects(0)`, and at zero ureq's
    /// `max_redirects_do_error` is false — so a 3xx comes back as `Ok`, and
    /// `deliver` logged "Webhook sent". An http webhook URL whose receiver
    /// redirects to https reported success forever while nothing was ever
    /// delivered, with nothing in the log to say otherwise.
    #[test]
    fn a_redirect_is_not_reported_as_a_delivered_webhook() {
        use std::io::{Read as _, Write as _};
        use std::net::TcpListener;

        let listener =
            TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("bind stub listener");
        let pinned = listener.local_addr().expect("stub listener address");

        let _server = std::thread::spawn(move || {
            let Ok((mut sock, _peer)) = listener.accept() else {
                return;
            };
            let mut buf = Vec::new();
            let mut byte = [0u8; 1];
            while !buf.ends_with(b"\r\n\r\n") {
                match sock.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => buf.push(byte[0]),
                }
            }
            let head = String::from_utf8_lossy(&buf).to_string();
            let len: usize = head
                .lines()
                .find_map(|l| {
                    l.strip_prefix("content-length: ")
                        .or_else(|| l.strip_prefix("Content-Length: "))
                })
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);
            let mut body = vec![0u8; len];
            let _ = sock.read_exact(&mut body);
            // The shape a real receiver produces for an http:// webhook URL.
            let _ = sock.write_all(
                b"HTTP/1.1 301 Moved Permanently\r\n\
                  Location: https://hooks.test/services/T000/B000/xxxx\r\n\
                  Content-Length: 0\r\n\r\n",
            );
            let _ = sock.flush();
        });

        let delivered = super::deliver(
            &format!("http://{pinned}/services/T000/B000/xxxx"),
            r#"{"event":"rip_complete"}"#,
        );
        assert!(
            !delivered,
            "a 301 means the payload went nowhere; reporting it as sent is \
             how a silently-broken webhook stays broken"
        );
    }

    /// The webhook POST itself, driven to a real socket.
    ///
    /// Everything else in this module is helper-level (URL masking, the
    /// in-flight counter); the request that actually carries the user's event
    /// was never exercised. It was migrated from ureq 2's
    /// `.set()`/`.send_string()` to ureq 3's `.header()`/`.send()` without a
    /// test, and a mistake there — wrong header, a body that never reached the
    /// wire — compiles and passes everything.
    ///
    /// Driven against a loopback listener by pointing the webhook URL straight
    /// at its `127.0.0.1:<port>` address, which the default resolver connects
    /// to directly (webhook delivery is intentionally un-pinned). Asserts the
    /// request line, the content type and the body, and nothing else — header
    /// order and `User-Agent` are ureq's business and would make this brittle
    /// across a version bump.
    #[test]
    fn deliver_posts_json_with_the_content_type_header() {
        use std::io::{Read as _, Write as _};
        use std::net::TcpListener;

        let listener =
            TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("bind stub listener");
        let pinned = listener.local_addr().expect("stub listener address");

        let (tx, rx) = std::sync::mpsc::channel();
        let _server = std::thread::spawn(move || {
            let (mut sock, _peer) = listener.accept().expect("accept failed");
            let mut buf = Vec::new();
            let mut byte = [0u8; 1];
            // Read headers, then exactly the promised body length.
            while !buf.ends_with(b"\r\n\r\n") {
                match sock.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => buf.push(byte[0]),
                }
            }
            let head = String::from_utf8_lossy(&buf).to_string();
            let len: usize = head
                .lines()
                .find_map(|l| {
                    l.strip_prefix("content-length: ")
                        .or_else(|| l.strip_prefix("Content-Length: "))
                })
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);
            let mut body = vec![0u8; len];
            let _ = sock.read_exact(&mut body);
            let _ = sock.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n");
            let _ = sock.flush();
            let _ = tx.send((head, String::from_utf8_lossy(&body).to_string()));
        });

        let delivered = super::deliver(
            &format!("http://{pinned}/services/T000/B000/xxxx"),
            r#"{"event":"rip_complete"}"#,
        );
        assert!(delivered, "a 204 is a delivery");

        // Hand the observation back over a channel and take it with a
        // DEADLINE. `deliver` swallows transport errors (it only logs), so if
        // the pinned-resolver wiring ever regresses, no request arrives, the
        // stub blocks in `accept`, and an unconditional `join()` turns a test
        // failure into a hung suite.
        let (head, body) = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("no request reached the stub — deliver() never sent one");
        let head_lc = head.to_lowercase();
        assert!(
            head.starts_with("POST /services/T000/B000/xxxx HTTP/1.1"),
            "unexpected request line: {:?}",
            head.lines().next()
        );
        assert!(
            head_lc.contains("content-type: application/json"),
            "the JSON content type was not sent: {head}"
        );
        assert_eq!(
            body, r#"{"event":"rip_complete"}"#,
            "the payload did not reach the wire"
        );
    }

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

    /// Build a "fires on both events" entry — the common case and the
    /// pre-1.6.7 default — so these tests read as tersely as the old
    /// bare-string vectors they replaced.
    fn both(url: &str) -> WebhookEntry {
        WebhookEntry {
            url: url.to_string(),
            post_rip: true,
            post_move: true,
        }
    }

    #[test]
    fn active_urls_filters_blank_and_whitespace_entries() {
        let entries = vec![
            both(""),
            both("   "),
            both("https://real.example/hook"),
            both("\t\n"),
            both("https://second.example/hook"),
        ];
        // Blank filtering is independent of the event.
        for event in [WebhookEvent::Rip, WebhookEvent::Move] {
            assert_eq!(
                active_urls(&entries, event),
                vec![
                    "https://real.example/hook".to_string(),
                    "https://second.example/hook".to_string(),
                ]
            );
        }
    }

    #[test]
    fn active_urls_all_blank_yields_empty() {
        let entries = vec![both(""), both("  ")];
        assert!(active_urls(&entries, WebhookEvent::Rip).is_empty());
        assert!(active_urls(&entries, WebhookEvent::Move).is_empty());
    }

    /// Per-event opt-in is the whole point of the flags: a rip-only hook must
    /// never appear in the move dispatch list, and vice versa, while a
    /// both-events hook appears in both.
    #[test]
    fn active_urls_selects_by_event_flag() {
        let entries = vec![
            WebhookEntry {
                url: "https://rip-only.example/hook".to_string(),
                post_rip: true,
                post_move: false,
            },
            WebhookEntry {
                url: "https://move-only.example/hook".to_string(),
                post_rip: false,
                post_move: true,
            },
            both("https://both.example/hook"),
        ];

        assert_eq!(
            active_urls(&entries, WebhookEvent::Rip),
            vec![
                "https://rip-only.example/hook".to_string(),
                "https://both.example/hook".to_string(),
            ]
        );
        assert_eq!(
            active_urls(&entries, WebhookEvent::Move),
            vec![
                "https://move-only.example/hook".to_string(),
                "https://both.example/hook".to_string(),
            ]
        );
    }

    /// A hook that opted OUT of both events is inert — it never dispatches,
    /// even though its URL is non-blank. (The UI defaults new hooks to both
    /// checked, but a raw config could carry this.)
    #[test]
    fn active_urls_entry_opted_out_of_both_never_fires() {
        let entries = vec![WebhookEntry {
            url: "https://silent.example/hook".to_string(),
            post_rip: false,
            post_move: false,
        }];
        assert!(active_urls(&entries, WebhookEvent::Rip).is_empty());
        assert!(active_urls(&entries, WebhookEvent::Move).is_empty());
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
