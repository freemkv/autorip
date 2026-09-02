# webhook.rs design notes

## `webhook_url_origin`

Webhook/keyserver URLs commonly embed a secret token in the path (Discord,
Slack, Jellyfin), in the query string, or as HTTP basic-auth userinfo
(`scheme://user:token@host/...`), so logging the full URL would expose that
secret in the system log, which `GET /api/system` serves unredacted to any
LAN client. Logging the origin is enough to identify the destination.

The userinfo-stripping step mirrors `web.rs`'s `mask_webhook_url` (which has
its own test proving it strips userinfo) — this is the same policy, applied
here as the one place it was missing rather than a second copy of the logic.

## `active_urls`

Pulled out of `fire` as a pure, directly-testable predicate — a
blank/whitespace-only entry (e.g. an unconfigured slot in the settings
array) must never be treated as a real destination to dispatch, and an
entry whose `post_rip`/`post_move` flag is false for this event is
deliberately skipped so a "move only" hook never receives a rip payload
(and vice versa).

## `try_acquire_slot`

Pulled out of `fire` as a pure function parameterised on `counter` (rather
than reaching for the module's `static INFLIGHT` directly) so a test can
drive the cap logic — including the exact boundary at `max` — against a
private counter instead of racing the real process-wide one shared with
every other test in the binary.

## `deliver`

Split out of `fire` so it can be tested against a loopback stub — until
this seam existed, the only HTTP call in this module (the one carrying the
user's rip-complete event) had no test at all. A mistake in it — a header
that stopped being sent, a body that never reached the wire — would
compile, pass every test, and be discovered by an operator whose Discord
webhook silently stopped arriving.

Uses `web::webhook_agent` (default resolver, redirects blocked, no SSRF
pinning): a webhook is a blind notification POST and is intended to be able
to reach LAN hosts. Tests drive it against a loopback stub by pointing the
URL straight at the listener's `127.0.0.1:<port>` address, which the
default resolver connects to directly.

## Test: `send_rich_delivers_the_rip_complete_payload_end_to_end`

`send_rich` end-to-end through `fire`'s real spawn path to a loopback
stub — the whole rip-complete webhook, the one delivery this module exists
for, which no prior test drove (`fire` needs a `Config` and spawns a
thread, so it was only exercised piecemeal). Asserts the spawned delivery
actually reaches the wire and that `send_rich`'s rounding + field mapping
produce the JSON receivers see.

## Test: `a_redirect_is_not_reported_as_a_delivered_webhook`

`webhook_agent` sets `max_redirects(0)`, and at zero ureq's
`max_redirects_do_error` is false — so a 3xx comes back as `Ok`, and
`deliver` logged "Webhook sent". An http webhook URL whose receiver
redirects to https reported success forever while nothing was ever
delivered, with nothing in the log to say otherwise.

## Test: `deliver_posts_json_with_the_content_type_header`

Everything else in this module is helper-level (URL masking, the in-flight
counter); the request that actually carries the user's event was never
exercised. It was migrated from ureq 2's `.set()`/`.send_string()` to
ureq 3's `.header()`/`.send()` without a test, and a mistake there — wrong
header, a body that never reached the wire — compiles and passes
everything.

Driven against a loopback listener by pointing the webhook URL straight at
its `127.0.0.1:<port>` address, which the default resolver connects to
directly (webhook delivery is intentionally un-pinned). Asserts the
request line, the content type and the body, and nothing else — header
order and `User-Agent` are ureq's business and would make this brittle
across a version bump.

## Test: `inflight_slot_cap_and_release_cycle`

Drives `try_acquire_slot`/`release_slot` directly against a private counter
(not the shared process-wide `INFLIGHT` static, to avoid cross-test
interference) through several full acquire/release cycles. This is the
real cap-and-release logic `fire()` uses, not a re-implementation of it —
so a regression in either function is caught here directly, including
`InflightGuard::drop` never decrementing (which would show up as slot 9+
never becoming available again).
