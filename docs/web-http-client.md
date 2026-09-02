# Outbound HTTP client entry points (`src/web.rs`)

## `webhook_agent`

Agent for webhook delivery — a plain outbound POST with the standard
resolver, deliberately NOT SSRF-guarded.

Unlike `keydb_url` / `keyserver_url` / `network_target`, a webhook is a
blind fire-and-forget notification: autorip POSTs a rip/move event and never
reads the response body back to any caller, so there is no disc-key or
plaintext-exfiltration channel to protect — and the operator who sets a
webhook is on the same LAN as any host it targets. Aiming a webhook at a LAN
service (Home Assistant, a NAS, an internal automation endpoint) is the
*intended* use, which the pinned-resolver guard on the other URL classes
actively prevents. So this agent uses the DEFAULT resolver
(`new_with_config`) — no private-address block, no DNS pinning — while
keeping the two properties that are about robustness rather than SSRF:
bounded timeouts (a dead receiver must not wedge the per-delivery thread)
and `max_redirects(0)` (a 3xx is not a delivery — see `webhook::deliver`).

## `guarded_get`

SSRF-guarded HTTP GET. Runs `validate_fetch_url` (scheme + resolved-IP
allow-list) and then issues the request through `guarded_agent_with_timeouts`
so the connection is pinned to the validated addresses and redirects are
blocked.

This is the single entry point any code path that fetches an
operator-supplied URL from inside the container should use — the KEYDB
download on startup and the daily-refresh thread (main.rs) both route
through here instead of calling `ureq::get` directly, which would bypass the
guard entirely. Returns the response on success or an operator-facing reason
string on rejection / transport failure.

It is `pub` (not `pub(crate)`): the binary's `main.rs` declares its own `mod
web`, but the library facade in `lib.rs` re-exports this module too. In the
lib build nothing inside the crate calls this helper — only the bin and the
test module do — so `pub(crate)` would trip `dead_code`. Exposing it as the
crate's public SSRF-guarded fetch entry point is also the honest description
of its role.

## `KEYDB_FETCH_TIMEOUT`

End-to-end ceiling on the unauthenticated `/api` KEYDB update. Deliberately
tighter than `KEYDB_TRANSFER_BUDGET`: this path is reachable without
authentication, holds an in-flight handler slot and the process-wide update
flag that 429s every other attempt, so a hostile peer must not be able to
hold it for minutes.

## `KEYDB_TRANSFER_BUDGET`

How long a KEYDB body may take IN TOTAL, once headers are in.
`guarded_agent_with_timeouts`'s 30s default is right for a webhook POST and
far too short here: `read_capped_keydb_body` accepts up to `KEYDB_MAX_BYTES`,
and this budget is the ceiling on the whole transfer, so a keydb that takes
longer than half a minute to arrive would otherwise fail on a link that is
merely slow rather than broken — and the daily refresh thread would then
retry on the same too-short budget once every 24 hours.

A dead peer is still caught in `STALL_TIMEOUT` seconds, because the idle
timeout is rolling and independent of this ceiling. This number only buys
patience for a transfer that is actually progressing.

Sized to the REAL artifact, not to `KEYDB_MAX_BYTES`: that 100 MiB is a
defensive ceiling on what will be read, and a published keydb is a
single-digit-MB compressed export. Two minutes covers ~10 MB at well under 1
Mbit/s. Deriving the budget from the DoS cap instead would put a five-minute
stall in front of the operator at first boot — `main.rs` fetches the keydb
BEFORE the web server thread starts, so a slow keydb host would hold back
the very Settings page they would use to fix the URL.
