# Pinned ureq agent and error redaction (`src/web.rs`)

## `guarded_agent_with_timeouts`

Builds a ureq agent that (a) follows zero redirects — so a permitted public
URL can't 30x-redirect into an internal address — and (b) pins DNS
resolution to `pinned`, the exact addresses `validate_fetch_url` already
vetted. Pinning closes the DNS-rebinding TOCTOU: ureq connects to the
validated IPs instead of re-resolving the hostname (which an attacker could
flip to 169.254.169.254 / RFC1918 in the window between validation and
fetch).

## `MAX_PINNED_ADDRS` / `pinned_addrs`

ureq's `ResolvedSocketAddrs` is a fixed 16-slot array whose `push` writes
straight into it (`self.arr[self.len]`, no bounds check), so handing it a
17th address is an out-of-bounds panic inside a resolver that runs on every
request — on a host that merely publishes a lot of A records.
`validate_fetch_url` returns whatever DNS gave it, with no count limit of
its own, so `pinned_addrs` caps to the first 16 (all already vetted).

`pinned_addrs` is separated from the `Resolver` impl so the cap is
TESTABLE: ureq's `ResolvedSocketAddrs` and `NextTimeout` are built from
types not nameable outside the crate, so nothing in the test suite could
otherwise construct a resolve call — and every socket test pins exactly one
address, so deleting the cap would leave the whole suite green.

## `PinnedResolver`

The pinned-address resolver behind `guarded_agent_with_timeouts`. ureq 3
replaced v2's resolver closure with the `Resolver` trait, and the agent must
be built through `Agent::with_parts` to take one. `Agent::new_with_config`
compiles identically and then silently uses the DEFAULT resolver — which
would re-resolve the hostname over live DNS and reopen the exact rebinding
TOCTOU this agent exists to close, with no visible symptom. Pinned by the
test `guarded_agent_connects_to_the_pinned_address_not_dns`.

## `STALL_TIMEOUT`

No progress for this long on an open connection means the peer is dead,
whatever it promised in its headers. This is the knob ureq 2's
`timeout_read` used to provide and the 2→3 migration dropped: it is
ROLLING, re-armed on every read that returns bytes, so it kills a stalled
transfer without putting a ceiling on a slow-but-progressing one.

## `guarded_agent_with_timeouts` (full detail)

Builds a DNS-pinned, redirect-blocking ureq agent with caller-chosen
timeouts. This is the ONE place the pinned agent is constructed, so no call
site can quietly drop the resolver.

ureq sets NO default connect/read timeout. Without one a peer that accepts
the connection but never responds would block the caller's thread (and hold
its socket) forever, so every caller must pass bounds. The key-service
reachability probe wants to give up much sooner than a keydb download; the
caller picks.

`response` is NOT just a header timeout, despite ureq naming it
`timeout_recv_response`. In ureq 3 the body read also checks its preceding
timeout, and that deadline is absolute — `headers_complete + response` — so
`response` is the ceiling on the WHOLE transfer. Measured against a real
socket: with `response = 2s` a server that trickles one byte every 500 ms is
killed at 2.0 s, four bytes in. Size it for the largest body this caller
should ever accept, not for how long a header may take.

`idle` is the rolling stall detector (`STALL_TIMEOUT`) — the one that
catches a dead peer quickly regardless of how generous `response` is.

## `ureq_error_kind`

A short, URL-FREE description of a ureq failure. ureq's own `Display`
embeds the full request URL, and these summaries reach syslog,
`autorip.jsonl`, and the unauthenticated `/api/system` + `/api/debug`
endpoints. The URLs involved carry secrets: a TMDB api_key in the query
string, a Discord/Slack/Jellyfin token in the webhook path, a token-bearing
keydb_url. So the error is never formatted — each variant maps to a fixed
label instead.

ureq 3 split v2's single `Transport(t)` (which had `.kind()`) across many
variants, and the enum is `non_exhaustive`, so the catch-all is both
required and the safe default: an unrecognised variant degrades to a bare
label rather than to something that might interpolate a URL.
