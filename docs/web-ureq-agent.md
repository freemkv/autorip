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

## `IdleReCapConnector` (ureq 3.4.1+ rolling-idle restoration)

ureq 3 has **no** config knob for a rolling idle bound. Early ureq 3
(3.4.0) *appeared* to give one: `timeout_recv_body` was implemented by
anchoring the active phase's deadline to `now` on every check, so it silently
re-armed per read — exactly the rolling behaviour `STALL_TIMEOUT` wants, so
autorip was built on it. ureq 3.4.1's "Fix timeout budgets restarting and
applying to later phases" (#1194) corrected that bug: `timeout_recv_body` is
now an ABSOLUTE deadline anchored at header completion — a TOTAL body budget
that never re-arms — and (same fix) `timeout_recv_response` no longer caps
the body at all. Under 3.4.2, then, a 20s `STALL_TIMEOUT` set as
`timeout_recv_body` becomes a hard 20s ceiling on the WHOLE keydb download:
a slow-but-progressing multi-MB body over a slow link is aborted mid-transfer.
That is a real production regression, not just a test artifact.

`IdleReCapConnector` reintroduces the rolling bound at the transport layer.
Chained after `DefaultConnector` (which opens the TCP/TLS socket), it wraps
the transport so every BODY `await_input` (the ones ureq tags
`Timeout::RecvBody`) is capped to `idle`. ureq issues a fresh `await_input`
per read and a read that returns bytes ends the wait, so capping each call to
`idle` makes the bound roll: a byte resets the clock, a genuine `idle`-long
stall trips it with `Error::Timeout(RecvBody)`. Connect and header phases are
left on ureq's own timeouts (the cap is keyed on the `RecvBody` reason).

With this in place `guarded_agent_with_timeouts` sets `timeout_recv_body =
response` (the total-transfer ceiling) and layers `idle` on top as the rolling
stall detector — restoring both properties the migration/3.4.1 took away.

## `guarded_agent_with_timeouts` (full detail)

Builds a DNS-pinned, redirect-blocking ureq agent with caller-chosen
timeouts. This is the ONE place the pinned agent is constructed, so no call
site can quietly drop the resolver.

ureq sets NO default connect/read timeout. Without one a peer that accepts
the connection but never responds would block the caller's thread (and hold
its socket) forever, so every caller must pass bounds. The key-service
reachability probe wants to give up much sooner than a keydb download; the
caller picks.

`response` is the ceiling on the WHOLE transfer, not just header arrival.
It is passed to BOTH `timeout_recv_response` (header wait) AND
`timeout_recv_body` (the total body budget). The double wiring is deliberate:
before ureq 3.4.1, `timeout_recv_response` alone capped the body too (the body
read checked its preceding deadline), but #1194 stopped that — `recv_response`
now bounds headers only, so the total-transfer ceiling has to be set as
`timeout_recv_body` explicitly. Size `response` for the largest body this
caller should ever accept, not for how long a header may take.

`idle` is the rolling stall detector (`STALL_TIMEOUT`), applied by
`IdleReCapConnector` (see above) — the one that catches a dead peer quickly
regardless of how generous `response` is. ureq 3 no longer exposes it as a
config knob, so it lives in the transport wrapper, not the `Config`.

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
