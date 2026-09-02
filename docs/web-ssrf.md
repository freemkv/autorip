# SSRF guard (`src/web.rs`)

Operator-supplied URLs autorip fetches (keydb_url, keyserver_url,
webhook_urls) are an SSRF vector; blocked at store and fetch time, pinned to
the validated IP to defeat DNS-rebinding TOCTOU.

## `validate_fetch_url`

Validates an operator-supplied fetch/POST URL against the SSRF guard.
Requires an `http`/`https` scheme, resolves the host **once**, and rejects
the URL if it has no addresses or any resolved address is in a blocked
class. On success returns the resolved+validated socket addresses so the
caller can pin the connection to them (avoiding a re-resolve race).
`Err(msg)` carries an operator-facing reason.

## `resolve_with_timeout`

Resolves `host:port` to socket addresses with a bounded deadline.
`ToSocketAddrs` performs a blocking DNS lookup, which can hang for the OS
resolver timeout (potentially tens of seconds) and freeze the calling
(unauthenticated) handler thread. It runs on a spawned thread and joins with
a short deadline, erroring on timeout. Shared by `validate_fetch_url` and
`validate_network_target` so neither can re-introduce an unbounded lookup.

## `RESOLVE_TIMEOUT_MSG` / `RESOLVE_FAILED_PREFIX` / `RESOLVE_NO_ADDRS_MSG`

The three failure strings `resolve_with_timeout` and `validate_fetch_url`
emit for "we could not find out", as opposed to "this URL is not allowed".
They are constants because `is_transient_resolve_error` classifies on them:
a caller that has to tell a DNS blip from a config error would otherwise be
matching literals typed twice, and the day one side is reworded the
classification silently inverts.

## `is_transient_resolve_error`

True when a `validate_fetch_url` / `resolve_with_timeout` error means the
host could not be LOOKED UP right now — a DNS timeout, a resolver failure,
or an empty answer — rather than a permanent verdict on the URL (bad scheme,
no host, blocked address).

The distinction matters wherever a failed validation is folded into a
judgement about the remote SERVICE: a resolver blip is not evidence that the
service answered. See `keysource::probe_online_reachability`, where getting
this wrong finalised a rippable disc as permanently keyless.
