# Connection admission caps (`src/web.rs`)

`run()` spawns one OS thread per connection, and `/events` holds its thread
until disconnect. With no cap a LAN client can pin N threads and exhaust the
container; handlers, body-carrying requests, and SSE streams are each bounded
separately.

## `MAX_INFLIGHT_HANDLERS`

Max concurrent request-handler threads. Generous — normal use is a handful
of browser tabs polling — but finite so a flood can't fork the box to death.

## `MAX_INFLIGHT_BODY_HANDLERS`

The cap for a request that carries a BODY, which the handler thread must
read off the socket while holding its admission token.

Lower than `MAX_INFLIGHT_HANDLERS` on purpose, and the gap is the whole
point: those remaining slots can only ever be taken by a bodyless request,
so no number of stalled POSTs can keep `GET /api/state` — the container
healthcheck — from being answered. Without the gap, 64 half-sent POSTs held
every slot until their sockets died, the healthcheck 503'd three times, and
the daemon was restarted, possibly mid-rip.

This bounds the DAMAGE, not the stall: the honest fix is a socket read
timeout, and tiny_http 0.12 neither sets one nor exposes the stream to set
it on. That needs a server change, which is not a thing to do quietly.

The gap between the two caps is what the healthcheck survives on, so it is
checked at COMPILE time (`const _: () = assert!(...)`) rather than in a
test — equalising them cannot even build. (A test asserting it would be an
assertion over two constants, which clippy rejects for exactly this reason:
the compiler is the right place.)

## `carries_body`

Whether a request will make its handler read a body off the socket. Read
from the headers tiny_http has ALREADY parsed by the time the request is
yielded, so this costs nothing and cannot itself block. A chunked or
unknown-length body counts too: the length is what the reader waits for.

## `MAX_SSE_CLIENTS`

Max concurrent SSE (`/events`) streams. Each pins a thread for its whole
lifetime, so this is the tighter bound.

## `ConnGuard`

RAII admission token for a counted connection slot. Decrements its counter
on drop, so the slot is freed no matter how the handler exits (return,
panic-unwind). `try_acquire` returns `None` when the cap is already reached.
