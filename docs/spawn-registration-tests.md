# spawn_rip_thread registration regression tests

## v0.13.6 -> v0.13.7 stop-drain bug (module background)

v0.13.6 introduced `RIP_THREADS` so `handle_stop` could join the rip
thread before wiping staging, but the HTTP spawn sites (`handle_rip`,
`handle_scan`) used un-registered `std::thread::spawn(...)` and so
`take_rip_thread()` returned `None` — stop returned in 27 ms without
draining. v0.13.7 fixed the call sites; `spawn_rip_thread_registers_handle`
pins the contract on the new helper `ripper::spawn_rip_thread` so any
future call site that uses the helper is guaranteed to register,
making the bug a compile-time impossibility for callers that route
through it.

## Duplicate-spawn-never-runs regression (a_rejected_duplicate_spawn_never_runs_its_closure)

Catches the mutation that restores `spawn_rip_thread`'s old
spawn-then-check ordering (or removes the registration gate the
worker parks on).

The old order spawned the worker and only THEN asked
`register_rip_thread` whether it was allowed to exist. For the "rip"
role that closure is `handle_rip_request` — an entire multi-hour disc
rip — so a duplicate spawn ran the whole thing against the incumbent's
staging dir before the rejection was noticed and the `join()` reaped
it. The observable damage was the HTTP worker thread blocking for the
length of the rip, plus a `rollback_failed_spawn` + HTTP 500 for a rip
that had actually completed.

The assertion is the one that matters: the rejected duplicate must
not execute a single line of its closure.
