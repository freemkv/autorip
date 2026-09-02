# Watchdog test notes

Tests for the 0.20.8 hang-path fixes that touch autorip-side code.

Covers: the hard watchdog must not touch NFS before exit — the hand-rolled
bounded-syscall pattern around `increment_restart_count` returns within its
5s deadline even when the underlying call would never complete; on the
happy path the counter does increment.

The settings-save guard-drop coverage that used to be claimed here now
lives in `src/web.rs`, where `handle_settings_post` is actually reachable.

Hard-to-test caveat: simulating an actually-wedged NFS write requires a
real wedged mount or kernel-level hook. We approximate by (a) verifying
the timeout-path message is emitted when the worker is sleeping past the
deadline, and (b) verifying the happy-path returns inside the deadline.
The full "kernel won't release the syscall" path is the production
failure we're fixing but can't be deterministically reproduced in unit
tests.

`bounded_call` in this file is a verbatim re-implementation of the
hand-rolled bounded-syscall pattern used inside the mux watchdog
escalation branch. It's duplicated here because the production copy is
inlined inside a closure in `mux.rs`; testing the inline copy directly
would require driving the entire mux loop. Keep this in sync if
`bounded_syscall` ever becomes `pub` from libfreemkv.
