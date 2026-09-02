# `ripper::join_all_rip_threads` shutdown drain

This test lives in its own integration binary on purpose. The function is
process-global: it cancels every registered device's `Halt` and joins every
registered thread. Run alongside other tests in a shared binary it would
drain their fixtures out from under them, so it gets a process to itself.

Before this file the function had no coverage at all — and it is the
function that decides whether a SIGTERM during a rip leaves a clean
resumable staging dir or a stale `.sweeping` that the startup classifier
reads as a crash (bumping `.restart_count` until a healthy rip is filed
`.failed`).

## What the single test here catches

* deleting the `halt.cancel()` loop that runs before the joins — without
  it the workers never leave their phase loops, every join times out, and
  the process dies without unwinding (no `Drop`, so `.sweeping` survives);
* replacing the single shared deadline with a per-device timeout, which
  makes an N-drive shutdown block N×timeout instead of 1×.
