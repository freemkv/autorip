//! Regression test for the v0.13.6 -> v0.13.7 stop-drain bug.
//!
//! Background: v0.13.6 introduced `RIP_THREADS` so `handle_stop`
//! could join the rip thread before wiping staging, but the HTTP
//! spawn sites (handle_rip, handle_scan) used un-registered
//! `std::thread::spawn(...)` and so `take_rip_thread()` returned
//! None — stop returned in 27 ms without draining. v0.13.7 fixed
//! the call sites; this test pins the contract on the new helper
//! `ripper::spawn_rip_thread` so any future site that uses the
//! helper is guaranteed to register, making the bug a compile-time
//! impossibility for callers that route through it.
//!
//! What this test asserts:
//!   1. `spawn_rip_thread(dev, role, f)` returns Ok.
//!   2. Immediately after spawn (well before the worker exits), a
//!      `JoinHandle` is retrievable via `take_rip_thread(dev)`.
//!   3. Joining the handle does not hang — the worker exits cleanly.

use std::time::{Duration, Instant};

use freemkv_autorip::ripper;

#[test]
fn spawn_rip_thread_registers_handle() {
    let device = "test-sg9";

    // Defensive: clear any stale entry from a prior test run in the
    // same process (the static map is process-global).
    let _ = ripper::take_rip_thread(device);

    let spawn_started = Instant::now();
    ripper::spawn_rip_thread(device, "rip", || {
        std::thread::sleep(Duration::from_millis(100));
    })
    .expect("spawn_rip_thread should succeed");

    // The 100 ms worker is still sleeping; the handle must be in
    // RIP_THREADS right now. If take returns None here, registration
    // didn't happen — that's exactly the v0.13.6 bug we're guarding
    // against.
    let handle = ripper::take_rip_thread(device);
    assert!(
        spawn_started.elapsed() < Duration::from_millis(80),
        "test setup took too long; the worker may have already exited \
         and been reaped, invalidating the registration check"
    );
    assert!(
        handle.is_some(),
        "spawn_rip_thread must register the JoinHandle in RIP_THREADS \
         so handle_stop / join_rip_thread can drain it"
    );

    // Drain the worker so it doesn't outlive the test process.
    handle
        .unwrap()
        .join()
        .expect("worker thread should join cleanly");

    // Post-condition: a second take returns None — we already removed
    // the entry above.
    assert!(
        ripper::take_rip_thread(device).is_none(),
        "RIP_THREADS entry should be consumed by take_rip_thread"
    );
}
