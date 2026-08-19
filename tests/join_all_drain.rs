//! Coverage for the shutdown drain, `ripper::join_all_rip_threads`.
//!
//! This lives in its own integration binary ON PURPOSE. The function is
//! process-global: it cancels EVERY registered device's `Halt` and joins EVERY
//! registered thread. Run alongside other tests in a shared binary it would
//! drain their fixtures out from under them, so it gets a process to itself.
//!
//! Before this file the function had no coverage at all — and it is the
//! function that decides whether a SIGTERM during a rip leaves a clean
//! resumable staging dir or a stale `.sweeping` that the startup classifier
//! reads as a crash (bumping `.restart_count` until a healthy rip is filed
//! `.failed`).

use std::time::{Duration, Instant};

use freemkv_autorip::ripper;

/// Catches two mutations:
///   * deleting the `halt.cancel()` loop that runs BEFORE the joins — without
///     it the workers never leave their phase loops, every join times out, and
///     the process dies without unwinding (no `Drop`, so `.sweeping` survives);
///   * replacing the single shared deadline with a per-device timeout, which
///     makes an N-drive shutdown block N×timeout instead of 1×.
#[test]
fn join_all_cancels_every_halt_and_shares_one_budget() {
    let devs: Vec<String> = (0..3)
        .map(|i| format!("join-all-drain-{}-{}", std::process::id(), i))
        .collect();
    let mut done = Vec::new();
    for d in &devs {
        let _ = ripper::take_rip_thread(d);
        let halt = libfreemkv::Halt::new();
        ripper::register_halt(d, halt.clone());
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        done.push(rx);
        ripper::spawn_rip_thread(d, "rip", move || {
            // A phase loop: polls its Halt exactly like sweep / patch / mux.
            while !halt.is_cancelled() {
                std::thread::sleep(Duration::from_millis(5));
            }
            let _ = tx.send(());
        })
        .expect("spawn must register");
    }

    let t0 = Instant::now();
    ripper::join_all_rip_threads(Duration::from_secs(5));
    let elapsed = t0.elapsed();

    for rx in done {
        rx.recv_timeout(Duration::from_secs(1))
            .expect("every worker must have been cancelled and drained");
    }
    assert!(
        elapsed < Duration::from_secs(5),
        "the drain took {elapsed:?}: the halts must be cancelled before the \
         joins begin, and three devices must share ONE budget"
    );
    for d in &devs {
        assert!(
            ripper::take_rip_thread(d).is_none(),
            "a drained thread must have been joined and unregistered"
        );
        ripper::unregister_halt(d);
    }
}
