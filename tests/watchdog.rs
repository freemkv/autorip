//! Tests for the 0.20.8 hang-path fixes that touch autorip-side code.
//!
//! Covers the hard watchdog not touching NFS before exit: the
//! bounded-syscall pattern around `increment_restart_count` returns
//! within its 5s deadline even when the underlying call would never
//! complete, and increments the counter on the happy path.
//!
//! See docs/watchdog-tests.md for the settings-save coverage note and
//! the hard-to-test-caveat rationale for how the wedged-NFS path is
//! approximated here.

use std::sync::mpsc::sync_channel;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use freemkv_autorip::ripper::staging;

// Verbatim mirror of the bounded-syscall pattern inlined in mux.rs's
// watchdog escalation closure. See docs/watchdog-tests.md.
fn bounded_call<F>(timeout: Duration, op: F) -> Result<(), ()>
where
    F: FnOnce() + Send + 'static,
{
    let (tx, rx) = sync_channel::<()>(0);
    let _ = std::thread::Builder::new()
        .name("test-bounded-call".into())
        .spawn(move || {
            op();
            let _ = tx.send(());
        });
    match rx.recv_timeout(timeout) {
        Ok(()) => Ok(()),
        Err(_) => Err(()),
    }
}

#[test]
fn watchdog_counter_bump_happy_path_increments() {
    // Sanity: on a healthy staging dir, the bounded counter bump returns
    // Ok within its deadline and the on-disk count increments — the
    // happy path the watchdog takes on healthy mounts.
    let tmp = tempdir().expect("tempdir");
    let staging_dir = tmp.path().to_path_buf();
    assert_eq!(staging::restart_count(&staging_dir), 0);

    let dir = staging_dir.clone();
    let started = Instant::now();
    let res = bounded_call(Duration::from_secs(5), move || {
        let _ = staging::increment_restart_count(&dir);
    });
    let elapsed = started.elapsed();

    assert!(res.is_ok(), "bounded counter bump should succeed");
    assert!(
        elapsed < Duration::from_secs(2),
        "happy-path bump took too long: {elapsed:?}"
    );
    assert_eq!(
        staging::restart_count(&staging_dir),
        1,
        "happy-path bump must increment the counter"
    );
}

#[test]
fn watchdog_counter_bump_times_out_when_op_hangs() {
    // Simulate a wedged increment_restart_count (sleep past the 5s
    // deadline); the bounded pattern must return Err near the deadline so
    // the watchdog can exit(1) instead of trapping in a wedged NFS syscall.
    let started = Instant::now();
    let res = bounded_call(Duration::from_millis(200), || {
        std::thread::sleep(Duration::from_secs(5));
    });
    let elapsed = started.elapsed();

    assert!(res.is_err(), "bounded call must time out on wedged op");
    // Returned at the deadline, not at op completion.
    assert!(
        elapsed < Duration::from_secs(2),
        "timeout returned far past deadline: {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(150),
        "timeout returned too early: {elapsed:?}"
    );
}

// The settings-save guard-drop test here was REMOVED, not moved: it never
// invoked `handle_settings_post` (private, unreachable) and only proved Rust
// drops a scoped guard. Real coverage now lives in `web::web_tests`.
