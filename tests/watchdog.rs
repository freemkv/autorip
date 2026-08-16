//! Tests for the 0.20.8 hang-path fixes that touch autorip-side code.
//!
//! Covers:
//!   - hard watchdog must not touch NFS before exit: the hand-rolled
//!     bounded-syscall pattern around `increment_restart_count` returns
//!     within its 5 s deadline even when the underlying call would never
//!     complete; on the happy path the counter does increment.
//!
//! The settings-save guard-drop coverage that used to be claimed here now
//! lives in `src/web.rs`, where `handle_settings_post` is actually reachable —
//! see the note further down.
//!
//! Hard-to-test caveat: simulating an actually-wedged NFS write
//! requires a real wedged mount or kernel-level hook. We approximate
//! by (a) verifying the timeout-path message is emitted when the
//! worker is sleeping past the deadline, and (b) verifying the
//! happy-path returns inside the deadline. The full "kernel won't
//! release the syscall" path is the production failure we're fixing
//! but can't be deterministically reproduced in unit tests.

use std::sync::mpsc::sync_channel;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use freemkv_autorip::ripper::staging;

/// Mirror of the hand-rolled bounded-syscall pattern used inside the
/// mux watchdog escalation branch. We re-implement it here verbatim
/// because the production copy is inlined inside a closure in
/// `mux.rs`; testing the inline copy directly would require driving
/// the entire mux loop. The shape is what matters — keep this in sync
/// if `bounded_syscall` ever becomes `pub` from libfreemkv.
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
    // Sanity: when the staging dir is healthy, the bounded counter
    // bump returns Ok within its deadline and the on-disk count
    // increments. This is the happy path the watchdog takes on
    // healthy mounts.
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
    // Simulate a wedged increment_restart_count by sleeping longer
    // than the 5 s deadline. The bounded pattern must return Err
    // within roughly the deadline so the watchdog can proceed to
    // `exit(1)` instead of trapping forever inside a kernel syscall
    // on a wedged NFS mount. Uses a short deadline (200 ms) and a
    // 5 s op so the test is fast.
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

// The settings-save guard-drop test that used to live here has been REMOVED,
// not moved: it never invoked `handle_settings_post`. It re-implemented the
// post-fix shape inline ("Same shape as handle_settings_post post-fix"),
// dropped its own guard at the end of its own block expression, and then
// asserted `try_write().is_ok()` — i.e. it asserted that Rust drops a scoped
// `RwLockWriteGuard`. Reintroducing the exact production bug (holding
// `cfg.write()` across `config::save`) left it green, so it was a guard in
// name only, and its presence here made the real gap look covered.
//
// `handle_settings_post` is private to `src/web.rs` and takes a
// `tiny_http::Request`, which has no public constructor — an integration test
// in this crate cannot reach it at all. The real coverage lives in-crate,
// where both are reachable:
//
//   * `web::web_tests::http::settings_post_persists_a_field_to_disk` — drives
//     the handler through a live loopback server and reads settings.json back.
//   * `web::web_tests::settings_post_saves_outside_the_config_write_guard` —
//     pins the drop-before-save ORDERING against the handler's own source.
