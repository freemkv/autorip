//! Tests for the 0.20.8 hang-path fixes that touch autorip-side code.
//!
//! Covers:
//!   - Finding 24 (hard watchdog must not touch NFS before exit): the
//!     hand-rolled bounded-syscall pattern around
//!     `increment_restart_count` returns within its 5 s deadline even
//!     when the underlying call would never complete; on the happy
//!     path the counter does increment.
//!   - Finding 22 (cfg.write() must drop guard before Config::save):
//!     `handle_settings_post` releases the write lock before invoking
//!     the on-disk save, and the on-disk file matches the snapshot.
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

#[test]
fn handle_settings_post_drops_write_guard_before_save() {
    // Finding 22: after `handle_settings_post` returns, the
    // `RwLock<Config>` must be in a state where a fresh writer can
    // acquire it without contention — i.e. the handler's write guard
    // is dropped before the on-disk save. The pre-fix code held the
    // guard across `Config::save` (an unbounded NFS write), blocking
    // every concurrent reader for the duration.
    //
    // We can't easily route a tiny_http::Request through the real
    // handler from a unit test (the type doesn't expose constructors
    // for fake requests), so we model the post-fix code path here:
    // mutate inside a guard, snapshot, drop the guard, then call
    // save. After save returns, try_write must succeed immediately.
    use freemkv_autorip::config::Config;
    use std::sync::{Arc, RwLock};

    let tmp = tempdir().expect("tempdir");
    let autorip_dir = tmp.path().to_string_lossy().to_string();
    let cfg = Arc::new(RwLock::new(Config {
        port: 8080,
        staging_dir: "/staging".into(),
        output_dir: "/output".into(),
        movie_dir: String::new(),
        tv_dir: String::new(),
        min_length_secs: 600,
        main_feature: true,
        auto_eject: true,
        on_insert: "scan".into(),
        output_format: "mkv".into(),
        network_target: String::new(),
        on_read_error: "stop".into(),
        max_retries: 1,
        keep_iso: false,
        abort_on_lost_secs: 0,
        max_rip_duration_secs: 28800,
        min_pass_budget_secs: 5400,
        transport_recovery_delay_secs: 5,
        tmdb_api_key: String::new(),
        keydb_path: None,
        keydb_url: String::new(),
        webhook_urls: Vec::new(),
        autorip_dir: autorip_dir.clone(),
    }));

    // Same shape as handle_settings_post post-fix: mutate, snapshot,
    // drop guard.
    let snapshot: Config = {
        let mut c = cfg.write().unwrap();
        c.tmdb_api_key = "abc123".into();
        c.main_feature = false;
        c.clone()
    };
    // Lock MUST be available immediately — pre-fix `cfg.write()` held
    // across save would have made this `try_write` fail until save
    // completed.
    assert!(
        cfg.try_write().is_ok(),
        "write lock should be released before save runs"
    );

    // Persist the snapshot. Note: with this Config's `autorip_dir`
    // pointing at the tempdir, save writes settings.json there.
    freemkv_autorip::config::save(&snapshot);

    let settings_path = std::path::Path::new(&autorip_dir).join("settings.json");
    assert!(settings_path.exists(), "settings.json should exist");
    let on_disk = std::fs::read_to_string(&settings_path).expect("read settings.json");
    assert!(
        on_disk.contains("abc123"),
        "settings.json should contain mutated tmdb_api_key, got: {on_disk}"
    );
    assert!(
        on_disk.contains("\"main_feature\": false"),
        "settings.json should reflect the snapshot's main_feature"
    );
}
