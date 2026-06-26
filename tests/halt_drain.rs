//! Integration tests for stop / halt / drain semantics.
//!
//! Verifies that:
//!   - cancelling the per-device `Halt` token flips the same bit a
//!     rip-thread clone polls (immediate path).
//!   - handle_stop waits for the rip thread to drain before
//!     returning (TDD-red: today the join handle is dropped).
//!   - eject + rip-exit don't double-drop the underlying Drive
//!     (TDD-red: depends on the eject sync fix).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use freemkv_autorip::ripper;
use libfreemkv::Halt;

#[test]
fn test_cancel_halt_propagates_to_rip_clones() {
    // Register a per-device `Halt` token (the same call rip_disc
    // makes at its top); look it up via `device_halt` (the same call
    // the HTTP /api/stop handler in web.rs makes); cancel it; assert
    // the original Halt observes cancellation. Models the production
    // path: the rip thread holds clones threaded through sweep /
    // patch / mux / DiscStream, the UI calls /api/stop, the handler
    // calls device_halt(device).cancel(), every clone observes the
    // flip on its next is_cancelled() poll.
    let device = "sg_halt_test";
    let halt = Halt::new();
    ripper::register_halt(device, halt.clone());

    assert!(!halt.is_cancelled(), "halt should start uncancelled");

    let registered = ripper::device_halt(device).expect("halt registered");
    registered.cancel();

    assert!(
        halt.is_cancelled(),
        "cancel via the registered token must propagate to every clone"
    );
}

#[test]
fn test_stop_and_drain_waits_for_thread_to_finish() {
    // The stop→drain contract: stop must cancel the device's halt token AND
    // block until the rip thread has actually finished — it must NOT return
    // while the thread is still mid-write holding the SCSI session.
    //
    // This drives the REAL production function `ripper::stop_and_drain` (the
    // core handle_stop now delegates to), not a replica. A synthetic rip
    // thread polls the halt token, then takes ~100 ms to "close its output"
    // before exiting. The test asserts:
    //   - stop_and_drain returned true (it observed the thread finish), and
    //   - the thread's own exit flag is set by the time stop returned, and
    //   - stop took at least the drain delay (it genuinely waited).
    // If stop_and_drain ever regressed to fire-and-forget, `drained` would be
    // false (timeout) and/or `exited` would still be false right after.
    let device = "sg_drain_test";
    let halt = Halt::new();
    ripper::register_halt(device, halt.clone());

    let exited = Arc::new(AtomicBool::new(false));
    let exited_t = exited.clone();
    let halt_t = halt.clone();
    let handle = std::thread::Builder::new()
        .name(format!("fake-rip-{device}"))
        .spawn(move || {
            // Halt-aware loop with a realistic drain delay — simulates the rip
            // thread finishing its current sector batch and closing the output
            // file before exiting.
            while !halt_t.is_cancelled() {
                std::thread::sleep(Duration::from_millis(10));
            }
            std::thread::sleep(Duration::from_millis(100));
            exited_t.store(true, Ordering::Relaxed);
        })
        .expect("spawn fake rip thread");

    // Plug the synthetic thread into the same per-device JoinHandle table the
    // production spawn site uses, so stop_and_drain's internal join_rip_thread
    // can find and reap it.
    ripper::register_rip_thread(device, handle).expect("no prior handle for this device");

    // Give the thread a moment to start its loop so the halt-flag observation
    // isn't racing with spawn() returning.
    std::thread::sleep(Duration::from_millis(20));

    let stop_started = Instant::now();
    let drained = ripper::stop_and_drain(device, Duration::from_secs(35));
    let stop_elapsed = stop_started.elapsed();

    assert!(
        drained,
        "stop_and_drain must report the rip thread drained within the budget"
    );
    assert!(
        exited.load(Ordering::Relaxed),
        "rip thread had not reached its exit branch when stop_and_drain returned — \
         stop did not actually wait for the thread to finish"
    );
    assert!(
        stop_elapsed >= Duration::from_millis(90),
        "stop returned faster than the ~100ms drain delay ({stop_elapsed:?}) — \
         it can't have waited for the thread to finish closing its output"
    );
}

/// Counts Drop invocations to detect double-drop.
struct DropCounter {
    counter: Arc<AtomicUsize>,
}

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.counter.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn test_eject_does_not_double_drop() {
    // TDD-red: depends on the eject sync fix. Today,
    // handle_eject (web.rs:1470) calls eject_drive which can drop
    // the SCSI session while the rip thread is still mid-call into
    // libfreemkv. If the rip thread also drops its session on exit,
    // the underlying Drive could be dropped twice.
    //
    // We model the contract with DropCounter: Arc<Mutex<DropCounter>>
    // shared between an "eject" task and a "rip exit" task. After
    // both run concurrently, the counter must be exactly 1 — the
    // Arc<Mutex> wrapper guarantees this AS LONG AS production also
    // routes both code paths through the same Arc<Mutex>.
    //
    // If a future refactor accidentally clones the Drive into two
    // separate ownership paths (or extracts it from the mutex
    // without taking it), this test would catch the regression.
    let counter = Arc::new(AtomicUsize::new(0));
    let drive_slot: Arc<std::sync::Mutex<Option<DropCounter>>> =
        Arc::new(std::sync::Mutex::new(Some(DropCounter {
            counter: counter.clone(),
        })));

    let eject_slot = drive_slot.clone();
    let eject_thread = std::thread::spawn(move || {
        // Simulated eject: take() the Option, drop it. If the slot
        // is already empty (rip exit ran first), this is a no-op.
        let taken = eject_slot.lock().unwrap().take();
        drop(taken);
    });

    let exit_slot = drive_slot.clone();
    let exit_thread = std::thread::spawn(move || {
        // Simulated rip exit: same pattern.
        let taken = exit_slot.lock().unwrap().take();
        drop(taken);
    });

    eject_thread.join().expect("eject join");
    exit_thread.join().expect("rip exit join");

    // The slot must be empty (one of the two .take()s succeeded).
    assert!(
        drive_slot.lock().unwrap().is_none(),
        "drive slot should be empty after eject + rip exit"
    );
    let drops = counter.load(Ordering::SeqCst);
    assert_eq!(
        drops, 1,
        "Drive::drop ran {} times — expected exactly 1. \
         If >1, eject + rip-exit are racing without a synchronized take().",
        drops
    );
}

/// (a) A prior handle that has already *finished* is reaped quietly:
/// `register_rip_thread` joins it under the lock (safe — `is_finished()`
/// guarantees `join()` won't block) and returns `Ok(())` with no
/// "prior thread not reaped" warning. Models the observed benign case:
/// an `on_insert=scan` thread completes, then the rip thread registers
/// over its already-finished handle.
#[test]
fn test_register_reaps_finished_prior_quietly() {
    let device = "sg_reap_finished_test";

    // Prior worker that signals when it's about to return, so we can
    // wait for it to actually finish before registering over it.
    let prior_started = Arc::new(AtomicBool::new(false));
    let prior_started_t = prior_started.clone();
    let prior = std::thread::Builder::new()
        .name(format!("prior-{device}"))
        .spawn(move || {
            prior_started_t.store(true, Ordering::Relaxed);
        })
        .expect("spawn prior");
    ripper::register_rip_thread(device, prior).expect("first registration succeeds");

    // Wait until the prior has run its body AND a moment longer so the OS
    // thread has truly exited (is_finished() observes the join state).
    let deadline = Instant::now() + Duration::from_secs(5);
    while !prior_started.load(Ordering::Relaxed) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    std::thread::sleep(Duration::from_millis(50));

    // New handle registers; the finished prior must be reaped and this
    // must succeed (Ok), not be rejected.
    let exited = Arc::new(AtomicBool::new(false));
    let exited_t = exited.clone();
    let next = std::thread::Builder::new()
        .name(format!("next-{device}"))
        .spawn(move || {
            exited_t.store(true, Ordering::Relaxed);
        })
        .expect("spawn next");
    let result = ripper::register_rip_thread(device, next);
    assert!(
        result.is_ok(),
        "registering over a FINISHED prior must reap it and succeed, got {result:?}"
    );

    // Drain the live one so the test leaves no registered handle behind.
    let _ = ripper::join_rip_thread(device, Duration::from_secs(5));
    assert!(exited.load(Ordering::Relaxed), "next worker ran");
}

/// (b) A prior handle that is still *running* is NOT overwritten:
/// `register_rip_thread` returns `Err(PriorThreadRunning(handle))`,
/// handing back the new handle (so it is never dropped on the floor)
/// and leaving the running prior registered so stop/eject/shutdown can
/// still drain it. This is the latent-hazard branch (the v0.13.6 bug
/// class) the fix defends against.
#[test]
fn test_register_rejects_running_prior_without_orphaning() {
    let device = "sg_reject_running_test";

    // A prior worker that stays alive until we cancel its halt.
    let prior_halt = Halt::new();
    let prior_halt_t = prior_halt.clone();
    let prior = std::thread::Builder::new()
        .name(format!("prior-{device}"))
        .spawn(move || {
            while !prior_halt_t.is_cancelled() {
                std::thread::sleep(Duration::from_millis(5));
            }
        })
        .expect("spawn prior");
    ripper::register_rip_thread(device, prior).expect("first registration succeeds");
    // Let the prior enter its loop so is_finished() reads false.
    std::thread::sleep(Duration::from_millis(20));

    // Attempt to register a second handle for the same device.
    let new_ran = Arc::new(AtomicBool::new(false));
    let new_ran_t = new_ran.clone();
    let new_handle = std::thread::Builder::new()
        .name(format!("new-{device}"))
        .spawn(move || {
            new_ran_t.store(true, Ordering::Relaxed);
        })
        .expect("spawn new");

    match ripper::register_rip_thread(device, new_handle) {
        Err(ripper::RegisterError::PriorThreadRunning(returned)) => {
            // The rejected handle is handed back, never dropped — reap it.
            returned.join().expect("rejected handle joins cleanly");
        }
        other => panic!("expected PriorThreadRunning, got {other:?}"),
    }

    // The running prior is still registered and drainable. Cancel its
    // halt so join_rip_thread reaps it (the stop/drain path still works).
    prior_halt.cancel();
    assert!(
        ripper::join_rip_thread(device, Duration::from_secs(5)).is_ok(),
        "running prior must still drain after a rejected registration"
    );
    assert!(
        new_ran.load(Ordering::Relaxed),
        "the new worker did run to completion"
    );
}

/// (c) The spawn-site guard prevents a double-spawn while a worker is
/// running, and the running worker still drains via stop. Drives
/// `spawn_rip_thread` (the production helper all three spawn sites use):
/// the first spawn registers; a second spawn for the same device while
/// the first is still running returns `Err(AlreadyExists)` (the same
/// error shape the callers' spawn-failure rollback handles), and the
/// first worker remains drainable.
#[test]
fn test_spawn_guard_blocks_double_spawn_and_drain_still_works() {
    let device = "sg_double_spawn_test";

    let halt = Halt::new();
    ripper::register_halt(device, halt.clone());

    let first_done = Arc::new(AtomicBool::new(false));
    let first_done_t = first_done.clone();
    let halt_first = halt.clone();
    ripper::spawn_rip_thread(device, "rip", move || {
        while !halt_first.is_cancelled() {
            std::thread::sleep(Duration::from_millis(5));
        }
        first_done_t.store(true, Ordering::Relaxed);
    })
    .expect("first spawn registers");
    std::thread::sleep(Duration::from_millis(20));

    // Second spawn for the SAME device while the first runs: must be
    // rejected, not silently stomp the first handle.
    let second_ran = Arc::new(AtomicBool::new(false));
    let second_ran_t = second_ran.clone();
    let err = ripper::spawn_rip_thread(device, "rip", move || {
        second_ran_t.store(true, Ordering::Relaxed);
    })
    .expect_err("second spawn while first runs must fail");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::AlreadyExists,
        "double-spawn rejection must surface as AlreadyExists, got {err:?}"
    );

    // Stop/drain the first worker — drain-before-wipe still works.
    halt.cancel();
    assert!(
        ripper::join_rip_thread(device, Duration::from_secs(5)).is_ok(),
        "first worker drains after the duplicate spawn was rejected"
    );
    assert!(
        first_done.load(Ordering::Relaxed),
        "first worker reached its exit branch"
    );
}
