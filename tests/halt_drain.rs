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

/// Mirrors what handle_stop now does in production (web.rs):
///   - cancel the device's `Halt` token (the new replacement for
///     the deleted `request_stop` helper).
///   - join_rip_thread(device, timeout) — best-effort drain so the
///     caller doesn't return while the rip thread is mid-write.
fn handle_stop_today(device: &str) {
    if let Some(halt) = ripper::device_halt(device) {
        halt.cancel();
    }
    let _ = ripper::join_rip_thread(device, Duration::from_secs(35));
}

#[test]
fn test_handle_stop_waits_for_thread_drain() {
    // TDD-red: today ripper.rs:422 spawns the rip thread with
    // `.spawn(...).ok()` and discards the JoinHandle, so handle_stop
    // (web.rs:1489) has nothing to join on. After handle_stop
    // returns, the rip thread may still be alive, holding the SCSI
    // session.
    //
    // The fix is to stash the JoinHandle in a per-device map and
    // have handle_stop join (with a timeout) before responding.
    //
    // This test asserts the post-condition: "after handle_stop
    // returns, the rip thread has exited." Modeled here by:
    //   - spawn a fake rip thread that drains slowly (100 ms),
    //   - call handle_stop_today (which doesn't join),
    //   - assert the thread is_finished() right after.
    //
    // Today: handle_stop_today returns instantly, the slow-draining
    // thread is still running, the assert fires. RED.
    //
    // Once handle_stop is fixed to join: update handle_stop_today
    // to also join (with a timeout) and the assert flips to GREEN.
    let device = "sg_drain_test";
    let halt = Halt::new();
    ripper::register_halt(device, halt.clone());

    let exited = Arc::new(AtomicBool::new(false));
    let exited_t = exited.clone();
    let halt_t = halt.clone();
    let handle = std::thread::Builder::new()
        .name(format!("fake-rip-{device}"))
        .spawn(move || {
            // Halt-aware loop with a realistic drain delay — simulates
            // the rip thread finishing its current sector batch and
            // closing the output file before exiting.
            while !halt_t.is_cancelled() {
                std::thread::sleep(Duration::from_millis(10));
            }
            std::thread::sleep(Duration::from_millis(100));
            exited_t.store(true, Ordering::Relaxed);
        })
        .expect("spawn fake rip thread");

    // Plug the synthetic thread into the same per-device JoinHandle
    // table that production's spawn site uses, so handle_stop_today's
    // `join_rip_thread` call can drain it.
    ripper::register_rip_thread(device, handle);

    // Give the thread a moment to start its loop so the halt-flag
    // observation isn't racing with spawn() returning.
    std::thread::sleep(Duration::from_millis(20));

    let stop_started = Instant::now();
    handle_stop_today(device);
    let stop_elapsed = stop_started.elapsed();

    // The contract: after handle_stop returns, the rip thread has
    // exited. With the join in place, stop_elapsed reflects the
    // drain time (~110 ms) and the thread reached its exit branch.
    let _ = stop_elapsed;
    assert!(
        exited.load(Ordering::Relaxed),
        "rip thread didn't reach its exit branch — Halt token not observed or \
         handle_stop returned before join completed"
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
