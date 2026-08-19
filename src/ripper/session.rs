//! Per-device drive sessions, halt/stop bookkeeping, and the registry
//! of in-flight rip threads.
//!
//! 0.18 round 2: the old `HALT_FLAGS` + `STOP_FLAGS` + `register_halt`
//! / `request_stop` / `stop_requested` / `reset_stop_flag` machinery
//! is gone. Each rip-thread spawn site now allocates a single
//! [`libfreemkv::Halt`] token, registers it in [`HALTS`] keyed by
//! device, and threads `halt.clone()` through every cancellable phase
//! (sweep / patch / mux). The HTTP `/api/stop/{device}` handler looks
//! up the device's `Halt` and calls `.cancel()`; phase loops poll
//! `halt.is_cancelled()` at their tops.

use libfreemkv::Halt;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::Duration;

/// Global table of rip-thread JoinHandles keyed by device. Populated
/// when the poll loop spawns the scan/rip thread; consumed by
/// `join_rip_thread` (called from `handle_stop`, `eject_drive`, and
/// the shutdown path).
static RIP_THREADS: once_cell::sync::Lazy<
    Mutex<std::collections::HashMap<String, JoinHandle<()>>>,
> = once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

/// Outcome of attempting to register a rip-thread `JoinHandle`.
///
/// `register_rip_thread` returns `Err(RegisterError)` instead of
/// silently overwriting (and thus orphaning) a prior handle that is
/// still running. The variant carries the rejected handle back to the
/// caller so it is never dropped on the floor — `spawn_rip_thread`
/// joins it so the just-spawned worker can't leak, and a test can
/// inspect it.
#[derive(Debug)]
pub enum RegisterError {
    /// A prior rip thread for this device is still *running*. We refuse
    /// to overwrite it (dropping a running handle breaks
    /// drain-before-wipe — the v0.13.6 bug class). The new handle the
    /// caller passed in is returned untouched so the caller can reap it.
    PriorThreadRunning(JoinHandle<()>),
}

/// Register a rip-thread JoinHandle for `device`. Production calls
/// this (via [`spawn_rip_thread`]) from the poll-loop and web spawn
/// sites; the integration tests under `tests/halt_drain.rs` also call
/// it to plug a synthetic thread into the same machinery `handle_stop`
/// uses.
///
/// Reap-or-reject semantics for a pre-existing entry (the device map
/// holds at most one handle):
///
/// * **Prior handle finished** → it is removed and joined *inside the
///   lock*. `JoinHandle::is_finished() == true` guarantees `join()`
///   returns without blocking, so holding `RIP_THREADS` across the
///   join cannot deadlock. This quietly reaps the common benign case
///   (a completed scan thread still registered when the rip thread
///   spawns) that previously logged a scary "prior thread not reaped"
///   warning. The new handle then takes its place and we return
///   `Ok(())`.
/// * **Prior handle still running** → we must NOT overwrite it; a
///   dropped running handle can never be joined, so a later
///   stop/eject/shutdown drain returns before the thread actually
///   exits (staging could be wiped mid-write — the v0.13.6 bug class).
///   We leave the running prior in place and return
///   `Err(RegisterError::PriorThreadRunning(new))`, handing the new
///   handle back so the caller reaps the worker it just spawned. The
///   spawn sites gate on [`try_claim_active`], which now consults thread
///   liveness as well as STATE status, so this branch should be
///   unreachable — but it is the last line of defense and is defended
///   rather than trusted. (It was previously justified by STATE status
///   ALONE, which is not a liveness fact: a worker writes its terminal
///   status and then keeps unwinding, so this branch was reachable from
///   an unauthenticated LAN POST.)
pub fn register_rip_thread(device: &str, handle: JoinHandle<()>) -> Result<(), RegisterError> {
    // Recover from poison rather than silently dropping the handle: a
    // dropped JoinHandle here can never be reaped, breaking
    // drain-before-wipe (the v0.13.6 bug class). Same recover-and-proceed
    // convention as update_state/is_busy/log.rs.
    let mut t = RIP_THREADS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(prior) = t.get(device) {
        if prior.is_finished() {
            // Safe to join under the lock: is_finished()==true means
            // join() won't block. Reap quietly — no warning.
            if let Some(prior) = t.remove(device)
                && let Err(e) = prior.join()
            {
                tracing::error!(
                    device = %device,
                    "reaped prior rip thread had panicked: {:?}", e
                );
            }
        } else {
            // A still-running prior would be orphaned by an overwrite.
            // Refuse and hand the new handle back to be reaped.
            tracing::warn!(
                device = %device,
                "register_rip_thread: prior rip thread still running — refusing to overwrite (the new worker will be drained)"
            );
            return Err(RegisterError::PriorThreadRunning(handle));
        }
    }
    t.insert(device.to_string(), handle);
    Ok(())
}

pub fn take_rip_thread(device: &str) -> Option<JoinHandle<()>> {
    // Recover from a poisoned lock (`into_inner`) rather than swallowing it with
    // `.ok()?`: a poisoned RIP_THREADS means a rip worker panicked, which is
    // exactly when `handle_stop` must still recover the JoinHandle to drain the
    // thread before wiping staging. Returning `None` here would lose the handle
    // and reintroduce the v0.13.6 stop-without-drain bug. Matches the
    // poison-recovering convention used everywhere else in this module.
    RIP_THREADS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(device)
}

/// Spawn a rip-related worker thread and register its `JoinHandle`
/// in `RIP_THREADS` atomically. Use this for every code path that
/// runs scan/rip work — `handle_stop` relies on the registration to
/// drain the thread before wiping staging. Bypassing this helper
/// (`std::thread::spawn` directly) reintroduces the v0.13.6 stop bug
/// where stop returned in 27 ms because no handle was registered.
///
/// `role` is a short tag (e.g. "rip", "scan") used for the OS thread
/// name; `device` is both the registration key and part of the name.
///
/// **Registration is decided BEFORE the worker does any work.** The spawned
/// thread parks on a one-shot channel and runs `f` only once this function has
/// confirmed it won the `RIP_THREADS` slot. The previous order — spawn, *then*
/// register — meant a duplicate spawn ran `f` to completion (for the rip role,
/// `handle_rip_request`: a full multi-hour disc rip) before
/// `register_rip_thread` rejected it and the `join()` below reaped it. That
/// turned a "refused" spawn into: an HTTP worker thread blocked for the whole
/// rip, a second worker writing the same staging dir as the incumbent, and
/// finally a `rollback_failed_spawn` + 500 for a rip that had actually
/// finished. Parking first makes the rejection cost a channel `recv` instead
/// of an hour of disk work.
///
/// The gate cannot hang either side. The worker's only pre-`f` action is the
/// `recv`, and every exit from this function either sends (accepted) or drops
/// the sender (rejected, or a panic between spawn and register) — a dropped
/// sender wakes the `recv` with `Err` and the worker returns without touching
/// anything. So the `join()` in the rejection branch is bounded by a thread
/// that is already on its way out.
pub fn spawn_rip_thread<F>(device: &str, role: &str, f: F) -> std::io::Result<()>
where
    F: FnOnce() + Send + 'static,
{
    let name = format!("{}-{}", role, device);
    // Enter a per-thread span carrying the build + device for the worker's whole
    // life. tracing spans are thread-local, so events from THIS crate AND from
    // libfreemkv (called synchronously on this thread — its read_error / wedge /
    // bus_key_unavailable warns) inherit these fields. That stamps the running
    // build onto every diagnostic line in autorip.log / autorip.jsonl — the lines
    // that previously couldn't be attributed to a build across a redeploy.
    let span_build = crate::VERSION_LABEL;
    let span_device = device.to_string();
    let span_role = role.to_string();
    // The registration gate (see this function's doc). `recv()` returns
    // `Ok(())` only if we won the slot; `Err` means the sender was dropped —
    // rejection, or an unexpected unwind between the spawn and the decision —
    // and the worker must abort before running a single line of `f`.
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let wrapped = move || {
        if go_rx.recv().is_err() {
            // Not registered: another worker owns this device. Returning here
            // is what keeps a duplicate `/api/rip` POST from executing a whole
            // second rip against the incumbent's staging dir.
            return;
        }
        let _span =
            tracing::info_span!("worker", build = span_build, device = %span_device, role = %span_role)
                .entered();
        f();
    };
    let handle = std::thread::Builder::new().name(name).spawn(wrapped)?;
    match register_rip_thread(device, handle) {
        Ok(()) => {
            // Release the worker. A send error is impossible in practice (the
            // receiver cannot be dropped before its `recv` returns) and would
            // only mean the worker is already gone, so it is not actionable.
            let _ = go_tx.send(());
            Ok(())
        }
        Err(RegisterError::PriorThreadRunning(new_handle)) => {
            // A prior worker for this device is still running and owns the
            // registration slot. We just spawned a duplicate: drop the gate's
            // sender so it aborts at its `recv` WITHOUT running `f` — it must
            // never touch the incumbent's device, staging dir or Halt token —
            // then join it so it can't leak, and surface failure so the caller
            // runs its existing spawn-failure rollback path.
            drop(go_tx);
            if let Err(e) = new_handle.join() {
                tracing::error!(
                    device = %device,
                    "duplicate-spawn worker panicked while being drained: {:?}", e
                );
            }
            Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "a rip thread is already running for this device",
            ))
        }
    }
}

/// Wait (up to `timeout`) for the rip thread for `device` to exit.
/// Returns `Ok(())` if the thread finished within the window or no
/// thread was registered. Returns `Err(())` on timeout.
///
/// Best-effort drain: callers should treat a timeout as a warning,
/// not a fatal error. The rip thread's `Halt` token was already
/// cancelled by the stop path before this is called; the thread will
/// exit eventually. The timeout just bounds how long the HTTP
/// response (or shutdown sequence) blocks.
///
/// # The handle is POLLED IN PLACE — it never leaves `RIP_THREADS`
///
/// This used to open with `take_rip_thread`, poll the handle it had removed,
/// and stash it back on timeout or self-join. That REMOVED THE DEVICE'S ONLY
/// LIVENESS FACT for the whole drain — up to 60 s for `/api/stop` — while the
/// worker was still running, and every gate that asks
/// [`rip_thread_running`] read `false` for that window:
///
/// * `POST /api/stop/sr0` lands on a worker in its terminal tail (it has
///   written `status = "done"` and is still inside auto-eject). `is_busy` is
///   already false. `handle_stop` takes the handle and starts polling.
/// * A concurrent `POST /api/rip/sr0` — unauthenticated, this server binds
///   `0.0.0.0` — now sees NEITHER fact: `is_busy` false, `rip_thread_running`
///   false. It wins `try_claim_active_checked`, `register_halt` clobbers the
///   incumbent's `Halt` (so the incumbent's trailing `unregister_halt` deletes
///   the NEW rip's token and `/api/stop` can no longer cancel it),
///   `register_rip_thread` finds the slot EMPTY because we took the handle,
///   and a full duplicate rip runs against the incumbent's staging dir.
///
/// Round 1's TOCTOU argument (see `try_claim_active_checked`) only covered a
/// handle APPEARING between the liveness read and the `STATE` lock. It never
/// covered one DISAPPEARING out from under a live worker, which is what the
/// drain itself was doing.
///
/// Polling in place also removes the re-stash entirely, and with it the
/// separate defect where the timeout path `insert`ed unconditionally and
/// could REPLACE a newer live handle with the stale one it was holding —
/// making the live rip unjoinable, so the next stop reported a clean drain
/// while that worker was still mid-write.
///
/// Implementation: every 25 ms, take `RIP_THREADS` briefly, look at the entry
/// for `device`, and drop the lock again. Nothing is ever held across the
/// sleep or across `join()`. `join()` runs only once `is_finished()` is true,
/// so it cannot block.
#[allow(clippy::result_unit_err)]
pub fn join_rip_thread(device: &str, timeout: Duration) -> Result<(), ()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        // One short lock per poll. `Observed` is computed under the lock and
        // the guard is dropped before we sleep, join, or return.
        enum Observed {
            /// No handle registered — nothing to drain.
            Absent,
            /// The registered handle is THIS thread (see the self-join note).
            SelfJoin,
            Finished,
            Running,
        }
        let observed = {
            // Recover from poison: a poisoned RIP_THREADS means a rip worker
            // panicked, which is exactly when the stop path must still drain
            // before staging is touched. Same convention as everywhere else in
            // this module.
            let t = RIP_THREADS.lock().unwrap_or_else(|e| e.into_inner());
            match t.get(device) {
                None => Observed::Absent,
                Some(h) if h.thread().id() == std::thread::current().id() => Observed::SelfJoin,
                Some(h) if h.is_finished() => Observed::Finished,
                Some(_) => Observed::Running,
            }
        };
        match observed {
            Observed::Absent => return Ok(()),
            // Self-join: we are *on* the registered rip thread (e.g.
            // `eject_drive` called from the rip's own auto-eject path at the
            // end of `rip_disc`). `is_finished()` can never become true while
            // we sit here, so polling would burn the whole timeout and log a
            // spurious "did not drain". The thread is by definition still
            // running (it's us) and will exit as soon as we return up the
            // stack. The handle stays registered — which is also what keeps
            // the device's liveness fact true for the rest of our tail.
            Observed::SelfJoin => return Ok(()),
            Observed::Finished => break,
            Observed::Running => {
                if std::time::Instant::now() >= deadline {
                    return Err(());
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }
    }
    // Finished: remove and reap. A concurrent joiner may have got there first,
    // in which case the entry is gone and there is nothing left to do.
    // `is_finished() == true` guarantees `join()` returns without blocking.
    if let Some(handle) = take_rip_thread(device)
        && let Err(e) = handle.join()
    {
        // join() returns Err(payload) if the thread panicked. The thread DID
        // finish (so we return Ok), but surface the panic so stop / eject /
        // shutdown don't treat a panicked rip as a clean exit.
        tracing::error!(device = %device, "rip thread panicked: {:?}", e);
    }
    Ok(())
}

/// Drain every known rip thread within a single shared `timeout`
/// budget (NOT per-device). The stop path cancels every device's
/// `Halt` token before this is called, so the threads are already
/// winding down in parallel — bounding each `join_rip_thread` by the
/// full `timeout` would let an N-drive shutdown block up to
/// N×`timeout`. Compute one deadline up front and hand each join the
/// time remaining against it, so a 4-drive shutdown is capped at 1×
/// `timeout` total.
pub fn join_all_rip_threads(timeout: Duration) {
    let devices: Vec<String> = RIP_THREADS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .keys()
        .cloned()
        .collect();
    // Cancel every active rip's halt FIRST, then join. Cancelling makes the rip
    // break out of its sweep/patch loop and RETURN, which runs the
    // SweepingGuard / MuxingGuard `Drop` impls that clear the `.sweeping` /
    // `.muxing` markers — leaving a CLEAN resumable dir (same end-state as
    // `/api/stop`). Without this the rip ignores SHUTDOWN, the join below times
    // out, the process exits, and the thread is killed WITHOUT unwinding, so
    // Drop never runs and a stale `.sweeping` survives. The startup classifier
    // then reads that as an in-progress crash and bumps `.restart_count` — so a
    // few operator redeploys / reboots / Watchtower updates DURING a rip walk a
    // perfectly healthy resumable rip to a false `.failed`. A graceful shutdown
    // is NOT a failure and must never increment. (Bitten 2026-06-30.)
    for device in &devices {
        if let Some(h) = device_halt(device) {
            h.cancel();
        }
    }
    let deadline = std::time::Instant::now() + timeout;
    for device in devices {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if join_rip_thread(&device, remaining).is_err() {
            tracing::warn!(device = %device, "rip thread did not drain within timeout");
        }
    }
}

/// Per-device cooperative-cancel tokens. The rip thread spawn site
/// allocates one [`Halt`] per rip and stashes its clone here so the
/// HTTP stop handler in `web.rs` (and `eject_drive`) can find it.
///
/// Replaces the 0.17 `HALT_FLAGS` + `STOP_FLAGS` pair (two parallel
/// `Arc<AtomicBool>` registries that the old `request_stop` flipped
/// in lockstep). One token, one bit, one source of truth — every
/// phase that holds a clone observes Stop on its next poll.
static HALTS: once_cell::sync::Lazy<Mutex<std::collections::HashMap<String, Halt>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

/// Stash the rip thread's [`Halt`] for later lookup by the stop /
/// eject paths. Called once at the top of every rip; any prior token
/// for the same device is dropped.
pub fn register_halt(device: &str, halt: Halt) {
    // Recover from poison: a silently-dropped registration means
    // /api/stop can never find this device's token, turning Stop into a
    // silent no-op. Recover-and-proceed (same convention as update_state).
    let mut halts = HALTS.lock().unwrap_or_else(|e| e.into_inner());
    halts.insert(device.to_string(), halt);
}

/// Look up the device's currently-registered [`Halt`]. Returns `None`
/// if no rip thread is registered for `device`. Cloning the returned
/// token is cheap (Arc bump) — clones share the underlying flag with
/// the rip-side clones already threaded into sweep / patch / mux.
pub fn device_halt(device: &str) -> Option<Halt> {
    // Recover from poison: returning None on poison would make /api/stop a
    // silent no-op (it looks up the token through here). Recover-and-proceed.
    let halts = HALTS.lock().unwrap_or_else(|e| e.into_inner());
    halts.get(device).cloned()
}

/// Stop a device's rip thread and drain it: cancel the per-device [`Halt`]
/// token (which every rip-phase clone polls), then block until the thread
/// finishes or `timeout` elapses. Returns `true` if the thread drained
/// (finished, or there was none) within the budget, `false` on timeout.
///
/// This is the core of the HTTP `/api/stop` handler — extracted so the
/// stop→drain contract (handle_stop must not return while the rip thread is
/// still mid-write, holding the SCSI session) is testable by driving the
/// REAL function rather than a replica. `handle_stop` calls this, then layers
/// on the verify-worker drain + STATE reset that are specific to the web path.
pub fn stop_and_drain(device: &str, timeout: Duration) -> bool {
    if let Some(halt) = device_halt(device) {
        halt.cancel();
    }
    join_rip_thread(device, timeout).is_ok()
}

/// Atomically swap in a new [`Halt`] for `device`, carrying forward a Stop
/// that landed on the outgoing token. Under a SINGLE acquisition of the
/// `HALTS` lock this: reads the outgoing token's `is_cancelled()`, inserts
/// `new`, and — if the old token was already cancelled — cancels `new`.
///
/// This closes a TOCTOU race at the placeholder→real token swap in
/// `rip_disc`: doing read-then-insert-then-cancel as three separate steps let
/// a concurrent `/api/stop` (which calls `device_halt(device).cancel()`)
/// landing between the read and the insert get lost — the user's Stop would
/// cancel a token nobody polls again, hanging the drain. Holding the lock
/// across the whole check+insert+carry serialises the two paths.
pub fn swap_halt_carrying_cancel(device: &str, new: Halt) {
    // Recover from poison (same convention as register_halt / device_halt):
    // a dropped swap would strand /api/stop on a token no phase loop reads.
    let mut halts = HALTS.lock().unwrap_or_else(|e| e.into_inner());
    let prior_cancelled = halts.get(device).map(|h| h.is_cancelled()).unwrap_or(false);
    if prior_cancelled {
        // Cancel BEFORE inserting so the token is already in its final state
        // when it becomes visible; a concurrent /api/stop now either sees the
        // old token (and we carry it) or the new cancelled one.
        new.cancel();
    }
    halts.insert(device.to_string(), new);
}

/// Drop the device's registered [`Halt`]. Called from the rip-thread
/// cleanup paths (every early-return branch in `rip_disc`) so a
/// subsequent rip on the same device starts with a fresh token.
pub fn unregister_halt(device: &str) {
    // Recover from poison rather than leaking a stale token that would
    // shadow the next rip's fresh Halt. Recover-and-proceed.
    let mut halts = HALTS.lock().unwrap_or_else(|e| e.into_inner());
    halts.remove(device);
}

/// Roll a device back to idle after a failed `spawn_rip_thread`, undoing the
/// claim identified by `claim_gen` — the value the caller's own
/// [`super::try_claim_active_checked`] returned — and nothing else.
///
/// A spawn failure after the device has been claimed (the claim set
/// `status="scanning"`) and a `Halt` token registered would otherwise wedge the
/// device in "scanning" forever (409 on every future scan/rip) and leak the
/// Halt. This is the single rollback used by BOTH web handlers (`/api/scan`,
/// `/api/rip`) and the disc-insert poll loop so they can't drift. The disc is
/// assumed still present.
///
/// # Why the generation, and not a liveness check
///
/// A spawn can fail for two very different reasons: the OS refused the thread
/// (nothing is registered for this device — the case this function exists
/// for), or `spawn_rip_thread` refused to displace a still-running incumbent.
/// In the second case the "failure" belongs to the duplicate, and an
/// unconditional rollback would vandalise the WINNER: `unregister_halt`
/// deletes the token `/api/stop` needs to cancel the live rip, and the idle
/// push overwrites a running rip's state.
///
/// The previous guard answered that with *liveness* — "is any worker thread
/// running for this device?" — and returned early if so. That is the wrong
/// question, and it introduced a wedge of its own: the early return left the
/// LOSER's claim standing. No thread, no Halt, `status == "scanning"`,
/// `is_busy()` true forever if the incumbent never writes state again (which
/// is precisely the case in the incumbent's terminal tail, the window this
/// whole race lives in). All four device routes then answer 409 indefinitely
/// and only `/api/stop` can recover the device.
///
/// The generation answers the *right* question — "is the claim I made still
/// the claim in force?" — and it is exactly what `RipState::claim_gen` was
/// added for. It is correct under every interleaving with no liveness read at
/// all: the loser always clears its own claim, and can never clear the
/// winner's, because a winner's claim necessarily bumped the generation past
/// the loser's. `update_state` carries the generation forward, so an ordinary
/// progress write does not look like a re-claim.
pub fn rollback_failed_spawn(device: &str, claim_gen: u64) {
    {
        // Recover-and-proceed on poison (module convention). Guard is dropped
        // before `unregister_halt` / `update_state` so no two locks are held.
        let s = super::STATE.lock().unwrap_or_else(|e| e.into_inner());
        let current = s.get(device).map(|r| r.claim_gen);
        if current != Some(claim_gen) {
            tracing::warn!(
                device = %device,
                rolling_back = claim_gen,
                in_force = ?current,
                "declining spawn rollback: the device has been re-claimed since \
                 (the spawn that failed was the duplicate, not the incumbent)"
            );
            return;
        }
    }
    super::unregister_halt(device);
    super::update_state(
        device,
        super::RipState {
            device: device.to_string(),
            status: "idle".to_string(),
            disc_present: true,
            ..Default::default()
        },
    );
}

// ─── Per-device drive session ──────────────────────────────────────────────

/// Persistent drive session — survives across scan → rip transitions.
/// Dropped on eject, stop, or error.
pub(super) struct DriveSession {
    pub(super) drive: libfreemkv::Drive,
    pub(super) disc: Option<libfreemkv::Disc>,
    pub(super) scanned: bool,
    pub(super) probed: bool,
    pub(super) tmdb: Option<crate::tmdb::TmdbResult>,
    pub(super) device_path: String,
}

/// Global drive sessions — one per device.
static SESSIONS: once_cell::sync::Lazy<Mutex<std::collections::HashMap<String, DriveSession>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

/// Last-known disc identity per device — the UDF Volume Identifier of
/// the disc that was scanned into the device's `DriveSession`. Kept in
/// a separate table (not on `DriveSession`) so it OUTLIVES the session:
/// the transport-failure recovery path drops the session before it
/// calls `rediscover_drive`, and the rediscovery needs the identity to
/// reject a neighbouring drive that merely happens to have an unrelated
/// disc loaded (see `rediscover_drive`). Populated automatically by
/// `store_session` from `session.disc.volume_id`.
static DISC_IDENTITY: once_cell::sync::Lazy<Mutex<std::collections::HashMap<String, String>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

pub(super) fn take_session(device: &str) -> Option<DriveSession> {
    // Recover-and-proceed on poison (matching store_session / register_halt):
    // returning None here would have the caller open a fresh drive even though
    // a usable session was sitting under the poisoned lock.
    SESSIONS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(device)
}

/// Record (or clear) the device's cached disc identity for the rediscovery
/// path. `volume_id` is the freshly-scanned disc's UDF Volume Identifier;
/// empty means the disc has no usable label.
///
/// An empty label is not a discriminator — storing it would match every
/// other label-less disc — but it must still CLEAR any previous disc's
/// entry. Skipping the write outright (what this did before) left the
/// PREVIOUS disc's volume id cached against a device that now holds a
/// different, unlabelled disc, and `rediscover_drive` would then "confirm" a
/// shifted candidate against a disc that is no longer in the drive: the
/// exact wrong-disc attachment the identity check exists to prevent.
pub(super) fn cache_disc_identity(device: &str, volume_id: &str) {
    let vid = volume_id.trim();
    // Recover-and-proceed on poison (module convention): a skipped clear is
    // a stale identity, which is worse than no identity.
    let mut ids = DISC_IDENTITY.lock().unwrap_or_else(|e| e.into_inner());
    if vid.is_empty() {
        ids.remove(device);
    } else {
        ids.insert(device.to_string(), vid.to_string());
    }
}

pub(super) fn store_session(device: &str, session: DriveSession) {
    // Cache the scanned disc's volume identifier before storing, so the
    // rediscovery path can match it after the session is dropped.
    cache_disc_identity(
        device,
        session
            .disc
            .as_ref()
            .map(|d| d.volume_id.as_str())
            .unwrap_or(""),
    );
    // Recover-and-proceed on poison (matching register_halt / register_rip_thread):
    // dropping the session silently would make session_is_scanned return false
    // and fire a redundant 10-30s re-scan (clearing TMDB metadata in the UI).
    SESSIONS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(device.to_string(), session);
}

/// Is a rip/scan worker for `device` still running?
///
/// The thread-liveness fact `is_busy` cannot give. `is_busy` is
/// `STATE[device].status == "scanning" | "ripping"`, and a worker writes its
/// TERMINAL status and then keeps running its tail on the same thread —
/// auto-eject (`Drive::open` + `session.eject()`, real hardware I/O), the
/// eject-failure device log lines, guard drops. Teardown paths that would
/// pull state out from under that tail must ask this too, exactly as
/// [`forget_device_session_state`] asks the handle itself rather than
/// trusting the status.
///
/// `false` for a device with no registered handle: nothing is running, so
/// nothing is deferred. Residual window (unchanged by this predicate, and
/// narrower than the one it closes): [`join_rip_thread`] takes the handle OUT
/// of the map for the duration of an off-thread drain, so while another
/// thread sits in that poll a live worker reads as not-running here. The
/// self-join drain — the auto-eject tail, which is the tail that actually
/// races the hot-unplug rescan — re-stashes the handle immediately and so is
/// covered.
pub(super) fn rip_thread_running(device: &str) -> bool {
    // Recover-and-proceed on poison (module convention): a poisoned map means
    // a worker panicked, and reporting "nothing is running" there is the
    // permission-to-tear-down this predicate exists to withhold.
    RIP_THREADS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(device)
        .is_some_and(|h| !h.is_finished())
}

/// Evict the per-device state THIS module owns, on hot-unplug teardown:
/// the cached [`DISC_IDENTITY`] entry, and — only if the rip thread has
/// already exited — its [`RIP_THREADS`] handle and its [`HALTS`] token.
/// Called from [`super::state::forget_device_state`], which is the single
/// teardown entry point; see its doc for the full per-device inventory.
///
/// Returns `true` if a finished rip-thread handle was reaped.
///
/// A *running* handle is deliberately left in place. Dropping it makes the
/// thread unjoinable, so a later stop/eject/shutdown drain returns while the
/// worker is still mid-write and staging can be wiped underneath it — the
/// v0.13.6 bug class that `register_rip_thread` and `join_rip_thread` are
/// both built around. `forget_removed_device` only calls in here for a
/// device that is not `is_busy`, but "not busy" is a STATE *status*, not a
/// thread-liveness fact: the worker clears its status and then keeps
/// unwinding (eject, log flush, guard drops). So we ask the handle itself,
/// and leave a live one for the next `register_rip_thread` reap or the
/// shutdown `join_all_rip_threads` to collect.
///
/// The `Halt` is evicted on the same condition, and only then: while a
/// thread is still running its token must stay reachable so `/api/stop`
/// can cancel it. Once the thread is gone nothing polls the token, and a
/// leftover entry would shadow the next rip's fresh one.
pub(super) fn forget_device_session_state(device: &str) -> bool {
    // Recover-and-proceed on poison (module convention): a skipped eviction
    // here is the unbounded growth this function exists to prevent.
    DISC_IDENTITY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(device);
    let reaped = {
        let mut t = RIP_THREADS.lock().unwrap_or_else(|e| e.into_inner());
        if t.get(device).is_some_and(|h| h.is_finished()) {
            // is_finished() == true means join() cannot block, so joining
            // under the lock is safe (same argument as register_rip_thread).
            if let Some(h) = t.remove(device)
                && let Err(e) = h.join()
            {
                tracing::error!(
                    device = %device,
                    "reaped rip thread of a removed device had panicked: {:?}", e
                );
            }
            true
        } else {
            false
        }
    };
    // Take HALTS only after RIP_THREADS is released — never nest the two.
    if reaped {
        unregister_halt(device);
    }
    reaped
}

/// The UDF Volume Identifier last scanned for `device`, if any. Used by
/// `rediscover_drive` to verify a re-enumerated candidate carries the
/// SAME disc, not an unrelated one in a neighbouring drive.
pub(super) fn expected_volume_id(device: &str) -> Option<String> {
    DISC_IDENTITY.lock().ok()?.get(device).cloned()
}

/// True iff `device` has a stored `DriveSession` with `scanned == true`.
/// Used by `handle_rip_request` to skip a redundant `scan_disc` call
/// when the disc has just been scanned (e.g. ON_INSERT=scan ran on
/// disc insertion, then the user clicked Rip). Without this check the
/// scan ran twice — clearing the TMDB poster + title in the UI and
/// burning 10-30 s redoing identify + lookup + full title scan.
///
/// Returns false if the lock can't be acquired, the device has no
/// session, or the session exists but was created without `scanned=true`
/// (currently impossible — every `store_session` call site passes true —
/// but keeps the check honest if that invariant ever loosens).
pub(super) fn session_is_scanned(device: &str) -> bool {
    SESSIONS
        .lock()
        .ok()
        .and_then(|s| s.get(device).map(|sess| sess.scanned))
        .unwrap_or(false)
}

pub(super) fn drop_session(device: &str) {
    // Recover-and-proceed on poison (matching store_session / take_session):
    // silently no-op'ing here would leak a stale DriveSession in the map, so a
    // later store_session would overwrite it and warn about a missing removal.
    SESSIONS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(device);
}

/// After a USB re-enumeration (bridge crash), the sg device number may
/// change. Probe the original path and its neighbors to find the drive
/// that still has the disc. Returns the new device path (e.g. "/dev/sg5").
pub(super) fn rediscover_drive(device: &str, original_path: &str) -> Option<String> {
    // TODO(step1-followup): /dev/sgN device-path synthesis for hot-plug
    // rediscovery is NOT moved into DiscSession. It is entangled with autorip's
    // disc-identity matching (expected_volume_id), device_log, and the sg-number
    // shift heuristic — out of the step-1 scope. Left in autorip per contract Q3.
    // Only meaningful for /dev/sgN paths. If the path is not sgN, a
    // numeric default (the old `unwrap_or(-1)`) plus the per-iteration
    // `< 0` skip would probe sg0..sg2 and could latch onto an unrelated
    // drive that merely happens to have a disc loaded. Bail instead.
    let sg_num = match original_path
        .rsplit('/')
        .next()
        .and_then(|s| s.strip_prefix("sg"))
        .and_then(|n| n.parse::<i32>().ok())
    {
        Some(n) => n,
        None => {
            tracing::warn!(
                device = %device,
                path = %original_path,
                "rediscover_drive: path is not /dev/sgN, skipping rediscovery"
            );
            return None;
        }
    };

    // Stable disc identifier from the last scan (UDF Volume Identifier).
    // When present, a candidate at a SHIFTED sg number must carry the
    // same disc before we accept it — otherwise a neighbouring drive
    // (e.g. sg2) holding an unrelated disc could win the probe while the
    // intended drive is still re-enumerating, silently attaching the
    // session to the WRONG disc. When absent (disc was never scanned, or
    // had no volume label) we fall back to the old disc-present heuristic
    // and log that the match is unverified.
    let expected_vid = expected_volume_id(device);

    for delta in [0i32, -1, 1, -2, 2, -3, 3] {
        let probe_num = sg_num + delta;
        if probe_num < 0 {
            continue;
        }
        let path = format!("/dev/sg{probe_num}");
        if !libfreemkv::drive_has_disc(std::path::Path::new(&path)).unwrap_or(false) {
            continue;
        }

        // delta == 0 means the sg number did not change — same physical
        // device node, so it is by definition the same drive. Accept
        // without a disc-identity read (which would be a redundant probe
        // of a drive we already trust).
        if path_unchanged(delta) {
            tracing::info!(
                device = %device,
                new_path = %path,
                "rediscovered drive after USB re-enumeration (path unchanged)"
            );
            return Some(path);
        }

        // Shifted sg number — could be the intended drive OR a neighbour.
        // If we know the disc identity, verify the candidate carries the
        // same disc before accepting it. No stored identity → keep the
        // legacy disc-present behaviour but flag it as unverified.
        let Some(expected) = expected_vid.as_deref() else {
            tracing::warn!(
                device = %device,
                new_path = %path,
                "rediscovered drive after USB re-enumeration (UNVERIFIED — no stored disc identity to confirm it is the same disc)"
            );
            return Some(path);
        };

        let probed = probe_volume_id(&path);
        if candidate_identity_confirmed(probed.as_deref(), expected) {
            let vid = probed.as_deref().unwrap_or_default();
            tracing::info!(
                device = %device,
                new_path = %path,
                volume_id = %vid_for_log(vid),
                "rediscovered drive after USB re-enumeration (disc identity confirmed)"
            );
            return Some(path);
        }
        match probed {
            Some(vid) => {
                tracing::warn!(
                    device = %device,
                    candidate = %path,
                    candidate_volume_id = %vid_for_log(&vid),
                    expected_volume_id = %vid_for_log(expected),
                    "skipping rediscovery candidate — disc identity mismatch (unrelated disc in a neighbouring drive)"
                );
            }
            None => {
                tracing::warn!(
                    device = %device,
                    candidate = %path,
                    "skipping rediscovery candidate — could not read disc identity to confirm match"
                );
            }
        }
    }
    None
}

/// True when a rediscovery probe's sg-number delta indicates the candidate
/// path did not change from the original — i.e. it's definitionally the
/// same physical device node, not a shifted candidate needing disc-identity
/// verification. Extracted from [`rediscover_drive`] so the branch decision
/// is unit-testable without the surrounding real-hardware probe loop
/// (`libfreemkv::drive_has_disc`).
fn path_unchanged(delta: i32) -> bool {
    delta == 0
}

/// Whether a shifted-sg rediscovery candidate's probed volume id confirms it
/// carries the expected disc. `probed` is `None` when the identity read
/// itself failed (drive not ready, no volume label, etc.). Extracted from
/// `rediscover_drive` so the accept/reject decision — "does this candidate
/// carry the SAME disc, or could it be an unrelated one in a neighbouring
/// bay" — is unit-testable without real drive I/O, which the enclosing
/// function otherwise requires end-to-end (`drive_has_disc`,
/// `probe_volume_id` both talk to hardware).
fn candidate_identity_confirmed(probed: Option<&str>, expected: &str) -> bool {
    probed == Some(expected)
}

/// A disc-supplied UDF Volume Identifier, made safe to put in a log field.
///
/// `log::sanitize_log_msg`'s own doc names the UDF volume-id as the string it
/// exists to defend against, but it only ran on `device_log`'s path — these
/// `tracing` fields reach the human-readable `autorip.log` and stderr (so
/// `docker logs` / `tail`) formatted with plain `Display` and no escaping, so a
/// crafted disc could inject ANSI into an operator's terminal through the
/// rediscovery path.
fn vid_for_log(vid: &str) -> String {
    crate::log::sanitize_log_msg(vid)
}

/// Read the UDF Volume Identifier of the disc currently in the drive at
/// `path`, for disc-identity matching during rediscovery. Returns None
/// on any failure (open / ready / init / identify) — the caller treats
/// "couldn't read identity" as "not a confirmed match" and keeps
/// probing. `Disc::identify` only reads the UDF filesystem (a handful of
/// sectors), so this is far lighter than a full `Disc::scan` and safe to
/// run once per shifted candidate.
fn probe_volume_id(path: &str) -> Option<String> {
    // TODO(step1-followup): NOT migrated to DiscSession. This path's error model
    // is fail-fast-to-None (a wait_ready/init failure returns None WITHOUT
    // attempting identify), whereas DiscSession::open treats wait_ready/init as
    // advisory (logs + continues). Folding it in would change the short-circuit
    // semantics of hot-plug rediscovery, so it stays a direct Drive open here.
    let mut drive = libfreemkv::Drive::open(std::path::Path::new(path)).ok()?;
    drive.wait_ready().ok()?;
    drive.init().ok()?;
    let id = libfreemkv::Disc::identify(&mut drive).ok()?;
    drive.close();
    let vid = id.volume_id.trim();
    if vid.is_empty() {
        None
    } else {
        Some(vid.to_string())
    }
}

#[cfg(test)]
mod rollback_tests {
    use super::*;

    /// A disc-supplied volume-id must not carry terminal escapes into a log.
    ///
    /// `log::sanitize_log_msg`'s own doc names the UDF volume-id as the string
    /// it exists to defend against, but it only ran on `device_log`'s path.
    /// The rediscovery `tracing` fields reach `autorip.log` and stderr — so
    /// `docker logs` and `tail` — formatted with plain `Display` and no
    /// escaping, so a crafted disc could paint an operator's terminal.
    ///
    /// The expectation is control-bytes-out, taken from what a terminal must
    /// never receive, not from what the sanitizer happens to do.
    #[test]
    fn a_volume_id_reaches_a_log_field_with_no_terminal_escapes() {
        // ESC [ 2 J is "clear screen"; a bare CR hides the line before it.
        let hostile = "DISC\x1b[2J\rHARMLESS\x07";
        let logged = vid_for_log(hostile);
        assert!(
            !logged.chars().any(|c| c.is_control()),
            "a disc-supplied volume-id must reach a log field with no control \
             bytes; got {logged:?}"
        );
        assert!(
            logged.contains("DISC") && logged.contains("HARMLESS"),
            "sanitising must not destroy the identifier's readable text; got {logged:?}"
        );
    }

    /// Catches the mutation that drops the generation check from
    /// `rollback_failed_spawn` (making it roll back unconditionally), and the
    /// mutation that restores the round-1 `rip_thread_running` early return
    /// (which left the loser's own claim standing forever).
    ///
    /// A spawn can fail for two unrelated reasons, and only one of them means
    /// "the claim I made is still the claim in force". When `spawn_rip_thread`
    /// refuses to displace a still-running incumbent, the loser's caller runs
    /// this same rollback. It must do BOTH of these, and the round-1 guard did
    /// only the first:
    ///
    /// * not vandalise the WINNER — no unregistering the `Halt` that
    ///   `/api/stop` needs to cancel the live rip, no idling a running rip's
    ///   state row;
    /// * still clear the LOSER's own claim, or the device sits at
    ///   `status="scanning"` with no thread and no Halt, `is_busy()` true, and
    ///   every route answering 409 until someone POSTs `/api/stop`.
    #[test]
    fn rollback_scoped_to_its_own_claim_spares_the_winner_and_clears_the_loser() {
        let dev = format!("rollback-live-worker-test-{}", std::process::id());
        let _ = super::take_rip_thread(&dev);
        let winner_gen = super::super::try_claim_active(&dev).expect("claim must succeed");
        let winner_halt = Halt::new();
        super::super::register_halt(&dev, winner_halt);

        // A worker that is still on the CPU — the incumbent/winner.
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        super::spawn_rip_thread(&dev, "rip", move || {
            let _ = release_rx.recv();
        })
        .expect("the first spawn owns the device");

        // The loser rolls back a claim that is NO LONGER in force (the winner's
        // generation is the current one). Nothing of the winner's may move.
        let stale_gen = winner_gen.saturating_sub(1);
        super::rollback_failed_spawn(&dev, stale_gen);
        assert!(
            super::super::device_halt(&dev).is_some(),
            "a rollback for a superseded claim must NOT unregister the live \
             worker's Halt — /api/stop would then have no token with which to \
             cancel the running rip"
        );
        assert_eq!(
            super::super::STATE
                .lock()
                .unwrap()
                .get(&dev)
                .map(|r| r.status.clone()),
            Some("scanning".to_string()),
            "a rollback for a superseded claim must NOT idle the winner"
        );

        // Now the H2 wedge itself: the claim that IS in force is rolled back
        // while a worker thread is still alive for this device. That is the
        // shape of a losing `/api/rip` whose `spawn_rip_thread` came back
        // `PriorThreadRunning` — it claimed (bumping the generation to the one
        // in force) and registered its own Halt before the spawn was refused.
        // Round 1 returned early here because a thread was running, leaving
        // "scanning" set with nothing left that would ever clear it.
        super::super::register_halt(&dev, Halt::new());
        super::rollback_failed_spawn(&dev, winner_gen);
        assert!(
            super::super::device_halt(&dev).is_none(),
            "rolling back the claim in force must also release the Halt that \
             claim registered, or the next rip inherits a stale token"
        );
        assert_eq!(
            super::super::STATE
                .lock()
                .unwrap()
                .get(&dev)
                .map(|r| r.status.clone()),
            Some("idle".to_string()),
            "rolling back the claim that is in force must clear it — leaving it \
             set wedges every route on this device at 409 with no thread and no \
             Halt to recover from"
        );

        drop(release_tx);
        let _ = super::join_rip_thread(&dev, std::time::Duration::from_secs(10));
        super::super::STATE.lock().unwrap().remove(&dev);
        let _ = super::take_rip_thread(&dev);
        super::super::unregister_halt(&dev);
    }

    #[test]
    fn rollback_failed_spawn_clears_halt_and_idles() {
        let dev = format!("rollback-test-{}", std::process::id());
        // Simulate the pre-spawn state: claim (sets status=scanning) +
        // register a Halt, exactly as the poll loop / web handlers do.
        let claim_gen = super::super::try_claim_active(&dev).expect("claim must succeed");
        super::super::register_halt(&dev, Halt::new());
        assert!(super::super::device_halt(&dev).is_some(), "halt registered");

        super::super::rollback_failed_spawn(&dev, claim_gen);

        // Halt is gone (no leak) and the device is idle with the disc still
        // present, so a future scan/rip is not wedged at 409.
        assert!(
            super::super::device_halt(&dev).is_none(),
            "rollback must unregister the halt"
        );
        let snap = super::super::STATE
            .lock()
            .unwrap()
            .get(&dev)
            .cloned()
            .expect("state entry exists");
        assert_eq!(snap.status, "idle", "must roll back to idle");
        assert!(snap.disc_present, "disc still present after rollback");
        assert!(!super::super::is_busy(&dev), "device no longer busy");
        // rollback_failed_spawn builds its own `RipState { device: ...,
        // status: "idle", disc_present: true, ..Default::default() }`
        // literal — the map lookup above only proves the KEY is right, not
        // that the struct's own `device` field survived. Assert it too.
        assert_eq!(
            snap.device, dev,
            "device field in the rollback RipState must match"
        );
    }

    #[test]
    fn swap_halt_carrying_cancel_carries_forward_a_pending_cancel() {
        // HIGH-ish (rule 3, TOCTOU): swap_halt_carrying_cancel exists
        // specifically so a Stop that lands on the OUTGOING placeholder
        // token between rip_disc allocating it and swapping in the real
        // one is not lost. No existing test calls it at all. Pin both
        // halves of the contract: (1) a cancelled outgoing token's Stop
        // carries onto the incoming token, and (2) an un-cancelled
        // outgoing token does NOT spuriously cancel the incoming one.
        let dev = format!("swap-halt-test-{}", std::process::id());

        // Case 1: outgoing token already cancelled (a Stop raced the swap).
        let placeholder = Halt::new();
        super::super::register_halt(&dev, placeholder.clone());
        placeholder.cancel();
        let real = Halt::new();
        super::super::swap_halt_carrying_cancel(&dev, real.clone());
        assert!(
            real.is_cancelled(),
            "a Stop that landed on the outgoing placeholder must carry \
             forward onto the freshly-swapped-in token, or the drain hangs \
             waiting for a Halt nobody ever cancels"
        );

        // Case 2: outgoing token NOT cancelled — the new token must stay
        // live, not get spuriously cancelled by the swap itself.
        let dev2 = format!("swap-halt-test-clean-{}", std::process::id());
        let placeholder2 = Halt::new();
        super::super::register_halt(&dev2, placeholder2);
        let real2 = Halt::new();
        super::super::swap_halt_carrying_cancel(&dev2, real2.clone());
        assert!(
            !real2.is_cancelled(),
            "swapping in a new Halt must not cancel it when nothing asked \
             to stop"
        );
    }

    #[test]
    fn path_unchanged_only_for_zero_delta() {
        // rediscover_drive's delta==0 fast path skips disc-identity
        // verification entirely (same physical device node => trusted by
        // construction). Any nonzero delta is a SHIFTED candidate and must
        // go through the identity check instead.
        assert!(path_unchanged(0));
        assert!(!path_unchanged(1));
        assert!(!path_unchanged(-1));
        assert!(!path_unchanged(3));
    }

    #[test]
    fn candidate_identity_confirmed_requires_exact_match() {
        // The whole point of the disc-identity check rediscover_drive relies
        // on: a shifted-sg candidate is only accepted if its probed volume
        // id EXACTLY matches the expected one. A failed probe (None) or a
        // different disc's id must both be rejected — accepting either
        // would let a neighbouring drive's unrelated disc silently take
        // over an in-flight rip's session after a USB re-enumeration.
        assert!(candidate_identity_confirmed(
            Some("DISC_VOL_123"),
            "DISC_VOL_123"
        ));
        assert!(!candidate_identity_confirmed(
            Some("SOME_OTHER_DISC"),
            "DISC_VOL_123"
        ));
        assert!(!candidate_identity_confirmed(None, "DISC_VOL_123"));
    }

    /// Swapping in a disc with NO volume label must not leave the previous
    /// disc's identity cached. `store_session` used to `filter(|v|
    /// !v.is_empty())` and simply skip the write, so `expected_volume_id`
    /// kept answering with a disc that had been ejected — and
    /// `rediscover_drive` would accept a shifted sg candidate carrying that
    /// old disc as "identity confirmed".
    #[test]
    fn an_unlabelled_disc_clears_the_previous_discs_cached_identity() {
        let dev = format!("disc-identity-swap-{}", std::process::id());

        cache_disc_identity(&dev, "FIRST_DISC_VOL");
        assert_eq!(
            expected_volume_id(&dev).as_deref(),
            Some("FIRST_DISC_VOL"),
            "a labelled disc is cached"
        );

        // Operator swaps in a disc with no UDF volume label.
        cache_disc_identity(&dev, "");
        assert_eq!(
            expected_volume_id(&dev),
            None,
            "an unlabelled disc must leave NO identity, not the ejected \
             disc's — verifying a rediscovery candidate against a disc that \
             is no longer in the drive is how the wrong disc gets attached"
        );

        // Whitespace-only is the same non-label (the label is trimmed).
        cache_disc_identity(&dev, "SECOND_DISC_VOL");
        cache_disc_identity(&dev, "   ");
        assert_eq!(expected_volume_id(&dev), None);
    }

    /// Regression: hot-unplug teardown must not leak this module's
    /// per-device maps. `forget_device_state` used to evict only
    /// `TITLE_OVERRIDES` + `STOP_COOLDOWNS` (and said so in a doc that
    /// claimed they were the only other per-device state), leaving the
    /// device's finished `JoinHandle` in `RIP_THREADS`, its cached volume
    /// id in `DISC_IDENTITY` and its token in `HALTS` for the container's
    /// lifetime — one set per device path the kernel ever handed out.
    #[test]
    fn forgetting_a_removed_device_reaps_its_finished_thread_and_identity() {
        // Fixture name unique to this test: RIP_THREADS / DISC_IDENTITY /
        // HALTS are process-global and shared across the whole test binary.
        let dev = format!("forget-reap-{}", std::process::id());

        // A worker that exits immediately, registered exactly as the poll
        // loop registers a real rip thread.
        spawn_rip_thread(&dev, "rip", || {}).expect("spawn must succeed");
        register_halt(&dev, Halt::new());
        DISC_IDENTITY
            .lock()
            .unwrap()
            .insert(dev.clone(), "VOL_FORGET_REAP".to_string());

        // Watchdog: wait for the worker to actually exit before asserting on
        // the reap, but never block the suite. The closure is empty, so 5 s
        // is ~4 orders of magnitude of margin over a thread spawn+exit; a
        // regression that never finishes fails here instead of hanging.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let finished = RIP_THREADS
                .lock()
                .unwrap()
                .get(&dev)
                .is_some_and(|h| h.is_finished());
            if finished {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "worker thread did not exit within 5s"
            );
            std::thread::sleep(Duration::from_millis(10));
        }

        super::super::state::forget_device_state(&dev);

        assert!(
            !RIP_THREADS.lock().unwrap().contains_key(&dev),
            "a finished JoinHandle must be reaped when the device is torn \
             down, not left in RIP_THREADS forever"
        );
        assert!(
            !DISC_IDENTITY.lock().unwrap().contains_key(&dev),
            "the cached disc identity must be dropped when the device is \
             torn down — nothing else ever removes from DISC_IDENTITY"
        );
        assert!(
            super::super::device_halt(&dev).is_none(),
            "the halt token of an exited thread must go with its handle"
        );
    }

    /// The other half of the contract: a rip thread that is still RUNNING
    /// must keep its registration. Dropping a live `JoinHandle` makes the
    /// thread unjoinable, so a later stop/eject/shutdown drain returns while
    /// the worker is still mid-write and staging is wiped underneath it —
    /// the v0.13.6 bug class. Teardown is allowed to be slower, never
    /// unsafe.
    #[test]
    fn forgetting_a_device_leaves_a_still_running_thread_registered() {
        let dev = format!("forget-keep-running-{}", std::process::id());
        let gate = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_gate = gate.clone();
        spawn_rip_thread(&dev, "rip", move || {
            // Watchdog: 5 s is the ceiling, not the expectation — the
            // assertions below run in microseconds, so the release comes
            // essentially immediately. The bound only stops a regression
            // from parking this thread for the life of the suite.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !worker_gate.load(std::sync::atomic::Ordering::SeqCst)
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(5));
            }
        })
        .expect("spawn must succeed");
        register_halt(&dev, Halt::new());

        super::super::state::forget_device_state(&dev);

        assert!(
            RIP_THREADS.lock().unwrap().contains_key(&dev),
            "a still-running rip thread's handle must survive teardown — \
             dropping it makes the thread unjoinable and breaks \
             drain-before-wipe"
        );
        assert!(
            super::super::device_halt(&dev).is_some(),
            "a still-running thread's Halt must stay reachable so /api/stop \
             can still cancel it"
        );

        gate.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = join_rip_thread(&dev, Duration::from_secs(5));
    }

    /// Regression: `take_session` and `drop_session` must recover from a
    /// poisoned `SESSIONS` lock (unwrap_or_else into_inner), not silently
    /// no-op as the old `.lock().ok()?` / `if let Ok(..)` forms did. A
    /// silent no-op in `drop_session` would leak a stale session; a silent
    /// no-op in `take_session` discards a usable one. We poison the lock
    /// (panic while holding it, caught) and assert neither helper panics.
    #[test]
    fn session_helpers_recover_from_poison() {
        // Poison SESSIONS by panicking while the guard is held.
        let _ = std::panic::catch_unwind(|| {
            let _guard = SESSIONS.lock().unwrap();
            panic!("intentional poison");
        });
        assert!(SESSIONS.is_poisoned(), "lock must be poisoned for the test");

        let dev = format!("poison-test-{}", std::process::id());
        // Neither helper may panic on the poisoned lock; both must run.
        assert!(
            take_session(&dev).is_none(),
            "take_session on poisoned lock returns None for an absent device, not panic"
        );
        drop_session(&dev); // must not panic
    }

    /// Catches the mutation that deletes `join_rip_thread`'s self-join branch
    /// (or turns it into an ordinary poll), and the mutation that has it
    /// UNREGISTER the handle on the way out.
    ///
    /// `eject_drive` is called from the rip's own auto-eject tail, so
    /// `join_rip_thread` regularly runs ON the thread it is being asked to
    /// join. `is_finished()` can never become true from there, so an ordinary
    /// poll burns the entire 60 s stop budget inside a rip that is doing
    /// nothing wrong and then logs "did not drain". And the handle must stay
    /// registered while we sit in that tail: it is the fact that keeps a
    /// concurrent `/api/rip` from claiming a device whose worker is still
    /// holding the drive.
    #[test]
    fn join_rip_thread_called_on_its_own_thread_returns_at_once() {
        let dev = format!("self-join-test-{}", std::process::id());
        let _ = super::take_rip_thread(&dev);
        let (tx, rx) = std::sync::mpsc::channel::<(std::time::Duration, bool, bool)>();
        let dev_inner = dev.clone();
        super::spawn_rip_thread(&dev, "rip", move || {
            let t0 = std::time::Instant::now();
            // A budget far longer than any test may block for: if the
            // self-join branch is gone this sleeps for all 30 s.
            let outcome = super::join_rip_thread(&dev_inner, std::time::Duration::from_secs(30));
            let still_registered = super::rip_thread_running(&dev_inner);
            let _ = tx.send((t0.elapsed(), outcome.is_ok(), still_registered));
        })
        .expect("spawn");

        let (elapsed, ok, still_registered) = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("a self-join must return immediately, not sit out its timeout");
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "self-join returned after {elapsed:?} — it must not poll its own thread"
        );
        assert!(ok, "a self-join is not a drain failure");
        assert!(
            still_registered,
            "a self-join must leave the handle registered — it is what keeps the \
             device unclaimable for the rest of the worker's tail"
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while super::rip_thread_running(&dev) {
            assert!(std::time::Instant::now() < deadline, "worker should exit");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let _ = super::join_rip_thread(&dev, std::time::Duration::from_secs(5));
    }
}
