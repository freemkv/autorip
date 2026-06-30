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
///   handle back so the caller reaps the worker it just spawned. In
///   practice the spawn sites gate on [`try_claim_active`] (STATE-level
///   mutual exclusion) so this branch should be unreachable, but we
///   defend it rather than trust that invariant.
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
            if let Some(prior) = t.remove(device) {
                if let Err(e) = prior.join() {
                    tracing::error!(
                        device = %device,
                        "reaped prior rip thread had panicked: {:?}", e
                    );
                }
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
    RIP_THREADS.lock().ok()?.remove(device)
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
    let wrapped = move || {
        let _span =
            tracing::info_span!("worker", build = span_build, device = %span_device, role = %span_role)
                .entered();
        f();
    };
    let handle = std::thread::Builder::new().name(name).spawn(wrapped)?;
    match register_rip_thread(device, handle) {
        Ok(()) => Ok(()),
        Err(RegisterError::PriorThreadRunning(new_handle)) => {
            // A prior worker for this device is still running and owns
            // the registration slot. We just spawned a duplicate — reap
            // it so it can't leak (its work closure shares no exclusive
            // resources yet because the caller gates on try_claim_active,
            // but join it to be sure it exits) and surface failure so the
            // caller runs its existing spawn-failure rollback path.
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
/// Implementation: poll `JoinHandle::is_finished()` every 25 ms
/// until it returns true or the deadline passes. Polling avoids the
/// extra channel plumbing of a one-shot signal and keeps the
/// registration API simple (test code can register a synthetic
/// thread without producing a paired Receiver).
#[allow(clippy::result_unit_err)]
pub fn join_rip_thread(device: &str, timeout: Duration) -> Result<(), ()> {
    let handle = match take_rip_thread(device) {
        Some(h) => h,
        None => return Ok(()),
    };
    // Self-join guard: if we are *on* the registered rip thread (e.g.
    // `eject_drive` called from the rip's own auto-eject path at the end
    // of `rip_disc`), `handle.is_finished()` can never become true while
    // we sit here, so the poll loop below would spin the full `timeout`
    // (60s), log a spurious "did not drain" warning, stash the handle
    // back, and only then proceed. Detect that case and return
    // immediately — the thread is by definition still running (it's us)
    // and will exit as soon as we return up the stack. We stash the
    // handle back so a later off-thread caller (or the shutdown drain)
    // can still reap it once we've actually exited.
    if handle.thread().id() == std::thread::current().id() {
        // Mirror the timeout path: on a poisoned mutex recover the guard and
        // re-stash so join_all_rip_threads can still reap the handle (the
        // no-silently-dropped-handle invariant this module documents).
        match RIP_THREADS.lock() {
            Ok(mut t) => {
                t.insert(device.to_string(), handle);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(device.to_string(), handle);
                tracing::error!(
                    device = %device,
                    "RIP_THREADS poisoned on self-join; recovered guard to re-stash handle"
                );
            }
        }
        return Ok(());
    }
    let deadline = std::time::Instant::now() + timeout;
    while !handle.is_finished() {
        if std::time::Instant::now() >= deadline {
            // Stash the handle back so a later caller (or shutdown)
            // can reap it; the thread is still running. `take_rip_thread`
            // already removed it, so on a poisoned mutex the handle would
            // otherwise be dropped here and could never be reaped at
            // shutdown — recover the poisoned guard and re-stash so the
            // leak doesn't go silent.
            match RIP_THREADS.lock() {
                Ok(mut t) => {
                    t.insert(device.to_string(), handle);
                }
                Err(poisoned) => {
                    poisoned.into_inner().insert(device.to_string(), handle);
                    tracing::error!(
                        device = %device,
                        "RIP_THREADS poisoned on join timeout; recovered guard to re-stash handle"
                    );
                }
            }
            return Err(());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    // join() returns Err(payload) if the thread panicked. The thread
    // DID finish (so we return Ok), but surface the panic so stop /
    // eject / shutdown don't treat a panicked rip as a clean exit.
    if let Err(e) = handle.join() {
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

/// Roll a device back to idle after a failed `spawn_rip_thread`.
///
/// A spawn failure after the device has been claimed (`try_claim_active`
/// set status="scanning") and a `Halt` token registered would otherwise
/// wedge the device in "scanning" forever (409 on every future scan/rip)
/// and leak the Halt. This is the single rollback used by BOTH web
/// handlers (`/api/scan`, `/api/rip`) and the disc-insert poll loop so
/// they can't drift. The disc is assumed still present.
pub fn rollback_failed_spawn(device: &str) {
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

pub(super) fn store_session(device: &str, session: DriveSession) {
    // Cache the scanned disc's volume identifier before storing, so the
    // rediscovery path can match it after the session is dropped. A
    // disc with no UDF volume label (empty string) is not a usable
    // discriminator — skip caching it rather than store an empty key
    // that would match every other label-less disc.
    if let Some(vid) = session
        .disc
        .as_ref()
        .map(|d| d.volume_id.trim())
        .filter(|v| !v.is_empty())
    {
        DISC_IDENTITY
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(device.to_string(), vid.to_string());
    }
    // Recover-and-proceed on poison (matching register_halt / register_rip_thread):
    // dropping the session silently would make session_is_scanned return false
    // and fire a redundant 10-30s re-scan (clearing TMDB metadata in the UI).
    SESSIONS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(device.to_string(), session);
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
        if delta == 0 {
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

        match probe_volume_id(&path) {
            Some(vid) if vid == expected => {
                tracing::info!(
                    device = %device,
                    new_path = %path,
                    volume_id = %vid,
                    "rediscovered drive after USB re-enumeration (disc identity confirmed)"
                );
                return Some(path);
            }
            Some(vid) => {
                tracing::warn!(
                    device = %device,
                    candidate = %path,
                    candidate_volume_id = %vid,
                    expected_volume_id = %expected,
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

/// Read the UDF Volume Identifier of the disc currently in the drive at
/// `path`, for disc-identity matching during rediscovery. Returns None
/// on any failure (open / ready / init / identify) — the caller treats
/// "couldn't read identity" as "not a confirmed match" and keeps
/// probing. `Disc::identify` only reads the UDF filesystem (a handful of
/// sectors), so this is far lighter than a full `Disc::scan` and safe to
/// run once per shifted candidate.
fn probe_volume_id(path: &str) -> Option<String> {
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

    #[test]
    fn rollback_failed_spawn_clears_halt_and_idles() {
        let dev = format!("rollback-test-{}", std::process::id());
        // Simulate the pre-spawn state: claim (sets status=scanning) +
        // register a Halt, exactly as the poll loop / web handlers do.
        assert!(super::super::try_claim_active(&dev), "claim must succeed");
        super::super::register_halt(&dev, Halt::new());
        assert!(super::super::device_halt(&dev).is_some(), "halt registered");

        super::super::rollback_failed_spawn(&dev);

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
}
