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

/// Register a rip-thread JoinHandle for `device`. Production calls
/// this from the poll-loop spawn site; the integration tests under
/// `tests/halt_drain.rs` also call it to plug a synthetic thread
/// into the same machinery `handle_stop` uses.
pub fn register_rip_thread(device: &str, handle: JoinHandle<()>) {
    if let Ok(mut t) = RIP_THREADS.lock() {
        // If an old entry is still here (e.g. prior thread crashed
        // without being reaped), drop the stale handle — we only
        // keep one live handle per device.
        t.insert(device.to_string(), handle);
    }
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
    let handle = std::thread::Builder::new().name(name).spawn(f)?;
    register_rip_thread(device, handle);
    Ok(())
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
    let deadline = std::time::Instant::now() + timeout;
    while !handle.is_finished() {
        if std::time::Instant::now() >= deadline {
            // Stash the handle back so a later caller (or shutdown)
            // can reap it; the thread is still running.
            if let Ok(mut t) = RIP_THREADS.lock() {
                t.insert(device.to_string(), handle);
            }
            return Err(());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let _ = handle.join();
    Ok(())
}

/// Drain every known rip thread, each bounded by `timeout`.
pub fn join_all_rip_threads(timeout: Duration) {
    let devices: Vec<String> = RIP_THREADS
        .lock()
        .ok()
        .map(|t| t.keys().cloned().collect())
        .unwrap_or_default();
    for device in devices {
        if join_rip_thread(&device, timeout).is_err() {
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
    if let Ok(mut halts) = HALTS.lock() {
        halts.insert(device.to_string(), halt);
    }
}

/// Look up the device's currently-registered [`Halt`]. Returns `None`
/// if no rip thread is registered for `device`. Cloning the returned
/// token is cheap (Arc bump) — clones share the underlying flag with
/// the rip-side clones already threaded into sweep / patch / mux.
pub fn device_halt(device: &str) -> Option<Halt> {
    HALTS.lock().ok().and_then(|h| h.get(device).cloned())
}

/// Drop the device's registered [`Halt`]. Called from the rip-thread
/// cleanup paths (every early-return branch in `rip_disc`) so a
/// subsequent rip on the same device starts with a fresh token.
pub fn unregister_halt(device: &str) {
    if let Ok(mut halts) = HALTS.lock() {
        halts.remove(device);
    }
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

pub(super) fn take_session(device: &str) -> Option<DriveSession> {
    SESSIONS.lock().ok()?.remove(device)
}

pub(super) fn store_session(device: &str, session: DriveSession) {
    if let Ok(mut s) = SESSIONS.lock() {
        s.insert(device.to_string(), session);
    }
}

pub(super) fn drop_session(device: &str) {
    if let Ok(mut s) = SESSIONS.lock() {
        s.remove(device);
    }
}

/// After a USB re-enumeration (bridge crash), the sg device number may
/// change. Probe the original path and its neighbors to find the drive
/// that still has the disc. Returns the new device path (e.g. "/dev/sg5").
pub(super) fn rediscover_drive(device: &str, original_path: &str) -> Option<String> {
    let sg_num = original_path
        .rsplit('/')
        .next()
        .and_then(|s| s.strip_prefix("sg").and_then(|n| n.parse::<i32>().ok()))
        .unwrap_or(-1);

    for delta in [0i32, -1, 1, -2, 2, -3, 3] {
        let probe_num = sg_num + delta;
        if probe_num < 0 {
            continue;
        }
        let path = format!("/dev/sg{probe_num}");
        if libfreemkv::drive_has_disc(std::path::Path::new(&path)).unwrap_or(false) {
            tracing::info!(
                device = %device,
                new_path = %path,
                "rediscovered drive after USB re-enumeration"
            );
            return Some(path);
        }
    }
    None
}
