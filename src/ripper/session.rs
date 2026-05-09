//! Per-device drive sessions, halt/stop bookkeeping, and the registry
//! of in-flight rip threads.
//!
//! Lifted verbatim from the monolithic `ripper.rs` as part of the 0.18
//! prep split — no semantic changes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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
/// not a fatal error. The rip thread's HALT flag was already flipped
/// by `request_stop`; the thread will exit eventually. The timeout
/// just bounds how long the HTTP response (or shutdown sequence)
/// blocks.
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

/// Per-device stop flag. Rip thread checks this and exits if true.
pub(super) static STOP_FLAGS: once_cell::sync::Lazy<
    Mutex<std::collections::HashMap<String, Arc<AtomicBool>>>,
> = once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

/// Drive halt flags — set by request_stop to interrupt Drive::read() recovery.
pub(super) static HALT_FLAGS: once_cell::sync::Lazy<
    Mutex<std::collections::HashMap<String, Arc<AtomicBool>>>,
> = once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

pub fn register_halt(device: &str, flag: Arc<AtomicBool>) {
    if let Ok(mut flags) = HALT_FLAGS.lock() {
        flags.insert(device.to_string(), flag);
    }
}

pub fn request_stop(device: &str) {
    if let Ok(flags) = STOP_FLAGS.lock() {
        if let Some(flag) = flags.get(device) {
            flag.store(true, Ordering::Relaxed);
        }
    }
    // Also halt the drive to break out of recovery loops
    if let Ok(flags) = HALT_FLAGS.lock() {
        if let Some(flag) = flags.get(device) {
            flag.store(true, Ordering::Relaxed);
        }
    }
}

pub(super) fn stop_requested(device: &str) -> bool {
    STOP_FLAGS
        .lock()
        .ok()
        .and_then(|f| f.get(device).map(|flag| flag.load(Ordering::Relaxed)))
        .unwrap_or(false)
}

pub(super) fn reset_stop_flag(device: &str) -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    if let Ok(mut flags) = STOP_FLAGS.lock() {
        flags.insert(device.to_string(), flag.clone());
    }
    flag
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
