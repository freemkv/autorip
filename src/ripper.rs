use libfreemkv::event::BatchSizeReason;
use libfreemkv::pes::Stream as PesStream;

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::config::Config;

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
pub static STOP_FLAGS: once_cell::sync::Lazy<
    Mutex<std::collections::HashMap<String, Arc<AtomicBool>>>,
> = once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

/// Drive halt flags — set by request_stop to interrupt Drive::read() recovery.
static HALT_FLAGS: once_cell::sync::Lazy<
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

fn stop_requested(device: &str) -> bool {
    STOP_FLAGS
        .lock()
        .ok()
        .and_then(|f| f.get(device).map(|flag| flag.load(Ordering::Relaxed)))
        .unwrap_or(false)
}

fn reset_stop_flag(device: &str) -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    if let Ok(mut flags) = STOP_FLAGS.lock() {
        flags.insert(device.to_string(), flag.clone());
    }
    flag
}

/// One contiguous bad range as seen in the UI. Derived from the mapfile
/// during a multi-pass rip; chapter/time-offset come from the scanned title's
/// playlist metadata when the bad region lands in AV content.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BadRange {
    pub lba: u64,
    pub count: u32,
    pub duration_ms: f64,
    pub chapter: Option<u32>,
    pub time_offset_secs: Option<f64>,
}

/// State broadcast for web UI.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RipState {
    pub device: String,
    pub status: String, // "idle", "scanning", "ripping", "moving", "done", "error"
    pub disc_present: bool,
    pub disc_name: String,
    pub disc_format: String, // "uhd", "bluray", "dvd"
    pub progress_pct: u8,
    pub progress_gb: f64,
    pub speed_mbs: f64,
    pub eta: String,
    pub errors: u32,
    /// Estimated seconds of video lost to skipped sectors. Uses the title's
    /// actual bitrate, not a hardcoded constant — the UI should prefer this
    /// over computing from `errors` client-side.
    pub lost_video_secs: f64,
    /// Last sector read (LBA). Shows forward motion through a bad zone even
    /// when bytes_written is stalled waiting for the demuxer.
    pub last_sector: u64,
    /// Current adaptive batch size. Equal to `preferred_batch` during clean
    /// reads; drops on failure, climbs back with sustained success.
    pub current_batch: u16,
    /// Kernel-reported preferred batch size (from detect_max_batch_sectors).
    pub preferred_batch: u16,
    /// Current pass number (1 = initial disc→ISO copy, 2..=N = retry patches,
    /// N+1 = mux). Zero when not in multi-pass mode.
    pub pass: u8,
    /// Total number of passes in this rip (max_retries + 1 + mux). Zero when
    /// not in multi-pass mode.
    pub total_passes: u8,
    /// Bytes confirmed good across all passes so far (from mapfile stats).
    pub bytes_good: u64,
    /// Bytes still unreadable or pending.
    pub bytes_bad: u64,
    /// Total disc size in bytes (for pass-relative progress).
    pub bytes_total_disc: u64,
    /// Bad sector ranges from the mapfile. Capped at 50 entries (biggest by
    /// duration) to keep SSE payloads bounded; `bad_ranges_truncated` reports
    /// how many more exist.
    pub bad_ranges: Vec<BadRange>,
    pub num_bad_ranges: u32,
    pub bad_ranges_truncated: u32,
    /// Sum of bad-range durations — the actual video time lost to this rip.
    pub total_lost_ms: f64,
    /// Largest single contiguous bad range's duration. Tells the difference
    /// between 1000 × 1ms gaps (unnoticeable) vs 1 × 1s gap (noticeable glitch).
    pub largest_gap_ms: f64,
    pub last_error: String,
    pub output_file: String,
    pub tmdb_title: String,
    pub tmdb_year: u16,
    pub tmdb_poster: String,
    pub tmdb_overview: String,
    pub duration: String,
    pub codecs: String,

    // ── v0.13.16 PipelineStats: the 5 user-visible numbers ────────────────
    /// Per-pass progress percent (0-100). Computed from libfreemkv's
    /// `work_done / work_total`. UI bar reads this directly — no math.
    pub pass_progress_pct: u8,
    /// Per-pass ETA, formatted as "MM:SS" or "HH:MM:SS". Empty when speed
    /// is too low to estimate.
    pub pass_eta: String,
    /// Total rip progress percent (0-100), summed across all passes +
    /// estimated retry work + mux. UI total bar reads this directly.
    pub total_progress_pct: u8,
    /// Total rip ETA across all remaining passes including mux estimate.
    pub total_eta: String,

    /// Damage severity tier (0.13.22). Computed from `errors` (bad
    /// sector count) and `total_lost_ms` (cumulative playback time lost).
    /// UI renders a colored badge: clean (green) / cosmetic (yellow) /
    /// moderate (orange) / serious (red).
    #[serde(default)]
    pub damage_severity: String,
}

impl Default for RipState {
    fn default() -> Self {
        Self {
            device: String::new(),
            status: "idle".to_string(),
            disc_present: false,
            disc_name: String::new(),
            disc_format: String::new(),
            progress_pct: 0,
            progress_gb: 0.0,
            speed_mbs: 0.0,
            eta: String::new(),
            errors: 0,
            lost_video_secs: 0.0,
            last_sector: 0,
            current_batch: 0,
            preferred_batch: 0,
            pass: 0,
            total_passes: 0,
            bytes_good: 0,
            bytes_bad: 0,
            bytes_total_disc: 0,
            bad_ranges: Vec::new(),
            num_bad_ranges: 0,
            bad_ranges_truncated: 0,
            total_lost_ms: 0.0,
            largest_gap_ms: 0.0,
            last_error: String::new(),
            output_file: String::new(),
            tmdb_title: String::new(),
            tmdb_year: 0,
            tmdb_poster: String::new(),
            tmdb_overview: String::new(),
            duration: String::new(),
            codecs: String::new(),
            pass_progress_pct: 0,
            pass_eta: String::new(),
            total_progress_pct: 0,
            total_eta: String::new(),
            damage_severity: String::new(),
        }
    }
}

/// Compute the damage-severity badge string from autorip's RipState
/// fields. Wraps libfreemkv's `classify_damage` so the UI gets a stable
/// lowercase string ("clean" / "cosmetic" / "moderate" / "serious").
pub(crate) fn damage_severity_for(errors: u32, total_lost_ms: f64) -> String {
    let s = libfreemkv::classify_damage(errors as u64, total_lost_ms);
    serde_json::to_value(s)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

// Global state for web UI.
pub static STATE: once_cell::sync::Lazy<Mutex<std::collections::HashMap<String, RipState>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

/// Stop cooldowns: device -> epoch seconds when cooldown expires.
pub static STOP_COOLDOWNS: once_cell::sync::Lazy<Mutex<std::collections::HashMap<String, u64>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

const STOP_COOLDOWN_SECS: u64 = 5;

pub fn set_stop_cooldown(device: &str) {
    let now = crate::util::epoch_secs();
    if let Ok(mut cd) = STOP_COOLDOWNS.lock() {
        cd.insert(device.to_string(), now + STOP_COOLDOWN_SECS);
    }
}

fn is_in_cooldown(device: &str) -> bool {
    let now = crate::util::epoch_secs();
    if let Ok(cd) = STOP_COOLDOWNS.lock() {
        if let Some(&expires) = cd.get(device) {
            return now < expires;
        }
    }
    false
}

// ─── Per-device drive session ──────────────────────────────────────────────

/// Persistent drive session — survives across scan → rip transitions.
/// Dropped on eject, stop, or error.
struct DriveSession {
    drive: libfreemkv::Drive,
    disc: Option<libfreemkv::Disc>,
    scanned: bool,
    probed: bool,
    tmdb: Option<crate::tmdb::TmdbResult>,
}

/// Global drive sessions — one per device.
static SESSIONS: once_cell::sync::Lazy<Mutex<std::collections::HashMap<String, DriveSession>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

fn take_session(device: &str) -> Option<DriveSession> {
    SESSIONS.lock().ok()?.remove(device)
}

fn store_session(device: &str, session: DriveSession) {
    if let Ok(mut s) = SESSIONS.lock() {
        s.insert(device.to_string(), session);
    }
}

fn drop_session(device: &str) {
    if let Ok(mut s) = SESSIONS.lock() {
        s.remove(device);
    }
}

/// Remove every subdirectory under `cfg.staging_dir`. Used on startup (all
/// prior session state is gone, so anything still on disk is orphaned from
/// a killed process) and on user-initiated stop (stop == reset — clean
/// slate so the next rip doesn't accidentally resume stale state).
///
/// Best-effort: logs each removal, silently ignores errors on individual
/// entries. A locked or not-yet-created staging root is not fatal.
pub fn wipe_staging(staging_dir: &str) {
    let entries = match std::fs::read_dir(staging_dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        match std::fs::remove_dir_all(&path) {
            Ok(_) => tracing::info!(path = %path.display(), "wiped stale staging entry"),
            Err(e) => tracing::warn!(path = %path.display(), error = %e, "staging wipe skipped"),
        }
    }
}

// ─── Poll loop ─────────────────────────────────────────────────────────────

const POLL_INTERVAL_SECS: u64 = 5;

/// Extract the trailing path component (`sg4` from `/dev/sg4`,
/// `disk2` from `/dev/disk2`, `CdRom0` from `\\.\CdRom0`) for use as a
/// device key in autorip's state map. autorip's UI / state machine
/// keys everything by this short name; the lib gives back full
/// platform paths in `DriveInfo`.
fn device_key(path: &str) -> String {
    path.rsplit(['/', '\\']).next().unwrap_or(path).to_string()
}

/// Poll drives for disc insertion. Only triggers on state change
/// (no disc → disc present), not on disc already being there.
///
/// **Architectural note (0.13.2):** autorip is dumb — it never touches
/// hardware paths, sysfs, SCSI, or USB. The lib's `list_drives()` does
/// the platform-specific enumeration (sg/disk/CdRom paths, peripheral-
/// type filtering, INQUIRY for vendor/model). The lib's
/// `drive_has_disc(path)` does the disc-presence probe with internal
/// wedge-recovery (SCSI reset → USB reset) hidden from the caller.
/// autorip just iterates the snapshot, tracks logical state
/// (idle/scanning/ripping/cooldown), and spawns rip threads.
pub fn drive_poll_loop(cfg: &Arc<RwLock<Config>>) {
    // v0.13.17: re-enumerate drives every RESCAN_INTERVAL_SECS so an unplug
    // + replug at the kernel level (drive moves from /dev/sg4 to /dev/sg5
    // after USB re-enumeration) is picked up without a container restart.
    // Pre-0.13.17 enumeration was one-shot at startup — the user had to
    // restart the autorip container after every replug.
    const RESCAN_INTERVAL_SECS: u64 = 30;
    // Startup sweep: anything under /staging is orphaned — there are no
    // live sessions yet. A prior autorip process killed mid-rip leaves its
    // in-progress ISO / mapfile / partial MKV here, which the old
    // resume=false path still couldn't guarantee away. Wipe unconditionally
    // so the next rip always starts clean.
    if let Ok(c) = cfg.read() {
        wipe_staging(&c.staging_dir);
    }

    let initial_drives = libfreemkv::list_drives();
    let mut drive_paths: Vec<String> = initial_drives.iter().map(|d| d.path.clone()).collect();
    for d in &initial_drives {
        tracing::info!(
            device = %device_key(&d.path),
            path = %d.path,
            vendor = %d.vendor,
            model = %d.model,
            firmware = %d.firmware,
            "drive enumerated"
        );
    }
    let mut last_rescan = std::time::Instant::now();

    let mut had_disc: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Devices that have surfaced a non-recoverable error from
    // `drive_has_disc`. Used to throttle repeat warnings — first
    // failure at warn, continued failures at debug. Without this, a
    // permanently-locked or permission-denied node would spam
    // autorip.log forever.
    let mut warned_probe_fail: std::collections::HashSet<String> = std::collections::HashSet::new();

    tracing::info!(
        interval_secs = POLL_INTERVAL_SECS,
        drive_count = drive_paths.len(),
        "drive poll loop starting"
    );

    while !crate::SHUTDOWN.load(Ordering::Relaxed) {
        // v0.13.17 hot-plug: every RESCAN_INTERVAL_SECS, re-enumerate drives
        // and reconcile against the cached path list. New devices get logged
        // and start being polled. Devices that disappeared get their state
        // cleared (drop_session + remove from STATE) so the UI doesn't show
        // a phantom drive.
        if last_rescan.elapsed().as_secs() >= RESCAN_INTERVAL_SECS {
            last_rescan = std::time::Instant::now();
            let fresh = libfreemkv::list_drives();
            let fresh_paths: Vec<String> = fresh.iter().map(|d| d.path.clone()).collect();
            // Added: in fresh but not in drive_paths.
            for d in &fresh {
                if !drive_paths.contains(&d.path) {
                    tracing::info!(
                        device = %device_key(&d.path),
                        path = %d.path,
                        vendor = %d.vendor,
                        model = %d.model,
                        firmware = %d.firmware,
                        "drive enumerated (hot-plug)"
                    );
                }
            }
            // Removed: in drive_paths but not in fresh_paths.
            for path in &drive_paths {
                if !fresh_paths.contains(path) {
                    let device = device_key(path);
                    tracing::info!(device = %device, path = %path, "drive removed (hot-unplug)");
                    drop_session(&device);
                    if let Ok(mut s) = STATE.lock() {
                        s.remove(&device);
                    }
                    had_disc.remove(&device);
                    warned_probe_fail.remove(&device);
                }
            }
            drive_paths = fresh_paths;
        }

        {
            let mut current_with_disc: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            for path in &drive_paths {
                let device = device_key(path);

                // Don't touch drives that are actively scanning/ripping —
                // they hold a Drive instance + sometimes the SCSI bus.
                // Probing them mid-rip would conflict.
                if is_busy(&device) {
                    current_with_disc.insert(device);
                    continue;
                }

                // The whole hardware probe — discovery, wedge detection,
                // SCSI reset, USB reset — is one lib call. autorip sees
                // a `bool` for present/absent, or an `Err` only after
                // recovery itself failed (drive permanently bricked).
                let disc_present = match libfreemkv::drive_has_disc(std::path::Path::new(path)) {
                    Ok(p) => {
                        warned_probe_fail.remove(&device);
                        p
                    }
                    Err(e) => {
                        if warned_probe_fail.insert(device.clone()) {
                            tracing::warn!(
                                device = %device,
                                path = %path,
                                error = %e,
                                "drive_has_disc failed — drive firmware unresponsive; physical reconnect or host reboot required"
                            );
                        } else {
                            tracing::debug!(
                                device = %device,
                                error = %e,
                                "drive_has_disc still failing"
                            );
                        }
                        continue;
                    }
                };

                if !disc_present {
                    // Disc removed — clean up session
                    if had_disc.contains(&device) {
                        tracing::info!(device = %device, "disc removed");
                        drop_session(&device);
                    }
                    if !is_busy(&device) {
                        update_state(
                            &device,
                            RipState {
                                device: device.clone(),
                                status: "idle".to_string(),
                                ..Default::default()
                            },
                        );
                    }
                    continue;
                }

                current_with_disc.insert(device.clone());

                let is_new_insert = !had_disc.contains(&device);

                if is_new_insert {
                    tracing::info!(device = %device, "disc inserted");
                }

                if is_new_insert && !is_busy(&device) && !is_in_cooldown(&device) {
                    let on_insert = cfg
                        .read()
                        .ok()
                        .map(|c| c.on_insert.clone())
                        .unwrap_or_else(|| "scan".to_string());

                    if on_insert == "nothing" {
                        update_state(
                            &device,
                            RipState {
                                device: device.clone(),
                                status: "idle".to_string(),
                                disc_present: true,
                                ..Default::default()
                            },
                        );
                        continue;
                    }

                    tracing::info!(
                        device = %device,
                        on_insert = %on_insert,
                        "spawning scan/rip thread"
                    );

                    update_state(
                        &device,
                        RipState {
                            device: device.clone(),
                            status: "scanning".to_string(),
                            disc_present: true,
                            ..Default::default()
                        },
                    );

                    let cfg = cfg.clone();
                    let dev_path = path.clone();
                    let device_for_thread = device.clone();

                    if let Err(e) = spawn_rip_thread(&device, "rip", move || {
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            scan_disc(&cfg, &device_for_thread, &dev_path);
                            if on_insert == "rip" && !stop_requested(&device_for_thread) {
                                rip_disc(&cfg, &device_for_thread, &dev_path);
                            }
                        }))
                        .is_err()
                        {
                            tracing::error!(
                                device = %device_for_thread,
                                "scan/rip thread panicked"
                            );
                            crate::log::device_log(&device_for_thread, "Thread panicked");
                            drop_session(&device_for_thread);
                            update_state(
                                &device_for_thread,
                                RipState {
                                    device: device_for_thread.clone(),
                                    status: "error".to_string(),
                                    last_error: "Internal error (panic)".to_string(),
                                    ..Default::default()
                                },
                            );
                        }
                    }) {
                        tracing::warn!(
                            device = %device,
                            error = %e,
                            "failed to spawn rip thread"
                        );
                    }
                } else if !is_new_insert && !is_busy(&device) {
                    if let Ok(mut s) = STATE.lock() {
                        if let Some(rs) = s.get_mut(&device) {
                            rs.disc_present = true;
                        }
                    }
                }
            }

            had_disc = current_with_disc;
        }

        // SHUTDOWN-responsive sleep — break early on signal so SIGTERM
        // doesn't have to wait the full 5 s tick to take effect.
        for _ in 0..(POLL_INTERVAL_SECS * 10) {
            if crate::SHUTDOWN.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    tracing::info!("drive poll loop stopping");
}

pub fn is_busy(device: &str) -> bool {
    STATE
        .lock()
        .map(|s| {
            s.get(device)
                .map(|r| r.status == "scanning" || r.status == "ripping")
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

pub fn update_state(device: &str, mut state: RipState) {
    // 0.13.22: derive damage_severity from errors + total_lost_ms on
    // every push so the UI badge stays in sync with the latest counters.
    state.damage_severity = damage_severity_for(state.errors, state.total_lost_ms);
    if let Ok(mut s) = STATE.lock() {
        s.insert(device.to_string(), state);
    }
}

/// Mutate a device's RipState via a closure. **Use this** instead of
/// `update_state` when changing specific fields without wanting to wipe
/// the rest. The `..Default::default()` pattern caused at least three
/// regressions (v0.11.20 watchdog, v0.11.17 errors-on-completion, v0.12.0
/// pass-progress fields) where a "small" state push silently zeroed a
/// field the UI was rendering.
///
/// Creates a default-initialized RipState if the device isn't in the map
/// yet so the first call after boot doesn't silently no-op.
pub fn update_state_with<F: FnOnce(&mut RipState)>(device: &str, f: F) {
    if let Ok(mut s) = STATE.lock() {
        let entry = s.entry(device.to_string()).or_insert_with(|| RipState {
            device: device.to_string(),
            ..Default::default()
        });
        f(entry);
    }
}

/// Shared context for the progress callbacks of a multi-pass rip. Built once
/// before pass 1, cheaply Arc-cloned per pass so each closure captures the
/// same immutable values without reallocating every callback.
#[derive(Clone)]
struct PassContext {
    device: String,
    display_name: String,
    disc_format: String,
    tmdb_title: String,
    tmdb_year: u16,
    tmdb_poster: String,
    tmdb_overview: String,
    duration: String,
    codecs: String,
    filename: String,
    bytes_total_disc: u64,
    /// Preferred batch size (kernel-reported max sectors per CDB) — surfaced
    /// in RipState during Pass 1 / Pass 2+ so the UI shows a non-zero
    /// `preferred_batch` / `current_batch`. Pass 1 never shrinks the batch
    /// (Disc::copy uses a fixed size); current_batch == preferred_batch
    /// throughout. The DiscStream batch halver only operates during the
    /// mux phase and is reported via the direct-mode stream loop.
    batch: u16,
    /// Configured retry-pass count. Used by `push_pass_state` to estimate the
    /// total-bar workload — only `max_retries × bytes_unreadable` worth of work
    /// is queued for retry passes (not the entire pending set, which during
    /// Pass 1 is the whole disc and produced a wildly inflated total ETA).
    /// 0 = single-pass mode (no ISO, no retries, no separate mux phase).
    max_retries: u8,
}

/// Walk the title's extents to find the byte offset *within the title* for a
/// given disc LBA. Returns None if the LBA falls outside every extent — meaning
/// the bad region is in UDF metadata or some other non-AV area, where chapter
/// mapping doesn't apply.
fn byte_offset_in_title(lba: u32, title: &libfreemkv::DiscTitle) -> Option<u64> {
    let mut cumulative = 0u64;
    for ext in &title.extents {
        if lba >= ext.start_lba && lba < ext.start_lba + ext.sector_count {
            return Some(cumulative + (lba - ext.start_lba) as u64 * 2048);
        }
        cumulative += ext.sector_count as u64 * 2048;
    }
    None
}

fn range_chapter(lba: u32, title: &libfreemkv::DiscTitle) -> (Option<u32>, Option<f64>) {
    if let Some(byte_offset) = byte_offset_in_title(lba, title) {
        if let Some((ch, t)) = libfreemkv::verify::VerifyResult::chapter_at_offset(
            &title.chapters,
            byte_offset,
            title.duration_secs,
            title.size_bytes,
        ) {
            return (Some(ch as u32), Some(t));
        }
    }
    (None, None)
}

/// Build the UI's bad-range list from the mapfile. Caps at 50 entries by size
/// (largest first); returns the truncation count so the UI can say "+X more".
fn build_bad_ranges(
    map: &libfreemkv::disc::mapfile::Mapfile,
    title: &libfreemkv::DiscTitle,
    bps: f64,
) -> (Vec<BadRange>, u32, u32, f64, f64) {
    use libfreemkv::disc::mapfile::SectorStatus;
    // Only Unreadable ranges count as "bad" in the UI. NonTried = unread work,
    // NonTrimmed / NonScraped = failed pass-1 but patch hasn't confirmed yet.
    // Showing those as "bad" during pass 1 falsely implies the whole disc is
    // damaged before the library has actually given up on anything.
    let raw = map.ranges_with(&[SectorStatus::Unreadable]);
    let total_count = raw.len() as u32;
    let mut ranges: Vec<BadRange> = raw
        .iter()
        .map(|(pos, size)| {
            let lba = pos / 2048;
            let count = (size / 2048) as u32;
            let duration_ms = if bps > 0.0 {
                (*size as f64) / bps * 1000.0
            } else {
                0.0
            };
            let (chapter, time_offset_secs) = range_chapter(lba as u32, title);
            BadRange {
                lba,
                count,
                duration_ms,
                chapter,
                time_offset_secs,
            }
        })
        .collect();
    ranges.sort_by(|a, b| {
        b.duration_ms
            .partial_cmp(&a.duration_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let total_lost_ms: f64 = ranges.iter().map(|r| r.duration_ms).sum();
    let largest_gap_ms = ranges.first().map(|r| r.duration_ms).unwrap_or(0.0);
    let truncated = ranges.len().saturating_sub(50) as u32;
    ranges.truncate(50);
    (
        ranges,
        total_count,
        truncated,
        total_lost_ms,
        largest_gap_ms,
    )
}

/// Per-pass speed tracker — bytes delta / wall-clock delta between progress
/// callbacks, smoothed exponentially. Held in a RefCell inside the callback
/// closure so interior mutability keeps the closure `Fn`.
#[derive(Debug)]
struct PassProgressState {
    /// Previous sample. `None` until the first `observe` call — required
    /// because priming the smoothed speed with the first sample would capture
    /// all bytes already copied (from resume, or from pre-throttle-window
    /// reads) over a short/arbitrary dt, producing absurd speeds like 2 GB/s
    /// that then decay very slowly toward the real rate.
    prev: Option<(std::time::Instant, u64)>,
    smooth_speed_mbs: f64,
    /// Wall-clock of the last throttled callback. The progress closure
    /// checks this to skip work when less than 1.5 s have passed.
    last_update: std::time::Instant,
    /// Wall-clock of the last device-log line emitted from this pass.
    last_log: std::time::Instant,
    /// Last `work_done` reported by libfreemkv's `Progress` trait — bytes
    /// processed in this pass so far. Drives `pass_progress_pct`.
    last_work_done: u64,
    /// Last `work_total` reported by libfreemkv's `Progress` trait — total
    /// bytes this pass will process. Drives `pass_progress_pct` denominator.
    last_work_total: u64,
}

impl PassProgressState {
    fn new() -> Self {
        let now = std::time::Instant::now();
        Self {
            prev: None,
            smooth_speed_mbs: 0.0,
            last_update: now,
            last_log: now,
            last_work_done: 0,
            last_work_total: 0,
        }
    }

    /// Feed a fresh sample. Returns the smoothed speed in MB/s.
    ///
    /// First call returns 0 and just records the sample (no prior dt to
    /// compute a delta against). Subsequent calls compute an instantaneous
    /// rate, cap it at a sanity ceiling (1 GB/s — 10× any real BD drive), and
    /// mix it into the smoothed value with alpha=0.3.
    fn observe(&mut self, now: std::time::Instant, bytes_good: u64) -> f64 {
        match self.prev {
            None => {
                self.prev = Some((now, bytes_good));
                0.0
            }
            Some((prev_t, prev_b)) => {
                let dt = now.duration_since(prev_t).as_secs_f64();
                if dt <= 0.0 {
                    return self.smooth_speed_mbs;
                }
                let delta_bytes = bytes_good.saturating_sub(prev_b);
                let instant = delta_bytes as f64 / 1_048_576.0 / dt;
                // Cap wild samples — real optical drives top out around
                // 70–140 MB/s. 1 GB/s is an order of magnitude above anything
                // physical, so anything larger is a measurement artifact
                // (callback jitter, mapfile replay, etc.) and should be
                // dropped, not smoothed in.
                let instant = instant.min(1024.0);
                self.prev = Some((now, bytes_good));
                self.smooth_speed_mbs = if self.smooth_speed_mbs < 0.01 {
                    instant
                } else {
                    0.7 * self.smooth_speed_mbs + 0.3 * instant
                };
                self.smooth_speed_mbs
            }
        }
    }
}

/// Read the live mapfile and push a fresh RipState snapshot for the current
/// pass. Computes smoothed speed + ETA from successive bytes_good samples —
/// otherwise the UI shows 0 KB/s through the whole rip since the main
/// stream loop's speed tracker isn't running during `Disc::copy` / `patch`.
/// No-op (quietly) if the mapfile can't be read — the next callback will
/// try again.
fn push_pass_state(
    ctx: &PassContext,
    title: &libfreemkv::DiscTitle,
    bps: f64,
    mapfile_path: &std::path::Path,
    pass: u8,
    total_passes: u8,
    state: &std::cell::RefCell<PassProgressState>,
) {
    let map = match libfreemkv::disc::mapfile::Mapfile::load(mapfile_path) {
        Ok(m) => m,
        Err(_) => return,
    };
    let stats = map.stats();
    let (ranges, total_count, truncated, total_lost_ms, largest_gap_ms) =
        build_bad_ranges(&map, title, bps);
    // `bytes_bad` is only `Unreadable` — confirmed-bad ranges where the drive
    // has exhausted our retry attempts. `NonTried` (unread) and `NonTrimmed` /
    // `NonScraped` (needs more work) are not "bad" — counting them makes the
    // UI show the whole disc as bad until pass 1 reaches each byte.
    let bytes_bad = stats.bytes_unreadable;
    // Live `errors` (skipped-sector count) from the mapfile so the yellow
    // "N sectors skipped" banner + lost-video-time readout works in multipass
    // mode. In direct mode this comes from `input.errors`; we overwrite it in
    // the main stream loop for that path.
    let errors = (bytes_bad / 2048) as u32;
    // v0.13.16: pass_progress_pct = work_done / work_total (per-pass).
    // The legacy progress_pct stays populated as a copy (back-compat for
    // any consumer reading the old field).
    let last_pos = state.borrow().last_work_done;
    let last_work_total = state.borrow().last_work_total;
    let pass_pct = if last_work_total > 0 {
        (last_pos * 100 / last_work_total).min(100) as u8
    } else {
        0
    };
    // Total bar: estimate cumulative work done across all passes.
    //
    // The retry passes (2..N) only re-read the *bad* set (`bytes_unreadable`),
    // not everything that was pending at the start of Pass 1. Using
    // `bytes_pending` here was wrong: at the start of Pass 1 the entire disc
    // is "pending," so the old formula computed total ≈ 6 × capacity and
    // the total bar showed Pass 1 as ~16% instead of ~50%.
    //
    //   total_work = capacity (Pass 1)
    //              + max_retries × bytes_unreadable (retry passes, shrinks Pass→Pass)
    //              + mux_estimate (only when there's an ISO intermediate)
    //
    // In single-pass mode (max_retries == 0) there is no ISO, no retry passes,
    // and no separate mux phase, so total_work simplifies to just capacity.
    let cfg_max_retries = ctx.max_retries as u64;
    let mux_estimate_bytes = if cfg_max_retries > 0 {
        ctx.bytes_total_disc // mux re-reads the ISO, ~1× capacity worth of I/O
    } else {
        0
    };
    let total_work_estimated = ctx
        .bytes_total_disc
        .saturating_add(cfg_max_retries.saturating_mul(bytes_bad))
        .saturating_add(mux_estimate_bytes);
    // Cumulative work done across all passes:
    //   pass 1: total_done = last_pos
    //   pass>=2 (retry): total_done = capacity + (pass-2) × bytes_bad + last_pos
    let total_done: u64 = if pass <= 1 {
        last_pos
    } else {
        let prior_retry_count = pass.saturating_sub(2) as u64;
        ctx.bytes_total_disc
            .saturating_add(prior_retry_count.saturating_mul(bytes_bad))
            .saturating_add(last_pos)
    };
    let total_pct = if total_work_estimated > 0 {
        (total_done * 100 / total_work_estimated).min(100) as u8
    } else {
        0
    };
    // Legacy field — keep populated for back-compat. Equals pass_pct.
    let pct = pass_pct;

    // Speed = rate of `last_pos` (work_done) advancement, NOT bytes_good.
    // v0.13.15 had this wrong: speed_mbs tracked bytes_good rate, so during
    // skip-forward zones (where work_done advances but bytes_good is frozen)
    // speed read 0 even though the bar was moving. Now speed reflects what
    // the bar shows.
    let (speed_mbs, pass_eta_str, total_eta_str) = {
        let mut s = state.borrow_mut();
        let now = std::time::Instant::now();
        let speed = s.observe(now, last_pos);
        s.last_update = now;
        let format_secs = |secs: u64| -> String {
            if secs < 60 {
                format!("{}s", secs)
            } else if secs < 3600 {
                format!("{}:{:02}", secs / 60, secs % 60)
            } else if secs < 360_000 {
                format!("{}:{:02}:{:02}", secs / 3600, (secs % 3600) / 60, secs % 60)
            } else {
                String::new()
            }
        };
        let pass_eta = if speed > 0.01 && last_work_total > last_pos {
            let rem_mb = (last_work_total - last_pos) as f64 / 1_048_576.0;
            format_secs((rem_mb / speed) as u64)
        } else {
            String::new()
        };
        let total_eta = if speed > 0.01 && total_work_estimated > total_done {
            let rem_mb = (total_work_estimated - total_done) as f64 / 1_048_576.0;
            format_secs((rem_mb / speed) as u64)
        } else {
            String::new()
        };
        (speed, pass_eta, total_eta)
    };
    // Back-compat: legacy `eta` mirrors pass_eta.
    let eta = pass_eta_str.clone();

    update_state(
        &ctx.device,
        RipState {
            device: ctx.device.clone(),
            status: "ripping".to_string(),
            disc_present: true,
            disc_name: ctx.display_name.clone(),
            disc_format: ctx.disc_format.clone(),
            progress_pct: pct,
            progress_gb: last_pos as f64 / 1_073_741_824.0,
            speed_mbs,
            eta,
            errors,
            lost_video_secs: total_lost_ms / 1000.0,
            output_file: ctx.filename.clone(),
            tmdb_title: ctx.tmdb_title.clone(),
            tmdb_year: ctx.tmdb_year,
            tmdb_poster: ctx.tmdb_poster.clone(),
            tmdb_overview: ctx.tmdb_overview.clone(),
            duration: ctx.duration.clone(),
            codecs: ctx.codecs.clone(),
            pass,
            total_passes,
            bytes_good: stats.bytes_good,
            bytes_bad,
            bytes_total_disc: ctx.bytes_total_disc,
            bad_ranges: ranges,
            num_bad_ranges: total_count,
            bad_ranges_truncated: truncated,
            total_lost_ms,
            largest_gap_ms,
            preferred_batch: ctx.batch,
            current_batch: ctx.batch,
            pass_progress_pct: pass_pct,
            pass_eta: pass_eta_str,
            total_progress_pct: total_pct,
            total_eta: total_eta_str,
            ..Default::default()
        },
    );

    // Periodic device-log line so a long pass doesn't go silent. Matches the
    // 60 s cadence the main stream loop uses in direct mode. Reports
    // SWEPT position (pos) prominently — that's what advances during a
    // skip-forward bad zone — and shows real-data-recovered (bytes_good)
    // separately so users can see clean-data progress vs sweep progress.
    {
        let mut s = state.borrow_mut();
        if s.last_log.elapsed().as_secs() >= 60 {
            s.last_log = std::time::Instant::now();
            let pos_gb = last_pos as f64 / 1_073_741_824.0;
            let good_gb = stats.bytes_good as f64 / 1_073_741_824.0;
            let total_gb = ctx.bytes_total_disc as f64 / 1_073_741_824.0;
            let speed_str = if speed_mbs >= 1.0 {
                format!("{speed_mbs:.1} MB/s")
            } else {
                format!("{:.0} KB/s", speed_mbs * 1024.0)
            };
            let bad_str = if bytes_bad > 0 {
                format!(
                    ", {} skipped ({:.2} MB)",
                    errors,
                    bytes_bad as f64 / 1_048_576.0
                )
            } else {
                String::new()
            };
            crate::log::device_log(
                &ctx.device,
                &format!(
                    "Pass {pass}/{total_passes}: swept {:.1} GB / {:.1} GB ({}%), good {:.1} GB, {}{}",
                    pos_gb, total_gb, pct, good_gb, speed_str, bad_str
                ),
            );
        }
    }
}

/// Build a RipState snapshot for a multi-pass rip in a specific pass, with
/// everything the UI needs to render pass progress. Status is always "ripping"
/// during the passes; pass=total_passes indicates the mux phase.
#[allow(clippy::too_many_arguments)]
fn set_pass_progress(
    device: &str,
    display_name: &str,
    disc_format: &str,
    tmdb_title: &str,
    tmdb_year: u16,
    tmdb_poster: &str,
    tmdb_overview: &str,
    duration: &str,
    codecs: &str,
    filename: &str,
    pass: u8,
    total_passes: u8,
    bytes_good: u64,
    bytes_bad: u64,
    bytes_total_disc: u64,
    batch: u16,
) {
    let pct = if bytes_total_disc > 0 {
        (bytes_good * 100 / bytes_total_disc).min(100) as u8
    } else {
        0
    };
    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "ripping".to_string(),
            disc_present: true,
            disc_name: display_name.to_string(),
            disc_format: disc_format.to_string(),
            progress_pct: pct,
            progress_gb: bytes_good as f64 / 1_073_741_824.0,
            output_file: filename.to_string(),
            tmdb_title: tmdb_title.to_string(),
            tmdb_year,
            tmdb_poster: tmdb_poster.to_string(),
            tmdb_overview: tmdb_overview.to_string(),
            duration: duration.to_string(),
            codecs: codecs.to_string(),
            pass,
            total_passes,
            bytes_good,
            bytes_bad,
            bytes_total_disc,
            preferred_batch: batch,
            current_batch: batch,
            ..Default::default()
        },
    );
}

// ─── Scan ──────────────────────────────────────────────────────────────────

/// Scan a disc — open, init, identify, TMDB, full scan. Stores session for rip.
pub fn scan_disc(cfg: &Arc<RwLock<Config>>, device: &str, device_path: &str) {
    let cfg_read = match cfg.read() {
        Ok(c) => c,
        Err(_) => return,
    };

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "scanning".to_string(),
            disc_present: true,
            ..Default::default()
        },
    );

    crate::log::archive_device_log(device);
    crate::log::device_log(device, "Opening drive...");

    let mut drive = match libfreemkv::Drive::open(std::path::Path::new(device_path)) {
        Ok(d) => d,
        Err(e) => {
            crate::log::device_log(device, &format!("Cannot open drive: {}", e));
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "error".to_string(),
                    last_error: format!("{}", e),
                    ..Default::default()
                },
            );
            return;
        }
    };
    if let Err(e) = drive.wait_ready() {
        tracing::warn!(device = %device, error = %e, "drive wait_ready failed (continuing)");
    }
    crate::log::device_log(device, "Initializing...");
    if let Err(e) = drive.init() {
        tracing::warn!(device = %device, error = %e, "drive init failed (continuing — scan may degrade)");
    }

    // Fast identify — disc name only, no playlists
    crate::log::device_log(device, "Identifying disc...");
    let disc_id = match libfreemkv::Disc::identify(&mut drive) {
        Ok(id) => id,
        Err(e) => {
            crate::log::device_log(device, &format!("Identify failed: {}", e));
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "error".to_string(),
                    last_error: format!("{}", e),
                    ..Default::default()
                },
            );
            return;
        }
    };

    let id_name = disc_id.name().to_string();

    crate::log::device_log(device, &format!("Disc: {}", id_name));

    // TMDB lookup — fast, user sees poster while full scan runs
    let tmdb = crate::tmdb::lookup(&crate::tmdb::clean_title(&id_name), &cfg_read.tmdb_api_key);
    let display_name = tmdb
        .as_ref()
        .map(|t| t.title.clone())
        .unwrap_or_else(|| id_name.clone());

    // Show identify results immediately — no format badge until full scan confirms UHD vs BD
    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "scanning".to_string(),
            disc_present: true,
            disc_name: display_name.clone(),
            disc_format: String::new(),
            tmdb_title: tmdb.as_ref().map(|t| t.title.clone()).unwrap_or_default(),
            tmdb_year: tmdb.as_ref().map(|t| t.year).unwrap_or(0),
            tmdb_poster: tmdb
                .as_ref()
                .map(|t| t.poster_url.clone())
                .unwrap_or_default(),
            tmdb_overview: tmdb
                .as_ref()
                .map(|t| t.overview.clone())
                .unwrap_or_default(),
            ..Default::default()
        },
    );

    // Full scan — titles, streams, AACS keys
    crate::log::device_log(device, "Scanning titles...");
    let scan_opts = match &cfg_read.keydb_path {
        Some(p) => libfreemkv::ScanOptions {
            keydb_path: Some(p.into()),
        },
        None => libfreemkv::ScanOptions::default(),
    };
    let disc = match libfreemkv::Disc::scan(&mut drive, &scan_opts) {
        Ok(d) => d,
        Err(e) => {
            crate::log::device_log(device, &format!("Scan failed: {}", e));
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "error".to_string(),
                    last_error: format!("{}", e),
                    ..Default::default()
                },
            );
            return;
        }
    };

    // Update format from full scan (UHD vs BD now known)
    let disc_name = disc
        .meta_title
        .as_deref()
        .unwrap_or(&disc.volume_id)
        .to_string();
    let disc_format = match disc.format {
        libfreemkv::DiscFormat::Uhd => "uhd",
        libfreemkv::DiscFormat::BluRay => "bluray",
        libfreemkv::DiscFormat::Dvd => "dvd",
        libfreemkv::DiscFormat::Unknown => "unknown",
    }
    .to_string();

    crate::log::device_log(
        device,
        &format!(
            "Scanned: {} ({}, {} titles)",
            disc_name,
            disc_format,
            disc.titles.len()
        ),
    );

    // Extract title info before storing session
    let duration = disc
        .titles
        .first()
        .map(|t| crate::util::format_duration_hm(t.duration_secs))
        .unwrap_or_default();
    let codecs = disc.titles.first().map(format_codecs).unwrap_or_default();

    // Store session — drive stays open for rip
    store_session(
        device,
        DriveSession {
            drive,
            disc: Some(disc),
            scanned: true,
            probed: false,
            tmdb: tmdb.clone(),
        },
    );

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "idle".to_string(),
            disc_present: true,
            disc_name: display_name,
            disc_format,
            tmdb_title: tmdb.as_ref().map(|t| t.title.clone()).unwrap_or_default(),
            tmdb_year: tmdb.as_ref().map(|t| t.year).unwrap_or(0),
            tmdb_poster: tmdb
                .as_ref()
                .map(|t| t.poster_url.clone())
                .unwrap_or_default(),
            tmdb_overview: tmdb
                .as_ref()
                .map(|t| t.overview.clone())
                .unwrap_or_default(),
            duration,
            codecs,
            ..Default::default()
        },
    );
}

// ─── Rip ───────────────────────────────────────────────────────────────────

/// Rip a disc. Reuses the existing drive session from scan_disc.
/// If no session exists, opens fresh (for on_insert=rip).
pub fn rip_disc(cfg: &Arc<RwLock<Config>>, device: &str, device_path: &str) {
    reset_stop_flag(device);

    // Archive the previous rip's per-device log so the live log only
    // shows events from the current attempt. Mirrors what scan_disc
    // does; previously rip_disc was missing this so a stop -> rip
    // cycle left "Stop requested..." / "Pass 1 cancelled" lines from
    // the prior run mixed into the new one.
    crate::log::archive_device_log(device);

    let cfg_read = match cfg.read() {
        Ok(c) => c,
        Err(_) => return,
    };

    // Preserve UI state
    let prev = STATE.lock().ok().and_then(|s| s.get(device).cloned());
    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "scanning".to_string(),
            disc_present: true,
            disc_name: prev
                .as_ref()
                .map(|p| p.disc_name.clone())
                .unwrap_or_default(),
            disc_format: prev
                .as_ref()
                .map(|p| p.disc_format.clone())
                .unwrap_or_default(),
            tmdb_title: prev
                .as_ref()
                .map(|p| p.tmdb_title.clone())
                .unwrap_or_default(),
            tmdb_year: prev.as_ref().map(|p| p.tmdb_year).unwrap_or(0),
            tmdb_poster: prev
                .as_ref()
                .map(|p| p.tmdb_poster.clone())
                .unwrap_or_default(),
            tmdb_overview: prev
                .as_ref()
                .map(|p| p.tmdb_overview.clone())
                .unwrap_or_default(),
            ..Default::default()
        },
    );

    // Take the existing session, or open fresh
    let mut session = match take_session(device) {
        Some(s) if s.scanned => {
            crate::log::device_log(device, "Reusing drive session");
            s
        }
        existing => {
            // No session or not scanned — open fresh
            if existing.is_some() {
                drop_session(device);
            }
            crate::log::device_log(device, "Opening drive...");
            let mut drive = match libfreemkv::Drive::open(std::path::Path::new(device_path)) {
                Ok(d) => d,
                Err(e) => {
                    let msg = format!("Cannot open drive: {}", e);
                    crate::log::device_log(device, &msg);
                    update_state(
                        device,
                        RipState {
                            device: device.to_string(),
                            status: "error".to_string(),
                            last_error: msg,
                            ..Default::default()
                        },
                    );
                    return;
                }
            };
            if let Err(e) = drive.wait_ready() {
                tracing::warn!(device = %device, error = %e, "drive wait_ready failed (continuing)");
            }
            crate::log::device_log(device, "Initializing...");
            if let Err(e) = drive.init() {
                tracing::warn!(device = %device, error = %e, "drive init failed (continuing)");
            }

            let scan_opts = match &cfg_read.keydb_path {
                Some(p) => libfreemkv::ScanOptions {
                    keydb_path: Some(p.into()),
                },
                None => libfreemkv::ScanOptions::default(),
            };
            crate::log::device_log(device, "Scanning titles...");
            let disc = match libfreemkv::Disc::scan(&mut drive, &scan_opts) {
                Ok(d) => d,
                Err(e) => {
                    let msg = format!("Scan failed: {}", e);
                    crate::log::device_log(device, &msg);
                    update_state(
                        device,
                        RipState {
                            device: device.to_string(),
                            status: "error".to_string(),
                            last_error: msg,
                            ..Default::default()
                        },
                    );
                    return;
                }
            };

            let disc_name = disc
                .meta_title
                .as_deref()
                .unwrap_or(&disc.volume_id)
                .to_string();

            let tmdb = crate::tmdb::lookup(
                &crate::tmdb::clean_title(&disc_name),
                &cfg_read.tmdb_api_key,
            );

            DriveSession {
                drive,
                disc: Some(disc),
                scanned: true,
                probed: false,
                tmdb,
            }
        }
    };

    let disc = match session.disc.take() {
        Some(d) => d,
        None => {
            tracing::error!(
                device = %device,
                "DriveSession had no disc — every code path that builds a session must set Some(disc); reaching this branch is a logic bug"
            );
            crate::log::device_log(device, "Internal error: session has no disc");
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "error".to_string(),
                    last_error: "Internal error: session has no disc".to_string(),
                    ..Default::default()
                },
            );
            drop_session(device);
            return;
        }
    };

    let disc_name = disc
        .meta_title
        .as_deref()
        .unwrap_or(&disc.volume_id)
        .to_string();
    let disc_format = match disc.format {
        libfreemkv::DiscFormat::Uhd => "uhd",
        libfreemkv::DiscFormat::BluRay => "bluray",
        libfreemkv::DiscFormat::Dvd => "dvd",
        libfreemkv::DiscFormat::Unknown => "unknown",
    }
    .to_string();
    // Pass 1 reads the WHOLE DISC (not a single title), so the total must be
    // disc.capacity_bytes — using titles[0].size_bytes (the chosen movie's
    // duration-weighted size estimate) was the v0.13.12 bug that made the UI
    // show "0.0 GB / 0.0 GB" during Pass 1. Mux phase below already
    // re-derives its own total from the input stream, so we don't lose that.
    let total_bytes = if disc.capacity_bytes > 0 {
        disc.capacity_bytes
    } else {
        disc.titles.first().map(|t| t.size_bytes).unwrap_or(0)
    };

    let tmdb = &session.tmdb;
    let tmdb_title = tmdb.as_ref().map(|t| t.title.clone()).unwrap_or_default();
    let tmdb_year = tmdb.as_ref().map(|t| t.year).unwrap_or(0);
    let tmdb_poster = tmdb
        .as_ref()
        .map(|t| t.poster_url.clone())
        .unwrap_or_default();
    let tmdb_overview = tmdb
        .as_ref()
        .map(|t| t.overview.clone())
        .unwrap_or_default();
    // Cloned for use in the finalize block (history record) — after multipass
    // we drop `session` to release the drive, so we can't borrow session.tmdb
    // at the tail of this function.
    let tmdb_media_type = tmdb
        .as_ref()
        .map(|t| t.media_type.clone())
        .unwrap_or_else(|| "unknown".to_string());

    let display_name = if tmdb_title.is_empty() {
        disc_name.clone()
    } else {
        tmdb_title.clone()
    };

    crate::log::device_log(
        device,
        &format!(
            "Disc: {} ({}, {} titles)",
            disc_name,
            disc_format,
            disc.titles.len()
        ),
    );

    if disc.titles.is_empty() {
        crate::log::device_log(device, "No titles found");
        update_state(
            device,
            RipState {
                device: device.to_string(),
                status: "error".to_string(),
                last_error: "No titles".to_string(),
                ..Default::default()
            },
        );
        return;
    }

    let duration = crate::util::format_duration_hm(disc.titles[0].duration_secs);
    let codecs = format_codecs(&disc.titles[0]);
    let title = disc.titles[0].clone();
    let keys = disc.decrypt_keys();

    if disc.encrypted && matches!(keys, libfreemkv::decrypt::DecryptKeys::None) {
        let msg = "Disc is encrypted but no decryption keys found (check KEYDB)";
        crate::log::device_log(device, msg);
        update_state(
            device,
            RipState {
                device: device.to_string(),
                status: "error".to_string(),
                last_error: msg.to_string(),
                disc_name: display_name,
                disc_format,
                tmdb_title,
                tmdb_year,
                tmdb_poster,
                tmdb_overview,
                ..Default::default()
            },
        );
        return;
    }

    // Probe for speed — only needed for rip, not scan
    if !session.probed {
        crate::log::device_log(device, "Probing disc speed...");
        let _ = session.drive.probe_disc();
        session.probed = true;
    }

    let batch = libfreemkv::disc::detect_max_batch_sectors(device_path);
    let format = disc.content_format;

    let output_format = cfg_read.output_format.clone();
    let ext = match output_format.as_str() {
        "m2ts" => "m2ts",
        _ => "mkv",
    };

    let staging = cfg_read.staging_device_dir(&crate::util::sanitize_path_compact(&display_name));
    let _ = std::fs::create_dir_all(&staging);
    let filename = format!(
        "{}.{}",
        crate::util::sanitize_path_compact(&display_name),
        ext
    );
    let output_path = format!("{}/{}", staging, filename);
    let dest_url = if output_format == "network" && !cfg_read.network_target.is_empty() {
        format!("network://{}", cfg_read.network_target)
    } else {
        format!("{}://{}", ext, output_path)
    };

    crate::log::device_log(device, &format!("Ripping {} to {}", display_name, filename));

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "ripping".to_string(),
            disc_present: true,
            disc_name: display_name.clone(),
            disc_format: disc_format.clone(),
            output_file: filename.clone(),
            tmdb_title: tmdb_title.clone(),
            tmdb_year,
            tmdb_poster: tmdb_poster.clone(),
            tmdb_overview: tmdb_overview.clone(),
            duration: duration.clone(),
            codecs: codecs.clone(),
            ..Default::default()
        },
    );

    // Per-title bitrate for lost-video-time math. Falls back to 66 Mbps
    // (sustained BD) if the scanner didn't populate size_bytes/duration.
    let title_bytes_per_sec: f64 = {
        let b = title.size_bytes as f64;
        let d = title.duration_secs;
        if b > 0.0 && d > 0.0 {
            b / d
        } else {
            8_250_000.0
        }
    };

    // Shared state read by event callbacks (no &mut self) and the main
    // rip loop (which copies atomics into RipState every ~1s). The watchdog
    // timestamp is updated on ANY sector-level event — not just frame writes —
    // so a long run of skipped sectors doesn't falsely register as "stalled".
    let wd_last_frame = Arc::new(AtomicU64::new(crate::util::epoch_secs()));
    let latest_bytes_read = Arc::new(AtomicU64::new(0));
    let rip_last_lba = Arc::new(AtomicU64::new(0));
    let rip_current_batch = Arc::new(AtomicU16::new(batch));

    // Create PES stream — same drive session, no re-open
    let halt = session.drive.halt_flag();
    register_halt(device, halt.clone());

    // Per-pass wallclock budget. Each pass (Pass 1 sweep + every retry) gets
    // its own `max(disc_runtime, 1h)` budget. v0.13.15 made this per-pass
    // (was total-rip in v0.13.12-14) — a fail-safe to bound a wedged pass,
    // not a bound on the whole rip. If ANY pass exceeds its budget the rip
    // ends with status=error (see cap_fired_any tracking below).
    const MIN_PASS_BUDGET_SECS: u64 = 3600;
    let chosen_runtime_secs: u64 = title.duration_secs.max(0.0) as u64;
    let max_pass_secs = chosen_runtime_secs.max(MIN_PASS_BUDGET_SECS);
    struct WallclockGuard(Arc<AtomicBool>);
    impl Drop for WallclockGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Relaxed);
        }
    }
    // Fires per-pass watcher. Returns a guard that, on drop, stops the
    // watcher thread. While alive: forwards user_halt → pass_halt; fires
    // cap_fired (and pass_halt) when wall-clock exceeds max_secs; writes
    // a per-pass `last_error` for UI surfacing.
    fn spawn_pass_watcher(
        pass_label: String,
        device: String,
        pass_halt: Arc<AtomicBool>,
        user_halt: Arc<AtomicBool>,
        cap_fired: Arc<AtomicBool>,
        max_secs: u64,
    ) -> WallclockGuard {
        let active = Arc::new(AtomicBool::new(true));
        let active_for_watcher = active.clone();
        let pass_start = std::time::Instant::now();
        std::thread::spawn(move || {
            while active_for_watcher.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_secs(2));
                if !active_for_watcher.load(Ordering::Relaxed) {
                    return;
                }
                if user_halt.load(Ordering::Relaxed) {
                    pass_halt.store(true, Ordering::Relaxed);
                    return;
                }
                if pass_halt.load(Ordering::Relaxed) {
                    return;
                }
                if pass_start.elapsed().as_secs() > max_secs {
                    let hrs = max_secs / 3600;
                    let mins = (max_secs % 3600) / 60;
                    let budget_str = if mins > 0 {
                        format!("{}h {:02}m", hrs, mins)
                    } else {
                        format!("{}h", hrs)
                    };
                    crate::log::device_log(
                        &device,
                        &format!(
                            "{} exceeded {} budget; halting pass",
                            pass_label, budget_str
                        ),
                    );
                    cap_fired.store(true, Ordering::Relaxed);
                    pass_halt.store(true, Ordering::Relaxed);
                    update_state_with(&device, |s| {
                        s.last_error = format!("{} exceeded {} budget", pass_label, budget_str);
                    });
                    return;
                }
            }
        });
        WallclockGuard(active)
    }
    // True if ANY pass cap-fired during this rip. v0.13.15: when true, mux
    // is skipped and status=error; ISO is retained in staging for manual
    // salvage. False = all passes completed naturally → mux normally.
    let cap_fired_any = Arc::new(AtomicBool::new(false));
    // The user-stop halt — the existing flag. Pass-specific halts forward
    // from this via spawn_pass_watcher. Renamed locally for clarity.
    let user_halt = halt.clone();

    let dev_for_events = device.to_string();
    let wdf_drive = wd_last_frame.clone();
    let lbr_drive = latest_bytes_read.clone();
    session.drive.on_event(move |event| {
        // Any drive-level event means something is happening — reset the
        // watchdog so the "stalled" timer doesn't monotonically climb
        // while the library is working through recovery.
        wdf_drive.store(crate::util::epoch_secs(), Ordering::Relaxed);
        match event.kind {
            libfreemkv::event::EventKind::BytesRead { bytes, .. } => {
                lbr_drive.store(bytes, Ordering::Relaxed);
            }
            libfreemkv::event::EventKind::ReadError { sector, .. } => {
                crate::log::device_log(
                    &dev_for_events,
                    &format!("Read error at sector {}", sector),
                );
            }
            _ => {}
        }
    });
    // Multi-pass vs direct flow.
    //
    // When max_retries > 0, we go through an ISO intermediate: Disc::copy writes
    // the disc to an ISO (fast skip-forward on failure, ddrescue-style mapfile),
    // then Disc::patch retries the bad ranges up to max_retries times, then the
    // mux pipeline reads from the ISO (no drive involvement past this point).
    //
    // When max_retries == 0, we keep the existing direct disc→MKV flow —
    // session.drive is passed to DiscStream::new and sectors stream straight
    // through decrypt/demux/mux. Fastest path, no ISO overhead, but no retry.
    let reader: Box<dyn libfreemkv::SectorReader> = if cfg_read.max_retries > 0 {
        let iso_filename = format!("{}.iso", crate::util::sanitize_path_compact(&display_name));
        let iso_path_str = format!("{}/{}", staging, iso_filename);
        let iso_path = std::path::Path::new(&iso_path_str);
        let total_passes = cfg_read.max_retries + 2; // pass 1 + retries + mux
        let bytes_total_disc = (session.drive.read_capacity().unwrap_or(0) as u64) * 2048;

        // Shared pass context + title reference for progress callbacks.
        let pass_ctx = PassContext {
            device: device.to_string(),
            display_name: display_name.clone(),
            disc_format: disc_format.clone(),
            tmdb_title: tmdb_title.clone(),
            tmdb_year,
            tmdb_poster: tmdb_poster.clone(),
            tmdb_overview: tmdb_overview.clone(),
            duration: duration.clone(),
            codecs: codecs.clone(),
            filename: filename.clone(),
            batch,
            bytes_total_disc,
            max_retries: cfg_read.max_retries,
        };
        let title_for_progress = title.clone();
        let mapfile_path_str = format!("{iso_path_str}.mapfile");
        let bps_progress = title_bytes_per_sec;

        // Pass 1: disc → ISO (fast sweep, skip-forward on failure).
        let pass_label = format!("Pass 1/{total_passes}: disc → ISO");
        crate::log::device_log(device, &pass_label);
        set_pass_progress(
            device,
            &display_name,
            &disc_format,
            &tmdb_title,
            tmdb_year,
            &tmdb_poster,
            &tmdb_overview,
            &duration,
            &codecs,
            &filename,
            1,
            total_passes,
            0,
            0,
            bytes_total_disc,
            batch,
        );

        // Progress callback — runs every read block (~64 KB). Throttle the
        // mapfile re-read + state push to once every 1.5 s so we don't pound
        // the mutex or the filesystem. State tracker holds last-sample
        // timestamp + bytes for speed/ETA calc.
        let pass1_state = std::cell::RefCell::new(PassProgressState::new());
        let pass1_ctx = &pass_ctx;
        let pass1_title = &title_for_progress;
        let pass1_map = std::path::Path::new(&mapfile_path_str);
        let pass1_progress = |p: &libfreemkv::progress::PassProgress| {
            // Stash work_done for push_pass_state to compute pass progress.
            pass1_state.borrow_mut().last_work_done = p.work_done;
            pass1_state.borrow_mut().last_work_total = p.work_total;
            // Throttle: only re-read mapfile + push state every 1.5s.
            if pass1_state.borrow().last_update.elapsed().as_millis() < 1500 {
                return;
            }
            push_pass_state(
                pass1_ctx,
                pass1_title,
                bps_progress,
                pass1_map,
                1,
                total_passes,
                &pass1_state,
            );
        };

        // Every rip starts fresh — no silent resume of stale ISO+mapfile from
        // a prior attempt. Pass 1 with resume=false wipes the sidecar mapfile
        // and recreates the ISO, so progress starts at 0 % and reflects reads
        // from this invocation only. See CHANGELOG 0.12.5 for the bug this
        // closes (stale mapfile showing 30 % at 10 s into a cold rip).
        let pass1_halt = Arc::new(AtomicBool::new(false));
        let _pass1_guard = spawn_pass_watcher(
            "Pass 1".to_string(),
            device.to_string(),
            pass1_halt.clone(),
            user_halt.clone(),
            cap_fired_any.clone(),
            max_pass_secs,
        );
        let copy_opts = libfreemkv::disc::CopyOptions {
            decrypt: false,
            resume: false,
            batch_sectors: Some(batch),
            skip_on_error: true,
            skip_forward: true,
            halt: Some(pass1_halt.clone()),
            progress: Some(&pass1_progress),
        };
        let result = match disc.copy(&mut session.drive, iso_path, &copy_opts) {
            Ok(r) => r,
            Err(e) => {
                // If halt is set, the error is the user-initiated stop racing
                // the worker (e.g. handle_stop wiped staging while disc.copy
                // was mid-write). Don't surface as "error" — handle_stop has
                // already cleared state to idle. Just clean up and exit.
                if halt.load(Ordering::Relaxed) {
                    crate::log::device_log(device, &format!("Pass 1 cancelled (halt): {e}"));
                    if let Ok(mut flags) = HALT_FLAGS.lock() {
                        flags.remove(device);
                    }
                    return;
                }
                crate::log::device_log(device, &format!("Pass 1 failed: {e}"));
                update_state(
                    device,
                    RipState {
                        device: device.to_string(),
                        status: "error".to_string(),
                        disc_present: true,
                        last_error: format!("{e}"),
                        disc_name: display_name,
                        disc_format,
                        tmdb_title,
                        tmdb_year,
                        tmdb_poster,
                        tmdb_overview,
                        duration,
                        codecs,
                        ..Default::default()
                    },
                );
                if let Ok(mut flags) = HALT_FLAGS.lock() {
                    flags.remove(device);
                }
                return;
            }
        };
        // Drop the Pass 1 watcher so its thread exits before Pass 2 spawns its own.
        drop(_pass1_guard);
        crate::log::device_log(
            device,
            &format!(
                "Pass 1 done: {:.2} GB good, {:.2} MB unreadable, {:.2} MB pending",
                result.bytes_good as f64 / 1_073_741_824.0,
                result.bytes_unreadable as f64 / 1_048_576.0,
                result.bytes_pending as f64 / 1_048_576.0,
            ),
        );

        // Track cross-pass state as raw fields, since CopyResult and
        // PatchResult are distinct types (same semantics, different structs).
        let mut bytes_good = result.bytes_good;
        let mut bytes_unreadable = result.bytes_unreadable;
        let mut bytes_pending = result.bytes_pending;

        // Retry passes: Disc::patch with per-pass block-size taper and
        // alternating direction (RIP_DESIGN.md §15). Pass 2 = reverse + half
        // batch, then alternates F/R while halving block, last pass = 1
        // sector. Each pass gets its own wallclock cap watcher; cap-fire
        // marks the rip as failed.
        let max_retries = cfg_read.max_retries;
        let mut pass_2_settled = false;
        for retry_n in 1..=max_retries {
            // If user hit stop OR a prior pass cap-fired, bail.
            if user_halt.load(Ordering::Relaxed) || cap_fired_any.load(Ordering::Relaxed) {
                break;
            }
            if bytes_pending == 0 && bytes_unreadable == 0 {
                break;
            }
            let pass = retry_n + 1;

            // Settle the drive between Pass 1 and Pass 2 only. Per
            // RIP_DESIGN.md §15 Fix F: the BU40N (and other Initio-bridge
            // drives) wedge after grinding on bad sectors. Giving the drive
            // 30 s of idle BEFORE we hammer it again with retry reads lets
            // its internal state recover. Cheap insurance.
            if !pass_2_settled {
                crate::log::device_log(device, "Settling drive for 30 s before retry pass");
                std::thread::sleep(std::time::Duration::from_secs(30));
                pass_2_settled = true;
                if user_halt.load(Ordering::Relaxed) {
                    break;
                }
            }

            // Per-pass block_sectors + direction.
            //
            // 0.13.22: All retry passes use bpt=1. Pass 1's hysteresis
            // already isolated every NonTrimmed range to a single sector
            // (one bad sector at a time, surrounded by Finished). Disc::patch
            // caps `block_sectors` to `range.size`, so any taper > 1 is a
            // no-op — every retry effectively runs at bpt=1 anyway. We drop
            // the cosmetic taper and just alternate direction across passes
            // (drive's read state differs forward vs reverse + 30 s settle
            // between passes is the actual recovery mechanism, not block
            // size).
            let block_sectors_pass: u16 = 1;
            let reverse_pass = (retry_n % 2) == 1;
            let dir_label = if reverse_pass { "reverse" } else { "forward" };
            crate::log::device_log(
                device,
                &format!(
                    "Pass {pass}/{total_passes}: retrying bad ranges ({dir_label}, bpt=1)"
                ),
            );
            set_pass_progress(
                device,
                &display_name,
                &disc_format,
                &tmdb_title,
                tmdb_year,
                &tmdb_poster,
                &tmdb_overview,
                &duration,
                &codecs,
                &filename,
                pass,
                total_passes,
                bytes_good,
                bytes_unreadable + bytes_pending,
                bytes_total_disc,
                batch,
            );

            // Per-pass progress + watcher.
            let patch_state = std::cell::RefCell::new(PassProgressState::new());
            let patch_ctx = &pass_ctx;
            let patch_title = &title_for_progress;
            let patch_map = std::path::Path::new(&mapfile_path_str);
            let patch_progress = |p: &libfreemkv::progress::PassProgress| {
                patch_state.borrow_mut().last_work_done = p.work_done;
                patch_state.borrow_mut().last_work_total = p.work_total;
                if patch_state.borrow().last_update.elapsed().as_millis() < 1500 {
                    return;
                }
                push_pass_state(
                    patch_ctx,
                    patch_title,
                    bps_progress,
                    patch_map,
                    pass,
                    total_passes,
                    &patch_state,
                );
            };
            let pass_halt = Arc::new(AtomicBool::new(false));
            let _pass_guard = spawn_pass_watcher(
                format!("Pass {pass}"),
                device.to_string(),
                pass_halt.clone(),
                user_halt.clone(),
                cap_fired_any.clone(),
                max_pass_secs,
            );

            // Wedged-drive early-exit threshold. After 50 consecutive
            // failures with zero successes, we conclude the drive is
            // wedged on the bad zone for THIS pass and bail. Saves the
            // wallclock cap for productive grinding; the next pass (with
            // smaller block_sectors and alternated direction) may still
            // recover.
            const WEDGED_THRESHOLD: u64 = 50;

            let patch_opts = libfreemkv::disc::PatchOptions {
                decrypt: false,
                block_sectors: Some(block_sectors_pass),
                full_recovery: true,
                reverse: reverse_pass,
                wedged_threshold: WEDGED_THRESHOLD,
                halt: Some(pass_halt.clone()),
                progress: Some(&patch_progress),
            };
            let prev_good = bytes_good;
            let pr = match disc.patch(&mut session.drive, iso_path, &patch_opts) {
                Ok(r) => r,
                Err(e) => {
                    // If user-halt is set, this is a clean stop; exit.
                    if user_halt.load(Ordering::Relaxed) {
                        crate::log::device_log(
                            device,
                            &format!("Pass {pass} cancelled (halt): {e}"),
                        );
                    } else {
                        crate::log::device_log(device, &format!("Pass {pass} failed: {e}"));
                    }
                    break;
                }
            };
            bytes_good = pr.bytes_good;
            bytes_unreadable = pr.bytes_unreadable;
            bytes_pending = pr.bytes_pending;
            let recovered = bytes_good.saturating_sub(prev_good);
            let exit_str = if pr.wedged_exit {
                " (drive wedged — abandoned this pass)"
            } else if pr.halted {
                " (halt)"
            } else {
                ""
            };
            crate::log::device_log(
                device,
                &format!(
                    "Pass {pass} done: recovered {:.2} MB; {:.2} MB still unreadable; \
                     blocks attempted={} read_ok={} read_failed={}{exit_str}",
                    recovered as f64 / 1_048_576.0,
                    bytes_unreadable as f64 / 1_048_576.0,
                    pr.blocks_attempted,
                    pr.blocks_read_ok,
                    pr.blocks_read_failed,
                ),
            );
            // Drop this pass's watcher before next iteration.
            drop(_pass_guard);
            // Stop early if user-halt or pass cap-fire happened during the
            // patch (the watcher set pass_halt + cap_fired_any).
            if user_halt.load(Ordering::Relaxed) || cap_fired_any.load(Ordering::Relaxed) {
                break;
            }
            // If THIS pass made no progress AND wasn't wedged-aborted, no
            // future pass with the same drive state will help. Give up
            // retries early so we still mux on what we have.
            if recovered == 0 && !pr.wedged_exit {
                crate::log::device_log(device, "No progress on last pass — stopping retries.");
                break;
            }
        }

        // Mux gating per RIP_DESIGN.md §15 Fix B.
        // Skip mux + return cleanly if user pressed stop.
        if user_halt.load(Ordering::Relaxed) {
            crate::log::device_log(device, "Rip cancelled — skipping mux.");
            if let Ok(mut flags) = HALT_FLAGS.lock() {
                flags.remove(device);
            }
            return;
        }
        // Skip mux + status=error if any pass cap-fired (per-pass wallclock
        // budget exceeded). The ISO is retained in staging for manual
        // salvage; this is a hard failure signal, not a partial success.
        if cap_fired_any.load(Ordering::Relaxed) {
            crate::log::device_log(
                device,
                "Pass cap-fired — rip failed; ISO retained in staging, no mux.",
            );
            update_state_with(device, |s| {
                s.status = "error".to_string();
                if s.last_error.is_empty() {
                    s.last_error = "rip failed — pass exceeded wallclock budget".to_string();
                }
            });
            if let Ok(mut flags) = HALT_FLAGS.lock() {
                flags.remove(device);
            }
            return;
        }

        // Close drive — all physical I/O done.
        crate::log::device_log(device, "Drive released; muxing ISO → MKV.");
        drop(session);

        // Open the ISO for the mux pipeline.
        let iso_reader = match libfreemkv::FileSectorReader::open(&iso_path_str) {
            Ok(r) => r,
            Err(e) => {
                crate::log::device_log(device, &format!("Open ISO failed: {e}"));
                update_state(
                    device,
                    RipState {
                        device: device.to_string(),
                        status: "error".to_string(),
                        disc_present: true,
                        last_error: format!("{e}"),
                        disc_name: display_name,
                        disc_format,
                        tmdb_title,
                        tmdb_year,
                        tmdb_poster,
                        tmdb_overview,
                        duration,
                        codecs,
                        ..Default::default()
                    },
                );
                if let Ok(mut flags) = HALT_FLAGS.lock() {
                    flags.remove(device);
                }
                return;
            }
        };
        // Entering mux phase — push final mapfile state so the UI keeps the
        // bad-range list visible through mux and into the "done" view.
        let mux_state = std::cell::RefCell::new(PassProgressState::new());
        push_pass_state(
            &pass_ctx,
            &title_for_progress,
            bps_progress,
            std::path::Path::new(&mapfile_path_str),
            total_passes,
            total_passes,
            &mux_state,
        );
        Box::new(iso_reader) as Box<dyn libfreemkv::SectorReader>
    } else {
        Box::new(session.drive) as Box<dyn libfreemkv::SectorReader>
    };

    let mut input = libfreemkv::DiscStream::new(reader, title, keys, batch, format);
    // Wire the same halt flag into DiscStream so Stop interrupts fill_extents'
    // internal retry loop — required for Stop to work during dense bad-sector
    // regions where the outer PES read() loop may never emit a frame.
    input.set_halt(halt.clone());
    if cfg_read.on_read_error == "skip" {
        input.skip_errors = true;
    }
    let dev_for_stream_events = device.to_string();
    let wdf_stream = wd_last_frame.clone();
    let llba_stream = rip_last_lba.clone();
    let rbs_stream = rip_current_batch.clone();
    let lbr_stream = latest_bytes_read.clone();
    input.on_event(move |event| {
        // Same rationale as the drive callback — DiscStream events prove
        // the rip is advancing even if no PES frame has been emitted yet.
        wdf_stream.store(crate::util::epoch_secs(), Ordering::Relaxed);
        match event.kind {
            libfreemkv::event::EventKind::BytesRead { bytes, .. } => {
                lbr_stream.store(bytes, Ordering::Relaxed);
            }
            libfreemkv::event::EventKind::BatchSizeChanged { new_size, reason } => {
                rbs_stream.store(new_size, Ordering::Relaxed);
                let label = match reason {
                    BatchSizeReason::Shrunk => "shrunk",
                    BatchSizeReason::Probed => "probed up",
                };
                crate::log::device_log(
                    &dev_for_stream_events,
                    &format!("Batch size → {} ({})", new_size, label),
                );
            }
            libfreemkv::event::EventKind::SectorSkipped { sector } => {
                llba_stream.store(sector, Ordering::Relaxed);
                crate::log::device_log(
                    &dev_for_stream_events,
                    &format!("Sector {} skipped (zero-filled)", sector),
                );
            }
            _ => {}
        }
    });

    // Read frames until codec headers are ready
    let mut buffered = Vec::new();
    let mut header_reads = 0u32;
    while !input.headers_ready() {
        if stop_requested(device) {
            crate::log::device_log(device, "Stop requested during header read");
            if let Ok(mut flags) = HALT_FLAGS.lock() {
                flags.remove(device);
            }
            return;
        }
        match input.read() {
            Ok(Some(frame)) => {
                header_reads += 1;
                if header_reads <= 3 || header_reads % 100 == 0 {
                    crate::log::device_log(
                        device,
                        &format!(
                            "Header frame {} track={} len={}",
                            header_reads,
                            frame.track,
                            frame.data.len()
                        ),
                    );
                }
                buffered.push(frame);
            }
            Ok(None) => {
                crate::log::device_log(device, "EOF during header read");
                break;
            }
            Err(e) => {
                crate::log::device_log(device, &format!("Header error: {}", e));
                break;
            }
        }
    }
    crate::log::device_log(
        device,
        &format!("Headers ready, {} frames buffered", buffered.len()),
    );

    let info = input.info().clone();
    let mut out_title = info.clone();
    out_title.playlist = display_name.clone();
    out_title.codec_privates = (0..info.streams.len())
        .map(|i| input.codec_private(i))
        .collect();
    let total_bytes = if total_bytes > 0 {
        total_bytes
    } else {
        info.size_bytes
    };

    crate::log::device_log(device, &format!("Opening output: {}", dest_url));
    let raw_output = match libfreemkv::output(&dest_url, &out_title) {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("Open output failed: {}", e);
            crate::log::device_log(device, &msg);
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "error".to_string(),
                    last_error: msg,
                    ..Default::default()
                },
            );
            return;
        }
    };
    let mut output = libfreemkv::pes::CountingStream::new(raw_output);

    let start = std::time::Instant::now();
    let mut last_update = start;
    let mut last_log = start;
    let mut last_speed_bytes: u64 = 0;
    let mut last_speed_time = start;
    let mut smooth_speed: f64 = 0.0;
    let mut first_update: bool = true;
    let mut seeded_speed: bool = false;

    // Watchdog: monitors the rip loop and logs when reads stall.
    // The rip thread updates last_frame_epoch on every frame. The watchdog
    // checks every 15s and logs if no frame has arrived in 30+ seconds.
    let wd_active = Arc::new(AtomicBool::new(true));
    // Drop guard: stops watchdog on return OR panic (catch_unwind unwinds stack)
    struct WatchdogGuard(Arc<AtomicBool>);
    impl Drop for WatchdogGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Relaxed);
        }
    }
    let _wd_guard = WatchdogGuard(wd_active.clone());
    // `wd_last_frame` is declared earlier (shared with the drive + stream event
    // callbacks, which reset it on any sector-level event). Don't shadow it —
    // the watchdog reader and the callback writers must share one Arc.
    let wd_bytes = Arc::new(AtomicU64::new(0));
    {
        let active = wd_active.clone();
        let last_frame = wd_last_frame.clone();
        let wbytes = wd_bytes.clone();
        let wd_device = device.to_string();
        let wd_display = display_name.clone();
        let wd_format = disc_format.clone();
        let wd_tmdb_title = tmdb_title.clone();
        let wd_tmdb_poster = tmdb_poster.clone();
        let wd_tmdb_overview = tmdb_overview.clone();
        let wd_duration = duration.clone();
        let wd_codecs = codecs.clone();
        let wd_total = total_bytes;
        let wd_tmdb_year = tmdb_year;
        let wd_filename = filename.clone();
        std::thread::spawn(move || {
            let mut was_stalled = false;
            let mut last_log_secs: u64 = 0;
            while active.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_secs(15));
                if !active.load(Ordering::Relaxed) {
                    break;
                }
                let now = crate::util::epoch_secs();
                let last = last_frame.load(Ordering::Relaxed);
                let stall_secs = now.saturating_sub(last);

                if stall_secs >= 30 {
                    // Log on first detection, then every 60s
                    let should_log = !was_stalled || stall_secs >= last_log_secs + 60;
                    if should_log {
                        last_log_secs = stall_secs;
                        let bytes = wbytes.load(Ordering::Relaxed);
                        let gb = bytes as f64 / 1_073_741_824.0;
                        let pct = if wd_total > 0 {
                            (bytes * 100 / wd_total).min(100) as u8
                        } else {
                            0
                        };
                        let mins = stall_secs / 60;
                        let secs = stall_secs % 60;
                        let stall_str = if mins > 0 {
                            format!("{}m {:02}s", mins, secs)
                        } else {
                            format!("{}s", secs)
                        };
                        crate::log::device_log(
                            &wd_device,
                            &format!(
                                "Drive stalled at {:.1} GB ({}%) — waiting for read ({})",
                                gb, pct, stall_str
                            ),
                        );
                    }
                    // Update UI state every cycle — keep speed/eta current
                    let bytes = wbytes.load(Ordering::Relaxed);
                    let gb = bytes as f64 / 1_073_741_824.0;
                    let pct = if wd_total > 0 {
                        (bytes * 100 / wd_total).min(100) as u8
                    } else {
                        0
                    };
                    let stall_str = {
                        let m = stall_secs / 60;
                        let s = stall_secs % 60;
                        if m > 0 {
                            format!("{}m {:02}s", m, s)
                        } else {
                            format!("{}s", s)
                        }
                    };
                    // Mutate-in-place via `update_state_with` so we no longer
                    // have to manually re-read errors/lost_video_secs/last_sector/
                    // current_batch/preferred_batch and copy them through —
                    // every field we don't touch keeps its prior value. This
                    // closes the v0.11.20 regression class (Default::default()
                    // wiping live progress fields during a stall).
                    update_state_with(&wd_device, |s| {
                        s.device = wd_device.clone();
                        s.status = "ripping".to_string();
                        s.disc_present = true;
                        s.disc_name = wd_display.clone();
                        s.disc_format = wd_format.clone();
                        s.progress_pct = pct;
                        s.progress_gb = gb;
                        s.speed_mbs = 0.0;
                        s.eta = format!("stalled {}", stall_str);
                        s.output_file = wd_filename.clone();
                        s.tmdb_title = wd_tmdb_title.clone();
                        s.tmdb_year = wd_tmdb_year;
                        s.tmdb_poster = wd_tmdb_poster.clone();
                        s.tmdb_overview = wd_tmdb_overview.clone();
                        s.duration = wd_duration.clone();
                        s.codecs = wd_codecs.clone();
                        // errors / lost_video_secs / last_sector / current_batch
                        // / preferred_batch / pass / total_passes / bytes_*
                        // / bad_ranges / largest_gap_ms intentionally untouched.
                    });
                    was_stalled = true;
                } else if was_stalled {
                    crate::log::device_log(&wd_device, "Drive recovered — reads resumed");
                    was_stalled = false;
                    last_log_secs = 0;
                }
            }
        });
    }

    // Write buffered frames
    let mut buffered_ok = true;
    for frame in &buffered {
        if stop_requested(device) {
            crate::log::device_log(device, "Stop requested during buffered write");
            buffered_ok = false;
            break;
        }
        if let Err(e) = output.write(frame) {
            crate::log::device_log(device, &format!("Write error (buffered): {}", e));
            buffered_ok = false;
            break;
        }
        // Update watchdog so it doesn't falsely report stall
        wd_last_frame.store(crate::util::epoch_secs(), Ordering::Relaxed);
        wd_bytes.store(output.bytes_written(), Ordering::Relaxed);
    }

    // Stream remaining frames
    let mut completed = false;
    if !buffered_ok {
        crate::log::device_log(device, "Skipping stream loop — buffered write failed");
    }
    loop {
        if !buffered_ok {
            break;
        }
        if stop_requested(device) {
            crate::log::device_log(device, "Stop requested");
            break;
        }
        match input.read() {
            Ok(Some(frame)) => {
                if let Err(e) = output.write(&frame) {
                    crate::log::device_log(device, &format!("Write error: {}", e));
                    break;
                }

                // Signal watchdog: frame received
                wd_last_frame.store(crate::util::epoch_secs(), Ordering::Relaxed);
                wd_bytes.store(output.bytes_written(), Ordering::Relaxed);

                let now = std::time::Instant::now();
                if !first_update && now.duration_since(last_update).as_secs_f64() < 1.0 {
                    continue;
                }
                first_update = false;
                last_update = now;

                let lbr = latest_bytes_read.load(Ordering::Relaxed);
                let bytes_done = if lbr > 0 { lbr } else { output.bytes_written() };
                let pct = if total_bytes > 0 {
                    (bytes_done * 100 / total_bytes).min(100) as u8
                } else {
                    0
                };
                let speed_interval = now.duration_since(last_speed_time).as_secs_f64();
                let instant_speed = if speed_interval > 0.0 {
                    (bytes_done.saturating_sub(last_speed_bytes)) as f64
                        / (1024.0 * 1024.0)
                        / speed_interval
                } else {
                    0.0
                };
                last_speed_bytes = bytes_done;
                last_speed_time = now;
                smooth_speed = if !seeded_speed {
                    seeded_speed = true;
                    instant_speed
                } else {
                    0.95 * smooth_speed + 0.05 * instant_speed
                };
                let speed = smooth_speed;
                let eta = if speed > 0.0 && total_bytes > bytes_done {
                    let secs =
                        ((total_bytes - bytes_done) as f64 / (1024.0 * 1024.0) / speed) as u32;
                    if secs > 359999 {
                        // > 99 hours — ETA is meaningless
                        String::new()
                    } else {
                        let h = secs / 3600;
                        let m = (secs % 3600) / 60;
                        let s = secs % 60;
                        if h > 0 {
                            format!("{}:{:02}:{:02}", h, m, s)
                        } else {
                            format!("{}:{:02}", m, s)
                        }
                    }
                } else {
                    String::new()
                };

                if now.duration_since(last_log).as_secs() >= 60 {
                    last_log = now;
                    let gb = bytes_done as f64 / 1_073_741_824.0;
                    let speed_str = if speed >= 1.0 {
                        format!("{:.1} MB/s", speed)
                    } else {
                        format!("{:.0} KB/s", speed * 1024.0)
                    };
                    let eta_str = if eta.is_empty() {
                        String::new()
                    } else {
                        format!(" ETA {}", eta)
                    };
                    if total_bytes > 0 {
                        let total_gb = total_bytes as f64 / 1_073_741_824.0;
                        crate::log::device_log(
                            device,
                            &format!(
                                "{:.1} GB / {:.1} GB ({}%) {}{}",
                                gb, total_gb, pct, speed_str, eta_str
                            ),
                        );
                    } else {
                        crate::log::device_log(device, &format!("{:.1} GB {}", gb, speed_str));
                    }
                }

                let skip_errors = input.errors as u32;
                let lost_video_secs = if title_bytes_per_sec > 0.0 {
                    (skip_errors as f64) * 2048.0 / title_bytes_per_sec
                } else {
                    0.0
                };
                update_state(
                    device,
                    RipState {
                        device: device.to_string(),
                        status: "ripping".to_string(),
                        disc_present: true,
                        disc_name: display_name.clone(),
                        disc_format: disc_format.clone(),
                        progress_pct: pct,
                        progress_gb: bytes_done as f64 / 1_073_741_824.0,
                        speed_mbs: speed,
                        eta,
                        errors: skip_errors,
                        lost_video_secs,
                        last_sector: rip_last_lba.load(Ordering::Relaxed),
                        current_batch: rip_current_batch.load(Ordering::Relaxed),
                        preferred_batch: batch,
                        output_file: filename.clone(),
                        tmdb_title: tmdb_title.clone(),
                        tmdb_year,
                        tmdb_poster: tmdb_poster.clone(),
                        tmdb_overview: tmdb_overview.clone(),
                        duration: duration.clone(),
                        codecs: codecs.clone(),
                        ..Default::default()
                    },
                );
            }
            Ok(None) => {
                completed = true;
                break;
            }
            Err(e) => {
                crate::log::device_log(device, &format!("Read error: {}", e));
                break;
            }
        }
    }

    // Watchdog stops automatically via _wd_guard Drop

    // Clean up halt flag
    if let Ok(mut flags) = HALT_FLAGS.lock() {
        flags.remove(device);
    }

    if let Err(e) = output.finish() {
        crate::log::device_log(device, &format!("Output finish error: {}", e));
    }

    let bytes_done = output.bytes_written();
    let elapsed = start.elapsed().as_secs_f64();
    let speed = if elapsed > 0.0 {
        bytes_done as f64 / (1024.0 * 1024.0) / elapsed
    } else {
        0.0
    };
    let mut final_errors = input.errors as u32;
    let final_last_sector = rip_last_lba.load(Ordering::Relaxed);
    let final_current_batch = rip_current_batch.load(Ordering::Relaxed);
    let mut final_lost_secs = if title_bytes_per_sec > 0.0 {
        (final_errors as f64) * 2048.0 / title_bytes_per_sec
    } else {
        0.0
    };
    // In multipass mode the `input.errors` counter above counts ISO→MKV demux
    // skips (usually zero — ISO reads don't fail). The real bad-sector count
    // lives in the mapfile sidecar. Prefer that when present.
    let mut final_num_bad_ranges: u32 = 0;
    let mut final_largest_gap_ms: f64 = 0.0;
    if cfg_read.max_retries > 0 {
        let iso_filename = format!("{}.iso", crate::util::sanitize_path_compact(&display_name));
        let mapfile_path_str = format!("{staging}/{iso_filename}.mapfile");
        if let Ok(map) =
            libfreemkv::disc::mapfile::Mapfile::load(std::path::Path::new(&mapfile_path_str))
        {
            let stats = map.stats();
            // Only Unreadable counts as "lost" — NonTried / NonTrimmed /
            // NonScraped at the END of a rip means the rip was interrupted,
            // not that those bytes are damaged. For an interrupted rip the
            // final history record reflects what we know: unreadable = bad.
            let bad_bytes = stats.bytes_unreadable;
            final_errors = (bad_bytes / 2048) as u32;
            final_lost_secs = if title_bytes_per_sec > 0.0 {
                bad_bytes as f64 / title_bytes_per_sec
            } else {
                0.0
            };
            use libfreemkv::disc::mapfile::SectorStatus;
            let bad_ranges = map.ranges_with(&[SectorStatus::Unreadable]);
            final_num_bad_ranges = bad_ranges.len() as u32;
            final_largest_gap_ms = bad_ranges
                .iter()
                .map(|(_, size)| {
                    if title_bytes_per_sec > 0.0 {
                        *size as f64 / title_bytes_per_sec * 1000.0
                    } else {
                        0.0
                    }
                })
                .fold(0.0f64, f64::max);
        }
    }

    // Write a history record for every rip attempt — completed OR stopped.
    // Stopped rips used to leave no persistent trace except the device log,
    // which gets clobbered on the next scan. Include errors/lost/last_sector
    // so damaged-disc attempts are auditable.
    let status_label = if completed { "complete" } else { "stopped" };
    {
        let marker = serde_json::json!({
            "title": display_name,
            "disc_name": disc_name,
            "format": disc_format,
            "year": tmdb_year,
            "media_type": tmdb_media_type,
            "poster_url": tmdb_poster,
            "overview": tmdb_overview,
            "date": crate::util::format_date(),
        });
        if completed {
            // Only mark staging as ready-to-move when the rip actually finished.
            let marker_path = format!("{}/.done", staging);
            let _ = std::fs::write(
                &marker_path,
                serde_json::to_string_pretty(&marker).unwrap_or_default(),
            );
        }

        let mut entry = marker.clone();
        entry["status"] = serde_json::json!(status_label);
        entry["staging_dir"] = serde_json::json!(staging);
        entry["size_gb"] =
            serde_json::json!((bytes_done as f64 / 1_073_741_824.0 * 10.0).round() / 10.0);
        entry["speed_mbs"] = serde_json::json!((speed * 10.0).round() / 10.0);
        entry["elapsed_secs"] = serde_json::json!(elapsed.round() as u64);
        entry["duration"] = serde_json::json!(duration);
        entry["codecs"] = serde_json::json!(codecs);
        entry["device"] = serde_json::json!(device);
        entry["errors"] = serde_json::json!(final_errors);
        entry["lost_video_secs"] = serde_json::json!((final_lost_secs * 1000.0).round() / 1000.0);
        entry["last_sector"] = serde_json::json!(final_last_sector);
        entry["num_bad_ranges"] = serde_json::json!(final_num_bad_ranges);
        entry["largest_gap_ms"] = serde_json::json!(final_largest_gap_ms.round());
        let log_lines = crate::log::get_device_log(device, 500);
        entry["log"] = serde_json::json!(log_lines.join("\n"));
        crate::history::record(&cfg_read.history_dir(), &entry);
    }

    if !completed {
        crate::log::device_log(
            device,
            &format!(
                "Stopped: {:.1} GB in {:.0}s ({:.0} MB/s), {} skipped (~{:.3}s lost)",
                bytes_done as f64 / 1_073_741_824.0,
                elapsed,
                speed,
                final_errors,
                final_lost_secs,
            ),
        );
        update_state(
            device,
            RipState {
                device: device.to_string(),
                status: "idle".to_string(),
                disc_present: true,
                disc_name: display_name.clone(),
                disc_format: disc_format.clone(),
                errors: final_errors,
                lost_video_secs: final_lost_secs,
                last_sector: final_last_sector,
                current_batch: final_current_batch,
                preferred_batch: batch,
                tmdb_title: tmdb_title.clone(),
                tmdb_year,
                tmdb_poster: tmdb_poster.clone(),
                tmdb_overview: tmdb_overview.clone(),
                duration: duration.clone(),
                codecs: codecs.clone(),
                ..Default::default()
            },
        );
        return;
    }

    crate::log::device_log(
        device,
        &format!(
            "Complete: {:.1} GB in {:.0}s ({:.0} MB/s), {} skipped (~{:.3}s lost)",
            bytes_done as f64 / 1_073_741_824.0,
            elapsed,
            speed,
            final_errors,
            final_lost_secs,
        ),
    );

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "done".to_string(),
            disc_present: true,
            disc_name: display_name.clone(),
            disc_format: disc_format.clone(),
            progress_pct: 100,
            errors: final_errors,
            lost_video_secs: final_lost_secs,
            last_sector: final_last_sector,
            current_batch: final_current_batch,
            preferred_batch: batch,
            output_file: staging.clone(),
            tmdb_title: tmdb_title.clone(),
            tmdb_year,
            tmdb_poster: tmdb_poster.clone(),
            tmdb_overview: tmdb_overview.clone(),
            duration: duration.clone(),
            codecs: codecs.clone(),
            ..Default::default()
        },
    );

    if cfg_read.auto_eject {
        eject_drive(device_path);
    }

    // Prune intermediate ISO + mapfile unless keep_iso is set. Only runs in
    // multipass mode (max_retries > 0) — direct mode never produced an ISO.
    if cfg_read.max_retries > 0 && !cfg_read.keep_iso {
        let iso_filename = format!("{}.iso", crate::util::sanitize_path_compact(&display_name));
        let iso_path_str = format!("{}/{}", staging, iso_filename);
        let mapfile_path = format!("{iso_path_str}.mapfile");
        match std::fs::remove_file(&iso_path_str) {
            Ok(_) => crate::log::device_log(device, "Pruned intermediate ISO"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => crate::log::device_log(device, &format!("ISO prune warning: {e}")),
        }
        let _ = std::fs::remove_file(&mapfile_path);
    }

    crate::log::device_log(device, "Rip complete");
    crate::webhook::send_rich(
        &cfg_read,
        &crate::webhook::RipEvent {
            event: "rip_complete",
            title: &display_name,
            year: tmdb_year,
            format: &disc_format,
            poster_url: &tmdb_poster,
            duration: &duration,
            codecs: &codecs,
            size_gb: bytes_done as f64 / 1_073_741_824.0,
            speed_mbs: speed,
            elapsed_secs: elapsed,
            output_path: &staging,
            errors: final_errors,
            lost_video_secs: final_lost_secs,
        },
    );
}

pub fn eject_drive(device_path: &str) {
    let dev = device_path.rsplit('/').next().unwrap_or("");
    // Halt and drain any in-flight rip on this device BEFORE dropping
    // the session — otherwise the rip thread could still be inside a
    // libfreemkv call holding the Drive while we yank it.
    request_stop(dev);
    if join_rip_thread(dev, Duration::from_secs(60)).is_err() {
        tracing::warn!(device = %dev, "rip thread did not drain within 60s of eject");
    }
    drop_session(dev);
    crate::log::archive_device_log(dev);
    if let Ok(mut session) = libfreemkv::Drive::open(std::path::Path::new(device_path)) {
        let _ = session.eject();
    }
}

// `sanitize_filename` and `format_duration` moved to `util` in 0.13.0.
// Callers below now use `crate::util::sanitize_path_compact` and
// `crate::util::format_duration_hm` directly.

fn format_codecs(title: &libfreemkv::DiscTitle) -> String {
    let mut parts = Vec::new();
    // Primary video
    for s in &title.streams {
        if let libfreemkv::Stream::Video(v) = s {
            if !v.secondary {
                let mut desc = format!("{} {}", v.codec.name(), v.resolution);
                if v.hdr != libfreemkv::HdrFormat::Sdr {
                    desc.push_str(&format!(" {}", v.hdr.name()));
                }
                parts.push(desc);
                break;
            }
        }
    }
    // First primary audio only
    for s in &title.streams {
        if let libfreemkv::Stream::Audio(a) = s {
            if !a.secondary {
                let mut audio = format!("{} {}", a.codec.name(), a.channels);
                // autorip is English-only — inline the purpose tags directly.
                if let Some(tag) = audio_purpose_tag(a.purpose) {
                    audio.push_str(&format!(" {}", tag));
                }
                parts.push(audio);
                break;
            }
        }
    }
    parts.join(" · ")
}

/// English purpose label for autorip rendering. None for Normal streams.
/// libfreemkv keeps strings out of the library; autorip is English-only so we
/// inline the words here rather than going through i18n.
#[allow(dead_code)]
fn audio_purpose_tag(p: libfreemkv::LabelPurpose) -> Option<&'static str> {
    match p {
        libfreemkv::LabelPurpose::Commentary => Some("Commentary"),
        libfreemkv::LabelPurpose::Descriptive => Some("Descriptive Audio"),
        libfreemkv::LabelPurpose::Score => Some("Score"),
        libfreemkv::LabelPurpose::Ime => Some("IME"),
        libfreemkv::LabelPurpose::Normal => None,
    }
}

/// English secondary suffix for autorip rendering. Empty when not secondary.
#[allow(dead_code)]
fn audio_secondary_suffix(secondary: bool) -> &'static str {
    if secondary { " (Secondary)" } else { "" }
}

#[cfg(test)]
mod tests {
    //! Regression guards for the multi-pass progress helpers.
    //!
    //! These tests exist because v0.11.22 shipped several UI regressions
    //! (bytes_bad counted NonTried as bad, speed_mbs was zero, errors=0
    //! during multipass) that would have been caught by basic assertions
    //! on push_pass_state's outputs. Keep this module lightweight but
    //! comprehensive enough that each new progress field gets a "does the
    //! right thing for the right status" check.

    use super::*;
    use libfreemkv::disc::mapfile::{Mapfile, SectorStatus};

    fn tmp_map(tag: &str, total: u64) -> (std::path::PathBuf, Mapfile) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(0);
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "autorip-ripper-test-{}-{}-{}.mapfile",
            std::process::id(),
            tag,
            n
        ));
        let _ = std::fs::remove_file(&path);
        let map = Mapfile::create(&path, total, "test").unwrap();
        (path, map)
    }

    fn minimal_title() -> libfreemkv::DiscTitle {
        // Build an almost-empty DiscTitle — enough for the helpers that
        // only touch extents, chapters, duration_secs, size_bytes.
        libfreemkv::DiscTitle {
            playlist: String::new(),
            playlist_id: 0,
            duration_secs: 0.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams: Vec::new(),
            chapters: Vec::new(),
            extents: Vec::new(),
            content_format: libfreemkv::ContentFormat::BdTs,
            codec_privates: Vec::new(),
        }
    }

    #[test]
    fn build_bad_ranges_excludes_not_yet_tried() {
        // Regression from v0.11.22: an empty rip (everything NonTried)
        // was reporting the entire disc as "bad" because bytes_pending
        // (including NonTried) was summed into bytes_bad. This test
        // guards the specific invariant: the list of "bad" ranges must
        // include only `-` (Unreadable), never `?`/`*`/`/`.
        let (_p, mf) = tmp_map("nontried", 10_000);
        let title = minimal_title();
        let (ranges, count, _trunc, lost, largest) = build_bad_ranges(&mf, &title, 1000.0);
        assert!(
            ranges.is_empty(),
            "no Unreadable yet — list should be empty"
        );
        assert_eq!(count, 0);
        assert_eq!(lost, 0.0);
        assert_eq!(largest, 0.0);
    }

    #[test]
    fn build_bad_ranges_ignores_non_trimmed_and_non_scraped() {
        // Post pass-1 on a damaged disc: some ranges become NonTrimmed or
        // NonScraped — meaning "pass 1 failed, pass 2 needs to retry."
        // Those MUST NOT appear in the UI's bad-range list yet; patch may
        // still recover them. Only `-` counts as confirmed bad.
        let (_p, mut mf) = tmp_map("trim_scrape", 10_000);
        mf.record(1000, 200, SectorStatus::NonTrimmed).unwrap();
        mf.record(3000, 100, SectorStatus::NonScraped).unwrap();
        let title = minimal_title();
        let (ranges, count, ..) = build_bad_ranges(&mf, &title, 1000.0);
        assert!(ranges.is_empty());
        assert_eq!(count, 0);
    }

    #[test]
    fn build_bad_ranges_includes_unreadable() {
        let (_p, mut mf) = tmp_map("unreadable", 10_000);
        mf.record(2000, 100, SectorStatus::Unreadable).unwrap();
        let title = minimal_title();
        // bps = 2048 bytes/sec → a 100-byte range is 50 ms.
        let (ranges, count, _trunc, lost, largest) = build_bad_ranges(&mf, &title, 2048.0);
        assert_eq!(count, 1);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].lba, 2000 / 2048);
        assert!((lost - 100.0 / 2048.0 * 1000.0).abs() < 0.001);
        assert!((largest - lost).abs() < 0.001);
    }

    #[test]
    fn build_bad_ranges_sorts_by_duration_desc() {
        let (_p, mut mf) = tmp_map("sort", 100_000);
        mf.record(1000, 100, SectorStatus::Unreadable).unwrap(); // small
        mf.record(20_000, 1000, SectorStatus::Unreadable).unwrap(); // big
        mf.record(50_000, 500, SectorStatus::Unreadable).unwrap(); // medium
        let title = minimal_title();
        let (ranges, ..) = build_bad_ranges(&mf, &title, 1000.0);
        assert_eq!(ranges.len(), 3);
        assert!(ranges[0].duration_ms > ranges[1].duration_ms);
        assert!(ranges[1].duration_ms > ranges[2].duration_ms);
    }

    #[test]
    fn build_bad_ranges_truncates_to_50() {
        let (_p, mut mf) = tmp_map("truncate", 10_000_000);
        // 60 unreadable ranges, all same size. Must truncate to 50 with
        // `bad_ranges_truncated = 10`.
        for i in 0..60u64 {
            mf.record(i * 10_000, 100, SectorStatus::Unreadable)
                .unwrap();
        }
        let title = minimal_title();
        let (ranges, count, trunc, ..) = build_bad_ranges(&mf, &title, 1000.0);
        assert_eq!(count, 60);
        assert_eq!(ranges.len(), 50);
        assert_eq!(trunc, 10);
    }

    #[test]
    fn byte_offset_in_title_within_single_extent() {
        let title = libfreemkv::DiscTitle {
            extents: vec![libfreemkv::Extent {
                start_lba: 1000,
                sector_count: 500,
            }],
            ..minimal_title()
        };
        // LBA 1100 is 100 sectors into the extent = 100 * 2048 bytes in title.
        assert_eq!(byte_offset_in_title(1100, &title), Some(100 * 2048));
    }

    #[test]
    fn byte_offset_in_title_across_multiple_extents() {
        let title = libfreemkv::DiscTitle {
            extents: vec![
                libfreemkv::Extent {
                    start_lba: 1000,
                    sector_count: 100,
                },
                libfreemkv::Extent {
                    start_lba: 5000,
                    sector_count: 200,
                },
            ],
            ..minimal_title()
        };
        // LBA 5050 is 50 sectors into the 2nd extent; first extent is 100*2048.
        assert_eq!(
            byte_offset_in_title(5050, &title),
            Some(100 * 2048 + 50 * 2048)
        );
    }

    #[test]
    fn byte_offset_in_title_returns_none_outside_extents() {
        let title = libfreemkv::DiscTitle {
            extents: vec![libfreemkv::Extent {
                start_lba: 1000,
                sector_count: 100,
            }],
            ..minimal_title()
        };
        // LBA 200 is before the only extent — probably UDF metadata, no
        // chapter mapping possible.
        assert_eq!(byte_offset_in_title(200, &title), None);
        assert_eq!(byte_offset_in_title(50_000, &title), None);
    }

    #[test]
    fn pass_progress_first_sample_returns_zero() {
        // Regression: v0.12.0 shipped with the tracker priming
        // `smooth_speed_mbs` on the first sample, which included all
        // already-copied bytes (e.g. from resume). Users saw "2197.8 MB/s" on
        // a BD rip — impossible. First call must not compute a speed.
        let mut s = PassProgressState::new();
        let t0 = std::time::Instant::now();
        // A disc is 20 GB in at first callback (e.g. because the 1.5 s
        // throttle let real ripping happen before we sampled).
        let speed = s.observe(t0, 20 * 1024 * 1024 * 1024);
        assert_eq!(speed, 0.0, "first sample must not synthesize a speed");
        assert_eq!(s.smooth_speed_mbs, 0.0);
    }

    #[test]
    fn pass_progress_second_sample_matches_physical_rate() {
        // 70 MB delta in 1 s → ~70 MB/s. No prior smoothing, so the instant
        // value becomes the first real smoothed value.
        let mut s = PassProgressState::new();
        let t0 = std::time::Instant::now();
        let _ = s.observe(t0, 1_000_000_000);
        let speed = s.observe(
            t0 + std::time::Duration::from_secs(1),
            1_000_000_000 + 70 * 1_048_576,
        );
        assert!((speed - 70.0).abs() < 1.0, "expected ~70 MB/s, got {speed}");
    }

    #[test]
    fn pass_progress_caps_absurd_instantaneous() {
        // If the caller feeds an 80 GB jump in 1 s (e.g. mapfile read of a
        // resumed disc on the first post-throttle callback), the tracker
        // must cap the instant to 1 GB/s instead of smoothing in nonsense.
        let mut s = PassProgressState::new();
        let t0 = std::time::Instant::now();
        let _ = s.observe(t0, 0);
        let speed = s.observe(
            t0 + std::time::Duration::from_secs(1),
            80 * 1024 * 1024 * 1024,
        );
        assert!(speed <= 1024.0, "speed {speed} MB/s not capped");
    }

    #[test]
    fn pass_progress_steady_state_converges() {
        // Feed 20 samples at a constant 70 MB/s rate. Smoothed value must
        // converge within ±2 MB/s.
        let mut s = PassProgressState::new();
        let mut t = std::time::Instant::now();
        let mut bytes: u64 = 1_000_000_000;
        let _ = s.observe(t, bytes);
        let mut last = 0.0;
        for _ in 0..20 {
            t += std::time::Duration::from_secs(1);
            bytes += 70 * 1_048_576;
            last = s.observe(t, bytes);
        }
        assert!(
            (last - 70.0).abs() < 2.0,
            "expected ~70 MB/s after convergence, got {last}"
        );
    }

    #[test]
    fn pass_progress_zero_dt_returns_previous() {
        // Two calls at the same instant must not divide by zero.
        let mut s = PassProgressState::new();
        let t0 = std::time::Instant::now();
        let _ = s.observe(t0, 0);
        let s1 = s.observe(t0, 100_000_000);
        let s2 = s.observe(t0, 200_000_000);
        assert_eq!(s1, s2, "zero-dt sample must not change smoothed speed");
    }

    #[test]
    fn update_state_with_preserves_untouched_fields() {
        // The whole point of `update_state_with` — fields the closure doesn't
        // touch must survive. Three regressions in autorip's history were
        // exactly this class (Default::default() wiping live progress fields
        // during a watchdog tick).
        let dev = format!("test-preserve-{}", std::process::id());
        update_state_with(&dev, |s| {
            s.errors = 7;
            s.lost_video_secs = 1.5;
            s.last_sector = 12345;
            s.current_batch = 32;
            s.preferred_batch = 60;
        });
        // Now simulate a watchdog tick that only updates progress + status:
        update_state_with(&dev, |s| {
            s.status = "ripping".to_string();
            s.progress_pct = 42;
        });
        let snap = STATE
            .lock()
            .unwrap()
            .get(&dev)
            .cloned()
            .expect("entry must exist");
        assert_eq!(snap.errors, 7, "errors wiped");
        assert_eq!(snap.lost_video_secs, 1.5, "lost_video_secs wiped");
        assert_eq!(snap.last_sector, 12345, "last_sector wiped");
        assert_eq!(snap.current_batch, 32, "current_batch wiped");
        assert_eq!(snap.preferred_batch, 60, "preferred_batch wiped");
        assert_eq!(snap.progress_pct, 42, "new field not applied");
        assert_eq!(snap.status, "ripping", "new field not applied");
    }

    #[test]
    fn device_key_strips_unix_path() {
        // autorip keys its state map by the trailing path component
        // ("sg4", "disk2", "CdRom0"); `device_key` strips the leading
        // /dev/ or \\.\ prefix the lib returns in DriveInfo.path.
        assert_eq!(super::device_key("/dev/sg4"), "sg4");
        assert_eq!(super::device_key("/dev/disk2"), "disk2");
        assert_eq!(super::device_key("\\\\.\\CdRom0"), "CdRom0");
        assert_eq!(super::device_key("sg4"), "sg4"); // already a bare name
    }
}
