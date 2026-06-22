//! Disc verification — sector-by-sector health check.
//!
//! Opens drive fresh (requires container restart for firmware unlock).
//! Stoppable via STOP flag. Reports live progress with good/bad/slow counts.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Live verify state, pushed to UI via SSE.
#[derive(Debug, Clone, serde::Serialize)]
pub struct VerifyState {
    /// Device this verify is running against (e.g. "sg0"). Lets the
    /// per-device state machine and the dashboard tell verifies apart.
    pub device: String,
    pub status: String, // "running", "done", "error", "stopped"
    pub disc_name: String,
    pub progress_pct: f64,
    pub sectors_done: u64,
    pub sectors_total: u64,
    pub speed_mbs: f64,
    pub good: u64,
    pub slow: u64,
    pub recovered: u64,
    pub bad: u64,
    pub bad_mb: f64,
    pub bad_secs: f64,
    pub sector_map: Vec<SectorMapEntry>,
    pub bad_ranges: Vec<BadRange>,
    pub elapsed_secs: f64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SectorMapEntry {
    pub offset_pct: f64,
    pub width_pct: f64,
    pub status: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BadRange {
    pub start_sector: u64,
    pub count: u32,
    pub gb_offset: f64,
    pub chapter: String,
    pub status: String,
}

/// Per-device live verify state, keyed by device id (e.g. "sg0"). Verify is a
/// per-device operation, so the state machine must be too: a stop or a status
/// read for device B must never touch device A's verify. (Before this was a
/// single process-global `Option<VerifyState>` + one global `STOP_FLAG`, so a
/// verify/stop on one drive cancelled the verify on another.)
pub static VERIFY_STATE: once_cell::sync::Lazy<Mutex<HashMap<String, VerifyState>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

/// Per-device stop flags. Keyed by device id. A `request_stop(device)` flips
/// only that device's flag; the worker's progress callback polls its own
/// device's flag. Stored as `Arc<AtomicBool>` so the worker can hold a cheap
/// clone for the duration of the run without re-locking the map per callback.
static STOP_FLAGS: once_cell::sync::Lazy<Mutex<HashMap<String, std::sync::Arc<AtomicBool>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

/// Fetch (creating if absent) the per-device stop flag.
fn stop_flag_for(device: &str) -> std::sync::Arc<AtomicBool> {
    let mut m = STOP_FLAGS.lock().unwrap_or_else(|e| e.into_inner());
    m.entry(device.to_string())
        .or_insert_with(|| std::sync::Arc::new(AtomicBool::new(false)))
        .clone()
}

/// Per-device drive halt tokens. While a verify is running, the worker stashes
/// the open drive's `halt_flag()` (wrapped as a [`libfreemkv::Halt`]) here so
/// `request_stop` can cancel it directly. Without this, the stop flag was only
/// polled by the progress callback *between* sectors — a verify wedged inside a
/// single blocking library read (a marginal sector under 60 s ECC recovery)
/// never observed the stop. Cancelling the drive's halt makes the in-flight
/// SCSI read bail at its next poll point (`POLL_INTERVAL`), so request_stop is
/// observable even mid-read.
static DRIVE_HALTS: once_cell::sync::Lazy<Mutex<HashMap<String, libfreemkv::Halt>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

/// Register the running verify's drive halt token for `device`.
fn register_drive_halt(device: &str, halt: libfreemkv::Halt) {
    let mut m = DRIVE_HALTS.lock().unwrap_or_else(|e| e.into_inner());
    m.insert(device.to_string(), halt);
}

/// Drop the verify's drive halt token for `device` once the run is done.
fn unregister_drive_halt(device: &str) {
    let mut m = DRIVE_HALTS.lock().unwrap_or_else(|e| e.into_inner());
    m.remove(device);
}

/// Request that the verify on `device` (if any) stop. No-op for other devices.
///
/// Flips the per-device stop flag (polled by the progress callback between
/// sectors) AND cancels the drive's halt token (polled inside the blocking
/// library read), so the stop is honoured both between sectors and mid-read.
pub fn request_stop(device: &str) {
    stop_flag_for(device).store(true, Ordering::Relaxed);
    if let Some(halt) = DRIVE_HALTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(device)
    {
        halt.cancel();
    }
}

/// True if a verify is currently "running" on `device`.
pub fn is_running(device: &str) -> bool {
    // Recover a poisoned lock: a panic in the progress callback (which
    // holds this lock) must not permanently wedge the verify state
    // machine. `.ok()` would silently treat a poisoned mutex as "not
    // running" and could mask a stuck state; into_inner reflects truth.
    let m = VERIFY_STATE.lock().unwrap_or_else(|e| e.into_inner());
    m.get(device)
        .map(|v| v.status == "running")
        .unwrap_or(false)
}

/// A single representative verify state for the dashboard's `_verify` field:
/// the running one if any, else the most recently updated terminal one. The
/// dashboard renders one Disc Health card; this picks which device it shows.
pub fn dashboard_state() -> Option<VerifyState> {
    let m = VERIFY_STATE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(running) = m.values().find(|v| v.status == "running") {
        return Some(running.clone());
    }
    // No running verify — fall back to any one terminal state (there's at
    // most one per device; arbitrary pick is fine for a single-card UI).
    m.values().next().cloned()
}

pub fn run_verify(device: &str, device_path: &str, keydb_path: Option<String>) {
    // Atomically claim the device under the same unified STATE lock that
    // scan/rip/eject use, so the rip/scan/eject/verify set is mutually
    // exclusive on one device. This replaces the old non-atomic
    // `ripper::is_busy()` check (which let verify race a rip/scan onto the same
    // drive) AND the old verify-local slot claim. If the claim is lost, another
    // operation already owns the device — reject without touching its state.
    if !crate::ripper::try_claim_active(device) {
        crate::log::device_log(device, "Verify: device is busy");
        set_state(
            device,
            VerifyState {
                status: "error".into(),
                disc_name: "Device busy".into(),
                ..empty()
            },
        );
        return;
    }

    // Capture the claim generation produced by our successful claim above. The
    // detached worker hands this back to release_claim, which only resets the
    // device to idle if the generation is still ours — so a stopped-then-
    // re-claimed device (a new rip/verify took it) is never clobbered by this
    // run's late release (finding #3).
    let claim_gen = crate::ripper::current_claim_gen(device);

    // Reset this device's stop flag and install a "running" placeholder, both
    // under their own locks. The rip claim above already serialized us against
    // any concurrent start, so a second verify can't reach here for this device.
    stop_flag_for(device).store(false, Ordering::Release);
    set_state(
        device,
        VerifyState {
            status: "running".into(),
            disc_name: "Starting…".into(),
            ..empty()
        },
    );

    let device = device.to_string();
    let device_path = device_path.to_string();

    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_verify_inner(&device, &device_path, keydb_path.as_deref())
        }));
        if result.is_err() {
            crate::log::device_log(&device, "Verify: thread panicked");
            set_state(
                &device,
                VerifyState {
                    status: "error".into(),
                    disc_name: "Internal error".into(),
                    ..empty()
                },
            );
        }
        // Drop our drive halt token now that the run is over, so a later
        // request_stop for a *different* operation on this device can't cancel
        // a stale token.
        unregister_drive_halt(&device);
        // Release the rip claim: verify is done (terminal state already set by
        // run_verify_inner or the panic branch above), so mark the device idle
        // again so the next scan/rip/verify can claim it. Mirrors how scan/rip
        // workers update_state to a terminal status to release their claim.
        // Pass our claim generation so we only reset the device if it's still
        // ours — a newer owner (rip/verify claimed after our stop) is untouched.
        release_claim(&device, claim_gen);
    });
}

/// Release the device's rip claim after a verify ends, returning it to "idle"
/// so the unified scan/rip/eject/verify mutual exclusion frees up. Preserves
/// `disc_present` so the UI still shows the disc.
///
/// `claim_gen` is the generation this verify run observed when it claimed the
/// device. We reset to idle ONLY if the device's current generation still
/// matches — i.e. no newer claim (a rip, scan, eject, or fresh verify) has
/// landed since. Without this guard a verify that was stopped while a new rip
/// claimed the same device would clobber the rip's claim back to idle (finding
/// #3), letting a concurrent operation open the same drive.
fn release_claim(device: &str, claim_gen: u64) {
    let mut s = crate::ripper::STATE
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(rs) = s.get_mut(device) {
        if rs.claim_gen != claim_gen {
            // A newer owner took the device after our claim — leave it alone.
            tracing::info!(
                device = %device,
                ours = claim_gen,
                current = rs.claim_gen,
                "verify release_claim: device re-claimed by a newer owner; not resetting",
            );
            return;
        }
        let disc = rs.disc_present;
        *rs = crate::ripper::RipState {
            device: device.to_string(),
            status: "idle".to_string(),
            disc_present: disc,
            claim_gen,
            ..Default::default()
        };
    }
}

fn run_verify_inner(device: &str, device_path: &str, keydb_path: Option<&str>) {
    crate::log::device_log(device, "Verify: opening drive...");

    let mut drive = match libfreemkv::Drive::open(std::path::Path::new(device_path)) {
        Ok(d) => d,
        Err(e) => {
            crate::log::device_log(device, &format!("Verify failed: {}", e));
            set_state(
                device,
                VerifyState {
                    status: "error".into(),
                    disc_name: format!("{}", e),
                    ..empty()
                },
            );
            return;
        }
    };
    // Register the drive's halt flag so request_stop can cancel an in-flight
    // blocking read (finding #4). The library's read/recovery loops poll this
    // flag every POLL_INTERVAL, so a stop lands even while wedged on a single
    // marginal sector under ECC recovery — not just between sectors. If a stop
    // already arrived before we got here, cancel the drive now so the very
    // first read bails immediately.
    register_drive_halt(device, libfreemkv::Halt::from_arc(drive.halt_flag()));
    if stop_flag_for(device).load(Ordering::Relaxed) {
        drive.halt();
    }

    if let Err(e) = drive.wait_ready() {
        tracing::warn!(device = %device, error = %e, "verify: drive wait_ready failed");
    }
    if let Err(e) = drive.init() {
        tracing::warn!(device = %device, error = %e, "verify: drive init failed");
    }

    crate::log::device_log(device, "Verify: scanning...");
    // Keyless scan; supply host credentials from the keydb for the live-drive
    // handshake (LibreDrive/OEM ignores them). Verify checks readability, not
    // decryption, so no key resolution is needed here.
    let scan_opts = match keydb_path {
        Some(p) => crate::keysource::drive_scan_opts_for_keydb(std::path::Path::new(p)),
        None => libfreemkv::ScanOptions::default(),
    };
    let disc = match libfreemkv::Disc::scan(&mut drive, &scan_opts) {
        Ok(d) => d,
        Err(e) => {
            crate::log::device_log(device, &format!("Verify scan failed: {}", e));
            set_state(
                device,
                VerifyState {
                    status: "error".into(),
                    disc_name: format!("{}", e),
                    ..empty()
                },
            );
            return;
        }
    };
    if disc.titles.is_empty() {
        set_state(
            device,
            VerifyState {
                status: "error".into(),
                disc_name: "No titles".into(),
                ..empty()
            },
        );
        return;
    }

    // Deliberate: verify the first title. Verify is a readability check over
    // the disc's primary title, not the rip's main-feature selection; titles[0]
    // is the library's primary listing. (If this should track the rip's
    // main-feature selection, switch to longest-by-duration here.)
    let title = &disc.titles[0];
    let disc_name = disc
        .meta_title
        .as_deref()
        .unwrap_or(&disc.volume_id)
        .to_string();
    let total_sectors: u64 = title.extents.iter().map(|e| e.sector_count as u64).sum();
    if total_sectors == 0 {
        // Guard divide-by-zero: every percentage below divides by
        // total_sectors, yielding inf/NaN that serializes to JSON `null`.
        crate::log::device_log(device, "Verify: title has no readable sectors");
        set_state(
            device,
            VerifyState {
                status: "error".into(),
                disc_name: "No readable sectors".into(),
                ..empty()
            },
        );
        return;
    }
    let total_bytes = total_sectors * 2048;
    let bytes_per_sec = if title.duration_secs > 0.0 {
        total_bytes as f64 / title.duration_secs
    } else {
        8_250_000.0
    };
    let batch = libfreemkv::disc::detect_max_batch_sectors(device_path);

    if let Err(e) = drive.probe_disc() {
        tracing::warn!(device = %device, error = %e, "verify: probe_disc failed");
    }

    crate::log::device_log(
        device,
        &format!(
            "Verify: {} ({:.1} GB, {} sectors)",
            disc_name,
            total_bytes as f64 / 1_073_741_824.0,
            total_sectors
        ),
    );

    set_state(
        device,
        VerifyState {
            status: "running".into(),
            disc_name: disc_name.clone(),
            sectors_total: total_sectors,
            ..empty()
        },
    );

    let start = std::time::Instant::now();
    let title_clone = title.clone();

    // Per-device stop flag, captured once so the hot callback never re-locks
    // the STOP_FLAGS map. A `request_stop(device)` flips this exact Arc.
    let stop_flag = stop_flag_for(device);
    let result = libfreemkv::verify::verify_title(
        &mut drive,
        title,
        batch,
        Some(&|p: &libfreemkv::progress::PassProgress| {
            let elapsed = start.elapsed().as_secs_f64();
            let speed = if elapsed > 0.0 {
                p.work_done as f64 * 2048.0 / (1024.0 * 1024.0) / elapsed
            } else {
                0.0
            };
            let pct = if p.work_total > 0 {
                p.work_done as f64 * 100.0 / p.work_total as f64
            } else {
                0.0
            };

            // Recover a poisoned lock rather than skipping the update: if
            // a prior callback panicked the mutex is poisoned forever, and
            // `if let Ok` would silently stop all progress updates.
            {
                let mut m = crate::verify::VERIFY_STATE
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if let Some(state) = m.get_mut(device) {
                    state.progress_pct = pct;
                    state.sectors_done = p.work_done;
                    state.speed_mbs = speed;
                    state.elapsed_secs = elapsed;
                }
            }

            // Return false to stop verification
            !stop_flag.load(Ordering::Relaxed)
        }),
    );

    let was_stopped = stop_flag.load(Ordering::Relaxed);

    // Build sector map
    let mut sector_map = Vec::new();
    let mut bad_ranges = Vec::new();

    for range in &result.ranges {
        // Clamp so a pathological range can't render past the bar (>100%) or
        // invisibly thin (<0.3%).
        let offset_pct =
            (range.byte_offset as f64 / (total_sectors as f64 * 2048.0) * 100.0).clamp(0.0, 100.0);
        let width_pct = (range.count as f64 / total_sectors as f64 * 100.0).clamp(0.3, 100.0);
        // Exhaustive match (no `_`) so a new SectorStatus variant forces a
        // deliberate decision here instead of being silently dropped.
        let status_str = match range.status {
            libfreemkv::verify::SectorStatus::Slow => "slow",
            libfreemkv::verify::SectorStatus::Recovered => "recovered",
            libfreemkv::verify::SectorStatus::Bad => "bad",
            libfreemkv::verify::SectorStatus::Good => continue,
        };
        sector_map.push(SectorMapEntry {
            offset_pct,
            width_pct,
            status: status_str.into(),
        });

        let gb = range.byte_offset as f64 / 1_073_741_824.0;
        let ch_str = libfreemkv::verify::VerifyResult::chapter_at_offset(
            &title_clone.chapters,
            range.byte_offset,
            title_clone.duration_secs,
            title_clone.size_bytes,
        )
        .map(|(ch, secs)| {
            // Guard a non-finite/negative duration: `secs as u32` saturates
            // (NaN -> 0, huge -> u32::MAX), rendering a misleading timestamp.
            if !secs.is_finite() || secs < 0.0 {
                return format!("Chapter {}", ch);
            }
            let t = secs as u64;
            let h = t / 3600;
            let m = (t % 3600) / 60;
            let s = t % 60;
            if h > 0 {
                format!("Chapter {}, {}:{:02}:{:02}", ch, h, m, s)
            } else {
                format!("Chapter {}, {:02}:{:02}", ch, m, s)
            }
        })
        .unwrap_or_default();

        bad_ranges.push(BadRange {
            start_sector: range.start_lba as u64,
            count: range.count,
            gb_offset: gb,
            chapter: ch_str,
            status: status_str.into(),
        });
    }

    let bad_bytes = result.bad * 2048;
    let bad_mb = bad_bytes as f64 / 1_048_576.0;
    let bad_secs = bad_bytes as f64 / bytes_per_sec;

    let status = if was_stopped { "stopped" } else { "done" };

    crate::log::device_log(
        device,
        &format!(
            "Verify {}: {:.4}% readable — {} good, {} bad ({:.1} MB, {:.1}s), {} slow, {} recovered",
            status,
            result.readable_pct(),
            result.good,
            result.bad,
            bad_mb,
            bad_secs,
            result.slow,
            result.recovered,
        ),
    );

    set_state(
        device,
        VerifyState {
            // Stamped again by set_state, but the struct literal requires it.
            device: device.to_string(),
            status: status.into(),
            disc_name,
            progress_pct: if was_stopped {
                (result.total_sectors as f64 * 100.0 / total_sectors.max(1) as f64)
                    .clamp(0.0, 100.0)
            } else {
                100.0
            },
            sectors_done: result.total_sectors,
            sectors_total: total_sectors,
            speed_mbs: 0.0,
            good: result.good,
            slow: result.slow,
            recovered: result.recovered,
            bad: result.bad,
            bad_mb,
            bad_secs,
            sector_map,
            bad_ranges,
            elapsed_secs: result.elapsed_secs,
        },
    );
}

/// Store `state` for `device`, stamping `state.device = device` so callers can
/// keep using `..empty()` without repeating the device field.
fn set_state(device: &str, mut state: VerifyState) {
    // Recover from poison: this is the path that writes the terminal
    // "error"/"done"/"stopped" state. If a panic in the progress
    // callback poisoned the lock, `if let Ok` would drop the error
    // state on the floor, leaving status stuck at "running" forever and
    // blocking every future verify (is_running stays true). into_inner
    // guarantees the terminal state always lands.
    state.device = device.to_string();
    let mut m = VERIFY_STATE.lock().unwrap_or_else(|e| e.into_inner());
    m.insert(device.to_string(), state);
}

fn empty() -> VerifyState {
    VerifyState {
        device: String::new(),
        status: String::new(),
        disc_name: String::new(),
        progress_pct: 0.0,
        sectors_done: 0,
        sectors_total: 0,
        speed_mbs: 0.0,
        good: 0,
        slow: 0,
        recovered: 0,
        bad: 0,
        bad_mb: 0.0,
        bad_secs: 0.0,
        sector_map: Vec::new(),
        bad_ranges: Vec::new(),
        elapsed_secs: 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// All tests below mutate the process-global `VERIFY_STATE` / `STOP_FLAGS`
    /// (and the ripper `STATE` claim registry) and one of them deliberately
    /// poisons the mutex, so they cannot run concurrently (the default
    /// multi-threaded test runner would otherwise interleave them). Serialize
    /// them on this guard. Recover from poison so a panicking test can't wedge
    /// the guard for the others.
    static SERIAL: Mutex<()> = Mutex::new(());

    /// Reset the verify map + stop flags to a clean slate, tolerating a
    /// poisoned lock (the poison test leaves it poisoned for later tests).
    fn reset_state() {
        VERIFY_STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        STOP_FLAGS.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    /// Finding 3 regression: verify state is PER-DEVICE. A stop on device B
    /// must not cancel a verify on device A, and a status read for one device
    /// must not reflect the other. (The old design used one process-global
    /// STOP_FLAG + one Option<VerifyState>, so B's stop killed A.)
    #[test]
    fn verify_state_and_stop_are_per_device() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        reset_state();

        // Two devices each "running".
        set_state(
            "sg0",
            VerifyState {
                status: "running".into(),
                disc_name: "Disc A".into(),
                ..empty()
            },
        );
        set_state(
            "sg1",
            VerifyState {
                status: "running".into(),
                disc_name: "Disc B".into(),
                ..empty()
            },
        );
        assert!(is_running("sg0"));
        assert!(is_running("sg1"));

        // Stopping sg1 must flip ONLY sg1's flag.
        request_stop("sg1");
        assert!(
            stop_flag_for("sg1").load(Ordering::Relaxed),
            "sg1 stop flag must be set"
        );
        assert!(
            !stop_flag_for("sg0").load(Ordering::Relaxed),
            "sg0 stop flag must NOT be set by a stop targeting sg1"
        );

        // Each device keeps its own state.
        let m = VERIFY_STATE.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(m.get("sg0").unwrap().disc_name, "Disc A");
        assert_eq!(m.get("sg1").unwrap().disc_name, "Disc B");
        drop(m);

        reset_state();
    }

    /// Finding 6 regression: a device already claimed by a rip/scan/eject must
    /// reject a concurrent verify start. run_verify routes its claim through
    /// the unified `ripper::try_claim_active`, so once the device reads as
    /// busy the verify is refused (state goes "error: Device busy") and never
    /// spawns a worker against the in-use drive.
    #[test]
    fn verify_rejected_when_device_already_claimed() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        reset_state();
        let dev = "sgVfyClaim";
        // Make sure the device starts unclaimed.
        crate::ripper::update_state(
            dev,
            crate::ripper::RipState {
                device: dev.to_string(),
                status: "idle".to_string(),
                ..Default::default()
            },
        );

        // Simulate a rip already owning the device.
        assert!(
            crate::ripper::try_claim_active(dev),
            "rip should win the first claim"
        );

        // Verify must now be refused — it can't claim the busy device.
        run_verify(dev, "/dev/does-not-exist", None);
        assert!(
            !is_running(dev),
            "verify must NOT start on a rip-claimed device"
        );
        let m = VERIFY_STATE.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(m.get(dev).map(|v| v.status.as_str()), Some("error"));
        assert_eq!(
            m.get(dev).map(|v| v.disc_name.as_str()),
            Some("Device busy")
        );
        drop(m);

        // Release the rip claim for cleanliness.
        crate::ripper::update_state(
            dev,
            crate::ripper::RipState {
                device: dev.to_string(),
                status: "idle".to_string(),
                ..Default::default()
            },
        );
        reset_state();
    }

    /// Simulate the exact failure mode the poison-recovery fix targets:
    /// a panic while holding VERIFY_STATE poisons the mutex. set_state
    /// must still land the terminal "error" state (not silently no-op),
    /// and is_running must reflect it — otherwise a single callback panic
    /// would wedge the verify state machine at "running" forever and
    /// block every subsequent verify.
    #[test]
    fn set_state_recovers_from_poisoned_lock() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        // Start from a clean, un-poisoned slot regardless of prior tests.
        reset_state();
        let dev = "sgPoison";
        // Seed a "running" state.
        set_state(
            dev,
            VerifyState {
                status: "running".into(),
                ..empty()
            },
        );
        assert!(is_running(dev));

        // Poison the mutex by panicking while the guard is held.
        let _ = std::panic::catch_unwind(|| {
            let _guard = VERIFY_STATE.lock().unwrap();
            panic!("poison the verify state lock");
        });
        assert!(
            VERIFY_STATE.lock().is_err(),
            "lock should be poisoned for this test"
        );

        // The terminal state must still land despite the poison.
        set_state(
            dev,
            VerifyState {
                status: "error".into(),
                disc_name: "boom".into(),
                ..empty()
            },
        );
        assert!(
            !is_running(dev),
            "is_running must observe the recovered state"
        );
        let m = VERIFY_STATE.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(m.get(dev).unwrap().status, "error");
        assert_eq!(m.get(dev).unwrap().disc_name, "boom");
        drop(m);
        reset_state();
    }

    /// Finding #3 (HIGH) regression: a late verify worker must NOT clobber a
    /// newer owner's claim. release_claim resets the device to idle ONLY when
    /// the claim generation it was given still matches the device's current
    /// generation. Here a verify claims (gen G), is then superseded by a new
    /// rip (gen G+1), and finally calls release_claim(G) — which must leave the
    /// rip's claim untouched, not reset it to idle.
    #[test]
    fn verify_release_claim_does_not_clobber_newer_owner() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        reset_state();
        let dev = "sgRelGen";
        // Start unclaimed.
        crate::ripper::update_state(
            dev,
            crate::ripper::RipState {
                device: dev.to_string(),
                status: "idle".to_string(),
                ..Default::default()
            },
        );

        // Verify claims the device and records its generation.
        assert!(crate::ripper::try_claim_active(dev));
        let verify_gen = crate::ripper::current_claim_gen(dev);

        // handle_stop resets the device to idle (its #2 reset path) so a new
        // operation can claim it. The generation is unaffected by this reset.
        crate::ripper::update_state_with(dev, |rs| {
            rs.status = "idle".to_string();
        });

        // A new rip now claims the freed device — the generation advances.
        assert!(crate::ripper::try_claim_active(dev));
        let rip_gen = crate::ripper::current_claim_gen(dev);
        assert!(rip_gen > verify_gen, "new claim must bump the generation");

        // Mark the device as the rip's (ripping) so we can detect a clobber.
        crate::ripper::update_state_with(dev, |rs| {
            rs.status = "ripping".to_string();
        });

        // The late verify worker releases with its STALE generation. It must
        // NOT reset the rip to idle.
        release_claim(dev, verify_gen);

        let m = crate::ripper::STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            m.get(dev).map(|r| r.status.as_str()),
            Some("ripping"),
            "stale verify release must not clobber the newer rip claim"
        );
        drop(m);

        // Sanity: releasing with the CURRENT generation does reset to idle.
        release_claim(dev, crate::ripper::current_claim_gen(dev));
        let m = crate::ripper::STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        assert_eq!(m.get(dev).map(|r| r.status.as_str()), Some("idle"));
        drop(m);
        reset_state();
    }

    /// Finding #4 (MEDIUM) regression: request_stop must cancel the running
    /// verify's drive halt token, not just flip the inter-sector stop flag — so
    /// a verify wedged inside a single blocking library read still observes the
    /// stop. Register a drive halt (as the worker does), call request_stop, and
    /// assert the halt is cancelled.
    #[test]
    fn request_stop_cancels_registered_drive_halt() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        reset_state();
        DRIVE_HALTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        let dev = "sgDriveHalt";

        let halt = libfreemkv::Halt::new();
        register_drive_halt(dev, halt.clone());
        assert!(!halt.is_cancelled());

        // A stop targeting a DIFFERENT device must not cancel this halt.
        request_stop("sgOther");
        assert!(
            !halt.is_cancelled(),
            "stop on another device must not cancel this drive halt"
        );

        // A stop on this device cancels the drive halt (observable mid-read).
        request_stop(dev);
        assert!(
            halt.is_cancelled(),
            "request_stop must cancel the running verify's drive halt"
        );

        unregister_drive_halt(dev);
        // After unregister, a stop is a no-op on the (now-dropped) token.
        let halt2 = libfreemkv::Halt::new();
        request_stop(dev);
        assert!(
            !halt2.is_cancelled(),
            "an unrelated fresh token must be unaffected"
        );

        DRIVE_HALTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        reset_state();
    }

    /// Finding #2 (HIGH) regression: handle_stop drains the detached verify
    /// worker by polling `is_running(device)` until it clears before resetting
    /// STATE. This guards the predicate that drain loop depends on: a verify is
    /// "running" until the worker writes a terminal state, after which
    /// is_running returns false and the drain completes. A running verify left
    /// observable (is_running == true) is exactly the condition that would
    /// otherwise let handle_stop reset STATE to idle while the drive is still
    /// open, enabling a concurrent /api/rip double-open.
    #[test]
    fn is_running_clears_only_after_worker_reaches_terminal() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        reset_state();
        let dev = "sgDrain";

        // Worker installs the running placeholder (as run_verify does).
        set_state(
            dev,
            VerifyState {
                status: "running".into(),
                disc_name: "Starting…".into(),
                ..empty()
            },
        );
        // Drain predicate observes the worker as still in flight.
        assert!(
            is_running(dev),
            "handle_stop drain must keep waiting while verify is running"
        );

        // Worker reaches a terminal state (request_stop -> drive halt cancelled
        // -> read bails -> "stopped"). Now the drain predicate clears.
        set_state(
            dev,
            VerifyState {
                status: "stopped".into(),
                disc_name: "Disc".into(),
                ..empty()
            },
        );
        assert!(
            !is_running(dev),
            "handle_stop drain completes once the worker writes a terminal state"
        );

        reset_state();
    }
}
