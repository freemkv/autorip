//! Disc verification — sector-by-sector health check.
//!
//! Opens drive fresh (requires container restart for firmware unlock).
//! Stoppable via STOP flag. Reports live progress with good/bad/slow counts.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Live verify state, pushed to UI via SSE.
#[derive(Debug, Clone, serde::Serialize)]
pub struct VerifyState {
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

pub static VERIFY_STATE: once_cell::sync::Lazy<Mutex<Option<VerifyState>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(None));

static STOP_FLAG: AtomicBool = AtomicBool::new(false);

pub fn request_stop() {
    STOP_FLAG.store(true, Ordering::Relaxed);
}

pub fn is_running() -> bool {
    // Recover a poisoned lock: a panic in the progress callback (which
    // holds this lock) must not permanently wedge the verify state
    // machine. `.ok()` would silently treat a poisoned mutex as "not
    // running" and could mask a stuck state; into_inner reflects truth.
    let vs = VERIFY_STATE.lock().unwrap_or_else(|e| e.into_inner());
    vs.as_ref().map(|v| v.status == "running").unwrap_or(false)
}

/// Atomically claim the verify slot: while holding the lock, if a verify is
/// already "running" bail (return false); otherwise install a "running"
/// placeholder state and return true. This closes a TOCTOU where the previous
/// design only set "running" *inside* the spawned thread after a slow disc
/// scan — two triggers arriving during that window both passed the busy check
/// and both spawned a verify against the same drive. Only the lock winner spawns.
fn try_claim_running(disc_name: &str) -> bool {
    // Recover a poisoned lock rather than returning false: a panic in the
    // progress callback poisons VERIFY_STATE permanently, and a bare
    // `let Ok else { return false }` would then wedge the slot so NO
    // future verify could ever claim it. Consistent with `is_running` /
    // `set_state`, which also `unwrap_or_else(into_inner)`.
    let mut vs = VERIFY_STATE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(state) = vs.as_ref() {
        if state.status == "running" {
            return false;
        }
    }
    *vs = Some(VerifyState {
        status: "running".into(),
        disc_name: disc_name.into(),
        ..empty()
    });
    true
}

pub fn run_verify(device: &str, device_path: &str, keydb_path: Option<String>) {
    // Check if device is busy (ripping or scanning)
    if crate::ripper::is_busy(device) {
        crate::log::device_log(device, "Verify: device is busy");
        set_state(VerifyState {
            status: "error".into(),
            disc_name: "Device busy".into(),
            ..empty()
        });
        return;
    }

    // Clear any STOP_FLAG left over from a PRIOR verify BEFORE claiming the
    // slot. The reset must happen before the claim, not after the spawn: once
    // `try_claim_running` flips status to "running" the slot is visible to
    // `/api/stop` (→ `request_stop` sets STOP_FLAG). If the worker reset the
    // flag as its first act instead, a Stop issued in the window between the
    // claim and the worker's reset would be silently cleared and lost. The
    // worker only ever READS the flag, so resetting here means every stop
    // after the claim is honored.
    STOP_FLAG.store(false, Ordering::Relaxed);

    // Claim the slot synchronously BEFORE spawning so a second trigger that
    // races in (e.g. during the slow drive scan) cannot also start a verify.
    if !try_claim_running("Starting…") {
        crate::log::device_log(device, "Verify: already running");
        return;
    }

    let device = device.to_string();
    let device_path = device_path.to_string();

    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_verify_inner(&device, &device_path, keydb_path.as_deref())
        }));
        if result.is_err() {
            crate::log::device_log(&device, "Verify: thread panicked");
            set_state(VerifyState {
                status: "error".into(),
                disc_name: "Internal error".into(),
                ..empty()
            });
        }
    });
}

fn run_verify_inner(device: &str, device_path: &str, keydb_path: Option<&str>) {
    crate::log::device_log(device, "Verify: opening drive...");

    let mut drive = match libfreemkv::Drive::open(std::path::Path::new(device_path)) {
        Ok(d) => d,
        Err(e) => {
            crate::log::device_log(device, &format!("Verify failed: {}", e));
            set_state(VerifyState {
                status: "error".into(),
                disc_name: format!("{}", e),
                ..empty()
            });
            return;
        }
    };
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
            set_state(VerifyState {
                status: "error".into(),
                disc_name: format!("{}", e),
                ..empty()
            });
            return;
        }
    };
    if disc.titles.is_empty() {
        set_state(VerifyState {
            status: "error".into(),
            disc_name: "No titles".into(),
            ..empty()
        });
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
        set_state(VerifyState {
            status: "error".into(),
            disc_name: "No readable sectors".into(),
            ..empty()
        });
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

    set_state(VerifyState {
        status: "running".into(),
        disc_name: disc_name.clone(),
        sectors_total: total_sectors,
        ..empty()
    });

    let start = std::time::Instant::now();
    let title_clone = title.clone();

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
                let mut vs = crate::verify::VERIFY_STATE
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if let Some(ref mut state) = *vs {
                    state.progress_pct = pct;
                    state.sectors_done = p.work_done;
                    state.speed_mbs = speed;
                    state.elapsed_secs = elapsed;
                }
            }

            // Return false to stop verification
            !STOP_FLAG.load(Ordering::Relaxed)
        }),
    );

    let was_stopped = STOP_FLAG.load(Ordering::Relaxed);

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

    set_state(VerifyState {
        status: status.into(),
        disc_name,
        progress_pct: if was_stopped {
            (result.total_sectors as f64 * 100.0 / total_sectors.max(1) as f64).clamp(0.0, 100.0)
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
    });
}

fn set_state(state: VerifyState) {
    // Recover from poison: this is the path that writes the terminal
    // "error"/"done"/"stopped" state. If a panic in the progress
    // callback poisoned the lock, `if let Ok` would drop the error
    // state on the floor, leaving status stuck at "running" forever and
    // blocking every future verify (is_running stays true). into_inner
    // guarantees the terminal state always lands.
    let mut vs = VERIFY_STATE.lock().unwrap_or_else(|e| e.into_inner());
    *vs = Some(state);
}

fn empty() -> VerifyState {
    VerifyState {
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

    /// Both tests below mutate the process-global `VERIFY_STATE` and one of
    /// them deliberately poisons it, so they cannot run concurrently (the
    /// default multi-threaded test runner would otherwise interleave them).
    /// Serialize them on this guard. Recover from poison so a panicking test
    /// can't wedge the guard for the other.
    static SERIAL: Mutex<()> = Mutex::new(());

    /// Reset `VERIFY_STATE` to a clean slot, tolerating a poisoned lock (the
    /// poison test leaves it poisoned for any test that runs afterwards).
    fn reset_state() {
        let mut vs = VERIFY_STATE.lock().unwrap_or_else(|e| e.into_inner());
        *vs = None;
    }

    /// The TOCTOU fix: claiming the slot is atomic. The first caller wins and
    /// installs a "running" state; a second caller arriving before the first
    /// finishes is rejected, so it can never also spawn a verify against the
    /// same drive. (Uses the global VERIFY_STATE, so this test owns it.)
    #[test]
    fn try_claim_running_is_exclusive() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        // Reset to a clean slot.
        reset_state();

        // First claim wins.
        assert!(try_claim_running("Disc A"));
        assert!(is_running());

        // Second claim, while still "running", loses — no double-start.
        assert!(!try_claim_running("Disc B"));

        // After the slot is released (terminal state), a new claim wins again.
        set_state(VerifyState {
            status: "done".into(),
            ..empty()
        });
        assert!(!is_running());
        assert!(try_claim_running("Disc C"));

        // Cleanup so we don't leave a stray "running" state for other tests.
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
        // Seed a "running" state.
        set_state(VerifyState {
            status: "running".into(),
            ..empty()
        });
        assert!(is_running());

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
        set_state(VerifyState {
            status: "error".into(),
            disc_name: "boom".into(),
            ..empty()
        });
        assert!(!is_running(), "is_running must observe the recovered state");
        let vs = VERIFY_STATE.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(vs.as_ref().unwrap().status, "error");
        assert_eq!(vs.as_ref().unwrap().disc_name, "boom");
    }
}
