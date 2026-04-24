use libfreemkv::event::BatchSizeReason;
use libfreemkv::pes::Stream as PesStream;

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::config::Config;

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
        }
    }
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

// ─── Poll loop ─────────────────────────────────────────────────────────────

/// Poll drives for disc insertion. Only triggers on state change
/// (no disc → disc present), not on disc already being there.
pub fn drive_poll_loop(cfg: &Arc<RwLock<Config>>) {
    let mut had_disc: std::collections::HashSet<String> = std::collections::HashSet::new();

    while !crate::SHUTDOWN.load(Ordering::Relaxed) {
        {
            let mut current_with_disc: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            for i in 0..16u8 {
                let path = format!("/dev/sg{}", i);
                if !std::path::Path::new(&path).exists() {
                    continue;
                }
                // Cheap sysfs pre-filter: SCSI type 5 = CD/DVD/BD optical.
                // Avoids Drive::open's 2s reset dance on non-optical sg nodes
                // (RAID controllers, NVMe passthroughs) when /dev is bind-mounted
                // live from the host. Falls through on read failure so we don't
                // accidentally exclude an optical drive if sysfs is unreadable.
                let type_path = format!("/sys/class/scsi_generic/sg{}/device/type", i);
                if let Ok(s) = std::fs::read_to_string(&type_path) {
                    if s.trim() != "5" {
                        continue;
                    }
                }
                let device = format!("sg{}", i);

                // Don't touch drives that are actively scanning/ripping
                if is_busy(&device) {
                    current_with_disc.insert(device);
                    continue;
                }

                // Open briefly to check status, then drop immediately
                let mut drive = match libfreemkv::Drive::open(std::path::Path::new(&path)) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let disc_present = drive.drive_status() == libfreemkv::DriveStatus::DiscPresent;
                drop(drive);

                if !disc_present {
                    // Disc removed — clean up session
                    if had_disc.contains(&device) {
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

                    std::thread::spawn(move || {
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            scan_disc(&cfg, &device, &dev_path);
                            if on_insert == "rip" && !stop_requested(&device) {
                                rip_disc(&cfg, &device, &dev_path);
                            }
                        }))
                        .is_err()
                        {
                            crate::log::device_log(&device, "Thread panicked");
                            drop_session(&device);
                            update_state(
                                &device,
                                RipState {
                                    device: device.clone(),
                                    status: "error".to_string(),
                                    last_error: "Internal error (panic)".to_string(),
                                    ..Default::default()
                                },
                            );
                        }
                    });
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
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
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

pub fn update_state(device: &str, state: RipState) {
    if let Ok(mut s) = STATE.lock() {
        s.insert(device.to_string(), state);
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
}

impl PassProgressState {
    fn new() -> Self {
        let now = std::time::Instant::now();
        Self {
            prev: None,
            smooth_speed_mbs: 0.0,
            last_update: now,
            last_log: now,
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
    let pct = if ctx.bytes_total_disc > 0 {
        (stats.bytes_good * 100 / ctx.bytes_total_disc).min(100) as u8
    } else {
        0
    };

    // Compute smoothed speed + ETA from successive samples.
    let (speed_mbs, eta) = {
        let mut s = state.borrow_mut();
        let now = std::time::Instant::now();
        let speed = s.observe(now, stats.bytes_good);
        s.last_update = now;
        let eta_str = if speed > 0.01 && ctx.bytes_total_disc > stats.bytes_good {
            let rem_mb = (ctx.bytes_total_disc - stats.bytes_good) as f64 / 1_048_576.0;
            let secs = (rem_mb / speed) as u64;
            if secs < 360_000 {
                let h = secs / 3600;
                let m = (secs % 3600) / 60;
                let se = secs % 60;
                if h > 0 {
                    format!("{h}:{m:02}:{se:02}")
                } else {
                    format!("{m}:{se:02}")
                }
            } else {
                String::new()
            }
        } else {
            String::new()
        };
        (speed, eta_str)
    };

    update_state(
        &ctx.device,
        RipState {
            device: ctx.device.clone(),
            status: "ripping".to_string(),
            disc_present: true,
            disc_name: ctx.display_name.clone(),
            disc_format: ctx.disc_format.clone(),
            progress_pct: pct,
            progress_gb: stats.bytes_good as f64 / 1_073_741_824.0,
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
            ..Default::default()
        },
    );

    // Periodic device-log line so a long pass doesn't go silent. Matches the
    // 60 s cadence the main stream loop uses in direct mode.
    {
        let mut s = state.borrow_mut();
        if s.last_log.elapsed().as_secs() >= 60 {
            s.last_log = std::time::Instant::now();
            let gb = stats.bytes_good as f64 / 1_073_741_824.0;
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
                    "Pass {pass}/{total_passes}: {:.1} GB / {:.1} GB ({}%) {}{}",
                    gb, total_gb, pct, speed_str, bad_str
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
    let _ = drive.wait_ready();
    crate::log::device_log(device, "Initializing...");
    let _ = drive.init();

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
        Some(p) => libfreemkv::ScanOptions::with_keydb(p),
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
        .map(|t| format_duration(t.duration_secs))
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
            let _ = drive.wait_ready();
            crate::log::device_log(device, "Initializing...");
            let _ = drive.init();

            let scan_opts = match &cfg_read.keydb_path {
                Some(p) => libfreemkv::ScanOptions::with_keydb(p),
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

    let disc = session.disc.take().unwrap();

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
    let total_bytes = disc.titles.first().map(|t| t.size_bytes).unwrap_or(0);

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

    let duration = format_duration(disc.titles[0].duration_secs);
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

    let staging = cfg_read.staging_device_dir(&sanitize_filename(&display_name));
    let _ = std::fs::create_dir_all(&staging);
    let filename = format!("{}.{}", sanitize_filename(&display_name), ext);
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
    let rip_last_lba = Arc::new(AtomicU64::new(0));
    let rip_current_batch = Arc::new(AtomicU16::new(batch));

    // Create PES stream — same drive session, no re-open
    let halt = session.drive.halt_flag();
    register_halt(device, halt.clone());
    let dev_for_events = device.to_string();
    let wdf_drive = wd_last_frame.clone();
    session.drive.on_event(move |event| {
        // Any drive-level event means something is happening — reset the
        // watchdog so the "stalled" timer doesn't monotonically climb
        // while the library is working through recovery.
        wdf_drive.store(crate::util::epoch_secs(), Ordering::Relaxed);
        match event.kind {
            libfreemkv::event::EventKind::ReadError { sector, .. } => {
                crate::log::device_log(
                    &dev_for_events,
                    &format!("Read error at sector {}", sector),
                );
            }
            libfreemkv::event::EventKind::Retry { attempt } => {
                crate::log::device_log(&dev_for_events, &format!("Retrying (attempt {})", attempt));
            }
            libfreemkv::event::EventKind::SectorRecovered { sector } => {
                crate::log::device_log(&dev_for_events, &format!("Sector {} recovered", sector));
            }
            libfreemkv::event::EventKind::SpeedChange { speed_kbs } => {
                if speed_kbs == 0 {
                    crate::log::device_log(&dev_for_events, "Recovery: min speed");
                } else {
                    crate::log::device_log(&dev_for_events, "Restoring full speed");
                }
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
        let iso_filename = format!("{}.iso", sanitize_filename(&display_name));
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
            bytes_total_disc,
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
        );

        // Progress callback — runs every read block (~64 KB). Throttle the
        // mapfile re-read + state push to once every 1.5 s so we don't pound
        // the mutex or the filesystem. State tracker holds last-sample
        // timestamp + bytes for speed/ETA calc.
        let pass1_state = std::cell::RefCell::new(PassProgressState::new());
        let pass1_ctx = &pass_ctx;
        let pass1_title = &title_for_progress;
        let pass1_map = std::path::Path::new(&mapfile_path_str);
        let pass1_progress = |_bytes_good: u64, _total: u64| {
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

        let copy_opts = libfreemkv::disc::CopyOptions {
            decrypt: false,
            resume: true,
            batch_sectors: Some(batch),
            skip_on_error: true,
            skip_forward: true,
            halt: Some(halt.clone()),
            on_progress: Some(&pass1_progress),
        };
        let result = match disc.copy(&mut session.drive, iso_path, &copy_opts) {
            Ok(r) => r,
            Err(e) => {
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
        let mut halted = result.halted;

        // Retry passes: Disc::patch until max_retries hit, all clean, or no
        // progress. Each call is one pass; the mapfile persists across them.
        for retry_n in 1..=cfg_read.max_retries {
            if halted || halt.load(Ordering::Relaxed) {
                break;
            }
            if bytes_pending == 0 && bytes_unreadable == 0 {
                break;
            }
            let pass = retry_n + 1;
            crate::log::device_log(
                device,
                &format!("Pass {pass}/{total_passes}: retrying bad ranges"),
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
            );
            // Per-pass progress callback (same throttle + speed-tracker
            // pattern as pass 1).
            let patch_state = std::cell::RefCell::new(PassProgressState::new());
            let patch_ctx = &pass_ctx;
            let patch_title = &title_for_progress;
            let patch_map = std::path::Path::new(&mapfile_path_str);
            let patch_progress = |_bytes_good: u64, _total: u64| {
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
            let patch_opts = libfreemkv::disc::PatchOptions {
                decrypt: false,
                full_recovery: true,
                halt: Some(halt.clone()),
                on_progress: Some(&patch_progress),
                ..Default::default()
            };
            let prev_good = bytes_good;
            let pr = match disc.patch(&mut session.drive, iso_path, &patch_opts) {
                Ok(r) => r,
                Err(e) => {
                    crate::log::device_log(device, &format!("Pass {pass} failed: {e}"));
                    break;
                }
            };
            bytes_good = pr.bytes_good;
            bytes_unreadable = pr.bytes_unreadable;
            bytes_pending = pr.bytes_pending;
            halted = pr.halted;
            let recovered = bytes_good.saturating_sub(prev_good);
            crate::log::device_log(
                device,
                &format!(
                    "Pass {pass} done: recovered {:.2} MB; {:.2} MB still unreadable",
                    recovered as f64 / 1_048_576.0,
                    bytes_unreadable as f64 / 1_048_576.0,
                ),
            );
            if recovered == 0 {
                crate::log::device_log(device, "No progress on last pass — stopping retries.");
                break;
            }
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
    input.on_event(move |event| {
        // Same rationale as the drive callback — DiscStream events prove
        // the rip is advancing even if no PES frame has been emitted yet.
        wdf_stream.store(crate::util::epoch_secs(), Ordering::Relaxed);
        match event.kind {
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
            libfreemkv::event::EventKind::SectorRecovered { sector } => {
                llba_stream.store(sector, Ordering::Relaxed);
                crate::log::device_log(
                    &dev_for_stream_events,
                    &format!("Sector {} recovered", sector),
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
                    // Preserve per-rip fields from existing STATE that the
                    // watchdog would otherwise wipe via Default::default().
                    // Without this, the UI shows 0/0 batch + no lost_video
                    // + last_sector=0 during stalls even though the library
                    // is actively working through bad sectors.
                    let (
                        prev_errors,
                        prev_current_batch,
                        prev_preferred_batch,
                        prev_last_sector,
                        prev_lost_video_secs,
                    ) = STATE
                        .lock()
                        .ok()
                        .and_then(|s| {
                            s.get(&wd_device).map(|r| {
                                (
                                    r.errors,
                                    r.current_batch,
                                    r.preferred_batch,
                                    r.last_sector,
                                    r.lost_video_secs,
                                )
                            })
                        })
                        .unwrap_or((0, 0, 0, 0, 0.0));
                    update_state(
                        &wd_device,
                        RipState {
                            device: wd_device.clone(),
                            status: "ripping".to_string(),
                            disc_present: true,
                            disc_name: wd_display.clone(),
                            disc_format: wd_format.clone(),
                            progress_pct: pct,
                            progress_gb: gb,
                            speed_mbs: 0.0,
                            eta: format!("stalled {}", stall_str),
                            errors: prev_errors,
                            lost_video_secs: prev_lost_video_secs,
                            last_sector: prev_last_sector,
                            current_batch: prev_current_batch,
                            preferred_batch: prev_preferred_batch,
                            output_file: wd_filename.clone(),
                            tmdb_title: wd_tmdb_title.clone(),
                            tmdb_year: wd_tmdb_year,
                            tmdb_poster: wd_tmdb_poster.clone(),
                            tmdb_overview: wd_tmdb_overview.clone(),
                            duration: wd_duration.clone(),
                            codecs: wd_codecs.clone(),
                            ..Default::default()
                        },
                    );
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
                if now.duration_since(last_update).as_secs_f64() < 1.0 {
                    continue;
                }
                last_update = now;

                let bytes_done = output.bytes_written();
                let pct = if total_bytes > 0 {
                    (bytes_done * 100 / total_bytes).min(100) as u8
                } else {
                    0
                };
                let speed_interval = now.duration_since(last_speed_time).as_secs_f64();
                let instant_speed = if speed_interval > 0.0 {
                    (bytes_done - last_speed_bytes) as f64 / (1024.0 * 1024.0) / speed_interval
                } else {
                    0.0
                };
                last_speed_bytes = bytes_done;
                last_speed_time = now;
                smooth_speed = if smooth_speed < 0.01 {
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
        let iso_filename = format!("{}.iso", sanitize_filename(&display_name));
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
        let iso_filename = format!("{}.iso", sanitize_filename(&display_name));
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
    drop_session(dev);
    crate::log::archive_device_log(dev);
    if let Ok(mut session) = libfreemkv::Drive::open(std::path::Path::new(device_path)) {
        let _ = session.eject();
    }
}

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == ' ' || *c == '-' || *c == '_' || *c == '.')
        .collect::<String>()
        .trim()
        .replace(' ', "_")
}

fn format_duration(secs: f64) -> String {
    let h = (secs / 3600.0) as u32;
    let m = ((secs % 3600.0) / 60.0) as u32;
    format!("{}h {:02}m", h, m)
}

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
                parts.push(format!("{} {}", a.codec.name(), a.channels));
                break;
            }
        }
    }
    parts.join(" · ")
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
}
