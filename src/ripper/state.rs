//! Per-device rip state, the global STATE map, and the per-frame
//! `update_state` building blocks (PassContext / PassProgressState /
//! push_pass_state / set_pass_progress / build_bad_ranges).
//!
//! Lifted verbatim from the monolithic `ripper.rs` as part of the 0.18
//! prep split — no semantic changes.

use std::sync::Mutex;

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
    /// **Bucket: GOOD** — sectors successfully read at least once.
    pub bytes_good: u64,
    /// Bytes still pending retry (`NonTrimmed` / `NonScraped` in the
    /// mapfile). Pass 2-N will revisit these. After the final retry pass,
    /// any remaining `Pending` bytes are reclassified as `Unreadable`.
    /// **Bucket: MAYBE** — drive returned a marginal-read sense; smaller
    /// block size may recover them.
    pub bytes_maybe: u64,
    /// Bytes the drive has given up on (`Unreadable` in the mapfile).
    /// **Bucket: LOST** — terminal; no more retries are scheduled.
    pub bytes_lost: u64,
    /// Total disc size in bytes (for pass-relative progress).
    pub bytes_total_disc: u64,
    /// Bad sector ranges from the mapfile. Capped at 50 entries (biggest by
    /// duration) to keep SSE payloads bounded; `bad_ranges_truncated` reports
    /// how many more exist.
    pub bad_ranges: Vec<BadRange>,
    pub num_bad_ranges: u32,
    pub bad_ranges_truncated: u32,
    /// Sum of `Unreadable` ranges' durations — the actual video time
    /// lost to this rip. Companion to [`Self::bytes_lost`]. UI's red
    /// "no chance" pill renders this.
    pub total_lost_ms: f64,
    /// Sum of `Unreadable` ranges' durations that fall within the
    /// main-feature title's extents. Mirrors `total_lost_ms` but
    /// scoped to the longest title only — enables the UI to render
    /// "(Xs in main movie)".
    pub main_lost_ms: f64,
    /// Sum of `Pending` ranges' durations — the video time at risk
    /// pending Pass 2-N retry. Companion to [`Self::bytes_maybe`]. UI's
    /// yellow "maybe" pill renders this. Drops as retry passes promote
    /// pending sectors to `Finished` (recovered) or `Unreadable` (final).
    pub total_maybe_ms: f64,
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
            bytes_maybe: 0,
            bytes_lost: 0,
            bytes_total_disc: 0,
            bad_ranges: Vec::new(),
            num_bad_ranges: 0,
            bad_ranges_truncated: 0,
            total_lost_ms: 0.0,
            main_lost_ms: 0.0,
            total_maybe_ms: 0.0,
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
pub(super) fn damage_severity_for(errors: u32, total_lost_ms: f64) -> String {
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
pub(super) static STOP_COOLDOWNS: once_cell::sync::Lazy<
    Mutex<std::collections::HashMap<String, u64>>,
> = once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

const STOP_COOLDOWN_SECS: u64 = 5;

pub fn set_stop_cooldown(device: &str) {
    let now = crate::util::epoch_secs();
    if let Ok(mut cd) = STOP_COOLDOWNS.lock() {
        cd.insert(device.to_string(), now + STOP_COOLDOWN_SECS);
    }
}

pub(super) fn is_in_cooldown(device: &str) -> bool {
    let now = crate::util::epoch_secs();
    if let Ok(cd) = STOP_COOLDOWNS.lock() {
        if let Some(&expires) = cd.get(device) {
            return now < expires;
        }
    }
    false
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
pub(super) struct PassContext {
    pub(super) device: String,
    pub(super) display_name: String,
    pub(super) disc_format: String,
    pub(super) tmdb_title: String,
    pub(super) tmdb_year: u16,
    pub(super) tmdb_poster: String,
    pub(super) tmdb_overview: String,
    pub(super) duration: String,
    pub(super) codecs: String,
    pub(super) filename: String,
    pub(super) bytes_total_disc: u64,
    /// Preferred batch size (kernel-reported max sectors per CDB) — surfaced
    /// in RipState during Pass 1 / Pass 2+ so the UI shows a non-zero
    /// `preferred_batch` / `current_batch`. Pass 1 never shrinks the batch
    /// (Disc::copy uses a fixed size); current_batch == preferred_batch
    /// throughout. The DiscStream batch halver only operates during the
    /// mux phase and is reported via the direct-mode stream loop.
    pub(super) batch: u16,
    /// Configured retry-pass count. Used by `push_pass_state` to estimate the
    /// total-bar workload — only `max_retries × bytes_unreadable` worth of work
    /// is queued for retry passes (not the entire pending set, which during
    /// Pass 1 is the whole disc and produced a wildly inflated total ETA).
    /// 0 = single-pass mode (no ISO, no retries, no separate mux phase).
    pub(super) max_retries: u8,
}

/// Walk the title's extents to find the byte offset *within the title* for a
/// given disc LBA. Returns None if the LBA falls outside every extent — meaning
/// the bad region is in UDF metadata or some other non-AV area, where chapter
/// mapping doesn't apply.
pub(super) fn byte_offset_in_title(lba: u32, title: &libfreemkv::DiscTitle) -> Option<u64> {
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
pub(super) fn build_bad_ranges(
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

/// Per-pass speed tracker — sliding window of `(Instant, bytes_good)`
/// samples over the last `SPEED_WINDOW_SECS`. Speed is the average rate
/// across the oldest and newest in-window samples. Held in a RefCell
/// inside the callback closure so interior mutability keeps the closure
/// `Fn`.
///
/// **Why a sliding window, not EWMA**: the previous design used
/// `0.7 × prev + 0.3 × instant`. Alpha=0.3 has a long memory tail — a
/// 12 s stall (drive briefly slow at a marginal LBA region, common on
/// damaged or boundary discs) drags the displayed speed for ~30 s after
/// recovery. Empirically this presents as "the rip looks dead for
/// minutes" when the drive recovered seconds ago. The sliding window
/// bounds the stall's influence on the display: at most
/// `SPEED_WINDOW_SECS` after recovery the slow samples have aged out
/// and the display reflects the true rate.
///
/// **Why this isn't just averaging**: with 1.5 s callback throttling,
/// the window holds ~6-7 samples. Each new sample shifts only one slot,
/// so the displayed value moves smoothly (20 → 19 → 18 → 17 instead of
/// 20 → 7 → 13 → 20 → 2 → 17 jitter). Updates are still fast — every
/// callback recomputes from the freshest window contents.
#[derive(Debug)]
pub(super) struct PassProgressState {
    /// Sliding window of `(observation_time, bytes_good)`. Oldest at the
    /// front, newest at the back. Pruned to the last `SPEED_WINDOW_SECS`
    /// on each `observe` call. Drives the *displayed* speed.
    pub(super) samples: std::collections::VecDeque<(std::time::Instant, u64)>,
    /// Wall-clock of this pass's first observation. Set on the first
    /// `observe` call (not `new()`) so a cold-start delay between
    /// PassProgressState construction and the first byte arriving doesn't
    /// stretch the running-average denominator. Used by
    /// [`Self::eta_speed_mbs`] for ETA — long-average rate that doesn't
    /// jump around on transient slow regions.
    pub(super) pass_start: Option<std::time::Instant>,
    /// `bytes_good` at the moment of the first observation. Lets the
    /// running average measure *bytes ripped during this pass* rather
    /// than total bytes_good (which on Pass 2-N starts from a non-zero
    /// baseline, since the previous pass already wrote some bytes).
    pub(super) pass_start_bytes: u64,
    /// Wall-clock of the last throttled callback. The progress closure
    /// checks this to skip work when less than 1.5 s have passed.
    pub(super) last_update: std::time::Instant,
    /// Wall-clock of the last device-log line emitted from this pass.
    pub(super) last_log: std::time::Instant,
    /// Last `work_done` reported by libfreemkv's `Progress` trait — bytes
    /// processed in this pass so far. Drives `pass_progress_pct`.
    pub(super) last_work_done: u64,
    /// Last `work_total` reported by libfreemkv's `Progress` trait — total
    /// bytes this pass will process. Drives `pass_progress_pct` denominator.
    pub(super) last_work_total: u64,
}

/// Display-speed sliding-window size, adapted to elapsed pass time:
///
/// - `0..STATIC_PHASE_SECS` → fixed at `STATIC_WINDOW_SECS` (10 s).
///   Early in a pass we have little history; small window stays
///   responsive while ETA hasn't settled yet.
/// - `STATIC_PHASE_SECS..STATIC_PHASE_SECS+GROWTH_PHASE_SECS` → linear
///   growth from `STATIC_WINDOW_SECS` to `MAX_WINDOW_SECS` over
///   `GROWTH_PHASE_SECS` of elapsed time. Smooths progressively as we
///   accumulate enough samples for a longer window to be reliable.
/// - `STATIC_PHASE_SECS+GROWTH_PHASE_SECS..` → fixed at
///   `MAX_WINDOW_SECS` (60 s). Steady-state averaging window: enough
///   samples (~40 at the 1.5 s callback throttle) that single-sample
///   jitter contributes ~2.5 % weight, but a real stall still shows up
///   within the window and recovers within `MAX_WINDOW_SECS` of return
///   to full speed.
///
/// Resulting schedule (1.5 s callback ⇒ ~40 samples in a 60 s window):
/// ```
///   t+ 30 s → 10 s window
///   t+ 60 s → 10 s window (start of growth phase)
///   t+210 s → 35 s window
///   t+360 s → 60 s window (cap reached)
///   t+1 h  → 60 s window
/// ```
pub(super) const STATIC_PHASE_SECS: f64 = 60.0;
pub(super) const STATIC_WINDOW_SECS: f64 = 10.0;
pub(super) const GROWTH_PHASE_SECS: f64 = 300.0;
pub(super) const MAX_WINDOW_SECS: f64 = 60.0;

/// Compute the appropriate sliding-window size for the displayed speed
/// given how long the pass has been running. See [`STATIC_PHASE_SECS`]
/// docs above for the curve shape.
pub(super) fn display_window_secs(elapsed_pass_secs: f64) -> f64 {
    if elapsed_pass_secs < STATIC_PHASE_SECS {
        STATIC_WINDOW_SECS
    } else if elapsed_pass_secs < STATIC_PHASE_SECS + GROWTH_PHASE_SECS {
        let t = elapsed_pass_secs - STATIC_PHASE_SECS;
        STATIC_WINDOW_SECS + (MAX_WINDOW_SECS - STATIC_WINDOW_SECS) * (t / GROWTH_PHASE_SECS)
    } else {
        MAX_WINDOW_SECS
    }
}

/// Minimum elapsed time before the pass-start running average is
/// trustworthy enough to use for ETA. Below this threshold the running
/// average is noisy (small denominator, first-sample artefacts) so we
/// fall back to the displayed speed. 10 s ≈ a few callbacks at the
/// 1.5 s throttle, enough to settle.
pub(super) const ETA_WARMUP_SECS: f64 = 10.0;

impl PassProgressState {
    pub(super) fn new() -> Self {
        let now = std::time::Instant::now();
        Self {
            samples: std::collections::VecDeque::with_capacity(16),
            pass_start: None,
            pass_start_bytes: 0,
            last_update: now,
            last_log: now,
            last_work_done: 0,
            last_work_total: 0,
        }
    }

    /// Feed a fresh sample. Returns the windowed speed in MB/s for the
    /// display. Also anchors the pass-start clock + byte counter on the
    /// first observation so [`Self::eta_speed_mbs`] can compute a stable
    /// running average.
    ///
    /// Drops samples older than the *current* window size (which grows
    /// with elapsed pass time per [`display_window_secs`]), pushes the
    /// new one, then computes
    /// `(newest_bytes - oldest_bytes) / (newest_t - oldest_t)`.
    /// Returns 0 when the window holds fewer than 2 samples (we need at
    /// least one prior point to compute a rate).
    pub(super) fn observe(&mut self, now: std::time::Instant, bytes_done: u64) -> f64 {
        if self.pass_start.is_none() {
            self.pass_start = Some(now);
            self.pass_start_bytes = bytes_done;
        }
        let elapsed_pass = self
            .pass_start
            .map(|t| now.duration_since(t).as_secs_f64())
            .unwrap_or(0.0);
        let window_secs = display_window_secs(elapsed_pass);
        let cutoff = now.checked_sub(std::time::Duration::from_secs_f64(window_secs));
        if let Some(cutoff) = cutoff {
            while let Some(&(t, _)) = self.samples.front() {
                if t < cutoff {
                    self.samples.pop_front();
                } else {
                    break;
                }
            }
        }
        self.samples.push_back((now, bytes_done));

        if self.samples.len() < 2 {
            return 0.0;
        }
        let &(oldest_t, oldest_b) = self.samples.front().unwrap();
        let &(newest_t, newest_b) = self.samples.back().unwrap();
        let dt = newest_t.duration_since(oldest_t).as_secs_f64();
        if dt <= 0.0 {
            return 0.0;
        }
        let bytes = newest_b.saturating_sub(oldest_b);
        let mbs = bytes as f64 / 1_048_576.0 / dt;
        // Sanity cap. Real optical drives top out around 70–140 MB/s;
        // 1 GB/s would be a measurement artifact (clock jitter, mapfile
        // replay, monotonic-clock anomaly). Drop rather than display.
        mbs.min(1024.0)
    }

    /// Long-average rate for ETA — bytes ripped this pass divided by
    /// elapsed-this-pass. Stable; transient stalls barely move it (a
    /// 12 s stall after 5 minutes of healthy ripping shifts the average
    /// by less than 5 %). Adapts slowly to sustained speed changes
    /// (e.g. a drive throttling on a damaged region).
    ///
    /// The displayed `speed_mbs` (10 s window) tells the user "what's
    /// happening right now"; the ETA tells them "when will this finish".
    /// They're different questions and should use different rates.
    /// Pre-2026-05-08 they shared the windowed speed and a 12 s stall
    /// made the displayed ETA jump from 1:30:00 to 30:00:00 — which is
    /// not when the rip will finish, just where the slope of the last
    /// 10 s would put it.
    ///
    /// Falls back to `display_speed` during the first `ETA_WARMUP_SECS`
    /// so the ETA isn't garbage early in the pass.
    pub(super) fn eta_speed_mbs(&self, now: std::time::Instant, display_speed: f64) -> f64 {
        let Some(start) = self.pass_start else {
            return display_speed;
        };
        let elapsed = now.duration_since(start).as_secs_f64();
        if elapsed < ETA_WARMUP_SECS {
            return display_speed;
        }
        let Some(&(_, latest_bytes)) = self.samples.back() else {
            return display_speed;
        };
        let bytes = latest_bytes.saturating_sub(self.pass_start_bytes);
        if bytes == 0 {
            return display_speed;
        }
        let mbs = bytes as f64 / 1_048_576.0 / elapsed;
        mbs.min(1024.0)
    }
}

/// Read the live mapfile and push a fresh RipState snapshot for the current
/// pass. Computes smoothed speed + ETA from successive bytes_good samples —
/// otherwise the UI shows 0 KB/s through the whole rip since the main
/// stream loop's speed tracker isn't running during `Disc::copy` / `patch`.
/// No-op (quietly) if the mapfile can't be read — the next callback will
/// try again.
pub(super) fn push_pass_state(
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
    // Main-title-only lost time: intersect Unreadable ranges with the
    // main feature's extents, then convert to ms using the same bps.
    let main_title_bad = map.ranges_with(&[
        libfreemkv::disc::mapfile::SectorStatus::NonTrimmed,
        libfreemkv::disc::mapfile::SectorStatus::Unreadable,
        libfreemkv::disc::mapfile::SectorStatus::NonScraped,
    ]);
    let main_title_bad_bytes = libfreemkv::disc::bytes_bad_in_title(title, &main_title_bad);
    let main_lost_ms = if bps > 0.0 {
        main_title_bad_bytes as f64 * 1000.0 / bps
    } else {
        0.0
    };
    // 0.13.23/0.13.24 three-bucket split (mapfile stats):
    //
    //   GOOD  (bytes_good)  = Finished — terminal success
    //   MAYBE (bytes_maybe) = NonTrimmed + NonScraped — Pass 2-N will retry
    //   LOST  (bytes_lost)  = Unreadable — terminal failure, no retries left
    //
    // `NonTried` (unread; the disc remainder during Pass 1) is **not**
    // in any UI bucket — it's still ahead of the read head, neither
    // confirmed good nor flagged for retry. v0.13.23 mistakenly used
    // `stats.bytes_pending` (= NonTried + NonTrimmed + NonScraped) here,
    // so the entire unread disc surfaced as "Maybe" at pct=0. v0.13.24
    // splits the aggregate via `bytes_retryable` — only the genuine
    // retry-eligible bytes show up in the yellow pill.
    let bytes_lost = stats.bytes_unreadable;
    let bytes_maybe = stats.bytes_retryable;
    // `errors` is the user-visible skipped-sector count: terminal-bad
    // sectors only (`bytes_lost`). Pending bytes are not "errors" — they
    // may still recover.
    let errors = (bytes_lost / 2048) as u32;
    // MAYBE-bucket time equivalent (yellow pill in the UI). Mirrors the
    // existing `total_lost_ms` (red pill) computed from per-range durations.
    let total_maybe_ms = if bps > 0.0 {
        bytes_maybe as f64 * 1000.0 / bps
    } else {
        0.0
    };
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
        .saturating_add(cfg_max_retries.saturating_mul(bytes_lost))
        .saturating_add(mux_estimate_bytes);
    // Cumulative work done across all passes:
    //   pass 1: total_done = last_pos
    //   pass>=2 (retry): total_done = capacity + (pass-2) × bytes_lost + last_pos
    let total_done: u64 = if pass <= 1 {
        last_pos
    } else {
        let prior_retry_count = pass.saturating_sub(2) as u64;
        ctx.bytes_total_disc
            .saturating_add(prior_retry_count.saturating_mul(bytes_lost))
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
        let display_speed = s.observe(now, last_pos);
        // ETA uses the long-running average (bytes ripped this pass /
        // elapsed-this-pass), NOT the displayed 10 s window. A transient
        // 12 s slow region can swing the windowed speed from 15 MB/s to
        // 1 MB/s; if ETA used that, it would jump from "1:30:00" to
        // "30:00:00" mid-rip and back. The user wants ETA = "when will
        // this finish" — that's a question about the whole pass's
        // average rate. Falls back to display_speed during the first
        // ETA_WARMUP_SECS while the running average is still noisy.
        let eta_speed = s.eta_speed_mbs(now, display_speed);
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
        let pass_eta = if eta_speed > 0.01 && last_work_total > last_pos {
            let rem_mb = (last_work_total - last_pos) as f64 / 1_048_576.0;
            format_secs((rem_mb / eta_speed) as u64)
        } else {
            String::new()
        };
        let total_eta = if eta_speed > 0.01 && total_work_estimated > total_done {
            let rem_mb = (total_work_estimated - total_done) as f64 / 1_048_576.0;
            format_secs((rem_mb / eta_speed) as u64)
        } else {
            String::new()
        };
        (display_speed, pass_eta, total_eta)
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
            bytes_maybe,
            bytes_lost,
            bytes_total_disc: ctx.bytes_total_disc,
            bad_ranges: ranges,
            num_bad_ranges: total_count,
            bad_ranges_truncated: truncated,
            total_lost_ms,
            main_lost_ms,
            total_maybe_ms,
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
            let bad_str = if bytes_lost > 0 {
                format!(
                    ", {} skipped ({:.2} MB)",
                    errors,
                    bytes_lost as f64 / 1_048_576.0
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
pub(super) fn set_pass_progress(
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
    bytes_maybe: u64,
    bytes_lost: u64,
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
            bytes_maybe,
            bytes_lost,
            bytes_total_disc,
            preferred_batch: batch,
            current_batch: batch,
            ..Default::default()
        },
    );
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
        // Regression: v0.12.0 shipped with the tracker priming the speed
        // on the first sample, which included all already-copied bytes
        // (e.g. from resume). Users saw "2197.8 MB/s" on a BD rip —
        // impossible. First call must not compute a speed; we need at
        // least two samples to compute a delta over time.
        let mut s = PassProgressState::new();
        let t0 = std::time::Instant::now();
        let speed = s.observe(t0, 20 * 1024 * 1024 * 1024);
        assert_eq!(speed, 0.0, "first sample must not synthesize a speed");
        assert_eq!(
            s.samples.len(),
            1,
            "first sample must be recorded for the next call"
        );
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
    fn pass_progress_stall_drops_out_within_window() {
        // The reason the EWMA was replaced. Real-world scenario from
        // 2026-05-08: BU40N reading a UHD disc, drive briefly slow at a
        // marginal LBA region (transient drop from 15 MB/s to ~0.5 MB/s
        // for ~12 s, then full recovery). With the prior EWMA design
        // (alpha=0.3), the stall sample dragged the displayed speed for
        // 30+ s after the drive had recovered, presenting as "the rip is
        // stuck" in the UI when it actually wasn't.
        //
        // Sliding window guarantee: at most STATIC_WINDOW_SECS after
        // recovery (in the early-pass static phase used here), the slow
        // samples have aged out and the displayed speed reflects the
        // current rate.
        let mut s = PassProgressState::new();
        let mut t = std::time::Instant::now();
        let mut bytes: u64 = 0;
        let _ = s.observe(t, bytes);
        // 10 s of healthy ripping at 70 MB/s
        for _ in 0..10 {
            t += std::time::Duration::from_secs(1);
            bytes += 70 * 1_048_576;
            let _ = s.observe(t, bytes);
        }
        let healthy = s.observe(t, bytes);
        assert!(
            (healthy - 70.0).abs() < 2.0,
            "pre-stall speed should be ~70 MB/s, got {healthy}"
        );

        // 12 s stall — drive only delivers 1 MB/s.
        for _ in 0..12 {
            t += std::time::Duration::from_secs(1);
            bytes += 1_048_576;
            let _ = s.observe(t, bytes);
        }
        let during_stall = s.observe(t, bytes);
        assert!(
            during_stall < 20.0,
            "stall must visibly drop the displayed speed (got {during_stall} MB/s)"
        );

        // Recovery — drive back to 70 MB/s. Within STATIC_WINDOW_SECS, the
        // stall samples should have aged out of the window entirely.
        for _ in 0..(STATIC_WINDOW_SECS as i32 + 2) {
            t += std::time::Duration::from_secs(1);
            bytes += 70 * 1_048_576;
            let _ = s.observe(t, bytes);
        }
        let recovered = s.observe(t, bytes);
        assert!(
            (recovered - 70.0).abs() < 2.0,
            "speed must return to ~70 MB/s once stall samples age out of \
             the window; got {recovered} MB/s"
        );
    }

    #[test]
    fn display_window_grows_with_elapsed_time() {
        // Schedule sanity: warmup at 10 s, growth from 60-360 s, cap at 60 s.
        assert_eq!(display_window_secs(0.0), 10.0);
        assert_eq!(display_window_secs(30.0), 10.0);
        assert_eq!(display_window_secs(59.9), 10.0);
        assert_eq!(display_window_secs(60.0), 10.0); // start of growth
        assert!((display_window_secs(210.0) - 35.0).abs() < 0.1); // mid growth
        assert!((display_window_secs(360.0) - 60.0).abs() < 0.1); // cap reached
        assert_eq!(display_window_secs(3600.0), 60.0); // long after cap
    }

    #[test]
    fn display_speed_is_smoother_in_steady_state() {
        // Run long enough for the adaptive window to reach the 60 s cap,
        // then inject a single-sample blip and assert the displayed
        // speed barely moves. Demonstrates that steady-state jitter
        // (a single 1.5 s sample of bad rate) contributes only ~2.5 %
        // weight in a 60 s window of ~40 samples.
        let mut s = PassProgressState::new();
        let mut t = std::time::Instant::now();
        let mut bytes: u64 = 0;
        let _ = s.observe(t, bytes);
        // 7 minutes of steady 70 MB/s — past STATIC_PHASE + GROWTH_PHASE,
        // so the window has reached the 60 s cap.
        for _ in 0..420 {
            t += std::time::Duration::from_secs(1);
            bytes += 70 * 1_048_576;
            let _ = s.observe(t, bytes);
        }
        let steady = s.observe(t, bytes);
        assert!(
            (steady - 70.0).abs() < 1.5,
            "steady-state speed should be ~70 MB/s after 7 min, got {steady}"
        );
        // Inject one slow second.
        t += std::time::Duration::from_secs(1);
        bytes += 1_048_576;
        let after_blip = s.observe(t, bytes);
        // Single slow sample in a 60 s / ~40-sample window: contribution
        // ≤ ~2 MB/s drop. Pre-adaptive 10 s window would have dropped
        // the displayed speed by ~7 MB/s (10 % of window weight).
        assert!(
            (steady - after_blip) < 3.0,
            "single-sample blip shouldn't move displayed speed by > 3 MB/s \
             in a steady-state 60 s window: before {steady}, after {after_blip}"
        );
    }

    #[test]
    fn eta_speed_stays_stable_through_a_stall() {
        // Companion to pass_progress_stall_drops_out_within_window: the
        // displayed speed CAN dip during a stall, but the ETA must not.
        // Old behaviour (eta = remaining / display_speed) made a 12 s
        // stall flip the displayed ETA from 1:30:00 to 30:00:00. The
        // running average from pass start barely moves (a 12 s stall
        // after 5 minutes of healthy rip changes the average by < 5 %).
        let mut s = PassProgressState::new();
        let mut t = std::time::Instant::now();
        let mut bytes: u64 = 0;
        let _ = s.observe(t, bytes);
        // 5 minutes of healthy ripping at 70 MB/s
        for _ in 0..300 {
            t += std::time::Duration::from_secs(1);
            bytes += 70 * 1_048_576;
            let _ = s.observe(t, bytes);
        }
        let display_before = s.observe(t, bytes);
        let eta_before = s.eta_speed_mbs(t, display_before);
        assert!(
            (eta_before - 70.0).abs() < 2.0,
            "ETA speed before stall should be ~70 MB/s, got {eta_before}"
        );

        // 12 s stall — drive only delivers 1 MB/s.
        for _ in 0..12 {
            t += std::time::Duration::from_secs(1);
            bytes += 1_048_576;
            let _ = s.observe(t, bytes);
        }
        let display_during_stall = s.observe(t, bytes);
        let eta_during_stall = s.eta_speed_mbs(t, display_during_stall);
        // After 5 min, the adaptive display window has grown to ~50 s,
        // so a 12 s stall is only ~24 % of the window — display dips
        // visibly but not catastrophically. The point of this test is
        // that ETA stays stable, not that display crashes; the prior
        // fixed-10s window dropped display to <2 MB/s here, but with
        // the adaptive window dilution the stall registers as a
        // moderate dip (which is *better* UX).
        assert!(
            display_during_stall < 65.0,
            "displayed speed must visibly dip during stall (got {display_during_stall} MB/s)"
        );
        // ETA speed should barely have moved — 12 s of slow on top of
        // 5 min of healthy still averages close to 70 MB/s.
        assert!(
            (eta_during_stall - 70.0).abs() < 5.0,
            "ETA speed during stall must stay close to true average, got \
             {eta_during_stall} MB/s (display was {display_during_stall} MB/s)"
        );
    }

    #[test]
    fn eta_falls_back_to_display_during_warmup() {
        // Before ETA_WARMUP_SECS of pass elapsed, the running average is
        // noisy (small denominator). Use the displayed speed instead.
        let mut s = PassProgressState::new();
        let t0 = std::time::Instant::now();
        let _ = s.observe(t0, 0);
        // 2 s elapsed — well below warmup
        let display = s.observe(t0 + std::time::Duration::from_secs(2), 100 * 1_048_576);
        let eta = s.eta_speed_mbs(t0 + std::time::Duration::from_secs(2), display);
        assert_eq!(eta, display, "ETA must fall back to display before warmup");
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
}
