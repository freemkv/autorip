//! Per-device rip state, the global STATE map, and the per-frame
//! `update_state` building blocks (PassContext / PassProgressState /
//! push_pass_state / set_pass_progress / build_bad_ranges).
//!
//! Lifted verbatim from the monolithic `ripper.rs` as part of the 0.18
//! prep split — no semantic changes.

use crate::util::{BYTES_PER_GIB, BYTES_PER_MIB, MILLIS_PER_SEC, SECTOR_BYTES};
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

/// Whether — and how — a disc's partial staging state can be resumed. Set on
/// [`RipState::resumable`] at scan time and rendered by the dashboard as a
/// Resume button. Serializes to a lowercase tag (`"remux"` / `"sweep"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Resumable {
    /// Sweep finished (no bytes pending) but the final MKV is missing — Resume
    /// just re-muxes the staged ISO (no disc reads).
    Remux,
    /// Partial sweep: the mapfile still has pending (NonTrimmed / non-tried)
    /// bytes. Resume continues Pass 1 from the mapfile, reading only the
    /// missing ranges.
    Sweep,
}

// TODO(1.2.0): replace the stringly-typed `status` with two explicit enums —
// DeviceStage (idle/scanning/sweeping/patching/done) for the drive on the
// Ripper tab, and PipelineStage (queued/muxing/moving/delivered/blocked) for
// the disc-in-pipeline on the System tab. Deferred this cycle: the web UI JS
// hard-depends on these exact status strings (web.rs buildSteps:
// `s.status==='scanning'` etc.), so the enum cutover must land together with
// the frontend rework rather than half-wired. The post-mux abort removal
// (mux never aborts) is the SAFE subset that shipped now.
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
    /// **Main-feature time still AT RISK** — the honest live "Maybe" metric.
    /// The duration of every not-yet-good range (`NonTrimmed` + `NonScraped` +
    /// `Unreadable`) that falls within the main title's extents. Unlike
    /// [`Self::main_lost_ms`] (terminal `Unreadable`-only — correct for the
    /// abort verdict, but trivially 0 mid-rip), this counts pending in-feature
    /// data as movie-at-risk, so the two-pill UI's `Maybe N · <time>` is honest
    /// during the rip: `0:00` when the pending bytes are out-of-feature, a real
    /// ms figure when they're in the movie. It melts toward `main_lost_ms` as
    /// retry passes resolve pending sectors to `Finished` or `Unreadable`.
    pub main_at_risk_ms: f64,
    /// Largest single contiguous bad range's duration. Tells the difference
    /// between 1000 × 1ms gaps (unnoticeable) vs 1 × 1s gap (noticeable glitch).
    pub largest_gap_ms: f64,
    /// True when this rip aborted because main-movie loss exceeded the
    /// threshold and a resumable `.aborted-loss` staging (the complete ISO) is
    /// on disk. The UI shows the **Accept damage & deliver** off-ramp when set —
    /// the operator can deliver the rip as-is instead of re-ripping.
    pub loss_aborted: bool,
    pub last_error: String,
    pub output_file: String,
    pub tmdb_title: String,
    pub tmdb_year: u16,
    pub tmdb_poster: String,
    pub tmdb_overview: String,
    /// TMDB media type ("movie" or "tv"). Carried into STATE so the
    /// auto-resume mux path can write a correct `media_type` into the
    /// `.done`/`.review` hand-off marker — otherwise the mover defaults
    /// every resumed rip to "movie" and files TV shows under the movie
    /// library. Empty string when unresolved.
    pub tmdb_media_type: String,
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

    /// Operator-readable failure reason for `status == "failed"`.
    /// Populated when the resume-on-startup logic finds a `.failed`
    /// marker in a disc's staging dir (e.g. "restart loop detected at
    /// patch phase"). Distinct from `last_error` because `last_error`
    /// gets overwritten on every transient hiccup; this one survives
    /// across renders for the operator-decision view. Optional /
    /// `skip_serializing_if = "Option::is_none"` so older dashboards
    /// that don't know the field don't see a stray `null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,

    /// v0.25.7: epoch-seconds timestamp of when the current rip
    /// transitioned into an active state (`scanning` or `ripping`).
    /// 0 when no rip is in flight. The UI uses this to render a live
    /// elapsed-time counter next to the Stop button — JS computes
    /// `now - started_epoch_secs` so the display advances every tick
    /// without server pressure. Preserved across `update_state` calls
    /// for the same rip; cleared when status returns to `idle`.
    #[serde(default)]
    pub started_epoch_secs: u64,
    /// Key readiness determined at scan time, for the dashboard tile:
    /// "Ready to rip", "Missing keys — <reason>", or "" (unknown).
    pub key_status: String,

    /// Resume affordance computed at scan time. `None` when there's no
    /// resumable staging for this disc (Rip only); `Some(_)` makes the
    /// dashboard show a Resume button alongside Rip. Omitted from the JSON
    /// when `None` so older dashboards don't see a stray field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumable: Option<Resumable>,

    /// Monotonic claim generation, bumped by every successful
    /// [`try_claim_active`]. Lets a detached worker (e.g. a verify thread)
    /// tell whether the device it claimed is *still* the one it owns: if a
    /// newer claim (a rip, scan, eject, or a fresh verify) has landed since,
    /// the generation will have moved and the stale worker must NOT reset the
    /// device to idle — doing so would clobber the new owner's claim. Not
    /// serialized: a pure server-side bookkeeping field the UI never reads.
    #[serde(skip)]
    pub claim_gen: u64,
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
            main_at_risk_ms: 0.0,
            largest_gap_ms: 0.0,
            loss_aborted: false,
            last_error: String::new(),
            output_file: String::new(),
            tmdb_title: String::new(),
            tmdb_year: 0,
            tmdb_poster: String::new(),
            tmdb_overview: String::new(),
            tmdb_media_type: String::new(),
            duration: String::new(),
            codecs: String::new(),
            pass_progress_pct: 0,
            pass_eta: String::new(),
            total_progress_pct: 0,
            total_eta: String::new(),
            damage_severity: String::new(),
            failure_reason: None,
            started_epoch_secs: 0,
            key_status: String::new(),
            resumable: None,
            claim_gen: 0,
        }
    }
}

/// Compute the damage-severity badge string from autorip's RipState
/// fields. Wraps libfreemkv's `classify_damage` so the UI gets a stable
/// lowercase string ("clean" / "cosmetic" / "moderate" / "serious").
pub(super) fn damage_severity_for(errors: u32, total_lost_ms: f64) -> String {
    use libfreemkv::DamageSeverity;
    // Direct match instead of round-tripping through serde_json::to_value
    // on every (throttled) progress callback. Strings match libfreemkv's
    // `#[serde(rename_all = "lowercase")]` repr so the UI is unchanged.
    match libfreemkv::classify_damage(errors as u64, total_lost_ms) {
        DamageSeverity::Clean => "clean",
        DamageSeverity::Cosmetic => "cosmetic",
        DamageSeverity::Moderate => "moderate",
        DamageSeverity::Serious => "serious",
    }
    .to_string()
}

// Global state for web UI.
pub static STATE: once_cell::sync::Lazy<Mutex<std::collections::HashMap<String, RipState>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

/// Operator-chosen TMDB title overrides, keyed by device. Set from the Ripper
/// card's "✎ change" picker BEFORE a manual rip; consumed once by `rip_disc`,
/// where it takes precedence over the scan's auto-match so the rip files under
/// the operator's pick (and counts as confident → no review hold).
pub static TITLE_OVERRIDES: once_cell::sync::Lazy<
    Mutex<std::collections::HashMap<String, crate::tmdb::TmdbResult>>,
> = once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

/// Record an operator title override for `device` (from the Ripper card picker).
pub fn set_title_override(device: &str, r: crate::tmdb::TmdbResult) {
    // Recover-and-proceed on poison (same convention as is_busy/update_state):
    // silently dropping the override would lose the operator's title pick.
    let mut m = TITLE_OVERRIDES.lock().unwrap_or_else(|e| e.into_inner());
    m.insert(device.to_string(), r);
}

/// Take (and clear) the operator title override for `device`, if any.
pub fn take_title_override(device: &str) -> Option<crate::tmdb::TmdbResult> {
    let mut m = TITLE_OVERRIDES.lock().unwrap_or_else(|e| e.into_inner());
    m.remove(device)
}

/// Stop cooldowns: device -> epoch seconds when cooldown expires.
pub(super) static STOP_COOLDOWNS: once_cell::sync::Lazy<
    Mutex<std::collections::HashMap<String, u64>>,
> = once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

const STOP_COOLDOWN_SECS: u64 = 5;

pub fn set_stop_cooldown(device: &str) {
    let now = crate::util::epoch_secs();
    // Recover-and-proceed on poison (same convention as is_busy/update_state).
    let mut cd = STOP_COOLDOWNS.lock().unwrap_or_else(|e| e.into_inner());
    cd.insert(device.to_string(), now + STOP_COOLDOWN_SECS);
}

pub(super) fn is_in_cooldown(device: &str) -> bool {
    let now = crate::util::epoch_secs();
    let cd = STOP_COOLDOWNS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(&expires) = cd.get(device) {
        return now < expires;
    }
    false
}

/// Drop the per-device entries in the auxiliary maps (`TITLE_OVERRIDES`,
/// `STOP_COOLDOWNS`) on hot-unplug. STATE/log/session are evicted by the
/// caller; these two maps are the only other per-device state, and without
/// this they'd accumulate stale entries if device paths churn over a long
/// container lifetime. Recover-and-proceed on poison (same convention as
/// the rest of this module).
pub(super) fn forget_device_state(device: &str) {
    TITLE_OVERRIDES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(device);
    STOP_COOLDOWNS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(device);
}

/// True when `device` is a known drive tracked in STATE. Used by routes
/// that mutate per-device state (e.g. the title override) to reject a
/// request for an unknown device with 404 rather than silently storing an
/// override for a drive that doesn't exist. Recovers a poisoned guard for
/// the same reason `is_busy` does (a stale poison must not make every
/// device look unknown).
pub fn device_known(device: &str) -> bool {
    let s = STATE.lock().unwrap_or_else(|e| e.into_inner());
    s.contains_key(device)
}

/// The disc display-name currently associated with `device` (from the live
/// state), if any. Used by the `Accept damage` handler to locate the disc's
/// staging dir without re-scanning.
pub fn current_disc_name(device: &str) -> Option<String> {
    let s = STATE.lock().unwrap_or_else(|e| e.into_inner());
    s.get(device).and_then(|r| {
        if r.disc_name.is_empty() {
            None
        } else {
            Some(r.disc_name.clone())
        }
    })
}

pub fn is_busy(device: &str) -> bool {
    // Recover the poisoned guard instead of treating poison as "not
    // busy". This is the double-rip guard: if a panic while holding
    // STATE poisoned the mutex, swallowing the error would make every
    // later is_busy() return false, opening the guards in ripper/mod.rs
    // (rip/scan dispatch) and letting a second rip launch concurrently
    // on the same drive. Matches the poison-recovery convention in
    // log.rs (`.lock().unwrap_or_else(|e| e.into_inner())`).
    let s = STATE.lock().unwrap_or_else(|e| e.into_inner());
    s.get(device)
        .map(|r| r.status == "scanning" || r.status == "ripping")
        .unwrap_or(false)
}

pub fn update_state(device: &str, mut state: RipState) {
    // 0.13.22: derive damage_severity from errors + total_lost_ms on
    // every push so the UI badge stays in sync with the latest counters.
    state.damage_severity = damage_severity_for(state.errors, state.total_lost_ms);

    // v0.25.7: maintain `started_epoch_secs` automatically so callers
    // don't have to remember to thread it through every RipState they
    // build. Most call sites (rip_disc, scan_disc, watchdog) drop a
    // fresh RipState into update_state with default zeros — without
    // this preservation step the UI's live elapsed-time counter
    // would reset on every state push.
    // Recover from a poisoned STATE mutex rather than silently dropping
    // the write — a dropped write freezes the dashboard on stale state
    // for the rest of the process. Matches `is_busy` / log.rs convention.
    let mut s = STATE.lock().unwrap_or_else(|e| e.into_inner());
    // Preserve the claim generation across pushes: callers build fresh
    // RipStates with `..Default::default()` (claim_gen = 0), so without this
    // carry-forward every state push would reset the generation and defeat the
    // stale-worker ownership check.
    let prev_claim_gen = s.get(device).map(|p| p.claim_gen).unwrap_or(0);
    if state.claim_gen == 0 {
        state.claim_gen = prev_claim_gen;
    }
    let prev_started = s.get(device).map(|p| p.started_epoch_secs).unwrap_or(0);
    let now_active = is_active_status(&state.status);
    let was_active = s.get(device).is_some_and(|p| is_active_status(&p.status));

    if state.started_epoch_secs == 0 {
        if now_active && was_active && prev_started > 0 {
            // Continuing an in-flight rip — keep the original start
            state.started_epoch_secs = prev_started;
        } else if now_active {
            // Transition into active — stamp now
            state.started_epoch_secs = crate::util::epoch_secs();
        }
        // else: idle / done / error / failed → leave at 0 (clears
        // the elapsed-counter in the UI)
    }
    s.insert(device.to_string(), state);
}

fn is_active_status(s: &str) -> bool {
    matches!(s, "scanning" | "ripping")
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
    // Recover from a poisoned STATE mutex rather than silently dropping
    // the mutation — see `update_state` / `is_busy` / log.rs.
    let mut s = STATE.lock().unwrap_or_else(|e| e.into_inner());
    let entry = s.entry(device.to_string()).or_insert_with(|| RipState {
        device: device.to_string(),
        ..Default::default()
    });
    f(entry);
    // Re-derive damage_severity after the mutation, matching `update_state`.
    // A closure that bumps `errors` / `total_lost_ms` (the patch-pass and
    // watchdog callbacks do) would otherwise leave a stale severity badge in
    // the UI, since this path skipped the re-derivation `update_state` does
    // on every push.
    entry.damage_severity = damage_severity_for(entry.errors, entry.total_lost_ms);
}

/// Atomically claim a device for active work. If it is already
/// `scanning`/`ripping`, returns `false` (the caller should reject with 409);
/// otherwise marks it `scanning` and returns `true`.
///
/// Folding the busy-check and the status-set into ONE `STATE` lock closes a
/// TOCTOU: the web handlers previously did a separate `is_busy`-style check and
/// then a separate `update_state`, so two concurrent POSTs could both observe
/// `idle` and both launch a rip on the same device (orphaned halt token +
/// concurrent writes to one staging dir). Poison-recovers like the rest of this
/// module. The caller may follow with a full `update_state` to populate the
/// remaining fields — the claim has already made the device read as busy.
pub fn try_claim_active(device: &str) -> bool {
    let mut s = STATE.lock().unwrap_or_else(|e| e.into_inner());
    if s.get(device)
        .map(|r| r.status == "scanning" || r.status == "ripping")
        .unwrap_or(false)
    {
        return false;
    }
    let entry = s.entry(device.to_string()).or_insert_with(|| RipState {
        device: device.to_string(),
        ..Default::default()
    });
    entry.status = "scanning".to_string();
    entry.disc_present = true;
    // Bump the claim generation so a previously-detached worker (e.g. a stale
    // verify) can detect that the device has been re-claimed and decline to
    // reset it to idle. Monotonic per device; saturating so it never wraps to
    // an old value mid-process.
    entry.claim_gen = entry.claim_gen.saturating_add(1);
    true
}

/// The device's current claim generation (0 if the device is unknown). A
/// detached worker reads this immediately after its own [`try_claim_active`]
/// and again before releasing the claim: if it changed, a newer owner has the
/// device and the worker must not reset it to idle.
pub fn current_claim_gen(device: &str) -> u64 {
    let s = STATE.lock().unwrap_or_else(|e| e.into_inner());
    s.get(device).map(|r| r.claim_gen).unwrap_or(0)
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
    pub(super) tmdb_media_type: String,
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
            return Some(cumulative + (lba - ext.start_lba) as u64 * SECTOR_BYTES);
        }
        cumulative += ext.sector_count as u64 * SECTOR_BYTES;
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

/// Build the **terminal** bad-range list (`Unreadable` only) — the done-card /
/// abort snapshot, where "bad" means the drive has finally given up. Thin
/// wrapper over [`located_ranges`].
pub(crate) fn build_bad_ranges(
    map: &libfreemkv::disc::mapfile::Mapfile,
    title: &libfreemkv::DiscTitle,
    bps: f64,
) -> (Vec<BadRange>, u32, u32, f64, f64) {
    located_ranges(
        map,
        title,
        bps,
        &[libfreemkv::disc::mapfile::SectorStatus::Unreadable],
    )
}

/// Build a located range list (LBA + sectors + duration + chapter) for the
/// given mapfile `statuses`, capped at 50 by duration (largest first); the
/// truncation count lets the UI say "+X more". The status set is the caller's:
/// terminal `Unreadable` for the verdict snapshot ([`build_bad_ranges`]), or the
/// full not-yet-good set (`NonTrimmed`/`NonScraped`/`Unreadable`) for the live
/// **Maybe** drilldown — so a patch pass shows WHERE it is working instead of a
/// black box. `NonTried` (unread, ahead of the head) is never included.
pub(crate) fn located_ranges(
    map: &libfreemkv::disc::mapfile::Mapfile,
    title: &libfreemkv::DiscTitle,
    bps: f64,
    statuses: &[libfreemkv::disc::mapfile::SectorStatus],
) -> (Vec<BadRange>, u32, u32, f64, f64) {
    let raw = map.ranges_with(statuses);
    let total_count = raw.len() as u32;
    let mut ranges: Vec<BadRange> = raw
        .iter()
        .map(|(pos, size)| {
            let lba = pos / SECTOR_BYTES;
            let count = (size / SECTOR_BYTES) as u32;
            let duration_ms = if bps > 0.0 {
                (*size as f64) / bps * MILLIS_PER_SEC
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

// `RipProgress` / `from_map` were deleted in the 1.2.0 mapfile-free rework:
// the live `push_pass_state` now reads the rendered drilldown straight from
// `PassProgress.located` (computed by libfreemkv), so autorip no longer parses
// the mapfile on the hot path.

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
    /// Hold the displayed-speed window at a fixed 10s (instead of growing to
    /// 60s) for bursty PATCH passes, so the speed stays responsive. Set by
    /// `push_pass_state` from the pass number; the steady sweep leaves it false.
    pub(super) responsive: bool,
    /// `bytes_unreadable` snapshotted on this pass's first
    /// `push_pass_state` callback, frozen for the rest of the pass. The
    /// total-progress denominator (`max_retries × bytes_lost`) uses this
    /// frozen value instead of the live mapfile figure: during Pass 1
    /// `bytes_unreadable` grows from 0 as new bad sectors are discovered,
    /// so a live read inflated the denominator mid-pass and made
    /// `total_pct` visibly stall or regress. Re-snapshotted each pass (a
    /// fresh `PassProgressState` is built per pass), so the estimate
    /// still tightens pass-to-pass.
    pub(super) frozen_bytes_lost: Option<u64>,
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
/// ```text
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
            responsive: false,
            frozen_bytes_lost: None,
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
        // Patch passes are bursty (grind one sector, then zip the readable
        // overshoot), so they hold a fixed 10s window — the displayed speed
        // stays responsive and a fast-capture burst shows up fast instead of
        // being diluted over a minute. The sweep is steady, so it keeps the
        // growing 10s→60s window that smooths a transient stall.
        let window_secs = if self.responsive {
            STATIC_WINDOW_SECS
        } else {
            display_window_secs(elapsed_pass)
        };
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
        let mbs = bytes as f64 / BYTES_PER_MIB / dt;
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
        let mbs = bytes as f64 / BYTES_PER_MIB / elapsed;
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
    p: &libfreemkv::progress::PassProgress,
    bps: f64,
    pass: u8,
    total_passes: u8,
    state: &std::cell::RefCell<PassProgressState>,
) {
    // Buckets + located drilldown come straight from the library's progress
    // contract (`p`) — autorip no longer reads or parses the mapfile. GOOD =
    // Finished, MAYBE = retry-eligible (NonTrimmed/NonScraped), LOST = terminal
    // Unreadable.
    let bytes_good = p.bytes_good_total;
    let bytes_maybe = p.bytes_retryable_total;
    let bytes_lost = p.bytes_unreadable_total;
    let total_lost_ms = if bps > 0.0 {
        bytes_lost as f64 * MILLIS_PER_SEC / bps
    } else {
        0.0
    };
    // The terminal Unreadable-in-main figure is owned by the done-card verdict
    // (resume.rs); it's structurally 0 mid-rip (Unreadable is promoted only
    // after the final pass), so the live state reports 0 here and the UI reads
    // `p.located.main_at_risk_ms` for the honest at-risk time instead.
    let main_lost_ms = 0.0;
    // Freeze `bytes_unreadable` on this pass's first callback for use as
    // the total-progress retry-work term. Re-reading it live each
    // callback let the Pass-1 denominator grow as bad sectors were
    // discovered, making total_pct stall/regress mid-pass. The red-pill
    // `bytes_lost` above stays live (terminal-bad truth); only the
    // total-progress estimate uses this frozen figure.
    let retry_denom_bytes = {
        let mut s = state.borrow_mut();
        *s.frozen_bytes_lost.get_or_insert(bytes_lost)
    };
    // `errors` is the user-visible skipped-sector count: terminal-bad
    // sectors only (`bytes_lost`). Pending bytes are not "errors" — they
    // may still recover.
    let errors = (bytes_lost / SECTOR_BYTES) as u32;
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
        .saturating_add(cfg_max_retries.saturating_mul(retry_denom_bytes))
        .saturating_add(mux_estimate_bytes);
    // Cumulative work done across all passes:
    //   pass 1: total_done = last_pos
    //   pass>=2 (retry): total_done = capacity + (pass-2) × bytes_lost + last_pos
    // Numerator uses the same frozen `retry_denom_bytes` as the
    // denominator so prior-pass retry work and total work stay consistent.
    let total_done: u64 = if pass <= 1 {
        last_pos
    } else {
        let prior_retry_count = pass.saturating_sub(2) as u64;
        ctx.bytes_total_disc
            .saturating_add(prior_retry_count.saturating_mul(retry_denom_bytes))
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
        // Patch passes (pass > 1) hold a fixed 10s speed window — bursty
        // recovery should read responsively, not be smoothed over a minute.
        s.responsive = pass > 1;
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
                // Very long ETA (>100 h, e.g. a 489 MB bad set grinding at
                // ~12 KB/s). Show days+hours so the field is never blank —
                // the operator still wants "≈ 8d" over an empty gap.
                format!("{}d{}h", secs / 86_400, (secs % 86_400) / 3600)
            }
        };
        // ETA floor is 0.1 KB/s (0.0001 MB/s), not the old 10 KB/s (0.01
        // MB/s). That floor blanked the ETA right at the patch rate (~12
        // KB/s hovers on the threshold) — exactly when the operator most
        // wants a number. Any forward motion now yields an ETA.
        let pass_eta = if eta_speed > 0.0001 && last_work_total > last_pos {
            let rem_mb = (last_work_total - last_pos) as f64 / BYTES_PER_MIB;
            format_secs((rem_mb / eta_speed) as u64)
        } else {
            String::new()
        };
        let total_eta = if eta_speed > 0.0001 && total_work_estimated > total_done {
            let rem_mb = (total_work_estimated - total_done) as f64 / BYTES_PER_MIB;
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
            progress_gb: last_pos as f64 / BYTES_PER_GIB,
            // Populate the documented last_sector (LBA) during sweep too,
            // not just mux. `last_pos` is the swept byte offset; dividing by
            // the sector size yields the equivalent LBA the UI playhead
            // expects. Previously left at Default (0), so the playhead never
            // moved during the sweep phase despite the field being documented.
            last_sector: last_pos / SECTOR_BYTES,
            speed_mbs,
            eta,
            errors,
            lost_video_secs: total_lost_ms / MILLIS_PER_SEC,
            output_file: ctx.filename.clone(),
            tmdb_title: ctx.tmdb_title.clone(),
            tmdb_year: ctx.tmdb_year,
            tmdb_poster: ctx.tmdb_poster.clone(),
            tmdb_overview: ctx.tmdb_overview.clone(),
            tmdb_media_type: ctx.tmdb_media_type.clone(),
            duration: ctx.duration.clone(),
            codecs: ctx.codecs.clone(),
            pass,
            total_passes,
            bytes_good,
            bytes_maybe,
            bytes_lost,
            bytes_total_disc: ctx.bytes_total_disc,
            // Live drilldown shows the located MAYBE ranges (pending + lost), so
            // a patch pass is visible instead of a black box. Rendered by the
            // library (`p.located`); autorip only maps it to its JSON DTO.
            bad_ranges: p
                .located
                .ranges
                .iter()
                .map(|r| BadRange {
                    lba: r.lba,
                    count: r.count,
                    duration_ms: r.duration_ms,
                    chapter: r.chapter,
                    time_offset_secs: r.time_offset_secs,
                })
                .collect(),
            num_bad_ranges: p.located.num_ranges,
            bad_ranges_truncated: p.located.truncated,
            total_lost_ms,
            main_lost_ms,
            main_at_risk_ms: p.located.main_at_risk_ms,
            largest_gap_ms: p.located.largest_gap_ms,
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
            let pos_gb = last_pos as f64 / BYTES_PER_GIB;
            let good_gb = bytes_good as f64 / BYTES_PER_GIB;
            let total_gb = ctx.bytes_total_disc as f64 / BYTES_PER_GIB;
            let speed_str = if speed_mbs >= 1.0 {
                format!("{speed_mbs:.1} MB/s")
            } else {
                format!("{:.0} KB/s", speed_mbs * 1024.0)
            };
            let bad_str = if bytes_lost > 0 {
                format!(
                    ", {} skipped ({:.2} MB)",
                    errors,
                    bytes_lost as f64 / BYTES_PER_MIB
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
/// everything the UI needs to render pass progress. The immutable per-rip
/// fields (disc / TMDB / batch / capacity) come from `ctx`; the rest are the
/// per-pass dynamic values. Status is always "ripping" during the passes;
/// pass=total_passes indicates the mux phase.
pub(super) fn set_pass_progress(
    ctx: &PassContext,
    pass: u8,
    total_passes: u8,
    bytes_good: u64,
    bytes_maybe: u64,
    bytes_lost: u64,
) {
    let pct = if ctx.bytes_total_disc > 0 {
        (bytes_good * 100 / ctx.bytes_total_disc).min(100) as u8
    } else {
        0
    };
    // Use update_state_with instead of a full RipState replacement so that
    // the CUMULATIVE fields (total_progress_*, errors, total_lost_ms,
    // bad_ranges, etc.) survive the pass boundary — a full RipState with
    // ..Default::default() would zero them, dropping the *total* bar to 0 at
    // every pass. The PER-PASS bar (pass_progress_pct) + ETA/speed, by
    // contrast, ARE reset below: they belong to a single pass, so carrying
    // pass 1's 99% made pass 2 read "pass 1/7 · 99%" through the settle.
    // push_pass_state callbacks refill them once the new pass is reading.
    update_state_with(&ctx.device, |s| {
        s.status = "ripping".to_string();
        s.disc_present = true;
        s.disc_name = ctx.display_name.clone();
        s.disc_format = ctx.disc_format.clone();
        s.progress_pct = pct;
        s.progress_gb = bytes_good as f64 / BYTES_PER_GIB;
        s.output_file = ctx.filename.clone();
        s.tmdb_title = ctx.tmdb_title.clone();
        s.tmdb_year = ctx.tmdb_year;
        s.tmdb_poster = ctx.tmdb_poster.clone();
        s.tmdb_overview = ctx.tmdb_overview.clone();
        s.tmdb_media_type = ctx.tmdb_media_type.clone();
        s.duration = ctx.duration.clone();
        s.codecs = ctx.codecs.clone();
        s.pass = pass;
        s.total_passes = total_passes;
        s.bytes_good = bytes_good;
        s.bytes_maybe = bytes_maybe;
        s.bytes_lost = bytes_lost;
        s.bytes_total_disc = ctx.bytes_total_disc;
        s.preferred_batch = ctx.batch;
        s.current_batch = ctx.batch;
        // Reset the PER-PASS bar + its ETA/speed at the pass boundary so a new
        // pass starts visibly at 0% rather than inheriting the prior pass's
        // 99% (which made pass 2 read "pass 1/7 · 99% · ETA 0s" through the
        // 30 s settle). The cumulative `total_progress_pct` is intentionally
        // left untouched. push_pass_state refills these on its first tick.
        s.pass_progress_pct = 0;
        s.pass_eta = String::new();
        s.eta = String::new();
        s.speed_mbs = 0.0;
    });
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

    /// Create a throwaway mapfile inside a fresh `TempDir`. The returned
    /// `TempDir` guard must be held for the test's lifetime; its Drop
    /// removes the directory (and the mapfile) so temp_dir() doesn't
    /// accumulate `autorip-ripper-test-*.mapfile` artifacts across runs.
    fn tmp_map(tag: &str, total: u64) -> (tempfile::TempDir, Mapfile) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("{tag}.mapfile"));
        let map = Mapfile::create(&path, total, "test").unwrap();
        (dir, map)
    }

    /// Regression guard: the single-pass done card must feed the real
    /// in-title loss (lost_video_secs * 1000) into the damage classifier,
    /// not the all-zero `sweep_damage_snapshot.total_lost_ms` (which is the
    /// Default in direct mode, since single-pass has no mapfile).
    ///
    /// A rip that skipped only a handful of sectors but each covered a
    /// large unit at low bitrate can lose >1s of video. With total_lost_ms
    /// starved to 0.0, classify_damage's ms-branch never fires and the rip
    /// is mis-rated "cosmetic" (10 < 51 sector threshold) when it should be
    /// "moderate" (>=1000 ms lost).
    #[test]
    fn single_pass_done_card_total_lost_ms_drives_severity() {
        // 10 skipped sectors -> below the 51-sector Moderate threshold, so
        // severity is decided purely by the ms-branch.
        let errors: u32 = 10;
        let lost_video_secs: f64 = 1.5; // 1500 ms lost

        // Buggy behavior: total_lost_ms starved to 0.0 -> Cosmetic.
        assert_eq!(
            damage_severity_for(errors, 0.0),
            "cosmetic",
            "starved total_lost_ms under-classifies a >1s loss"
        );

        // Fixed behavior: feed the real loss -> Moderate.
        assert_eq!(
            damage_severity_for(errors, lost_video_secs * MILLIS_PER_SEC),
            "moderate",
            "single-pass done card must derive total_lost_ms from lost_video_secs"
        );
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

    /// A title whose feature occupies one extent of `sector_count` sectors
    /// starting at `start_lba` — i.e. bytes `[start_lba*2048, (start_lba+count)*2048)`.
    fn title_with_extent(start_lba: u32, sector_count: u32) -> libfreemkv::DiscTitle {
        let mut t = minimal_title();
        t.extents = vec![libfreemkv::Extent {
            start_lba,
            sector_count,
        }];
        t
    }

    // The live at-risk / located-drilldown behaviour these tests used to cover
    // (in-feature pending counts as at-risk; out-of-feature reads 0:00; the live
    // drilldown shows pending while the terminal verdict hides it) moved into
    // libfreemkv with `RipProgress::from_map` — it's now exercised by
    // `locate_ranges` tests in libfreemkv (src/disc/mod.rs). The terminal
    // `build_bad_ranges` path (still autorip-side, for the done card) keeps its
    // own coverage below.

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
    fn pass_progress_zero_dt_leaves_speed_unchanged() {
        // Two calls at the same instant must not divide by zero, and a
        // zero-dt sample must leave the smoothed speed untouched.
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

    fn minimal_pass_ctx(device: &str) -> PassContext {
        PassContext {
            device: device.to_string(),
            display_name: "Test Disc".to_string(),
            disc_format: "uhd".to_string(),
            tmdb_title: String::new(),
            tmdb_year: 0,
            tmdb_poster: String::new(),
            tmdb_overview: String::new(),
            tmdb_media_type: String::new(),
            duration: String::new(),
            codecs: String::new(),
            filename: "test.mkv".to_string(),
            bytes_total_disc: 50 * 1_073_741_824, // 50 GB
            batch: 32,
            max_retries: 5,
        }
    }

    /// Regression: set_pass_progress must not zero total_progress_pct /
    /// total_progress_eta that were set by a previous pass's push_pass_state.
    /// Before the fix, the `..Default::default()` in the full RipState
    /// replacement zeroed those fields at the start of every new pass, causing
    /// the UI total-progress bar to visibly drop to 0 between passes.
    #[test]
    fn set_pass_progress_preserves_total_progress_fields() {
        let dev = format!("test-spp-preserve-{}", std::process::id());
        // Simulate what push_pass_state would have written at the end of Pass 1.
        update_state_with(&dev, |s| {
            s.status = "ripping".to_string();
            s.total_progress_pct = 48;
            s.total_eta = "1:30:00".to_string();
            s.pass_progress_pct = 100;
            s.errors = 12;
            s.total_lost_ms = 500.0;
        });
        // Now call set_pass_progress as it is at the start of Pass 2.
        let ctx = minimal_pass_ctx(&dev);
        set_pass_progress(
            &ctx,
            2,                  // pass
            7,                  // total_passes
            40 * 1_073_741_824, // bytes_good
            1_048_576,          // bytes_maybe
            2048,               // bytes_lost
        );
        let snap = STATE
            .lock()
            .unwrap()
            .get(&dev)
            .cloned()
            .expect("entry must exist");
        // These fields must survive the pass-boundary update.
        assert_eq!(
            snap.total_progress_pct, 48,
            "total_progress_pct must not be zeroed by set_pass_progress"
        );
        assert_eq!(
            snap.total_eta, "1:30:00",
            "total_eta must not be cleared by set_pass_progress"
        );
        // pass-specific fields are updated to the new pass.
        assert_eq!(snap.pass, 2, "pass not updated");
        assert_eq!(snap.total_passes, 7, "total_passes not updated");
        // damage fields must also survive (were written by push_pass_state).
        assert_eq!(
            snap.errors, 12,
            "errors must not be zeroed by set_pass_progress"
        );
        assert!(
            (snap.total_lost_ms - 500.0).abs() < 0.001,
            "total_lost_ms must not be zeroed by set_pass_progress"
        );
    }

    /// Regression: the post-promotion damage snapshot pushed via
    /// update_state_with must reflect the final Unreadable sectors
    /// (NonTrimmed promoted → Unreadable) and produce non-zero damage
    /// fields.  This guards the build_bad_ranges + update_state_with
    /// pattern used in mod.rs after the promotion+flush block.
    #[test]
    fn post_promotion_damage_push_is_non_zero_for_damaged_rip() {
        let dev = format!("test-promo-damage-{}", std::process::id());
        // Start with a "clean" state — as push_pass_state would leave it
        // if the last pass saw everything as NonTrimmed (not yet promoted).
        update_state_with(&dev, |s| {
            s.errors = 0;
            s.total_lost_ms = 0.0;
            s.bad_ranges = vec![];
            s.num_bad_ranges = 0;
        });
        // Build a mapfile with some Unreadable sectors (as if promotion
        // already ran and converted NonTrimmed → Unreadable). Total must
        // cover the highest byte position we record at: sector 30050 ×
        // 2048 = 61,542,400 bytes → round up to 100,000 sectors × 2048.
        let total_bytes = 100_000u64 * 2048;
        let (_dir, mut map) = tmp_map("promo-damage", total_bytes);
        // Record two separate Unreadable ranges (by byte position).
        map.record(5_000 * 2048, 200 * 2048, SectorStatus::Unreadable)
            .unwrap();
        map.record(30_000 * 2048, 50 * 2048, SectorStatus::Unreadable)
            .unwrap();
        let title = minimal_title();
        let bps = 40_000.0 * 2048.0; // 40k sectors/s

        // Mirror the fix: re-derive damage from the promoted map and push.
        let (bad_ranges, num_bad, truncated, total_lost_ms, largest_gap_ms) =
            build_bad_ranges(&map, &title, bps);
        let main_title_bad = map.ranges_with(&[SectorStatus::Unreadable]);
        let main_bad_bytes = libfreemkv::disc::bytes_bad_in_title(&title, &main_title_bad);
        let main_lost_ms = if bps > 0.0 {
            main_bad_bytes as f64 * MILLIS_PER_SEC / bps
        } else {
            0.0
        };
        let errors = (map.stats().bytes_unreadable / 2048) as u32;
        update_state_with(&dev, |s| {
            s.errors = errors;
            s.total_lost_ms = total_lost_ms;
            s.main_lost_ms = main_lost_ms;
            s.bad_ranges = bad_ranges;
            s.num_bad_ranges = num_bad;
            s.bad_ranges_truncated = truncated;
            s.largest_gap_ms = largest_gap_ms;
        });

        let snap = STATE
            .lock()
            .unwrap()
            .get(&dev)
            .cloned()
            .expect("entry must exist");
        // The marker_damage read from STATE must see non-zero damage.
        assert_eq!(
            snap.errors, 250,
            "errors must reflect promoted unreadable sectors"
        );
        assert!(
            snap.total_lost_ms > 0.0,
            "total_lost_ms must be non-zero after promotion push"
        );
        assert_eq!(
            snap.num_bad_ranges, 2,
            "num_bad_ranges must reflect both unreadable ranges"
        );
        assert!(snap.largest_gap_ms > 0.0, "largest_gap_ms must be non-zero");
    }

    #[test]
    fn spawn_failure_reset_to_idle_clears_busy() {
        // HIGH: handle_scan/handle_rip set status="scanning" BEFORE spawning
        // the worker. If the spawn fails the handlers now roll the device
        // back to idle. Pin the contract the rollback relies on: an idle
        // push clears is_busy so the next scan/rip isn't rejected with 409.
        let dev = format!("test-spawnfail-{}", std::process::id());
        // Pre-state set by the handler before spawn.
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "scanning".to_string(),
                ..Default::default()
            },
        );
        assert!(is_busy(&dev), "scanning device must read as busy");
        // The exact rollback the handlers perform on spawn failure.
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "idle".to_string(),
                ..Default::default()
            },
        );
        assert!(
            !is_busy(&dev),
            "after spawn-failure reset the device must no longer be busy \
             (else every future scan/rip 409s until restart)"
        );
    }

    #[test]
    fn forget_device_state_clears_title_override_and_cooldown() {
        // Regression: on hot-unplug, TITLE_OVERRIDES and STOP_COOLDOWNS were
        // the only per-device maps not evicted, so stale entries accumulated
        // as device paths churned. forget_device_state must drop both.
        let dev = "/dev/sg-forget-test";
        set_title_override(
            dev,
            crate::tmdb::TmdbResult {
                title: "Test".to_string(),
                year: 2000,
                poster_url: String::new(),
                overview: String::new(),
                media_type: "movie".to_string(),
            },
        );
        set_stop_cooldown(dev);
        assert!(is_in_cooldown(dev), "cooldown must be set before eviction");

        forget_device_state(dev);

        assert!(
            !TITLE_OVERRIDES
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(dev),
            "title override must be gone after forget_device_state"
        );
        assert!(
            !STOP_COOLDOWNS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(dev),
            "stop cooldown must be gone after forget_device_state"
        );
        assert!(
            !is_in_cooldown(dev),
            "device must not read as in cooldown after eviction"
        );
    }
}
