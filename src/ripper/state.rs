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
// the frontend rework rather than half-wired.
/// State broadcast for web UI.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RipState {
    pub device: String,
    pub status: String, // "idle", "scanning", "ripping", "moving", "done", "error"
    pub disc_present: bool,
    pub disc_name: String,
    /// The disc's RAW volume label (`DiscId::name()`), before the TMDB lookup.
    ///
    /// [`Self::disc_name`] is the TMDB-resolved title, and it is deliberately
    /// NOT unique per disc: `tmdb::clean_title` strips "disc 1".."disc 4"
    /// before the lookup, so every disc of a boxset resolves to one title and
    /// wants one staging directory. This is the field that still tells them
    /// apart, and it is what
    /// [`staging::staging_name_for_disc`](crate::ripper::staging::staging_name_for_disc)
    /// uses to decide whether an existing staging dir belongs to THIS disc.
    ///
    /// Server-side bookkeeping only — not serialized, the UI shows the TMDB
    /// title. Carried forward across state pushes by [`update_state`] (see
    /// there), because nearly every caller builds a fresh `RipState`.
    #[serde(skip)]
    pub disc_label: String,
    /// This device's terminal state is a DEFERRAL, not a failure: the work
    /// stopped for a reason that fixes itself (keys arrive), staging is
    /// intact, and the next pass will pick it up unchanged.
    ///
    /// Set only by the deferral exits themselves. The alternative — inferring
    /// it from `status == "idle"` — is wrong, and was: `"idle"` is also what
    /// several HARD failures in `resume_remux` write (no title after key
    /// resolution, an unreadable mapfile, an over-threshold loss abort), so a
    /// corrupt ISO was presented to the operator as "no keys yet, it'll mux
    /// itself". Status says how the device LOOKS; this says what happened.
    ///
    /// Server-side bookkeeping only — not serialized. Deliberately NOT
    /// carried forward across state pushes: it describes one terminal push.
    #[serde(skip)]
    pub failure_deferred: bool,
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
            disc_label: String::new(),
            failure_deferred: false,
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
/// fields. Wraps freemkv-engine's `classify_damage` so the UI gets a stable
/// lowercase string ("clean" / "cosmetic" / "moderate" / "serious").
pub(super) fn damage_severity_for(errors: u32, total_lost_ms: f64) -> String {
    use freemkv_engine::DamageSeverity;
    // Direct match instead of round-tripping through serde_json::to_value
    // on every (throttled) progress callback. Strings match libfreemkv's
    // `#[serde(rename_all = "lowercase")]` repr so the UI is unchanged.
    match freemkv_engine::classify_damage(errors as u64, total_lost_ms) {
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

/// Stop cooldowns: device -> the MONOTONIC instant the cooldown expires.
///
/// `Instant`, not an `epoch_secs()` deadline. This is a 5-second interval, and
/// a wall-clock deadline is only as good as the wall clock: a backward step
/// bigger than the window (NTP correction, host clock reset, VM snapshot
/// resume) keeps `now < expires` true until the clock catches up, and
/// `insert_tick` suppresses the auto-scan/auto-rip dispatch for that whole
/// time — an unattended box quietly ignoring the disc in the drive. `Instant`
/// cannot step backwards.
pub(super) static STOP_COOLDOWNS: once_cell::sync::Lazy<
    Mutex<std::collections::HashMap<String, std::time::Instant>>,
> = once_cell::sync::Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

const STOP_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(5);

pub fn set_stop_cooldown(device: &str) {
    let expires = std::time::Instant::now() + STOP_COOLDOWN;
    // Recover-and-proceed on poison (same convention as is_busy/update_state).
    let mut cd = STOP_COOLDOWNS.lock().unwrap_or_else(|e| e.into_inner());
    cd.insert(device.to_string(), expires);
}

pub(super) fn is_in_cooldown(device: &str) -> bool {
    let now = std::time::Instant::now();
    let cd = STOP_COOLDOWNS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(&expires) = cd.get(device) {
        return now < expires;
    }
    false
}

/// Drop the auxiliary per-device state on hot-unplug, so nothing accumulates
/// as device paths churn over a long container lifetime.
///
/// The complete per-device inventory, and who evicts each map — this list is
/// the contract, so add to it when a new per-device map appears:
///
/// | map | where | evicted by |
/// |---|---|---|
/// | `STATE` | `state.rs` | `forget_removed_device` directly |
/// | log ring | `log.rs` | `forget_removed_device` (`log::forget_device`) |
/// | `SESSIONS` | `session.rs` | `forget_removed_device` (`drop_session`) |
/// | `TITLE_OVERRIDES` | `state.rs` | here |
/// | `STOP_COOLDOWNS` | `state.rs` | here |
/// | `DISC_IDENTITY` | `session.rs` | here, via `forget_device_session_state` |
/// | `RIP_THREADS` | `session.rs` | here, via `forget_device_session_state` (finished handles only) |
/// | `HALTS` | `session.rs` | here, via `forget_device_session_state` (with the handle) |
///
/// The doc this replaces claimed `TITLE_OVERRIDES` and `STOP_COOLDOWNS` were
/// "the only other per-device state". They are not: `DISC_IDENTITY` had no
/// remover anywhere in the crate, and a torn-down device's finished
/// `JoinHandle` sat in `RIP_THREADS` forever — one entry per device path the
/// kernel ever handed out.
///
/// Recover-and-proceed on poison (same convention as the rest of this module).
pub(super) fn forget_device_state(device: &str) {
    TITLE_OVERRIDES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(device);
    STOP_COOLDOWNS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(device);
    super::session::forget_device_session_state(device);
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

// `current_disc_name` lived here: the display name alone, used by the
// `Accept damage` handler to locate a staging dir. Removed with the boxset
// fix — the display name is NOT enough to identify a disc (every disc of a set
// shares one TMDB title), and every staging lookup now goes through
// `ripper::staging_basename_for_device`, which reads the raw volume label too.
// Reintroducing a name-only accessor invites the same bug back.

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
    // Preserve the raw volume label across pushes, for the same reason as
    // `claim_gen`: nearly every caller drops a fresh `RipState` with
    // `..Default::default()` into `update_state` and sets only `disc_name`.
    // Without this carry-forward the label — the ONLY thing distinguishing two
    // discs of a boxset, which share one TMDB title — would be erased by the
    // first progress push, and every staging lookup after that would collapse
    // back onto disc 1's directory.
    //
    // Guarded on the display name being unchanged (and non-empty): a state
    // push for a different disc, or for an empty/ejected drive, must NOT
    // inherit the previous disc's label and claim its staging dir.
    if state.disc_label.is_empty()
        && !state.disc_name.is_empty()
        && let Some(prev) = s.get(device)
        && prev.disc_name == state.disc_name
    {
        state.disc_label = prev.disc_label.clone();
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
///
/// Thin wrapper over [`try_claim_active_checked`] with `known = true` —
/// every existing call site (the poll loop's own enumerated device list;
/// `handle_scan`/`handle_rip`/`handle_eject` in web.rs) is left with
/// identical behaviour. See `try_claim_active_checked`'s doc for why a
/// caller that has NOT independently verified the device is a real,
/// enumerated drive must use that function instead.
pub fn try_claim_active(device: &str) -> Option<u64> {
    try_claim_active_checked(device, true)
}

/// Same contract as [`try_claim_active`], but `known` tells it whether the
/// caller has already verified `device` names a real, currently-enumerated
/// drive. When `known` is `false` and the device has no existing STATE
/// entry, the claim is refused instead of creating one.
///
/// Why this matters: `device` on the web path is caller-supplied (the URL
/// segment of `/api/scan/{device}`, `/api/rip/{device}`,
/// `/api/eject/{device}`) and is only shape-checked by
/// `web.rs::is_valid_device_name` — a 3..=64-character ASCII-alphanumeric
/// check, explicitly documented there as NOT an existence check. This
/// server binds `0.0.0.0:8080` with no authentication, so any LAN host can
/// loop `POST /api/scan/<random-alnum-name>`. Before this guard, every such
/// call reached the old `try_claim_active`'s `.entry(...).or_insert_with(..)`
/// and created a brand-new, permanent `RipState` — and, once `handle_scan`'s
/// `spawn_rip_thread` follows a successful claim, a permanent `JoinHandle` in
/// `session::RIP_THREADS` too. Nothing ever prunes either: the hot-unplug
/// sweep in `drive_poll_loop` only removes devices that were previously in
/// its own enumerated `drive_paths` list, and a forged name never enters
/// that list. The two maps would grow without bound until the process is
/// OOM-killed.
///
/// A device that legitimately exists always has a STATE entry already —
/// `drive_poll_loop` pushes one (idle or with a disc) on every poll tick for
/// every device it enumerates, before the operator can ever see it in the
/// dashboard (the UI only offers Scan/Rip/Eject for devices `/api/state`
/// already reports) — so gating the *new-entry* path on `known` does not
/// affect any real device, only a name nothing has ever enumerated.
///
/// `known` should be `true` for the poll loop's own internal claim (its
/// `device` always comes from a just-enumerated `drive_paths` entry) and for
/// any caller that has cross-checked `device` against the live drive list.
/// Callers that receive `device` verbatim from an untrusted request and
/// have NOT done that cross-check must pass `false`.
///
/// # The claim needs TWO facts, not one
///
/// The claim refuses a device whose `STATE` status is scanning/ripping **and**
/// a device whose rip thread is still alive. Those are independent facts. A
/// worker writes its TERMINAL status (`done` / `error`) and then keeps running
/// its tail on the same thread: auto-eject (`Drive::open` + `eject()`, real
/// hardware I/O), the eject-failure device-log lines, guard drops. For that
/// whole window `is_busy` is false while the worker still owns the drive, the
/// staging dir and the device's `Halt` token.
///
/// Checking status alone was directly reachable from the network. This server
/// binds `0.0.0.0` with no authentication, so a `POST /api/rip/sr0` landing in
/// that tail won the claim, `spawn_rip_after_claim` overwrote the incumbent's
/// `Halt` with a fresh token (so the incumbent's trailing `unregister_halt`
/// then deleted the NEW rip's token and `/api/stop` could not cancel it), and
/// the duplicate worker ran until `register_rip_thread` rejected it. The other
/// half of that fix lives in [`super::session::spawn_rip_thread`], which now
/// decides registration before the worker runs any work; this half stops the
/// duplicate from being admitted in the first place.
///
/// **Ordering, and why it cannot deadlock.** The liveness question is asked
/// BEFORE `STATE` is locked, so `RIP_THREADS` and `STATE` are never held at
/// the same time and no lock-order inversion is possible; `rip_thread_running`
/// only reads `JoinHandle::is_finished`, never `join`, so it cannot block
/// either. Splitting the two checks is not a TOCTOU in the dangerous
/// direction: a handle can only APPEAR for a device after some caller wins
/// this very claim, so a `false -> true` transition between our read and our
/// `STATE` lock implies a competitor already took the `STATE` lock and our own
/// claim fails anyway. The benign direction (`true -> false`: the worker
/// exited in between) merely admits a claim for a device that is now genuinely
/// free, which is correct.
///
/// The accepted cost is the same one `forget_removed_device` already accepts:
/// a worker that hangs forever keeps its device unclaimable. A finished
/// handle reads as not-running, so an ordinary completed rip frees the device
/// the instant the thread exits, with no reaping required first.
pub fn try_claim_active_checked(device: &str, known: bool) -> Option<u64> {
    // Liveness first, and OUTSIDE the STATE lock (see the doc above for both
    // the why and the ordering argument).
    if super::session::rip_thread_running(device) {
        tracing::warn!(
            device = %device,
            "refusing claim: a worker thread for this device is still running \
             (its status is already terminal, but it has not exited yet)"
        );
        return None;
    }
    let mut s = STATE.lock().unwrap_or_else(|e| e.into_inner());
    if s.get(device)
        .map(|r| r.status == "scanning" || r.status == "ripping")
        .unwrap_or(false)
    {
        return None;
    }
    if !known && !s.contains_key(device) {
        return None;
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
    // The generation IS the claim's identity, and it is returned so the caller
    // can hand it to `rollback_failed_spawn` and undo THIS claim and no other.
    // See that function for the wedge that a device-only rollback produced.
    Some(entry.claim_gen)
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
    /// (freemkv_engine::sweep uses a fixed size); current_batch == preferred_batch
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
        // `start_lba` / `sector_count` are disc-supplied (UDF allocation
        // descriptors), so their sum is untrusted arithmetic: a corrupt or
        // hostile image can overflow it, panicking the rip thread in debug and
        // wrapping the containment test in release. Widen to u64 — an extent
        // whose end does not fit in u32 simply cannot contain a u32 LBA past
        // it, and this keeps the comparison honest instead of wrapped.
        let end = ext.start_lba as u64 + ext.sector_count as u64;
        if lba >= ext.start_lba && (lba as u64) < end {
            return Some(cumulative + (lba - ext.start_lba) as u64 * SECTOR_BYTES);
        }
        cumulative += ext.sector_count as u64 * SECTOR_BYTES;
    }
    None
}

fn range_chapter(lba: u32, title: &libfreemkv::DiscTitle) -> (Option<u32>, Option<f64>) {
    if let Some(byte_offset) = byte_offset_in_title(lba, title)
        && let Some((ch, t)) = libfreemkv::disc::chapter_at_offset(
            &title.chapters,
            byte_offset,
            title.duration_secs,
            title.size_bytes,
        )
    {
        return (Some(ch as u32), Some(t));
    }
    (None, None)
}

/// Build the **terminal** bad-range list (`Unreadable` only) — the done-card /
/// abort snapshot, where "bad" means the drive has finally given up. Thin
/// wrapper over [`located_ranges`].
pub(crate) fn build_bad_ranges(
    map: &freemkv_engine::Mapfile,
    title: &libfreemkv::DiscTitle,
    bps: f64,
) -> (Vec<BadRange>, u32, u32, f64, f64) {
    located_ranges(map, title, bps, &[freemkv_engine::SectorStatus::Unreadable])
}

/// Build a located range list (LBA + sectors + duration + chapter) for the
/// given mapfile `statuses`, capped at 50 by duration (largest first); the
/// truncation count lets the UI say "+X more". The status set is the caller's:
/// terminal `Unreadable` for the verdict snapshot ([`build_bad_ranges`]), or the
/// full not-yet-good set (`NonTrimmed`/`NonScraped`/`Unreadable`) for the live
/// **Maybe** drilldown — so a patch pass shows WHERE it is working instead of a
/// black box. `NonTried` (unread, ahead of the head) is never included.
pub(crate) fn located_ranges(
    map: &freemkv_engine::Mapfile,
    title: &libfreemkv::DiscTitle,
    bps: f64,
    statuses: &[freemkv_engine::SectorStatus],
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

/// Per-pass progress state. The speed/ETA math (windowed display speed +
/// pass-start ETA rate, and why a sliding window beats an EWMA through a stall)
/// now lives in `freemkv_engine::SpeedEstimator` — see its docs; this struct
/// just holds one estimator plus autorip's own per-pass bookkeeping. Held in a
/// RefCell inside the callback closure so interior mutability keeps the closure
/// `Fn`.
#[derive(Debug)]
pub(super) struct PassProgressState {
    /// The engine's canonical speed/ETA estimator (windowed display speed +
    /// pass-start ETA rate + responsive mode). Autorip's finely-tuned speed
    /// math was promoted into `freemkv_engine::SpeedEstimator` so every
    /// front-end shares it; this holds the per-pass instance. A fresh
    /// `PassProgressState` is built per pass, so this anchors cleanly each pass.
    pub(super) speed: freemkv_engine::SpeedEstimator,
    /// Wall-clock of the last throttled callback. The progress closure
    /// checks this to skip work when less than 250 ms have passed.
    pub(super) last_update: std::time::Instant,
    /// Wall-clock of the last device-log line emitted from this pass.
    pub(super) last_log: std::time::Instant,
    /// Last `work_done` reported by libfreemkv's `Progress` trait — bytes
    /// processed in this pass so far. Drives `pass_progress_pct`.
    pub(super) last_work_done: u64,
    /// Last `work_total` reported by libfreemkv's `Progress` trait — total
    /// bytes this pass will process. Drives `pass_progress_pct` denominator.
    pub(super) last_work_total: u64,
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

/// Above this, the displayed ETA is shown as a steady ">Nh" rather than a
/// precise-looking huge number. On a dead-media residue `remaining / rate`
/// explodes and whipsaws; clamping it keeps the display honest and stable.
pub(super) const ETA_CAP_SECS: u64 = 6 * 3600;

impl PassProgressState {
    pub(super) fn new() -> Self {
        let now = std::time::Instant::now();
        Self {
            speed: freemkv_engine::SpeedEstimator::new(),
            last_update: now,
            last_log: now,
            last_work_done: 0,
            last_work_total: 0,
            frozen_bytes_lost: None,
        }
    }
}

/// Push a fresh RipState snapshot for the current pass. Feeds the pass's
/// work_done into the engine `SpeedEstimator` for the displayed speed + ETA —
/// otherwise the UI shows 0 KB/s through the whole rip, since the main stream
/// loop's speed tracker isn't running during `freemkv_engine::sweep` / `patch`.
/// Buckets/drilldown come straight from `PassProgress.located` (autorip no
/// longer parses the mapfile on this path).
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
    let pass_pct = if let Some(p) = (last_pos * 100).checked_div(last_work_total) {
        p.min(100) as u8
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
    let total_pct = if let Some(p) = (total_done * 100).checked_div(total_work_estimated) {
        p.min(100) as u8
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
        s.speed.set_responsive(pass > 1);
        let display_speed = s.speed.observe(now, last_pos);
        // ETA uses the long-running average (bytes ripped this pass /
        // elapsed-this-pass), NOT the displayed 10 s window. A transient
        // 12 s slow region can swing the windowed speed from 15 MB/s to
        // 1 MB/s; if ETA used that, it would jump from "1:30:00" to
        // "30:00:00" mid-rip and back. The user wants ETA = "when will
        // this finish" — that's a question about the whole pass's
        // average rate. Falls back to display_speed during the first
        // ETA_WARMUP_SECS while the running average is still noisy.
        let eta_speed = s.speed.eta_speed_mbs(now, display_speed);
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
        // Cap the displayed ETA. On a dead-media residue the recovery rate is
        // near zero, so `remaining / rate` explodes to tens of hours and
        // whipsaws (50 h → 12 h → …) as the rate twitches — a precise-looking
        // number that is really "unknown / effectively never". Above the cap we
        // show a steady ">Nh" instead; a real ETA reappears the moment the rate
        // is high enough to put it back under the cap (i.e. on readable data).
        let eta_str = |secs: u64| -> String {
            if secs > ETA_CAP_SECS {
                format!(">{}h", ETA_CAP_SECS / 3600)
            } else {
                format_secs(secs)
            }
        };
        let pass_eta = if eta_speed > 0.0001 && last_work_total > last_pos {
            let rem_mb = (last_work_total - last_pos) as f64 / BYTES_PER_MIB;
            eta_str((rem_mb / eta_speed) as u64)
        } else {
            String::new()
        };
        let total_eta = if eta_speed > 0.0001 && total_work_estimated > total_done {
            let rem_mb = (total_work_estimated - total_done) as f64 / BYTES_PER_MIB;
            eta_str((rem_mb / eta_speed) as u64)
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
    let pct = if let Some(p) = (bytes_good * 100).checked_div(ctx.bytes_total_disc) {
        p.min(100) as u8
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
    use freemkv_engine::{Mapfile, SectorStatus};

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

    /// Catches the mutation that feeds the done card the STARVED
    /// `sweep_damage_snapshot.total_lost_ms` (0.0 in single-pass, which has no
    /// mapfile) instead of the real in-title loss — a damaged rip filed as
    /// clean.
    ///
    /// The previous version of this test only called `damage_severity_for`
    /// with two literals. That pins the classifier, which was never the thing
    /// at risk: the wiring lives in `rip_disc`, and reintroducing the bug there
    /// left this test green. The decision now lives in one function
    /// (`super::super::done_card_lost_ms`) and this drives it, then pushes the
    /// result through the REAL `update_state` — which is where
    /// `damage_severity` is actually computed for the card.
    #[test]
    fn single_pass_done_card_total_lost_ms_drives_severity() {
        // 10 skipped sectors -> below the 51-sector Moderate threshold, so
        // severity is decided purely by the ms-branch.
        let errors: u32 = 10;
        let final_lost_secs: f64 = 1.5; // 1500 ms of in-title loss

        // The wiring: single-pass must carry the real in-title loss.
        let single_pass = super::super::done_card_lost_ms(false, final_lost_secs, 0.0, 0.0);
        assert_eq!(
            single_pass,
            final_lost_secs * MILLIS_PER_SEC,
            "single-pass must derive the done card's lost-ms from the real \
             in-title loss, not from the mapfile snapshot it does not have"
        );

        // End to end, exactly as `rip_disc` publishes it.
        let dev = format!("sg_done_card_severity_{}", std::process::id());
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "done".to_string(),
                errors,
                total_lost_ms: single_pass,
                ..Default::default()
            },
        );
        let snap = STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&dev)
            .cloned()
            .expect("state entry exists");
        assert_eq!(
            snap.damage_severity, "moderate",
            "a >1s in-title loss must reach the done card as moderate damage"
        );

        // And the bug, spelled out: the starved value classifies the same rip
        // as cosmetic, so the two must not be interchangeable.
        assert_eq!(
            damage_severity_for(errors, 0.0),
            "cosmetic",
            "starved total_lost_ms under-classifies a >1s loss — this is the \
             value the wiring must NOT pick in single-pass mode"
        );

        // Multipass keeps the mapfile-derived value, plus the demux extra.
        assert_eq!(
            super::super::done_card_lost_ms(true, final_lost_secs, 4000.0, 100.0),
            4100.0,
            "multipass must keep the snapshot's mapfile-derived loss"
        );

        STATE.lock().unwrap_or_else(|e| e.into_inner()).remove(&dev);
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

    // The live at-risk / located-drilldown behaviour these tests used to cover
    // (in-feature pending counts as at-risk; out-of-feature reads 0:00; the live
    // drilldown shows pending while the terminal verdict hides it) moved into
    // libfreemkv with `RipProgress::from_map` — it's now exercised by
    // `locate_ranges` tests in libfreemkv (src/disc/mod.rs). The terminal
    // `build_bad_ranges` path (still autorip-side, for the done card) keeps its
    // own coverage below.

    /// The post-Stop cooldown is a SHORT INTERVAL, so it must be measured on
    /// the monotonic clock, not the wall clock.
    ///
    /// It was stored as an absolute `epoch_secs()` deadline. A backward wall
    /// clock step larger than the 5s window — an NTP correction, a container
    /// host clock reset, a VM resuming from a snapshot — leaves `now <
    /// expires` true for as long as the clock takes to catch back up, and the
    /// device stays wedged in "just stopped, ignore this insert" for that whole
    /// time: `insert_tick` suppresses the auto-scan/auto-rip dispatch, so an
    /// unattended box quietly ignores the disc sitting in the drive. `Instant`
    /// cannot step backwards, which is the whole reason it exists.
    ///
    /// Proven structurally: stepping the system clock inside the test process
    /// is not portable. The behavioural halves (active when set, inactive once
    /// the deadline has passed) are pinned below.
    #[test]
    fn the_stop_cooldown_is_not_measured_on_the_wall_clock() {
        let src = crate::util::source_lf(include_str!("state.rs"));
        // Start at the STATIC, not at `set_stop_cooldown`: the stored TYPE is
        // half the guarantee (an `Instant` map cannot hold a wall-clock
        // deadline at all), and it is declared above the setter.
        let start = src
            .find("pub(super) static STOP_COOLDOWNS")
            .expect("the cooldown map must exist");
        let end = src
            .find("/// Drop the auxiliary per-device state on hot-unplug")
            .expect("forget_device_state's doc must follow the cooldown fns");
        // Strip comment lines before matching. Without this the pin can be
        // satisfied — or broken — by its own prose: the doc comment on
        // STOP_COOLDOWNS names `epoch_secs()` while explaining why it must not
        // be used.
        let region: String = src[start..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        // Not "no `epoch_secs()`" — that pins ONE spelling, and the wall clock
        // has several. Any of these reads the clock that can step backwards.
        for spelling in [
            "epoch_secs",
            "SystemTime",
            "UNIX_EPOCH",
            "duration_since",
            "chrono",
        ] {
            assert!(
                !region.contains(spelling),
                "the stop cooldown must not be derived from the wall clock \
                 (found `{spelling}`) — a backward NTP step, host clock reset \
                 or VM snapshot resume wedges the device out of auto-dispatch \
                 for as long as the clock is behind"
            );
        }
        assert!(
            region.contains("std::time::Instant>"),
            "the cooldown deadline must be STORED as a monotonic Instant, so a \
             wall-clock value cannot be put in the map at all"
        );
        assert!(
            region.contains("Instant::now()"),
            "the cooldown deadline must be computed from the monotonic clock"
        );
    }

    /// Both ends of the cooldown's observable contract, through the real
    /// accessors: a freshly-set cooldown suppresses dispatch, and a deadline
    /// that has already passed does not.
    #[test]
    fn a_stop_cooldown_expires() {
        let dev = "sg_cooldown_expiry_test";
        set_stop_cooldown(dev);
        assert!(
            is_in_cooldown(dev),
            "a cooldown just set must suppress the next insert tick"
        );

        // A deadline in the past is exactly what the poll loop sees once the
        // window has elapsed.
        STOP_COOLDOWNS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                dev.to_string(),
                std::time::Instant::now() - std::time::Duration::from_secs(1),
            );
        assert!(
            !is_in_cooldown(dev),
            "once the deadline has passed the device must dispatch again — \
             a cooldown that never expires silently stops ripping the disc"
        );

        STOP_COOLDOWNS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(dev);
    }

    /// A disc-supplied extent must never be able to panic the rip thread.
    ///
    /// `start_lba` and `sector_count` come straight from the UDF allocation
    /// descriptors — untrusted, disc-controlled data. `start_lba +
    /// sector_count` is `u32` arithmetic: a corrupt or hostile image with
    /// `start_lba` near `u32::MAX` overflows, which panics in debug (killing
    /// the rip thread mid-rip, on damage attribution of all things) and wraps
    /// in release, making the containment test answer nonsense. An extent that
    /// cannot express its own end simply does not contain the LBA.
    #[test]
    fn byte_offset_in_title_survives_an_overflowing_extent() {
        let mut title = minimal_title();
        title.extents = vec![libfreemkv::Extent {
            start_lba: u32::MAX - 4,
            sector_count: u32::MAX,
        }];

        // Any LBA at all: the question is only whether this panics/wraps.
        let below = byte_offset_in_title(1000, &title);
        let inside = byte_offset_in_title(u32::MAX - 2, &title);

        assert_eq!(
            below, None,
            "an LBA below an extent that cannot express its own end must not \
             be reported as inside it (wrapped comparison)"
        );
        // Two sectors past the extent's start, so two sectors' worth of bytes
        // into the title.
        assert_eq!(
            inside,
            Some(2 * SECTOR_BYTES),
            "an LBA inside the extent must still map to its byte offset"
        );
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
        // update_state_with's or_insert_with builds `RipState { device:
        // device.to_string(), ..Default::default() }` for a brand-new entry
        // — that field is independent of the HashMap key above, so assert
        // it directly.
        assert_eq!(snap.device, dev, "device field not set on first insert");
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
                tmdb_id: 0,
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

    #[test]
    fn try_claim_active_checked_refuses_unknown_device_and_state_does_not_grow() {
        // Security: `/api/scan/{device}`, `/api/rip/{device}`, and
        // `/api/eject/{device}` are unauthenticated and reachable from any
        // LAN host; `device` is only shape-checked (3..=64 ASCII
        // alphanumeric), never checked against the real drive list. Before
        // this guard, `try_claim_active` unconditionally inserted a brand
        // new permanent RipState for ANY such name, and nothing ever prunes
        // an entry for a device that was never actually enumerated (the
        // hot-unplug sweep only removes devices that were in its own
        // enumerated list). A caller looping fabricated names could grow
        // STATE (and, via a successful claim unlocking spawn_rip_thread,
        // RIP_THREADS) without bound. Pin: an unknown device with
        // known=false must be refused, and must leave no trace in STATE.
        let dev = format!("test-forged-device-{}-xyz", std::process::id());
        assert!(
            STATE.lock().unwrap().get(&dev).is_none(),
            "precondition: device must not already exist"
        );

        assert!(
            try_claim_active_checked(&dev, false).is_none(),
            "an unknown device must not be claimable"
        );
        assert!(
            STATE.lock().unwrap().get(&dev).is_none(),
            "refusing the claim must not have inserted a STATE entry \
             (else looping forged names grows STATE without bound)"
        );

        // Looping the same forged name must not eventually succeed either.
        for _ in 0..5 {
            assert!(try_claim_active_checked(&dev, false).is_none());
        }
        assert!(
            STATE.lock().unwrap().get(&dev).is_none(),
            "repeated attempts on an unknown device must never insert an entry"
        );
    }

    #[test]
    fn try_claim_active_checked_allows_known_true_for_new_device() {
        // known=true is exactly today's try_claim_active behaviour (used by
        // the poll loop's own trusted, just-enumerated device list) — must
        // still create a fresh entry and succeed.
        let dev = format!("test-known-new-{}", std::process::id());
        assert!(try_claim_active_checked(&dev, true).is_some());
        let snap = STATE.lock().unwrap().get(&dev).cloned().unwrap();
        assert_eq!(snap.status, "scanning");
        // The map key and the RipState.device field are independent — the
        // struct literal's `device: device.to_string()` could be deleted
        // (falling back to Default's "") without affecting the HashMap
        // lookup above. Assert it explicitly.
        assert_eq!(
            snap.device, dev,
            "device field in the freshly-inserted RipState must match"
        );
    }

    #[test]
    fn try_claim_active_checked_allows_unknown_flag_once_device_already_present() {
        // A real device always has a STATE entry before an operator can act
        // on it (the poll loop pushes one every tick for every enumerated
        // drive, and the UI only offers Scan/Rip/Eject for devices
        // /api/state already reports) — so `known=false` must not block a
        // second legitimate claim on a device that is already known to
        // STATE by other means (e.g. idle after a previous rip).
        let dev = format!("test-known-existing-{}", std::process::id());
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "idle".to_string(),
                ..Default::default()
            },
        );
        assert!(
            try_claim_active_checked(&dev, false).is_some(),
            "a device already present in STATE must remain claimable even \
             when the caller couldn't independently verify it"
        );
    }

    /// Catches the mutation that deletes the thread-liveness half of the
    /// claim (leaving only the STATE-status check it had before).
    ///
    /// The device is left with a TERMINAL status — exactly the state a worker
    /// writes just before it starts unwinding (auto-eject, log archive, guard
    /// drops) — while its thread is still on the CPU. `is_busy` is false for
    /// that whole window, so a status-only claim admitted an unauthenticated
    /// LAN `POST /api/rip/{device}` into a device another worker still owns:
    /// the new claim overwrote the incumbent's `Halt` token (so `/api/stop`
    /// could no longer cancel either rip) and launched a second worker against
    /// the same staging dir.
    ///
    /// The second half of the test is just as load-bearing: once the worker
    /// really has exited, the device must be claimable again immediately. A
    /// "fix" that latched the device shut would break every normal
    /// rip-then-rip-again sequence.
    #[test]
    fn try_claim_active_refuses_a_device_whose_worker_is_still_unwinding() {
        let dev = format!("sg_claim_liveness_test_{}", std::process::id());
        let _ = super::super::take_rip_thread(&dev);
        // Terminal status: the worker has published "done" and is now in its
        // tail. STATE says free; the thread says otherwise.
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "done".to_string(),
                disc_present: true,
                ..Default::default()
            },
        );
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        super::super::session::spawn_rip_thread(&dev, "rip", move || {
            let _ = release_rx.recv();
        })
        .expect("the worker owns the device");

        assert!(
            !is_busy(&dev),
            "test setup: the status half of the claim must already read free, \
             otherwise this test cannot distinguish the two facts"
        );
        assert!(
            try_claim_active_checked(&dev, false).is_none(),
            "a claim must be refused while the device's worker thread is still \
             running, even though its status is terminal"
        );
        assert!(
            try_claim_active(&dev).is_none(),
            "the known=true wrapper must refuse on the same grounds"
        );

        // The worker exits. Its handle is deliberately left REGISTERED and
        // unreaped — that is the normal state after any completed rip, and the
        // gate must read a finished handle as "not running" or every device
        // would be claimable exactly once per process.
        drop(release_tx);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while super::super::session::rip_thread_running(&dev) {
            assert!(
                std::time::Instant::now() < deadline,
                "the worker should have exited as soon as its channel closed"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            try_claim_active_checked(&dev, false).is_some(),
            "once the worker has exited the device must be claimable again, \
             with no reaping step in between — the liveness gate must not \
             latch a device shut"
        );
        super::super::take_rip_thread(&dev)
            .expect("the finished handle is still registered")
            .join()
            .expect("worker joins cleanly");
        STATE.lock().unwrap().remove(&dev);
    }

    /// Catches the mutation that puts `take_rip_thread` back at the top of
    /// `join_rip_thread` (i.e. drains by REMOVING the handle and stashing it
    /// back afterwards) — the H1 duplicate-rip window.
    ///
    /// The drain is what `POST /api/stop/{device}` does, for up to 60 s. While
    /// it ran, the handle was OUT of `RIP_THREADS`, so the device's only
    /// liveness fact read `false`. Land that on a worker in its terminal tail
    /// (status already "done", so `is_busy` is false too) and a concurrent
    /// `POST /api/rip/{device}` — unauthenticated, this server binds 0.0.0.0 —
    /// wins the claim, clobbers the incumbent's `Halt`, finds an empty
    /// registration slot and runs a full duplicate rip on the same drive and
    /// the same staging dir.
    ///
    /// So: a claim must be refused for the WHOLE life of the worker thread,
    /// including while another thread is draining it.
    #[test]
    fn a_drain_in_flight_never_makes_a_live_worker_claimable() {
        let dev = format!("sg_claim_during_drain_test_{}", std::process::id());
        let _ = super::super::take_rip_thread(&dev);
        // The terminal tail: the worker has published "done" and is still
        // running. The status half of the gate is already open.
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "done".to_string(),
                disc_present: true,
                ..Default::default()
            },
        );
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        super::super::session::spawn_rip_thread(&dev, "rip", move || {
            let _ = release_rx.recv();
        })
        .expect("the worker owns the device");
        assert!(!is_busy(&dev), "test setup: the status half must read free");

        // `/api/stop` arrives and drains. The worker is blocked, so this
        // occupies the full budget and then reports a timeout.
        let drain_dev = dev.clone();
        let drain = std::thread::spawn(move || {
            super::super::session::join_rip_thread(
                &drain_dev,
                std::time::Duration::from_millis(600),
            )
        });

        // Hammer the claim for the whole drain window. Every one must lose.
        let until = std::time::Instant::now() + std::time::Duration::from_millis(400);
        let mut attempts = 0u32;
        while std::time::Instant::now() < until {
            assert!(
                try_claim_active_checked(&dev, false).is_none(),
                "a claim must be refused while the device's worker is alive, \
                 even while a concurrent /api/stop drain is polling it"
            );
            attempts += 1;
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            attempts > 5,
            "test setup: the drain window was never sampled"
        );

        assert!(
            drain.join().expect("drain thread joins").is_err(),
            "test setup: the drain must have timed out against the blocked \
             worker, which is the window this test is about"
        );
        // And the handle must still be registered after that timeout — a drain
        // that loses the handle also loses the ability to reap the thread.
        assert!(
            super::super::session::rip_thread_running(&dev),
            "a timed-out drain must leave the live worker's handle registered"
        );

        drop(release_tx);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while super::super::session::rip_thread_running(&dev) {
            assert!(std::time::Instant::now() < deadline, "worker should exit");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let _ = super::super::session::join_rip_thread(&dev, std::time::Duration::from_secs(5));
        STATE.lock().unwrap().remove(&dev);
    }

    #[test]
    fn try_claim_active_rejects_second_claim_on_busy_device() {
        // HIGH (rule 1/2): try_claim_active's whole purpose is to refuse a
        // second claim on a device that is already scanning/ripping,
        // closing the double-rip TOCTOU. No existing test calls it twice on
        // the same device — every test only checks the first call
        // succeeds. `r.status == "scanning" || r.status == "ripping"`
        // mutated to `&&` can never be true for one string, silently
        // disabling the whole guard.
        let dev = format!("test-doubleclaim-{}", std::process::id());
        assert!(
            try_claim_active(&dev).is_some(),
            "first claim on a fresh device must succeed"
        );
        assert!(
            try_claim_active(&dev).is_none(),
            "a second claim on an already-scanning device must be refused"
        );
    }

    #[test]
    fn update_state_carries_forward_zero_claim_gen() {
        // MEDIUM (concurrency): callers build a fresh RipState with
        // `..Default::default()` (claim_gen = 0) for almost every push; if
        // update_state didn't carry the real generation forward, every
        // ordinary state push after try_claim_active would silently reset
        // claim_gen to 0, defeating the stale-worker-detach guard the
        // generation exists for.
        let dev = format!("test-claimgen-carry-{}", std::process::id());
        assert!(
            try_claim_active(&dev).is_some(),
            "claim bumps claim_gen to 1"
        );
        // A normal mid-rip push, exactly as push_pass_state/set_pass_progress
        // build it: claim_gen defaults to 0 via ..Default::default().
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "ripping".to_string(),
                ..Default::default()
            },
        );
        let snap = STATE.lock().unwrap().get(&dev).cloned().unwrap();
        assert_eq!(
            snap.claim_gen, 1,
            "claim_gen must be carried forward from the prior push, not reset to 0"
        );
    }

    #[test]
    fn update_state_does_not_clobber_explicit_nonzero_claim_gen() {
        // Companion to the above: a caller that DOES set a real nonzero
        // claim_gen explicitly (not the ..Default::default() zero) must
        // have that value stored verbatim, not overwritten by the
        // previous push's generation.
        let dev = format!("test-claimgen-explicit-{}", std::process::id());
        assert!(try_claim_active(&dev).is_some()); // claim_gen -> 1
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "ripping".to_string(),
                claim_gen: 99,
                ..Default::default()
            },
        );
        let snap = STATE.lock().unwrap().get(&dev).cloned().unwrap();
        assert_eq!(
            snap.claim_gen, 99,
            "an explicit nonzero claim_gen must not be overwritten by the carried-forward value"
        );
    }

    #[test]
    fn take_title_override_returns_and_clears_the_override() {
        // set_title_override / take_title_override are otherwise only
        // exercised via forget_device_state's eviction test, which never
        // calls take_title_override at all — so a `take_title_override` ->
        // `()` mutant (return type would fail to compile, but a body ->
        // `None` mutant) or a `set_title_override` -> `()` mutant both pass
        // the whole suite today.
        let dev = format!("/dev/test-override-{}", std::process::id());
        assert!(take_title_override(&dev).is_none(), "no override set yet");
        let picked = crate::tmdb::TmdbResult {
            title: "Override Title".to_string(),
            year: 1999,
            poster_url: String::new(),
            overview: String::new(),
            media_type: "movie".to_string(),
            tmdb_id: 0,
        };
        set_title_override(&dev, picked.clone());
        let taken = take_title_override(&dev).expect("override must be present after set");
        assert_eq!(taken.title, "Override Title");
        assert_eq!(taken.year, 1999);
        // take clears it — a second take must come back empty.
        assert!(
            take_title_override(&dev).is_none(),
            "take_title_override must remove the entry, not just read it"
        );
    }

    #[test]
    fn device_known_reflects_state_membership() {
        let dev = format!("test-deviceknown-{}", std::process::id());
        assert!(
            !device_known(&dev),
            "unclaimed device must not read as known"
        );
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "idle".to_string(),
                ..Default::default()
            },
        );
        assert!(
            device_known(&dev),
            "device with a STATE entry must read as known"
        );
    }

    /// `disc_label` is the raw volume label — the only thing telling two discs
    /// of a boxset apart behind their one shared TMDB title, and what decides
    /// which staging dir a disc owns. Nearly every caller pushes a fresh
    /// `RipState` with `..Default::default()`, so `update_state` must carry it
    /// forward or the first progress push erases it and the disc collapses back
    /// onto its sibling's directory. It must NOT be carried onto a different
    /// disc, or onto an empty drive.
    #[test]
    fn update_state_carries_the_disc_label_but_never_onto_another_disc() {
        let dev = format!("test-disclabel-{}", std::process::id());
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "scanning".to_string(),
                disc_name: "Boxset Movie".to_string(),
                disc_label: "BOXSET_DISC_2".to_string(),
                ..Default::default()
            },
        );

        // A progress push that names the same disc but sets no label.
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "ripping".to_string(),
                disc_name: "Boxset Movie".to_string(),
                ..Default::default()
            },
        );
        let s = STATE.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            s.get(&dev).map(|r| r.disc_label.as_str()),
            Some("BOXSET_DISC_2"),
            "the raw volume label must survive a default-built state push"
        );
        drop(s);

        // A DIFFERENT disc must not inherit it.
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "scanning".to_string(),
                disc_name: "Some Other Film".to_string(),
                ..Default::default()
            },
        );
        let s = STATE.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            s.get(&dev).map(|r| r.disc_label.as_str()),
            Some(""),
            "a different disc must not inherit the previous disc's label"
        );
        drop(s);

        // Nor must an ejected / empty drive.
        update_state(
            &dev,
            RipState {
                device: dev.clone(),
                status: "idle".to_string(),
                disc_name: String::new(),
                ..Default::default()
            },
        );
        let s = STATE.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            s.get(&dev).map(|r| r.disc_label.as_str()),
            Some(""),
            "an empty drive must not keep a stale label"
        );
    }

    #[test]
    fn push_pass_state_preserves_every_damage_indicator_field() {
        // HIGH (rule 1) — by far the largest survivor cluster in this file:
        // push_pass_state builds a ~40-field RipState struct literal ending
        // in `..Default::default()`. Deleting ANY one named field from that
        // literal (cargo-mutants' "delete field X from struct expression")
        // makes the field silently fall back to RipState::default()'s
        // zero/empty value. No existing test reads push_pass_state's output
        // back field-by-field, so ~30 of those deletions passed the whole
        // suite untouched. The fields checked here are exactly the ones the
        // UI/API use to tell a damaged rip from a clean one: if any of them
        // regressed to Default, a disc actively losing sectors would
        // present as if it had none.
        let dev = format!("test-pps-fields-{}", std::process::id());
        let mut ctx = minimal_pass_ctx(&dev);
        // Give the metadata pass-through fields non-default values too —
        // `minimal_pass_ctx`'s empty-string/0 defaults are indistinguishable
        // from `RipState::default()`, so a field-deletion mutant on
        // tmdb_title/year/poster/overview/media_type/duration/codecs would
        // be invisible against them.
        ctx.tmdb_title = "Example Movie".to_string();
        ctx.tmdb_year = 2024;
        ctx.tmdb_poster = "https://example/poster.jpg".to_string();
        ctx.tmdb_overview = "An example overview.".to_string();
        ctx.tmdb_media_type = "movie".to_string();
        ctx.duration = "2:15:00".to_string();
        ctx.codecs = "HEVC/DTS-HD".to_string();
        let pass_state = std::cell::RefCell::new(PassProgressState::new());
        // Mirrors what the real caller (mod.rs's sweep/patch progress
        // closures) does before invoking push_pass_state: seed the
        // per-pass work_done/work_total the library just reported.
        {
            let mut s = pass_state.borrow_mut();
            s.last_work_done = 1_000_000;
            s.last_work_total = 2_000_000;
        }

        let located = libfreemkv::progress::LocatedProgress {
            ranges: vec![libfreemkv::progress::LocatedRange {
                lba: 12345,
                count: 7,
                duration_ms: 42.0,
                chapter: Some(3),
                time_offset_secs: Some(120.0),
            }],
            num_ranges: 1,
            truncated: 3,
            main_at_risk_ms: 999.0,
            largest_gap_ms: 888.0,
        };
        let p = libfreemkv::progress::PassProgress {
            kind: libfreemkv::progress::PassKind::Sweep,
            work_done: 1_000_000,
            work_total: 2_000_000,
            bytes_good_total: 500_000,
            bytes_unreadable_total: 20_480, // 10 sectors * 2048
            bytes_pending_total: 4_096,
            bytes_retryable_total: 4_096,
            bytes_total_disc: ctx.bytes_total_disc,
            disc_duration_secs: None,
            bytes_bad_in_main_title: 20_480,
            main_title_duration_secs: None,
            main_title_size_bytes: None,
            located,
        };

        push_pass_state(&ctx, &p, 2048.0, 3, 7, &pass_state);

        let snap = STATE
            .lock()
            .unwrap()
            .get(&dev)
            .cloned()
            .expect("push_pass_state must insert an entry for the device");

        assert_eq!(snap.device, dev, "device field dropped to Default");
        assert_eq!(snap.status, "ripping", "status field dropped to Default");
        assert!(snap.disc_present, "disc_present field dropped to Default");
        assert_eq!(
            snap.disc_name, "Test Disc",
            "disc_name field dropped to Default"
        );
        assert_eq!(
            snap.disc_format, "uhd",
            "disc_format field dropped to Default"
        );
        assert_eq!(
            snap.output_file, "test.mkv",
            "output_file field dropped to Default"
        );
        assert_eq!(snap.pass, 3, "pass field dropped to Default");
        assert_eq!(
            snap.total_passes, 7,
            "total_passes field dropped to Default"
        );
        assert_eq!(
            snap.bytes_good, 500_000,
            "bytes_good field dropped to Default"
        );
        assert_eq!(
            snap.bytes_maybe, 4_096,
            "bytes_maybe field dropped to Default"
        );
        assert_eq!(
            snap.bytes_lost, 20_480,
            "bytes_lost field dropped to Default"
        );
        assert_eq!(
            snap.bytes_total_disc, ctx.bytes_total_disc,
            "bytes_total_disc field dropped to Default"
        );
        assert_eq!(
            snap.num_bad_ranges, 1,
            "num_bad_ranges field dropped to Default"
        );
        assert_eq!(
            snap.bad_ranges_truncated, 3,
            "bad_ranges_truncated field dropped to Default"
        );
        assert_eq!(
            snap.bad_ranges.len(),
            1,
            "bad_ranges field dropped to Default"
        );
        assert_eq!(
            snap.bad_ranges[0].lba, 12345,
            "bad_ranges content lost across the DTO mapping"
        );
        assert!(
            (snap.main_at_risk_ms - 999.0).abs() < 0.001,
            "main_at_risk_ms field dropped to Default"
        );
        assert!(
            (snap.largest_gap_ms - 888.0).abs() < 0.001,
            "largest_gap_ms field dropped to Default"
        );
        assert_eq!(
            snap.errors, 10,
            "errors field dropped to Default (bytes_lost / SECTOR_BYTES)"
        );
        assert!(
            snap.total_lost_ms > 0.0,
            "total_lost_ms field dropped to Default"
        );
        assert_eq!(
            snap.preferred_batch, 32,
            "preferred_batch field dropped to Default"
        );
        assert_eq!(
            snap.current_batch, 32,
            "current_batch field dropped to Default"
        );
        assert_eq!(
            snap.last_sector,
            1_000_000 / SECTOR_BYTES,
            "last_sector field dropped to Default"
        );
        assert_eq!(
            snap.progress_pct, 50,
            "progress_pct field dropped to Default (1_000_000 / 2_000_000)"
        );
        assert_eq!(
            snap.tmdb_title, "Example Movie",
            "tmdb_title field dropped to Default"
        );
        assert_eq!(snap.tmdb_year, 2024, "tmdb_year field dropped to Default");
        assert_eq!(
            snap.tmdb_poster, "https://example/poster.jpg",
            "tmdb_poster field dropped to Default"
        );
        assert_eq!(
            snap.tmdb_overview, "An example overview.",
            "tmdb_overview field dropped to Default"
        );
        assert_eq!(
            snap.tmdb_media_type, "movie",
            "tmdb_media_type field dropped to Default"
        );
        assert_eq!(
            snap.duration, "2:15:00",
            "duration field dropped to Default"
        );
        assert_eq!(
            snap.codecs, "HEVC/DTS-HD",
            "codecs field dropped to Default"
        );
    }
}
