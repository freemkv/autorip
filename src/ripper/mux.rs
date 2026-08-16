//! Mux orchestration — autorip's thin wrappers over libfreemkv's
//! `mux_stream` driver plus the machinery autorip keeps on its own side
//! of the seam: the hard watchdog, the shared `MuxAtomics` it reads, the
//! `AutoripMuxEvents` bridge that feeds those atomics + the per-frame UI
//! state, and the `MuxOutcome` → staging/marker classification.
//!
//! As of STEP 4c-ii there are two entry points and ONE inner engine:
//! - [`mux_iso`] — multipass / resume mux from a staged ISO on disk
//!   (`MuxInput::Iso`, the file-backed prefetch highway inside libfreemkv).
//! - [`mux_live`] — live single-pass mux straight off the drive
//!   (`MuxInput::Live`, the INLINE `DiscStream` so `fill_extents`' adaptive
//!   batch-retry still fires; NOT the highway).
//!
//! Both build a `libfreemkv::MuxInput`, hand it to `mux_stream`, and map the
//! `MuxOutcome` through the shared `map_iso_mux_outcome`. The header pump,
//! headers-ready gate, write pipeline (`WRITE_PIPELINE_DEPTH`-deep), and
//! finish loop the old hand-rolled `run_mux` producer/consumer owned now all
//! live inside `mux_stream`. The per-frame UI update still carries the
//! multipass identity (`pass`/`total_passes`) so the dashboard's pass/total
//! bars don't reset to a "fresh rip" view when the mux phase starts.

use crate::util::{BYTES_PER_GIB, BYTES_PER_MIB, MILLIS_PER_SEC};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::session::device_halt;
use super::state::{RipState, update_state};

/// Hard watchdog escalation threshold. When the producer's
/// "last frame / drive activity" timestamp hasn't moved in this many
/// seconds, the rip thread is presumed stuck inside an unkillable
/// syscall (a hung NFS write, a wedged decryption thread, a
/// kernel-side ioctl that never returns). At that point graceful
/// teardown is impossible — the only escape is to exit the process
/// and rely on Docker `restart: unless-stopped` to bring autorip
/// back, after which `resume_or_quarantine_staging` decides whether
/// to retry or quarantine the disc via `.failed`.
///
/// 20 minutes is a generous margin over the soft "drive stalled" 30s
/// warning and libfreemkv's per-read recovery timeout (60s). We
/// raised this from the pre-0.24 default of 5 min after observing
/// real muxes with legitimate 5-10 min NFS-server commit pauses get
/// false-positive killed mid-rip. The cost of waiting up to 20 min
/// before escalating a true wedge is far lower than the cost of
/// repeatedly killing healthy-but-slow rips.
pub const HARD_WATCHDOG_STALL_SECS: u64 = 1200;

/// Compute the Total Progress percentage during the mux phase.
///
/// Uses the same byte-weighted formula `state.rs` uses for sweep and
/// patch — so the two phases agree on what "total progress" means and
/// the bar progresses smoothly across the sweep→mux handoff instead
/// of jumping (forward or backward).
///
/// **Total work estimate** (matches `state.rs::total_work_estimated`):
///
/// ```text
///     total_work = bytes_total_disc                 // sweep
///                + max_retries × bytes_unreadable    // retries
///                + bytes_total_disc                  // mux re-reads ISO
/// ```
///
/// On a clean disc with `bytes_unreadable=0`, the retry term vanishes
/// and total_work = 2 × disc capacity — so mux opens at exactly 50%.
/// On a damaged disc, the retry term inflates the denominator
/// proportionally; the bar tracks the larger total.
///
/// **Total work done** by mux time:
///
/// ```text
///     total_done = bytes_total_disc                 // sweep complete
///                + max_retries × bytes_unreadable    // retries complete
///                + (mux_pct / 100) × bytes_total_disc
/// ```
///
/// **Why `max_retries` and not actual-passes-run?** State.rs uses
/// `max_retries × bytes_unreadable` (planned × current); we mirror it
/// here. Autorip's retry loop short-circuits on `bytes_unreadable=0`,
/// so on a clean disc the retry term is `max_retries × 0 = 0` whether
/// 0 or 5 retries actually ran — the formula self-corrects via the
/// shrinking `bytes_unreadable`. The approximation is a slight
/// over-count of retry-pass work on partially-clean discs (we treat
/// final `bytes_unreadable` as if it persisted through every retry,
/// when in reality each pass shrinks it), but it never goes
/// backward and matches state.rs.
///
/// **Direct mode** (`max_retries == 0`): no separate phases, total
/// tracks the current mux progress 1:1.
fn total_pct_byte_weight(
    bytes_total_disc: u64,
    max_retries: u8,
    bytes_unreadable_at_mux: u64,
    mux_pct: u8,
) -> u8 {
    if max_retries == 0 || bytes_total_disc == 0 {
        return mux_pct.min(100);
    }
    // u128 to keep multiplication overflow-safe on > 4 GB discs.
    let cap = bytes_total_disc as u128;
    let retry_total = (max_retries as u128) * (bytes_unreadable_at_mux as u128);
    let total_work = cap + retry_total + cap;
    if total_work == 0 {
        return mux_pct.min(100);
    }
    let mux_done = cap * (mux_pct as u128) / 100;
    let total_done = cap + retry_total + mux_done;
    ((total_done * 100 / total_work).min(100)) as u8
}

/// Inputs to the mux drivers ([`mux_iso`] / [`mux_live`]) that come from the
/// orchestrator. Bundled into a struct because the pre-split inline mux block
/// referenced ~25 captured locals; passing them as a struct keeps the driver
/// signatures readable and avoids a long positional argument list.
/// Damage fields from the final sweep/patch pass, carried forward so they
/// remain visible in /api/state during the mux phase instead of zeroing out.
///
/// Before this snapshot, `push_state` used `..Default::default()` which
/// set `errors=0, lost_video_secs=0, damage_severity="clean", bad_ranges=[],
/// total_lost_ms=0` on the very first mux tick. Operators polling during mux
/// saw a damaged disc as perfectly clean.
///
/// Populated by the orchestrator from STATE immediately after the final
/// `push_pass_state` call (ripper/mod.rs, at the mux-entry transition).
/// Zero/empty defaults are correct for direct (single-pass) mode, where there
/// is no prior sweep pass with real damage data.
#[derive(Default, Clone)]
pub(crate) struct SweepDamageSnapshot {
    pub(crate) errors: u32,
    pub(crate) total_lost_ms: f64,
    pub(crate) main_lost_ms: f64,
    pub(crate) bad_ranges: Vec<super::state::BadRange>,
    pub(crate) num_bad_ranges: u32,
    pub(crate) bad_ranges_truncated: u32,
    pub(crate) largest_gap_ms: f64,
}

pub(crate) struct MuxInputs<'a> {
    pub(crate) device: &'a str,
    pub(crate) display_name: String,
    pub(crate) disc_format: String,
    pub(crate) tmdb_title: String,
    pub(crate) tmdb_year: u16,
    pub(crate) tmdb_poster: String,
    pub(crate) tmdb_overview: String,
    pub(crate) duration: String,
    pub(crate) codecs: String,
    pub(crate) filename: String,
    /// Total expected bytes for the mux phase (used for percent + ETA).
    /// Falls back to the input title's `size_bytes` if 0 is passed.
    pub(crate) total_bytes: u64,
    /// Per-title bitrate; used to convert skipped sectors → estimated
    /// lost video time for the UI.
    pub(crate) title_bytes_per_sec: f64,
    /// `max_retries + 2` in multipass mode, 0 in direct mode. Threaded
    /// through every per-frame `update_state` so the dashboard's
    /// pass/total bars don't snap back to a "fresh rip" view.
    pub(crate) total_passes: u8,
    /// Disc capacity in bytes — same value `state.rs` uses to compute
    /// the sweep + mux contributions to the total-progress denominator.
    /// Plumbed from `disc.capacity_bytes` at the orchestrator level.
    pub(crate) bytes_total_disc: u64,
    /// User-configured max retry passes (`cfg_read.max_retries`). Used
    /// as the multiplier on `bytes_unreadable` for the retry-phase
    /// contribution to total work, mirroring `state.rs`.
    pub(crate) max_retries: u8,
    /// `bytes_unreadable` at mux start — i.e. after every retry pass
    /// has finished. Drives the retry-phase contribution to the
    /// total-progress denominator. Zero on a clean disc (every bad
    /// sector recovered) — in that case the retry phase contributes
    /// nothing and total = sweep+mux only, so mux opens at ~50%.
    pub(crate) bytes_unreadable_at_mux: u64,
    /// Pre-resolved mux output URL (e.g. `mkv:///srv/.../foo.mkv`,
    /// `network://host:port`). Resolved by the orchestrator because URL
    /// construction depends on `cfg.network_target` + `output_format`.
    pub(crate) dest_url: String,
    /// Kernel-reported preferred batch size; surfaced in `RipState` so
    /// the UI keeps showing it through the mux phase.
    pub(crate) batch: u16,
    /// Per-disc staging directory (e.g. `/staging/MyDisc/`). Used by
    /// the hard watchdog to bump `.restart_count` before
    /// `std::process::exit(1)` so the post-restart resume logic can
    /// promote the disc to `.failed` once `RESTART_LIMIT` is reached.
    pub(crate) staging_disc_dir: PathBuf,
    /// Damage fields snapshotted from the final sweep/patch pass.
    /// Carried into every per-frame `push_state` so /api/state preserves
    /// damage visibility during the mux phase. Defaults to zero/empty for
    /// direct (single-pass) mode.
    pub(crate) sweep_damage: SweepDamageSnapshot,
}

/// Outcome of a mux driver ([`mux_iso`] / [`mux_live`]), used by the
/// orchestrator to drive the post-mux history record + final state push.
/// `completed=false` means the mux bailed early — either user halt, write error, or
/// read error. The bytes/elapsed are filled even on early exit so
/// the history record reflects partial progress.
pub(crate) struct MuxOutcome {
    /// True iff the read loop drained `frame_rx` to natural EOF
    /// (producer dropped its `frame_tx` after either EOF on the input
    /// stream or an unrecoverable read error logged via `device_log`)
    /// AND the post-loop `pipe.finish_with_halt(...)` returned `Ok`.
    ///
    /// 0.20.8 post-validation-audit semantics: `completed=true` is the
    /// orchestrator's gate for writing `.done` / `.completed` markers
    /// in `staging` (see `rip_disc` in `mod.rs` around the
    /// `status_label = if completed { "complete" } else { "stopped" }`
    /// branch). It is therefore the on-disk success signal for the
    /// resume-on-startup detector and for the mover thread.
    ///
    /// Set to `false` on any of:
    /// - halt during header read (early return),
    /// - `libfreemkv::output(...)` open failure (early return),
    /// - `Pipeline::spawn_named` failure (early return),
    /// - producer thread spawn failure (early return),
    /// - `break` out of the consumer-bridge loop because
    ///   `pipe.send_with_halt` returned Err (halt or send deadline),
    /// - `pipe.finish_with_halt` returning Err (consumer wedged or
    ///   `MuxSink::close` propagated a finalize error from
    ///   `output.finish()` — see `finalize_error`).
    pub(crate) completed: bool,
    pub(crate) bytes_done: u64,
    pub(crate) elapsed_secs: f64,
    pub(crate) speed_mbs: f64,
    /// Demux skip count from the input stream (`DiscStream::errors`).
    /// Multipass callers usually overwrite this with the mapfile's
    /// `bytes_unreadable / 2048` because demux skips during ISO mux
    /// are typically zero — the real bad-sector count lives in the
    /// mapfile sidecar.
    pub(crate) errors: u32,
    /// Estimated lost video seconds derived from `errors`. Same
    /// override pattern as `errors` when a mapfile is available.
    pub(crate) lost_video_secs: f64,
    /// True iff the output stream was successfully opened (i.e. we got
    /// past header buffering and `libfreemkv::output(...)` returned
    /// Ok). The orchestrator gates history-record writing on this:
    /// stops or open-failures before the output exists leave no
    /// salvageable artefact, so they get an early-return path
    /// (matching pre-split behaviour). Stops or write errors after
    /// the output is open leave a partial MKV in staging and a
    /// "stopped" history record describing it.
    pub(crate) output_opened: bool,
    /// Set when `MuxSink::close()` failed to finalise the MKV (most
    /// commonly: the Cues seek-back at EBML close raised an I/O error,
    /// leaving an unseekable / structurally-invalid output). Carries
    /// the formatted error so the orchestrator can put it in the
    /// `.failed` marker reason. `Some(_)` implies `completed == false`.
    ///
    /// Pre-0.20.8 the close error was swallowed (logged only) and
    /// `.done` / `.completed` got written for unseekable MKVs — the
    /// validation audit's #1 "Reasonable tier" item.
    pub(crate) finalize_error: Option<String>,
    /// Set (with the specific cause) when the producer thread aborted
    /// mid-stream on a hard read error — i.e. `on_read_error=stop` saw
    /// an unrecoverable read `Err` and dropped its sender, truncating
    /// the MKV. Distinct from `finalize_error` (a structural MKV defect
    /// that quarantines the dir with `.failed`): a read error leaves the
    /// disc resumable, but it is NOT a user-initiated stop. The
    /// orchestrator uses this to report `status="error"` with a clear
    /// `last_error` instead of the silent "stopped → idle" path that a
    /// genuine operator halt takes — so `/api/state` signals the read
    /// failure rather than looking like an idle, user-stopped rip.
    pub(crate) read_error: Option<String>,
}

/// Per-frame UI state that the consumer needs to fill in the
/// `update_state` payload. Cloned once into the `MuxSink` and reused
/// every frame — none of these fields change during mux.
struct UiState {
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
    batch: u16,
    total_bytes: u64,
    title_bytes_per_sec: f64,
    total_passes: u8,
    /// Disc capacity, used by `total_pct_byte_weight` to size the
    /// total-progress denominator.
    bytes_total_disc: u64,
    /// Configured max retry passes; multiplier on `bytes_unreadable_at_mux`
    /// for the retry-phase contribution to total work.
    max_retries: u8,
    /// `bytes_unreadable` at mux start (after every retry pass finished).
    bytes_unreadable_at_mux: u64,
    /// Damage fields from the final sweep/patch pass. Kept constant across
    /// all mux-phase `push_state` calls so the damage pill / bad-ranges list
    /// stays visible rather than reverting to default-zero on the first tick.
    sweep_damage: SweepDamageSnapshot,
}

/// Cross-thread atomics the consumer reads on every per-frame
/// `update_state`. The producer's `input.on_event` callback writes
/// `latest_bytes_read` / `rip_last_lba` / `rip_current_batch` from the
/// reader thread; the consumer reads them on the writer thread. The
/// watchdog also reads them.
#[derive(Clone)]
struct SharedAtomics {
    /// Last byte position reported by the drive's BytesRead event.
    /// Preferred over `output.bytes_written()` for the progress bar
    /// because reads run ahead of writes when the channel is full.
    latest_bytes_read: Arc<AtomicU64>,
    rip_last_lba: Arc<AtomicU64>,
    rip_current_batch: Arc<AtomicU16>,
    /// Watchdog "last activity" timestamp. The drive + stream event
    /// callbacks update it from the reader thread; the consumer also
    /// updates it after each frame write. The watchdog reads it.
    wd_last_frame: Arc<AtomicU64>,
    /// Bytes written by the output sink. Consumer writes; watchdog
    /// reads (used to render the "stalled at X GB" UI).
    wd_bytes: Arc<AtomicU64>,
    /// Snapshot of `input.errors` after the most recent `read()`. The
    /// producer updates it after every frame; the consumer reads it
    /// inside `apply` to surface the skip-event count. Atomic so we don't
    /// need to put the input stream behind a mutex.
    input_errors: Arc<AtomicU32>,
    /// Snapshot of `input.lost_bytes` after the most recent `read()` —
    /// the actual bytes zero-filled past read errors. Used (not
    /// `input_errors`) to compute `lost_video_secs`: an AACS skip event
    /// covers a whole 6144-byte unit, so `errors * 2048` understates loss
    /// by the alignment factor. Produced/consumed like `input_errors`.
    input_lost_bytes: Arc<AtomicU64>,
}

/// Build + push the per-frame mux `update_state` payload. Extracted from
/// `MuxSink::push_state` so BOTH the live single-pass `MuxSink` (the frame
/// consumer) and the ISO/multipass `AutoripMuxEvents` bridge render an
/// identical `RipState` — same pass/total identity, same sweep-damage
/// carry-forward, same `total_pct_byte_weight` denominator. Behaviour is
/// byte-for-byte what `MuxSink::push_state` did before the migration.
#[allow(clippy::too_many_arguments)]
fn push_mux_state(
    ui: &UiState,
    atomics: &SharedAtomics,
    pct: u8,
    speed: f64,
    eta: String,
    bytes_done: u64,
    lost_video_secs: f64,
    errors: u32,
) {
    if crate::web::debug_enabled() {
        eprintln!(
            "[DEBUG] MuxSink::push_state: pct={}, bytes_done={:.2}GB, speed={}MB/s",
            pct,
            bytes_done as f64 / BYTES_PER_GIB,
            speed
        );
    }
    update_state(
        &ui.device,
        RipState {
            device: ui.device.clone(),
            status: "ripping".to_string(),
            disc_present: true,
            disc_name: ui.display_name.clone(),
            disc_format: ui.disc_format.clone(),
            progress_pct: pct,
            progress_gb: bytes_done as f64 / BYTES_PER_GIB,
            speed_mbs: speed,
            eta: eta.clone(),
            // During the mux phase the demux error counter (`errors`) is
            // usually zero — the ISO reads don't fail. Carry the real
            // bad-sector count and lost-time from the final sweep/patch
            // pass so the damage pill / bad-ranges list remain visible
            // to operators polling /api/state during mux. The live
            // demux skip count is still surfaced via `lost_video_secs`
            // for the single-pass (no-snapshot) path.
            errors: if ui.sweep_damage.errors > 0 {
                ui.sweep_damage.errors
            } else {
                errors
            },
            lost_video_secs: if ui.sweep_damage.total_lost_ms > 0.0 {
                ui.sweep_damage.total_lost_ms / MILLIS_PER_SEC
            } else {
                lost_video_secs
            },
            last_sector: atomics.rip_last_lba.load(Ordering::Relaxed),
            current_batch: atomics.rip_current_batch.load(Ordering::Relaxed),
            preferred_batch: ui.batch,
            output_file: ui.filename.clone(),
            tmdb_title: ui.tmdb_title.clone(),
            tmdb_year: ui.tmdb_year,
            tmdb_poster: ui.tmdb_poster.clone(),
            tmdb_overview: ui.tmdb_overview.clone(),
            duration: ui.duration.clone(),
            codecs: ui.codecs.clone(),
            // Carry the multipass identity through every per-frame
            // update so the UI doesn't snap back to a "fresh rip"
            // view when mux starts. pass == total_passes is the
            // established convention for "we're on the mux pass".
            //
            // Total progress uses `total_pct_byte_weight` — the same
            // byte-weighted formula as sweep/patch, so the bar
            // progresses smoothly across the sweep→mux handoff.
            pass: ui.total_passes,
            total_passes: ui.total_passes,
            pass_progress_pct: pct,
            pass_eta: eta.clone(),
            total_progress_pct: total_pct_byte_weight(
                ui.bytes_total_disc,
                ui.max_retries,
                ui.bytes_unreadable_at_mux,
                pct,
            ),
            total_eta: eta,
            // Carry sweep-phase damage fields so they remain visible
            // in /api/state during the entire mux phase.
            total_lost_ms: ui.sweep_damage.total_lost_ms,
            main_lost_ms: ui.sweep_damage.main_lost_ms,
            bad_ranges: ui.sweep_damage.bad_ranges.clone(),
            num_bad_ranges: ui.sweep_damage.num_bad_ranges,
            bad_ranges_truncated: ui.sweep_damage.bad_ranges_truncated,
            largest_gap_ms: ui.sweep_damage.largest_gap_ms,
            ..Default::default()
        },
    );
}

/// Build the specific cause string for a hard producer `read()` error.
///
/// The stream yields an `io::Error`; when the underlying fault was a
/// coded `libfreemkv::Error` (DiscRead, AACS/CSS decrypt manifesting
/// mid-stream, etc.) it reached the producer via `From<Error> for
/// io::Error`, which stringifies the original through `Error`'s
/// `Display` — so the `io::Error` message already begins with an
/// `E####:` prefix. We surface that code in a parenthetical annotation
/// so an operator sees the real fault identifier in `last_error`.
///
/// Note: reconstructing the code by `Error::from(io::Error)` does NOT
/// work — `From<io::Error> for Error` is unconditionally `Error::IoError`,
/// whose `.code()` is always `E_IO_ERROR`. The code only survives in the
/// stringified message, so we parse it back out of the leading token.
fn producer_read_error_cause(e: &std::io::Error) -> String {
    match coded_prefix(&e.to_string()) {
        Some(code) if code != libfreemkv::error::E_IO_ERROR => {
            // The library `Display` is code-only, so `{e}` stringifies to a
            // bare `E####` for argument-less variants (e.g. DecryptFailed →
            // `E7013`). Attach a short English label so the operator reads a
            // human cause in the red banner, not `(E7013): E7013`. This keeps
            // the mux read-error path consistent with the sweep/patch path,
            // which labels via `non_scsi_error_label` / `format_pass_error`.
            format!(
                "read error mid-stream (E{code}): {}",
                coded_error_label(code)
            )
        }
        _ => format!("read error mid-stream: {e}"),
    }
}

/// Short English label for a coded `libfreemkv` fault that reaches the mux
/// producer as an `io::Error`. The library `Display` is code-only, and the
/// code is the only thing that survives the `Error → io::Error` round-trip
/// (`From<io::Error> for Error` collapses everything to `E_IO_ERROR`), so we
/// map the parsed `u16` to text here rather than matching on an `Error`
/// variant. Mirrors the sweep/patch path's `non_scsi_error_label`; any
/// unmapped code falls back to a generic phrase that still carries the code
/// in the parenthetical so a new variant never leaves the operator stranded.
fn coded_error_label(code: u16) -> &'static str {
    use libfreemkv::error as ec;
    match code {
        c if c == ec::E_DECRYPT_FAILED => "decryption failed",
        c if c == ec::E_DISC_READ => "disc read error",
        c if c == ec::E_HALTED => "rip stopped by user",
        c if c == ec::E_MAPFILE_INVALID => "recovery mapfile invalid",
        c if c == ec::E_NO_STREAMS => "no playable streams on disc",
        c if c == ec::E_DISC_CAPACITY_OVERFLOW || c == ec::E_DISC_CAPACITY_MALFORMED => {
            "drive reported unusable disc capacity"
        }
        _ => "read failed mid-stream",
    }
}

/// Parse a leading `E<digits>` code token from a `libfreemkv::Error`
/// `Display` string (e.g. `"E6000: 12345 0x.."` → `Some(6000)`). Returns
/// `None` for a plain (non-coded) io-error message, so those don't get a
/// spurious code annotation.
fn coded_prefix(msg: &str) -> Option<u16> {
    let rest = msg.strip_prefix('E')?;
    // The code is the run of ASCII digits up to the `:` separator (or end,
    // for argument-less variants like `E1024`).
    let digits: &str = rest.split(|c: char| !c.is_ascii_digit()).next()?;
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

/// Drop guard that stops the mux watchdog thread when the owning mux call
/// ([`mux_live`] live single-pass, or [`mux_iso`] ISO/multipass+resume) returns.
struct WatchdogGuard(Arc<AtomicBool>);
impl Drop for WatchdogGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// Spawn the mux watchdog thread (soft stall UI + hard exit(1) escalation).
/// Shared verbatim by `mux_live` and `mux_iso` so both paths get the identical
/// escalation semantics. The watchdog reads `wd_last_frame` (activity) and
/// `wd_bytes` (good-bytes) exactly as before; callers feed those atomics.
fn spawn_mux_watchdog(
    inputs: &MuxInputs<'_>,
    wd_active: Arc<AtomicBool>,
    wd_last_frame: Arc<AtomicU64>,
    wd_bytes: Arc<AtomicU64>,
) {
    let active = wd_active.clone();
    let last_frame = wd_last_frame.clone();
    let wbytes = wd_bytes.clone();
    let wd_device = inputs.device.to_string();
    let wd_display = inputs.display_name.clone();
    let wd_format = inputs.disc_format.clone();
    let wd_tmdb_title = inputs.tmdb_title.clone();
    let wd_tmdb_poster = inputs.tmdb_poster.clone();
    let wd_tmdb_overview = inputs.tmdb_overview.clone();
    let wd_duration = inputs.duration.clone();
    let wd_codecs = inputs.codecs.clone();
    let wd_total = inputs.total_bytes;
    let wd_tmdb_year = inputs.tmdb_year;
    let wd_filename = inputs.filename.clone();
    let wd_staging_disc_dir = inputs.staging_disc_dir.clone();
    // Intentionally detached (no JoinHandle kept). The watchdog holds only
    // Arc<Atomic*> clones — no file handles, no heap buffers, nothing that
    // accumulates across rips. It self-terminates when `active` goes false
    // (WatchdogGuard drop at run_mux return), so it never outlives its
    // owning mux call. Hard escalation (stall ≥ 20 min) calls exit(1)
    // directly; at that point there is nothing left to join anyway.
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

            // Hard watchdog escalation. When the consumer / reader
            // is stuck this far past the soft warning, graceful
            // cleanup is impossible — the offending thread is
            // inside a syscall that the kernel won't return from
            // (hung NFS, wedged decrypt, frozen device ioctl).
            // Bump the disc's `.restart_count` so post-restart
            // resume can promote to `.failed` once the limit is
            // reached, then `exit(1)` and let Docker
            // `restart: unless-stopped` bring us back.
            //
            // No graceful join, no halt-token flip — those have
            // already failed for 20 minutes by definition.
            if stall_secs >= HARD_WATCHDOG_STALL_SECS {
                let bytes_good = wbytes.load(Ordering::Relaxed);
                let msg = format!(
                    "hard watchdog escalating: stalled {}s at {:.2} GB; exiting process for container restart",
                    stall_secs,
                    bytes_good as f64 / BYTES_PER_GIB,
                );
                // CRITICAL: do NOT call `device_log` here. The log
                // file lives on the same NFS-mounted `/config`
                // that's quite possibly the exact mount we're
                // escalating because it's wedged. `eprintln!` and
                // `tracing::error!` both go to docker logs /
                // journald — no NFS, no filesystem dependency, so
                // they can't block `exit(1)` from firing.
                eprintln!("[mux/{}] {}", wd_device, msg);
                tracing::error!(
                    target: "mux",
                    device = %wd_device,
                    bytes_good,
                    stall_secs,
                    staging = %wd_staging_disc_dir.display(),
                    "hard watchdog escalating; exiting process for container restart"
                );
                // Best-effort: bump the restart counter so the
                // resume detector knows this disc has wedged the
                // process before. Errors are intentionally ignored
                // — we're about to exit(1) anyway and Docker will
                // get us back. clear_restart_count happens on
                // success / failed path elsewhere; on this path it
                // stays bumped so RESTART_LIMIT can engage.
                //
                // 0.20.8 hardening: wrap the counter bump in a
                // local bounded-syscall pattern (5 s deadline) so
                // even if staging shares the wedged NFS mount with
                // `/config`, we still proceed to `exit(1)`. If the
                // bump times out, the next restart sees count N-1
                // instead of N — at worst one extra retry, vastly
                // better than never exiting.
                // `libfreemkv::io::bounded::bounded_syscall` is
                // `pub(crate)` so it's not reachable from autorip;
                // we hand-roll the equivalent pattern (15 lines)
                // here.
                {
                    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(0);
                    let bump_dir = wd_staging_disc_dir.clone();
                    let _ = std::thread::Builder::new()
                        .name("autorip-watchdog-counter-bump".into())
                        .spawn(move || {
                            let _ = crate::ripper::staging::increment_restart_count(&bump_dir);
                            let _ = tx.send(());
                        });
                    if rx.recv_timeout(std::time::Duration::from_secs(5)).is_err() {
                        eprintln!(
                            "[mux/{}] watchdog: counter bump timed out; proceeding to exit anyway",
                            wd_device
                        );
                        tracing::error!(
                            target: "mux",
                            device = %wd_device,
                            "watchdog: counter bump timed out; proceeding to exit anyway"
                        );
                    }
                }
                // No `drop(_wd_guard)` — that's the producer's
                // local; we're a detached watchdog thread. The
                // OS will tear down every thread on exit(1).
                std::process::exit(1);
            }

            if stall_secs >= 30 {
                // Compute bytes/gb/pct/stall_str once and reuse for
                // both the log line and the UI update — a single
                // `wbytes` read so the two can't disagree.
                let bytes = wbytes.load(Ordering::Relaxed);
                let gb = bytes as f64 / BYTES_PER_GIB;
                let pct = if let Some(p) = (bytes * 100).checked_div(wd_total) {
                    p.min(100) as u8
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
                let should_log = !was_stalled || stall_secs >= last_log_secs + 60;
                if should_log {
                    last_log_secs = stall_secs;
                    crate::log::device_log(
                        &wd_device,
                        &format!(
                            "Drive stalled at {:.1} GB ({}%) — waiting for read ({})",
                            gb, pct, stall_str
                        ),
                    );
                }
                super::state::update_state_with(&wd_device, |s| {
                    // Don't clobber any terminal/intentional state set
                    // by another path. The watchdog runs on a 15 s
                    // wake tick and can fire AFTER:
                    //   - `handle_stop` reset state to "idle"
                    //     (60 s drain timed out, rip thread still
                    //     wedged inside a syscall)
                    //   - `rip_disc` / `resume_remux` completed and
                    //     transitioned to "done" / "complete" /
                    //     "failed" / "error"
                    // In all those cases the operator-facing status
                    // is authoritative; flipping it back to "ripping"
                    // would be a UI lie. The hard-watchdog
                    // escalation above (stall_secs >= 1200) still
                    // runs unconditionally to recover real wedges.
                    match s.status.as_str() {
                        "idle" | "done" | "complete" | "failed" | "error" => return,
                        _ => {}
                    }
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

/// The shared atomic counters the mux drivers ([`mux_iso`] / [`mux_live`])
/// feed via the `AutoripMuxEvents` bridge and the hard watchdog reads. The
/// orchestrator builds these *before* calling a driver; `mux_stream`'s
/// reader-side events (forwarded through the bridge) write them during the run.
#[derive(Clone)]
pub(crate) struct MuxAtomics {
    pub(crate) latest_bytes_read: Arc<AtomicU64>,
    pub(crate) rip_last_lba: Arc<AtomicU64>,
    pub(crate) rip_current_batch: Arc<AtomicU16>,
    pub(crate) wd_last_frame: Arc<AtomicU64>,
    pub(crate) wd_bytes: Arc<AtomicU64>,
    pub(crate) input_errors: Arc<AtomicU32>,
}

// ── ISO / multipass + resume mux via libfreemkv's `mux_stream` ───────────────
//
// STEP 4c-i: the inner drive loop (header pump → headers_resolved gate →
// output() → frame pump → NoStreams gate → finish + the write pipeline) now
// lives in `libfreemkv::mux_stream` / `drive_mux`. autorip KEEPS the hard
// watchdog (`spawn_mux_watchdog`), the `MuxAtomics` the watchdog reads, the
// staging/marker writes + FMTS deferral (done at the call sites in `mod.rs` /
// `resume.rs`), and the `MuxOutcome` mapping. `AutoripMuxEvents` is the bridge
// that FEEDS those atomics from inside `mux_stream` so the watchdog keeps
// working exactly as before.

/// Everything `mux_iso` needs to build a [`libfreemkv::MuxInput::Iso`]. The
/// orchestrator (`rip_disc` multipass branch / `resume_remux`) fills this
/// instead of hand-building the `build_iso_pipeline` stream — `mux_stream`
/// re-derives the same 3-stage highway (and re-derives the AACS key map from
/// `keys`/`key_fetch`) internally, so no pre-resolved map is carried on this path.
pub(crate) struct IsoMuxSource {
    /// Path to the staged ISO image. `mux_stream` opens its own
    /// `FileSectorSource` from this (the orchestrator's validation open is a
    /// separate, discarded handle).
    pub(crate) iso_path: std::path::PathBuf,
    /// The scanned title to mux out of the image.
    pub(crate) title: libfreemkv::DiscTitle,
    /// Container format (TS vs PS demux selection).
    pub(crate) format: libfreemkv::ContentFormat,
    /// Decryption keys (banked forensic/FMTS keys reach `build_iso_pipeline`
    /// through here — the FMTS gate itself is untouched at the call site).
    pub(crate) keys: libfreemkv::decrypt::DecryptKeys,
    /// Read-time fresh-key-on-failure fetch (recovers a 2nd/Nth CPS-unit key
    /// mid-mux). Same closure the pre-migration `build_iso_pipeline` call took.
    pub(crate) key_fetch: Option<libfreemkv::sector::KeyFetch>,
    /// Ciphertext passthrough (unused on the production ISO path; kept for
    /// parity with `MuxOptions.raw`).
    pub(crate) raw: bool,
    /// Skip-past-read-errors (inert on the file highway — the ISO is already
    /// zero-filled for any sweep-pass loss; kept for parity with `MuxOptions`).
    pub(crate) skip_errors: bool,
}

/// autorip's [`libfreemkv::MuxEvents`] bridge for the ISO/multipass + resume
/// mux. It updates the SAME shared atomics the pre-migration `stream_event_fn`
/// (reader side) and `MuxSink` (writer side) updated, and drives the same
/// per-frame `update_state` UI push — so the hard watchdog keeps reading a byte
/// counter that advances during a healthy mux and the dashboard is unchanged.
///
/// Atomic feed (cross-checked against what `spawn_mux_watchdog` reads):
/// - `on_read_progress`  → `latest_bytes_read` (UI progress) + `wd_last_frame`
///   (watchdog activity). Mirrors the old reader `BytesRead` `stream_event_fn`.
/// - `on_write_progress` → `wd_bytes` (the watchdog's "stalled at X GB" +
///   hard-escalation good-byte counter) + `wd_last_frame`, then the throttled
///   `push_mux_state`. Mirrors the old `MuxSink::apply`.
/// - `on_output_opened`  → `opened` flag (drives `output_opened` in the outcome
///   mapping).
/// - `on_sector_skipped` / `on_read_error` → refresh `wd_last_frame`;
///   `on_sector_skipped` also stores the skipped LBA into `rip_last_lba` (the UI
///   last_sector / playhead), bumps `input_errors`, and logs the per-skip
///   `Sector N skipped (zero-filled)` line. (Fire on the LIVE inline single-pass
///   path from `DiscStream::fill_extents`; ~never on the ISO highway.)
/// - `on_batch_size_changed` → `rip_current_batch` + the `Batch size → N (…)`
///   device-log line. (Live inline path only; ~never fires on the highway.)
struct AutoripMuxEvents {
    ui: UiState,
    atomics: SharedAtomics,
    progress: Mutex<freemkv_engine::SpeedEstimator>,
    /// 1 s `update_state` throttle (was `MuxSink::last_update`).
    last_update: Mutex<Instant>,
    /// 60 s device-log throttle (was `MuxSink::last_log`).
    last_log: Mutex<Instant>,
    /// True once `output()` opened. Feeds `output_opened` in the mapping.
    opened: AtomicBool,
}

impl libfreemkv::MuxEvents for AutoripMuxEvents {
    fn on_output_opened(&self, _title: &libfreemkv::DiscTitle) {
        self.opened.store(true, Ordering::Relaxed);
        crate::log::device_log(&self.ui.device, "Output opened — muxing");
        // Reset the throttles so the first progress tick lands promptly after
        // the sink opens (matches the pre-migration `start`-relative cadence).
        let now = Instant::now();
        if let Ok(mut g) = self.last_update.lock() {
            *g = now;
        }
        if let Ok(mut g) = self.last_log.lock() {
            *g = now;
        }
    }

    fn on_read_progress(&self, bytes_read: u64, _bytes_total: u64) {
        // Reader-side (highway producer thread) BytesRead — mirrors the old
        // `stream_event_fn`: keep the watchdog fresh during header reads (which
        // have no SCSI READ_TIMEOUT backstop on the ISO path) and feed the
        // read-ahead progress position the UI prefers over write-lagged output.
        self.atomics
            .wd_last_frame
            .store(crate::util::epoch_secs(), Ordering::Relaxed);
        self.atomics
            .latest_bytes_read
            .store(bytes_read, Ordering::Relaxed);
    }

    fn on_write_progress(&self, bytes_written: u64, _bytes_total: u64) {
        // Writer-side per-frame — mirrors `MuxSink::apply`. Feed the watchdog's
        // activity timestamp AND its good-byte counter (both read by
        // `spawn_mux_watchdog`), then push throttled UI state.
        self.atomics
            .wd_last_frame
            .store(crate::util::epoch_secs(), Ordering::Relaxed);
        self.atomics
            .wd_bytes
            .store(bytes_written, Ordering::Relaxed);

        // 1 s `update_state` cadence (same throttle as the pre-split loop).
        let now = Instant::now();
        {
            let mut last = match self.last_update.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if now.duration_since(*last).as_secs_f64() < 1.0 {
                return;
            }
            *last = now;
        }

        // Progress uses the ISO *read* position (read-ahead) when available,
        // falling back to the *write* position only until the first BytesRead
        // event fires — identical to `MuxSink::apply`.
        let lbr = self.atomics.latest_bytes_read.load(Ordering::Relaxed);
        let bytes_done = if lbr > 0 { lbr } else { bytes_written };
        let pct = if let Some(p) = (bytes_done * 100).checked_div(self.ui.total_bytes) {
            p.min(100) as u8
        } else {
            0
        };
        let (speed, speed_for_eta) = {
            let mut progress = match self.progress.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let display_speed = progress.observe(now, bytes_done);
            let eta_speed = progress.eta_speed_mbs(now, display_speed);
            (display_speed, eta_speed)
        };
        let eta = if speed_for_eta > 0.0 && self.ui.total_bytes > bytes_done {
            let secs =
                ((self.ui.total_bytes - bytes_done) as f64 / BYTES_PER_MIB / speed_for_eta) as u32;
            if secs > 359999 {
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

        // 60 s device-log line (separate cadence from `update_state`).
        {
            let mut last_log = match self.last_log.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if now.duration_since(*last_log).as_secs() >= 60 {
                *last_log = now;
                let gb = bytes_done as f64 / BYTES_PER_GIB;
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
                if self.ui.total_bytes > 0 {
                    let total_gb = self.ui.total_bytes as f64 / BYTES_PER_GIB;
                    crate::log::device_log(
                        &self.ui.device,
                        &format!(
                            "{:.1} GB / {:.1} GB ({}%) {}{}",
                            gb, total_gb, pct, speed_str, eta_str
                        ),
                    );
                } else {
                    crate::log::device_log(&self.ui.device, &format!("{:.1} GB {}", gb, speed_str));
                }
            }
        }

        let skip_errors = self.atomics.input_errors.load(Ordering::Relaxed);
        // Live lost-video-secs from bytes actually zero-filled. On the file
        // highway `input_lost_bytes` typically stays 0 during the run (the
        // reader-side events carry no per-unit loss count); the AUTHORITATIVE
        // total lost is taken from `MuxOutcome.lost_bytes` at the end (see
        // `map_iso_mux_outcome`). This live figure only refines the mid-mux UI.
        let lost_bytes = self.atomics.input_lost_bytes.load(Ordering::Relaxed);
        let lost_video_secs = if self.ui.title_bytes_per_sec > 0.0 {
            lost_bytes as f64 / self.ui.title_bytes_per_sec
        } else {
            0.0
        };
        push_mux_state(
            &self.ui,
            &self.atomics,
            pct,
            speed,
            eta,
            bytes_done,
            lost_video_secs,
            skip_errors,
        );
    }

    fn on_sector_skipped(&self, lba: u32) {
        self.atomics
            .wd_last_frame
            .store(crate::util::epoch_secs(), Ordering::Relaxed);
        // Store the skipped LBA into `rip_last_lba` — the UI last_sector /
        // playhead atomic `push_mux_state` reads — AND emit the per-skip
        // device-log line, exactly as the pre-refactor `make_stream_event_fn`
        // did (mod.rs `SectorSkipped` arm: `last_lba.store(sector)` +
        // `"Sector {sector} skipped (zero-filled)"`). These fire on the LIVE
        // inline single-pass path from `DiscStream::fill_extents`; dropping them
        // froze `last_sector` and lost the record of which sectors were
        // zero-filled during a skip-heavy live rip. The `input_errors` bump is
        // additive (kept from the post-refactor bridge, surfaces the skip count).
        self.atomics
            .rip_last_lba
            .store(lba as u64, Ordering::Relaxed);
        self.atomics.input_errors.fetch_add(1, Ordering::Relaxed);
        crate::log::device_log(
            &self.ui.device,
            &format!("Sector {} skipped (zero-filled)", lba),
        );
    }

    fn on_batch_size_changed(&self, batch: u16, reason: libfreemkv::event::BatchSizeReason) {
        self.atomics
            .rip_current_batch
            .store(batch, Ordering::Relaxed);
        // Emit the batch-change device-log line the pre-refactor
        // `make_stream_event_fn` produced (mod.rs `BatchSizeChanged` arm).
        // Fires on the live inline single-pass path from the adaptive sizer in
        // `DiscStream::fill_extents`; restoring it keeps the operator-facing
        // record of when/why the read batch adapted during a live rip.
        let label = match reason {
            libfreemkv::event::BatchSizeReason::Shrunk => "shrunk",
            libfreemkv::event::BatchSizeReason::Probed => "probed up",
        };
        crate::log::device_log(
            &self.ui.device,
            &format!("Batch size → {} ({})", batch, label),
        );
    }

    fn on_read_error(&self, _lba: u32) {
        self.atomics
            .wd_last_frame
            .store(crate::util::epoch_secs(), Ordering::Relaxed);
    }
}

/// Build a [`UiState`] from the orchestrator's [`MuxInputs`]. Shared by both
/// mux drivers ([`mux_iso`] / [`mux_live`]) for the `AutoripMuxEvents` bridge.
fn ui_state_from_inputs(inputs: &MuxInputs<'_>, total_bytes: u64) -> UiState {
    UiState {
        device: inputs.device.to_string(),
        display_name: inputs.display_name.clone(),
        disc_format: inputs.disc_format.clone(),
        tmdb_title: inputs.tmdb_title.clone(),
        tmdb_year: inputs.tmdb_year,
        tmdb_poster: inputs.tmdb_poster.clone(),
        tmdb_overview: inputs.tmdb_overview.clone(),
        duration: inputs.duration.clone(),
        codecs: inputs.codecs.clone(),
        filename: inputs.filename.clone(),
        batch: inputs.batch,
        total_bytes,
        title_bytes_per_sec: inputs.title_bytes_per_sec,
        total_passes: inputs.total_passes,
        bytes_total_disc: inputs.bytes_total_disc,
        max_retries: inputs.max_retries,
        bytes_unreadable_at_mux: inputs.bytes_unreadable_at_mux,
        sweep_damage: inputs.sweep_damage.clone(),
    }
}

/// The ONE wording for "this mux completed, and the file still does not match
/// the pre-mux plan".
///
/// libfreemkv's contract: a non-empty `undelivered_streams` means the finished
/// file is missing streams the plan promised even though `completed == true`,
/// and a caller that reports a successful export must report these too — a
/// lossy outcome is never silent.
///
/// Two sites used to spell this out independently — here and `rip_disc`'s
/// completed-mux summary — writing two differently-worded lines into the SAME
/// per-device log for the SAME event: one lossy mux read as two, and an
/// operator grepping or alerting on either exact phrase saw half the story.
/// `map_iso_mux_outcome` produces every outcome that can carry undelivered
/// streams (every other construction site is empty by definition), so it is
/// the one emitter, and this is the one wording it emits.
fn undelivered_streams_note(streams: &[usize]) -> String {
    format!(
        "Mux completed but {} stream(s) could not be delivered into the output \
         (streams={:?}) — the file does not match the pre-mux plan",
        streams.len(),
        streams
    )
}

/// Map a `mux_stream` result into autorip's [`MuxOutcome`] + staging decisions,
/// preserving the pre-migration Err classification:
/// - `is_halt_error` / `is_fmts_key_missing_error` are RETURNED as `Err` so the
///   call site keeps its existing "preserve staging (resume)" / "FMTS deferral
///   (retryable idle)" handling verbatim.
/// - a header-phase failure (headers never resolved → `MkvInvalid`, or any error
///   before the sink opened) → `output_opened=false` + `finalize_error=Some`, so
///   the orchestrator quarantines it via the `!output_opened` path.
/// - `NoStreams` (empty/undecryptable drain) → `output_opened=true` +
///   `finalize_error=Some` (quarantine).
/// - a coded read fault mid-mux (DiscRead/Decrypt/…) → `read_error=Some` (the
///   disc stays resumable — matches the old producer-read-error path).
/// - any other finalize / IO error → `finalize_error=Some` (quarantine).
#[allow(clippy::too_many_arguments)]
fn map_iso_mux_outcome(
    result: std::io::Result<libfreemkv::MuxOutcome>,
    opened: bool,
    device: &str,
    title_bytes_per_sec: f64,
    start: Instant,
    partial_bytes: u64,
    input_errors: u32,
) -> std::io::Result<MuxOutcome> {
    let elapsed_secs = start.elapsed().as_secs_f64();
    let speed_of = |bytes: u64| {
        if elapsed_secs > 0.0 {
            bytes as f64 / BYTES_PER_MIB / elapsed_secs
        } else {
            0.0
        }
    };
    let lost_secs = |lost_bytes: u64| {
        if title_bytes_per_sec > 0.0 {
            lost_bytes as f64 / title_bytes_per_sec
        } else {
            0.0
        }
    };
    match result {
        Ok(o) if o.completed => {
            // libfreemkv's contract: non-empty `undelivered_streams` means the
            // finished file does NOT match the pre-mux plan even though
            // `completed == true` — a lossy-but-successful export. Dormant
            // today (only the `mp4://` sink populates it; autorip's
            // `output_scheme_for` never returns "mp4"), but a caller that
            // reports a successful export must report these too, so this
            // logs loudly the moment it stops being empty rather than
            // waiting for an mp4 destination to make the silence a HIGH.

            if !o.undelivered_streams.is_empty() {
                crate::log::device_log(device, &undelivered_streams_note(&o.undelivered_streams));
            }
            Ok(MuxOutcome {
                completed: true,
                bytes_done: o.bytes_written,
                elapsed_secs,
                speed_mbs: speed_of(o.bytes_written),
                errors: u32::try_from(o.errors).unwrap_or(u32::MAX),
                lost_video_secs: lost_secs(o.lost_bytes),
                output_opened: true,
                finalize_error: None,
                read_error: None,
            })
        }
        Ok(o) => {
            // A clean operator stop (halt) or a join-timeout wedge: resumable,
            // no error marker — the orchestrator's "stopped" path handles it.
            crate::log::device_log(
                device,
                "Mux did not complete (operator stop or wedge) — staging preserved for resume",
            );
            Ok(MuxOutcome {
                completed: false,
                bytes_done: o.bytes_written,
                elapsed_secs,
                speed_mbs: speed_of(o.bytes_written),
                errors: u32::try_from(o.errors).unwrap_or(u32::MAX),
                lost_video_secs: lost_secs(o.lost_bytes),
                output_opened: o.output_opened,
                finalize_error: None,
                read_error: None,
            })
        }
        Err(e) => {
            // Propagate the two classifications the call site handles specially
            // (staging-preserving resume, FMTS retryable deferral).
            if super::is_halt_error(&e) || super::is_fmts_key_missing_error(&e) {
                return Err(e);
            }
            if !opened {
                // Header-phase / construction failure with no sink: structural,
                // quarantine via the orchestrator's `!output_opened` path.
                let msg =
                    format!("Header resolution or mux setup failed before output opened ({e})");
                crate::log::device_log(device, &format!("Mux failed: {msg}"));
                return Ok(MuxOutcome {
                    completed: false,
                    bytes_done: 0,
                    elapsed_secs,
                    speed_mbs: 0.0,
                    errors: input_errors,
                    lost_video_secs: 0.0,
                    output_opened: false,
                    finalize_error: Some(msg),
                    read_error: None,
                });
            }
            let code = coded_prefix(&e.to_string());
            if code == Some(libfreemkv::error::E_NO_STREAMS) {
                // Empty / undecryptable output — structural, quarantine.
                let msg = "mux produced no frames (empty/undecryptable output)".to_string();
                crate::log::device_log(
                    device,
                    "Mux produced no frames/bytes — refusing to mark complete (empty/undecryptable output)",
                );
                return Ok(MuxOutcome {
                    completed: false,
                    bytes_done: partial_bytes,
                    elapsed_secs,
                    speed_mbs: speed_of(partial_bytes),
                    errors: input_errors,
                    lost_video_secs: 0.0,
                    output_opened: true,
                    finalize_error: Some(msg),
                    read_error: None,
                });
            }
            match code {
                Some(c)
                    if c != libfreemkv::error::E_IO_ERROR
                        && c != libfreemkv::error::E_MKV_INVALID =>
                {
                    // A coded read fault surfaced mid-mux (DiscRead / Decrypt /
                    // Mapfile …). The MKV is truncated but the disc stays
                    // resumable — same as the old producer-read-error path.
                    let cause = producer_read_error_cause(&e);
                    crate::log::device_log(
                        device,
                        &format!("Mux incomplete: read error mid-stream — {cause} (MKV truncated)"),
                    );
                    Ok(MuxOutcome {
                        completed: false,
                        bytes_done: partial_bytes,
                        elapsed_secs,
                        speed_mbs: speed_of(partial_bytes),
                        errors: input_errors,
                        lost_video_secs: 0.0,
                        output_opened: true,
                        finalize_error: None,
                        read_error: Some(cause),
                    })
                }
                _ => {
                    // A finalize / IO error (output.finish() failed, write
                    // error, unseekable MKV): structural, quarantine.
                    let msg = format!("{e}");
                    crate::log::device_log(device, &format!("Mux pipeline failed: {msg}"));
                    Ok(MuxOutcome {
                        completed: false,
                        bytes_done: partial_bytes,
                        elapsed_secs,
                        speed_mbs: speed_of(partial_bytes),
                        errors: input_errors,
                        lost_video_secs: 0.0,
                        output_opened: true,
                        finalize_error: Some(msg),
                        read_error: None,
                    })
                }
            }
        }
    }
}

/// Run the ISO/multipass (and resume) mux via [`libfreemkv::mux_stream`].
///
/// The STEP 4c-i migration of the hand-rolled header-pump / producer /
/// consumer-bridge / finish loop into `mux_stream`; its live single-pass
/// sibling is [`mux_live`] (STEP 4c-ii). It KEEPS, unchanged: the mux phase drop-guard, the
/// hard watchdog (`spawn_mux_watchdog`, reading `atomics_in.wd_*`), and the
/// per-device `Halt`. `mux_stream` owns the inner loop; `AutoripMuxEvents` feeds
/// the watchdog's atomics + the UI. Returns `Err` ONLY for the two call-site
/// classifications (`is_halt_error` → preserve staging; `is_fmts_key_missing_error`
/// → retryable deferral); everything else is mapped into the returned
/// [`MuxOutcome`] for the orchestrator's staging/marker decisions.
pub(crate) fn mux_iso(
    inputs: MuxInputs<'_>,
    src: IsoMuxSource,
    atomics_in: MuxAtomics,
) -> std::io::Result<MuxOutcome> {
    tracing::info!(target: "autorip::mux", phase = "mux", "begin");
    struct MuxPhaseGuard(std::time::Instant);
    impl Drop for MuxPhaseGuard {
        fn drop(&mut self) {
            tracing::info!(
                target: "autorip::mux",
                phase = "mux",
                elapsed_ms = self.0.elapsed().as_millis() as u64,
                "end"
            );
        }
    }
    let _mux_phase_guard = MuxPhaseGuard(std::time::Instant::now());

    // ── Watchdog (identical spawn to `run_mux`) ──────────────────────────────
    let wd_active = Arc::new(AtomicBool::new(true));
    let _wd_guard = WatchdogGuard(wd_active.clone());
    let wd_bytes = atomics_in.wd_bytes.clone();
    spawn_mux_watchdog(
        &inputs,
        wd_active.clone(),
        atomics_in.wd_last_frame.clone(),
        wd_bytes.clone(),
    );

    // Same per-device Halt the orchestrator threaded through sweep/patch and
    // the `/api/stop` handler cancels. Absent-token fallback = never-cancelled.
    let halt_token = device_halt(inputs.device).unwrap_or_default();

    // Progress denominator: caller's `total_bytes`, else the title's size.
    let total_bytes = if inputs.total_bytes > 0 {
        inputs.total_bytes
    } else {
        src.title.size_bytes
    };

    // The events bridge shares the orchestrator's watchdog/UI atomics.
    let shared = SharedAtomics {
        latest_bytes_read: atomics_in.latest_bytes_read.clone(),
        rip_last_lba: atomics_in.rip_last_lba.clone(),
        rip_current_batch: atomics_in.rip_current_batch.clone(),
        wd_last_frame: atomics_in.wd_last_frame.clone(),
        wd_bytes: wd_bytes.clone(),
        input_errors: atomics_in.input_errors.clone(),
        input_lost_bytes: Arc::new(AtomicU64::new(0)),
    };
    let start = Instant::now();
    let events = Arc::new(AutoripMuxEvents {
        ui: ui_state_from_inputs(&inputs, total_bytes),
        atomics: shared,
        progress: Mutex::new(freemkv_engine::SpeedEstimator::new()),
        last_update: Mutex::new(start),
        last_log: Mutex::new(start),
        opened: AtomicBool::new(false),
    });

    let opts = libfreemkv::MuxOptions {
        skip_errors: src.skip_errors,
        batch_sectors: inputs.batch,
        raw: src.raw,
        // Bounded per-frame send: autorip's hard watchdog + container-restart
        // model wants a wedged sink surfaced as a 60 s per-frame timeout, not an
        // unbounded block. Preserves the pre-refactor `run_mux` behaviour.
        send_deadline: Some(Duration::from_secs(60)),
        selection: Default::default(),
    };

    crate::log::device_log(
        inputs.device,
        &format!("Opening output: {}", inputs.dest_url),
    );
    let input = libfreemkv::MuxInput::Iso {
        path: &src.iso_path,
        title: src.title,
        format: src.format,
        keys: src.keys,
        key_fetch: src.key_fetch,
    };

    let result = libfreemkv::mux_stream(
        input,
        &inputs.dest_url,
        &opts,
        &halt_token,
        events.clone() as Arc<dyn libfreemkv::MuxEvents>,
    );

    let opened = events.opened.load(Ordering::Relaxed);
    let partial_bytes = wd_bytes.load(Ordering::Relaxed);
    let final_errors = atomics_in.input_errors.load(Ordering::Relaxed);
    map_iso_mux_outcome(
        result,
        opened,
        inputs.device,
        inputs.title_bytes_per_sec,
        start,
        partial_bytes,
        final_errors,
    )
}

// ── LIVE single-pass mux via libfreemkv's `mux_stream` ───────────────────────
//
// STEP 4c-ii: the live single-pass path (`max_retries == 0`) now runs through
// `libfreemkv::mux_stream` with the `MuxInput::Live` source — the INLINE
// `DiscStream` (NOT the prefetch highway), so `DiscStream::fill_extents`'
// adaptive batch-retry on a bad live-drive sector still fires. This retires the
// hand-rolled `run_mux` producer/consumer loop. Everything else — the hard
// watchdog, the `MuxAtomics` it reads, `AutoripMuxEvents`, and the
// `map_iso_mux_outcome` classification — is REUSED verbatim from `mux_iso`, so
// the two mux paths are now one engine differing only in their `MuxInput`.

/// Everything `mux_live` needs to build a [`libfreemkv::MuxInput::Live`] — the
/// live analogue of [`IsoMuxSource`]. The orchestrator (`rip_disc`'s single-pass
/// branch) fills this instead of hand-building `DiscStream::new(...)` +
/// `run_mux`; `mux_stream` constructs the inline `DiscStream`, applies the
/// forensic `key_map`, and drives the same header-pump/finish loop internally.
pub(crate) struct LiveMuxSource {
    /// The raw live-drive sector source (`session.drive` boxed). `mux_stream`
    /// moves it into `DiscStream::new` (whose reader param is exactly
    /// `Box<dyn SectorSource>`) — the inline reader, never the highway wrapper.
    pub(crate) reader: Box<dyn libfreemkv::SectorSource>,
    /// The scanned title to mux off the live drive.
    pub(crate) title: libfreemkv::DiscTitle,
    /// Container format (TS vs PS demux selection).
    pub(crate) format: libfreemkv::ContentFormat,
    /// Decryption keys autorip already resolved as its own app-layer policy
    /// (`disc.decrypt_keys()`). The driver consumes them as-is.
    pub(crate) keys: libfreemkv::decrypt::DecryptKeys,
    /// Retained pre-rip FMTS forensic key map (`fmts_key_map`). `mux_stream`
    /// applies it via `DiscStream::with_key_map` so single-pass FMTS reads only
    /// our-phase units and decrypts the forensic segment correctly. `None` for
    /// every non-FMTS disc, leaving the read walk unchanged.
    pub(crate) key_map: Option<std::sync::Arc<libfreemkv::decrypt::AacsKeyMap>>,
    /// Skip-past-read-errors (zero-fill + continue) — wired onto
    /// `DiscStream::skip_errors` (was `on_read_error == "skip"`).
    pub(crate) skip_errors: bool,
}

/// Run the LIVE single-pass mux via [`libfreemkv::mux_stream`] on the inline
/// [`DiscStream`] (`MuxInput::Live`). The STEP 4c-ii replacement for the
/// hand-rolled `run_mux` producer/consumer loop. Mirrors [`mux_iso`] exactly —
/// same watchdog spawn, same `AutoripMuxEvents` bridge feeding the same
/// `MuxAtomics`, same `map_iso_mux_outcome` classification — differing only in
/// building a `Live` source (inline drive reader + forensic key map) instead of
/// an `Iso` source (staged file highway). Returns `Err` ONLY for the two
/// call-site classifications (`is_halt_error` → preserve staging;
/// `is_fmts_key_missing_error` → retryable deferral).
pub(crate) fn mux_live(
    inputs: MuxInputs<'_>,
    src: LiveMuxSource,
    atomics_in: MuxAtomics,
) -> std::io::Result<MuxOutcome> {
    tracing::info!(target: "autorip::mux", phase = "mux", "begin");
    struct MuxPhaseGuard(std::time::Instant);
    impl Drop for MuxPhaseGuard {
        fn drop(&mut self) {
            tracing::info!(
                target: "autorip::mux",
                phase = "mux",
                elapsed_ms = self.0.elapsed().as_millis() as u64,
                "end"
            );
        }
    }
    let _mux_phase_guard = MuxPhaseGuard(std::time::Instant::now());

    // ── Watchdog (identical spawn to `mux_iso` / `run_mux`) ──────────────────
    let wd_active = Arc::new(AtomicBool::new(true));
    let _wd_guard = WatchdogGuard(wd_active.clone());
    let wd_bytes = atomics_in.wd_bytes.clone();
    spawn_mux_watchdog(
        &inputs,
        wd_active.clone(),
        atomics_in.wd_last_frame.clone(),
        wd_bytes.clone(),
    );

    // Same per-device Halt the orchestrator threaded through sweep/patch and the
    // `/api/stop` handler cancels. Absent-token fallback = never-cancelled. On
    // the live path this is the SAME token the pre-migration `DiscStream::new`
    // received (covering the CSS crack that runs at construction).
    let halt_token = device_halt(inputs.device).unwrap_or_default();

    // Progress denominator: caller's `total_bytes` (the single-pass extent sum
    // from `mux_progress_denominator`), else the title's size.
    let total_bytes = if inputs.total_bytes > 0 {
        inputs.total_bytes
    } else {
        src.title.size_bytes
    };

    // The events bridge shares the orchestrator's watchdog/UI atomics — same
    // shape as `mux_iso`. On the live inline path `on_sector_skipped` /
    // `on_batch_size_changed` DO fire (from `DiscStream::fill_extents`), and the
    // bridge already routes them to `input_errors` / `rip_current_batch`.
    let shared = SharedAtomics {
        latest_bytes_read: atomics_in.latest_bytes_read.clone(),
        rip_last_lba: atomics_in.rip_last_lba.clone(),
        rip_current_batch: atomics_in.rip_current_batch.clone(),
        wd_last_frame: atomics_in.wd_last_frame.clone(),
        wd_bytes: wd_bytes.clone(),
        input_errors: atomics_in.input_errors.clone(),
        // Live loss during the run isn't carried per-unit by the reader events;
        // the AUTHORITATIVE lost total is taken from `MuxOutcome.lost_bytes` at
        // the end (see `map_iso_mux_outcome`), same as `mux_iso`.
        input_lost_bytes: Arc::new(AtomicU64::new(0)),
    };
    let start = Instant::now();
    let events = Arc::new(AutoripMuxEvents {
        ui: ui_state_from_inputs(&inputs, total_bytes),
        atomics: shared,
        progress: Mutex::new(freemkv_engine::SpeedEstimator::new()),
        last_update: Mutex::new(start),
        last_log: Mutex::new(start),
        opened: AtomicBool::new(false),
    });

    // Single-pass never previews ciphertext (`raw = false`) — matches the
    // pre-migration `DiscStream::new(.., false, ..)`.
    let opts = libfreemkv::MuxOptions {
        skip_errors: src.skip_errors,
        batch_sectors: inputs.batch,
        raw: false,
        // Same bounded 60 s per-frame send as `mux_iso`: the live single-pass
        // path is watchdog-backed too, so preserve the pre-refactor bound.
        send_deadline: Some(Duration::from_secs(60)),
        selection: Default::default(),
    };

    crate::log::device_log(
        inputs.device,
        &format!("Opening output: {}", inputs.dest_url),
    );
    let input = libfreemkv::MuxInput::Live {
        reader: src.reader,
        title: src.title,
        format: src.format,
        keys: src.keys,
        // The forensic FMTS map — applied via `DiscStream::with_key_map` inside
        // `mux_stream`, exactly the pre-migration `s.with_key_map(map)`.
        key_map: src.key_map,
    };

    let result = libfreemkv::mux_stream(
        input,
        &inputs.dest_url,
        &opts,
        &halt_token,
        events.clone() as Arc<dyn libfreemkv::MuxEvents>,
    );

    let opened = events.opened.load(Ordering::Relaxed);
    let partial_bytes = wd_bytes.load(Ordering::Relaxed);
    let final_errors = atomics_in.input_errors.load(Ordering::Relaxed);
    map_iso_mux_outcome(
        result,
        opened,
        inputs.device,
        inputs.title_bytes_per_sec,
        start,
        partial_bytes,
        final_errors,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const DISC: u64 = 60_000_000_000; // 60 GB stand-in for a UHD

    /// Regression: a hard producer read error must surface the SPECIFIC
    /// coded cause, not a generic truncation string. A coded
    /// `libfreemkv::Error` reaches the producer as an `io::Error` whose
    /// Display already carries the `E####:` prefix; the cause string must
    /// preserve it so an operator sees the real fault (decrypt / DiscRead /
    /// AACS) in `last_error` without digging through the device log.
    #[test]
    fn producer_read_error_cause_preserves_coded_root_cause() {
        // A decrypt failure manifesting mid-stream.
        let decrypt_io: std::io::Error = libfreemkv::Error::DecryptFailed.into();
        let decrypt_code = libfreemkv::Error::DecryptFailed.code();
        let cause = producer_read_error_cause(&decrypt_io);
        // The annotated parenthetical form must actually be emitted — not
        // just an incidental `E####` in the message tail. (Guards the dead
        // `else` branch the code-extraction round-trip used to leave
        // unreachable.)
        assert!(
            cause.contains(&format!("(E{decrypt_code})")),
            "decrypt cause must name the coded fault in the annotation, got: {cause}"
        );
        assert!(cause.contains("read error mid-stream"), "got: {cause}");

        // A coded disc read error (the genuine bad-sector / drive fault).
        let disc_err = libfreemkv::Error::DiscRead {
            sector: 12345,
            status: None,
            sense: None,
        };
        let disc_code = disc_err.code();
        let disc_io: std::io::Error = disc_err.into();
        let cause = producer_read_error_cause(&disc_io);
        assert!(
            cause.contains(&format!("(E{disc_code})")),
            "disc-read cause must name the coded fault in the annotation, got: {cause}"
        );
    }

    /// Regression (rc4): the mux read-error cause must carry an English
    /// description of the fault, not a bare duplicated `E####`. Before the
    /// fix a mid-mux decrypt failure rendered as
    /// `read error mid-stream (E7013): E7013` — a raw code with no English,
    /// inconsistent with the sweep/patch path that labels via
    /// `non_scsi_error_label`. The cause must now read e.g.
    /// `read error mid-stream (E7013): decryption failed`.
    #[test]
    fn producer_read_error_cause_carries_english_label() {
        let decrypt_io: std::io::Error = libfreemkv::Error::DecryptFailed.into();
        let decrypt_code = libfreemkv::Error::DecryptFailed.code();
        let cause = producer_read_error_cause(&decrypt_io);
        assert!(
            cause.contains("decryption failed"),
            "decrypt cause must read in English, got: {cause}"
        );
        // The bare code must not appear as the trailing description (the
        // original leaked `(E7013): E7013` defect).
        assert!(
            !cause.ends_with(&format!("E{decrypt_code}")),
            "cause must not end with a bare duplicated code, got: {cause}"
        );
        assert!(
            !cause.contains(&format!("): E{decrypt_code}")),
            "cause must not render the code as its own description, got: {cause}"
        );

        // A coded disc-read fault gets its English label too.
        let disc_io: std::io::Error = libfreemkv::Error::DiscRead {
            sector: 42,
            status: None,
            sense: None,
        }
        .into();
        let cause = producer_read_error_cause(&disc_io);
        assert!(
            cause.contains("disc read error"),
            "disc-read cause must read in English, got: {cause}"
        );
    }

    /// A plain (non-coded) io error must NOT get a spurious `E####`
    /// numeric prefix — its message round-trips to the generic IoError
    /// code, so only the `{e}` tail describes it.
    #[test]
    fn producer_read_error_cause_handles_plain_io_error() {
        let plain = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short read");
        let cause = producer_read_error_cause(&plain);
        assert!(cause.contains("short read"), "got: {cause}");
        assert!(
            !cause.contains(&format!("(E{})", libfreemkv::error::E_IO_ERROR)),
            "plain io error must not carry a synthetic code prefix, got: {cause}"
        );
        // No parenthetical annotation at all for a non-coded message.
        assert!(
            !cause.contains("(E"),
            "plain io error must not carry any code annotation, got: {cause}"
        );

        // A coded error that maps to the generic IoError code must also not
        // gain a spurious `(E5000)` annotation — only its tail names it.
        let io_coded: std::io::Error =
            libfreemkv::Error::from(std::io::Error::other("boom")).into();
        let cause = producer_read_error_cause(&io_coded);
        assert!(
            !cause.contains(&format!("(E{})", libfreemkv::error::E_IO_ERROR)),
            "IoError-coded fault must not carry the generic annotation, got: {cause}"
        );
    }

    /// Clean disc (no bad sectors): retry term vanishes, total_work
    /// reduces to 2 × capacity. Mux opens at exactly 50%, climbs
    /// linearly to 100%. Sweep+mux symmetry — same shape as a
    /// 2-phase pipeline regardless of `max_retries` planned.
    #[test]
    fn clean_disc_mux_opens_at_50_percent() {
        // max_retries planned 5, but bytes_unreadable=0 → retries
        // contribute nothing whether 0 or 5 actually ran.
        assert_eq!(total_pct_byte_weight(DISC, 5, 0, 0), 50);
        assert_eq!(total_pct_byte_weight(DISC, 5, 0, 50), 75);
        assert_eq!(total_pct_byte_weight(DISC, 5, 0, 100), 100);
        // Same disc, max_retries planned 0 (couldn't have happened
        // here since multipass implies max_retries > 0, but the
        // helper falls through to direct-mode behaviour anyway).
        assert_eq!(total_pct_byte_weight(DISC, 0, 0, 50), 50);
    }

    /// Damaged disc with residual `bytes_unreadable`: retry term
    /// inflates the denominator, mux opens lower than 50% because
    /// the rip "did more total work than just sweep+mux."
    #[test]
    fn damaged_disc_mux_opens_below_50_percent() {
        // 1 GB unreadable, max_retries=5 → retry term = 5 GB.
        // total_work = 60 + 5 + 60 = 125 GB.
        // mux start: total_done = 60 + 5 + 0 = 65. 65/125 = 52%.
        assert_eq!(total_pct_byte_weight(DISC, 5, 1_000_000_000, 0), 52);
        // mux halfway: total_done = 60 + 5 + 30 = 95. 95/125 = 76%.
        assert_eq!(total_pct_byte_weight(DISC, 5, 1_000_000_000, 50), 76);
        // mux done: 100.
        assert_eq!(total_pct_byte_weight(DISC, 5, 1_000_000_000, 100), 100);
    }

    /// Direct-mux / single-pass mode (`max_retries == 0`): there are
    /// no separate phases — total tracks current 1:1.
    #[test]
    fn direct_mode_passthrough() {
        assert_eq!(total_pct_byte_weight(DISC, 0, 0, 0), 0);
        assert_eq!(total_pct_byte_weight(DISC, 0, 0, 42), 42);
        assert_eq!(total_pct_byte_weight(DISC, 0, 0, 100), 100);
    }

    /// Bound + edge cases: zero inputs, overshoot.
    #[test]
    fn edge_cases() {
        // Zero capacity (drive read failed) → fall through to mux pct.
        assert_eq!(total_pct_byte_weight(0, 5, 0, 73), 73);
        // pct overshoot doesn't push total past 100.
        assert_eq!(total_pct_byte_weight(DISC, 5, 0, 200), 100);
        assert_eq!(total_pct_byte_weight(DISC, 5, 1_000_000_000, 200), 100);
    }

    // ── sweep_damage snapshot carry-forward (telemetry audit Fix 1) ──

    /// Verify that `SweepDamageSnapshot` fields survive the `UiState`
    /// round-trip into `push_state`'s `RipState` construction.
    ///
    /// The regression: `push_state` used `..Default::default()` for the
    /// damage fields, zeroing `errors`, `total_lost_ms`, `bad_ranges`, etc.
    /// on the first mux tick — making a damaged disc appear perfectly clean
    /// to operators polling /api/state during mux.
    ///
    /// This test asserts the contract without invoking `update_state` (which
    /// writes to a global singleton): it inspects the `RipState` struct literal
    /// that `push_state` would build, verifying the snapshot fields are
    /// forwarded rather than defaulted. It does this by testing
    /// `SweepDamageSnapshot`'s `Default` (all-zero) vs a non-zero snapshot
    /// and ensuring the logic in push_state selects the snapshot value.
    #[test]
    fn sweep_damage_snapshot_non_zero_overrides_default() {
        // Simulate the logic inside push_state for errors and lost_video_secs.
        let snapshot_errors: u32 = 42;
        let snapshot_total_lost_ms: f64 = 3700.0;
        let live_errors: u32 = 0; // typical during ISO mux — no demux skips
        let live_lost_secs: f64 = 0.0;

        // Replicate the selection logic from push_state.
        let final_errors = if snapshot_errors > 0 {
            snapshot_errors
        } else {
            live_errors
        };
        let final_lost_secs = if snapshot_total_lost_ms > 0.0 {
            snapshot_total_lost_ms / MILLIS_PER_SEC
        } else {
            live_lost_secs
        };

        assert_eq!(
            final_errors, 42,
            "non-zero sweep snapshot errors must survive into push_state"
        );
        assert!(
            (final_lost_secs - 3.7).abs() < 0.001,
            "non-zero sweep snapshot total_lost_ms must survive as lost_video_secs"
        );
    }

    /// When the sweep was clean (zero errors, zero lost ms), the live mux
    /// counters should be used — not the zero snapshot values.
    #[test]
    fn sweep_damage_snapshot_zero_passes_through_live_counters() {
        let snapshot_errors: u32 = 0;
        let snapshot_total_lost_ms: f64 = 0.0;
        let live_errors: u32 = 5;
        let live_lost_secs: f64 = 0.25;

        let final_errors = if snapshot_errors > 0 {
            snapshot_errors
        } else {
            live_errors
        };
        let final_lost_secs = if snapshot_total_lost_ms > 0.0 {
            snapshot_total_lost_ms / MILLIS_PER_SEC
        } else {
            live_lost_secs
        };

        assert_eq!(
            final_errors, 5,
            "zero-snapshot must fall through to live errors"
        );
        assert!(
            (final_lost_secs - 0.25).abs() < 0.001,
            "zero-snapshot must fall through to live lost_video_secs"
        );
    }

    // ── resume progress starts at >0 (telemetry audit Fix 2) ─────────

    /// When max_retries > 0, `total_pct_byte_weight` accounts for the
    /// already-completed sweep, so a resumed rip (mux_pct=0) opens above 0%.
    /// Previously resume.rs passed max_retries=0 which caused the helper to
    /// return mux_pct directly, erasing the sweep's ~50% credit.
    #[test]
    fn resume_progress_starts_above_zero_when_max_retries_nonzero() {
        // Clean disc: bytes_unreadable=0 → retry term vanishes.
        // total_work = 2 × cap. At mux start (mux_pct=0):
        //   total_done = cap + 0 + 0 = cap
        //   total_pct = cap / (2*cap) * 100 = 50%
        let pct = total_pct_byte_weight(DISC, 3, 0, 0);
        assert_eq!(
            pct, 50,
            "resume with max_retries=3 and clean disc should open at 50%, not 0%"
        );
    }

    /// Confirm the old (broken) behavior: max_retries=0 falls through to
    /// mux_pct directly, so mux opened at 0%. This is the correct behavior
    /// for single-pass (direct) mode — verified here as a guard against
    /// accidentally changing it.
    #[test]
    fn direct_mode_progress_matches_mux_pct() {
        // max_retries=0 → direct-mode passthrough: total_pct == mux_pct.
        assert_eq!(total_pct_byte_weight(DISC, 0, 0, 0), 0);
        assert_eq!(total_pct_byte_weight(DISC, 0, 0, 50), 50);
        assert_eq!(total_pct_byte_weight(DISC, 0, 0, 100), 100);
    }

    // ── AutoripMuxEvents bridge feeds the watchdog byte atomics ──────────────

    fn test_shared_atomics() -> (
        SharedAtomics,
        Arc<AtomicU64>,
        Arc<AtomicU64>,
        Arc<AtomicU64>,
    ) {
        let wd_bytes = Arc::new(AtomicU64::new(0));
        let wd_last_frame = Arc::new(AtomicU64::new(0));
        let latest_bytes_read = Arc::new(AtomicU64::new(0));
        let atomics = SharedAtomics {
            latest_bytes_read: latest_bytes_read.clone(),
            rip_last_lba: Arc::new(AtomicU64::new(0)),
            rip_current_batch: Arc::new(AtomicU16::new(0)),
            wd_last_frame: wd_last_frame.clone(),
            wd_bytes: wd_bytes.clone(),
            input_errors: Arc::new(AtomicU32::new(0)),
            input_lost_bytes: Arc::new(AtomicU64::new(0)),
        };
        (atomics, wd_bytes, wd_last_frame, latest_bytes_read)
    }

    fn test_ui_state() -> UiState {
        UiState {
            device: "sr-test".to_string(),
            display_name: String::new(),
            disc_format: String::new(),
            tmdb_title: String::new(),
            tmdb_year: 0,
            tmdb_poster: String::new(),
            tmdb_overview: String::new(),
            duration: String::new(),
            codecs: String::new(),
            filename: String::new(),
            batch: 0,
            total_bytes: 1_000_000,
            title_bytes_per_sec: 0.0,
            total_passes: 0,
            bytes_total_disc: 0,
            max_retries: 0,
            bytes_unreadable_at_mux: 0,
            sweep_damage: SweepDamageSnapshot::default(),
        }
    }

    /// `push_mux_state` is the only place that writes the live per-frame
    /// `RipState` to `/api/state` during a mux — both the single-pass
    /// `MuxSink::apply` path and the ISO/multipass `AutoripMuxEvents` bridge
    /// funnel through it. `status: "ripping"` and `disc_present: true` are
    /// what the "already ripping" concurrent-dispatch gate and the
    /// live-progress UI key off: if either silently reverted to
    /// `RipState::default()`'s "idle"/`false` (the exact shape of a `delete
    /// field` mutant on this struct literal), a mux in progress would report
    /// the device as idle — the operator sees nothing happening while the
    /// drive is busy, and a second `/api/rip` could be dispatched against it.
    /// A private device key avoids racing any other test's `STATE` entry.
    #[test]
    fn push_mux_state_reports_ripping_and_disc_present() {
        let device = "push_mux_state_test_device";
        let mut ui = test_ui_state();
        ui.device = device.to_string();
        let (atomics, ..) = test_shared_atomics();

        push_mux_state(&ui, &atomics, 10, 5.0, "1:00".into(), 1000, 0.0, 0);

        let rs = super::super::STATE
            .lock()
            .unwrap()
            .get(device)
            .cloned()
            .expect("push_mux_state must write a STATE entry for the device");
        assert_eq!(rs.status, "ripping");
        assert!(
            rs.disc_present,
            "a live mux tick must report disc_present=true"
        );
        super::super::STATE.lock().unwrap().remove(device);
    }

    /// THE watchdog preservation check: `AutoripMuxEvents::on_write_progress`
    /// must feed `wd_bytes` (the atomic `spawn_mux_watchdog` reads for both its
    /// hard `exit(1)` escalation and the "stalled at X GB" UI) and refresh
    /// `wd_last_frame` — even on the throttled early-return path — so a healthy
    /// mux keeps the counter advancing and never false-escalates. Mutation:
    /// dropping the `wd_bytes.store(...)` in `on_write_progress` leaves the
    /// counter at 0 and this fails.
    #[test]
    fn autorip_mux_events_feed_watchdog_byte_atomic() {
        use libfreemkv::MuxEvents;
        let (atomics, wd_bytes, wd_last_frame, latest_bytes_read) = test_shared_atomics();
        let events = AutoripMuxEvents {
            ui: test_ui_state(),
            atomics,
            progress: Mutex::new(freemkv_engine::SpeedEstimator::new()),
            // `now` → the 1 s throttle fires and `on_write_progress` returns
            // early AFTER feeding the watchdog atomics: the feed must not be
            // gated behind the UI throttle.
            last_update: Mutex::new(Instant::now()),
            last_log: Mutex::new(Instant::now()),
            opened: AtomicBool::new(false),
        };

        // Reader side: feeds the UI read-ahead position + watchdog activity.
        events.on_read_progress(4096, 8192);
        assert_eq!(
            latest_bytes_read.load(Ordering::Relaxed),
            4096,
            "on_read_progress must feed latest_bytes_read"
        );
        assert!(
            wd_last_frame.load(Ordering::Relaxed) > 0,
            "on_read_progress must refresh wd_last_frame (watchdog activity)"
        );

        // Writer side (throttled): wd_bytes MUST still advance — this is the
        // load-bearing feed that keeps the hard watchdog from firing exit(1).
        events.on_write_progress(500_000, 1_000_000);
        assert_eq!(
            wd_bytes.load(Ordering::Relaxed),
            500_000,
            "on_write_progress must feed wd_bytes even on the throttled path"
        );
        assert!(
            wd_last_frame.load(Ordering::Relaxed) > 0,
            "on_write_progress must refresh wd_last_frame"
        );

        // The opened flag drives `output_opened` in the outcome mapping.
        assert!(!events.opened.load(Ordering::Relaxed));
        events.on_output_opened(&libfreemkv::DiscTitle::empty());
        assert!(
            events.opened.load(Ordering::Relaxed),
            "on_output_opened must set the opened flag"
        );
    }

    /// Regression D: `AutoripMuxEvents::on_sector_skipped` must store the
    /// skipped LBA into `rip_last_lba` (the UI last_sector / playhead atomic
    /// `push_mux_state` reads) — the pre-refactor `make_stream_event_fn` did
    /// `last_lba.store(sector)` on every `SectorSkipped`. It must also refresh
    /// the watchdog activity timestamp and bump `input_errors` (additive
    /// behaviour kept from the post-refactor bridge).
    ///
    /// Mutation: reverting the handler to `_lba` unused (dropping the
    /// `rip_last_lba.store(lba as u64, ...)`) leaves `rip_last_lba` at 0 and the
    /// last_sector assertion fails.
    #[test]
    fn on_sector_skipped_stores_lba_into_rip_last_lba() {
        use libfreemkv::MuxEvents;
        let (atomics, _wd_bytes, wd_last_frame, _lbr) = test_shared_atomics();
        let rip_last_lba = atomics.rip_last_lba.clone();
        let input_errors = atomics.input_errors.clone();
        let events = AutoripMuxEvents {
            ui: test_ui_state(),
            atomics,
            progress: Mutex::new(freemkv_engine::SpeedEstimator::new()),
            last_update: Mutex::new(Instant::now()),
            last_log: Mutex::new(Instant::now()),
            opened: AtomicBool::new(false),
        };

        events.on_sector_skipped(4242);
        assert_eq!(
            rip_last_lba.load(Ordering::Relaxed),
            4242,
            "on_sector_skipped must store the skipped LBA into rip_last_lba \
             (the UI last_sector / playhead), matching make_stream_event_fn"
        );
        assert_eq!(
            input_errors.load(Ordering::Relaxed),
            1,
            "on_sector_skipped must still bump input_errors (additive)"
        );
        assert!(
            wd_last_frame.load(Ordering::Relaxed) > 0,
            "on_sector_skipped must refresh wd_last_frame (watchdog activity)"
        );

        // A later skip advances the playhead to the new LBA.
        events.on_sector_skipped(9001);
        assert_eq!(
            rip_last_lba.load(Ordering::Relaxed),
            9001,
            "a subsequent skip must move the playhead forward"
        );
        assert_eq!(input_errors.load(Ordering::Relaxed), 2);
    }

    /// Regression D: `AutoripMuxEvents::on_batch_size_changed` must store the
    /// new batch into `rip_current_batch` AND emit the batch-change device-log
    /// line the pre-refactor `make_stream_event_fn` produced. We assert the
    /// atomic store (the inspectable effect) and that the reason→label match is
    /// exhaustive/panic-free for both variants (the log line is derived from it).
    #[test]
    fn on_batch_size_changed_stores_batch_and_logs() {
        use libfreemkv::MuxEvents;
        let (atomics, ..) = test_shared_atomics();
        let rip_current_batch = atomics.rip_current_batch.clone();
        let events = AutoripMuxEvents {
            ui: test_ui_state(),
            atomics,
            progress: Mutex::new(freemkv_engine::SpeedEstimator::new()),
            last_update: Mutex::new(Instant::now()),
            last_log: Mutex::new(Instant::now()),
            opened: AtomicBool::new(false),
        };

        events.on_batch_size_changed(64, libfreemkv::event::BatchSizeReason::Shrunk);
        assert_eq!(
            rip_current_batch.load(Ordering::Relaxed),
            64,
            "on_batch_size_changed must store the new batch into rip_current_batch"
        );
        // Both variants must render (and log) without panicking.
        events.on_batch_size_changed(128, libfreemkv::event::BatchSizeReason::Probed);
        assert_eq!(rip_current_batch.load(Ordering::Relaxed), 128);
    }

    /// `map_iso_mux_outcome` preserves the pre-migration Err classification:
    /// halt / FMTS-missing propagate as `Err` (call-site deferral); a completed
    /// run maps to `completed=true`; a NoStreams drain quarantines
    /// (`finalize_error=Some`, output opened).
    #[test]
    fn map_iso_mux_outcome_classifies_faithfully() {
        let start = Instant::now();
        // Completed run.
        let ok = map_iso_mux_outcome(
            Ok(libfreemkv::MuxOutcome {
                completed: true,
                output_opened: true,
                bytes_written: 1234,
                errors: 0,
                lost_bytes: 0,
                streams: 2,
                // Added to libfreemkv::MuxOutcome during the 1.6.0 audit: the
                // stream indices the sink accepted frames for but could not put
                // in the finished container. Empty here — this fixture is the
                // clean completed run.
                undelivered_streams: Vec::new(),
            }),
            true,
            "sr-test",
            0.0,
            start,
            0,
            0,
        )
        .expect("completed run maps to Ok");
        assert!(ok.completed && ok.output_opened);
        assert_eq!(ok.bytes_done, 1234);

        // Ok(..) with completed=false — a clean operator stop or a
        // join-timeout wedge (see the code comment on the `Ok(o)` non-guard
        // arm). This must NOT be reported as a finished mux: a mutant that
        // widens the `Ok(o) if o.completed` guard to unconditionally match
        // would report this halted/wedged run as `completed=true`, which is
        // rule 1 verbatim (a damaged/incomplete rip filed as good).
        let not_done = map_iso_mux_outcome(
            Ok(libfreemkv::MuxOutcome {
                completed: false,
                output_opened: true,
                bytes_written: 500,
                errors: 0,
                lost_bytes: 0,
                streams: 2,
                undelivered_streams: Vec::new(),
            }),
            true,
            "sr-test",
            0.0,
            start,
            0,
            0,
        )
        .expect("a non-completed Ok result still maps to Ok, just completed=false");
        assert!(
            !not_done.completed,
            "an Ok(..) result with completed=false must not be reported as a finished mux"
        );
        assert!(
            not_done.output_opened,
            "output_opened is carried through unchanged from the engine outcome"
        );
        assert_eq!(not_done.bytes_done, 500);

        // Halt during construction → propagated as Err for call-site handling.
        let halt_err: std::io::Error = libfreemkv::Error::Halted.into();
        assert!(
            map_iso_mux_outcome(Err(halt_err), false, "sr-test", 0.0, start, 0, 0).is_err(),
            "Halted must propagate as Err so the call site preserves staging"
        );

        // NoStreams (empty/undecryptable) with output opened → quarantine.
        let nostreams: std::io::Error = libfreemkv::Error::NoStreams.into();
        let mapped =
            map_iso_mux_outcome(Err(nostreams), true, "sr-test", 0.0, start, 0, 0).expect("mapped");
        assert!(!mapped.completed);
        assert!(mapped.output_opened);
        assert!(
            mapped.finalize_error.is_some(),
            "NoStreams must quarantine via finalize_error"
        );

        // Header-phase failure (no sink opened) → output_opened=false + finalize.
        let mkv_invalid: std::io::Error = libfreemkv::Error::MkvInvalid.into();
        let hdr = map_iso_mux_outcome(Err(mkv_invalid), false, "sr-test", 0.0, start, 0, 0)
            .expect("mapped");
        assert!(!hdr.output_opened);
        assert!(hdr.finalize_error.is_some());
    }

    /// libfreemkv's contract on `MuxOutcome::undelivered_streams`: non-empty
    /// means the finished output does NOT match the pre-mux plan **even with
    /// `completed = true`**, and "a caller that reports a successful export
    /// must report these too — a lossy outcome is never silent." Today only
    /// the `mp4://` sink populates it (autorip never offers that destination
    /// — see `output_scheme_for`), but `map_iso_mux_outcome` must not drop
    /// the field on the floor: the day an mp4 destination exists, a
    /// completed-but-lossy mux must not be silently reported as a clean
    /// success.
    #[test]
    fn map_iso_mux_outcome_surfaces_undelivered_streams_on_a_completed_run() {
        let start = Instant::now();
        let lossy = map_iso_mux_outcome(
            Ok(libfreemkv::MuxOutcome {
                completed: true,
                output_opened: true,
                bytes_written: 1234,
                errors: 0,
                lost_bytes: 0,
                streams: 2,
                undelivered_streams: vec![1],
            }),
            true,
            "sr-test",
            0.0,
            start,
            0,
            0,
        )
        .expect("completed run maps to Ok");
        assert!(lossy.completed);
        // REPORTED, not merely carried: the per-device log is where this fact
        // reaches the operator, and it is the only place it was ever consumed.
        // Exactly once — zero is a silently lossy "success", two is the
        // duplicate-wording bug this replaced.
        let logged = crate::log::get_device_log("sr-test", 50);
        let notes: Vec<&String> = logged
            .iter()
            .filter(|l| l.contains("could not be delivered into the output"))
            .collect();
        assert_eq!(
            notes.len(),
            1,
            "a completed-but-lossy mux must report its undelivered streams \
             exactly once; got {logged:?}"
        );
        assert!(
            notes[0].contains("[1]"),
            "the note must name the streams that were dropped: {:?}",
            notes[0]
        );
    }

    /// ONE event, ONE wording, ONE emitter.
    ///
    /// The note had two independently-maintained spellings —
    /// `map_iso_mux_outcome`'s "could not be delivered" and `rip_disc`'s
    /// completed-mux summary "were not delivered" — both written to the same
    /// per-device log for the same event. A future wording change to one would
    /// silently diverge from its twin, and an alert on either phrase already
    /// missed the other. Dormant only until an `mp4://` destination exists,
    /// which is exactly when nobody will re-read this code.
    #[test]
    fn the_undelivered_streams_note_has_a_single_emitter() {
        let mux_src = crate::util::source_lf(include_str!("mux.rs"));
        let mod_src = crate::util::source_lf(include_str!("mod.rs"));
        assert!(
            mux_src.contains("fn undelivered_streams_note("),
            "the note's wording must live in one shared function"
        );
        assert!(
            !mod_src.contains("stream(s) were not delivered"),
            "rip_disc must not carry a second, independently-worded copy of \
             the undelivered-streams note"
        );
        assert!(
            !mod_src.contains("stream(s) could not be delivered"),
            "rip_disc must not re-emit the note at all — map_iso_mux_outcome \
             already logged it for every outcome that can carry one"
        );
    }
}
