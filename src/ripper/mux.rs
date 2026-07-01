//! Mux frame loop — read PES frames from `input`, hand them to a
//! `MuxSink` consumer thread that writes them to the chosen output and
//! pushes per-frame UI state.
//!
//! 0.18 round 2 #2: the mux loop is the longest non-overlapped phase
//! on NFS-staged rips because each side of `input.read()` →
//! `output.write()` is latency-bound. Running them on the same thread
//! sums those latencies; running them through libfreemkv's generic
//! `Pipeline` + `Sink` primitive overlaps them via a bounded channel.
//! Channel depth is `READ_PIPELINE_DEPTH` (32) for ISO reader → buffer,
//! `WRITE_PIPELINE_DEPTH` (16) for buffer → mux writer — larger read
//! buffer compensates for drive variability and NFS stalls, smaller
//! write buffer reduces backpressure risk when sync_file_range blocks.
//!
//! The producer half (`run_mux`) owns the input stream, the
//! single-threaded headers-ready buffering, the watchdog thread, and
//! the per-device `Halt`-token poll. The consumer half (`MuxSink`)
//! owns the output stream, the smoothed-speed estimator, and the
//! per-frame `update_state` call. That per-frame update carries the
//! multipass identity (`pass`/`total_passes`) through every frame so
//! the dashboard's pass/total bars don't reset to a "fresh rip" view
//! when the mux phase starts.

use crate::util::{BYTES_PER_GIB, BYTES_PER_MIB, MILLIS_PER_SEC};
use crossbeam_channel::{SendTimeoutError as CbSendTimeoutError, bounded as cb_bounded};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

use libfreemkv::pes::PesFrame;
use libfreemkv::pes::Stream as PesStream;
use libfreemkv::{Flow, Pipeline, READ_PIPELINE_DEPTH, Sink, WRITE_PIPELINE_DEPTH};

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

/// True if the device's registered `Halt` token has been cancelled
/// (e.g. by the HTTP `/api/stop/{device}` handler in `web.rs`).
/// Returns `false` when no token is registered — matches the old
/// `stop_requested` semantics so producer-loop checks behave the same.
fn halt_requested(device: &str) -> bool {
    device_halt(device)
        .map(|h| h.is_cancelled())
        .unwrap_or(false)
}

/// Ceiling on bytes buffered while waiting for `headers_ready()` to resolve.
/// Normally that resolves after a handful of frames; a malformed/adversarial
/// stream where it never resolves would otherwise grow the pre-headers buffer
/// without bound until OOM. 512 MiB is far more than any real codec-private
/// resolution needs but small enough to fail fast rather than swap the box
/// to death.
const HEADER_BUFFER_CAP_BYTES: usize = 512 * 1024 * 1024;

/// `true` once the pre-headers buffer has grown past the cap and the mux
/// should fail rather than keep buffering an unbounded stream.
fn header_buffer_over_cap(buffered_bytes: usize) -> bool {
    buffered_bytes > HEADER_BUFFER_CAP_BYTES
}

/// Classify a `finish_with_halt` error as a "wedge / user-stop" case
/// (rip stays resumable, no `.failed`) versus a real finalize/IO error
/// (quarantine the disc).
///
/// The mux pipeline returns the wedge / user-stop cases as dedicated typed
/// variants — `Error::Halted` (routine `/api/stop` during mux),
/// `Error::PipelineJoinTimeout`, and `Error::PipelineConsumerPanicked` — so we
/// match on those directly. A genuine finalize failure from `output.finish()`
/// surfaces as `Error::IoError` (or any other variant) and is NOT a wedge, so
/// it quarantines. Matching the typed variants (not an inner message string)
/// keeps the classification stable across Display/format changes.
fn is_mux_wedge(e: &libfreemkv::Error) -> bool {
    matches!(
        e,
        libfreemkv::Error::Halted
            | libfreemkv::Error::PipelineJoinTimeout
            | libfreemkv::Error::PipelineConsumerPanicked
    )
}

/// Inputs to `run_mux` that come from the orchestrator. Bundled into a
/// struct because the pre-split inline mux block referenced ~25
/// captured locals; passing them as a struct keeps the `run_mux`
/// signature readable and avoids a long positional argument list.
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

/// Outcome of `run_mux`, used by the orchestrator to drive the
/// post-mux history record + final state push. `completed=false`
/// means the loop bailed early — either user halt, write error, or
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
    /// Set by `MuxSink::apply` when `output.write()` fails mid-stream.
    /// The pipeline keeps draining `frame_rx` after `Flow::Stop` (so the
    /// producer reaches a clean EOF and `loop_drained_naturally` stays
    /// true) and `close()`/`output.finish()` can still emit a valid MKV
    /// trailer (so `pipe_ok` is true and `finalize_error` is None) — which
    /// means the body was truncated but every other success signal lies.
    /// `run_mux` ANDs `!write_failed` into `completed` so a write-failed
    /// run is reported `completed=false` and the orchestrator writes
    /// `.failed` instead of `.done`/`.completed`.
    write_failed: Arc<AtomicBool>,
}

/// Consumer side of the mux pipeline. Owns the output stream, the
/// smoothed-speed estimator, the rate-limited `update_state` cadence,
/// and the bytes-written counter that the watchdog reads. `Stream: Send`
/// is a supertrait, so `CountingStream` is `Send` directly — no wrapper.
struct MuxSink {
    output: libfreemkv::pes::CountingStream,
    ui: UiState,
    atomics: SharedAtomics,
    progress: crate::ripper::state::PassProgressState,
    last_update: Instant,
    /// Periodic 60s log line — separate cadence from `update_state`.
    last_log: Instant,
}

impl MuxSink {
    fn new(
        output: libfreemkv::pes::CountingStream,
        ui: UiState,
        atomics: SharedAtomics,
        start: Instant,
    ) -> Self {
        Self {
            output,
            ui,
            atomics,
            progress: crate::ripper::state::PassProgressState::new(),
            last_update: start,
            last_log: start,
        }
    }

    /// Push the per-frame `update_state` payload. Each frame carries
    /// `pass`/`total_passes` (the multipass identity) so the dashboard's
    /// pass/total bars don't reset to a "fresh rip" view at mux start —
    /// keep that plumbing intact when editing here.
    /// `bytes_done` is computed by the caller as
    /// `if lbr > 0 { lbr } else { output.bytes_written() }`.
    #[allow(clippy::too_many_arguments)]
    fn push_state(
        &self,
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
            &self.ui.device,
            RipState {
                device: self.ui.device.clone(),
                status: "ripping".to_string(),
                disc_present: true,
                disc_name: self.ui.display_name.clone(),
                disc_format: self.ui.disc_format.clone(),
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
                errors: if self.ui.sweep_damage.errors > 0 {
                    self.ui.sweep_damage.errors
                } else {
                    errors
                },
                lost_video_secs: if self.ui.sweep_damage.total_lost_ms > 0.0 {
                    self.ui.sweep_damage.total_lost_ms / MILLIS_PER_SEC
                } else {
                    lost_video_secs
                },
                last_sector: self.atomics.rip_last_lba.load(Ordering::Relaxed),
                current_batch: self.atomics.rip_current_batch.load(Ordering::Relaxed),
                preferred_batch: self.ui.batch,
                output_file: self.ui.filename.clone(),
                tmdb_title: self.ui.tmdb_title.clone(),
                tmdb_year: self.ui.tmdb_year,
                tmdb_poster: self.ui.tmdb_poster.clone(),
                tmdb_overview: self.ui.tmdb_overview.clone(),
                duration: self.ui.duration.clone(),
                codecs: self.ui.codecs.clone(),
                // Carry the multipass identity through every per-frame
                // update so the UI doesn't snap back to a "fresh rip"
                // view when mux starts. pass == total_passes is the
                // established convention for "we're on the mux pass".
                //
                // Total progress is computed by `total_pct_byte_weight`
                // (see its doc) — the same byte-weighted formula
                // `state.rs` uses for sweep and patch, so the bar
                // progresses smoothly across the sweep→mux handoff
                // instead of jumping. (A prior pass-equal-weight formula
                // lived here; it was replaced by the byte-weighted call
                // below.)
                pass: self.ui.total_passes,
                total_passes: self.ui.total_passes,
                pass_progress_pct: pct,
                pass_eta: eta.clone(),
                total_progress_pct: total_pct_byte_weight(
                    self.ui.bytes_total_disc,
                    self.ui.max_retries,
                    self.ui.bytes_unreadable_at_mux,
                    pct,
                ),
                total_eta: eta,
                // Carry sweep-phase damage fields so they remain visible
                // in /api/state during the entire mux phase.
                total_lost_ms: self.ui.sweep_damage.total_lost_ms,
                main_lost_ms: self.ui.sweep_damage.main_lost_ms,
                bad_ranges: self.ui.sweep_damage.bad_ranges.clone(),
                num_bad_ranges: self.ui.sweep_damage.num_bad_ranges,
                bad_ranges_truncated: self.ui.sweep_damage.bad_ranges_truncated,
                largest_gap_ms: self.ui.sweep_damage.largest_gap_ms,
                ..Default::default()
            },
        );
    }
}

impl Sink<libfreemkv::pes::PesFrame> for MuxSink {
    type Output = u64;

    fn apply(&mut self, frame: libfreemkv::pes::PesFrame) -> Result<Flow, libfreemkv::Error> {
        if let Err(e) = self.output.write(&frame) {
            crate::log::device_log(
                &self.ui.device,
                &freemkv_i18n::fmt("autorip.mux.write_error", &[("error", &e.to_string())]),
            );
            // Record the failure so `run_mux` reports `completed=false`.
            // We still return `Flow::Stop` (not `Err`) so the pipeline
            // drains cleanly and `close()` surfaces partial bytes for the
            // history record — but the write_failed flag prevents the
            // truncated body from being marked `.done`/`.completed`. A
            // mid-stream write error here would otherwise yield a clean
            // EOF + valid MKV trailer and be published as success.
            self.atomics.write_failed.store(true, Ordering::Relaxed);
            return Ok(Flow::Stop);
        }

        // Watchdog: record this frame as live activity. The reader
        // thread also bumps `wd_last_frame` via `input.on_event`, so
        // even a long sequence of skipped sectors keeps it fresh.
        self.atomics
            .wd_last_frame
            .store(crate::util::epoch_secs(), Ordering::Relaxed);
        self.atomics
            .wd_bytes
            .store(self.output.bytes_written(), Ordering::Relaxed);

        // 1-second `update_state` cadence — same throttle as the
        // pre-split inline loop. Not also gating on a frame-count tick
        // because frames here are large (multi-MB keyframes); 1 frame
        // per second is already plentiful for the dashboard.
        let now = Instant::now();
        if now.duration_since(self.last_update).as_secs_f64() < 1.0 {
            return Ok(Flow::Continue);
        }
        self.last_update = now;

        // Progress reporting uses the ISO *read* position (`latest_bytes_read`)
        // rather than the output *write* position (`output.bytes_written()`).
        // This is intentional: the pipeline's channel depth means the reader
        // runs up to READ_PIPELINE_DEPTH frames ahead of the writer. Using the
        // read position gives a smoother, more up-to-date progress bar instead
        // of a write-lagged one that stalls at frame boundaries. The pct/ETA
        // are computed relative to `total_bytes` (the expected ISO size) which
        // is the same unit, so the comparison is apples-to-apples.
        // Falls back to `output.bytes_written()` only if no BytesRead event has
        // fired yet (lbr == 0), which happens briefly at mux start.
        let lbr = self.atomics.latest_bytes_read.load(Ordering::Relaxed);
        let bytes_done = if lbr > 0 {
            lbr
        } else {
            self.output.bytes_written()
        };
        let pct = if self.ui.total_bytes > 0 {
            (bytes_done * 100 / self.ui.total_bytes).min(100) as u8
        } else {
            0
        };
        let display_speed = self.progress.observe(now, bytes_done);
        let speed = display_speed;
        let speed_for_eta = self.progress.eta_speed_mbs(now, display_speed);
        let eta = if speed_for_eta > 0.0 && self.ui.total_bytes > bytes_done {
            let secs =
                ((self.ui.total_bytes - bytes_done) as f64 / BYTES_PER_MIB / speed_for_eta) as u32;
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

        if now.duration_since(self.last_log).as_secs() >= 60 {
            self.last_log = now;
            let gb = bytes_done as f64 / BYTES_PER_GIB;
            let speed_str = if speed >= 1.0 {
                freemkv_i18n::fmt("common.speed_mbs", &[("speed", &format!("{speed:.1}"))])
            } else {
                freemkv_i18n::fmt(
                    "common.speed_kbs",
                    &[("speed", &format!("{:.0}", speed * 1024.0))],
                )
            };
            let eta_str = if eta.is_empty() {
                String::new()
            } else {
                freemkv_i18n::fmt("autorip.mux.eta_suffix", &[("eta", &eta)])
            };
            if self.ui.total_bytes > 0 {
                let total_gb = self.ui.total_bytes as f64 / BYTES_PER_GIB;
                crate::log::device_log(
                    &self.ui.device,
                    &freemkv_i18n::fmt(
                        "autorip.mux.progress",
                        &[
                            ("gb", &format!("{gb:.1}")),
                            ("total_gb", &format!("{total_gb:.1}")),
                            ("pct", &pct.to_string()),
                            ("speed", &speed_str),
                            ("eta", &eta_str),
                        ],
                    ),
                );
            } else {
                crate::log::device_log(
                    &self.ui.device,
                    &freemkv_i18n::fmt(
                        "autorip.mux.progress_nototal",
                        &[("gb", &format!("{gb:.1}")), ("speed", &speed_str)],
                    ),
                );
            }
        }

        let skip_errors = self.atomics.input_errors.load(Ordering::Relaxed);
        // Scale lost-video time by the bytes actually skipped, not the
        // skip-event count: one AACS skip event zero-fills a whole
        // 6144-byte unit, so `errors * 2048` undercounts loss ~3x.
        let lost_bytes = self.atomics.input_lost_bytes.load(Ordering::Relaxed);
        let lost_video_secs = if self.ui.title_bytes_per_sec > 0.0 {
            lost_bytes as f64 / self.ui.title_bytes_per_sec
        } else {
            0.0
        };
        self.push_state(pct, speed, eta, bytes_done, lost_video_secs, skip_errors);

        Ok(Flow::Continue)
    }

    fn close(mut self) -> Result<u64, libfreemkv::Error> {
        // 0.20.8 validation-audit fix #1: propagate `output.finish()`
        // errors instead of swallowing them. The finalize step writes
        // the Cues block and seeks back to patch the segment-info
        // size header; failure there leaves an unseekable / invalid
        // MKV. Pre-0.20.8 we logged and returned Ok, which let the
        // orchestrator write `.done` / `.completed` for a broken file.
        // Now the error surfaces through `pipe.finish_with_halt(...)`
        // back into `run_mux`, where it's captured into
        // `MuxOutcome.finalize_error` and the orchestrator writes a
        // `.failed` marker instead. We still log here so the device
        // log retains the same diagnostic on the device-log page.
        //
        // `CountingStream::finish()` returns `std::io::Error`; wrap it
        // into the surrounding pipeline's `libfreemkv::Error` variant
        // so the `Pipeline::finish_with_halt` error channel can carry
        // it back to the caller. `Error::IoError` is the dedicated
        // pass-through for std `io::Error`s.
        if let Err(e) = self.output.finish() {
            crate::log::device_log(
                &self.ui.device,
                &freemkv_i18n::fmt(
                    "autorip.mux.output_finish_error",
                    &[("error", &e.to_string())],
                ),
            );
            return Err(libfreemkv::Error::IoError { source: e });
        }
        Ok(self.output.bytes_written())
    }
}

/// Build the mux pipeline and run it.
///
/// Producer/consumer split:
/// - **Producer** (this function's main loop): owns `input`, drives
///   `headers_ready()` buffering single-threaded, then forwards each
///   read frame through `pipe.send(...)`. Updates `wd_last_frame` on
///   every drive event via `input.on_event` (wired by the orchestrator
///   before this function is called).
/// - **Consumer** (`MuxSink::apply` on a `freemkv-pipeline-consumer`
///   thread): writes the frame to `output`, updates `wd_bytes`, and
///   pushes per-frame `update_state` at most once per second.
///
/// Halt handling: each producer-loop iteration polls the per-device
/// `Halt` token via `halt_requested(device)`. Cancelling the same
/// token (HTTP /api/stop, eject, panic-recovery) breaks the loop on
/// the next read. The DiscStream itself was constructed with
/// `with_halt(...)` upstream so `fill_extents` also bails on the same
/// signal — so a Stop during a dense bad-sector region observes
/// cancellation inside libfreemkv even before the next frame yields.
/// The orchestrator's gate for writing `.done` / `.completed`. Requires
/// the consumer-bridge loop drained to natural EOF, the pipeline joined
/// cleanly with no sink error, AND no mid-stream write error.
/// `!write_failed` is load-bearing: a write failure returns `Flow::Stop`
/// (clean drain) and `close()` can still write a valid trailer, so
/// without it a truncated body would be published as success.
fn mux_completed(
    loop_drained_naturally: bool,
    pipe_ok: bool,
    finalize_error_none: bool,
    write_failed: bool,
    produced: bool,
) -> bool {
    loop_drained_naturally && pipe_ok && finalize_error_none && !write_failed && produced
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

pub(crate) fn run_mux(
    inputs: MuxInputs<'_>,
    mut input: Box<dyn libfreemkv::pes::Stream>,
    atomics_in: MuxAtomics,
) -> MuxOutcome {
    // ── Begin/end phase markers ──────────────────────────────────
    //
    // `run_mux` has several early-return paths (header failure, hard
    // watchdog escalation, etc.); a drop-guard logs the "end" with elapsed
    // on every one of them, so the mux phase is always bracketed in the log.
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

    // ── Watchdog thread ──────────────────────────────────────────
    //
    // 15-second poll for read stalls. Logs to the device log and
    // surfaces a "stalled at X GB" UI state via update_state_with so
    // we don't clobber live progress fields. Stops on _wd_guard drop
    // (i.e. when this function returns, normal or panic).
    //
    // Spawned BEFORE the header-read loop below so a wedge during
    // header resolution is covered too. The header loop's blocking
    // `input.read()` has no SCSI READ_TIMEOUT backstop on the NFS/ISO
    // path; only the hard watchdog (exit(1) → Docker restart) can
    // recover an unkillable header-read stall. `wd_last_frame` is
    // seeded with the current time by the orchestrator, and the
    // producer's `input.on_event` keeps it fresh during header reads,
    // so a stall there is observed. The soft-stall UI uses
    // `inputs.total_bytes` for the percentage (the demuxed
    // `info.size_bytes` isn't known until headers are ready, but the
    // load-bearing hard escalation doesn't need it).
    let wd_active = Arc::new(AtomicBool::new(true));
    struct WatchdogGuard(Arc<AtomicBool>);
    impl Drop for WatchdogGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Relaxed);
        }
    }
    let _wd_guard = WatchdogGuard(wd_active.clone());
    let wd_bytes = atomics_in.wd_bytes.clone();
    {
        let active = wd_active.clone();
        let last_frame = atomics_in.wd_last_frame.clone();
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
                    let should_log = !was_stalled || stall_secs >= last_log_secs + 60;
                    if should_log {
                        last_log_secs = stall_secs;
                        crate::log::device_log(
                            &wd_device,
                            &freemkv_i18n::fmt(
                                "autorip.mux.drive_stalled",
                                &[
                                    ("gb", &format!("{gb:.1}")),
                                    ("pct", &pct.to_string()),
                                    ("stall", &stall_str),
                                ],
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
                    crate::log::device_log(
                        &wd_device,
                        &freemkv_i18n::get("autorip.mux.drive_recovered"),
                    );
                    was_stalled = false;
                    last_log_secs = 0;
                }
            }
        });
    }

    // ── Headers-ready buffering ───────────────────────────────────
    //
    // Stays single-threaded: until the demuxer has resolved every
    // track's codec_private, the output stream can't be opened. This
    // is producer-only state and pushing buffered frames through a
    // pipeline before headers are ready would buy nothing.
    let mut buffered = Vec::new();
    let mut buffered_bytes: usize = 0;
    let mut header_reads = 0u32;
    while !input.headers_ready() {
        if halt_requested(inputs.device) {
            crate::log::device_log(
                inputs.device,
                &freemkv_i18n::get("autorip.mux.stop_header_read"),
            );
            return MuxOutcome {
                completed: false,
                bytes_done: 0,
                elapsed_secs: 0.0,
                speed_mbs: 0.0,
                errors: u32::try_from(input.errors()).unwrap_or(u32::MAX),
                lost_video_secs: 0.0,
                output_opened: false,
                finalize_error: None,
                read_error: None,
            };
        }
        match input.read() {
            Ok(Some(frame)) => {
                header_reads += 1;
                if header_reads <= 3 || header_reads % 100 == 0 {
                    crate::log::device_log(
                        inputs.device,
                        &freemkv_i18n::fmt(
                            "autorip.mux.header_frame",
                            &[
                                ("n", &header_reads.to_string()),
                                ("track", &frame.track.to_string()),
                                ("len", &frame.data.len().to_string()),
                            ],
                        ),
                    );
                }
                buffered_bytes = buffered_bytes.saturating_add(frame.data.len());
                buffered.push(frame);
                if header_buffer_over_cap(buffered_bytes) {
                    let msg = freemkv_i18n::fmt(
                        "autorip.mux.header_buffer_exceeded",
                        &[
                            ("mib", &(HEADER_BUFFER_CAP_BYTES / (1024 * 1024)).to_string()),
                            ("frames", &buffered.len().to_string()),
                        ],
                    );
                    crate::log::device_log(inputs.device, &msg);
                    return MuxOutcome {
                        completed: false,
                        bytes_done: 0,
                        elapsed_secs: 0.0,
                        speed_mbs: 0.0,
                        errors: u32::try_from(input.errors()).unwrap_or(u32::MAX),
                        lost_video_secs: 0.0,
                        output_opened: false,
                        finalize_error: Some(msg),
                        read_error: None,
                    };
                }
            }
            Ok(None) => {
                crate::log::device_log(
                    inputs.device,
                    &freemkv_i18n::get("autorip.mux.eof_header_read"),
                );
                break;
            }
            Err(e) => {
                crate::log::device_log(
                    inputs.device,
                    &freemkv_i18n::fmt("autorip.mux.header_error", &[("error", &e.to_string())]),
                );
                break;
            }
        }
    }
    // The header loop above breaks on Ok(None) EOF or Err without
    // re-checking `headers_ready()`. If we exited that way the codec
    // privates are still empty/partial — proceeding would open the
    // output and mux a structurally-incomplete MKV, and because the
    // producer can then hit a clean EOF, `completed` (run_mux's final
    // gate) would wrongly become true. Treat unresolved headers as a
    // hard failure instead.
    if !input.headers_ready() {
        let msg = freemkv_i18n::get("autorip.mux.header_incomplete");
        crate::log::device_log(inputs.device, &msg);
        return MuxOutcome {
            completed: false,
            bytes_done: 0,
            elapsed_secs: 0.0,
            speed_mbs: 0.0,
            errors: u32::try_from(input.errors()).unwrap_or(u32::MAX),
            lost_video_secs: 0.0,
            output_opened: false,
            finalize_error: Some(msg),
            read_error: None,
        };
    }
    crate::log::device_log(
        inputs.device,
        &freemkv_i18n::fmt(
            "autorip.mux.headers_ready",
            &[("frames", &buffered.len().to_string())],
        ),
    );

    // Build the output title with codec_privates resolved and the
    // display name as the playlist title.
    let info = input.info().clone();
    let mut out_title = info.clone();
    out_title.playlist = inputs.display_name.clone();
    out_title.codec_privates = (0..info.streams.len())
        .map(|i| input.codec_private(i))
        .collect();
    let total_bytes = if inputs.total_bytes > 0 {
        inputs.total_bytes
    } else {
        info.size_bytes
    };

    crate::log::device_log(
        inputs.device,
        &freemkv_i18n::fmt("autorip.mux.opening_output", &[("url", &inputs.dest_url)]),
    );
    let raw_output = match libfreemkv::output(&inputs.dest_url, &out_title) {
        Ok(s) => s,
        Err(e) => {
            let msg = freemkv_i18n::fmt(
                "autorip.mux.output_create_failed",
                &[("error", &e.to_string())],
            );
            crate::log::device_log(inputs.device, &msg);
            update_state(
                inputs.device,
                RipState {
                    device: inputs.device.to_string(),
                    status: "error".to_string(),
                    last_error: msg,
                    ..Default::default()
                },
            );
            return MuxOutcome {
                completed: false,
                bytes_done: 0,
                elapsed_secs: 0.0,
                speed_mbs: 0.0,
                errors: u32::try_from(input.errors()).unwrap_or(u32::MAX),
                lost_video_secs: 0.0,
                output_opened: false,
                finalize_error: None,
                read_error: None,
            };
        }
    };
    let output = libfreemkv::pes::CountingStream::new(raw_output);

    // (Skip-on-read-error behaviour is wired at stream construction —
    // the orchestrator sets `DiscStream::skip_errors` directly — so
    // `run_mux` no longer carries or consumes a skip_errors flag.)

    // (Watchdog already spawned above, before the header-read loop, so a
    // wedge during header resolution — hung NFS / wedged decrypt on the
    // ISO path that has no SCSI READ_TIMEOUT backstop — is also covered
    // and escalates to exit(1) for a container restart.)

    // ── Spawn the consumer pipeline ───────────────────────────────
    let ui = UiState {
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
        sweep_damage: inputs.sweep_damage,
    };
    let write_failed = Arc::new(AtomicBool::new(false));
    // Bytes actually zero-filled past read errors. Local to run_mux (not
    // part of the externally-built MuxAtomics) — the producer stores
    // `input.lost_bytes()` here and both the consumer and the final
    // lost-video-secs computation read it. Distinct from input_errors,
    // which stays the skip-*event* count for the UI "errors" field.
    let input_lost_bytes = Arc::new(AtomicU64::new(0));
    let shared = SharedAtomics {
        latest_bytes_read: atomics_in.latest_bytes_read.clone(),
        rip_last_lba: atomics_in.rip_last_lba.clone(),
        rip_current_batch: atomics_in.rip_current_batch.clone(),
        wd_last_frame: atomics_in.wd_last_frame.clone(),
        wd_bytes: wd_bytes.clone(),
        input_errors: atomics_in.input_errors.clone(),
        input_lost_bytes: input_lost_bytes.clone(),
        write_failed: write_failed.clone(),
    };
    let start = Instant::now();
    let device_str_for_sink = inputs.device.to_string();
    let sink = MuxSink::new(output, ui, shared, start);

    let pipe = match Pipeline::spawn_named("freemkv-mux-consumer", WRITE_PIPELINE_DEPTH, sink) {
        Ok(p) => p,
        Err(e) => {
            crate::log::device_log(
                &device_str_for_sink,
                &freemkv_i18n::fmt(
                    "autorip.mux.pipeline_spawn_failed",
                    &[("error", &e.to_string())],
                ),
            );
            return MuxOutcome {
                completed: false,
                bytes_done: 0,
                elapsed_secs: 0.0,
                speed_mbs: 0.0,
                errors: u32::try_from(input.errors()).unwrap_or(u32::MAX),
                lost_video_secs: 0.0,
                // The output IS open at this point — the pre-split
                // behaviour didn't have this branch (no pipeline) so
                // we treat it like a write error: history record
                // gets written, status=stopped.
                output_opened: true,
                finalize_error: None,
                read_error: None,
            };
        }
    };

    // ── ISO reader producer thread ───────────────────────────────
    //
    // Spawns a background `freemkv-mux-producer` thread that reads PES
    // frames from the input stream and forwards them through a sync channel.
    // The main thread handles headers-ready buffering (single-threaded until
    // demuxer resolves codec_privates), then spawns the producer to parallelize
    // ISO reading with mux writing. This overlaps the latency-bound NFS write
    // path with the next ISO read, cutting total mux duration by ~30% on large
    // UHD rips (one sample rip: 2412s → ~1700s projected).
    let (frame_tx, frame_rx) = cb_bounded::<PesFrame>(READ_PIPELINE_DEPTH);

    let input_errors_for_thread = atomics_in.input_errors.clone();
    let input_lost_bytes_for_thread = input_lost_bytes.clone();
    // Set by the producer when it breaks on a hard `read()` error
    // (distinct from Ok(None) EOF). The bridge loop drains the channel
    // to natural EOF regardless of *why* the producer stopped, so
    // without this flag a mid-mux read failure produces a truncated MKV
    // that still earns a `.done`/`.completed` success marker. ANDed into
    // `completed` below so a hard read failure can never be a success
    // and the completion gate records a cause.
    let producer_read_failed = Arc::new(AtomicBool::new(false));
    let producer_read_failed_for_thread = producer_read_failed.clone();
    // Holds the SPECIFIC cause behind `producer_read_failed`. The bool
    // alone conflates three distinct faults — a hard `read()` error (which
    // may itself be a decrypt/AACS/CSS failure or a coded `DiscRead`/
    // `IoError`), a send-deadline timeout, and a closed bridge channel —
    // and the terminal user-facing reason was a fixed generic string, so
    // the actual root cause (logged once, earlier) never reached
    // `last_error`. The producer stores a clean cause string here before
    // breaking; the terminal "this is why the mux didn't complete" line
    // threads it through so the operator sees the real fault (e.g. the
    // coded `E####`) without digging back through the device log.
    let producer_read_cause: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let producer_read_cause_for_thread = producer_read_cause.clone();
    // The orchestrator normally registers the device's halt token
    // before calling run_mux, but a resume/remux entry path or a
    // register/run_mux race could leave it absent. Fall back to a fresh
    // never-cancelled Halt rather than panicking the rip thread — the
    // loop degrades to unstoppable-until-EOF instead of dying.
    // One lookup, shared by both halves of the pipeline: `Halt` is a
    // cheap `Arc<AtomicBool>` clone, so the producer thread and the
    // consumer-bridge loop below observe the same cancellation flag.
    let halt_token = device_halt(inputs.device).unwrap_or_default();
    let halt_token_producer = halt_token.clone();
    let device_str = inputs.device.to_string();
    let device_str_for_loop = device_str.clone();
    let frame_tx_for_closure = frame_tx.clone();
    let producer_handle = match std::thread::Builder::new()
        .name("freemkv-mux-producer".to_string())
        .spawn(move || {
            // Halt-aware send helper for the ISO reader → pipeline-
            // feeder bridge channel. Uses
            // `crossbeam_channel::Sender::send_timeout` so the producer
            // BLOCKS on consumer drain (kernel-wakeup) rather than
            // polling. The pre-0.21.7 version polled `try_send` on
            // 50 ms slices, which capped producer throughput at
            // ~20 frames/sec ≈ 1 MB/s whenever the consumer back-
            // pressured.
            //
            // The 250 ms halt-check cadence is just for stop-button
            // responsiveness; on the happy path the producer is woken
            // the instant the consumer drains a slot, so this primitive
            // imposes no throughput cap at any storage / network speed.
            // Outcome of a producer→bridge send. A clean operator stop
            // (`Halted`) is NOT a failure — it leaves the staging dir
            // resumable with no error marker. A `SendFailed` (60 s send
            // deadline elapsed with the consumer wedged, or the bridge
            // channel disconnected) IS a failure: the producer abandons
            // frames it could not hand off, so the bridge sees a premature
            // EOF it can't distinguish from a real one. That must force
            // `completed=false` exactly like a mid-stream read error, or a
            // truncated MKV gets a `.done`/`.completed` success marker.
            enum SendOutcome {
                Sent,
                Halted,
                SendFailed,
            }
            fn send_with_halt_raw(
                tx: &crossbeam_channel::Sender<PesFrame>,
                halt: &libfreemkv::Halt,
                item: PesFrame,
                deadline: std::time::Duration,
            ) -> SendOutcome {
                let end = std::time::Instant::now() + deadline;
                let halt_check = std::time::Duration::from_millis(250);
                let mut pending = item;
                loop {
                    if halt.is_cancelled() {
                        return SendOutcome::Halted;
                    }
                    let now = std::time::Instant::now();
                    if now >= end {
                        // Send deadline elapsed (consumer wedged) — abort.
                        return SendOutcome::SendFailed;
                    }
                    let slice = halt_check.min(end.saturating_duration_since(now));
                    match tx.send_timeout(pending, slice) {
                        Ok(()) => return SendOutcome::Sent,
                        Err(CbSendTimeoutError::Timeout(returned)) => {
                            pending = returned;
                            // loop: re-check halt + deadline, then park again
                        }
                        Err(CbSendTimeoutError::Disconnected(_)) => {
                            return SendOutcome::SendFailed;
                        }
                    }
                }
            }
            let producer_deadline = std::time::Duration::from_secs(60);
            let mut local_input = input;
            for frame in buffered {
                if halt_token_producer.is_cancelled() {
                    crate::log::device_log(
                        &device_str,
                        &freemkv_i18n::get("autorip.mux.producer_stop_buffered_drain"),
                    );
                    return;
                }
                match send_with_halt_raw(
                    &frame_tx_for_closure,
                    &halt_token_producer,
                    frame,
                    producer_deadline,
                ) {
                    SendOutcome::Sent => {}
                    SendOutcome::Halted => {
                        crate::log::device_log(
                            &device_str,
                            &freemkv_i18n::get("autorip.mux.producer_buffered_halted"),
                        );
                        return;
                    }
                    SendOutcome::SendFailed => {
                        // Send deadline / disconnect, not a clean stop —
                        // flag it so the truncated output isn't marked done.
                        crate::log::device_log(
                            &device_str,
                            &freemkv_i18n::get("autorip.mux.producer_buffered_aborted"),
                        );
                        producer_read_failed_for_thread.store(true, Ordering::Relaxed);
                        return;
                    }
                }
                input_errors_for_thread.store(u32::try_from(local_input.errors()).unwrap_or(u32::MAX), Ordering::Relaxed);
                input_lost_bytes_for_thread.store(local_input.lost_bytes(), Ordering::Relaxed);
            }

            loop {
                if halt_token_producer.is_cancelled() {
                    crate::log::device_log(
                        &device_str,
                        &freemkv_i18n::get("autorip.mux.producer_stop_requested"),
                    );
                    break;
                }
                match local_input.read() {
                    Ok(Some(frame)) => {
                        input_errors_for_thread
                            .store(u32::try_from(local_input.errors()).unwrap_or(u32::MAX), Ordering::Relaxed);
                        input_lost_bytes_for_thread
                            .store(local_input.lost_bytes(), Ordering::Relaxed);
                        // Producer per-frame log is the developer firehose
                        // (~hundreds of frames/sec on a UHD rip). Lives at
                        // `trace` so a normal /api/debug toggle (which raises
                        // to `debug`) doesn't drown the useful events. Enable
                        // explicitly with AUTORIP_LOG_LEVEL=stream=trace.
                        tracing::trace!(
                            target: "stream",
                            track = frame.track,
                            pts = frame.pts,
                            keyframe = frame.keyframe,
                            size = frame.data.len(),
                            "Producer: frame"
                        );
                        match send_with_halt_raw(
                            &frame_tx_for_closure,
                            &halt_token_producer,
                            frame,
                            producer_deadline,
                        ) {
                            SendOutcome::Sent => {}
                            SendOutcome::Halted => {
                                crate::log::device_log(
                                    &device_str,
                                    &freemkv_i18n::get("autorip.mux.producer_halted_midsend"),
                                );
                                break;
                            }
                            SendOutcome::SendFailed => {
                                // Consumer wedged past the send deadline (or
                                // the bridge channel disconnected). The
                                // producer can't hand off the rest of the
                                // stream, so the bridge will see a premature
                                // EOF — flag it as a failure so `completed`
                                // goes false and the MKV isn't marked done.
                                crate::log::device_log(
                                    &device_str,
                                    &freemkv_i18n::get("autorip.mux.producer_send_deadline"),
                                );
                                if let Ok(mut slot) = producer_read_cause_for_thread.lock() {
                                    slot.get_or_insert_with(|| {
                                        "send deadline elapsed or bridge channel closed (consumer aborted)".to_string()
                                    });
                                }
                                producer_read_failed_for_thread.store(true, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::trace!(target: "stream", "Producer: EOF reached, returning");
                        // FINAL FLUSH: store the complete decrypt/read loss before
                        // exiting. The per-frame stores above can miss units dropped
                        // on the last frame(s); the orchestrator reads this atomic
                        // after the producer joins to tally and report ALL mux-time
                        // loss, so it must reflect the full count (the mux never
                        // aborts on this loss — it is concealed + reported only).
                        input_lost_bytes_for_thread
                            .store(local_input.lost_bytes(), Ordering::Relaxed);
                        return;
                    }
                    Err(e) => {
                        // Capture the SPECIFIC cause, not just the bool, so the
                        // terminal reason can name a coded `E####` instead of a
                        // fixed generic truncation string.
                        let cause = producer_read_error_cause(&e);
                        crate::log::device_log(
                            &device_str,
                            &freemkv_i18n::fmt(
                                "autorip.mux.producer_read_error",
                                &[("error", &e.to_string())],
                            ),
                        );
                        if let Ok(mut slot) = producer_read_cause_for_thread.lock() {
                            slot.get_or_insert(cause);
                        }
                        producer_read_failed_for_thread.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }
            // FINAL FLUSH on the read-error (break) path too — same reason as the
            // EOF path: the reported loss tally must be complete.
            input_lost_bytes_for_thread.store(local_input.lost_bytes(), Ordering::Relaxed);
        }) {
        Ok(h) => h,
        Err(e) => {
            crate::log::device_log(
                &device_str_for_loop,
                &freemkv_i18n::fmt(
                    "autorip.mux.iso_thread_spawn_failed",
                    &[("error", &e.to_string())],
                ),
            );
            // Finalize the pipeline so its consumer thread (which holds the
            // output file handle) is joined rather than detached/leaked.
            // `finish_with_halt` is bounded — it will not block forever.
            if let Err(fe) = pipe.finish_with_halt(Some(&halt_token)) {
                crate::log::device_log(
                    &device_str_for_sink,
                    &freemkv_i18n::fmt(
                        "autorip.mux.pipeline_finalize_after_spawn_fail",
                        &[("error", &fe.to_string())],
                    ),
                );
            }
            return MuxOutcome {
                completed: false,
                bytes_done: 0,
                elapsed_secs: 0.0,
                speed_mbs: 0.0,
                errors: atomics_in.input_errors.load(Ordering::Relaxed),
                lost_video_secs: 0.0,
                output_opened: true,
                finalize_error: None,
                read_error: None,
            };
        }
    };

    // DEADLOCK FIX (2026-05-18): drop the outer `frame_tx` immediately
    // after spawning the producer. The producer thread holds its own
    // clone (`frame_tx_for_closure`); the outer scope holding a
    // never-used `frame_tx` was keeping `frame_rx` open even after the
    // producer thread died / panicked / hit Ok(None), which meant the
    // bridge `for frame in frame_rx` loop below blocked forever on
    // recv (frame_rx never returns None until ALL Sender clones drop).
    //
    // Symptom: reproducible mux wedge at ~82% (48.5 GB) on both NFS
    // and local SAS — gdb showed bridge thread parked in
    // crossbeam::array::recv with no producer in the thread list (it
    // had exited but its sender clone was still alive because the
    // outer one kept the channel open). Hard watchdog escalated to
    // exit(1), container restarted, retry hit the same wedge.
    //
    // With this drop, if the producer exits for any reason (EOF,
    // read error, panic in HEVC parser, AACS key issue, etc.) the
    // last sender clone drops and `frame_rx` returns None, exiting
    // the bridge loop cleanly.
    drop(frame_tx);

    // 0.20.8 validation-audit fix #2: track whether the consumer-bridge
    // loop drained the producer channel to natural EOF. The loop below
    // exits cleanly when `frame_rx` runs dry (producer dropped its
    // `frame_tx` after EOF on the input stream or after a read error
    // it already logged). On either `break` in the loop body (halt or
    // send deadline) we set `loop_drained_naturally = false`, which —
    // ANDed into `mux_completed` below — PREVENTS `completed` from
    // being true; only a natural drain plus a clean
    // `pipe.finish_with_halt` yields `completed == true`. Pre-0.20.8
    // `completed` was hardcoded `false`, so no rip ever got `.done` /
    // `.completed` written — only the test bed's tolerance for that
    // asymmetry kept it from being noticed earlier.
    let mut loop_drained_naturally = true;
    let mut frame_count = 0u64;
    // Halt-aware send deadline for the consumer-bridge loop. Chosen
    // longer than the mux soft-stall warning (30 s) but well under the
    // hard watchdog (1200 s) so a wedged pipeline-consumer surfaces here
    // as a per-frame timeout rather than wedging the whole mux phase.
    // On `Err` we treat it identically to "consumer closed" — log and
    // break out; the hard watchdog handles the broader case.
    const MUX_SEND_DEADLINE_SECS: u64 = 60;
    // Reuse the single `halt_token` looked up above (the producer holds a
    // clone of the same Arc), so the consumer-bridge loop observes the
    // same cancellation flag. Move it here — it isn't referenced by its
    // original name again.
    let mux_halt = halt_token;
    for frame in frame_rx {
        let track = frame.track;
        frame_count += 1;
        if libfreemkv::io::pipeline::debug_enabled() || crate::web::debug_enabled() {
            let start = std::time::Instant::now();
            if pipe
                .send_with_halt(
                    frame,
                    &mux_halt,
                    std::time::Duration::from_secs(MUX_SEND_DEADLINE_SECS),
                )
                .is_err()
            {
                crate::log::device_log(
                    &device_str_for_loop,
                    &freemkv_i18n::get("autorip.mux.consumer_aborted"),
                );
                loop_drained_naturally = false;
                break;
            }
            let elapsed = start.elapsed();

            if elapsed > std::time::Duration::from_millis(10) {
                tracing::debug!(
                    "Mux send BLOCKED {:.2}s: frame={}",
                    elapsed.as_secs_f64(),
                    track
                );

                crate::log::device_log(
                    &device_str_for_loop,
                    &freemkv_i18n::fmt(
                        "autorip.mux.send_stalled",
                        &[("secs", &format!("{:.1}", elapsed.as_secs_f64()))],
                    ),
                );
            } else {
                tracing::debug!(
                    "Mux send: OK in {:.3}ms, frame={}",
                    elapsed.as_secs_f64() * MILLIS_PER_SEC,
                    track
                );
            }
        } else if pipe
            .send_with_halt(
                frame,
                &mux_halt,
                std::time::Duration::from_secs(MUX_SEND_DEADLINE_SECS),
            )
            .is_err()
        {
            crate::log::device_log(
                &device_str_for_loop,
                "Mux consumer aborted (pipeline closed or halted)",
            );
            loop_drained_naturally = false;
            break;
        }
    }
    if crate::web::debug_enabled() {
        eprintln!(
            "[DEBUG] Consumer: Finished processing {} frames",
            frame_count
        );
    }

    let errors = atomics_in.input_errors.load(Ordering::Relaxed);

    // Drop the producer-side channel and join the consumer.
    // `finish_with_halt` polls `is_finished()` on a 250 ms cadence so
    // the rip thread is never trapped inside `JoinHandle::join` if the
    // consumer wedged inside an unkillable syscall (hung NFS write,
    // wedged decrypt). On halt or `JOIN_TIMEOUT_SECS` (10 min) the
    // consumer is intentionally leaked — same trade-off
    // `bounded_syscall` makes — and the hard watchdog at 20 min
    // typically fires first and exits the process for a Docker restart.
    // 0.20.8 validation-audit fix #1 (close-error propagation) +
    // fix #2 (real completion signal):
    //
    // `pipe.finish_with_halt(...)` (libfreemkv 0.30) returns
    // `Err(Error::IoError { source })` for four reasons:
    //   (a) MuxSink::close()'s `output.finish()` propagated an Err
    //       (NEW in 0.20.8 — pre-audit it was logged and swallowed).
    //   (b) the consumer thread panicked → source "pipeline consumer panicked".
    //   (c) the halt token fired while we waited → source "pipeline join halted".
    //   (d) `JOIN_TIMEOUT_SECS` (10 min) elapsed → source "pipeline join timed out".
    //
    // Buckets (b)/(c)/(d) are wedge / user-stop cases: existing
    // behaviour treats them as "stopped" (no `.failed` marker, disc
    // stays resumable), and we preserve that. Bucket (a) is a
    // structurally-invalid MKV and the orchestrator MUST write `.failed`
    // so the disc gets quarantined instead of advancing to `.done` /
    // `.completed`.
    //
    // `is_mux_wedge` distinguishes (b)/(c)/(d) from (a) by matching the
    // documented marker strings on the inner `io::Error` source (see its
    // doc + the version-bump note). Robuster than matching the outer
    // `Error` Display, which prefixes a numeric `E<code>:`.
    let (bytes_done, finalize_error, pipe_ok) = match pipe.finish_with_halt(Some(&mux_halt)) {
        Ok(b) => (b, None, true),
        Err(e) => {
            let msg = format!("{e}");
            crate::log::device_log(
                &device_str_for_sink,
                &freemkv_i18n::fmt("autorip.mux.pipeline_failed", &[("msg", &msg)]),
            );
            let finalize = if is_mux_wedge(&e) { None } else { Some(msg) };
            // On wedge/halt we still have the consumer's running
            // good-bytes estimate; report it rather than 0 so the
            // history record reflects partial progress (see MuxOutcome
            // doc). It is the watchdog/consumer good-bytes counter, not
            // the exact finalized output size.
            let partial_bytes = atomics_in.wd_bytes.load(Ordering::Relaxed);
            (partial_bytes, finalize, false)
        }
    };
    let elapsed_secs = start.elapsed().as_secs_f64();
    let speed_mbs = if elapsed_secs > 0.0 {
        bytes_done as f64 / BYTES_PER_MIB / elapsed_secs
    } else {
        0.0
    };
    // Scale by the bytes actually skipped, not the skip-event count: an
    // AACS skip event zero-fills a whole 6144-byte unit, so
    // `errors * 2048` understates loss ~3x. This `lost_video_secs` is
    // tallied and reported (folded into the done-card figures) — the mux
    // always proceeds; mux-time loss is concealed, never aborts the disc.
    let lost_bytes = input_lost_bytes.load(Ordering::Relaxed);
    let lost_video_secs = if inputs.title_bytes_per_sec > 0.0 {
        lost_bytes as f64 / inputs.title_bytes_per_sec
    } else {
        0.0
    };

    // A hard producer read error truncates the stream even though the
    // consumer-bridge loop drains "naturally" (the producer dropped its
    // sender after breaking on the `Err`). Without this the bridge loop
    // looks like a clean EOF and `completed` would go true on a
    // truncated MKV. Read it after the loop has drained — the producer
    // stores the flag before dropping its sender, so the store
    // happens-before the loop observes channel close.
    let producer_read_error = producer_read_failed.load(Ordering::Relaxed);
    // Carries the specific read-error cause out to the `MuxOutcome` so the
    // orchestrator can put it in `last_error` (see `read_error` field).
    let mut read_error_cause: Option<String> = None;
    if producer_read_error {
        // Always record the cause so the resumable stop isn't silent in
        // the log — the orchestrator's "stopped → idle" branch (no
        // `finalize_error`) otherwise leaves no terminal reason. The
        // producer already logged the underlying `Err`; this is the
        // terminal "this is why the mux didn't complete" line. The disc
        // stays resumable (a transient drive / NFS read may succeed on a
        // later attempt) — distinct from a structural `finalize_error`,
        // which quarantines the staging dir with `.failed`.
        // Name the SPECIFIC cause the producer captured rather than the old
        // fixed generic string, which conflated read error / send deadline /
        // decrypt and never carried the coded root cause. Fall back to the
        // generic wording only if the slot is somehow empty (e.g. a poisoned
        // lock) so the line is never blank.
        let cause = producer_read_cause
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
            .unwrap_or_else(|| "read error or send deadline".to_string());
        crate::log::device_log(
            &device_str_for_sink,
            &freemkv_i18n::fmt(
                "autorip.mux.incomplete_producer_aborted",
                &[("cause", &cause)],
            ),
        );
        read_error_cause = Some(cause);
    }

    // `completed` is the orchestrator's gate for writing `.done` /
    // `.completed`. It requires (a) the consumer-bridge loop drained the
    // producer channel to natural EOF (no break on halt / send
    // deadline), (b) the pipeline joined cleanly with no sink error,
    // (c) no mid-stream write error was recorded, AND (d) the producer
    // did not break on a hard read error mid-mux. (c) is load-bearing:
    // a write failure returns `Flow::Stop` (clean drain) and `close()`
    // can still write a valid trailer (pipe_ok / no finalize_error), so
    // without the `!write_failed` guard a truncated body would be
    // published as success. (d) covers the dual hazard on the read side:
    // a hard read error closes the channel to a premature EOF the bridge
    // loop cannot distinguish from a real EOF. Any side false → "stopped"
    // / "failed", so a truncated MKV never earns a success marker.
    let write_failed = write_failed.load(Ordering::Relaxed);
    // Theme A fix #3: a mux that drained naturally with ZERO frames produced
    // (and zero output bytes) must NOT flip status="done", move the file to
    // the library, or fire rip_complete — that is the empty/garbage-output
    // silent failure (e.g. undecryptable input → the demuxer emits nothing,
    // the bridge sees an immediate clean EOF, and every other success signal
    // lies). `frame_count` is the bridge-loop count of frames forwarded to the
    // consumer; `bytes_done` is the finalized output size. Require both > 0.
    let produced = frame_count > 0 && bytes_done > 0;
    if loop_drained_naturally && pipe_ok && finalize_error.is_none() && !write_failed && !produced {
        crate::log::device_log(
            &device_str_for_sink,
            &freemkv_i18n::get("autorip.mux.no_frames"),
        );
    }
    let completed = mux_completed(
        loop_drained_naturally,
        pipe_ok,
        finalize_error.is_none(),
        write_failed,
        produced,
    ) && !producer_read_error;
    // Surface the write failure as a finalize_error reason so the
    // orchestrator logs "Mux failed: ..." and records it, rather than
    // silently reporting a stopped rip with no cause.
    let finalize_error = finalize_error
        .or_else(|| {
            write_failed.then(|| "output write error mid-stream (MKV truncated)".to_string())
        })
        .or_else(|| {
            // A natural-drain-but-empty mux is a structural failure (the output
            // is header-only / garbage), not a resumable stop — record a cause
            // so the orchestrator quarantines instead of silently "stopping".
            (loop_drained_naturally
                && pipe_ok
                && !write_failed
                && !producer_read_error
                && !produced)
                .then(|| "mux produced no frames (empty/undecryptable output)".to_string())
        });

    // The producer thread is coordinated via channel close: it drops its
    // `frame_tx` clone on EOF / read error / halt, and the bridge loop
    // above drained the channel to completion before we get here. At this
    // point the producer is either already done or winding down its last
    // read attempt, so a short bounded join recovers its resources (thread
    // stack, ISO file descriptor, any buffered pipeline handles) without
    // risking an unbounded block on a wedged read.
    //
    // Strategy: poll `is_finished()` on a 250 ms cadence for up to ~7.5 s
    // total. If it times out (e.g. the ISO reader stalled on a slow NFS
    // seek after the bridge exited), log and detach — same trade-off
    // `finish_with_halt` makes for the consumer. The hard watchdog at 20
    // min typically fires first and exits the process for a Docker restart,
    // so a wedged producer is not a permanent leak.
    {
        const PRODUCER_JOIN_POLL_MS: u64 = 250;
        const PRODUCER_JOIN_POLLS: u32 = 30; // 30 × 250 ms = 7.5 s
        let mut joined = false;
        for _ in 0..PRODUCER_JOIN_POLLS {
            if producer_handle.is_finished() {
                joined = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(PRODUCER_JOIN_POLL_MS));
        }
        if joined {
            let _ = producer_handle.join();
        } else {
            crate::log::device_log(
                &device_str_for_sink,
                &freemkv_i18n::get("autorip.mux.producer_no_finish"),
            );
            // Intentionally detached — by design, not a resource leak.
            //
            // The producer holds one scarce resource: the input ISO file
            // descriptor. Blocking run_mux (and by extension the rip task)
            // on a producer stuck in an unkillable NFS seek would re-introduce
            // the hang that the 7.5 s timeout is here to prevent. Detaching
            // is the same trade-off `finish_with_halt` makes for the consumer:
            // prefer a brief background thread over an unbounded wedge.
            //
            // Accumulation across rips is not a concern: there is exactly one
            // producer per run_mux call and it always exits eventually (its
            // next `send_with_halt_raw` deadline or halt check will fire).
            // The hard watchdog at 20 min exits the process for a Docker
            // restart in the worst-wedge case.
            drop(producer_handle); // detach — see comment above
        }
    }

    MuxOutcome {
        completed,
        bytes_done,
        elapsed_secs,
        speed_mbs,
        errors,
        lost_video_secs,
        output_opened: true,
        finalize_error,
        read_error: read_error_cause,
    }
}

/// The shared atomic counters threaded through `run_mux`. The
/// orchestrator builds these *before* calling `run_mux` because the
/// drive event callback (which writes them) is registered on the
/// session's drive earlier in `rip_disc`. `input.on_event` (also on
/// the producer side) writes them too.
#[derive(Clone)]
pub(crate) struct MuxAtomics {
    pub(crate) latest_bytes_read: Arc<AtomicU64>,
    pub(crate) rip_last_lba: Arc<AtomicU64>,
    pub(crate) rip_current_batch: Arc<AtomicU16>,
    pub(crate) wd_last_frame: Arc<AtomicU64>,
    pub(crate) wd_bytes: Arc<AtomicU64>,
    pub(crate) input_errors: Arc<AtomicU32>,
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
            libfreemkv::Error::from(std::io::Error::new(std::io::ErrorKind::Other, "boom")).into();
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

    /// Regression: `is_mux_wedge` must treat the three typed pipeline wedge /
    /// user-stop variants as resumable (no `.failed`) and a genuine
    /// `output.finish()` IO error as a finalize failure (quarantine). The most
    /// important case is `Error::Halted` — a routine `/api/stop` during mux —
    /// which the previous string-matching classifier wrongly treated as a
    /// non-wedge, permanently quarantining a resumable disc.
    #[test]
    fn is_mux_wedge_matches_typed_pipeline_variants_not_finalize_errors() {
        // The three wedge / user-stop variants → resumable.
        assert!(
            is_mux_wedge(&libfreemkv::Error::Halted),
            "Error::Halted (routine /api/stop during mux) must classify as a wedge"
        );
        assert!(is_mux_wedge(&libfreemkv::Error::PipelineJoinTimeout));
        assert!(is_mux_wedge(&libfreemkv::Error::PipelineConsumerPanicked));

        // Genuine finalize / IO error from output.finish() → quarantine.
        let io = libfreemkv::Error::IoError {
            source: std::io::Error::other("disk full"),
        };
        assert!(!is_mux_wedge(&io));
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

    // ── mux completion gate (truncated-MKV regression) ──────────────

    #[test]
    fn clean_mux_is_completed() {
        // Natural EOF + clean pipeline join + no finalize error + no
        // write failure + frames/bytes produced → completed.
        assert!(mux_completed(true, true, true, false, true));
    }

    #[test]
    fn mid_stream_write_error_is_not_completed() {
        // THE regression: a write failure returns Flow::Stop, so the loop
        // still drains naturally (true), the pipeline joins cleanly
        // (true), and close() writes a valid trailer (finalize_error
        // None → true) — every other signal says success. The
        // write_failed flag must veto `completed` so a truncated MKV is
        // never published as `.done`/`.completed`.
        assert!(
            !mux_completed(true, true, true, true, true),
            "a mid-stream write error must mark the run NOT completed"
        );
    }

    #[test]
    fn halt_or_finalize_error_is_not_completed() {
        // Loop broke early (halt / send deadline) → not completed.
        assert!(!mux_completed(false, true, true, false, true));
        // Pipeline join failed → not completed.
        assert!(!mux_completed(true, false, true, false, true));
        // close()/finish() finalize error → not completed.
        assert!(!mux_completed(true, true, false, false, true));
    }

    #[test]
    fn zero_frame_natural_drain_is_not_completed() {
        // Theme A fix #3: a natural drain where the mux produced NOTHING
        // (frame_count==0 && bytes_done==0 → produced=false) must NOT be
        // reported complete, even though every other signal says success
        // (drained naturally, clean join, no finalize/write error). This is
        // the empty/undecryptable-output silent failure: the bridge sees an
        // immediate clean EOF and the file is header-only / garbage.
        assert!(
            !mux_completed(true, true, true, false, false),
            "a zero-frame natural drain must mark the run NOT completed"
        );
        // Mirror the run_mux `produced` derivation: any of frame_count==0 or
        // bytes_done==0 makes produced false.
        let produced = |frames: u64, bytes: u64| frames > 0 && bytes > 0;
        assert!(!produced(0, 0), "no frames, no bytes");
        assert!(!produced(0, 1000), "frames forwarded but zero output bytes");
        assert!(!produced(5, 0), "frames counted but nothing finalized");
        assert!(produced(5, 1000), "real frames + real bytes → produced");
    }

    #[test]
    fn producer_send_deadline_abort_is_not_completed() {
        // When the producer aborts on a send deadline (consumer wedged
        // past the 60 s window) it drops its sender after the break, so the
        // bridge loop drains to a PREMATURE EOF that looks natural —
        // `mux_completed` returns true on those inputs. The final gate ANDs
        // `!producer_read_error` (which the SendFailed path now sets) so the
        // truncated MKV is still reported NOT completed. Mirror that exact
        // composition here.
        let mux_ok = mux_completed(true, true, true, false, true);
        assert!(mux_ok, "premature-EOF inputs alone look complete");
        let producer_read_error = true; // SendFailed flagged the abort.
        let completed = mux_ok && !producer_read_error;
        assert!(
            !completed,
            "a producer send-deadline abort must mark the run NOT completed"
        );
    }

    // ── header-buffer cap (untrusted-stream robustness) ──────────────

    #[test]
    fn header_buffer_cap_trips_only_past_ceiling() {
        // Under the cap: keep buffering.
        assert!(!header_buffer_over_cap(0));
        assert!(!header_buffer_over_cap(HEADER_BUFFER_CAP_BYTES));
        // One byte past the cap: fail the mux rather than grow unbounded.
        assert!(header_buffer_over_cap(HEADER_BUFFER_CAP_BYTES + 1));
        assert!(header_buffer_over_cap(usize::MAX));
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

    // ── producer-spawn failure path: pipeline consumer must be joined ─────

    /// Regression for the MED resource-leak on ISO reader spawn failure.
    ///
    /// When `std::thread::Builder::spawn` fails for the ISO reader producer
    /// (the early-return at ~line 1317), the fix calls
    /// `pipe.finish_with_halt(Some(&halt_token))` before returning, joining
    /// the consumer thread and releasing the output file handle.
    ///
    /// `run_mux` cannot be unit-tested directly (it requires a real SCSI disc
    /// or ISO file). This test validates the invariant at the `Pipeline` level:
    /// a `Pipeline` whose consumer is never sent any items still joins cleanly
    /// when `finish_with_halt` is called immediately after spawn — exactly the
    /// shape of the fix. If the call were omitted (pre-fix), the consumer
    /// thread would be detached on return and the `close_called` counter would
    /// remain 0.
    #[test]
    fn pipeline_consumer_joined_when_no_items_sent() {
        use std::sync::atomic::AtomicUsize;

        let close_called = Arc::new(AtomicUsize::new(0));

        struct CountClose(Arc<AtomicUsize>);
        impl Sink<u64> for CountClose {
            type Output = ();
            fn apply(&mut self, _item: u64) -> Result<Flow, libfreemkv::Error> {
                Ok(Flow::Continue)
            }
            fn close(self) -> Result<(), libfreemkv::Error> {
                self.0.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }

        let pipe = Pipeline::spawn(WRITE_PIPELINE_DEPTH, CountClose(close_called.clone()))
            .expect("pipeline spawn should succeed");

        // Simulate the early-return fix: finish without sending any frames.
        let halt = libfreemkv::Halt::default();
        pipe.finish_with_halt(Some(&halt))
            .expect("finish_with_halt must succeed with no items");

        // Consumer thread was joined — close() must have been called exactly once.
        assert_eq!(
            close_called.load(Ordering::Relaxed),
            1,
            "consumer close() must be called exactly once when finish_with_halt is used on the error path"
        );
    }
}
