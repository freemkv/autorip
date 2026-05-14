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
//! per-frame `update_state` call (the morning's "fresh-rip snap-back"
//! fix is preserved verbatim).
//!
//! See `freemkv-private/memory/0_18_redesign.md` § "Module layout".

use crossbeam_channel::{SendTimeoutError as CbSendTimeoutError, bounded as cb_bounded};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;
use std::time::Instant;

use libfreemkv::pes::PesFrame;
use libfreemkv::pes::Stream as PesStream;
use libfreemkv::{Flow, Pipeline, READ_PIPELINE_DEPTH, Sink, WRITE_PIPELINE_DEPTH};

use super::session::device_halt;
use super::state::{RipState, update_state};

/// Hard watchdog escalation threshold. When the producer's
/// "last frame / drive activity" timestamp hasn't moved in this many
/// seconds, the mux consumer thread is presumed stuck inside an
/// unkillable syscall (a hung NFS write, a wedged decryption thread,
/// a kernel-side ioctl that never returns). At that point graceful
/// teardown is impossible — the only escape is to exit the process and
/// rely on Docker `restart: unless-stopped` to bring autorip back,
/// after which `resume_or_quarantine_staging` decides whether to retry
/// or quarantine the disc via `.failed`.
///
/// 5 minutes was chosen as the smallest value comfortably above
/// libfreemkv's per-read recovery timeout (60s) and above the soft
/// "drive stalled" 30s warning. Anything under 60s would race with
/// normal slow-recovery reads.
pub const HARD_WATCHDOG_STALL_SECS: u64 = 300;

/// True if the device's registered `Halt` token has been cancelled
/// (e.g. by the HTTP `/api/stop/{device}` handler in `web.rs`).
/// Returns `false` when no token is registered — matches the old
/// `stop_requested` semantics so producer-loop checks behave the same.
/// Compute the Total Progress percentage during the mux phase.
///
/// Uses the same byte-weighted formula `state.rs` uses for sweep and
/// patch — so the two phases agree on what "total progress" means and
/// the bar progresses smoothly across the sweep→mux handoff instead
/// of jumping (forward or backward).
///
/// **Total work estimate** (matches `state.rs::total_work_estimated`):
///
///     total_work = bytes_total_disc                 // sweep
///                + max_retries × bytes_unreadable    // retries
///                + bytes_total_disc                  // mux re-reads ISO
///
/// On a clean disc with `bytes_unreadable=0`, the retry term vanishes
/// and total_work = 2 × disc capacity — so mux opens at exactly 50%.
/// On a damaged disc, the retry term inflates the denominator
/// proportionally; the bar tracks the larger total.
///
/// **Total work done** by mux time:
///
///     total_done = bytes_total_disc                 // sweep complete
///                + max_retries × bytes_unreadable    // retries complete
///                + (mux_pct / 100) × bytes_total_disc
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

fn halt_requested(device: &str) -> bool {
    device_halt(device)
        .map(|h| h.is_cancelled())
        .unwrap_or(false)
}

/// Inputs to `run_mux` that come from the orchestrator. Bundled into a
/// struct because the pre-split inline mux block referenced ~25
/// captured locals; passing them as a struct keeps the `run_mux`
/// signature readable and avoids a long positional argument list.
pub(super) struct MuxInputs<'a> {
    pub(super) device: &'a str,
    pub(super) display_name: String,
    pub(super) disc_format: String,
    pub(super) tmdb_title: String,
    pub(super) tmdb_year: u16,
    pub(super) tmdb_poster: String,
    pub(super) tmdb_overview: String,
    pub(super) duration: String,
    pub(super) codecs: String,
    pub(super) filename: String,
    /// Total expected bytes for the mux phase (used for percent + ETA).
    /// Falls back to the input title's `size_bytes` if 0 is passed.
    pub(super) total_bytes: u64,
    /// Per-title bitrate; used to convert skipped sectors → estimated
    /// lost video time for the UI.
    pub(super) title_bytes_per_sec: f64,
    /// `max_retries + 2` in multipass mode, 0 in direct mode. Threaded
    /// through every per-frame `update_state` so the dashboard's
    /// pass/total bars don't snap back to a "fresh rip" view.
    pub(super) total_passes: u8,
    /// Disc capacity in bytes — same value `state.rs` uses to compute
    /// the sweep + mux contributions to the total-progress denominator.
    /// Plumbed from `disc.capacity_bytes` at the orchestrator level.
    pub(super) bytes_total_disc: u64,
    /// User-configured max retry passes (`cfg_read.max_retries`). Used
    /// as the multiplier on `bytes_unreadable` for the retry-phase
    /// contribution to total work, mirroring `state.rs`.
    pub(super) max_retries: u8,
    /// `bytes_unreadable` at mux start — i.e. after every retry pass
    /// has finished. Drives the retry-phase contribution to the
    /// total-progress denominator. Zero on a clean disc (every bad
    /// sector recovered) — in that case the retry phase contributes
    /// nothing and total = sweep+mux only, so mux opens at ~50%.
    pub(super) bytes_unreadable_at_mux: u64,
    /// Pre-resolved mux output URL (e.g. `mkv:///srv/.../foo.mkv`,
    /// `network://host:port`). Resolved by the orchestrator because URL
    /// construction depends on `cfg.network_target` + `output_format`.
    pub(super) dest_url: String,
    /// Kernel-reported preferred batch size; surfaced in `RipState` so
    /// the UI keeps showing it through the mux phase.
    pub(super) batch: u16,
    /// `cfg.on_read_error == "skip"`. When set, `input.skip_errors`
    /// is true so demux failures during mux yield zero-filled frames
    /// instead of aborting.
    pub(super) skip_errors: bool,
    /// Per-disc staging directory (e.g. `/staging/MyDisc/`). Used by
    /// the hard watchdog to bump `.restart_count` before
    /// `std::process::exit(1)` so the post-restart resume logic can
    /// promote the disc to `.failed` once `RESTART_LIMIT` is reached.
    pub(super) staging_disc_dir: PathBuf,
}

/// Outcome of `run_mux`, used by the orchestrator to drive the
/// post-mux history record + final state push. `completed=false`
/// means the loop bailed early — either user halt, write error, or
/// read error. The bytes/elapsed are filled even on early exit so
/// the history record reflects partial progress.
pub(super) struct MuxOutcome {
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
    pub(super) completed: bool,
    pub(super) bytes_done: u64,
    pub(super) elapsed_secs: f64,
    pub(super) speed_mbs: f64,
    /// Demux skip count from the input stream (`DiscStream::errors`).
    /// Multipass callers usually overwrite this with the mapfile's
    /// `bytes_unreadable / 2048` because demux skips during ISO mux
    /// are typically zero — the real bad-sector count lives in the
    /// mapfile sidecar.
    pub(super) errors: u32,
    /// Estimated lost video seconds derived from `errors`. Same
    /// override pattern as `errors` when a mapfile is available.
    pub(super) lost_video_secs: f64,
    /// True iff the output stream was successfully opened (i.e. we got
    /// past header buffering and `libfreemkv::output(...)` returned
    /// Ok). The orchestrator gates history-record writing on this:
    /// stops or open-failures before the output exists leave no
    /// salvageable artefact, so they get an early-return path
    /// (matching pre-split behaviour). Stops or write errors after
    /// the output is open leave a partial MKV in staging and a
    /// "stopped" history record describing it.
    pub(super) output_opened: bool,
    /// Set when `MuxSink::close()` failed to finalise the MKV (most
    /// commonly: the Cues seek-back at EBML close raised an I/O error,
    /// leaving an unseekable / structurally-invalid output). Carries
    /// the formatted error so the orchestrator can put it in the
    /// `.failed` marker reason. `Some(_)` implies `completed == false`.
    ///
    /// Pre-0.20.8 the close error was swallowed (logged only) and
    /// `.done` / `.completed` got written for unseekable MKVs — the
    /// validation audit's #1 "Reasonable tier" item.
    pub(super) finalize_error: Option<String>,
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
    /// inside `apply` to compute `lost_video_secs`. Atomic so we don't
    /// need to put the input stream behind a mutex.
    input_errors: Arc<AtomicU32>,
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

    /// Push the per-frame `update_state` payload. Verbatim of the
    /// pre-split block (the morning fix); any change here risks
    /// regressing the "mux snap-back to fresh-rip" dashboard bug.
    /// `bytes_done` is what the original computed inline as
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
                bytes_done as f64 / 1_073_741_824.0,
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
                progress_gb: bytes_done as f64 / 1_073_741_824.0,
                speed_mbs: speed,
                eta: eta.clone(),
                errors,
                lost_video_secs,
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
                // Total progress: in multipass mode (total_passes >= 2) the
                // prior (total_passes - 1) passes are 100% complete by the
                // time mux runs; the mux contributes its `pct` over a
                // 1/total_passes share. So:
                //
                //     total_pct = ((total_passes - 1) × 100 + pct) / total_passes
                //
                // For total_passes=7 mux at pct=0:   total = 6×100 / 7 = 85%.
                // For total_passes=7 mux at pct=100: total = 700 / 7 = 100%.
                //
                // 0.18.1/0.18.2 set total_progress_pct = pct (same as
                // pass), which made the Total bar visually reset on the
                // sweep→mux handoff. Pass-equal-weight is a coarse
                // approximation (sweep + each retry is treated as one
                // 1/total_passes "step") but it preserves monotonic
                // forward motion across the handoff, which is what the
                // user wants to see. Sweep/patch use a byte-weighted
                // formula in `state.rs` — the two can drift slightly
                // at the transition but neither goes backward.
                //
                // Single-pass / direct-mux mode (total_passes < 2) has
                // no prior passes — total tracks current 1:1.
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
                ..Default::default()
            },
        );
    }
}

impl Sink<libfreemkv::pes::PesFrame> for MuxSink {
    type Output = u64;

    fn apply(&mut self, frame: libfreemkv::pes::PesFrame) -> Result<Flow, libfreemkv::Error> {
        if let Err(e) = self.output.write(&frame) {
            crate::log::device_log(&self.ui.device, &format!("Write error: {e}"));
            // Stop the pipeline cleanly — `close()` still runs and
            // surfaces whatever bytes_written we got to the orchestrator
            // for the partial-progress history record.
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
            let secs = ((self.ui.total_bytes - bytes_done) as f64
                / (1024.0 * 1024.0)
                / speed_for_eta) as u32;
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
            if self.ui.total_bytes > 0 {
                let total_gb = self.ui.total_bytes as f64 / 1_073_741_824.0;
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

        let skip_errors = self.atomics.input_errors.load(Ordering::Relaxed);
        let lost_video_secs = if self.ui.title_bytes_per_sec > 0.0 {
            (skip_errors as f64) * 2048.0 / self.ui.title_bytes_per_sec
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
            crate::log::device_log(&self.ui.device, &format!("Output finish error: {e}"));
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
pub(super) fn run_mux(
    inputs: MuxInputs<'_>,
    mut input: libfreemkv::DiscStream,
    atomics_in: MuxAtomics,
) -> MuxOutcome {
    // ── Headers-ready buffering ───────────────────────────────────
    //
    // Stays single-threaded: until the demuxer has resolved every
    // track's codec_private, the output stream can't be opened. This
    // is producer-only state and pushing buffered frames through a
    // pipeline before headers are ready would buy nothing.
    let mut buffered = Vec::new();
    let mut header_reads = 0u32;
    while !input.headers_ready() {
        if halt_requested(inputs.device) {
            crate::log::device_log(inputs.device, "Stop requested during header read");
            return MuxOutcome {
                completed: false,
                bytes_done: 0,
                elapsed_secs: 0.0,
                speed_mbs: 0.0,
                errors: input.errors as u32,
                lost_video_secs: 0.0,
                output_opened: false,
                finalize_error: None,
            };
        }
        match input.read() {
            Ok(Some(frame)) => {
                header_reads += 1;
                if header_reads <= 3 || header_reads % 100 == 0 {
                    crate::log::device_log(
                        inputs.device,
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
                crate::log::device_log(inputs.device, "EOF during header read");
                break;
            }
            Err(e) => {
                crate::log::device_log(inputs.device, &format!("Header error: {e}"));
                break;
            }
        }
    }
    crate::log::device_log(
        inputs.device,
        &format!("Headers ready, {} frames buffered", buffered.len()),
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
        &format!("Opening output: {}", inputs.dest_url),
    );
    let raw_output = match libfreemkv::output(&inputs.dest_url, &out_title) {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("Open output failed: {e}");
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
                errors: input.errors as u32,
                lost_video_secs: 0.0,
                output_opened: false,
                finalize_error: None,
            };
        }
    };
    let output = libfreemkv::pes::CountingStream::new(raw_output);

    // Sync `input.skip_errors` with the orchestrator's choice. Done
    // here (after headers, before main loop) for parity with the
    // pre-split code.
    if inputs.skip_errors {
        input.skip_errors = true;
    }

    // ── Watchdog thread ──────────────────────────────────────────
    //
    // 15-second poll for read stalls. Logs to the device log and
    // surfaces a "stalled at X GB" UI state via update_state_with so
    // we don't clobber live progress fields. Stops on _wd_guard drop
    // (i.e. when this function returns, normal or panic).
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
        let wd_total = total_bytes;
        let wd_tmdb_year = inputs.tmdb_year;
        let wd_filename = inputs.filename.clone();
        let wd_staging_disc_dir = inputs.staging_disc_dir.clone();
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
                // already failed for 5 minutes by definition.
                if stall_secs >= HARD_WATCHDOG_STALL_SECS {
                    let bytes_good = wbytes.load(Ordering::Relaxed);
                    let msg = format!(
                        "hard watchdog escalating: stalled {}s at {:.2} GB; exiting process for container restart",
                        stall_secs,
                        bytes_good as f64 / 1_073_741_824.0,
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
                    super::state::update_state_with(&wd_device, |s| {
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
    };
    let shared = SharedAtomics {
        latest_bytes_read: atomics_in.latest_bytes_read.clone(),
        rip_last_lba: atomics_in.rip_last_lba.clone(),
        rip_current_batch: atomics_in.rip_current_batch.clone(),
        wd_last_frame: atomics_in.wd_last_frame.clone(),
        wd_bytes: wd_bytes.clone(),
        input_errors: atomics_in.input_errors.clone(),
    };
    let start = Instant::now();
    let device_str_for_sink = inputs.device.to_string();
    let sink = MuxSink::new(output, ui, shared, start);

    let pipe = match Pipeline::spawn_named("freemkv-mux-consumer", WRITE_PIPELINE_DEPTH, sink) {
        Ok(p) => p,
        Err(e) => {
            crate::log::device_log(&device_str_for_sink, &format!("Pipeline spawn failed: {e}"));
            return MuxOutcome {
                completed: false,
                bytes_done: 0,
                elapsed_secs: 0.0,
                speed_mbs: 0.0,
                errors: input.errors as u32,
                lost_video_secs: 0.0,
                // The output IS open at this point — the pre-split
                // behaviour didn't have this branch (no pipeline) so
                // we treat it like a write error: history record
                // gets written, status=stopped.
                output_opened: true,
                finalize_error: None,
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
    // UHD rips (Civil War: 2412s → ~1700s projected).
    let (frame_tx, frame_rx) = cb_bounded::<PesFrame>(READ_PIPELINE_DEPTH);

    let _latest_bytes_read = atomics_in.latest_bytes_read.clone();
    let _rip_last_lba = atomics_in.rip_last_lba.clone();
    let _rip_current_batch = atomics_in.rip_current_batch.clone();
    let _wd_last_frame = atomics_in.wd_last_frame.clone();
    let _wd_bytes = atomics_in.wd_bytes.clone();
    let input_errors_for_thread = atomics_in.input_errors.clone();
    let input_errors_clone = atomics_in.input_errors.clone();
    let halt_token = device_halt(inputs.device).expect("Halt token must exist for mux thread");
    let device_str = inputs.device.to_string();
    let device_str_for_loop = device_str.clone();
    let frame_tx_for_closure = frame_tx.clone();
    let _input_handle = match std::thread::Builder::new()
        .name("freemkv-mux-producer".to_string())
        .spawn(move || {
            // Halt-aware send helper for the ISO reader → pipeline-
            // feeder bridge channel. Uses
            // `crossbeam_channel::Sender::send_timeout` so the producer
            // BLOCKS on consumer drain (kernel-wakeup) rather than
            // polling. The pre-0.21.7 version polled `try_send` on
            // 50 ms slices, which capped producer throughput at
            // ~20 frames/sec ≈ 1 MB/s whenever the consumer back-
            // pressured — see freemkv-private/memory/
            // feedback_send_with_halt_poll_throttle.md.
            //
            // The 250 ms halt-check cadence is just for stop-button
            // responsiveness; on the happy path the producer is woken
            // the instant the consumer drains a slot, so this primitive
            // imposes no throughput cap at any storage / network speed.
            fn send_with_halt_raw(
                tx: &crossbeam_channel::Sender<PesFrame>,
                halt: &libfreemkv::Halt,
                item: PesFrame,
                deadline: std::time::Duration,
            ) -> Result<(), PesFrame> {
                let end = std::time::Instant::now() + deadline;
                let halt_check = std::time::Duration::from_millis(250);
                let mut pending = item;
                loop {
                    if halt.is_cancelled() {
                        return Err(pending);
                    }
                    let now = std::time::Instant::now();
                    if now >= end {
                        return Err(pending);
                    }
                    let slice = halt_check.min(end.saturating_duration_since(now));
                    match tx.send_timeout(pending, slice) {
                        Ok(()) => return Ok(()),
                        Err(CbSendTimeoutError::Timeout(returned)) => {
                            pending = returned;
                            // loop: re-check halt + deadline, then park again
                        }
                        Err(CbSendTimeoutError::Disconnected(returned)) => {
                            return Err(returned);
                        }
                    }
                }
            }
            let producer_deadline = std::time::Duration::from_secs(60);
            let mut local_input = input;
            for frame in buffered {
                if halt_token.is_cancelled() {
                    crate::log::device_log(&device_str, "Producer: Stop during buffered drain");
                    return;
                }
                if send_with_halt_raw(&frame_tx_for_closure, &halt_token, frame, producer_deadline)
                    .is_err()
                {
                    crate::log::device_log(
                        &device_str,
                        "Producer: buffered drain aborted (channel closed or halted)",
                    );
                    return;
                }
                input_errors_for_thread.store(local_input.errors as u32, Ordering::Relaxed);
            }

            loop {
                if halt_token.is_cancelled() {
                    crate::log::device_log(&device_str, "Producer: Stop requested");
                    break;
                }
                match local_input.read() {
                    Ok(Some(frame)) => {
                        input_errors_for_thread.store(local_input.errors as u32, Ordering::Relaxed);
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
                        if send_with_halt_raw(
                            &frame_tx_for_closure,
                            &halt_token,
                            frame,
                            producer_deadline,
                        )
                        .is_err()
                        {
                            crate::log::device_log(
                                &device_str,
                                "Producer: Channel closed or halted (consumer aborted)",
                            );
                            break;
                        }
                    }
                    Ok(None) => {
                        tracing::trace!(target: "stream", "Producer: EOF reached, returning");
                        return;
                    }
                    Err(e) => {
                        crate::log::device_log(&device_str, &format!("Producer read error: {e}"));
                        break;
                    }
                }
            }
        }) {
        Ok(h) => h,
        Err(e) => {
            crate::log::device_log(
                &device_str_for_loop,
                &format!("Failed to spawn ISO reader thread: {e}"),
            );
            return MuxOutcome {
                completed: false,
                bytes_done: 0,
                elapsed_secs: 0.0,
                speed_mbs: 0.0,
                errors: input_errors_clone.load(Ordering::Relaxed),
                lost_video_secs: 0.0,
                output_opened: true,
                finalize_error: None,
            };
        }
    };

    // 0.20.8 validation-audit fix #2: track whether the consumer-bridge
    // loop drained the producer channel to natural EOF. The loop below
    // exits cleanly when `frame_rx` runs dry (producer dropped its
    // `frame_tx` after EOF on the input stream or after a read error
    // it already logged). On either `break` in the loop body (halt or
    // send deadline) we set `loop_drained_naturally = false`, which
    // joins with a clean `pipe.finish_with_halt` to flip `completed`
    // to true. Pre-0.20.8 `completed` was hardcoded `false`, so no
    // rip ever got `.done` / `.completed` written — only the test bed's
    // tolerance for that asymmetry kept it from being noticed earlier.
    let mut loop_drained_naturally = true;
    let mut frame_count = 0u64;
    // Halt-aware send deadline for the consumer-bridge loop. Chosen
    // longer than the mux soft-stall warning (30 s) but well under the
    // hard watchdog (300 s) so a wedged pipeline-consumer surfaces here
    // as a per-frame timeout rather than wedging the whole mux phase.
    // On `Err` we treat it identically to "consumer closed" — log and
    // break out; the hard watchdog handles the broader case.
    const MUX_SEND_DEADLINE_SECS: u64 = 60;
    let mux_halt = device_halt(inputs.device).expect("Halt token must exist for mux send loop");
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
                    "Mux consumer aborted (pipeline closed or halted)",
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
                    &format!(
                        "Mux SEND STALLED {:.1}s (channel full)",
                        elapsed.as_secs_f64()
                    ),
                );
            } else {
                tracing::debug!(
                    "Mux send: OK in {:.3}ms, frame={}",
                    elapsed.as_micros(),
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
    // `bounded_syscall` makes — and the hard watchdog at 5 min
    // typically fires first and exits the process for a Docker restart.
    // 0.20.8 validation-audit fix #1 (close-error propagation) +
    // fix #2 (real completion signal):
    //
    // `pipe.finish_with_halt(...)` can return Err for four reasons:
    //   (a) MuxSink::close()'s `output.finish()` propagated an Err
    //       (NEW in 0.20.8 — pre-audit it was logged and swallowed).
    //       Display string is the libfreemkv `Error` Display.
    //   (b) the consumer thread panicked → "pipeline consumer panicked".
    //   (c) the halt token fired while we waited → "pipeline join halted".
    //   (d) `JOIN_TIMEOUT_SECS` (10 min) elapsed → "pipeline join timed out".
    //
    // Buckets (b)/(c)/(d) are wedge / user-stop cases: existing
    // behaviour treated them as "stopped" (no `.failed` marker), and
    // we preserve that. Bucket (a) is a structurally-invalid MKV and
    // the orchestrator MUST write `.failed` so the disc gets quarantined
    // instead of advancing to `.done` / `.completed`. The three wedge
    // prefixes are matched by literal string — same convention
    // `Pipeline::finish_with_halt`'s doc-comment exposes for callers.
    let (bytes_done, finalize_error, pipe_ok) = match pipe.finish_with_halt(Some(&mux_halt)) {
        Ok(b) => (b, None, true),
        Err(e) => {
            let msg = format!("{e}");
            crate::log::device_log(&device_str_for_sink, &format!("Mux pipeline failed: {msg}"));
            let is_wedge = msg.contains("pipeline consumer panicked")
                || msg.contains("pipeline join halted")
                || msg.contains("pipeline join timed out");
            let finalize = if is_wedge { None } else { Some(msg) };
            (0, finalize, false)
        }
    };
    let elapsed_secs = start.elapsed().as_secs_f64();
    let speed_mbs = if elapsed_secs > 0.0 {
        bytes_done as f64 / (1024.0 * 1024.0) / elapsed_secs
    } else {
        0.0
    };
    let lost_video_secs = if inputs.title_bytes_per_sec > 0.0 {
        (errors as f64) * 2048.0 / inputs.title_bytes_per_sec
    } else {
        0.0
    };

    // `completed` is the orchestrator's gate for writing `.done` /
    // `.completed`. It requires BOTH (a) the consumer-bridge loop
    // drained the producer channel to natural EOF (no break on
    // halt / send deadline) AND (b) the pipeline joined cleanly with
    // no sink error. Either side false → "stopped" / "failed".
    let completed = loop_drained_naturally && pipe_ok && finalize_error.is_none();

    MuxOutcome {
        completed,
        bytes_done,
        elapsed_secs,
        speed_mbs,
        errors,
        lost_video_secs,
        output_opened: true,
        finalize_error,
    }
}

/// The shared atomic counters threaded through `run_mux`. The
/// orchestrator builds these *before* calling `run_mux` because the
/// drive event callback (which writes them) is registered on the
/// session's drive earlier in `rip_disc`. `input.on_event` (also on
/// the producer side) writes them too.
#[derive(Clone)]
pub(super) struct MuxAtomics {
    pub(super) latest_bytes_read: Arc<AtomicU64>,
    pub(super) rip_last_lba: Arc<AtomicU64>,
    pub(super) rip_current_batch: Arc<AtomicU16>,
    pub(super) wd_last_frame: Arc<AtomicU64>,
    pub(super) wd_bytes: Arc<AtomicU64>,
    pub(super) input_errors: Arc<AtomicU32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const DISC: u64 = 60_000_000_000; // 60 GB stand-in for a UHD

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
}
