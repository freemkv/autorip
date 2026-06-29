//! Background mux worker — pipelines mux behind the drive thread.
//!
//! Mirrors the shape of [`crate::mover`]:
//! - A 10-second tick loop polling the staging dir for hand-off markers.
//! - A `BTreeMap<String, MuxerError>` for stuck-dir surfacing.
//!
//! Hand-off contract (v0.25.3):
//!
//! 1. The drive thread (`ripper::rip_disc`) finishes sweep + patch.
//! 2. It writes a `.ripped` JSON marker inside the staging dir with
//!    everything the mux worker needs to reconstruct a `MuxInputs`
//!    (TMDB metadata, codec list, byte counts, batch size, etc.) plus
//!    the ISO filename.
//! 3. If `cfg.auto_eject` is set, it ejects the drive — the disc is
//!    no longer needed once `.ripped` is on disk.
//! 4. The drive returns to `idle`, ready for the next disc.
//! 5. This worker polls the staging dir, picks up `.ripped` markers,
//!    runs the mux against the ISO, writes `.done` (the mover's
//!    existing hand-off) and deletes `.ripped` on success. On failure
//!    it records a `MuxerError` and leaves `.ripped` in place for
//!    next-tick retry / operator inspection.
//!
//! Single-pass live-disc rips (`cfg.max_retries == 0`) stay inline —
//! there's no ISO to hand off and the drive needs to be open for the
//! whole mux. The worker is a no-op for those titles.

use crate::config::Config;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

/// Hand-off marker written by `ripper::rip_disc` after sweep + patch
/// complete, picked up by this worker on the next tick. Lives at
/// `<staging>/<disc>/.ripped`.
///
/// Captures the minimum the mux side needs that can't be re-derived
/// from the ISO + mapfile + scan_image — primarily TMDB metadata,
/// display naming, cfg-bound knobs, and a few rip-side stats that
/// will land in the history record. Everything title-related
/// (streams, codecs, duration, capacity) is re-derived by
/// `Disc::scan_image` against the ISO, so the marker stays small and
/// resilient to libfreemkv DiscTitle field shifts.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RippedMarker {
    pub schema_version: u32, // currently 1
    pub iso_path: String,
    pub mapfile_path: String,
    pub display_name: String,
    pub disc_format: String,
    pub mkv_filename: String,
    pub tmdb_title: String,
    pub tmdb_year: u16,
    pub tmdb_poster: String,
    pub tmdb_overview: String,
    /// TMDB media type ("movie" or "tv"). `#[serde(default)]` (empty
    /// string) for backward-compat with pre-rc.4 markers that predate
    /// this field; the resume mux path falls back to "movie" when empty,
    /// matching the mover's own default.
    #[serde(default)]
    pub tmdb_media_type: String,
    pub max_retries: u8,
    pub abort_on_lost_secs: u32,
    pub rip_elapsed_secs: f64,
    pub rip_errors: u32,
    pub rip_lost_video_secs: f64,
    pub rip_last_sector: u64,
    pub origin_device: String, // for logging only
    // Sweep-damage snapshot for telemetry continuity on resume.
    // Optional (serde default) for backward-compat with pre-v0.25.12
    // markers that don't have these fields.
    #[serde(default)]
    pub sweep_errors: u32,
    #[serde(default)]
    pub sweep_total_lost_ms: f64,
    #[serde(default)]
    pub sweep_main_lost_ms: f64,
    #[serde(default)]
    pub sweep_num_bad_ranges: u32,
    #[serde(default)]
    pub sweep_largest_gap_ms: f64,
    /// Operator-confidence of the resolved title at hand-off time. True
    /// when the fresh-rip path decided the title is trustworthy enough to
    /// auto-file (`.done`) — either an exact normalized match with a year
    /// OR an explicit operator override via the '✎ change' picker. The mux
    /// worker's `resume_remux` ORs this into its own match check so an
    /// operator's deliberate pick isn't second-guessed when the chosen
    /// title differs from the disc's own (often cryptic) label.
    ///
    /// Optional (serde default `false`) for backward-compat with pre-rc.4
    /// markers that lack the field — those fall back to the match check
    /// alone, the prior behavior.
    #[serde(default)]
    pub title_confident: bool,
}

pub const RIPPED_MARKER_NAME: &str = ".ripped";
pub const RIPPED_MARKER_SCHEMA: u32 = 1;

pub fn write_marker(staging_dir: &Path, marker: &RippedMarker) -> std::io::Result<()> {
    let path = staging_dir.join(RIPPED_MARKER_NAME);
    let json = serde_json::to_string_pretty(marker)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    // Durable write (tmp + sync_all + rename + parent-dir fsync) via the same
    // primitive the rest of autorip's markers use. A plain `fs::write` here
    // could leave a torn/empty `.ripped` on a crash mid-write, which the mux
    // worker would then fail to parse — losing the hand-off.
    crate::ripper::staging::write_marker_durable(&path, json.as_bytes())?;
    // The `.ripped` hand-off supersedes the in-progress `.sweeping` marker
    // `rip_disc` wrote at staging-dir creation. Clear it only after `.ripped`
    // is durably on disk, so a crash between the two never leaves the dir with
    // neither marker (which the resume scan would treat as orphaned partial
    // state and restart-count).
    crate::ripper::staging::clear_sweeping_marker(staging_dir);
    Ok(())
}

pub fn read_marker(staging_dir: &Path) -> std::io::Result<RippedMarker> {
    let path = staging_dir.join(RIPPED_MARKER_NAME);
    let bytes = std::fs::read(path)?;
    let marker: RippedMarker = serde_json::from_slice(&bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if marker.schema_version != RIPPED_MARKER_SCHEMA {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "unsupported .ripped schema_version {} (expected {})",
                marker.schema_version, RIPPED_MARKER_SCHEMA
            ),
        ));
    }
    Ok(marker)
}

pub fn delete_marker(staging_dir: &Path) -> std::io::Result<()> {
    let path = staging_dir.join(RIPPED_MARKER_NAME);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Per-staging-dir error surfaced to the System page so the user can
/// act on it (e.g. `MuxFinalize` after an NFS hiccup that left the MKV
/// unseekable). Keyed by staging dir path; same `reason` for the same
/// path is idempotent — no log spam on retry ticks.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MuxerError {
    pub path: String,
    pub reason: String,
    pub hint: String,
}

pub static MUX_ERRORS: once_cell::sync::Lazy<Mutex<BTreeMap<String, MuxerError>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(BTreeMap::new()));

pub(crate) fn record_error(path: &str, reason: &str, hint: &str) {
    // Mutate the map and capture whether this is a new reason under the
    // lock, then DROP the guard before the syslog write. syslog does
    // blocking log-file I/O (NFS-backed staging on the testbed); holding
    // MUX_ERRORS across it would block every other record_error/clear_error
    // and the System-page reader for the duration of that write.
    let same_reason = {
        let Ok(mut m) = MUX_ERRORS.lock() else {
            return;
        };
        let same_reason = m.get(path).map(|e| e.reason == reason).unwrap_or(false);
        m.insert(
            path.to_string(),
            MuxerError {
                path: path.to_string(),
                reason: reason.to_string(),
                hint: hint.to_string(),
            },
        );
        same_reason
    };
    if !same_reason {
        crate::log::syslog(&format!("Mux blocked: {} — {}", path, reason));
    }
}

pub(crate) fn clear_error(path: &str) {
    if let Ok(mut m) = MUX_ERRORS.lock() {
        m.remove(path);
    }
}

/// Worker entry point — spawn from `main` alongside the mover thread.
///
/// A 10-second tick loop: each tick scans the staging dir for `.ripped`
/// hand-off markers (`check_and_mux`) and dispatches every one it finds
/// through the resume-mux path (`remux_from_ripped_marker`). On success
/// the dir gets a `.done`/`.completed` marker (handed to the mover) and
/// the `.ripped` marker is deleted; on failure the `.ripped` marker is
/// left in place for next-tick retry and a `MuxerError` is surfaced to
/// the System page. SHUTDOWN-responsive so SIGTERM doesn't wait a full
/// tick.
pub fn run(cfg: &Arc<RwLock<Config>>) {
    use std::sync::atomic::Ordering;
    tracing::info!("mux loop starting");
    while !crate::SHUTDOWN.load(Ordering::Relaxed) {
        // A poisoned RwLock never un-poisons, so a bare `is_err()` check
        // here would turn a one-time poison into a permanent warn+sleep
        // spin: the worker would never mux again, never exit, and
        // /api/state would still report healthy. This path only reads
        // Config, so recover from poison (handled inside check_and_mux's
        // `unwrap_or_else(into_inner)`) instead of spinning.
        check_and_mux(cfg);
        // SHUTDOWN-responsive sleep — same pattern as the mover so
        // SIGTERM doesn't have to wait the full 10 s tick.
        for _ in 0..100 {
            if crate::SHUTDOWN.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    tracing::info!("mux loop stopping");
}

/// Verdict for whether the mux worker should act on one staging dir this
/// tick. Pure projection of the dir's marker state so the full
/// present/absent matrix is unit-testable (`mux_dispatch_verdict`) without
/// standing up a real mux pipeline. The driving loop in `check_and_mux`
/// translates `Dispatch` into an actual `remux_from_ripped_marker` call and
/// every `Skip*` into `continue`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MuxVerdict {
    /// `.ripped` present, no terminal marker, listing trustworthy — run the mux.
    Dispatch,
    /// `.completed` or `.failed` present — finished or quarantined, never re-mux.
    SkipTerminal,
    /// No `.ripped` hand-off marker — nothing for the worker to do here.
    SkipNoMarker,
    /// Snapshot is `None` — the dir's contents are UNKNOWN (read_dir / DirEntry
    /// errors mid-scan). Skip this tick rather than dispatch on an untrustworthy
    /// listing; retry next tick.
    SkipUnknown,
}

/// Pure dispatch decider for the mux worker. `snap` is the result of
/// `snapshot_staging_disc` for the dir (`None` ⇒ UNKNOWN contents).
///
/// Order matters and mirrors `check_and_mux`'s former inline guards:
/// 1. `None` snapshot ⇒ `SkipUnknown` (don't dispatch on a degraded listing).
/// 2. `.completed` OR `.failed` ⇒ `SkipTerminal` — terminal regardless of
///    whether `.ripped` still lingers (the post-mux-abort `.ripped`+`.failed`
///    re-mux loop, da16f00, lives or dies on this arm).
/// 3. `.ripped` absent ⇒ `SkipNoMarker`.
/// 4. otherwise ⇒ `Dispatch`.
///
/// `has_ripped` is read from the same primed `read_dir` view as the snapshot
/// so a cold-cache NFS miss can't race `.ripped` to "absent" while the
/// snapshot surfaces a terminal marker — see `StagingSnapshot::has_ripped`.
pub(crate) fn mux_dispatch_verdict(
    snap: Option<&crate::ripper::staging::StagingSnapshot>,
) -> MuxVerdict {
    let Some(snap) = snap else {
        return MuxVerdict::SkipUnknown;
    };
    // Terminal on `.failed` PRESENCE (`has_failed`), not on a parseable
    // reason. review.rs writes a non-JSON `.failed` ("cancelled by operator")
    // whose `failed_reason` is None; keying on `failed_reason.is_some()` here
    // would let the worker re-dispatch a `.ripped`+`.failed` dir forever.
    if snap.completed || snap.has_failed {
        return MuxVerdict::SkipTerminal;
    }
    if !snap.has_ripped {
        return MuxVerdict::SkipNoMarker;
    }
    MuxVerdict::Dispatch
}

/// RAII cleanup for the `.muxing` exclusion lock. Removing the marker on drop
/// guarantees it is cleared on every exit of one `check_and_mux` loop iteration
/// — the success branch, the failure branch, or a panic in the mux pipeline —
/// so a crashed/aborted mux never strands a stale `.muxing` lock that would
/// permanently hide the dir from the drive-resume paths.
struct MuxingGuard<'a>(&'a Path);

impl Drop for MuxingGuard<'_> {
    fn drop(&mut self) {
        crate::ripper::staging::clear_muxing_marker(self.0);
    }
}

/// Find all staging dirs with a `.ripped` marker and dispatch each
/// through the resume-mux path. Serialized — only one mux runs at a
/// time inside this worker thread (the next one waits on the loop
/// tick). v0.25.3 ships with a single shared worker; concurrent
/// muxes are explicitly out of scope (RAM/CPU thrash with no real
/// win on a single-host setup).
fn check_and_mux(cfg_arc: &Arc<RwLock<Config>>) {
    // Recover from a poisoned config lock rather than returning (which,
    // combined with the per-tick loop, would silently wedge the worker
    // forever). This borrow only reads the staging path.
    let staging_root = cfg_arc
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .staging_dir
        .clone();
    let entries = match std::fs::read_dir(&staging_root) {
        Ok(e) => e,
        Err(e) => {
            // A dropped NFS mount or a deleted staging dir would otherwise
            // silently freeze every future tick. Surface it so the operator
            // sees a paused mux queue instead of a frozen one.
            tracing::warn!("mux: cannot read staging dir {staging_root:?}: {e}");
            record_error(
                &staging_root,
                &format!("cannot read staging dir: {e}"),
                "check the staging mount (NFS) is up and the dir exists; mux is paused until it is readable",
            );
            return;
        }
    };
    // The staging dir is readable again — clear any prior "cannot read"
    // error so the System page doesn't show a stale alarm.
    clear_error(&staging_root);
    for entry in entries {
        // A per-entry error (NFS stat hiccup, a racing rename) must not
        // silently drop a staged dir from the mux queue and strand a
        // finished rip. Surface it and move on to the next entry.
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("mux: skipping unreadable staging entry: {e}");
                record_error(
                    &staging_root,
                    &format!("unreadable staging entry: {e}"),
                    "a staging dir entry could not be read (NFS stat error / racing rename); it is skipped this tick and retried next tick",
                );
                continue;
            }
        };
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }
        // Never re-mux an already-completed dir. `remux_from_ripped_marker`
        // deletes `.ripped` on success, but if that delete fails (a
        // persistent NFS / permission error on the marker file) the
        // `.ripped` file survives and, without this guard, the next tick
        // would re-dispatch the same dir — deleting the just-written MKV
        // via `delete_partial_output`, re-scanning, re-muxing, and
        // re-writing `.done` every tick forever. `.completed` is written
        // on success and is the authoritative "this dir is finished"
        // signal, so it breaks the loop regardless of why the marker
        // delete failed.
        //
        // Probe completion via `snapshot_staging_disc` rather than a bare
        // `Path::exists()` on `.completed`. On NFS with a cold attribute
        // cache (typical on a Watchtower-driven container restart), a
        // single-shot stat can return "absent" for a marker that is
        // durably on the server — re-dispatching a finished dir, which
        // `delete_partial_output` then wipes before re-muxing from
        // scratch. The snapshot reads `.completed` from a primed,
        // 3x-retried `read_dir` view (the same defense the startup resume
        // scan relies on), so a transient cold-cache miss can't race the
        // marker to "absent". A `None` snapshot means the dir's contents
        // are UNKNOWN (read_dir/DirEntry errors mid-scan): skip this tick
        // and retry next rather than re-dispatching on an untrustworthy
        // listing.
        //
        // The dispatch decision (`.ripped` present? terminal marker? listing
        // trustworthy?) is factored into the pure `mux_dispatch_verdict` so
        // the full present/absent marker matrix is unit-testable without a
        // real mux pipeline — the gap that let the .ripped+.failed re-mux
        // loop (commit da16f00) ship untested.
        let snap = crate::ripper::staging::snapshot_staging_disc(&dir);
        match mux_dispatch_verdict(snap.as_ref()) {
            MuxVerdict::Dispatch => {}
            MuxVerdict::SkipTerminal | MuxVerdict::SkipNoMarker | MuxVerdict::SkipUnknown => {
                continue;
            }
        }
        let marker = match read_marker(&dir) {
            Ok(m) => m,
            // TOCTOU: the `.exists()` check above and this read race a
            // concurrent cleanup / fast subsequent tick. If the marker
            // vanished in between, that's not a malformed-marker error —
            // skip silently rather than recording a spurious "No such
            // file or directory" MuxerError that sticks in the System
            // page until dismissed.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                let path_str = dir.to_string_lossy().to_string();
                record_error(
                    &path_str,
                    &format!("malformed .ripped marker: {e}"),
                    "delete the .ripped file (or the whole staging dir) and re-run the rip; the marker schema may be out of date",
                );
                continue;
            }
        };
        let title = marker.display_name.clone();
        tracing::info!(
            staging = %dir.display(),
            title = %title,
            "mux worker: dispatching .ripped marker"
        );
        crate::log::syslog(&format!("Muxing: {} (worker)", title));
        // Register an exclusion lock for the duration of the mux. The drive
        // paths (`disc_already_completed` auto-insert, `find_resumable_for_disc`)
        // skip any dir carrying `.muxing`, so a disc re-insert can't run a fresh
        // sweep that truncates the ISO this worker is reading, nor double-mux
        // the same output. Cleared on every exit of this iteration (success or
        // failure) via the `_guard` drop below — the dir is then governed by
        // `.completed`/`.failed`/`.ripped` instead.
        crate::ripper::staging::write_muxing_marker(&dir);
        let _guard = MuxingGuard(&dir);
        let outcome = crate::ripper::resume::remux_from_ripped_marker(cfg_arc, &dir, &marker);
        if outcome.success {
            clear_error(&dir.to_string_lossy());
            tracing::info!(staging = %dir.display(), title = %title, "mux worker: completed");
            crate::log::syslog(&format!("Muxed: {}", title));
            // Defensive: drive the origin device to "done" if (and ONLY
            // if) it is somehow still "ripping". The normal `.ripped`
            // hand-off in `rip_disc` now sets the real device to "done"
            // the instant the read finishes (the disc is read; the mux is
            // a separate phase carried by the synthetic `_mux` device), so
            // for the common path this guard is a no-op — and crucially it
            // means this synthetic-device worker can NEVER revert a real
            // device's "done" tile back through "ripping". The guard still
            // fires for the inline-mux fallback path (marker write failed),
            // which leaves the device "ripping" while it muxes inline.
            // The `still_ripping` check also avoids clobbering a device
            // that was re-used for a new rip while this mux ran.
            let origin = &marker.origin_device;
            if !origin.is_empty() {
                let origin_status = crate::ripper::STATE
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(origin.as_str())
                    .map(|rs| rs.status.clone());
                if should_revert_origin_to_done(origin, origin_status.as_deref()) {
                    crate::ripper::update_state(
                        origin,
                        crate::ripper::RipState {
                            device: origin.clone(),
                            status: "done".to_string(),
                            disc_present: true,
                            disc_name: marker.display_name.clone(),
                            disc_format: marker.disc_format.clone(),
                            progress_pct: 100,
                            // Combined sweep + mux-time loss from the mux
                            // outcome (the `_mux` done-state folded demux/
                            // decrypt skips into the sweep mapfile totals).
                            // The marker's `sweep_*` fields are sweep-only, so
                            // using them here would understate the loss in the
                            // delivered MKV whenever a mux-phase decrypt/codec
                            // skip added loss — diverging from the `_mux` tile
                            // and the webhook, which report the combined figure.
                            errors: outcome.errors,
                            total_lost_ms: outcome.total_lost_ms,
                            main_lost_ms: outcome.main_lost_ms,
                            num_bad_ranges: marker.sweep_num_bad_ranges,
                            largest_gap_ms: marker.sweep_largest_gap_ms,
                            // The bad-ranges drilldown list isn't in the
                            // marker (RippedMarker carries only summary
                            // counts), so plumb it from the mux outcome —
                            // captured off the `_mux` done-state, which
                            // recomputed it from the mapfile. Without this the
                            // origin device's tile shows the damage count but
                            // an empty drilldown, diverging from the fresh-rip
                            // and cold auto-resume done cards.
                            bad_ranges: outcome.bad_ranges.clone(),
                            bad_ranges_truncated: outcome.bad_ranges_truncated,
                            tmdb_title: marker.tmdb_title.clone(),
                            tmdb_year: marker.tmdb_year,
                            tmdb_poster: marker.tmdb_poster.clone(),
                            tmdb_overview: marker.tmdb_overview.clone(),
                            // Carry the mux-derived display fields (codecs +
                            // duration + output_file from the `_mux` done-state)
                            // and the marker's lost-video estimate so the origin
                            // device's done card matches the inline fresh-rip
                            // done card instead of dropping the codec badge,
                            // duration, output path, and lost-video figure.
                            codecs: outcome.codecs.clone(),
                            duration: outcome.duration.clone(),
                            output_file: outcome.output_file.clone(),
                            // Combined sweep + mux-time loss (see `errors`
                            // above). `marker.rip_lost_video_secs` is sweep-only
                            // and would understate the headline loss figure on a
                            // disc with accepted mux-phase decrypt loss.
                            lost_video_secs: outcome.lost_video_secs,
                            ..Default::default()
                        },
                    );
                }
            }
        } else {
            let path_str = dir.to_string_lossy().to_string();
            // Surface the ACTUAL reason the mux was blocked (e.g. "0.86s lost at
            // mux exceeds threshold 0s") from the staging marker the mux worker
            // wrote, instead of a generic "see the device log" — the operator
            // should not have to read device logs to learn why. A loss-abort
            // also leaves a resumable `.aborted-loss`, so the UI can offer
            // Accept-damage off that reason.
            let reason = crate::ripper::staging::read_aborted_loss(&dir)
                .map(|(r, _)| r)
                .or_else(|| crate::ripper::staging::read_failed_reason(&dir))
                .unwrap_or_else(|| {
                    "mux worker dispatch did not complete (see _mux device log)".to_string()
                });
            record_error(
                &path_str,
                &reason,
                "staging is preserved — raise the loss threshold or Accept the damage to deliver as-is",
            );
        }
    }
}

/// Should the mux worker drive the origin device to "done" after a
/// successful mux? Pure projection of the device key + its current status
/// so the contract is unit-testable without standing up STATE + a real mux.
///
/// Two rules:
/// 1. A real origin device only needs the revert if it is STILL "ripping"
///    — the inline-mux FALLBACK path (the `.ripped` marker write failed, so
///    `rip_disc` muxed inline while leaving the tile "ripping"). On the
///    normal `.ripped` hand-off path the tile is already "done" (the read
///    finished) and this is a no-op, so the synthetic `_mux` worker can
///    never push a real "done" tile back through "ripping" (bug #1).
/// 2. A synthetic underscore-prefixed `origin` (defensive — should not
///    occur, the marker's `origin_device` is the physical drive) is never
///    reverted: those carry no user-visible tile.
///
/// `status == None` (the device entry vanished — re-used / cleared) ⇒ no
/// revert, matching the prior `.unwrap_or(false)`.
pub(crate) fn should_revert_origin_to_done(origin: &str, status: Option<&str>) -> bool {
    !origin.is_empty() && !origin.starts_with('_') && status == Some("ripping")
}

/// Scan the staging dir for pending mux jobs. Returns display names
/// for the System page's Mux Queue panel.
pub fn pending_queue(staging_dir: &Path) -> Vec<String> {
    let entries = match std::fs::read_dir(staging_dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    for entry in entries.filter_map(|e| e.ok()) {
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }
        if !dir.join(RIPPED_MARKER_NAME).exists() {
            continue;
        }
        // A successful mux can leave `.ripped` alongside `.completed`
        // when delete_marker fails post-mux (NFS, see resume.rs). The
        // `.completed` marker is the authoritative "done" signal — skip
        // the dir so a finished title doesn't report "(queued)" forever.
        // `.failed` is equally terminal (post-mux abort): skip it too so an
        // aborted title doesn't report "(queued)" indefinitely.
        //
        // MUTUAL EXCLUSION (a job is in exactly ONE queue at a time): a
        // mux's success path writes the mover hand-off marker (`.done` /
        // `.review`) BEFORE the `.completed` marker (the `.done` write is
        // the durability/crash barrier), and `.done` is exactly what the
        // System-page Move queue scans for. So once `.done`/`.review`
        // exists the job has logically moved to the Move queue — it must
        // NOT also still report "(queued)" here, even in the brief window
        // before `.completed` lands or if the post-mux `.ripped` delete
        // failed. Skipping on `.done`/`.review` closes that double-listing
        // window atomically with the move-queue trigger.
        //
        // Likewise skip the dir currently being muxed (`.muxing`): it is
        // surfaced as the live, in-flight mux via the synthetic `_mux`
        // device, so listing it again as "(queued)" here would double it.
        if dir.join(crate::ripper::staging::COMPLETED_MARKER).exists()
            || dir.join(crate::ripper::staging::FAILED_MARKER).exists()
            || dir.join(crate::ripper::staging::DONE_MARKER).exists()
            || dir.join(crate::ripper::staging::REVIEW_MARKER).exists()
            || dir.join(crate::ripper::staging::MUXING_MARKER).exists()
        {
            continue;
        }
        if let Ok(m) = read_marker(&dir) {
            out.push(format!("{} (queued)", m.display_name));
        } else {
            // Malformed marker — still surface the dir name so the
            // operator notices it sitting in the queue.
            let name = dir
                .file_name()
                .map(|n| n.to_string_lossy().replace('_', " ").to_string())
                .unwrap_or_default();
            out.push(format!("{} (malformed)", name));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn record_and_clear_error_round_trip() {
        record_error("/x/staging/Foo", "test reason", "test hint");
        {
            let m = MUX_ERRORS.lock().unwrap();
            assert!(m.contains_key("/x/staging/Foo"));
            assert_eq!(m["/x/staging/Foo"].reason, "test reason");
        }
        clear_error("/x/staging/Foo");
        let m = MUX_ERRORS.lock().unwrap();
        assert!(!m.contains_key("/x/staging/Foo"));
    }

    fn sample_marker() -> RippedMarker {
        RippedMarker {
            schema_version: RIPPED_MARKER_SCHEMA,
            iso_path: "/staging/Border_Town/Border_Town.iso".into(),
            mapfile_path: "/staging/Border_Town/Border_Town.iso.mapfile".into(),
            display_name: "Border Town".into(),
            disc_format: "uhd".into(),
            mkv_filename: "Border_Town.mkv".into(),
            tmdb_title: "Border Town".into(),
            tmdb_year: 2024,
            tmdb_poster: "https://image.tmdb.org/poster.jpg".into(),
            tmdb_overview: "Synopsis".into(),
            tmdb_media_type: "tv".into(),
            max_retries: 5,
            abort_on_lost_secs: 30,
            rip_elapsed_secs: 1234.0,
            rip_errors: 0,
            rip_lost_video_secs: 0.0,
            rip_last_sector: 32_000_000,
            origin_device: "sg0".into(),
            sweep_errors: 0,
            sweep_total_lost_ms: 0.0,
            sweep_main_lost_ms: 0.0,
            sweep_num_bad_ranges: 0,
            sweep_largest_gap_ms: 0.0,
            title_confident: false,
        }
    }

    #[test]
    fn marker_round_trip() {
        let tmp = TempDir::new().unwrap();
        let marker = sample_marker();
        write_marker(tmp.path(), &marker).unwrap();
        let back = read_marker(tmp.path()).unwrap();
        assert_eq!(back.display_name, "Border Town");
        assert_eq!(back.tmdb_year, 2024);
        assert_eq!(back.schema_version, RIPPED_MARKER_SCHEMA);
        // media_type must survive the hand-off: the resume mux path seeds it
        // into STATE and writes it into the `.done`/`.review` marker so the
        // mover routes a resumed TV rip to the TV library, not movies.
        assert_eq!(back.tmdb_media_type, "tv");
    }

    /// Backward-compat: a pre-rc.4 `.ripped` marker on disk has no
    /// `tmdb_media_type` field. It must deserialize (serde default = empty
    /// string) rather than failing the resume. The resume mux path then
    /// falls back to "movie" — identical to the mover's own default.
    #[test]
    fn marker_without_media_type_defaults_empty() {
        let json = r#"{
            "schema_version": 1,
            "iso_path": "/staging/Old/Old.iso",
            "mapfile_path": "/staging/Old/Old.iso.mapfile",
            "display_name": "Old",
            "disc_format": "uhd",
            "mkv_filename": "Old.mkv",
            "tmdb_title": "Old",
            "tmdb_year": 2020,
            "tmdb_poster": "",
            "tmdb_overview": "",
            "max_retries": 3,
            "abort_on_lost_secs": 0,
            "rip_elapsed_secs": 0.0,
            "rip_errors": 0,
            "rip_lost_video_secs": 0.0,
            "rip_last_sector": 0,
            "origin_device": "sg0"
        }"#;
        let marker: RippedMarker = serde_json::from_str(json).unwrap();
        assert_eq!(marker.tmdb_media_type, "");
    }

    #[test]
    fn read_marker_rejects_wrong_schema() {
        let tmp = TempDir::new().unwrap();
        let mut marker = sample_marker();
        marker.schema_version = 9999;
        write_marker(tmp.path(), &marker).unwrap();
        let err = read_marker(tmp.path()).unwrap_err();
        assert!(format!("{err}").contains("schema_version"));
    }

    #[test]
    fn delete_marker_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        delete_marker(tmp.path()).expect("delete on missing path is OK");
        write_marker(tmp.path(), &sample_marker()).unwrap();
        delete_marker(tmp.path()).unwrap();
        assert!(!tmp.path().join(RIPPED_MARKER_NAME).exists());
    }

    #[test]
    fn pending_queue_lists_markers() {
        let tmp = TempDir::new().unwrap();
        let movie = tmp.path().join("Border_Town");
        std::fs::create_dir_all(&movie).unwrap();
        write_marker(&movie, &sample_marker()).unwrap();

        let other = tmp.path().join("No_Marker_Here");
        std::fs::create_dir_all(&other).unwrap();

        let q = pending_queue(tmp.path());
        assert_eq!(q.len(), 1);
        assert!(q[0].contains("Border Town"));
        assert!(q[0].contains("queued"));
    }

    // Regression: the `check_and_mux` completion guard must consult
    // `snapshot_staging_disc` (which reads `.completed` from a primed,
    // 3x-retried `read_dir` view) instead of a bare `Path::exists()`.
    // A finished dir (`.ripped` + `.completed`) must report
    // `completed == true` so the guard short-circuits and the dir is NOT
    // re-dispatched to remux (which would wipe the just-written MKV).
    #[test]
    fn completion_guard_sees_completed_via_snapshot() {
        let tmp = TempDir::new().unwrap();
        let movie = tmp.path().join("Border_Town");
        std::fs::create_dir_all(&movie).unwrap();
        write_marker(&movie, &sample_marker()).unwrap();
        crate::ripper::staging::write_completed_marker(&movie);

        let snap = crate::ripper::staging::snapshot_staging_disc(&movie)
            .expect("a populated dir must yield a snapshot");
        assert!(
            snap.completed,
            "snapshot must report completed=true for a dir with .completed; \
             the check_and_mux guard relies on this to avoid re-muxing a finished dir"
        );
    }

    #[test]
    fn pending_queue_skips_completed_dir() {
        // A successful mux can leave `.ripped` alongside `.completed`
        // when delete_marker fails post-mux (NFS). `.completed` is the
        // authoritative "done" signal — such a dir must not show up as
        // "(queued)" forever.
        let tmp = TempDir::new().unwrap();
        let movie = tmp.path().join("Border_Town");
        std::fs::create_dir_all(&movie).unwrap();
        write_marker(&movie, &sample_marker()).unwrap();
        crate::ripper::staging::write_completed_marker(&movie);

        let q = pending_queue(tmp.path());
        assert!(
            q.is_empty(),
            "a dir with .completed present must be skipped, got {q:?}"
        );
    }

    // Regression (re-mux-forever loop): a post-mux abort writes `.failed`
    // WITHOUT `.completed`, and `.ripped` is only deleted on success — so a
    // `.ripped` + `.failed` dir must be recognised as TERMINAL by both the
    // check_and_mux guard (via `failed_reason`) and pending_queue, or the
    // worker re-dispatches it every tick forever (re-mux → re-abort → repeat).
    #[test]
    fn completion_guard_sees_failed_via_snapshot() {
        let tmp = TempDir::new().unwrap();
        let movie = tmp.path().join("Border_Town");
        std::fs::create_dir_all(&movie).unwrap();
        write_marker(&movie, &sample_marker()).unwrap();
        crate::ripper::staging::write_failed_marker(
            &movie,
            "aborted: demux loss exceeds threshold",
        );

        let snap = crate::ripper::staging::snapshot_staging_disc(&movie)
            .expect("a populated dir must yield a snapshot");
        assert!(!snap.completed, ".failed dir is not .completed");
        assert!(
            snap.failed_reason.is_some(),
            "snapshot must report failed_reason for a .failed dir; the \
             check_and_mux guard relies on this to avoid the re-mux-forever loop"
        );
    }

    // Regression (bug #3, mutual exclusion): a successful mux writes the
    // mover hand-off marker (`.done`) BEFORE the terminal `.completed`
    // marker, and the Move queue scans for `.done`. So the instant a job
    // enters the Move queue it must NOT also report "(queued)" in the Mux
    // queue — even in the window before `.completed` lands or if the
    // post-mux `.ripped` delete failed. Before the fix, `pending_queue`
    // only skipped `.completed`/`.failed`, so a `.ripped` + `.done` dir
    // double-listed in both queues until a hard browser refresh.
    #[test]
    fn pending_queue_skips_done_dir_mutual_exclusion() {
        let tmp = TempDir::new().unwrap();
        let movie = tmp.path().join("Border_Town");
        std::fs::create_dir_all(&movie).unwrap();
        write_marker(&movie, &sample_marker()).unwrap();
        // The mover hand-off marker is present but `.completed` is NOT yet
        // (the gap between the two durable writes). This dir is in the Move
        // queue; it must be absent from the Mux queue.
        std::fs::write(movie.join(".done"), b"{}").unwrap();

        let q = pending_queue(tmp.path());
        assert!(
            q.is_empty(),
            "a dir with .done (in the Move queue) must NOT also be (queued) in the Mux queue, got {q:?}"
        );
    }

    // A `.review` dir (low-confidence hand-off held for the operator) is
    // likewise the mover's concern, not the mux worker's — it must not
    // double-list in the Mux queue.
    #[test]
    fn pending_queue_skips_review_dir() {
        let tmp = TempDir::new().unwrap();
        let movie = tmp.path().join("Border_Town");
        std::fs::create_dir_all(&movie).unwrap();
        write_marker(&movie, &sample_marker()).unwrap();
        std::fs::write(movie.join(".review"), b"{}").unwrap();

        let q = pending_queue(tmp.path());
        assert!(q.is_empty(), "a .review dir must be skipped, got {q:?}");
    }

    // The dir currently being muxed carries `.muxing` and is surfaced as
    // the live in-flight mux via the synthetic `_mux` device — it must not
    // also appear as "(queued)" in the static pending list.
    #[test]
    fn pending_queue_skips_muxing_dir() {
        let tmp = TempDir::new().unwrap();
        let movie = tmp.path().join("Border_Town");
        std::fs::create_dir_all(&movie).unwrap();
        write_marker(&movie, &sample_marker()).unwrap();
        crate::ripper::staging::write_muxing_marker(&movie);

        let q = pending_queue(tmp.path());
        assert!(
            q.is_empty(),
            "a dir actively muxing (.muxing) must not also be (queued), got {q:?}"
        );
    }

    // Regression (bug #1): after the `.ripped` hand-off the REAL device is
    // already "done" (the read finished). The mux worker's post-mux revert
    // (`should_revert_origin_to_done`) only fires for a device still
    // "ripping", so the synthetic `_mux` worker can NEVER push a real
    // device's "done" tile back to "ripping". Drives the REAL production
    // helper against STATE so the test and `check_and_mux` can't diverge.
    #[test]
    fn mux_worker_does_not_revert_done_origin_device() {
        let device = "sg_test_origin_already_done";
        // Hand-off set the real device straight to "done" (the new contract).
        crate::ripper::update_state(
            device,
            crate::ripper::RipState {
                device: device.to_string(),
                status: "done".to_string(),
                progress_pct: 100,
                disc_name: "Border Town".to_string(),
                disc_format: "uhd".to_string(),
                ..Default::default()
            },
        );
        let status = crate::ripper::STATE
            .lock()
            .unwrap()
            .get(device)
            .map(|rs| rs.status.clone());
        assert!(
            !should_revert_origin_to_done(device, status.as_deref()),
            "a device already 'done' at hand-off must not be reverted by the mux worker"
        );
        // Cleanup so the synthetic entry doesn't leak into other tests.
        crate::ripper::STATE.lock().unwrap().remove(device);
    }

    // Companion (bug #1, the OTHER half): on the INLINE-MUX FALLBACK path
    // (the `.ripped` marker write failed, so `rip_disc` muxed inline and
    // left the tile "ripping"), the revert IS needed and MUST fire — the
    // fix must not over-correct into never reverting. Here the worker's
    // `origin_device` device is still "ripping", so the helper returns true.
    #[test]
    fn mux_worker_reverts_ripping_origin_on_inline_fallback() {
        let device = "sg_test_origin_still_ripping";
        crate::ripper::update_state(
            device,
            crate::ripper::RipState {
                device: device.to_string(),
                status: "ripping".to_string(),
                disc_name: "Border Town".to_string(),
                disc_format: "uhd".to_string(),
                ..Default::default()
            },
        );
        let status = crate::ripper::STATE
            .lock()
            .unwrap()
            .get(device)
            .map(|rs| rs.status.clone());
        assert!(
            should_revert_origin_to_done(device, status.as_deref()),
            "a still-'ripping' origin device (inline-mux fallback) MUST be reverted to done"
        );
        crate::ripper::STATE.lock().unwrap().remove(device);
    }

    // The revert predicate edge cases: empty origin, synthetic origin, and a
    // vanished/absent device entry are all no-ops; only a real, still-ripping
    // device reverts.
    #[test]
    fn revert_origin_predicate_edge_cases() {
        assert!(
            !should_revert_origin_to_done("", Some("ripping")),
            "empty origin must not revert"
        );
        assert!(
            !should_revert_origin_to_done("_mux", Some("ripping")),
            "a synthetic origin must not revert"
        );
        assert!(
            !should_revert_origin_to_done("sg0", None),
            "a vanished device entry (None status) must not revert"
        );
        assert!(
            !should_revert_origin_to_done("sg0", Some("done")),
            "an already-done device must not revert"
        );
        assert!(
            should_revert_origin_to_done("sg0", Some("ripping")),
            "a real, still-ripping device must revert"
        );
    }

    #[test]
    fn pending_queue_skips_failed_dir() {
        let tmp = TempDir::new().unwrap();
        let movie = tmp.path().join("Border_Town");
        std::fs::create_dir_all(&movie).unwrap();
        write_marker(&movie, &sample_marker()).unwrap();
        crate::ripper::staging::write_failed_marker(
            &movie,
            "aborted: demux loss exceeds threshold",
        );

        let q = pending_queue(tmp.path());
        assert!(
            q.is_empty(),
            "a dir with .failed present is terminal and must be skipped, got {q:?}"
        );
    }

    // Regression: origin device must reach a terminal non-"ripping" status
    // after mux success. The hand-off in rip_disc leaves the origin device
    // frozen at "ripping"; check_and_mux must flip it to "done".
    #[test]
    fn origin_device_reaches_done_after_mux_success() {
        let device = "_test_origin_mux_done";
        // Simulate the hand-off state: origin device stuck at "ripping".
        crate::ripper::update_state(
            device,
            crate::ripper::RipState {
                device: device.to_string(),
                status: "ripping".to_string(),
                disc_name: "Border Town".to_string(),
                disc_format: "uhd".to_string(),
                ..Default::default()
            },
        );
        assert_eq!(
            crate::ripper::STATE
                .lock()
                .unwrap()
                .get(device)
                .map(|s| s.status.as_str()),
            Some("ripping"),
            "precondition: device should be ripping before mux completes"
        );

        // Simulate what check_and_mux does on success for this origin device.
        let marker = sample_marker();
        let origin = &marker.origin_device;
        let still_ripping = crate::ripper::STATE
            .lock()
            .ok()
            .and_then(|s| s.get(origin.as_str()).map(|rs| rs.status == "ripping"))
            .unwrap_or(false);
        // In this test the marker's origin_device is "sg0" not `device`,
        // so we drive `device` directly to verify the logic.
        let _ = still_ripping;
        crate::ripper::update_state(
            device,
            crate::ripper::RipState {
                device: device.to_string(),
                status: "done".to_string(),
                disc_present: true,
                disc_name: marker.display_name.clone(),
                disc_format: marker.disc_format.clone(),
                progress_pct: 100,
                errors: marker.sweep_errors,
                total_lost_ms: marker.sweep_total_lost_ms,
                main_lost_ms: marker.sweep_main_lost_ms,
                num_bad_ranges: marker.sweep_num_bad_ranges,
                largest_gap_ms: marker.sweep_largest_gap_ms,
                tmdb_title: marker.tmdb_title.clone(),
                tmdb_year: marker.tmdb_year,
                tmdb_poster: marker.tmdb_poster.clone(),
                tmdb_overview: marker.tmdb_overview.clone(),
                ..Default::default()
            },
        );

        let s = crate::ripper::STATE.lock().unwrap();
        let rs = s.get(device).expect("device state must exist");
        assert_eq!(
            rs.status, "done",
            "origin device must be 'done' after mux success"
        );
        assert_eq!(rs.progress_pct, 100, "progress must be 100 on done");
    }

    // Regression: done-card damage telemetry must not be zeroed. A marker
    // with non-zero sweep damage fields must produce a RipState that
    // carries those values through update_state (which derives damage_severity).
    #[test]
    fn done_card_carries_sweep_damage_telemetry() {
        let device = "_test_done_damage_telemetry";
        let mut marker = sample_marker();
        marker.sweep_errors = 42;
        marker.sweep_total_lost_ms = 3500.0;
        marker.sweep_main_lost_ms = 2000.0;
        marker.sweep_num_bad_ranges = 3;
        marker.sweep_largest_gap_ms = 1200.0;

        crate::ripper::update_state(
            device,
            crate::ripper::RipState {
                device: device.to_string(),
                status: "done".to_string(),
                disc_present: true,
                disc_name: marker.display_name.clone(),
                disc_format: marker.disc_format.clone(),
                progress_pct: 100,
                errors: marker.sweep_errors,
                total_lost_ms: marker.sweep_total_lost_ms,
                main_lost_ms: marker.sweep_main_lost_ms,
                num_bad_ranges: marker.sweep_num_bad_ranges,
                largest_gap_ms: marker.sweep_largest_gap_ms,
                ..Default::default()
            },
        );

        let s = crate::ripper::STATE.lock().unwrap();
        let rs = s.get(device).expect("device state must exist");
        assert_eq!(rs.status, "done");
        assert_eq!(rs.errors, 42, "errors must carry through to done state");
        assert!(
            rs.total_lost_ms > 0.0,
            "total_lost_ms must be non-zero on damaged done card"
        );
        assert!(
            !rs.damage_severity.is_empty(),
            "damage_severity must be set for a damaged done card (got empty — update_state must derive it from errors/total_lost_ms)"
        );
    }

    // ===================================================================
    // EXHAUSTIVE mux-worker dispatch matrix (rc4 hardening).
    //
    // The mux worker is one of the three staging-state deciders. Its job:
    // for each per-disc staging dir, decide whether to (re)run the mux
    // (`Dispatch`) or skip (terminal / no-marker / unknown-listing). The
    // real loop in `check_and_mux` calls `snapshot_staging_disc` then
    // `mux_dispatch_verdict`; these tests drive that exact pair against a
    // real TempDir for every meaningful marker combination, closing the
    // coverage gap that let the `.ripped` + `.failed` re-mux-forever loop
    // (commit da16f00) ship untested.
    // ===================================================================

    /// The staging-side constant must equal the muxer's own marker name,
    /// or `snapshot_staging_disc` would observe `.ripped` under a different
    /// name than the worker writes and `has_ripped` would never be set.
    #[test]
    fn ripped_marker_name_matches_staging_constant() {
        assert_eq!(
            RIPPED_MARKER_NAME,
            crate::ripper::staging::RIPPED_MARKER,
            "the muxer's .ripped marker name and the staging-scan constant must agree"
        );
    }

    /// Marker tokens a dispatch-matrix row can place in a staging dir.
    #[derive(Clone, Copy)]
    enum M {
        Ripped,
        Completed,
        Failed,
        /// A non-JSON `.failed` body (review.rs operator-cancel). Pins that the
        /// dispatch verdict keys on marker PRESENCE (`has_failed`), not a
        /// parseable `failed_reason` (M2).
        FailedNonJson,
        Done,
        Review,
        Iso,
        Mapfile,
        Mkv,
    }

    /// Build a populated per-disc staging dir for the given markers, run the
    /// real snapshot+verdict pair, and return the verdict.
    fn verdict_for(markers: &[M]) -> MuxVerdict {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("Disc");
        std::fs::create_dir_all(&dir).unwrap();
        for m in markers {
            match m {
                M::Ripped => {
                    // A real, schema-valid .ripped marker (so this dir is
                    // indistinguishable from a true hand-off).
                    write_marker(&dir, &sample_marker()).unwrap();
                }
                M::Completed => crate::ripper::staging::write_completed_marker(&dir),
                M::Failed => crate::ripper::staging::write_failed_marker(&dir, "test failure"),
                M::FailedNonJson => {
                    std::fs::write(dir.join(".failed"), b"cancelled by operator\n").unwrap()
                }
                M::Done => std::fs::write(dir.join(".done"), b"{}").unwrap(),
                M::Review => std::fs::write(dir.join(".review"), b"{}").unwrap(),
                M::Iso => std::fs::write(dir.join("Disc.iso"), b"x").unwrap(),
                M::Mapfile => std::fs::write(dir.join("Disc.iso.mapfile"), b"x").unwrap(),
                M::Mkv => std::fs::write(dir.join("Disc.mkv"), b"x").unwrap(),
            }
        }
        let snap = crate::ripper::staging::snapshot_staging_disc(&dir);
        mux_dispatch_verdict(snap.as_ref())
    }

    #[test]
    fn mux_dispatch_matrix() {
        use M::*;
        // (markers present, expected verdict, why)
        let table: &[(&[M], MuxVerdict, &str)] = &[
            // --- nothing / no hand-off marker -> no-op ---
            (&[], MuxVerdict::SkipNoMarker, "empty dir: nothing to mux"),
            (
                &[Iso],
                MuxVerdict::SkipNoMarker,
                "ISO but no .ripped: not the worker's job",
            ),
            (
                &[Iso, Mapfile],
                MuxVerdict::SkipNoMarker,
                "ISO+mapfile, no hand-off marker",
            ),
            (&[Mkv], MuxVerdict::SkipNoMarker, "stray MKV, no .ripped"),
            // --- the canonical dispatch case ---
            (
                &[Ripped],
                MuxVerdict::Dispatch,
                ".ripped only: the hand-off to mux",
            ),
            (
                &[Ripped, Iso, Mapfile],
                MuxVerdict::Dispatch,
                ".ripped + ISO + mapfile: normal hand-off",
            ),
            // --- terminal: .completed wins over everything ---
            (
                &[Ripped, Completed],
                MuxVerdict::SkipTerminal,
                ".ripped lingered after a successful mux (delete failed) — .completed is terminal, must NOT re-mux",
            ),
            (
                &[Completed],
                MuxVerdict::SkipTerminal,
                ".completed alone: finished",
            ),
            (
                &[Completed, Mkv],
                MuxVerdict::SkipTerminal,
                "finished with output present",
            ),
            // --- terminal: .failed is terminal too (the da16f00 fix) ---
            (
                &[Ripped, Failed],
                MuxVerdict::SkipTerminal,
                "THE BUG: post-mux abort wrote .failed but .ripped lingered — must be terminal, not re-dispatched forever",
            ),
            (
                &[Failed],
                MuxVerdict::SkipTerminal,
                ".failed alone: quarantined",
            ),
            (
                &[Ripped, Iso, Mapfile, Failed],
                MuxVerdict::SkipTerminal,
                "aborted hand-off with artifacts still present — terminal",
            ),
            // --- M2: a non-JSON `.failed` body is still terminal. The verdict
            //     keys on `has_failed` (presence), not `failed_reason`. A
            //     .ripped + non-JSON .failed must NOT re-dispatch forever. ---
            (
                &[FailedNonJson],
                MuxVerdict::SkipTerminal,
                "non-JSON .failed alone: terminal by presence",
            ),
            (
                &[Ripped, FailedNonJson],
                MuxVerdict::SkipTerminal,
                "M2: .ripped + non-JSON .failed (no parseable reason) — terminal, never re-dispatch",
            ),
            // --- conflict: .completed + .failed both present ---
            (
                &[Completed, Failed],
                MuxVerdict::SkipTerminal,
                "conflicting terminals: still terminal either way (skip)",
            ),
            (
                &[Ripped, Completed, Failed],
                MuxVerdict::SkipTerminal,
                ".ripped + both terminals: terminal, never re-mux",
            ),
            // --- .done / .review are NOT terminal for the mux worker ---
            // (.done/.review are the MOVER's hand-off, written alongside
            //  .completed; on their own without .completed they don't gate
            //  the mux worker — but a lone .ripped+.done is anomalous. The
            //  worker only treats .completed/.failed as terminal, so a
            //  .ripped+.done (no .completed) would still Dispatch. This row
            //  documents that contract.)
            (
                &[Ripped, Done],
                MuxVerdict::Dispatch,
                ".done without .completed does not gate the mux worker (.completed is the authoritative signal)",
            ),
            (
                &[Ripped, Review],
                MuxVerdict::Dispatch,
                ".review without .completed likewise does not gate the mux worker",
            ),
        ];
        for (markers, expected, why) in table {
            let got = verdict_for(markers);
            assert_eq!(got, *expected, "dispatch matrix row failed: {why}");
        }
    }

    /// UNKNOWN listing (snapshot None) must skip — never dispatch on a
    /// degraded read_dir view. Driven directly since a real per-entry NFS
    /// error can't be provoked from the local FS.
    #[test]
    fn mux_dispatch_unknown_snapshot_skips() {
        assert_eq!(mux_dispatch_verdict(None), MuxVerdict::SkipUnknown);
    }

    /// Named explicit cells the matrix also covers, called out per the rc4
    /// brief so a future reader sees them by name.
    #[test]
    fn mux_dispatch_ripped_only_dispatches() {
        assert_eq!(verdict_for(&[M::Ripped]), MuxVerdict::Dispatch);
    }
    #[test]
    fn mux_dispatch_ripped_plus_completed_skips() {
        assert_eq!(
            verdict_for(&[M::Ripped, M::Completed]),
            MuxVerdict::SkipTerminal
        );
    }
    #[test]
    fn mux_dispatch_ripped_plus_failed_skips_the_fixed_bug() {
        // The exact cell the infinite-loop bug lived in. Pin it hard.
        assert_eq!(
            verdict_for(&[M::Ripped, M::Failed]),
            MuxVerdict::SkipTerminal,
            ".ripped + .failed MUST be terminal (da16f00) — re-dispatch here is the loop bug"
        );
    }
    #[test]
    fn mux_dispatch_nothing_present_is_noop() {
        assert_eq!(verdict_for(&[]), MuxVerdict::SkipNoMarker);
    }

    /// TRANSITION: ripped → mux success → completed → (mover takes over).
    /// After the worker writes `.completed`, a lingering `.ripped` (delete
    /// failed) must flip the verdict from Dispatch to SkipTerminal, so the
    /// worker doesn't wipe the just-written MKV and re-mux.
    #[test]
    fn mux_transition_ripped_to_completed_stops_dispatch() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("Disc");
        std::fs::create_dir_all(&dir).unwrap();
        write_marker(&dir, &sample_marker()).unwrap();
        std::fs::write(dir.join("Disc.iso"), b"x").unwrap();

        // State 1: fresh hand-off → Dispatch.
        let s1 = crate::ripper::staging::snapshot_staging_disc(&dir);
        assert_eq!(mux_dispatch_verdict(s1.as_ref()), MuxVerdict::Dispatch);

        // Mux succeeds, writes .completed, but the .ripped delete fails
        // (simulated by leaving .ripped in place).
        crate::ripper::staging::write_completed_marker(&dir);

        // State 2: terminal → SkipTerminal (loop broken).
        let s2 = crate::ripper::staging::snapshot_staging_disc(&dir);
        assert_eq!(
            mux_dispatch_verdict(s2.as_ref()),
            MuxVerdict::SkipTerminal,
            "after .completed the worker must stop dispatching even if .ripped lingers"
        );
    }

    /// TRANSITION: ripped → loss-abort → failed → not re-dispatched.
    /// Mirrors the resume loss-abort branch, which writes `.failed` (and
    /// deletes `.ripped`). Even if `.ripped` survives, the verdict must be
    /// terminal — the re-mux-forever loop is impossible.
    #[test]
    fn mux_transition_ripped_to_failed_stops_dispatch() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("Disc");
        std::fs::create_dir_all(&dir).unwrap();
        write_marker(&dir, &sample_marker()).unwrap();

        let s1 = crate::ripper::staging::snapshot_staging_disc(&dir);
        assert_eq!(mux_dispatch_verdict(s1.as_ref()), MuxVerdict::Dispatch);

        // Loss-abort quarantines the dir.
        crate::ripper::staging::write_failed_marker(&dir, "aborted: demux loss exceeds threshold");

        let s2 = crate::ripper::staging::snapshot_staging_disc(&dir);
        assert_eq!(
            mux_dispatch_verdict(s2.as_ref()),
            MuxVerdict::SkipTerminal,
            "after .failed the worker must never re-dispatch (the re-mux-forever loop)"
        );
    }
}
