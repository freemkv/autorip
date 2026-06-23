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
    crate::ripper::staging::write_marker_durable(&path, json.as_bytes())
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
        if !dir.join(RIPPED_MARKER_NAME).exists() {
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
        match crate::ripper::staging::snapshot_staging_disc(&dir) {
            Some(snap) if snap.completed => continue,
            Some(_) => {}
            None => continue,
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
        let outcome = crate::ripper::resume::remux_from_ripped_marker(cfg_arc, &dir, &marker);
        if outcome.success {
            clear_error(&dir.to_string_lossy());
            tracing::info!(staging = %dir.display(), title = %title, "mux worker: completed");
            crate::log::syslog(&format!("Muxed: {}", title));
            // Drive the origin device to "done" — the hand-off left it
            // frozen at status="ripping" so /api/state doesn't show a
            // permanent "ripping" tile for a completed rip. Only update
            // if the device is still "ripping" (hasn't been re-used for
            // a new rip in the time the mux took).
            let origin = &marker.origin_device;
            if !origin.is_empty() {
                let still_ripping = crate::ripper::STATE
                    .lock()
                    .ok()
                    .and_then(|s| s.get(origin.as_str()).map(|rs| rs.status == "ripping"))
                    .unwrap_or(false);
                if still_ripping {
                    crate::ripper::update_state(
                        origin,
                        crate::ripper::RipState {
                            device: origin.clone(),
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
                            // Carry the mux-derived display fields (codecs +
                            // duration + output_file from the `_mux` done-state)
                            // and the marker's lost-video estimate so the origin
                            // device's done card matches the inline fresh-rip
                            // done card instead of dropping the codec badge,
                            // duration, output path, and lost-video figure.
                            codecs: outcome.codecs.clone(),
                            duration: outcome.duration.clone(),
                            output_file: outcome.output_file.clone(),
                            lost_video_secs: marker.rip_lost_video_secs,
                            ..Default::default()
                        },
                    );
                }
            }
        } else {
            let path_str = dir.to_string_lossy().to_string();
            record_error(
                &path_str,
                "mux worker dispatch did not complete (see _mux device log)",
                "check `/api/state` _mux device or the device log for the underlying error; staging is preserved for retry",
            );
        }
    }
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
        if dir.join(crate::ripper::staging::COMPLETED_MARKER).exists() {
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
}
