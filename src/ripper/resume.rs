//! Auto-resume from a staged ISO (Minimal scope, 0.20.8).
//!
//! Companion to `staging::resume_or_quarantine_staging`. The staging
//! pass classifies what's left in `<staging_dir>/<disc>/` after a
//! container restart and preserves partial state. **This** module
//! decides what to do with the preserved state: if Pass 1 finished
//! cleanly to ISO + mapfile but mux never wrote the final MKV, we can
//! skip every disc-side operation and just re-mux from the ISO.
//!
//! The classifier (`classify_resume`) is pure: takes a hint + the
//! configured `abort_on_lost_secs`, inspects the staging dir, returns
//! a verdict. The actor (`resume_remux`) does the side effects:
//! delete partial MKV, `Disc::scan_image` the ISO, `mux::run_mux`,
//! write `.completed` + clear `.restart_count` on success.
//!
//! Counter-clearing semantics: the counter is cleared **only** on
//! successful remux. Failure of `Disc::scan_image`, `mux::run_mux`,
//! or any helper leaves the counter intact so the next-startup pass
//! through `resume_or_quarantine_staging` will bump it. After
//! `RESTART_LIMIT` consecutive failures the partial state is
//! promoted to `.failed` and the loop ends.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64};
use std::sync::{Arc, RwLock};

use crate::config::Config;

use super::staging::{self, ResumeAction, StagingResumeHint};

/// Fallback title bitrate (bytes/sec) used to convert bad-byte counts
/// into estimated lost title-seconds when the real per-title bitrate is
/// unknown (≈ 8.25 Mbps). Shared by `classify_resume`'s pre-flight
/// estimate and `resume_remux`'s post-`scan_image` re-validation so the
/// two cannot silently diverge. The authoritative title-scoped re-check
/// in `resume_remux` (real per-title bitrate + `bytes_bad_in_title`) is
/// what ultimately gates the mux. Single source of truth so the value
/// can't drift across call sites.
pub(super) const FALLBACK_BITRATE_BYTES_PER_SEC: f64 = 8_250_000.0;

/// Sentinel device path passed to `detect_max_batch_sectors` from the
/// resume-remux path. There is no live drive here — we mux from a staged
/// ISO — so we deliberately probe a non-optical, non-existent node. The
/// probe finds no SCSI peripheral type for it and falls back to the
/// library's default optical batch size, which is the correct read batch
/// for a file-backed ISO source.
const DEFAULT_BATCH_PROBE_PATH: &str = "/dev/null";

/// Classification of a `ResumePreserved` staging hint. Anything that
/// isn't `ResumePreserved` is mapped here too so the orchestrator can
/// fan out a single `match`.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ResumeClass {
    /// Auto-resume candidate: ISO + mapfile both on disk, mapfile is
    /// `bytes_pending == 0` and the bad bytes that overlap the muxable
    /// title fit inside `abort_on_lost_secs`. Carries the resolved
    /// paths so the actor doesn't have to re-walk the directory.
    Remux {
        iso_path: PathBuf,
        mapfile_path: PathBuf,
        /// Sanitized display name — the staging subdirectory's
        /// `file_name()`. Used for the MKV filename, dest URL, and the
        /// `display_name` in `MuxInputs`. The original TMDB-resolved
        /// title isn't available at resume time (no fresh scan_disc
        /// has run yet); the sanitized form is what every other
        /// downstream path keys on anyway.
        display_name: String,
        /// Operator-confidence carried from the fresh-rip hand-off, when
        /// known. `Some(true)` means the rip side already decided the title
        /// is auto-file-worthy (exact match OR an explicit operator
        /// override); `resume_remux` ORs it into its own match check so an
        /// override whose chosen title differs from the disc's own label
        /// isn't second-guessed into `.review`. `None` on the cold
        /// auto-resume path (no hand-off marker, no override concept) —
        /// `resume_remux` then relies on the match check alone.
        title_confident: Option<bool>,
    },
    /// Hint is `ResumePreserved` but doesn't satisfy the auto-resume
    /// criteria. The orchestrator should fall through to the regular
    /// disc-insertion flow (which may itself reuse the partial state
    /// via libfreemkv's sweep_opts.resume on the next Pass 1).
    NotEligible,
    /// Hint was `AlreadyCompleted` — nothing to do. The mover (if
    /// configured) will pick the staged output up via `.done`.
    AlreadyCompleted,
    /// Hint was `AlreadyFailed` / `RestartLoopFailed` — leave it for
    /// the operator. `reason` is forwarded for surfacing in the UI.
    AlreadyFailed { reason: String },
}

/// Pure classifier. No I/O beyond reading the mapfile (which the
/// orchestrator was going to do at mux time anyway). Returns a
/// verdict that fully describes what should happen next.
///
/// Eligibility for `Remux` requires ALL of:
/// - hint action is `ResumePreserved`
/// - `has_iso && has_mapfile` (the boolean fields the staging snapshot
///   already computed)
/// - mapfile loads cleanly via `Mapfile::load`
/// - mapfile `stats().bytes_pending == 0` — no NonTried / NonTrimmed /
///   NonScraped left, i.e. every sector has a terminal verdict
/// - if the disc has a muxable title (UDF read via `Disc::scan_image`
///   is deferred to the actor for cost reasons; the classifier
///   approximates with the whole-disc `Unreadable` bytes), the bad
///   bytes converted to title-seconds (via the 8.25 Mbps fallback
///   bitrate, same constant `rip_disc` uses) are within
///   `abort_on_lost_secs`.
///
/// The conservative bitrate fallback is intentional: at classification
/// time we don't have a `DiscTitle` to call `bytes_bad_in_title`
/// against. The actor re-validates with the real titles after
/// `scan_image` and aborts the resume if the per-title check fails.
pub fn classify_resume(hint: &StagingResumeHint, abort_on_lost_secs: u64) -> ResumeClass {
    match &hint.action {
        ResumeAction::AlreadyCompleted => return ResumeClass::AlreadyCompleted,
        ResumeAction::AlreadyFailed { reason } => {
            return ResumeClass::AlreadyFailed {
                reason: reason.clone(),
            };
        }
        ResumeAction::RestartLoopFailed { reason } => {
            return ResumeClass::AlreadyFailed {
                reason: reason.clone(),
            };
        }
        // Dir is actively owned/in progress (`.sweeping` sweep running, or
        // `.muxing` mux worker holds it). The startup resume classifier must
        // not claim it — the live worker owns the transition. Treat as
        // NotEligible so this path leaves it alone.
        ResumeAction::InProgress => return ResumeClass::NotEligible,
        ResumeAction::ResumePreserved { .. } => {}
    }
    let ResumeAction::ResumePreserved {
        has_iso,
        has_mapfile,
        ..
    } = &hint.action
    else {
        return ResumeClass::NotEligible;
    };
    if !has_iso || !has_mapfile {
        return ResumeClass::NotEligible;
    }

    // Resolve the ISO + mapfile filenames by walking the dir. The
    // staging-snapshot booleans tell us they exist but not their
    // exact names; the orchestrator names them `<sanitized>.iso` and
    // `<sanitized>.iso.mapfile` but we don't want to depend on the
    // sanitization matching exactly post-restart (it does today, but
    // guard against staging-filename sanitization drift across
    // restarts rather than reconstructing the expected names).
    let (iso_path, mapfile_path) = match find_iso_and_mapfile(&hint.dir) {
        Some(p) => p,
        None => return ResumeClass::NotEligible,
    };

    // Mapfile load. A corrupt mapfile means the post-Pass-1 state is
    // ambiguous — fall back to a full re-rip.
    let map = match libfreemkv::disc::mapfile::Mapfile::load(&mapfile_path) {
        Ok(m) => m,
        Err(e) => {
            // Don't swallow this: a corrupt/unreadable mapfile silently
            // demotes resume to a full re-rip, which looks like the
            // resume logic "just didn't fire". Make it observable.
            tracing::warn!(
                mapfile = %mapfile_path.display(),
                error = %e,
                "resume: mapfile load failed; classifying as not-eligible (full re-rip)"
            );
            return ResumeClass::NotEligible;
        }
    };
    // Log the persisted AACS Volume ID (if Pass 1 recorded one) — the resume
    // key resolution reads it back from the mapfile below.
    if let Some(vid) = map.vid() {
        let hex: String = vid.iter().map(|b| format!("{b:02x}")).collect();
        tracing::info!(vid = %hex, mapfile = %mapfile_path.display(), "resume: recovered AACS Volume ID from mapfile");
    }
    let stats = map.stats();

    // ISO-size validation. The `bytes_pending==0` and coverage gates below both
    // trust the mapfile's `bytes_total`. If that total is short of the real
    // disc, NonTried sectors past it are invisible to those checks. We can't
    // stat the disc here, but we CAN stat the on-disk ISO: a settled Pass-1 ISO
    // must be at least as large as the mapfile claims. If it's short, the ISO is
    // truncated/incomplete — reject and re-sweep fresh.
    match std::fs::metadata(&iso_path) {
        Ok(meta) if meta.len() < stats.bytes_total => {
            tracing::warn!(
                iso = %iso_path.display(),
                iso_len = meta.len(),
                bytes_total = stats.bytes_total,
                "resume: ISO is shorter than mapfile total_size; classifying as not-eligible (fresh sweep)"
            );
            return ResumeClass::NotEligible;
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(
                iso = %iso_path.display(),
                error = %e,
                "resume: cannot stat ISO; classifying as not-eligible (fresh sweep)"
            );
            return ResumeClass::NotEligible;
        }
    }

    if stats.bytes_pending != 0 {
        // Pass 1 didn't fully settle the disc (some sectors still
        // NonTried / NonTrimmed / NonScraped) — let the regular rip
        // path resume sweep + retry instead of jumping to mux.
        return ResumeClass::NotEligible;
    }

    // Coverage note (no live check here — intentionally): one might want to
    // verify the mapfile's entries span its whole `bytes_total`. That check is
    // DEAD: `Mapfile::load` partitions exactly [0, total_size) — leading and
    // internal gaps are backfilled with synthetic NonTried entries and any
    // trailing gap extends `bytes_total` — so after load,
    // `bytes_good + bytes_unreadable + bytes_pending == bytes_total` is an
    // identity, never a strict-less. (See `Mapfile::load`'s gap backfill.)
    //
    // The real protection against a SHORT mapfile (one truncated below the true
    // disc capacity) is not in this function: `resume_remux` re-scans the
    // actual on-disk ISO — which the sweep `set_len`s to full disc capacity
    // regardless of the mapfile — and the ISO content (not the mapfile's
    // bytes_total) drives the mux. A mapfile short of the real disc therefore
    // can't cause silent tail loss at mux time. Plus the ISO-size guard above
    // rejects an ISO shorter than the mapfile claims. So there is no gate to
    // add here; this comment replaces a dead `bytes_accounted < bytes_total`
    // identity check that gave a false sense of protection.

    // Bad-bytes pre-filter. Two cases:
    //
    // abort_on_lost_secs == 0 ("perfect rip required"): using whole-disc
    // bad bytes as the gate is too strict. A disc whose unreadable sectors
    // are entirely OUTSIDE the main title is still a valid mux candidate —
    // the authoritative per-title re-validation in `resume_remux` (which
    // runs after `Disc::scan_image` and calls `bytes_bad_in_title`) will
    // correctly allow it. Blocking here on whole-disc damage means that
    // disc never reaches the title-scoped check. ALLOW and defer.
    //
    // abort_on_lost_secs > 0: apply the coarse whole-disc estimate as a
    // cheap early-reject (avoids `scan_image` for heavily damaged discs).
    // This is still a pre-flight estimate; the actor re-validates with the
    // real per-title bitrate after `Disc::scan_image`.
    if abort_on_lost_secs > 0 {
        let bad_bytes = stats.bytes_unreadable;
        let lost_secs = bad_bytes as f64 / FALLBACK_BITRATE_BYTES_PER_SEC;
        if lost_secs > abort_on_lost_secs as f64 {
            return ResumeClass::NotEligible;
        }
    }

    // file_name() returns None only for a path ending in `..`. Defaulting to
    // "" here would make the output filename ".mkv"/".m2ts" and point
    // delete_partial_output at "<staging>/.mkv" at the staging root — a
    // destructive write outside the disc subdir. hint.dir is always a child
    // of staging_dir today so this never fires, but bail loudly if it ever does.
    let display_name = match hint.dir.file_name() {
        Some(n) => n.to_string_lossy().into_owned(),
        None => {
            tracing::warn!(dir = %hint.dir.display(), "resume: staging dir has no file_name component; not eligible");
            return ResumeClass::NotEligible;
        }
    };

    ResumeClass::Remux {
        iso_path,
        mapfile_path,
        display_name,
        // Cold auto-resume from preserved staging: no `.ripped` hand-off
        // and no operator-override concept here, so confidence is unknown.
        // resume_remux falls back to its own match check.
        title_confident: None,
    }
}

/// Walk a staging dir and find the unique `.iso` plus its matching
/// mapfile (`<iso>.mapfile`, i.e. `foo.iso.mapfile` for `foo.iso`).
/// Returns None if there is no ISO, more than one ISO (ambiguous), or
/// no mapfile keyed to that exact ISO name.
///
/// `read_dir` order is unspecified, so we cannot rely on last-wins
/// pairing: a stale/extra `.iso` or an unrelated `.mapfile` left in the
/// dir would otherwise pair `foo.iso` with `bar.mapfile`, corrupting the
/// loss accounting and reading the wrong Volume ID. We therefore pin the
/// mapfile to the chosen ISO's name and bail on any ISO ambiguity.
pub(super) fn find_iso_and_mapfile(dir: &Path) -> Option<(PathBuf, PathBuf)> {
    let mut isos: Vec<PathBuf> = Vec::new();
    let mut mapfiles: Vec<PathBuf> = Vec::new();
    let read_dir_iter = match std::fs::read_dir(dir) {
        Ok(iter) => iter,
        Err(e) => {
            tracing::warn!(
                dir = %dir.display(),
                error = %e,
                "resume: read_dir failed (NFS ESTALE or missing dir?) — \
                 staging contents unknown, not resuming from this dir"
            );
            return None;
        }
    };
    for entry in read_dir_iter {
        // Don't `.flatten()` away per-entry errors: a partial NFS
        // degradation can error on individual DirEntry I/O while the dir
        // is genuinely populated. Silently dropping such an entry could
        // hide the ISO or its mapfile, making this return None and the
        // orchestrator re-sweep a disc that was already ripped. We can't
        // trust a partial listing to pick the unique ISO / pin its
        // mapfile, so bail loudly rather than guess. Mirrors the
        // per-entry defense in `snapshot_staging_disc` (staging.rs).
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    dir = %dir.display(),
                    error = %e,
                    "resume: read_dir entry errored (partial NFS degradation?) — \
                     staging contents unknown, not resuming from this dir"
                );
                return None;
            }
        };
        let p = entry.path();
        let name = match p.file_name() {
            Some(n) => n.to_string_lossy().into_owned(),
            None => continue,
        };
        if name.ends_with(".mapfile") {
            mapfiles.push(p);
        } else if name.ends_with(".iso") {
            isos.push(p);
        }
    }
    // Exactly one ISO, or we can't say which staging artefact is the
    // real one — refuse to guess.
    if isos.len() != 1 {
        if isos.len() > 1 {
            tracing::warn!(
                dir = %dir.display(),
                count = isos.len(),
                "resume: multiple .iso files in staging dir — ambiguous, not resuming"
            );
        }
        return None;
    }
    let iso = isos.into_iter().next()?;
    let iso_name = iso.file_name()?.to_string_lossy().into_owned();
    // Canonical mapfile is `<iso-name>.mapfile` (the orchestrator names
    // it `<sanitized>.iso.mapfile`). Match exactly on that.
    let want = format!("{}.mapfile", iso_name);
    let mapfile = mapfiles.into_iter().find(|m| {
        m.file_name()
            .map(|n| n.to_string_lossy() == want.as_str())
            .unwrap_or(false)
    })?;
    Some((iso, mapfile))
}

/// Delete the partial MKV (or `.m2ts`) at `<dir>/<sanitized>.<ext>`.
/// Best-effort: missing file is success, any other error is logged
/// and ignored — the mux step will overwrite whatever's there.
///
/// Extracted as a free function so the unit test can exercise it
/// without touching `Disc::scan_image` or `run_mux`.
pub fn delete_partial_output(staging_disc_dir: &Path, sanitized_name: &str) {
    for ext in ["mkv", "m2ts"] {
        let p = staging_disc_dir.join(format!("{}.{}", sanitized_name, ext));
        match std::fs::remove_file(&p) {
            Ok(_) => tracing::info!(path = %p.display(), "removed partial mux output for resume"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                tracing::warn!(path = %p.display(), error = %e, "could not delete partial mux output (continuing)")
            }
        }
    }
}

/// Reset the device to a terminal idle/error UI state at any early-return
/// site inside [`resume_remux`], whether or not `status="ripping"` was
/// ever set. Preserves disc identity (so the dashboard tile keeps its
/// title / format / duration) and zeroes everything else via
/// `Default::default()`.
///
/// Several callers (the config-poison, scan, and key-resolution early
/// returns) run BEFORE the `status="ripping"` update, but the reset is
/// still correct there — it just writes the terminal state directly.
/// When it runs after "ripping" was set, it un-sticks the API state so
/// the "already ripping" gate in `web.rs::handle_rip` doesn't reject all
/// subsequent /api/rip requests. Mirrors `rip_disc`'s stopped → "idle"
/// pattern.
fn reset_status_after_ripping(
    device: &str,
    terminal_status: &str,
    display_name: &str,
    disc_format: &str,
    duration: &str,
    last_error: Option<String>,
) {
    let err = last_error.unwrap_or_default();
    super::update_state(
        device,
        super::RipState {
            device: device.to_string(),
            status: terminal_status.to_string(),
            disc_present: true,
            disc_name: display_name.to_string(),
            disc_format: disc_format.to_string(),
            duration: duration.to_string(),
            last_error: err,
            ..Default::default()
        },
    );
}

/// Callers and what they actually provide:
/// - `handle_rip_request` (real device) passes a `ResumeClass::Remux`
///   from `find_resumable_for_disc` and the spawn site has already
///   registered the per-device `Halt` token (same as `rip_disc`).
/// - `remux_from_ripped_marker` (the `_mux` worker path) passes a
///   freshly-built `ResumeClass::Remux` but does NOT register a `Halt`
///   token for the `_mux` pseudo-device.
///
/// So neither precondition is relied upon here: a non-`Remux`
/// classification is handled by the early-return below (logged as a
/// caller bug rather than assumed away), and the halt-token lookups in
/// the mux loop tolerate an absent token (the `_mux` path).
///
/// On success: writes `.completed` + clears `.restart_count`. On any
/// failure (scan_image, mux open, mux loop): preserves the partial
/// state and leaves the counter intact so the next-startup pass
/// promotes the dir to `.failed` once `RESTART_LIMIT` is reached.
/// Pick the codecs string for the resumed-rip done card. The mux frame
/// loop writes the real codecs into STATE during muxing (the `_mux`
/// worker path starts from an empty seed), so prefer the post-mux STATE
/// value; fall back to the pre-mux snapshot when STATE has nothing useful.
fn resolve_done_codecs(post_mux_state: Option<String>, pre_mux_snapshot: String) -> String {
    post_mux_state
        .filter(|c| !c.is_empty())
        .unwrap_or(pre_mux_snapshot)
}

/// Resolve the `media_type` written into the resume `.done`/`.review` marker.
/// The mover routes by this field (movie library vs TV library) and defaults a
/// missing/empty value to "movie"; we resolve the same default here so a cold
/// auto-resume — where STATE is empty and no media_type was carried — writes an
/// explicit value rather than relying on the reader's fallback. A carried
/// "movie"/"tv" (warm `_mux` resume, seeded from the `.ripped` hand-off) passes
/// through unchanged, fixing the prior bug where TV resumes were filed as movies.
fn resolve_media_type(carried: &str) -> String {
    if carried.is_empty() {
        "movie".to_string()
    } else {
        carried.to_string()
    }
}

/// Handle a durability-gate (`fsync`) failure on the resume mux output.
///
/// Both the ISO-output and the MKV/M2TS-output success paths call
/// `staging::fsync_output_file` before writing any success marker; a `false`
/// return means the output is not provably durable and we must NOT hand it to
/// the mover. The naive response — preserve staging, return — is correct on the
/// startup-scan path (which bumps `.restart_count` via
/// `resume_or_quarantine_staging` on the NEXT restart). But `resume_remux` is
/// ALSO driven by the live `_mux` worker loop (`check_and_mux`), which leaves
/// `.ripped` in place and re-dispatches the SAME dir on its next tick — so a
/// deterministic fsync failure (e.g. a wedged NFS export) would re-mux + re-fsync
/// the same possibly-corrupt output forever, never consulting any restart cap.
///
/// This caps that loop the same way `resume_or_quarantine_staging` caps its
/// partial-state path: bump `.restart_count`, and once it reaches
/// `RESTART_LIMIT`, promote the dir to terminal `.failed` (which the worker's
/// `mux_dispatch_verdict` and `resumable_dir_blocked` both treat as terminal,
/// stopping the re-dispatch) and drop the `.ripped` hand-off so it can't be
/// re-queued. Below the limit it leaves staging intact for the next retry.
///
/// Returns `true` when the dir was promoted to `.failed` (terminal), `false`
/// when staging was preserved for another attempt. The caller resets device
/// status and returns either way.
fn handle_resume_fsync_failure(device: &str, staging_dir: &Path, output_desc: &str) -> bool {
    let count = staging::increment_restart_count(staging_dir).unwrap_or_else(|e| {
        // A failed counter bump must not green-light an infinite loop, but it
        // also can't know the true count — log and treat as below-limit so the
        // next tick re-reads/re-bumps from disk rather than quarantining blindly.
        tracing::warn!(
            staging = %staging_dir.display(),
            error = %e,
            "resume: failed to bump .restart_count after fsync failure"
        );
        0
    });
    if count >= staging::RESTART_LIMIT {
        let reason =
            format!("{output_desc} fsync failed repeatedly ({count} attempts); giving up",);
        crate::log::device_log(
            device,
            &format!("Auto-resume: {reason} — quarantining staging (.failed)."),
        );
        staging::write_failed_marker(staging_dir, &reason);
        staging::clear_restart_count(staging_dir);
        // Drop the `.ripped` hand-off so the mux worker can't re-queue this
        // now-terminal dir (belt-and-suspenders with the `.failed` guard).
        if let Err(e) = crate::muxer::delete_marker(staging_dir) {
            tracing::warn!(
                staging = %staging_dir.display(),
                error = %e,
                "resume: failed to delete .ripped after fsync-failure quarantine; .failed guard prevents re-mux"
            );
        }
        true
    } else {
        false
    }
}

/// RAII exclusion lock for the cold operator-resume mux path.
///
/// The `_mux` worker path (`muxer::check_and_mux`) already writes `.muxing`
/// and holds its own `MuxingGuard` for the duration of the dispatch, so it
/// must NOT have a second guard write/clear the same marker underneath it.
/// Every OTHER caller of [`resume_remux`] — the cold operator-resume path
/// (`ResumeMode::Require` → `find_resumable_for_disc` → `resume_remux`) — runs
/// a multi-minute mux with only `<name>.iso` + `<name>.iso.mapfile` on disk and
/// NO governing marker. Without `.muxing`, `disc_owned_by_worker` /
/// `resumable_dir_blocked` return false, so a concurrent `ResumeMode::Wipe` of
/// the same disc `remove_dir_all`s the staging dir and deletes the ISO out from
/// under this in-flight mux (the exact data loss the Wipe guard at
/// `mod.rs` was added to prevent), and a second cold resume double-muxes the
/// same ISO. Writing `.muxing` here closes both holes; the terminal marker
/// writers (`write_completed_marker` / `write_failed_marker`) already clear it,
/// and this guard's `Drop` clears it on every early-return / panic path too.
struct ResumeMuxingGuard<'a> {
    dir: &'a Path,
    /// True when the synthetic `_mux` worker device already owns the lock — we
    /// then neither write nor clear it, leaving the worker's `MuxingGuard` in
    /// sole charge.
    worker_owned: bool,
}

impl<'a> ResumeMuxingGuard<'a> {
    /// Write `.muxing` (unless the `_mux` worker already holds it) and return a
    /// guard that clears it on drop.
    fn acquire(device: &str, dir: &'a Path) -> Self {
        let worker_owned = device == "_mux";
        if !worker_owned {
            staging::write_muxing_marker(dir);
        }
        Self { dir, worker_owned }
    }
}

impl Drop for ResumeMuxingGuard<'_> {
    fn drop(&mut self) {
        if !self.worker_owned {
            staging::clear_muxing_marker(self.dir);
        }
    }
}

pub fn resume_remux(cfg: &Arc<RwLock<Config>>, device: &str, classification: ResumeClass) {
    let ResumeClass::Remux {
        iso_path,
        mapfile_path,
        display_name,
        title_confident: carried_confident,
    } = classification
    else {
        // Caller bug — should never happen. Log loudly so a future
        // refactor catches it.
        tracing::error!(
            device = %device,
            "resume_remux called with non-Remux classification"
        );
        return;
    };

    // Archive the prior session's per-device log so the live log shows
    // only this resumed-mux operation. Mirrors what scan_disc and
    // rip_disc do on entry. On the common "scan then resume" path
    // (session_is_scanned=true) scan_disc is skipped entirely, so
    // without this the resumed-mux entries would interleave with the
    // prior scan's log, making errors hard to correlate.
    crate::log::archive_device_log(device);

    let cfg_read = match cfg.read() {
        Ok(c) => c.clone(),
        Err(_) => {
            // status="ripping" is not set yet here so there is no stuck
            // gate, but leave a trace so the silently-vanished resume is
            // diagnosable instead of disappearing with zero explanation.
            crate::log::device_log(device, "Auto-resume aborted: config lock poisoned");
            return;
        }
    };

    let staging_dir = iso_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from(&cfg_read.staging_dir));

    crate::log::device_log(
        device,
        &format!(
            "Auto-resume: re-muxing ISO from staging ({})",
            staging_dir.display()
        ),
    );

    // 1. Delete the partial MKV/m2ts if present.
    delete_partial_output(&staging_dir, &display_name);

    // Acquire the `.muxing` exclusion lock for the duration of this mux. On the
    // cold operator-resume path the staging dir otherwise carries only the ISO
    // + mapfile and NO governing marker, so a concurrent `ResumeMode::Wipe` (or
    // a second cold resume) of the same disc would delete/double-mux the ISO
    // out from under this in-flight mux. With `.muxing` present,
    // `disc_owned_by_worker` / `resumable_dir_blocked` correctly block both.
    // Skips the `_mux` worker device, which already holds the lock via
    // `check_and_mux`'s `MuxingGuard`. Cleared on every exit (incl. early
    // return / panic) by Drop; the terminal `.completed` / `.failed` writers
    // also clear it.
    let _muxing_guard = ResumeMuxingGuard::acquire(device, &staging_dir);

    // 2. Open the ISO via FileSectorSource.
    let mut iso_reader = match libfreemkv::FileSectorSource::open(&iso_path) {
        Ok(r) => r,
        Err(e) => {
            crate::log::device_log(
                device,
                &format!(
                    "Auto-resume aborted: cannot open ISO {}: {}",
                    iso_path.display(),
                    e
                ),
            );
            // Don't clear counter — fall through to next-restart's
            // resume_or_quarantine_staging which will bump it.
            return;
        }
    };

    // 3. Disc::scan_image to recover Disc + titles. A sample-based key source
    //    scans structure-only first (the title extents are needed to read the
    //    on-disc samples), then resolves a key and re-scans with it (see
    //    `resolve_keys_from_iso`). A local source resolves keys here.
    // ISO scan: keyless, no handshake (so no drive credentials). Keys are
    // resolved afterward in `resolve_keys_from_iso` via the source list.
    let struct_opts = crate::keysource::iso_scan_opts();
    use libfreemkv::SectorSource;
    let capacity = iso_reader.capacity_sectors();
    let disc = match libfreemkv::Disc::scan_image(&mut iso_reader, capacity, &struct_opts) {
        Ok(d) => d,
        Err(e) => {
            crate::log::device_log(
                device,
                &format!("Auto-resume aborted: scan_image failed: {}", e),
            );
            // The live-device dispatch (`handle_rip_request` → `scan_disc`)
            // already moved this device to status="scanning". Bailing here
            // without resetting wedges the "already ripping" gate in
            // `web.rs::handle_rip` so no further /api/rip is accepted. Reset
            // to idle so the operator can retry. (For the `_mux` worker
            // device this is a harmless no-op — nothing gates on it.)
            super::update_state(
                device,
                super::RipState {
                    device: device.to_string(),
                    status: "idle".to_string(),
                    ..Default::default()
                },
            );
            return;
        }
    };

    // Defensive title check — `scan_image` succeeds for any UDF disc
    // but a truncated ISO can still yield zero-duration titles.
    let title_ok = disc
        .titles
        .first()
        .map(|t| t.duration_secs > 0.0)
        .unwrap_or(false);
    if !title_ok {
        crate::log::device_log(
            device,
            "Auto-resume aborted: scan_image produced no usable title",
        );
        // Same wedge as the scan_image failure above: reset scanning → idle
        // so the "already ripping" gate doesn't reject every later /api/rip.
        super::update_state(
            device,
            super::RipState {
                device: device.to_string(),
                status: "idle".to_string(),
                ..Default::default()
            },
        );
        return;
    }

    // Sample-based key source: read the disc's files + Volume ID (from the
    // mapfile) + on-disc samples (from the ISO), resolve a Unit Key, and re-scan
    // with it so decryption keys populate. No-op for a local source.
    let (disc, _key_outcome) = resolve_keys_from_iso(&cfg_read, &iso_path, &mapfile_path, disc);

    // Real-bitrate re-validation: now that we have the actual title,
    // recompute bytes-bad-in-title (vs the classifier's whole-disc
    // estimate) and re-check against abort_on_lost_secs.
    //
    // Re-check `.first()` on the post-key `disc` binding rather than
    // indexing `[0]`: `resolve_keys_from_iso` rebinds `disc` (line
    // above), and although today it preserves titles, a future change
    // that re-scans could leave it empty — index-into-empty would panic
    // the rip thread. The earlier `title_ok` guard ran on the *pre-key*
    // binding, so it doesn't cover this one.
    let title = match disc.titles.first() {
        Some(t) => t.clone(),
        None => {
            crate::log::device_log(device, "Auto-resume aborted: no title after key resolution");
            reset_status_after_ripping(
                device,
                "idle",
                &display_name,
                "",
                "",
                Some("no title after key resolution".to_string()),
            );
            return;
        }
    };
    let title_bytes_per_sec: f64 = {
        let b = title.size_bytes as f64;
        let d = title.duration_secs;
        if b > 0.0 && d > 0.0 {
            b / d
        } else {
            FALLBACK_BITRATE_BYTES_PER_SEC
        }
    };
    // Compute disc_format + duration up front so the abort/early-return
    // paths below can surface them in the UI state (they were previously
    // only available after the mux-build block).
    let disc_format = match disc.format {
        libfreemkv::DiscFormat::Uhd => "uhd",
        libfreemkv::DiscFormat::BluRay => "bluray",
        libfreemkv::DiscFormat::Dvd => "dvd",
        libfreemkv::DiscFormat::Unknown => "unknown",
    }
    .to_string();
    let duration = crate::util::format_duration_hm(title.duration_secs);
    let map = match libfreemkv::disc::mapfile::Mapfile::load(&mapfile_path) {
        Ok(m) => m,
        Err(e) => {
            // The classifier already loaded this mapfile cleanly; a
            // failure here is a TOCTOU (file removed/corrupted/IO error
            // between classify and act). We must NOT skip the per-title
            // loss guard and mux blind — abort the resume and let the
            // next pass re-classify against fresh state.
            crate::log::device_log(
                device,
                &format!(
                    "Auto-resume aborted: mapfile reload failed for loss re-validation: {}",
                    e
                ),
            );
            reset_status_after_ripping(
                device,
                "idle",
                &display_name,
                &disc_format,
                &duration,
                Some(format!("mapfile reload failed: {}", e)),
            );
            return;
        }
    };
    {
        use libfreemkv::disc::mapfile::SectorStatus;
        let bad_ranges = map.ranges_with(&[SectorStatus::Unreadable]);
        // Scope the loss exactly as the fresh-rip post-retry abort gate does
        // (`abort_lost_ms` in mod.rs): for `output_format == "iso"` every
        // sector matters, so loss is the whole-disc bad-byte sum; for a real
        // MKV/M2TS mux only in-title damage counts. Computing in-title loss
        // unconditionally here would make resume ACCEPT a disc with
        // `output_format=iso` + `abort_on_lost_secs=0` and unreadable sectors
        // OUTSIDE the title, while a fresh rip of the same disc + config would
        // ABORT — the two paths must reach the same verdict.
        let lost_secs = super::abort_lost_ms(
            cfg_read.output_format == "iso",
            &title,
            &bad_ranges,
            title_bytes_per_sec,
        ) / 1000.0;
        if super::should_abort_for_loss(
            lost_secs * 1000.0,
            (cfg_read.abort_on_lost_secs * 1000) as f64,
        ) {
            // "disc loss" for raw ISO (whole-disc scope), "title loss" for a
            // muxed MKV/M2TS (in-title scope) — matching how `lost_secs` was
            // computed just above.
            let scope = if cfg_read.output_format == "iso" {
                "disc"
            } else {
                "title"
            };
            crate::log::device_log(
                device,
                &format!(
                    "Auto-resume aborted: {scope} loss {:.2}s exceeds threshold {}s",
                    lost_secs, cfg_read.abort_on_lost_secs
                ),
            );
            reset_status_after_ripping(
                device,
                "idle",
                &display_name,
                &disc_format,
                &duration,
                Some(format!(
                    "{scope} loss {:.2}s exceeds threshold {}s",
                    lost_secs, cfg_read.abort_on_lost_secs
                )),
            );
            return;
        }
    }

    // 4. Build MuxInputs + run mux exactly as rip_disc does.
    // (`disc_format` + `duration` were computed up front, above.)
    let format = disc.content_format;
    let keys = disc.decrypt_keys();
    let batch = libfreemkv::disc::detect_max_batch_sectors(DEFAULT_BATCH_PROBE_PATH);

    // Keyless-capture deferral: the ISO was swept raw (no keys needed),
    // but the MUX needs decryption keys. If this encrypted disc still has
    // no usable keys, DO NOT mux — muxing with `DecryptKeys::None` would
    // write a garbage/encrypted MKV. Return early (without writing the
    // `.completed` marker) so `remux_from_ripped_marker` leaves the
    // `.ripped` marker + ISO + mapfile in staging. The mux worker will
    // re-attempt on its next tick, and once a KEYDB update / keydb refresh
    // provides keys, the same ISO muxes cleanly. This is the deferred-mux
    // half of the no-keys capture flow started in `rip_disc`.
    if disc.encrypted
        && matches!(keys, libfreemkv::decrypt::DecryptKeys::None)
        && !super::output_is_iso_image(&cfg_read.output_format)
    {
        let msg = super::keyless_failure_message(&disc);
        crate::log::device_log(
            device,
            &format!(
                "{msg}\nRipped to ISO — no keys, mux deferred. \
                 Staging preserved; will mux automatically once keys are available."
            ),
        );
        // We have not set status="ripping" yet (that happens via the
        // update_state call further below). reset_status_after_ripping
        // actively writes status="idle" here — a clear non-error
        // terminal state that keeps the disc identity and surfaces the
        // deferral reason without flagging a hard failure.
        reset_status_after_ripping(
            device,
            "idle",
            &display_name,
            &disc_format,
            &duration,
            Some(format!("Ripped to ISO — no keys, mux deferred. {msg}")),
        );
        return;
    }

    let output_format = cfg_read.output_format.clone();
    let ext = match output_format.as_str() {
        "m2ts" => "m2ts",
        _ => "mkv",
    };
    let filename = format!("{}.{}", display_name, ext);
    let staging_str = staging_dir.to_string_lossy().into_owned();
    let output_path = format!("{}/{}", staging_str, filename);
    let dest_url = if output_format == "network" && !cfg_read.network_target.is_empty() {
        format!("network://{}", cfg_read.network_target)
    } else {
        format!("{}://{}", ext, output_path)
    };

    let total_bytes = if disc.capacity_bytes > 0 {
        disc.capacity_bytes
    } else {
        title.size_bytes
    };

    // Halt token: register a fresh one so /api/stop has something to
    // cancel during the mux. Mirrors `rip_disc`'s pattern.
    super::register_halt(device, libfreemkv::Halt::new());
    let halt_token = match super::device_halt(device) {
        Some(h) => h,
        None => {
            // `register_halt` no-ops when the HALTS mutex is poisoned, so
            // device_halt then returns None. The fallback token below was
            // never inserted into HALTS, so /api/stop's lookup can't find
            // it and this resume mux would be uncancellable. Surface it so
            // the degraded stop guarantee is at least visible in the log.
            crate::log::device_log(
                device,
                "Warning: halt registry unavailable (poisoned); this resume mux will not be stoppable via /api/stop",
            );
            libfreemkv::Halt::new()
        }
    };

    super::update_state(
        device,
        super::RipState {
            device: device.to_string(),
            status: "ripping".to_string(),
            disc_present: true,
            disc_name: display_name.clone(),
            disc_format: disc_format.clone(),
            output_file: filename.clone(),
            duration: duration.clone(),
            ..Default::default()
        },
    );

    // Build the DiscStream from the ISO reader (re-open — we consumed
    // `iso_reader` for scan_image; constructing a new one is cheap and
    // gives the mux a clean position-zero handle).
    let iso_reader_for_mux = match libfreemkv::FileSectorSource::open(&iso_path) {
        Ok(r) => r,
        Err(e) => {
            crate::log::device_log(
                device,
                &format!("Auto-resume aborted: cannot re-open ISO for mux: {}", e),
            );
            // Reset from "ripping" (set above) → "error" so the next
            // /api/rip isn't blocked by the "already ripping" gate.
            reset_status_after_ripping(
                device,
                "error",
                &display_name,
                &disc_format,
                &duration,
                Some(format!("ISO re-open failed: {}", e)),
            );
            super::unregister_halt(device);
            return;
        }
    };
    // Compute sweep damage snapshot before `title` is moved into
    // `build_iso_pipeline`. Re-derives all damage fields from the
    // already-loaded mapfile so /api/state shows correct damage during
    // a resumed mux. The mapfile holds the same Unreadable ranges that
    // push_pass_state would have read at sweep end — re-reading them
    // here is equivalent. `errors` mirrors push_pass_state's formula:
    // bytes_unreadable / 2048. `main_lost_ms` uses `bytes_bad_in_title`
    // scoped to the longest title.
    let sweep_damage_for_resume = {
        use libfreemkv::disc::mapfile::SectorStatus;
        let (bad_ranges, num_bad_ranges, bad_ranges_truncated, total_lost_ms, largest_gap_ms) =
            super::state::build_bad_ranges(&map, &title, title_bytes_per_sec);
        let main_title_bad = map.ranges_with(&[SectorStatus::Unreadable]);
        let main_title_bad_bytes = libfreemkv::disc::bytes_bad_in_title(&title, &main_title_bad);
        let main_lost_ms = if title_bytes_per_sec > 0.0 {
            main_title_bad_bytes as f64 * 1000.0 / title_bytes_per_sec
        } else {
            0.0
        };
        let errors = (map.stats().bytes_unreadable / 2048) as u32;
        super::mux::SweepDamageSnapshot {
            errors,
            total_lost_ms,
            main_lost_ms,
            bad_ranges,
            num_bad_ranges,
            bad_ranges_truncated,
            largest_gap_ms,
        }
    };

    // Clone before move into MuxInputs so the done-state update below
    // can carry sweep damage into the terminal RipState.
    let done_sweep_damage = sweep_damage_for_resume.clone();

    let reader: Box<dyn libfreemkv::SectorSource> = Box::new(iso_reader_for_mux);

    // Progress + watchdog atomics shared between the stream-event
    // callback (below), this function's terminal-state updates, and
    // `run_mux`'s `MuxAtomics`. Created before `build_iso_pipeline` so
    // the producer-thread `BytesRead` events can update them in place.
    let latest_bytes_read = Arc::new(AtomicU64::new(0));
    let rip_last_lba = Arc::new(AtomicU64::new(0));
    let rip_current_batch = Arc::new(AtomicU16::new(batch));
    let wd_last_frame = Arc::new(AtomicU64::new(crate::util::epoch_secs()));
    let mux_input_errors = Arc::new(AtomicU32::new(0));

    // Stream-event callback — mirrors `rip_disc`'s multipass highway
    // wiring (mod.rs). The producer thread fires `BytesRead` on every
    // sector read from the ISO; we feed that into `latest_bytes_read`
    // (so the progress bar tracks read-ahead, not write-lagged output)
    // and refresh `wd_last_frame` (so the soft-stall watchdog observes
    // a pre-frame read stall, e.g. a slow NFS seek before the first
    // frame). `BatchSizeChanged` / `SectorSkipped` never fire on the
    // highway (no adaptive retry — the ISO is already zero-filled for
    // any sweep-pass loss), so only `BytesRead` is handled here.
    let wdf_stream = wd_last_frame.clone();
    let lbr_stream = latest_bytes_read.clone();
    let stream_event_fn = move |event: libfreemkv::event::Event| {
        use std::sync::atomic::Ordering;
        wdf_stream.store(crate::util::epoch_secs(), Ordering::Relaxed);
        if let libfreemkv::event::EventKind::BytesRead { bytes, .. } = event.kind {
            lbr_stream.store(bytes, Ordering::Relaxed);
        }
    };

    // Resume path routes through the same `PipelinedPesStream`
    // highway as multipass mux — same 3-stage threaded pipeline,
    // same producer-thread BytesRead events for the UI. No skip-
    // errors plumbing because the on-disk ISO is already clean (any
    // sweep-pass loss got zero-filled in Pass 1).
    let input: Box<dyn libfreemkv::pes::Stream> = match libfreemkv::build_iso_pipeline(
        reader,
        title,
        keys,
        batch,
        format,
        Some(halt_token.clone()),
        Some(Box::new(stream_event_fn) as libfreemkv::sector::prefetched::EventFn),
    ) {
        Ok(s) => Box::new(s),
        Err(e) => {
            tracing::error!(target: "mux", device=%device, "build_iso_pipeline failed: {e}");
            crate::log::device_log(
                device,
                &format!("Auto-resume aborted: mux pipeline build failed: {}", e),
            );
            // Reset from "ripping" (set above) → "error" so the next
            // /api/rip isn't blocked by the "already ripping" gate.
            reset_status_after_ripping(
                device,
                "error",
                &display_name,
                &disc_format,
                &duration,
                Some(format!("Mux pipeline build failed: {}", e)),
            );
            super::unregister_halt(device);
            return;
        }
    };

    // Pick up the TMDB metadata + codecs string that scan_disc
    // populated in STATE before this path was entered. Without this,
    // the mux's per-frame `update_state` would overwrite them with
    // empty strings and the dashboard would lose the poster / title /
    // year / codec badge for the entire mux phase.
    let (tmdb_title, tmdb_year, tmdb_poster, tmdb_overview, tmdb_media_type, state_codecs) =
        super::STATE
            .lock()
            .ok()
            .and_then(|s| s.get(device).cloned())
            .map(|rs| {
                (
                    rs.tmdb_title,
                    rs.tmdb_year,
                    rs.tmdb_poster,
                    rs.tmdb_overview,
                    rs.tmdb_media_type,
                    rs.codecs,
                )
            })
            .unwrap_or_default();
    // The mover routes by `media_type` (movie_dir vs tv_dir) and defaults a
    // missing/empty value to "movie". Resolve the same default here so a cold
    // auto-resume (empty STATE, no carried media_type) is explicit in the
    // marker rather than relying on the reader's fallback.
    let media_type = resolve_media_type(&tmdb_media_type);

    // Title-confidence gate — mirror rip_disc's completion path
    // (mod.rs: `if title_confident { ".done" } else { ".review" }`).
    // Auto-resume previously wrote `.done` unconditionally, auto-filing a
    // resumed rip into the library under a possibly-guessed title and
    // bypassing the operator-review hold the fresh-rip path enforces.
    // Compute confidence the same way: an exact normalized-title match
    // that carries a year, comparing the resolved TMDB title against the
    // disc's own label. On the PRIMARY multipass path (the `.ripped`
    // hand-off), the fresh-rip side already made this verdict — and folded
    // in any operator '✎ change' override — so it carries it in
    // `carried_confident`. We OR that in: an operator's deliberate pick
    // (whose chosen title intentionally differs from the disc's own, often
    // cryptic, label) must NOT be second-guessed back into `.review` by
    // recomputing the match check here. On the cold auto-resume path
    // (`carried_confident == None`) there is no hand-off and no override
    // concept, so confidence is purely the match check.
    let disc_label = disc
        .meta_title
        .as_deref()
        .unwrap_or(&disc.volume_id)
        .to_string();
    let title_for_match = if tmdb_title.is_empty() {
        display_name.clone()
    } else {
        tmdb_title.clone()
    };
    // When TMDB is NOT configured (no API key), there is no metadata source
    // that could ever yield a confident match, so EVERY rip would otherwise
    // land in `.review` and never auto-file. Operators running without a TMDB
    // key expect the disc-label filename, not a review hold they may not know
    // exists. Treat "no API key" as confident: file under the disc label and
    // write `.done` so the mover promotes it. The review hold is preserved
    // ONLY when TMDB IS configured but returns a low-confidence match.
    let tmdb_unconfigured = cfg_read.tmdb_api_key.trim().is_empty();
    let title_confident = tmdb_unconfigured
        || carried_confident.unwrap_or(false)
        || crate::tmdb::is_confident_match(
            &crate::tmdb::clean_title(&disc_label),
            &title_for_match,
            tmdb_year,
        );

    // ISO output: deliver the whole-disc image, don't re-mux a title. Mirrors
    // rip_disc's inline ISO terminal so the two completion routes can't diverge
    // (the fresh-rip path completes ISO in rip_disc and never hands off here;
    // this branch covers cold auto-resume from preserved staging). The abort
    // gate above already scoped loss whole-disc for this mode; the mover
    // validates + moves `.iso` and the prune below retains it for ISO output.
    if super::output_is_iso_image(&output_format) {
        if !staging::fsync_output_file(&iso_path) {
            let quarantined = handle_resume_fsync_failure(device, &staging_dir, "ISO image output");
            let detail = if quarantined {
                "ISO image not durable (fsync failed repeatedly); quarantined (.failed)"
            } else {
                "ISO image not durable (fsync failed); preserved for retry"
            };
            crate::log::device_log(
                device,
                &format!(
                    "Auto-resume: durability gate failed (could not fsync ISO image); {detail}."
                ),
            );
            reset_status_after_ripping(
                device,
                "error",
                &display_name,
                &disc_format,
                &duration,
                Some(detail.to_string()),
            );
            super::unregister_halt(device);
            return;
        }
        let marker_name = if title_confident { ".done" } else { ".review" };
        // Mirror the fresh-rip ISO marker (mod.rs) field-for-field so the
        // mover gets identical metadata on a resume: `disc_name` (the disc's
        // own label, distinct from the resolved `title`), `media_type` (mover
        // routing — TV vs movie), `poster_url`, and `overview`. Omitting these
        // surfaced empty poster/overview in Plex and filed TV resumes as movies.
        let done_marker = serde_json::json!({
            "title": display_name,
            "disc_name": disc_label,
            "format": disc_format,
            "year": tmdb_year,
            "media_type": media_type,
            "poster_url": tmdb_poster,
            "overview": tmdb_overview,
            "date": crate::util::format_date(),
            "resumed": true,
        });
        // `to_string_pretty` on a `json!`-constructed Value is effectively
        // infallible; `.expect` makes the invariant explicit (mirrors
        // staging::write_failed_marker) so a real serialization failure
        // surfaces as a panic rather than silently writing an empty marker
        // that the mover skips, stranding the output in staging forever.
        let marker_body =
            serde_json::to_string_pretty(&done_marker).expect("json! value is always serialisable");
        if let Err(e) =
            staging::write_handoff_marker(&staging_dir.join(marker_name), marker_body.as_bytes())
        {
            crate::log::device_log(
                device,
                &format!(
                    "Auto-resume: {} marker write failed ({}). Preserving staging for retry.",
                    marker_name, e
                ),
            );
            reset_status_after_ripping(
                device,
                "error",
                &display_name,
                &disc_format,
                &duration,
                Some(format!("{} marker write failed: {}", marker_name, e)),
            );
            super::unregister_halt(device);
            return;
        }
        staging::write_completed_marker(&staging_dir);
        staging::clear_restart_count(&staging_dir);
        let iso_name = iso_path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        crate::log::device_log(
            device,
            &format!("Auto-resume: ISO output complete — disc image staged as {iso_name}"),
        );
        super::update_state_with(device, |s| {
            s.status = "done".to_string();
            s.output_file = iso_name;
        });
        super::unregister_halt(device);
        // Honor auto_eject on the resume ISO path the same way the
        // resume MKV terminal (below) and the fresh-rip ISO terminal
        // (mod.rs) do. When resume_remux is entered for a real device
        // (operator clicked Resume with output_format=iso), a disc is
        // physically present and auto_eject=true expects it ejected on
        // completion. Skip synthetic underscore-prefixed devices (the
        // `_mux` worker): they reach this path after the drive thread
        // already ejected, and the drive may hold a different disc.
        if cfg_read.auto_eject && !device.starts_with('_') {
            let device_path = format!("/dev/{}", device);
            super::eject_drive(&device_path);
        }
        return;
    }

    let mux_outcome = super::mux::run_mux(
        super::mux::MuxInputs {
            device,
            display_name: display_name.clone(),
            disc_format: disc_format.clone(),
            tmdb_title: tmdb_title.clone(),
            tmdb_year,
            tmdb_poster: tmdb_poster.clone(),
            tmdb_overview: tmdb_overview.clone(),
            duration: duration.clone(),
            codecs: state_codecs.clone(),
            filename: filename.clone(),
            total_bytes,
            title_bytes_per_sec,
            // Auto-resume bypasses sweep/retry entirely — we open the
            // existing ISO and run only the mux phase. Surface that
            // with the *same* `total_passes` value the multipass
            // orchestrator would have used for this disc on this rig
            // (`max_retries + 2`), so the UI renders `pass N/N · muxing`
            // identically whether we got here via a fresh
            // sweep+retries+mux or a direct resume. Operator-facing
            // consistency: the UI is phase-aware, not path-aware.
            //
            // `max_retries == 0` (direct/single-pass mode) yields
            // `total_passes = 2` (sweep + mux) so the label still has
            // non-zero values to render. Multi-pass uses `+ 2` (sweep
            // + retries + mux); we match.
            total_passes: cfg_read.max_retries.saturating_add(2).max(2),
            bytes_total_disc: disc.capacity_bytes,
            // Pass the real max_retries and bytes_unreadable so that
            // total_pct_byte_weight accounts for the already-completed sweep.
            // Previously max_retries=0 caused the helper to return mux_pct
            // directly (0→100%) — erasing the sweep's ~50% credit — so the
            // progress bar started at 0% on every resumed rip even though all
            // the sweep work was already done.
            max_retries: cfg_read.max_retries,
            bytes_unreadable_at_mux: map.stats().bytes_unreadable,
            dest_url,
            batch,
            staging_disc_dir: staging_dir.clone(),
            sweep_damage: sweep_damage_for_resume,
        },
        input,
        super::mux::MuxAtomics {
            latest_bytes_read: latest_bytes_read.clone(),
            rip_last_lba: rip_last_lba.clone(),
            rip_current_batch: rip_current_batch.clone(),
            wd_last_frame: wd_last_frame.clone(),
            wd_bytes: Arc::new(AtomicU64::new(0)),
            input_errors: mux_input_errors,
        },
    );

    super::unregister_halt(device);

    if !mux_outcome.output_opened || !mux_outcome.completed {
        // Mirror rip_disc's incomplete-mux handling (mod.rs): a mid-mux
        // finalize failure or hard producer read error must surface, not be
        // silently flattened to "idle". `incomplete_mux_status` maps
        // finalize_error → status="failed", read_error → status="error" with
        // the cause string, and only a genuine user halt (both None) →
        // "idle"/no last_error. Previously this path discarded both causes,
        // so an auto-resume mux that died on an ISO/drive read error showed
        // "idle" — indistinguishable from a clean /api/stop — leaving the
        // operator no clue why it stopped or that staging is still resumable.
        let (log_prefix, ui_status, ui_failure_reason) = super::incomplete_mux_status(
            mux_outcome.finalize_error.as_deref(),
            mux_outcome.read_error.as_deref(),
        );
        crate::log::device_log(
            device,
            &format!(
                "Auto-resume mux did not complete ({log_prefix}) — preserving partial state for next restart",
            ),
        );
        // Reset from "ripping" → the verdict status so the next /api/rip isn't
        // blocked by the "already ripping" gate. Staging is preserved either
        // way (no `.failed` written here) so the next restart can resume.
        reset_status_after_ripping(
            device,
            &ui_status,
            &display_name,
            &disc_format,
            &duration,
            ui_failure_reason,
        );
        return;
    }

    // Post-mux loss abort gate — mirror the fresh single-pass gate
    // (mod.rs, "Single-pass abort gate"). The PRE-mux gate above
    // (resume.rs §3) only sees mapfile `Unreadable` sectors. It cannot
    // see demux-time loss: sectors that read into the ISO fine but fail
    // AACS/CSS decrypt at mux time, or codec-level corruption that forces
    // the demuxer to skip — both zero-fill and surface only as
    // `mux_outcome.lost_video_secs`. Without this gate a resumed rip would
    // auto-file (`.done`) a disc whose identical loss the fresh single-pass
    // path quarantines (`.failed`) under `abort_on_lost_secs=0`. The two
    // paths must reach the same verdict.
    //
    // `lost_video_secs` from `run_mux` is the live demux-skip estimate
    // (bytes skipped / title_bytes_per_sec), already in-title-scoped — the
    // same quantity and semantics the single-pass gate compares.
    let demux_lost_secs = mux_outcome.lost_video_secs;
    let abort_threshold_ms = (cfg_read.abort_on_lost_secs * 1000) as f64;
    if super::should_abort_for_loss(demux_lost_secs * 1000.0, abort_threshold_ms) {
        crate::log::device_log(
            device,
            &format!(
                "Auto-resume aborted: {:.2}s lost at mux (demux/decrypt skips) exceeds threshold {}s",
                demux_lost_secs, cfg_read.abort_on_lost_secs
            ),
        );
        // Quarantine the lossy output: write `.failed` (no `.done`/
        // `.completed`) so the mover never files it and the resume detector
        // treats the dir as terminal-failed — exactly as the single-pass
        // gate does. Clear the restart count so a benign restart doesn't
        // re-attempt a deterministically-lossy mux.
        staging::write_failed_marker(
            &staging_dir,
            &format!(
                "aborted: {:.2}s lost at mux exceeds threshold {}s",
                demux_lost_secs, cfg_read.abort_on_lost_secs
            ),
        );
        staging::clear_restart_count(&staging_dir);
        // Remove the `.ripped` hand-off marker: this job is now terminal-failed,
        // so the mux worker must not re-dispatch it. The worker also treats
        // `.failed` as terminal (muxer.rs), but deleting `.ripped` here removes
        // the re-queue source outright — belt and suspenders against the
        // re-mux-forever loop.
        if let Err(e) = crate::muxer::delete_marker(&staging_dir) {
            tracing::warn!(
                staging = %staging_dir.display(),
                error = %e,
                "failed to delete .ripped marker after abort-on-lost-secs; .failed guard prevents re-mux"
            );
        }
        reset_status_after_ripping(
            device,
            "error",
            &display_name,
            &disc_format,
            &duration,
            Some(format!(
                "aborted: {:.2}s lost at mux exceeds threshold {}s",
                demux_lost_secs, cfg_read.abort_on_lost_secs
            )),
        );
        return;
    }

    // Operator-facing loss for an ACCEPTED resume = sweep loss + demux loss.
    // The abort gate above only refuses a resume whose demux loss EXCEEDS the
    // threshold; a resume accepted under a non-zero `abort_on_lost_secs` can
    // still carry real demux-time loss (undecryptable sectors zero-filled,
    // codec-corruption demux skips) that the sweep mapfile never saw. Reporting
    // `done_sweep_damage` alone would file such a disc as clean/low-loss even
    // though the MKV is materially lossier — the same demux loss the fresh
    // single-pass path surfaces via `final_lost_secs` (mod.rs). The two sources
    // are disjoint (sweep = Unreadable sectors baked into the ISO; demux =
    // decrypt/codec skips at mux), so they add.
    let done_errors = done_sweep_damage.errors.saturating_add(mux_outcome.errors);
    let done_lost_video_secs = done_sweep_damage.main_lost_ms / 1000.0 + demux_lost_secs;

    // 5. Success — write .completed marker, drop the hand-off marker for
    // the mover, clear .restart_count. Same shape as the rip_disc
    // completion path so the mover treats this output identically.
    //
    // Honor the SAME title-confidence gate the fresh-rip path uses: a
    // confident match (.done) hands straight to the mover; a low-confidence
    // match (.review) HOLDS the rip for operator review instead of
    // auto-filing it under a guessed name. Unconditionally writing .done
    // here bypassed that hold for every resumed rip.
    // Durability gate: fsync the finished MKV/M2TS before any success
    // marker so a crash can't leave a "done" marker over a page-cache-only
    // (on-disk-truncated) file. The library mux finish() swallows an fsync
    // timeout/halt (returns Ok to bound the hang), so THIS fsync is the
    // real durability gate. Skip network:// output (no local file).
    //
    // If the fsync fails, do NOT write .done/.completed: preserve the
    // staging dir so the next restart's resume re-runs the durable flush
    // rather than handing a possibly-truncated file to the mover.
    let is_network = output_format == "network" && !cfg_read.network_target.is_empty();
    if !is_network && !staging::fsync_output_file(std::path::Path::new(&output_path)) {
        let quarantined = handle_resume_fsync_failure(device, &staging_dir, "mux output");
        let detail = if quarantined {
            "mux output not durable (fsync failed repeatedly); quarantined (.failed)"
        } else {
            "mux output not durable (fsync failed); preserved for retry"
        };
        crate::log::device_log(
            device,
            &format!("Auto-resume: durability gate failed (could not fsync mux output); {detail}."),
        );
        reset_status_after_ripping(
            device,
            "error",
            &display_name,
            &disc_format,
            &duration,
            Some(detail.to_string()),
        );
        return;
    }
    let marker_name = if title_confident { ".done" } else { ".review" };
    // Mirror the fresh-rip MKV marker (mod.rs) field-for-field: `disc_name`,
    // `media_type`, `poster_url`, `overview`. Without them a resume completion
    // handed the mover impoverished metadata vs a fresh rip of the same disc
    // (empty poster/overview, TV shows misfiled as movies).
    let done_marker = serde_json::json!({
        "title": display_name,
        "disc_name": disc_label,
        "format": disc_format,
        "year": tmdb_year,
        "media_type": media_type,
        "poster_url": tmdb_poster,
        "overview": tmdb_overview,
        "date": crate::util::format_date(),
        "resumed": true,
    });
    let done_path = staging_dir.join(marker_name);
    // Durable, atomic marker write (tmp + fsync + rename + staging-dir fsync).
    // The dir-fsync is the crash barrier: it guarantees this hand-off marker
    // is observed on disk before the later `.completed` write, so a crash can
    // never strand `.completed` (terminal) without a durable `.done`.
    // `to_string_pretty` on a `json!`-constructed Value is effectively
    // infallible; `.expect` makes the invariant explicit (mirrors
    // staging::write_failed_marker) so a real serialization failure surfaces
    // as a panic rather than silently writing an empty marker that the mover
    // skips, stranding the output in staging forever.
    let marker_body =
        serde_json::to_string_pretty(&done_marker).expect("json! value is always serialisable");
    if let Err(e) = staging::write_handoff_marker(&done_path, marker_body.as_bytes()) {
        // The hand-off marker is what the mover / review UI keys on. If it
        // fails to write (NFS / perms), do NOT write .completed or clear
        // .restart_count — leaving the partial state intact means the
        // next restart's resume_or_quarantine_staging re-attempts the
        // hand-off rather than stranding a finished MKV in staging with
        // no surfaced error and a "completed" accounting that the mover
        // never sees.
        crate::log::device_log(
            device,
            &format!(
                "Auto-resume: {} marker write failed ({}): {}. \
                 Preserving staging for next-restart retry.",
                marker_name,
                done_path.display(),
                e
            ),
        );
        reset_status_after_ripping(
            device,
            "error",
            &display_name,
            &disc_format,
            &duration,
            Some(format!("{} marker write failed: {}", marker_name, e)),
        );
        return;
    }
    staging::write_completed_marker(&staging_dir);
    staging::clear_restart_count(&staging_dir);
    if !title_confident {
        crate::log::device_log(
            device,
            "Auto-resume: title match not confident — held for operator review (.review)",
        );
    }

    // Prune the disc-sized intermediate ISO + its mapfile unless keep_iso is
    // set, mirroring rip_disc's inline terminal path. The mover normally frees
    // the ISO when it tears down a `.done` staging dir, but a low-confidence
    // match writes `.review` instead (mover skips it) and a setup with no
    // output/mover dir never relocates at all — in both cases the inline path
    // would have freed a 90+ GB UHD ISO immediately while the resume path
    // leaked it. The `keep_iso=false` reclaim must not diverge between the two
    // completion routes; both now share `prune_intermediate_iso`.
    super::prune_intermediate_iso(
        device,
        &iso_path,
        &mapfile_path,
        cfg_read.max_retries,
        super::retain_intermediate_iso(cfg_read.keep_iso, &output_format),
    );

    // Prefer the codecs the mux frame loop wrote into STATE (the
    // `_mux` worker path seeds an empty codecs and only fills it
    // during mux); fall back to the pre-mux snapshot. Resolved once
    // here so both the done-state card and the completion webhook
    // below report the same codec string.
    let done_codecs = resolve_done_codecs(
        super::STATE
            .lock()
            .ok()
            .and_then(|s| s.get(device).map(|rs| rs.codecs.clone())),
        state_codecs,
    );

    super::update_state(
        device,
        super::RipState {
            device: device.to_string(),
            status: "done".to_string(),
            disc_present: true,
            disc_name: display_name.clone(),
            disc_format: disc_format.clone(),
            progress_pct: 100,
            output_file: staging_str.clone(),
            duration: duration.clone(),
            // Carry the TMDB metadata + codecs into the done card, mirroring
            // rip_disc's terminal state. Without these the done-card for a
            // resumed rip loses the poster, TMDB title (showing only the
            // sanitized disc name), year, and codec badge.
            tmdb_title,
            tmdb_year,
            tmdb_poster: tmdb_poster.clone(),
            tmdb_overview,
            codecs: done_codecs.clone(),
            // Carry sweep damage so the done card reflects real damage
            // instead of showing a clean result for a damaged rip. `errors`
            // and the headline `lost_video_secs` additionally fold in
            // demux-time loss (see `done_errors` / `done_lost_video_secs`
            // above) so an accepted-but-lossy resume reports the loss the
            // single-pass path also surfaces, instead of the sweep-only zero.
            // `main_lost_ms` likewise includes the demux loss so the damage
            // classifier rates the disc on the loss actually in the MKV.
            errors: done_errors,
            lost_video_secs: done_lost_video_secs,
            total_lost_ms: done_sweep_damage.total_lost_ms + demux_lost_secs * 1000.0,
            main_lost_ms: done_sweep_damage.main_lost_ms + demux_lost_secs * 1000.0,
            bad_ranges: done_sweep_damage.bad_ranges.clone(),
            num_bad_ranges: done_sweep_damage.num_bad_ranges,
            bad_ranges_truncated: done_sweep_damage.bad_ranges_truncated,
            largest_gap_ms: done_sweep_damage.largest_gap_ms,
            ..Default::default()
        },
    );
    crate::log::device_log(device, "Auto-resume complete");

    // Fire the completion webhook, mirroring rip_disc's terminal
    // branch. Both the cold auto-resume (`?resume=yes`) path and the
    // `_mux` worker `.ripped` hand-off reach success here; without this
    // an operator configured for completion notifications (Discord,
    // Plex, etc.) silently received nothing when a rip finished via
    // resume or the mux worker — only inline rip_disc completions
    // notified. Metadata is the same set rip_disc sends.
    crate::webhook::send_rich(
        &cfg_read,
        &crate::webhook::RipEvent {
            event: "rip_complete",
            title: &display_name,
            year: tmdb_year,
            format: &disc_format,
            poster_url: &tmdb_poster,
            duration: &duration,
            codecs: &done_codecs,
            size_gb: mux_outcome.bytes_done as f64 / 1_073_741_824.0,
            speed_mbs: mux_outcome.speed_mbs,
            elapsed_secs: mux_outcome.elapsed_secs,
            output_path: &staging_str,
            // Sweep loss + demux loss (same combined figures as the done
            // card) so the completion notification reports the real loss in
            // the delivered MKV, not the sweep-mapfile-only subset.
            errors: done_errors,
            lost_video_secs: done_lost_video_secs,
        },
    );

    // Honor auto_eject after a successful resume the same way
    // rip_disc's terminal branch does. Pre-0.25.2 the resume path
    // silently skipped this, so a user with auto_eject=true would
    // find a finished disc still in the drive whenever a rip was
    // recovered after a container restart.
    if cfg_read.auto_eject && !device.starts_with('_') {
        // Underscore-prefixed devices are synthetic — used by the
        // v0.25.3 mux worker which gets here from a `.ripped`
        // hand-off after the drive thread already ejected. Don't
        // re-eject; the drive may even hold a different disc by now.
        let device_path = format!("/dev/{}", device);
        super::eject_drive(&device_path);
    }
}

/// Run a mux-from-staging pass as if it were an auto-resume, against
/// a synthetic device key. Used by `crate::muxer` to dispatch the
/// `.ripped` hand-off from the drive thread without itself
/// re-implementing scan_image + run_mux + history bookkeeping. The
/// device key is `"_mux"` — underscore-prefixed so the UI tile grid
/// ignores it, but `update_state` / `device_log` / halt-token
/// plumbing all still work through the existing per-device shape.
///
/// Returns true on a clean mux (`.completed` marker written, `.ripped`
/// safe to delete). False on any failure path that left `.ripped` in
/// place for next-tick retry.
/// Result of a `.ripped` hand-off mux. `success` mirrors the prior
/// `bool` return; the rest carries the mux-derived display fields the
/// `_mux` done-state computed (codecs/duration/output_file) so the
/// origin-device's secondary done-state update in `crate::muxer` can
/// show the same codec badge, duration, and output path the inline
/// fresh-rip done card does. These are captured from the synthetic
/// `_mux` STATE entry just before it's removed; empty when the mux
/// didn't succeed.
#[derive(Default)]
pub(crate) struct MuxHandoffOutcome {
    pub success: bool,
    pub codecs: String,
    pub duration: String,
    pub output_file: String,
    /// The full bad-ranges drilldown list (plus its truncation count),
    /// captured off the `_mux` done-state — which recomputed it from the
    /// mapfile in `resume_remux`. The `RippedMarker` carries only the
    /// summary counts (`sweep_num_bad_ranges`, `sweep_largest_gap_ms`),
    /// not the list, so without plumbing these through, the origin
    /// device's secondary done card would show the damage count but an
    /// empty drilldown — diverging from the inline fresh-rip and cold
    /// auto-resume done cards, which both populate `bad_ranges`.
    pub bad_ranges: Vec<super::state::BadRange>,
    pub bad_ranges_truncated: u32,
    /// Combined sweep + mux-time loss figures, captured off the `_mux`
    /// done-state — which folded demux/decrypt-time loss into the
    /// sweep-only mapfile totals (`done_errors` / `done_lost_video_secs`
    /// / `done_*_lost_ms` in `resume_remux`). The `RippedMarker` carries
    /// only sweep-phase loss (`rip_lost_video_secs`, `sweep_*`), so without
    /// plumbing these through the origin device's secondary done card would
    /// understate the loss in the delivered MKV whenever a mux-phase
    /// decrypt/codec skip added loss the sweep never saw — diverging from
    /// the `_mux` tile and the completion webhook, which are correct.
    pub lost_video_secs: f64,
    pub errors: u32,
    pub total_lost_ms: f64,
    pub main_lost_ms: f64,
}

pub(crate) fn remux_from_ripped_marker(
    cfg: &Arc<RwLock<Config>>,
    staging_dir: &std::path::Path,
    marker: &crate::muxer::RippedMarker,
) -> MuxHandoffOutcome {
    let iso_path = std::path::PathBuf::from(&marker.iso_path);
    let mapfile_path = std::path::PathBuf::from(&marker.mapfile_path);
    let mux_device = "_mux";

    // Pre-seed STATE so `run_mux`'s TMDB-from-STATE lookup finds the
    // metadata we want on the history record. Codecs gets filled by
    // the worker's scan_image below — for the initial seed we write
    // an empty string and `run_mux` will overwrite via its frame loop.
    super::update_state(
        mux_device,
        super::RipState {
            device: mux_device.to_string(),
            tmdb_title: marker.tmdb_title.clone(),
            tmdb_year: marker.tmdb_year,
            tmdb_poster: marker.tmdb_poster.clone(),
            tmdb_overview: marker.tmdb_overview.clone(),
            tmdb_media_type: marker.tmdb_media_type.clone(),
            ..Default::default()
        },
    );

    let classification = ResumeClass::Remux {
        iso_path: iso_path.clone(),
        mapfile_path: mapfile_path.clone(),
        display_name: marker.display_name.clone(),
        // Carry the fresh-rip confidence verdict (incl. operator override)
        // from the hand-off marker so resume_remux doesn't recompute it
        // from the match check alone.
        title_confident: Some(marker.title_confident),
    };
    resume_remux(cfg, mux_device, classification);

    // Success signal: `resume_remux` wrote `.completed` to staging.
    // Anything else (halt, scan_image failure, mux loop break)
    // leaves `.completed` absent.
    //
    // Probe via `snapshot_staging_disc` (3-retry, NFS-resilient) rather
    // than a bare `Path::exists()`. On NFS with a cold attribute cache —
    // the scenario `snapshot_staging_disc` exists to defend against — a
    // bare `.exists()` can false-negative immediately after
    // `write_completed_marker`, making `check_and_mux` record a spurious
    // `MuxerError` (the success path's `clear_error` is skipped) that
    // sticks on the System page even though the MKV was fully written.
    // This mirrors `check_and_mux`'s completion guard.
    let success = crate::ripper::staging::snapshot_staging_disc(staging_dir)
        .map(|s| s.completed)
        .unwrap_or(false);
    let mut outcome = MuxHandoffOutcome {
        success,
        ..Default::default()
    };
    if success {
        // Hand-off consumed. Drop the marker so this dir doesn't get
        // re-queued on the next muxer tick. If the delete fails, surface
        // it: the `.completed` guard in `check_and_mux` now prevents an
        // infinite re-mux loop even when `.ripped` lingers, but a stuck
        // marker is still worth a warning so the operator can clear it.
        if let Err(e) = crate::muxer::delete_marker(staging_dir) {
            tracing::warn!(
                staging = %staging_dir.display(),
                error = %e,
                "failed to delete .ripped marker after successful mux; .completed guard prevents re-mux"
            );
        }
        // Capture the mux-derived display fields the `_mux` done-state
        // wrote (codecs filled by the frame loop, duration + output_file
        // from the resumed-rip terminal state) BEFORE removing the entry
        // below — the caller carries them into the origin device's
        // secondary done-state so its tile shows the codec badge and
        // duration, matching the inline fresh-rip done card.
        if let Ok(mut s) = super::STATE.lock() {
            if let Some(rs) = s.get(mux_device) {
                outcome.codecs = rs.codecs.clone();
                outcome.duration = rs.duration.clone();
                outcome.output_file = rs.output_file.clone();
                // Carry the full bad-ranges drilldown (recomputed from the
                // mapfile by resume_remux) so the origin device's done card
                // matches the `_mux` card and the fresh-rip card instead of
                // showing an empty drilldown for a damaged disc.
                outcome.bad_ranges = rs.bad_ranges.clone();
                outcome.bad_ranges_truncated = rs.bad_ranges_truncated;
                // Carry the COMBINED sweep + mux-time loss the `_mux`
                // done-state computed (sweep mapfile loss folded with
                // demux/decrypt skips). The marker only holds sweep-phase
                // loss, so the origin device's secondary done card must
                // take these to match the `_mux` tile and the webhook
                // instead of understating loss in the delivered MKV.
                outcome.lost_video_secs = rs.lost_video_secs;
                outcome.errors = rs.errors;
                outcome.total_lost_ms = rs.total_lost_ms;
                outcome.main_lost_ms = rs.main_lost_ms;
            }
            // Clean up the synthetic STATE entry so the device tile grid
            // (which already filters underscore keys, but still — be tidy)
            // doesn't accumulate per-mux ghosts.
            s.remove(mux_device);
        }
    }
    outcome
}

/// Resolve keys for a resumed disc via the configured source. A thin ISO/mapfile
/// binding over [`crate::keysource::resolve_keys`]; returns the disc re-scanned
/// with the key (sample-based source) or unchanged (local / no key).
fn resolve_keys_from_iso(
    cfg: &Config,
    iso_path: &Path,
    mapfile_path: &Path,
    disc: libfreemkv::Disc,
) -> (libfreemkv::Disc, crate::keysource::KeyOutcome) {
    // The mapfile cache is the resume fast-path: when the sweep persisted unit
    // keys (the disc was keyed at scan), `MapfileSource` — first in the source
    // list — hands them straight to `Disc::decrypt_with(Key::Unit(..))`, no
    // keydb parse and no key-service call. Keys XOR VID, so a UK there is the
    // final answer. If the mapfile holds no keys (disc never keyed), resolution
    // falls through to the configured source (which reads the VID from the
    // mapfile via `IsoAccess`); a genuinely-unkeyed disc returns NoKey.
    let sources = crate::keysource::build_sources(cfg, Some(mapfile_path));
    let mut access = crate::keysource::IsoAccess::new(iso_path, mapfile_path);
    crate::keysource::resolve_keys(sources, &mut access, disc)
}

// Tests live in `tests/resume_remux.rs` (integration tests) — they
// pattern-match on the public `ResumeClass` variants and exercise
// `classify_resume` + `delete_partial_output` directly. The deeper
// integration paths (`Disc::scan_image` + `run_mux` against a real
// UDF ISO) are covered by the live test bed only — feeding synthetic
// bytes into `scan_image` reliably fails, so unit tests cap at the
// boundary helpers rather than exercising the disc-read path.
//
// `find_iso_and_mapfile` is `pub(super)` (not reachable from the
// integration test crate), so its deterministic-pairing contract is
// unit-tested in-module here.

#[cfg(test)]
mod find_iso_tests {
    use super::find_iso_and_mapfile;
    use std::fs;

    fn tmpdir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        // Repo-local scratch, never /tmp — anchor to the crate's own
        // target/ dir so artifacts are cleaned by `cargo clean` (mirrors
        // the staging.rs test helper).
        let p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-scratch")
            .join(format!(
                "autorip-find-iso-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed),
            ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn pairs_iso_with_matching_mapfile() {
        let d = tmpdir();
        fs::write(d.join("Movie.iso"), b"x").unwrap();
        fs::write(d.join("Movie.iso.mapfile"), b"x").unwrap();
        let (iso, map) = find_iso_and_mapfile(&d).expect("should pair");
        assert!(iso.ends_with("Movie.iso"));
        assert!(map.ends_with("Movie.iso.mapfile"));
    }

    #[test]
    fn rejects_when_mapfile_does_not_match_iso() {
        let d = tmpdir();
        fs::write(d.join("Movie.iso"), b"x").unwrap();
        // Mapfile keyed to a *different* ISO name — must not be paired.
        fs::write(d.join("Other.iso.mapfile"), b"x").unwrap();
        assert!(find_iso_and_mapfile(&d).is_none());
    }

    #[test]
    fn rejects_multiple_isos_as_ambiguous() {
        let d = tmpdir();
        fs::write(d.join("A.iso"), b"x").unwrap();
        fs::write(d.join("A.iso.mapfile"), b"x").unwrap();
        fs::write(d.join("B.iso"), b"x").unwrap();
        assert!(find_iso_and_mapfile(&d).is_none());
    }

    #[test]
    fn rejects_missing_mapfile() {
        let d = tmpdir();
        fs::write(d.join("Movie.iso"), b"x").unwrap();
        assert!(find_iso_and_mapfile(&d).is_none());
    }

    // Regression: the loop no longer uses `.flatten()` (which silently
    // dropped per-DirEntry I/O errors). Per-entry error handling must not
    // break the happy path — extra unrelated entries alongside the ISO +
    // mapfile must still pair correctly.
    #[test]
    fn pairs_despite_extra_entries() {
        let d = tmpdir();
        fs::write(d.join("Movie.iso"), b"x").unwrap();
        fs::write(d.join("Movie.iso.mapfile"), b"x").unwrap();
        // Noise the scan must skip over.
        fs::write(d.join("Movie.mkv"), b"x").unwrap();
        fs::write(d.join(".keep"), b"x").unwrap();
        fs::create_dir(d.join("subdir")).unwrap();
        let (iso, map) = find_iso_and_mapfile(&d).expect("should pair");
        assert!(iso.ends_with("Movie.iso"));
        assert!(map.ends_with("Movie.iso.mapfile"));
    }
}

// Convergence M (finding 5): `remux_from_ripped_marker` detects whether
// `resume_remux` succeeded by checking for `.completed`. It must use the
// NFS-resilient `snapshot_staging_disc(...).completed` (3-retry, primed
// read) rather than a bare `Path::join(".completed").exists()`: on NFS with
// a cold attribute cache the bare `.exists()` can false-negative right after
// `write_completed_marker`, making `check_and_mux` record a spurious
// `MuxerError` that sticks on the System page even though the MKV landed.
// These tests pin the success-detection helper's contract on the two states
// that matter.
#[cfg(test)]
mod completion_detection_tests {
    use crate::ripper::staging;

    fn tmpdir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-scratch")
            .join(format!(
                "autorip-resume-complete-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed),
            ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// The exact success expression used by `remux_from_ripped_marker`. After
    /// `write_completed_marker`, the snapshot must report `completed=true`, so
    /// the success path runs `clear_error` and no spurious MuxerError sticks.
    #[test]
    fn snapshot_reports_completed_after_marker_write() {
        let d = tmpdir();
        staging::write_completed_marker(&d);
        let success = staging::snapshot_staging_disc(&d)
            .map(|s| s.completed)
            .unwrap_or(false);
        assert!(
            success,
            "snapshot_staging_disc must report completed after write_completed_marker"
        );
    }

    /// And without the marker (halt / scan_image failure / mux loop break) it
    /// must report `completed=false` so the failure path records the error.
    #[test]
    fn snapshot_reports_not_completed_without_marker() {
        let d = tmpdir();
        // A partial dir with ISO/mapfile but no `.completed`.
        std::fs::write(d.join("Movie.iso"), b"x").unwrap();
        std::fs::write(d.join("Movie.iso.mapfile"), b"x").unwrap();
        let success = staging::snapshot_staging_disc(&d)
            .map(|s| s.completed)
            .unwrap_or(false);
        assert!(
            !success,
            "snapshot_staging_disc must report not-completed without the marker"
        );
    }
}

// Regression guard for the resume abort-gate scoping. The fresh-rip
// post-retry abort check scopes loss by `output_format` via
// `abort_lost_ms` (whole-disc for iso, in-title for mkv/m2ts). The
// `resume_remux` re-validation previously hard-coded in-title scoping,
// so a disc with `output_format=iso`, `abort_on_lost_secs=0`, and
// unreadable sectors OUTSIDE the title was ABORTED on a fresh rip but
// ACCEPTED on resume — opposite verdicts on the same disc + config.
// resume_remux now routes through the same `abort_lost_ms` helper; these
// tests pin the scoping the resume gate must use so the two paths stay
// in lockstep.
#[cfg(test)]
mod resume_abort_scope_tests {
    fn title_lba(start_lba: u32, sector_count: u32) -> libfreemkv::DiscTitle {
        let mut t = libfreemkv::DiscTitle::empty();
        t.extents.push(libfreemkv::disc::Extent {
            start_lba,
            sector_count,
        });
        t
    }

    #[test]
    fn iso_resume_counts_out_of_title_loss() {
        // Out-of-title unreadable range only. For output_format=iso the
        // resume gate must see positive loss (whole-disc scope) — same as
        // a fresh ISO rip would, so both abort under abort_on_lost_secs=0.
        let bps = 8_250_000.0;
        let title = title_lba(1000, 1000);
        let bad = vec![(0u64, 50 * 2048)];
        let lost_secs =
            crate::ripper::abort_lost_ms(/* output_is_iso */ true, &title, &bad, bps) / 1000.0;
        assert!(
            lost_secs > 0.0,
            "iso resume must count whole-disc (out-of-title) loss"
        );
    }

    #[test]
    fn mkv_resume_ignores_out_of_title_loss() {
        // Same out-of-title range, mkv/m2ts output → in-title scope → 0,
        // so the resume gate proceeds to mux (matching fresh-rip mkv).
        let bps = 8_250_000.0;
        let title = title_lba(1000, 1000);
        let bad = vec![(0u64, 50 * 2048)];
        let lost_secs =
            crate::ripper::abort_lost_ms(/* output_is_iso */ false, &title, &bad, bps) / 1000.0;
        assert_eq!(
            lost_secs, 0.0,
            "mkv resume must ignore out-of-title loss (in-title scope)"
        );
    }
}

#[cfg(test)]
mod resume_remux_log_archive_tests {
    use super::*;

    fn tmpdir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-scratch")
            .join(format!(
                "autorip-resume-archive-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed),
            ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(p.join("logs")).unwrap();
        p
    }

    /// Regression: resume_remux did not archive the prior session's
    /// per-device log on entry (unlike scan_disc and rip_disc), so on the
    /// common "scan then resume" path the resumed-mux log entries
    /// interleaved with the prior scan's log.
    ///
    /// This drives resume_remux through its real entry path: a prior log
    /// entry is seeded, then resume_remux runs with a Remux classification
    /// pointing at a non-openable ISO (it archives, logs the resume line,
    /// then aborts on ISO open). The in-memory live ring (keyed by the
    /// unique device name, independent of AUTORIP_DIR) must afterwards
    /// contain only the new operation's entries — the prior line must have
    /// been archived out.
    #[test]
    fn resume_remux_archives_prior_device_log() {
        let d = tmpdir();
        // Route logs to the tempdir for this test. SAFETY: env access in
        // tests; the assertion that matters reads the in-memory ring
        // (keyed by the unique device name), not the env-routed file.
        unsafe {
            std::env::set_var("AUTORIP_DIR", &d);
        }

        let dev = format!("test_resume_archive_sg_{}", std::process::id());

        // Seed a prior session's log line, as a scan/rip would leave behind.
        crate::log::device_log(&dev, "PRIOR-SESSION-SCAN-LINE");
        assert!(
            crate::log::get_device_log(&dev, 100)
                .iter()
                .any(|l| l.contains("PRIOR-SESSION-SCAN-LINE")),
            "prior line should be present before resume"
        );

        // A Remux classification pointing at a non-existent ISO so the
        // function archives + logs the resume line, then aborts on open.
        let class = ResumeClass::Remux {
            iso_path: d.join("does-not-exist.iso"),
            mapfile_path: d.join("does-not-exist.iso.mapfile"),
            display_name: "Nonexistent".to_string(),
            title_confident: None,
        };
        let cfg = Arc::new(RwLock::new(Config::default()));

        resume_remux(&cfg, &dev, class);

        let live = crate::log::get_device_log(&dev, 100);
        assert!(
            !live.iter().any(|l| l.contains("PRIOR-SESSION-SCAN-LINE")),
            "prior session line must be archived out of the live log, got: {:?}",
            live
        );
        assert!(
            live.iter().any(|l| l.contains("Auto-resume: re-muxing")),
            "live log should contain the new resume entry, got: {:?}",
            live
        );

        let _ = std::fs::remove_dir_all(&d);
    }
}

#[cfg(test)]
mod resume_remux_webhook_tests {
    /// Regression: the success path of `resume_remux` must fire the
    /// `rip_complete` webhook, exactly as `rip_disc`'s terminal branch
    /// does. Both the cold auto-resume (`?resume=yes`) path and the
    /// `_mux` worker `.ripped` hand-off complete through `resume_remux`,
    /// so without this call an operator with completion webhooks
    /// configured (Discord, Plex, etc.) silently received nothing on any
    /// rip that finished via resume or the mux worker.
    ///
    /// A full behavioural test would need a real openable ISO + mapfile
    /// + decrypt + mux pipeline plus a mock HTTP endpoint, which is out
    /// of proportion to a single call site. Instead this pins, at source
    /// level, that the success region of `resume_remux` (between the
    /// "Auto-resume complete" log line and the `auto_eject` honoring)
    /// invokes `send_rich`, so a refactor can't silently drop it again.
    #[test]
    fn success_path_fires_completion_webhook() {
        let src = include_str!("resume.rs");
        let start = src
            .find("Auto-resume complete")
            .expect("resume.rs should log \"Auto-resume complete\" on the success path");
        // Bound the search to the success region: from the completion log
        // line up to the auto_eject comment that immediately follows it.
        let end = src[start..]
            .find("Honor auto_eject after a successful resume")
            .map(|i| start + i)
            .unwrap_or(src.len());
        let region = &src[start..end];
        assert!(
            region.contains("crate::webhook::send_rich"),
            "resume_remux success path must fire send_rich (the rip_complete \
             webhook), matching rip_disc; none found between \"Auto-resume \
             complete\" and the auto_eject branch"
        );
        assert!(
            region.contains("event: \"rip_complete\""),
            "the resume completion webhook must use the rip_complete event \
             name, matching rip_disc"
        );
    }
}

#[cfg(test)]
mod resume_iso_auto_eject_tests {
    /// Regression: the ISO-output success path of `resume_remux` must
    /// honor `auto_eject`, exactly as the resume MKV terminal and the
    /// fresh-rip ISO terminal (`mod.rs`) do. When `resume_remux` is
    /// entered for a real device (operator clicked Resume with
    /// `output_format=iso`) a disc is physically present, and an
    /// operator with `auto_eject=true` expects it ejected on
    /// completion. Pre-fix the ISO branch returned without ejecting, so
    /// the finished disc stayed in the drive. The synthetic `_mux`
    /// worker device reaches this branch too, so the guard must skip
    /// underscore-prefixed devices (the drive thread already ejected and
    /// may now hold a different disc) — mirroring the MKV terminal.
    ///
    /// A full behavioural test would need a real openable ISO + mapfile
    /// plus an actual eject syscall against a device — out of proportion
    /// to one call site (same rationale as
    /// `success_path_fires_completion_webhook`). Instead this pins, at
    /// source level, that the ISO success region (between its
    /// "ISO output complete" log line and the `run_mux` call that begins
    /// the MKV path) honors `cfg_read.auto_eject` with the synthetic
    /// device guard.
    #[test]
    fn resume_iso_success_path_honors_auto_eject() {
        let src = include_str!("resume.rs");
        let start = src
            .find("Auto-resume: ISO output complete")
            .expect("resume.rs should log \"Auto-resume: ISO output complete\" on the ISO path");
        // Bound the search to the ISO success region: from the ISO
        // completion log line up to the run_mux call that opens the MKV
        // path (the next distinct terminal in the function).
        let end = src[start..]
            .find("super::mux::run_mux")
            .map(|i| start + i)
            .expect("resume.rs should call run_mux after the ISO branch");
        let region = &src[start..end];
        assert!(
            region.contains("cfg_read.auto_eject"),
            "resume_remux ISO success path must honor cfg_read.auto_eject, \
             matching the MKV terminal and the fresh-rip ISO terminal; none \
             found between the ISO completion log and run_mux"
        );
        assert!(
            region.contains("!device.starts_with('_')"),
            "the ISO auto_eject branch must guard synthetic underscore-\
             prefixed devices (the _mux worker), matching the MKV terminal"
        );
        assert!(
            region.contains("super::eject_drive"),
            "the ISO auto_eject branch must call super::eject_drive"
        );
    }
}

#[cfg(test)]
mod post_mux_loss_gate_tests {
    /// Regression: `resume_remux` must enforce the SAME post-mux loss
    /// abort gate the fresh single-pass path does. The PRE-mux gate only
    /// counts mapfile `Unreadable` sectors; demux-time loss (AACS/CSS
    /// decrypt failures during ISO mux, codec-corruption demux skips) is
    /// invisible there and surfaces only as `mux_outcome.lost_video_secs`.
    /// Without a post-mux check, a resumed rip auto-files (`.done`) a disc
    /// whose identical loss the single-pass path quarantines (`.failed`)
    /// under `abort_on_lost_secs=0`.
    ///
    /// A full behavioural test would need a real ISO that decodes into
    /// demux skips plus a mux pipeline — out of proportion to one call
    /// site (same rationale as `success_path_fires_completion_webhook`).
    /// Instead this pins, at source level, that the region between the
    /// mux-incomplete early-return and the `.completed` success marker
    /// inspects `lost_video_secs`, compares it via `should_abort_for_loss`,
    /// and quarantines with `write_failed_marker` on exceedance.
    #[test]
    fn resume_enforces_post_mux_loss_gate() {
        let src = include_str!("resume.rs");
        // Bound to the post-mux success region: from the mux-incomplete
        // early-return up to the success-marker section header.
        let start = src
            .find("Auto-resume mux did not complete")
            .expect("resume.rs should have the mux-incomplete early-return");
        let end = src[start..]
            .find("5. Success — write .completed marker")
            .map(|i| start + i)
            .expect("resume.rs should have the success-marker section");
        let region = &src[start..end];
        assert!(
            region.contains("mux_outcome.lost_video_secs"),
            "resume post-mux region must read mux_outcome.lost_video_secs \
             (the only signal carrying demux-time loss)"
        );
        assert!(
            region.contains("should_abort_for_loss"),
            "resume post-mux region must gate loss via should_abort_for_loss, \
             matching the single-pass gate"
        );
        assert!(
            region.contains("write_failed_marker"),
            "resume post-mux region must quarantine an over-threshold lossy \
             mux with write_failed_marker (no .done), matching single-pass"
        );
    }

    /// Regression: an ACCEPTED resume (demux loss within `abort_on_lost_secs`)
    /// must report sweep loss + demux loss to the operator, not the sweep
    /// mapfile alone. The abort gate only refuses resumes whose demux loss
    /// EXCEEDS the threshold; loss within the threshold is real loss in the
    /// delivered MKV that the fresh single-pass path surfaces (mod.rs
    /// `final_lost_secs = mux_outcome.lost_video_secs`). Previously the resume
    /// done card and `rip_complete` webhook sourced `errors` /
    /// `lost_video_secs` solely from `done_sweep_damage`, so any demux-time
    /// loss (undecryptable sectors, codec corruption) was invisible and the
    /// disc was filed as clean.
    ///
    /// A behavioural test would need a real lossy ISO + mux pipeline (same
    /// rationale as the gate test above); instead this pins, at source level,
    /// that the accepted-success region folds the demux loss into the
    /// reported figures.
    #[test]
    fn resume_reports_demux_loss_on_accepted_rip() {
        let src = include_str!("resume.rs");
        // Bound to the accepted-success region: from the post-mux abort gate's
        // combined-loss computation up to the auto-eject tail.
        let start = src
            .find("Operator-facing loss for an ACCEPTED resume")
            .expect("resume.rs should compute combined accepted-resume loss");
        let end = src[start..]
            .find("Honor auto_eject after a successful resume")
            .map(|i| start + i)
            .expect("resume.rs should have the auto_eject tail after success");
        let region = &src[start..end];

        // The combined figures must be derived from BOTH sweep damage and the
        // demux loss signal.
        assert!(
            region.contains("done_sweep_damage.errors.saturating_add(mux_outcome.errors)"),
            "accepted resume must add demux errors to sweep errors"
        );
        assert!(
            region.contains("done_sweep_damage.main_lost_ms / 1000.0 + demux_lost_secs"),
            "accepted resume must add demux lost seconds to sweep main loss"
        );
        // Both the done card and the webhook must consume the combined figures,
        // not the sweep-only fields.
        assert!(
            region.contains("errors: done_errors"),
            "done card / webhook must report combined errors (done_errors)"
        );
        assert!(
            region.contains("lost_video_secs: done_lost_video_secs"),
            "done card / webhook must report combined loss (done_lost_video_secs)"
        );
        // Guard against regressing to the sweep-only webhook figures.
        assert!(
            !region.contains("lost_video_secs: done_sweep_damage.main_lost_ms / 1000.0"),
            "webhook must not report sweep-only loss, hiding demux loss"
        );
    }

    /// Regression: the mux-incomplete early-return in `resume_remux` must route
    /// through `incomplete_mux_status` (mirroring rip_disc in mod.rs) so a
    /// mid-mux finalize error or hard producer read error surfaces. Previously
    /// this branch hardcoded `status="idle"` with `last_error=None`, silently
    /// discarding `mux_outcome.read_error` / `finalize_error`: an auto-resume
    /// mux that died on an ISO/drive read error showed plain "idle" — visually
    /// identical to a clean /api/stop — leaving the operator no indication of
    /// the failure or that staging was still resumable.
    ///
    /// A behavioural test would need a mux pipeline that fails mid-stream (same
    /// rationale as the gate tests above); instead this pins, at source level,
    /// that the early-return consults both causes and no longer hardcodes the
    /// idle/None verdict.
    #[test]
    fn resume_incomplete_mux_surfaces_read_error_not_silent_idle() {
        let src = include_str!("resume.rs");
        // Bound to the mux-incomplete early-return: from the guard up to the
        // post-mux loss-abort gate comment that follows it.
        let start = src
            .find("if !mux_outcome.output_opened || !mux_outcome.completed {")
            .expect("resume.rs should have the mux-incomplete guard");
        let end = src[start..]
            .find("Post-mux loss abort gate")
            .map(|i| start + i)
            .expect("resume.rs should have the post-mux loss gate after the guard");
        let region = &src[start..end];

        assert!(
            region.contains("super::incomplete_mux_status("),
            "resume mux-incomplete branch must route through incomplete_mux_status, \
             matching rip_disc, so finalize/read-error causes surface"
        );
        assert!(
            region.contains("mux_outcome.read_error.as_deref()"),
            "resume mux-incomplete branch must pass mux_outcome.read_error so a \
             mid-mux drive/ISO read error surfaces as status=error with the cause"
        );
        assert!(
            region.contains("mux_outcome.finalize_error.as_deref()"),
            "resume mux-incomplete branch must pass mux_outcome.finalize_error so a \
             structural mux finalize failure surfaces as status=failed"
        );
        // Guard against regressing to the silent idle/None verdict that
        // discarded the cause and looked like a clean /api/stop.
        assert!(
            !region.contains(
                "reset_status_after_ripping(device, \"idle\", &display_name, \
                 &disc_format, &duration, None)"
            ),
            "resume mux-incomplete branch must not hardcode idle/None, hiding the \
             read-error cause and aliasing a failure to a clean stop"
        );
    }
}

#[cfg(test)]
mod sweep_damage_marker_tests {
    /// Regression: resume_remux previously passed SweepDamageSnapshot::default()
    /// (all zeros) so a resumed mux showed zero damage even when the original
    /// sweep had bad sectors.
    ///
    /// Fix: RippedMarker gained sweep_* fields (serde-defaulted for back-compat)
    /// populated at hand-off time. This test verifies round-trip: a marker with
    /// non-zero sweep_* fields serializes and deserializes correctly, and the
    /// resulting values are what remux_from_ripped_marker would carry into
    /// SweepDamageSnapshot.
    #[test]
    fn ripped_marker_sweep_fields_round_trip() {
        let marker = crate::muxer::RippedMarker {
            schema_version: crate::muxer::RIPPED_MARKER_SCHEMA,
            iso_path: "/staging/Foo/Foo.iso".into(),
            mapfile_path: "/staging/Foo/Foo.iso.mapfile".into(),
            display_name: "Foo".into(),
            disc_format: "uhd".into(),
            mkv_filename: "Foo.mkv".into(),
            tmdb_title: "Foo".into(),
            tmdb_year: 2024,
            tmdb_poster: String::new(),
            tmdb_overview: String::new(),
            tmdb_media_type: String::new(),
            max_retries: 3,
            abort_on_lost_secs: 0,
            rip_elapsed_secs: 0.0,
            rip_errors: 0,
            rip_lost_video_secs: 1.23,
            rip_last_sector: 0,
            origin_device: "sg0".into(),
            sweep_errors: 77,
            sweep_total_lost_ms: 2500.0,
            sweep_main_lost_ms: 1200.0,
            sweep_num_bad_ranges: 5,
            sweep_largest_gap_ms: 900.0,
            title_confident: false,
        };

        // Serialize then deserialize (mirrors write_marker / read_marker).
        let json = serde_json::to_string(&marker).expect("serialize");
        let back: crate::muxer::RippedMarker = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(back.sweep_errors, 77);
        assert!((back.sweep_total_lost_ms - 2500.0).abs() < 0.001);
        assert!((back.sweep_main_lost_ms - 1200.0).abs() < 0.001);
        assert_eq!(back.sweep_num_bad_ranges, 5);
        assert!((back.sweep_largest_gap_ms - 900.0).abs() < 0.001);
    }

    /// Backward-compat: a marker JSON without sweep_* fields (pre-v0.25.12)
    /// must deserialize successfully with sweep_* defaulting to zero.
    #[test]
    fn ripped_marker_missing_sweep_fields_default_to_zero() {
        // JSON without any sweep_* keys — simulates an old marker on disk.
        let json = r#"{
            "schema_version": 1,
            "iso_path": "/staging/Bar/Bar.iso",
            "mapfile_path": "/staging/Bar/Bar.iso.mapfile",
            "display_name": "Bar",
            "disc_format": "bluray",
            "mkv_filename": "Bar.mkv",
            "tmdb_title": "Bar",
            "tmdb_year": 2020,
            "tmdb_poster": "",
            "tmdb_overview": "",
            "max_retries": 5,
            "abort_on_lost_secs": 30,
            "rip_elapsed_secs": 0.0,
            "rip_errors": 0,
            "rip_lost_video_secs": 0.0,
            "rip_last_sector": 0,
            "origin_device": "sg0"
        }"#;
        let marker: crate::muxer::RippedMarker =
            serde_json::from_str(json).expect("old marker must deserialize");
        // schema_version check is done by read_marker, not serde; skip it here.
        assert_eq!(marker.sweep_errors, 0, "missing field must default to 0");
        assert_eq!(
            marker.sweep_total_lost_ms, 0.0,
            "missing field must default to 0.0"
        );
        assert_eq!(
            marker.sweep_main_lost_ms, 0.0,
            "missing field must default to 0.0"
        );
        assert_eq!(
            marker.sweep_num_bad_ranges, 0,
            "missing field must default to 0"
        );
        assert_eq!(
            marker.sweep_largest_gap_ms, 0.0,
            "missing field must default to 0.0"
        );
        assert!(
            !marker.title_confident,
            "missing title_confident must default to false (prior match-check-only behavior)"
        );
    }

    /// Regression: an operator title override (or any high-confidence
    /// fresh-rip verdict) must survive the `.ripped` hand-off so the mux
    /// worker's resume_remux auto-files into `.done` instead of holding it
    /// for review. Before the fix, RippedMarker did not carry the verdict
    /// and resume_remux recomputed it from `is_confident_match(disc_label,
    /// title, year)` alone — which an override (chosen title != disc label)
    /// fails by construction, forcing the deliberate pick into `.review`.
    #[test]
    fn ripped_marker_title_confident_round_trips() {
        let mut marker = crate::muxer::RippedMarker {
            schema_version: crate::muxer::RIPPED_MARKER_SCHEMA,
            iso_path: "/staging/Baz/Baz.iso".into(),
            mapfile_path: "/staging/Baz/Baz.iso.mapfile".into(),
            display_name: "Operator Chosen Title".into(),
            disc_format: "uhd".into(),
            mkv_filename: "Operator_Chosen_Title.mkv".into(),
            tmdb_title: "Operator Chosen Title".into(),
            tmdb_year: 2024,
            tmdb_poster: String::new(),
            tmdb_overview: String::new(),
            tmdb_media_type: String::new(),
            max_retries: 3,
            abort_on_lost_secs: 0,
            rip_elapsed_secs: 0.0,
            rip_errors: 0,
            rip_lost_video_secs: 0.0,
            rip_last_sector: 0,
            origin_device: "sg0".into(),
            sweep_errors: 0,
            sweep_total_lost_ms: 0.0,
            sweep_main_lost_ms: 0.0,
            sweep_num_bad_ranges: 0,
            sweep_largest_gap_ms: 0.0,
            title_confident: true,
        };
        let json = serde_json::to_string(&marker).expect("serialize");
        let back: crate::muxer::RippedMarker = serde_json::from_str(&json).expect("deserialize");
        assert!(
            back.title_confident,
            "operator-confident verdict must survive the .ripped hand-off"
        );

        // And the low-confidence case round-trips as false.
        marker.title_confident = false;
        let json = serde_json::to_string(&marker).expect("serialize");
        let back: crate::muxer::RippedMarker = serde_json::from_str(&json).expect("deserialize");
        assert!(!back.title_confident);
    }

    /// Regression: the resumed-rip done card must carry codecs. The `_mux`
    /// worker path seeds an empty codecs into STATE and only fills it during
    /// muxing, so the done state must prefer the post-mux STATE value over the
    /// (empty) pre-mux snapshot. The user-triggered path has codecs in the
    /// pre-mux snapshot already; either way the done card must not be blank.
    #[test]
    fn resolve_done_codecs_prefers_post_mux_then_snapshot() {
        // _mux path: pre-mux snapshot empty, post-mux STATE has real codecs.
        assert_eq!(
            super::resolve_done_codecs(Some("HEVC · TrueHD".into()), String::new()),
            "HEVC · TrueHD",
            "post-mux codecs must win when present"
        );
        // User-triggered path: STATE empty post-mux, snapshot carries codecs.
        assert_eq!(
            super::resolve_done_codecs(Some(String::new()), "AVC · DTS".into()),
            "AVC · DTS",
            "empty post-mux STATE must fall back to the pre-mux snapshot"
        );
        // No STATE entry at all → snapshot.
        assert_eq!(
            super::resolve_done_codecs(None, "AVC · DTS".into()),
            "AVC · DTS",
            "absent STATE must fall back to the pre-mux snapshot"
        );
        // Both populated → post-mux is the fresher truth.
        assert_eq!(
            super::resolve_done_codecs(Some("HEVC".into()), "AVC".into()),
            "HEVC"
        );
    }

    /// Regression: resume `.done`/`.review` markers omitted `media_type`, so the
    /// mover defaulted every resumed rip to "movie" and filed TV-show resumes
    /// under the movie library. The resume path now resolves media_type the same
    /// way the mover reads it: a carried "tv"/"movie" passes through, and an
    /// empty value (cold auto-resume, no carried metadata) becomes "movie".
    #[test]
    fn resolve_media_type_defaults_empty_to_movie() {
        assert_eq!(
            super::resolve_media_type("tv"),
            "tv",
            "a carried TV media_type must survive into the marker, not collapse to movie"
        );
        assert_eq!(super::resolve_media_type("movie"), "movie");
        assert_eq!(
            super::resolve_media_type(""),
            "movie",
            "empty (cold resume) must resolve to the mover's own default"
        );
    }

    /// Regression: the origin device's secondary done-state update (in
    /// `crate::muxer::check_and_mux`) was dropping the codec badge,
    /// duration, and output_file because `remux_from_ripped_marker`
    /// returned a bare `bool` and the worker had nothing to plumb them
    /// from — the `_mux` STATE entry it would read had already been
    /// removed on success. The fix captures those three mux-derived
    /// fields off the `_mux` done-state just before removal and returns
    /// them in `MuxHandoffOutcome`. This exercises that exact capture
    /// expression against the real STATE map, including the combined
    /// sweep + mux-time loss figures (`lost_video_secs` / `errors` /
    /// `total_lost_ms` / `main_lost_ms`), which the `_mux` done-state
    /// folds demux/decrypt loss into and the origin device's done card
    /// must take instead of the marker's sweep-only subset.
    #[test]
    fn mux_handoff_outcome_captures_mux_derived_fields() {
        // A private device key so this doesn't race the shared "_mux".
        let key = "_mux_test_capture";
        let bad_ranges = vec![
            super::super::state::BadRange {
                lba: 100,
                count: 32,
                duration_ms: 1500.0,
                chapter: Some(2),
                time_offset_secs: Some(42.0),
            },
            super::super::state::BadRange {
                lba: 5000,
                count: 8,
                duration_ms: 375.0,
                chapter: None,
                time_offset_secs: None,
            },
        ];
        super::super::update_state(
            key,
            super::super::RipState {
                device: key.to_string(),
                status: "done".to_string(),
                codecs: "HEVC · TrueHD".into(),
                duration: "2:14".into(),
                output_file: "/staging/Foo".into(),
                bad_ranges: bad_ranges.clone(),
                bad_ranges_truncated: 3,
                // Combined sweep + mux-time loss the `_mux` done-state writes:
                // these must be captured so the origin device's done card
                // reports the loss in the delivered MKV, not the sweep-only
                // subset from the marker.
                errors: 7,
                lost_video_secs: 12.5,
                total_lost_ms: 12500.0,
                main_lost_ms: 9000.0,
                ..Default::default()
            },
        );

        // Mirror the capture-then-remove block in remux_from_ripped_marker.
        let mut outcome = super::MuxHandoffOutcome {
            success: true,
            ..Default::default()
        };
        if let Ok(mut s) = super::super::STATE.lock() {
            if let Some(rs) = s.get(key) {
                outcome.codecs = rs.codecs.clone();
                outcome.duration = rs.duration.clone();
                outcome.output_file = rs.output_file.clone();
                outcome.bad_ranges = rs.bad_ranges.clone();
                outcome.bad_ranges_truncated = rs.bad_ranges_truncated;
                outcome.lost_video_secs = rs.lost_video_secs;
                outcome.errors = rs.errors;
                outcome.total_lost_ms = rs.total_lost_ms;
                outcome.main_lost_ms = rs.main_lost_ms;
            }
            s.remove(key);
        }

        assert_eq!(outcome.codecs, "HEVC · TrueHD");
        assert_eq!(outcome.duration, "2:14");
        assert_eq!(outcome.output_file, "/staging/Foo");
        // The bad-ranges drilldown list + truncation count must survive the
        // capture so the origin device's done card isn't left with an empty
        // drilldown for a damaged disc.
        assert_eq!(outcome.bad_ranges.len(), 2);
        assert_eq!(outcome.bad_ranges[0].lba, 100);
        assert_eq!(outcome.bad_ranges[1].count, 8);
        assert_eq!(outcome.bad_ranges_truncated, 3);
        // Combined sweep + mux-time loss figures must survive the capture so
        // the origin device's done card reports the loss in the delivered MKV
        // (matching the `_mux` tile and the webhook), not the sweep-only
        // subset the `RippedMarker` carries.
        assert_eq!(outcome.errors, 7);
        assert_eq!(outcome.lost_video_secs, 12.5);
        assert_eq!(outcome.total_lost_ms, 12500.0);
        assert_eq!(outcome.main_lost_ms, 9000.0);
        // STATE entry is cleaned up so the origin update can't read it later.
        assert!(super::super::STATE.lock().unwrap().get(key).is_none());
    }
}

// Convergence round 4 (H1 + M4): the cold operator-resume mux path acquires the
// `.muxing` exclusion lock so a concurrent Wipe / second cold resume can't
// delete or double-mux the in-flight ISO; and a repeated durability-gate
// (fsync) failure is capped via `.restart_count` → `.failed` rather than
// re-muxing the same possibly-corrupt output forever on the `_mux` worker loop.
#[cfg(test)]
mod resume_lock_and_fsync_tests {
    use super::*;
    use crate::ripper::staging;

    fn tmpdir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-scratch")
            .join(format!(
                "autorip-resume-lock-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed),
            ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// H1: a cold operator-resume (real device) must write `.muxing` for the
    /// duration of the mux so `disc_owned_by_worker` / `resumable_dir_blocked`
    /// see the dir as owned and a concurrent Wipe / second cold resume is
    /// blocked. The guard clears the marker on drop.
    #[test]
    fn cold_resume_guard_writes_and_clears_muxing() {
        let d = tmpdir();
        assert!(!d.join(".muxing").exists());
        {
            let _g = ResumeMuxingGuard::acquire("sg0", &d);
            assert!(
                d.join(".muxing").exists(),
                "cold-resume guard must write .muxing while the mux is in flight"
            );
            // While held, the snapshot the ownership/blocked checks consult
            // reports the dir as owned.
            let snap = staging::snapshot_staging_disc(&d).expect("snapshot");
            assert!(snap.has_muxing);
        }
        assert!(
            !d.join(".muxing").exists(),
            ".muxing must be cleared on guard drop (covers early-return / panic)"
        );
    }

    /// H1: the `_mux` worker already holds the lock via `check_and_mux`'s own
    /// MuxingGuard, so `resume_remux`'s guard must NOT touch the marker — neither
    /// write a redundant one nor clear the worker's on drop (a clear would
    /// release the worker's exclusion mid-dispatch).
    #[test]
    fn worker_mux_device_does_not_double_manage_muxing() {
        let d = tmpdir();
        // Simulate the worker having written .muxing before dispatch.
        staging::write_muxing_marker(&d);
        assert!(d.join(".muxing").exists());
        {
            let _g = ResumeMuxingGuard::acquire("_mux", &d);
            assert!(d.join(".muxing").exists(), "worker's lock stays put");
        }
        assert!(
            d.join(".muxing").exists(),
            "the `_mux` guard must leave the worker's .muxing intact on drop"
        );
    }

    /// M4: below `RESTART_LIMIT`, a fsync failure bumps `.restart_count` and
    /// preserves staging (no `.failed`) for the next retry.
    #[test]
    fn fsync_failure_below_limit_preserves_and_bumps() {
        let d = tmpdir();
        // Seed a `.ripped` so we can assert it survives below the limit.
        std::fs::write(d.join(".ripped"), b"{}").unwrap();
        let quarantined = handle_resume_fsync_failure("_mux", &d, "mux output");
        assert!(!quarantined, "first failure must not quarantine");
        assert_eq!(staging::restart_count(&d), 1);
        assert!(!d.join(".failed").exists(), "no .failed below the limit");
        assert!(d.join(".ripped").exists(), ".ripped preserved for retry");
    }

    /// M4: once `.restart_count` reaches `RESTART_LIMIT`, the repeated fsync
    /// failure promotes the dir to terminal `.failed`, drops `.ripped` so the
    /// worker can't re-queue it, and clears the counter.
    #[test]
    fn fsync_failure_at_limit_quarantines() {
        let d = tmpdir();
        std::fs::write(d.join(".ripped"), b"{}").unwrap();
        // Pre-seed the count to one below the limit so the next bump trips it.
        staging::write_marker_durable(
            &d.join(".restart_count"),
            format!("{}\n", staging::RESTART_LIMIT - 1).as_bytes(),
        )
        .unwrap();
        let quarantined = handle_resume_fsync_failure("_mux", &d, "mux output");
        assert!(quarantined, "reaching RESTART_LIMIT must quarantine");
        assert!(d.join(".failed").exists(), ".failed written (terminal)");
        assert!(
            !d.join(".ripped").exists(),
            ".ripped dropped so the worker can't re-queue the terminal dir"
        );
        assert_eq!(
            staging::restart_count(&d),
            0,
            ".restart_count cleared after quarantine"
        );
    }
}
