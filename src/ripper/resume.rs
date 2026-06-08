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
    if stats.bytes_pending != 0 {
        // Pass 1 didn't fully settle the disc (some sectors still
        // NonTried / NonTrimmed / NonScraped) — let the regular rip
        // path resume sweep + retry instead of jumping to mux.
        return ResumeClass::NotEligible;
    }

    // Bad-bytes → estimated title-seconds. Use the same 8.25 Mbps
    // fallback `rip_disc` uses when bitrate is unknown. This is a
    // pre-flight estimate; the actor re-validates with the real
    // title bitrate after `Disc::scan_image`.
    let bad_bytes = stats.bytes_unreadable;
    let lost_secs = bad_bytes as f64 / FALLBACK_BITRATE_BYTES_PER_SEC;
    if lost_secs > abort_on_lost_secs as f64 {
        return ResumeClass::NotEligible;
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
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
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
pub fn resume_remux(cfg: &Arc<RwLock<Config>>, device: &str, classification: ResumeClass) {
    let ResumeClass::Remux {
        iso_path,
        mapfile_path,
        display_name,
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
        let bad_in_title = libfreemkv::disc::bytes_bad_in_title(&title, &bad_ranges);
        let lost_secs = if title_bytes_per_sec > 0.0 {
            bad_in_title as f64 / title_bytes_per_sec
        } else {
            0.0
        };
        if lost_secs > cfg_read.abort_on_lost_secs as f64 {
            crate::log::device_log(
                device,
                &format!(
                    "Auto-resume aborted: title loss {:.2}s exceeds threshold {}s",
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
                    "title loss {:.2}s exceeds threshold {}s",
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
    if disc.encrypted && matches!(keys, libfreemkv::decrypt::DecryptKeys::None) {
        let msg = super::aacs_failure_message(disc.aacs_error.as_ref());
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
    let reader: Box<dyn libfreemkv::SectorSource> = Box::new(iso_reader_for_mux);
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
        None,
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

    let latest_bytes_read = Arc::new(AtomicU64::new(0));
    let rip_last_lba = Arc::new(AtomicU64::new(0));
    let rip_current_batch = Arc::new(AtomicU16::new(batch));
    let wd_last_frame = Arc::new(AtomicU64::new(crate::util::epoch_secs()));
    let mux_input_errors = Arc::new(AtomicU32::new(0));

    // Pick up the TMDB metadata + codecs string that scan_disc
    // populated in STATE before this path was entered. Without this,
    // the mux's per-frame `update_state` would overwrite them with
    // empty strings and the dashboard would lose the poster / title /
    // year / codec badge for the entire mux phase.
    let (tmdb_title, tmdb_year, tmdb_poster, tmdb_overview, state_codecs) = super::STATE
        .lock()
        .ok()
        .and_then(|s| s.get(device).cloned())
        .map(|rs| {
            (
                rs.tmdb_title,
                rs.tmdb_year,
                rs.tmdb_poster,
                rs.tmdb_overview,
                rs.codecs,
            )
        })
        .unwrap_or_default();

    // Title-confidence gate — mirror rip_disc's completion path
    // (mod.rs: `if title_confident { ".done" } else { ".review" }`).
    // Auto-resume previously wrote `.done` unconditionally, auto-filing a
    // resumed rip into the library under a possibly-guessed title and
    // bypassing the operator-review hold the fresh-rip path enforces.
    // Compute confidence the same way: an exact normalized-title match
    // that carries a year, comparing the resolved TMDB title against the
    // disc's own label. No operator-override concept exists on the resume
    // path, so confidence is purely the match check.
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
    let title_confident = crate::tmdb::is_confident_match(
        &crate::tmdb::clean_title(&disc_label),
        &title_for_match,
        tmdb_year,
    );

    let mux_outcome = super::mux::run_mux(
        super::mux::MuxInputs {
            device,
            display_name: display_name.clone(),
            disc_format: disc_format.clone(),
            tmdb_title,
            tmdb_year,
            tmdb_poster,
            tmdb_overview,
            duration: duration.clone(),
            codecs: state_codecs,
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
            max_retries: 0,
            bytes_unreadable_at_mux: 0,
            dest_url,
            batch,
            staging_disc_dir: staging_dir.clone(),
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
        crate::log::device_log(
            device,
            "Auto-resume mux did not complete — preserving partial state for next restart",
        );
        // Reset from "ripping" → "idle" so the next /api/rip isn't
        // blocked by the "already ripping" gate. Halt-via-/api/stop is
        // the common path here. Mirrors rip_disc's stopped → "idle".
        reset_status_after_ripping(device, "idle", &display_name, &disc_format, &duration, None);
        return;
    }

    // 5. Success — write .completed marker, drop the hand-off marker for
    // the mover, clear .restart_count. Same shape as the rip_disc
    // completion path so the mover treats this output identically.
    //
    // Honor the SAME title-confidence gate the fresh-rip path uses: a
    // confident match (.done) hands straight to the mover; a low-confidence
    // match (.review) HOLDS the rip for operator review instead of
    // auto-filing it under a guessed name. Unconditionally writing .done
    // here bypassed that hold for every resumed rip.
    let marker_name = if title_confident { ".done" } else { ".review" };
    let done_marker = serde_json::json!({
        "title": display_name,
        "format": disc_format,
        "year": tmdb_year,
        "date": crate::util::format_date(),
        "resumed": true,
    });
    let done_path = staging_dir.join(marker_name);
    if let Err(e) = std::fs::write(
        &done_path,
        serde_json::to_string_pretty(&done_marker).unwrap_or_default(),
    ) {
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

    super::update_state(
        device,
        super::RipState {
            device: device.to_string(),
            status: "done".to_string(),
            disc_present: true,
            disc_name: display_name,
            disc_format,
            progress_pct: 100,
            output_file: staging_str,
            duration,
            ..Default::default()
        },
    );
    crate::log::device_log(device, "Auto-resume complete");

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
pub(crate) fn remux_from_ripped_marker(
    cfg: &Arc<RwLock<Config>>,
    staging_dir: &std::path::Path,
    marker: &crate::muxer::RippedMarker,
) -> bool {
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
            ..Default::default()
        },
    );

    let classification = ResumeClass::Remux {
        iso_path: iso_path.clone(),
        mapfile_path: mapfile_path.clone(),
        display_name: marker.display_name.clone(),
    };
    resume_remux(cfg, mux_device, classification);

    // Success signal: `resume_remux` wrote `.completed` to staging.
    // Anything else (halt, scan_image failure, mux loop break)
    // leaves `.completed` absent.
    let success = staging_dir.join(".completed").exists();
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
        // Clean up the synthetic STATE entry so the device tile grid
        // (which already filters underscore keys, but still — be tidy)
        // doesn't accumulate per-mux ghosts.
        if let Ok(mut s) = super::STATE.lock() {
            s.remove(mux_device);
        }
    }
    success
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
}
