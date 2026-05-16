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
    // sanitization matching exactly post-restart (it does today but
    // CLAUDE.md's hard rule #5 says guard against drift).
    let (iso_path, mapfile_path) = match find_iso_and_mapfile(&hint.dir) {
        Some(p) => p,
        None => return ResumeClass::NotEligible,
    };

    // Mapfile load. A corrupt mapfile means the post-Pass-1 state is
    // ambiguous — fall back to a full re-rip.
    let map = match libfreemkv::disc::mapfile::Mapfile::load(&mapfile_path) {
        Ok(m) => m,
        Err(_) => return ResumeClass::NotEligible,
    };
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
    let lost_secs = bad_bytes as f64 / 8_250_000.0;
    if lost_secs > abort_on_lost_secs as f64 {
        return ResumeClass::NotEligible;
    }

    let display_name = hint
        .dir
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();

    ResumeClass::Remux {
        iso_path,
        mapfile_path,
        display_name,
    }
}

/// Walk a staging dir and find the first `.iso` + matching
/// `.iso.mapfile`. Returns None if either is missing.
fn find_iso_and_mapfile(dir: &Path) -> Option<(PathBuf, PathBuf)> {
    let mut iso = None;
    let mut mapfile = None;
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let p = entry.path();
        let name = p.file_name()?.to_string_lossy().into_owned();
        if name.ends_with(".iso.mapfile") || name.ends_with(".mapfile") {
            mapfile = Some(p);
        } else if name.ends_with(".iso") {
            iso = Some(p);
        }
    }
    Some((iso?, mapfile?))
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

/// The action half of the auto-resume flow.
///
/// Preconditions enforced by the caller:
/// - `classification` is `ResumeClass::Remux { .. }`
/// - the per-device `Halt` token has been (re-)registered by the
///   spawn site (mirrors how `rip_disc` is entered)
///
/// On success: writes `.completed` + clears `.restart_count`. On any
/// failure (scan_image, mux open, mux loop): preserves the partial
/// state and leaves the counter intact so the next-startup pass
/// promotes the dir to `.failed` once `RESTART_LIMIT` is reached.
pub fn resume_remux(cfg: &Arc<RwLock<Config>>, device: &str, classification: ResumeClass) {
    let ResumeClass::Remux {
        iso_path,
        mapfile_path: _mapfile_path,
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
        Err(_) => return,
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

    // 3. Disc::scan_image to recover Disc + titles.
    let scan_opts = match &cfg_read.keydb_path {
        Some(p) => libfreemkv::ScanOptions {
            keydb_path: Some(p.into()),
        },
        None => libfreemkv::ScanOptions::default(),
    };
    use libfreemkv::SectorSource;
    let capacity = iso_reader.capacity_sectors();
    let disc = match libfreemkv::Disc::scan_image(&mut iso_reader, capacity, &scan_opts) {
        Ok(d) => d,
        Err(e) => {
            crate::log::device_log(
                device,
                &format!("Auto-resume aborted: scan_image failed: {}", e),
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
        return;
    }

    // Real-bitrate re-validation: now that we have the actual title,
    // recompute bytes-bad-in-title (vs the classifier's whole-disc
    // estimate) and re-check against abort_on_lost_secs.
    let title = disc.titles[0].clone();
    let title_bytes_per_sec: f64 = {
        let b = title.size_bytes as f64;
        let d = title.duration_secs;
        if b > 0.0 && d > 0.0 {
            b / d
        } else {
            8_250_000.0
        }
    };
    if let Ok(map) = libfreemkv::disc::mapfile::Mapfile::load(&_mapfile_path) {
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
            return;
        }
    }

    // 4. Build MuxInputs + run mux exactly as rip_disc does.
    let disc_format = match disc.format {
        libfreemkv::DiscFormat::Uhd => "uhd",
        libfreemkv::DiscFormat::BluRay => "bluray",
        libfreemkv::DiscFormat::Dvd => "dvd",
        libfreemkv::DiscFormat::Unknown => "unknown",
    }
    .to_string();
    let format = disc.content_format;
    let keys = disc.decrypt_keys();
    let batch = libfreemkv::disc::detect_max_batch_sectors("/dev/null");

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

    let duration = crate::util::format_duration_hm(title.duration_secs);
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
        None => libfreemkv::Halt::new(),
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
            super::unregister_halt(device);
            return;
        }
    };
    let reader: Box<dyn libfreemkv::SectorSource> = Box::new(iso_reader_for_mux);
    let mut input = libfreemkv::DiscStream::new(reader, title, keys, batch, format)
        .with_halt(halt_token.clone());
    // ISO-read demux glitches: same skip-on-error policy as multipass mux.
    input.skip_errors = true;

    let latest_bytes_read = Arc::new(AtomicU64::new(0));
    let rip_last_lba = Arc::new(AtomicU64::new(0));
    let rip_current_batch = Arc::new(AtomicU16::new(batch));
    let wd_last_frame = Arc::new(AtomicU64::new(crate::util::epoch_secs()));
    let mux_input_errors = Arc::new(AtomicU32::new(0));

    let mux_outcome = super::mux::run_mux(
        super::mux::MuxInputs {
            device,
            display_name: display_name.clone(),
            disc_format: disc_format.clone(),
            tmdb_title: String::new(),
            tmdb_year: 0,
            tmdb_poster: String::new(),
            tmdb_overview: String::new(),
            duration: duration.clone(),
            codecs: String::new(),
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
            skip_errors: true,
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
        return;
    }

    // 5. Success — write .completed marker, drop .done hand-off
    // marker for the mover, clear .restart_count. Same shape as the
    // rip_disc completion path so the mover treats this output
    // identically.
    let done_marker = serde_json::json!({
        "title": display_name,
        "format": disc_format,
        "date": crate::util::format_date(),
        "resumed": true,
    });
    let done_path = staging_dir.join(".done");
    let _ = std::fs::write(
        &done_path,
        serde_json::to_string_pretty(&done_marker).unwrap_or_default(),
    );
    staging::write_completed_marker(&staging_dir);
    staging::clear_restart_count(&staging_dir);

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
}

// Tests live in `tests/resume_remux.rs` (integration tests) — they
// pattern-match on the public `ResumeClass` variants and exercise
// `classify_resume` + `delete_partial_output` directly. The deeper
// integration paths (`Disc::scan_image` + `run_mux` against a real
// UDF ISO) are covered by the live test bed only — feeding synthetic
// bytes into `scan_image` reliably fails (CLAUDE.md hard rule re:
// live-drive testing), so unit tests cap at the boundary helpers.
