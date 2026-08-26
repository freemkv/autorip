//! Live-drive rip orchestration — `rip_disc`.
//!
//! This is the single high-level entry point that drives a real
//! `libfreemkv::Drive` end to end (open → scan → key-resolve → sweep →
//! mux → move → eject). It hardcodes the physical drive session and
//! therefore cannot execute under CI / unit tests; the logic here is
//! validated on the live test bed. It was lifted verbatim out of
//! `mod.rs` (a pure mechanical move, zero behavior change) so the
//! untestable live-drive span can be excluded from coverage while the
//! tested helper functions stay in `mod.rs`.
//!
//! All shared types and helpers resolve through the parent module via
//! the glob below; `rip_disc` is called only from `handle_rip_request`
//! in `mod.rs`.

#[allow(unused_imports)]
use super::*;

/// Rip a disc. Reuses the existing drive session from scan_disc.
/// If no session exists, opens fresh (for on_insert=rip).
///
/// `resume_sweep` continues an existing partial sweep: when true, Pass 1's
/// first attempt runs with libfreemkv `SweepOptions.resume = true`, so the
/// existing ISO + mapfile are kept and only the missing (NonTrimmed /
/// non-tried) ranges are read. When false, Pass 1 starts fresh (the mapfile
/// is recreated and the ISO truncated) — the classic full sweep.
pub fn rip_disc(cfg: &Arc<RwLock<Config>>, device: &str, device_path: &str, resume_sweep: bool) {
    // The poll-loop spawn site already registered a fresh `Halt` for
    // this device (so an HTTP stop during scan has something to flip).
    // Replace it with a Halt backed by the drive's halt-flag once the
    // drive is open below — that way Stop also pre-empts in-flight
    // `Drive::read` calls inside libfreemkv. The swap CARRIES a Stop that
    // landed on the spawn-site token (see `install_rip_halt`); it cannot
    // carry a stale one from a prior rip, because `HaltGuard` unregisters
    // on every exit and the spawn site registers fresh per attempt.
    install_rip_halt(device);

    // RAII cleanup for the halt-map entry. Every exit path from `rip_disc`
    // (including the many early returns on scan/open/keys/staging errors)
    // must drop this device's `Halt` so a subsequent rip starts with a
    // fresh token; leaking it on an error path was the v0.13.6 class of
    // bug. Holding the guard for the function's whole body guarantees the
    // `unregister_halt` runs on return, panic, and `?`-style early exits
    // alike. `unregister_halt` is idempotent (a `HashMap::remove`), so it
    // composes safely with the eject path that also unregisters.
    let _halt_guard = HaltGuard {
        device: device.to_string(),
    };

    // NOTE: the per-device log is archived/cleared at SCAN start
    // (`scan_disc`), NOT here. A rip always follows a scan for this attempt, so
    // clearing again on rip start would wipe the scan context (disc identity,
    // the unlocker matrix, key resolution) the operator wants to see carry into
    // the rip. One clear per attempt, at scan.

    // Snapshot the Config struct (it's Clone) and drop the read guard
    // immediately. Holding the guard across the rip body would block
    // any settings POST (Auto Eject, on_read_error, max_retries, …)
    // for the rip's full duration, and Linux's writer-priority RwLock
    // would queue all subsequent GETs behind the pending writer —
    // the live observed bug where /api/settings, /api/history, and
    // /api/system stopped responding mid-rip until the rip ended.
    let cfg_read = match cfg.read() {
        Ok(c) => c.clone(),
        Err(_) => {
            // `_halt_guard` still unregisters the Halt token on return.
            mark_config_lock_poisoned(device, "Rip");
            return;
        }
    };

    // Preserve UI state
    let prev = STATE.lock().ok().and_then(|s| s.get(device).cloned());
    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "scanning".to_string(),
            disc_present: true,
            disc_name: prev
                .as_ref()
                .map(|p| p.disc_name.clone())
                .unwrap_or_default(),
            disc_format: prev
                .as_ref()
                .map(|p| p.disc_format.clone())
                .unwrap_or_default(),
            tmdb_title: prev
                .as_ref()
                .map(|p| p.tmdb_title.clone())
                .unwrap_or_default(),
            tmdb_year: prev.as_ref().map(|p| p.tmdb_year).unwrap_or(0),
            tmdb_poster: prev
                .as_ref()
                .map(|p| p.tmdb_poster.clone())
                .unwrap_or_default(),
            tmdb_overview: prev
                .as_ref()
                .map(|p| p.tmdb_overview.clone())
                .unwrap_or_default(),
            ..Default::default()
        },
    );

    // Take the existing session, or open fresh
    let mut session = match take_session(device) {
        Some(s) if s.scanned => {
            crate::log::device_log(device, "Reusing drive session");
            s
        }
        existing => {
            // No session or not scanned — open fresh
            if existing.is_some() {
                drop_session(device);
            }
            crate::log::device_log(device, "Opening drive...");
            let mut drive = match libfreemkv::Drive::open(std::path::Path::new(device_path)) {
                Ok(d) => d,
                Err(e) => {
                    let msg = format_lib_error("Cannot open drive", &e);
                    crate::log::device_log(device, &msg);
                    update_state(
                        device,
                        RipState {
                            device: device.to_string(),
                            status: "error".to_string(),
                            last_error: msg,
                            ..Default::default()
                        },
                    );
                    return;
                }
            };
            if let Err(e) = drive.wait_ready() {
                tracing::warn!(device = %device, error = %e, "drive wait_ready failed (continuing)");
            }
            crate::log::device_log(device, "Initializing...");
            if let Err(e) = drive.init() {
                tracing::warn!(device = %device, error = %e, "drive init failed (continuing)");
            }
            // Engage the drive's disc-type read mode before any read. Idempotent.
            if let Err(e) = drive.probe_disc() {
                tracing::warn!(device = %device, error = %e, "drive probe_disc failed (continuing)");
            }

            let scan_opts = scan_opts_for(&cfg_read);
            crate::log::device_log(device, "Scanning titles...");
            // Scan-phase watchdog (same as scan_disc): WARNs every 15s while
            // scan/resolve runs, torn down by the drop-guard.
            let scan_wd = ScanWatchdog::arm(device);
            let scan_t0 = std::time::Instant::now();
            tracing::info!(device = %device, "scan: begin");
            let disc = match libfreemkv::Disc::scan(&mut drive, &scan_opts) {
                Ok(d) => d,
                Err(e) => {
                    let msg = format_lib_error("Disc scan", &e);
                    crate::log::device_log(device, &msg);
                    update_state(
                        device,
                        RipState {
                            device: device.to_string(),
                            status: "error".to_string(),
                            last_error: msg,
                            ..Default::default()
                        },
                    );
                    return;
                }
            };
            tracing::info!(device = %device, elapsed_ms = scan_t0.elapsed().as_millis() as u64, "scan: structure done");
            // DVD is CSS (resolved in scan) — skip the AACS key-resolution path
            // entirely; it doesn't apply and reads the disc as if it were UHD.
            let disc = if matches!(disc.format, libfreemkv::DiscFormat::Dvd) {
                tracing::info!(device = %device, "resolve_keys: skipped (DVD/CSS — no AACS)");
                disc
            } else {
                scan_wd.enter_resolve();
                let (disc, _key_outcome) = resolve_keys_from_drive(&cfg_read, &mut drive, disc);
                disc
            };
            drop(scan_wd);

            let disc_name = disc
                .meta_title
                .as_deref()
                .unwrap_or(&disc.volume_id)
                .to_string();

            let tmdb = crate::tmdb::lookup(&disc_name, &cfg_read.tmdb_api_key);

            DriveSession {
                drive,
                disc: Some(disc),
                scanned: true,
                probed: false,
                tmdb,
                device_path: device_path.to_string(),
            }
        }
    };

    let mut disc = match session.disc.take() {
        Some(d) => d,
        None => {
            tracing::error!(
                device = %device,
                "DriveSession had no disc — every code path that builds a session must set Some(disc); reaching this branch is a logic bug"
            );
            crate::log::device_log(device, "Internal error: session has no disc");
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "error".to_string(),
                    last_error: "Internal error: session has no disc".to_string(),
                    ..Default::default()
                },
            );
            drop_session(device);
            return;
        }
    };

    let disc_name = disc
        .meta_title
        .as_deref()
        .unwrap_or(&disc.volume_id)
        .to_string();
    let disc_format = match disc.format {
        libfreemkv::DiscFormat::Uhd => "uhd",
        libfreemkv::DiscFormat::Fmts => "fmts",
        libfreemkv::DiscFormat::BluRay => "bluray",
        libfreemkv::DiscFormat::HdDvd => "hddvd",
        libfreemkv::DiscFormat::Dvd => "dvd",
        libfreemkv::DiscFormat::Unknown => "unknown",
    }
    .to_string();
    // Pass 1 reads the WHOLE DISC (not a single title), so the total must be
    // disc.capacity_bytes — using titles[0].size_bytes (the chosen movie's
    // duration-weighted size estimate) was the v0.13.12 bug that made the UI
    // show "0.0 GB / 0.0 GB" during Pass 1. Mux phase below already
    // re-derives its own total from the input stream, so we don't lose that.
    let total_bytes = if disc.capacity_bytes > 0 {
        disc.capacity_bytes
    } else {
        disc.titles.first().map(|t| t.size_bytes).unwrap_or(0)
    };

    // An operator title override (set from the Ripper card's "✎ change" picker
    // before clicking Rip) takes precedence over the scan's auto-match — the rip
    // then files under the operator's pick. Taken once; falls back to the scan
    // result. A picked title is trusted (treated as confident → no review hold).
    let title_override = take_title_override(device);
    let overridden = title_override.is_some();
    let tmdb_owned: Option<crate::tmdb::TmdbResult> =
        title_override.or_else(|| session.tmdb.clone());
    let tmdb = &tmdb_owned;
    let tmdb_title = tmdb.as_ref().map(|t| t.title.clone()).unwrap_or_default();
    let tmdb_year = tmdb.as_ref().map(|t| t.year).unwrap_or(0);
    let tmdb_poster = tmdb
        .as_ref()
        .map(|t| t.poster_url.clone())
        .unwrap_or_default();
    let tmdb_overview = tmdb
        .as_ref()
        .map(|t| t.overview.clone())
        .unwrap_or_default();
    // Cloned for use in the finalize block (history record) — after multipass
    // we drop `session` to release the drive, so we can't borrow session.tmdb
    // at the tail of this function.
    // No TMDB result → the EMPTY string, which is the mover's documented
    // no-match sentinel: `routing_media_type` coalesces "" to the movie root and
    // the mover/muxer tests assert exactly that ("" routes as a movie). A literal
    // "unknown" here is neither empty nor "movie"/"tv", so it fell through to the
    // output-root dump — a no-TMDB-key rip (a supported mode) never reached the
    // movie library.
    let tmdb_media_type = tmdb
        .as_ref()
        .map(|t| t.media_type.clone())
        .unwrap_or_default();
    // TMDB numeric id (0 = no match). Persisted in the hand-off marker so a
    // consumer can enrich metadata by id later; the mover reads it back.
    let tmdb_id = tmdb.as_ref().map(|t| t.tmdb_id).unwrap_or(0);

    let display_name = if tmdb_title.is_empty() {
        disc_name.clone()
    } else {
        tmdb_title.clone()
    };
    // Confident = an exact title match WITH a year. Carried to the finalize block
    // to decide auto-file (.done) vs hold-for-review (.review). disc_name is the
    // disc's volume label; display_name is the resolved (TMDB) title.
    //
    // When TMDB is NOT configured (no API key) no rip can ever produce a
    // confident match, so every rip would land in `.review` and never
    // auto-file. Operators running without a TMDB key expect the disc-label
    // filename, not a review hold. Treat "no API key" as confident so the rip
    // files under the disc label and writes `.done`. The review hold is
    // preserved ONLY when TMDB IS configured but returns low confidence.
    let title_confident = title_is_confident(
        &cfg_read.tmdb_api_key,
        overridden,
        &disc_name,
        &display_name,
        tmdb_year,
    );

    crate::log::device_log(
        device,
        &format!(
            "Disc: {} ({}, {} titles)",
            disc_name,
            disc_format,
            disc.titles.len()
        ),
    );

    if disc.titles.is_empty() {
        crate::log::device_log(device, "No titles found");
        update_state(
            device,
            RipState {
                device: device.to_string(),
                status: "error".to_string(),
                last_error: "No titles".to_string(),
                ..Default::default()
            },
        );
        return;
    }

    let duration = crate::util::format_duration_hm(disc.titles[0].duration_secs);
    let codecs = format_codecs(&disc.titles[0]);
    let title = disc.titles[0].clone();

    // Down-vs-no-key (rip path): if the ONLINE key service left this encrypted
    // disc keyless, that may be a transient service outage (502 / timeout), not
    // a genuinely missing key. Classify the service's reachability and bounded-
    // retry resolution before we permanently fail the rip. A recovered service
    // resolves keys here and the rip proceeds; a persistent outage yields
    // `Some(reach)` and is parked below in a retryable/pending state instead of
    // the old permanent "no keys found" error. No-op unless online + keyless;
    // capture-without-keys keeps its ISO-now path untouched.
    let mut key_outage: Option<crate::keysource::ServiceReachability> = None;
    if should_retry_online_keys(
        crate::keysource::uses_online(&cfg_read),
        cfg_read.capture_without_keys,
        disc.encrypted,
        matches!(disc.decrypt_keys(), libfreemkv::decrypt::DecryptKeys::None),
    ) {
        let (rdisc, _outcome, reach) =
            retry_online_keys_on_outage(device, &cfg_read, &mut session.drive, disc);
        disc = rdisc;
        key_outage = reach;
    }

    // Base decode keys. `mut` because the shared FMTS pre-decode step below may
    // re-derive them after banking forensic index keys onto the disc, so BOTH
    // rip paths (single-pass feeds `keys` straight into the inline reader) see
    // the complete key pool.
    let mut keys = disc.decrypt_keys();

    // No-keys decision. An encrypted disc with no usable keys can still be swept
    // to a raw ISO (the sweep uses `decrypt: false`); only the MUX needs keys.
    // The operator's `capture_without_keys` toggle decides what happens:
    //   * enabled  → capture to ISO now, defer the mux until keys are available
    //                (the mux is skipped below; staging is preserved for resume).
    //   * disabled → don't rip; surface the explicit reason and stop here.
    let keys_missing = disc.encrypted && matches!(keys, libfreemkv::decrypt::DecryptKeys::None);
    if keys_missing {
        // A persistent online-key-service outage is NOT a missing key: park the
        // disc in a retryable/pending state (status idle, distinct message) so a
        // later insert / rescan retries — do not permanently fail, do not eject.
        if let Some(reach) = key_outage {
            let status_msg = key_service_transient_status(reach).unwrap_or(KEY_SERVICE_DOWN_STATUS);
            crate::log::device_log(device, &format!("Not ripping now — {status_msg}"));
            update_state_with(device, |s| {
                s.status = "idle".to_string();
                s.key_status = status_msg.to_string();
                s.last_error = status_msg.to_string();
            });
            unregister_halt(device);
            return;
        }
        let msg = keyless_failure_message(&disc);
        if cfg_read.capture_without_keys {
            crate::log::device_log(
                device,
                &format!(
                    "{msg}\nNo keys yet — capturing to ISO; mux deferred until keys are available."
                ),
            );
        } else {
            crate::log::device_log(
                device,
                &format!(
                    "{msg}\nNo keys — not ripping. Enable \"capture without keys\" to save an ISO for later."
                ),
            );
            update_state_with(device, |s| {
                s.status = "error".to_string();
                s.last_error = format!("No keys — not ripping. {msg}");
            });
            unregister_halt(device);
            return;
        }
    }

    // Probe for speed — only needed for rip, not scan
    if !session.probed {
        crate::log::device_log(device, "Probing disc speed...");
        let _ = session.drive.probe_disc();
        session.probed = true;
    }

    // Detect the kernel-reported max batch size (aligned to AACS unit
    // boundaries). Fall back to libfreemkv's documented default of 60
    // sectors if detection fails. Pre-fix this was hardcoded to 1,
    // which:
    //   - made the API display `current_batch: 1` (misleading — it
    //     suggested the rip was reading sector-by-sector during sweep,
    //     but the actual sweep batch is determined inside libfreemkv's
    //     freemkv_engine::sweep and is unaffected by this value)
    //   - made the mux phase read the ISO **one sector at a time**
    //     (2 KB chunks) via DiscStream::new(reader, title, keys, batch,
    //     format) — a real perf bug on the mux read path
    let batch = libfreemkv::disc::detect_max_batch_sectors(device_path);
    let format = disc.content_format;

    let output_format = cfg_read.output_format.clone();

    // `output_format == "iso"` means "capture the whole disc image", and its
    // abort accounting is whole-disc scoped: every unreadable sector counts,
    // including scratched menus / trailers OUTSIDE any title's extents (see the
    // multi-pass pre-mux gate and `abort_lost_ms`). Single-pass mode streams
    // only the selected title's sectors straight to the muxer — it never reads
    // (let alone recovers) out-of-title sectors and produces no whole-disc ISO.
    // So single-pass cannot honour ISO semantics at all: there is no whole-disc
    // image to hand the operator, and its rip-phase loss accounting is in-title
    // only, so out-of-title damage that the multi-pass / resume ISO paths would
    // capture (and gate on, whole-disc, under `abort_on_lost_secs=0`) is never
    // seen. Refuse the incoherent combination up front and point the operator
    // at multi-pass (the only path that captures a real whole-disc ISO and
    // applies whole-disc loss accounting), mirroring the single-pass no-keys
    // guidance below.
    if iso_output_needs_multipass(&output_format, cfg_read.max_retries) {
        crate::log::device_log(
            device,
            "ISO output requires multi-pass mode — single-pass streams only the \
             selected title and cannot capture a whole-disc image. Enable multi-pass \
             mode (Retry Passes > 0) to rip an ISO.",
        );
        update_state_with(device, |s| {
            s.status = "error".to_string();
            if s.last_error.is_empty() {
                s.last_error =
                    "ISO output requires multi-pass mode (enable Retry Passes).".to_string();
            }
        });
        unregister_halt(device);
        return;
    }

    let ext = output_extension_for(&output_format, &disc);

    // `disc_name` is the RAW volume label (meta_title, else volume_id) — the
    // same value `DiscId::name()` gives the scan path, and the only thing that
    // distinguishes two discs of a boxset behind their one shared TMDB title.
    // The dir this resolves to must agree exactly with the one
    // `disc_already_completed` / `find_resumable_for_disc` looked at a moment
    // ago, which is why both go through `staging::staging_basename`.
    let staging = cfg_read.staging_device_dir(&staging::staging_basename(
        std::path::Path::new(&cfg_read.staging_dir),
        &display_name,
        &disc_name,
    ));
    if let Err(e) = std::fs::create_dir_all(&staging) {
        // Bail loudly instead of pressing on: a missing staging dir
        // makes the free-space preflight skip its check and the sweep
        // later dies with a confusing ENOENT/EACCES far from the cause.
        crate::log::device_log(device, &format!("Cannot create staging dir {staging}: {e}"));
        update_state_with(device, |s| {
            s.status = "error".to_string();
            if s.last_error.is_empty() {
                s.last_error = format!("cannot create staging dir: {e}");
            }
        });
        unregister_halt(device);
        return;
    }
    // Stamp the dir with this disc's raw volume label so the NEXT disc of the
    // same boxset — same TMDB title, different disc — is routed to its own dir
    // instead of reading this one's `.completed` and being skipped unread.
    // Also adopts a legacy dir that predates labels: it matches every disc
    // until someone claims it, and the disc ripping into it now is the owner.
    // Never overwrites an existing, different label (see `adopt_disc_label`).
    staging::adopt_disc_label(std::path::Path::new(&staging), &disc_name);
    // Write the `.sweeping` in-progress marker immediately after the staging
    // dir exists, before Pass 1. This governs the whole multi-hour sweep+patch
    // window: without it the dir has only ISO+mapfile until `.ripped`, so a
    // crash mid-sweep leaves it ungoverned — the startup resume scan would
    // restart-count a healthy long rip toward `.failed`, and the mover would
    // WARN-flood every 10s tick on the absent `.done`. Replaced by `.ripped`
    // (hand-off) or `.failed` (abort) on every exit path below.
    staging::write_sweeping_marker(std::path::Path::new(&staging));
    // RAII cleanup for the `.sweeping` marker. The terminal-marker writers
    // clear it first, so this is a no-op on success/`.ripped`/`.failed`
    // paths and only fires on the early-return error branches and panic,
    // preventing a stale `.sweeping` from stranding the dir `InProgress`
    // across restarts.
    let _sweeping_guard = SweepingGuard {
        staging: std::path::PathBuf::from(&staging),
    };
    // FILE names inside the staging dir, NOT the dir basename — deliberately
    // plain `sanitize(display_name)`, with no `_2` disc suffix. The dir already
    // separates the discs, so these are unambiguous on disk, and two things
    // break if they take the suffix: `resume::delete_partial_output` looks for
    // `<dir>/<display_name>.<ext>` when clearing a partial mux, and the mover's
    // TV/fallback branches derive the DELIVERED filename from the staged
    // filename (the movie branch rebuilds it from the TMDB title). Whether the
    // suffix should reach output naming is a separate decision — see the mover.
    let filename = format!(
        "{}.{}",
        crate::util::sanitize_path_compact(&display_name),
        ext
    );
    let output_path = format!("{}/{}", staging, filename);
    // Intermediate-ISO + mapfile-sidecar paths for multipass mode, derived
    // once here from `staging` + `display_name`. Only the `max_retries > 0`
    // branch writes/reads these; single-pass rips never produce an ISO. They
    // were previously rebuilt at ~5 sites scattered through this function.
    // Plain title, no disc suffix — same reasoning as `filename` above.
    let iso_filename = format!("{}.iso", crate::util::sanitize_path_compact(&display_name));
    let iso_path_str = format!("{staging}/{iso_filename}");
    let mapfile_path_str = format!("{iso_path_str}.mapfile");
    let dest_url = if staging::is_network_output(&output_format, &cfg_read.network_target) {
        format!("network://{}", cfg_read.network_target)
    } else {
        // Scheme is the container (mkv/m2ts), NOT the filename extension: a 3D rip
        // writes `Title.mk3d` but must still mux through `mkv://` (no `mk3d://` scheme).
        format!("{}://{}", output_scheme_for(&output_format), output_path)
    };

    crate::log::device_log(device, &format!("Ripping {} to {}", display_name, filename));

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "ripping".to_string(),
            disc_present: true,
            disc_name: display_name.clone(),
            // Set explicitly rather than relying on `update_state`'s carry —
            // an operator title override changes `disc_name` mid-flight, which
            // (correctly) suppresses the carry, and the label must survive it.
            disc_label: disc_name.clone(),
            disc_format: disc_format.clone(),
            output_file: filename.clone(),
            tmdb_title: tmdb_title.clone(),
            tmdb_year,
            tmdb_poster: tmdb_poster.clone(),
            tmdb_overview: tmdb_overview.clone(),
            duration: duration.clone(),
            codecs: codecs.clone(),
            ..Default::default()
        },
    );

    // Per-title bitrate for lost-video-time math. Falls back to 66 Mbps
    // (sustained BD) if the scanner didn't populate size_bytes/duration.
    let title_bytes_per_sec: f64 = {
        let b = title.size_bytes as f64;
        let d = title.duration_secs;
        if b > 0.0 && d > 0.0 {
            b / d
        } else {
            resume::FALLBACK_BITRATE_BYTES_PER_SEC
        }
    };

    // Shared state read by event callbacks (no &mut self) and the main
    // rip loop (which copies atomics into RipState every ~1s). The watchdog
    // timestamp is updated on ANY sector-level event — not just frame writes —
    // so a long run of skipped sectors doesn't falsely register as "stalled".
    let wd_last_frame = Arc::new(AtomicU64::new(crate::util::epoch_secs()));
    let latest_bytes_read = Arc::new(AtomicU64::new(0));
    let rip_last_lba = Arc::new(AtomicU64::new(0));
    let rip_current_batch = Arc::new(AtomicU16::new(batch));

    // Wire the drive's halt-flag into the per-device `Halt` token.
    // Before this point the registered token was a placeholder
    // (allocated at the top of `rip_disc` so a stop click had
    // *something* to cancel); now we swap it for a `Halt` that views
    // the same `Arc<AtomicBool>` the drive's internal recovery loops
    // poll on — so `device_halt(device).cancel()` simultaneously
    // propagates to libfreemkv's `Drive::read` and every phase loop
    // here in autorip that holds a `halt.clone()`.
    let drive_halt_arc = session.drive.halt_flag();
    let halt_token = libfreemkv::Halt::from_arc(drive_halt_arc.clone());
    // Carry a Stop that landed on the OLD (placeholder) token in the
    // window between the dispatch-site cancellation check and this swap
    // into the new token. Without this, the first stop click would
    // cancel a token nobody reads again and silently no-op (the user
    // would have to click again). The check+insert+carry is done under a
    // single HALTS-lock acquisition so a concurrent /api/stop landing during
    // the swap can't be lost (TOCTOU).
    swap_halt_carrying_cancel(device, halt_token.clone());
    // Local alias: pre-existing call sites refer to `halt` as the
    // legacy `Arc<AtomicBool>`. Keep the same name so the watcher
    // helpers (which still take `Arc<AtomicBool>`) compile unchanged
    // — this is a deprecated bridge, dropped together with
    // `freemkv_engine::sweep()` in round 3.
    let halt = drive_halt_arc;

    // Rip-level wallclock watcher. Historically capped the ENTIRE rip at
    // max(disc_runtime, 1h); the cap itself was removed 2026-06-04 (the
    // watcher now just exits silently when the budget elapses — see the
    // body below). Kept as a no-op poll loop that bails cleanly on
    // rip_complete / halt. Configurable via MAX_RIP_DURATION_SECS.
    // Snapshot every cfg field the rip needs upfront, then drop the read
    // lock immediately. Pre-fix this binding shadowed the outer `cfg`
    // RwLock<Config> with the read guard for the entire `rip_disc` body,
    // holding the lock for the whole 60+ minute rip. The settings POST
    // handler takes a write lock, so a user toggling Auto Eject (or any
    // setting) hung on `cfg.write()` for the duration; once a writer is
    // queued, Linux's writer-priority RwLock blocks subsequent reads
    // too — so `/api/settings`, `/api/history`, `/api/system` all stop
    // responding until the rip ends. `/api/state` survived because it
    // uses a separate lock.
    let (rip_budget_secs, transport_recovery_delay_secs) = {
        // Recover the guard if the RwLock is poisoned (a settings writer
        // panicked mid-write) rather than unwrapping and killing the rip
        // thread — the snapshotted config values are still valid to read.
        // Every other cfg read in this file degrades gracefully; this was
        // the lone `.unwrap()`.
        let c = cfg.read().unwrap_or_else(|e| e.into_inner());
        (c.max_rip_duration_secs, c.transport_recovery_delay_secs)
    };
    // Rip-level wallclock watcher. Cancellable via `rip_complete` —
    // when the main rip thread finishes (success or graceful eject),
    // it flips this flag and the watcher exits silently. Without this,
    // the thread sleeps blindly for `rip_budget_secs` and fires the
    // "budget exceeded" warning long after the rip already succeeded
    // — empirically (2026-05-11): rip done at 13:27, false warning
    // at 13:31. Now: poll every 5s, bail early when
    // rip_complete is set.
    let halt_rip_watcher = halt.clone();
    let device_rip_watcher = device.to_string();
    let rip_complete = Arc::new(AtomicBool::new(false));
    let rip_complete_watcher = rip_complete.clone();
    let _rip_watcher_guard = std::thread::spawn(move || {
        tracing::info!(
            device = %device_rip_watcher,
            rip_budget_secs,
            "Rip-level wallclock watcher started"
        );
        let start = std::time::Instant::now();
        let budget = std::time::Duration::from_secs(rip_budget_secs);
        while start.elapsed() < budget {
            // Coarse poll — 5s granularity is fine for a multi-hour
            // budget. Smaller intervals would just burn wakeups.
            std::thread::sleep(std::time::Duration::from_secs(5));
            if rip_complete_watcher.load(std::sync::atomic::Ordering::Relaxed) {
                // Rip ended on its own. Exit silently — no warning,
                // no halt flag mutation. The rip succeeded (or was
                // halted by some other path that already set state).
                return;
            }
            if halt_rip_watcher.load(std::sync::atomic::Ordering::Relaxed) {
                // External halt (user, transport failure, etc.).
                // Same exit: don't double-warn.
                return;
            }
        }
        // Arbitrary whole-rip time cap REMOVED (2026-06-04). A rip stops on
        // failure or pass exhaustion, never on a wall-clock: `passes = N` is the
        // budget for recovering not-good data, and libfreemkv's own
        // progress/stall watchdogs catch a genuinely stuck pass. The watcher
        // no longer fires a halt when the (legacy) budget elapses — it just
        // exits. The loop above still bails cleanly on rip_complete / halt.
    });
    // Drop guard that signals rip_complete on scope exit (rip
    // function returns). The watcher polls this and exits cleanly.
    struct RipCompleteGuard(Arc<AtomicBool>);
    impl Drop for RipCompleteGuard {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }
    let _rip_complete_guard = RipCompleteGuard(rip_complete);

    // Per-pass user-stop forwarding. The per-pass wall-clock cap was
    // removed 2026-06-04: a pass is bounded by its own work +
    // libfreemkv's failure/stall watchdogs, never an arbitrary clock, so
    // MIN_PASS_BUDGET_SECS no longer gates anything here.
    struct WallclockGuard(Arc<AtomicBool>);
    impl Drop for WallclockGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Relaxed);
        }
    }
    // Per-pass user-stop forwarder. Returns a guard that, on drop, stops
    // the watcher thread. While alive it only forwards a user stop
    // (`user_halt`) into the per-pass `pass_halt` flag. The per-pass
    // wall-clock cap was REMOVED (2026-06-04): a pass is bounded by its
    // own work + libfreemkv's failure/stall watchdogs, never an arbitrary
    // clock — so this is no longer a "watcher", just a halt bridge.
    fn spawn_pass_watcher(
        pass_halt: Arc<AtomicBool>,
        user_halt: Arc<AtomicBool>,
    ) -> WallclockGuard {
        let active = Arc::new(AtomicBool::new(true));
        let active_for_watcher = active.clone();
        std::thread::spawn(move || {
            while active_for_watcher.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_secs(2));
                if !active_for_watcher.load(Ordering::Relaxed) {
                    return;
                }
                if user_halt.load(Ordering::Relaxed) {
                    pass_halt.store(true, Ordering::Relaxed);
                    return;
                }
                if pass_halt.load(Ordering::Relaxed) {
                    return;
                }
            }
        });
        WallclockGuard(active)
    }
    // The user-stop halt — the existing flag. Pass-specific halts forward
    // from this via spawn_pass_watcher. Renamed locally for clarity.
    let user_halt = halt.clone();

    // Drive-level events: any one means something is happening, so the
    // handler resets the watchdog so the "stalled" timer doesn't monotonically
    // climb while the library works through recovery. See
    // [`make_drive_event_fn`].
    session.drive.on_event(make_drive_event_fn(
        device.to_string(),
        wd_last_frame.clone(),
        latest_bytes_read.clone(),
    ));
    // Multi-pass vs direct flow.
    //
    // When max_retries > 0, we go through an ISO intermediate: freemkv_engine::sweep writes
    // the disc to an ISO (fast skip-forward on failure, ddrescue-style mapfile),
    // then freemkv_engine::patch retries the bad ranges up to max_retries times, then the
    // mux pipeline reads from the ISO (no drive involvement past this point).
    //
    // When max_retries == 0, we keep the existing direct disc→MKV flow —
    // session.drive is passed to DiscStream::new and sectors stream straight
    // through decrypt/demux/mux. Fastest path, no ISO overhead, but no retry.
    // Lifted out of the multipass branch below so the mux progress loop
    // (which lives in the outer scope) can reference it. Single-pass mode
    // (max_retries == 0) has no multipass concept; the mux loop's checks
    // gate on `total_passes > 0` before threading it into UI state.
    let total_passes: u8 = plan_passes(cfg_read.max_retries).total_passes;
    // Captured from the multipass branch so the mux call site (which is
    // outside that branch) can pass it into MuxInputs for total-progress
    // weighting. Stays at 0 in direct (single-pass) mode — the mux's
    // total_pct helper falls through to mux-pct passthrough when
    // max_retries == 0 anyway.
    let mut bytes_unreadable_at_mux: u64 = 0;
    // Damage snapshot from the final sweep/patch pass, carried forward into
    // every mux-phase push_state call so /api/state damage fields don't
    // zero out the moment mux starts. Defaults (all-zero) for direct mode.
    let mut sweep_damage_snapshot = mux::SweepDamageSnapshot::default();
    // In-title-scoped loss computed by the abort gate (abort_lost_ms).
    // Hoisted here so the final status=done update can use the same
    // in-title value the abort check used, instead of recomputing from
    // whole-disc bytes_unreadable (which inflates the 'done' card when
    // menus/trailers outside title extents are scratched).
    // 0.0 in single-pass mode or when no unreadable sectors exist.
    let mut main_lost_ms_for_history_outer = 0.0f64;

    // Retained FMTS forensic key map — set by the shared pre-decode gate BELOW
    // (which runs for BOTH rip paths, before the workflow split) and read by the
    // live single-pass inline reader after the split. Declared here to bridge
    // those scopes. `None` for every non-FMTS disc.
    let mut fmts_key_map: Option<std::sync::Arc<libfreemkv::decrypt::AacsKeyMap>> = None;
    // FMTS CaptureOnly deferral flag. Set true by the pre-decode gate below when it
    // resolved an INCOMPLETE forensic key map but the operator opted into capture-
    // without-keys. Base AACS keys are present (so `keys_missing` is false and the
    // no-keys mux-skip won't fire), yet muxing now would emit forensic garbage — so
    // both mux-skip points honour this to defer the mux and preserve the ISO,
    // mirroring the no-keys capture flow. `false` for every other disc.
    let mut defer_forensic_mux = false;
    // On-decrypt-miss key fetch. Online/sample-driven sources resolve only the
    // CPS units sampled up front; when a read hits an orphan unit no held key
    // opens, this asks the SAME key sources with that unit's ciphertext, caches
    // the returned key, and retries — recovering an orphan CPS unit (e.g. a
    // bonus clip not reachable from any playlist) instead of hard-failing the
    // read. `None` for non-AACS discs. Built once, shared (cloned Arc) into the
    // sweep and patch read paths; the mux reads main-title (already-resolved)
    // extents only, so it doesn't need it. Only its consumers are the FMTS gate
    // below and the multipass sweep/patch, so skip building it (and its live-drive
    // MKB read) for a single-pass non-FMTS disc, where nothing reads it.
    let key_fetch: Option<libfreemkv::sector::KeyFetch> = disc
        .inputs()
        .filter(|_| {
            disc.format == libfreemkv::DiscFormat::Fmts || uses_multipass(cfg_read.max_retries)
        })
        .map(|mut inputs| {
            // The live scan reads the MKB for the up-front resolve but does NOT
            // retain it on the disc state, so `disc.inputs()` carries an EMPTY
            // MKB. An online key service NEEDS the MKB to derive an orphan unit's
            // key (decode rejects `mkb=0` with 404). Read it once here so every
            // refetch request carries the full inf+MKB, exactly like the up-front
            // resolve did. One drive read at rip start; `inf` filled too if absent.
            if inputs.mkb.is_empty()
                && let Ok((inf, mkb, _version)) =
                    libfreemkv::Disc::read_aacs_inputs_from_drive(&mut session.drive)
            {
                if inputs.unit_key_ro.is_empty() {
                    inputs.unit_key_ro = inf;
                }
                inputs.mkb = mkb;
            }
            let cfg = Arc::clone(cfg);
            let make: std::sync::Arc<
                dyn Fn() -> Vec<Box<dyn libfreemkv::keysource::KeySource>> + Send + Sync,
            > = std::sync::Arc::new(move || {
                // Recover the guard if the config lock was poisoned by a panicking
                // settings writer, rather than panicking this rip thread — matches
                // the file's graceful-degradation convention.
                crate::keysource::build_sources(&cfg.read().unwrap_or_else(|e| e.into_inner()))
            });
            libfreemkv::keysource::key_fetch(inputs, make)
        });

    // ── Shared pre-decode: FMTS forensic key resolution (BOTH rip paths) ──
    // Key resolution is a DECODE concern, so it runs here — before the single-
    // pass/multipass workflow split — and both paths consume the result the same
    // way. It used to be nested in the multipass-only reader block, so single-
    // pass FMTS got neither the gate nor the read plan (its inline reader muxed
    // the alternate device-group half as garbage). Base keys already passed the
    // gate above; the forensic index keys are online-only and used to be resolved
    // only at mux, so a base-keyed-but-forensic-missing FMTS disc swept for ~an
    // hour and THEN failed. Prove the full (base + per-index-phase) map up front
    // off the live drive — the `resolve_mux_key_map` seam does base + FMTS and
    // folds the forensic keys into the pool. Fail fast unless the operator opted
    // into capture-without-keys. Non-FMTS discs are unaffected (base gate already
    // ran; `key_fetch` is `None` for CSS and for single-pass non-FMTS).
    //
    // The resolved map is retained (`fmts_key_map`, declared at the function
    // body level) so the LIVE single-pass mux reads only our-phase units —
    // without it, single-pass FMTS would pass the alternate device-group half
    // to the demux (the pre-fix bug).
    if disc.format == libfreemkv::DiscFormat::Fmts {
        // Honor a Stop that lands during forensic key resolution. `resolve_mux_key_map`
        // runs BEFORE the sweep and takes no cancel token, so a user Stop (marginal
        // disc → repeated read-fault attempts, or a slow keyserver) can't interrupt it
        // by signature. Gate the call with a halt check on the SAME `Arc<AtomicBool>`
        // the sweep loop polls: a Stop before or right after the resolve exits cleanly
        // — no `.failed` marker, because a Stop is not a failure — and libfreemkv's own
        // read loops already poll this flag, so an in-flight resolve unwinds at its next
        // read boundary and surfaces here on return.
        let gate_stopped = || -> bool {
            if halt.load(Ordering::Relaxed) {
                crate::log::device_log(
                    device,
                    "Rip stopped by user during FMTS key resolution — staging preserved.",
                );
                unregister_halt(device);
                true
            } else {
                false
            }
        };
        if gate_stopped() {
            return;
        }
        let gate_title = disc.titles[0].clone();
        let mut gate_keys = disc.decrypt_keys();
        let resolved_map = libfreemkv::resolve_mux_key_map(
            &mut session.drive,
            &gate_title,
            &mut gate_keys,
            key_fetch.as_ref(),
            disc.content_format,
            // Thread the SAME cancel token the sweep polls: the resolve now unwinds
            // at its next read boundary on a Stop by signature (the surrounding
            // `gate_stopped()` checks only catch a Stop before/after the call).
            Some(&halt_token),
        );
        if gate_stopped() {
            return;
        }
        let gate = fmts_gate_decision(resolved_map.is_ok(), cfg_read.capture_without_keys);
        // Pure side-effect routing (unit-tested via `fmts_gate_plan`): CaptureOnly sets
        // the deferred-mux flag; Skip quarantines the staging dir. Driving both from the
        // plan keeps the gate's behavior mutation-verifiable.
        let plan = fmts_gate_plan(gate);
        defer_forensic_mux = plan.defer_forensic_mux;
        match gate {
            FmtsGate::Proceed => {
                // Bank the resolved forensic keys onto the disc so the sweep's
                // key persist + the mux reuse them.
                if let libfreemkv::decrypt::DecryptKeys::Aacs { unit_keys, .. } = &gate_keys
                    && let Some(a) = disc.aacs.as_mut()
                {
                    a.unit_keys = unit_keys.clone();
                }
                // Re-derive the shared decode keys so BOTH paths see the forensic
                // index keys just banked. This is essential for SINGLE-PASS, whose
                // inline reader is handed `keys` directly (a stale base-only pool
                // would miss the `fmts_key_map` forensic slots → DecryptFailed).
                // Multi-pass re-resolves its own map from the ISO at resume-mux, so
                // for it this is harmless; the `fmts_key_map` below is consumed only
                // by the single-pass inline reader (via `with_key_map`).
                keys = disc.decrypt_keys();
                fmts_key_map = resolved_map.ok().map(std::sync::Arc::new);
                crate::log::device_log(device, "FMTS: complete forensic key map resolved pre-rip.");
            }
            FmtsGate::CaptureOnly => {
                // `defer_forensic_mux` is now set (from `plan`), so the mux-skip below
                // (single-pass) or the resume path (`resume_remux` re-defers on
                // `FmtsKeyMissing`, multi-pass) will actually arrange the deferral this
                // log promises — previously this arm was a no-op and the mux ran with
                // base-only keys, producing garbage / a hard quarantine.
                crate::log::device_log(
                    device,
                    "FMTS: forensic keys unavailable — capturing raw ISO now \
                         (Capture Discs Without Keys is on); mux deferred until keys arrive.",
                );
            }
            FmtsGate::Skip => {
                crate::log::device_log(
                    device,
                    "FMTS: forensic keys missing — not ripping. Enable \
                         \"capture without keys\" to save an ISO for later.",
                );
                // Clean up the staging dir like every sibling early-failure exit in
                // `rip_disc` (write the `.failed` marker + clear the restart count),
                // instead of leaving an empty, marker-less dir orphaned until a
                // container restart. Driven by `plan.quarantine` so the routing is
                // pinned by `fmts_gate_plan`'s unit test.
                if plan.quarantine {
                    let staging_disc_path = std::path::Path::new(&staging);
                    staging::write_failed_marker(
                        staging_disc_path,
                        "FMTS forensic keys missing — not ripping.",
                    );
                    staging::clear_restart_count(staging_disc_path);
                }
                update_state_with(device, |s| {
                    s.status = "error".to_string();
                    s.last_error = "FMTS forensic keys missing — not ripping.".to_string();
                });
                unregister_halt(device);
                return;
            }
        }
    }

    let reader: Box<dyn libfreemkv::SectorSource> = if uses_multipass(cfg_read.max_retries) {
        let iso_path = std::path::Path::new(&iso_path_str);
        let bytes_total_disc = (session.drive.read_capacity().unwrap_or(0) as u64) * 2048;

        // Pre-flight disk-space check. Multipass needs:
        //   - one disc-sized ISO in staging (Pass 1 sweep target)
        //   - one MKV being written by mux (~25-50 % of disc; counted as
        //     1× to be conservative — the ISO is removed mid-mux when
        //     keep_iso=false but only AFTER the MKV completes)
        // → require at least 2× capacity_bytes free at staging.
        // Without this, a UHD rip on a too-small disk runs ~30 minutes
        // before ENOSPC at the boundary; user loses the time and the
        // staging dir is left half-full of partial ISO (cleanup on
        // ENOSPC failure isn't perfect).
        // Escape hatch: AUTORIP_SKIP_DISKCHECK=1 bypasses the pre-flight
        // check. Used to deliberately rip onto a smaller volume than 2×
        // disc capacity for diagnostics (speed isolation, partial ISO
        // tests). The rip will run and predictably ENOSPC mid-stream;
        // the operator accepts that. Don't use in production.
        if bytes_total_disc == 0 && std::env::var("AUTORIP_SKIP_DISKCHECK").is_err() {
            // read_capacity() returned 0/unknown, so we can't compute the
            // 2×-capacity requirement. Don't silently skip the preflight —
            // tell the operator why the space check didn't run, so an
            // eventual mid-stream ENOSPC isn't a surprise.
            crate::log::device_log(
                device,
                "disk-space preflight skipped: drive reported unknown capacity (read_capacity=0); \
                 a too-small staging volume will ENOSPC mid-rip",
            );
        }
        if bytes_total_disc > 0 && std::env::var("AUTORIP_SKIP_DISKCHECK").is_err() {
            let required = bytes_total_disc.saturating_mul(2);
            if let Some(avail) = staging_free_bytes(&staging) {
                if avail < required {
                    let msg = disk_space_preflight_message(required, &staging, avail);
                    crate::log::device_log(device, &msg);
                    update_state_with(device, |s| {
                        s.status = "error".to_string();
                        s.last_error = msg.clone();
                    });
                    unregister_halt(device);
                    drop_session(device);
                    return;
                }
            } else {
                // statvfs failed: staging path doesn't exist yet, the
                // volume isn't mounted, or the path isn't a POSIX
                // filesystem. We can't compute free space, so the 2×
                // requirement can't be checked. Don't silently skip the
                // preflight — tell the operator why, so an eventual
                // mid-stream ENOSPC (e.g. an unmounted staging volume)
                // isn't a surprise. Mirrors the unknown-capacity branch.
                crate::log::device_log(
                    device,
                    &format!(
                        "disk-space preflight skipped: could not read free space at {} \
                         (path missing or volume not mounted?); a too-small or unmounted \
                         staging volume will ENOSPC mid-rip",
                        staging,
                    ),
                );
            }
        }

        // Shared pass context + title reference for progress callbacks.
        let pass_ctx = PassContext {
            device: device.to_string(),
            display_name: display_name.clone(),
            disc_format: disc_format.clone(),
            tmdb_title: tmdb_title.clone(),
            tmdb_year,
            tmdb_poster: tmdb_poster.clone(),
            tmdb_overview: tmdb_overview.clone(),
            tmdb_media_type: tmdb_media_type.clone(),
            duration: duration.clone(),
            codecs: codecs.clone(),
            filename: filename.clone(),
            batch,
            bytes_total_disc,
            max_retries: cfg_read.max_retries,
        };
        let title_for_progress = title.clone();
        let bps_progress = title_bytes_per_sec;

        // Pass 1: disc → ISO (fast sweep, skip-forward on failure).
        let pass_label = format!("Pass 1/{total_passes}: disc → ISO");
        crate::log::device_log(device, &pass_label);
        set_pass_progress(
            &pass_ctx,
            1,
            total_passes,
            0, // bytes_good
            0, // bytes_maybe
            0, // bytes_lost
        );

        // Progress callback — runs every read block (~64 KB). Throttle the
        // mapfile re-read + state push to once every 1.5 s so we don't pound
        // the mutex or the filesystem. State tracker holds last-sample
        // timestamp + bytes for speed/ETA calc.
        let pass1_state = std::cell::RefCell::new(PassProgressState::new());
        let pass1_ctx = &pass_ctx;
        let pass1_progress = |p: &libfreemkv::progress::PassProgress| -> bool {
            // Stash work_done for push_pass_state to compute pass progress.
            pass1_state.borrow_mut().last_work_done = p.work_done;
            pass1_state.borrow_mut().last_work_total = p.work_total;
            // Throttle: only re-read mapfile + push state every 1.5s.
            // 250 ms UI push cadence (see the patch closure below for rationale).
            if pass1_state.borrow().last_update.elapsed().as_millis() < 250 {
                return true;
            }
            push_pass_state(pass1_ctx, p, bps_progress, 1, total_passes, &pass1_state);
            true
        };

        // Pass 1: disc → ISO with transport-failure recovery.
        //
        // The Initio USB-SATA bridge crashes when reading damaged sectors,
        // causing a USB re-enumeration (sg device changes number). The copy
        // aborts, but the mapfile captures all progress. We retry with
        // resume=true after re-opening the drive on its new device path.
        let pass1_halt = Arc::new(AtomicBool::new(false));
        let _pass1_guard = spawn_pass_watcher(pass1_halt.clone(), user_halt.clone());

        const MAX_PASS1_ATTEMPTS: u32 = 10;
        let mut attempt = 0;
        let mut result = None;
        // The most recent sweep error, kept so the `result = None`
        // fallthrough can translate the underlying SCSI cause through
        // `format_pass_error` rather than surfacing a bare internal
        // strategy identifier to the operator.
        let mut last_sweep_err: Option<libfreemkv::Error> = None;

        'pass1: loop {
            attempt += 1;
            if attempt > MAX_PASS1_ATTEMPTS {
                crate::log::device_log(device, "Pass 1: max attempts reached");
                break;
            }

            // 0.18 round 3: Pass 1 calls freemkv_engine::sweep directly. The old
            // disc.copy(opts.multipass=true) dispatched to sweep_internal
            // which forwarded {decrypt, skip_on_error=multipass} to
            // SweepOptions. resume=true on retry attempts so the existing
            // mapfile state continues where the bridge crash left it
            // (matches the pre-existing implicit resume behaviour: the
            // first attempt is fresh and each retry resumes from mapfile).
            //
            // `resume_sweep` (user clicked Resume on a partial) makes even the
            // FIRST attempt resume from the existing mapfile + ISO, so the
            // ~40 GB already swept isn't re-read off the disc.
            let sweep_opts = freemkv_engine::SweepOptions {
                decrypt: false,
                resume: resume_sweep || attempt > 1,
                batch_sectors: None,
                skip_on_error: true,
                progress: Some(&pass1_progress),
                halt: Some(pass1_halt.clone()),
                // Persist the disc's decryption state into the mapfile so it
                // survives to deferred-mux / resume. KEYS XOR VID: if the disc
                // resolved a key, persist the unit keys (the final answer — the
                // mux decrypts directly, no second key-service call); otherwise
                // persist the VID (the retry marker). libfreemkv writes whichever
                // applies (set_unit_keys clears vid when keys are present).
                vid: disc.aacs.as_ref().map(|a| a.volume_id),
                unit_keys: disc
                    .aacs
                    .as_ref()
                    .map(|a| a.unit_keys.clone())
                    .unwrap_or_default(),
                key_fetch: key_fetch.clone(),
            };

            match freemkv_engine::sweep(&disc, &mut session.drive, iso_path, &sweep_opts) {
                Ok(r) => {
                    result = Some(r);
                    break 'pass1;
                }
                Err(e) => {
                    if halt.load(Ordering::Relaxed) {
                        crate::log::device_log(device, &format!("Pass 1 cancelled (halt): {e}"));
                        // `_halt_guard` unregisters this device's Halt token on
                        // drop (i.e. on this `return`); no explicit call needed.
                        return;
                    }

                    let is_transport = e.is_scsi_transport_failure();

                    if !is_transport {
                        crate::log::device_log(device, &format!("Pass 1 failed: {e}"));
                        let user_msg = format_pass_error("Pass 1", &e);
                        update_state(
                            device,
                            RipState {
                                device: device.to_string(),
                                status: "error".to_string(),
                                disc_present: true,
                                last_error: user_msg,
                                disc_name: display_name.clone(),
                                disc_format: disc_format.clone(),
                                tmdb_title: tmdb_title.clone(),
                                tmdb_year,
                                tmdb_poster: tmdb_poster.clone(),
                                tmdb_overview: tmdb_overview.clone(),
                                duration: duration.clone(),
                                codecs: codecs.clone(),
                                ..Default::default()
                            },
                        );
                        unregister_halt(device);
                        return;
                    }

                    // Transport failure — bridge crashed. Remember the
                    // underlying cause so the exhaustion fallthrough can
                    // translate it to operator-facing text via
                    // `format_pass_error` rather than leaking the internal
                    // strategy identifier. (`e` is unused past here; the
                    // recovery arms shadow it with their own local errors.)
                    last_sweep_err = Some(e);

                    // Drop stale drive, wait for USB re-enumeration, re-open
                    // on new path.
                    crate::log::device_log(
                        device,
                        &format!(
                            "Pass 1 attempt {attempt}: transport failure (bridge crash), waiting for USB re-enumeration"
                        ),
                    );
                    drop_session(device);

                    // Wait for USB re-enumeration with configurable delay.
                    // Value snapshotted at the top of `rip_disc`; we no
                    // longer hold the cfg read guard here.
                    std::thread::sleep(std::time::Duration::from_secs(
                        transport_recovery_delay_secs,
                    ));

                    // Re-discover the device. The poll loop may have already
                    // found it; if not, try probing the original path and its
                    // neighbors (sg numbers shift by ±1 on re-enumeration).
                    let new_path = rediscover_drive(device, device_path);
                    match (new_path.as_deref(), &device_path) {
                        (Some(p), _) if p != device_path => {
                            crate::log::device_log(
                                device,
                                &format!(
                                    "Pass 1 attempt {attempt}: drive rediscovered at {p} (original={}), attempting re-open",
                                    device_path
                                ),
                            );

                            // Retry Drive::open with exponential backoff (firmware may not be ready yet).
                            let mut drive = match open_drive_with_backoff(
                                device,
                                attempt,
                                p,
                                transport_recovery_delay_secs,
                            ) {
                                Some(d) => d,
                                None => break 'pass1,
                            };

                            if let Err(e) = drive.wait_ready() {
                                crate::log::device_log(
                                    device,
                                    &format!(
                                        "Pass 1 attempt {attempt}: Drive::wait_ready({}) failed strategy=transport_failure_recovery error={} — recovery path exhausted",
                                        p,
                                        e.code()
                                    ),
                                );

                                let failure_category = if e.code() == 4000 {
                                    "SCSI_ERROR"
                                } else {
                                    &format!("ERROR_CODE_{}", e.code())
                                };

                                crate::log::device_log(
                                    device,
                                    &format!(
                                        "STRATEGY_FAILURE: transport_failure_recovery FAILED at Drive::wait_ready category={} error_code={}",
                                        failure_category,
                                        e.code()
                                    ),
                                );

                                break 'pass1;
                            }

                            if let Err(e) = drive.init() {
                                crate::log::device_log(
                                    device,
                                    &format!(
                                        "Pass 1 attempt {attempt}: Drive::init({}) failed strategy=transport_failure_recovery error={} sense_key={:?} ASC={:?} — recovery path exhausted",
                                        p,
                                        e.code(),
                                        e.scsi_sense().map(|s| s.sense_key),
                                        e.scsi_sense().map(|s| s.asc)
                                    ),
                                );

                                log_init_recovery_failure(device, &e);

                                break 'pass1;
                            }

                            // Engage the drive's disc-type read mode before any
                            // read. Idempotent. Kept here to stay structurally
                            // identical to scan_disc / the fresh-open path / the
                            // initial session probe, which all call probe_disc()
                            // after init().
                            if let Err(e) = drive.probe_disc() {
                                tracing::warn!(device = %device, error = %e, "drive probe_disc failed (continuing)");
                            }

                            session.drive = drive;
                            session.device_path = p.to_string();

                            crate::log::device_log(
                                device,
                                &format!(
                                    "PASS 1/{}: transport_failure_recovery SUCCESS — resuming from mapfile at {}",
                                    attempt + 1,
                                    p
                                ),
                            );
                        }

                        (Some(p), _) if p == device_path => {
                            crate::log::device_log(
                                device,
                                &format!(
                                    "Pass 1 attempt {attempt}: drive still at original path {}, attempting re-open",
                                    p
                                ),
                            );

                            // Retry Drive::open with exponential backoff (firmware
                            // may not be ready yet) — same as the new-path arm, since
                            // a same-sg re-enumeration leaves firmware just as cold.
                            let mut drive = match open_drive_with_backoff(
                                device,
                                attempt,
                                p,
                                transport_recovery_delay_secs,
                            ) {
                                Some(d) => d,
                                None => break 'pass1,
                            };

                            if let Err(e) = drive.wait_ready() {
                                crate::log::device_log(
                                    device,
                                    &format!(
                                        "Pass 1 attempt {attempt}: Drive::wait_ready({}) failed strategy=transport_failure_recovery error={} — recovery path exhausted",
                                        p,
                                        e.code()
                                    ),
                                );

                                let failure_category = if e.code() == 4000 {
                                    "SCSI_ERROR"
                                } else {
                                    &format!("ERROR_CODE_{}", e.code())
                                };

                                crate::log::device_log(
                                    device,
                                    &format!(
                                        "STRATEGY_FAILURE: transport_failure_recovery FAILED at Drive::wait_ready category={} error_code={}",
                                        failure_category,
                                        e.code()
                                    ),
                                );

                                break 'pass1;
                            }

                            if let Err(e) = drive.init() {
                                crate::log::device_log(
                                    device,
                                    &format!(
                                        "Pass 1 attempt {attempt}: Drive::init({}) failed strategy=transport_failure_recovery error={} sense_key={:?} ASC={:?} — recovery path exhausted",
                                        p,
                                        e.code(),
                                        e.scsi_sense().map(|s| s.sense_key),
                                        e.scsi_sense().map(|s| s.asc)
                                    ),
                                );

                                // Same wedged-firmware diagnostic as the
                                // new-path arm: an ILLEGAL REQUEST after a
                                // same-sg re-enumeration also means the
                                // firmware needs a power-cycle.
                                log_init_recovery_failure(device, &e);

                                break 'pass1;
                            }

                            // Engage the drive's disc-type read mode before any
                            // read. Idempotent. Kept here to stay structurally
                            // identical to scan_disc / the fresh-open path / the
                            // initial session probe, which all call probe_disc()
                            // after init().
                            if let Err(e) = drive.probe_disc() {
                                tracing::warn!(device = %device, error = %e, "drive probe_disc failed (continuing)");
                            }

                            session.drive = drive;
                            session.device_path = p.to_string();

                            crate::log::device_log(
                                device,
                                &format!(
                                    "PASS 1/{}: transport_failure_recovery SUCCESS — resuming from mapfile at {}",
                                    attempt + 1,
                                    p
                                ),
                            );
                        }

                        (None, _) => {
                            crate::log::device_log(
                                device,
                                "Pass 1: could not re-discover drive after transport failure strategy=usb_re_enumeration FAILED",
                            );

                            // Log detailed breakdown of what was tried
                            let sg_num = device_path
                                .rsplit('/')
                                .next()
                                .and_then(|s| {
                                    s.strip_prefix("sg").and_then(|n| n.parse::<i32>().ok())
                                })
                                .unwrap_or(-1);

                            crate::log::device_log(
                                device,
                                &format!(
                                    "usb_re_enumeration strategy tried probe paths: sg{} (original), sg{}, sg{}, sg{}, sg{}, sg{}, sg{}",
                                    sg_num,
                                    sg_num - 1,
                                    sg_num + 1,
                                    sg_num - 2,
                                    sg_num + 2,
                                    sg_num - 3,
                                    sg_num + 3
                                ),
                            );

                            crate::log::device_log(
                                device,
                                "STRATEGY_FAILURE: usb_re_enumeration FAILED — no valid drive path found after USB re-enumeration",
                            );

                            break 'pass1;
                        }

                        // Fallback for any other case (shouldn't happen but compiler requires exhaustiveness)
                        _ => {
                            crate::log::device_log(
                                device,
                                "STRATEGY_FAILURE: usb_re_enumeration FAILED — unexpected match state",
                            );

                            break 'pass1;
                        }
                    }
                }
            }
        }

        let result = match result {
            Some(r) => r,
            None => {
                // All attempts exhausted or unrecoverable.

                // Determine which recovery strategy failed and why
                let failure_reason = if attempt >= MAX_PASS1_ATTEMPTS {
                    "transport_failure_recovery_exhausted".to_string()
                } else {
                    "unrecoverable_error".to_string()
                };

                crate::log::device_log(
                    device,
                    &format!(
                        "Pass 1: recovery failed at attempt {}/{}, strategy={}",
                        // `attempt` is already 1-based (incremented at the top
                        // of the loop), so print it directly — `attempt + 1`
                        // overcounted, yielding e.g. "12/10" at exhaustion.
                        attempt.min(MAX_PASS1_ATTEMPTS),
                        MAX_PASS1_ATTEMPTS,
                        failure_reason
                    ),
                );

                // Translate the underlying SCSI cause into operator-facing
                // text. `format_pass_error` turns sense data into an
                // actionable message (e.g. "power-cycle the drive"); fall
                // back to a plain message only if no error was captured.
                let user_msg = match &last_sweep_err {
                    Some(e) => format_pass_error("Pass 1", e),
                    None => "Pass 1 failed — see logs for detailed error breakdown".to_string(),
                };

                update_state(
                    device,
                    RipState {
                        device: device.to_string(),
                        status: "error".to_string(),
                        disc_present: true,
                        last_error: user_msg,
                        disc_name: display_name.clone(),
                        disc_format: disc_format.clone(),
                        tmdb_title: tmdb_title.clone(),
                        tmdb_year,
                        tmdb_poster: tmdb_poster.clone(),
                        tmdb_overview: tmdb_overview.clone(),
                        duration: duration.clone(),
                        codecs: codecs.clone(),
                        ..Default::default()
                    },
                );

                // Log recovery guidance for user action based on failure type
                if failure_reason == "transport_failure_recovery_exhausted" {
                    crate::log::device_log(
                        device,
                        &format!(
                            "RECOVERY_GUIDANCE: Transport failure recovery exhausted after {} attempts. Check logs for specific error category (SCSI_ERROR, DEVICE_ERROR). If ILLEGAL REQUEST errors present, drive firmware wedged — eject disc and power-cycle USB drive before retrying.",
                            MAX_PASS1_ATTEMPTS
                        ),
                    );

                    crate::log::device_log(
                        device,
                        &format!(
                            "NEXT_STEPS: 1) Check /api/logs/{device} for STRATEGY_FAILURE entries. 2) Identify which phase failed (Drive::open/wait_ready/init). 3) If firmware wedged, power-cycle the drive and retry.",
                        ),
                    );
                } else {
                    crate::log::device_log(
                        device,
                        "RECOVERY_GUIDANCE: Unrecoverable error occurred before transport failure recovery could complete. Check logs for first ERROR entry to identify root cause.",
                    );
                }

                unregister_halt(device);
                return;
            }
        };
        // Drop the Pass 1 watcher so its thread exits before Pass 2 spawns its own.
        drop(_pass1_guard);
        crate::log::device_log(
            device,
            &format!(
                "Pass 1 done: {:.2} GB good, {:.2} MB unreadable, {:.2} MB pending",
                result.bytes_good as f64 / BYTES_PER_GIB,
                result.bytes_unreadable as f64 / BYTES_PER_MIB,
                result.bytes_pending as f64 / BYTES_PER_MIB,
            ),
        );

        // Track cross-pass state from CopyResult.
        let mut bytes_good = result.bytes_good;
        let mut bytes_unreadable = result.bytes_unreadable;
        let mut bytes_pending = result.bytes_pending;

        // Retry passes: freemkv_engine::patch re-reads only the bad ranges
        // recorded in the mapfile sector-by-sector with full
        // drive-level recovery. Each pass gets its own wallclock cap
        // watcher; cap-fire marks the rip as failed.

        let max_retries = cfg_read.max_retries;
        // The patch-pass count comes from the pass plan (== max_retries); this
        // ties the retry-loop bound to the same pure plan `total_passes` uses.
        let patch_passes = plan_passes(cfg_read.max_retries).patch_passes;

        crate::log::device_log(
            device,
            &format!(
                "PASS 2-{}: retry loop starting max_retries={} bytes_pending={}",
                max_retries, max_retries, bytes_pending
            ),
        );
        for retry_n in 1..=patch_passes {
            // If user hit stop, bail.
            if user_halt.load(Ordering::Relaxed) {
                crate::log::device_log(
                    device,
                    &format!("PASS {} STOPPED: user halt before retry pass", retry_n + 1),
                );
                break;
            }

            // Skip remaining retry passes once the *muxable* scope is
            // 100 % recovered. The user setting that decides scope:
            //   - output_format = "iso"  → whole disc must be clean
            //                              (every sector is part of what
            //                              gets handed off; nothing to
            //                              skip elsewhere)
            //   - output_format = "mkv"/"m2ts" → only the title that
            //                              actually gets muxed needs to
            //                              be clean. Bad ranges that
            //                              fall outside that title's
            //                              extents (deleted scenes,
            //                              menus, trailers) are not
            //                              going into the output and
            //                              do not earn additional retry
            //                              passes.
            //
            // Note: `abort_on_lost_secs` is *not* the trigger here. That
            // setting is the user's tolerance for content that ends up
            // in the MKV; it gates abort vs. mux at the END of all
            // retries. The skip-passes check is strictly "is everything
            // we'll mux now Finished in the mapfile?". A disc with 5 s
            // of loss when the threshold allows 10 s does NOT earn a
            // skip — there's still recoverable damage in the muxed
            // scope, so we keep trying.
            let mux_scope_bad =
                match freemkv_engine::Mapfile::load(std::path::Path::new(&mapfile_path_str)) {
                    Ok(map) => {
                        let bad = map.ranges_with(&bad_sector_statuses());
                        scope_bad_bytes(
                            output_is_iso_image(&cfg_read.output_format),
                            &bad,
                            &title_for_progress,
                        )
                    }
                    Err(_) => {
                        // Conservative fallback if we can't read the mapfile —
                        // fall back to the whole-disc check so we don't skip
                        // a needed pass on a transient read error.
                        bytes_pending + bytes_unreadable
                    }
                };
            // Loop-top convergence gate, via the unified strategy decision
            // (`None` recovery ⇒ this is the pre-pass evaluation): Converged
            // means the muxable scope is 100% recovered — stop and mux.
            if patch_pass_decision(mux_scope_bad, None) == PatchDecision::Converged {
                let scope_label = if output_is_iso_image(&cfg_read.output_format) {
                    "whole disc"
                } else {
                    "muxed title"
                };
                crate::log::device_log(
                    device,
                    &format!(
                        "PASS {} SKIPPED: {} is 100% recovered in mapfile — proceeding to mux",
                        retry_n + 1,
                        scope_label
                    ),
                );
                break;
            }

            let pass = retry_n + 1;

            // Flip the UI to the new pass BEFORE the settle, so the tile shows
            // "pass N · retrying · 0%" immediately instead of carrying the prior
            // pass's stale 99% through the 30 s drive settle below.
            set_pass_progress(
                &pass_ctx,
                pass,
                total_passes,
                bytes_good,
                bytes_pending,    // MAYBE bucket — Pass 2-N may still recover
                bytes_unreadable, // LOST bucket — terminal
            );

            // Per-pass progress state — created BEFORE the settle so the disc
            // map can be painted immediately.
            let patch_state = std::cell::RefCell::new(PassProgressState::new());
            let patch_ctx = &pass_ctx;
            let patch_title = &title_for_progress;
            let patch_map = std::path::Path::new(&mapfile_path_str);

            // Paint the map (red bad ranges + at-risk movie time) at pass start,
            // BEFORE the 30 s settle. Otherwise the bar sits all-green with
            // "Maybe … · 0:00" for 30 s until the patch loop's first emission —
            // most visible on resume, where there's no prior sweep push to carry
            // the ranges across the pass boundary. The lib builds the snapshot
            // from the mapfile (autorip never parses it); there's no live `p`
            // yet at the boundary.
            if let Some(snap) = freemkv_engine::progress_snapshot_from_mapfile(
                patch_map,
                Some(patch_title),
                libfreemkv::progress::PassKind::Trim { reverse: true },
                patch_ctx.bytes_total_disc,
            ) {
                push_pass_state(
                    patch_ctx,
                    &snap,
                    bps_progress,
                    pass,
                    total_passes,
                    &patch_state,
                );
            }

            crate::log::device_log(
                device,
                &format!(
                    "PASS {}/{total_passes}: retrying bad ranges (bpt=1) bytes_pending={}",
                    pass, bytes_pending
                ),
            );
            let patch_progress = |p: &libfreemkv::progress::PassProgress| -> bool {
                patch_state.borrow_mut().last_work_done = p.work_done;
                patch_state.borrow_mut().last_work_total = p.work_total;
                // 250 ms UI push cadence (matches libfreemkv's snapshot
                // republish). The speed/ETA window is time-based, so finer
                // samples only smooth it; the per-push mapfile reload is cheap
                // for the usual handful of ranges.
                if patch_state.borrow().last_update.elapsed().as_millis() < 250 {
                    return true;
                }
                push_pass_state(patch_ctx, p, bps_progress, pass, total_passes, &patch_state);
                true
            };
            let pass_halt = Arc::new(AtomicBool::new(false));
            let _pass_guard = spawn_pass_watcher(pass_halt.clone(), user_halt.clone());

            // 0.18 round 3: Pass 2..N calls freemkv_engine::patch directly. The old
            // disc.copy(opts.multipass=true) dispatched to patch_internal
            // when the mapfile already had retryable bytes; these PatchOptions
            // mirror what patch_internal was constructing internally.
            let patch_opts = freemkv_engine::PatchOptions {
                decrypt: false,
                // Enter each bad range BATCHED (not single-sector). A bad
                // range from Pass 1 is mostly the good skip-ahead overshoot
                // with a small damaged core; reading it one sector at a time
                // (the old Some(1)) paid a SCSI op + AACS-unit decrypt per
                // sector even through the good part. With a batch, the good
                // overshoot reads in bulk and a batch failure bisects
                // (handle_read_failure halves) down to the actual bad sector
                // — single-sector cost is confined to real damage. Matches
                // the sweep's batch size.
                block_sectors: Some(32),
                full_recovery: true,
                reverse: true,
                wedged_threshold: 50,
                progress: Some(&patch_progress),
                halt: Some(pass_halt.clone()),
                key_fetch: key_fetch.clone(),
            };
            // Un-wedge the drive in SOFTWARE before each retry pass. Grinding a
            // bad cluster leaves the BU40N in a HARDWARE_ERROR fast-fail wedge
            // where it fails even readable sectors; our notes say that needs a
            // power-cycle. spin_cycle() is exactly that WITHOUT ejecting (the
            // drive is slot-loading — a human eject is a product failure for an
            // unattended service). Validated live: took the drive from
            // failing-every-read back to reading at MB/s. This ACTIVE reset
            // supersedes the old 30 s passive settle (which used to also run
            // before pass 2 — the two stacked to 45 s of redundant delay on a
            // fresh rip); a power-cycle recovers a wedged drive better than idle.
            if let Err(e) = session.drive.spin_cycle() {
                // spin_cycle's SCSI command itself failed (dead bus / file-backed
                // resume with no real drive). Fall back to a short passive idle so
                // a drive that just ground a sweep still gets SOME recovery time
                // before the retry reads — never zero (a bridge transport fault
                // self-recovers in ~15 s of idle).
                crate::log::device_log(
                    device,
                    &format!(
                        "drive spin-cycle before pass {pass} failed ({e}); settling 15 s instead"
                    ),
                );
                // Short idle in 1 s slices so a user halt stays responsive.
                for _ in 0..15 {
                    if user_halt.load(Ordering::Relaxed) {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            } else {
                crate::log::device_log(
                    device,
                    &format!("drive spin-cycled (soft un-wedge, no eject) before pass {pass}"),
                );
            }
            let cr = match freemkv_engine::patch(&disc, &mut session.drive, iso_path, &patch_opts) {
                Ok(r) => r,
                Err(e) => {
                    // Categorize the failure for debugging
                    let error_category = if e.code() == 4000 {
                        "SCSI_ERROR"
                    } else if e.code() >= 6000 && e.code() < 7000 {
                        "DISC_READ_ERROR"
                    } else if e.code() >= 1000 && e.code() < 2000 {
                        "DEVICE_ERROR"
                    } else {
                        &format!("ERROR_CODE_{}", e.code())
                    };

                    let sense_info = e.scsi_sense().map(|s| {
                        format!(
                            "sense_key={:02x} ASC={:02x} ASCQ={:02x}",
                            s.sense_key, s.asc, s.ascq
                        )
                    });

                    if user_halt.load(Ordering::Relaxed) {
                        crate::log::device_log(
                            device,
                            &format!(
                                "PASS {} CANCELLED: user halt category={} error_code={}",
                                pass,
                                error_category,
                                e.code()
                            ),
                        );

                        if let Some(info) = sense_info {
                            crate::log::device_log(device, &info);
                        }
                    } else {
                        crate::log::device_log(
                            device,
                            &format!(
                                "PASS {} FAILED: strategy=patch_recovery category={} error_code={} {}",
                                pass,
                                error_category,
                                e.code(),
                                sense_info.unwrap_or_default()
                            ),
                        );

                        // Log which recovery phase failed
                        crate::log::device_log(
                            device,
                            &format!(
                                "STRATEGY_FAILURE: patch_recovery FAILED at disc.patch() with category={} (sense_key={:?}, ASC={:?})",
                                error_category,
                                e.scsi_sense().map(|s| s.sense_key),
                                e.scsi_sense().map(|s| s.asc)
                            ),
                        );

                        // Provide actionable guidance based on error type
                        if e.code() == 4000 && e.is_scsi_transport_failure() {
                            crate::log::device_log(
                                device,
                                "ACTION_REQUIRED: Transport failure detected — USB bridge crashed. Eject disc and power-cycle drive before retrying.",
                            );
                        } else if e.code() >= 6000
                            && e.scsi_sense()
                                .map(|s| s.is_hardware_error())
                                .unwrap_or(false)
                        {
                            crate::log::device_log(
                                device,
                                "ACTION_REQUIRED: Drive hardware error detected — drive may be failing. Consider replacing optical drive.",
                            );
                        } else if e.code() == 4000
                            && e.scsi_sense().map(|s| s.asc == 0x20).unwrap_or(false)
                        {
                            crate::log::device_log(
                                device,
                                "ACTION_REQUIRED: ILLEGAL REQUEST (ASC=0x20) — drive firmware wedged. Power-cycle USB drive to clear state.",
                            );
                        }
                    }

                    break;
                }
            };
            bytes_good = cr.bytes_good;
            bytes_unreadable = cr.bytes_unreadable;
            bytes_pending = cr.bytes_pending;
            // PatchOutcome renames recovered_this_pass → bytes_recovered_this_pass.
            let recovered = cr.bytes_recovered_this_pass;
            let exit_str = if cr.halted {
                " (halt)"
            } else if cr.wedged_exit {
                " (DRIVE WEDGED: fast-fail sense — retries aborted, needs spin-cycle/power-cycle)"
            } else {
                ""
            };
            // Report the three buckets that actually tell success from failure:
            // recovered THIS pass, how much is still bad (pending — retryable next
            // pass), and how much has been given up (unreadable). The old line
            // showed only `unreadable`, which is 0 until the post-loop promotion —
            // so a pass that recovered nothing read as "0 recovered; 0 unreadable"
            // and told you nothing.
            crate::log::device_log(
                device,
                &format!(
                    "Pass {pass} done: recovered {:.2} MB this pass; {:.2} MB still bad, {:.2} MB unreadable{exit_str}",
                    recovered as f64 / BYTES_PER_MIB,
                    bytes_pending as f64 / BYTES_PER_MIB,
                    bytes_unreadable as f64 / BYTES_PER_MIB,
                ),
            );
            // Drop this pass's watcher before next iteration.
            drop(_pass_guard);
            // Stop early if the user hit stop during the patch (the
            // watcher forwards user_halt into pass_halt).
            if user_halt.load(Ordering::Relaxed) {
                break;
            }
            // If THIS pass made no progress, no future pass with the same
            // drive state will help. Give up retries early so we still
            // mux on what we have.
            if !patch_made_progress(recovered) {
                crate::log::device_log(
                    device,
                    &format!(
                        "PASS {} STOPPED: strategy=patch_recovery exhausted — no progress (recovered={} MB) after all retry attempts",
                        pass,
                        recovered as f64 / BYTES_PER_MIB
                    ),
                );

                crate::log::device_log(
                    device,
                    "STRATEGY_FAILURE: patch_recovery exhausted — drive cannot recover more data from bad sectors with current settings",
                );

                crate::log::device_log(
                    device,
                    "RECOVERY_GUIDANCE: Consider increasing max_retries or abort_on_lost_secs if tolerating some data loss is acceptable.",
                );

                break;
            }
        }

        // End-of-recovery promotion: walk the mapfile and promote any
        // still-NonTrimmed bytes to Unreadable. This is the "good or
        // maybe until all passes are done, then it's gone" step that
        // libfreemkv's patch loop intentionally defers to the
        // orchestrator (see PatchItem::NonTrimmed doc + libfreemkv
        // commit 863e04c). Pre-promotion: failed Pass-N bytes are
        // still "maybe" in the mapfile. Post-promotion: they're
        // confirmed lost, feeding the abort_on_lost_secs check below
        // and the final Cosmetic-vs-Maybe display.
        //
        // Only runs in multi-pass mode (max_retries > 0); single-pass
        // rips don't have a "final pass" boundary and have no mapfile,
        // so their abort_on_lost_secs check runs AFTER the mux instead,
        // gating on the demux skip count (see the single-pass abort gate
        // below `run_mux`). Sweep never marks Unreadable either.
        // End-of-recovery promotion + abort check: a single block so the
        // abort check operates on the already-promoted in-memory map rather
        // than re-loading from disk (the previous two-block design dropped
        // `map` without flushing, then re-loaded the pre-promotion file,
        // causing the abort check to see zero Unreadable bytes even after
        // promotion — MED logic bug fixed here).
        // A user STOP is NOT recovery-exhaustion. If the user halted (before or
        // within the retry passes), skip the NonTrimmed→Unreadable promotion AND
        // the abort-on-loss check below — those un-retried ranges are still
        // recoverable. Preserve the partial sweep (ISO + mapfile, ranges left
        // NonTrimmed, no terminal marker) and stop cleanly so a later Resume
        // continues Pass 1 + Pass N from the mapfile. (Bug fixed here: this halt
        // check previously lived only at the mux gate, AFTER this promote+abort
        // block — so a stop before the retry passes promoted un-retried ranges to
        // Unreadable and wrote a spurious `.aborted-loss`, mis-routing the disc to
        // the accept-loss/remux resume path instead of continue-the-sweep.)
        if user_halt.load(Ordering::Relaxed) {
            crate::log::device_log(
                device,
                "Rip stopped by user — preserving partial sweep for resume.",
            );
            unregister_halt(device);
            return;
        }

        let mut main_lost_ms_for_history = 0.0f64;
        let mut main_lost_bytes_for_history = 0u64;
        if uses_multipass(cfg_read.max_retries) {
            let mapfile_path = std::path::Path::new(&mapfile_path_str);
            if let Ok(mut map) = freemkv_engine::Mapfile::load(mapfile_path) {
                use freemkv_engine::SectorStatus;
                // Promote still-NonTrimmed bytes to Unreadable — these are
                // bytes that remained "maybe" across all patch passes and are
                // now confirmed lost. The (from, to) pair is the pinned
                // end-of-recovery promotion decision.
                let (promote_from, promote_to) = end_of_recovery_promotion();
                // `from` is a SET: NonTrimmed and NonScraped are both "maybe"
                // states that survived every pass, and the abort gate reads
                // only Unreadable, so a state left unpromoted is loss the
                // gate cannot see.
                let nontrimmed_ranges = map.ranges_with(promote_from);
                let total_promoted: u64 = nontrimmed_ranges.iter().map(|(_, sz)| *sz).sum();
                let n_ranges = nontrimmed_ranges.len();
                // Promotion is what MAKES the loss visible: the abort gate below
                // reads Unreadable ranges only, so a range that fails to promote
                // is loss the gate cannot see. Logging and carrying on turns a
                // write error into a rip delivered as good — the comment above
                // says exactly this, and the code did it anyway.
                let mut promotion_intact = true;
                for (pos, size) in nontrimmed_ranges {
                    if let Err(e) = map.record(pos, size, promote_to) {
                        promotion_intact = false;
                        tracing::error!(
                            device = %device,
                            error = %e,
                            "end_of_recovery_promote: failed to mark range Unreadable"
                        );
                    }
                }
                tracing::info!(
                    device = %device,
                    ranges_promoted = n_ranges,
                    bytes_promoted = total_promoted,
                    "end_of_recovery_promote: NonTrimmed -> Unreadable after final retry pass"
                );
                // Flush the promoted state to disk so downstream consumers
                // (muxer, resume check) see the terminal Unreadable marks.
                // Surface flush errors as warnings rather than silently
                // dropping them.
                if let Err(e) = map.flush() {
                    // Downstream (mux, resume) re-reads the mapfile from DISK,
                    // so an unflushed promotion means they see the pre-promotion
                    // state and report the delivered rip as undamaged.
                    promotion_intact = false;
                    tracing::error!(
                        device = %device,
                        error = %e,
                        "end_of_recovery_promote: failed to flush promoted mapfile"
                    );
                }
                // Refresh bytes_unreadable from the promoted in-memory map
                // (not from disk — re-loading here would race the flush and
                // could return the pre-promotion state on slow storage).
                bytes_unreadable = map.stats().bytes_unreadable;

                // Abort check: use the already-promoted in-memory map so
                // bad_ranges reflects the just-promoted Unreadable sectors.
                // The previous design re-loaded the mapfile here, which
                // returned the pre-promotion state when the flush above had
                // not yet hit disk.
                if !promotion_intact {
                    tracing::error!(
                        device = %device,
                        "end_of_recovery_promote: damage record is \
                         incomplete — treating loss as unquantifiable"
                    );
                }
                // The measurement the abort gate decides on, from the
                // already-promoted in-memory map.
                let loss = end_of_recovery_loss(
                    &map,
                    promotion_intact,
                    output_is_iso_image(&cfg_read.output_format),
                    &title_for_progress,
                    title_bytes_per_sec,
                );
                main_lost_bytes_for_history = loss.lost_bytes;
                main_lost_ms_for_history = loss.lost_ms;
                // Mirror into the outer binding so the final done/stopped state
                // update (after run_mux) can use the same in-title value without
                // re-reading the mapfile.
                main_lost_ms_for_history_outer = main_lost_ms_for_history;
                // Re-derive damage fields from the just-promoted in-memory map
                // and push them to STATE before `map` is dropped. The
                // marker_damage snapshot (~80 lines below) reads from STATE; if
                // we skip this step it reads the last push_pass_state snapshot,
                // which predates the NonTrimmed→Unreadable promotion and therefore
                // under-reports errors / total_lost_ms / bad_ranges for a damaged
                // rip. Mirrors resume.rs build_bad_ranges + damage-aggregation
                // logic (see resume.rs ~692-713).
                {
                    let (
                        promoted_bad_ranges,
                        promoted_num_bad,
                        promoted_truncated,
                        promoted_total_lost_ms,
                        promoted_largest_gap_ms,
                    ) = state::build_bad_ranges(&map, &title_for_progress, bps_progress);
                    let promoted_main_title_bad = map.ranges_with(&[SectorStatus::Unreadable]);
                    let promoted_main_bad_bytes = libfreemkv::disc::bytes_bad_in_title(
                        &title_for_progress,
                        &promoted_main_title_bad,
                    );
                    let promoted_main_lost_ms = if bps_progress > 0.0 {
                        promoted_main_bad_bytes as f64 * MILLIS_PER_SEC / bps_progress
                    } else {
                        0.0
                    };
                    let promoted_errors = (map.stats().bytes_unreadable / 2048) as u32;
                    update_state_with(device, |s| {
                        s.errors = promoted_errors;
                        s.total_lost_ms = promoted_total_lost_ms;
                        s.main_lost_ms = promoted_main_lost_ms;
                        s.bad_ranges = promoted_bad_ranges;
                        s.num_bad_ranges = promoted_num_bad;
                        s.bad_ranges_truncated = promoted_truncated;
                        s.largest_gap_ms = promoted_largest_gap_ms;
                    });
                }
            } else {
                // Fail-safe: the mapfile could not be loaded, so we cannot
                // promote residual NonTrimmed bytes to Unreadable nor measure
                // in-title loss. Leaving `main_lost_*_for_history` at their 0
                // initializers would let the abort gate below conclude "no
                // loss" and deliver a possibly-lossy rip as perfect. Instead,
                // mark loss unquantifiable (NaN ms) so `loss_aborts` fires
                // regardless of threshold — a mapfile we cannot read at the
                // abort-decision point means the rip's damage record is gone,
                // and the safe verdict is abort/quarantine, never silent
                // delivery.
                crate::log::device_log(
                    device,
                    "Recovery mapfile could not be loaded to verify loss — forcing abort (cannot confirm a clean rip)",
                );
                tracing::error!(
                    device = %device,
                    mapfile = %mapfile_path_str,
                    "end_of_recovery_promote: mapfile load failed at abort-decision point; forcing abort (loss unquantifiable)"
                );
                main_lost_ms_for_history = f64::NAN;
            }

            // ISO output is whole-disc and must be byte-complete: the per-title
            // tolerance is ignored (forced to 0). MKV/M2TS use the configured value.
            let effective_abort =
                effective_abort_secs(&cfg_read.output_format, cfg_read.abort_on_lost_secs);
            if loss_aborts(
                main_lost_bytes_for_history,
                main_lost_ms_for_history,
                effective_abort,
            ) {
                crate::log::device_log(
                    device,
                    &format!(
                        "ABORT: strategy=abort_check triggered — {:.2}s lost in main movie (threshold: {}s)",
                        main_lost_ms_for_history / MILLIS_PER_SEC,
                        effective_abort
                    ),
                );

                crate::log::device_log(
                    device,
                    &format!(
                        "STRATEGY_FAILURE: abort_check FAILED — data loss ({:.2}s) exceeds threshold ({}s)",
                        main_lost_ms_for_history / MILLIS_PER_SEC,
                        effective_abort
                    ),
                );

                crate::log::device_log(
                    device,
                    &if output_is_iso_image(&cfg_read.output_format) {
                        "RECOVERY_GUIDANCE: ISO output is a whole-disc image and requires 100% — abort_on_lost_secs does not apply (it is a MUXED-output setting, ignored for ISO). The loss is unrecoverable media: clean or replace the disc, or choose MKV output to tolerate non-title damage.".to_string()
                    } else if effective_abort == 0 {
                        "RECOVERY_GUIDANCE: abort_on_lost_secs=0 requires a perfect rip — ANY unrecoverable loss in the main movie aborts here. To let a rip complete despite some loss, RAISE abort_on_lost_secs to the number of seconds of main-movie loss you can tolerate (e.g. 5 or 30).".to_string()
                    } else {
                        format!(
                            "RECOVERY_GUIDANCE: abort_on_lost_secs={}s limit exceeded — raise abort_on_lost_secs further or accept the loss after disc recovery.",
                            effective_abort
                        )
                    },
                );
                update_state_with(device, |s| {
                    s.status = "error".to_string();
                    // Surface the Accept-damage off-ramp: the complete ISO is on
                    // disk as a resumable `.aborted-loss`, so the operator can
                    // deliver it as-is instead of re-ripping.
                    s.loss_aborted = true;
                    if s.last_error.is_empty() {
                        s.last_error = format!(
                            "aborted — {} lost in main movie ({})",
                            fmt_loss(main_lost_ms_for_history),
                            fmt_threshold(effective_abort)
                        );
                    }
                });
                // Record the abort as a RESUMABLE `.aborted-loss` (not a
                // terminal `.failed`): the full ISO + mapfile are on disk, so a
                // raised `abort_on_lost_secs`, a fresh patch pass, or a code
                // change may bring the loss under threshold on a later attempt.
                // `mark_aborted_on_loss` keeps the dir RESUMABLE indefinitely:
                // a loss-abort is deterministic media damage a plain re-rip
                // won't change, so it is NEVER promoted to terminal `.failed` by
                // attempt count (the operator resolves it via Accept or another
                // pass). It also clears `.restart_count` so a deterministically-
                // lossy rip doesn't ALSO walk the crash-restart loop. The `bool`
                // it returns is now always `false`; the `if terminal` log below
                // is inert (retained so the call site compiles unchanged).
                let staging_disc_path = std::path::Path::new(&staging);
                let terminal = staging::mark_aborted_on_loss(
                    staging_disc_path,
                    &format!(
                        "aborted: {} lost in main movie ({})",
                        fmt_loss(main_lost_ms_for_history),
                        fmt_threshold(effective_abort)
                    ),
                );
                if terminal {
                    crate::log::device_log(
                        device,
                        "Abort-on-loss retry budget exhausted — quarantining (.failed).",
                    );
                }
                unregister_halt(device);
                return; // Skip mux entirely
            }

            if main_lost_ms_for_history > 0.0 {
                crate::log::device_log(
                    device,
                    &format!(
                        "Main movie loss after retries: {:.2}s (threshold: {}s)",
                        main_lost_ms_for_history / MILLIS_PER_SEC,
                        effective_abort
                    ),
                );
            } else {
                crate::log::device_log(device, "All data recovered — proceeding with mux.");
            }
        }

        // Mux gating: skip mux + return cleanly if user pressed stop.
        if user_halt.load(Ordering::Relaxed) {
            crate::log::device_log(device, "Rip cancelled — skipping mux.");
            unregister_halt(device);
            return;
        }
        // (The per-pass wall-clock cap and its mux-skip branch were
        // removed 2026-06-04 along with `cap_fired_any` — a pass is now
        // bounded only by its own work + libfreemkv's stall watchdogs, so
        // there is no cap-fire failure signal to gate the mux on.)

        // ISO output: the deliverable is the whole-disc image we just swept,
        // not a re-muxed title. The settings UI promises "ISO copies the whole
        // disc; the other formats mux selected titles" — so skip the title mux
        // entirely and hand the operator the intermediate `<name>.iso`. The
        // abort gate above already scoped loss whole-disc for this mode, and
        // the mover validates + moves `.iso` (its move filter is widened for
        // ISO output via `retain_intermediate_iso`, and the ISO is never
        // pruned here). Without this branch an ISO rip would fall through to
        // the MKV mux below and the user would receive a `.mkv` selected-title
        // mux — the opposite of what was requested.
        if output_is_iso_image(&cfg_read.output_format) {
            let iso_path = std::path::Path::new(&iso_path_str);
            // Durability gate before the success markers, mirroring the MKV
            // path's fsync-before-.done: a crash must not leave a `.done`
            // pointing at a page-cache-only ISO. If the fsync fails, withhold
            // the markers and preserve staging so a later attempt re-runs the
            // flush rather than handing the mover a possibly-truncated image.
            if !staging::durability_gate_passes(false, || staging::fsync_output_file(iso_path)) {
                crate::log::device_log(
                    device,
                    "Durability gate failed: could not fsync ISO image to stable storage; \
                     withholding .done/.completed and preserving staging for retry",
                );
                update_state_with(device, |s| {
                    if s.last_error.is_empty() {
                        s.last_error =
                            "ISO image not durable (fsync failed); rip preserved for retry"
                                .to_string();
                    }
                });
                unregister_halt(device);
                return;
            }
            let staging_path = std::path::Path::new(&staging);
            // Confident match → `state: Done` (mover files it); otherwise
            // `state: Review` (operator confirms the title before it leaves
            // staging). One `state.json` transition carries the mover metadata +
            // the single ISO output. TV structuring metadata (`season`/`disc`)
            // is parsed from the raw disc label; `tmdb_id` threads through so the
            // mover can fold the rip under `Show (Year)/Season NN/`.
            let marker_name = staging::handoff_label(title_confident);
            let iso_leaf = iso_filename.clone();
            if let Err(e) = staging::mark_handoff(staging_path, title_confident, |s| {
                s.title = display_name.clone();
                s.disc_name = disc_name.clone();
                s.disc_format = disc_format.clone();
                s.year = tmdb_year;
                s.media_type = tmdb_media_type.clone();
                s.tmdb_id = tmdb_id;
                s.tmdb_poster = tmdb_poster.clone();
                s.tmdb_overview = tmdb_overview.clone();
                s.season = crate::tmdb::season_from_label(&disc_name);
                s.disc_number = crate::tmdb::disc_from_label(&disc_name);
                s.outputs = vec![staging::Output {
                    filename: iso_leaf,
                    ..Default::default()
                }];
            }) {
                crate::log::device_log(
                    device,
                    &format!(
                        "{marker_name} state write failed ({e}); ISO is staged but the mover cannot pick it up"
                    ),
                );
                update_state_with(device, |s| {
                    if s.last_error.is_empty() {
                        s.last_error = format!("{marker_name} state write failed: {e}");
                    }
                });
                unregister_halt(device);
                return;
            }
            staging::write_completed_marker(staging_path);
            staging::clear_restart_count(staging_path);
            crate::log::device_log(
                device,
                &format!("ISO output complete — disc image staged as {iso_filename}"),
            );
            update_state_with(device, |s| {
                s.status = "done".to_string();
                s.output_file = iso_filename.clone();
            });
            // Rip stage done (ISO delivery — no mux stage follows; the ISO is
            // the deliverable and the mover fires move_complete later). Fire
            // the drive-free hook at the eject decision point.
            fire_rip_complete_webhook(
                &cfg_read,
                device,
                &display_name,
                &disc_format,
                &tmdb_poster,
                tmdb_year,
                &duration,
                &codecs,
                &iso_path_str,
            );
            if should_auto_eject(cfg_read.auto_eject, device) {
                if let Some(h) = device_halt(device) {
                    h.cancel();
                }
                drop(session);
                eject_drive(device_path);
            } else {
                drop(session);
                unregister_halt(device);
            }
            return;
        }

        // v0.25.3 parallel pipeline hand-off — sweep + patch are done,
        // the ISO is on disk, the drive is no longer needed. Write
        // the `.ripped` marker so the muxer worker can pick this
        // staging dir up on its next tick, eject the disc if the
        // operator asked for it, return the drive tile to idle, and
        // exit `rip_disc`. The mux + post-mux bookkeeping that used
        // to run below now runs inside `muxer::check_and_mux ->
        // ripper::resume::remux_from_ripped_marker`.
        //
        // Snapshot sweep damage from STATE before building the marker.
        // Snapshot the final post-promotion damage from STATE (already updated
        // above, including promoted NonTrimmed→Unreadable bytes) and carry it
        // into the marker so remux_from_ripped_marker can restore
        // SweepDamageSnapshot on resume without re-reading the mapfile.
        let marker_damage = {
            let s = state::STATE.lock().unwrap_or_else(|e| e.into_inner());
            s.get(device).map(|rs| mux::SweepDamageSnapshot {
                errors: rs.errors,
                total_lost_ms: rs.total_lost_ms,
                main_lost_ms: rs.main_lost_ms,
                bad_ranges: rs.bad_ranges.clone(),
                num_bad_ranges: rs.num_bad_ranges,
                bad_ranges_truncated: rs.bad_ranges_truncated,
                largest_gap_ms: rs.largest_gap_ms,
            })
        };
        let marker = crate::muxer::RippedMarker {
            schema_version: crate::muxer::RIPPED_MARKER_SCHEMA,
            iso_path: iso_path_str.clone(),
            mapfile_path: mapfile_path_str.clone(),
            display_name: display_name.clone(),
            disc_format: disc_format.clone(),
            mkv_filename: filename.clone(),
            tmdb_title: tmdb_title.clone(),
            tmdb_year,
            tmdb_poster: tmdb_poster.clone(),
            tmdb_overview: tmdb_overview.clone(),
            tmdb_media_type: tmdb_media_type.clone(),
            max_retries: cfg_read.max_retries,
            abort_on_lost_secs: cfg_read.abort_on_lost_secs as u32,
            rip_elapsed_secs: 0.0, // mux worker re-derives elapsed from its own start
            rip_errors: 0,
            rip_lost_video_secs: main_lost_ms_for_history / MILLIS_PER_SEC,
            rip_last_sector: rip_last_lba.load(Ordering::Relaxed),
            origin_device: device.to_string(),
            sweep_errors: marker_damage.as_ref().map(|d| d.errors).unwrap_or(0),
            sweep_total_lost_ms: marker_damage
                .as_ref()
                .map(|d| d.total_lost_ms)
                .unwrap_or(0.0),
            sweep_main_lost_ms: marker_damage
                .as_ref()
                .map(|d| d.main_lost_ms)
                .unwrap_or(0.0),
            sweep_num_bad_ranges: marker_damage
                .as_ref()
                .map(|d| d.num_bad_ranges)
                .unwrap_or(0),
            sweep_largest_gap_ms: marker_damage
                .as_ref()
                .map(|d| d.largest_gap_ms)
                .unwrap_or(0.0),
            // Carry the fresh-rip confidence verdict (which already folds in
            // the operator '✎ change' override) so the mux worker's
            // resume_remux doesn't recompute confidence from the match check
            // alone and second-guess a deliberate operator pick into .review.
            title_confident,
        };
        let staging_path = std::path::Path::new(&staging);
        if let Err(e) = crate::muxer::write_marker(staging_path, &marker) {
            // Couldn't hand off — fall back to the inline mux below
            // by NOT taking the early-return branch. Log the failure
            // so the cause is on the device log.
            crate::log::device_log(
                device,
                &format!(".ripped marker write failed ({e}); falling back to inline mux"),
            );
        } else {
            // Record the TV-routing metadata the `RippedMarker` does not carry
            // (`tmdb_id`, parsed `season`/`disc`, raw `disc_name`) and the
            // deliverable PLAN (`outputs[]`) onto the just-written `state: Ripped`
            // so it propagates through the mux/resume hand-off into the mover.
            // For a movie the plan is one output (byte-identical); for a TV disc
            // under `tv_auto` it is one per episode. The mux worker fans out over
            // this list; the mover files each. Recording it here also fixes TV
            // MKV rips previously losing their season on every non-ISO path.
            let plan = plan_mux_outputs(
                &disc.titles,
                &cfg_read,
                &tmdb_media_type,
                &disc_name,
                tmdb_id,
                &filename,
            );
            staging::mutate_state_if_present(staging_path, |s| {
                s.tmdb_id = tmdb_id;
                s.disc_name = disc_name.clone();
                s.season = crate::tmdb::season_from_label(&disc_name);
                s.disc_number = crate::tmdb::disc_from_label(&disc_name);
                s.outputs = plan;
            });
            crate::log::device_log(
                device,
                "Sweep + patch complete; handed off to mux worker via .ripped marker.",
            );
            // Status: "done" — the DISC READ is complete. Sweep + patch
            // captured the whole-disc ISO; the drive is no longer needed
            // and (with auto_eject) is ejected just below. The mux is a
            // SEPARATE phase that runs off the staged ISO and is tracked
            // in the System tab's Mux queue via the synthetic `_mux`
            // device — it must NOT keep the real drive's tile on
            // "ripping". Marking the real device "done" here also means
            // the mux worker's post-mux `still_ripping` revert
            // (`crate::muxer::check_and_mux`) is a no-op for this device:
            // the synthetic mux device can never revert this tile's
            // status, so the read-complete view is stable for the whole
            // mux. (Previously this set "ripping", leaving the tile stuck
            // on "Ripping" for the entire mux — the user-visible bug.)
            //
            // Carry damage fields (errors, total_lost_ms, main_lost_ms,
            // bad_ranges, largest_gap_ms) from the current STATE entry so
            // /api/state doesn't show zeroed damage during the hand-off
            // window. push_pass_state wrote those fields; a bare
            // ..Default::default() would zero them until the mux worker's
            // first push_state tick re-derives them from sweep_damage.
            let handoff_damage = {
                let s = state::STATE.lock().unwrap_or_else(|e| e.into_inner());
                s.get(device).map(|rs| mux::SweepDamageSnapshot {
                    errors: rs.errors,
                    total_lost_ms: rs.total_lost_ms,
                    main_lost_ms: rs.main_lost_ms,
                    bad_ranges: rs.bad_ranges.clone(),
                    num_bad_ranges: rs.num_bad_ranges,
                    bad_ranges_truncated: rs.bad_ranges_truncated,
                    largest_gap_ms: rs.largest_gap_ms,
                })
            };
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "done".to_string(),
                    // The read is finished; the tile shows a completed
                    // (100%) card while the mux runs separately. The
                    // delivered output file is the MKV the mux worker will
                    // write under this name into the same staging dir.
                    progress_pct: 100,
                    output_file: filename.clone(),
                    disc_present: true,
                    disc_name: display_name.clone(),
                    disc_format: disc_format.clone(),
                    tmdb_title: tmdb_title.clone(),
                    tmdb_year,
                    tmdb_poster: tmdb_poster.clone(),
                    tmdb_overview: tmdb_overview.clone(),
                    duration: duration.clone(),
                    codecs: codecs.clone(),
                    errors: handoff_damage
                        .as_ref()
                        .map(|d| d.errors)
                        .unwrap_or_default(),
                    total_lost_ms: handoff_damage
                        .as_ref()
                        .map(|d| d.total_lost_ms)
                        .unwrap_or_default(),
                    main_lost_ms: handoff_damage
                        .as_ref()
                        .map(|d| d.main_lost_ms)
                        .unwrap_or_default(),
                    bad_ranges: handoff_damage
                        .as_ref()
                        .map(|d| d.bad_ranges.clone())
                        .unwrap_or_default(),
                    num_bad_ranges: handoff_damage
                        .as_ref()
                        .map(|d| d.num_bad_ranges)
                        .unwrap_or_default(),
                    bad_ranges_truncated: handoff_damage
                        .as_ref()
                        .map(|d| d.bad_ranges_truncated)
                        .unwrap_or_default(),
                    largest_gap_ms: handoff_damage
                        .as_ref()
                        .map(|d| d.largest_gap_ms)
                        .unwrap_or_default(),
                    ..Default::default()
                },
            );
            // Rip stage done: the disc read finished, the ISO is staged and
            // handed off to the mux worker, and the drive is now free for the
            // next disc. Fire the drive-free hook here — at the eject decision
            // point — BEFORE the separate mux worker later fires mux_complete.
            fire_rip_complete_webhook(
                &cfg_read,
                device,
                &display_name,
                &disc_format,
                &tmdb_poster,
                tmdb_year,
                &duration,
                &codecs,
                &iso_path_str,
            );
            if should_auto_eject(cfg_read.auto_eject, device) {
                // eject_drive handles drain + drop_session + unregister_halt
                // internally. Cancel the halt first so any in-flight work
                // exits cleanly before the eject SCSI command issues.
                if let Some(h) = device_halt(device) {
                    h.cancel();
                }
                drop(session);
                eject_drive(device_path);
            } else {
                drop(session);
                unregister_halt(device);
            }
            return;
        }

        // Fallback inline-mux path (only reached if the marker write
        // above failed). Closes drive, opens ISO, runs mux as before.
        crate::log::device_log(device, "Drive released; muxing ISO → MKV.");
        // Rip stage done even on the fallback path: the drive is released here,
        // before the inline mux runs, so fire the drive-free hook now (the
        // inline mux fires mux_complete when the .mkv is written, below).
        fire_rip_complete_webhook(
            &cfg_read,
            device,
            &display_name,
            &disc_format,
            &tmdb_poster,
            tmdb_year,
            &duration,
            &codecs,
            &iso_path_str,
        );
        drop(session);

        // Open the ISO for the mux pipeline.
        let iso_reader =
            match libfreemkv::FileSectorSource::open(std::path::Path::new(&iso_path_str)) {
                Ok(r) => {
                    use libfreemkv::sector::SectorSource;
                    crate::log::device_log(
                        device,
                        &format!("ISO opened successfully: {} sectors", r.capacity_sectors()),
                    );
                    r
                }
                Err(e) => {
                    let msg = format_lib_error("Open ISO", &e);
                    crate::log::device_log(device, &msg);
                    // Cannot open the ISO for mux — if the sweep was
                    // interrupted before any ISO data flushed (and the
                    // `.ripped` hand-off also failed, which is the only way
                    // this inline-mux fallback is reached), this ENOENT
                    // repeats on every startup. Quarantine with `.failed`
                    // so the restart scan classifies it terminal instead of
                    // leaving it stranded `InProgress`.
                    let staging_disc_path = std::path::Path::new(&staging);
                    staging::write_failed_marker(staging_disc_path, &msg);
                    staging::clear_restart_count(staging_disc_path);
                    update_state(
                        device,
                        RipState {
                            device: device.to_string(),
                            status: "failed".to_string(),
                            disc_present: true,
                            last_error: msg.clone(),
                            failure_reason: Some(msg),
                            disc_name: display_name,
                            disc_format,
                            tmdb_title,
                            tmdb_year,
                            tmdb_poster,
                            tmdb_overview,
                            duration,
                            codecs,
                            ..Default::default()
                        },
                    );
                    unregister_halt(device);
                    return;
                }
            };
        // Capture the final bytes_unreadable for the mux call site (which
        // is outside this multipass branch). Used by `total_pct_byte_weight`
        // to size the total-progress denominator. By this point retries
        // are done and the abort check has passed (we're entering mux).
        bytes_unreadable_at_mux = bytes_unreadable;

        // Entering mux phase — push final mapfile state so the UI keeps the
        // bad-range list visible through mux and into the "done" view. The lib
        // builds the snapshot from the mapfile (autorip never parses it).
        let mux_state = std::cell::RefCell::new(PassProgressState::new());
        if let Some(snap) = freemkv_engine::progress_snapshot_from_mapfile(
            std::path::Path::new(&mapfile_path_str),
            Some(&title_for_progress),
            libfreemkv::progress::PassKind::Mux,
            pass_ctx.bytes_total_disc,
        ) {
            push_pass_state(
                &pass_ctx,
                &snap,
                bps_progress,
                total_passes,
                total_passes,
                &mux_state,
            );
        }
        // Snapshot the damage fields just written to STATE so the mux phase
        // can carry them forward in every per-frame push_state call.
        // Without this snapshot, push_state's `..Default::default()` would
        // zero out errors / total_lost_ms / bad_ranges on the very first
        // mux tick, making a damaged disc look perfectly clean during mux.
        sweep_damage_snapshot = {
            let s = state::STATE.lock().unwrap_or_else(|e| e.into_inner());
            s.get(device)
                .map(|rs| mux::SweepDamageSnapshot {
                    errors: rs.errors,
                    total_lost_ms: rs.total_lost_ms,
                    main_lost_ms: rs.main_lost_ms,
                    bad_ranges: rs.bad_ranges.clone(),
                    num_bad_ranges: rs.num_bad_ranges,
                    bad_ranges_truncated: rs.bad_ranges_truncated,
                    largest_gap_ms: rs.largest_gap_ms,
                })
                .unwrap_or_default()
        };
        Box::new(iso_reader) as Box<dyn libfreemkv::SectorSource>
    } else {
        Box::new(session.drive) as Box<dyn libfreemkv::SectorSource>
    };

    // Keyless-capture mux-skip: an encrypted disc with no usable keys was
    // swept to a raw ISO above (sweep needs no keys). Muxing now with
    // `DecryptKeys::None` would write a garbage/encrypted MKV, so SKIP the
    // mux entirely and PRESERVE staging (ISO + mapfile) so the deferred
    // mux can run once keys exist.
    //
    // Reachability:
    //   - multipass (max_retries > 0): the primary route already returned
    //     via the `.ripped` marker hand-off above; the muxer worker re-tries
    //     and `resume_remux` applies the same no-keys deferral. We only land
    //     here on the rare marker-write-failure fallback — keep the ISO.
    //   - single-pass (max_retries == 0): live disc→MKV with no ISO
    //     intermediate. There's nothing to defer to, but we must NOT write a
    //     garbage MKV. Skip and surface the deferral reason.
    // `defer_forensic_mux` (FMTS CaptureOnly) joins `keys_missing` here: the base
    // AACS keys are present but the forensic index keys are not, so muxing now would
    // emit garbage — defer it exactly like the no-keys capture flow. Multi-pass FMTS
    // CaptureOnly normally returns via the `.ripped` hand-off above (and defers in
    // `resume_remux` on `FmtsKeyMissing`); it only lands here on the rare
    // marker-write-failure fallback, where preserving the ISO is still correct.
    if keys_missing || defer_forensic_mux {
        let msg = keyless_failure_message(&disc);
        if uses_multipass(cfg_read.max_retries) {
            let (log_line, state_err) = if defer_forensic_mux {
                (
                    format!(
                        "Ripped to ISO — forensic keys unavailable, mux deferred. ISO + mapfile \
                         preserved in staging ({staging}); auto-resume will mux once keys are available."
                    ),
                    "Ripped to ISO — forensic keys unavailable, mux deferred.".to_string(),
                )
            } else {
                (
                    format!(
                        "Ripped to ISO — no keys, mux deferred. ISO + mapfile preserved in staging \
                         ({staging}); auto-resume will mux once keys are available. {msg}"
                    ),
                    format!("Ripped to ISO — no keys, mux deferred. {msg}"),
                )
            };
            crate::log::device_log(device, &log_line);
            update_state_with(device, |s| {
                s.status = "idle".to_string();
                s.last_error = state_err;
            });
        } else {
            let (log_line, state_err) = if defer_forensic_mux {
                (
                    "FMTS single-pass rip — forensic keys unavailable, cannot mux (no ISO captured). \
                     Enable multi-pass mode to capture a deferred-mux ISO."
                        .to_string(),
                    "Forensic keys unavailable — cannot mux. \
                     (multi-pass mode captures an ISO for deferred mux.)"
                        .to_string(),
                )
            } else {
                (
                    format!(
                        "Single-pass rip with no keys — cannot mux (no ISO captured). \
                         Enable multi-pass mode to capture a deferred-mux ISO. {msg}"
                    ),
                    format!(
                        "No keys — cannot mux. {msg} (multi-pass mode captures an ISO for deferred mux.)"
                    ),
                )
            };
            crate::log::device_log(device, &log_line);
            update_state_with(device, |s| {
                s.status = "error".to_string();
                s.last_error = state_err;
            });
        }
        unregister_halt(device);
        return;
    }

    // Debug log reader type for mux - confirms ISO vs drive source
    tracing::debug!(target: "mux", " mux using reader: {}", if uses_multipass(cfg_read.max_retries) { "ISO file (multipass)" } else { "physical drive" });

    // DiscStream gets the per-device `Halt` at construction (passed
    // directly to `DiscStream::new`, so it also covers the CSS crack
    // that runs there). Stop interrupts `fill_extents` at the next
    // retry boundary on the same signal that breaks sweep, patch, and
    // the mux frame loop — required for Stop to work during dense
    // bad-sector regions where the outer PES read() loop may never
    // emit a frame.
    //
    // Multipass ISO path: wrap the reader in a `PrefetchedSectorSource`
    // so the read+decrypt work runs on a dedicated producer thread
    // while the mux consumer (demux + codec parsers + writer) runs on
    // the main thread. On the testbed this took the null:// /
    // consumer ceiling from ~70 MB/s to ~124 MB/s and lets production
    // mux push closer to the disk's combined r+w wall. The drive
    // single-pass path keeps the inline reader because `DiscStream`'s
    // adaptive batch retry only fires inside `fill_extents` and the
    // prefetch wrapper would bypass it.
    //
    // The reader-side stream events (BytesRead / BatchSizeChanged /
    // SectorSkipped) are now forwarded by libfreemkv's `mux_stream` through the
    // `AutoripMuxEvents` bridge (in `mux.rs`) for BOTH the multipass ISO path
    // (`mux_iso`) and the single-pass inline path (`mux_live`) — so the old
    // shared `make_stream_event_fn` closure is gone; the atomics it fed
    // (`wd_last_frame`, `rip_last_lba`, `rip_current_batch`, `latest_bytes_read`)
    // are handed to the bridge via `MuxAtomics` below.

    // Mux-phase progress denominator. The multipass/resume highway reads the
    // WHOLE disc-capacity ISO, so its `BytesRead` climbs to `disc.capacity_bytes`
    // — keep `total_bytes` (disc capacity) as-is there. The single-pass path
    // (max_retries == 0) streams ONLY the selected title's extents over the live
    // drive, so `DiscStream`'s `BytesRead` caps at the title's extent byte sum
    // (`bytes_total_extents` = Σ sector_count × 2048). Using disc capacity as the
    // denominator there made the live progress bar / ETA plateau at
    // title_size ÷ disc_capacity (e.g. ~50% for a 25 GB title on a 50 GB disc).
    // Scope the denominator to the same extent sum the read source reports so the
    // bar reaches 100%. Computed before `title` is moved into the stream below.
    let mux_total_bytes = mux_progress_denominator(cfg_read.max_retries, total_bytes, &title);

    let _mux_span =
        tracing::span!(tracing::Level::TRACE, "rip_disc::run_mux", device=%device, total_bytes)
            .entered();
    let mux_input_errors = Arc::new(AtomicU32::new(0));
    let mux_inputs = mux::MuxInputs {
        device,
        display_name: display_name.clone(),
        disc_format: disc_format.clone(),
        tmdb_title: tmdb_title.clone(),
        tmdb_year,
        tmdb_poster: tmdb_poster.clone(),
        tmdb_overview: tmdb_overview.clone(),
        duration: duration.clone(),
        codecs: codecs.clone(),
        filename: filename.clone(),
        total_bytes: mux_total_bytes,
        title_bytes_per_sec,
        total_passes,
        bytes_total_disc: disc.capacity_bytes,
        max_retries: cfg_read.max_retries,
        bytes_unreadable_at_mux,
        dest_url: dest_url.clone(),
        batch,
        // Hand the mux watchdog the per-disc staging dir so its
        // hard-escalation path (5-minute stall → exit + Docker
        // restart) can bump `.restart_count` before exiting.
        staging_disc_dir: std::path::PathBuf::from(&staging),
        sweep_damage: sweep_damage_snapshot.clone(),
    };
    let mux_atomics = mux::MuxAtomics {
        latest_bytes_read: latest_bytes_read.clone(),
        rip_last_lba: rip_last_lba.clone(),
        rip_current_batch: rip_current_batch.clone(),
        wd_last_frame: wd_last_frame.clone(),
        wd_bytes: Arc::new(AtomicU64::new(0)),
        input_errors: mux_input_errors,
    };

    let mux_outcome = if uses_multipass(cfg_read.max_retries) {
        // Multipass ISO mux → libfreemkv::mux_stream (STEP 4c-i). The header
        // pump / producer / write-pipeline / finish all live inside mux_stream
        // now; AutoripMuxEvents feeds the same watchdog + UI atomics.
        let iso_src = mux::IsoMuxSource {
            iso_path: std::path::PathBuf::from(&iso_path_str),
            title,
            format,
            keys,
            // Fresh-key-on-failure fetch (recover a 2nd/Nth CPS-unit key mid-mux)
            // — the SAME closure the pre-migration build_iso_pipeline call took.
            key_fetch: crate::keysource::build_iso_key_fetch(
                &cfg_read,
                std::path::Path::new(&iso_path_str),
            ),
            raw: false,
            skip_errors: false,
        };
        match mux::mux_iso(mux_inputs, iso_src, mux_atomics) {
            Ok(o) => o,
            Err(e) => {
                // Construction-time classification preserved verbatim: a Stop
                // pressed during the CSS crack inside pipeline construction
                // surfaces as `Error::Halted` — a user halt, not a structural
                // failure: preserve staging for auto-resume, no `.failed`.
                if is_halt_error(&e) {
                    crate::log::device_log(
                        device,
                        "Rip stopped by user during mux setup — staging preserved for resume.",
                    );
                    unregister_halt(device);
                    return;
                }
                // A pipeline BUILD failure (header resolution, codec negotiation,
                // format error) is structural and permanent — retries won't fix
                // it. Quarantine the dir with `.failed` (mirrors the header-phase
                // failure path below).
                tracing::error!(target: "mux", device=%device, "mux_stream setup failed: {e}");
                let msg = format!(
                    "Mux setup failed — the disc's title or stream layout could not be prepared for muxing. The source may be damaged or use an unsupported format ({e})."
                );
                crate::log::device_log(device, &msg);
                let staging_disc_path = std::path::Path::new(&staging);
                staging::write_failed_marker(staging_disc_path, &msg);
                staging::clear_restart_count(staging_disc_path);
                update_state_with(device, |s| {
                    s.status = "failed".to_string();
                    s.last_error = msg.clone();
                    s.failure_reason = Some(msg.clone());
                });
                unregister_halt(device);
                return;
            }
        }
    } else {
        // Drive single-pass path (STEP 4c-ii): the live inline `DiscStream` via
        // libfreemkv::mux_stream (`MuxInput::Live`). It stays INLINE — NOT the
        // prefetch highway — because `DiscStream::fill_extents`' adaptive
        // batch-retry on a bad live-drive sector only fires on the inline reader;
        // the highway wrapper would bypass it. The header pump / write pipeline /
        // finish loop the hand-rolled `run_mux` used to own now live inside
        // `mux_stream`; `AutoripMuxEvents` feeds the same watchdog + UI atomics.
        // The forensic `fmts_key_map` is applied inside `mux_stream` via
        // `DiscStream::with_key_map` (single-pass FMTS: read only our-phase units
        // and decrypt the forensic segment correctly); `None` for non-FMTS discs.
        let live_src = mux::LiveMuxSource {
            reader,
            title,
            format,
            keys,
            key_map: fmts_key_map.clone(),
            skip_errors: skip_read_errors(&cfg_read.on_read_error),
        };
        match mux::mux_live(mux_inputs, live_src, mux_atomics) {
            Ok(o) => o,
            Err(e) => {
                // Same construction-time classification as the multipass branch
                // and the pre-migration inline path: a Stop pressed during the CSS
                // crack inside `DiscStream::new` (run within `mux_stream`) surfaces
                // as `Error::Halted` — a user halt, not a structural failure:
                // preserve staging for auto-resume, no `.failed`.
                if is_halt_error(&e) {
                    crate::log::device_log(
                        device,
                        "Rip stopped by user during mux setup — staging preserved for resume.",
                    );
                    unregister_halt(device);
                    return;
                }
                // A build failure (header resolution, codec negotiation, or a
                // scrambled-but-uncrackable CSS DVD → CssKeyMissing) is structural
                // and permanent — retries won't fix it. Quarantine with `.failed`
                // (mirrors the multipass branch and the header-phase path below).
                tracing::error!(target: "mux", device=%device, "mux_stream (live) setup failed: {e}");
                let msg = format!(
                    "Mux setup failed — the disc's title or stream layout could not be prepared for muxing. The source may be damaged or use an unsupported format ({e})."
                );
                crate::log::device_log(device, &msg);
                let staging_disc_path = std::path::Path::new(&staging);
                staging::write_failed_marker(staging_disc_path, &msg);
                staging::clear_restart_count(staging_disc_path);
                update_state_with(device, |s| {
                    s.status = "failed".to_string();
                    s.last_error = msg.clone();
                    s.failure_reason = Some(msg.clone());
                });
                unregister_halt(device);
                return;
            }
        }
    };

    // Output never opened. Two sub-cases:
    //
    //   a) `finalize_error == None` — a clean stop during header read
    //      (halt / EOF with headers already resolvable but cancelled).
    //      The pre-split code returned early without writing a history
    //      record or marker, leaving the dir resumable. Preserve that.
    //
    //   b) `finalize_error == Some(msg)` — run_mux gave up in the header
    //      phase because the stream is structurally unusable: the header
    //      buffer exceeded its cap before codec_privates resolved, or EOF
    //      / a read error hit before `headers_ready()` (the
    //      header-resolution-incomplete path). No output file exists, but
    //      this is a terminal failure, not a resumable stop. Falling
    //      through to the bare return drops the reason on the floor: no
    //      `.failed` marker (so resume-on-startup may re-resume a dir that
    //      can never succeed) and the device tile stays in its prior
    //      `status="ripping"` with the reason only in the device log.
    //      Quarantine + surface it, mirroring the post-finalize failure
    //      path below (write `.failed`, status="failed", reason in
    //      `last_error`). No output file to fsync (none was opened).
    let header_phase = header_phase_disposition(
        mux_outcome.output_opened,
        mux_outcome.finalize_error.as_deref(),
    );
    if let HeaderPhase::ResumableStop | HeaderPhase::Failed = header_phase {
        unregister_halt(device);
        if let HeaderPhase::Failed = header_phase {
            let reason = mux_outcome
                .finalize_error
                .as_ref()
                .expect("finalize_error is Some when the header phase is HeaderPhase::Failed");
            crate::log::device_log(device, &format!("Mux failed: {reason}"));
            let staging_disc_path = std::path::Path::new(&staging);
            staging::write_failed_marker(
                staging_disc_path,
                &format!("mux header phase failed: {reason}"),
            );
            staging::clear_restart_count(staging_disc_path);
            let failure_reason = Some(format!("mux header phase failed: {reason}"));
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "failed".to_string(),
                    disc_present: true,
                    disc_name: display_name.clone(),
                    disc_format: disc_format.clone(),
                    tmdb_title: tmdb_title.clone(),
                    tmdb_year,
                    tmdb_poster: tmdb_poster.clone(),
                    tmdb_overview: tmdb_overview.clone(),
                    duration: duration.clone(),
                    codecs: codecs.clone(),
                    last_error: failure_reason.clone().unwrap_or_default(),
                    failure_reason,
                    ..Default::default()
                },
            );
        }
        return;
    }

    // Clean up halt flag
    unregister_halt(device);

    let completed = mux_outcome.completed;
    let bytes_done = mux_outcome.bytes_done;
    let elapsed = mux_outcome.elapsed_secs;
    let speed = mux_outcome.speed_mbs;
    // 0.20.8 validation-audit fix #1: if `MuxSink::close` failed inside
    // `output.finish()`, the MKV is structurally invalid (unseekable —
    // Cues never landed and the segment-info length header wasn't
    // patched). Quarantine the staging dir with `.failed` and report
    // status=failed in the history record. Skipped here for halt /
    // timeout / panic — those are wedge cases handled by the existing
    // "stopped" path so the user can retry.
    let finalize_error = mux_outcome.finalize_error.clone();
    // A hard producer read error (on_read_error=stop saw an unrecoverable
    // read Err and truncated the MKV) is reported here, distinct from a
    // user-initiated halt. Both yield `completed=false` with no
    // `finalize_error`, but only a halt should fall through to the silent
    // "stopped → idle" path: a read failure must surface on `/api/state`
    // (status="error" + last_error) so the operator sees the rip failed
    // due to a read error rather than a user stop. The disc stays
    // resumable (no `.failed` quarantine — a transient drive/NFS read may
    // succeed on retry), matching run_mux's resumable-stop semantics.
    let read_error = mux_outcome.read_error.clone();
    // Streams the sink accepted frames for but could not deliver into the
    // finished container are NOT re-reported here. `map_iso_mux_outcome`
    // produces every outcome that can carry them and already logs
    // `undelivered_streams_note` into this same device log, so a summary copy
    // here was a second, differently-worded line for one event — one lossy mux
    // reading as two, and an alert on either phrase seeing half the story.
    let mut final_errors = mux_outcome.errors;
    let final_last_sector = rip_last_lba.load(Ordering::Relaxed);
    let final_current_batch = rip_current_batch.load(Ordering::Relaxed);
    let mut final_lost_secs = mux_outcome.lost_video_secs;
    // Demux-time loss (sectors that read into the ISO fine but fail AACS/CSS
    // decrypt at mux, or codec-corruption demux skips that zero-fill output).
    // This is the in-title-scoped demux-skip estimate from `run_mux`, the same
    // quantity the single-pass (mod.rs) and resume (resume.rs) success paths
    // fold into their reported figures. Captured BEFORE the multipass overwrite
    // below replaces `final_lost_secs` with the sweep-mapfile loss for the UI
    // card, so a fresh multi-pass rip can still report this demux loss. The mux
    // never aborts on it — it is concealed and tallied only.
    let demux_lost_secs = mux_outcome.lost_video_secs;
    // In multipass mode the `input.errors` counter above counts ISO→MKV demux
    // skips (usually zero — ISO reads don't fail). The real bad-sector count
    // lives in the mapfile sidecar. Prefer that when present.
    if uses_multipass(cfg_read.max_retries)
        && let Ok(map) = freemkv_engine::Mapfile::load(std::path::Path::new(&mapfile_path_str))
    {
        let stats = map.stats();
        // Only Unreadable counts as "lost" — NonTried / NonTrimmed /
        // NonScraped at the END of a rip means the rip was interrupted,
        // not that those bytes are damaged. For an interrupted rip the
        // final history record reflects what we know: unreadable = bad.
        let bad_bytes = stats.bytes_unreadable;
        final_errors = (bad_bytes / 2048) as u32;
        // Use the in-title-scoped loss already computed by abort_lost_ms()
        // (same gate the abort check used above). Whole-disc `bad_bytes /
        // title_bytes_per_sec` inflates the 'done' card when menus or trailers
        // outside the title extents are scratched — the abort gate correctly
        // accepted the rip because in-title loss was within threshold, but the
        // final UI card would show a larger number from out-of-title damage.
        final_lost_secs = if main_lost_ms_for_history_outer > 0.0 {
            main_lost_ms_for_history_outer / MILLIS_PER_SEC
        } else {
            // main_lost_ms_for_history_outer is 0 when either: no bad sectors
            // exist (clean disc), or bytes_unreadable == 0. In those cases
            // fall back to the mux outcome's own lost_video_secs (usually 0
            // on a clean disc, or the demux skip count for single-pass mode).
            mux_outcome.lost_video_secs
        };
    }

    // A loss is a loss. Mux-time (decrypt/codec) loss is missing in-title data
    // just like a read error — it's concealed for playability (NULL-TS fill +
    // drop-to-keyframe, so the file decodes clean) but the data is still gone.
    // So it is gated against `abort_on_lost_secs` below, right before the `.done`
    // marker, exactly as read-time loss is. The PRE-mux gate can't catch it (it
    // reads only the mapfile Unreadable set — decrypt/codec skips never appear
    // there), so this is the sole enforcement point for mux-time loss.

    // Emit a final mux summary line so the history record's captured log
    // ends with a clean terminal event instead of whatever the last 60s
    // progress tick happened to be. Without this, a mux that finishes
    // within 60s of its last tick freezes the log at e.g. "(84%) 21.8 MB/s
    // ETA 9:27" — visibly truncated even though the rip completed cleanly.
    // History snapshot below captures whatever's in LOGS, so write here.
    if completed {
        crate::log::device_log(
            device,
            &format!(
                "Mux complete: {:.1} GB in {}s ({:.1} MB/s avg)",
                bytes_done as f64 / BYTES_PER_GIB,
                elapsed.round() as u64,
                speed
            ),
        );
    } else if let Some(reason) = finalize_error.as_ref() {
        crate::log::device_log(device, &format!("Mux failed: {reason}"));
    }

    // ── Mux-time loss gate (a loss is a loss) ───────────────────────────────
    // Read-time loss was already gated PRE-mux (it reads the mapfile Unreadable
    // set); this catches the mux-time (decrypt/codec) loss the pre-mux gate can't
    // see, before filing `.done`. Over `abort_on_lost_secs` → quarantine to a
    // RESUMABLE `.aborted-loss` (a keydb refresh + re-mux can complete it),
    // exactly as the pre-mux abort does. ISO output is exempt (whole-disc, gated
    // by 100% elsewhere). Only fires when the MUX itself contributed loss —
    // read-time loss alone already passed the pre-mux gate.
    {
        let effective_abort =
            effective_abort_secs(&cfg_read.output_format, cfg_read.abort_on_lost_secs);
        let read_lost_secs = main_lost_ms_for_history_outer / MILLIS_PER_SEC;
        let total_lost_secs = read_lost_secs + demux_lost_secs;
        if mux_loss_aborts(
            completed,
            output_is_iso_image(&cfg_read.output_format),
            total_lost_secs,
            demux_lost_secs,
            effective_abort,
        ) {
            crate::log::device_log(
                device,
                &format!(
                    "ABORT: mux-time loss — {:.2}s missing in main movie (decrypt/codec) exceeds threshold ({}s). A loss is a loss.",
                    total_lost_secs, effective_abort
                ),
            );
            update_state_with(device, |s| {
                s.status = "error".to_string();
                s.loss_aborted = true;
                if s.last_error.is_empty() {
                    s.last_error = format!(
                        "aborted — {} lost at mux, decrypt/codec ({})",
                        fmt_loss(total_lost_secs * MILLIS_PER_SEC),
                        fmt_threshold(effective_abort)
                    );
                }
            });
            let _ = staging::mark_aborted_on_loss(
                std::path::Path::new(&staging),
                &format!(
                    "aborted: {:.2}s lost at mux, decrypt/codec ({})",
                    total_lost_secs,
                    fmt_threshold(effective_abort)
                ),
            );
            unregister_halt(device);
            return;
        }
    }

    // Write the staging markers (.done / .completed / .failed) the mover and the
    // resume-on-startup detector depend on. (The per-rip history record/log that
    // used to be written here was removed in 0.30.1 — the History tab was
    // unmaintained and didn't work; see web.rs.)
    {
        if completed {
            // Durability gate: fsync the finished MKV/M2TS before any
            // success marker so a crash/power-loss can't leave a "done"
            // marker pointing at a page-cache-only (truncated on disk)
            // file. Library mux finish() only flushes to the OS and the
            // bounded fsync inside it returns Ok even on timeout/halt — so
            // THIS fsync is the real durability gate. Skip for network://
            // output, which has no local file.
            //
            // If the fsync fails (false), do NOT write the
            // `.done`/`.completed` markers: the output is not provably
            // durable, so treat the rip as resumable this cycle. Leaving
            // the staging dir intact lets a later attempt re-run the flush
            // rather than handing a possibly-truncated file to the mover.
            let is_network = staging::is_network_output(&output_format, &cfg_read.network_target);
            if !staging::durability_gate_passes(is_network, || {
                staging::fsync_output_file(std::path::Path::new(&output_path))
            }) {
                abort_post_mux_preserving_staging(
                    device,
                    "Durability gate failed: could not fsync mux output to stable storage; \
                     withholding .done/.completed and preserving staging for retry",
                    "mux output not durable (fsync failed); rip preserved for retry",
                );
                return;
            }
            // Confident match (exact title + year) → hand straight to the mover
            // (.done). Otherwise HOLD for operator review (.review): the rip is
            // complete and staged, but we will NOT auto-file it into the library
            // under a guessed name. The Needs-review UI resolves it (pick the
            // right title → promotes to .done, or proceed as-is). "Better to
            // pause; worst case the operator clicks proceed." A would-overwrite
            // collision is still caught later by the mover's own guard.
            let marker_name = staging::handoff_label(title_confident);
            // One durable `state.json` transition (tmp + fsync + rename +
            // dir-fsync). The staging-dir fsync is the crash barrier: `state:
            // Done` is observed on disk before the later `.completed`/ISO-prune,
            // so a crash can never leave a completed/pruned dir without a durable
            // hand-off. Carries the mover metadata + the single MKV output, plus
            // the `season`/`tmdb_id`/`disc` the pre-unification MKV path dropped
            // (the bug that filed every TV MKV rip under `Season 01`).
            let staging_disc_path = std::path::Path::new(&staging);
            let mkv_leaf = filename.clone();
            if let Err(e) = staging::mark_handoff(staging_disc_path, title_confident, |s| {
                s.title = display_name.clone();
                s.disc_name = disc_name.clone();
                s.disc_format = disc_format.clone();
                s.year = tmdb_year;
                s.media_type = tmdb_media_type.clone();
                s.tmdb_id = tmdb_id;
                s.tmdb_poster = tmdb_poster.clone();
                s.tmdb_overview = tmdb_overview.clone();
                s.season = crate::tmdb::season_from_label(&disc_name);
                s.disc_number = crate::tmdb::disc_from_label(&disc_name);
                s.outputs = vec![staging::Output {
                    filename: mkv_leaf,
                    ..Default::default()
                }];
            }) {
                // The mux finished and the MKV is in staging, but the
                // mover keys off this marker — without it the file sits
                // in staging forever with no signal. Surface it so the
                // operator can see the rip is staged-but-unqueued rather
                // than silently lost.
                abort_post_mux_preserving_staging(
                    device,
                    &format!(
                        "{marker_name} marker write failed ({e}); MKV is staged but the mover cannot pick it up"
                    ),
                    &format!("MKV staged but {marker_name} marker write failed: {e}"),
                );
                // The durable hand-off marker never landed. Do NOT proceed to
                // `.completed` / `clear_restart_count`: that would make the
                // staging dir look terminal-complete while the mover has no
                // signal to pick it up, and the resume detector would never
                // re-run — a data-integrity gap. Return early, leaving the dir
                // resumable so a later attempt re-writes the marker.
                return;
            }
            if !title_confident {
                crate::log::device_log(
                    device,
                    &format!(
                        "Held for review: uncertain title match for \"{}\" — confirm/correct in the UI",
                        display_name
                    ),
                );
            }
            // Advance to process-level clean completion. `write_completed_marker`
            // does NOT downgrade the `Done`/`Review` hand-off state (the resume
            // detector's "finished" check covers both); it only releases the
            // in-progress lock and clears the restart counter.
            staging::write_completed_marker(staging_disc_path);
            staging::clear_restart_count(staging_disc_path);
        } else if let Some(reason) = finalize_error.as_ref() {
            // 0.20.8 validation-audit fix #1: post-mux validation gate.
            // `MuxSink::close()` propagated a `output.finish()` error,
            // which means the MKV's Cues / segment-size header didn't
            // get written. The file on disk is unseekable / invalid;
            // shipping it to the user's library would surface as a
            // broken playback later. Quarantine the staging dir with
            // `.failed` so:
            //   1. The mover never writes a half-baked file into the
            //      output dir (no `.done`, so `mover.rs::check_and_move`
            //      skips this staging entry entirely).
            //   2. The resume-on-startup detector recognises the dir as
            //      terminal-failed instead of bumping `.restart_count`
            //      and trying to "resume" a broken rip.
            //   3. The UI surfaces the reason in `last_error` via the
            //      same path used by `resume_or_quarantine_staging`.
            let staging_disc_path = std::path::Path::new(&staging);
            staging::write_failed_marker(
                staging_disc_path,
                &format!("mux finalize failed: {reason}"),
            );
            staging::clear_restart_count(staging_disc_path);
        }
    }

    if !completed {
        // 0.20.8 validation-audit fix #1: a finalize error means the
        // MKV is broken. Log + surface `status="failed"` so the device
        // tile flips red with the underlying reason; otherwise fall
        // through to the pre-existing "stopped → idle" behaviour
        // (halt / write error / wedge).
        let (log_prefix, ui_status, ui_failure_reason) =
            incomplete_mux_status(finalize_error.as_deref(), read_error.as_deref());
        crate::log::device_log(
            device,
            &format!(
                "{}: {:.1} GB in {:.0}s ({:.0} MB/s), {} skipped (~{:.3}s lost)",
                log_prefix,
                bytes_done as f64 / BYTES_PER_GIB,
                elapsed,
                speed,
                final_errors,
                final_lost_secs,
            ),
        );
        update_state(
            device,
            RipState {
                device: device.to_string(),
                status: ui_status,
                disc_present: true,
                disc_name: display_name.clone(),
                disc_format: disc_format.clone(),
                errors: final_errors,
                lost_video_secs: final_lost_secs,
                last_sector: final_last_sector,
                current_batch: final_current_batch,
                preferred_batch: batch,
                tmdb_title: tmdb_title.clone(),
                tmdb_year,
                tmdb_poster: tmdb_poster.clone(),
                tmdb_overview: tmdb_overview.clone(),
                duration: duration.clone(),
                codecs: codecs.clone(),
                last_error: ui_failure_reason.clone().unwrap_or_default(),
                failure_reason: ui_failure_reason,
                ..Default::default()
            },
        );
        return;
    }

    // Operator-facing done figures fold in demux-time loss the same way the
    // single-pass path (above) and the resume path (resume.rs) do, so a fresh
    // multi-pass rip and a resume of the IDENTICAL ISO report the same loss
    // figures whenever demux loss is non-zero. (The mux always proceeds; this is
    // reporting only — demux-time loss never aborts the disc.)
    //
    // Single-pass (max_retries == 0): the 3812 overwrite block was skipped, so
    // `final_lost_secs == demux_lost_secs` and `final_errors == mux_outcome.errors`
    // already — report them as-is to avoid double-counting.
    //
    // Multi-pass (max_retries > 0): `final_lost_secs` was overwritten with the
    // sweep-mapfile loss and `final_errors` with the mapfile bad-sector count;
    // both are disjoint from the demux skip count (sweep = Unreadable sectors
    // baked into the ISO; demux = decrypt/codec skips at mux), so add the demux
    // loss / errors. (The resume path in resume.rs folds the two the same way,
    // so a fresh multi-pass rip and a resume of the identical ISO report
    // matching figures.)
    let (done_errors, done_lost_secs, done_demux_extra_ms) =
        if !uses_multipass(cfg_read.max_retries) {
            (final_errors, final_lost_secs, 0.0)
        } else {
            (
                final_errors.saturating_add(mux_outcome.errors),
                final_lost_secs + demux_lost_secs,
                demux_lost_secs * MILLIS_PER_SEC,
            )
        };

    crate::log::device_log(
        device,
        &format!(
            "Complete: {:.1} GB in {:.0}s ({:.0} MB/s), {} skipped (~{:.3}s lost)",
            bytes_done as f64 / BYTES_PER_GIB,
            elapsed,
            speed,
            done_errors,
            done_lost_secs,
        ),
    );

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "done".to_string(),
            disc_present: true,
            disc_name: display_name.clone(),
            disc_format: disc_format.clone(),
            progress_pct: 100,
            errors: done_errors,
            lost_video_secs: done_lost_secs,
            last_sector: final_last_sector,
            current_batch: final_current_batch,
            preferred_batch: batch,
            output_file: staging.clone(),
            tmdb_title: tmdb_title.clone(),
            tmdb_year,
            tmdb_poster: tmdb_poster.clone(),
            tmdb_overview: tmdb_overview.clone(),
            duration: duration.clone(),
            codecs: codecs.clone(),
            // Carry sweep damage so the done card reflects real damage
            // instead of showing a clean result for a damaged rip.
            //
            // Single-pass mode (max_retries == 0) has no mapfile, so
            // `sweep_damage_snapshot` is the all-zero Default (see the
            // comment where it's declared). Feeding its `total_lost_ms`
            // (0.0) into update_state's damage_severity_for() starves the
            // ms-branch of classify_damage: a rip that skipped a handful
            // of sectors but lost >1s of low-bitrate video would be rated
            // Cosmetic instead of Moderate. Derive total_lost_ms from the
            // real in-title loss (`final_lost_secs`) instead. Multipass
            // keeps the snapshot's whole-disc value, which is genuinely
            // computed from the mapfile's per-range durations.
            total_lost_ms: done_card_lost_ms(
                uses_multipass(cfg_read.max_retries),
                final_lost_secs,
                sweep_damage_snapshot.total_lost_ms,
                done_demux_extra_ms,
            ),
            // Single-pass mode has no mapfile, so `sweep_damage_snapshot` is
            // the all-zero Default and its `main_lost_ms` is always 0.0 —
            // leaving the done card showing "(0s in main movie)" even when the
            // demux skipped in-title sectors. `final_lost_secs` is already the
            // in-title loss for single-pass (the demux-skip estimate), so mirror
            // the `total_lost_ms` branch above. Multipass keeps the snapshot's
            // value, which is derived from the mapfile's in-title bad ranges.
            main_lost_ms: done_card_lost_ms(
                uses_multipass(cfg_read.max_retries),
                final_lost_secs,
                sweep_damage_snapshot.main_lost_ms,
                done_demux_extra_ms,
            ),
            bad_ranges: sweep_damage_snapshot.bad_ranges.clone(),
            num_bad_ranges: sweep_damage_snapshot.num_bad_ranges,
            bad_ranges_truncated: sweep_damage_snapshot.bad_ranges_truncated,
            largest_gap_ms: sweep_damage_snapshot.largest_gap_ms,
            ..Default::default()
        },
    );

    // Prune intermediate ISO + mapfile unless keep_iso is set. Shared with the
    // resume/`.ripped` completion path (resume::resume_remux) so the
    // keep_iso=false reclaim can't diverge between the two completion routes.
    prune_intermediate_iso(
        device,
        std::path::Path::new(&iso_path_str),
        std::path::Path::new(&mapfile_path_str),
        cfg_read.max_retries,
        retain_intermediate_iso(cfg_read.keep_iso, &cfg_read.output_format),
    );

    crate::log::device_log(device, "Mux complete");
    // Mux stage: the `.mkv` now exists. In this inline-fallback path the rip
    // (drive-free) webhook already fired before the mux began; this is the
    // separate mux_complete notification.
    crate::webhook::send_rich(
        &cfg_read,
        crate::webhook::WebhookEvent::Mux,
        &crate::webhook::RipEvent {
            event: "mux_complete",
            title: &display_name,
            year: tmdb_year,
            format: &disc_format,
            poster_url: &tmdb_poster,
            duration: &duration,
            codecs: &codecs,
            size_gb: bytes_done as f64 / BYTES_PER_GIB,
            speed_mbs: speed,
            elapsed_secs: elapsed,
            output_path: &staging,
            // Sweep loss + demux loss (same combined figures as the done card)
            // so the completion notification reports the real loss in the
            // delivered MKV, not the sweep-mapfile-only subset.
            errors: done_errors,
            lost_video_secs: done_lost_secs,
        },
    );

    // Eject LAST. `eject_drive` archives the device log partway through, so
    // every line this rip still had to write — the ISO prune above, "Rip
    // complete", and anything the webhook logs — has to be emitted first or
    // it lands in the NEXT rip's ring instead of this rip's archived log.
    // Routed through `should_auto_eject` like the other two completion
    // terminals: that predicate, not an inline flag test, is where the
    // "fires once, at read-complete, never from the mux worker" rule lives.
    if should_auto_eject(cfg_read.auto_eject, device) {
        eject_drive(device_path);
    }
}
