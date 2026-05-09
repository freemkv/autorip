//! Rip orchestrator — drive poll loop + scan/rip/eject entry points.
//!
//! 0.18 prep: this module was a single 4350-line `ripper.rs`. The
//! state types, thread/halt bookkeeping, and staging-dir helpers have
//! been lifted into sibling sub-modules (`state`, `session`,
//! `staging`). The high-level orchestration — `drive_poll_loop`,
//! `scan_disc`, `rip_disc`, `eject_drive` — stays here. Sweep + mux
//! sub-modules exist as placeholders for the 0.18 trait-migration
//! commit that will lift those loops out of `rip_disc`. See
//! `freemkv-private/memory/0_18_redesign.md`.
//!
//! 0.18 also splits libfreemkv's `pes::Stream` (combined read+write)
//! into `FrameSource` / `FrameSink`. autorip is *intentionally* kept
//! on the deprecated `Stream` API in this slice — the migration to
//! the typed traits happens after the trait-migration commit lifts
//! the mux loop into `mux.rs`. The file-scope allow below is the
//! marker for that intentional, time-bounded deprecation use.

#![allow(deprecated)]

mod mux;
mod session;
pub mod staging;
pub mod state;
mod sweep;

// Re-export every symbol the rest of the crate (and integration tests)
// addresses as `crate::ripper::*`. The split is mechanical — every
// import that worked before this commit must work after it.
// `pub use` re-exports the symbols every existing `crate::ripper::*`
// caller (web.rs, verify.rs, main.rs, the integration tests under
// `tests/`, plus the lib facade in `lib.rs`) used to address directly.
// `#[allow(unused_imports)]` is required because the binary build of
// autorip (`mod ripper;` in main.rs is private) doesn't itself reach
// for every re-export — but `pub mod ripper;` in lib.rs and the
// integration tests do, so the re-exports must stay.
#[allow(unused_imports)]
pub use session::{
    STOP_FLAGS, join_all_rip_threads, join_rip_thread, register_halt, register_rip_thread,
    request_stop, spawn_rip_thread, take_rip_thread,
};
pub use staging::wipe_staging;
#[allow(unused_imports)]
pub use state::{
    BadRange, RipState, STATE, STOP_COOLDOWNS, is_busy, set_stop_cooldown, update_state,
    update_state_with,
};

// Internal-use imports for the orchestrator code that lives in this
// file. Sub-module-private helpers (`pub(super)`) are reachable from
// here because we are the parent of `state` / `session` / `staging`.
use libfreemkv::event::BatchSizeReason;
use libfreemkv::pes::Stream as PesStream;

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::config::Config;

use session::{
    DriveSession, HALT_FLAGS, drop_session, rediscover_drive, reset_stop_flag, stop_requested,
    store_session, take_session,
};
use staging::staging_free_bytes;
use state::{PassContext, PassProgressState, is_in_cooldown, push_pass_state, set_pass_progress};

// ─── Poll loop ─────────────────────────────────────────────────────────────

const POLL_INTERVAL_SECS: u64 = 5;

/// Extract the trailing path component (`sg4` from `/dev/sg4`,
/// `disk2` from `/dev/disk2`, `CdRom0` from `\\.\CdRom0`) for use as a
/// device key in autorip's state map. autorip's UI / state machine
/// keys everything by this short name; the lib gives back full
/// platform paths in `DriveInfo`.
fn device_key(path: &str) -> String {
    path.rsplit(['/', '\\']).next().unwrap_or(path).to_string()
}

/// Poll drives for disc insertion. Only triggers on state change
/// (no disc → disc present), not on disc already being there.
///
/// **Architectural note (0.13.2):** autorip is dumb — it never touches
/// hardware paths, sysfs, SCSI, or USB. The lib's `list_drives()` does
/// the platform-specific enumeration (sg/disk/CdRom paths, peripheral-
/// type filtering, INQUIRY for vendor/model). The lib's
/// `drive_has_disc(path)` does the disc-presence probe with internal
/// wedge-recovery (SCSI reset → USB reset) hidden from the caller.
/// autorip just iterates the snapshot, tracks logical state
/// (idle/scanning/ripping/cooldown), and spawns rip threads.
pub fn drive_poll_loop(cfg: &Arc<RwLock<Config>>) {
    // v0.13.17: re-enumerate drives every RESCAN_INTERVAL_SECS so an unplug
    // + replug at the kernel level (drive moves from /dev/sg4 to /dev/sg5
    // after USB re-enumeration) is picked up without a container restart.
    // Pre-0.13.17 enumeration was one-shot at startup — the user had to
    // restart the autorip container after every replug.
    const RESCAN_INTERVAL_SECS: u64 = 30;
    // Startup sweep: anything under /staging is orphaned — there are no
    // live sessions yet. A prior autorip process killed mid-rip leaves its
    // in-progress ISO / mapfile / partial MKV here, which the old
    // resume=false path still couldn't guarantee away. Wipe unconditionally
    // so the next rip always starts clean.
    if let Ok(c) = cfg.read() {
        wipe_staging(&c.staging_dir);
    }

    let initial_drives = libfreemkv::list_drives();
    let mut drive_paths: Vec<String> = initial_drives.iter().map(|d| d.path.clone()).collect();
    for d in &initial_drives {
        tracing::info!(
            device = %device_key(&d.path),
            path = %d.path,
            vendor = %d.vendor,
            model = %d.model,
            firmware = %d.firmware,
            "drive enumerated"
        );
    }
    let mut last_rescan = std::time::Instant::now();

    let mut had_disc: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut warned_probe_fail: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut device_first_seen: std::collections::HashMap<String, std::time::Instant> =
        std::collections::HashMap::new();
    for d in &initial_drives {
        let key = device_key(&d.path);
        device_first_seen.insert(
            key,
            std::time::Instant::now() - std::time::Duration::from_secs(60),
        );
    }

    tracing::info!(
        interval_secs = POLL_INTERVAL_SECS,
        drive_count = drive_paths.len(),
        "drive poll loop starting"
    );

    while !crate::SHUTDOWN.load(Ordering::Relaxed) {
        // v0.13.17 hot-plug: every RESCAN_INTERVAL_SECS, re-enumerate drives
        // and reconcile against the cached path list. New devices get logged
        // and start being polled. Devices that disappeared get their state
        // cleared (drop_session + remove from STATE) so the UI doesn't show
        // a phantom drive.
        if last_rescan.elapsed().as_secs() >= RESCAN_INTERVAL_SECS {
            last_rescan = std::time::Instant::now();
            let fresh = libfreemkv::list_drives();
            let fresh_paths: Vec<String> = fresh.iter().map(|d| d.path.clone()).collect();
            // Added: in fresh but not in drive_paths.
            for d in &fresh {
                if !drive_paths.contains(&d.path) {
                    let key = device_key(&d.path);
                    device_first_seen
                        .entry(key)
                        .or_insert(std::time::Instant::now());
                    tracing::info!(
                        device = %device_key(&d.path),
                        path = %d.path,
                        vendor = %d.vendor,
                        model = %d.model,
                        firmware = %d.firmware,
                        "drive enumerated (hot-plug)"
                    );
                }
            }
            // Removed: in drive_paths but not in fresh_paths.
            for path in &drive_paths {
                if !fresh_paths.contains(path) {
                    let device = device_key(path);
                    tracing::info!(device = %device, path = %path, "drive removed (hot-unplug)");
                    drop_session(&device);
                    if let Ok(mut s) = STATE.lock() {
                        s.remove(&device);
                    }
                    had_disc.remove(&device);
                    warned_probe_fail.remove(&device);
                }
            }
            drive_paths = fresh_paths;
        }

        {
            let mut current_with_disc: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            for path in &drive_paths {
                let device = device_key(path);

                // Don't touch drives that are actively scanning/ripping —
                // they hold a Drive instance + sometimes the SCSI bus.
                // Probing them mid-rip would conflict.
                if is_busy(&device) {
                    current_with_disc.insert(device.clone());
                    continue;
                }

                if device_first_seen
                    .get(&device)
                    .is_some_and(|t| t.elapsed() < std::time::Duration::from_secs(60))
                {
                    continue;
                }

                // The whole hardware probe — discovery, wedge detection,
                // SCSI reset, USB reset — is one lib call. autorip sees
                // a `bool` for present/absent, or an `Err` only after
                // recovery itself failed (drive permanently bricked).
                let disc_present = match libfreemkv::drive_has_disc(std::path::Path::new(path)) {
                    Ok(p) => {
                        warned_probe_fail.remove(&device);
                        p
                    }
                    Err(e) => {
                        if warned_probe_fail.insert(device.clone()) {
                            tracing::warn!(
                                device = %device,
                                path = %path,
                                error = %e,
                                "drive_has_disc failed — drive firmware unresponsive; physical reconnect or host reboot required"
                            );
                        } else {
                            tracing::debug!(
                                device = %device,
                                error = %e,
                                "drive_has_disc still failing"
                            );
                        }
                        continue;
                    }
                };

                if !disc_present {
                    // Disc removed — clean up session
                    if had_disc.contains(&device) {
                        tracing::info!(device = %device, "disc removed");
                        drop_session(&device);
                    }
                    if !is_busy(&device) {
                        update_state(
                            &device,
                            RipState {
                                device: device.clone(),
                                status: "idle".to_string(),
                                ..Default::default()
                            },
                        );
                    }
                    continue;
                }

                current_with_disc.insert(device.clone());

                let is_new_insert = !had_disc.contains(&device);

                if is_new_insert {
                    tracing::info!(device = %device, "disc inserted");
                }

                if is_new_insert && !is_busy(&device) && !is_in_cooldown(&device) {
                    let on_insert = cfg
                        .read()
                        .ok()
                        .map(|c| c.on_insert.clone())
                        .unwrap_or_else(|| "scan".to_string());

                    if on_insert == "nothing" {
                        update_state(
                            &device,
                            RipState {
                                device: device.clone(),
                                status: "idle".to_string(),
                                disc_present: true,
                                ..Default::default()
                            },
                        );
                        continue;
                    }

                    tracing::info!(
                        device = %device,
                        on_insert = %on_insert,
                        "spawning scan/rip thread"
                    );

                    update_state(
                        &device,
                        RipState {
                            device: device.clone(),
                            status: "scanning".to_string(),
                            disc_present: true,
                            ..Default::default()
                        },
                    );

                    let cfg = cfg.clone();
                    let dev_path = path.clone();
                    let device_for_thread = device.clone();

                    if let Err(e) = spawn_rip_thread(&device, "rip", move || {
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            scan_disc(&cfg, &device_for_thread, &dev_path);
                            if on_insert == "rip" && !stop_requested(&device_for_thread) {
                                rip_disc(&cfg, &device_for_thread, &dev_path);
                            }
                        }))
                        .is_err()
                        {
                            tracing::error!(
                                device = %device_for_thread,
                                "scan/rip thread panicked"
                            );
                            crate::log::device_log(&device_for_thread, "Thread panicked");
                            drop_session(&device_for_thread);
                            update_state(
                                &device_for_thread,
                                RipState {
                                    device: device_for_thread.clone(),
                                    status: "error".to_string(),
                                    last_error: "Internal error (panic)".to_string(),
                                    ..Default::default()
                                },
                            );
                        }
                    }) {
                        tracing::warn!(
                            device = %device,
                            error = %e,
                            "failed to spawn rip thread"
                        );
                    }
                } else if !is_new_insert && !is_busy(&device) {
                    if let Ok(mut s) = STATE.lock() {
                        if let Some(rs) = s.get_mut(&device) {
                            rs.disc_present = true;
                        }
                    }
                }
            }

            had_disc = current_with_disc;
        }

        // SHUTDOWN-responsive sleep — break early on signal so SIGTERM
        // doesn't have to wait the full 5 s tick to take effect.
        for _ in 0..(POLL_INTERVAL_SECS * 10) {
            if crate::SHUTDOWN.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    tracing::info!("drive poll loop stopping");
}

// ─── Scan ──────────────────────────────────────────────────────────────────

/// Scan a disc — open, init, identify, TMDB, full scan. Stores session for rip.
pub fn scan_disc(cfg: &Arc<RwLock<Config>>, device: &str, device_path: &str) {
    let cfg_read = match cfg.read() {
        Ok(c) => c,
        Err(_) => return,
    };

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "scanning".to_string(),
            disc_present: true,
            ..Default::default()
        },
    );

    crate::log::archive_device_log(device);
    crate::log::device_log(device, "Opening drive...");

    let mut drive = match libfreemkv::Drive::open(std::path::Path::new(device_path)) {
        Ok(d) => d,
        Err(e) => {
            crate::log::device_log(device, &format!("Cannot open drive: {}", e));
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "error".to_string(),
                    last_error: format!("{}", e),
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
        tracing::warn!(device = %device, error = %e, "drive init failed (continuing — scan may degrade)");
    }

    // Fast identify — disc name only, no playlists
    crate::log::device_log(device, "Identifying disc...");
    let disc_id = match libfreemkv::Disc::identify(&mut drive) {
        Ok(id) => id,
        Err(e) => {
            crate::log::device_log(device, &format!("Identify failed: {}", e));
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "error".to_string(),
                    last_error: format!("{}", e),
                    ..Default::default()
                },
            );
            return;
        }
    };

    let id_name = disc_id.name().to_string();

    crate::log::device_log(device, &format!("Disc: {}", id_name));

    // TMDB lookup — fast, user sees poster while full scan runs
    let tmdb = crate::tmdb::lookup(&crate::tmdb::clean_title(&id_name), &cfg_read.tmdb_api_key);
    let display_name = tmdb
        .as_ref()
        .map(|t| t.title.clone())
        .unwrap_or_else(|| id_name.clone());

    // Show identify results immediately — no format badge until full scan confirms UHD vs BD
    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "scanning".to_string(),
            disc_present: true,
            disc_name: display_name.clone(),
            disc_format: String::new(),
            tmdb_title: tmdb.as_ref().map(|t| t.title.clone()).unwrap_or_default(),
            tmdb_year: tmdb.as_ref().map(|t| t.year).unwrap_or(0),
            tmdb_poster: tmdb
                .as_ref()
                .map(|t| t.poster_url.clone())
                .unwrap_or_default(),
            tmdb_overview: tmdb
                .as_ref()
                .map(|t| t.overview.clone())
                .unwrap_or_default(),
            ..Default::default()
        },
    );

    // Full scan — titles, streams, AACS keys
    crate::log::device_log(device, "Scanning titles...");
    let scan_opts = match &cfg_read.keydb_path {
        Some(p) => libfreemkv::ScanOptions {
            keydb_path: Some(p.into()),
        },
        None => libfreemkv::ScanOptions::default(),
    };
    let disc = match libfreemkv::Disc::scan(&mut drive, &scan_opts) {
        Ok(d) => d,
        Err(e) => {
            crate::log::device_log(device, &format!("Scan failed: {}", e));
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "error".to_string(),
                    last_error: format!("{}", e),
                    ..Default::default()
                },
            );
            return;
        }
    };

    // Update format from full scan (UHD vs BD now known)
    let disc_name = disc
        .meta_title
        .as_deref()
        .unwrap_or(&disc.volume_id)
        .to_string();
    let disc_format = match disc.format {
        libfreemkv::DiscFormat::Uhd => "uhd",
        libfreemkv::DiscFormat::BluRay => "bluray",
        libfreemkv::DiscFormat::Dvd => "dvd",
        libfreemkv::DiscFormat::Unknown => "unknown",
    }
    .to_string();

    crate::log::device_log(
        device,
        &format!(
            "Scanned: {} ({}, {} titles)",
            disc_name,
            disc_format,
            disc.titles.len()
        ),
    );

    // Extract title info before storing session
    let duration = disc
        .titles
        .first()
        .map(|t| crate::util::format_duration_hm(t.duration_secs))
        .unwrap_or_default();
    let codecs = disc.titles.first().map(format_codecs).unwrap_or_default();

    // Store session — drive stays open for rip
    store_session(
        device,
        DriveSession {
            drive,
            disc: Some(disc),
            scanned: true,
            probed: false,
            tmdb: tmdb.clone(),
            device_path: device_path.to_string(),
        },
    );

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "idle".to_string(),
            disc_present: true,
            disc_name: display_name,
            disc_format,
            tmdb_title: tmdb.as_ref().map(|t| t.title.clone()).unwrap_or_default(),
            tmdb_year: tmdb.as_ref().map(|t| t.year).unwrap_or(0),
            tmdb_poster: tmdb
                .as_ref()
                .map(|t| t.poster_url.clone())
                .unwrap_or_default(),
            tmdb_overview: tmdb
                .as_ref()
                .map(|t| t.overview.clone())
                .unwrap_or_default(),
            duration,
            codecs,
            ..Default::default()
        },
    );
}

// ─── Rip ───────────────────────────────────────────────────────────────────

/// Rip a disc. Reuses the existing drive session from scan_disc.
/// If no session exists, opens fresh (for on_insert=rip).
pub fn rip_disc(cfg: &Arc<RwLock<Config>>, device: &str, device_path: &str) {
    reset_stop_flag(device);

    // Archive the previous rip's per-device log so the live log only
    // shows events from the current attempt. Mirrors what scan_disc
    // does; previously rip_disc was missing this so a stop -> rip
    // cycle left "Stop requested..." / "Pass 1 cancelled" lines from
    // the prior run mixed into the new one.
    crate::log::archive_device_log(device);

    let cfg_read = match cfg.read() {
        Ok(c) => c,
        Err(_) => return,
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
                    let msg = format!("Cannot open drive: {}", e);
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

            let scan_opts = match &cfg_read.keydb_path {
                Some(p) => libfreemkv::ScanOptions {
                    keydb_path: Some(p.into()),
                },
                None => libfreemkv::ScanOptions::default(),
            };
            crate::log::device_log(device, "Scanning titles...");
            let disc = match libfreemkv::Disc::scan(&mut drive, &scan_opts) {
                Ok(d) => d,
                Err(e) => {
                    let msg = format!("Scan failed: {}", e);
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

            let disc_name = disc
                .meta_title
                .as_deref()
                .unwrap_or(&disc.volume_id)
                .to_string();

            let tmdb = crate::tmdb::lookup(
                &crate::tmdb::clean_title(&disc_name),
                &cfg_read.tmdb_api_key,
            );

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

    let disc = match session.disc.take() {
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
        libfreemkv::DiscFormat::BluRay => "bluray",
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

    let tmdb = &session.tmdb;
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
    let tmdb_media_type = tmdb
        .as_ref()
        .map(|t| t.media_type.clone())
        .unwrap_or_else(|| "unknown".to_string());

    let display_name = if tmdb_title.is_empty() {
        disc_name.clone()
    } else {
        tmdb_title.clone()
    };

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
    let keys = disc.decrypt_keys();

    if disc.encrypted && matches!(keys, libfreemkv::decrypt::DecryptKeys::None) {
        let msg = "Disc is encrypted but no decryption keys found (check KEYDB)";
        crate::log::device_log(device, msg);
        update_state(
            device,
            RipState {
                device: device.to_string(),
                status: "error".to_string(),
                last_error: msg.to_string(),
                disc_name: display_name,
                disc_format,
                tmdb_title,
                tmdb_year,
                tmdb_poster,
                tmdb_overview,
                ..Default::default()
            },
        );
        return;
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
    //     Disc::copy and is unaffected by this value)
    //   - made the mux phase read the ISO **one sector at a time**
    //     (2 KB chunks) via DiscStream::new(reader, title, keys, batch,
    //     format) — a real perf bug on the mux read path
    let batch = libfreemkv::disc::detect_max_batch_sectors(device_path);
    let format = disc.content_format;

    let output_format = cfg_read.output_format.clone();
    let ext = match output_format.as_str() {
        "m2ts" => "m2ts",
        _ => "mkv",
    };

    let staging = cfg_read.staging_device_dir(&crate::util::sanitize_path_compact(&display_name));
    let _ = std::fs::create_dir_all(&staging);
    let filename = format!(
        "{}.{}",
        crate::util::sanitize_path_compact(&display_name),
        ext
    );
    let output_path = format!("{}/{}", staging, filename);
    let dest_url = if output_format == "network" && !cfg_read.network_target.is_empty() {
        format!("network://{}", cfg_read.network_target)
    } else {
        format!("{}://{}", ext, output_path)
    };

    crate::log::device_log(device, &format!("Ripping {} to {}", display_name, filename));

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "ripping".to_string(),
            disc_present: true,
            disc_name: display_name.clone(),
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
            8_250_000.0
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

    // Create PES stream — same drive session, no re-open
    let halt = session.drive.halt_flag();
    register_halt(device, halt.clone());

    // Rip-level wallclock budget (Fix 3). Caps the ENTIRE rip — all passes
    // combined — at max(disc_runtime, 1h). Implemented as a background thread
    // that sleeps for the budget then fires halt if the rip hasn't finished.
    // Per-pass caps (spawn_pass_watcher) still apply as a finer-grained
    // safety net; this is the backstop so a rip can't run forever even if
    // individual passes keep resetting their timers. Configurable via
    // MAX_RIP_DURATION_SECS env var or settings.json.
    let cfg = cfg.read().unwrap();
    let rip_budget_secs = cfg.max_rip_duration_secs;
    let halt_rip_watcher = halt.clone();
    let device_rip_watcher = device.to_string();
    let _rip_watcher_guard = std::thread::spawn(move || {
        tracing::info!(
            device = %device_rip_watcher,
            rip_budget_secs,
            "Rip-level wallclock watcher started"
        );
        std::thread::sleep(std::time::Duration::from_secs(rip_budget_secs));
        if !halt_rip_watcher.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::warn!(
                device = %device_rip_watcher,
                rip_budget_secs,
                "Rip budget exceeded — firing halt flag"
            );
            halt_rip_watcher.store(true, std::sync::atomic::Ordering::Relaxed);
            crate::log::device_log(
                &device_rip_watcher,
                &format!("exceeded {}-second rip budget", rip_budget_secs),
            );
        }
    });

    // Per-pass wallclock budget. Each pass (Pass 1 sweep + every retry) gets
    // its own `max(disc_runtime, min_budget)` budget. Configurable via
    // MIN_PASS_BUDGET_SECS env var or settings.json. If ANY pass exceeds
    // its budget the rip ends with status=error (see cap_fired_any tracking below).
    let chosen_runtime_secs: u64 = title.duration_secs.max(0.0) as u64;
    let max_pass_secs = chosen_runtime_secs.max(cfg.min_pass_budget_secs);
    struct WallclockGuard(Arc<AtomicBool>);
    impl Drop for WallclockGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Relaxed);
        }
    }
    // Fires per-pass watcher. Returns a guard that, on drop, stops the
    // watcher thread. While alive: forwards user_halt → pass_halt; fires
    // cap_fired (and pass_halt) when wall-clock exceeds max_secs; writes
    // a per-pass `last_error` for UI surfacing.
    fn spawn_pass_watcher(
        pass_label: String,
        device: String,
        pass_halt: Arc<AtomicBool>,
        user_halt: Arc<AtomicBool>,
        cap_fired: Arc<AtomicBool>,
        max_secs: u64,
    ) -> WallclockGuard {
        let active = Arc::new(AtomicBool::new(true));
        let active_for_watcher = active.clone();
        let pass_start = std::time::Instant::now();
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
                if pass_start.elapsed().as_secs() > max_secs {
                    let hrs = max_secs / 3600;
                    let mins = (max_secs % 3600) / 60;
                    let budget_str = if mins > 0 {
                        format!("{}h {:02}m", hrs, mins)
                    } else {
                        format!("{}h", hrs)
                    };
                    crate::log::device_log(
                        &device,
                        &format!(
                            "{} exceeded {} budget; halting pass",
                            pass_label, budget_str
                        ),
                    );
                    cap_fired.store(true, Ordering::Relaxed);
                    pass_halt.store(true, Ordering::Relaxed);
                    update_state_with(&device, |s| {
                        s.last_error = format!("{} exceeded {} budget", pass_label, budget_str);
                    });
                    return;
                }
            }
        });
        WallclockGuard(active)
    }
    // True if ANY pass cap-fired during this rip. v0.13.15: when true, mux
    // is skipped and status=error; ISO is retained in staging for manual
    // salvage. False = all passes completed naturally → mux normally.
    let cap_fired_any = Arc::new(AtomicBool::new(false));
    // The user-stop halt — the existing flag. Pass-specific halts forward
    // from this via spawn_pass_watcher. Renamed locally for clarity.
    let user_halt = halt.clone();

    let dev_for_events = device.to_string();
    let wdf_drive = wd_last_frame.clone();
    let lbr_drive = latest_bytes_read.clone();
    session.drive.on_event(move |event| {
        // Any drive-level event means something is happening — reset the
        // watchdog so the "stalled" timer doesn't monotonically climb
        // while the library is working through recovery.
        wdf_drive.store(crate::util::epoch_secs(), Ordering::Relaxed);
        match event.kind {
            libfreemkv::event::EventKind::BytesRead { bytes, .. } => {
                lbr_drive.store(bytes, Ordering::Relaxed);
            }
            libfreemkv::event::EventKind::ReadError { sector, .. } => {
                crate::log::device_log(
                    &dev_for_events,
                    &format!("Read error at sector {}", sector),
                );
            }
            _ => {}
        }
    });
    // Multi-pass vs direct flow.
    //
    // When max_retries > 0, we go through an ISO intermediate: Disc::copy writes
    // the disc to an ISO (fast skip-forward on failure, ddrescue-style mapfile),
    // then Disc::patch retries the bad ranges up to max_retries times, then the
    // mux pipeline reads from the ISO (no drive involvement past this point).
    //
    // When max_retries == 0, we keep the existing direct disc→MKV flow —
    // session.drive is passed to DiscStream::new and sectors stream straight
    // through decrypt/demux/mux. Fastest path, no ISO overhead, but no retry.
    // Lifted out of the multipass branch below so the mux progress loop
    // (which lives in the outer scope) can reference it. Single-pass mode
    // (max_retries == 0) has no multipass concept; the mux loop's checks
    // gate on `total_passes > 0` before threading it into UI state.
    let total_passes: u8 = if cfg_read.max_retries > 0 {
        cfg_read.max_retries + 2 // pass 1 + retries + mux
    } else {
        0
    };
    let reader: Box<dyn libfreemkv::SectorReader> = if cfg_read.max_retries > 0 {
        let iso_filename = format!("{}.iso", crate::util::sanitize_path_compact(&display_name));
        let iso_path_str = format!("{}/{}", staging, iso_filename);
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
        // ENOSPC failure isn't perfect — see audit finding #13).
        // Escape hatch: AUTORIP_SKIP_DISKCHECK=1 bypasses the pre-flight
        // check. Used to deliberately rip onto a smaller volume than 2×
        // disc capacity for diagnostics (speed isolation, partial ISO
        // tests). The rip will run and predictably ENOSPC mid-stream;
        // the operator accepts that. Don't use in production.
        if bytes_total_disc > 0 && std::env::var("AUTORIP_SKIP_DISKCHECK").is_err() {
            let required = bytes_total_disc.saturating_mul(2);
            if let Some(avail) = staging_free_bytes(&staging) {
                if avail < required {
                    let msg = format!(
                        "E5000: insufficient staging disk space — need ≥ {:.1} GB free at {} (2× disc capacity), have {:.1} GB. Free up space or point STAGING_DIR at a larger volume.",
                        required as f64 / 1_073_741_824.0,
                        &staging,
                        avail as f64 / 1_073_741_824.0,
                    );
                    crate::log::device_log(device, &msg);
                    update_state_with(device, |s| {
                        s.status = "error".to_string();
                        s.last_error = msg.clone();
                    });
                    if let Ok(mut flags) = HALT_FLAGS.lock() {
                        flags.remove(device);
                    }
                    drop_session(device);
                    return;
                }
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
            duration: duration.clone(),
            codecs: codecs.clone(),
            filename: filename.clone(),
            batch,
            bytes_total_disc,
            max_retries: cfg_read.max_retries,
        };
        let title_for_progress = title.clone();
        let mapfile_path_str = format!("{iso_path_str}.mapfile");
        let bps_progress = title_bytes_per_sec;

        // Pass 1: disc → ISO (fast sweep, skip-forward on failure).
        let pass_label = format!("Pass 1/{total_passes}: disc → ISO");
        crate::log::device_log(device, &pass_label);
        set_pass_progress(
            device,
            &display_name,
            &disc_format,
            &tmdb_title,
            tmdb_year,
            &tmdb_poster,
            &tmdb_overview,
            &duration,
            &codecs,
            &filename,
            1,
            total_passes,
            0, // bytes_good
            0, // bytes_maybe
            0, // bytes_lost
            bytes_total_disc,
            batch,
        );

        // Progress callback — runs every read block (~64 KB). Throttle the
        // mapfile re-read + state push to once every 1.5 s so we don't pound
        // the mutex or the filesystem. State tracker holds last-sample
        // timestamp + bytes for speed/ETA calc.
        let pass1_state = std::cell::RefCell::new(PassProgressState::new());
        let pass1_ctx = &pass_ctx;
        let pass1_title = &title_for_progress;
        let pass1_map = std::path::Path::new(&mapfile_path_str);
        let pass1_progress = |p: &libfreemkv::progress::PassProgress| -> bool {
            // Stash work_done for push_pass_state to compute pass progress.
            pass1_state.borrow_mut().last_work_done = p.work_done;
            pass1_state.borrow_mut().last_work_total = p.work_total;
            // Throttle: only re-read mapfile + push state every 1.5s.
            if pass1_state.borrow().last_update.elapsed().as_millis() < 1500 {
                return true;
            }
            push_pass_state(
                pass1_ctx,
                pass1_title,
                bps_progress,
                pass1_map,
                1,
                total_passes,
                &pass1_state,
            );
            true
        };

        // Pass 1: disc → ISO with transport-failure recovery.
        //
        // The Initio USB-SATA bridge crashes when reading damaged sectors,
        // causing a USB re-enumeration (sg device changes number). The copy
        // aborts, but the mapfile captures all progress. We retry with
        // resume=true after re-opening the drive on its new device path.
        let pass1_halt = Arc::new(AtomicBool::new(false));
        let _pass1_guard = spawn_pass_watcher(
            "Pass 1".to_string(),
            device.to_string(),
            pass1_halt.clone(),
            user_halt.clone(),
            cap_fired_any.clone(),
            max_pass_secs,
        );

        const MAX_PASS1_ATTEMPTS: u32 = 10;
        let mut attempt = 0;
        let mut result = None;

        'pass1: loop {
            attempt += 1;
            if attempt > MAX_PASS1_ATTEMPTS {
                crate::log::device_log(device, "Pass 1: max attempts reached");
                break;
            }

            let copy_opts = libfreemkv::disc::CopyOptions {
                decrypt: false,
                multipass: true,
                halt: Some(pass1_halt.clone()),
                progress: Some(&pass1_progress),
            };

            match disc.copy(&mut session.drive, iso_path, &copy_opts) {
                Ok(r) => {
                    result = Some(r);
                    break 'pass1;
                }
                Err(e) => {
                    if halt.load(Ordering::Relaxed) {
                        crate::log::device_log(device, &format!("Pass 1 cancelled (halt): {e}"));
                        if let Ok(mut flags) = HALT_FLAGS.lock() {
                            flags.remove(device);
                        }
                        return;
                    }

                    let is_transport = e.is_scsi_transport_failure();

                    if !is_transport {
                        crate::log::device_log(device, &format!("Pass 1 failed: {e}"));
                        update_state(
                            device,
                            RipState {
                                device: device.to_string(),
                                status: "error".to_string(),
                                disc_present: true,
                                last_error: format!("{e}"),
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
                        if let Ok(mut flags) = HALT_FLAGS.lock() {
                            flags.remove(device);
                        }
                        return;
                    }

                    // Transport failure — bridge crashed. Drop stale drive,
                    // wait for USB re-enumeration, re-open on new path.
                    crate::log::device_log(
                        device,
                        &format!(
                            "Pass 1 attempt {attempt}: transport failure (bridge crash), waiting for USB re-enumeration"
                        ),
                    );
                    drop_session(device);

                    // Wait for USB re-enumeration with configurable delay.
                    // `cfg` already holds the read guard from the outer scope.
                    std::thread::sleep(std::time::Duration::from_secs(
                        cfg.transport_recovery_delay_secs,
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
                            let mut drive = None;
                            for retry in 0..3 {
                                match libfreemkv::Drive::open(std::path::Path::new(&p)) {
                                    Ok(d) => {
                                        drive = Some(d);
                                        break;
                                    }
                                    Err(e) if retry < 2 => {
                                        let backoff_secs =
                                            cfg.transport_recovery_delay_secs * (1u64 << retry);
                                        crate::log::device_log(
                                            device,
                                            &format!(
                                                "Pass 1 attempt {attempt}: Drive::open({}) failed, retrying in {}s: error={} sense_key={:?} ASC={:?}",
                                                p,
                                                backoff_secs,
                                                e.code(),
                                                e.scsi_sense().map(|s| s.sense_key),
                                                e.scsi_sense().map(|s| s.asc)
                                            ),
                                        );
                                        std::thread::sleep(std::time::Duration::from_secs(
                                            backoff_secs,
                                        ));
                                    }
                                    Err(e) => {
                                        crate::log::device_log(
                                            device,
                                            &format!(
                                                "Pass 1 attempt {attempt}: Drive::open({}) failed strategy=transport_failure_recovery error={} sense_key={:?} ASC={:?} — recovery path exhausted",
                                                p,
                                                e.code(),
                                                e.scsi_sense().map(|s| s.sense_key),
                                                e.scsi_sense().map(|s| s.asc)
                                            ),
                                        );

                                        let failure_category = if e.code() == 4000 {
                                            "SCSI_ERROR"
                                        } else if e.code() >= 1000 && e.code() < 2000 {
                                            "DEVICE_ERROR"
                                        } else {
                                            &format!("ERROR_CODE_{}", e.code())
                                        };

                                        crate::log::device_log(
                                            device,
                                            &format!(
                                                "STRATEGY_FAILURE: transport_failure_recovery FAILED at Drive::open category={} error_code={}",
                                                failure_category,
                                                e.code()
                                            ),
                                        );

                                        break 'pass1;
                                    }
                                }
                            }

                            let mut drive = match drive {
                                Some(d) => d,
                                None => continue 'pass1,
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

                                // Special handling for ILLEGAL REQUEST (0x20/0x00) which indicates wedged firmware
                                let is_wedged_firmware = e.code() == 4000
                                    && e.scsi_sense().map(|s| s.asc == 0x20).unwrap_or(false);

                                if is_wedged_firmware {
                                    crate::log::device_log(
                                        device,
                                        "STRATEGY_FAILURE: transport_failure_recovery FAILED at Drive::init with ILLEGAL_REQUEST (ASC=0x20) — drive firmware wedged",
                                    );

                                    // Log user action required
                                    crate::log::device_log(
                                        device,
                                        "USER_ACTION_REQUIRED: Eject disc and physically power-cycle USB optical drive to clear firmware state before retrying",
                                    );
                                } else {
                                    let failure_category = if e.code() == 4000 {
                                        "SCSI_ERROR"
                                    } else {
                                        &format!("ERROR_CODE_{}", e.code())
                                    };

                                    crate::log::device_log(
                                        device,
                                        &format!(
                                            "STRATEGY_FAILURE: transport_failure_recovery FAILED at Drive::init category={} error_code={}",
                                            failure_category,
                                            e.code()
                                        ),
                                    );
                                }

                                break 'pass1;
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

                            let mut drive = match libfreemkv::Drive::open(std::path::Path::new(&p))
                            {
                                Ok(d) => d,
                                Err(e) => {
                                    crate::log::device_log(
                                        device,
                                        &format!(
                                            "Pass 1 attempt {attempt}: Drive::open({}) failed strategy=transport_failure_recovery error={} — recovery path exhausted",
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
                                            "STRATEGY_FAILURE: transport_failure_recovery FAILED at Drive::open category={} error_code={}",
                                            failure_category,
                                            e.code()
                                        ),
                                    );

                                    break 'pass1;
                                }
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
                                        "Pass 1 attempt {attempt}: Drive::init({}) failed strategy=transport_failure_recovery error={} — recovery path exhausted",
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
                                        "STRATEGY_FAILURE: transport_failure_recovery FAILED at Drive::init category={} error_code={}",
                                        failure_category,
                                        e.code()
                                    ),
                                );

                                break 'pass1;
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
                        attempt + 1,
                        MAX_PASS1_ATTEMPTS,
                        failure_reason
                    ),
                );

                update_state(
                    device,
                    RipState {
                        device: device.to_string(),
                        status: "error".to_string(),
                        disc_present: true,
                        last_error: format!(
                            "Pass 1 failed: {} — see logs for detailed error breakdown",
                            failure_reason
                        ),
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
                        "NEXT_STEPS: 1) Check /api/logs/sg4 for STRATEGY_FAILURE entries. 2) Identify which phase failed (Drive::open/wait_ready/init). 3) If firmware wedged, power-cycle drive. 4) Reprogram autorip and retry rip.",
                    );
                } else {
                    crate::log::device_log(
                        device,
                        "RECOVERY_GUIDANCE: Unrecoverable error occurred before transport failure recovery could complete. Check logs for first ERROR entry to identify root cause.",
                    );
                }

                if let Ok(mut flags) = HALT_FLAGS.lock() {
                    flags.remove(device);
                }
                return;
            }
        };
        // Drop the Pass 1 watcher so its thread exits before Pass 2 spawns its own.
        drop(_pass1_guard);
        crate::log::device_log(
            device,
            &format!(
                "Pass 1 done: {:.2} GB good, {:.2} MB unreadable, {:.2} MB pending",
                result.bytes_good as f64 / 1_073_741_824.0,
                result.bytes_unreadable as f64 / 1_048_576.0,
                result.bytes_pending as f64 / 1_048_576.0,
            ),
        );

        // Track cross-pass state from CopyResult.
        let mut bytes_good = result.bytes_good;
        let mut bytes_unreadable = result.bytes_unreadable;
        let mut bytes_pending = result.bytes_pending;

        // Retry passes: Disc::copy with multipass=true re-reads only bad
        // ranges (patch) sector-by-sector with full drive-level recovery.
        // Each pass gets its own wallclock cap watcher; cap-fire
        // marks the rip as failed.

        let max_retries = cfg_read.max_retries;

        crate::log::device_log(
            device,
            &format!(
                "PASS 2-{}: retry loop starting max_retries={} bytes_pending={} bytes_unreadable={}",
                max_retries, max_retries, bytes_pending, bytes_unreadable
            ),
        );
        let mut pass_2_settled = false;
        for retry_n in 1..=max_retries {
            // If user hit stop OR a prior pass cap-fired, bail.
            if user_halt.load(Ordering::Relaxed) || cap_fired_any.load(Ordering::Relaxed) {
                crate::log::device_log(
                    device,
                    &format!(
                        "PASS {} STOPPED: user halt={}/cap_fired={} before retry pass",
                        retry_n + 1,
                        user_halt.load(Ordering::Relaxed),
                        cap_fired_any.load(Ordering::Relaxed)
                    ),
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
            let mux_scope_bad = match libfreemkv::disc::mapfile::Mapfile::load(
                std::path::Path::new(&mapfile_path_str),
            ) {
                Ok(map) => {
                    use libfreemkv::disc::mapfile::SectorStatus;
                    let bad = map.ranges_with(&[
                        SectorStatus::NonTried,
                        SectorStatus::NonTrimmed,
                        SectorStatus::NonScraped,
                        SectorStatus::Unreadable,
                    ]);
                    if cfg_read.output_format == "iso" {
                        bad.iter().map(|(_, sz)| *sz).sum::<u64>()
                    } else {
                        libfreemkv::disc::bytes_bad_in_title(&title_for_progress, &bad)
                    }
                }
                Err(_) => {
                    // Conservative fallback if we can't read the mapfile —
                    // fall back to the whole-disc check so we don't skip
                    // a needed pass on a transient read error.
                    bytes_pending + bytes_unreadable
                }
            };
            if mux_scope_bad == 0 {
                let scope_label = if cfg_read.output_format == "iso" {
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

            // Settle the drive between Pass 1 and Pass 2 only. Per
            // RIP_DESIGN.md §15 Fix F: the BU40N (and other Initio-bridge
            // drives) wedge after grinding on bad sectors. Giving the drive
            // 30 s of idle BEFORE we hammer it again with retry reads lets
            // its internal state recover. Cheap insurance.
            if !pass_2_settled {
                crate::log::device_log(device, "Settling drive for 30 s before retry pass");
                std::thread::sleep(std::time::Duration::from_secs(30));
                pass_2_settled = true;
                if user_halt.load(Ordering::Relaxed) {
                    crate::log::device_log(device, "PASS 2 STOPPED: user halt during drive settle");
                    break;
                }
            }

            crate::log::device_log(
                device,
                &format!(
                    "PASS {}/{total_passes}: retrying bad ranges (bpt=1) bytes_pending={} bytes_unreadable={}",
                    pass, bytes_pending, bytes_unreadable
                ),
            );

            set_pass_progress(
                device,
                &display_name,
                &disc_format,
                &tmdb_title,
                tmdb_year,
                &tmdb_poster,
                &tmdb_overview,
                &duration,
                &codecs,
                &filename,
                pass,
                total_passes,
                bytes_good,
                bytes_pending,    // MAYBE bucket — Pass 2-N may still recover
                bytes_unreadable, // LOST bucket — terminal
                bytes_total_disc,
                batch,
            );

            // Per-pass progress + watcher.
            let patch_state = std::cell::RefCell::new(PassProgressState::new());
            let patch_ctx = &pass_ctx;
            let patch_title = &title_for_progress;
            let patch_map = std::path::Path::new(&mapfile_path_str);
            let patch_progress = |p: &libfreemkv::progress::PassProgress| -> bool {
                patch_state.borrow_mut().last_work_done = p.work_done;
                patch_state.borrow_mut().last_work_total = p.work_total;
                if patch_state.borrow().last_update.elapsed().as_millis() < 1500 {
                    return true;
                }
                push_pass_state(
                    patch_ctx,
                    patch_title,
                    bps_progress,
                    patch_map,
                    pass,
                    total_passes,
                    &patch_state,
                );
                true
            };
            let pass_halt = Arc::new(AtomicBool::new(false));
            let _pass_guard = spawn_pass_watcher(
                format!("Pass {pass}"),
                device.to_string(),
                pass_halt.clone(),
                user_halt.clone(),
                cap_fired_any.clone(),
                max_pass_secs,
            );

            let copy_opts = libfreemkv::disc::CopyOptions {
                decrypt: false,
                multipass: true,
                halt: Some(pass_halt.clone()),
                progress: Some(&patch_progress),
            };
            let cr = match disc.copy(&mut session.drive, iso_path, &copy_opts) {
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
                                "STRATEGY_FAILURE: patch_recovery FAILED at disc.copy() with category={} (sense_key={:?}, ASC={:?})",
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
            let recovered = cr.recovered_this_pass;
            let exit_str = if cr.halted { " (halt)" } else { "" };
            crate::log::device_log(
                device,
                &format!(
                    "Pass {pass} done: recovered {:.2} MB; {:.2} MB still unreadable{exit_str}",
                    recovered as f64 / 1_048_576.0,
                    bytes_unreadable as f64 / 1_048_576.0,
                ),
            );
            // Drop this pass's watcher before next iteration.
            drop(_pass_guard);
            // Stop early if user-halt or pass cap-fire happened during the
            // patch (the watcher set pass_halt + cap_fired_any).
            if user_halt.load(Ordering::Relaxed) || cap_fired_any.load(Ordering::Relaxed) {
                break;
            }
            // If THIS pass made no progress, no future pass with the same
            // drive state will help. Give up retries early so we still
            // mux on what we have.
            if recovered == 0 {
                crate::log::device_log(
                    device,
                    &format!(
                        "PASS {} STOPPED: strategy=patch_recovery exhausted — no progress (recovered={} MB) after all retry attempts",
                        pass,
                        recovered as f64 / 1_048_576.0
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

        // Abort check: load mapfile and calculate main movie loss after all retries.
        // If loss exceeds configured threshold, abort instead of muxing damaged content.
        let mut main_lost_ms_for_history = 0.0f64;
        if cfg_read.max_retries > 0 && bytes_unreadable > 0 {
            let iso_filename = format!("{}.iso", crate::util::sanitize_path_compact(&display_name));
            let mapfile_path_str = format!("{staging}/{iso_filename}.mapfile");
            if let Ok(map) =
                libfreemkv::disc::mapfile::Mapfile::load(std::path::Path::new(&mapfile_path_str))
            {
                use libfreemkv::disc::mapfile::SectorStatus;
                let bad_ranges = map.ranges_with(&[SectorStatus::Unreadable]);
                if !bad_ranges.is_empty() && title_bytes_per_sec > 0.0 {
                    main_lost_ms_for_history = bad_ranges
                        .iter()
                        .map(|(_, size)| *size as f64 / title_bytes_per_sec * 1000.0)
                        .fold(0.0f64, f64::max);
                }
            }

            let abort_threshold_ms = (cfg_read.abort_on_lost_secs * 1000) as f64;
            if main_lost_ms_for_history >= abort_threshold_ms {
                crate::log::device_log(
                    device,
                    &format!(
                        "ABORT: strategy=abort_check triggered — {:.2}s lost in main movie (threshold: {}s)",
                        main_lost_ms_for_history / 1000.0,
                        cfg_read.abort_on_lost_secs
                    ),
                );

                crate::log::device_log(
                    device,
                    &format!(
                        "STRATEGY_FAILURE: abort_check FAILED — data loss ({:.2}s) exceeds threshold ({}s)",
                        main_lost_ms_for_history / 1000.0,
                        cfg_read.abort_on_lost_secs
                    ),
                );

                crate::log::device_log(
                    device,
                    "RECOVERY_GUIDANCE: To allow this rip to complete with data loss, increase abort_on_lost_secs in settings or set to 0 for perfect-rip-only mode.",
                );
                update_state_with(device, |s| {
                    s.status = "error".to_string();
                    if s.last_error.is_empty() {
                        s.last_error = format!(
                            "aborted — {:.2}s lost in main movie (threshold: {}s)",
                            main_lost_ms_for_history / 1000.0,
                            cfg_read.abort_on_lost_secs
                        );
                    }
                });
                if let Ok(mut flags) = HALT_FLAGS.lock() {
                    flags.remove(device);
                }
                return; // Skip mux entirely
            }

            if main_lost_ms_for_history > 0.0 {
                crate::log::device_log(
                    device,
                    &format!(
                        "Main movie loss after retries: {:.2}s (threshold: {}s)",
                        main_lost_ms_for_history / 1000.0,
                        cfg_read.abort_on_lost_secs
                    ),
                );
            } else {
                crate::log::device_log(device, "All data recovered — proceeding with mux.");
            }
        }

        // Mux gating per RIP_DESIGN.md §15 Fix B.
        // Skip mux + return cleanly if user pressed stop.
        if user_halt.load(Ordering::Relaxed) {
            crate::log::device_log(device, "Rip cancelled — skipping mux.");
            if let Ok(mut flags) = HALT_FLAGS.lock() {
                flags.remove(device);
            }
            return;
        }
        // Skip mux + status=error if any pass cap-fired (per-pass wallclock
        // budget exceeded). The ISO is retained in staging for manual
        // salvage; this is a hard failure signal, not a partial success.
        if cap_fired_any.load(Ordering::Relaxed) {
            crate::log::device_log(
                device,
                "Pass cap-fired — rip failed; ISO retained in staging, no mux.",
            );
            update_state_with(device, |s| {
                s.status = "error".to_string();
                if s.last_error.is_empty() {
                    s.last_error = "rip failed — pass exceeded wallclock budget".to_string();
                }
            });
            if let Ok(mut flags) = HALT_FLAGS.lock() {
                flags.remove(device);
            }
            return;
        }

        // Close drive — all physical I/O done.
        crate::log::device_log(device, "Drive released; muxing ISO → MKV.");
        drop(session);

        // Open the ISO for the mux pipeline.
        let iso_reader = match libfreemkv::FileSectorReader::open(&iso_path_str) {
            Ok(r) => r,
            Err(e) => {
                crate::log::device_log(device, &format!("Open ISO failed: {e}"));
                update_state(
                    device,
                    RipState {
                        device: device.to_string(),
                        status: "error".to_string(),
                        disc_present: true,
                        last_error: format!("{e}"),
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
                if let Ok(mut flags) = HALT_FLAGS.lock() {
                    flags.remove(device);
                }
                return;
            }
        };
        // Entering mux phase — push final mapfile state so the UI keeps the
        // bad-range list visible through mux and into the "done" view.
        let mux_state = std::cell::RefCell::new(PassProgressState::new());
        push_pass_state(
            &pass_ctx,
            &title_for_progress,
            bps_progress,
            std::path::Path::new(&mapfile_path_str),
            total_passes,
            total_passes,
            &mux_state,
        );
        Box::new(iso_reader) as Box<dyn libfreemkv::SectorReader>
    } else {
        Box::new(session.drive) as Box<dyn libfreemkv::SectorReader>
    };

    let mut input = libfreemkv::DiscStream::new(reader, title, keys, batch, format);
    // Wire the same halt flag into DiscStream so Stop interrupts fill_extents'
    // internal retry loop — required for Stop to work during dense bad-sector
    // regions where the outer PES read() loop may never emit a frame.
    input.set_halt(halt.clone());
    if cfg_read.on_read_error == "skip" {
        input.skip_errors = true;
    }
    let dev_for_stream_events = device.to_string();
    let wdf_stream = wd_last_frame.clone();
    let llba_stream = rip_last_lba.clone();
    let rbs_stream = rip_current_batch.clone();
    let lbr_stream = latest_bytes_read.clone();
    input.on_event(move |event| {
        // Same rationale as the drive callback — DiscStream events prove
        // the rip is advancing even if no PES frame has been emitted yet.
        wdf_stream.store(crate::util::epoch_secs(), Ordering::Relaxed);
        match event.kind {
            libfreemkv::event::EventKind::BytesRead { bytes, .. } => {
                lbr_stream.store(bytes, Ordering::Relaxed);
            }
            libfreemkv::event::EventKind::BatchSizeChanged { new_size, reason } => {
                rbs_stream.store(new_size, Ordering::Relaxed);
                let label = match reason {
                    BatchSizeReason::Shrunk => "shrunk",
                    BatchSizeReason::Probed => "probed up",
                };
                crate::log::device_log(
                    &dev_for_stream_events,
                    &format!("Batch size → {} ({})", new_size, label),
                );
            }
            libfreemkv::event::EventKind::SectorSkipped { sector } => {
                llba_stream.store(sector, Ordering::Relaxed);
                crate::log::device_log(
                    &dev_for_stream_events,
                    &format!("Sector {} skipped (zero-filled)", sector),
                );
            }
            _ => {}
        }
    });

    // Read frames until codec headers are ready
    let mut buffered = Vec::new();
    let mut header_reads = 0u32;
    while !input.headers_ready() {
        if stop_requested(device) {
            crate::log::device_log(device, "Stop requested during header read");
            if let Ok(mut flags) = HALT_FLAGS.lock() {
                flags.remove(device);
            }
            return;
        }
        match input.read() {
            Ok(Some(frame)) => {
                header_reads += 1;
                if header_reads <= 3 || header_reads % 100 == 0 {
                    crate::log::device_log(
                        device,
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
                crate::log::device_log(device, "EOF during header read");
                break;
            }
            Err(e) => {
                crate::log::device_log(device, &format!("Header error: {}", e));
                break;
            }
        }
    }
    crate::log::device_log(
        device,
        &format!("Headers ready, {} frames buffered", buffered.len()),
    );

    let info = input.info().clone();
    let mut out_title = info.clone();
    out_title.playlist = display_name.clone();
    out_title.codec_privates = (0..info.streams.len())
        .map(|i| input.codec_private(i))
        .collect();
    let total_bytes = if total_bytes > 0 {
        total_bytes
    } else {
        info.size_bytes
    };

    crate::log::device_log(device, &format!("Opening output: {}", dest_url));
    let raw_output = match libfreemkv::output(&dest_url, &out_title) {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("Open output failed: {}", e);
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
    let mut output = libfreemkv::pes::CountingStream::new(raw_output);

    let start = std::time::Instant::now();
    let mut last_update = start;
    let mut last_log = start;
    let mut last_speed_bytes: u64 = 0;
    let mut last_speed_time = start;
    let mut smooth_speed: f64 = 0.0;
    let mut first_update: bool = true;
    let mut seeded_speed: bool = false;

    // Watchdog: monitors the rip loop and logs when reads stall.
    // The rip thread updates last_frame_epoch on every frame. The watchdog
    // checks every 15s and logs if no frame has arrived in 30+ seconds.
    let wd_active = Arc::new(AtomicBool::new(true));
    // Drop guard: stops watchdog on return OR panic (catch_unwind unwinds stack)
    struct WatchdogGuard(Arc<AtomicBool>);
    impl Drop for WatchdogGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Relaxed);
        }
    }
    let _wd_guard = WatchdogGuard(wd_active.clone());
    // `wd_last_frame` is declared earlier (shared with the drive + stream event
    // callbacks, which reset it on any sector-level event). Don't shadow it —
    // the watchdog reader and the callback writers must share one Arc.
    let wd_bytes = Arc::new(AtomicU64::new(0));
    {
        let active = wd_active.clone();
        let last_frame = wd_last_frame.clone();
        let wbytes = wd_bytes.clone();
        let wd_device = device.to_string();
        let wd_display = display_name.clone();
        let wd_format = disc_format.clone();
        let wd_tmdb_title = tmdb_title.clone();
        let wd_tmdb_poster = tmdb_poster.clone();
        let wd_tmdb_overview = tmdb_overview.clone();
        let wd_duration = duration.clone();
        let wd_codecs = codecs.clone();
        let wd_total = total_bytes;
        let wd_tmdb_year = tmdb_year;
        let wd_filename = filename.clone();
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

                if stall_secs >= 30 {
                    // Log on first detection, then every 60s
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
                    // Update UI state every cycle — keep speed/eta current
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
                    // Mutate-in-place via `update_state_with` so we no longer
                    // have to manually re-read errors/lost_video_secs/last_sector/
                    // current_batch/preferred_batch and copy them through —
                    // every field we don't touch keeps its prior value. This
                    // closes the v0.11.20 regression class (Default::default()
                    // wiping live progress fields during a stall).
                    update_state_with(&wd_device, |s| {
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

    // Write buffered frames
    let mut buffered_ok = true;
    for frame in &buffered {
        if stop_requested(device) {
            crate::log::device_log(device, "Stop requested during buffered write");
            buffered_ok = false;
            break;
        }
        if let Err(e) = output.write(frame) {
            crate::log::device_log(device, &format!("Write error (buffered): {}", e));
            buffered_ok = false;
            break;
        }
        // Update watchdog so it doesn't falsely report stall
        wd_last_frame.store(crate::util::epoch_secs(), Ordering::Relaxed);
        wd_bytes.store(output.bytes_written(), Ordering::Relaxed);
    }

    // Stream remaining frames
    let mut completed = false;
    if !buffered_ok {
        crate::log::device_log(device, "Skipping stream loop — buffered write failed");
    }
    loop {
        if !buffered_ok {
            break;
        }
        if stop_requested(device) {
            crate::log::device_log(device, "Stop requested");
            break;
        }
        match input.read() {
            Ok(Some(frame)) => {
                if let Err(e) = output.write(&frame) {
                    crate::log::device_log(device, &format!("Write error: {}", e));
                    break;
                }

                // Signal watchdog: frame received
                wd_last_frame.store(crate::util::epoch_secs(), Ordering::Relaxed);
                wd_bytes.store(output.bytes_written(), Ordering::Relaxed);

                let now = std::time::Instant::now();
                if !first_update && now.duration_since(last_update).as_secs_f64() < 1.0 {
                    continue;
                }
                first_update = false;
                last_update = now;

                let lbr = latest_bytes_read.load(Ordering::Relaxed);
                let bytes_done = if lbr > 0 { lbr } else { output.bytes_written() };
                let pct = if total_bytes > 0 {
                    (bytes_done * 100 / total_bytes).min(100) as u8
                } else {
                    0
                };
                let speed_interval = now.duration_since(last_speed_time).as_secs_f64();
                let instant_speed = if speed_interval > 0.0 {
                    (bytes_done.saturating_sub(last_speed_bytes)) as f64
                        / (1024.0 * 1024.0)
                        / speed_interval
                } else {
                    0.0
                };
                last_speed_bytes = bytes_done;
                last_speed_time = now;
                smooth_speed = if !seeded_speed {
                    seeded_speed = true;
                    instant_speed
                } else {
                    0.95 * smooth_speed + 0.05 * instant_speed
                };
                let speed = smooth_speed;
                let eta = if speed > 0.0 && total_bytes > bytes_done {
                    let secs =
                        ((total_bytes - bytes_done) as f64 / (1024.0 * 1024.0) / speed) as u32;
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

                if now.duration_since(last_log).as_secs() >= 60 {
                    last_log = now;
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
                    if total_bytes > 0 {
                        let total_gb = total_bytes as f64 / 1_073_741_824.0;
                        crate::log::device_log(
                            device,
                            &format!(
                                "{:.1} GB / {:.1} GB ({}%) {}{}",
                                gb, total_gb, pct, speed_str, eta_str
                            ),
                        );
                    } else {
                        crate::log::device_log(device, &format!("{:.1} GB {}", gb, speed_str));
                    }
                }

                let skip_errors = input.errors as u32;
                let lost_video_secs = if title_bytes_per_sec > 0.0 {
                    (skip_errors as f64) * 2048.0 / title_bytes_per_sec
                } else {
                    0.0
                };
                update_state(
                    device,
                    RipState {
                        device: device.to_string(),
                        status: "ripping".to_string(),
                        disc_present: true,
                        disc_name: display_name.clone(),
                        disc_format: disc_format.clone(),
                        progress_pct: pct,
                        progress_gb: bytes_done as f64 / 1_073_741_824.0,
                        speed_mbs: speed,
                        eta: eta.clone(),
                        errors: skip_errors,
                        lost_video_secs,
                        last_sector: rip_last_lba.load(Ordering::Relaxed),
                        current_batch: rip_current_batch.load(Ordering::Relaxed),
                        preferred_batch: batch,
                        output_file: filename.clone(),
                        tmdb_title: tmdb_title.clone(),
                        tmdb_year,
                        tmdb_poster: tmdb_poster.clone(),
                        tmdb_overview: tmdb_overview.clone(),
                        duration: duration.clone(),
                        codecs: codecs.clone(),
                        // Carry the multipass identity through every per-frame
                        // update so the UI doesn't snap back to a "fresh rip"
                        // view when mux starts. pass == total_passes is the
                        // established convention for "we're on the mux pass";
                        // pass/total bars and ETAs mirror local mux progress
                        // (sweep + retries are already 100% by the time we're
                        // here — total_progress reflects the work that's left).
                        pass: total_passes,
                        total_passes,
                        pass_progress_pct: pct,
                        pass_eta: eta.clone(),
                        total_progress_pct: pct,
                        total_eta: eta,
                        ..Default::default()
                    },
                );
            }
            Ok(None) => {
                completed = true;
                break;
            }
            Err(e) => {
                crate::log::device_log(device, &format!("Read error: {}", e));
                break;
            }
        }
    }

    // Watchdog stops automatically via _wd_guard Drop

    // Clean up halt flag
    if let Ok(mut flags) = HALT_FLAGS.lock() {
        flags.remove(device);
    }

    if let Err(e) = output.finish() {
        crate::log::device_log(device, &format!("Output finish error: {}", e));
    }

    let bytes_done = output.bytes_written();
    let elapsed = start.elapsed().as_secs_f64();
    let speed = if elapsed > 0.0 {
        bytes_done as f64 / (1024.0 * 1024.0) / elapsed
    } else {
        0.0
    };
    let mut final_errors = input.errors as u32;
    let final_last_sector = rip_last_lba.load(Ordering::Relaxed);
    let final_current_batch = rip_current_batch.load(Ordering::Relaxed);
    let mut final_lost_secs = if title_bytes_per_sec > 0.0 {
        (final_errors as f64) * 2048.0 / title_bytes_per_sec
    } else {
        0.0
    };
    // In multipass mode the `input.errors` counter above counts ISO→MKV demux
    // skips (usually zero — ISO reads don't fail). The real bad-sector count
    // lives in the mapfile sidecar. Prefer that when present.
    let mut final_num_bad_ranges: u32 = 0;
    let mut final_largest_gap_ms: f64 = 0.0;
    if cfg_read.max_retries > 0 {
        let iso_filename = format!("{}.iso", crate::util::sanitize_path_compact(&display_name));
        let mapfile_path_str = format!("{staging}/{iso_filename}.mapfile");
        if let Ok(map) =
            libfreemkv::disc::mapfile::Mapfile::load(std::path::Path::new(&mapfile_path_str))
        {
            let stats = map.stats();
            // Only Unreadable counts as "lost" — NonTried / NonTrimmed /
            // NonScraped at the END of a rip means the rip was interrupted,
            // not that those bytes are damaged. For an interrupted rip the
            // final history record reflects what we know: unreadable = bad.
            let bad_bytes = stats.bytes_unreadable;
            final_errors = (bad_bytes / 2048) as u32;
            final_lost_secs = if title_bytes_per_sec > 0.0 {
                bad_bytes as f64 / title_bytes_per_sec
            } else {
                0.0
            };
            use libfreemkv::disc::mapfile::SectorStatus;
            let bad_ranges = map.ranges_with(&[SectorStatus::Unreadable]);
            final_num_bad_ranges = bad_ranges.len() as u32;
            final_largest_gap_ms = bad_ranges
                .iter()
                .map(|(_, size)| {
                    if title_bytes_per_sec > 0.0 {
                        *size as f64 / title_bytes_per_sec * 1000.0
                    } else {
                        0.0
                    }
                })
                .fold(0.0f64, f64::max);
        }
    }

    // Write a history record for every rip attempt — completed OR stopped.
    // Stopped rips used to leave no persistent trace except the device log,
    // which gets clobbered on the next scan. Include errors/lost/last_sector
    // so damaged-disc attempts are auditable.
    let status_label = if completed { "complete" } else { "stopped" };
    {
        let marker = serde_json::json!({
            "title": display_name,
            "disc_name": disc_name,
            "format": disc_format,
            "year": tmdb_year,
            "media_type": tmdb_media_type,
            "poster_url": tmdb_poster,
            "overview": tmdb_overview,
            "date": crate::util::format_date(),
        });
        if completed {
            // Only mark staging as ready-to-move when the rip actually finished.
            let marker_path = format!("{}/.done", staging);
            let _ = std::fs::write(
                &marker_path,
                serde_json::to_string_pretty(&marker).unwrap_or_default(),
            );
        }

        let mut entry = marker.clone();
        entry["status"] = serde_json::json!(status_label);
        entry["staging_dir"] = serde_json::json!(staging);
        entry["size_gb"] =
            serde_json::json!((bytes_done as f64 / 1_073_741_824.0 * 10.0).round() / 10.0);
        entry["speed_mbs"] = serde_json::json!((speed * 10.0).round() / 10.0);
        entry["elapsed_secs"] = serde_json::json!(elapsed.round() as u64);
        entry["duration"] = serde_json::json!(duration);
        entry["codecs"] = serde_json::json!(codecs);
        entry["device"] = serde_json::json!(device);
        entry["errors"] = serde_json::json!(final_errors);
        entry["lost_video_secs"] = serde_json::json!((final_lost_secs * 1000.0).round() / 1000.0);
        entry["last_sector"] = serde_json::json!(final_last_sector);
        entry["num_bad_ranges"] = serde_json::json!(final_num_bad_ranges);
        entry["largest_gap_ms"] = serde_json::json!(final_largest_gap_ms.round());
        let log_lines = crate::log::get_device_log(device, 500);
        entry["log"] = serde_json::json!(log_lines.join("\n"));
        crate::history::record(&cfg_read.history_dir(), &entry);
    }

    if !completed {
        crate::log::device_log(
            device,
            &format!(
                "Stopped: {:.1} GB in {:.0}s ({:.0} MB/s), {} skipped (~{:.3}s lost)",
                bytes_done as f64 / 1_073_741_824.0,
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
                status: "idle".to_string(),
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
                ..Default::default()
            },
        );
        return;
    }

    crate::log::device_log(
        device,
        &format!(
            "Complete: {:.1} GB in {:.0}s ({:.0} MB/s), {} skipped (~{:.3}s lost)",
            bytes_done as f64 / 1_073_741_824.0,
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
            status: "done".to_string(),
            disc_present: true,
            disc_name: display_name.clone(),
            disc_format: disc_format.clone(),
            progress_pct: 100,
            errors: final_errors,
            lost_video_secs: final_lost_secs,
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
            ..Default::default()
        },
    );

    if cfg_read.auto_eject {
        eject_drive(device_path);
    }

    // Prune intermediate ISO + mapfile unless keep_iso is set. Only runs in
    // multipass mode (max_retries > 0) — direct mode never produced an ISO.
    if cfg_read.max_retries > 0 && !cfg_read.keep_iso {
        let iso_filename = format!("{}.iso", crate::util::sanitize_path_compact(&display_name));
        let iso_path_str = format!("{}/{}", staging, iso_filename);
        let mapfile_path = format!("{iso_path_str}.mapfile");
        match std::fs::remove_file(&iso_path_str) {
            Ok(_) => crate::log::device_log(device, "Pruned intermediate ISO"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => crate::log::device_log(device, &format!("ISO prune warning: {e}")),
        }
        let _ = std::fs::remove_file(&mapfile_path);
    }

    crate::log::device_log(device, "Rip complete");
    crate::webhook::send_rich(
        &cfg_read,
        &crate::webhook::RipEvent {
            event: "rip_complete",
            title: &display_name,
            year: tmdb_year,
            format: &disc_format,
            poster_url: &tmdb_poster,
            duration: &duration,
            codecs: &codecs,
            size_gb: bytes_done as f64 / 1_073_741_824.0,
            speed_mbs: speed,
            elapsed_secs: elapsed,
            output_path: &staging,
            errors: final_errors,
            lost_video_secs: final_lost_secs,
        },
    );
}

pub fn eject_drive(device_path: &str) {
    let dev = device_path.rsplit('/').next().unwrap_or("");
    // Halt and drain any in-flight rip on this device BEFORE dropping
    // the session — otherwise the rip thread could still be inside a
    // libfreemkv call holding the Drive while we yank it.
    request_stop(dev);
    if join_rip_thread(dev, Duration::from_secs(60)).is_err() {
        tracing::warn!(device = %dev, "rip thread did not drain within 60s of eject");
    }
    drop_session(dev);
    crate::log::archive_device_log(dev);
    if let Ok(mut session) = libfreemkv::Drive::open(std::path::Path::new(device_path)) {
        let _ = session.eject();
    }
}

// `sanitize_filename` and `format_duration` moved to `util` in 0.13.0.
// Callers below now use `crate::util::sanitize_path_compact` and
// `crate::util::format_duration_hm` directly.

fn format_codecs(title: &libfreemkv::DiscTitle) -> String {
    let mut parts = Vec::new();
    // Primary video
    for s in &title.streams {
        if let libfreemkv::Stream::Video(v) = s {
            if !v.secondary {
                let mut desc = format!("{} {}", v.codec.name(), v.resolution);
                if v.hdr != libfreemkv::HdrFormat::Sdr {
                    desc.push_str(&format!(" {}", v.hdr.name()));
                }
                parts.push(desc);
                break;
            }
        }
    }
    // First primary audio only
    for s in &title.streams {
        if let libfreemkv::Stream::Audio(a) = s {
            if !a.secondary {
                let mut audio = format!("{} {}", a.codec.name(), a.channels);
                // autorip is English-only — inline the purpose tags directly.
                if let Some(tag) = audio_purpose_tag(a.purpose) {
                    audio.push_str(&format!(" {}", tag));
                }
                parts.push(audio);
                break;
            }
        }
    }
    parts.join(" · ")
}

/// English purpose label for autorip rendering. None for Normal streams.
/// libfreemkv keeps strings out of the library; autorip is English-only so we
/// inline the words here rather than going through i18n.
#[allow(dead_code)]
fn audio_purpose_tag(p: libfreemkv::LabelPurpose) -> Option<&'static str> {
    match p {
        libfreemkv::LabelPurpose::Commentary => Some("Commentary"),
        libfreemkv::LabelPurpose::Descriptive => Some("Descriptive Audio"),
        libfreemkv::LabelPurpose::Score => Some("Score"),
        libfreemkv::LabelPurpose::Ime => Some("IME"),
        libfreemkv::LabelPurpose::Normal => None,
    }
}

/// English secondary suffix for autorip rendering. Empty when not secondary.
#[allow(dead_code)]
fn audio_secondary_suffix(secondary: bool) -> &'static str {
    if secondary { " (Secondary)" } else { "" }
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

    use super::state::{
        PassProgressState, STATIC_WINDOW_SECS, build_bad_ranges, byte_offset_in_title,
        display_window_secs,
    };
    use super::{STATE, update_state_with};
    use libfreemkv::disc::mapfile::{Mapfile, SectorStatus};

    fn tmp_map(tag: &str, total: u64) -> (std::path::PathBuf, Mapfile) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(0);
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "autorip-ripper-test-{}-{}-{}.mapfile",
            std::process::id(),
            tag,
            n
        ));
        let _ = std::fs::remove_file(&path);
        let map = Mapfile::create(&path, total, "test").unwrap();
        (path, map)
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
    fn pass_progress_first_sample_returns_zero() {
        // Regression: v0.12.0 shipped with the tracker priming the speed
        // on the first sample, which included all already-copied bytes
        // (e.g. from resume). Users saw "2197.8 MB/s" on a BD rip —
        // impossible. First call must not compute a speed; we need at
        // least two samples to compute a delta over time.
        let mut s = PassProgressState::new();
        let t0 = std::time::Instant::now();
        let speed = s.observe(t0, 20 * 1024 * 1024 * 1024);
        assert_eq!(speed, 0.0, "first sample must not synthesize a speed");
        assert_eq!(
            s.samples.len(),
            1,
            "first sample must be recorded for the next call"
        );
    }

    #[test]
    fn pass_progress_second_sample_matches_physical_rate() {
        // 70 MB delta in 1 s → ~70 MB/s. No prior smoothing, so the instant
        // value becomes the first real smoothed value.
        let mut s = PassProgressState::new();
        let t0 = std::time::Instant::now();
        let _ = s.observe(t0, 1_000_000_000);
        let speed = s.observe(
            t0 + std::time::Duration::from_secs(1),
            1_000_000_000 + 70 * 1_048_576,
        );
        assert!((speed - 70.0).abs() < 1.0, "expected ~70 MB/s, got {speed}");
    }

    #[test]
    fn pass_progress_caps_absurd_instantaneous() {
        // If the caller feeds an 80 GB jump in 1 s (e.g. mapfile read of a
        // resumed disc on the first post-throttle callback), the tracker
        // must cap the instant to 1 GB/s instead of smoothing in nonsense.
        let mut s = PassProgressState::new();
        let t0 = std::time::Instant::now();
        let _ = s.observe(t0, 0);
        let speed = s.observe(
            t0 + std::time::Duration::from_secs(1),
            80 * 1024 * 1024 * 1024,
        );
        assert!(speed <= 1024.0, "speed {speed} MB/s not capped");
    }

    #[test]
    fn pass_progress_steady_state_converges() {
        // Feed 20 samples at a constant 70 MB/s rate. Smoothed value must
        // converge within ±2 MB/s.
        let mut s = PassProgressState::new();
        let mut t = std::time::Instant::now();
        let mut bytes: u64 = 1_000_000_000;
        let _ = s.observe(t, bytes);
        let mut last = 0.0;
        for _ in 0..20 {
            t += std::time::Duration::from_secs(1);
            bytes += 70 * 1_048_576;
            last = s.observe(t, bytes);
        }
        assert!(
            (last - 70.0).abs() < 2.0,
            "expected ~70 MB/s after convergence, got {last}"
        );
    }

    #[test]
    fn pass_progress_stall_drops_out_within_window() {
        // The reason the EWMA was replaced. Real-world scenario from
        // 2026-05-08: BU40N reading a UHD disc, drive briefly slow at a
        // marginal LBA region (transient drop from 15 MB/s to ~0.5 MB/s
        // for ~12 s, then full recovery). With the prior EWMA design
        // (alpha=0.3), the stall sample dragged the displayed speed for
        // 30+ s after the drive had recovered, presenting as "the rip is
        // stuck" in the UI when it actually wasn't.
        //
        // Sliding window guarantee: at most STATIC_WINDOW_SECS after
        // recovery (in the early-pass static phase used here), the slow
        // samples have aged out and the displayed speed reflects the
        // current rate.
        let mut s = PassProgressState::new();
        let mut t = std::time::Instant::now();
        let mut bytes: u64 = 0;
        let _ = s.observe(t, bytes);
        // 10 s of healthy ripping at 70 MB/s
        for _ in 0..10 {
            t += std::time::Duration::from_secs(1);
            bytes += 70 * 1_048_576;
            let _ = s.observe(t, bytes);
        }
        let healthy = s.observe(t, bytes);
        assert!(
            (healthy - 70.0).abs() < 2.0,
            "pre-stall speed should be ~70 MB/s, got {healthy}"
        );

        // 12 s stall — drive only delivers 1 MB/s.
        for _ in 0..12 {
            t += std::time::Duration::from_secs(1);
            bytes += 1_048_576;
            let _ = s.observe(t, bytes);
        }
        let during_stall = s.observe(t, bytes);
        assert!(
            during_stall < 20.0,
            "stall must visibly drop the displayed speed (got {during_stall} MB/s)"
        );

        // Recovery — drive back to 70 MB/s. Within STATIC_WINDOW_SECS, the
        // stall samples should have aged out of the window entirely.
        for _ in 0..(STATIC_WINDOW_SECS as i32 + 2) {
            t += std::time::Duration::from_secs(1);
            bytes += 70 * 1_048_576;
            let _ = s.observe(t, bytes);
        }
        let recovered = s.observe(t, bytes);
        assert!(
            (recovered - 70.0).abs() < 2.0,
            "speed must return to ~70 MB/s once stall samples age out of \
             the window; got {recovered} MB/s"
        );
    }

    #[test]
    fn display_window_grows_with_elapsed_time() {
        // Schedule sanity: warmup at 10 s, growth from 60-360 s, cap at 60 s.
        assert_eq!(display_window_secs(0.0), 10.0);
        assert_eq!(display_window_secs(30.0), 10.0);
        assert_eq!(display_window_secs(59.9), 10.0);
        assert_eq!(display_window_secs(60.0), 10.0); // start of growth
        assert!((display_window_secs(210.0) - 35.0).abs() < 0.1); // mid growth
        assert!((display_window_secs(360.0) - 60.0).abs() < 0.1); // cap reached
        assert_eq!(display_window_secs(3600.0), 60.0); // long after cap
    }

    #[test]
    fn display_speed_is_smoother_in_steady_state() {
        // Run long enough for the adaptive window to reach the 60 s cap,
        // then inject a single-sample blip and assert the displayed
        // speed barely moves. Demonstrates that steady-state jitter
        // (a single 1.5 s sample of bad rate) contributes only ~2.5 %
        // weight in a 60 s window of ~40 samples.
        let mut s = PassProgressState::new();
        let mut t = std::time::Instant::now();
        let mut bytes: u64 = 0;
        let _ = s.observe(t, bytes);
        // 7 minutes of steady 70 MB/s — past STATIC_PHASE + GROWTH_PHASE,
        // so the window has reached the 60 s cap.
        for _ in 0..420 {
            t += std::time::Duration::from_secs(1);
            bytes += 70 * 1_048_576;
            let _ = s.observe(t, bytes);
        }
        let steady = s.observe(t, bytes);
        assert!(
            (steady - 70.0).abs() < 1.5,
            "steady-state speed should be ~70 MB/s after 7 min, got {steady}"
        );
        // Inject one slow second.
        t += std::time::Duration::from_secs(1);
        bytes += 1_048_576;
        let after_blip = s.observe(t, bytes);
        // Single slow sample in a 60 s / ~40-sample window: contribution
        // ≤ ~2 MB/s drop. Pre-adaptive 10 s window would have dropped
        // the displayed speed by ~7 MB/s (10 % of window weight).
        assert!(
            (steady - after_blip) < 3.0,
            "single-sample blip shouldn't move displayed speed by > 3 MB/s \
             in a steady-state 60 s window: before {steady}, after {after_blip}"
        );
    }

    #[test]
    fn eta_speed_stays_stable_through_a_stall() {
        // Companion to pass_progress_stall_drops_out_within_window: the
        // displayed speed CAN dip during a stall, but the ETA must not.
        // Old behaviour (eta = remaining / display_speed) made a 12 s
        // stall flip the displayed ETA from 1:30:00 to 30:00:00. The
        // running average from pass start barely moves (a 12 s stall
        // after 5 minutes of healthy rip changes the average by < 5 %).
        let mut s = PassProgressState::new();
        let mut t = std::time::Instant::now();
        let mut bytes: u64 = 0;
        let _ = s.observe(t, bytes);
        // 5 minutes of healthy ripping at 70 MB/s
        for _ in 0..300 {
            t += std::time::Duration::from_secs(1);
            bytes += 70 * 1_048_576;
            let _ = s.observe(t, bytes);
        }
        let display_before = s.observe(t, bytes);
        let eta_before = s.eta_speed_mbs(t, display_before);
        assert!(
            (eta_before - 70.0).abs() < 2.0,
            "ETA speed before stall should be ~70 MB/s, got {eta_before}"
        );

        // 12 s stall — drive only delivers 1 MB/s.
        for _ in 0..12 {
            t += std::time::Duration::from_secs(1);
            bytes += 1_048_576;
            let _ = s.observe(t, bytes);
        }
        let display_during_stall = s.observe(t, bytes);
        let eta_during_stall = s.eta_speed_mbs(t, display_during_stall);
        // After 5 min, the adaptive display window has grown to ~50 s,
        // so a 12 s stall is only ~24 % of the window — display dips
        // visibly but not catastrophically. The point of this test is
        // that ETA stays stable, not that display crashes; the prior
        // fixed-10s window dropped display to <2 MB/s here, but with
        // the adaptive window dilution the stall registers as a
        // moderate dip (which is *better* UX).
        assert!(
            display_during_stall < 65.0,
            "displayed speed must visibly dip during stall (got {display_during_stall} MB/s)"
        );
        // ETA speed should barely have moved — 12 s of slow on top of
        // 5 min of healthy still averages close to 70 MB/s.
        assert!(
            (eta_during_stall - 70.0).abs() < 5.0,
            "ETA speed during stall must stay close to true average, got \
             {eta_during_stall} MB/s (display was {display_during_stall} MB/s)"
        );
    }

    #[test]
    fn eta_falls_back_to_display_during_warmup() {
        // Before ETA_WARMUP_SECS of pass elapsed, the running average is
        // noisy (small denominator). Use the displayed speed instead.
        let mut s = PassProgressState::new();
        let t0 = std::time::Instant::now();
        let _ = s.observe(t0, 0);
        // 2 s elapsed — well below warmup
        let display = s.observe(t0 + std::time::Duration::from_secs(2), 100 * 1_048_576);
        let eta = s.eta_speed_mbs(t0 + std::time::Duration::from_secs(2), display);
        assert_eq!(eta, display, "ETA must fall back to display before warmup");
    }

    #[test]
    fn pass_progress_zero_dt_returns_previous() {
        // Two calls at the same instant must not divide by zero.
        let mut s = PassProgressState::new();
        let t0 = std::time::Instant::now();
        let _ = s.observe(t0, 0);
        let s1 = s.observe(t0, 100_000_000);
        let s2 = s.observe(t0, 200_000_000);
        assert_eq!(s1, s2, "zero-dt sample must not change smoothed speed");
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
    }

    #[test]
    fn device_key_strips_unix_path() {
        // autorip keys its state map by the trailing path component
        // ("sg4", "disk2", "CdRom0"); `device_key` strips the leading
        // /dev/ or \\.\ prefix the lib returns in DriveInfo.path.
        assert_eq!(super::device_key("/dev/sg4"), "sg4");
        assert_eq!(super::device_key("/dev/disk2"), "disk2");
        assert_eq!(super::device_key("\\\\.\\CdRom0"), "CdRom0");
        assert_eq!(super::device_key("sg4"), "sg4"); // already a bare name
    }
}
