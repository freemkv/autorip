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
// addresses as `crate::ripper::*`. Names that aren't reached for
// outside the ripper module (e.g. STOP_COOLDOWNS) stay `pub(super)`
// in their owning sub-module and don't appear here.
//
// `#[allow(unused_imports)]` is retained because the binary build
// (`mod ripper;` is private in `main.rs`) doesn't itself reach for
// every re-export — but `pub mod ripper;` in `lib.rs` and the
// integration tests under `tests/` do, so the re-exports must stay.
#[allow(unused_imports)]
pub use session::{
    device_halt, join_all_rip_threads, join_rip_thread, register_halt, register_rip_thread,
    spawn_rip_thread, take_rip_thread, unregister_halt,
};
pub use staging::wipe_staging;
#[allow(unused_imports)]
pub use state::{BadRange, RipState, STATE, is_busy, set_stop_cooldown, update_state};

// Internal-use imports for the orchestrator code that lives in this
// file. Sub-module-private helpers (`pub(super)`) are reachable from
// here because we are the parent of `state` / `session` / `staging`.
use libfreemkv::event::BatchSizeReason;

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::config::Config;

use session::{DriveSession, drop_session, rediscover_drive, store_session, take_session};
use staging::staging_free_bytes;
use state::{
    PassContext, PassProgressState, is_in_cooldown, push_pass_state, set_pass_progress,
    update_state_with,
};

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
                            // Surface the wedge in the UI. Pre-fix:
                            // drive_has_disc Err -> continue, no
                            // update_state, drive never enters STATE
                            // and /api/state returns {} as if no
                            // drives existed. The drive was visible
                            // only as a one-time "drive enumerated"
                            // INFO log at startup that scrolled away.
                            // Now: drive appears in STATE with status
                            // "error" + a last_error explaining the
                            // wedge and the recovery action. Once
                            // drive_has_disc starts succeeding again
                            // (post power-cycle), the warned_probe_fail
                            // removal at the Ok(_) arm clears the
                            // state and the idle/disc-present path
                            // takes over.
                            update_state(
                                &device,
                                RipState {
                                    device: device.clone(),
                                    status: "error".to_string(),
                                    last_error: format!(
                                        "Drive firmware unresponsive ({}). Power-cycle drive or host required.",
                                        e
                                    ),
                                    ..Default::default()
                                },
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

                    // Allocate the rip's single `Halt` token at the spawn
                    // site so the HTTP /api/stop/{device} handler can find
                    // it via `device_halt(device).cancel()` even before
                    // `rip_disc` starts (e.g. while `scan_disc` is still
                    // running). `rip_disc` and the in-thread cleanup paths
                    // call `unregister_halt(device)` on exit.
                    register_halt(&device, libfreemkv::Halt::new());

                    if let Err(e) = spawn_rip_thread(&device, "rip", move || {
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            scan_disc(&cfg, &device_for_thread, &dev_path);
                            let cancelled = device_halt(&device_for_thread)
                                .map(|h| h.is_cancelled())
                                .unwrap_or(false);
                            if on_insert == "rip" && !cancelled {
                                rip_disc(&cfg, &device_for_thread, &dev_path);
                            } else {
                                // scan-only or scan+stop: clear the
                                // registered token so the next insert
                                // starts fresh.
                                unregister_halt(&device_for_thread);
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
                            unregister_halt(&device_for_thread);
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
    // Snapshot Config (it's Clone) and drop the read guard immediately —
    // see rip_disc for the full rationale. Scans can take 10-30s on
    // damaged discs; that's long enough to noticeably block any
    // settings POST that races with a scan.
    let cfg_read = match cfg.read() {
        Ok(c) => c.clone(),
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
    // The poll-loop spawn site already registered a fresh `Halt` for
    // this device (so an HTTP stop during scan has something to flip).
    // Replace it with a Halt backed by the drive's halt-flag once the
    // drive is open below — that way Stop also pre-empts in-flight
    // `Drive::read` calls inside libfreemkv. Until then a stale halt
    // from a prior rip on the same device must NOT survive into this
    // rip's checks.
    register_halt(device, libfreemkv::Halt::new());

    // Archive the previous rip's per-device log so the live log only
    // shows events from the current attempt. Mirrors what scan_disc
    // does; previously rip_disc was missing this so a stop -> rip
    // cycle left "Stop requested..." / "Pass 1 cancelled" lines from
    // the prior run mixed into the new one.
    crate::log::archive_device_log(device);

    // Snapshot the Config struct (it's Clone) and drop the read guard
    // immediately. Holding the guard across the rip body would block
    // any settings POST (Auto Eject, on_read_error, max_retries, …)
    // for the rip's full duration, and Linux's writer-priority RwLock
    // would queue all subsequent GETs behind the pending writer —
    // the live observed bug where /api/settings, /api/history, and
    // /api/system stopped responding mid-rip until the rip ended.
    let cfg_read = match cfg.read() {
        Ok(c) => c.clone(),
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
    register_halt(device, halt_token.clone());
    // Local alias: pre-existing call sites refer to `halt` as the
    // legacy `Arc<AtomicBool>`. Keep the same name so the watcher
    // helpers (which still take `Arc<AtomicBool>`) compile unchanged
    // — this is the deprecated bridge documented in
    // freemkv-private/memory/0_18_round3_migration_audit.md and is
    // dropped together with `Disc::copy()` in round 3.
    let halt = drive_halt_arc;

    // Rip-level wallclock budget (Fix 3). Caps the ENTIRE rip — all passes
    // combined — at max(disc_runtime, 1h). Implemented as a background thread
    // that sleeps for the budget then fires halt if the rip hasn't finished.
    // Per-pass caps (spawn_pass_watcher) still apply as a finer-grained
    // safety net; this is the backstop so a rip can't run forever even if
    // individual passes keep resetting their timers. Configurable via
    // MAX_RIP_DURATION_SECS env var or settings.json.
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
    let (rip_budget_secs, min_pass_budget_secs, transport_recovery_delay_secs) = {
        let c = cfg.read().unwrap();
        (
            c.max_rip_duration_secs,
            c.min_pass_budget_secs,
            c.transport_recovery_delay_secs,
        )
    };
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
    let max_pass_secs = chosen_runtime_secs.max(min_pass_budget_secs);
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
    // Captured from the multipass branch so the mux call site (which is
    // outside that branch) can pass it into MuxInputs for total-progress
    // weighting. Stays at 0 in direct (single-pass) mode — the mux's
    // total_pct helper falls through to mux-pct passthrough when
    // max_retries == 0 anyway.
    let mut bytes_unreadable_at_mux: u64 = 0;

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
                    unregister_halt(device);
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

            // 0.18 round 3: Pass 1 calls Disc::sweep directly. The old
            // disc.copy(opts.multipass=true) dispatched to sweep_internal
            // which forwarded {decrypt, skip_on_error=multipass} to
            // SweepOptions. resume=true on retry attempts so the existing
            // mapfile state continues where the bridge crash left it
            // (matches the pre-existing implicit resume behaviour: the
            // first attempt is fresh and each retry resumes from mapfile).
            let sweep_opts = libfreemkv::SweepOptions {
                decrypt: false,
                resume: attempt > 1,
                batch_sectors: None,
                skip_on_error: true,
                progress: Some(&pass1_progress),
                halt: Some(pass1_halt.clone()),
            };

            match disc.sweep(&mut session.drive, iso_path, &sweep_opts) {
                Ok(r) => {
                    result = Some(r);
                    break 'pass1;
                }
                Err(e) => {
                    if halt.load(Ordering::Relaxed) {
                        crate::log::device_log(device, &format!("Pass 1 cancelled (halt): {e}"));
                        unregister_halt(device);
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
                            let mut drive = None;
                            for retry in 0..3 {
                                match libfreemkv::Drive::open(std::path::Path::new(&p)) {
                                    Ok(d) => {
                                        drive = Some(d);
                                        break;
                                    }
                                    Err(e) if retry < 2 => {
                                        let backoff_secs =
                                            transport_recovery_delay_secs * (1u64 << retry);
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
                result.bytes_good as f64 / 1_073_741_824.0,
                result.bytes_unreadable as f64 / 1_048_576.0,
                result.bytes_pending as f64 / 1_048_576.0,
            ),
        );

        // Track cross-pass state from CopyResult.
        let mut bytes_good = result.bytes_good;
        let mut bytes_unreadable = result.bytes_unreadable;
        let mut bytes_pending = result.bytes_pending;

        // Retry passes: Disc::patch re-reads only the bad ranges
        // recorded in the mapfile sector-by-sector with full
        // drive-level recovery. Each pass gets its own wallclock cap
        // watcher; cap-fire marks the rip as failed.

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

            // 0.18 round 3: Pass 2..N calls Disc::patch directly. The old
            // disc.copy(opts.multipass=true) dispatched to patch_internal
            // when the mapfile already had retryable bytes; these PatchOptions
            // mirror what patch_internal was constructing internally.
            let patch_opts = libfreemkv::PatchOptions {
                decrypt: false,
                block_sectors: Some(1),
                full_recovery: true,
                reverse: true,
                wedged_threshold: 50,
                progress: Some(&patch_progress),
                halt: Some(pass_halt.clone()),
            };
            let cr = match disc.patch(&mut session.drive, iso_path, &patch_opts) {
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
                unregister_halt(device);
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
            unregister_halt(device);
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
            unregister_halt(device);
            return;
        }

        // Close drive — all physical I/O done.
        crate::log::device_log(device, "Drive released; muxing ISO → MKV.");
        drop(session);

        // Open the ISO for the mux pipeline.
        let iso_reader =
            match libfreemkv::FileSectorReader::open(std::path::Path::new(&iso_path_str)) {
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

    // 0.18 round 2: DiscStream gets the per-device `Halt` at
    // construction via the new `with_halt(...)` builder. Stop
    // interrupts `fill_extents` at the next retry boundary on the
    // same signal that breaks sweep, patch, and the mux frame loop —
    // required for Stop to work during dense bad-sector regions
    // where the outer PES read() loop may never emit a frame.
    let mut input = libfreemkv::DiscStream::new(reader, title, keys, batch, format)
        .with_halt(halt_token.clone());
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

    // 0.18 round 2 #2: the headers-ready buffering, the spawning of
    // the consumer thread, the watchdog, and the per-frame
    // `update_state` cadence all live in `mux::run_mux`. Round 1
    // shipped the mux module as a placeholder; this is the lift onto
    // libfreemkv's `Pipeline` + `Sink` primitive. See
    // `freemkv-private/memory/0_18_redesign.md` § "Module layout".
    //
    // The producer side of `run_mux` polls the per-device `Halt`
    // token (looked up via `device_halt(device)`) at the top of each
    // frame iteration — same token the orchestrator threaded through
    // sweep / patch and the same one the HTTP /api/stop handler
    // cancels.
    let mux_input_errors = Arc::new(AtomicU32::new(0));
    let mux_outcome = mux::run_mux(
        mux::MuxInputs {
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
            total_bytes,
            title_bytes_per_sec,
            total_passes,
            bytes_total_disc: disc.capacity_bytes,
            max_retries: cfg_read.max_retries,
            bytes_unreadable_at_mux,
            dest_url: dest_url.clone(),
            batch,
            // In multi-pass mode (max_retries > 0) the on_read_error setting is
            // hidden from the UI: sweep always skips by design, retries always
            // retry, and the post-retry abort decision is `abort_on_lost_secs`
            // (time-based). The only place on_read_error could touch behaviour
            // is here at mux — file-read / demux glitches on the local ISO.
            // Force skip in multi-pass so the user's stale single-pass setting
            // doesn't trip a spurious abort on a near-finished rip; the
            // accepted-loss intent is already encoded in abort_on_lost_secs.
            skip_errors: cfg_read.max_retries > 0 || cfg_read.on_read_error == "skip",
        },
        input,
        mux::MuxAtomics {
            latest_bytes_read: latest_bytes_read.clone(),
            rip_last_lba: rip_last_lba.clone(),
            rip_current_batch: rip_current_batch.clone(),
            wd_last_frame: wd_last_frame.clone(),
            wd_bytes: Arc::new(AtomicU64::new(0)),
            input_errors: mux_input_errors,
        },
    );

    // Output never opened (stop during headers, or `libfreemkv::output`
    // failed): the pre-split code returned early without writing a
    // history record. Preserve that.
    if !mux_outcome.output_opened {
        unregister_halt(device);
        return;
    }

    // Clean up halt flag
    unregister_halt(device);

    let completed = mux_outcome.completed;
    let bytes_done = mux_outcome.bytes_done;
    let elapsed = mux_outcome.elapsed_secs;
    let speed = mux_outcome.speed_mbs;
    let mut final_errors = mux_outcome.errors;
    let final_last_sector = rip_last_lba.load(Ordering::Relaxed);
    let final_current_batch = rip_current_batch.load(Ordering::Relaxed);
    let mut final_lost_secs = mux_outcome.lost_video_secs;
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
    if let Some(halt) = device_halt(dev) {
        halt.cancel();
    }
    if join_rip_thread(dev, Duration::from_secs(60)).is_err() {
        tracing::warn!(device = %dev, "rip thread did not drain within 60s of eject");
    }
    drop_session(dev);
    unregister_halt(dev);
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

/// Translate a libfreemkv read-error into a user-facing message for
/// /api/state's last_error field. Raw libfreemkv errors like
/// `E6000: 19965280 0x02/0x04/0x3e` are diagnostic-grade — fine for
/// logs, terrible for the UI. This helper renders the same condition
/// as e.g.: "Pass 1 failed at 40.7 GB (sector 19,965,280) — drive
/// firmware unresponsive (HARDWARE_ERROR). Power-cycle the drive and
/// retry the rip."
fn format_pass_error(pass_label: &str, e: &libfreemkv::Error) -> String {
    // Pull sector + sense out of the structured error variants.
    let sector = match e {
        libfreemkv::Error::DiscRead { sector, .. } => Some(*sector),
        _ => None,
    };
    let sense = e.scsi_sense();

    let location = match sector {
        Some(s) => format!(
            " at {:.1} GB (sector {})",
            (s as f64 * 2048.0) / 1_000_000_000.0,
            s
        ),
        None => String::new(),
    };

    let Some(sense) = sense else {
        // Non-SCSI error (transport / IO / other) — append the raw
        // error string; less common path, fewer user complaints.
        return format!("{}{} failed: {}", pass_label, location, e);
    };

    // SCSI sense-key reference (SPC-4 §4.5):
    //   2 NOT_READY, 3 MEDIUM_ERROR, 4 HARDWARE_ERROR,
    //   5 ILLEGAL_REQUEST, 6 UNIT_ATTENTION, 7 DATA_PROTECT, ...
    let (cause, action) = match (sense.sense_key, sense.asc) {
        // MEDIUM_ERROR — physical media damage.
        (3, 0x11) => (
            "bad sector (media damage)",
            "rip will skip this region and retry in Pass 2",
        ),
        (3, 0x02) | (3, 0x03) => (
            "head positioning failure (media damage)",
            "rip will skip this region and retry in Pass 2",
        ),
        (3, _) => (
            "media error (physical damage)",
            "rip will skip this region and retry in Pass 2",
        ),
        // HARDWARE_ERROR — drive firmware-level fault.
        (4, 0x3E) => (
            "drive firmware unresponsive (LOGICAL UNIT NOT CONFIGURED)",
            "power-cycle the drive and retry the rip",
        ),
        (4, _) => (
            "drive hardware error",
            "power-cycle the drive and retry the rip",
        ),
        // ILLEGAL_REQUEST — drive refuses the command. Almost
        // always wedge-state on this drive class.
        (5, 0x24) => (
            "drive rejected command (Invalid Field in CDB — wedge state)",
            "power-cycle the drive and retry the rip",
        ),
        (5, _) => (
            "drive rejected command",
            "power-cycle the drive and retry the rip",
        ),
        // NOT_READY — usually transient, but if we got here it's
        // persistent enough that retries already failed.
        (2, _) => (
            "drive reports not ready",
            "wait a few seconds and retry; if it persists, power-cycle the drive",
        ),
        _ => (
            "drive read error",
            "see autorip logs for the full SCSI sense breakdown",
        ),
    };

    format!("{}{} failed: {} — {}", pass_label, location, cause, action)
}

#[cfg(test)]
mod tests {
    //! Tests for orchestrator-level helpers that live in this file.
    //! State-only helpers and their tests live in `state.rs`.

    use super::format_pass_error;
    use libfreemkv::{Error, ScsiSense};

    #[test]
    fn format_pass_error_hardware_wedge() {
        let e = Error::DiscRead {
            sector: 19_965_280,
            status: Some(2),
            sense: Some(ScsiSense {
                sense_key: 4,
                asc: 0x3E,
                ascq: 0,
            }),
        };
        let s = format_pass_error("Pass 1", &e);
        assert!(s.contains("40.9 GB") || s.contains("40.8 GB") || s.contains("40.7 GB"));
        assert!(s.contains("sector 19965280"));
        assert!(s.to_lowercase().contains("firmware unresponsive"));
        assert!(s.to_lowercase().contains("power-cycle"));
        // No raw "E6000" / hex-tuple cruft.
        assert!(!s.contains("E6000"));
        assert!(!s.contains("0x04/0x3e"));
    }

    #[test]
    fn format_pass_error_medium_error_advises_pass2() {
        let e = Error::DiscRead {
            sector: 1_000_000,
            status: Some(2),
            sense: Some(ScsiSense {
                sense_key: 3,
                asc: 0x11,
                ascq: 0,
            }),
        };
        let s = format_pass_error("Pass 1", &e);
        assert!(s.to_lowercase().contains("bad sector"));
        assert!(s.to_lowercase().contains("pass 2"));
    }

    #[test]
    fn format_pass_error_illegal_request_advises_powercycle() {
        let e = Error::DiscRead {
            sector: 1_000,
            status: Some(2),
            sense: Some(ScsiSense {
                sense_key: 5,
                asc: 0x24,
                ascq: 0,
            }),
        };
        let s = format_pass_error("Pass 1", &e);
        assert!(s.to_lowercase().contains("rejected command"));
        assert!(s.to_lowercase().contains("power-cycle"));
    }

    #[test]
    fn format_pass_error_no_sense_keeps_raw() {
        // Non-SCSI errors (e.g. transport) pass through the original
        // error display so we don't lose information.
        let e = Error::IoError {
            source: std::io::Error::other("io test"),
        };
        let s = format_pass_error("Pass 1", &e);
        assert!(s.contains("Pass 1"));
        assert!(s.contains("io test"));
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
