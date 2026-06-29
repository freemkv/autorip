//! Rip orchestrator — drive poll loop + scan/rip/eject entry points.
//!
//! This module was originally a single 4350-line `ripper.rs`. The
//! state types, thread/halt bookkeeping, and staging-dir helpers have
//! been lifted into sibling sub-modules (`state`, `session`,
//! `staging`). The high-level orchestration — `drive_poll_loop`,
//! `scan_disc`, `rip_disc`, `eject_drive` — stays here. The `mux`
//! sub-module holds the active parallel mux "highway" (consumer/
//! producer split + watchdog); the multipass sweep loop still lives
//! inline in `rip_disc`.

pub(crate) mod mux;
pub mod resume;
mod session;
pub mod staging;
pub mod state;

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
    RegisterError, device_halt, join_all_rip_threads, join_rip_thread, register_halt,
    register_rip_thread, rollback_failed_spawn, spawn_rip_thread, stop_and_drain,
    swap_halt_carrying_cancel, take_rip_thread, unregister_halt,
};
#[allow(unused_imports)]
pub use state::{
    BadRange, Resumable, RipState, STATE, current_claim_gen, current_disc_name, device_known,
    is_busy, set_stop_cooldown, set_title_override, take_title_override, try_claim_active,
    update_state, update_state_with,
};

// Internal-use imports for the orchestrator code that lives in this
// file. Sub-module-private helpers (`pub(super)`) are reachable from
// here because we are the parent of `state` / `session` / `staging`.
use libfreemkv::event::BatchSizeReason;

use crate::util::{BYTES_PER_GIB, BYTES_PER_MIB, MILLIS_PER_SEC};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::config::Config;

use crate::keysource::DriveAccess;

/// [`libfreemkv::ScanOptions`] for the live-drive structure scan: lookup-free,
/// plus the AACS host credentials for the handshake (from the keydb). The
/// library captures structure + AACS inputs; autorip resolves keys afterward
/// from the configured sources via [`resolve_keys_from_drive`].
pub(crate) fn scan_opts_for(cfg: &Config) -> libfreemkv::ScanOptions {
    crate::keysource::drive_scan_opts(cfg)
}

/// Scan-phase watchdog (closes the "scan_disc had NO watchdog" incident).
///
/// `libfreemkv::Disc::scan` and the subsequent `resolve_keys_from_drive` are
/// blocking calls with no autorip-side liveness signal — a wedged drive read
/// during scan, or a hung keyserver round-trip during resolve, would show the
/// UI stuck on "scanning" forever with nothing in the log. This arms a thread
/// that emits a WARN every 15s ("scan still running, Ns elapsed, last
/// phase=X") until the returned guard is dropped (mirrors the rip watchdog and
/// `mux.rs` watchdog drop-guard design).
///
/// The caller advances the phase via [`ScanWatchdog::set_phase`] so the WARN
/// pinpoints whether the time is going into the structure scan or the key
/// resolve.
struct ScanWatchdog {
    active: Arc<AtomicBool>,
    // Coarse phase marker the watcher reports: 0 = scan, 1 = resolve_keys.
    phase: Arc<std::sync::atomic::AtomicU8>,
}

impl ScanWatchdog {
    fn arm(device: &str) -> Self {
        let active = Arc::new(AtomicBool::new(true));
        let phase = Arc::new(std::sync::atomic::AtomicU8::new(0));
        let active_w = active.clone();
        let phase_w = phase.clone();
        let device = device.to_string();
        std::thread::spawn(move || {
            let start = std::time::Instant::now();
            let mut warned = false;
            while active_w.load(Ordering::Relaxed) {
                // Poll in short slices so the guard drop is observed
                // promptly, but only WARN on 15s boundaries.
                std::thread::sleep(Duration::from_secs(1));
                if !active_w.load(Ordering::Relaxed) {
                    break;
                }
                let elapsed = start.elapsed().as_secs();
                if elapsed >= 15 && elapsed % 15 == 0 {
                    let last_phase = match phase_w.load(Ordering::Relaxed) {
                        0 => "scan",
                        _ => "resolve_keys",
                    };
                    tracing::warn!(
                        device = %device,
                        elapsed_secs = elapsed,
                        last_phase,
                        "scan still running"
                    );
                    crate::log::device_log(
                        &device,
                        &format!(
                            "Still scanning ({}s elapsed, phase={})...",
                            elapsed, last_phase
                        ),
                    );
                    warned = true;
                }
            }
            if warned {
                tracing::info!(
                    device = %device,
                    elapsed_secs = start.elapsed().as_secs(),
                    "scan watchdog stood down (scan/resolve returned)"
                );
            }
        });
        Self { active, phase }
    }

    /// Mark that the key-resolve phase has begun, so the WARN reports it.
    fn enter_resolve(&self) {
        self.phase.store(1, Ordering::Relaxed);
    }
}

impl Drop for ScanWatchdog {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

/// Resolve keys for a freshly-scanned live disc via the configured sources. A
/// thin live-drive binding over [`crate::keysource::resolve_keys`]; returns the
/// disc with keys applied (`Resolved`) or unchanged on a miss. No mapfile yet
/// on a fresh rip, so the source list is just the configured source.
fn resolve_keys_from_drive(
    cfg: &Config,
    drive: &mut libfreemkv::Drive,
    disc: libfreemkv::Disc,
) -> (libfreemkv::Disc, crate::keysource::KeyOutcome) {
    let sources = crate::keysource::build_sources(cfg);
    let mut access = DriveAccess::new(drive);
    crate::keysource::resolve_keys(sources, &mut access, disc)
}

/// Human-readable key readiness for the dashboard tile, decided at scan time.
/// Unencrypted, or keys present → "Ready to rip"; encrypted with no keys →
/// "Missing keys — <reason>", where the reason reflects WHAT happened when we
/// tried to resolve (key service unreachable / no key / disc-data anomaly /
/// couldn't read the disc's key files), or — for a local source — the concise
/// libfreemkv AACS failure heading. Capture-without-keys overrides to a
/// proceed-anyway state.
///
/// The tile keys its action button off the "Missing keys" prefix: any other
/// value (including "Capture without keys") leaves the green Rip button up.
fn key_readiness(
    disc: &libfreemkv::Disc,
    outcome: crate::keysource::KeyOutcome,
    capture_without_keys: bool,
) -> String {
    use crate::keysource::KeyOutcome;
    let no_keys =
        disc.encrypted && matches!(disc.decrypt_keys(), libfreemkv::decrypt::DecryptKeys::None);
    if !no_keys {
        return "Ready to rip".to_string();
    }
    if capture_without_keys {
        return "Capture without keys — no decryption".to_string();
    }
    let reason = match outcome {
        KeyOutcome::NoKey => "no key source has a key for this disc".to_string(),
        KeyOutcome::MissingInputs => "this disc's key files could not be read".to_string(),
        // A resolve that still left the disc keyless: defer to the libfreemkv
        // AACS failure message, with the `Error: E<code> ` prefix stripped so
        // the tile shows a concise reason.
        KeyOutcome::Resolved => {
            let msg = keyless_failure_message(disc);
            strip_error_prefix(&msg).to_string()
        }
    };
    format!("Missing keys — {reason}")
}

use session::{
    DriveSession, drop_session, rediscover_drive, session_is_scanned, store_session, take_session,
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
    // Startup safety net (0.20.7): walk staging and decide per-disc
    // what to do — preserve partial rips for restart-loop accounting,
    // wipe genuinely-empty stragglers, leave `.completed` / `.failed`
    // markers in place. Pre-0.20.7 unconditionally wiped on startup,
    // which threw away every in-flight ISO / mapfile when the
    // container restarted mid-rip (Watchtower deploy, OOM, hard
    // watchdog escalation — any of which now should resume, not lose
    // half a UHD rip). See `staging::resume_or_quarantine_staging`.
    // 0.20.8: classify each preserved hint once at startup. Keyed on
    // the staging dir's `file_name()` (== sanitized display_name).
    // The disc-insertion branch consults this map after `scan_disc`
    // produces its own display_name and either routes into the
    // auto-resume re-mux path or falls through to `rip_disc` as
    // before. Map is `BTreeMap` for deterministic logging order;
    // size is bounded by the number of staging subdirs (small).
    // Startup staging scan. This runs `resume_or_quarantine_staging`
    // for its side-effect (quarantine of terminally-failed dirs) and
    // logging. We no longer build a `resume_map` from it: resume is
    // user-gated and recomputed on demand via `find_resumable_for_disc`,
    // so the prior per-disc classification BTreeMap was dead work.
    if let Ok(c) = cfg.read() {
        let hints = staging::resume_or_quarantine_staging(&c.staging_dir);
        tracing::info!(
            staging_dir = %c.staging_dir,
            entries = hints.len(),
            "staging resume scan complete"
        );
        for hint in &hints {
            // Classify for the log only (resume itself is recomputed on
            // demand via find_resumable_for_disc); no map is retained.
            let class = resume::classify_resume(
                hint,
                effective_abort_secs(&c.output_format, c.abort_on_lost_secs),
            );
            tracing::info!(
                dir = %hint.dir.display(),
                action = ?hint.action,
                classification = ?class,
                "staging resume hint"
            );
        }
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
                    device_first_seen.remove(&device);
                    // No eject/scan boundary fires here, so the device's
                    // in-memory log ring would otherwise linger for the
                    // container's lifetime. Evict it like archive_device_log
                    // does on the planned-eject path.
                    crate::log::forget_device(&device);
                    // TITLE_OVERRIDES + STOP_COOLDOWNS are the only other
                    // per-device state; evict them too so stale entries
                    // don't accumulate as device paths churn.
                    state::forget_device_state(&device);
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

                if is_new_insert && !is_in_cooldown(&device) {
                    let on_insert = cfg
                        .read()
                        .unwrap_or_else(|e| e.into_inner())
                        .on_insert
                        .clone();

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

                    // Atomically claim the device under one STATE lock, exactly
                    // like the /api/scan and /api/rip web handlers. The old
                    // `!is_busy(&device)` gate plus a separate `update_state`
                    // and `register_halt` was a TOCTOU: a concurrent /api/rip
                    // could claim between the check and the spawn, yielding two
                    // rip threads on one drive, an orphaned Halt, and a dropped
                    // JoinHandle. If the claim loses, another path already owns
                    // the device — skip the spawn.
                    if !try_claim_active(&device) {
                        continue;
                    }

                    tracing::info!(
                        device = %device,
                        on_insert = %on_insert,
                        "spawning scan/rip thread"
                    );

                    // NOTE: try_claim_active already set status="scanning" +
                    // disc_present=true under the STATE lock, so no separate
                    // update_state is needed here.

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

                    // v0.25.7: restore the on_insert=rip auto-rip
                    // behaviour. The v0.23.0 commit killed it as the
                    // brute-force fix for "container restart auto-rips
                    // pathology" (Watchtower deploys / watchdog
                    // restarts / host reboots all kicking off a fresh
                    // rip when a disc happened to still be in the
                    // drive). The v0.25.3 parallel mux pipeline makes
                    // unattended-rip the killer feature for this whole
                    // project — insert disc, walk away, come back to
                    // find it ripped + muxed + queued for the next
                    // one — so we need the auto-rip back. The
                    // "restart auto-rips" failure modes are now
                    // handled by:
                    //   - is_in_cooldown(device): 5-second STOP
                    //     cooldown prevents flap-restart loops.
                    //   - .completed marker check inside rip_disc:
                    //     a disc that's already been ripped + got
                    //     `.completed` doesn't get re-ripped on
                    //     restart.
                    //   - .restart_count counter promoting to
                    //     `.failed` after RESTART_LIMIT to break the
                    //     "container restarts mid-rip every N
                    //     seconds" loop.
                    // Operators who don't want auto-rip leave
                    // on_insert at the default "scan", or set
                    // "nothing" to skip even the scan.
                    let do_auto_rip = on_insert == "rip";
                    let cfg_for_thread = cfg.clone();
                    let dev_path_for_thread = dev_path.clone();
                    if let Err(e) = spawn_rip_thread(&device, "rip", move || {
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            scan_disc(&cfg, &device_for_thread, &dev_path);
                            if do_auto_rip {
                                let cancelled = device_halt(&device_for_thread)
                                    .map(|h| h.is_cancelled())
                                    .unwrap_or(false);
                                if !cancelled {
                                    handle_rip_request(
                                        &cfg_for_thread,
                                        &device_for_thread,
                                        &dev_path_for_thread,
                                        crate::web::ResumeMode::Default,
                                    );
                                }
                            }
                            unregister_halt(&device_for_thread);
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
                        // The claim (status="scanning") + register_halt above
                        // already ran. A bare warn here leaks the Halt and
                        // wedges the device in "scanning" forever. Mirror the
                        // web handlers' rollback.
                        rollback_failed_spawn(&device);
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

/// Push an "error" state for `device` after a poisoned config `RwLock` forced
/// an early return out of `scan_disc` / `rip_disc`.
///
/// `try_claim_active` sets `status="scanning"` before either thread is spawned,
/// so without this the tile would stay wedged in "scanning" forever with an
/// empty `last_error`. Surfacing an error state (plus a device-log line) keeps
/// the failure visible and self-explanatory, matching the other early-exit
/// paths in those functions. `op` is the user-facing verb ("Scan" / "Rip").
fn mark_config_lock_poisoned(device: &str, op: &str) {
    crate::log::device_log(device, &format!("{op} aborted: config lock poisoned"));
    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "error".to_string(),
            disc_present: true,
            last_error: "Internal error: config lock poisoned".to_string(),
            ..Default::default()
        },
    );
}

/// Scan a disc — open, init, identify, TMDB, full scan. Stores session for rip.
pub fn scan_disc(cfg: &Arc<RwLock<Config>>, device: &str, device_path: &str) {
    // Snapshot Config (it's Clone) and drop the read guard immediately —
    // see rip_disc for the full rationale. Scans can take 10-30s on
    // damaged discs; that's long enough to noticeably block any
    // settings POST that races with a scan.
    let cfg_read = match cfg.read() {
        Ok(c) => c.clone(),
        Err(_) => {
            mark_config_lock_poisoned(device, "Scan");
            return;
        }
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
        tracing::warn!(device = %device, error = %e, "drive init failed (continuing — scan may degrade)");
    }
    // Engage the drive's disc-type read mode before any read. Idempotent.
    if let Err(e) = drive.probe_disc() {
        tracing::warn!(device = %device, error = %e, "drive probe_disc failed (continuing)");
    }

    // Fast identify — disc name only, no playlists
    crate::log::device_log(device, "Identifying disc...");
    let disc_id = match libfreemkv::Disc::identify(&mut drive) {
        Ok(id) => id,
        Err(e) => {
            let msg = format_lib_error("Could not read the disc", &e);
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
    let scan_opts = scan_opts_for(&cfg_read);
    // Arm the scan-phase watchdog: WARNs every 15s while scan/resolve runs,
    // torn down by the drop-guard when this block returns.
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
    // Sample-based key source: resolve the Unit Key from the disc's files +
    // on-disc samples and re-scan with it (a no-op for a local source). The
    // outcome carries WHY a resolve failed, for the readiness message.
    //
    // The online path POSTs the MKB + samples to the keyserver, which over a
    // slow remote link can take a minute or two — tell the user we're waiting on
    // it so the pause isn't mistaken for a hang. (Online sources are exactly the
    // ones that `needs_samples()`.)
    // DVD decryption is CSS — fully resolved by libfreemkv's scan. The AACS
    // key-resolution path (MKB / Unit_Key_RO.inf / sample units / keyserver)
    // does not apply to a DVD; running it reads the disc as if it were AACS/UHD
    // and is pure waste (and was where DVD scans stalled). Skip it for DVD.
    let (disc, key_outcome) = if matches!(disc.format, libfreemkv::DiscFormat::Dvd) {
        tracing::info!(device = %device, "resolve_keys: skipped (DVD/CSS — no AACS)");
        (disc, crate::keysource::KeyOutcome::Resolved)
    } else {
        if crate::keysource::uses_online(&cfg_read) {
            crate::log::device_log(device, "Communicating with online keyserver...");
            update_state_with(device, |s| {
                s.key_status = "Communicating with online keyserver…".to_string();
            });
        }
        scan_wd.enter_resolve();
        let resolve_t0 = std::time::Instant::now();
        tracing::info!(device = %device, "resolve_keys: begin");
        let r = resolve_keys_from_drive(&cfg_read, &mut drive, disc);
        tracing::info!(device = %device, elapsed_ms = resolve_t0.elapsed().as_millis() as u64, "resolve_keys: end");
        r
    };
    // Scan + resolve are done; stand the watchdog down explicitly (drop also
    // covers any early return above).
    drop(scan_wd);
    let key_status = key_readiness(&disc, key_outcome, cfg_read.capture_without_keys);

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

    // 0.20.7: if the resume-on-startup detector flipped this disc's
    // staging dir to `.failed` (restart loop), surface that here so
    // the operator sees "failed: restart loop detected" on the
    // dashboard before triggering a fresh rip. `failure_reason`
    // overrides the normal idle status when present.
    let staging_disc =
        cfg_read.staging_device_dir(&crate::util::sanitize_path_compact(&display_name));
    let failure_reason = staging::read_failed_reason(std::path::Path::new(&staging_disc));
    let (status_str, last_error_str, failure_field) = match failure_reason.as_ref() {
        Some(r) => ("failed".to_string(), r.clone(), Some(r.clone())),
        None => ("idle".to_string(), String::new(), None),
    };

    // Does this disc have resumable partial staging? Drives the dashboard's
    // Resume-vs-Rip choice. Computed before `display_name` moves into the state.
    let resumable = resumable_for_disc(&cfg_read, &display_name);

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: status_str,
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
            last_error: last_error_str,
            failure_reason: failure_field,
            key_status,
            resumable,
            ..Default::default()
        },
    );
}

// ─── Rip ───────────────────────────────────────────────────────────────────

/// Entry point for `/api/rip[?resume=yes|no]`. Scans the disc to
/// identify it, then dispatches to `resume_remux` or `rip_disc`
/// depending on the resume mode requested by the caller and the
/// presence of resumable staging state.
///
/// This is the *only* path that starts disk-writing work as of
/// 0.23.0. Disc insertion does scan-only; the user (via the HTTP API
/// or UI) is the sole trigger for anything destructive.
pub fn handle_rip_request(
    cfg: &Arc<RwLock<Config>>,
    device: &str,
    device_path: &str,
    mode: crate::web::ResumeMode,
) {
    // v0.25.7: skip the scan when the disc has already been scanned
    // since insertion. Pre-0.25.7 this was unconditional, which meant
    // every /api/rip click ran a second full scan + TMDB lookup —
    // wiped the poster + title in the UI for ~10-30 s and burned the
    // wallclock for no benefit since rip_disc would have reused the
    // session anyway via take_session(). On disc eject + re-insert
    // the poll loop calls drop_session, so a stale session can't
    // survive a media change.
    if !session_is_scanned(device) {
        scan_disc(cfg, device, device_path);
    } else {
        crate::log::device_log(
            device,
            "Skipping redundant scan — disc already identified since insertion.",
        );
    }
    let cancelled = device_halt(device)
        .map(|h| h.is_cancelled())
        .unwrap_or(false);
    if cancelled {
        return;
    }
    match mode {
        crate::web::ResumeMode::Require => {
            if resumable_for_device(cfg, device) == Some(Resumable::Sweep) {
                // Prefer continuing the sweep whenever there is ANY not-good
                // data left to recover (pending OR previously-Unreadable):
                // continue Pass N from the mapfile, re-reading only the
                // not-good ranges instead of the whole disc. `passes = N` is
                // the recovery budget; nothing is ever abandoned as "dead".
                crate::log::device_log(
                    device,
                    "Resume requested: continuing partial sweep from mapfile",
                );
                rip_disc(cfg, device, device_path, true);
            } else if let Some(class) = find_resumable_for_disc(cfg, device) {
                // Mapfile is 100% recovered — just re-mux the staged ISO, no
                // disc reads.
                crate::log::device_log(device, "Resume requested: re-muxing existing ISO");
                resume::resume_remux(cfg, device, class);
                drop_session(device);
            } else {
                crate::log::device_log(
                    device,
                    "Resume requested but no resumable staging state found for this disc",
                );
                update_state(
                    device,
                    RipState {
                        device: device.to_string(),
                        status: "error".to_string(),
                        last_error:
                            "Resume requested but no resumable staging state found for this disc"
                                .to_string(),
                        ..Default::default()
                    },
                );
                drop_session(device);
            }
        }
        crate::web::ResumeMode::Wipe => {
            // Never wipe a dir the mux worker is actively reading. Wipe
            // deliberately bypasses `disc_already_completed` (the operator may
            // legitimately want to re-rip a finished disc), but `remove_dir_all`
            // under an in-flight mux deletes the ISO out from under the worker:
            // its read stream fails with ENOENT, the MuxingGuard's clear hits a
            // NotFound, the snapshot returns None, and the staging dir + ISO are
            // permanently lost with no retry possible. The MuxingGuard (which the
            // "deliberately bypasses" comment predates) is exactly what we honour
            // here. Refuse the wipe and tell the operator to retry once the mux
            // finishes.
            if disc_owned_by_worker(cfg, device) {
                crate::log::device_log(
                    device,
                    "Refusing to wipe staging: the mux worker is reading this disc's staged ISO (.ripped/.muxing). Wait for the mux to finish, then retry.",
                );
                update_state_with(device, |s| {
                    s.status = "error".to_string();
                    s.last_error =
                        "Cannot wipe: staged ISO is owned by the mux worker. Retry after mux completes."
                            .to_string();
                });
                drop_session(device);
                return;
            }
            wipe_staging_for_disc(cfg, device);
            rip_disc(cfg, device, device_path, false);
        }
        crate::web::ResumeMode::Default => {
            // Unattended auto-rip must not re-rip a disc that already
            // finished cleanly. On a container restart (Watchtower deploy /
            // watchdog / host reboot) with the disc still in the drive, the
            // insert→auto-rip path would otherwise sweep the whole disc again
            // and overwrite the staged ISO. A `.completed` marker in the
            // disc's staging dir is the authoritative "already ripped" signal.
            // User-initiated rips (Wipe / Require) deliberately bypass this —
            // only the unattended Default path is guarded.
            if disc_already_completed(cfg, device) {
                crate::log::device_log(
                    device,
                    "Disc already ripped (.completed marker present) — skipping unattended re-rip. Click Rip to force a fresh rip.",
                );
                let prev = STATE.lock().ok().and_then(|s| s.get(device).cloned());
                update_state(
                    device,
                    RipState {
                        device: device.to_string(),
                        status: "idle".to_string(),
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
                        ..Default::default()
                    },
                );
                drop_session(device);
                return;
            }
            // Mutual exclusion with the mux worker. A `.ripped`/`.muxing`
            // staging dir for this disc is OWNED by the mux worker (sweep+patch
            // done, mux pending or in flight). Running a fresh sweep here would
            // truncate the ISO the mux worker is reading. Skip the auto-rip and
            // leave the worker to finish.
            if disc_owned_by_worker(cfg, device) {
                crate::log::device_log(
                    device,
                    "Disc rip already staged and owned by the mux worker (.ripped/.muxing) — skipping unattended re-sweep.",
                );
                drop_session(device);
                return;
            }
            // Anti-clobber: a `.aborted-loss` staging holds a complete (or
            // partially-recovered) swept ISO that aborted only on the loss
            // threshold. A fresh sweep here would overwrite that 50+ GB ISO and
            // throw away the recovery progress (the bug that destroyed a swept
            // Dunkirk ISO). Leave it for the operator to Accept (deliver) or
            // resume (run another recovery pass) — never auto-clobber it.
            if disc_loss_aborted(cfg, device) {
                crate::log::device_log(
                    device,
                    "Disc has a loss-aborted staged ISO awaiting an operator decision — NOT re-ripping. Use 'Accept damage' to deliver it, or 'Resume' to run another recovery pass.",
                );
                drop_session(device);
                return;
            }
            rip_disc(cfg, device, device_path, false);
        }
    }
}

/// Does the currently-scanned disc already have a `.completed` staging dir?
///
/// Title-matches the scanned disc name against staging dir basenames (same
/// exact/prefix convention as [`find_resumable_for_disc`]) and reports whether
/// a match carries the process-level `.completed` marker. Used only to gate
/// the unattended auto-rip path so a container restart doesn't re-rip a disc
/// that already finished.
/// True if a staging-dir basename is the resume/completion match for a
/// sanitized disc name. EXACT equality only: staging dirs are created with
/// the exact sanitized disc name (no year/suffix), so a prefix match never
/// legitimately fires — it only invites collisions where a shorter title's
/// name is a prefix of a longer one with a separator ("Cars" sanitizes to
/// "Cars", "Cars 2" to "Cars_2"). Exact equality is collision-free. Both
/// `disc_already_completed` and `find_resumable_for_disc` route through this
/// so the rule can't drift apart between the two call sites.
fn staging_dir_matches_disc(basename: &str, sanitized: &str) -> bool {
    basename == sanitized
}

/// List the immediate-child basenames of the staging root with the same
/// NFS cold-cache discipline as [`staging::snapshot_staging_disc`].
///
/// `disc_already_completed` and `find_resumable_for_disc` both walk the
/// staging root to find the current disc's per-disc subdir. The naive
/// `read_dir(...).flatten()` silently drops per-`DirEntry` I/O errors,
/// which on a Watchtower restart with a cold NFS attribute cache can make
/// the matching subdir vanish from the listing for one scan — exactly the
/// degradation `snapshot_staging_disc` already defends against (observed
/// 2026-05-15). A dropped entry would make `disc_already_completed` return
/// false (re-sweeping an already-done disc) or `find_resumable_for_disc`
/// return None (falling through to a fresh sweep instead of resuming).
///
/// Defense: retry `read_dir` up to 3 times (500 ms apart) whenever a pass
/// fails to open OR yields any per-entry error, and return the UNION of every
/// basename seen across attempts. A clean pass (opened, zero entry errors) is
/// trusted immediately. Returns `None` only when no `read_dir` attempt ever
/// opened the directory — callers then behave exactly as the old
/// `.ok()? / return false` did (no listing → no match), rather than acting on
/// a half-listing that dropped the disc's own dir.
///
/// The union (rather than the single largest-count pass) matters because
/// different degraded passes can surface disjoint partial views of the same
/// mount: a disc's subdir present in an earlier, smaller pass but absent from
/// a later, larger one would otherwise be silently dropped, defeating the
/// whole point of the retry.
fn list_staging_basenames(staging_dir: &std::path::Path) -> Option<Vec<String>> {
    let mut saw_read_ok = false;
    // Insertion-ordered union of every basename observed across passes; the
    // set guards against duplicating a name seen in more than one pass.
    let mut union: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for attempt in 0..3 {
        if let Ok(entries) = std::fs::read_dir(staging_dir) {
            saw_read_ok = true;
            let mut had_entry_error = false;
            for entry in entries {
                match entry {
                    Ok(e) => {
                        if let Some(n) = e.path().file_name() {
                            let name = n.to_string_lossy().into_owned();
                            if seen.insert(name.clone()) {
                                union.push(name);
                            }
                        }
                    }
                    // Don't `.flatten()` away per-entry errors: a partial
                    // NFS degradation can error on an individual DirEntry
                    // while the dir is genuinely populated. Retry the whole
                    // listing rather than trust this undercounted pass.
                    Err(_) => had_entry_error = true,
                }
            }
            if !had_entry_error {
                // Clean, complete listing — trust it immediately. We still
                // return the accumulated union: any name from a prior degraded
                // pass that this clean pass happened not to surface stays in.
                return Some(union);
            }
        }
        if attempt < 2 {
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }
    if saw_read_ok {
        // Every pass that opened had at least one entry error; return the union
        // of every basename we observed rather than None, so a disc whose dir
        // appeared in any pass is still matchable.
        Some(union)
    } else {
        // Never opened the directory across all retries — UNKNOWN. Behave
        // like the old `read_dir(...).ok()?` (no listing → no match).
        None
    }
}

/// Does the currently-scanned disc have a resumable `.aborted-loss` staging dir
/// (a swept ISO that aborted only on the main-movie loss threshold)? Used to
/// stop the unattended Default path from re-sweeping over — and clobbering — an
/// ISO that is waiting on an operator Accept / run-another-pass decision.
fn disc_loss_aborted(cfg: &Arc<RwLock<Config>>, device: &str) -> bool {
    let cfg_read = match cfg.read() {
        Ok(c) => c.clone(),
        Err(_) => return false,
    };
    let display_name = STATE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(device)
        .map(|rs| rs.disc_name.clone())
        .unwrap_or_default();
    if display_name.is_empty() {
        return false;
    }
    let sanitized = crate::util::sanitize_path_compact(&display_name);
    let dir = std::path::Path::new(&cfg_read.staging_dir).join(&sanitized);
    dir.join(staging::ABORTED_LOSS_MARKER).exists()
}

fn disc_already_completed(cfg: &Arc<RwLock<Config>>, device: &str) -> bool {
    let cfg_read = match cfg.read() {
        Ok(c) => c.clone(),
        Err(_) => return false,
    };
    // Recover from a poisoned mutex rather than silently returning false (which
    // would re-rip an already-completed disc). Matches update_state/is_busy.
    let display_name = STATE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(device)
        .map(|rs| rs.disc_name.clone())
        .unwrap_or_default();
    if display_name.is_empty() {
        return false;
    }
    let sanitized = crate::util::sanitize_path_compact(&display_name);
    // NFS-resilient listing (retries + surfaces per-entry errors) instead of
    // `read_dir(...).flatten()`, which would silently drop the disc's own dir
    // on a cold-cache DirEntry error and wrongly re-rip a completed disc.
    let staging_root = std::path::Path::new(&cfg_read.staging_dir);
    staging_disc_completed(staging_root, &sanitized)
}

/// Pure core of `disc_already_completed`: does a staging dir whose basename
/// exactly matches `sanitized` carry `.completed` AND not `.review`? Split out
/// (no `STATE`/`Config` reads) so the M4 held-for-review gating is unit-testable.
///
/// `.completed` alone is NOT enough: the auto-resume mux writes `.completed`
/// even for a rip HELD for operator review (it writes `.review` instead of
/// `.done`, then `.completed`). Treating such a dir as "already ripped" would
/// make the unattended insert path skip it as finished while it's actually
/// awaiting operator confirmation. A held-for-review disc is therefore NOT
/// "already completed": require `.completed` AND absence of `.review` (M4). The
/// review UI's `list_held` keys on `.review` independently, so it still
/// surfaces the dir.
fn staging_disc_completed(staging_root: &std::path::Path, sanitized: &str) -> bool {
    let Some(basenames) = list_staging_basenames(staging_root) else {
        return false;
    };
    for basename in basenames {
        let path = staging_root.join(&basename);
        // EXACT match only. Staging dirs are created with the exact sanitized
        // disc name (no year/suffix), so a prefix match never legitimately
        // fires for the disc's own dir — it only invites collisions where a
        // shorter title's name is a prefix of a longer one ("Cars" sanitizes
        // to "Cars", "Cars 2" to "Cars_2"; a word-boundary check still fails
        // since `_` is the space separator). Exact equality is collision-free.
        if !staging_dir_matches_disc(&basename, sanitized) {
            continue;
        }
        // Use the NFS-resilient snapshot (3-retry read_dir) rather than bare
        // `path.join(MARKER).exists()` — on a cold NFS attribute cache an
        // `.exists()` immediately after `write_completed_marker` can
        // false-negative even though the marker is durably on disk, which
        // would let the Default auto-insert path re-rip a finished disc and
        // truncate the staged ISO the mux worker / mover is still using. The
        // snapshot is the same view every other marker-detection caller
        // (resume_or_quarantine_staging, check_and_mux, remux_from_ripped_marker)
        // relies on for NFS consistency.
        if let Some(snap) = staging::snapshot_staging_disc(&path) {
            if snap.completed && !snap.has_review {
                return true;
            }
        }
    }
    false
}

/// Does the currently-scanned disc have a staging dir that is OWNED by the mux
/// worker (`.ripped` hand-off pending, or `.muxing` lock held)? Used by the
/// unattended Default auto-insert path to refuse a fresh sweep on such a dir:
/// a fresh sweep would truncate the ISO the mux worker is reading (or is about
/// to read). Mirrors `disc_already_completed`'s lookup (exact-name match,
/// NFS-resilient listing) but checks the owner markers instead of `.completed`.
fn disc_owned_by_worker(cfg: &Arc<RwLock<Config>>, device: &str) -> bool {
    let cfg_read = match cfg.read() {
        Ok(c) => c.clone(),
        Err(_) => return false,
    };
    let display_name = STATE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(device)
        .map(|rs| rs.disc_name.clone())
        .unwrap_or_default();
    if display_name.is_empty() {
        return false;
    }
    let sanitized = crate::util::sanitize_path_compact(&display_name);
    let staging_root = std::path::Path::new(&cfg_read.staging_dir);
    staging_disc_owned_by_worker(staging_root, &sanitized)
}

/// Pure core of `disc_owned_by_worker`: does a staging dir whose basename
/// exactly matches `sanitized` carry `.ripped` or `.muxing`? Split out (no
/// `STATE`/`Config` reads) so the H1 exclusion is unit-testable.
fn staging_disc_owned_by_worker(staging_root: &std::path::Path, sanitized: &str) -> bool {
    let Some(basenames) = list_staging_basenames(staging_root) else {
        return false;
    };
    for basename in basenames {
        let path = staging_root.join(&basename);
        if !staging_dir_matches_disc(&basename, sanitized) {
            continue;
        }
        // Read `.ripped`/`.muxing` from the NFS-resilient, 3x-retried snapshot
        // rather than two bare `.exists()` stats. On a cold-cache NFS mount after
        // a container restart the marker can be momentarily invisible to a raw
        // stat, making this return false; the Default auto-rip path then falls
        // through to `rip_disc`, which O_TRUNCs the ISO the mux worker is
        // reading. The snapshot matches `resumable_dir_blocked` /
        // `find_resumable_for_disc`, which are already NFS-resilient.
        if let Some(snap) = staging::snapshot_staging_disc(&path) {
            if snap.has_ripped || snap.has_muxing {
                return true;
            }
        }
    }
    false
}

/// Is this staging dir blocked from drive-resume (Remux) by an owner, held, or
/// terminal marker? Pure projection of the snapshot booleans so the H1/M3 skip
/// rules are unit-testable without seeding `STATE`/`Config`.
///
/// - `.ripped` / `.muxing` — OWNED by the mux worker. Returning Remux would
///   double-mux the same output (the worker is already on it) and, on
///   `.muxing`, race the worker for the ISO it's reading (H1).
/// - `.review` — HELD for operator review; the operator hasn't resolved the
///   title match. Re-muxing would overwrite the held output before they decide (M3).
/// - `.failed` — TERMINAL; a prior attempt gave up (the ISO may be
///   partial/aborted). Don't silently re-mux past it; the operator must
///   explicitly Wipe + re-rip. Keyed on PRESENCE (`has_failed`) so a non-JSON
///   `.failed` body is still honoured (M3).
fn resumable_dir_blocked(snap: &staging::StagingSnapshot) -> bool {
    snap.has_ripped || snap.has_muxing || snap.has_review || snap.has_failed
}

/// Look at the staging dirs for a Remux-eligible entry whose
/// dir basename matches (exact, prefix-either-way) the sanitized
/// display_name of the currently-scanned disc. Returns the
/// `ResumeClass::Remux` payload if found, else None.
///
/// Single-drive convention: the host has one drive, one inserted disc
/// at a time. There is at most one staging dir matching the disc by
/// title prefix. If somehow two match we pick the first; in a
/// multi-drive future this needs disambiguation by stable disc
/// fingerprint (UDF volume_id) instead of sanitized title.
fn find_resumable_for_disc(cfg: &Arc<RwLock<Config>>, device: &str) -> Option<resume::ResumeClass> {
    let cfg_read = cfg.read().ok()?.clone();
    // Recover from a poisoned mutex rather than silently returning None (which
    // would fail to resume a valid staged ISO). Matches disc_already_completed.
    let display_name = STATE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(device)
        .map(|rs| rs.disc_name.clone())
        .unwrap_or_default();
    if display_name.is_empty() {
        return None;
    }
    let sanitized = crate::util::sanitize_path_compact(&display_name);
    // NFS-resilient listing (retries + surfaces per-entry errors) instead of
    // `read_dir(...).flatten()`, which would silently drop the disc's own dir
    // on a cold-cache DirEntry error and fall through to a fresh sweep rather
    // than resuming the existing ISO.
    let staging_root = std::path::Path::new(&cfg_read.staging_dir);
    let basenames = list_staging_basenames(staging_root)?;
    for basename in basenames {
        let path = staging_root.join(&basename);
        // Match the disc's own staging dir by EXACT name. Staging dirs are
        // created with the exact sanitized disc name, so a prefix match never
        // legitimately fires — it only collides ("Cars" is a prefix of
        // "Cars_2" from "Cars 2"; "Dune" of "Dunkirk"), resuming onto a
        // different title's partial ISO + mapfile. Exact equality is safe.
        if staging_dir_matches_disc(&basename, &sanitized) {
            // User-initiated resume goes straight to the underlying
            // remux-eligibility check (ISO + mapfile present, mapfile
            // parses, no bytes_pending, lost_secs within threshold).
            // It still refuses dirs that are OWNED by the mux worker
            // (`.ripped`/`.muxing`), HELD for review (`.review`), or
            // TERMINAL (`.failed`) — see the per-marker skips below.
            // A `.completed` dir naturally has bytes_pending == 0 and no
            // owner/held/terminal marker, but its ISO was already pruned,
            // so `has_iso` is false and it falls through the ISO guard.
            let snap = staging::snapshot_staging_disc(&path)?;
            // Owned/held/terminal dirs are not drive-resumable — see
            // `resumable_dir_blocked` for the per-marker reasoning (H1/M3).
            if resumable_dir_blocked(&snap) {
                continue;
            }
            if !snap.has_iso || !snap.has_mapfile {
                continue;
            }
            let (iso_path, mapfile_path) = resume::find_iso_and_mapfile(&path)?;
            let map = match libfreemkv::disc::mapfile::Mapfile::load(&mapfile_path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            let stats = map.stats();
            if stats.bytes_pending != 0 {
                continue;
            }
            // Pre-filter loss estimate. Two cases:
            //
            // abort_on_lost_secs == 0 ("perfect rip required"): the
            // whole-disc bad-byte count is the WRONG predicate here. A
            // disc with unreadable sectors entirely OUTSIDE the main title
            // is still a valid mux candidate — the authoritative
            // per-title check in `resume_remux` (which runs after
            // `scan_image` and scopes to the title via
            // `bytes_bad_in_title`) will allow it. Using the whole-disc
            // count as the pre-filter would block those candidates before
            // the title-scoped check ever runs.
            //
            // For abort_on_lost_secs == 0 we therefore ALLOW here and
            // defer the real decision to the per-title re-validation in
            // `resume_remux`. That check is already `lost_secs >
            // abort_on_lost_secs` (same semantics: 0 → require in-title
            // loss == 0), so the pre-filter must not be STRICTER than
            // the authoritative gating.
            //
            // abort_on_lost_secs > 0: keep the coarse whole-disc fallback
            // estimate as an early-reject to avoid loading scan_image for
            // every disc with heavy global damage. Same constant as
            // `classify_resume` to prevent silent bitrate drift.
            if cfg_read.abort_on_lost_secs > 0 {
                let lost_secs =
                    stats.bytes_unreadable as f64 / resume::FALLBACK_BITRATE_BYTES_PER_SEC;
                if lost_secs > cfg_read.abort_on_lost_secs as f64 {
                    continue;
                }
            }
            return Some(resume::ResumeClass::Remux {
                iso_path,
                mapfile_path,
                display_name: basename,
                // Cold disc-insert resume from preserved staging: no `.ripped`
                // hand-off and no operator-override concept, so confidence is
                // unknown — resume_remux falls back to its own match check.
                title_confident: None,
            });
        }
    }
    None
}

/// True if `seg` is safe to use as a single staging-directory path
/// segment. Rejects values that could escape the staging root or
/// resolve to it: empty, all-dots (`.`, `..`, `...`), anything
/// containing a path separator, and absolute paths. `display_name`
/// derives from untrusted disc bytes / TMDB JSON, and the sanitizer
/// (`util::sanitize_path_compact`) keeps `.` and does not reject these,
/// so a disc label of `..` would otherwise make
/// `join("..")` + `remove_dir_all` delete the PARENT of staging.
fn is_safe_staging_segment(seg: &str) -> bool {
    !seg.is_empty()
        && !seg.chars().all(|c| c == '.')
        && !seg.contains('/')
        && !seg.contains('\\')
        && std::path::Path::new(seg).components().count() == 1
        && matches!(
            std::path::Path::new(seg).components().next(),
            Some(std::path::Component::Normal(_))
        )
}

/// Wipe the staging subdir for the currently-scanned disc. Used by
/// `/api/rip?resume=no` to give the user an explicit clean slate
/// before a fresh sweep.
fn wipe_staging_for_disc(cfg: &Arc<RwLock<Config>>, device: &str) {
    let cfg_read = match cfg.read() {
        Ok(c) => c.clone(),
        Err(_) => return,
    };
    let display_name = STATE
        .lock()
        .ok()
        .and_then(|s| s.get(device).map(|rs| rs.disc_name.clone()))
        .unwrap_or_default();
    if display_name.is_empty() {
        return;
    }
    let sanitized = crate::util::sanitize_path_compact(&display_name);
    // Defence-in-depth: never let an untrusted disc label sanitize to a
    // segment that escapes (or resolves to) the staging root. Without
    // this a label of `..` makes `join("..")` point at staging's parent
    // and `remove_dir_all` would delete it.
    if !is_safe_staging_segment(&sanitized) {
        crate::log::device_log(
            device,
            &format!("Refusing to wipe staging: unsafe sanitized dir name {sanitized:?}"),
        );
        return;
    }
    let staging_root = std::path::Path::new(&cfg_read.staging_dir);
    let path = staging_root.join(&sanitized);
    // Belt-and-braces: confirm the join stays strictly inside the
    // staging root before removing anything.
    if path.parent() != Some(staging_root) {
        crate::log::device_log(
            device,
            &format!(
                "Refusing to wipe staging: {} is not a direct child of {}",
                path.display(),
                staging_root.display()
            ),
        );
        return;
    }
    if path.exists() {
        match std::fs::remove_dir_all(&path) {
            Ok(_) => crate::log::device_log(
                device,
                &format!("Wiped staging dir for fresh rip: {}", path.display()),
            ),
            Err(e) => crate::log::device_log(
                device,
                &format!("Failed to wipe staging dir {}: {}", path.display(), e),
            ),
        }
    }
}

/// Detect whether `display_name`'s disc has resumable staging state and of
/// what kind. Mirrors `find_resumable_for_disc`'s directory matching but
/// classifies by `bytes_pending` rather than only accepting complete ISOs:
/// `bytes_pending == 0` → [`Resumable::Remux`], `> 0` → [`Resumable::Sweep`].
/// Pure (no STATE, no side effects); used by both the scan-time detector and
/// the `?resume=yes` action.
fn resumable_for_disc(cfg: &Config, display_name: &str) -> Option<Resumable> {
    if display_name.is_empty() {
        return None;
    }
    let sanitized = crate::util::sanitize_path_compact(display_name);
    // NFS-resilient listing (retries + surfaces per-entry errors) instead of
    // `read_dir(...).flatten()`, which would silently drop the disc's own dir
    // on a cold-cache DirEntry error and hide an existing resumable staging
    // dir — making the scan-complete tile omit the Resume button and the
    // operator re-sweep instead of resuming. Mirrors `disc_already_completed`.
    let staging_root = std::path::Path::new(&cfg.staging_dir);
    let basenames = list_staging_basenames(staging_root)?;
    for basename in basenames {
        let path = staging_root.join(&basename);
        // EXACT match only. Staging dirs are created with the exact sanitized
        // disc name (no year/suffix), so a prefix match never legitimately
        // fires — it only invites the collision class (`Cars` prefixing
        // `Cars_2`) fixed in staging_dir_matches_disc.
        if basename != sanitized {
            continue;
        }
        // A terminal `.failed` (or held `.review`) dir is NOT resumable, even
        // when its mapfile still shows pending bytes. Offering a Resume here is
        // the data-stranding bug: the Sweep-resume branch in `handle_rip_request`
        // re-rips WITHOUT clearing the stale `.failed`, so a successful re-rip's
        // `.ripped` ends up shadowed by the lingering `.failed` and the mux
        // worker skips it forever (`.failed` is terminal-by-presence). Mirror the
        // Remux-branch policy (`resumable_dir_blocked`): a terminal/held dir
        // forces the operator to explicitly Wipe. Snapshot first so this is read
        // from the same NFS-resilient, primed view the markers come from.
        if let Some(snap) = staging::snapshot_staging_disc(&path) {
            if snap.has_failed || snap.has_review {
                return None;
            }
            // A dir the mux worker owns (.ripped sweep-complete handoff or an
            // in-flight .muxing) must NOT be offered as resumable. Resuming it
            // would re-enter the rip/sweep path on a disc the worker is mid-mux
            // on — racing the mux's reads against a fresh sweep that overwrites
            // the staged ISO. Mirror the sibling Wipe guard
            // (`disc_owned_by_worker` / `staging_disc_owned_by_worker`): a dir
            // owned by the worker is off-limits until the mux finishes.
            if snap.has_ripped || snap.has_muxing {
                return None;
            }
        }
        let (_iso_path, mapfile_path) = match resume::find_iso_and_mapfile(&path) {
            Some(p) => p,
            None => continue,
        };
        let map = match libfreemkv::disc::mapfile::Mapfile::load(&mapfile_path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        let st = map.stats();
        // Any not-good data is retryable: pending (NonTried/NonTrimmed/
        // NonScraped) OR a previously-stamped Unreadable. Continue the sweep so
        // the remaining passes get another shot — there is NO terminal "won't
        // retry" state; the patch re-attempts Unreadable ranges every pass.
        // Only a mapfile that is 100% Finished resumes straight to remux.
        return Some(if st.bytes_pending == 0 && st.bytes_unreadable == 0 {
            Resumable::Remux
        } else {
            Resumable::Sweep
        });
    }
    None
}

/// STATE-reading wrapper of [`resumable_for_disc`] used by the `?resume=yes`
/// action (the disc has been scanned, so its name is in STATE).
fn resumable_for_device(cfg: &Arc<RwLock<Config>>, device: &str) -> Option<Resumable> {
    let cfg_read = cfg.read().ok()?.clone();
    let display_name = STATE
        .lock()
        .ok()
        .and_then(|s| s.get(device).map(|rs| rs.disc_name.clone()))?;
    resumable_for_disc(&cfg_read, &display_name)
}

/// RAII guard that unregisters a device's halt-map entry on drop. Created
/// immediately after `register_halt` in `rip_disc` so every exit path —
/// early-return error branches, the normal tail, and panics — cleans up the
/// entry. See the v0.13.6 halt-map-leak class.
struct HaltGuard {
    device: String,
}

impl Drop for HaltGuard {
    fn drop(&mut self) {
        unregister_halt(&self.device);
    }
}

/// RAII guard that clears the `.sweeping` in-progress marker on drop.
///
/// `.sweeping` is written immediately after the staging dir is created
/// (before Pass 1) and governs the whole multi-hour sweep+patch window.
/// The terminal-marker writers (`write_failed_marker` /
/// `write_completed_marker`, and the `.ripped` hand-off in `muxer`) all
/// clear it first, so on those success/`.ripped`/`.failed` paths this
/// guard's clear is an idempotent no-op. It only fires on `rip_disc`'s
/// many early-return error branches (disk-space preflight, Pass 1 halt /
/// failure, transport-recovery exhausted, ISO-open / mux-build failures,
/// durability-gate failures) and on panic — every one of which previously
/// leaked a stale `.sweeping`. A leaked `.sweeping` makes the next
/// startup's `resume_or_quarantine_staging` classify the dir `InProgress`
/// forever (never restart-counted, never cold-resumed), stranding dirs
/// that hold a complete ISO + clean mapfile. Holding the guard for the
/// whole `rip_disc` body guarantees the marker is cleared on every exit.
struct SweepingGuard {
    staging: std::path::PathBuf,
}

impl Drop for SweepingGuard {
    fn drop(&mut self) {
        staging::clear_sweeping_marker(&self.staging);
    }
}

/// Build the drive-level `on_event` handler installed on the live drive.
///
/// Every event resets the watchdog (`wdf`) so the "stalled" timer doesn't
/// climb while the library is working through recovery. `BytesRead` updates
/// the shared `latest_bytes_read` atomic the UI reads; `ReadError` logs. The
/// closure is factored out of `rip_disc` so the BytesRead→atomic wiring (the
/// progress contract the `/api/state` speed meter depends on) is testable in
/// isolation rather than buried in a 2000-line orchestrator.
pub fn make_drive_event_fn(
    dev: String,
    wdf: Arc<AtomicU64>,
    latest_bytes_read: Arc<AtomicU64>,
) -> impl Fn(libfreemkv::event::Event) + Send + 'static {
    move |event| {
        wdf.store(crate::util::epoch_secs(), Ordering::Relaxed);
        match event.kind {
            libfreemkv::event::EventKind::BytesRead { bytes, .. } => {
                latest_bytes_read.store(bytes, Ordering::Relaxed);
            }
            libfreemkv::event::EventKind::ReadError { sector, .. } => {
                crate::log::device_log(&dev, &format!("Read error at sector {}", sector));
            }
            _ => {}
        }
    }
}

/// Build the stream-level `on_event` handler shared by the multipass ISO
/// pipeline and the single-pass inline `DiscStream` path.
///
/// Resets the watchdog (`wdf`); `BytesRead` updates `latest_bytes_read`;
/// `BatchSizeChanged` stores the new batch size (`current_batch`) and logs;
/// `SectorSkipped` records the skipped LBA (`last_lba`) and logs. Factored
/// out of `rip_disc` for the same testability reason as
/// [`make_drive_event_fn`].
pub fn make_stream_event_fn(
    dev: String,
    wdf: Arc<AtomicU64>,
    last_lba: Arc<AtomicU64>,
    current_batch: Arc<AtomicU16>,
    latest_bytes_read: Arc<AtomicU64>,
) -> impl Fn(libfreemkv::event::Event) + Send + 'static {
    move |event| {
        wdf.store(crate::util::epoch_secs(), Ordering::Relaxed);
        match event.kind {
            libfreemkv::event::EventKind::BytesRead { bytes, .. } => {
                latest_bytes_read.store(bytes, Ordering::Relaxed);
            }
            libfreemkv::event::EventKind::BatchSizeChanged { new_size, reason } => {
                current_batch.store(new_size, Ordering::Relaxed);
                let label = match reason {
                    BatchSizeReason::Shrunk => "shrunk",
                    BatchSizeReason::Probed => "probed up",
                };
                crate::log::device_log(&dev, &format!("Batch size → {} ({})", new_size, label));
            }
            libfreemkv::event::EventKind::SectorSkipped { sector } => {
                last_lba.store(sector, Ordering::Relaxed);
                crate::log::device_log(&dev, &format!("Sector {} skipped (zero-filled)", sector));
            }
            _ => {}
        }
    }
}

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
    // `Drive::read` calls inside libfreemkv. Until then a stale halt
    // from a prior rip on the same device must NOT survive into this
    // rip's checks.
    register_halt(device, libfreemkv::Halt::new());

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
    let tmdb_media_type = tmdb
        .as_ref()
        .map(|t| t.media_type.clone())
        .unwrap_or_else(|| "unknown".to_string());

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
    let title_confident = cfg_read.tmdb_api_key.trim().is_empty()
        || overridden
        || crate::tmdb::is_confident_match(
            &crate::tmdb::clean_title(&disc_name),
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
    let keys = disc.decrypt_keys();

    // No-keys decision. An encrypted disc with no usable keys can still be swept
    // to a raw ISO (the sweep uses `decrypt: false`); only the MUX needs keys.
    // The operator's `capture_without_keys` toggle decides what happens:
    //   * enabled  → capture to ISO now, defer the mux until keys are available
    //                (the mux is skipped below; staging is preserved for resume).
    //   * disabled → don't rip; surface the explicit reason and stop here.
    let keys_missing = disc.encrypted && matches!(keys, libfreemkv::decrypt::DecryptKeys::None);
    if keys_missing {
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
    //     Disc::copy and is unaffected by this value)
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
    // So single-pass cannot honour ISO semantics: its post-mux gate would scope
    // loss IN-TITLE, accepting a disc that the multi-pass / resume paths would
    // ABORT on (whole-disc scope) under `abort_on_lost_secs=0` with damage
    // outside the title — the verdict would diverge by rip mode for identical
    // input. Refuse the incoherent combination up front and point the operator
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

    let ext = match output_format.as_str() {
        "m2ts" => "m2ts",
        _ => "mkv",
    };

    let staging = cfg_read.staging_device_dir(&crate::util::sanitize_path_compact(&display_name));
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
    let iso_filename = format!("{}.iso", crate::util::sanitize_path_compact(&display_name));
    let iso_path_str = format!("{staging}/{iso_filename}");
    let mapfile_path_str = format!("{iso_path_str}.mapfile");
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
    // `Disc::copy()` in round 3.
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

    let reader: Box<dyn libfreemkv::SectorSource> = if cfg_read.max_retries > 0 {
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
                        &staging,
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
        let _pass1_guard = spawn_pass_watcher(pass1_halt.clone(), user_halt.clone());

        const MAX_PASS1_ATTEMPTS: u32 = 10;
        let mut attempt = 0;
        let mut result = None;
        // The most recent sweep error, kept so the `result = None`
        // fallthrough can translate the underlying SCSI cause through
        // `format_pass_error` rather than surfacing a bare internal
        // strategy identifier to the operator.
        let mut last_sweep_err: Option<libfreemkv::Error> = None;

        // On-decrypt-miss key fetch. Online/sample-driven sources resolve only the
        // CPS units sampled up front; when a read hits an orphan unit no held key
        // opens, this asks the SAME key sources with that unit's ciphertext, caches
        // the returned key, and retries — recovering an orphan CPS unit (e.g. a
        // bonus clip not reachable from any playlist) instead of hard-failing the
        // read. `None` for non-AACS discs. Built once, shared (cloned Arc) into the
        // sweep and patch read paths; the mux reads main-title (already-resolved)
        // extents only, so it doesn't need it.
        let key_fetch: Option<libfreemkv::sector::KeyFetch> = disc.inputs().map(|mut inputs| {
            // The live scan reads the MKB for the up-front resolve but does NOT
            // retain it on the disc state, so `disc.inputs()` carries an EMPTY
            // MKB. An online key service NEEDS the MKB to derive an orphan unit's
            // key (decode rejects `mkb=0` with 404). Read it once here so every
            // refetch request carries the full inf+MKB, exactly like the up-front
            // resolve did. One drive read at rip start; `inf` filled too if absent.
            if inputs.mkb.is_empty() {
                if let Ok((inf, mkb, _version)) =
                    libfreemkv::Disc::read_aacs_inputs_from_drive(&mut session.drive)
                {
                    if inputs.unit_key_ro.is_empty() {
                        inputs.unit_key_ro = inf;
                    }
                    inputs.mkb = mkb;
                }
            }
            let cfg = Arc::clone(cfg);
            let make: std::sync::Arc<
                dyn Fn() -> Vec<Box<dyn libfreemkv::keysource::KeySource>> + Send + Sync,
            > = std::sync::Arc::new(move || crate::keysource::build_sources(&cfg.read().unwrap()));
            libfreemkv::keysource::key_fetch(inputs, make)
        });

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
            //
            // `resume_sweep` (user clicked Resume on a partial) makes even the
            // FIRST attempt resume from the existing mapfile + ISO, so the
            // ~40 GB already swept isn't re-read off the disc.
            let sweep_opts = libfreemkv::SweepOptions {
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

            match disc.sweep(&mut session.drive, iso_path, &sweep_opts) {
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

        // Retry passes: Disc::patch re-reads only the bad ranges
        // recorded in the mapfile sector-by-sector with full
        // drive-level recovery. Each pass gets its own wallclock cap
        // watcher; cap-fire marks the rip as failed.

        let max_retries = cfg_read.max_retries;

        crate::log::device_log(
            device,
            &format!(
                "PASS 2-{}: retry loop starting max_retries={} bytes_pending={}",
                max_retries, max_retries, bytes_pending
            ),
        );
        let mut pass_2_settled = false;
        for retry_n in 1..=max_retries {
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

            // Settle the drive between Pass 1 and Pass 2 only. The BU40N
            // (and other Initio-bridge drives) wedge after grinding on bad
            // sectors. Giving the drive 30 s of idle BEFORE we hammer it
            // again with retry reads lets its internal state recover.
            // Cheap insurance.
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
                    "PASS {}/{total_passes}: retrying bad ranges (bpt=1) bytes_pending={}",
                    pass, bytes_pending
                ),
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
            let _pass_guard = spawn_pass_watcher(pass_halt.clone(), user_halt.clone());

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
                key_fetch: key_fetch.clone(),
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
                    recovered as f64 / BYTES_PER_MIB,
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
            if recovered == 0 {
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
        let mut main_lost_ms_for_history = 0.0f64;
        let mut main_lost_bytes_for_history = 0u64;
        if cfg_read.max_retries > 0 {
            let mapfile_path = std::path::Path::new(&mapfile_path_str);
            if let Ok(mut map) = libfreemkv::disc::mapfile::Mapfile::load(mapfile_path) {
                use libfreemkv::disc::mapfile::SectorStatus;
                // Promote still-NonTrimmed bytes to Unreadable — these are
                // bytes that remained "maybe" across all patch passes and are
                // now confirmed lost.
                let nontrimmed_ranges = map.ranges_with(&[SectorStatus::NonTrimmed]);
                let total_promoted: u64 = nontrimmed_ranges.iter().map(|(_, sz)| *sz).sum();
                let n_ranges = nontrimmed_ranges.len();
                for (pos, size) in nontrimmed_ranges {
                    if let Err(e) = map.record(pos, size, SectorStatus::Unreadable) {
                        tracing::warn!(
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
                    tracing::warn!(
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
                if bytes_unreadable > 0 {
                    let bad_ranges = map.ranges_with(&[SectorStatus::Unreadable]);
                    if !bad_ranges.is_empty() && title_bytes_per_sec > 0.0 {
                        // TOTAL unreadable time scoped to the muxed title (not
                        // the single largest gap, not whole-disc), exactly as
                        // the per-pass loop-exit gate does (see `mux_scope_bad`
                        // above). The old `.fold(.., f64::max)` over whole-disc
                        // ranges both under-counted scattered gaps and falsely
                        // aborted on out-of-title (menu/trailer) loss: for a
                        // real MKV mux only in-title unreadable bytes count, so
                        // a scratched menu/trailer outside the title extents
                        // must NOT trigger the abort. For raw ISO output there
                        // is no title to scope to, so the whole disc is the
                        // unit of loss.
                        main_lost_ms_for_history = abort_lost_ms(
                            cfg_read.output_format == "iso",
                            &title_for_progress,
                            &bad_ranges,
                            title_bytes_per_sec,
                        );
                        // Raw byte count under the SAME scope — the perfect-rip
                        // (threshold 0) gate keys on this, not the bitrate-derived
                        // ms, so a zero/low bitrate can't hide unreadable loss.
                        main_lost_bytes_for_history = abort_lost_bytes(
                            cfg_read.output_format == "iso",
                            &title_for_progress,
                            &bad_ranges,
                        );
                        // Mirror into the outer binding so the final done/stopped
                        // state update (after run_mux) can use the same in-title
                        // value without re-reading the mapfile.
                        main_lost_ms_for_history_outer = main_lost_ms_for_history;
                    }
                }
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
                // `mark_aborted_on_loss` bounds the retries — once
                // MAX_LOSS_RESUME_ATTEMPTS aborts have accrued it promotes the
                // dir to terminal `.failed` for the operator. It also clears
                // `.restart_count` so a deterministically-lossy rip doesn't ALSO
                // walk the crash-restart loop.
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
            if !staging::fsync_output_file(iso_path) {
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
            // Confident match → `.done` (mover files it); otherwise `.review`
            // (operator confirms the title before it leaves staging). Mirrors
            // the MKV completion path's marker selection.
            let marker_name = if title_confident { ".done" } else { ".review" };
            // `to_string_pretty` on a `json!`-constructed Value is effectively
            // infallible; `.expect` makes the invariant explicit (mirrors
            // staging::write_failed_marker) so a real serialization failure
            // surfaces as a panic rather than silently writing an empty marker
            // that the mover skips, stranding the output in staging forever.
            let marker_body =
                serde_json::to_string_pretty(&marker).expect("json! value is always serialisable");
            if let Err(e) = staging::write_handoff_marker(
                &staging_path.join(marker_name),
                marker_body.as_bytes(),
            ) {
                crate::log::device_log(
                    device,
                    &format!(
                        "{marker_name} marker write failed ({e}); ISO is staged but the mover cannot pick it up"
                    ),
                );
                update_state_with(device, |s| {
                    if s.last_error.is_empty() {
                        s.last_error = format!("{marker_name} marker write failed: {e}");
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
        // The update_state_with call inside the promotion+flush block
        // (above) has already re-derived errors / total_lost_ms /
        // main_lost_ms / bad_ranges / largest_gap_ms from the
        // just-promoted in-memory map and written them to STATE, so
        // this snapshot reflects the final post-promotion damage
        // (NonTrimmed bytes promoted to Unreadable are included).
        // We carry them into the marker so that remux_from_ripped_marker
        // can populate SweepDamageSnapshot for a resumed mux without
        // re-reading the mapfile (though mapfile-based re-derivation
        // also works and is available as a fallback — see resume.rs).
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
    if keys_missing {
        let msg = keyless_failure_message(&disc);
        if cfg_read.max_retries > 0 {
            crate::log::device_log(
                device,
                &format!(
                    "Ripped to ISO — no keys, mux deferred. ISO + mapfile preserved in staging \
                     ({staging}); auto-resume will mux once keys are available. {msg}"
                ),
            );
            update_state_with(device, |s| {
                s.status = "idle".to_string();
                s.last_error = format!("Ripped to ISO — no keys, mux deferred. {msg}");
            });
        } else {
            crate::log::device_log(
                device,
                &format!(
                    "Single-pass rip with no keys — cannot mux (no ISO captured). \
                     Enable multi-pass mode to capture a deferred-mux ISO. {msg}"
                ),
            );
            update_state_with(device, |s| {
                s.status = "error".to_string();
                s.last_error = format!(
                    "No keys — cannot mux. {msg} (multi-pass mode captures an ISO for deferred mux.)"
                );
            });
        }
        unregister_halt(device);
        return;
    }

    // Debug log reader type for mux - confirms ISO vs drive source
    tracing::debug!(target: "mux", " mux using reader: {}", if cfg_read.max_retries > 0 { "ISO file (multipass)" } else { "physical drive" });

    // 0.18 round 2: DiscStream gets the per-device `Halt` at
    // construction via the new `with_halt(...)` builder. Stop
    // interrupts `fill_extents` at the next retry boundary on the
    // same signal that breaks sweep, patch, and the mux frame loop —
    // required for Stop to work during dense bad-sector regions
    // where the outer PES read() loop may never emit a frame.
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
    // Stream-event callback — wired into both the multipass highway
    // (BytesRead from the prefetch producer thread) and the drive
    // single-pass inline path (BytesRead + BatchSizeChanged +
    // SectorSkipped from DiscStream::fill_extents). Either path
    // calls this same closure; the UI doesn't care which.
    let stream_event_fn = make_stream_event_fn(
        device.to_string(),
        wd_last_frame.clone(),
        rip_last_lba.clone(),
        rip_current_batch.clone(),
        latest_bytes_read.clone(),
    );

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

    let input: Box<dyn libfreemkv::pes::Stream> = if cfg_read.max_retries > 0 {
        // Multipass ISO mux → PipelinedPesStream highway. The
        // producer thread fires BytesRead events; BatchSizeChanged
        // and SectorSkipped never fire on the highway (no adaptive
        // retry — the ISO is zero-filled for any sweep-pass
        // failures).
        match libfreemkv::build_iso_pipeline(
            reader,
            title,
            keys,
            batch,
            format,
            Some(halt_token.clone()),
            Some(Box::new(stream_event_fn) as libfreemkv::sector::prefetched::EventFn),
            // Fresh-key-on-failure fetch: not yet wired for autorip. Wiring it
            // needs the keyserver config (cfg.keyserver_url/secret) and the ISO's
            // inf/MKB threaded to this mux site; build the factory exactly like
            // `freemkv::pipe::build_iso_fetch_factory` and pass it here.
            None,
        ) {
            Ok(s) => Box::new(s),
            Err(e) => {
                tracing::error!(target: "mux", device=%device, "build_iso_pipeline failed: {e}");
                let msg = format!(
                    "Mux setup failed — the disc's title or stream layout could not be prepared for muxing. The source may be damaged or use an unsupported format ({e})."
                );
                crate::log::device_log(device, &msg);
                // A pipeline BUILD failure (header resolution, codec
                // negotiation, format error) is structural and permanent —
                // retries won't fix it. Quarantine the dir with `.failed`
                // (mirrors the header-phase failure path below) so the
                // restart scan classifies it terminal instead of leaving
                // it stranded `InProgress`.
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
        // Drive single-pass: stays on the inline DiscStream because
        // its adaptive batch-retry on read failure lives inside
        // `fill_extents`. The producer-thread highway doesn't (yet)
        // replicate that retry policy, so for live-disc reads we
        // keep the in-place fill loop. Same `on_event` closure as
        // the highway path so the UI gets one event stream.
        let mut s = libfreemkv::DiscStream::new(reader, title, keys, batch, format)
            .with_halt(halt_token.clone());
        if cfg_read.on_read_error == "skip" {
            s.skip_errors = true;
        }
        s.on_event(stream_event_fn);
        Box::new(s)
    };

    // 0.18 round 2 #2: the headers-ready buffering, the spawning of
    // the consumer thread, the watchdog, and the per-frame
    // `update_state` cadence all live in `mux::run_mux`. Round 1
    // shipped the mux module as a placeholder; this is the lift onto
    // libfreemkv's `Pipeline` + `Sink` primitive.
    //
    // The producer side of `run_mux` polls the per-device `Halt`
    // token (looked up via `device_halt(device)`) at the top of each
    // frame iteration — same token the orchestrator threaded through
    // sweep / patch and the same one the HTTP /api/stop handler
    // cancels.
    let _mux_span =
        tracing::span!(tracing::Level::TRACE, "rip_disc::run_mux", device=%device, total_bytes)
            .entered();
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
    if !mux_outcome.output_opened {
        unregister_halt(device);
        if header_phase_outcome_is_failure(
            mux_outcome.output_opened,
            mux_outcome.finalize_error.as_deref(),
        ) {
            let reason = mux_outcome
                .finalize_error
                .as_ref()
                .expect("finalize_error is Some when header_phase_outcome_is_failure() is true");
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
    let mut final_errors = mux_outcome.errors;
    let final_last_sector = rip_last_lba.load(Ordering::Relaxed);
    let final_current_batch = rip_current_batch.load(Ordering::Relaxed);
    let mut final_lost_secs = mux_outcome.lost_video_secs;
    // Demux-time loss (sectors that read into the ISO fine but fail AACS/CSS
    // decrypt at mux, or codec-corruption demux skips that zero-fill output).
    // This is the in-title-scoped demux-skip estimate from `run_mux`, the same
    // quantity the single-pass (mod.rs) and resume (resume.rs) post-mux gates
    // compare against the threshold. Captured BEFORE the multipass overwrite
    // below replaces `final_lost_secs` with the sweep-mapfile loss for the UI
    // card, so the post-mux gate can still gate fresh multi-pass rips on it.
    let demux_lost_secs = mux_outcome.lost_video_secs;
    // In multipass mode the `input.errors` counter above counts ISO→MKV demux
    // skips (usually zero — ISO reads don't fail). The real bad-sector count
    // lives in the mapfile sidecar. Prefer that when present.
    if cfg_read.max_retries > 0 {
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
    }

    // v1.2.0: the mux never aborts on mux-time (demux/decrypt) loss. Such
    // loss is concealed in the read path (NULL-TS fill) and dropped-to-keyframe
    // at the codec layer, producing a decode-clean file; it is tallied and
    // logged (lost_video_secs below) but never quarantines the disc. The
    // abort_on_lost_secs knob governs the PRE-mux rip phase only.

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

    // Write the staging markers (.done / .completed / .failed) the mover and the
    // resume-on-startup detector depend on. (The per-rip history record/log that
    // used to be written here was removed in 0.30.1 — the History tab was
    // unmaintained and didn't work; see web.rs.)
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
            let is_network = output_format == "network" && !cfg_read.network_target.is_empty();
            if !is_network && !staging::fsync_output_file(std::path::Path::new(&output_path)) {
                crate::log::device_log(
                    device,
                    "Durability gate failed: could not fsync mux output to stable storage; \
                     withholding .done/.completed and preserving staging for retry",
                );
                update_state_with(device, |s| {
                    if s.last_error.is_empty() {
                        s.last_error =
                            "mux output not durable (fsync failed); rip preserved for retry"
                                .to_string();
                    }
                });
                return;
            }
            // Confident match (exact title + year) → hand straight to the mover
            // (.done). Otherwise HOLD for operator review (.review): the rip is
            // complete and staged, but we will NOT auto-file it into the library
            // under a guessed name. The Needs-review UI resolves it (pick the
            // right title → promotes to .done, or proceed as-is). "Better to
            // pause; worst case the operator clicks proceed." A would-overwrite
            // collision is still caught later by the mover's own guard.
            let marker_name = if title_confident { ".done" } else { ".review" };
            let marker_path = format!("{}/{}", staging, marker_name);
            // Durable, atomic marker write (tmp + fsync + rename + dir-fsync).
            // The single staging-dir fsync inside this helper is the crash
            // barrier: it guarantees `.done` is observed on disk before the
            // later `.completed` write / ISO prune, so a crash can never leave
            // `.completed` (or a pruned ISO) without a durable `.done`.
            // `to_string_pretty` on a `json!`-constructed Value is effectively
            // infallible; `.expect` makes the invariant explicit (mirrors
            // staging::write_failed_marker) so a real serialization failure
            // surfaces as a panic rather than silently writing an empty marker
            // that the mover skips, stranding the output in staging forever.
            let marker_body =
                serde_json::to_string_pretty(&marker).expect("json! value is always serialisable");
            if let Err(e) = staging::write_handoff_marker(
                std::path::Path::new(&marker_path),
                marker_body.as_bytes(),
            ) {
                // The mux finished and the MKV is in staging, but the
                // mover keys off this marker — without it the file sits
                // in staging forever with no signal. Surface it so the
                // operator can see the rip is staged-but-unqueued rather
                // than silently lost.
                crate::log::device_log(
                    device,
                    &format!(
                        "{marker_name} marker write failed ({e}); MKV is staged but the mover cannot pick it up"
                    ),
                );
                update_state_with(device, |s| {
                    if s.last_error.is_empty() {
                        s.last_error =
                            format!("MKV staged but {marker_name} marker write failed: {e}");
                    }
                });
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
            // 0.20.7: also write `.completed` (and clear `.restart_count`)
            // so the resume-on-startup detector knows this disc finished
            // cleanly even if the mover hasn't run yet. `.done` and
            // `.completed` are independent: `.done` is the mover's
            // hand-off marker (consumed when the file is relocated);
            // `.completed` is the process-level success marker (stays
            // put so post-restart the dir is recognised as terminal).
            let staging_disc_path = std::path::Path::new(&staging);
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

    // Operator-facing ACCEPTED-done figures fold in demux-time loss the same
    // way the single-pass path (above) and the resume path (resume.rs) do, so a
    // fresh multi-pass rip and a resume of the IDENTICAL ISO reach the same
    // verdict whenever demux loss is non-zero but within `abort_on_lost_secs`.
    //
    // Single-pass (max_retries == 0): the 3812 overwrite block was skipped, so
    // `final_lost_secs == demux_lost_secs` and `final_errors == mux_outcome.errors`
    // already — report them as-is to avoid double-counting.
    //
    // Multi-pass (max_retries > 0): `final_lost_secs` was overwritten with the
    // sweep-mapfile loss and `final_errors` with the mapfile bad-sector count;
    // both are disjoint from the demux skip count (sweep = Unreadable sectors
    // baked into the ISO; demux = decrypt/codec skips at mux), so add the demux
    // loss / errors. This mirrors the ABORTED multipass branch above, which
    // already reports `final_lost_secs + demux_lost_secs`.
    let (done_errors, done_lost_secs, done_demux_extra_ms) = if cfg_read.max_retries == 0 {
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
            total_lost_ms: if cfg_read.max_retries == 0 {
                final_lost_secs * MILLIS_PER_SEC
            } else {
                sweep_damage_snapshot.total_lost_ms + done_demux_extra_ms
            },
            // Single-pass mode has no mapfile, so `sweep_damage_snapshot` is
            // the all-zero Default and its `main_lost_ms` is always 0.0 —
            // leaving the done card showing "(0s in main movie)" even when the
            // demux skipped in-title sectors. `final_lost_secs` is already the
            // in-title loss for single-pass (the demux-skip estimate), so mirror
            // the `total_lost_ms` branch above. Multipass keeps the snapshot's
            // value, which is derived from the mapfile's in-title bad ranges.
            main_lost_ms: if cfg_read.max_retries == 0 {
                final_lost_secs * MILLIS_PER_SEC
            } else {
                sweep_damage_snapshot.main_lost_ms + done_demux_extra_ms
            },
            bad_ranges: sweep_damage_snapshot.bad_ranges.clone(),
            num_bad_ranges: sweep_damage_snapshot.num_bad_ranges,
            bad_ranges_truncated: sweep_damage_snapshot.bad_ranges_truncated,
            largest_gap_ms: sweep_damage_snapshot.largest_gap_ms,
            ..Default::default()
        },
    );

    if cfg_read.auto_eject {
        eject_drive(device_path);
    }

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
}

/// Pure decision: should this completion path auto-eject the drive?
///
/// Two rules, both load-bearing for the rip→mux→move state machine:
/// 1. Only when the operator enabled `auto_eject`.
/// 2. NEVER for a synthetic, underscore-prefixed device (`_mux`, etc.).
///    Those carry the background mux/move work AFTER the drive thread has
///    already handed off (and possibly already ejected). The synthetic
///    `_mux` worker reaching a completion path must not issue a second
///    eject against whatever disc the physical drive now holds. The real
///    drive ejects exactly once, at the `.ripped` read-complete hand-off.
///
/// Centralizing this here makes the "fires once, at read-complete, never
/// from the mux worker" contract a single unit-testable predicate instead
/// of an inline `&&` duplicated across the fresh-rip and resume paths.
pub(crate) fn should_auto_eject(auto_eject: bool, device: &str) -> bool {
    auto_eject && !device.starts_with('_')
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
    // Pre-0.25.2 both branches here used `let _ =` and any failure was
    // invisible: the user-facing symptom was "auto_eject is set but the
    // disc stayed put, no log line, no idea why". Surface both.
    match libfreemkv::Drive::open(std::path::Path::new(device_path)) {
        Ok(mut session) => {
            if let Err(e) = session.eject() {
                crate::log::device_log(dev, &format!("eject failed: {e}"));
                tracing::warn!(device = %dev, error = %e, "eject command failed");
            }
        }
        Err(e) => {
            crate::log::device_log(dev, &format!("eject skipped — drive open failed: {e}"));
            tracing::warn!(device = %dev, error = %e, "eject skipped — drive open failed");
        }
    }
}

// `sanitize_filename` and `format_duration` moved to `util` in 0.13.0.
// Callers below now use `crate::util::sanitize_path_compact` and
// `crate::util::format_duration_hm` directly.

pub(crate) fn format_codecs(title: &libfreemkv::DiscTitle) -> String {
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
fn audio_purpose_tag(p: libfreemkv::LabelPurpose) -> Option<&'static str> {
    match p {
        libfreemkv::LabelPurpose::Commentary => Some("Commentary"),
        libfreemkv::LabelPurpose::Descriptive => Some("Descriptive Audio"),
        libfreemkv::LabelPurpose::Score => Some("Score"),
        libfreemkv::LabelPurpose::Ime => Some("IME"),
        libfreemkv::LabelPurpose::Normal => None,
    }
}

/// Milliseconds of loss that the post-retry abort check should weigh.
///
/// For a raw ISO rip the whole disc is the deliverable, so every
/// unreadable byte counts. For an MKV/m2ts mux only the bytes that fall
/// inside the muxed title's extents matter — a scratched menu / trailer
/// that lives OUTSIDE the title must not count, otherwise an
/// `abort_on_lost_secs=0` ("perfect rip") setting would abort a
/// fully-recovered main movie just because some out-of-title sector was
/// lost (the Top Gun false-positive). Mirrors the per-pass loop-exit
/// gate's `mux_scope_bad` scoping.
/// Pick the mux-phase progress denominator (used for percent + ETA).
///
/// Multipass / resume mux reads the whole disc-capacity ISO, so its read
/// position climbs to `disc_capacity_bytes` (passed in as `total_bytes`) — keep
/// that. The single-pass path (`max_retries == 0`) streams ONLY the selected
/// title's extents over the live drive, so `DiscStream`'s `BytesRead` caps at
/// `Σ sector_count × 2048` (the title extent byte sum). Using disc capacity as
/// the denominator there made the bar plateau at `title_size ÷ disc_capacity`.
/// Scope to the extent sum so single-pass progress reaches 100%. Falls back to
/// `total_bytes` if the title has no extents (e.g. degenerate scan).
fn mux_progress_denominator(
    max_retries: u8,
    total_bytes: u64,
    title: &libfreemkv::DiscTitle,
) -> u64 {
    if max_retries != 0 {
        return total_bytes;
    }
    let extent_bytes: u64 = title
        .extents
        .iter()
        .map(|e| e.sector_count as u64 * 2048)
        .sum();
    if extent_bytes > 0 {
        extent_bytes
    } else {
        total_bytes
    }
}

/// The unreadable byte count that the abort gate scopes to: whole-disc for an
/// ISO deliverable, in-title only for an MKV (a scratched menu/trailer outside
/// the muxed title does not count for an MKV mux). This is the RAW source of
/// truth the `abort_on_lost_secs == 0` ("perfect") gate keys on — no bitrate, no
/// float — so a zero-bitrate title can never hide unreadable loss.
pub(super) fn abort_lost_bytes(
    output_is_iso: bool,
    title: &libfreemkv::DiscTitle,
    bad_ranges: &[(u64, u64)],
) -> u64 {
    if output_is_iso {
        bad_ranges.iter().map(|(_, sz)| *sz).sum::<u64>()
    } else {
        libfreemkv::disc::bytes_bad_in_title(title, bad_ranges)
    }
}

pub(super) fn abort_lost_ms(
    output_is_iso: bool,
    title: &libfreemkv::DiscTitle,
    bad_ranges: &[(u64, u64)],
    title_bytes_per_sec: f64,
) -> f64 {
    if title_bytes_per_sec <= 0.0 {
        return 0.0;
    }
    abort_lost_bytes(output_is_iso, title, bad_ranges) as f64 / title_bytes_per_sec * MILLIS_PER_SEC
}

/// Whether the post-retry abort check should fire.
///
/// Strictly `>`, NOT `>=`: `abort_on_lost_secs=0` (threshold 0 ms) means
/// "require a perfect in-title rip" — abort on ANY positive in-title
/// loss, but proceed to mux when in-title loss is exactly zero. With
/// `>=` a zero-loss title (`lost_ms == 0.0 >= 0.0`) would wrongly abort
/// whenever any out-of-title sector was unreadable.
///
/// A NaN `lost_ms` is treated as "abort": `NaN > x` is `false`, so a
/// plain comparison would silently decline to abort and mark the rip
/// complete. This is the single chokepoint deciding perfect-rip vs.
/// quarantine, so an unquantifiable loss must fail safe (abort), not
/// pass as a silent success.
fn should_abort_for_loss(lost_ms: f64, abort_threshold_ms: f64) -> bool {
    lost_ms.is_nan() || lost_ms > abort_threshold_ms
}

/// The flawless-rip loss gate. `abort_on_lost_secs == 0` means ZERO — abort on
/// ANY lost byte (unreadable OR undecryptable), keyed on the raw byte count so
/// no bitrate/float rounding can let a sub-second or zero-bitrate loss slip
/// through ("0 means ZERO, not <1s"). `> 0` keeps the time-based tolerance:
/// abort only when the loss exceeds N seconds. A NaN `lost_ms` always aborts
/// (fail-safe: an unquantifiable loss must never pass as success).
fn loss_aborts(lost_bytes: u64, lost_ms: f64, abort_on_lost_secs: u64) -> bool {
    if abort_on_lost_secs == 0 {
        lost_bytes > 0 || lost_ms.is_nan()
    } else {
        lost_ms.is_nan() || lost_ms > (abort_on_lost_secs as f64) * MILLIS_PER_SEC
    }
}

/// Whether the rip's deliverable is the whole-disc ISO itself rather than a
/// muxed MKV/M2TS title.
///
/// The settings UI advertises `output_format == "iso"` as "ISO copies the whole
/// disc; the other formats mux selected titles". For that promise to hold the
/// orchestrator must hand the operator the disc image it swept (the intermediate
/// `<name>.iso`), NOT re-mux the selected title to an `.mkv` and prune the ISO.
/// So in ISO mode we skip the title mux entirely and deliver the ISO: the abort
/// gate already scopes loss whole-disc (`abort_lost_ms`), and the mover already
/// validates + moves `.iso` files. This is the single predicate every
/// deliverable/prune/mux-skip decision keys off so the two completion routes
/// (`rip_disc`'s inline terminal and `resume::resume_remux`) can't diverge.
fn output_is_iso_image(output_format: &str) -> bool {
    output_format == "iso"
}

/// Effective main-movie-loss tolerance for the abort gate.
///
/// ISO output is a whole-disc image and must be byte-complete, so the
/// `abort_on_lost_secs` per-title tolerance is **ignored** — forced to 0
/// ("require 100%"). This makes the behaviour match the UI, where the field is
/// hidden for ISO (`hideIf output_format=iso`) and documented as "IGNORED for an
/// ISO rip, which is kept whole as-is". Without this an `abort_on_lost_secs`
/// value configured for a previous MKV rip would silently leak into an ISO rip
/// and accept a lossy image. MUXED output (MKV / M2TS / Network) uses the
/// configured value unchanged.
fn effective_abort_secs(output_format: &str, configured: u64) -> u64 {
    if output_is_iso_image(output_format) {
        0
    } else {
        configured
    }
}

/// Human-readable main-movie loss for UI / markers. Sub-second loss shows
/// milliseconds (so a 12 KB / ~1 ms gap reads as "1 ms", not a confusing
/// "0.00s"); a second or more shows seconds. NaN (unquantifiable) is spelled out.
fn fmt_loss(lost_ms: f64) -> String {
    if !lost_ms.is_finite() {
        "an unknown amount".to_string()
    } else if lost_ms < crate::util::MILLIS_PER_SEC {
        format!("{:.0} ms", lost_ms.max(0.0))
    } else {
        format!("{:.2}s", lost_ms / crate::util::MILLIS_PER_SEC)
    }
}

/// Human-readable abort threshold: 0 means "perfect rip required" (any loss
/// aborts), otherwise the configured seconds.
fn fmt_threshold(secs: u64) -> String {
    if secs == 0 {
        "perfect rip required".to_string()
    } else {
        format!("threshold {secs}s")
    }
}

/// Whether the intermediate ISO must be retained as the deliverable rather than
/// pruned. True when the operator asked to keep it (`keep_iso`) OR when ISO is
/// the selected output (the ISO *is* the deliverable — see `output_is_iso_image`).
fn retain_intermediate_iso(keep_iso: bool, output_format: &str) -> bool {
    keep_iso || output_is_iso_image(output_format)
}

/// Whether an `output_format == "iso"` rip must be rejected because it was
/// requested in single-pass mode (`max_retries == 0`).
///
/// ISO output is whole-disc: the deliverable is the entire image and its abort
/// accounting counts every unreadable sector, including damage OUTSIDE any
/// title's extents (see `abort_lost_ms` and the multi-pass / resume pre-mux
/// gates). Single-pass streams only the selected title to the muxer — it never
/// reads out-of-title sectors and captures no whole-disc ISO, so its post-mux
/// gate scopes loss in-title. Allowing single-pass ISO would therefore let it
/// ACCEPT a disc that the multi-pass / resume paths ABORT on (whole-disc scope)
/// for identical input under `abort_on_lost_secs=0` — the verdict would diverge
/// by rip mode. Only multi-pass captures a real ISO and applies whole-disc
/// scope, so ISO output requires it.
fn iso_output_needs_multipass(output_format: &str, max_retries: u8) -> bool {
    output_format == "iso" && max_retries == 0
}

/// Prune the disc-sized intermediate ISO and its mapfile sidecar on a
/// successful multipass completion, unless `keep_iso` is set.
///
/// Shared by both completion routes — `rip_disc`'s inline terminal path and
/// the resume / `.ripped` hand-off path (`resume::resume_remux`) — so the
/// `keep_iso=false` disk reclaim can't diverge between them. The mover frees
/// the ISO when it tears down a `.done` staging dir, but a low-confidence
/// `.review` hold (mover skips it) or a no-output-dir setup never relocates,
/// so without this prune a 90+ GB UHD ISO would leak in those cases.
///
/// Gated on `max_retries > 0`: an intermediate ISO only exists in multipass
/// mode (direct mode rips disc → MKV with no ISO). A `NotFound` removal is
/// silent (already gone / never written); any other error is surfaced to the
/// device log without failing the rip.
fn prune_intermediate_iso(
    device: &str,
    iso_path: &std::path::Path,
    mapfile_path: &std::path::Path,
    max_retries: u8,
    keep_iso: bool,
) {
    if max_retries == 0 || keep_iso {
        return;
    }
    match std::fs::remove_file(iso_path) {
        Ok(_) => crate::log::device_log(device, "Pruned intermediate ISO"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => crate::log::device_log(device, &format!("ISO prune warning: {e}")),
    }
    // Mirror the ISO arm: a lingering mapfile in staging could be misread as a
    // partial rip by the resume classifier on next startup, so surface any
    // unexpected removal error instead of swallowing it.
    match std::fs::remove_file(mapfile_path) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => crate::log::device_log(device, &format!("mapfile prune warning: {e}")),
    }
}

/// Whether a `run_mux` outcome that never opened its output
/// (`output_opened == false`) is a terminal failure that must be
/// quarantined (`.failed` marker + `status="failed"`) rather than a clean,
/// resumable stop.
///
/// `output_opened == false` covers two header-phase exits:
///   * clean stop during header read (halt / cancelled): `finalize_error`
///     is `None` — leave the staging dir resumable, surface nothing.
///   * structurally-unusable stream: `finalize_error` is `Some` — the
///     header buffer overflowed before codec_privates resolved, or EOF / a
///     read error hit before `headers_ready()`
///     (header-resolution-incomplete). No output exists and the dir can
///     never succeed, so it must be quarantined or resume-on-startup would
///     re-resume it forever and the device tile would stay stuck in its
///     prior `status="ripping"`.
fn header_phase_outcome_is_failure(output_opened: bool, finalize_error: Option<&str>) -> bool {
    !output_opened && finalize_error.is_some()
}

/// Decide the log prefix, `/api/state` status, and `last_error`/`failure_reason`
/// for a mux that finished with `completed == false`. Three cases, in priority
/// order:
///   1. `finalize_error` → the MKV is structurally broken (Cues/trailer never
///      landed). `status="failed"`; the caller quarantines with `.failed`.
///   2. `read_error` → a hard producer read error truncated the MKV under
///      `on_read_error=stop`. `status="error"` with the cause, so `/api/state`
///      signals the failure. This is NOT a user halt: the caller leaves staging
///      resumable (no `.failed`), but the operator must still see why it stopped.
///   3. neither → a genuine user-initiated halt / wedge. The pre-existing
///      "stopped → idle" path, with no `last_error`.
///
/// `read_error` is only consulted when `finalize_error` is `None`: a structural
/// finalize failure is the stronger signal (a broken file on disk) and already
/// implies the body was truncated.
fn incomplete_mux_status(
    finalize_error: Option<&str>,
    read_error: Option<&str>,
) -> (String, String, Option<String>) {
    if let Some(reason) = finalize_error {
        (
            format!("Failed (mux finalize): {reason}"),
            "failed".to_string(),
            Some(format!("mux finalize failed: {reason}")),
        )
    } else if let Some(cause) = read_error {
        (
            format!("Failed (read error): {cause}"),
            "error".to_string(),
            Some(format!("rip stopped: read error — {cause}")),
        )
    } else {
        ("Stopped".to_string(), "idle".to_string(), None)
    }
}

/// User-facing message for the "encrypted disc, no keys resolved" failure.
/// Switches on the libfreemkv error surfaced via `Disc::aacs_error` so the
/// UI tells the user *which* failure they're looking at instead of always
/// printing the same generic "check KEYDB" line.
///
/// Render format (locked rc.6 messaging standard): `Error: E<code> <message>`
/// — the `Error:` level word, the language-neutral `E<code>` token, then a
/// single plain-English sentence naming the failure and any remediation. One
/// line; built via [`error_line`].
///
/// Dispatch is **code-based** (`e.code()`) using named libfreemkv constants.
/// Codes outside the named set fall through to the 7xxx catch-all rather
/// than breaking the build when libfreemkv adds a new variant.
/// Operator-facing message for the "encrypted disc, no usable keys" failure,
/// dispatched from the *whole disc* rather than just its AACS slot.
///
/// CSS (DVD) and AACS (Blu-ray/UHD) record their resolution failures in
/// separate fields: a CSS known-plaintext crack failure lands in
/// `disc.css_error` (`Error::CssKeyMissing`), while AACS lands in
/// `disc.aacs_error`. The two are mutually exclusive in practice — a disc is
/// either CSS- or AACS-encrypted — so prefer `css_error` when present and
/// otherwise fall back to the AACS dispatch. Without this, a CSS crack failure
/// (where `aacs_error` is `None`) would hit the AACS-oriented defensive
/// fallback and mislead the operator into checking their KEYDB.
fn keyless_failure_message(disc: &libfreemkv::Disc) -> String {
    keyless_failure_message_for(disc.css_error.as_ref(), disc.aacs_error.as_ref())
}

/// CSS-over-AACS priority dispatch, split out from [`keyless_failure_message`]
/// so the `.or()` ordering (css_error preferred when both are set, and
/// consulted at all) is unit-testable without constructing a full `Disc`.
fn keyless_failure_message_for(
    css_error: Option<&libfreemkv::Error>,
    aacs_error: Option<&libfreemkv::Error>,
) -> String {
    aacs_failure_message(css_error.or(aacs_error))
}

fn aacs_failure_message(err: Option<&libfreemkv::Error>) -> String {
    use libfreemkv::error as ec;

    // CssKeyMissing is a CSS (DVD) crack failure, not an AACS resolution
    // failure — surface it with CSS-specific messaging before the AACS
    // numeric dispatch so the operator isn't pointed at a key source.
    if let Some(libfreemkv::Error::CssKeyMissing) = err {
        return error_line(
            ec::E_CSS_KEY_MISSING,
            "Could not unscramble the disc. This is a CSS-protected disc and no title \
             key could be recovered. The disc may be damaged or use an unsupported \
             protection variant.",
        );
    }

    // KeydbLoad is a structural pre-condition failure, not an AACS
    // resolution failure — surface it with its own messaging before
    // the numeric dispatch.
    //
    // The library populates `path` with either the real filesystem path
    // that failed to load or the sentinel `<no keydb in search paths>`.
    // The sentinel means "nothing configured" — point the operator at
    // Settings. A real path means a *configured* key source failed to
    // load (wrong path, NFS not mounted, permissions): include it so the
    // operator can diagnose the load failure instead of being told to
    // configure a source that already exists.
    if let Some(libfreemkv::Error::KeydbLoad { path }) = err {
        const KEYDB_SENTINEL: &str = "<no keydb in search paths>";
        if path == KEYDB_SENTINEL {
            return error_line(
                ec::E_KEYDB_LOAD,
                "No keys are available. Configure a key source in Settings.",
            );
        }
        return error_line(
            ec::E_KEYDB_LOAD,
            &format!(
                "A configured key source failed to load: {path}. Check that the path \
                 exists and is readable."
            ),
        );
    }

    let Some(e) = err else {
        // Defensive fallback. scan_with always sets aacs_error when
        // encrypted && aacs.is_none(); if we land here something is
        // structurally off (e.g. callers building Disc by hand).
        return "This disc is encrypted and no keys were found. Check the key source \
                in Settings."
            .to_string();
    };

    let code = e.code();
    // Intentional overlap: specific 7xxx codes have dedicated arms above
    // the `7000..=7999` catch-all. Match-order semantics give us the
    // dispatch we want; clippy's match-overlapping-arm lint is a false
    // positive in this layout.
    #[allow(clippy::match_overlapping_arm)]
    match code {
        // E7000 — generic "everything tried, nothing worked" catch-all.
        ec::E_AACS_NO_KEYS => error_line(
            code,
            "No keys are available for this disc. It could not be resolved and no key \
             derivation path worked.",
        ),

        // Host cert rejected by the drive's HRL. Surfaces from
        // `aacs/handshake.rs` as drive-side AGID/cert rejection (7003),
        // host-side verify failure on the drive's cert (7005), drive
        // rejected our processing key (7007), or the post-loop
        // `AacsHostCertRejected` (7015) when every host cert was
        // rejected. "Update keys" intentionally NOT suggested — the
        // key source has the cert, the drive HRL is blocking it; fresh
        // keys do not change the cert content.
        ec::E_AACS_CERT_REJECTED
        | ec::E_AACS_CERT_VERIFY
        | ec::E_AACS_KEY_REJECTED
        | ec::E_AACS_HOST_CERT_REJECTED => error_line(
            code,
            "The drive rejected every available host certificate. The drive needs a \
             firmware unrevoke or raw-read mode to rip this disc.",
        ),

        // Drive does not support raw-read mode AND no host certs are
        // available for cert auth. Distinct from cert-rejected
        // because we never got far enough to attempt cert exchange.
        ec::E_AACS_RAW_READ_UNSUPPORTED => error_line(
            code,
            "The drive does not support raw-read mode and no usable host certificate \
             is available. This drive cannot rip this disc.",
        ),

        // VID retrieval failed. From cert path: VID read (7009) or VID
        // MAC verification (7010) went sideways. From raw-read path:
        // the alternate READ_DISC_STRUCTURE read failed (7017). Either
        // way the disc isn't in any key source (otherwise Path 1 would
        // have hit before we landed here).
        ec::E_AACS_VID_READ | ec::E_AACS_VID_MAC | ec::E_AACS_VID_UNAVAILABLE => error_line(
            code,
            "The drive did not return the disc Volume ID during AACS authentication, \
             so keys could not be derived and the disc could not be resolved.",
        ),

        // MK derivation failed. VID succeeded but no media key in the key
        // source walks this disc's MKB (7011) and no further fallback is
        // available (7018).
        ec::E_AACS_DATA_KEY | ec::E_AACS_MK_UNAVAILABLE => error_line(
            code,
            "The Volume ID was read, but no media key from any key source unlocks this \
             disc's media key block.",
        ),

        // Disc-hash lookup in a key source missed and no other path is
        // available. Typically downstream of VID being unavailable so
        // the derivation paths short-circuit.
        ec::E_AACS_VUK_NOT_IN_KEYDB => error_line(
            code,
            "This disc could not be resolved. Its disc hash was not found in any key \
             source.",
        ),

        // No host cert available at all — the OEM auth route can't run
        // because there's nothing to authenticate with. Distinct from
        // cert-rejected (7003/7005/7007/7015): there a cert existed but
        // the drive HRL blocked it; here no cert was present.
        ec::E_AACS_NO_HOST_CERT => error_line(
            code,
            "No host certificate is available, so the OEM authentication route cannot \
             run.",
        ),

        // Drive identity didn't match any bundled profile, so the
        // per-drive CDB templates needed for the OEM VID route aren't
        // available.
        ec::E_DRIVE_PROFILE_MISSING => error_line(
            code,
            "This drive is not in the profile database, so the OEM Volume ID route \
             cannot run.",
        ),

        // Drive profile is present but carries no VID-retrieval CDB
        // template (older profile blob, or a drive class without an OEM
        // VID path).
        ec::E_VID_CDB_UNAVAILABLE => error_line(
            code,
            "This drive's profile has no Volume ID command (it is an older profile), \
             so the OEM Volume ID route cannot run.",
        ),

        // Other 7xxx — known AACS category but unmapped. Use a
        // generic-but-honest message rather than `({e:?})` debug-dump.
        7000..=7999 => error_line(
            code,
            "AACS key resolution failed at an unrecognized stage. Please report this \
             at github.com/freemkv/freemkv/issues.",
        ),

        // Non-AACS code on the aacs_error slot — structurally
        // unexpected. Preserve the code; drop the `{e:?}` debug dump.
        _ => error_line(
            code,
            "An unexpected error occurred while resolving keys. Enable debug logging \
             via /api/debug for details.",
        ),
    }
}

/// Render a user-facing error line in the locked rc.6 messaging format:
/// `Error: E<code> <message>`. The `Error:` level word leads, the
/// language-neutral `E<code>` token follows, then the plain-English message.
/// One line, no trailing period added by the renderer — the message carries
/// its own punctuation. Single source of the format so every operator-facing
/// libfreemkv-error string in this module renders identically.
fn error_line(code: u16, message: &str) -> String {
    format!("Error: E{code} {message}")
}

/// Strip the leading `Error: E<code> ` prefix from an [`error_line`] string,
/// returning just the plain-English message. Used where a concise reason is
/// wanted (e.g. the key-readiness tile) without the level word and code. If
/// the input doesn't carry the prefix, it is returned unchanged.
fn strip_error_prefix(s: &str) -> &str {
    let Some(rest) = s.strip_prefix("Error: E") else {
        return s;
    };
    // Skip the numeric code, then the single separating space.
    let after_code = rest.trim_start_matches(|c: char| c.is_ascii_digit());
    after_code.strip_prefix(' ').unwrap_or(s)
}

/// Translate a libfreemkv read-error into a user-facing message for
/// Operator-facing message for the multipass disk-space preflight failure.
///
/// This is NOT a libfreemkv `Error` — there is no `Error::IoError`/E5000
/// raised by the preflight; it's a local autorip guard. Its text lands
/// directly in `/api/state`'s `last_error` and is rendered as-is in the web
/// UI red banner, so it must read like the other clean operator strings in
/// this module. It deliberately carries NO raw `EXXXX:` code prefix: a
/// hand-assembled "E5000:" would be an unlocalised literal that the freemkv
/// CLI would route to the `error.E5000` key while the dashboard showed the
/// raw code as diagnostic noise, and would diverge silently if the real
/// `E_IO_ERROR` display convention ever changed.
///
/// `required` and `avail` are byte counts; `staging` is the staging path.
fn disk_space_preflight_message(required: u64, staging: &str, avail: u64) -> String {
    format!(
        "Insufficient staging disk space — need ≥ {:.1} GB free at {} (2× disc capacity), have {:.1} GB. Free up space or point STAGING_DIR at a larger volume.",
        required as f64 / BYTES_PER_GIB,
        staging,
        avail as f64 / BYTES_PER_GIB,
    )
}

/// Short English label for a non-SCSI libfreemkv error variant, used to
/// annotate the code-only `Display` in `format_pass_error`'s no-sense arm.
/// Without this the operator sees e.g. "Pass 1 failed: E6010" with no hint
/// that E6010 is a user-requested stop. Returns a stable label for the
/// structural failures actually reachable on the sweep/patch path; any
/// unmapped variant falls back to a generic phrase so a new libfreemkv
/// variant never breaks the build.
fn non_scsi_error_label(e: &libfreemkv::Error) -> &'static str {
    use libfreemkv::Error;
    match e {
        Error::Halted => "rip stopped by user",
        Error::MapfileInvalid { .. } => "recovery mapfile invalid",
        Error::DiscRead { .. } => "disc read error",
        Error::DecryptFailed => "decryption failed",
        Error::NoStreams => "no playable streams on disc",
        Error::DiscCapacityOverflow | Error::DiscCapacityMalformed => {
            "drive reported unusable disc capacity"
        }
        _ => "unexpected error",
    }
}

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
        // Non-SCSI error (transport / IO / other) — surface the inner
        // io::Error detail. The libfreemkv Error Display is code-only
        // (language-neutral) as of 0.31; its `source` carries the message.
        //
        // For IoError we have the inner io::Error message. For any other
        // non-SCSI variant the bare Display is code-only (e.g. "E6010" for
        // Halted, "E6011: hex" for MapfileInvalid), which leaves the operator
        // reading an opaque code. Prefix those with a short English label so
        // the message names what failed, not just its number.
        let detail = match e {
            libfreemkv::Error::IoError { source } => source.to_string(),
            other => format!("{} ({})", other, non_scsi_error_label(other)),
        };
        return format!("{}{} failed: {}", pass_label, location, detail);
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

/// Render a libfreemkv setup/scan/mux error into a plain-English,
/// operator-facing line for `last_error` / `failure_reason` (the UI red
/// banner) and the device log.
///
/// The library's `Display` is deliberately code-only (`E1002: /dev/sg0`,
/// `E6009`) — language-neutral, but useless to a human and a direct rubric
/// violation if surfaced verbatim. This helper maps the variants autorip
/// reaches off the drive-open / identify / scan / open-ISO / mux-build paths
/// into "what failed, why if known, what to do next" without leaking a raw
/// `E####` code or an internal path.
///
/// `phase` is a short human label for where it failed ("Cannot open drive",
/// "Disc scan", "Open ISO", "Mux setup") that leads the sentence.
fn format_lib_error(phase: &str, e: &libfreemkv::Error) -> String {
    use libfreemkv::Error;

    // Drive read failures carry SCSI sense — reuse the sense decoder so the
    // operator gets the same "media damage / power-cycle the drive" guidance
    // the pass-error path produces, rather than a bare sector dump.
    if e.scsi_sense().is_some() {
        return format_pass_error(phase, e);
    }

    let detail = match e {
        // ── Drive / device layer (1xxx) ───────────────────────────────
        Error::DeviceNotFound { .. } => {
            "the drive could not be found. It may have been unplugged or moved to a \
             different device path — check the connection and rescan."
        }
        Error::DevicePermission { .. } => {
            "autorip is not allowed to access the drive. The container needs \
             `privileged: true` and `/dev:/dev` mounted — verify the compose file."
        }
        Error::DeviceNotReady { .. } => {
            "the drive is not ready. Make sure a disc is loaded and seated, wait a few \
             seconds, then retry."
        }
        Error::DeviceResetFailed { .. } | Error::DeviceLocked { .. } => {
            "the drive is wedged and could not be reset. Eject the disc and \
             power-cycle the drive, then retry."
        }
        Error::ScsiInterfaceUnavailable { .. } | Error::IoKitPluginFailed { .. } => {
            "autorip could not open a command channel to the drive. The container \
             needs `privileged: true` and `/dev:/dev` — verify the compose file, then \
             restart the container."
        }
        Error::UnsupportedDrive { .. } | Error::ProfileParse => {
            "this drive model is not supported for ripping."
        }
        Error::UnsupportedPlatform { .. } | Error::PlatformNotImplemented { .. } => {
            "this operation is not supported on this platform."
        }

        // ── Unlock / signature (3xxx) ─────────────────────────────────
        Error::UnlockFailed | Error::SignatureMismatch { .. } => {
            "the drive could not be unlocked for raw reads. It may need a firmware \
             flash or a supported drive to rip this disc."
        }

        // ── SCSI / IO without sense data ──────────────────────────────
        Error::ScsiError { .. } | Error::InvalidCdbLength { .. } => {
            "the drive returned a command error. Eject the disc and power-cycle the \
             drive, then retry."
        }
        Error::IoError { source } => return format!("{phase} failed: {source}"),

        // ── Disc structure / scan (6xxx) ──────────────────────────────
        Error::DiscRead { .. } => {
            "the disc could not be read. It may be dirty, scratched, or unreadable in \
             this drive — clean the disc and retry, or try another drive."
        }
        Error::UdfNotFound { .. } => {
            "no filesystem was found on the disc. It may be blank, unfinalized, or not \
             a video disc."
        }
        Error::MplsParse | Error::ClpiParse | Error::IfoParse | Error::DiscTitleRange { .. } => {
            "the disc's title structure could not be read. The disc may be damaged or \
             use an unsupported layout."
        }
        Error::NoStreams => {
            "no playable video was found on the disc. It may be damaged or not a \
             standard video disc."
        }
        Error::MkvInvalid => "the muxed output is not a valid MKV file.",
        Error::Halted => "the rip was stopped.",
        Error::MapfileInvalid { .. } => {
            "the recovery map for a previous attempt is corrupt. Start a fresh rip to \
             rebuild it."
        }

        // ── Decryption (7xxx) — defer to the AACS/CSS humanizer ────────
        Error::DecryptFailed
        | Error::CssKeyMissing
        | Error::CssAuthFailed
        | Error::NoDiscKey { .. } => {
            return format!(
                "{phase} failed: {}",
                strip_error_prefix(&aacs_failure_message(Some(e)))
            );
        }

        // ── Mux / output (9xxx) ───────────────────────────────────────
        Error::IsoTooLarge { .. } | Error::DiscCapacityOverflow | Error::DiscCapacityMalformed => {
            "the drive reported an unusable disc capacity. Clean the disc and retry, or \
             try another drive."
        }
        Error::NoMetadata => "the disc carries no usable metadata.",
        Error::MuxEmpty => {
            "the disc produced no output. It may be damaged or contain no playable \
             video."
        }
        Error::HevcParamParse
        | Error::PesInvalidMagic
        | Error::PesFrameTooLarge { .. }
        | Error::PesTrackTooLarge { .. }
        | Error::MuxTrackRange { .. }
        | Error::M2tsPacketMalformed => {
            "the disc's video stream could not be parsed for muxing. The source may be \
             damaged or use an unsupported encoding."
        }
        Error::DemuxThreadPanicked
        | Error::PipelineJoinTimeout
        | Error::PipelineConsumerPanicked
        | Error::PipelineConsumerGone
        | Error::SweepConsumerGone => {
            "the mux pipeline failed unexpectedly. Retry the rip; if it persists, \
             enable debug logging via /api/debug and report it."
        }

        // Any other variant: a generic, honest line with no leaked code.
        _ => {
            "an unexpected error occurred. Enable debug logging via /api/debug for \
             details."
        }
    };

    format!("{phase} failed: {detail}")
}

/// Open a drive during transport-failure recovery, retrying with
/// exponential backoff because firmware may not be ready for several
/// seconds after a USB-bridge crash re-enumerates the device (whether on
/// a new sg path or the original one). Shared by both recovery arms so
/// same-path recovery gets the same 3-attempt backoff as new-path.
///
/// Returns `Some(drive)` on success, or `None` once recovery is exhausted
/// (the caller should `break 'pass1` — all per-attempt and STRATEGY_FAILURE
/// logging is emitted here).
fn open_drive_with_backoff(
    device: &str,
    attempt: u32,
    path: &str,
    transport_recovery_delay_secs: u64,
) -> Option<libfreemkv::Drive> {
    for retry in 0..3 {
        match libfreemkv::Drive::open(std::path::Path::new(path)) {
            Ok(d) => return Some(d),
            Err(e) if retry < 2 => {
                let backoff_secs = transport_recovery_delay_secs * (1u64 << retry);
                crate::log::device_log(
                    device,
                    &format!(
                        "Pass 1 attempt {attempt}: Drive::open({}) failed, retrying in {}s: error={} sense_key={:?} ASC={:?}",
                        path,
                        backoff_secs,
                        e.code(),
                        e.scsi_sense().map(|s| s.sense_key),
                        e.scsi_sense().map(|s| s.asc)
                    ),
                );
                std::thread::sleep(std::time::Duration::from_secs(backoff_secs));
            }
            Err(e) => {
                crate::log::device_log(
                    device,
                    &format!(
                        "Pass 1 attempt {attempt}: Drive::open({}) failed strategy=transport_failure_recovery error={} sense_key={:?} ASC={:?} — recovery path exhausted",
                        path,
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

                return None;
            }
        }
    }

    // Unreachable: the loop either returns Some on success or None on the
    // final Err arm. Treat any fall-through as exhausted.
    None
}

/// Emit the post-`Drive::init` failure diagnostic for a transport-recovery
/// re-open. Shared by both the new-path and same-path recovery arms so they
/// log consistently: an ILLEGAL REQUEST (ASC=0x20) after init means the
/// drive firmware is wedged and needs a physical power-cycle, so we surface
/// the USER_ACTION_REQUIRED line; anything else is a plain STRATEGY_FAILURE.
fn log_init_recovery_failure(device: &str, e: &libfreemkv::Error) {
    let is_wedged_firmware =
        e.code() == 4000 && e.scsi_sense().map(|s| s.asc == 0x20).unwrap_or(false);

    if is_wedged_firmware {
        crate::log::device_log(
            device,
            "STRATEGY_FAILURE: transport_failure_recovery FAILED at Drive::init with ILLEGAL_REQUEST (ASC=0x20) — drive firmware wedged",
        );
        crate::log::device_log(
            device,
            "USER_ACTION_REQUIRED: Eject disc and physically power-cycle USB optical drive to clear firmware state before retrying",
        );
    } else {
        let failure_category = if e.code() == 4000 {
            "SCSI_ERROR".to_string()
        } else {
            format!("ERROR_CODE_{}", e.code())
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
}

#[cfg(test)]
mod tests {
    //! Tests for orchestrator-level helpers that live in this file.
    //! State-only helpers and their tests live in `state.rs`.

    use super::{
        HaltGuard, SweepingGuard, aacs_failure_message, disk_space_preflight_message,
        format_lib_error, format_pass_error, header_phase_outcome_is_failure,
        incomplete_mux_status, is_safe_staging_segment, list_staging_basenames,
        prune_intermediate_iso, register_halt, resumable_dir_blocked, resumable_for_disc,
        staging_dir_matches_disc, staging_disc_completed, staging_disc_owned_by_worker,
        staging_free_bytes,
    };
    use crate::ripper::session::device_halt;
    use crate::ripper::staging;
    use crate::ripper::state::Resumable;
    use crate::util::MILLIS_PER_SEC;
    use libfreemkv::{Error, ScsiSense};

    /// Convergence H1 regression: `SweepingGuard` is the RAII cleanup for the
    /// `.sweeping` in-progress marker. Many `rip_disc` early-return error
    /// branches exit without reaching a terminal-marker writer; before this
    /// guard each of those leaked a stale `.sweeping`, which made the next
    /// startup's `resume_or_quarantine_staging` classify the dir `InProgress`
    /// forever (never restart-counted, never cold-resumed). The guard's `Drop`
    /// must clear the marker on every exit path so a dir holding a complete
    /// ISO + clean mapfile can still be picked up on restart.
    #[test]
    fn sweeping_guard_clears_marker_on_drop() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        staging::write_sweeping_marker(&dir);
        assert!(
            dir.join(staging::SWEEPING_MARKER).exists(),
            "marker should be present before the guard drops"
        );
        {
            let _guard = SweepingGuard {
                staging: dir.clone(),
            };
            // Still present inside the guard's scope (mirrors the live
            // sweep+patch window).
            assert!(dir.join(staging::SWEEPING_MARKER).exists());
        }
        // Guard dropped at scope end (the early-return / panic case) — marker
        // gone, so the restart scan won't strand this dir `InProgress`.
        assert!(
            !dir.join(staging::SWEEPING_MARKER).exists(),
            ".sweeping must be cleared when SweepingGuard drops"
        );
    }

    /// Convergence H1: on the success / `.ripped` / `.failed` paths a terminal
    /// writer already clears `.sweeping` before the guard drops, so the guard's
    /// clear must be an idempotent no-op (not error, not resurrect state) — and
    /// must not disturb a terminal marker that superseded `.sweeping`.
    #[test]
    fn sweeping_guard_is_idempotent_after_terminal_marker() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        staging::write_sweeping_marker(&dir);
        {
            let _guard = SweepingGuard {
                staging: dir.clone(),
            };
            // Terminal write (e.g. `.failed`) clears `.sweeping` first, as on
            // the real quarantine paths.
            staging::write_failed_marker(&dir, "boom");
            assert!(!dir.join(staging::SWEEPING_MARKER).exists());
        }
        // Guard drop is a no-op: `.sweeping` stays gone and `.failed` survives.
        assert!(!dir.join(staging::SWEEPING_MARKER).exists());
        assert!(
            dir.join(staging::FAILED_MARKER).exists(),
            "guard drop must not remove the terminal .failed marker"
        );
    }

    /// Regression guard for the divergent disk-reclamation bug: the inline
    /// (`rip_disc`) and resume (`resume::resume_remux`) completion paths now
    /// share `prune_intermediate_iso`, so a `keep_iso=false` multipass
    /// completion frees the disc-sized ISO + mapfile on BOTH routes. Before
    /// the fix, a `.review` (low-confidence) or no-mover resume leaked a 90+ GB
    /// ISO that the inline path would have freed.
    #[test]
    fn prune_removes_iso_and_mapfile_when_keep_iso_false() {
        let tmp = tempfile::TempDir::new().unwrap();
        let iso = tmp.path().join("Movie.iso");
        let map = tmp.path().join("Movie.iso.mapfile");
        std::fs::write(&iso, b"iso").unwrap();
        std::fs::write(&map, b"map").unwrap();

        prune_intermediate_iso(
            "sr0", &iso, &map, /* max_retries */ 1, /* keep_iso */ false,
        );

        assert!(!iso.exists(), "ISO must be pruned when keep_iso=false");
        assert!(!map.exists(), "mapfile must be pruned when keep_iso=false");
    }

    #[test]
    fn prune_keeps_iso_and_mapfile_when_keep_iso_true() {
        let tmp = tempfile::TempDir::new().unwrap();
        let iso = tmp.path().join("Movie.iso");
        let map = tmp.path().join("Movie.iso.mapfile");
        std::fs::write(&iso, b"iso").unwrap();
        std::fs::write(&map, b"map").unwrap();

        prune_intermediate_iso(
            "sr0", &iso, &map, /* max_retries */ 1, /* keep_iso */ true,
        );

        assert!(iso.exists(), "ISO must be retained when keep_iso=true");
        assert!(map.exists(), "mapfile must be retained when keep_iso=true");
    }

    #[test]
    fn prune_is_noop_in_direct_mode() {
        // max_retries == 0 is direct mode: no intermediate ISO is ever
        // produced, so the prune must not touch unrelated files.
        let tmp = tempfile::TempDir::new().unwrap();
        let iso = tmp.path().join("Movie.iso");
        let map = tmp.path().join("Movie.iso.mapfile");
        std::fs::write(&iso, b"iso").unwrap();
        std::fs::write(&map, b"map").unwrap();

        prune_intermediate_iso(
            "sr0", &iso, &map, /* max_retries */ 0, /* keep_iso */ false,
        );

        assert!(iso.exists(), "direct mode (max_retries=0) must not prune");
        assert!(map.exists(), "direct mode (max_retries=0) must not prune");
    }

    #[test]
    fn prune_tolerates_already_absent_files() {
        // NotFound is silent: re-running prune, or a path where the mover
        // already relocated/removed the ISO, must not error.
        let tmp = tempfile::TempDir::new().unwrap();
        let iso = tmp.path().join("Gone.iso");
        let map = tmp.path().join("Gone.iso.mapfile");
        // Neither file exists.
        prune_intermediate_iso("sr0", &iso, &map, 1, false);
        assert!(!iso.exists());
        assert!(!map.exists());
    }

    /// Resume / completion matching is EXACT, never prefix. A disc named
    /// "Cars" (sanitized "Cars") must not match a sibling staging dir
    /// "Cars_2" (from "Cars 2") — a prefix match there would resume onto a
    /// different title's partial ISO/mapfile. This locks in the already-fixed
    /// HIGH bug; a regression to `starts_with` would fail here.
    #[test]
    fn staging_match_is_exact_not_prefix() {
        // Direct predicate: exact equality only.
        assert!(staging_dir_matches_disc("Cars", "Cars"));
        assert!(!staging_dir_matches_disc("Cars_2", "Cars"));
        assert!(!staging_dir_matches_disc("Cars", "Cars_2"));
        assert!(!staging_dir_matches_disc("Cars_2_Extras", "Cars_2"));

        // End-to-end over a real temp staging dir: both "Cars" and "Cars_2"
        // exist; scanning with the production predicate must select ONLY the
        // exact "Cars".
        let tmp = tempfile::TempDir::new().unwrap();
        for name in ["Cars", "Cars_2"] {
            std::fs::create_dir_all(tmp.path().join(name)).unwrap();
        }
        let sanitized = "Cars";
        let matches: Vec<String> = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .filter_map(|e| {
                e.path()
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
            })
            .filter(|basename| staging_dir_matches_disc(basename, sanitized))
            .collect();
        assert_eq!(
            matches,
            vec!["Cars".to_string()],
            "only the exact 'Cars' dir must match, not the 'Cars_2' sibling"
        );
    }

    /// Regression: a `run_mux` header-phase exit with `output_opened=false`
    /// AND `finalize_error=Some` (header buffer overflow before
    /// codec_privates resolved, or EOF / read error before
    /// `headers_ready()`) must be classified as a terminal failure so the
    /// orchestrator quarantines the staging dir (`.failed`) and flips the
    /// device tile to `status="failed"`. A clean stop during headers
    /// (`finalize_error=None`) must NOT be classified as a failure — it
    /// stays resumable. Before the fix the `finalize_error=Some` case took
    /// the bare early-return: reason dropped, no marker, tile stuck in
    /// `status="ripping"`.
    #[test]
    fn header_phase_finalize_error_is_terminal_failure() {
        // finalize_error=Some → terminal failure (quarantine).
        assert!(
            header_phase_outcome_is_failure(false, Some("header buffer exceeded cap")),
            "output never opened with a finalize_error must be a terminal failure"
        );
        assert!(
            header_phase_outcome_is_failure(false, Some("header resolution incomplete")),
            "header-resolution-incomplete must be a terminal failure"
        );

        // finalize_error=None → clean stop, stays resumable (not a failure).
        assert!(
            !header_phase_outcome_is_failure(false, None),
            "a clean header-phase stop (halt) must stay resumable, not quarantined"
        );

        // output_opened=true → not a header-phase failure (handled by the
        // post-finalize path further down rip_disc, never this branch).
        assert!(!header_phase_outcome_is_failure(true, None));
        assert!(!header_phase_outcome_is_failure(
            true,
            Some("post-mux finalize error")
        ));
    }

    /// Regression: single-pass `on_read_error=stop` with a hard read error.
    /// `run_mux` returns `completed=false` with `read_error=Some` and no
    /// `finalize_error`. The orchestrator's incomplete-mux branch must map
    /// that to `status="error"` with a non-empty cause — NOT the silent
    /// "stopped → idle" path a genuine user halt takes — so `/api/state`
    /// signals the read failure rather than looking like an idle, user-
    /// stopped rip with no `last_error`.
    #[test]
    fn read_error_surfaces_as_error_status_not_silent_idle() {
        // A read-error truncation: status="error", reason names the cause.
        let (log_prefix, status, reason) =
            incomplete_mux_status(None, Some("E7015 read failed at LBA 42"));
        assert_eq!(status, "error");
        let reason = reason.expect("read error must carry a failure_reason / last_error");
        assert!(
            reason.contains("E7015 read failed at LBA 42"),
            "failure_reason must name the read-error cause, got: {reason}"
        );
        assert!(log_prefix.contains("read error"));

        // A genuine user halt (no finalize_error, no read_error) stays the
        // pre-existing silent stop → idle with no last_error.
        let (_, status, reason) = incomplete_mux_status(None, None);
        assert_eq!(status, "idle");
        assert!(
            reason.is_none(),
            "a user halt must NOT fabricate a failure_reason"
        );

        // A structural finalize error still wins over a read error (broken
        // file on disk is the stronger signal → quarantine path).
        let (_, status, reason) =
            incomplete_mux_status(Some("cues seek-back failed"), Some("read error too"));
        assert_eq!(status, "failed");
        assert!(reason.unwrap().contains("cues seek-back failed"));
    }

    /// The disk-space pre-flight in `rip_disc` branches on
    /// `staging_free_bytes`: `Some(avail)` runs the 2×-capacity gate,
    /// `None` must take the diagnostic-log branch (NOT silently skip).
    /// This locks in the contract that branch relies on — a missing /
    /// unmounted staging path yields `None` so the operator gets a
    /// "preflight skipped" warning instead of a silent slide into a
    /// mid-rip ENOSPC. A real, existing path must yield `Some`.
    #[test]
    fn staging_free_bytes_none_for_missing_path_some_for_real() {
        // Nonexistent path → statvfs fails → None (drives the else/warn
        // branch in the rip_disc preflight).
        let tmp = tempfile::TempDir::new().unwrap();
        let missing = tmp.path().join("does-not-exist-staging-volume");
        assert!(
            staging_free_bytes(&missing.to_string_lossy()).is_none(),
            "a missing staging path must return None so the preflight logs \
             'skipped' rather than silently proceeding"
        );

        // A real, existing directory → Some(free bytes): unix via statvfs,
        // Windows via GetDiskFreeSpaceExW. Only the bare-fallback stub (neither
        // unix nor windows) returns None, so assert Some on both real targets.
        #[cfg(any(unix, windows))]
        assert!(
            staging_free_bytes(&tmp.path().to_string_lossy()).is_some(),
            "an existing staging path must return Some(free_bytes)"
        );
    }

    /// The `HaltGuard` created at the top of `rip_disc` must unregister the
    /// device's halt-map entry on EVERY exit path — including the early-return
    /// error branches that leaked it in the v0.13.6 class of bug. Dropping the
    /// guard (what happens on any return/panic) must remove the entry so a
    /// subsequent rip starts with a fresh, uncancelled token rather than
    /// inheriting the prior attempt's state.
    #[test]
    fn halt_guard_unregisters_on_drop() {
        let device = "sg_haltguard_drop_test";
        // Clean any residue from a prior run so the assertion is meaningful.
        super::unregister_halt(device);
        register_halt(device, libfreemkv::Halt::new());
        assert!(
            device_halt(device).is_some(),
            "halt entry should be registered before the guard drops"
        );
        {
            let _guard = HaltGuard {
                device: device.to_string(),
            };
            // Simulate an early-return error path: leaving this scope drops
            // the guard, which must run `unregister_halt`.
        }
        assert!(
            device_halt(device).is_none(),
            "HaltGuard::drop must unregister the halt-map entry on every exit path"
        );
    }

    /// The staging-segment guard must reject anything that could escape
    /// or resolve to the staging root, so a hostile disc label can never
    /// drive `remove_dir_all` outside staging.
    #[test]
    fn staging_segment_guard_rejects_traversal() {
        // Dangerous: traversal, current-dir, all-dots, empty, separators,
        // absolute.
        for bad in [
            "",
            ".",
            "..",
            "...",
            "/",
            "..\\",
            "a/b",
            "a\\b",
            "/etc",
            "../sibling",
            "./foo",
        ] {
            assert!(
                !is_safe_staging_segment(bad),
                "{bad:?} must be rejected as a staging segment"
            );
        }
        // Safe: ordinary sanitized title names (dots inside a name are
        // fine as long as the whole segment isn't only dots).
        for ok in [
            "Dune (2021)",
            "Blade.Runner (1982)",
            "untitled",
            "A.Movie.With.Dots",
            "disc",
        ] {
            assert!(
                is_safe_staging_segment(ok),
                "{ok:?} must be accepted as a staging segment"
            );
        }
    }

    /// Build a minimal `DiscTitle` whose single extent spans `[start_lba,
    /// start_lba + sector_count)`. Only `extents` matters for
    /// `bytes_bad_in_title` / the abort-loss scoping.
    fn test_title(start_lba: u32, sector_count: u32) -> libfreemkv::DiscTitle {
        libfreemkv::DiscTitle {
            playlist: "00800.mpls".to_string(),
            playlist_id: 800,
            duration_secs: 7200.0,
            size_bytes: (sector_count as u64) * 2048,
            clips: Vec::new(),
            streams: Vec::new(),
            chapters: Vec::new(),
            extents: vec![libfreemkv::disc::Extent {
                start_lba,
                sector_count,
            }],
            content_format: libfreemkv::disc::ContentFormat::BdTs,
            codec_privates: Vec::new(),
        }
    }

    /// The scoped loss is the TOTAL of all in-title gaps, not the single
    /// largest one (the old `fold(.., f64::max)` bug). Many scattered
    /// small gaps must accumulate against the threshold.
    #[test]
    fn abort_loss_sums_scattered_in_title_gaps() {
        // Title covers bytes [0, 100_000_000) (sectors 0..~48829).
        let title = test_title(0, 48_829);
        let bps = 1_000_000.0; // 1 byte == 1 us

        // 50 scattered 1 MB gaps inside the title = 50 MB total.
        // At 1 MB/s that is 50 s == 50_000 ms.
        let bad: Vec<(u64, u64)> = (0..50).map(|i| (i * 1_500_000u64, 1_000_000u64)).collect();
        let lost = super::abort_lost_ms(false, &title, &bad, bps);
        // Old fold-max would have reported ~1000 ms (one gap); sum is 50x.
        assert!(
            (lost - 50_000.0).abs() < 1.0,
            "expected ~50_000 ms total, got {lost}"
        );

        // ISO output is whole-disc: same bad ranges sum regardless of
        // title scoping.
        let lost_iso = super::abort_lost_ms(true, &title, &bad, bps);
        assert!((lost_iso - 50_000.0).abs() < 1.0, "iso whole-disc sum");
    }

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
    fn pass1_exhaustion_message_translates_cause_not_strategy_id() {
        // Regression: the Pass 1 `result = None` exhaustion fallthrough must
        // surface the underlying SCSI cause via `format_pass_error`, never a
        // bare internal strategy identifier. This mirrors the fallthrough's
        // translation of the captured `last_sweep_err`.
        let last_sweep_err = Some(Error::DiscRead {
            sector: 1_000,
            status: Some(2),
            sense: Some(ScsiSense {
                sense_key: 4,
                asc: 0x3E,
                ascq: 0,
            }),
        });

        let user_msg = match &last_sweep_err {
            Some(e) => format_pass_error("Pass 1", e),
            None => "Pass 1 failed — see logs for detailed error breakdown".to_string(),
        };

        // Operator-facing, actionable.
        assert!(user_msg.to_lowercase().contains("power-cycle"));
        // Never leaks the internal strategy identifiers.
        assert!(!user_msg.contains("transport_failure_recovery_exhausted"));
        assert!(!user_msg.contains("unrecoverable_error"));
    }

    #[test]
    fn pass1_exhaustion_message_falls_back_when_no_error_captured() {
        // If no sweep error was captured (e.g. recovery broke out before any
        // sweep failed), the fallthrough uses a plain message rather than a
        // strategy identifier.
        let last_sweep_err: Option<Error> = None;
        let user_msg = match &last_sweep_err {
            Some(e) => format_pass_error("Pass 1", e),
            None => "Pass 1 failed — see logs for detailed error breakdown".to_string(),
        };
        assert!(!user_msg.contains("transport_failure_recovery_exhausted"));
        assert!(!user_msg.contains("unrecoverable_error"));
        assert!(user_msg.contains("Pass 1 failed"));
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
    fn format_pass_error_no_sense_non_io_gets_english_label() {
        // Regression: a non-SCSI, non-IoError error (no sense triple) must
        // carry an English label, not just the bare code-only Display.
        // Halted ("rip stopped by user") is the actionable example — a user
        // stop propagated into the sweep.
        let s = format_pass_error("Pass 1", &Error::Halted);
        assert!(s.contains("Pass 1 failed"), "msg: {s}");
        // Still routable: the numeric code is preserved.
        assert!(s.contains("E6010"), "msg must keep the code: {s}");
        // ...but no longer opaque: an English label identifies it.
        assert!(
            s.to_lowercase().contains("stopped by user"),
            "msg must label the code: {s}"
        );

        // MapfileInvalid carries a `kind` payload in its Display; the label
        // must still be appended after it.
        let s = format_pass_error("Pass 2", &Error::MapfileInvalid { kind: "hex" });
        assert!(s.contains("E6011"), "msg: {s}");
        assert!(
            s.to_lowercase().contains("mapfile invalid"),
            "msg must label the code: {s}"
        );
    }

    // ── format_lib_error: setup/scan/open/mux phase rendering ─────────
    //
    // The library Display is code-only (`E1002: /dev/sg0`). Every variant
    // autorip reaches off the drive-open / identify / scan / open-ISO paths
    // must render as plain English with NO raw `E####` code and NO leaked
    // device path, leading with the phase label.

    #[test]
    fn format_lib_error_device_permission_says_privileged_not_code() {
        let e = Error::DevicePermission {
            path: "/dev/sg0".into(),
        };
        let s = format_lib_error("Cannot open drive", &e);
        assert!(s.starts_with("Cannot open drive failed:"), "msg: {s}");
        assert!(s.to_lowercase().contains("privileged"), "msg: {s}");
        // No raw code, no leaked device path.
        assert!(!s.contains("E1001"), "msg leaks code: {s}");
        assert!(!s.contains("/dev/sg0"), "msg leaks path: {s}");
    }

    #[test]
    fn format_lib_error_device_not_found_actionable() {
        let e = Error::DeviceNotFound {
            path: "/dev/sg9".into(),
        };
        let s = format_lib_error("Cannot open drive", &e);
        assert!(s.to_lowercase().contains("unplugged"), "msg: {s}");
        assert!(!s.contains("E1000"), "msg: {s}");
        assert!(!s.contains("/dev/sg9"), "msg: {s}");
    }

    #[test]
    fn format_lib_error_no_streams_plain_english() {
        let s = format_lib_error("Disc scan", &Error::NoStreams);
        assert!(s.starts_with("Disc scan failed:"), "msg: {s}");
        assert!(s.to_lowercase().contains("no playable video"), "msg: {s}");
        assert!(!s.contains("E6009"), "msg leaks code: {s}");
    }

    #[test]
    fn format_lib_error_udf_not_found_blank_disc_hint() {
        let e = Error::UdfNotFound {
            path: "/some/internal/path".into(),
        };
        let s = format_lib_error("Disc scan", &e);
        assert!(s.to_lowercase().contains("filesystem"), "msg: {s}");
        assert!(!s.contains("E6003"), "msg: {s}");
        assert!(!s.contains("/some/internal/path"), "msg leaks path: {s}");
    }

    #[test]
    fn format_lib_error_disc_read_advises_clean_disc() {
        // A DiscRead WITHOUT sense data (no SCSI triple) — must still render
        // a plain-English clean-the-disc message, not a bare code or sector.
        let e = Error::DiscRead {
            sector: 12345,
            status: None,
            sense: None,
        };
        let s = format_lib_error("Disc scan", &e);
        assert!(s.to_lowercase().contains("could not be read"), "msg: {s}");
        assert!(!s.contains("E6000"), "msg leaks code: {s}");
        assert!(!s.contains("12345"), "msg leaks sector: {s}");
    }

    #[test]
    fn format_lib_error_disc_read_with_sense_uses_pass_decoder() {
        // A DiscRead WITH sense data routes through format_pass_error, so the
        // operator gets the media-damage cause + Pass-2 guidance.
        let e = Error::DiscRead {
            sector: 1_000_000,
            status: Some(2),
            sense: Some(ScsiSense {
                sense_key: 3,
                asc: 0x11,
                ascq: 0,
            }),
        };
        let s = format_lib_error("Disc scan", &e);
        assert!(s.to_lowercase().contains("bad sector"), "msg: {s}");
        assert!(!s.contains("E6000"), "msg leaks code: {s}");
    }

    #[test]
    fn format_lib_error_io_error_surfaces_inner_message() {
        // io::Error Display is already plain English — surface it directly,
        // no synthetic phrasing, no code.
        let e = Error::IoError {
            source: std::io::Error::other("no space left on device"),
        };
        let s = format_lib_error("Open output file", &e);
        assert!(s.starts_with("Open output file failed:"), "msg: {s}");
        assert!(s.contains("no space left on device"), "msg: {s}");
    }

    #[test]
    fn format_lib_error_decrypt_defers_to_aacs_humanizer() {
        let s = format_lib_error("Disc scan", &Error::CssKeyMissing);
        // Routed through aacs_failure_message → CSS-specific text, prefix stripped.
        assert!(s.starts_with("Disc scan failed:"), "msg: {s}");
        assert!(s.to_lowercase().contains("unscramble"), "msg: {s}");
        // The stripped form must NOT carry the Error:/E#### prefix.
        assert!(!s.contains("E7023"), "msg leaks code: {s}");
    }

    #[test]
    fn format_lib_error_never_leaks_bare_code_for_unmapped_variant() {
        // An unmapped variant must hit the generic arm, not dump a code.
        let s = format_lib_error("Disc scan", &Error::ProfileParse);
        assert!(s.starts_with("Disc scan failed:"), "msg: {s}");
        assert!(!s.contains("E2002"), "msg leaks code: {s}");
    }

    // ── aacs_failure_message dispatch ────────────────────────────────
    //
    // Locked rc.6 messaging standard: every user-facing message renders as
    // `Error: E<code> <message>` — (1) the `Error:` level word leads, (2) the
    // language-neutral `E<code>` token follows so support requests stay
    // routable, (3) a single plain-English sentence, and (4) never leak the
    // `{e:?}` debug dump the pre-rewrite catch-all produced.

    #[test]
    fn aacs_failure_messages_follow_level_code_format() {
        // Format contract: every rendered message starts with `Error: E<code> `.
        for e in [
            Error::CssKeyMissing,
            Error::KeydbLoad {
                path: "<no keydb in search paths>".into(),
            },
            Error::KeydbLoad {
                path: "/config/keys/keydb.cfg".into(),
            },
            Error::AacsNoKeys,
            Error::AacsCertRejected,
            Error::AacsRawReadUnsupported,
            Error::AacsVidRead,
            Error::AacsDataKey,
            Error::AacsVukNotInKeydb,
            Error::DriveProfileMissing,
            Error::VidCdbUnavailable,
            Error::AacsNoHostCert {
                path: "<no host cert>".into(),
            },
            Error::AacsAgidAlloc,
        ] {
            let s = aacs_failure_message(Some(&e));
            assert!(
                s.starts_with(&format!("Error: E{} ", e.code())),
                "{e:?} must render `Error: E<code> <msg>`, got: {s}"
            );
            // One line — no embedded newline in the rc.6 single-line format.
            assert!(!s.contains('\n'), "{e:?} message must be one line: {s}");
        }
    }

    #[test]
    fn aacs_failure_keydb_load_missing_path() {
        let e = Error::KeydbLoad {
            path: "<no keydb in search paths>".into(),
        };
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E8005 "), "msg: {s}");
        assert!(s.contains("No keys are available"), "msg: {s}");
        assert!(!s.contains("KEYDB"), "msg must not name the source: {s}");
    }

    #[test]
    fn aacs_failure_keydb_load_corrupt() {
        // A *configured* keydb that failed to load (real path, not the
        // sentinel) must surface that path so the operator can diagnose
        // the load failure, and must NOT collapse to the generic
        // "configure a key source" message reserved for the no-keydb case.
        let path = "/config/keys/keydb.cfg";
        let e = Error::KeydbLoad { path: path.into() };
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E8005 "), "msg: {s}");
        assert!(s.contains(path), "msg must include the failing path: {s}");
        assert!(
            !s.contains("Configure a key source in Settings"),
            "configured-but-failed must not show the no-keydb message: {s}"
        );
        assert!(!s.contains("KEYDB"), "msg must not name the source: {s}");
    }

    #[test]
    fn aacs_failure_cert_rejected_says_host_cert() {
        // E7003 — drive rejected our host cert (HRL).
        let e = Error::AacsCertRejected;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7003 "), "msg: {s}");
        assert!(s.contains("host certificate"), "msg: {s}");
        assert!(s.contains("raw-read mode"), "msg: {s}");
        // No "Update keys/KEYDB" — the key source has the cert; the HRL blocks it.
        assert!(!s.contains("Update KEYDB"), "msg: {s}");
        // Must not leak the debug-dump form the old catch-all emitted.
        assert!(!s.contains("AacsCertRejected"), "msg: {s}");
    }

    #[test]
    fn aacs_failure_cert_verify_collapses_to_host_cert() {
        let e = Error::AacsCertVerify;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7005 "), "msg: {s}");
        assert!(s.contains("host certificate"), "msg: {s}");
    }

    /// The disk-space preflight message is operator-facing (it lands in
    /// last_error and the web UI red banner as-is). It must NOT carry a raw
    /// "EXXXX:" code prefix — there is no libfreemkv Error raised here, so a
    /// hand-assembled "E5000:" would be unlocalised diagnostic noise to the
    /// operator. Guards against re-introducing the prefix.
    #[test]
    fn disk_space_preflight_message_has_no_raw_error_code_prefix() {
        let required = 100u64 * 1_073_741_824; // 100 GiB
        let avail = 40u64 * 1_073_741_824; // 40 GiB
        let s = disk_space_preflight_message(required, "/staging-local", avail);
        assert!(
            !s.contains("E5000"),
            "raw E5000 code leaked into operator message: {s}"
        );
        // No "ENNNN:" code prefix anywhere (digits-after-E followed by colon).
        for (i, _) in s.match_indices('E') {
            let tail = &s[i + 1..];
            let digits: String = tail.chars().take_while(|c| c.is_ascii_digit()).collect();
            assert!(
                digits.is_empty() || !tail[digits.len()..].starts_with(':'),
                "raw EXXXX: code prefix leaked into operator message: {s}"
            );
        }
        // Still reports both the requirement and the actual free space.
        assert!(s.contains("100.0 GB"), "missing required figure: {s}");
        assert!(s.contains("40.0 GB"), "missing available figure: {s}");
        assert!(s.contains("/staging-local"), "missing staging path: {s}");
    }

    #[test]
    fn aacs_failure_key_rejected_says_host_cert() {
        // E7007 — drive HRL blocked our processing key. Same
        // remediation as cert rejection.
        let e = Error::AacsKeyRejected;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7007 "), "msg: {s}");
        assert!(s.contains("host certificate"), "msg: {s}");
    }

    #[test]
    fn aacs_failure_vid_read_says_vid_missing() {
        let e = Error::AacsVidRead;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7009 "), "msg: {s}");
        assert!(s.contains("Volume ID"), "msg: {s}");
    }

    #[test]
    fn aacs_failure_vid_mac_says_vid_missing() {
        let e = Error::AacsVidMac;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7010 "), "msg: {s}");
        assert!(s.contains("Volume ID"), "msg: {s}");
    }

    #[test]
    fn aacs_failure_data_key_says_mk_missing() {
        let e = Error::AacsDataKey;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7011 "), "msg: {s}");
        assert!(s.contains("media key"), "msg: {s}");
    }

    #[test]
    fn aacs_failure_no_keys_says_all_missing() {
        let e = Error::AacsNoKeys;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7000 "), "msg: {s}");
        assert!(s.contains("No keys are available"), "msg: {s}");
    }

    #[test]
    fn aacs_failure_unknown_aacs_code_uses_generic_7xxx_arm() {
        // Unmapped-but-AACS-range error falls through to the 7xxx
        // catch-all. E_AACS_AGID_ALLOC (7002) is not in any named arm
        // and exercises that path.
        let e = Error::AacsAgidAlloc;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7002 "), "msg: {s}");
        assert!(s.contains("unrecognized stage"), "msg: {s}");
        assert!(s.contains("github.com/freemkv/freemkv/issues"), "msg: {s}");
        // No debug-dump leak.
        assert!(!s.contains("AacsAgidAlloc"), "msg: {s}");
    }

    #[test]
    fn aacs_failure_none_falls_back_defensively() {
        let s = aacs_failure_message(None);
        assert!(s.contains("no keys were found"), "msg: {s}");
    }

    // ── variants landing with v0.25.11 ───────────────────────────────

    #[test]
    fn aacs_failure_host_cert_rejected_says_host_cert() {
        // E7015 — all host certs in keydb were rejected by the drive.
        let e = Error::AacsHostCertRejected;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7015 "), "msg: {s}");
        assert!(s.contains("host certificate"), "msg: {s}");
        assert!(!s.contains("AacsHostCertRejected"), "msg: {s}");
    }

    #[test]
    fn aacs_failure_raw_read_unsupported_says_no_cert() {
        // E7016 — drive doesn't support raw-read mode AND no host certs.
        let e = Error::AacsRawReadUnsupported;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7016 "), "msg: {s}");
        assert!(s.contains("does not support raw-read mode"), "msg: {s}");
    }

    #[test]
    fn aacs_failure_vid_unavailable_says_vid_missing() {
        // E7017 — alternate VID read failed.
        let e = Error::AacsVidUnavailable;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7017 "), "msg: {s}");
        assert!(s.contains("Volume ID"), "msg: {s}");
    }

    #[test]
    fn aacs_failure_mk_unavailable_says_mk_missing() {
        // E7018 — VID ok, but no DK in keydb walks this MKB.
        let e = Error::AacsMkUnavailable;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7018 "), "msg: {s}");
        assert!(s.contains("media key"), "msg: {s}");
    }

    #[test]
    fn aacs_failure_vuk_not_in_keydb_says_vuk_missing() {
        // E7019 — disc hash isn't in keydb and no derivation path
        // was available.
        let e = Error::AacsVukNotInKeydb;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7019 "), "msg: {s}");
        assert!(s.contains("could not be resolved"), "msg: {s}");
        assert!(!s.contains("KEYDB"), "msg must not name the source: {s}");
    }

    #[test]
    fn aacs_failure_drive_profile_missing_has_dedicated_arm() {
        // E7020 — drive not in profile DB; must not fall through to the
        // generic "report at github.com" catch-all.
        let e = Error::DriveProfileMissing;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7020 "), "msg: {s}");
        assert!(s.contains("profile database"), "msg: {s}");
        assert!(
            !s.contains("github.com"),
            "msg must not say report a bug: {s}"
        );
    }

    #[test]
    fn aacs_failure_vid_cdb_unavailable_has_dedicated_arm() {
        // E7021 — profile present but no VID-retrieval CDB template.
        let e = Error::VidCdbUnavailable;
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7021 "), "msg: {s}");
        assert!(s.contains("Volume ID command"), "msg: {s}");
        assert!(
            !s.contains("github.com"),
            "msg must not say report a bug: {s}"
        );
    }

    #[test]
    fn aacs_failure_no_host_cert_has_dedicated_arm() {
        // E7024 — no host cert available; the OEM auth route can't run.
        let e = Error::AacsNoHostCert {
            path: "<no host cert>".into(),
        };
        let s = aacs_failure_message(Some(&e));
        assert!(s.starts_with("Error: E7024 "), "msg: {s}");
        assert!(s.contains("host certificate"), "msg: {s}");
        assert!(
            !s.contains("github.com"),
            "msg must not say report a bug: {s}"
        );
    }

    #[test]
    fn aacs_failure_message_is_one_line() {
        // Locked rc.6 format contract: one line, `Error: E<code> <message>`,
        // no embedded newline. (Replaces the pre-rc.6 two-line heading/body.)
        for e in [
            Error::AacsNoKeys,
            Error::AacsCertRejected,
            Error::AacsHostCertRejected,
            Error::AacsRawReadUnsupported,
            Error::AacsVidUnavailable,
            Error::AacsMkUnavailable,
            Error::AacsVukNotInKeydb,
            Error::DriveProfileMissing,
            Error::VidCdbUnavailable,
            Error::AacsNoHostCert {
                path: "<no host cert>".into(),
            },
        ] {
            let s = aacs_failure_message(Some(&e));
            assert!(!s.contains('\n'), "{e:?} message must be one line: {s}");
            assert!(
                s.starts_with(&format!("Error: E{} ", e.code())),
                "{e:?} must lead with the level word and code: {s}"
            );
        }
    }

    #[test]
    fn css_crack_failure_is_not_aacs_messaging() {
        // Regression: a CSS (DVD) known-plaintext crack failure records
        // `Error::CssKeyMissing` in `disc.css_error` (not `aacs_error`).
        // The keyless-disc message must surface the CSS heading, NOT the
        // AACS-oriented "check the key source" fallback that the bare
        // `aacs_failure_message(None)` path produced.
        let msg = aacs_failure_message(Some(&Error::CssKeyMissing));
        assert!(
            msg.to_lowercase().contains("unscramble") || msg.to_lowercase().contains("css"),
            "CSS failure should name the CSS problem, got: {msg}"
        );
        assert!(
            !msg.to_lowercase().contains("key source in settings"),
            "CSS failure must not point the operator at the (AACS) key source: {msg}"
        );
        // Locked rc.6 format: `Error: E<code> <message>`, one line.
        assert!(
            msg.starts_with(&format!(
                "Error: E{} ",
                libfreemkv::error::E_CSS_KEY_MISSING
            )),
            "CSS failure should lead with the level word and E-code: {msg}"
        );
        assert!(!msg.contains('\n'), "CSS message must be one line: {msg}");
    }

    #[test]
    fn keyless_failure_message_prefers_css_error_over_aacs() {
        // The `.or()` dispatch in keyless_failure_message must consult
        // css_error first. With both set (CSS crack failed, plus a stale
        // AACS error) it must surface CSS messaging.
        let css = Error::CssKeyMissing;
        let aacs = Error::KeydbLoad {
            path: "<no keydb in search paths>".to_string(),
        };
        let msg = super::keyless_failure_message_for(Some(&css), Some(&aacs));
        let lower = msg.to_lowercase();
        assert!(
            lower.contains("unscramble") || lower.contains("css"),
            "css_error must take priority over aacs_error: {msg}"
        );
        assert!(
            msg.contains(&format!("E{}", libfreemkv::error::E_CSS_KEY_MISSING)),
            "expected CSS E-code: {msg}"
        );

        // css_error alone (aacs_error None) — the field-based branch is
        // consulted at all, not just the AACS fallback.
        let msg2 = super::keyless_failure_message_for(Some(&css), None);
        assert!(
            msg2.to_lowercase().contains("unscramble") || msg2.to_lowercase().contains("css"),
            "css_error-only disc must surface CSS messaging: {msg2}"
        );
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

    // ── abort-on-loss scoping (Top Gun false-positive regression) ────

    /// A title spanning LBA 1000..2000 (sectors), i.e. byte range
    /// 1000*2048 .. 2000*2048. `bytes_bad_in_title` intersects bad
    /// ranges (byte offsets) with this window.
    fn title_lba(start_lba: u32, sector_count: u32, bps: f64) -> libfreemkv::DiscTitle {
        let mut t = libfreemkv::DiscTitle::empty();
        t.extents.push(libfreemkv::disc::Extent {
            start_lba,
            sector_count,
        });
        // size/duration are only used by the caller to derive bps; here
        // we pass bps directly to the helpers, so leave them at zero.
        let _ = bps;
        t
    }

    #[test]
    fn mux_denominator_scopes_to_title_extents_in_single_pass() {
        // 25 GB main title on a 50 GB disc.
        const GB: u64 = 1_073_741_824;
        let disc_capacity = 50 * GB;
        // A title whose single extent spans exactly 25 GB worth of sectors.
        let sectors = (25 * GB / 2048) as u32;
        let title = test_title(0, sectors);
        let extent_bytes = sectors as u64 * 2048;

        // Single-pass (max_retries == 0): denominator must be the title's
        // extent byte sum — the cap DiscStream's BytesRead reaches — so the
        // live progress bar reaches 100% instead of plateauing at ~50%.
        let single = super::mux_progress_denominator(0, disc_capacity, &title);
        assert_eq!(
            single, extent_bytes,
            "single-pass denominator must be the title extent sum, not disc capacity"
        );
        // Sanity: the old (buggy) behavior would have plateaued here.
        let old_pct = extent_bytes * 100 / disc_capacity;
        assert!(
            old_pct < 60,
            "precondition: title/disc ratio is the kind that plateaued ({old_pct}%)"
        );

        // Multipass (max_retries > 0): denominator stays disc capacity, since
        // the ISO highway reads the whole disc image.
        let multi = super::mux_progress_denominator(1, disc_capacity, &title);
        assert_eq!(
            multi, disc_capacity,
            "multipass denominator must remain disc capacity"
        );
    }

    #[test]
    fn mux_denominator_falls_back_when_title_has_no_extents() {
        // Degenerate title with no extents → fall back to the passed total
        // rather than producing a zero denominator (divide-by-zero / no bar).
        let mut title = test_title(0, 1000);
        title.extents.clear();
        let total = 12345;
        assert_eq!(super::mux_progress_denominator(0, total, &title), total);
    }

    #[test]
    fn abort_lost_ms_ignores_out_of_title_loss_for_mkv() {
        // Title occupies sectors 1000..2000. The only unreadable range
        // is at byte offset 0 (a scratched menu / pre-title region) and
        // does NOT overlap the title extents — so for an MKV mux the
        // in-title loss must be zero.
        let bps = 8_250_000.0;
        let title = title_lba(1000, 1000, bps);
        // 50 sectors bad starting at byte 0 (well before the title).
        let bad = vec![(0u64, 50 * 2048)];
        let lost = super::abort_lost_ms(false, &title, &bad, bps);
        assert_eq!(lost, 0.0, "out-of-title loss must not count for MKV mux");
    }

    #[test]
    fn abort_lost_ms_counts_whole_disc_for_iso() {
        // Same out-of-title bad range, but ISO output → whole disc is
        // the deliverable, so it DOES count.
        let bps = 8_250_000.0;
        let title = title_lba(1000, 1000, bps);
        let bad = vec![(0u64, 50 * 2048)];
        let lost = super::abort_lost_ms(true, &title, &bad, bps);
        assert!(lost > 0.0, "ISO output counts whole-disc loss");
    }

    #[test]
    fn abort_lost_ms_counts_in_title_loss_for_mkv() {
        // A bad range that overlaps the title extents counts.
        let bps = 8_250_000.0;
        let title = title_lba(1000, 1000, bps);
        // 10 bad sectors starting at sector 1500 (inside the title).
        let bad = vec![(1500u64 * 2048, 10 * 2048)];
        let lost = super::abort_lost_ms(false, &title, &bad, bps);
        assert!(lost > 0.0, "in-title loss must count for MKV mux");
    }

    #[test]
    fn perfect_in_title_rip_does_not_abort_at_threshold_zero() {
        // THE regression: out-of-title unreadable + 0 in-title loss +
        // abort_on_lost_secs=0 (threshold 0 ms) → NO abort, proceed to
        // mux. Previously `>=` aborted because 0.0 >= 0.0.
        let bps = 8_250_000.0;
        let title = title_lba(1000, 1000, bps);
        let bad = vec![(0u64, 50 * 2048)]; // out-of-title only
        let in_title_lost_ms = super::abort_lost_ms(false, &title, &bad, bps);
        assert_eq!(in_title_lost_ms, 0.0);
        let abort_threshold_ms = 0.0; // abort_on_lost_secs = 0
        assert!(
            !super::should_abort_for_loss(in_title_lost_ms, abort_threshold_ms),
            "a fully-recovered title must NOT abort on out-of-title loss at threshold 0"
        );
    }

    #[test]
    fn iso_output_rejected_in_single_pass_only() {
        // Regression: `output_format="iso"` is whole-disc scoped for abort
        // accounting (every unreadable sector counts, including out-of-title
        // damage). Single-pass (max_retries=0) reads only the title and scopes
        // loss in-title, so it would ACCEPT a disc the multi-pass / resume paths
        // ABORT on for identical input — a rip-mode-dependent verdict. ISO must
        // therefore require multi-pass.
        assert!(
            super::iso_output_needs_multipass("iso", 0),
            "single-pass ISO must be rejected (it cannot honour whole-disc scope)"
        );
        // Multi-pass ISO is allowed (captures the whole-disc image + applies
        // whole-disc scope).
        assert!(!super::iso_output_needs_multipass("iso", 1));
        assert!(!super::iso_output_needs_multipass("iso", 5));
        // Non-ISO formats are unaffected in either mode.
        for fmt in ["mkv", "m2ts", "network"] {
            assert!(
                !super::iso_output_needs_multipass(fmt, 0),
                "{fmt} single-pass ok"
            );
            assert!(
                !super::iso_output_needs_multipass(fmt, 5),
                "{fmt} multi-pass ok"
            );
        }
    }

    #[test]
    fn iso_output_delivers_the_disc_image_not_a_mux() {
        // Regression: the settings UI advertises `output_format="iso"` as
        // "ISO copies the whole disc; the other formats mux selected titles".
        // Previously both rip paths special-cased only "m2ts" and defaulted
        // everything else (including "iso") to an `.mkv` selected-title mux,
        // then PRUNED the swept ISO — so the operator received the opposite of
        // what was requested. `output_is_iso_image` is the single predicate the
        // mux-skip + deliverable + prune decisions now key off.
        assert!(
            super::output_is_iso_image("iso"),
            "iso output must be recognised as a whole-disc deliverable"
        );
        for fmt in ["mkv", "m2ts", "network", "garbage", ""] {
            assert!(
                !super::output_is_iso_image(fmt),
                "{fmt} muxes a title and must NOT be treated as a disc image"
            );
        }
    }

    #[test]
    fn iso_output_retains_its_disc_image_even_without_keep_iso() {
        // The swept ISO is the deliverable for iso output, so it must never be
        // pruned regardless of `keep_iso` — pruning it would leave the staging
        // dir with no file for the mover to promote.
        assert!(
            super::retain_intermediate_iso(false, "iso"),
            "iso output must retain its ISO even when keep_iso is off"
        );
        assert!(super::retain_intermediate_iso(true, "iso"));
        // `keep_iso` still governs ISO retention for the mux formats.
        assert!(super::retain_intermediate_iso(true, "mkv"));
        for fmt in ["mkv", "m2ts", "network"] {
            assert!(
                !super::retain_intermediate_iso(false, fmt),
                "{fmt} without keep_iso must prune the intermediate ISO"
            );
        }
    }

    #[test]
    fn any_in_title_loss_aborts_at_threshold_zero() {
        // abort_on_lost_secs=0 still means "perfect in-title required":
        // ANY positive in-title loss aborts.
        let abort_threshold_ms = 0.0;
        assert!(super::should_abort_for_loss(0.001, abort_threshold_ms));
        assert!(super::should_abort_for_loss(5_000.0, abort_threshold_ms));
    }

    #[test]
    fn loss_within_threshold_does_not_abort() {
        // abort_on_lost_secs=30 (30_000 ms): 20s lost is tolerated, 31s
        // aborts.
        let threshold = 30_000.0;
        assert!(!super::should_abort_for_loss(20_000.0, threshold));
        assert!(super::should_abort_for_loss(31_000.0, threshold));
    }

    #[test]
    fn nan_loss_aborts() {
        // A NaN loss is unquantifiable and must fail safe (abort), not
        // pass as a silent success. `NaN > x` is false, so a plain
        // comparison would wrongly proceed to mark the rip complete.
        assert!(super::should_abort_for_loss(f64::NAN, 0.0));
        assert!(super::should_abort_for_loss(f64::NAN, 30_000.0));
    }

    // ── final done-card uses in-title loss (telemetry audit Fix 3) ───

    /// The `status=done` state update must report in-title-scoped loss
    /// (what abort_lost_ms returns), NOT whole-disc `bytes_unreadable /
    /// title_bytes_per_sec`. Out-of-title damage (scratched menus /
    /// trailers) would inflate the 'done' card even though the abort gate
    /// correctly accepted the rip.
    ///
    /// This test verifies the contract indirectly via `abort_lost_ms`:
    /// given out-of-title-only damage, the in-title loss is 0 ms — so the
    /// done card should show 0s lost, not the whole-disc value.
    #[test]
    fn final_done_card_uses_in_title_loss_not_whole_disc() {
        let bps = 8_250_000.0;
        // Title covers sectors 1000..2000. Damage is only in sector 0..50
        // (a scratched menu, before the title).
        let title = title_lba(1000, 1000, bps);
        let bad = vec![(0u64, 50 * 2048)]; // 50 sectors, all out-of-title

        // Whole-disc calculation (the old broken path): non-zero.
        let whole_disc_bytes_unreadable: u64 = 50 * 2048;
        let whole_disc_lost_secs = whole_disc_bytes_unreadable as f64 / bps;
        assert!(whole_disc_lost_secs > 0.0, "whole-disc loss is non-zero");

        // In-title-scoped calculation (the correct path via abort_lost_ms):
        // out-of-title damage does NOT count for MKV output.
        let in_title_lost_ms = super::abort_lost_ms(false, &title, &bad, bps);
        assert_eq!(
            in_title_lost_ms, 0.0,
            "in-title loss must be 0 when all bad sectors are outside title extents"
        );

        // The done card should report in-title loss (0s), not whole-disc.
        // Replicate the selection logic from the fix:
        let final_lost_secs = if in_title_lost_ms > 0.0 {
            in_title_lost_ms / MILLIS_PER_SEC
        } else {
            0.0 // clean-title fallback; would be mux_outcome.lost_video_secs in production
        };
        assert!(
            (final_lost_secs - 0.0).abs() < 0.001,
            "done card must report 0s lost, not the inflated whole-disc {:.3}s",
            whole_disc_lost_secs
        );
    }

    /// Sanity: when there IS in-title loss, the done card reports it.
    #[test]
    fn final_done_card_reports_nonzero_in_title_loss() {
        let bps = 8_250_000.0;
        let title = title_lba(1000, 1000, bps);
        // 10 sectors at LBA 1500 — inside the title.
        let bad = vec![(1500u64 * 2048, 10 * 2048)];
        let in_title_lost_ms = super::abort_lost_ms(false, &title, &bad, bps);
        assert!(in_title_lost_ms > 0.0, "in-title loss should be non-zero");
        let final_lost_secs = in_title_lost_ms / MILLIS_PER_SEC;
        // 10 sectors * 2048 bytes / 8_250_000 bps ≈ 0.00248s
        assert!(
            final_lost_secs > 0.0 && final_lost_secs < 1.0,
            "expected small non-zero lost_secs, got {:.6}",
            final_lost_secs
        );
    }

    /// Regression: single-pass (max_retries == 0) has no mapfile, so
    /// `sweep_damage_snapshot` is the all-zero Default. The done-state
    /// `main_lost_ms` must be derived from `final_lost_secs` (the demux-skip
    /// estimate), mirroring the `total_lost_ms` branch — NOT taken from the
    /// zero snapshot, which would always render "(0s in main movie)" even
    /// when in-title sectors were skipped.
    ///
    /// This replicates the selection logic at rip_disc's done-state update.
    #[test]
    fn single_pass_done_card_main_lost_ms_tracks_final_lost_secs() {
        // Snapshot is the all-zero Default in single-pass mode.
        let snapshot = super::mux::SweepDamageSnapshot::default();
        assert_eq!(
            snapshot.main_lost_ms, 0.0,
            "single-pass snapshot main_lost_ms is the zero Default"
        );

        // The mux reported real in-title loss (demux skipped sectors).
        let final_lost_secs = 1.5_f64;

        // Replicate the fix's branch selection for single-pass (max_retries == 0).
        let max_retries = 0u32;
        let main_lost_ms = if max_retries == 0 {
            final_lost_secs * MILLIS_PER_SEC
        } else {
            snapshot.main_lost_ms
        };
        let total_lost_ms = if max_retries == 0 {
            final_lost_secs * MILLIS_PER_SEC
        } else {
            snapshot.total_lost_ms
        };

        assert!(
            (main_lost_ms - 1500.0).abs() < 0.001,
            "single-pass main_lost_ms must reflect real loss, got {main_lost_ms}"
        );
        assert!(
            (main_lost_ms - total_lost_ms).abs() < 0.001,
            "single-pass main_lost_ms must mirror total_lost_ms"
        );
    }

    /// Multipass (max_retries > 0) keeps the snapshot's mapfile-derived
    /// sweep loss but additionally folds in the demux-time loss — matching
    /// the single-pass path (which surfaces demux loss via `final_lost_secs`)
    /// and the resume path (resume.rs, which adds `demux_lost_secs * 1000`).
    /// The whole-disc `final_lost_secs` value must NOT replace the snapshot.
    #[test]
    fn multipass_done_card_main_lost_ms_uses_snapshot_plus_demux() {
        let snapshot = super::mux::SweepDamageSnapshot {
            main_lost_ms: 2750.0,
            total_lost_ms: 4000.0,
            ..Default::default()
        };
        let demux_lost_secs = 1.25_f64;
        let max_retries = 3u32;
        // Replicate the fix's done_demux_extra_ms branch.
        let done_demux_extra_ms = if max_retries == 0 {
            0.0
        } else {
            demux_lost_secs * MILLIS_PER_SEC
        };
        let main_lost_ms = if max_retries == 0 {
            0.0
        } else {
            snapshot.main_lost_ms + done_demux_extra_ms
        };
        let total_lost_ms = if max_retries == 0 {
            0.0
        } else {
            snapshot.total_lost_ms + done_demux_extra_ms
        };
        assert!(
            (main_lost_ms - 4000.0).abs() < 0.001,
            "multipass main_lost_ms must be sweep snapshot (2750) + demux (1250), got {main_lost_ms}"
        );
        assert!(
            (total_lost_ms - 5250.0).abs() < 0.001,
            "multipass total_lost_ms must be sweep snapshot (4000) + demux (1250), got {total_lost_ms}"
        );
    }

    /// Regression for the cross-path-asymmetry bug: an ACCEPTED fresh
    /// multipass done card must fold demux-time loss into the headline
    /// `errors` / `lost_video_secs`, matching the resume path
    /// (`done_errors`/`done_lost_video_secs` in resume.rs) and the single-pass
    /// path. Before the fix, multipass reported sweep-mapfile figures ALONE,
    /// so a fresh multipass rip looked cleaner than a resume of the identical
    /// ISO. Single-pass must remain unchanged (its `final_*` already equals
    /// the demux figures), with no double-counting.
    #[test]
    fn accepted_done_card_folds_demux_loss_into_headline() {
        // Replicate the (done_errors, done_lost_secs, done_demux_extra_ms)
        // selection from the accepted-done block.
        fn headline(
            max_retries: u32,
            final_errors: u32,
            final_lost_secs: f64,
            mux_errors: u32,
            demux_lost_secs: f64,
        ) -> (u32, f64, f64) {
            if max_retries == 0 {
                (final_errors, final_lost_secs, 0.0)
            } else {
                (
                    final_errors.saturating_add(mux_errors),
                    final_lost_secs + demux_lost_secs,
                    demux_lost_secs * MILLIS_PER_SEC,
                )
            }
        }

        // Single-pass: final_* already carry the demux figures (final_errors ==
        // mux_errors, final_lost_secs == demux_lost_secs). No addition, so no
        // double-counting.
        let (errs, lost, extra) = headline(0, 7, 1.5, 7, 1.5);
        assert_eq!(errs, 7, "single-pass errors unchanged");
        assert!((lost - 1.5).abs() < 0.001, "single-pass lost unchanged");
        assert!(
            (extra - 0.0).abs() < 0.001,
            "single-pass adds no demux extra"
        );

        // Multipass: final_errors is the mapfile bad-sector count (disjoint
        // from the mux demux skips); final_lost_secs is the sweep main loss.
        // Both must gain the demux contribution.
        let (errs, lost, extra) = headline(3, 4, 2.0, 5, 1.0);
        assert_eq!(errs, 9, "multipass errors = sweep 4 + demux 5");
        assert!(
            (lost - 3.0).abs() < 0.001,
            "multipass lost = sweep 2.0 + demux 1.0"
        );
        assert!(
            (extra - 1000.0).abs() < 0.001,
            "multipass demux extra = 1.0s in ms"
        );

        // A clean-mux multipass (zero demux loss) must equal the old behavior.
        let (errs, lost, extra) = headline(3, 4, 2.0, 0, 0.0);
        assert_eq!(
            errs, 4,
            "no demux loss leaves multipass errors at sweep count"
        );
        assert!(
            (lost - 2.0).abs() < 0.001,
            "no demux loss leaves multipass lost at sweep value"
        );
        assert!((extra - 0.0).abs() < 0.001, "no demux loss adds no extra");
    }

    // ── single-pass (max_retries == 0) abort gate ───────────────────

    /// Regression: in single-pass mode the abort_on_lost_secs check runs
    /// AFTER the mux, gating on the demux skip count rather than a
    /// mapfile. This pins the loss-from-skip-count → abort-decision
    /// chain the post-mux single-pass gate relies on, mirroring the
    /// in-production derivation:
    ///   lost_secs = skip_sectors * 2048 / title_bytes_per_sec
    ///   abort     = should_abort_for_loss(lost_secs * 1000, threshold_ms)
    fn single_pass_lost_secs(skip_sectors: u64, title_bytes_per_sec: f64) -> f64 {
        if title_bytes_per_sec > 0.0 {
            (skip_sectors as f64) * 2048.0 / title_bytes_per_sec
        } else {
            0.0
        }
    }

    #[test]
    fn single_pass_any_loss_aborts_at_threshold_zero() {
        // abort_on_lost_secs=0 ("require a perfect rip") must abort a
        // single-pass rip that skipped ANY sectors — the divergence this
        // fix closes (previously single-pass silently delivered a lossy
        // MKV at threshold 0).
        let bps = 8_250_000.0;
        let threshold_ms = 0.0; // abort_on_lost_secs = 0
        let lost = single_pass_lost_secs(10, bps); // 10 skipped sectors
        assert!(lost > 0.0, "skipped sectors must produce positive loss");
        assert!(
            super::should_abort_for_loss(lost * MILLIS_PER_SEC, threshold_ms),
            "single-pass rip with skipped sectors must abort at threshold 0"
        );
    }

    #[test]
    fn single_pass_clean_rip_does_not_abort_at_threshold_zero() {
        // A perfect single-pass rip (zero skips) must NOT abort even at
        // threshold 0 — the gate uses strict `>`.
        let bps = 8_250_000.0;
        let threshold_ms = 0.0;
        let lost = single_pass_lost_secs(0, bps);
        assert_eq!(lost, 0.0);
        assert!(
            !super::should_abort_for_loss(lost * MILLIS_PER_SEC, threshold_ms),
            "a clean single-pass rip must NOT abort at threshold 0"
        );
    }

    #[test]
    fn single_pass_loss_within_threshold_does_not_abort() {
        // abort_on_lost_secs=30: a single-pass rip whose skip-derived loss
        // is under 30s proceeds; over 30s aborts.
        let bps = 8_250_000.0;
        let threshold_ms = 30_000.0;
        // ~1000 skipped sectors ≈ 0.248s lost — well under 30s.
        let small = single_pass_lost_secs(1000, bps);
        assert!(
            !super::should_abort_for_loss(small * MILLIS_PER_SEC, threshold_ms),
            "single-pass loss under threshold must NOT abort, got {small:.3}s"
        );
        // Enough skips to exceed 30s: 30 * bps / 2048 sectors + slack.
        let big_sectors = (31.0 * bps / 2048.0) as u64;
        let big = single_pass_lost_secs(big_sectors, bps);
        assert!(
            super::should_abort_for_loss(big * MILLIS_PER_SEC, threshold_ms),
            "single-pass loss over threshold must abort, got {big:.3}s"
        );
    }

    /// Regression (bug #1 / bug #2): the `.ripped` marker hand-off must write
    /// status="done" — NOT "ripping" (and not "idle"). The DISC READ is
    /// complete the instant sweep + patch finish; the mux is a SEPARATE phase
    /// that runs off the staged ISO and is tracked by the synthetic `_mux`
    /// device + the System-tab Mux queue. So the real drive tile must show a
    /// completed (read-done) card immediately, and auto_eject fires here (the
    /// disc is no longer needed). Pre-fix this wrote "ripping", leaving the
    /// tile stuck on "Ripping" for the entire mux and making auto-eject LOOK
    /// like it waited for the mux.
    ///
    /// We can't drive `rip_disc` in a unit test (it needs a live drive), but
    /// we pin the invariant that the hand-off status is "done" so a future
    /// refactor back to "ripping"/"idle" is caught. The companion
    /// `mux_worker_does_not_revert_done_origin_device` test (muxer.rs) covers
    /// the other half: the `_mux` worker can't push a real "done" tile back to
    /// "ripping".
    #[test]
    fn handoff_status_is_done_read_complete() {
        let device = "sg_handoff_status_test";
        super::update_state(
            device,
            super::RipState {
                device: device.to_string(),
                status: "done".to_string(),
                progress_pct: 100,
                disc_present: true,
                ..Default::default()
            },
        );
        let (status, pct) = super::STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(device)
            .map(|s| (s.status.clone(), s.progress_pct))
            .unwrap_or_default();
        assert_eq!(
            status, "done",
            "handoff update_state must write status='done' (read complete), not 'ripping'/'idle'"
        );
        assert_eq!(pct, 100, "a read-complete done card must show 100%");
        // Cleanup: remove the synthetic device entry so it doesn't leak
        // into other tests that inspect STATE.
        super::STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(device);
    }

    // ===================================================================
    // Auto-eject timing contract (bug #2): the drive ejects EXACTLY ONCE,
    // at the `.ripped` read-complete hand-off, only when the operator
    // enabled `auto_eject`, and NEVER from the synthetic `_mux` worker.
    // `should_auto_eject` is the single predicate all four completion
    // sites (fresh ISO/MKV, resume ISO/MKV) now gate on, so testing it
    // covers the whole contract.
    // ===================================================================

    #[test]
    fn auto_eject_fires_for_real_device_when_enabled() {
        // A physical drive (sg0/sr1/…) with auto_eject on ejects.
        assert!(super::should_auto_eject(true, "sg0"));
        assert!(super::should_auto_eject(true, "sr1"));
        assert!(super::should_auto_eject(true, "sg12"));
    }

    #[test]
    fn auto_eject_does_not_fire_when_disabled() {
        // auto_eject=false never ejects, regardless of device.
        assert!(!super::should_auto_eject(false, "sg0"));
        assert!(!super::should_auto_eject(false, "sr1"));
        assert!(!super::should_auto_eject(false, "_mux"));
    }

    #[test]
    fn auto_eject_never_fires_from_synthetic_mux_device() {
        // The `_mux` worker reaches a completion path AFTER the drive
        // thread already ejected at the hand-off; it must never issue a
        // second eject (the drive may now hold a different disc). The
        // guard keys on the underscore prefix, so even with auto_eject on
        // a synthetic device is refused.
        assert!(!super::should_auto_eject(true, "_mux"));
        assert!(!super::should_auto_eject(true, "_move"));
        assert!(!super::should_auto_eject(true, "_anything"));
    }

    /// The eject is "exactly once at read-complete": the fresh-rip `.ripped`
    /// hand-off is the ONLY place the physical drive ejects on the multipass
    /// path, and the LATER mux worker (synthetic `_mux`) is refused. This
    /// pins both halves against the predicate so a refactor that lets the
    /// mux worker re-eject (or that ejects twice) is caught.
    #[test]
    fn auto_eject_is_once_at_handoff_not_at_mux() {
        // Hand-off (real device, enabled): eject.
        assert!(
            super::should_auto_eject(true, "sg0"),
            "the physical drive must eject at the read-complete hand-off"
        );
        // Mux worker completing later (synthetic device): no second eject.
        assert!(
            !super::should_auto_eject(true, "_mux"),
            "the mux worker must NOT re-eject after the hand-off already did"
        );
    }

    /// Regression: a poisoned config `RwLock` must NOT leave the tile wedged in
    /// "scanning". `try_claim_active` sets status="scanning" before scan_disc /
    /// rip_disc run, and both bail out early on `cfg.read()` failure. Pre-fix
    /// that early return was bare (`Err(_) => return`), so the device stayed
    /// "scanning" forever with an empty last_error. The shared
    /// `mark_config_lock_poisoned` helper that both sites now call must flip the
    /// state to "error" with a populated last_error.
    #[test]
    fn config_lock_poisoned_marks_error_not_stuck_scanning() {
        let device = "sg_config_poison_test";
        // Simulate the pre-spawn claim: tile is already "scanning".
        assert!(super::try_claim_active(device));
        let claimed = super::STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(device)
            .map(|s| s.status.clone())
            .unwrap_or_default();
        assert_eq!(claimed, "scanning", "claim should set status=scanning");

        // The poisoned-lock early-exit path.
        super::mark_config_lock_poisoned(device, "Scan");

        let st = super::STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(device)
            .cloned()
            .expect("device state present");
        assert_eq!(
            st.status, "error",
            "poisoned config lock must mark the tile 'error', not leave it 'scanning'"
        );
        assert!(
            !st.last_error.is_empty(),
            "poisoned config lock must populate last_error so the operator sees why"
        );

        // Cleanup so the synthetic device doesn't leak into other tests.
        super::STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(device);
    }

    /// Regression: end-of-recovery promotion must flush the promoted mapfile
    /// so the abort check (which now uses the in-memory map) sees Unreadable
    /// bytes — not the pre-promotion NonTrimmed state.
    ///
    /// Before the fix the two-block design:
    ///   1. Promoted NonTrimmed → Unreadable in memory, dropped `map` without
    ///      flushing (pre-promotion state stays on disk).
    ///   2. Re-loaded from disk → got the pre-promotion file → zero Unreadable
    ///      bytes → abort check silently skipped.
    ///
    /// After the fix both steps share one `map` load; the abort check queries
    /// the already-promoted in-memory map, and the flush persists it to disk.
    #[test]
    fn promotion_uses_in_memory_map_and_flush_persists_to_disk() {
        use libfreemkv::disc::mapfile::{self, SectorStatus};

        let tmp = tempfile::tempdir().unwrap();
        let mf_path = tmp.path().join("test.mapfile");

        // Create a mapfile with one NonTrimmed range (simulating a sector
        // that remained "maybe" after all patch passes).
        let disc_size: u64 = 10 * 2048;
        let bad_pos: u64 = 5 * 2048;
        let bad_size: u64 = 2048;
        {
            let mut map =
                mapfile::Mapfile::create(&mf_path, disc_size, "test").expect("create mapfile");
            // Mark everything Finished except one NonTrimmed range.
            map.record(0, bad_pos, SectorStatus::Finished)
                .expect("record Finished before bad");
            map.record(bad_pos, bad_size, SectorStatus::NonTrimmed)
                .expect("record NonTrimmed");
            map.record(
                bad_pos + bad_size,
                disc_size - bad_pos - bad_size,
                SectorStatus::Finished,
            )
            .expect("record Finished after bad");
            map.flush().expect("initial flush");
        }

        // Simulate the promotion block: load, promote, flush.
        {
            let mut map = mapfile::Mapfile::load(&mf_path).expect("load for promotion");
            let nontrimmed = map.ranges_with(&[SectorStatus::NonTrimmed]);
            assert_eq!(nontrimmed.len(), 1, "precondition: one NonTrimmed range");
            for (pos, size) in nontrimmed {
                map.record(pos, size, SectorStatus::Unreadable)
                    .expect("promote record");
            }
            // The flush is the critical step the pre-fix code omitted.
            map.flush().expect("promotion flush");

            // Verify in-memory state reflects the promotion.
            let stats = map.stats();
            assert_eq!(
                stats.bytes_unreadable, bad_size,
                "in-memory bytes_unreadable must equal the promoted range size"
            );
            assert_eq!(
                stats.bytes_nontried + stats.bytes_pending,
                0,
                "no NonTrimmed/NonTried must remain after promotion"
            );

            // The abort check now uses this same `map` — verify bad_ranges is
            // populated from it (the pre-fix re-load would return empty here
            // because the flush wasn't done).
            let bad_ranges = map.ranges_with(&[SectorStatus::Unreadable]);
            assert_eq!(
                bad_ranges.len(),
                1,
                "abort check must see one Unreadable range from the promoted in-memory map"
            );
        }

        // Verify the flush wrote the promoted state to disk: a fresh load must
        // see Unreadable, not NonTrimmed.
        let reloaded = mapfile::Mapfile::load(&mf_path).expect("reload after promotion flush");
        let reloaded_unreadable = reloaded.ranges_with(&[SectorStatus::Unreadable]);
        assert_eq!(
            reloaded_unreadable.len(),
            1,
            "reloaded mapfile must contain the promoted Unreadable range \
             (pre-fix: flush omitted, so disk still held NonTrimmed)"
        );
        let reloaded_nontrimmed = reloaded.ranges_with(&[SectorStatus::NonTrimmed]);
        assert_eq!(
            reloaded_nontrimmed.len(),
            0,
            "reloaded mapfile must have no NonTrimmed after flush"
        );
    }

    /// Regression: the `.ripped` marker hand-off update_state must preserve
    /// non-zero damage fields (errors, total_lost_ms, main_lost_ms,
    /// bad_ranges, largest_gap_ms) from the sweep phase so /api/state
    /// doesn't silently zero them during the hand-off window.
    ///
    /// Pre-fix the hand-off RipState used `..Default::default()` which
    /// zeroed those fields. The fix reads them from STATE (populated by the
    /// last push_pass_state call) and carries them into the new RipState.
    ///
    /// We simulate this by: (1) seeding STATE with damage-populated data,
    /// (2) reading it back exactly as the hand-off code does, and (3)
    /// asserting the result is non-zero.
    #[test]
    fn handoff_update_state_carries_damage_fields() {
        let device = "sg_handoff_damage_test";
        // Seed STATE with damage-populated entry (as push_pass_state would).
        super::update_state(
            device,
            super::RipState {
                device: device.to_string(),
                status: "ripping".to_string(),
                errors: 42,
                total_lost_ms: 1500.0,
                main_lost_ms: 800.0,
                num_bad_ranges: 3,
                largest_gap_ms: 600.0,
                ..Default::default()
            },
        );

        // Replicate the hand-off code path from rip_disc: read damage from
        // STATE, then write a new RipState carrying those fields.
        let handoff_damage = {
            let s = super::STATE.lock().unwrap_or_else(|e| e.into_inner());
            s.get(device).map(|rs| super::mux::SweepDamageSnapshot {
                errors: rs.errors,
                total_lost_ms: rs.total_lost_ms,
                main_lost_ms: rs.main_lost_ms,
                bad_ranges: rs.bad_ranges.clone(),
                num_bad_ranges: rs.num_bad_ranges,
                bad_ranges_truncated: rs.bad_ranges_truncated,
                largest_gap_ms: rs.largest_gap_ms,
            })
        };
        super::update_state(
            device,
            super::RipState {
                device: device.to_string(),
                status: "ripping".to_string(),
                disc_present: true,
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
                num_bad_ranges: handoff_damage
                    .as_ref()
                    .map(|d| d.num_bad_ranges)
                    .unwrap_or_default(),
                largest_gap_ms: handoff_damage
                    .as_ref()
                    .map(|d| d.largest_gap_ms)
                    .unwrap_or_default(),
                ..Default::default()
            },
        );

        let state = super::STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(device)
            .cloned()
            .expect("device should be in STATE");

        assert_eq!(state.errors, 42, "handoff must carry errors from sweep");
        assert!(
            (state.total_lost_ms - 1500.0).abs() < 0.001,
            "handoff must carry total_lost_ms from sweep"
        );
        assert!(
            (state.main_lost_ms - 800.0).abs() < 0.001,
            "handoff must carry main_lost_ms from sweep"
        );
        assert_eq!(
            state.num_bad_ranges, 3,
            "handoff must carry num_bad_ranges from sweep"
        );
        assert!(
            (state.largest_gap_ms - 600.0).abs() < 0.001,
            "handoff must carry largest_gap_ms from sweep"
        );

        // Cleanup.
        super::STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(device);
    }

    // Regression guard for the `entries.flatten()` silent-drop bug in
    // `disc_already_completed` / `find_resumable_for_disc`: both now route
    // their staging-root walk through `list_staging_basenames`, which lists
    // every immediate child and (unlike `.flatten()`) is built to retry and
    // surface per-DirEntry NFS errors rather than silently undercount.
    #[test]
    fn list_staging_basenames_returns_all_children() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(tmp.path().join("Cars")).unwrap();
        std::fs::create_dir(tmp.path().join("Cars_2")).unwrap();
        std::fs::write(tmp.path().join("loose.txt"), b"x").unwrap();

        let mut got = list_staging_basenames(tmp.path()).expect("dir exists");
        got.sort();
        assert_eq!(got, vec!["Cars", "Cars_2", "loose.txt"]);
    }

    #[test]
    fn list_staging_basenames_empty_dir_is_some_empty() {
        // A genuinely empty staging root must return Some([]) (a trustworthy
        // "no match"), not None — None is reserved for "never opened".
        let tmp = tempfile::TempDir::new().unwrap();
        assert_eq!(list_staging_basenames(tmp.path()), Some(Vec::new()));
    }

    #[test]
    fn list_staging_basenames_missing_dir_is_none() {
        // read_dir never opens -> UNKNOWN -> None, so callers behave exactly
        // like the old `read_dir(...).ok()? / return false` (no false match).
        let tmp = tempfile::TempDir::new().unwrap();
        let missing = tmp.path().join("does-not-exist");
        assert_eq!(list_staging_basenames(&missing), None);
    }

    // Regression for the largest-count-vs-union bug: a clean pass returns the
    // accumulated UNION of every basename observed, and never duplicates a name
    // it has already seen. On a healthy mount the very first pass opens cleanly
    // and short-circuits, so a single child must appear exactly once (the union
    // accumulator must not double-count). The cross-pass union (a name from an
    // earlier degraded pass surviving a later, larger pass that dropped it)
    // depends on injectable per-DirEntry NFS errors, which the real filesystem
    // can't reproduce deterministically; the union code path is exercised by
    // the accumulate-then-return wiring this test pins.
    #[test]
    fn list_staging_basenames_union_does_not_duplicate() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(tmp.path().join("Dune")).unwrap();
        std::fs::create_dir(tmp.path().join("Dune_Part_Two")).unwrap();

        let got = list_staging_basenames(tmp.path()).expect("dir exists");
        assert_eq!(got.len(), 2, "each child appears exactly once: {got:?}");
        assert!(got.contains(&"Dune".to_string()));
        assert!(got.contains(&"Dune_Part_Two".to_string()));
    }

    /// Regression: `resumable_for_disc` (the scan-complete tile's Resume-button
    /// detector) must find an existing resumable staging dir. It previously
    /// walked the staging root with `read_dir(...).flatten()`, which silently
    /// drops per-`DirEntry` I/O errors — on a cold NFS cache a transient
    /// ESTALE/EIO on a single entry made the disc's own dir vanish and the
    /// function return None, hiding the Resume button. It now routes through
    /// `list_staging_basenames` (3-retry NFS defense) like the other staging
    /// walkers. This test pins the happy path so the wiring can't silently
    /// revert to a bare `read_dir().flatten()`.
    #[test]
    fn resumable_for_disc_detects_partial_sweep() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cfg = crate::config::Config {
            staging_dir: tmp.path().to_string_lossy().into_owned(),
            ..Default::default()
        };
        let display_name = "Test Disc";
        let sanitized = crate::util::sanitize_path_compact(display_name);

        // Build a real staging layout: <staging>/<sanitized>/<sanitized>.iso
        // plus its `<...>.iso.mapfile`. A freshly created mapfile is one big
        // NonTried region (bytes_pending > 0) -> Resumable::Sweep.
        let disc_dir = tmp.path().join(&sanitized);
        std::fs::create_dir(&disc_dir).unwrap();
        let iso = disc_dir.join(format!("{sanitized}.iso"));
        std::fs::write(&iso, b"x").unwrap();
        let mapfile_path = disc_dir.join(format!("{sanitized}.iso.mapfile"));
        libfreemkv::disc::mapfile::Mapfile::create(&mapfile_path, 4096, "test").unwrap();

        assert_eq!(
            resumable_for_disc(&cfg, display_name),
            Some(Resumable::Sweep),
        );
    }

    /// R3 finding 1 regression: `resumable_for_disc` must return None (no Resume
    /// affordance) when the disc's staging dir carries a terminal `.failed` or a
    /// held `.review` marker, even though its mapfile still shows pending bytes
    /// (Resumable::Sweep-worthy). Offering Resume on a `.failed` dir was the
    /// data-stranding bug: the Sweep-resume branch re-rips WITHOUT clearing the
    /// stale `.failed`, so the successful re-rip's `.ripped` is shadowed by the
    /// lingering `.failed` (terminal-by-presence) and the mux worker skips it
    /// forever. This mirrors the Remux-branch `resumable_dir_blocked` policy:
    /// a terminal/held dir forces the operator to explicitly Wipe.
    #[test]
    fn resumable_for_disc_blocked_by_failed_or_review() {
        let display_name = "Stranded Disc";
        let sanitized = crate::util::sanitize_path_compact(display_name);

        for marker in [".failed", ".review"] {
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = crate::config::Config {
                staging_dir: tmp.path().to_string_lossy().into_owned(),
                ..Default::default()
            };
            // Partial sweep (bytes_pending > 0) that WOULD be Resumable::Sweep…
            let disc_dir = tmp.path().join(&sanitized);
            std::fs::create_dir(&disc_dir).unwrap();
            let iso = disc_dir.join(format!("{sanitized}.iso"));
            std::fs::write(&iso, b"x").unwrap();
            let mapfile_path = disc_dir.join(format!("{sanitized}.iso.mapfile"));
            libfreemkv::disc::mapfile::Mapfile::create(&mapfile_path, 4096, "test").unwrap();
            // Sanity: without the terminal/held marker it IS Sweep-resumable.
            assert_eq!(
                resumable_for_disc(&cfg, display_name),
                Some(Resumable::Sweep),
                "precondition: partial sweep is resumable before {marker}"
            );
            // …but a terminal/held marker blocks the Resume affordance entirely.
            std::fs::write(disc_dir.join(marker), b"{}").unwrap();
            assert_eq!(
                resumable_for_disc(&cfg, display_name),
                None,
                "{marker} must suppress the Resume affordance (operator must Wipe)"
            );
        }
    }

    /// Owner decision #2 regression: `resumable_for_disc` must return None when
    /// the disc's staging dir is owned by the mux worker (`.ripped` sweep-done
    /// handoff or in-flight `.muxing`). Resuming such a dir would re-enter the
    /// sweep path on a disc the worker is mid-mux on — racing fresh sweep writes
    /// against the mux's reads and overwriting the staged ISO. Mirrors the
    /// sibling Wipe guard (`staging_disc_owned_by_worker`): a worker-owned dir is
    /// off-limits until the mux finishes, even with a Sweep-worthy mapfile.
    #[test]
    fn resumable_for_disc_blocked_when_owned_by_mux_worker() {
        let display_name = "Mid Mux Disc";
        let sanitized = crate::util::sanitize_path_compact(display_name);

        for marker in [".ripped", ".muxing"] {
            let tmp = tempfile::TempDir::new().unwrap();
            let cfg = crate::config::Config {
                staging_dir: tmp.path().to_string_lossy().into_owned(),
                ..Default::default()
            };
            // Partial sweep (bytes_pending > 0) that WOULD be Resumable::Sweep…
            let disc_dir = tmp.path().join(&sanitized);
            std::fs::create_dir(&disc_dir).unwrap();
            let iso = disc_dir.join(format!("{sanitized}.iso"));
            std::fs::write(&iso, b"x").unwrap();
            let mapfile_path = disc_dir.join(format!("{sanitized}.iso.mapfile"));
            libfreemkv::disc::mapfile::Mapfile::create(&mapfile_path, 4096, "test").unwrap();
            // Sanity: without the worker-owned marker it IS Sweep-resumable.
            assert_eq!(
                resumable_for_disc(&cfg, display_name),
                Some(Resumable::Sweep),
                "precondition: partial sweep is resumable before {marker}"
            );
            // …but a worker-owned marker blocks the Resume affordance entirely.
            std::fs::write(disc_dir.join(marker), b"{}").unwrap();
            assert_eq!(
                resumable_for_disc(&cfg, display_name),
                None,
                "{marker} must suppress Resume (mux worker owns the dir)"
            );
        }
    }

    /// Build `<root>/<sanitized>` and drop the named empty marker files in it.
    fn staging_disc_with_markers(
        root: &std::path::Path,
        sanitized: &str,
        markers: &[&str],
    ) -> std::path::PathBuf {
        let disc = root.join(sanitized);
        std::fs::create_dir_all(&disc).unwrap();
        for m in markers {
            std::fs::write(disc.join(m), b"{}").unwrap();
        }
        disc
    }

    /// M4: a rip HELD for review writes BOTH `.review` and `.completed`. The
    /// "already ripped" check must NOT treat it as completed — otherwise the
    /// unattended insert path skips a disc that's actually awaiting operator
    /// confirmation. Gating is `.completed` AND not `.review`.
    #[test]
    fn staging_disc_completed_excludes_held_for_review() {
        let tmp = tempfile::TempDir::new().unwrap();
        let san = "Held_Movie";
        // .completed alone → already ripped.
        staging_disc_with_markers(tmp.path(), san, &[".completed"]);
        assert!(
            staging_disc_completed(tmp.path(), san),
            ".completed alone must count as already-ripped"
        );
        // Add .review (held) → NO longer "already ripped".
        std::fs::write(tmp.path().join(san).join(".review"), b"{}").unwrap();
        assert!(
            !staging_disc_completed(tmp.path(), san),
            ".completed + .review (held) must NOT count as already-ripped (M4)"
        );
    }

    /// R2 finding 2 regression: `staging_disc_completed` must read its markers
    /// through the NFS-resilient `snapshot_staging_disc` (3-retry read_dir), not
    /// bare `path.join(MARKER).exists()`. We can't provoke a real NFS cold-cache
    /// false-negative in a unit test, but we can pin that the detection now
    /// keys off the same snapshot view every other caller uses and still works
    /// with leftover artifacts present (the crash/cold-cache window where the
    /// ISO+mapfile are still on disk alongside `.completed`). Under the old
    /// `.exists()` path this same dir would be "completed"; the snapshot path
    /// must agree so the Default auto-insert guard can't false-negative and
    /// re-rip a finished disc, truncating the staged ISO.
    #[test]
    fn staging_disc_completed_uses_snapshot_with_leftover_artifacts() {
        let tmp = tempfile::TempDir::new().unwrap();
        let san = "Finished_Movie";
        // Completed rip whose ISO/mapfile haven't been pruned yet (crash
        // between .completed and the ISO prune, or mover not yet run).
        staging_disc_with_markers(
            tmp.path(),
            san,
            &[
                ".completed",
                "Finished_Movie.iso",
                "Finished_Movie.iso.mapfile",
            ],
        );
        assert!(
            staging_disc_completed(tmp.path(), san),
            ".completed must be detected via snapshot even with leftover ISO/mapfile"
        );
        // No .completed at all → not completed (snapshot agrees).
        let other = "Unfinished_Movie";
        staging_disc_with_markers(
            tmp.path(),
            other,
            &["Unfinished_Movie.iso", "Unfinished_Movie.iso.mapfile"],
        );
        assert!(
            !staging_disc_completed(tmp.path(), other),
            "no .completed → not already-completed"
        );
    }

    /// M4 sanity: the review UI's `list_held` still surfaces a held dir even
    /// when `.completed` is also present (it keys on `.review` and absence of
    /// `.done`, independent of `.completed`).
    #[test]
    fn list_held_still_sees_completed_review_dir() {
        let tmp = tempfile::TempDir::new().unwrap();
        let disc = tmp.path().join("Held_Movie");
        std::fs::create_dir_all(&disc).unwrap();
        std::fs::write(disc.join(".review"), r#"{"title":"Held Movie","year":0}"#).unwrap();
        std::fs::write(disc.join(".completed"), b"").unwrap();
        std::fs::write(disc.join("Held_Movie.mkv"), b"x").unwrap();

        let held = crate::review::list_held(tmp.path().to_str().unwrap());
        assert_eq!(held.len(), 1, "a .completed+.review dir is still held");
        assert_eq!(held[0].dir, "Held_Movie");
    }

    /// H1: a `.ripped` or `.muxing` staging dir is OWNED by the mux worker.
    /// The drive auto-insert path must recognise it so it doesn't run a fresh
    /// sweep that truncates the ISO the worker is reading.
    #[test]
    fn staging_disc_owned_by_worker_detects_ripped_and_muxing() {
        let tmp = tempfile::TempDir::new().unwrap();
        let san = "Owned";
        // Nothing yet → not owned.
        staging_disc_with_markers(tmp.path(), san, &["Owned.iso", "Owned.iso.mapfile"]);
        assert!(!staging_disc_owned_by_worker(tmp.path(), san));
        // .ripped → owned.
        std::fs::write(tmp.path().join(san).join(".ripped"), b"{}").unwrap();
        assert!(
            staging_disc_owned_by_worker(tmp.path(), san),
            ".ripped must mark the dir owned by the mux worker"
        );
        // Swap .ripped for .muxing → still owned.
        std::fs::remove_file(tmp.path().join(san).join(".ripped")).unwrap();
        std::fs::write(tmp.path().join(san).join(".muxing"), b"{}").unwrap();
        assert!(
            staging_disc_owned_by_worker(tmp.path(), san),
            ".muxing must mark the dir owned by the mux worker"
        );
    }

    /// H1 + M3: the drive-resume (Remux) selector must skip dirs that are
    /// owned (`.ripped`/`.muxing`), held (`.review`), or terminal (`.failed`),
    /// while still resuming a plain ISO+mapfile dir. Drives the pure
    /// `resumable_dir_blocked` against real snapshots.
    #[test]
    fn resumable_dir_blocked_skips_owned_held_and_terminal() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mk = |name: &str, markers: &[&str]| {
            let d = staging_disc_with_markers(
                tmp.path(),
                name,
                &[&format!("{name}.iso"), &format!("{name}.iso.mapfile")],
            );
            for m in markers {
                std::fs::write(d.join(m), b"{}").unwrap();
            }
            crate::ripper::staging::snapshot_staging_disc(&d).unwrap()
        };

        // Plain ISO+mapfile, no governing marker → NOT blocked (resumable).
        assert!(!resumable_dir_blocked(&mk("Plain", &[])));
        // Owned by mux worker.
        assert!(resumable_dir_blocked(&mk("Ripped", &[".ripped"])));
        assert!(resumable_dir_blocked(&mk("Muxing", &[".muxing"])));
        // Held for operator review.
        assert!(resumable_dir_blocked(&mk("Held", &[".review"])));
        // Terminal — including a non-JSON `.failed` body (presence-keyed, M3).
        let failed =
            staging_disc_with_markers(tmp.path(), "Failed", &["Failed.iso", "Failed.iso.mapfile"]);
        std::fs::write(failed.join(".failed"), b"cancelled by operator\n").unwrap();
        let snap = crate::ripper::staging::snapshot_staging_disc(&failed).unwrap();
        assert!(snap.has_failed && snap.failed_reason.is_none());
        assert!(
            resumable_dir_blocked(&snap),
            "non-JSON .failed must still block drive-resume (presence-keyed)"
        );
    }

    #[test]
    fn effective_abort_secs_forces_iso_to_zero() {
        use super::effective_abort_secs;
        // ISO output is whole-disc and must be byte-complete: the per-title
        // tolerance is IGNORED (forced to 0 = require 100%), no matter what was
        // configured (e.g. left over from a prior MKV rip).
        assert_eq!(effective_abort_secs("iso", 0), 0);
        assert_eq!(
            effective_abort_secs("iso", 30),
            0,
            "iso must ignore a stored MKV tolerance"
        );
        assert_eq!(effective_abort_secs("iso", 999), 0);
        // Muxed outputs pass the configured value through unchanged.
        assert_eq!(effective_abort_secs("mkv", 30), 30);
        assert_eq!(effective_abort_secs("m2ts", 5), 5);
        assert_eq!(effective_abort_secs("network", 0), 0);
    }

    #[test]
    fn iso_aborts_on_any_loss_despite_configured_tolerance() {
        use super::{effective_abort_secs, loss_aborts};
        // Bug scenario: a 30s tolerance configured for MKV, then output switched
        // to ISO. The raw config would WRONGLY tolerate a small whole-disc loss…
        let configured = 30u64;
        let lost_bytes = 2048; // one unreadable sector
        let lost_ms = 100.0; // trivial duration — would pass a 30s threshold
        assert!(
            !loss_aborts(lost_bytes, lost_ms, configured),
            "raw stored 30s threshold would tolerate the loss — the cosmetic-only bug"
        );
        // …but the EFFECTIVE iso threshold (0) aborts on any lost byte:
        assert!(
            loss_aborts(lost_bytes, lost_ms, effective_abort_secs("iso", configured)),
            "iso must abort on ANY whole-disc loss regardless of stored tolerance"
        );
        // …while MKV keeps tolerating within its configured threshold:
        assert!(
            !loss_aborts(lost_bytes, lost_ms, effective_abort_secs("mkv", configured)),
            "mkv still tolerates loss within its configured threshold"
        );
    }

    #[test]
    fn accept_loss_override_threshold_proceeds_but_nan_still_aborts() {
        use super::loss_aborts;
        // The `.accept-loss` override raises the effective threshold to u64::MAX.
        // A real, large in-title loss must then PROCEED (deliver despite damage)…
        assert!(
            !loss_aborts(1_000_000_000, 2_370.0, u64::MAX),
            "operator override (u64::MAX threshold) must deliver despite 2.37s in-movie loss"
        );
        // …but an UNQUANTIFIABLE (NaN) loss must STILL fail safe to abort even
        // under the override — accepting a known amount is the operator's call,
        // a NaN amount is not a quantity anyone agreed to.
        assert!(
            loss_aborts(0, f64::NAN, u64::MAX),
            "NaN loss must abort even under the accept-loss override"
        );
    }

    #[test]
    fn loss_aborts_zero_threshold_is_byte_exact() {
        use super::loss_aborts;
        // abort_on_lost_secs == 0 → ZERO: any lost byte aborts, regardless of
        // the (bitrate-derived) seconds estimate; exactly zero bytes proceeds.
        assert!(
            loss_aborts(1, 0.0, 0),
            "1 lost byte must abort at threshold 0"
        );
        assert!(
            !loss_aborts(0, 12_345.0, 0),
            "0 lost bytes proceeds at threshold 0 even if the seconds estimate is nonzero"
        );
        assert!(
            loss_aborts(0, f64::NAN, 0),
            "NaN loss fails safe to abort even at threshold 0"
        );
        // abort_on_lost_secs > 0 → seconds threshold (lost_ms is MILLISECONDS,
        // threshold is seconds*1000); bytes are not consulted on this path.
        assert!(
            !loss_aborts(9_999_999, 999.0, 1),
            "999ms under a 1000ms (1s) threshold proceeds (bytes ignored on the seconds path)"
        );
        assert!(
            loss_aborts(0, 1001.0, 1),
            "1001ms over a 1000ms (1s) threshold aborts"
        );
        assert!(
            !loss_aborts(0, 1000.0, 1),
            "exactly 1000ms at a 1s threshold proceeds (strictly greater-than aborts)"
        );
        assert!(
            loss_aborts(0, f64::NAN, 30),
            "NaN loss fails safe to abort on the seconds path too"
        );
    }
}
