//! Staging-directory bookkeeping: free-space probe, wipe, marker files
//! (`.done`, `.completed`, `.failed`, `.restart_count`).
//!
//! Marker files live at the per-disc subdirectory level
//! (`<staging_dir>/<disc_name>/<marker>`):
//!
//! - `.done` — hand-off marker for the mover thread (set on successful
//!   rip; preserved across `wipe_staging`). Pre-existing.
//! - `.completed` — process-level "this rip finished cleanly" marker.
//!   Independent of `.done`; consumed by the 0.20.7 restart-loop
//!   detector so the orchestrator can skip already-finished discs on
//!   startup without depending on the mover's `.done` semantics.
//! - `.failed` — `{"reason": "...", "timestamp": "..."}` written when
//!   the restart-loop detector gives up on a disc after `RESTART_LIMIT`
//!   process restarts. Surfaces in the UI as a "failed" status with
//!   the reason in `last_error`.
//! - `.restart_count` — single ASCII u64. Bumped on every startup that
//!   sees partial state and no completion/failed marker; cleared on
//!   either success (`.completed`) or terminal failure (`.failed`).
//!   Three-strike gate against an infinite container restart loop
//!   caused by a deterministic post-startup crash.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Restart-loop attempt cap. After this many consecutive container
/// restarts that find a partial staging dir for the same disc with no
/// completion / failed marker, write `.failed` and stop trying.
pub const RESTART_LIMIT: u64 = 3;

/// Marker filenames — kept as constants so the resume-on-startup logic
/// and the rip orchestrator agree on the on-disk vocabulary.
pub const DONE_MARKER: &str = ".done";
pub const COMPLETED_MARKER: &str = ".completed";
pub const FAILED_MARKER: &str = ".failed";
pub const RESTART_COUNT_FILE: &str = ".restart_count";

/// Available bytes at the given path's filesystem, via `statvfs(3)`.
/// Returns None on any error (path missing, not POSIX, syscall failure).
/// Used by the pre-flight check in `rip_disc` to refuse rips that would
/// run out of space mid-stream.
///
/// `clippy::unnecessary_cast` allowed here intentionally: libc's
/// `f_bavail` / `f_frsize` are `c_ulong` which is u64 on
/// x86_64 Linux (so clippy on x86_64 sees the cast as a no-op) but
/// u32 on some 32-bit / BSD targets (where the cast is required).
/// Drop the cast and the build breaks on the latter.
#[cfg(unix)]
#[allow(clippy::unnecessary_cast)]
pub(super) fn staging_free_bytes(path: &str) -> Option<u64> {
    use std::ffi::CString;
    let cpath = CString::new(path).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let r = unsafe { libc::statvfs(cpath.as_ptr(), &mut stat) };
    if r != 0 {
        return None;
    }
    // f_bavail = blocks available to non-superuser. Multiply by frsize
    // (fundamental block size). Saturate to avoid overflow on 32-bit
    // platforms with absurdly large filesystems.
    Some((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

#[cfg(not(unix))]
pub(super) fn staging_free_bytes(_path: &str) -> Option<u64> {
    None
}

/// Read the restart counter at `<staging_disc_dir>/.restart_count`.
/// Returns 0 if missing, unreadable, or unparseable — a missing/corrupt
/// counter must NOT cause the loop detector to trip, because that would
/// flip the dir to `.failed` on a single stray byte in the file.
pub fn restart_count(staging_disc_dir: &Path) -> u64 {
    let p = staging_disc_dir.join(RESTART_COUNT_FILE);
    match std::fs::read_to_string(&p) {
        Ok(s) => s.trim().parse::<u64>().unwrap_or(0),
        Err(_) => 0,
    }
}

/// Increment the restart counter by 1, atomically as far as best-effort
/// goes (read → +1 → write). Creates the file on first call with value
/// `1`. Returns the new value on success.
pub fn increment_restart_count(staging_disc_dir: &Path) -> io::Result<u64> {
    let next = restart_count(staging_disc_dir).saturating_add(1);
    let p = staging_disc_dir.join(RESTART_COUNT_FILE);
    let mut f = std::fs::File::create(&p)?;
    writeln!(f, "{}", next)?;
    Ok(next)
}

/// Best-effort delete of `.restart_count`. Not finding the file is not
/// an error — the goal is "after this call, the file is absent".
pub fn clear_restart_count(staging_disc_dir: &Path) {
    let p = staging_disc_dir.join(RESTART_COUNT_FILE);
    match std::fs::remove_file(&p) {
        Ok(_) | Err(_) => {}
    }
}

/// Write the `.failed` marker with a structured reason. Best-effort —
/// logs but does not propagate errors, because the only caller is on
/// the giving-up path where there's nothing useful to do with a write
/// failure.
pub fn write_failed_marker(staging_disc_dir: &Path, reason: &str) {
    let p = staging_disc_dir.join(FAILED_MARKER);
    let body = serde_json::json!({
        "reason": reason,
        "timestamp": crate::util::format_iso_datetime(),
    });
    if let Err(e) = std::fs::write(&p, serde_json::to_string_pretty(&body).unwrap_or_default()) {
        tracing::warn!(path = %p.display(), error = %e, "failed to write .failed marker");
    }
}

/// Read the `.failed` marker's reason string. Returns None if missing
/// or unparseable.
pub fn read_failed_reason(staging_disc_dir: &Path) -> Option<String> {
    let p = staging_disc_dir.join(FAILED_MARKER);
    let body = std::fs::read_to_string(&p).ok()?;
    let v: serde_json::Value = serde_json::from_str(&body).ok()?;
    v.get("reason")?.as_str().map(|s| s.to_string())
}

/// Write the `.completed` marker. Empty file — its existence is the
/// signal. Best-effort; logs on failure.
pub fn write_completed_marker(staging_disc_dir: &Path) {
    let p = staging_disc_dir.join(COMPLETED_MARKER);
    if let Err(e) = std::fs::write(&p, b"") {
        tracing::warn!(path = %p.display(), error = %e, "failed to write .completed marker");
    }
}

/// Snapshot of what's in a per-disc staging directory at startup. Used
/// by the resume-on-startup decision tree.
#[derive(Debug)]
pub struct StagingSnapshot {
    pub dir: PathBuf,
    pub completed: bool,
    pub failed_reason: Option<String>,
    pub has_iso: bool,
    pub has_mapfile: bool,
    pub has_mkv: bool,
}

impl StagingSnapshot {
    /// True iff there's any sign of an interrupted rip — at least one
    /// of ISO / mapfile / partial MKV is present. Used by the resume
    /// gate to distinguish "completely empty dir, nothing to do" from
    /// "rip was running when the process died".
    pub fn has_partial_state(&self) -> bool {
        self.has_iso || self.has_mapfile || self.has_mkv
    }
}

/// Probe a single per-disc staging dir. Cheap — just stats a handful
/// of well-known names. Returns None if the path isn't a directory.
pub fn snapshot_staging_disc(dir: &Path) -> Option<StagingSnapshot> {
    if !dir.is_dir() {
        return None;
    }
    let completed = dir.join(COMPLETED_MARKER).exists();
    let failed_reason = read_failed_reason(dir);

    // The orchestrator names the ISO `<sanitize(display_name)>.iso` and
    // the mapfile `<...>.iso.mapfile`. The MKV is `<sanitize(...)>.mkv`
    // or `.m2ts`. We don't know the exact display_name from the disc
    // dir name (which IS the sanitised display_name), so we just scan
    // for any matching extension.
    //
    // NFS cache-coherency defense: at container startup the kernel
    // NFS attribute cache may not be primed yet, and a fresh
    // `read_dir` against a recently-written share can return 0
    // entries even when the dir contains files. Observed empirically
    // 2026-05-15: Watchtower restart -> new container's startup scan
    // ran `read_dir` immediately, got 0 entries, wiped an 85 GB ISO
    // + partial MKV that genuinely existed on the server. Retry up to
    // 3 times with a 500 ms gap before trusting an empty result.
    let mut has_iso = false;
    let mut has_mapfile = false;
    let mut has_mkv = false;
    let mut saw_any_entries = false;
    for attempt in 0..3 {
        if let Ok(entries) = std::fs::read_dir(dir) {
            let mut empty_this_pass = true;
            for entry in entries.flatten() {
                empty_this_pass = false;
                saw_any_entries = true;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.ends_with(".iso") {
                    has_iso = true;
                } else if name.ends_with(".iso.mapfile") || name.ends_with(".mapfile") {
                    has_mapfile = true;
                } else if name.ends_with(".mkv") || name.ends_with(".m2ts") {
                    has_mkv = true;
                }
            }
            if !empty_this_pass {
                break;
            }
        }
        if attempt < 2 {
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }
    if !saw_any_entries {
        tracing::warn!(
            path = %dir.display(),
            "staging dir read_dir returned 0 entries on all 3 retries — treating as empty"
        );
    }

    Some(StagingSnapshot {
        dir: dir.to_path_buf(),
        completed,
        failed_reason,
        has_iso,
        has_mapfile,
        has_mkv,
    })
}

/// Remove every subdirectory under `cfg.staging_dir`. Used on
/// user-initiated stop (stop == reset — clean slate so the next rip
/// doesn't accidentally resume stale state).
///
/// **Not used on startup any more (v0.20.7).** Startup goes through
/// `resume_or_quarantine_staging` which preserves partial state for
/// the restart-loop detector and only wipes if the dir is already
/// terminal. The old behaviour deleted in-flight ISO/mapfile pairs
/// the moment the container restarted — exactly what 0.20.7 is trying
/// to defend against.
///
/// Best-effort: logs each removal, silently ignores errors on
/// individual entries. A locked or not-yet-created staging root is not
/// fatal.
pub fn wipe_staging(staging_dir: &str) {
    let entries = match std::fs::read_dir(staging_dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        // Skip directories that contain a `.done` marker — those are
        // completed rips waiting for the mover thread to relocate them
        // to their final destination. A Watchtower-triggered container
        // restart between "rip finished" and "mover finished" must not
        // delete a completed UHD rip from staging (90 minutes of work
        // lost). Genuinely-orphaned in-flight rips (no .done) still get
        // wiped as before.
        if path.join(DONE_MARKER).exists() {
            tracing::info!(path = %path.display(), "preserving staging entry — .done marker present (mover will handle)");
            continue;
        }
        match std::fs::remove_dir_all(&path) {
            Ok(_) => tracing::info!(path = %path.display(), "wiped stale staging entry"),
            Err(e) => tracing::warn!(path = %path.display(), error = %e, "staging wipe skipped"),
        }
    }
}

/// Startup safety net: walk `<staging_dir>/*` and classify each
/// per-disc subdirectory. Decisions:
///
/// - `.completed` exists → idle/clean, leave alone. (The mover will
///   pick it up via `.done` if that's also present.)
/// - `.failed` exists → leave alone; the orchestrator will surface
///   the reason in `RipState` once a device claims the dir.
/// - Partial state (ISO and/or mapfile and/or partial MKV present,
///   no completion/failed marker):
///   - read `.restart_count`. If `>= RESTART_LIMIT`, write `.failed`
///     with a "restart loop detected" reason and clear the counter.
///   - else bump the counter; leave the partial state in place so the
///     next rip on the same disc can reuse the mapfile/ISO (libfreemkv's
///     `sweep_opts.resume` path on transport-failure retries).
/// - Empty/junk subdir with no recognisable artefacts → wipe.
///
/// Returns a list of per-disc resume hints so the caller can log a
/// summary at startup. **Never deletes user data that looks like an
/// in-flight or recovered rip** — that's the whole point of this
/// function. The only `remove_dir_all` is the "no partial state, no
/// markers, dir is just orphaned junk" branch.
pub fn resume_or_quarantine_staging(staging_dir: &str) -> Vec<StagingResumeHint> {
    let mut hints = Vec::new();
    let entries = match std::fs::read_dir(staging_dir) {
        Ok(e) => e,
        Err(_) => return hints,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(snap) = snapshot_staging_disc(&path) else {
            continue;
        };

        if snap.completed {
            tracing::info!(path = %path.display(), "staging entry has .completed — leaving for mover/ack");
            hints.push(StagingResumeHint {
                dir: snap.dir,
                action: ResumeAction::AlreadyCompleted,
            });
            continue;
        }
        if let Some(reason) = snap.failed_reason.clone() {
            tracing::warn!(path = %path.display(), reason = %reason, "staging entry has .failed — leaving for operator");
            hints.push(StagingResumeHint {
                dir: snap.dir,
                action: ResumeAction::AlreadyFailed { reason },
            });
            continue;
        }
        if !snap.has_partial_state() {
            // Truly empty subdir with no markers — old-style wipe is
            // safe here. Preserves the `.done` carve-out from the
            // legacy logic (handled inside `wipe_staging`'s
            // per-entry path but rechecked here for clarity).
            if path.join(DONE_MARKER).exists() {
                hints.push(StagingResumeHint {
                    dir: snap.dir,
                    action: ResumeAction::AlreadyCompleted,
                });
                continue;
            }
            match std::fs::remove_dir_all(&path) {
                Ok(_) => tracing::info!(path = %path.display(), "wiped empty staging entry"),
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "empty staging wipe skipped")
                }
            }
            continue;
        }

        // Partial state, no terminal marker.
        let rc = restart_count(&path);
        if rc >= RESTART_LIMIT {
            let reason = format!(
                "restart loop detected ({} attempts); partial state preserved at {}",
                rc,
                path.display()
            );
            tracing::error!(path = %path.display(), restart_count = rc, "marking staging entry .failed");
            write_failed_marker(&path, &reason);
            clear_restart_count(&path);
            hints.push(StagingResumeHint {
                dir: snap.dir,
                action: ResumeAction::RestartLoopFailed { reason },
            });
        } else {
            match increment_restart_count(&path) {
                Ok(new_rc) => {
                    tracing::warn!(
                        path = %path.display(),
                        attempt = new_rc,
                        limit = RESTART_LIMIT,
                        has_iso = snap.has_iso,
                        has_mapfile = snap.has_mapfile,
                        has_mkv = snap.has_mkv,
                        "partial staging state preserved across restart"
                    );
                    hints.push(StagingResumeHint {
                        dir: snap.dir,
                        action: ResumeAction::ResumePreserved {
                            attempt: new_rc,
                            has_iso: snap.has_iso,
                            has_mapfile: snap.has_mapfile,
                            has_mkv: snap.has_mkv,
                        },
                    });
                }
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "could not bump .restart_count; leaving partial state in place");
                    hints.push(StagingResumeHint {
                        dir: snap.dir,
                        action: ResumeAction::ResumePreserved {
                            attempt: rc,
                            has_iso: snap.has_iso,
                            has_mapfile: snap.has_mapfile,
                            has_mkv: snap.has_mkv,
                        },
                    });
                }
            }
        }
    }
    hints
}

/// Outcome of inspecting a single per-disc staging directory at
/// startup. Used by the orchestrator for summary logging and by tests.
#[derive(Debug)]
pub struct StagingResumeHint {
    pub dir: PathBuf,
    pub action: ResumeAction,
}

/// Fields are read by the `Debug` impl (via `tracing::info!(... ?action)`)
/// but clippy's dead-code analysis intentionally ignores derived `Debug`
/// — see the lint note. They're also read by tests via pattern-matching.
/// `#[allow(dead_code)]` keeps the structure self-documenting for future
/// consumers (e.g. a future API endpoint that exposes resume hints).
#[derive(Debug)]
#[allow(dead_code)]
pub enum ResumeAction {
    AlreadyCompleted,
    AlreadyFailed {
        reason: String,
    },
    RestartLoopFailed {
        reason: String,
    },
    ResumePreserved {
        attempt: u64,
        has_iso: bool,
        has_mapfile: bool,
        has_mkv: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmpdir() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "autorip-staging-test-{}-{}",
            std::process::id(),
            crate::util::epoch_secs()
        ));
        fs::create_dir_all(&p).unwrap();
        // Ensure each invocation gets a fresh subdir even when two
        // tests land on the same epoch second (the test runner is
        // multi-threaded by default).
        let sub = p.join(format!("{:p}", &p));
        fs::create_dir_all(&sub).unwrap();
        sub
    }

    #[test]
    fn restart_count_missing_returns_zero() {
        let d = tmpdir();
        assert_eq!(restart_count(&d), 0);
    }

    #[test]
    fn increment_creates_then_advances() {
        let d = tmpdir();
        assert_eq!(increment_restart_count(&d).unwrap(), 1);
        assert_eq!(restart_count(&d), 1);
        assert_eq!(increment_restart_count(&d).unwrap(), 2);
        assert_eq!(restart_count(&d), 2);
    }

    #[test]
    fn clear_is_idempotent() {
        let d = tmpdir();
        clear_restart_count(&d); // missing — must not panic
        increment_restart_count(&d).unwrap();
        clear_restart_count(&d);
        assert_eq!(restart_count(&d), 0);
        clear_restart_count(&d); // already gone — must not error
    }

    #[test]
    fn corrupt_restart_count_returns_zero() {
        let d = tmpdir();
        fs::write(d.join(RESTART_COUNT_FILE), b"garbage\n").unwrap();
        assert_eq!(restart_count(&d), 0);
    }

    #[test]
    fn failed_marker_roundtrip() {
        let d = tmpdir();
        write_failed_marker(&d, "test reason");
        assert_eq!(read_failed_reason(&d).as_deref(), Some("test reason"));
    }

    #[test]
    fn resume_marks_failed_after_limit() {
        // Build a fake staging tree: <root>/<disc>/foo.iso plus
        // .restart_count == RESTART_LIMIT.
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("foo.iso"), b"x").unwrap();
        fs::write(
            disc.join(RESTART_COUNT_FILE),
            format!("{}\n", RESTART_LIMIT).as_bytes(),
        )
        .unwrap();

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(matches!(
            hints[0].action,
            ResumeAction::RestartLoopFailed { .. }
        ));
        assert!(disc.join(FAILED_MARKER).exists());
        // Counter cleared after promotion to .failed.
        assert_eq!(restart_count(&disc), 0);
    }

    #[test]
    fn resume_bumps_counter_below_limit() {
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("foo.iso"), b"x").unwrap();

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        match &hints[0].action {
            ResumeAction::ResumePreserved { attempt, .. } => assert_eq!(*attempt, 1),
            other => panic!("unexpected action: {:?}", other),
        }
        assert_eq!(restart_count(&disc), 1);
        assert!(!disc.join(FAILED_MARKER).exists());
    }

    #[test]
    fn resume_preserves_completed_dirs() {
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("foo.mkv"), b"x").unwrap();
        write_completed_marker(&disc);

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(matches!(hints[0].action, ResumeAction::AlreadyCompleted));
        // Marker must still be there afterwards.
        assert!(disc.join(COMPLETED_MARKER).exists());
        // MKV must still be there afterwards.
        assert!(disc.join("foo.mkv").exists());
    }

    #[test]
    fn resume_preserves_failed_dirs() {
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("foo.iso"), b"x").unwrap();
        write_failed_marker(&disc, "prior failure");

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        match &hints[0].action {
            ResumeAction::AlreadyFailed { reason } => assert_eq!(reason, "prior failure"),
            other => panic!("unexpected action: {:?}", other),
        }
        assert!(disc.join("foo.iso").exists());
    }
}
