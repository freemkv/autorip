//! Staging-directory bookkeeping: free-space probe + marker files
//! (`.done`, `.completed`, `.failed`, `.restart_count`).
//!
//! Marker files live at the per-disc subdirectory level
//! (`<staging_dir>/<disc_name>/<marker>`):
//!
//! - `.done` — hand-off marker for the mover thread (set on successful
//!   rip). The mover relocates the dir to its final destination.
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
pub const REVIEW_MARKER: &str = ".review";
pub const COMPLETED_MARKER: &str = ".completed";
pub const FAILED_MARKER: &str = ".failed";
/// Hand-off marker written by `rip_disc` and consumed by the mux worker.
/// Kept here (duplicated from `crate::muxer::RIPPED_MARKER_NAME`) so the
/// startup-scan vocabulary is self-contained; a `debug_assert` in the mux
/// worker tests pins the two equal.
pub const RIPPED_MARKER: &str = ".ripped";
/// In-progress marker written by `rip_disc` at staging-dir creation (before
/// Pass 1) and replaced by `.ripped` (or `.failed`) on exit. Its presence
/// means a sweep+patch is actively running (or crashed mid-sweep) and the
/// dir is OWNED by the ripper, not orphaned partial state. Carries a JSON
/// heartbeat/started timestamp so a future stale-heartbeat policy can tell a
/// live sweep from a dead one. Without it the multi-hour sweep window has no
/// governing marker: the resume scan restart-counts a healthy long rip toward
/// `.failed`, and the mover WARNs every 10s tick on the absent `.done`.
pub const SWEEPING_MARKER: &str = ".sweeping";
/// Exclusion lock written by the mux worker when it begins muxing a `.ripped`
/// dir and removed on completion. Its presence means the dir is OWNED by the
/// mux worker; the drive-resume paths (`disc_already_completed` auto-insert,
/// `find_resumable_for_disc`) must not select it (they would truncate the ISO
/// the mux worker is reading, or double-mux the same output).
pub const MUXING_MARKER: &str = ".muxing";
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

/// Available bytes at the given path's volume on Windows, via
/// `GetDiskFreeSpaceExW`. Returns None on any error (path missing, not a real
/// volume), matching the unix `statvfs` contract so the pre-flight check
/// behaves identically across platforms. Without this the guard was dead on
/// Windows (`cfg(not(unix)) → None`), so a too-small staging volume would
/// ENOSPC mid-rip with no warning.
#[cfg(windows)]
pub(super) fn staging_free_bytes(path: &str) -> Option<u64> {
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;

    // Wide, NUL-terminated path for the …W API.
    let wide: Vec<u16> = OsStr::new(path)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();

    // `lpFreeBytesAvailableToCaller` is the quota-aware free space for the
    // calling user — the closest analogue to statvfs `f_bavail`.
    #[allow(non_snake_case)]
    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            lpDirectoryName: *const u16,
            lpFreeBytesAvailableToCaller: *mut u64,
            lpTotalNumberOfBytes: *mut u64,
            lpTotalNumberOfFreeBytes: *mut u64,
        ) -> i32;
    }

    let mut free_to_caller: u64 = 0;
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut free_to_caller,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    // Win32 BOOL: nonzero = success.
    if ok == 0 {
        return None;
    }
    Some(free_to_caller)
}

#[cfg(not(any(unix, windows)))]
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
    // Atomic write: a crash between create()-truncate and the writeln would
    // otherwise leave an empty/torn file that restart_count() reads back as 0,
    // silently downgrading the counter and defeating the restart-loop guard.
    // Write a temp file, fsync it, then rename(2) over the target (atomic
    // within a filesystem) so the counter is never observed half-written.
    let tmp = staging_disc_dir.join(format!("{}.tmp", RESTART_COUNT_FILE));
    (|| -> io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        writeln!(f, "{}", next)?;
        f.sync_all()
    })()
    .inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })?;
    if let Err(e) = std::fs::rename(&tmp, &p) {
        // A permanent rename failure (cross-device move, ESTALE, full
        // directory) would otherwise leave the `.tmp` sibling on disk
        // forever. Best-effort cleanup, then propagate the real error.
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(next)
}

/// Durably write a marker file: write `<path>.tmp`, `sync_all()` it, rename(2)
/// over the final name, then fsync the containing directory. A crash mid-write
/// thus never leaves an empty/torn marker — readers observe either the old
/// state or the complete new one. Mirrors `increment_restart_count`.
pub(crate) fn write_marker_durable(path: &Path, contents: &[u8]) -> io::Result<()> {
    let tmp = match path.file_name() {
        Some(name) => {
            let mut t = name.to_os_string();
            t.push(".tmp");
            path.with_file_name(t)
        }
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "marker has no file name",
            ));
        }
    };
    (|| -> io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(contents)?;
        f.sync_all()
    })()
    .inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })?;
    if let Err(e) = std::fs::rename(&tmp, path) {
        // Clean up the `.tmp` sibling on a permanent rename failure
        // (cross-device move, ESTALE, full directory) so it is not
        // leaked. Best-effort, then propagate the real error.
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    if let Some(parent) = path.parent() {
        libfreemkv::io::fsync::dir(parent);
    }
    Ok(())
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
    // `to_string_pretty` on a `json!`-constructed Value is effectively
    // infallible; `.expect` makes the invariant explicit so a real
    // serialization failure surfaces as a panic rather than silently
    // writing an empty `.failed` marker that `read_failed_reason` would
    // then parse as `None`, masking the failure reason.
    let serialized =
        serde_json::to_string_pretty(&body).expect("json! value is always serialisable");
    if let Err(e) = write_marker_durable(&p, serialized.as_bytes()) {
        tracing::warn!(path = %p.display(), error = %e, "failed to write .failed marker");
    }
    // A `.failed` terminal supersedes any in-progress `.sweeping` marker; clear
    // it so a quarantined dir isn't also mis-read as an active sweep.
    clear_sweeping_marker(staging_disc_dir);
    // It supersedes the `.muxing` exclusion lock too — same reasoning as
    // `write_completed_marker`. A terminal write must reliably release the
    // in-progress lock regardless of which path (worker guard, cold operator
    // resume, or startup quarantine) reaches it, so a stale `.muxing` can't keep
    // `disc_owned_by_worker` true on a now-terminal dir.
    clear_muxing_marker(staging_disc_dir);
}

/// Read the `.failed` marker's reason string. Returns None if missing
/// or unparseable.
pub fn read_failed_reason(staging_disc_dir: &Path) -> Option<String> {
    let p = staging_disc_dir.join(FAILED_MARKER);
    let body = std::fs::read_to_string(&p).ok()?;
    let v: serde_json::Value = serde_json::from_str(&body).ok()?;
    v.get("reason")?.as_str().map(|s| s.to_string())
}

/// Write the `.sweeping` in-progress marker durably. Called at staging-dir
/// creation in `rip_disc`, before Pass 1. Carries a JSON `started` epoch-secs
/// timestamp (the heartbeat) so a future stale-sweep policy can distinguish a
/// live multi-hour sweep from a dead one. Best-effort — logs on failure; a
/// missing `.sweeping` just degrades to the pre-fix markerless-window
/// behaviour, it never corrupts state.
pub fn write_sweeping_marker(staging_disc_dir: &Path) {
    let p = staging_disc_dir.join(SWEEPING_MARKER);
    let body = serde_json::json!({
        "started": crate::util::epoch_secs(),
        "heartbeat": crate::util::epoch_secs(),
    });
    let serialized = serde_json::to_string_pretty(&body)
        .unwrap_or_else(|_| "{\"started\":0,\"heartbeat\":0}".to_string());
    if let Err(e) = write_marker_durable(&p, serialized.as_bytes()) {
        tracing::warn!(path = %p.display(), error = %e, "failed to write .sweeping marker");
    }
}

/// Write the `.muxing` exclusion lock durably. Called by the mux worker when
/// it begins muxing a `.ripped` dir; removed on completion (RAII guard).
/// Carries a JSON `started` epoch-secs timestamp for observability. Best-effort
/// — a missing `.muxing` only loses the exclusion, it never corrupts state.
pub fn write_muxing_marker(staging_disc_dir: &Path) {
    let p = staging_disc_dir.join(MUXING_MARKER);
    let body = serde_json::json!({ "started": crate::util::epoch_secs() });
    let serialized =
        serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{\"started\":0}".to_string());
    if let Err(e) = write_marker_durable(&p, serialized.as_bytes()) {
        tracing::warn!(path = %p.display(), error = %e, "failed to write .muxing marker");
    }
}

/// Best-effort delete of the `.muxing` exclusion lock. Called when the mux
/// worker finishes (or aborts) a dir. Not finding the file is not an error.
pub fn clear_muxing_marker(staging_disc_dir: &Path) {
    let p = staging_disc_dir.join(MUXING_MARKER);
    if let Err(e) = std::fs::remove_file(&p) {
        if e.kind() != io::ErrorKind::NotFound {
            tracing::warn!(path = %p.display(), error = %e, "failed to clear .muxing marker");
        }
    }
}

/// Best-effort delete of the `.sweeping` in-progress marker. Called by
/// `rip_disc` right before it writes the terminal `.ripped` (or `.failed`)
/// marker for this dir. Not finding the file is not an error.
pub fn clear_sweeping_marker(staging_disc_dir: &Path) {
    let p = staging_disc_dir.join(SWEEPING_MARKER);
    if let Err(e) = std::fs::remove_file(&p) {
        if e.kind() != io::ErrorKind::NotFound {
            tracing::warn!(path = %p.display(), error = %e, "failed to clear .sweeping marker");
        }
    }
}

/// Write the `.completed` marker. Empty file — its existence is the
/// signal. Best-effort; logs on failure.
pub fn write_completed_marker(staging_disc_dir: &Path) {
    let p = staging_disc_dir.join(COMPLETED_MARKER);
    if let Err(e) = write_marker_durable(&p, b"") {
        tracing::warn!(path = %p.display(), error = %e, "failed to write .completed marker");
    }
    // `.completed` is terminal-clean; clear any leftover in-progress `.sweeping`
    // marker (the inline-mux success path writes `.completed` directly without
    // going through `.ripped`).
    clear_sweeping_marker(staging_disc_dir);
    // A terminal write supersedes the `.muxing` exclusion lock too. The worker's
    // MuxingGuard normally clears it, but the cold operator-resume path
    // (`resume::resume_remux`) writes `.completed` WITHOUT going through the
    // guard. If a `.muxing` marker was left on the dir (e.g. a prior worker mux
    // the hard-watchdog exit(1)'d without clearing), it would persist and make
    // `disc_owned_by_worker` return true forever on a dir that is actually
    // `.completed`, silently blocking the unattended re-insert path.
    clear_muxing_marker(staging_disc_dir);
}

/// Durably write a hand-off/review marker (`.done` / `.review`) containing
/// JSON the mover parses. Returns the same `io::Result` shape as a plain
/// write so the caller's error handling is unchanged, but the bytes hit disk
/// atomically (tmp + fsync + rename + dir-fsync) — a crash mid-write never
/// leaves an empty/torn marker the mover would mis-handle.
pub fn write_handoff_marker(marker_path: &Path, contents: &[u8]) -> io::Result<()> {
    write_marker_durable(marker_path, contents)
}

/// Force the just-muxed output file to durable storage before any
/// success marker (`.done` / `.completed`) is written.
///
/// The library's mux `finish()` only flushes its `BufWriter` down to the
/// OS — the bytes can still be sitting in the page cache when autorip
/// writes the staging markers and the mover acts on them. On a crash or
/// power loss in that window the marker says "done" but the file on disk
/// is truncated. `sync_all()` (fsync) closes that gap.
///
/// Returns `true` only when the output was provably synced to durable
/// storage. The library's mux `finish()` swallows an fsync timeout/halt
/// (returns Ok to bound the hang), so durability cannot be assumed from a
/// successful mux alone — this fsync is the gate. A `false` return means
/// the open or fsync failed; the caller MUST NOT write the
/// `.done`/`.completed` success marker this cycle, leaving the staging dir
/// resumable so a later attempt re-runs the durable flush.
///
/// Call this ONLY on the success path, immediately before the marker
/// write, and only for a real local output file (skip `network://` sinks,
/// which have no local path).
pub fn fsync_output_file(output_path: &Path) -> bool {
    // Delegate to the shared, platform-aware durability primitive. It opens the
    // file read+write before `sync_all` so the flush works on Windows, where
    // `FlushFileBuffers` rejects a read-only handle with `ERROR_ACCESS_DENIED`
    // (os error 5). A read-only open was legal on Linux/macOS but made this gate
    // fail every cycle on Windows — the `.done` marker was never written, so
    // auto-resume re-muxed the same ISO forever.
    match libfreemkv::io::fsync::file_durable(output_path) {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(
                path = %output_path.display(),
                error = %e,
                "failed to fsync mux output before completion marker"
            );
            false
        }
    }
}

/// Snapshot of what's in a per-disc staging directory at startup. Used
/// by the resume-on-startup decision tree.
#[derive(Debug)]
pub struct StagingSnapshot {
    pub dir: PathBuf,
    pub completed: bool,
    /// `.failed` terminal marker present. This is the authoritative terminal
    /// signal — keyed on PRESENCE, not parse-success, so a `.failed` written
    /// with a non-JSON body (e.g. review.rs's "cancelled by operator") is
    /// still recognised as terminal. `failed_reason` carries the parsed
    /// reason when the body is JSON (None otherwise).
    pub has_failed: bool,
    pub failed_reason: Option<String>,
    /// `.done` hand-off marker present. A completed mux writes `.done`
    /// (for the mover) before `.completed` (the process-level marker)
    /// and before the ISO prune. A crash in that window leaves `.done`
    /// present but `.completed` absent and the ISO still on disk — the
    /// resume scan must recognise this as a finished rip, not partial
    /// state to be retried. Hoisting `.done` into the snapshot lets the
    /// resume gate short-circuit before the partial-state branch.
    pub has_done: bool,
    /// `.review` hand-off marker present. When the rip's title match is
    /// not confident the mux writes `.review` (instead of `.done`) before
    /// `.completed` — the rip is finished and staged but held for operator
    /// title confirmation rather than auto-filed. Like `.done`, a crash
    /// between the `.review` and `.completed` writes leaves `.review`
    /// present, `.completed` absent, and the ISO/mapfile still on disk, so
    /// the resume scan must recognise this as a finished rip, not partial
    /// state to be restart-counted (and eventually promoted to `.failed`).
    pub has_review: bool,
    /// `.ripped` hand-off marker present (written by `rip_disc` after
    /// sweep+patch, consumed by the mux worker). Read from the same primed,
    /// 3x-retried `read_dir` view as the terminal markers so a cold-cache NFS
    /// miss can't race it to "absent" while the snapshot surfaces `.completed`/
    /// `.failed` — the mux-worker dispatch decider (`mux_dispatch_verdict`)
    /// relies on this consistency.
    pub has_ripped: bool,
    /// `.sweeping` in-progress marker present (written by `rip_disc` at
    /// staging-dir creation, before Pass 1; replaced by `.ripped`/`.failed`
    /// on exit). Its presence means a sweep+patch is actively running (or
    /// crashed mid-sweep) — the dir is OWNED by the ripper, not orphaned
    /// partial state. The resume scan treats it as "owned, in progress":
    /// state is left intact, but `.restart_count` IS bumped on each restart so
    /// a deterministically-crashing owned sweep still converges to `.failed`
    /// within `RESTART_LIMIT`.
    pub has_sweeping: bool,
    /// `.muxing` exclusion lock present (written by the mux worker while it
    /// muxes a `.ripped` dir, removed on completion). Its presence means the
    /// dir is OWNED by the mux worker; the drive-resume paths must not select
    /// it for a fresh sweep or a double-mux.
    pub has_muxing: bool,
    pub has_iso: bool,
    pub has_mapfile: bool,
    pub has_mkv: bool,
    /// Set when a per-entry `read_dir` error occurred during the scan
    /// (partial NFS degradation). When true the snapshot must NOT be
    /// classified as empty, because the artifact counts may be undercounts.
    pub had_entry_error: bool,
}

impl StagingSnapshot {
    /// True iff there's any sign of an interrupted rip — at least one
    /// of ISO / mapfile / partial MKV is present. Used by the resume
    /// gate to distinguish "completely empty dir, nothing to do" from
    /// "rip was running when the process died". Also returns true when a
    /// per-entry scan error occurred, so partial NFS degradation can't
    /// undercount artifacts and trigger the remove_dir_all wipe on a
    /// populated dir.
    pub fn has_partial_state(&self) -> bool {
        self.has_iso || self.has_mapfile || self.has_mkv || self.had_entry_error
    }
}

/// Raw, untrusted observations from scanning a staging dir's entries.
/// Separated from the classification decision so the "what does this
/// mean?" logic (`classify_observations`) is unit-testable without
/// having to provoke real per-entry NFS I/O errors from the filesystem.
#[derive(Debug, Default, Clone, Copy)]
struct ScanObservations {
    has_done: bool,
    has_review: bool,
    has_ripped: bool,
    has_sweeping: bool,
    has_muxing: bool,
    has_completed: bool,
    has_failed: bool,
    has_iso: bool,
    has_mapfile: bool,
    has_mkv: bool,
    /// At least one `read_dir` attempt returned `Ok(entries)`.
    saw_read_ok: bool,
    /// At least one `Ok(DirEntry)` was yielded across all attempts.
    saw_any_entries: bool,
    /// At least one DirEntry yielded `Err(_)` (partial NFS degradation).
    had_entry_error: bool,
}

impl ScanObservations {
    /// True iff no marker and no artifact was observed — nothing we can
    /// act on.
    fn observed_nothing(&self) -> bool {
        !self.has_done
            && !self.has_review
            && !self.has_ripped
            && !self.has_sweeping
            && !self.has_muxing
            && !self.has_completed
            && !self.has_failed
            && !self.has_iso
            && !self.has_mapfile
            && !self.has_mkv
    }

    /// True iff the dir's contents must be treated as UNKNOWN (not empty,
    /// not partial) — the caller must skip it without wiping OR bumping
    /// `.restart_count`. Two cases, both NFS-startup degradation:
    ///
    /// 1. Every `read_dir` attempt errored (`!saw_read_ok`) — never got
    ///    a listing at all.
    /// 2. `read_dir` opened but every DirEntry I/O errored
    ///    (`had_entry_error`) and nothing trustworthy was observed
    ///    (`observed_nothing`) — a possibly-completed 85 GB rip whose
    ///    listing degraded mid-scan must NOT be counted as partial state
    ///    and walked toward `.failed` over RESTART_LIMIT restarts.
    fn contents_unknown(&self) -> bool {
        !self.saw_read_ok || (self.had_entry_error && self.observed_nothing())
    }
}

/// Probe a single per-disc staging dir. Cheap — just stats a handful
/// of well-known names. Returns None if the path isn't a directory.
pub fn snapshot_staging_disc(dir: &Path) -> Option<StagingSnapshot> {
    if !dir.is_dir() {
        return None;
    }
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
    //
    // The `.done` / `.completed` / `.failed` markers are read from this
    // SAME primed `read_dir` view (not a separate un-retried `.exists()`
    // stat) so a transient cold-cache NFS error can't race them to
    // "absent" while the retry loop surfaces the ISO/mapfile — the
    // exact case where a genuinely-completed rip would otherwise bump
    // `.restart_count` every cold restart and be wrongly promoted to
    // `.failed`.
    let mut obs = ScanObservations::default();
    for attempt in 0..3 {
        if let Ok(entries) = std::fs::read_dir(dir) {
            obs.saw_read_ok = true;
            let mut empty_this_pass = true;
            for entry in entries {
                // Don't `.flatten()` away per-entry errors: a partial NFS
                // degradation can error on individual DirEntry I/O while
                // the dir is genuinely populated. Silently dropping those
                // would undercount artifacts and could trip the
                // remove_dir_all wipe on a non-empty dir. Treat any entry
                // error like the all-attempts-errored case (suppress the
                // empty classification) — same defense as `saw_any_entries`.
                let entry = match entry {
                    Ok(e) => e,
                    Err(_) => {
                        obs.had_entry_error = true;
                        continue;
                    }
                };
                empty_this_pass = false;
                obs.saw_any_entries = true;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name == DONE_MARKER {
                    obs.has_done = true;
                } else if name == REVIEW_MARKER {
                    obs.has_review = true;
                } else if name == RIPPED_MARKER {
                    obs.has_ripped = true;
                } else if name == SWEEPING_MARKER {
                    obs.has_sweeping = true;
                } else if name == MUXING_MARKER {
                    obs.has_muxing = true;
                } else if name == COMPLETED_MARKER {
                    obs.has_completed = true;
                } else if name == FAILED_MARKER {
                    obs.has_failed = true;
                } else if name.ends_with(".iso") {
                    obs.has_iso = true;
                } else if name.ends_with(".mapfile") {
                    // ".iso.mapfile" is subsumed by ".mapfile" — one arm covers both.
                    obs.has_mapfile = true;
                } else if name.ends_with(".mkv") || name.ends_with(".m2ts") {
                    obs.has_mkv = true;
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

    // UNKNOWN contents — never got a trustworthy listing. Two NFS-startup
    // degradation cases (see `ScanObservations::contents_unknown`):
    //   1. every `read_dir` attempt errored, or
    //   2. `read_dir` opened but every DirEntry I/O errored and nothing
    //      was observed.
    // Return None so the caller skips the dir entirely (its
    // `let Some(snap) = ... else { continue }`) rather than treating it
    // as empty (→ wipe) or as partial state (→ bump `.restart_count`,
    // eventually promoting a possibly-completed 85 GB rip to `.failed`).
    if obs.contents_unknown() {
        tracing::warn!(
            path = %dir.display(),
            saw_read_ok = obs.saw_read_ok,
            had_entry_error = obs.had_entry_error,
            "staging dir contents UNKNOWN (read_dir/DirEntry errors, nothing observed) — skipping, not wiping or restart-counting"
        );
        return None;
    }
    if !obs.saw_any_entries {
        tracing::warn!(
            path = %dir.display(),
            "staging dir read_dir returned 0 entries on all 3 retries — treating as empty"
        );
    }

    // Only read the `.failed` reason file when the primed scan actually
    // saw the marker, so the content read is consistent with the
    // presence check above.
    let failed_reason = if obs.has_failed {
        read_failed_reason(dir)
    } else {
        None
    };

    Some(StagingSnapshot {
        dir: dir.to_path_buf(),
        completed: obs.has_completed,
        has_failed: obs.has_failed,
        failed_reason,
        has_done: obs.has_done,
        has_review: obs.has_review,
        has_ripped: obs.has_ripped,
        has_sweeping: obs.has_sweeping,
        has_muxing: obs.has_muxing,
        has_iso: obs.has_iso,
        has_mapfile: obs.has_mapfile,
        has_mkv: obs.has_mkv,
        had_entry_error: obs.had_entry_error,
    })
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
        Err(e) => {
            tracing::warn!(staging_dir, error = %e, "could not list staging root at startup; nothing resumed this cycle");
            return hints;
        }
    };
    for entry in entries {
        // Mirror the inner scan's defense: don't `.flatten()` away a
        // per-entry error (NFS ESTALE on a specific dentry), which would
        // silently skip a whole disc subdir for a container cycle.
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(staging_dir, error = %e, "per-entry error listing staging root - skipping this entry, share may be degraded");
                continue;
            }
        };
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
        // Terminal `.failed` — keyed on marker PRESENCE (`has_failed`), not on
        // a parseable reason. A `.failed` written with a non-JSON body (e.g.
        // review.rs's operator-cancel "cancelled by operator") has
        // `failed_reason == None` but is still terminal; keying on
        // `failed_reason.is_some()` here would let such a dir slip past into
        // the partial-state restart-count path. Surface the reason when it
        // parsed; otherwise fall back to a generic terminal reason string.
        if snap.has_failed {
            let reason = snap
                .failed_reason
                .clone()
                .unwrap_or_else(|| "failed (no machine-readable reason recorded)".to_string());
            tracing::warn!(path = %path.display(), reason = %reason, "staging entry has .failed — leaving for operator");
            hints.push(StagingResumeHint {
                dir: snap.dir,
                action: ResumeAction::AlreadyFailed { reason },
            });
            continue;
        }
        // `.done` carve-out — checked BEFORE the partial-state branch.
        // The mux writes `.done` then `.completed` then prunes the ISO;
        // a crash between `.done` and `.completed` leaves `.done`
        // present, `.completed` absent, and the ISO/mapfile still on
        // disk (so `has_partial_state()` is true). That dir is a
        // *finished* rip awaiting the mover, NOT partial state to be
        // re-rip-counted. If this check stayed inside the
        // `!has_partial_state()` branch it would be unreachable in that
        // crash window, the dir would fall through to the restart-loop
        // path, and after RESTART_LIMIT crashes a completed rip would be
        // wrongly marked `.failed`. Short-circuit to AlreadyCompleted
        // whenever `.done` exists, regardless of leftover ISO/mapfile.
        if snap.has_done {
            tracing::info!(path = %path.display(), "staging entry has .done — completed rip awaiting mover, leaving alone");
            hints.push(StagingResumeHint {
                dir: snap.dir,
                action: ResumeAction::AlreadyCompleted,
            });
            continue;
        }
        // `.review` carve-out — same crash-window reasoning as `.done`
        // above. When the title match isn't confident the mux writes
        // `.review` (not `.done`) then `.completed` then prunes the ISO.
        // A crash between `.review` and `.completed` leaves `.review`
        // present, `.completed` absent, and the ISO/mapfile on disk
        // (so `has_partial_state()` is true). That dir is a *finished*
        // rip held for operator title confirmation, NOT partial state to
        // be restart-counted — without this short-circuit it would fall
        // through to the restart-loop path and, after RESTART_LIMIT
        // crashes in that window, a completed rip would be wrongly marked
        // `.failed`. Short-circuit to AlreadyCompleted whenever `.review`
        // exists, regardless of leftover ISO/mapfile.
        if snap.has_review {
            tracing::info!(path = %path.display(), "staging entry has .review — completed rip held for operator review, leaving alone");
            hints.push(StagingResumeHint {
                dir: snap.dir,
                action: ResumeAction::AlreadyCompleted,
            });
            continue;
        }
        // `.sweeping` / `.muxing` carve-out — checked BEFORE the partial-state
        // branch. `.sweeping` is written by `rip_disc` at staging-dir creation
        // (before Pass 1) and replaced by `.ripped`/`.failed` on exit; `.muxing`
        // is written by the mux worker while it owns a `.ripped` dir. Either
        // marker means the dir is actively OWNED and in progress, NOT orphaned
        // partial state to be restart-counted. Without this carve-out a crash
        // mid-sweep would leave `.sweeping` + ISO/mapfile on disk, the scan
        // would treat it as partial state, bump `.restart_count` every cold
        // restart, and after RESTART_LIMIT silently quarantine a healthy
        // long-running rip as `.failed`.
        //
        // BUT a deterministically-wedging sweep/mux that gets killed mid-flight
        // re-acquires `.sweeping`/`.muxing` on every restart, so a pure "always
        // skip" carve-out would spin forever. Only the 20-minute hard-watchdog
        // mux escalation (mux.rs) bumps `.restart_count` itself before exit(1);
        // EVERY other hard kill — OOM-kill, `docker kill`/SIGKILL, panic=abort,
        // host power loss, or a libfreemkv panic that aborts the sweep in under
        // 20 min — leaves the marker on disk with the count UNbumped (nothing
        // ran to bump it). If we only skipped here, such a deterministically-
        // crashing sweep would loop restart → skip-as-InProgress → re-sweep →
        // crash forever, count pinned at 0, never promoted to `.failed`.
        //
        // So bump `.restart_count` on the InProgress skip too (mirroring the
        // partial-state branch below), and once it reaches RESTART_LIMIT promote
        // the dir to `.failed` rather than spin. A healthy long sweep survives a
        // small number of benign restarts (count below the limit is still
        // skipped, state preserved); a deterministic wedge is capped at
        // RESTART_LIMIT crashes regardless of whether the watchdog or a raw kill
        // ended it.
        if snap.has_sweeping || snap.has_muxing {
            let rc = restart_count(&path);
            if rc >= RESTART_LIMIT {
                let reason = format!(
                    "restart loop detected ({} attempts) on owned/in-progress dir ({}); state preserved at {}",
                    rc,
                    if snap.has_muxing {
                        ".muxing"
                    } else {
                        ".sweeping"
                    },
                    path.display()
                );
                tracing::error!(
                    path = %path.display(),
                    restart_count = rc,
                    has_sweeping = snap.has_sweeping,
                    has_muxing = snap.has_muxing,
                    "owned/in-progress staging entry exceeded restart limit — marking .failed"
                );
                // write_failed_marker already clears BOTH the .sweeping and
                // .muxing markers unconditionally, so no explicit clear is
                // needed here for the owned/in-progress dir.
                write_failed_marker(&path, &reason);
                clear_restart_count(&path);
                hints.push(StagingResumeHint {
                    dir: snap.dir,
                    action: ResumeAction::RestartLoopFailed { reason },
                });
            } else {
                // Bump on every InProgress skip so a deterministically-crashing
                // owned sweep/mux walks toward `.failed` over RESTART_LIMIT
                // restarts even when no watchdog ran to bump it. Best-effort: a
                // bump failure just leaves the count where it was (we still skip
                // and preserve state), exactly like the partial-state branch.
                let attempt = increment_restart_count(&path).unwrap_or(rc);
                tracing::info!(
                    path = %path.display(),
                    has_sweeping = snap.has_sweeping,
                    has_muxing = snap.has_muxing,
                    restart_count = attempt,
                    limit = RESTART_LIMIT,
                    "staging entry is owned/in-progress (.sweeping/.muxing) — leaving alone, restart-counted"
                );
                hints.push(StagingResumeHint {
                    dir: snap.dir,
                    action: ResumeAction::InProgress,
                });
            }
            continue;
        }
        if !snap.has_partial_state() {
            // Truly empty subdir with no markers — safe to wipe.
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
                        // The failure gate above checks pre-bump
                        // `rc >= RESTART_LIMIT`, so this dir is promoted to
                        // `.failed` only once its count reaches
                        // RESTART_LIMIT — i.e. on a restart where the
                        // pre-bump count is already RESTART_LIMIT. Surface
                        // that threshold so attempt=RESTART_LIMIT here
                        // doesn't read as "should already have failed".
                        fails_after = RESTART_LIMIT,
                        has_iso = snap.has_iso,
                        has_mapfile = snap.has_mapfile,
                        has_mkv = snap.has_mkv,
                        "partial staging state preserved across restart (fails on next restart once attempt reaches the limit)"
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
    /// Dir is actively owned/in progress (`.sweeping` sweep+patch running, or
    /// `.muxing` mux worker holds it). State is left intact, but `.restart_count`
    /// IS bumped on every such skip so a deterministically-crashing owned dir
    /// still converges to `.failed` within `RESTART_LIMIT` (a healthy long rip
    /// survives the few benign bumps below the limit).
    InProgress,
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
        // Repo-local scratch, never /tmp — /tmp is wiped on reboot and a
        // stray collision there can leak across unrelated runs. Anchor to
        // the crate's own target/ dir so artifacts land inside the build
        // tree and are cleaned by `cargo clean`.
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-scratch");
        let p = base.join(format!(
            "autorip-staging-test-{}-{}",
            std::process::id(),
            crate::util::epoch_secs()
        ));
        fs::create_dir_all(&p).unwrap();
        // Ensure each invocation gets a fresh subdir even when two tests
        // land on the same epoch second (the test runner is multi-threaded
        // by default). A process-lifetime monotonic counter is guaranteed
        // non-repeating; a stack-address discriminator ({:p}) is not, since
        // sequential tests on the same pool thread can reuse the address.
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let sub = p.join(format!("t-{}", COUNTER.fetch_add(1, Ordering::Relaxed)));
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

    /// `fsync_output_file` is the mux durability gate: `true` only when the
    /// output was provably synced (lets the `.done` marker be written), `false`
    /// when there is no file to sync (caller must preserve staging and retry).
    /// The rc.4.1 Windows remux loop was this returning `false` forever because
    /// it opened the output read-only and `FlushFileBuffers` rejects a
    /// read-only handle; it now delegates to `io::fsync::file_durable`, which
    /// opens read+write. This test pins both arms of the contract.
    #[test]
    fn fsync_output_file_true_for_real_false_for_missing() {
        let d = tmpdir();
        let f = d.join("out.mkv");
        fs::write(&f, b"muxed bytes").unwrap();
        assert!(
            fsync_output_file(&f),
            "an existing output file must fsync successfully (gate passes)"
        );
        assert!(
            !fsync_output_file(&d.join("never-written.mkv")),
            "a missing output file must fail the gate so staging is preserved"
        );
    }

    /// `increment_restart_count` must round-trip the incremented value and
    /// leave NO `.restart_count.tmp` behind — the temp file is renamed over
    /// the target (atomic), so a dangling `.tmp` would mean the rename never
    /// happened (torn write) or a stray file the resume scan could trip on.
    #[test]
    fn increment_roundtrips_and_cleans_up_tmp() {
        let d = tmpdir();
        let tmp = d.join(format!("{}.tmp", RESTART_COUNT_FILE));

        let v1 = increment_restart_count(&d).unwrap();
        assert_eq!(v1, 1);
        assert_eq!(restart_count(&d), 1, "incremented value must round-trip");
        assert!(
            !tmp.exists(),
            "{} must be renamed away, not left behind",
            tmp.display()
        );

        let v2 = increment_restart_count(&d).unwrap();
        assert_eq!(v2, 2);
        assert_eq!(restart_count(&d), 2);
        assert!(!tmp.exists(), "tmp file must not persist across increments");
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

    /// A hand-off marker (`.done`/`.review`) must never be written empty: the
    /// mover skips directories whose marker won't parse, so an empty marker
    /// strands a finished output in staging with no operator-facing signal.
    /// The hand-off sites serialize a `json!` Value and `.expect` the (today
    /// infallible) result rather than falling back to empty bytes; this guards
    /// that the durable write path produces a non-empty, parseable marker.
    #[test]
    fn handoff_marker_is_nonempty_and_parseable() {
        let d = tmpdir();
        let marker = serde_json::json!({
            "title": "Some Movie",
            "format": "Blu-ray",
            "year": 2024,
            "date": "2024-01-01",
        });
        let body =
            serde_json::to_string_pretty(&marker).expect("json! value is always serialisable");
        let path = d.join(".done");
        write_handoff_marker(&path, body.as_bytes()).unwrap();

        let written = fs::read(&path).unwrap();
        assert!(!written.is_empty(), ".done marker must not be empty bytes");
        let parsed: serde_json::Value = serde_json::from_slice(&written).unwrap();
        assert_eq!(
            parsed.get("title").and_then(|v| v.as_str()),
            Some("Some Movie")
        );
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
    fn done_marker_with_partial_state_is_completed_not_retried() {
        // A crash between writing .done and .completed leaves .done +
        // the ISO/mapfile on disk. The resume scan must treat this as a
        // completed rip awaiting the mover, NOT bump .restart_count.
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("foo.iso"), b"x").unwrap();
        fs::write(disc.join("foo.iso.mapfile"), b"x").unwrap();
        fs::write(disc.join(DONE_MARKER), b"{}").unwrap();
        // No .completed marker (the crash happened before it landed).

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(
            matches!(hints[0].action, ResumeAction::AlreadyCompleted),
            "got {:?}",
            hints[0].action
        );
        // Counter must NOT have been bumped — this was a finished rip.
        assert_eq!(restart_count(&disc), 0);
        assert!(!disc.join(FAILED_MARKER).exists());
        // Data preserved for the mover.
        assert!(disc.join("foo.iso").exists());
        assert!(disc.join(DONE_MARKER).exists());
    }

    #[test]
    fn review_marker_with_partial_state_is_completed_not_retried() {
        // When the title match isn't confident the mux writes .review
        // (instead of .done) then .completed. A crash between .review and
        // .completed leaves .review + the ISO/mapfile/MKV on disk. The
        // resume scan must treat this as a finished rip held for operator
        // review, NOT bump .restart_count (which would promote a completed
        // rip to .failed after RESTART_LIMIT restarts in that window).
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("foo.iso"), b"x").unwrap();
        fs::write(disc.join("foo.iso.mapfile"), b"x").unwrap();
        fs::write(disc.join("MyDisc.mkv"), b"x").unwrap();
        fs::write(disc.join(REVIEW_MARKER), b"{}").unwrap();
        // No .completed marker (the crash happened before it landed).

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(
            matches!(hints[0].action, ResumeAction::AlreadyCompleted),
            "got {:?}",
            hints[0].action
        );
        // Counter must NOT have been bumped — this was a finished rip.
        assert_eq!(restart_count(&disc), 0);
        assert!(!disc.join(FAILED_MARKER).exists());
        // Data preserved for the operator/mover.
        assert!(disc.join("MyDisc.mkv").exists());
        assert!(disc.join(REVIEW_MARKER).exists());
    }

    #[test]
    fn snapshot_reports_unknown_on_unreadable_dir() {
        // A path that isn't a directory (read_dir errors) must yield
        // None, not a "looks empty" snapshot that the caller might wipe.
        let root = tmpdir();
        let not_a_dir = root.join("a_file");
        fs::write(&not_a_dir, b"x").unwrap();
        assert!(snapshot_staging_disc(&not_a_dir).is_none());
    }

    #[test]
    fn all_direntry_errors_with_no_artifacts_is_unknown_not_partial() {
        // read_dir opened fine but every DirEntry I/O errored (partial
        // NFS degradation mid-listing at container startup) and nothing
        // trustworthy was observed. This MUST be classified UNKNOWN — the
        // caller skips it without bumping `.restart_count`. Bumping would,
        // over RESTART_LIMIT cold restarts, wrongly promote a possibly-
        // completed 85 GB rip to `.failed` (the NFS-startup-wipe class).
        let obs = ScanObservations {
            saw_read_ok: true,
            had_entry_error: true,
            ..Default::default()
        };
        assert!(obs.observed_nothing());
        assert!(
            obs.contents_unknown(),
            "all-DirEntry-error + no artifacts must be UNKNOWN, not partial state"
        );
    }

    #[test]
    fn all_read_dir_attempts_errored_is_unknown() {
        // The original all-attempts-errored defense: never got a listing.
        let obs = ScanObservations {
            saw_read_ok: false,
            ..Default::default()
        };
        assert!(obs.contents_unknown());
    }

    #[test]
    fn entry_error_alongside_real_artifact_is_not_unknown() {
        // A populated dir where one DirEntry errored but the ISO was
        // still seen is NOT unknown — the snapshot is kept so the normal
        // resume/restart handling runs. (has_iso alone already makes
        // has_partial_state() true; the entry error must not erase that.)
        let obs = ScanObservations {
            saw_read_ok: true,
            saw_any_entries: true,
            had_entry_error: true,
            has_iso: true,
            ..Default::default()
        };
        assert!(!obs.observed_nothing());
        assert!(!obs.contents_unknown());
    }

    #[test]
    fn clean_empty_dir_is_not_unknown() {
        // read_dir succeeded, dir was genuinely empty, no entry errors.
        // Not UNKNOWN — the caller may legitimately wipe a truly-empty,
        // marker-less staging dir.
        let obs = ScanObservations {
            saw_read_ok: true,
            ..Default::default()
        };
        assert!(!obs.contents_unknown());
    }

    #[test]
    fn unknown_contents_snapshot_does_not_bump_restart_count() {
        // End-to-end shape of the bug: a snapshot that returns None for
        // UNKNOWN contents means resume_or_quarantine_staging skips the
        // dir entirely, leaving `.restart_count` untouched. We can't
        // provoke real per-entry NFS errors from the local FS, so this
        // asserts the contract the None-return relies on: a dir we never
        // touch keeps its restart count at 0 and gains no `.failed`.
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        // Pre-seed restart_count near the limit to make a wrongful bump
        // (which would push it to .failed) maximally visible.
        fs::write(
            disc.join(RESTART_COUNT_FILE),
            format!("{}\n", RESTART_LIMIT - 1).as_bytes(),
        )
        .unwrap();
        // The contract: when snapshot_staging_disc returns None (UNKNOWN),
        // the dir is skipped. Verify the predicate that drives that None.
        let unknown = ScanObservations {
            saw_read_ok: true,
            had_entry_error: true,
            ..Default::default()
        };
        assert!(unknown.contents_unknown());
        // And confirm that simply NOT processing the dir leaves the
        // counter where it was — no bump, no promotion to .failed.
        assert_eq!(restart_count(&disc), RESTART_LIMIT - 1);
        assert!(!disc.join(FAILED_MARKER).exists());
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

    /// R3 finding 2 regression: a `.sweeping` dir from a NON-watchdog hard crash
    /// (OOM-kill / SIGKILL / panic=abort) lands with `.restart_count == 0`
    /// because nothing ran to bump it. The InProgress carve-out must STILL
    /// restart-count it, so a deterministically-crashing owned sweep walks toward
    /// `.failed` over RESTART_LIMIT restarts instead of looping forever. Before
    /// the fix the carve-out skipped without counting and the count stayed pinned
    /// at 0.
    #[test]
    fn sweeping_in_progress_is_restart_counted() {
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("foo.iso"), b"x").unwrap();
        // `.sweeping` present, count at 0 (raw kill — no watchdog bump).
        write_sweeping_marker(&disc);
        assert_eq!(restart_count(&disc), 0);

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(
            matches!(hints[0].action, ResumeAction::InProgress),
            "below the limit a .sweeping dir is left in progress, got {:?}",
            hints[0].action
        );
        assert_eq!(
            restart_count(&disc),
            1,
            ".sweeping InProgress skip must bump .restart_count (else a crash loop never escapes)"
        );
        assert!(disc.join("foo.iso").exists());
        assert!(!disc.join(FAILED_MARKER).exists());
    }

    /// R3 finding 2 regression (terminal end): once a `.sweeping` dir's restart
    /// count reaches RESTART_LIMIT the carve-out must promote it to `.failed` and
    /// clear the in-progress marker, capping a deterministic wedge instead of
    /// spinning forever.
    #[test]
    fn sweeping_in_progress_fails_after_limit() {
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("foo.iso"), b"x").unwrap();
        write_sweeping_marker(&disc);
        fs::write(
            disc.join(RESTART_COUNT_FILE),
            format!("{}\n", RESTART_LIMIT).as_bytes(),
        )
        .unwrap();

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(
            matches!(hints[0].action, ResumeAction::RestartLoopFailed { .. }),
            "got {:?}",
            hints[0].action
        );
        assert!(disc.join(FAILED_MARKER).exists());
        // The in-progress marker is cleared on promotion.
        assert!(!disc.join(SWEEPING_MARKER).exists());
        assert_eq!(restart_count(&disc), 0);
    }

    /// R3 finding 3 regression: a terminal write must release the `.muxing`
    /// exclusion lock. The cold operator-resume path (`resume::resume_remux`)
    /// writes `.completed`/`.failed` WITHOUT going through the worker's
    /// MuxingGuard; if a stale `.muxing` lingered, `disc_owned_by_worker` would
    /// read true forever on a terminal dir and silently block the re-insert path.
    #[test]
    fn terminal_writers_clear_muxing_lock() {
        // .completed clears .muxing.
        let d1 = tmpdir();
        write_muxing_marker(&d1);
        assert!(d1.join(MUXING_MARKER).exists());
        write_completed_marker(&d1);
        assert!(
            !d1.join(MUXING_MARKER).exists(),
            ".completed must clear a leftover .muxing lock"
        );

        // .failed clears .muxing.
        let d2 = tmpdir();
        write_muxing_marker(&d2);
        assert!(d2.join(MUXING_MARKER).exists());
        write_failed_marker(&d2, "terminal");
        assert!(
            !d2.join(MUXING_MARKER).exists(),
            ".failed must clear a leftover .muxing lock"
        );
    }

    /// Regression (HIGH audit #7): if the durable hand-off (`.done`) marker
    /// write FAILS, the caller must NOT proceed to write `.completed` or clear
    /// `.restart_count`. Otherwise the staging dir looks terminal-complete
    /// while the mover has no `.done` to act on and the resume detector never
    /// re-runs — a data-integrity gap.
    ///
    /// This pins the early-return invariant in `rip_disc`'s marker-write block:
    /// `write_handoff_marker` Err ⇒ leave the dir resumable (no `.completed`,
    /// `.restart_count` preserved).
    #[test]
    fn failed_done_write_leaves_no_completed_and_preserves_restart_count() {
        let disc = tmpdir();

        // Seed a restart counter so we can prove it is NOT cleared.
        increment_restart_count(&disc).unwrap();
        assert_eq!(restart_count(&disc), 1);

        // Force the `.done` write to fail by targeting a path whose parent is
        // a non-existent subdirectory (the durable write can't create the tmp
        // file there). This mirrors a real I/O failure at the marker site.
        let bad_done = disc.join("missing-subdir").join(DONE_MARKER);
        let handoff = write_handoff_marker(&bad_done, b"{}");
        assert!(
            handoff.is_err(),
            "precondition: the hand-off marker write must fail for this test"
        );

        // The fix: on that Err, `rip_disc` returns early — so the following
        // two calls are SKIPPED. We assert the post-state that skipping yields.
        // (We deliberately do NOT call write_completed_marker / clear_restart_count.)

        assert!(
            !disc.join(COMPLETED_MARKER).exists(),
            ".completed must not exist when the .done write failed"
        );
        assert!(
            !disc.join(DONE_MARKER).exists(),
            "no durable .done landed in the staging dir"
        );
        assert_eq!(
            restart_count(&disc),
            1,
            ".restart_count must be preserved (not cleared) when .done failed"
        );
    }

    /// A rename failure in `write_marker_durable` must not leak the `.tmp`
    /// sibling. We force the failure by making the target `path` a non-empty
    /// directory: `rename(file, non_empty_dir)` fails on both Linux and macOS,
    /// and it fails AFTER the `.tmp` file has been created + fsynced — so it
    /// exercises the cleanup-on-rename-error path specifically.
    #[test]
    fn marker_rename_failure_cleans_up_tmp() {
        let d = tmpdir();
        let target = d.join(".done");
        // Make the target a non-empty directory so rename-over it fails.
        fs::create_dir(&target).unwrap();
        fs::write(target.join("occupant"), b"x").unwrap();

        let res = write_marker_durable(&target, b"{}");
        assert!(
            res.is_err(),
            "precondition: rename onto a non-empty dir must fail"
        );

        let tmp = d.join(".done.tmp");
        assert!(
            !tmp.exists(),
            "the .tmp sibling must be cleaned up after a rename failure, found: {}",
            tmp.display()
        );
    }

    // ===================================================================
    // EXHAUSTIVE resume-on-startup classifier matrix (rc4 hardening).
    //
    // `resume_or_quarantine_staging` is the second of the three staging-state
    // deciders. For each per-disc subdir it produces a `ResumeAction` (or
    // silently wipes/skips). These tests drive the REAL function against a
    // real staging tree for every meaningful combination of:
    //   markers: .completed / .failed / .done / .review / .ripped
    //   artifacts: ISO / mapfile / MKV
    //   restart_count: below / at RESTART_LIMIT
    // and assert the resulting action (or absence of one).
    //
    // Verdict vocabulary (what the action means downstream):
    //   AlreadyCompleted   — leave for mover/ack, never re-rip
    //   AlreadyFailed      — leave for operator
    //   RestartLoopFailed  — promoted to .failed this pass (3-strike gate)
    //   ResumePreserved    — partial state kept, counter bumped, resumable
    //   <wiped>            — empty/junk dir removed, no hint emitted
    // ===================================================================

    #[derive(Clone, Copy)]
    enum Mk {
        Completed,
        Failed,
        Done,
        Review,
        Ripped,
        Sweeping,
        Muxing,
        Iso,
        Mapfile,
        Mkv,
        RestartAtLimit,
        RestartBelowLimit,
        /// A non-JSON `.failed` body (e.g. review.rs's operator-cancel). Used
        /// to pin that terminal-ness keys on marker PRESENCE, not parse.
        FailedNonJson,
    }

    /// What `resume_or_quarantine_staging` must decide for one disc dir.
    #[derive(Debug, PartialEq)]
    enum Verdict {
        Completed,
        Failed,
        RestartLoopFailed,
        ResumePreserved,
        /// Dir is owned/in progress (`.sweeping`/`.muxing`) — left alone, not
        /// restart-counted.
        InProgress,
        Wiped,
    }

    fn resume_verdict(markers: &[Mk]) -> Verdict {
        let root = tmpdir();
        let disc = root.join("Disc");
        fs::create_dir_all(&disc).unwrap();
        for m in markers {
            match m {
                Mk::Completed => write_completed_marker(&disc),
                Mk::Failed => write_failed_marker(&disc, "prior failure"),
                Mk::Done => fs::write(disc.join(DONE_MARKER), b"{}").unwrap(),
                Mk::Review => fs::write(disc.join(REVIEW_MARKER), b"{}").unwrap(),
                Mk::Ripped => fs::write(disc.join(RIPPED_MARKER), b"{}").unwrap(),
                Mk::Sweeping => fs::write(disc.join(SWEEPING_MARKER), b"{}").unwrap(),
                Mk::Muxing => fs::write(disc.join(MUXING_MARKER), b"{}").unwrap(),
                Mk::FailedNonJson => {
                    // Mimic the legacy review.rs body: a non-JSON `.failed`.
                    fs::write(disc.join(FAILED_MARKER), b"cancelled by operator\n").unwrap()
                }
                Mk::Iso => fs::write(disc.join("Disc.iso"), b"x").unwrap(),
                Mk::Mapfile => fs::write(disc.join("Disc.iso.mapfile"), b"x").unwrap(),
                Mk::Mkv => fs::write(disc.join("Disc.mkv"), b"x").unwrap(),
                Mk::RestartAtLimit => fs::write(
                    disc.join(RESTART_COUNT_FILE),
                    format!("{}\n", RESTART_LIMIT).as_bytes(),
                )
                .unwrap(),
                Mk::RestartBelowLimit => fs::write(
                    disc.join(RESTART_COUNT_FILE),
                    format!("{}\n", RESTART_LIMIT - 1).as_bytes(),
                )
                .unwrap(),
            }
        }
        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        if hints.is_empty() {
            // No hint emitted: the dir was either wiped (empty/junk) or
            // skipped (UNKNOWN). For these local-FS test rows both cases
            // collapse to Wiped — the dir's continued existence makes no
            // difference to the verdict here.
            return Verdict::Wiped;
        }
        assert_eq!(hints.len(), 1, "expected exactly one disc dir");
        match &hints[0].action {
            ResumeAction::AlreadyCompleted => Verdict::Completed,
            ResumeAction::AlreadyFailed { .. } => Verdict::Failed,
            ResumeAction::RestartLoopFailed { .. } => Verdict::RestartLoopFailed,
            ResumeAction::ResumePreserved { .. } => Verdict::ResumePreserved,
            ResumeAction::InProgress => Verdict::InProgress,
        }
    }

    #[test]
    fn resume_classifier_matrix() {
        use Mk::*;
        let table: &[(&[Mk], Verdict, &str)] = &[
            // --- empty / junk: wiped ---
            (
                &[],
                Verdict::Wiped,
                "empty dir, no markers/artifacts → wipe",
            ),
            // --- .completed: terminal, leave for mover ---
            (&[Completed], Verdict::Completed, ".completed alone"),
            (
                &[Completed, Mkv],
                Verdict::Completed,
                ".completed with output",
            ),
            (
                &[Completed, Iso, Mapfile],
                Verdict::Completed,
                ".completed with leftover ISO",
            ),
            // --- .failed: terminal, leave for operator. Checked BEFORE the
            //     .done/.review carve-outs and the partial-state branch. ---
            (&[Failed], Verdict::Failed, ".failed alone"),
            (
                &[Failed, Iso],
                Verdict::Failed,
                ".failed with partial ISO still present",
            ),
            (
                &[Failed, Iso, Mapfile, RestartAtLimit],
                Verdict::Failed,
                ".failed wins even at the restart limit (terminal precedence)",
            ),
            // --- .done carve-out: crash between .done and .completed.
            //     Finished rip awaiting mover; must NOT be restart-counted. ---
            (
                &[Done],
                Verdict::Completed,
                ".done alone → AlreadyCompleted",
            ),
            (
                &[Done, Iso, Mapfile],
                Verdict::Completed,
                "CRASH WINDOW: .done + ISO + mapfile, no .completed → finished, not retried",
            ),
            (
                &[Done, Iso, Mapfile, RestartAtLimit],
                Verdict::Completed,
                ".done short-circuits even with restart_count at limit (must not become .failed)",
            ),
            // --- .review carve-out: same crash-window reasoning ---
            (
                &[Review],
                Verdict::Completed,
                ".review alone → AlreadyCompleted",
            ),
            (
                &[Review, Iso, Mapfile, Mkv],
                Verdict::Completed,
                "CRASH WINDOW: .review + artifacts, no .completed → finished/held, not retried",
            ),
            // --- partial state, no terminal marker, below limit → preserve+bump ---
            (
                &[Iso],
                Verdict::ResumePreserved,
                "ISO only → partial, preserve",
            ),
            (
                &[Iso, Mapfile],
                Verdict::ResumePreserved,
                "ISO+mapfile → partial, preserve",
            ),
            (
                &[Mapfile],
                Verdict::ResumePreserved,
                "mapfile only → partial, preserve",
            ),
            (
                &[Mkv],
                Verdict::ResumePreserved,
                "partial MKV only → partial, preserve",
            ),
            (
                &[Iso, Mapfile, RestartBelowLimit],
                Verdict::ResumePreserved,
                "partial below limit → preserve + bump",
            ),
            // --- partial state AT the restart limit → promote to .failed ---
            (
                &[Iso, RestartAtLimit],
                Verdict::RestartLoopFailed,
                "partial at RESTART_LIMIT → quarantine (.failed)",
            ),
            (
                &[Iso, Mapfile, RestartAtLimit],
                Verdict::RestartLoopFailed,
                "partial at limit with full ISO+mapfile → quarantine",
            ),
            // --- ISO present but no mapfile: still partial (the resume CLASSIFIER
            //     downstream rejects it as not-eligible, but the staging scan still
            //     preserves it as partial state to resume the sweep). ---
            (
                &[Iso, RestartBelowLimit],
                Verdict::ResumePreserved,
                "ISO + no mapfile → partial, preserve (classify_resume later rejects remux)",
            ),
            // --- .ripped-only with no artifacts: NOT in has_partial_state(),
            //     so the resume scan treats it as junk and wipes it. The mux
            //     worker (separate tick) is what acts on .ripped; the startup
            //     resume scan is artifact-driven. Documents the contract. ---
            (
                &[Ripped],
                Verdict::Wiped,
                ".ripped with no ISO/mapfile/MKV is not partial state to the resume scan → wiped",
            ),
            // --- .ripped alongside real artifacts: partial state, preserved ---
            (
                &[Ripped, Iso, Mapfile],
                Verdict::ResumePreserved,
                ".ripped + artifacts → partial state preserved (mux worker handles the .ripped)",
            ),
            // --- H2/M1: .sweeping in-progress marker. A crash mid-sweep leaves
            //     .sweeping + ISO/mapfile. Verdict is InProgress (owned) — state
            //     left intact but `.restart_count` IS bumped each skip, so a
            //     deterministic wedge converges to .failed within RESTART_LIMIT. ---
            (
                &[Sweeping],
                Verdict::InProgress,
                ".sweeping alone → owned/in-progress, leave alone",
            ),
            (
                &[Sweeping, Iso, Mapfile],
                Verdict::InProgress,
                "CRASH MID-SWEEP: .sweeping + artifacts → in-progress, not partial state",
            ),
            // R2 finding 1: BELOW the limit a healthy long sweep is left
            // InProgress (state untouched except for the per-restart
            // `.restart_count` bump) — but a sweep
            // that has wedged the watchdog RESTART_LIMIT times (the watchdog
            // bumps the count and exit(1)s, leaving `.sweeping` on disk) MUST
            // be promoted to `.failed`, else the carve-out defeats the
            // watchdog's restart-loop guard and spins forever.
            (
                &[Sweeping, Iso, Mapfile, RestartBelowLimit],
                Verdict::InProgress,
                ".sweeping below limit → healthy long rip, leave alone",
            ),
            (
                &[Sweeping, Iso, Mapfile, RestartAtLimit],
                Verdict::RestartLoopFailed,
                ".sweeping AT restart limit → deterministic wedge, quarantine (honors watchdog guard)",
            ),
            // --- H1: .muxing exclusion lock. Owned by the mux worker; same
            //     in-progress treatment as .sweeping. ---
            (
                &[Muxing, Iso, Mapfile],
                Verdict::InProgress,
                ".muxing + artifacts → mux worker owns it, in-progress",
            ),
            (
                &[Muxing, Iso, Mapfile, RestartBelowLimit],
                Verdict::InProgress,
                ".muxing below limit → mux worker owns it, leave alone",
            ),
            (
                &[Muxing, Iso, Mapfile, RestartAtLimit],
                Verdict::RestartLoopFailed,
                ".muxing AT restart limit → deterministically-wedging mux, quarantine",
            ),
            // --- M2: a non-JSON `.failed` body (review.rs operator-cancel)
            //     must still be TERMINAL — keyed on marker presence, not
            //     parse-success. ---
            (
                &[FailedNonJson],
                Verdict::Failed,
                "non-JSON .failed body is still terminal (presence-keyed)",
            ),
            (
                &[FailedNonJson, Iso, Mapfile, RestartAtLimit],
                Verdict::Failed,
                "non-JSON .failed + artifacts at restart limit → terminal, not restart-counted",
            ),
        ];
        for (markers, expected, why) in table {
            let got = resume_verdict(markers);
            assert_eq!(&got, expected, "resume matrix row failed: {why}");
        }
    }

    /// Named explicit cells (per the rc4 brief).
    #[test]
    fn resume_restart_count_at_limit_quarantines() {
        assert_eq!(
            resume_verdict(&[Mk::Iso, Mk::RestartAtLimit]),
            Verdict::RestartLoopFailed
        );
    }
    #[test]
    fn resume_completed_plus_failed_conflict_is_terminal() {
        // .completed is checked first, so the verdict is Completed — but the
        // key property is that it is NEVER re-ripped. Pin Completed.
        assert_eq!(
            resume_verdict(&[Mk::Completed, Mk::Failed]),
            Verdict::Completed,
            ".completed precedes .failed in the scan; both are terminal so the dir is never re-ripped"
        );
    }
    #[test]
    fn resume_done_only_crash_window_treated_finished() {
        assert_eq!(
            resume_verdict(&[Mk::Done, Mk::Iso, Mk::Mapfile, Mk::RestartAtLimit]),
            Verdict::Completed,
            "a .done crash-window dir must be finished, never promoted to .failed by the restart gate"
        );
    }
    #[test]
    fn resume_nothing_present_is_wiped() {
        assert_eq!(resume_verdict(&[]), Verdict::Wiped);
    }

    /// H2/M1 + R3 finding 2 regression: a dir with `.sweeping` + ISO/mapfile (a
    /// crash mid-sweep) is classified InProgress and left in place (artifacts +
    /// marker preserved). As of R3 finding 2 it is ALSO restart-counted on each
    /// InProgress skip, so a deterministically-crashing owned sweep (whose count
    /// no watchdog bumped) walks toward `.failed` over RESTART_LIMIT restarts
    /// instead of looping forever. Below the limit the dir is still preserved
    /// and never promoted to `.failed`.
    #[test]
    fn sweeping_marker_is_in_progress_and_restart_counted() {
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("MyDisc.iso"), b"x").unwrap();
        fs::write(disc.join("MyDisc.iso.mapfile"), b"x").unwrap();
        write_sweeping_marker(&disc);

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(
            matches!(hints[0].action, ResumeAction::InProgress),
            "got {:?}",
            hints[0].action
        );
        // R3 finding 2: counter is bumped on the InProgress skip (was 0 before).
        assert_eq!(
            restart_count(&disc),
            1,
            ".sweeping InProgress skip must bump .restart_count"
        );
        assert!(!disc.join(FAILED_MARKER).exists());
        // Artifacts + marker preserved for the resuming rip.
        assert!(disc.join("MyDisc.iso").exists());
        assert!(disc.join(SWEEPING_MARKER).exists());
    }

    /// Convergence M (findings 3 & 4): a STRUCTURAL mux failure in the inline
    /// fallback (ISO-open / build_iso_pipeline) must quarantine the dir, not
    /// leak `.sweeping`. The fix writes `.failed` (which clears `.sweeping`)
    /// and clears the restart count. Verify the resulting dir — ISO + mapfile
    /// present, `.sweeping` gone, `.failed` present — classifies terminal
    /// `AlreadyFailed`, NOT stranded `InProgress`, so the operator sees the
    /// failure and the dir isn't re-resumed against a permanent error.
    #[test]
    fn structural_mux_failure_quarantines_instead_of_stranding() {
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("MyDisc.iso"), b"x").unwrap();
        fs::write(disc.join("MyDisc.iso.mapfile"), b"x").unwrap();
        // Pre-state: `.sweeping` was written at staging-dir creation and the
        // inline-mux fallback is only reached because the `.ripped` hand-off
        // write failed, so `.sweeping` is still on disk here.
        write_sweeping_marker(&disc);

        // The fix's quarantine sequence (mirrors mod.rs's ISO-open /
        // build_iso_pipeline Err arms).
        write_failed_marker(&disc, "cannot open ISO for mux: ENOENT");
        clear_restart_count(&disc);

        // `.sweeping` superseded by `.failed`.
        assert!(!disc.join(SWEEPING_MARKER).exists());
        assert!(disc.join(FAILED_MARKER).exists());

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(
            matches!(hints[0].action, ResumeAction::AlreadyFailed { .. }),
            "structural mux failure must be terminal AlreadyFailed, got {:?}",
            hints[0].action
        );
    }

    /// H1 + R3 finding 2 regression: a `.muxing` lock dir is owned by the mux
    /// worker — the resume scan leaves it in place (InProgress). As of R3
    /// finding 2 it is restart-counted on the InProgress skip too, so a
    /// non-watchdog hard kill mid-mux (which left the count un-bumped) still
    /// walks toward `.failed` over RESTART_LIMIT restarts rather than looping.
    #[test]
    fn muxing_marker_is_in_progress_and_restart_counted() {
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("MyDisc.iso"), b"x").unwrap();
        fs::write(disc.join("MyDisc.iso.mapfile"), b"x").unwrap();
        write_muxing_marker(&disc);

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(matches!(hints[0].action, ResumeAction::InProgress));
        assert_eq!(restart_count(&disc), 1);
        assert!(!disc.join(FAILED_MARKER).exists());
    }

    /// Convergence R2 finding 1 regression: a deterministically-wedging mux
    /// killed by the hard watchdog re-acquires `.muxing` on every restart and
    /// the watchdog leaves `.restart_count` bumped (it exit(1)s without running
    /// any guard). The owned/in-progress carve-out must HONOR that counter:
    /// once it reaches RESTART_LIMIT the dir is promoted to `.failed` (clearing
    /// `.muxing` + the count), not left InProgress to re-dispatch and re-wedge
    /// forever. Without the fix the carve-out short-circuits before the
    /// restart-loop gate and the watchdog's guard is defeated.
    #[test]
    fn muxing_at_restart_limit_is_promoted_to_failed() {
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("MyDisc.iso"), b"x").unwrap();
        fs::write(disc.join("MyDisc.iso.mapfile"), b"x").unwrap();
        write_muxing_marker(&disc);
        // Watchdog has crashed RESTART_LIMIT times; count is on disk.
        fs::write(
            disc.join(RESTART_COUNT_FILE),
            format!("{}\n", RESTART_LIMIT).as_bytes(),
        )
        .unwrap();

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(
            matches!(hints[0].action, ResumeAction::RestartLoopFailed { .. }),
            "wedging .muxing at the restart limit must be promoted to .failed, got {:?}",
            hints[0].action
        );
        assert!(disc.join(FAILED_MARKER).exists());
        // The lock is cleared so the dir reads terminal, not owned, next pass.
        assert!(!disc.join(MUXING_MARKER).exists());
        // Count cleared so a manual re-queue starts fresh.
        assert_eq!(restart_count(&disc), 0);
    }

    /// Convergence R2 finding 1 companion: the `.sweeping` inline-mux path has
    /// the same loop. A `.sweeping` dir whose `.restart_count` already reached
    /// RESTART_LIMIT must be quarantined, with `.sweeping` cleared.
    #[test]
    fn sweeping_at_restart_limit_is_promoted_to_failed() {
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("MyDisc.iso"), b"x").unwrap();
        fs::write(disc.join("MyDisc.iso.mapfile"), b"x").unwrap();
        write_sweeping_marker(&disc);
        fs::write(
            disc.join(RESTART_COUNT_FILE),
            format!("{}\n", RESTART_LIMIT).as_bytes(),
        )
        .unwrap();

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(
            matches!(hints[0].action, ResumeAction::RestartLoopFailed { .. }),
            "wedging .sweeping at the restart limit must be promoted to .failed, got {:?}",
            hints[0].action
        );
        assert!(disc.join(FAILED_MARKER).exists());
        assert!(!disc.join(SWEEPING_MARKER).exists());
        assert_eq!(restart_count(&disc), 0);
    }

    /// Convergence R2 finding 1 boundary, updated for R3 finding 2: a restart of
    /// an owned `.muxing` dir whose count is BELOW the limit (pre-bump
    /// `rc < RESTART_LIMIT`) is still left InProgress and preserved — but the
    /// scan now bumps the counter on the skip (R3 finding 2) so a non-watchdog
    /// hard kill doesn't loop with the count pinned. Pre-bump RESTART_LIMIT-1 is
    /// below the failure gate, so the dir stays InProgress; the count advances to
    /// RESTART_LIMIT, failing on the NEXT restart.
    #[test]
    fn muxing_below_restart_limit_stays_in_progress() {
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join("MyDisc.iso"), b"x").unwrap();
        fs::write(disc.join("MyDisc.iso.mapfile"), b"x").unwrap();
        write_muxing_marker(&disc);
        fs::write(
            disc.join(RESTART_COUNT_FILE),
            format!("{}\n", RESTART_LIMIT - 1).as_bytes(),
        )
        .unwrap();

        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(
            matches!(hints[0].action, ResumeAction::InProgress),
            "below-limit .muxing must stay InProgress, got {:?}",
            hints[0].action
        );
        assert!(!disc.join(FAILED_MARKER).exists());
        assert!(disc.join(MUXING_MARKER).exists());
        // R3 finding 2: the scan bumps the counter on the InProgress skip — the
        // dir advances to the limit and fails on the next restart.
        assert_eq!(restart_count(&disc), RESTART_LIMIT);
    }

    /// H2/M1: the `.sweeping` marker is superseded by every terminal/hand-off
    /// transition. `write_failed_marker`, `write_completed_marker`, and
    /// `muxer::write_marker` (`.ripped`) all clear it so a finished/quarantined
    /// dir isn't also mis-read as an active sweep.
    #[test]
    fn sweeping_marker_cleared_by_terminal_writes() {
        let d = tmpdir();
        write_sweeping_marker(&d);
        assert!(d.join(SWEEPING_MARKER).exists());
        write_completed_marker(&d);
        assert!(
            !d.join(SWEEPING_MARKER).exists(),
            ".completed must clear .sweeping"
        );

        let d2 = tmpdir();
        write_sweeping_marker(&d2);
        write_failed_marker(&d2, "boom");
        assert!(
            !d2.join(SWEEPING_MARKER).exists(),
            ".failed must clear .sweeping"
        );

        let d3 = tmpdir();
        write_sweeping_marker(&d3);
        clear_sweeping_marker(&d3);
        assert!(!d3.join(SWEEPING_MARKER).exists());
        // Idempotent: clearing an already-gone marker must not panic/error.
        clear_sweeping_marker(&d3);
    }

    /// M2 regression: a `.failed`-only dir with a non-JSON body (review.rs's
    /// legacy "cancelled by operator") is still terminal to the resume scan.
    /// `read_failed_reason` returns None for it, but the scan keys on
    /// `has_failed` (presence), not parse-success.
    #[test]
    fn non_json_failed_is_terminal() {
        let root = tmpdir();
        let disc = root.join("MyDisc");
        fs::create_dir_all(&disc).unwrap();
        fs::write(disc.join(FAILED_MARKER), b"cancelled by operator\n").unwrap();
        // The parser can't read a reason out of it...
        assert_eq!(read_failed_reason(&disc), None);
        // ...but the scan still treats it as terminal.
        let hints = resume_or_quarantine_staging(root.to_str().unwrap());
        assert_eq!(hints.len(), 1);
        assert!(
            matches!(hints[0].action, ResumeAction::AlreadyFailed { .. }),
            "got {:?}",
            hints[0].action
        );
        // And the snapshot exposes has_failed even with no parseable reason.
        let snap = snapshot_staging_disc(&disc).unwrap();
        assert!(snap.has_failed);
        assert!(snap.failed_reason.is_none());
    }

    /// Same guarantee for `increment_restart_count`: a rename failure must not
    /// leak `.restart_count.tmp`.
    #[test]
    fn restart_count_rename_failure_cleans_up_tmp() {
        let d = tmpdir();
        // Make the final target a non-empty directory so rename-over fails.
        let target = d.join(RESTART_COUNT_FILE);
        fs::create_dir(&target).unwrap();
        fs::write(target.join("occupant"), b"x").unwrap();

        let res = increment_restart_count(&d);
        assert!(
            res.is_err(),
            "precondition: rename onto a non-empty dir must fail"
        );

        let tmp = d.join(format!("{}.tmp", RESTART_COUNT_FILE));
        assert!(
            !tmp.exists(),
            "the .tmp sibling must be cleaned up after a rename failure, found: {}",
            tmp.display()
        );
    }
}
