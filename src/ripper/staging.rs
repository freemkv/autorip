//! Staging-directory bookkeeping: free-space probe, wipe, .done marker.
//!
//! Lifted verbatim from the monolithic `ripper.rs` as part of the 0.18
//! prep split — no semantic changes.

/// Remove every subdirectory under `cfg.staging_dir`. Used on startup (all
/// prior session state is gone, so anything still on disk is orphaned from
/// a killed process) and on user-initiated stop (stop == reset — clean
/// slate so the next rip doesn't accidentally resume stale state).
///
/// Best-effort: logs each removal, silently ignores errors on individual
/// entries. A locked or not-yet-created staging root is not fatal.
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
        if path.join(".done").exists() {
            tracing::info!(path = %path.display(), "preserving staging entry — .done marker present (mover will handle)");
            continue;
        }
        match std::fs::remove_dir_all(&path) {
            Ok(_) => tracing::info!(path = %path.display(), "wiped stale staging entry"),
            Err(e) => tracing::warn!(path = %path.display(), error = %e, "staging wipe skipped"),
        }
    }
}
