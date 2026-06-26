use crate::config::Config;
use crate::tmdb;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

/// Move progress — separate from device/rip state.
/// Read by the System page's renderMoves() via SSE.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MoveState {
    pub name: String,
    pub progress_pct: u8,
    pub progress_gb: f64,
    pub total_gb: f64,
    pub speed_mbs: f64,
    pub eta: String,
}

pub static MOVE_STATE: once_cell::sync::Lazy<Mutex<Option<MoveState>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(None));

/// Per-staging-dir error surfaced to the System page so the user can act
/// on it (e.g. orphaned source files that the container can't unlink due
/// to NFS squash perms). Keyed by staging dir path. The stored entry is
/// always refreshed, but the syslog line is only emitted when the
/// `reason` changes — so repeating the same error on every loop tick
/// updates the UI without spamming the log.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MoverError {
    pub path: String,
    pub reason: String,
    pub hint: String,
}

pub static MOVE_ERRORS: once_cell::sync::Lazy<Mutex<BTreeMap<String, MoverError>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(BTreeMap::new()));

fn record_error(path: &str, reason: &str, hint: &str) {
    let Ok(mut m) = MOVE_ERRORS.lock() else {
        return;
    };
    let same_reason = m.get(path).map(|e| e.reason == reason).unwrap_or(false);
    m.insert(
        path.to_string(),
        MoverError {
            path: path.to_string(),
            reason: reason.to_string(),
            hint: hint.to_string(),
        },
    );
    if !same_reason {
        crate::log::syslog(&format!("Move blocked: {} — {}", path, reason));
    }
}

fn clear_error(path: &str) {
    if let Ok(mut m) = MOVE_ERRORS.lock() {
        m.remove(path);
    }
}

/// Outcome of moving a single file. Distinguishes between an active move
/// (Moved / MovedDirty) and a no-op re-check (Skipped) so the caller can
/// suppress webhook spam and log noise on subsequent loop ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MoveOutcome {
    /// Dest already exists with the same size as src — already moved on
    /// a previous tick. Source may or may not still be present.
    Skipped,
    /// Atomic rename succeeded — src is gone, dest has the bytes.
    Moved,
    /// Copy succeeded but unlink of src failed (perm/NFS issue). Dest has
    /// the bytes. Caller should record an error and stop trying to clean
    /// the staging dir; subsequent ticks will Skip.
    MovedDirty,
    /// Copy itself failed. Caller can retry on the next tick.
    Failed,
    /// 0.20.8 validation-audit fix #3: `cp` reported success but the
    /// post-copy size check found dst != src. Surfaces as a distinct
    /// error reason on the System page so a half-copied destination
    /// (e.g. NFS server ran out of space mid-cp without returning an
    /// error to the cp process) isn't silently treated as a successful
    /// move + source unlink. Caller leaves the staging dir alone — the
    /// dst is the broken copy, src is the source of truth.
    SizeMismatch,
    /// Post-copy validation failed for a NON-size reason: a structural
    /// check (missing EBML head, short/garbled tail, insufficient TS sync)
    /// or an unreadable destination. Kept distinct from `SizeMismatch` so
    /// the operator gets an accurate hint — an ENOSPC/short-write hint is
    /// wrong for a structurally-invalid-but-correctly-sized copy. Like
    /// `SizeMismatch`, the caller leaves the staging dir alone (src is the
    /// source of truth) and retries next tick.
    PostCopyInvalid,
    /// Destination already exists as a DIFFERENT file (present, non-empty, and a
    /// different size than src). A wrong title match can resolve two distinct
    /// discs to the same `Title (Year)/Title (Year).ext` path; overwriting would
    /// destroy a good prior rip. We refuse the move, leave the new file in
    /// staging, and surface a collision error for the operator to resolve.
    Collision,
}

/// 0.20.8 validation-audit fix #3 (revised v0.25.3): errors from the
/// post-copy validation step inside `move_file`. Kept distinct from
/// `MoveOutcome` because the outcome is the move-loop's view (Skipped /
/// Moved / Failed / SizeMismatch), whereas `MoveError` is the
/// validation-helper's view of *why* the cp result was rejected. The
/// helper is unit-testable in isolation (see `check_post_copy`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MoveError {
    /// The post-copy `stat()` of src and dst disagreed on length. `cp`
    /// returned 0 but the destination is short (or, in pathological
    /// cases, longer than src). Reported via the same `record_error`
    /// path that surfaces other move failures on the System page.
    SizeDoesNotMatch { src_size: u64, dst_size: u64 },
    /// MKV-specific: the destination didn't start with the EBML magic
    /// `1A 45 DF A3`. Either the cp truncated at the head, or the
    /// destination wasn't really an MKV to begin with.
    MkvBadHead,
    /// MKV-specific: the destination is too short, or its tail bytes
    /// couldn't be read back. This is a truncation/readability gate, not
    /// a structural EBML parse — see `check_post_copy_mkv`.
    MkvBadTail,
    /// TS / m2ts: not enough sync bytes (0x47) at TS-packet boundaries
    /// in the file head or tail to consider the file structurally
    /// sound. Likely a truncated cp.
    M2tsBadSync,
    /// Could not open the destination for read (NFS gone away, perm,
    /// etc.). Treat as a serious post-copy condition that warrants
    /// quarantine.
    Unreadable(String),
}

impl std::fmt::Display for MoveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MoveError::SizeDoesNotMatch { src_size, dst_size } => write!(
                f,
                "post-cp size mismatch: src={} bytes, dst={} bytes",
                src_size, dst_size
            ),
            MoveError::MkvBadHead => {
                write!(f, "destination MKV missing EBML header (1A 45 DF A3)")
            }
            MoveError::MkvBadTail => write!(f, "destination MKV tail too short or unreadable"),
            MoveError::M2tsBadSync => write!(
                f,
                "destination m2ts has insufficient TS sync (0x47) at packet boundaries"
            ),
            MoveError::Unreadable(e) => write!(f, "destination unreadable: {}", e),
        }
    }
}

/// Stat a path while bypassing the NFS attribute cache — opens a fresh
/// FD and fstats it. Hit this instead of `std::fs::metadata` whenever
/// the value matters within an attribute-cache window (acregmin, NFS
/// default 3 s) of a write by another process. Stat'ing the dest right
/// after cp closes triggered phantom SizeMismatch errors on an NFS
/// share before this helper landed.
fn fresh_metadata(path: &Path) -> std::io::Result<std::fs::Metadata> {
    let f = std::fs::File::open(path)?;
    f.metadata()
}

/// Cheap content-identity probe for two files KNOWN to be the same length.
/// Reads a fixed-size head and tail window from each and compares them.
/// Used by the collision guard to tell an idempotent re-move (the
/// dest IS this rip's output, copied on a prior tick whose unlink failed)
/// apart from a genuine collision (a wrong title match routed two
/// DIFFERENT discs to the same path, and their muxes happen to be the same
/// byte length). Returns `true` only when both windows match on both files.
///
/// Fixed-size reads keep this O(1) — we never read the whole multi-GB file.
/// A false "same" would require two different discs to be byte-identical in
/// both their first and last 64 KiB AND identical in length; that is not a
/// realistic mux collision. On any read error we conservatively return
/// `false` (treat as NOT confirmed identical → real collision), so a probe
/// failure can never green-light clobbering a different file.
fn same_head_and_tail(a: &Path, b: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    const WINDOW: u64 = 64 * 1024;

    fn windows(path: &Path, window: u64) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
        let mut f = std::fs::File::open(path)?;
        let size = f.metadata()?.len();
        let n = window.min(size) as usize;
        let mut head = vec![0u8; n];
        f.read_exact(&mut head)?;
        let mut tail = vec![0u8; n];
        f.seek(SeekFrom::End(-(n as i64)))?;
        f.read_exact(&mut tail)?;
        Ok((head, tail))
    }

    match (windows(a, WINDOW), windows(b, WINDOW)) {
        (Ok(wa), Ok(wb)) => wa == wb,
        _ => false,
    }
}

/// Copy `src` → `dest` in 4 MiB chunks, publishing the running
/// bytes-written count into `written` as we go.
///
/// This is what lets the mover show real progress: the move loop reads
/// `written` (bytes WE have pushed) instead of `stat()`-ing the
/// destination. On an NFS share under concurrent rip+mover I/O a dest
/// stat blocks for minutes or reads a stale `0`, which used to freeze the
/// System page move telemetry at 0 % for the entire copy (pre-0.26.x bug
/// this replaces). Counting our own writes can't stall and can't go stale.
///
/// `std::fs::copy`'s kernel fast paths (`copy_file_range`/`sendfile`) don't
/// apply across filesystems, and staging→library is the only path that
/// reaches here — same-filesystem moves take the `rename(2)` fast path — so
/// a plain buffered loop loses no acceleration in practice.
fn copy_counting(
    src: &Path,
    dest: &Path,
    written: &std::sync::atomic::AtomicU64,
) -> std::io::Result<u64> {
    use std::io::{Read, Write};
    use std::sync::atomic::Ordering;

    // Write to a sibling temp on the DEST filesystem, fsync it, then
    // rename(2) over the final name (atomic within the dest fs). Writing
    // directly to the final path would leave a truncated file at the real
    // name if we're SIGKILLed / OOM-killed / lose power mid-copy — the
    // mover's post-copy size check would then see a wrong-size file and
    // wedge the move on a phantom Collision. The temp+rename makes the
    // final name appear only once the bytes are fully written and durable.
    let tmp = {
        let mut name = dest.file_name().unwrap_or_default().to_os_string();
        name.push(format!(".part-{}", std::process::id()));
        dest.with_file_name(name)
    };
    // Remove any stale temp from a prior interrupted run before we start.
    let _ = std::fs::remove_file(&tmp);
    // The temp name embeds OUR pid, so the line above only clears our own
    // exact name. Orphaned `.part-<other-pid>` temps from prior crashed runs
    // (different pid) would otherwise linger forever. Scan the dest parent for
    // any `<dest-name>.part-*` and remove them before creating the new temp.
    if let Some(parent) = dest.parent() {
        if let Some(stem) = dest.file_name().and_then(|n| n.to_str()) {
            let prefix = format!("{stem}.part-");
            if let Ok(entries) = std::fs::read_dir(parent) {
                for entry in entries {
                    // Don't `.flatten()` away per-entry errors: a partial
                    // NFS degradation can error on an individual DirEntry,
                    // skipping a `.part-*` orphan we'd otherwise remove. The
                    // current copy still writes its own fresh `.part-<pid>`,
                    // so correctness is unaffected — but without this WARN a
                    // persistently degraded mount would let orphaned temps
                    // accumulate with no operator signal at all. (Mirrors the
                    // no-`flatten()` rationale in `list_staging_basenames`.)
                    let entry = match entry {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                dir = %parent.display(),
                                "mover: cannot read dir entry while clearing orphaned \
                                 .part-* temps; an orphan may be left behind"
                            );
                            continue;
                        }
                    };
                    if let Some(name) = entry.file_name().to_str() {
                        if name.starts_with(&prefix) {
                            let _ = std::fs::remove_file(entry.path());
                        }
                    }
                }
            }
        }
    }

    let copy_to_tmp = || -> std::io::Result<u64> {
        let mut reader = std::fs::File::open(src)?;
        let mut writer = std::fs::File::create(&tmp)?;
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        let mut total = 0u64;
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            writer.write_all(&buf[..n])?;
            total += n as u64;
            written.store(total, Ordering::Relaxed);
        }
        writer.flush()?;
        // fsync the temp's data+metadata before the rename: move_file
        // unlinks the source once this returns Ok, so the destination must
        // be durable on stable storage first. On a cross-fs (NFS) move,
        // flush() on a File is a no-op — without sync_all a server/host
        // crash in the close-to-commit window would lose the only copy.
        writer.sync_all()?;
        Ok(total)
    };

    let total = match copy_to_tmp() {
        Ok(t) => t,
        Err(e) => {
            // Drop the partial temp so the next attempt starts clean and
            // no orphan lingers on the dest fs.
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
    };

    // fsync the dest parent dir so the temp's dirent is durable, then
    // rename(2) over the final name (atomic within the fs), then fsync the
    // dir again so the rename itself is durable before move_file unlinks
    // the source. A crash at any point leaves either no final-name file or
    // the complete one — never a truncated file at the real name.
    if let Some(parent) = dest.parent() {
        libfreemkv::io::fsync::dir(parent);
    }
    if let Err(e) = std::fs::rename(&tmp, dest) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    if let Some(parent) = dest.parent() {
        libfreemkv::io::fsync::dir(parent);
    }
    Ok(total)
}

/// Verify a destination MKV by confirming the EBML head magic
/// (`1A 45 DF A3`) and that the tail bytes are present and readable.
/// This is a truncation/readability gate, NOT a structural EBML parse:
/// it does not verify the tail forms a valid EBML element (a
/// structurally-wrong-but-readable tail passes). Full parsing would drag
/// in the EBML reader and is overkill per move tick — the mux already
/// validated the stream when it wrote the file; here we only need to
/// confirm cp didn't truncate, without relying on NFS attribute freshness.
fn check_post_copy_mkv(dst: &Path) -> Result<(), MoveError> {
    use std::io::{Read, Seek, SeekFrom};

    let mut f = std::fs::File::open(dst).map_err(|e| MoveError::Unreadable(e.to_string()))?;

    // Head: EBML magic 1A 45 DF A3 in the first 4 bytes.
    let mut head = [0u8; 4];
    f.read_exact(&mut head)
        .map_err(|e| MoveError::Unreadable(e.to_string()))?;
    if head != [0x1A, 0x45, 0xDF, 0xA3] {
        return Err(MoveError::MkvBadHead);
    }

    // Tail: confirm the last 8 bytes are readable (the file isn't
    // truncated to zero and the kernel is willing to surface the tail).
    // We do NOT structurally parse EBML here — the mux already validated
    // the stream when it wrote the file; this gate only catches a cp that
    // truncated the output. Stronger structural parsing would require
    // dragging in the EBML reader, which is overkill for the move gate.
    let size = f
        .metadata()
        .map_err(|e| MoveError::Unreadable(e.to_string()))?
        .len();
    if size < 5 {
        return Err(MoveError::MkvBadTail);
    }
    let tail_len = 8u64.min(size);
    f.seek(SeekFrom::End(-(tail_len as i64)))
        .map_err(|e| MoveError::Unreadable(e.to_string()))?;
    let mut tail = [0u8; 8];
    let read = f
        .read(&mut tail[..tail_len as usize])
        .map_err(|e| MoveError::Unreadable(e.to_string()))?;
    if read < tail_len as usize {
        return Err(MoveError::MkvBadTail);
    }
    Ok(())
}

/// Verify a destination m2ts file has plausible TS sync bytes (0x47)
/// at 192-byte BD-TS packet boundaries in the head and tail. BD-TS
/// uses 192-byte packets (4-byte arrival-time prefix + 188-byte TS
/// payload), so the sync byte lives at offset 4 within each packet.
/// We sample the first and last 8 packets — if cp truncated, the tail
/// won't align.
fn check_post_copy_m2ts(dst: &Path) -> Result<(), MoveError> {
    use std::io::{Read, Seek, SeekFrom};

    const PKT: u64 = 192;
    const SYNC_OFFSET: usize = 4;
    const SAMPLE_PACKETS: u64 = 8;
    const THRESHOLD: usize = 6; // out of 2 * SAMPLE_PACKETS (head + tail = 16 samples)

    let mut f = std::fs::File::open(dst).map_err(|e| MoveError::Unreadable(e.to_string()))?;
    let size = f
        .metadata()
        .map_err(|e| MoveError::Unreadable(e.to_string()))?
        .len();
    // Require room for two DISTINCT, non-overlapping sample windows. With
    // a single window (`PKT * SAMPLE_PACKETS`) a file of 8..16 packets
    // would have its head window (0..1536) overlap the tail window
    // (End(-1536)), so the same 8 intact head sync bytes get counted
    // twice and reach THRESHOLD=6 from a single intact head — a tail-
    // truncated cp would pass. Demanding 2x the sample size keeps head
    // and tail strictly disjoint.
    if size < PKT * SAMPLE_PACKETS * 2 {
        return Err(MoveError::M2tsBadSync);
    }

    let mut count = 0usize;
    let mut buf = vec![0u8; (PKT * SAMPLE_PACKETS) as usize];

    // Head
    f.read_exact(&mut buf)
        .map_err(|e| MoveError::Unreadable(e.to_string()))?;
    for i in 0..SAMPLE_PACKETS as usize {
        let off = i * PKT as usize + SYNC_OFFSET;
        if buf[off] == 0x47 {
            count += 1;
        }
    }

    // Tail
    f.seek(SeekFrom::End(-((PKT * SAMPLE_PACKETS) as i64)))
        .map_err(|e| MoveError::Unreadable(e.to_string()))?;
    f.read_exact(&mut buf)
        .map_err(|e| MoveError::Unreadable(e.to_string()))?;
    for i in 0..SAMPLE_PACKETS as usize {
        let off = i * PKT as usize + SYNC_OFFSET;
        if buf[off] == 0x47 {
            count += 1;
        }
    }

    // 6 / 16 sync bytes is loose; gives us cushion for a non-BD-TS
    // m2ts variant with a slightly different prefix layout, while
    // still catching a truncated cp where the tail packets are all
    // garbage.
    if count < THRESHOLD {
        return Err(MoveError::M2tsBadSync);
    }
    Ok(())
}

/// Verify a destination by size only, using a fresh-FD stat that
/// bypasses the NFS attribute cache. Used for ISO files (no
/// lightweight structural check available without parsing UDF).
fn check_post_copy_size(src: &Path, dst: &Path) -> Result<(), MoveError> {
    // Do NOT default to 0 on a stat failure. The old `unwrap_or(0)`
    // turned a failed dst stat into "size 0" — and if the src stat also
    // failed, 0 == 0 validated a *missing* destination, after which
    // `move_file` would `remove_file(src)` and destroy the only copy of
    // the bytes. A post-copy destination must always be readable; a stat
    // error there is itself a validation failure, not a size.
    let dst_size = fresh_metadata(dst)
        .map_err(|e| MoveError::Unreadable(format!("dst stat failed: {e}")))?
        .len();
    let src_size = fresh_metadata(src)
        .map_err(|e| MoveError::Unreadable(format!("src stat failed: {e}")))?
        .len();
    if src_size != dst_size {
        return Err(MoveError::SizeDoesNotMatch { src_size, dst_size });
    }
    Ok(())
}

/// Format-aware post-cp validation. Routes to a structural check
/// (EBML head/tail for .mkv; TS sync for .m2ts) when possible, falls
/// back to a fresh-FD size compare for .iso (which is large enough
/// that the fresh-FD stat dodge is the practical fix anyway).
///
/// Replaces the v0.25.x `check_post_copy_sizes` helper, which used
/// `std::fs::metadata` directly on the dest immediately after cp
/// closed — that read could be served from the NFS attribute cache
/// and produced phantom SizeMismatch failures on an NFS share (the
/// file was intact, the stat lied). A v0.25.2 release-test rip hit
/// this on a 58 GiB mkv that had byte-for-byte landed.
pub(crate) fn check_post_copy(src: &Path, dst: &Path) -> Result<(), MoveError> {
    let ext = dst
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    // The structural checks (EBML head/tail, TS sync) only inspect a
    // fixed-size head/tail window — a cp truncated to anything above
    // that window (>= a few KiB) still passes them. That is a DATA-LOSS
    // hazard: a passing validation lets `move_file` unlink the source,
    // so a short destination becomes the only (broken) copy. Pair every
    // structural check with the same fresh-FD src-vs-dst size compare the
    // ISO path already does — the size cross-check is what actually
    // guarantees the destination isn't truncated.
    match ext.as_deref() {
        Some("mkv") => {
            check_post_copy_mkv(dst)?;
            check_post_copy_size(src, dst)
        }
        Some("m2ts") => {
            check_post_copy_m2ts(dst)?;
            check_post_copy_size(src, dst)
        }
        _ => check_post_copy_size(src, dst),
    }
}

pub fn run(cfg: &Arc<RwLock<Config>>) {
    use std::sync::atomic::Ordering;
    tracing::info!("mover loop starting");
    while !crate::SHUTDOWN.load(Ordering::Relaxed) {
        let cfg_snapshot = match cfg.read() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "mover: config lock poisoned, retrying");
                std::thread::sleep(std::time::Duration::from_secs(10));
                continue;
            }
        };
        check_and_move(&cfg_snapshot);
        drop(cfg_snapshot);
        // SHUTDOWN-responsive sleep — break early on signal so SIGTERM
        // doesn't have to wait the full 10 s tick.
        for _ in 0..100 {
            if crate::SHUTDOWN.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    tracing::info!("mover loop stopping");
}

/// How the mover should treat a failed `.done` read on a staging dir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DoneAbsence {
    /// `.done` is absent but a governing marker (`.sweeping`/`.muxing`/
    /// `.ripped`/`.completed`/`.failed`/`.review`) shows the ripper/mux worker
    /// still owns the dir — expected "not ready yet" state. Quiet debug, skip
    /// (no WARN).
    InProgress,
    /// A real fault: non-NotFound read error (NFS ESTALE, EACCES), or a
    /// NotFound on a stranded dir with no governing marker. Worth a WARN.
    Fault,
}

/// Classify a `.done` read error. NotFound + any governing marker present is
/// the by-design in-progress state; everything else is a fault. Pulled out so
/// the warn-vs-debug split is unit-testable without a tracing capture.
///
/// The governing-marker probe is advisory, not a lock: between the `.done` read
/// and these `exists()` calls the ripper/mux worker can land a marker, or the
/// whole staging dir can be removed (a finished move, or `/api/stop` cleanup).
/// That TOCTOU is inherent to a best-effort classification on a hot staging
/// path and is handled deliberately:
///   - If the dir itself has vanished (NotFound) we treat it as InProgress, not
///     Fault — a dir that disappeared out from under us is a normal lifecycle
///     transition, not a stranded-dir condition worth a WARN. Probing the dir
///     once (rather than each marker) also collapses the race window: we read
///     the directory's existence a single time instead of four marker joins.
fn classify_done_absence(err_kind: std::io::ErrorKind, dir: &Path) -> DoneAbsence {
    if err_kind == std::io::ErrorKind::NotFound {
        // The staging dir was removed between the `.done` read and now (move
        // finalised, or stop-cleanup): not a stranded dir, so don't WARN.
        if !dir.exists() {
            return DoneAbsence::InProgress;
        }
        // `.sweeping` (multi-hour sweep+patch in progress, before `.ripped`)
        // and `.muxing` (mux worker owns the dir) join the governed set so a
        // dir in either phase is the by-design "not ready yet" state, not a
        // stranded `Fault`. The `.sweeping` window was previously ungoverned:
        // the mover saw no marker every 10s tick and WARN-flooded (182 warns
        // for one healthy in-progress disc, see the comment in check_and_move).
        //
        // Probe the markers via `snapshot_staging_disc`, NOT bare
        // `dir.join(m).exists()`. On a cold NFS attribute cache (typical right
        // after a container restart) a single-shot `exists()` can return false
        // for a marker that is durably on the server — that false-negative
        // would classify a healthy in-progress sweep as a `Fault` and WARN
        // every 10s tick for the whole multi-hour window, the exact 182-warn
        // bug `.sweeping` was added to kill. The snapshot retries `read_dir`
        // up to 3x with 500ms gaps (the same defense `disc_owned_by_worker`
        // and the startup resume scan rely on). A `None` snapshot means the
        // dir contents are unknown (read_dir errored every retry): treat that
        // as ungoverned and fall through to `Fault` so a genuinely stranded /
        // unreadable dir still surfaces a WARN.
        let governed = crate::ripper::staging::snapshot_staging_disc(dir)
            .map(|s| {
                s.has_sweeping
                    || s.has_muxing
                    || s.has_ripped
                    || s.completed
                    || s.has_failed
                    || s.has_review
            })
            .unwrap_or(false);
        if governed {
            return DoneAbsence::InProgress;
        }
    }
    DoneAbsence::Fault
}

fn check_and_move(cfg: &Config) {
    // Scan staging directory for completed rips (directories with .done marker)
    let staging_root = &cfg.staging_dir;
    let entries = match std::fs::read_dir(staging_root) {
        Ok(e) => e,
        Err(e) => {
            // Don't swallow this: a dropped NFS mount or a deleted staging
            // root surfaces here, and a silent return makes the mover look
            // healthy while moving nothing. Make it observable.
            tracing::warn!(
                staging = %staging_root,
                error = %e,
                "mover: failed to read staging directory; skipping this pass"
            );
            return;
        }
    };

    for entry in entries {
        // Don't silently drop a per-entry error (e.g. NFS ESTALE on a
        // specific dentry): on a loaded share a completed rip would be
        // missed for the whole tick with no trace. Log and skip.
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    staging = %staging_root,
                    error = %e,
                    "mover: per-entry error listing staging root; skipping entry"
                );
                continue;
            }
        };
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }

        let marker_path = dir.join(".done");
        // No pre-flight exists() check: it races with the read below (a `.done`
        // can be created or removed in the window between the two syscalls).
        // The read_to_string Err arm is the single atomic gate — a NotFound is
        // handled there (skip) exactly like any other read failure.

        // Read marker for TMDB metadata
        let marker: serde_json::Value = match std::fs::read_to_string(&marker_path) {
            Ok(data) => match serde_json::from_str(&data) {
                Ok(v) => v,
                Err(e) => {
                    // An empty or torn `.done` (e.g. a crash mid-write before
                    // the durable-marker fix landed, or a partial NFS write)
                    // parses as an error. Treat it as NOT READY: skip — do NOT
                    // `unwrap_or_default()` into a `null` marker, which would
                    // give empty title+disc_name and blind-move the file to the
                    // output root under a garbage name. Leaving the dir in
                    // staging lets the next pass (or a rewritten marker) recover.
                    tracing::warn!(
                        marker = %marker_path.display(),
                        error = %e,
                        "mover: .done marker is empty/unparsable; skipping staging dir (not ready)"
                    );
                    continue;
                }
            },
            Err(e) => {
                // A `.done` that is simply ABSENT is the EXPECTED state for any
                // staging dir the ripper/mux worker still governs — the mover
                // is not the dir's hand-off until `.done` lands. The lifecycle
                // is: `.sweeping` (sweep+patch in progress) → `.ripped` (awaiting
                // mux) → mux runs (`.muxing`) → `.done`/`.review` + `.completed`.
                // For the whole rip+mux phase (which for a long disc is many
                // minutes — the sweep alone can be hours, i.e. thousands of 10s
                // ticks) the dir has a
                // `.sweeping`/`.muxing`/`.ripped`/`.completed`/`.failed`/`.review`
                // marker but no `.done`. WARNing every tick on that absence is
                // misleading spam that looks like the mover is broken (seen
                // live: 182 warns for one in-progress disc that ultimately
                // ripped and moved cleanly). Treat NotFound while a governing
                // marker is present as the by-design "not ready yet" state:
                // quiet debug, skip. Only a `.done` NotFound on a dir with NO
                // governing marker (truly stranded) — or any non-NotFound read
                // error (NFS ESTALE, EACCES) — is a real fault worth a WARN.
                if classify_done_absence(e.kind(), &dir) == DoneAbsence::InProgress {
                    tracing::debug!(
                        dir = %dir.display(),
                        "mover: staging dir in progress (no .done yet); skipping"
                    );
                    continue;
                }
                // A genuine .done read failure (NFS ESTALE, permission denied,
                // or NotFound on a stranded dir with no governing marker) leaves
                // the dir in staging looking healthy from the mover's view until
                // the handle recovers; surface it.
                tracing::warn!(
                    marker = %marker_path.display(),
                    error = %e,
                    "mover: failed to read .done marker; skipping staging dir"
                );
                continue;
            }
        };

        let disc_name = marker["disc_name"].as_str().unwrap_or("").to_string();
        let display_name = marker["title"].as_str().unwrap_or(&disc_name).to_string();

        // A parsable-but-content-empty marker (both `title` and `disc_name`
        // absent/empty) carries no usable destination name. Filing it would
        // route the MKV to the output root under an empty name. Treat as NOT
        // READY and skip — never `remove_dir_all` or blind-move on this path.
        if display_name.trim().is_empty() {
            tracing::warn!(
                marker = %marker_path.display(),
                "mover: .done marker has empty title and disc_name; skipping staging dir (not ready)"
            );
            continue;
        }

        // Build TMDB result from marker
        let tmdb_result = if !marker["title"].is_null() {
            Some(tmdb::TmdbResult {
                title: marker["title"].as_str().unwrap_or("").to_string(),
                // Clamp before the cast: a year above 65535 would wrap to a
                // small number (e.g. 70000 -> 4464) and mislabel the folder.
                // 9999 is well past any real release year.
                year: marker["year"].as_u64().unwrap_or(0).min(9999) as u16,
                poster_url: marker["poster_url"].as_str().unwrap_or("").to_string(),
                overview: marker["overview"].as_str().unwrap_or("").to_string(),
                media_type: marker["media_type"].as_str().unwrap_or("movie").to_string(),
            })
        } else {
            None
        };

        // Find ripped files. `keep_iso=false` means the operator does not
        // want the intermediate ISO promoted to the output library — only
        // the muxed MKV. Pre-0.25.10 this filter accepted any `.iso` in
        // staging regardless, so the mover happily moved 90+ GB of ISO
        // bytes to NFS the moment `.done` appeared (the ripper's own
        // ISO-prune in `rip_disc` only runs after `.done` is written, so
        // the mover's 10s scan loop typically wins the race). Hit live
        // (2026-05-20, a 94 GB ISO copied into the movies library). So we
        // filter the ISO out at planning time; the staging-cleanup branch
        // below deletes any leftover .iso from disk before tearing the
        // dir down so we don't leak intermediate ISOs in /staging.
        //
        // `output_format == "iso"` also moves the ISO: there the disc image
        // IS the deliverable (the ripper skipped the title mux), so the
        // staging dir holds no `.mkv` — only the `.iso` to promote.
        let move_iso = cfg.keep_iso || cfg.output_format == "iso";
        let ripped_files: Vec<std::path::PathBuf> = match std::fs::read_dir(&dir) {
            Ok(entries) => {
                // Don't `.filter_map(|e| e.ok())` away per-entry errors: on a
                // cold-cache or degraded NFS mount a single DirEntry I/O error
                // can silently drop the only .mkv, leaving `ripped_files` empty
                // and the job skipped every tick with no operator visibility.
                // Mirror the outer staging-root loop (line ~550) and
                // `find_iso_and_mapfile` (resume.rs): match each entry
                // explicitly and surface the error via record_error.
                let mut files = Vec::new();
                for entry in entries {
                    let entry = match entry {
                        Ok(e) => e,
                        Err(e) => {
                            record_error(
                                &dir.to_string_lossy(),
                                &format!(
                                    "per-entry error listing staging directory {}: {}",
                                    dir.display(),
                                    e
                                ),
                                "check that the staging mount is healthy and readable; \
                                 staging contents are unknown for this directory",
                            );
                            continue;
                        }
                    };
                    let p = entry.path();
                    if p.extension()
                        .and_then(|x| x.to_str())
                        .map(|ext| match ext {
                            "mkv" | "m2ts" => true,
                            "iso" => move_iso,
                            _ => false,
                        })
                        .unwrap_or(false)
                    {
                        files.push(p);
                    }
                }
                files
            }
            Err(e) => {
                // Enumerating the staging dir's contents failed (e.g. a
                // transient NFS read_dir error). Without this arm the dir
                // would be skipped silently every tick — a `.done` marker
                // that never gets acted on, invisible on the System page.
                // Surface it like every other error path in this function.
                record_error(
                    &dir.to_string_lossy(),
                    &format!("cannot list staging directory {}: {}", dir.display(), e),
                    "check that the staging mount is healthy and readable",
                );
                continue;
            }
        };

        if ripped_files.is_empty() {
            // Nothing the mover should promote. Skip; the dir's lifetime
            // is governed by the ripper (which writes the .done marker
            // and is responsible for its own ISO-prune in the
            // keep_iso=false multipass path).
            continue;
        }

        let dir_str = dir.to_string_lossy().to_string();

        // Build destination paths
        let mut planned_moves: Vec<(std::path::PathBuf, String)> = Vec::new();
        for file_path in &ripped_files {
            let filename = file_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            let dest = build_destination(cfg, &tmdb_result, &filename);
            planned_moves.push((file_path.clone(), dest));
        }

        // FAIL-LOUD destination-root validation (Mercy incident hardening).
        // Before creating ANY per-title subdir, confirm the configured root
        // (the mount point: movie_dir / tv_dir / output_dir) ALREADY EXISTS,
        // is a directory, is absolute, and is writable. If the mount has
        // vanished, ERROR and PRESERVE the output in staging — never
        // `create_dir_all` a fresh tree (which would resolve into the
        // container's writable overlay and silently swallow an 80 GB rip).
        let dest_root = destination_root(cfg, &tmdb_result);
        if let Err(reason) = validate_destination_root(&dest_root) {
            record_error(
                &dir_str,
                &format!(
                    "destination not available — refusing to move (output preserved in staging): {reason}"
                ),
                "the destination directory/mount is missing or not writable. \
                 Check the configured movie/tv/output directory exists and its \
                 bind-mount (e.g. the NAS share) is present and writable. The \
                 mover will NOT auto-create the root — fix the mount, then it \
                 retries on the next tick.",
            );
            crate::log::syslog(&format!(
                "Move BLOCKED — destination root {} unavailable: {} (output preserved in staging: {})",
                absolute_for_log(&dest_root),
                reason,
                dir.display()
            ));
            continue;
        }

        // Create destination directories. The ROOT is confirmed present +
        // writable above, so this only materializes the per-title subdir
        // UNDER that real root — never the root (and thus mount) itself.
        let mut dest_ok = true;
        for (_, dest) in &planned_moves {
            if let Some(parent) = Path::new(dest).parent() {
                if std::fs::create_dir_all(parent).is_err() {
                    record_error(
                        &dir_str,
                        &format!(
                            "cannot create destination directory {}",
                            absolute_for_log(&parent.to_string_lossy())
                        ),
                        "check write permissions on the output / movie / tv directory",
                    );
                    dest_ok = false;
                }
            }
        }
        if !dest_ok {
            continue;
        }

        // Move files
        let mut outcomes: Vec<MoveOutcome> = Vec::new();
        let mut announced_moving = false;
        for (src, dest) in &planned_moves {
            let name_for_progress = display_name.clone();
            let on_progress = move |pct: u8, gb: f64, total_gb: f64, speed: f64| {
                let eta = if speed > 1.0 && total_gb > gb {
                    let secs = ((total_gb - gb) * 1024.0 / speed) as u32;
                    let m = secs / 60;
                    let s = secs % 60;
                    format!("{}:{:02}", m, s)
                } else {
                    String::new()
                };
                if let Ok(mut ms) = MOVE_STATE.lock() {
                    *ms = Some(MoveState {
                        name: name_for_progress.clone(),
                        progress_pct: pct,
                        progress_gb: gb,
                        total_gb,
                        speed_mbs: speed,
                        eta,
                    });
                }
            };
            // Overwrite guard: never clobber an existing destination that is
            // a DIFFERENT file. A wrong TMDB match can route two discs to the
            // same `Title (Year)/Title (Year).ext` name; overwriting would
            // destroy a good prior rip.
            //
            // A DIFFERENT-size dest is unambiguously a collision. A SAME-size
            // dest is the tricky case: it is normally the idempotent re-move
            // (this rip's output was copied on a prior tick whose unlink
            // failed — move_file returns Skipped/Moved and staging cleans up).
            // But two DIFFERENT discs can mux to the same byte length, in
            // which case a same-size dest is a real collision and the
            // size-only guard would wave it through to a Skipped, then
            // remove_dir_all would delete the NEW rip while the library keeps
            // the OLD wrong file. So when sizes are equal we content-probe
            // (head+tail) to confirm the dest really is this rip's output
            // before allowing the idempotent path. We must NOT just require
            // `d.len() > 0` here — that would flag every legitimate same-size
            // re-move as a permanent Collision and staging would never clean
            // up (regressing MovedDirty idempotency).
            //
            // Use fresh_metadata (fresh-FD stat) on BOTH sides, consistent
            // with the rest of mover.rs: a cache-served stat here can
            // mis-size the dest on NFS — either flagging a spurious
            // Collision (blocking a legitimate move) or missing a real
            // different-size dest, which move_file's same-size guard then
            // also misses, letting copy_counting truncate a good library
            // file. (Note: a regular file always reports is_file via the
            // fresh-FD stat; fresh_metadata returns Err for a non-file.)
            // Stat the destination first. A NotFound error means there is no
            // dest and the move is safe; ANY other stat error is transient
            // (NFS ESTALE/EIO, a dropped mount) and we must NOT fall through
            // to the destructive move_file — a transient stat error could
            // otherwise let a real collision slip past this guard and have
            // copy_counting truncate a good library file. Defer this entry to
            // a later tick instead.
            let dest_meta = match fresh_metadata(Path::new(dest)) {
                Ok(d) => Some(d),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => {
                    crate::log::syslog(&format!(
                        "Move deferred (could not stat destination {}): {} — will retry next tick",
                        dest, e
                    ));
                    outcomes.push(MoveOutcome::Failed);
                    continue;
                }
            };
            if let Some(d) = dest_meta {
                // Dest exists. We need a fresh stat of the source too; a
                // transient src-stat error here is likewise conservative —
                // defer rather than risk clobbering an existing dest.
                let s = match fresh_metadata(src) {
                    Ok(s) => s,
                    Err(e) => {
                        crate::log::syslog(&format!(
                            "Move deferred (destination {} exists but could not stat source {:?}): {} — will retry next tick",
                            dest, src, e
                        ));
                        outcomes.push(MoveOutcome::Failed);
                        continue;
                    }
                };
                if s.is_file() && d.is_file() && d.len() > 0 {
                    let collision = if s.len() != d.len() {
                        true
                    } else {
                        // Equal sizes: only a confirmed content match is the
                        // idempotent re-move. Anything else is a collision.
                        !same_head_and_tail(src, Path::new(dest))
                    };
                    if collision {
                        crate::log::syslog(&format!(
                            "Move blocked (destination exists, different file): {} ({} B) vs existing {} ({} B)",
                            src.display(),
                            s.len(),
                            dest,
                            d.len()
                        ));
                        outcomes.push(MoveOutcome::Collision);
                        continue;
                    }
                }
            }
            let outcome = move_file(src, Path::new(dest), &on_progress);
            outcomes.push(outcome);
            match outcome {
                MoveOutcome::Collision => {}
                MoveOutcome::Skipped => {
                    // Quiet — already moved on a prior tick; no log noise.
                }
                MoveOutcome::Moved => {
                    if !announced_moving {
                        crate::log::syslog(&format!(
                            "Moving: {} ({} files)",
                            display_name,
                            ripped_files.len()
                        ));
                        announced_moving = true;
                    }
                    // Log the FULL ABSOLUTE destination (filename → path) so
                    // the operator can always see exactly where the bytes
                    // landed — never a cwd-relative "movies/Mercy/..." that
                    // could hide a wrong-filesystem write (Mercy incident).
                    crate::log::syslog(&format!(
                        "Moved {} → {}",
                        src.file_name().unwrap_or_default().to_string_lossy(),
                        absolute_for_log(dest)
                    ));
                }
                MoveOutcome::MovedDirty => {
                    if !announced_moving {
                        crate::log::syslog(&format!(
                            "Moving: {} ({} files)",
                            display_name,
                            ripped_files.len()
                        ));
                        announced_moving = true;
                    }
                    crate::log::syslog(&format!(
                        "Moved {} → {} but source could not be removed",
                        src.file_name().unwrap_or_default().to_string_lossy(),
                        absolute_for_log(dest)
                    ));
                }
                MoveOutcome::Failed => {
                    crate::log::syslog(&format!(
                        "Failed to move {} → {}",
                        src.display(),
                        absolute_for_log(dest)
                    ));
                }
                MoveOutcome::SizeMismatch => {
                    crate::log::syslog(&format!(
                        "Move blocked (post-cp size mismatch): {:?} -> {}",
                        src, dest
                    ));
                }
                MoveOutcome::PostCopyInvalid => {
                    crate::log::syslog(&format!(
                        "Move blocked (post-cp validation failed — structural/unreadable): {:?} -> {}",
                        src, dest
                    ));
                }
            }
        }

        let any_collision = outcomes.iter().any(|o| matches!(o, MoveOutcome::Collision));
        let any_failed = outcomes.iter().any(|o| matches!(o, MoveOutcome::Failed));
        let any_size_mismatch = outcomes
            .iter()
            .any(|o| matches!(o, MoveOutcome::SizeMismatch));
        let any_post_copy_invalid = outcomes
            .iter()
            .any(|o| matches!(o, MoveOutcome::PostCopyInvalid));
        let any_dirty = outcomes
            .iter()
            .any(|o| matches!(o, MoveOutcome::MovedDirty));
        let any_actively_moved = outcomes
            .iter()
            .any(|o| matches!(o, MoveOutcome::Moved | MoveOutcome::MovedDirty));

        // 0.20.8 validation-audit fix #3: surface size-mismatch through
        // the same `record_error` UI path the other failures use, but
        // with a distinct reason so the operator knows the destination
        // is the short / broken side (src is intact, dst should be
        // discarded). Checked BEFORE `any_failed` so a mixed batch
        // (one Failed, one SizeMismatch) still surfaces the more
        // diagnostic message — both lead to "leave dir alone, retry
        // next tick", so ordering only affects the surfaced reason.
        if any_collision {
            record_error(
                &dir_str,
                "destination already exists as a different file — not overwriting",
                "likely a wrong title match (two discs resolving to the same name). Verify/rename the existing library file, or correct the title, then re-run; the new rip is preserved in staging.",
            );
            continue;
        }

        if any_size_mismatch {
            record_error(
                &dir_str,
                "post-cp validation failed: destination size does not match source",
                "check the destination filesystem for ENOSPC / short writes; remove the partial dst file and the mover will retry",
            );
            continue;
        }

        if any_post_copy_invalid {
            record_error(
                &dir_str,
                "post-cp validation failed: destination is structurally invalid or unreadable",
                "the copy is the correct size but failed a format/readability check (truncated header/tail, bad TS sync, or unreadable dst); remove the dst file and the mover will retry — see device_system.log for the specific check",
            );
            continue;
        }

        if any_failed {
            // Leave the dir alone; mover will retry next tick.
            // Surface a summary error so the UI shows what's failing.
            record_error(
                &dir_str,
                "copy to destination failed",
                "see device_system.log for the underlying error",
            );
            continue;
        }

        // All files are accounted for (Skipped / Moved / MovedDirty). Try to
        // tear down the staging dir; if it can't be removed (typically
        // because the orphan source files can't be unlinked), surface the
        // dir on the UI with a remediation hint.
        let cleanup_err = std::fs::remove_dir_all(&dir).err();

        if cleanup_err.is_none() {
            clear_error(&dir_str);
            crate::log::syslog(&format!("Move complete: {}", display_name));
        } else if any_dirty {
            record_error(
                &dir_str,
                "destination has the file but source could not be removed",
                "manually `rm -rf` the staging dir from a host that can write to the staging share, or fix the NFS export so the container can unlink files there",
            );
        } else if let Some(e) = cleanup_err {
            record_error(
                &dir_str,
                &format!("staging cleanup failed: {}", e),
                "manually `rm -rf` the staging dir",
            );
        }

        // Webhook: only fire on cycles where we actually moved bits.
        // Skipped-only ticks are no-ops and must not re-notify.
        if any_actively_moved {
            let dest_path = planned_moves.last().map(|(_, d)| d.as_str()).unwrap_or("");
            crate::webhook::send_move(cfg, &display_name, dest_path);
        }

        // Clear move state
        if let Ok(mut ms) = MOVE_STATE.lock() {
            *ms = None;
        }
    }
}

fn build_destination(cfg: &Config, tmdb: &Option<tmdb::TmdbResult>, filename: &str) -> String {
    // Source extension wins. Pre-0.25.7 this hardcoded ".mkv" for the
    // movie branch, which collided when keep_iso=true left both the
    // mux output and the source ISO in staging — both planned to the
    // same `Title.mkv` destination path. Successive mover ticks then
    // alternated overwriting one with the other, ultimately destroying
    // the MKV (observed 2026-05-20). Preserving the source
    // extension routes companions to distinct paths
    // (`Title.mkv`, `Title.iso`) and lets the format-aware post-cp
    // check correctly validate each.
    let src_ext = Path::new(filename)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("mkv");
    if let Some(result) = tmdb {
        let safe_title = crate::util::sanitize_path_display(&result.title);
        match result.media_type.as_str() {
            "movie" if !cfg.movie_dir.is_empty() => {
                let year_str = if result.year > 0 {
                    format!(" ({})", result.year)
                } else {
                    String::new()
                };
                // The movie root is `movie_dir` resolved UNDER `output_dir`
                // (see `resolve_media_root`): a RELATIVE `movie_dir`
                // ("movies") is joined onto output_dir → /mnt/.../movies; an
                // ABSOLUTE `movie_dir` wins via Path::join (back-compat).
                // Pre-fix this used `cfg.movie_dir` standalone, so a relative
                // "movies" resolved against the container root `/` → `/movies`
                // (the overlay), the 2026-06 "Mercy" incident.
                let root = resolve_media_root(&cfg.output_dir, &cfg.movie_dir);
                let dir = format!("{root}/{safe_title}{year_str}");
                // Filename carries the year too, matching the folder and the
                // Plex/Jellyfin `Title (Year)/Title (Year).ext` convention
                // (pre-fix the file was bare `Title.ext` — folder had the year
                // but the file did not).
                let name = format!("{safe_title}{year_str}.{src_ext}");
                format!("{dir}/{name}")
            }
            "tv" if !cfg.tv_dir.is_empty() => {
                // Same join fix as the movie branch: `tv_dir` resolved under
                // `output_dir` (relative joins, absolute wins).
                let root = resolve_media_root(&cfg.output_dir, &cfg.tv_dir);
                let dir = format!("{root}/{safe_title}/Season 1");
                // Sanitize the leaf too — the movie branch already derives
                // its leaf from a sanitized title, but this branch used the
                // RAW source filename, so a filename carrying a path
                // separator or traversal sequence could escape tv_dir.
                // sanitize_path_display drops '/' and '\' (keeps '.' and '_'
                // so the extension and episode tags survive).
                let safe_filename = crate::util::sanitize_path_display(filename);
                format!("{}/{}", dir, safe_filename)
            }
            _ => {
                // Sanitize the leaf for consistency with the movie/tv
                // branches (they sanitize; this fallback used the raw leaf,
                // so e.g. "..mkv" would reach output_dir verbatim).
                format!(
                    "{}/{}",
                    cfg.output_dir,
                    crate::util::sanitize_path_display(filename)
                )
            }
        }
    } else {
        format!(
            "{}/{}",
            cfg.output_dir,
            crate::util::sanitize_path_display(filename)
        )
    }
}

/// Resolve a media subdirectory (`movie_dir` / `tv_dir`) UNDER the base
/// `output_dir` using `Path::join` semantics:
///   - a RELATIVE `sub` ("movies") is joined onto `output_dir`
///     ("/mnt/unraid-1/media/" + "movies" → "/mnt/unraid-1/media/movies").
///   - an ABSOLUTE `sub` ("/srv/movies") REPLACES `output_dir` entirely
///     (Path::join semantics) — preserving back-compat for operators who
///     configured an absolute movie/tv dir.
///   - an EMPTY `sub` yields `output_dir` unchanged (the empty-dir
///     fall-through; callers only reach here with a non-empty `sub`).
///
/// This is the core of the 2026-06 "Mercy" fix: previously the movie/tv
/// branches used `cfg.movie_dir` / `cfg.tv_dir` STANDALONE, so a relative
/// "movies" resolved against the container root `/` → `/movies` (the
/// ephemeral overlay) instead of under the NFS mount at output_dir.
///
/// Returns a clean, slash-joined string (no trailing slash from a
/// trailing-slash `output_dir`, since Path::join normalizes that).
fn resolve_media_root(output_dir: &str, sub: &str) -> String {
    if sub.is_empty() {
        return output_dir.to_string();
    }
    Path::new(output_dir)
        .join(sub)
        .to_string_lossy()
        .into_owned()
}

/// The configured destination ROOT directory that governs a planned move,
/// mirroring `build_destination`'s root selection exactly. This is the
/// resolved mount-relative root (e.g. `/mnt/unraid-1/media/movies`), NOT
/// the per-title subdirectory under it. Returned so the mover can validate
/// the root is present + writable BEFORE creating any subdir tree under it
/// — the guard against silently writing an 80 GB rip into a container
/// overlay when the NAS bind-mount has vanished.
///
/// Selection must stay in lock-step with `build_destination`:
///   - movie with a non-empty `movie_dir` → `output_dir` ⨝ `movie_dir`
///   - tv with a non-empty `tv_dir` → `output_dir` ⨝ `tv_dir`
///   - everything else (no tmdb, empty movie/tv dir) → `output_dir`
///
/// (The join is the "Mercy" fix — see `resolve_media_root`. Returns an
/// owned String now that the root is computed, not a borrowed cfg field.)
fn destination_root(cfg: &Config, tmdb: &Option<tmdb::TmdbResult>) -> String {
    if let Some(result) = tmdb {
        match result.media_type.as_str() {
            "movie" if !cfg.movie_dir.is_empty() => {
                return resolve_media_root(&cfg.output_dir, &cfg.movie_dir);
            }
            "tv" if !cfg.tv_dir.is_empty() => {
                return resolve_media_root(&cfg.output_dir, &cfg.tv_dir);
            }
            _ => {}
        }
    }
    cfg.output_dir.clone()
}

/// Fail-loud destination-root validation: the configured root must ALREADY
/// EXIST as a directory AND be writable. Returns `Err(reason)` otherwise —
/// the caller then preserves the output in staging and surfaces the error
/// rather than `create_dir_all`-ing a fresh (wrong) tree.
///
/// This is the code hardening for the 2026-06 "Mercy" incident: the
/// docker-compose lost its `/mnt/unraid-1/media/movies` bind-mount, so
/// `/movies` resolved to the container's writable overlay. The mover
/// silently `create_dir_all`'d `/movies/Mercy (2024)/` there and wrote
/// ~80 GB into the ephemeral layer, logging a relative-path "success".
/// Requiring the root to pre-exist makes a missing mount a hard, loud
/// error — a mount point is provisioned out-of-band (the bind target),
/// never auto-created by the mover.
///
/// Crucially this does NOT create the root (that's the whole point); it
/// only probes. The per-title subdir under a confirmed-present root is
/// still created on demand by the caller.
fn validate_destination_root(root: &str) -> Result<(), String> {
    if root.is_empty() {
        // An empty root means "no configured dir"; `build_destination`
        // only routes here via `output_dir`, which defaults to a non-empty
        // path. An empty string would `create_dir_all("")` → cwd-relative
        // writes, exactly the silent-wrong-path failure we're closing.
        return Err("destination root is empty (no output/movie/tv directory configured)".into());
    }
    let root_path = Path::new(root);
    // 1. The root must be ABSOLUTE. A relative root (e.g. `movies`) resolves
    //    against the process cwd — which is how the incident produced the
    //    relative "Moved to movies/Mercy/..." log and wrote inside the
    //    container. A destination mount is always an absolute path.
    if !root_path.is_absolute() {
        return Err(format!(
            "destination root '{root}' is not an absolute path; \
             a destination mount must be configured as an absolute path \
             (e.g. /mnt/unraid-1/media/movies) so it can never resolve \
             relative to the container's working directory"
        ));
    }
    // 2. The root must already EXIST as a directory. If it doesn't, the
    //    mount is absent — do NOT create it (that writes into the overlay).
    match std::fs::metadata(root_path) {
        Ok(m) if m.is_dir() => {}
        Ok(_) => {
            return Err(format!(
                "destination root '{root}' exists but is not a directory"
            ));
        }
        Err(e) => {
            return Err(format!(
                "destination root '{root}' does not exist (mount missing?): {e}"
            ));
        }
    }
    // 3. The root must be WRITABLE. Probe by creating + removing a unique
    //    temp marker inside it (honest test of dir write/exec perms, RO
    //    filesystem, NFS squash). Unique-named so concurrent ticks / a real
    //    `<root>/.autorip-writable-probe` can't collide.
    let probe = root_path.join(format!(
        ".autorip-writable-probe-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
    {
        Ok(_) => {
            let _ = std::fs::remove_file(&probe);
            Ok(())
        }
        Err(e) => Err(format!("destination root '{root}' is not writable: {e}")),
    }
}

/// Fail-loud-EARLY destination check, mirroring the CLI's `preflight_validate`
/// intent: validate every configured destination root (movie / tv / output)
/// that is non-empty, returning a `(root, reason)` for each that is missing,
/// not a directory, relative, or not writable. Empty (unset/inherit) roots
/// are skipped. Used at startup and after a settings save to warn the
/// operator BEFORE a rip finishes and the per-move guard blocks it.
///
/// Non-fatal by design: a mount can be transiently down at boot / save time,
/// and the per-move `validate_destination_root` gate is the hard stop that
/// preserves output in staging. This just surfaces the problem early.
pub(crate) fn check_configured_destinations(cfg: &Config) -> Vec<(String, String)> {
    let mut problems = Vec::new();
    // Validate the RESOLVED roots — the same `output_dir`-joined paths the
    // move actually uses (`resolve_media_root`), not the raw relative
    // `movie_dir` / `tv_dir`. Without this the early check would flag a
    // perfectly-valid relative "movies" as "not absolute" even though it
    // resolves to a present /mnt/.../movies (and conversely would miss a
    // join that lands somewhere missing).
    let movie_root = if cfg.movie_dir.is_empty() {
        None
    } else {
        Some(resolve_media_root(&cfg.output_dir, &cfg.movie_dir))
    };
    let tv_root = if cfg.tv_dir.is_empty() {
        None
    } else {
        Some(resolve_media_root(&cfg.output_dir, &cfg.tv_dir))
    };
    // Deduplicate identical resolved roots (movie_dir resolving to the same
    // path as output_dir is common) so the operator doesn't see the same
    // warning twice.
    let mut seen: Vec<String> = Vec::new();
    for root in [movie_root, tv_root, Some(cfg.output_dir.clone())]
        .into_iter()
        .flatten()
    {
        if root.is_empty() || seen.contains(&root) {
            continue;
        }
        if let Err(reason) = validate_destination_root(&root) {
            problems.push((root.clone(), reason));
        }
        seen.push(root);
    }
    problems
}

/// Render a destination path as an ABSOLUTE path for logging. The mover
/// must always log where it wrote in unambiguous absolute terms (never a
/// cwd-relative `movies/Mercy/...` that hides a wrong-filesystem write).
/// Already-absolute paths pass through unchanged; a relative path is
/// joined onto the process cwd so the log still names a real location.
fn absolute_for_log(dest: &str) -> String {
    let p = Path::new(dest);
    if p.is_absolute() {
        return dest.to_string();
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(p).to_string_lossy().to_string(),
        Err(_) => dest.to_string(),
    }
}

/// Move a file with idempotent retry semantics.
///
/// 1. **Pre-flight**: if `dest` is a regular file with the same size as
///    `src`, treat the move as already done (`Skipped`). This is the
///    circuit breaker for the "cp succeeded but unlink failed" loop —
///    on the next tick we re-detect the completed dest and don't
///    re-copy 50+ GB across the network.
///
/// 2. **Atomic path**: try `rename(2)`. On the same filesystem this is
///    instant and unlinks src for free.
///
/// 3. **Cross-fs / fallback**: `std::fs::copy` on a worker thread
///    (v0.25.7 — pre-0.25.7 this shelled out to `cp -f --`), then try
///    to unlink src. If unlink fails (typical NFS squash-perm scenario
///    where the staging dir is owned by an identity the container
///    can't write into), return `MovedDirty` so the caller can
///    surface the orphan to the UI.
///
/// Worker thread + polling loop here prevents NFS/CIFS stalls from
/// blocking the main autorip thread. Calls `on_progress(pct, gb_done,
/// gb_total, speed_mbs)` every 1 s while the copy is running.
fn move_file(src: &Path, dest: &Path, on_progress: &dyn Fn(u8, f64, f64, f64)) -> MoveOutcome {
    // Fresh-FD stat on both sides (consistent with the rest of mover.rs):
    // a cache-served stat on NFS can mis-size either side, spuriously
    // tripping the matching-content Skipped pre-flight or the src-missing
    // Moved pre-flight below.
    let src_meta = fresh_metadata(src);
    let dest_meta = fresh_metadata(dest);

    // Pre-flight: dest already has matching content. Stops the infinite
    // re-copy loop when src can't be unlinked.
    //
    // Defensive content probe: the move-loop caller already gates this with
    // `same_head_and_tail` before calling us, but equal LENGTH alone does
    // not prove equal CONTENT — a wrong title match can route two distinct
    // discs to the same path with byte-identical mux lengths. If `move_file`
    // is ever called WITHOUT the caller guard (a future refactor, a new
    // call site), trusting size-only here would silently keep the wrong file
    // as "already moved". Re-confirm head+tail so the skip can never clobber
    // a different file; a mismatch surfaces as a Collision instead.
    if let (Ok(s), Ok(d)) = (&src_meta, &dest_meta) {
        if s.is_file() && d.is_file() && s.len() == d.len() && s.len() > 0 {
            if same_head_and_tail(src, dest) {
                // Equal length + matching head/tail still isn't proof of a
                // DURABLE dest: a prior copy that failed post-copy validation
                // (short/structurally-invalid on NFS) can leave a dest that
                // happens to match these cheap probes. Run the same fresh-FD
                // post-copy validation the copy path runs before accepting it
                // as already-moved; on failure, fall through to a real copy.
                if check_post_copy(src, dest).is_ok() {
                    return MoveOutcome::Skipped;
                }
                crate::log::syslog(&format!(
                    "Pre-existing destination failed post-copy validation; re-copying: {:?}",
                    dest
                ));
                // Fall through to the copy path below.
            } else {
                crate::log::syslog(&format!(
                    "Move blocked (destination same size but different content): {:?} vs {:?}",
                    src, dest
                ));
                return MoveOutcome::Collision;
            }
        }
    }
    // Pre-flight: src missing but dest present — earlier rename succeeded.
    if let (Err(_), Ok(d)) = (&src_meta, &dest_meta) {
        if d.is_file() && d.len() > 0 {
            return MoveOutcome::Moved;
        }
    }

    if std::fs::rename(src, dest).is_ok() {
        return MoveOutcome::Moved;
    }

    let dest_str = dest.to_string_lossy().to_string();
    let src_size = src_meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let total_gb = src_size as f64 / crate::util::BYTES_PER_GIB;

    // v0.25.7: replaced the `cp` subprocess with an in-process copy on a
    // worker thread, polled here for live progress. Drops the cp package
    // dependency (the image-slim work doesn't ship a busybox cp by
    // default). The copy itself is `copy_counting`, a plain buffered
    // read/write loop — not `std::fs::copy` — so we can count bytes as we
    // write them for progress; the kernel fast paths
    // (copy_file_range/sendfile) wouldn't apply here anyway since this
    // branch only runs for cross-filesystem moves (see `copy_counting`).
    // Behaviour unchanged: progress ticks every 1 s, post-copy validation
    // runs before unlink, src stays intact on any failure path.
    let src_owned = src.to_path_buf();
    let dest_owned = dest.to_path_buf();
    // Copy on a worker thread, counting bytes as we write them. Progress is
    // derived from THIS counter (see `copy_counting`), not from stat()-ing the
    // NFS destination — that stat was the pre-0.26.x bug that pinned the move
    // bar at 0 % for the whole copy. Reads come from local staging (fast); only
    // the writes touch NFS, and they stay on this worker thread so a write
    // stall can never block the poll loop below.
    let written = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let written_w = std::sync::Arc::clone(&written);
    let (tx, rx) = std::sync::mpsc::channel::<std::io::Result<u64>>();
    let copy_handle = std::thread::spawn(move || {
        let _ = tx.send(copy_counting(&src_owned, &dest_owned, &written_w));
    });

    let start = std::time::Instant::now();
    loop {
        match rx.try_recv() {
            Ok(Ok(_bytes)) => {
                let _ = copy_handle.join();
                on_progress(100, total_gb, total_gb, 0.0);
                // Post-copy validation. v0.25.3 made this format-aware
                // (EBML head+tail for mkv, TS-sync for m2ts, fresh-FD
                // stat for iso) so NFS attribute cache can't phantom-
                // fail it. Runs BEFORE the unlink so src bytes stay
                // intact on any mismatch — the operator can retry.
                if let Err(e) = check_post_copy(src, Path::new(&dest_str)) {
                    crate::log::syslog(&format!(
                        "Post-cp validation failed for {}: {}",
                        dest_str, e
                    ));
                    // Map the failure KIND to the outcome so the operator
                    // gets an accurate hint. Only a true length disagreement
                    // is SizeMismatch (ENOSPC / short-write hint); structural
                    // and readability failures get the generic PostCopyInvalid
                    // path instead of a misleading size hint.
                    return match e {
                        MoveError::SizeDoesNotMatch { .. } => MoveOutcome::SizeMismatch,
                        MoveError::MkvBadHead
                        | MoveError::MkvBadTail
                        | MoveError::M2tsBadSync
                        | MoveError::Unreadable(_) => MoveOutcome::PostCopyInvalid,
                    };
                }
                return match std::fs::remove_file(src) {
                    Ok(_) => MoveOutcome::Moved,
                    Err(_) => MoveOutcome::MovedDirty,
                };
            }
            Ok(Err(e)) => {
                let _ = copy_handle.join();
                // Remove the partial/truncated destination so the next tick
                // retries cleanly instead of seeing a phantom size-mismatch
                // Collision (which would wedge the move permanently). If the
                // removal ITSELF fails, the stuck partial silently wedges the
                // move — surface it so the operator can delete it by hand.
                if let Err(rm) = std::fs::remove_file(&dest_str) {
                    record_error(
                        &dest_str,
                        "partial copy could not be removed",
                        &format!(
                            "partial copy could not be removed from {dest_str}; delete manually to unblock ({rm})"
                        ),
                    );
                }
                crate::log::syslog(&format!("fs::copy failed for {}: {}", dest_str, e));
                return MoveOutcome::Failed;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                // Sender dropped without sending — worker panicked.
                let _ = std::fs::remove_file(&dest_str);
                crate::log::syslog(&format!("fs::copy thread panicked for {}", dest_str));
                return MoveOutcome::Failed;
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                // Honor SIGTERM mid-copy: the run() loop's SHUTDOWN-aware
                // sleep only gates BETWEEN ticks, so without this a multi-GB
                // cross-fs copy would run to completion ignoring the signal,
                // and docker stop's 10 s grace would SIGKILL mid-write. Join
                // the worker (bounded to its current chunk write) and bail.
                if crate::SHUTDOWN.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = copy_handle.join();
                    // Drop the partial destination (see Ok(Err) arm) so a
                    // restart's first tick doesn't wedge on a size-mismatch
                    // Collision against this interrupted copy.
                    let _ = std::fs::remove_file(&dest_str);
                    crate::log::syslog(&format!("Move aborted (shutdown) mid-copy: {}", dest_str));
                    return MoveOutcome::Failed;
                }
                // Progress straight from the bytes we've written — no NFS stat,
                // so it can't stall and can't read stale. `speed` is the simple
                // average so far (bytes/elapsed), surfaced in MB/s.
                let done = written.load(std::sync::atomic::Ordering::Relaxed);
                let elapsed = start.elapsed().as_secs_f64();
                let pct = if src_size > 0 {
                    (done.saturating_mul(100) / src_size).min(100) as u8
                } else {
                    0
                };
                let gb = done as f64 / crate::util::BYTES_PER_GIB;
                let speed_mbs = if elapsed > 0.0 {
                    (done as f64 / elapsed) / crate::util::BYTES_PER_MIB
                } else {
                    0.0
                };
                on_progress(pct, gb, total_gb, speed_mbs);
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    }
}

// `sanitize_dir_name` moved to `crate::util::sanitize_path_display` in 0.13.0.
// Single source of truth shared with the staging path in `ripper`.

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with_dirs(movie_dir: &str, tv_dir: &str, output_dir: &str) -> Config {
        Config {
            output_dir: output_dir.into(),
            movie_dir: movie_dir.into(),
            tv_dir: tv_dir.into(),
            ..Config::default()
        }
    }

    fn tmdb_movie(title: &str, year: u16) -> tmdb::TmdbResult {
        tmdb::TmdbResult {
            title: title.into(),
            year,
            poster_url: String::new(),
            overview: String::new(),
            media_type: "movie".into(),
        }
    }

    #[test]
    fn sanitize_dir_name_strips_unsafe_characters() {
        assert_eq!(
            crate::util::sanitize_path_display("Aurora: Drift Two"),
            "Aurora Drift Two"
        );
        assert_eq!(crate::util::sanitize_path_display("M*A*S*H"), "MASH");
        assert_eq!(
            crate::util::sanitize_path_display("Alien/Predator"),
            "AlienPredator"
        );
        assert_eq!(
            crate::util::sanitize_path_display("What's Up, Doc?"),
            "What's Up Doc"
        );
    }

    #[test]
    fn sanitize_dir_name_keeps_allowed_punctuation() {
        assert_eq!(
            crate::util::sanitize_path_display("Side Quest - A Long Journey"),
            "Side Quest - A Long Journey"
        );
        assert_eq!(
            crate::util::sanitize_path_display("Director_Cut.2019"),
            "Director_Cut.2019"
        );
    }

    #[test]
    fn sanitize_dir_name_trims_whitespace() {
        assert_eq!(
            crate::util::sanitize_path_display("  spaced title  "),
            "spaced title"
        );
    }

    #[test]
    fn build_destination_movie_with_year() {
        let cfg = cfg_with_dirs("/out/Movies", "/out/TV", "/out");
        let tmdb = Some(tmdb_movie("Aurora Drift Two", 2024));
        let dest = build_destination(&cfg, &tmdb, "disc.mkv");
        assert_eq!(
            dest,
            "/out/Movies/Aurora Drift Two (2024)/Aurora Drift Two (2024).mkv"
        );
    }

    // ===================================================================
    // Mercy incident ROOT CAUSE (2026-06): a RELATIVE `movie_dir` must be
    // joined UNDER `output_dir`, not used standalone. The rig's exact
    // config was output_dir="/mnt/unraid-1/media/", movie_dir="movies".
    // Pre-fix, build_destination used `cfg.movie_dir` ("movies") directly,
    // so the dest was "movies/Mercy (2024)/..." → resolved against the
    // container root `/` → "/movies/..." (the ephemeral overlay), NOT the
    // NFS mount. The fix joins movie_dir under output_dir.
    // ===================================================================
    #[test]
    fn build_destination_relative_movie_dir_joins_under_output_dir() {
        // The EXACT rig config that caused the Mercy incident.
        let cfg = cfg_with_dirs("movies", "", "/mnt/unraid-1/media/");
        let tmdb = Some(tmdb_movie("Mercy", 2023));
        let dest = build_destination(&cfg, &tmdb, "Mercy.mkv");
        assert_eq!(
            dest, "/mnt/unraid-1/media/movies/Mercy (2023)/Mercy (2023).mkv",
            "a relative movie_dir must resolve UNDER output_dir on the NFS mount"
        );
        // The regression we're guarding against: it must NOT be the bare
        // container-overlay path.
        assert!(
            !dest.starts_with("/movies/"),
            "movie_dir must never be used standalone (Mercy bug: wrote to /movies on the overlay)"
        );
        // And destination_root must agree — it's what validate_destination_root
        // checks, so a correct config validates the REAL mount root.
        assert_eq!(
            destination_root(&cfg, &tmdb),
            "/mnt/unraid-1/media/movies",
            "destination_root must be the joined mount-relative root, not bare 'movies'"
        );
    }

    /// A relative `tv_dir` is likewise joined under `output_dir` (same bug
    /// class as the movie branch).
    #[test]
    fn build_destination_relative_tv_dir_joins_under_output_dir() {
        let cfg = cfg_with_dirs("", "tv", "/mnt/unraid-1/media/");
        let tmdb = Some(tmdb::TmdbResult {
            title: "Severance".into(),
            year: 2022,
            poster_url: String::new(),
            overview: String::new(),
            media_type: "tv".into(),
        });
        let dest = build_destination(&cfg, &tmdb, "sev_s01e01.mkv");
        assert_eq!(
            dest,
            "/mnt/unraid-1/media/tv/Severance/Season 1/sev_s01e01.mkv"
        );
        assert!(!dest.starts_with("/tv/"));
    }

    /// Back-compat: an ABSOLUTE `movie_dir` still wins via Path::join
    /// semantics (replaces output_dir entirely) — operators who configured
    /// an absolute movie/tv dir keep their existing layout.
    #[test]
    fn build_destination_absolute_movie_dir_overrides_output_dir() {
        let cfg = cfg_with_dirs("/srv/library/movies", "", "/mnt/unraid-1/media/");
        let tmdb = Some(tmdb_movie("Mercy", 2023));
        let dest = build_destination(&cfg, &tmdb, "Mercy.mkv");
        assert_eq!(
            dest, "/srv/library/movies/Mercy (2023)/Mercy (2023).mkv",
            "an absolute movie_dir must override output_dir (Path::join semantics)"
        );
        assert_eq!(destination_root(&cfg, &tmdb), "/srv/library/movies");
    }

    /// `resolve_media_root` unit semantics: relative joins, absolute wins,
    /// trailing slashes normalize, empty sub → output_dir.
    #[test]
    fn resolve_media_root_semantics() {
        assert_eq!(
            resolve_media_root("/mnt/unraid-1/media/", "movies"),
            "/mnt/unraid-1/media/movies"
        );
        assert_eq!(
            resolve_media_root("/mnt/unraid-1/media", "movies"),
            "/mnt/unraid-1/media/movies"
        );
        assert_eq!(
            resolve_media_root("/mnt/media", "/srv/movies"),
            "/srv/movies"
        );
        assert_eq!(resolve_media_root("/mnt/media", ""), "/mnt/media");
    }

    #[test]
    fn build_destination_movie_without_year_falls_through() {
        let cfg = cfg_with_dirs("/out/Movies", "/out/TV", "/out");
        let tmdb = Some(tmdb_movie("Unknown Year", 0));
        let dest = build_destination(&cfg, &tmdb, "disc.mkv");
        // year=0 skips the "(YEAR)" suffix; mkv name derived from cleaned title.
        assert_eq!(dest, "/out/Movies/Unknown Year/Unknown Year.mkv");
    }

    #[test]
    fn build_destination_tv_uses_season_1_layout() {
        let cfg = cfg_with_dirs("/out/Movies", "/out/TV", "/out");
        let tmdb = Some(tmdb::TmdbResult {
            title: "Severance".into(),
            year: 2022,
            poster_url: String::new(),
            overview: String::new(),
            media_type: "tv".into(),
        });
        let dest = build_destination(&cfg, &tmdb, "sev_s01e01.mkv");
        assert_eq!(dest, "/out/TV/Severance/Season 1/sev_s01e01.mkv");
    }

    #[test]
    fn build_destination_no_tmdb_falls_to_output_dir() {
        let cfg = cfg_with_dirs("/out/Movies", "/out/TV", "/out");
        let dest = build_destination(&cfg, &None, "disc.mkv");
        assert_eq!(dest, "/out/disc.mkv");
    }

    #[test]
    fn build_destination_empty_movie_dir_falls_to_output_dir() {
        let cfg = cfg_with_dirs("", "/out/TV", "/out");
        let tmdb = Some(tmdb_movie("Movie", 2020));
        let dest = build_destination(&cfg, &tmdb, "disc.mkv");
        // movie_dir empty → fall-through to output_dir + filename.
        assert_eq!(dest, "/out/disc.mkv");
    }

    #[test]
    fn build_destination_movie_preserves_iso_extension() {
        // Bug fix: pre-0.25.7 a keep_iso=true rip left both .mkv and
        // .iso in staging; build_destination hardcoded ".mkv" so both
        // planned to the same path and the mover overwrote one with
        // the other in alternating ticks. Source extension must win.
        let cfg = cfg_with_dirs("/out/Movies", "/out/TV", "/out");
        let tmdb = Some(tmdb_movie("Lumina", 2023));
        let dest_iso = build_destination(&cfg, &tmdb, "Lumina.iso");
        let dest_mkv = build_destination(&cfg, &tmdb, "Lumina.mkv");
        assert_eq!(dest_iso, "/out/Movies/Lumina (2023)/Lumina (2023).iso");
        assert_eq!(dest_mkv, "/out/Movies/Lumina (2023)/Lumina (2023).mkv");
        assert_ne!(
            dest_iso, dest_mkv,
            "iso and mkv companions must not collide"
        );
    }

    #[test]
    fn build_destination_movie_preserves_m2ts_extension() {
        let cfg = cfg_with_dirs("/out/Movies", "/out/TV", "/out");
        let tmdb = Some(tmdb_movie("Movie", 2024));
        let dest = build_destination(&cfg, &tmdb, "00800.m2ts");
        assert_eq!(dest, "/out/Movies/Movie (2024)/Movie (2024).m2ts");
    }

    fn noop_progress(_: u8, _: f64, _: f64, _: f64) {}

    #[test]
    fn move_file_skips_when_dest_size_matches() {
        // Circuit breaker: a prior tick already cp'd the file but
        // couldn't unlink src. Re-detecting the same-size dest must NOT
        // recopy — that's the bug this fix exists for.
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("a.mkv");
        let dest = tmp.path().join("b.mkv");
        // Valid EBML-framed payload: Skipped now requires the pre-existing
        // dest to pass the same post-copy validation the copy path runs.
        write_minimal_mkv(&src, b"hello world");
        write_minimal_mkv(&dest, b"hello world");
        let outcome = move_file(&src, &dest, &noop_progress);
        assert_eq!(outcome, MoveOutcome::Skipped);
        assert!(src.exists(), "src must remain untouched on Skipped");
        assert!(dest.exists());
    }

    #[test]
    fn move_file_moves_when_dest_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("a.mkv");
        let dest = tmp.path().join("sub/b.mkv");
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        std::fs::write(&src, b"data data data").unwrap();
        let outcome = move_file(&src, &dest, &noop_progress);
        assert_eq!(outcome, MoveOutcome::Moved);
        assert!(!src.exists(), "rename consumes src");
        assert_eq!(std::fs::read(&dest).unwrap(), b"data data data");
    }

    #[test]
    fn move_file_overwrites_when_dest_size_differs() {
        // A partial dest from a previous failed cp must NOT cause a
        // permanent stall — the new full src should overwrite it.
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("a.mkv");
        let dest = tmp.path().join("b.mkv");
        std::fs::write(&src, b"new full content").unwrap();
        std::fs::write(&dest, b"partial").unwrap();
        let outcome = move_file(&src, &dest, &noop_progress);
        assert_eq!(outcome, MoveOutcome::Moved);
        assert_eq!(std::fs::read(&dest).unwrap(), b"new full content");
    }

    /// Partial-dest cleanup contract (already-landed fix). When the copy
    /// path fails, `move_file` must NOT leave a partial/garbage destination
    /// behind — otherwise the next mover tick sees a phantom size-mismatch
    /// Collision and wedges the move permanently.
    ///
    /// Forcing a *mid-copy* truncation deterministically needs fault
    /// injection (a writer that fails after N bytes), which isn't available
    /// here. Instead we force the copy branch (rename must fail first) and a
    /// copy failure, and assert the outcome is `Failed` with no leftover
    /// dest. The complementary "stale partial dest doesn't wedge the next
    /// tick" case is covered by `move_file_overwrites_when_dest_size_differs`
    /// above (a pre-existing partial is cleanly overwritten).
    #[test]
    fn move_file_copy_failure_leaves_no_partial_dest() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("a.mkv");
        std::fs::write(&src, b"source bytes").unwrap();

        // dest sits under a path whose "parent" is a regular FILE, not a
        // directory. Both rename(2) and File::create(dest) then fail with
        // ENOTDIR — exercising the copy branch's failure cleanup without a
        // cross-filesystem mount. No dest can ever be created here, so the
        // post-condition "no partial dest left" must hold.
        let not_a_dir = tmp.path().join("blocker");
        std::fs::write(&not_a_dir, b"x").unwrap();
        let dest = not_a_dir.join("b.mkv");

        let outcome = move_file(&src, &dest, &noop_progress);
        assert_eq!(outcome, MoveOutcome::Failed, "copy failure → Failed");
        assert!(!dest.exists(), "no partial destination may be left behind");
        // Source is the only copy and must be preserved on any failure.
        assert!(src.exists(), "source must survive a failed move");
    }

    #[test]
    fn move_file_does_not_skip_an_invalid_same_size_dest() {
        // Regression (finding 9): the Skipped pre-flight accepted a
        // pre-existing dest on equal length + matching head/tail WITHOUT the
        // post-copy validation the copy path runs. A dest left undurable by a
        // prior failed copy (here: structurally invalid — no EBML magic) must
        // NOT be treated as already-moved. With src present, rename now
        // overwrites it → Moved, not Skipped.
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("a.mkv");
        let dest = tmp.path().join("b.mkv");
        // Byte-identical, same length, matching head/tail — but no EBML magic,
        // so check_post_copy rejects it.
        let bytes = vec![0xAAu8; 4096];
        std::fs::write(&src, &bytes).unwrap();
        std::fs::write(&dest, &bytes).unwrap();
        let outcome = move_file(&src, &dest, &noop_progress);
        assert_ne!(
            outcome,
            MoveOutcome::Skipped,
            "an invalid same-size dest must not be accepted as already-moved"
        );
        assert_eq!(
            outcome,
            MoveOutcome::Moved,
            "rename overwrites the bad dest"
        );
    }

    #[test]
    fn move_file_returns_moved_when_src_missing_but_dest_present() {
        // Earlier atomic rename succeeded; src is gone, dest is fine.
        // Re-entering move_file (e.g. on next tick before staging
        // cleanup) must not error out.
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("a.mkv");
        let dest = tmp.path().join("b.mkv");
        std::fs::write(&dest, b"already there").unwrap();
        let outcome = move_file(&src, &dest, &noop_progress);
        assert_eq!(outcome, MoveOutcome::Moved);
    }

    #[test]
    fn move_file_collides_when_dest_same_size_different_content() {
        // Atomicity/safety contract: when the destination already holds a
        // DIFFERENT file of the SAME length (a wrong-title match routing two
        // discs to one path with byte-identical mux lengths), move_file must
        // NOT clobber it. It returns Collision and leaves BOTH files intact so
        // the operator can disambiguate — never silently overwriting the
        // existing title.
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("a.mkv");
        let dest = tmp.path().join("b.mkv");
        // Equal length, differing content: same 4-byte EBML magic, same total
        // size, but the payload differs (so head/tail probe mismatches).
        write_minimal_mkv(&src, b"AAAAAAAAAAAA");
        write_minimal_mkv(&dest, b"BBBBBBBBBBBB");
        assert_eq!(
            std::fs::metadata(&src).unwrap().len(),
            std::fs::metadata(&dest).unwrap().len(),
            "precondition: equal length"
        );

        let outcome = move_file(&src, &dest, &noop_progress);
        assert_eq!(
            outcome,
            MoveOutcome::Collision,
            "same-size different-content dest must collide, not overwrite"
        );
        // Both originals survive untouched.
        assert_eq!(std::fs::read(&src).unwrap(), {
            let mut b = vec![0x1A, 0x45, 0xDF, 0xA3];
            b.extend_from_slice(b"AAAAAAAAAAAA");
            b
        });
        assert_eq!(std::fs::read(&dest).unwrap(), {
            let mut b = vec![0x1A, 0x45, 0xDF, 0xA3];
            b.extend_from_slice(b"BBBBBBBBBBBB");
            b
        });
    }

    /// Cross-device (EXDEV) copy+unlink SUCCESS path, driven end-to-end
    /// through `move_file` against a SEPARATE real filesystem.
    ///
    /// `move_file`'s fast path is `fs::rename`; the copy+validate+unlink
    /// fallback only runs when rename fails with EXDEV (src and dest on
    /// different filesystems). Within one tempdir rename always succeeds, so
    /// this branch is unreachable without a second mount. On Linux CI we can
    /// often create one without root via a user-namespace `tmpfs`/`tmpdir` on
    /// a distinct device — but that isn't guaranteed. So this test probes for
    /// two distinct filesystems among well-known mount points and SKIPS
    /// (documenting the gap) when it can't find them, rather than faking the
    /// EXDEV condition.
    ///
    /// KNOWN GAP: when no second filesystem is available (typical dev laptop /
    /// sandboxed CI), the `move_file` copy-success → post-copy validate →
    /// `remove_file(src)` → `Moved`, and the `MovedDirty` (copy ok, unlink
    /// fails) branch, are NOT exercised end-to-end. The constituent pieces ARE
    /// covered: `copy_counting` success/atomicity (its own tests),
    /// `check_post_copy_*` validation (its own tests), and the copy-FAILURE
    /// cleanup path (`move_file_copy_failure_leaves_no_partial_dest`). Closing
    /// the gap fully needs a real EXDEV mount or an injectable rename seam.
    #[test]
    fn move_file_cross_device_copy_unlink_success_when_two_filesystems_exist() {
        // Find a tempdir on a filesystem different from std::env::temp_dir().
        let primary = tempfile::tempdir().unwrap();
        let primary_dev = std::fs::metadata(primary.path())
            .ok()
            .and_then(dev_id_of)
            .expect("stat primary tempdir");

        // Candidate roots that are commonly a distinct filesystem.
        let candidates = ["/dev/shm", "/run/user", "/tmp", "/var/tmp"];
        let secondary_root = candidates.iter().find_map(|root| {
            let p = std::path::Path::new(root);
            if !p.is_dir() {
                return None;
            }
            let dev = std::fs::metadata(p).ok().and_then(dev_id_of)?;
            if dev != primary_dev { Some(p) } else { None }
        });

        let Some(secondary_root) = secondary_root else {
            eprintln!(
                "SKIP move_file_cross_device_copy_unlink_success: no second \
                 filesystem available — EXDEV copy path not exercised (see test doc)"
            );
            return;
        };

        // Source on the secondary fs, dest on the primary fs → rename across
        // them returns EXDEV, forcing the copy+unlink fallback.
        let work = secondary_root.join(format!("autorip-xdev-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&work);
        let src = work.join("a.mkv");
        let dest = primary.path().join("a.mkv");
        write_minimal_mkv(&src, b"cross device payload bytes");

        let outcome = move_file(&src, &dest, &noop_progress);

        // Best-effort cleanup of the secondary-fs scratch dir.
        let _ = std::fs::remove_dir_all(&work);

        // If for some reason rename still succeeded (same fs after all), we'd
        // get Moved too — both acceptable; the key assertions are that the
        // dest holds the bytes and the src was unlinked.
        assert_eq!(
            outcome,
            MoveOutcome::Moved,
            "cross-device move must succeed"
        );
        assert!(
            !src.exists(),
            "src must be unlinked after a successful copy"
        );
        let moved = std::fs::read(&dest).unwrap();
        assert_eq!(
            &moved[..4],
            &[0x1A, 0x45, 0xDF, 0xA3],
            "dest is the moved MKV"
        );
    }

    fn dev_id_of(m: std::fs::Metadata) -> Option<u64> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Some(m.dev())
        }
        #[cfg(not(unix))]
        {
            let _ = m;
            None
        }
    }

    // Helpers for the structural checks. Real MKVs are EBML-framed
    // with the magic [1A 45 DF A3] at offset 0. Real BD-TS .m2ts uses
    // 192-byte packets with TS sync 0x47 at offset 4 within each
    // packet.

    fn write_minimal_mkv(path: &std::path::Path, payload: &[u8]) {
        let mut bytes = vec![0x1A, 0x45, 0xDF, 0xA3];
        bytes.extend_from_slice(payload);
        std::fs::write(path, bytes).unwrap();
    }

    fn write_minimal_m2ts(path: &std::path::Path, packets: u64) {
        let mut bytes = Vec::with_capacity((packets * 192) as usize);
        for _ in 0..packets {
            // 4-byte arrival-time prefix, then 0x47 sync, then 187 bytes.
            bytes.extend_from_slice(&[0, 0, 0, 0]);
            bytes.push(0x47);
            bytes.extend_from_slice(&[0u8; 187]);
        }
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn check_post_copy_size_passes_on_equal_sizes() {
        // Non-mkv/m2ts path: routes to fresh-FD size compare.
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("a.iso");
        let dst = tmp.path().join("b.iso");
        std::fs::write(&src, b"identical bytes here").unwrap();
        std::fs::write(&dst, b"identical bytes here").unwrap();
        assert!(check_post_copy(&src, &dst).is_ok());
    }

    #[test]
    fn check_post_copy_size_catches_short_dst_for_iso() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src.iso");
        let dst = tmp.path().join("dst.iso");
        std::fs::write(&src, vec![0u8; 4096]).unwrap();
        std::fs::write(&dst, vec![0u8; 1024]).unwrap();
        let err = check_post_copy(&src, &dst).unwrap_err();
        assert!(matches!(err, MoveError::SizeDoesNotMatch { .. }));
    }

    #[test]
    fn check_post_copy_size_catches_missing_dst() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src.iso");
        let dst = tmp.path().join("never_created.iso");
        std::fs::write(&src, b"some bytes").unwrap();
        // A missing destination must surface as an error — never as a
        // silent pass. Previously fresh_metadata's Err defaulted to 0
        // for both sides (0 == 0) and validated the missing dst, which
        // would let move_file unlink the source ISO and lose the bytes.
        let err = check_post_copy(&src, &dst).unwrap_err();
        assert!(matches!(err, MoveError::Unreadable(_)), "got {:?}", err);
    }

    #[test]
    fn check_post_copy_mkv_passes_on_valid_ebml_head_and_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("good.mkv");
        // Body is at least 5 bytes so the tail check has bytes to read.
        write_minimal_mkv(&dst, &vec![0xAA; 256]);
        // src must match dst size: check_post_copy now pairs the
        // structural check with a src-vs-dst size cross-check.
        let src = tmp.path().join("src.mkv");
        write_minimal_mkv(&src, &vec![0xAA; 256]);
        assert!(check_post_copy(&src, &dst).is_ok());
    }

    #[test]
    fn check_post_copy_mkv_rejects_bad_head() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("bad_head.mkv");
        // First 4 bytes are NOT EBML magic.
        std::fs::write(&dst, b"NOPE bytes after").unwrap();
        let src = tmp.path().join("src.mkv");
        std::fs::write(&src, b"any").unwrap();
        let err = check_post_copy(&src, &dst).unwrap_err();
        assert!(matches!(err, MoveError::MkvBadHead));
    }

    #[test]
    fn check_post_copy_mkv_rejects_truncated_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("trunc.mkv");
        // Only 4 bytes total — head OK, but tail check requires >= 5.
        std::fs::write(&dst, [0x1A, 0x45, 0xDF, 0xA3]).unwrap();
        let src = tmp.path().join("src.mkv");
        std::fs::write(&src, b"any").unwrap();
        let err = check_post_copy(&src, &dst).unwrap_err();
        assert!(matches!(err, MoveError::MkvBadTail));
    }

    #[test]
    fn check_post_copy_m2ts_passes_on_aligned_sync_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("good.m2ts");
        write_minimal_m2ts(&dst, 32); // > 16 packets, plenty for head+tail
        // src must match dst size for the size cross-check.
        let src = tmp.path().join("src.m2ts");
        write_minimal_m2ts(&src, 32);
        assert!(check_post_copy(&src, &dst).is_ok());
    }

    #[test]
    fn check_post_copy_m2ts_rejects_garbage() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("bad.m2ts");
        // Garbage 16 * 192 bytes — no 0x47 at the sync offsets.
        std::fs::write(&dst, vec![0xFE; 16 * 192]).unwrap();
        let src = tmp.path().join("src.m2ts");
        std::fs::write(&src, b"any").unwrap();
        let err = check_post_copy(&src, &dst).unwrap_err();
        assert!(matches!(err, MoveError::M2tsBadSync));
    }

    #[test]
    fn check_post_copy_m2ts_rejects_short_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("short.m2ts");
        std::fs::write(&dst, [0u8; 100]).unwrap(); // smaller than 8 * 192
        let src = tmp.path().join("src.m2ts");
        std::fs::write(&src, b"any").unwrap();
        let err = check_post_copy(&src, &dst).unwrap_err();
        assert!(matches!(err, MoveError::M2tsBadSync));
    }

    #[test]
    fn record_error_dedups_same_reason_without_logging_again() {
        // Same path + same reason twice → second insert is a no-op
        // logger-wise (the syslog call is gated on reason change).
        // We assert state by checking the map snapshot.
        let path = "/tmp/fakemover-dedup-test";
        record_error(path, "stuck", "do thing");
        record_error(path, "stuck", "do thing");
        let m = MOVE_ERRORS.lock().unwrap();
        let entry = m.get(path).expect("error recorded");
        assert_eq!(entry.reason, "stuck");
        drop(m);
        clear_error(path);
        assert!(MOVE_ERRORS.lock().unwrap().get(path).is_none());
    }

    // 0.25.10 fixes regression tests.

    fn marker_json(title: &str) -> String {
        serde_json::json!({
            "title": title,
            "disc_name": title,
            "format": "BD",
            "year": 2024,
            "media_type": "movie",
            "poster_url": "",
            "overview": "",
            "date": "2026-05-20",
        })
        .to_string()
    }

    fn cfg_for_staging(staging: &std::path::Path, movie_dir: &str, keep_iso: bool) -> Config {
        Config {
            staging_dir: staging.to_string_lossy().to_string(),
            output_dir: staging
                .parent()
                .unwrap()
                .join("output")
                .to_string_lossy()
                .to_string(),
            movie_dir: movie_dir.to_string(),
            tv_dir: String::new(),
            keep_iso,
            ..Config::default()
        }
    }

    #[test]
    fn check_and_move_skips_iso_when_keep_iso_false() {
        // Regression for 0.25.10: pre-fix, the mover blindly moved ANY
        // .iso it found in a .done staging dir. Result: a 90+ GB
        // intermediate ISO landed in the user's movie library
        // (observed 2026-05-20, an ISO promoted into the movie
        // library) even though keep_iso=false was set, because the
        // mover's 10 s scan loop ran before the ripper's post-mux
        // ISO-prune did. The fix: filter .iso out of the planned-moves
        // set when keep_iso=false; the existing `remove_dir_all` cleanup
        // at the end of check_and_move then sweeps the orphan ISO out
        // of staging when the .mkv move succeeds.
        let tmp = tempfile::tempdir().unwrap();
        let staging = tmp.path().join("staging");
        let movie_dir = tmp.path().join("output/Movies");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::create_dir_all(&movie_dir).unwrap();
        let cfg = cfg_for_staging(&staging, &movie_dir.to_string_lossy(), false);

        // One staging "disc dir" with .done + a valid .mkv + an .iso.
        let disc_dir = staging.join("Gleaming For Good");
        std::fs::create_dir_all(&disc_dir).unwrap();
        std::fs::write(disc_dir.join(".done"), marker_json("Gleaming For Good")).unwrap();
        // Valid EBML head + tail-safe body so check_post_copy_mkv passes.
        let mut mkv = vec![0x1A, 0x45, 0xDF, 0xA3];
        mkv.extend_from_slice(&[0xAAu8; 1024]);
        std::fs::write(disc_dir.join("Gleaming For Good.mkv"), &mkv).unwrap();
        std::fs::write(disc_dir.join("Gleaming For Good.iso"), vec![0u8; 4096]).unwrap();

        check_and_move(&cfg);

        // MKV landed in the movie library.
        let mkv_dest = movie_dir.join("Gleaming For Good (2024)/Gleaming For Good (2024).mkv");
        assert!(
            mkv_dest.exists(),
            "MKV should have been moved to {}",
            mkv_dest.display()
        );

        // ISO must NOT have been promoted to the movie library.
        let iso_dest = movie_dir.join("Gleaming For Good (2024)/Gleaming For Good (2024).iso");
        assert!(
            !iso_dest.exists(),
            "ISO must not be moved when keep_iso=false (found at {})",
            iso_dest.display()
        );

        // Staging is torn down on the same tick because the MKV moved
        // cleanly and the orphan ISO was swept by remove_dir_all.
        assert!(
            !disc_dir.exists(),
            "staging disc dir should have been removed after successful MKV move"
        );
    }

    #[cfg(unix)]
    #[test]
    fn check_and_move_records_error_when_inner_read_dir_fails() {
        // Regression: when read_dir on the staging disc dir fails (e.g. a
        // transient NFS error) after the .done marker is already parsed,
        // the dir must NOT be skipped silently. Pre-fix the `Err(_) =>
        // continue` arm dropped the failure with no record_error and no
        // log, leaving a .done-marked dir that the mover re-evaluated and
        // re-skipped every tick, invisible on the System page.
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let staging = tmp.path().join("staging");
        let movie_dir = tmp.path().join("output/Movies");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::create_dir_all(&movie_dir).unwrap();
        let cfg = cfg_for_staging(&staging, &movie_dir.to_string_lossy(), false);

        let disc_dir = staging.join("Unlistable");
        std::fs::create_dir_all(&disc_dir).unwrap();
        std::fs::write(disc_dir.join(".done"), marker_json("Unlistable")).unwrap();

        let dir_str = disc_dir.to_string_lossy().to_string();
        clear_error(&dir_str);

        // Owner execute-only (0o100): search bit lets read_to_string open
        // the known-path .done, but read bit cleared makes read_dir EACCES.
        std::fs::set_permissions(&disc_dir, std::fs::Permissions::from_mode(0o100)).unwrap();

        check_and_move(&cfg);

        // Restore perms so tempdir teardown can recurse.
        std::fs::set_permissions(&disc_dir, std::fs::Permissions::from_mode(0o755)).unwrap();

        let recorded = {
            let m = MOVE_ERRORS.lock().unwrap();
            m.get(&dir_str).cloned()
        };
        clear_error(&dir_str);
        assert!(
            recorded.is_some(),
            "a read_dir failure on the staging dir must record a mover error"
        );
    }

    #[test]
    fn check_and_move_moves_iso_when_keep_iso_true() {
        // Companion to the regression above: with keep_iso=true the
        // operator explicitly wants the ISO promoted alongside the
        // MKV. The 0.25.7 build_destination fix already routes them to
        // distinct paths (Title.iso vs Title.mkv); this test just
        // pins the filter behaviour on the cfg flag.
        let tmp = tempfile::tempdir().unwrap();
        let staging = tmp.path().join("staging");
        let movie_dir = tmp.path().join("output/Movies");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::create_dir_all(&movie_dir).unwrap();
        let cfg = cfg_for_staging(&staging, &movie_dir.to_string_lossy(), true);

        let disc_dir = staging.join("Keepme");
        std::fs::create_dir_all(&disc_dir).unwrap();
        std::fs::write(disc_dir.join(".done"), marker_json("Keepme")).unwrap();
        let mut mkv = vec![0x1A, 0x45, 0xDF, 0xA3];
        mkv.extend_from_slice(&[0xAAu8; 1024]);
        std::fs::write(disc_dir.join("Keepme.mkv"), &mkv).unwrap();
        std::fs::write(disc_dir.join("Keepme.iso"), vec![0u8; 4096]).unwrap();

        check_and_move(&cfg);

        assert!(movie_dir.join("Keepme (2024)/Keepme (2024).mkv").exists());
        assert!(movie_dir.join("Keepme (2024)/Keepme (2024).iso").exists());
    }

    #[test]
    fn copy_counting_copies_bytes_and_publishes_total() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src.bin");
        let dst = tmp.path().join("dst.bin");
        // Larger than the 4 MiB chunk so the counter ticks more than once.
        let data = vec![0xABu8; 5 * 1024 * 1024 + 17];
        std::fs::write(&src, &data).unwrap();
        let written = AtomicU64::new(0);
        let n = copy_counting(&src, &dst, &written).unwrap();
        assert_eq!(n, data.len() as u64, "returns total bytes copied");
        assert_eq!(
            written.load(Ordering::Relaxed),
            data.len() as u64,
            "final published count equals the source size"
        );
        assert_eq!(
            std::fs::read(&dst).unwrap(),
            data,
            "dest is a faithful copy"
        );
    }

    #[test]
    fn copy_counting_errors_on_missing_source() {
        use std::sync::atomic::AtomicU64;
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("nope.bin");
        let dst = tmp.path().join("dst.bin");
        let written = AtomicU64::new(0);
        assert!(copy_counting(&src, &dst, &written).is_err());
    }

    /// Regression (temp + rename atomicity): a failed/interrupted copy must
    /// NOT leave any file at the FINAL dest name — the bytes land on a
    /// sibling `.part-<pid>` temp and only `rename(2)` over the real name
    /// once fully written + fsynced. A truncated file at the real name
    /// would fail the mover's post-copy size check and wedge the move.
    #[test]
    fn copy_counting_failure_leaves_no_file_at_final_name() {
        use std::sync::atomic::AtomicU64;
        let tmp = tempfile::tempdir().unwrap();
        // Missing source → the copy errors out. (The same no-final-file
        // invariant holds for a mid-stream SIGKILL: bytes only ever exist
        // at the temp name until the atomic rename.)
        let src = tmp.path().join("missing.bin");
        let dst = tmp.path().join("final.mkv");
        let written = AtomicU64::new(0);
        assert!(copy_counting(&src, &dst, &written).is_err());
        assert!(
            !dst.exists(),
            "a failed copy must leave no file at the final dest name"
        );
        // And no orphan temp lingers next to it.
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("final.mkv.part-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "interrupted copy must not orphan a .part temp, found {leftovers:?}"
        );
    }

    /// Positive path: a successful `copy_counting` produces the final file
    /// atomically (via rename) with the exact source bytes, and leaves no
    /// `.part-` temp behind.
    #[test]
    fn copy_counting_success_renames_atomically_and_cleans_temp() {
        use std::sync::atomic::AtomicU64;
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src.bin");
        let dst = tmp.path().join("final.bin");
        let data = vec![0x5Au8; 3 * 1024 * 1024 + 5];
        std::fs::write(&src, &data).unwrap();
        let written = AtomicU64::new(0);
        let n = copy_counting(&src, &dst, &written).unwrap();
        assert_eq!(n, data.len() as u64);
        assert_eq!(
            std::fs::read(&dst).unwrap(),
            data,
            "final is a faithful copy"
        );
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".part-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "successful copy must leave no .part temp, found {leftovers:?}"
        );
    }

    #[test]
    fn copy_counting_clears_orphaned_part_temps_from_other_pids() {
        use std::sync::atomic::AtomicU64;
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src.bin");
        let dst = tmp.path().join("final.bin");
        std::fs::write(&src, vec![0x11u8; 1024]).unwrap();
        // Simulate orphaned temps left by prior crashed copies of THIS dest
        // under different pids. The current copy must sweep them before
        // writing its own fresh `.part-<pid>`.
        let orphan_a = tmp.path().join("final.bin.part-999991");
        let orphan_b = tmp.path().join("final.bin.part-999992");
        std::fs::write(&orphan_a, b"stale").unwrap();
        std::fs::write(&orphan_b, b"stale").unwrap();
        // An unrelated `.part-*` for a DIFFERENT dest must be left untouched.
        let unrelated = tmp.path().join("other.bin.part-999993");
        std::fs::write(&unrelated, b"keep").unwrap();

        let written = AtomicU64::new(0);
        copy_counting(&src, &dst, &written).unwrap();

        assert!(!orphan_a.exists(), "orphaned .part for this dest removed");
        assert!(!orphan_b.exists(), "orphaned .part for this dest removed");
        assert!(unrelated.exists(), "unrelated .part for other dest kept");
        assert_eq!(std::fs::read(&dst).unwrap(), vec![0x11u8; 1024]);
    }

    // ---- post-copy integrity + collision hardening tests ----

    /// Repo-local, gitignored scratch dir (never /tmp). Each call makes a
    /// unique subdir so parallel test threads don't collide.
    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-scratch")
            .join(format!("{}-{}-{}", tag, std::process::id(), n));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn check_post_copy_mkv_rejects_truncated_above_head_window() {
        // Load-bearing: a structurally-valid head/tail must NOT pass
        // when the destination is shorter than the source. Pre-fix the
        // mkv arm did head+tail only, so a copy truncated to anything
        // above the 5-byte tail window passed and move_file then unlinked
        // the only complete copy.
        let dir = scratch_dir("mkv-trunc");
        let src = dir.join("src.mkv");
        let dst = dir.join("dst.mkv");
        // Full source: valid EBML + 1 MiB body.
        write_minimal_mkv(&src, &vec![0xAA; 1024 * 1024]);
        // Truncated dest: valid EBML head and a readable tail, but far
        // shorter than src. Structural check alone would pass.
        write_minimal_mkv(&dst, &vec![0xAA; 4096]);
        let err = check_post_copy(&src, &dst).unwrap_err();
        assert!(
            matches!(err, MoveError::SizeDoesNotMatch { .. }),
            "truncated mkv must be rejected by the size cross-check, got {:?}",
            err
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn check_post_copy_m2ts_rejects_truncated_above_head_window() {
        // Load-bearing: same as mkv but for the TS-sync path. A copy
        // truncated to fewer packets than src — but still enough intact
        // head+tail sync bytes to clear THRESHOLD — must be rejected by
        // the size cross-check.
        let dir = scratch_dir("m2ts-trunc");
        let src = dir.join("src.m2ts");
        let dst = dir.join("dst.m2ts");
        write_minimal_m2ts(&src, 4096); // full source
        write_minimal_m2ts(&dst, 64); // truncated, but structurally fine
        let err = check_post_copy(&src, &dst).unwrap_err();
        assert!(
            matches!(err, MoveError::SizeDoesNotMatch { .. }),
            "truncated m2ts must be rejected by the size cross-check, got {:?}",
            err
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn check_post_copy_m2ts_rejects_overlapping_window_truncation() {
        // Secondary case: an 8..16-packet m2ts is too small for two disjoint
        // sample windows; pre-fix the head and tail windows overlapped and
        // a single intact head was counted twice to clear THRESHOLD. The
        // size floor (2x sample) now rejects such a file outright.
        let dir = scratch_dir("m2ts-overlap");
        let dst = dir.join("dst.m2ts");
        write_minimal_m2ts(&dst, 10); // 1920 bytes — between 1536 and 3072
        let src = dir.join("src.m2ts");
        std::fs::write(&src, b"any").unwrap();
        let err = check_post_copy(&src, &dst).unwrap_err();
        assert!(
            matches!(err, MoveError::M2tsBadSync),
            "8..16-packet m2ts must be rejected (overlapping sample windows), got {:?}",
            err
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn check_post_copy_m2ts_threshold_is_out_of_sixteen_not_eight() {
        // Regression for the line-390 doc bug: THRESHOLD=6 is counted across
        // BOTH the head (8) and tail (8) sample windows — 6 out of 16, not
        // 6 out of 8. Prove it behaviorally: a file with exactly 3 sync bytes
        // in the head window and 3 in the tail window (6 total, but only 3 in
        // any single window) must PASS. If the gate were truly "6 of 8 per
        // window" this would be rejected. Calling the m2ts checker directly
        // sidesteps check_post_copy's size cross-check so only the sync logic
        // is under test.
        const PKT: usize = 192;
        const SYNC_OFFSET: usize = 4;
        const PACKETS: usize = 24; // head=0..8, tail=16..24, disjoint middle gap
        let dir = scratch_dir("m2ts-threshold-16");
        let dst = dir.join("dst.m2ts");
        let mut bytes = vec![0u8; PACKETS * PKT];
        // 3 sync bytes in the head window (packets 0,1,2).
        for i in [0, 1, 2] {
            bytes[i * PKT + SYNC_OFFSET] = 0x47;
        }
        // 3 sync bytes in the tail window (last 8 packets: 16..24 → 21,22,23).
        for i in [21, 22, 23] {
            bytes[i * PKT + SYNC_OFFSET] = 0x47;
        }
        std::fs::write(&dst, &bytes).unwrap();
        assert!(
            check_post_copy_m2ts(&dst).is_ok(),
            "3 head + 3 tail = 6 of 16 must clear THRESHOLD; the gate counts \
             across both windows, not 6 of 8 in one"
        );
        // And confirm 5 total (3 head + 2 tail) is below THRESHOLD → rejected,
        // so the gate isn't trivially passing everything.
        bytes[23 * PKT + SYNC_OFFSET] = 0x00;
        std::fs::write(&dst, &bytes).unwrap();
        assert!(
            matches!(check_post_copy_m2ts(&dst), Err(MoveError::M2tsBadSync)),
            "5 of 16 must fall below THRESHOLD=6"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn check_post_copy_mkv_tail_comment_does_not_claim_64kib_ebml_scan() {
        // Regression for the line-349 doc bug: the MKV tail comment used to
        // claim it "read the last 64 KiB and confirm at least one well-formed
        // EBML element close" — neither of which the code does (it reads 8
        // tail bytes). Source-pin the corrected comment so the false claim
        // can't creep back in.
        let src = include_str!("mover.rs");
        let start = src
            .find("fn check_post_copy_mkv")
            .expect("check_post_copy_mkv present");
        let body = &src[start..start + 1200];
        assert!(
            !body.contains("64 KiB"),
            "MKV tail comment must not claim a 64 KiB read the code never does"
        );
        assert!(
            !body.contains("EBML element close"),
            "MKV tail comment must not claim EBML-element-close detection"
        );
    }

    #[test]
    fn check_and_move_collision_same_size_different_content_preserves_staging() {
        // Two DIFFERENT discs route to the same Title (Year) path and
        // their muxes happen to be the SAME byte length. The size-only
        // guard waved this through to Skipped, then remove_dir_all deleted
        // the NEW rip's staging while the library kept the OLD wrong file.
        // The content-aware guard must catch it: Collision, staging
        // preserved, library file untouched.
        let dir = scratch_dir("collision");
        let staging = dir.join("staging");
        let movie_dir = dir.join("output/Movies");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::create_dir_all(&movie_dir).unwrap();
        let cfg = cfg_for_staging(&staging, &movie_dir.to_string_lossy(), false);

        // Pre-existing (OLD, wrong) library file at the destination path.
        let dest_dir = movie_dir.join("Clash (2024)");
        std::fs::create_dir_all(&dest_dir).unwrap();
        let dest_file = dest_dir.join("Clash (2024).mkv");
        let mut old = vec![0x1A, 0x45, 0xDF, 0xA3];
        old.extend_from_slice(&[0x11u8; 4096]);
        std::fs::write(&dest_file, &old).unwrap();

        // NEW rip in staging — SAME byte length, DIFFERENT content.
        let disc_dir = staging.join("Clash");
        std::fs::create_dir_all(&disc_dir).unwrap();
        std::fs::write(disc_dir.join(".done"), marker_json("Clash")).unwrap();
        let mut new = vec![0x1A, 0x45, 0xDF, 0xA3];
        new.extend_from_slice(&[0x22u8; 4096]); // differs in body
        assert_eq!(new.len(), old.len(), "test setup: sizes must match");
        let staged_mkv = disc_dir.join("Clash.mkv");
        std::fs::write(&staged_mkv, &new).unwrap();

        check_and_move(&cfg);

        // Library file must be untouched (still the OLD content).
        assert_eq!(
            std::fs::read(&dest_file).unwrap(),
            old,
            "existing library file must NOT be overwritten or removed"
        );
        // The NEW rip must still be in staging — NOT deleted.
        assert!(
            staged_mkv.exists(),
            "new rip must be preserved in staging on a collision"
        );
        assert!(disc_dir.exists(), "staging dir must not be torn down");
        // A collision error must be surfaced for the operator.
        let key = disc_dir.to_string_lossy().to_string();
        {
            let m = MOVE_ERRORS.lock().unwrap();
            assert!(
                m.contains_key(&key),
                "collision must be recorded for the staging dir"
            );
        }
        clear_error(&key);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn check_and_move_idempotent_same_size_same_content_cleans_up() {
        // Regression guard: the content-aware collision check must NOT
        // break the legitimate idempotent re-move. A prior tick copied the
        // file to the library (same content, same size); on a later tick
        // the dest already exists identically. This must be treated as the
        // idempotent path (Skipped/Moved), NOT a collision, and staging
        // must clean up.
        let dir = scratch_dir("idempotent");
        let staging = dir.join("staging");
        let movie_dir = dir.join("output/Movies");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::create_dir_all(&movie_dir).unwrap();
        let cfg = cfg_for_staging(&staging, &movie_dir.to_string_lossy(), false);

        let mut content = vec![0x1A, 0x45, 0xDF, 0xA3];
        content.extend_from_slice(&[0x33u8; 4096]);

        // Dest already present with identical bytes (prior successful copy).
        let dest_dir = movie_dir.join("Echo (2024)");
        std::fs::create_dir_all(&dest_dir).unwrap();
        std::fs::write(dest_dir.join("Echo (2024).mkv"), &content).unwrap();

        // Staging still holds the same file (its unlink failed last tick).
        let disc_dir = staging.join("Echo");
        std::fs::create_dir_all(&disc_dir).unwrap();
        std::fs::write(disc_dir.join(".done"), marker_json("Echo")).unwrap();
        std::fs::write(disc_dir.join("Echo.mkv"), &content).unwrap();

        check_and_move(&cfg);

        // Staging is torn down — the re-move was recognized as idempotent.
        assert!(
            !disc_dir.exists(),
            "idempotent same-content re-move must clean up staging"
        );
        // No collision error recorded.
        let key = disc_dir.to_string_lossy().to_string();
        {
            let m = MOVE_ERRORS.lock().unwrap();
            assert!(
                !m.contains_key(&key),
                "idempotent re-move must not surface a collision error"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    // ===================================================================
    // EXHAUSTIVE mover decider matrix (rc4 hardening).
    //
    // The mover is the third staging-state decider. It keys ONLY on `.done`
    // (the mux/resume hand-off): for each staging dir it either moves the
    // muxed output to the library (and tears the dir down) or leaves the dir
    // alone. These tests drive the REAL `check_and_move` against a real
    // staging tree for every meaningful `.done`-state combination and assert
    // the observable outcome (dest present? staging dir gone?).
    // ===================================================================

    /// Outcome of one mover decision, observed from the filesystem.
    #[derive(Debug, PartialEq)]
    enum MoverVerdict {
        /// Output landed in the library and staging was torn down.
        MovedAndCleaned,
        /// Staging dir left in place, nothing moved to the library.
        LeftAlone,
    }

    /// Build a single staging disc dir, run the real `check_and_move`, and
    /// report whether the MKV reached the library and staging was cleaned.
    /// `done_body`: `None` = no `.done` marker; `Some(bytes)` = that exact
    /// `.done` content. `with_mkv`: whether a valid EBML MKV is staged.
    /// `extra`: extra marker filenames to drop in (e.g. `.completed`).
    fn mover_verdict(done_body: Option<&[u8]>, with_mkv: bool, extra: &[&str]) -> MoverVerdict {
        let tmp = tempfile::tempdir().unwrap();
        let staging = tmp.path().join("staging");
        let movie_dir = tmp.path().join("output/Movies");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::create_dir_all(&movie_dir).unwrap();
        let cfg = cfg_for_staging(&staging, &movie_dir.to_string_lossy(), false);

        let disc = staging.join("Disc");
        std::fs::create_dir_all(&disc).unwrap();
        if let Some(body) = done_body {
            std::fs::write(disc.join(".done"), body).unwrap();
        }
        if with_mkv {
            let mut mkv = vec![0x1A, 0x45, 0xDF, 0xA3];
            mkv.extend_from_slice(&[0xAAu8; 1024]);
            std::fs::write(disc.join("Disc.mkv"), &mkv).unwrap();
        }
        for e in extra {
            std::fs::write(disc.join(e), b"x").unwrap();
        }

        // Clear any stale error for this dir before/after so the shared
        // MOVE_ERRORS map doesn't leak across rows.
        let key = disc.to_string_lossy().to_string();
        clear_error(&key);
        check_and_move(&cfg);
        clear_error(&key);

        let moved = movie_dir.join("Disc.mkv").exists()
            || movie_dir
                .join("Disc")
                .join("Disc.mkv")
                .exists()
            // marker has title "Disc" → movie path is "Disc/Disc.mkv" with no year,
            // but marker_json sets year 2024 → "Disc (2024)/Disc (2024).mkv".
            || movie_dir.join("Disc (2024)/Disc (2024).mkv").exists();
        let cleaned = !disc.exists();
        if moved && cleaned {
            MoverVerdict::MovedAndCleaned
        } else {
            MoverVerdict::LeftAlone
        }
    }

    #[test]
    fn mover_decider_matrix() {
        let valid = marker_json("Disc");
        let valid_b = valid.as_bytes();

        // --- no .done marker: mover never acts ---
        assert_eq!(
            mover_verdict(None, true, &[]),
            MoverVerdict::LeftAlone,
            "no .done marker → mover must not move (it keys solely on .done)"
        );
        // .completed / .ripped without .done are not the mover's hand-off.
        assert_eq!(
            mover_verdict(None, true, &[".completed"]),
            MoverVerdict::LeftAlone,
            ".completed without .done is not the mover's signal"
        );
        assert_eq!(
            mover_verdict(None, true, &[".ripped"]),
            MoverVerdict::LeftAlone,
            ".ripped without .done is the mux worker's signal, not the mover's"
        );

        // --- valid .done + movable output → move + clean ---
        assert_eq!(
            mover_verdict(Some(valid_b), true, &[]),
            MoverVerdict::MovedAndCleaned,
            "valid .done + MKV → move to library and tear down staging"
        );

        // --- valid .done but NO movable output → left alone ---
        assert_eq!(
            mover_verdict(Some(valid_b), false, &[]),
            MoverVerdict::LeftAlone,
            "valid .done but no .mkv/.m2ts to move → skip (nothing to promote)"
        );

        // --- torn / empty .done → not-ready, skip (never blind-move) ---
        assert_eq!(
            mover_verdict(Some(b""), true, &[]),
            MoverVerdict::LeftAlone,
            "empty .done (torn write) → not ready, skip"
        );
        assert_eq!(
            mover_verdict(Some(b"{ this is not json"), true, &[]),
            MoverVerdict::LeftAlone,
            "unparseable .done → not ready, skip"
        );

        // --- parseable .done with empty title AND disc_name → skip ---
        let empty_title = serde_json::json!({ "title": "", "disc_name": "" }).to_string();
        assert_eq!(
            mover_verdict(Some(empty_title.as_bytes()), true, &[]),
            MoverVerdict::LeftAlone,
            "parseable .done with empty title+disc_name → no usable dest name, skip"
        );

        // --- valid .done coexisting with .completed → still moves
        //     (.completed does not block the mover; the mux worker's terminal
        //     guard is separate). The mover only needs .done + output. ---
        assert_eq!(
            mover_verdict(Some(valid_b), true, &[".completed"]),
            MoverVerdict::MovedAndCleaned,
            "valid .done + .completed + MKV → mover still files it"
        );
    }

    #[test]
    fn done_absence_in_progress_vs_fault() {
        use std::io::ErrorKind;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("disc");
        std::fs::create_dir_all(&dir).unwrap();

        // Bare dir, .done NotFound, no governing marker → stranded → Fault (WARN).
        assert_eq!(
            classify_done_absence(ErrorKind::NotFound, &dir),
            DoneAbsence::Fault,
            "NotFound with no governing marker is a stranded dir → WARN"
        );

        // A non-NotFound read error is always a fault, even mid-rip.
        assert_eq!(
            classify_done_absence(ErrorKind::PermissionDenied, &dir),
            DoneAbsence::Fault,
            "EACCES/ESTALE etc. → WARN regardless of governing marker"
        );

        // Each governing marker turns a .done NotFound into the by-design
        // in-progress state (quiet skip, no WARN). This is the 182-warn bug:
        // a long rip sits at .sweeping (sweep), .ripped (awaiting mux),
        // .muxing (mux running), then .completed/.failed/.review with no .done
        // for many ticks. `.sweeping` is the load-bearing addition here — the
        // multi-hour sweep window had no governing marker and WARN-flooded.
        for m in [
            ".sweeping",
            ".muxing",
            ".ripped",
            ".completed",
            ".failed",
            ".review",
        ] {
            let governed = tmp.path().join(format!("disc{m}"));
            std::fs::create_dir_all(&governed).unwrap();
            std::fs::write(governed.join(m), b"x").unwrap();
            assert_eq!(
                classify_done_absence(ErrorKind::NotFound, &governed),
                DoneAbsence::InProgress,
                "NotFound while {m} present is the in-progress state → no WARN"
            );
            // ...but a non-NotFound error on the same dir is still a fault.
            assert_eq!(
                classify_done_absence(ErrorKind::Other, &governed),
                DoneAbsence::Fault,
                "non-NotFound error is a fault even with {m} present"
            );
        }
    }

    /// Convergence round 4 (M3): the governed-marker probe must route through
    /// `snapshot_staging_disc` (NFS-attribute-cache-resilient, 3x-retried
    /// read_dir) rather than bare per-marker `exists()` calls, so a cold-cache
    /// mount right after a container restart can't false-negative a durably
    /// present `.sweeping` into a `Fault` and WARN-flood the multi-hour sweep
    /// window (the original 182-warn bug). A dir whose ONLY entry is `.sweeping`
    /// classifies InProgress — and the same snapshot the rest of the resume
    /// machinery uses agrees it's owned.
    #[test]
    fn done_absence_sweeping_governed_via_snapshot() {
        use std::io::ErrorKind;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("disc-sweeping");
        std::fs::create_dir_all(&dir).unwrap();
        crate::ripper::staging::write_sweeping_marker(&dir);
        // The same snapshot the governed check now consults sees the marker.
        let snap = crate::ripper::staging::snapshot_staging_disc(&dir).expect("snapshot");
        assert!(snap.has_sweeping);
        assert_eq!(
            classify_done_absence(ErrorKind::NotFound, &dir),
            DoneAbsence::InProgress,
            "a durably-present .sweeping marker is the in-progress state → no WARN"
        );
    }

    /// Regression: a staging dir that vanished between the `.done` read and the
    /// governing-marker probe (a finished move, or `/api/stop` cleanup) must be
    /// treated as InProgress, not a stranded-dir Fault — that transition is a
    /// normal lifecycle event and must not emit a spurious WARN.
    #[test]
    fn done_absence_vanished_dir_is_in_progress_not_fault() {
        use std::io::ErrorKind;
        let tmp = tempfile::tempdir().unwrap();
        // Path under the temp root that was never created (or already removed).
        let gone = tmp.path().join("disc-removed");
        assert!(!gone.exists());
        assert_eq!(
            classify_done_absence(ErrorKind::NotFound, &gone),
            DoneAbsence::InProgress,
            "a dir that disappeared out from under the mover is a lifecycle \
             transition, not a stranded-dir fault → no WARN"
        );
        // A non-NotFound error on a missing dir is still surfaced (the read
        // failure itself, not the absence, is what we report).
        assert_eq!(
            classify_done_absence(ErrorKind::PermissionDenied, &gone),
            DoneAbsence::Fault,
            "non-NotFound errors stay faults even when the dir is gone"
        );
    }

    /// Precedence guard for the TOCTOU fix: the vanished-dir check runs BEFORE
    /// the governing-marker probe. A marker on a SIBLING dir must not leak into
    /// a vanished dir's classification, and a marker placed on the vanished
    /// dir's would-be path doesn't exist (the dir is gone) so it can't rescue a
    /// genuine stranded condition into a false InProgress.
    #[test]
    fn done_absence_vanished_dir_ignores_sibling_markers() {
        use std::io::ErrorKind;
        let tmp = tempfile::tempdir().unwrap();

        // A live sibling dir that DOES carry a governing marker.
        let sibling = tmp.path().join("disc-live");
        std::fs::create_dir_all(&sibling).unwrap();
        std::fs::write(sibling.join(".ripped"), b"x").unwrap();

        // The dir we classify never existed — its marker join would resolve
        // under it, not under the sibling.
        let gone = tmp.path().join("disc-gone");
        assert!(!gone.exists());

        assert_eq!(
            classify_done_absence(ErrorKind::NotFound, &gone),
            DoneAbsence::InProgress,
            "a vanished dir is InProgress on its own merits, independent of any \
             sibling's markers"
        );
        // The sibling's classification is independent and unaffected: present
        // dir + marker → InProgress.
        assert_eq!(
            classify_done_absence(ErrorKind::NotFound, &sibling),
            DoneAbsence::InProgress
        );
    }

    /// A dir that EXISTS but carries no governing marker is a genuine stranded
    /// condition (Fault) — the vanished-dir early-return must NOT swallow it.
    /// This pins that the `!dir.exists()` guard is specifically about
    /// disappearance, not "absent marker".
    #[test]
    fn done_absence_present_dir_without_marker_is_fault() {
        use std::io::ErrorKind;
        let dir = scratch_dir("strandedfault");
        assert!(dir.exists(), "dir is present");
        // No .ripped/.completed/.failed/.review marker written.
        assert_eq!(
            classify_done_absence(ErrorKind::NotFound, &dir),
            DoneAbsence::Fault,
            "a present dir with no governing marker is genuinely stranded → WARN"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn same_head_and_tail_distinguishes_identical_from_different() {
        let dir = scratch_dir("headtail");
        let a = dir.join("a.bin");
        let b = dir.join("b.bin");
        let c = dir.join("c.bin");
        let base = vec![0x5Au8; 200 * 1024]; // larger than the 64 KiB window
        std::fs::write(&a, &base).unwrap();
        std::fs::write(&b, &base).unwrap();
        // c: same length, differs only in the middle (outside both windows)
        // — head+tail probe treats it as identical (acceptable: a real mux
        // collision differing only in the interior of a multi-GB file is
        // not realistic, and the cost of a full compare every tick is not).
        let mut mid = base.clone();
        let m = mid.len() / 2;
        mid[m] ^= 0xFF;
        std::fs::write(&c, &mid).unwrap();
        assert!(same_head_and_tail(&a, &b), "identical files match");
        assert!(same_head_and_tail(&a, &c), "interior-only diff matches");

        // d: differs at the head → not identical.
        let d = dir.join("d.bin");
        let mut headdiff = base.clone();
        headdiff[0] ^= 0xFF;
        std::fs::write(&d, &headdiff).unwrap();
        assert!(!same_head_and_tail(&a, &d), "head diff must not match");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ===================================================================
    // Fail-loud destination validation (Mercy incident hardening).
    // The mover must ERROR + preserve-in-staging when the configured
    // destination root is missing/unwritable, and must NEVER silently
    // create the root (which, with a lost bind-mount, writes into the
    // container's ephemeral overlay). It must also log FULL ABSOLUTE
    // destination paths, never a cwd-relative path.
    // ===================================================================

    /// dest root MISSING → error, and the validation must NOT create it
    /// (no silent `create_dir_all` of a dead mount point).
    #[test]
    fn validate_destination_root_errors_when_missing_and_does_not_create() {
        let tmp = tempfile::TempDir::new().unwrap();
        let missing = tmp.path().join("movies-mount-gone");
        let missing_str = missing.to_string_lossy().to_string();
        // Precondition: it really is absent.
        assert!(!missing.exists());

        let res = validate_destination_root(&missing_str);
        assert!(res.is_err(), "a missing destination root must be an error");
        let reason = res.unwrap_err();
        assert!(
            reason.contains("does not exist"),
            "error must explain the root is missing, got: {reason}"
        );
        // THE KEY GUARANTEE: validation did not auto-create the root. A real
        // move would then preserve the output in staging, not write 80 GB
        // into a fresh overlay dir.
        assert!(
            !missing.exists(),
            "validate_destination_root must NOT create the missing root (no silent create)"
        );
    }

    /// dest root PRESENT + WRITABLE → Ok, and the writability probe leaves
    /// no marker file behind.
    #[test]
    fn validate_destination_root_ok_when_present_and_writable() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_string_lossy().to_string();
        assert!(validate_destination_root(&root).is_ok());
        // The probe file must be cleaned up.
        let leftover: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".autorip-writable-probe")
            })
            .collect();
        assert!(leftover.is_empty(), "writability probe must be removed");
    }

    /// A RELATIVE root is rejected — the exact shape that produced the
    /// incident's "Moved to movies/Mercy/..." cwd-relative write.
    #[test]
    fn validate_destination_root_rejects_relative_path() {
        let res = validate_destination_root("movies");
        assert!(res.is_err(), "a relative root must be rejected");
        assert!(res.unwrap_err().contains("absolute"));
    }

    /// An empty root is rejected (would `create_dir_all("")` → cwd writes).
    #[test]
    fn validate_destination_root_rejects_empty() {
        assert!(validate_destination_root("").is_err());
    }

    /// A root that exists but is a FILE (not a directory) is rejected.
    #[test]
    fn validate_destination_root_rejects_non_directory() {
        let tmp = tempfile::TempDir::new().unwrap();
        let file = tmp.path().join("not-a-dir");
        std::fs::write(&file, b"x").unwrap();
        let res = validate_destination_root(&file.to_string_lossy());
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("not a directory"));
    }

    /// A present-but-read-only root is rejected by the writability probe.
    #[cfg(unix)]
    #[test]
    fn validate_destination_root_rejects_read_only() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::TempDir::new().unwrap();
        let ro = tmp.path().join("ro-root");
        std::fs::create_dir(&ro).unwrap();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o500)).unwrap();
        let res = validate_destination_root(&ro.to_string_lossy());
        // Restore perms so TempDir cleanup works regardless of the outcome.
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o700)).ok();
        // Root runs as uid 0 in some CI sandboxes and can write through 0o500;
        // only assert the failure when the probe genuinely couldn't write.
        if let Err(reason) = res {
            assert!(
                reason.contains("not writable"),
                "read-only root must fail with a writability reason, got: {reason}"
            );
        }
    }

    /// `destination_root` selects the SAME root `build_destination` routes
    /// to, for every media-type / configured-dir combination — so the
    /// validation guards exactly the root the move will use.
    #[test]
    fn destination_root_matches_build_destination_root() {
        let cfg = cfg_with_dirs("/mnt/movies", "/mnt/tv", "/mnt/out");
        // movie with movie_dir set → movie_dir
        assert_eq!(
            destination_root(&cfg, &Some(tmdb_movie("X", 2024))),
            "/mnt/movies"
        );
        // tv with tv_dir set → tv_dir
        let tv = tmdb::TmdbResult {
            title: "Y".into(),
            year: 2024,
            poster_url: String::new(),
            overview: String::new(),
            media_type: "tv".into(),
        };
        assert_eq!(destination_root(&cfg, &Some(tv)), "/mnt/tv");
        // no tmdb → output_dir
        assert_eq!(destination_root(&cfg, &None), "/mnt/out");
        // movie but movie_dir empty → output_dir (matches build_destination
        // fall-through)
        let cfg2 = cfg_with_dirs("", "/mnt/tv", "/mnt/out");
        assert_eq!(
            destination_root(&cfg2, &Some(tmdb_movie("X", 2024))),
            "/mnt/out"
        );
    }

    /// `absolute_for_log` never yields a cwd-relative path: absolute passes
    /// through; relative is anchored to an absolute cwd.
    #[test]
    fn absolute_for_log_is_always_absolute() {
        assert_eq!(
            absolute_for_log("/mnt/unraid-1/media/movies/Mercy (2024)/Mercy (2024).mkv"),
            "/mnt/unraid-1/media/movies/Mercy (2024)/Mercy (2024).mkv"
        );
        let rel = absolute_for_log("movies/Mercy/Mercy.mkv");
        assert!(
            std::path::Path::new(&rel).is_absolute(),
            "a relative dest must be rendered as an absolute path for logging, got: {rel}"
        );
        assert!(rel.ends_with("movies/Mercy/Mercy.mkv"));
    }

    /// `check_configured_destinations` reports each broken root once and
    /// stays silent on good/empty roots.
    #[test]
    fn check_configured_destinations_reports_missing_roots() {
        let tmp = tempfile::TempDir::new().unwrap();
        let good = tmp.path().join("good");
        std::fs::create_dir(&good).unwrap();
        let missing = tmp.path().join("missing");

        // movie_dir missing, tv_dir empty (skipped), output_dir good.
        let cfg = cfg_with_dirs(&missing.to_string_lossy(), "", &good.to_string_lossy());
        let problems = check_configured_destinations(&cfg);
        assert_eq!(
            problems.len(),
            1,
            "only the missing movie_dir should be flagged"
        );
        assert_eq!(problems[0].0, missing.to_string_lossy());

        // All-good config → no problems.
        let cfg_ok = cfg_with_dirs(&good.to_string_lossy(), "", &good.to_string_lossy());
        assert!(
            check_configured_destinations(&cfg_ok).is_empty(),
            "a fully-present config must report no problems"
        );
    }

    /// Dedup: when movie_dir == output_dir and both are missing, the
    /// operator sees ONE warning, not two.
    #[test]
    fn check_configured_destinations_deduplicates_identical_roots() {
        let tmp = tempfile::TempDir::new().unwrap();
        let missing = tmp.path().join("same-missing-root");
        let m = missing.to_string_lossy().to_string();
        let cfg = cfg_with_dirs(&m, "", &m); // movie_dir == output_dir
        let problems = check_configured_destinations(&cfg);
        assert_eq!(
            problems.len(),
            1,
            "an identical movie/output root must be reported once, got {problems:?}"
        );
    }
}
