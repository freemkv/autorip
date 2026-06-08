use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::sync::Mutex;

/// Per-device in-memory ring buffer cap (lines). The file log is the
/// durable record; this is just the live UI view.
const RING_CAP: usize = 500;

/// Size threshold above which the non-device `system` log file is rotated
/// into `logs/rips/` on startup. The system log is never archived per-rip
/// (no eject/scan boundary), so without this it would grow unbounded for
/// the container lifetime.
const SYSTEM_LOG_ROTATE_BYTES: u64 = 5 * 1024 * 1024;

static LOGS: once_cell::sync::Lazy<Mutex<HashMap<String, VecDeque<String>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

fn log_dir() -> String {
    std::env::var("AUTORIP_DIR").unwrap_or_else(|_| "/config".to_string())
}

/// Neutralize a device string into a safe single path component for the
/// log filename. Enforced at the single construction point so no caller
/// (web routes, SCSI enumeration, the literal "system" syslog channel, or
/// any future caller) can write or rename outside `logs/` via a `/`, `\`,
/// or `..` in the device name. Reachable callers are all well-behaved
/// today (web gates on is_valid_device_name), so this is a hard invariant,
/// not a live exploit fix.
fn sanitize_device(device: &str) -> String {
    if device.is_empty()
        || device == "."
        || device == ".."
        || device.contains('/')
        || device.contains('\\')
        || device.contains("..")
    {
        tracing::warn!(device = %device, "unsafe device name neutralized to 'invalid' for log path");
        return "invalid".to_string();
    }
    device.to_string()
}

fn device_log_path(device: &str) -> String {
    format!("{}/logs/device_{}.log", log_dir(), sanitize_device(device))
}

/// Log a message for a specific device. Three sinks:
///
/// - **In-memory ring** (last 500 lines per device) — read by the web UI's
///   `/api/logs/{device}` endpoint to render the live log view.
/// - **`{AUTORIP_DIR}/logs/device_{dev}.log`** — per-device file, archived
///   per-rip via `archive_device_log`. Operators tail when troubleshooting
///   one drive.
/// - **Tracing event** at info level with `device` field — flows into
///   `autorip.log` (everything) and `autorip.jsonl` (machine-greppable).
///   See `observe.rs`.
///
/// Line format in the per-device file: `[YYYY-MM-DDTHH:MM:SSZ] msg`. The
/// in-memory ring stores the same. ISO-8601 timestamps so rip log archives
/// sort correctly and midnight isn't ambiguous.
pub fn device_log(device: &str, msg: &str) {
    let line = format!("[{}] {}", crate::util::format_iso_datetime(), msg);

    // In-memory ring (last RING_CAP lines per device). VecDeque gives O(1)
    // eviction from the front instead of Vec::remove(0)'s O(n) shift.
    if let Ok(mut logs) = LOGS.lock() {
        let log = logs.entry(device.to_string()).or_default();
        log.push_back(line.clone());
        if log.len() > RING_CAP {
            log.pop_front();
        }
    }

    // File log — per-device, append-only between archive points. A disk-full
    // or NFS stale-handle condition here must not break a rip (logging is
    // never load-bearing), but it should be observable rather than fully
    // silent.
    let path = device_log_path(device);
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        Ok(mut f) => {
            if let Err(e) = writeln!(f, "{}", line) {
                tracing::warn!(device = %device, path = %path, error = %e, "device log write failed");
            }
        }
        Err(e) => {
            tracing::warn!(device = %device, path = %path, error = %e, "device log open failed");
        }
    }

    // Structured event into the central log stream. The `device` field is
    // what makes `jq 'select(.fields.device == "sg4")' autorip.jsonl`
    // possible — the per-device files don't carry that grouping in a single
    // file the way the JSONL stream does.
    tracing::info!(device = %device, "{}", msg);
}

/// Get the most recent `lines` log lines for a device, oldest-first.
pub fn get_device_log(device: &str, lines: usize) -> Vec<String> {
    LOGS.lock()
        .ok()
        .and_then(|logs| {
            logs.get(device).map(|log| {
                // Single allocation of just the tail slice, computed while
                // holding the lock — no intermediate double-reverse Vecs.
                let start = log.len().saturating_sub(lines);
                log.iter().skip(start).cloned().collect()
            })
        })
        .unwrap_or_default()
}

/// Move the device's current live log to `logs/rips/{device}_{iso_ts}.log`
/// and clear the in-memory buffer. Called at the start of a new scan and on
/// eject so each rip attempt gets its own self-contained archive — no more
/// "yesterday's 12h saga mixed with tonight's run" confusion.
///
/// No-op if the current log is empty or missing. Archive failures are
/// logged to stderr but never propagated — logging must never break a rip.
pub fn archive_device_log(device: &str) {
    let current = device_log_path(device);
    let should_archive = std::fs::metadata(&current)
        .map(|m| m.len() > 0)
        .unwrap_or(false);

    // Track whether the on-disk archive actually happened. When there is
    // nothing to archive (`!should_archive`) there is nothing to lose, so
    // that counts as "ok" to clear. Only an actual rename failure must
    // keep the in-memory ring intact — otherwise the web UI's live view
    // would go empty while the un-archived device_*.log still sits on
    // disk, with no signal to the operator.
    let mut archived_ok = !should_archive;

    if should_archive {
        let rips_dir = format!("{}/logs/rips", log_dir());
        if let Err(e) = std::fs::create_dir_all(&rips_dir) {
            tracing::warn!(
                device = %device,
                path = %rips_dir,
                error = %e,
                "log archive: cannot create rips dir"
            );
        } else {
            let archive = format!(
                "{}/{}_{}.log",
                rips_dir,
                sanitize_device(device),
                crate::util::format_iso_datetime_filename(),
            );
            match std::fs::rename(&current, &archive) {
                Ok(()) => archived_ok = true,
                Err(e) => tracing::warn!(
                    device = %device,
                    src = %current,
                    dst = %archive,
                    error = %e,
                    "log archive: rename failed; keeping in-memory ring so the live view stays populated"
                ),
            }
        }
    }

    // Only clear the in-memory ring once the live file is safely archived
    // (or there was nothing to archive). On a rename failure we leave the
    // ring so the live view still reflects the on-disk log.
    if archived_ok {
        if let Ok(mut logs) = LOGS.lock() {
            logs.remove(device);
        }
    }
}

/// Log to system log (not device-specific).
pub fn syslog(msg: &str) {
    device_log("system", msg);
}

/// Rotate the non-device `system` log into `logs/rips/` if it has grown past
/// `SYSTEM_LOG_ROTATE_BYTES`. Unlike per-device logs (archived on each
/// scan/eject boundary), the system log has no natural archive point, so
/// without this it grows unbounded for the container's lifetime. Called once
/// at startup; reuses `archive_device_log`'s rename-into-rips behaviour.
/// Best-effort and never propagates — logging must not break startup.
pub fn rotate_system_log_if_large() {
    let path = device_log_path("system");
    let too_big = std::fs::metadata(&path)
        .map(|m| m.len() > SYSTEM_LOG_ROTATE_BYTES)
        .unwrap_or(false);
    if too_big {
        archive_device_log("system");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Log tests manipulate the process-wide AUTORIP_DIR env var and the
    // module-global LOGS mutex. Cargo runs tests in parallel by default,
    // which caused `archive_device_log_moves_to_rips_dir` to fail
    // intermittently when another test mutated AUTORIP_DIR between our
    // `device_log` write and our assertion on the file's location. Serialize
    // all tests in this module through a local mutex so env-and-state setup
    // is atomic per-test.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(0);
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        // Per project convention, never /tmp (wiped on reboot). Anchor
        // under the workspace's target/ (gitignored) instead.
        let d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target/test-scratch")
            .join(format!(
                "autorip-log-test-{}-{}-{}",
                std::process::id(),
                tag,
                n
            ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(d.join("logs")).unwrap();
        d
    }

    #[test]
    fn device_log_writes_iso_timestamped_line() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let d = tmpdir("iso_ts");
        // Route the test's logs to the tempdir.
        // SAFETY: env access in single-threaded tests.
        unsafe {
            std::env::set_var("AUTORIP_DIR", &d);
        }
        let dev = format!("test_sg_{}", std::process::id());
        device_log(&dev, "hello");
        let content = std::fs::read_to_string(device_log_path(&dev)).unwrap();
        // Format: [YYYY-MM-DDTHH:MM:SSZ] hello
        assert!(content.starts_with('['));
        assert!(content.contains(']'));
        assert!(content.trim_end().ends_with("hello"));
        let bracket = &content[1..21]; // 20-char ISO datetime inside brackets
        assert_eq!(bracket.len(), 20);
        assert!(bracket.ends_with('Z'));
        assert_eq!(bracket.as_bytes()[10], b'T');
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn archive_device_log_moves_to_rips_dir() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let d = tmpdir("archive_move");
        unsafe {
            std::env::set_var("AUTORIP_DIR", &d);
        }
        let dev = format!("test_mv_{}", std::process::id());
        device_log(&dev, "pre-archive");
        let live = device_log_path(&dev);
        assert!(std::path::Path::new(&live).exists());

        archive_device_log(&dev);

        // Live file gone after archive.
        assert!(!std::path::Path::new(&live).exists());

        // Rips dir has exactly one file matching the device name.
        let rips_dir = d.join("logs").join("rips");
        let archived: Vec<_> = std::fs::read_dir(&rips_dir)
            .unwrap()
            .filter_map(|r| r.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(&dev))
            .collect();
        assert_eq!(archived.len(), 1, "expected one archived log file");

        let content = std::fs::read_to_string(archived[0].path()).unwrap();
        assert!(content.contains("pre-archive"));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn archive_device_log_no_op_when_empty() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let d = tmpdir("archive_empty");
        unsafe {
            std::env::set_var("AUTORIP_DIR", &d);
        }
        let dev = format!("test_empty_{}", std::process::id());
        // Don't call device_log — file doesn't exist yet. archive_device_log
        // must not panic or create a junk archive entry.
        archive_device_log(&dev);
        let rips_dir = d.join("logs").join("rips");
        if rips_dir.exists() {
            let entries: Vec<_> = std::fs::read_dir(&rips_dir)
                .unwrap()
                .filter_map(|r| r.ok())
                .filter(|e| e.file_name().to_string_lossy().contains(&dev))
                .collect();
            assert!(entries.is_empty());
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn archive_device_log_clears_in_memory_buffer() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let d = tmpdir("archive_buf");
        unsafe {
            std::env::set_var("AUTORIP_DIR", &d);
        }
        let dev = format!("test_buf_{}", std::process::id());
        device_log(&dev, "first");
        device_log(&dev, "second");
        assert!(!get_device_log(&dev, 100).is_empty());

        archive_device_log(&dev);
        assert!(get_device_log(&dev, 100).is_empty());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn archive_failure_keeps_in_memory_ring() {
        // If the on-disk archive fails, the in-memory ring MUST survive so
        // the live UI view doesn't go empty while the log is still on disk.
        // Force create_dir_all("logs/rips") to fail by planting a regular
        // file where the rips directory needs to be.
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let d = tmpdir("archive_fail");
        unsafe {
            std::env::set_var("AUTORIP_DIR", &d);
        }
        // Plant a file at logs/rips so the dir can't be created.
        std::fs::write(d.join("logs").join("rips"), b"not a dir").unwrap();

        let dev = format!("test_fail_{}", std::process::id());
        device_log(&dev, "live line");
        assert!(!get_device_log(&dev, 100).is_empty());
        let live = device_log_path(&dev);
        assert!(std::path::Path::new(&live).exists());

        archive_device_log(&dev);

        // Ring preserved (archive failed), live file still on disk.
        assert!(
            !get_device_log(&dev, 100).is_empty(),
            "in-memory ring must be kept when archive rename/setup fails"
        );
        assert!(
            std::path::Path::new(&live).exists(),
            "live log file must remain on disk after a failed archive"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn sanitize_device_neutralizes_traversal() {
        // The hard invariant at the construction point: a device with a
        // path separator or traversal sequence can't escape logs/.
        for bad in ["../etc/passwd", "a/b", "..", "", ".", "x\\y"] {
            let path = format!("{}/logs/device_{}.log", "/cfg", super::sanitize_device(bad));
            assert!(
                !path.contains(".."),
                "path must not contain traversal for {bad:?}: {path}"
            );
            assert_eq!(
                path.matches("/logs/device_").count(),
                1,
                "device must be a single component for {bad:?}: {path}"
            );
            // The component after device_ must not introduce a new dir.
            assert!(!path.contains("device_../") && !path.contains("device_a/b"));
        }
        // A normal device name passes through unchanged.
        assert_eq!(super::sanitize_device("sg0"), "sg0");
    }

    #[test]
    fn get_device_log_respects_line_limit() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let d = tmpdir("line_limit");
        unsafe {
            std::env::set_var("AUTORIP_DIR", &d);
        }
        let dev = format!("test_lim_{}", std::process::id());
        for i in 0..5 {
            device_log(&dev, &format!("line {i}"));
        }
        let lines = get_device_log(&dev, 3);
        assert_eq!(lines.len(), 3);
        // Tail of the buffer — last 3 lines are 2, 3, 4.
        assert!(lines[2].contains("line 4"));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn ring_evicts_oldest_past_cap() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let d = tmpdir("ring_cap");
        unsafe {
            std::env::set_var("AUTORIP_DIR", &d);
        }
        let dev = format!("test_ring_{}", std::process::id());
        for i in 0..(RING_CAP + 50) {
            device_log(&dev, &format!("line {i}"));
        }
        // Ring is capped: asking for more than the cap returns at most cap.
        let all = get_device_log(&dev, RING_CAP + 1000);
        assert_eq!(all.len(), RING_CAP, "ring must be capped at RING_CAP");
        // Oldest lines evicted: the first retained line is line 50.
        assert!(all[0].contains("line 50"), "got: {}", all[0]);
        assert!(all[RING_CAP - 1].contains(&format!("line {}", RING_CAP + 49)));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn rotate_system_log_archives_only_when_large() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let d = tmpdir("sys_rotate");
        unsafe {
            std::env::set_var("AUTORIP_DIR", &d);
        }
        // Small system log: must NOT rotate.
        syslog("small system message");
        rotate_system_log_if_large();
        assert!(
            std::path::Path::new(&device_log_path("system")).exists(),
            "small system log must be left in place"
        );

        // Grow it past the threshold, then rotate.
        let big = "x".repeat((SYSTEM_LOG_ROTATE_BYTES + 1024) as usize);
        std::fs::write(device_log_path("system"), big).unwrap();
        rotate_system_log_if_large();
        assert!(
            !std::path::Path::new(&device_log_path("system")).exists(),
            "oversized system log must be rotated out"
        );
        let rips_dir = d.join("logs").join("rips");
        let archived: Vec<_> = std::fs::read_dir(&rips_dir)
            .unwrap()
            .filter_map(|r| r.ok())
            .filter(|e| e.file_name().to_string_lossy().contains("system"))
            .collect();
        assert_eq!(archived.len(), 1, "expected one archived system log");
        let _ = std::fs::remove_dir_all(&d);
    }
}
