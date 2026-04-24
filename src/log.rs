use std::collections::HashMap;
use std::io::Write;
use std::sync::Mutex;

static LOGS: once_cell::sync::Lazy<Mutex<HashMap<String, Vec<String>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

fn log_dir() -> String {
    std::env::var("AUTORIP_DIR").unwrap_or_else(|_| "/config".to_string())
}

fn device_log_path(device: &str) -> String {
    format!("{}/logs/device_{}.log", log_dir(), device)
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

    // In-memory buffer (last 500 lines per device)
    if let Ok(mut logs) = LOGS.lock() {
        let log = logs.entry(device.to_string()).or_insert_with(Vec::new);
        log.push(line.clone());
        if log.len() > 500 {
            log.remove(0);
        }
    }

    // File log — per-device, append-only between archive points.
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(device_log_path(device))
    {
        let _ = writeln!(f, "{}", line);
    }

    // Structured event into the central log stream. The `device` field is
    // what makes `jq 'select(.fields.device == "sg4")' autorip.jsonl`
    // possible — the per-device files don't carry that grouping in a single
    // file the way the JSONL stream does.
    tracing::info!(device = %device, "{}", msg);
}

/// Get recent log lines for a device.
pub fn get_device_log(device: &str, lines: usize) -> Vec<String> {
    LOGS.lock()
        .ok()
        .and_then(|logs| logs.get(device).cloned())
        .map(|log| {
            log.into_iter()
                .rev()
                .take(lines)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect()
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
                device,
                crate::util::format_iso_datetime_filename(),
            );
            if let Err(e) = std::fs::rename(&current, &archive) {
                tracing::warn!(
                    device = %device,
                    src = %current,
                    dst = %archive,
                    error = %e,
                    "log archive: rename failed"
                );
            }
        }
    }

    if let Ok(mut logs) = LOGS.lock() {
        logs.remove(device);
    }
}

/// Log to system log (not device-specific).
pub fn syslog(msg: &str) {
    device_log("system", msg);
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
        let d = std::env::temp_dir().join(format!(
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
}
