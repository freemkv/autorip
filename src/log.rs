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

/// Log a message for a specific device. Stored in memory + written to file + stderr.
///
/// Line format: `[YYYY-MM-DDTHH:MM:SSZ] msg`. Full ISO-8601 timestamps are
/// intentional — wall-clock-only `[HH:MM:SS]` breaks across midnight and
/// makes archived rip logs ambiguous.
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

    // File log
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(device_log_path(device))
    {
        let _ = writeln!(f, "{}", line);
    }

    eprintln!("[{}] {}", device, msg);
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
            eprintln!("[{}] log archive: create {}: {}", device, rips_dir, e);
        } else {
            let archive = format!(
                "{}/{}_{}.log",
                rips_dir,
                device,
                crate::util::format_iso_datetime_filename(),
            );
            if let Err(e) = std::fs::rename(&current, &archive) {
                eprintln!(
                    "[{}] log archive: rename {} -> {}: {}",
                    device, current, archive, e
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
