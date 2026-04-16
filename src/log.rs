use std::collections::HashMap;
use std::io::Write;
use std::sync::Mutex;

static LOGS: once_cell::sync::Lazy<Mutex<HashMap<String, Vec<String>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

/// Log a message for a specific device. Stored in memory + written to file.
pub fn device_log(device: &str, msg: &str) {
    let timestamp = crate::util::epoch_secs();
    let line = format!("[{}] {}", timestamp, msg);

    // In-memory buffer (last 500 lines per device)
    if let Ok(mut logs) = LOGS.lock() {
        let log = logs.entry(device.to_string()).or_insert_with(Vec::new);
        log.push(line.clone());
        if log.len() > 500 {
            log.remove(0);
        }
    }

    // File log
    let log_dir = std::env::var("AUTORIP_DIR").unwrap_or_else(|_| "/config".to_string());
    let log_path = format!("{}/logs/device_{}.log", log_dir, device);
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
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

/// Log to system log (not device-specific).
pub fn syslog(msg: &str) {
    device_log("system", msg);
}

