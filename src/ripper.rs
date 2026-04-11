use libfreemkv::{Disc, DiscFormat, DriveSession, IOStream, MkvStream, ScanOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::config::Config;

/// State broadcast for web UI.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RipState {
    pub device: String,
    pub status: String, // "idle", "scanning", "ripping", "moving", "done", "error"
    pub disc_name: String,
    pub disc_format: String, // "uhd", "bluray", "dvd"
    pub progress_pct: u8,
    pub speed_mbs: f64,
    pub eta: String,
    pub errors: u32,
    pub last_error: String,
    pub output_file: String,
    pub tmdb_title: String,
    pub tmdb_year: u16,
    pub tmdb_poster: String,
    pub tmdb_overview: String,
}

impl Default for RipState {
    fn default() -> Self {
        Self {
            device: String::new(),
            status: "idle".to_string(),
            disc_name: String::new(),
            disc_format: String::new(),
            progress_pct: 0,
            speed_mbs: 0.0,
            eta: String::new(),
            errors: 0,
            last_error: String::new(),
            output_file: String::new(),
            tmdb_title: String::new(),
            tmdb_year: 0,
            tmdb_poster: String::new(),
            tmdb_overview: String::new(),
        }
    }
}

// Global state for web UI.
pub static STATE: once_cell::sync::Lazy<
    std::sync::Mutex<std::collections::HashMap<String, RipState>>,
> = once_cell::sync::Lazy::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// Stop cooldowns: device -> epoch seconds when cooldown expires.
/// After a manual stop, suppress auto-rip for STOP_COOLDOWN_SECS.
pub static STOP_COOLDOWNS: once_cell::sync::Lazy<
    std::sync::Mutex<std::collections::HashMap<String, u64>>,
> = once_cell::sync::Lazy::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

const STOP_COOLDOWN_SECS: u64 = 15;

/// Record a stop cooldown for the given device.
pub fn set_stop_cooldown(device: &str) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if let Ok(mut cd) = STOP_COOLDOWNS.lock() {
        cd.insert(device.to_string(), now + STOP_COOLDOWN_SECS);
    }
}

/// Check if a device is within its stop cooldown period.
fn is_in_cooldown(device: &str) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if let Ok(cd) = STOP_COOLDOWNS.lock() {
        if let Some(&expires) = cd.get(device) {
            return now < expires;
        }
    }
    false
}

/// Poll drives for disc insertion.
pub fn drive_poll_loop(cfg: &Arc<RwLock<Config>>) {
    loop {
        let drives = libfreemkv::find_drives();
        for (path, _id) in &drives {
            let device = path.rsplit('/').next().unwrap_or(path);
            if has_disc(path) && !is_ripping(device) && !is_in_cooldown(device) {
                let cfg = cfg.clone();
                let device = device.to_string();
                let path = path.clone();
                std::thread::spawn(move || {
                    rip_disc(&cfg, &device, &path);
                });
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}

fn has_disc(device_path: &str) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        if let Ok(f) = std::fs::File::open(device_path) {
            let fd = f.as_raw_fd();
            // CDROM_DRIVE_STATUS = 0x5326, CDS_DISC_OK = 4
            let status = unsafe { libc::ioctl(fd, 0x5326) };
            return status == 4;
        }
        false
    }
    #[cfg(not(target_os = "linux"))]
    {
        Path::new(device_path).exists()
    }
}

fn is_ripping(device: &str) -> bool {
    STATE
        .lock()
        .map(|s| {
            s.get(device)
                .map(|r| r.status == "scanning" || r.status == "ripping")
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

pub fn update_state(device: &str, state: RipState) {
    if let Ok(mut s) = STATE.lock() {
        s.insert(device.to_string(), state);
    }
}

/// Rip a disc from start to finish.
fn rip_disc(cfg: &Arc<RwLock<Config>>, device: &str, device_path: &str) {
    let cfg_read = cfg.read().unwrap();

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "scanning".to_string(),
            ..Default::default()
        },
    );

    // Open drive
    let mut session = match DriveSession::open(Path::new(device_path)) {
        Ok(s) => s,
        Err(e) => {
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "error".to_string(),
                    last_error: e.to_string(),
                    ..Default::default()
                },
            );
            return;
        }
    };

    // Init (unlock + firmware)
    let _ = session.wait_ready();
    let _ = session.init();
    let _ = session.probe_disc();

    // Scan
    let scan_opts = match &cfg_read.keydb_path {
        Some(p) => ScanOptions::with_keydb(p),
        None => ScanOptions::default(),
    };
    let disc = match Disc::scan(&mut session, &scan_opts) {
        Ok(d) => d,
        Err(e) => {
            update_state(
                device,
                RipState {
                    device: device.to_string(),
                    status: "error".to_string(),
                    last_error: format!("Scan failed: {}", e),
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
    let disc_format = match disc.format {
        DiscFormat::Uhd => "uhd",
        DiscFormat::BluRay => "bluray",
        DiscFormat::Dvd => "dvd",
        DiscFormat::Unknown => "unknown",
    }
    .to_string();

    // TMDB lookup
    let tmdb = crate::tmdb::lookup(
        &crate::tmdb::clean_title(&disc_name),
        &cfg_read.tmdb_api_key,
    );
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

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "scanning".to_string(),
            disc_name: disc_name.clone(),
            disc_format: disc_format.clone(),
            tmdb_title: tmdb_title.clone(),
            tmdb_year,
            tmdb_poster: tmdb_poster.clone(),
            tmdb_overview: tmdb_overview.clone(),
            ..Default::default()
        },
    );

    // Select titles
    let titles_to_rip: Vec<usize> = if cfg_read.main_feature {
        vec![0] // longest title (already sorted)
    } else {
        (0..disc.titles.len())
            .filter(|&i| disc.titles[i].duration_secs >= cfg_read.min_length_secs as f64)
            .collect()
    };

    if titles_to_rip.is_empty() {
        update_state(
            device,
            RipState {
                device: device.to_string(),
                status: "error".to_string(),
                disc_name: disc_name.clone(),
                last_error: "No titles found".to_string(),
                ..Default::default()
            },
        );
        return;
    }

    // Rip each title
    let staging = format!(
        "{}/{}",
        cfg_read.staging_dir,
        sanitize_filename(&disc_name)
    );
    let _ = std::fs::create_dir_all(&staging);

    for &title_idx in &titles_to_rip {
        let title = &disc.titles[title_idx];
        let filename = if titles_to_rip.len() == 1 {
            format!("{}.mkv", sanitize_filename(&disc_name))
        } else {
            format!(
                "{}_t{:02}.mkv",
                sanitize_filename(&disc_name),
                title_idx + 1
            )
        };
        let output_path = format!("{}/{}", staging, filename);

        update_state(
            device,
            RipState {
                device: device.to_string(),
                status: "ripping".to_string(),
                disc_name: disc_name.clone(),
                disc_format: disc_format.clone(),
                output_file: filename.clone(),
                ..Default::default()
            },
        );

        // Create MKV output
        let file = match std::fs::File::create(&output_path) {
            Ok(f) => f,
            Err(e) => {
                update_state(
                    device,
                    RipState {
                        device: device.to_string(),
                        status: "error".to_string(),
                        last_error: format!("Cannot create output: {}", e),
                        ..Default::default()
                    },
                );
                return;
            }
        };

        let buf_writer = std::io::BufWriter::with_capacity(4 * 1024 * 1024, file);
        let mut mkv = MkvStream::new(buf_writer)
            .meta(title)
            .max_buffer(10 * 1024 * 1024);

        // Open title for reading
        let mut reader = match disc.open_title(&mut session, title_idx) {
            Ok(r) => r,
            Err(e) => {
                update_state(
                    device,
                    RipState {
                        device: device.to_string(),
                        status: "error".to_string(),
                        last_error: format!("Open title failed: {}", e),
                        ..Default::default()
                    },
                );
                return;
            }
        };

        let total_bytes = title.size_bytes;
        let mut bytes_written: u64 = 0;
        let start = std::time::Instant::now();

        loop {
            match reader.read_batch() {
                Ok(Some(batch)) => {
                    if mkv.write_all(batch).is_err() {
                        break;
                    }
                    bytes_written += batch.len() as u64;

                    let elapsed = start.elapsed().as_secs_f64();
                    let pct = if total_bytes > 0 {
                        (bytes_written * 100 / total_bytes) as u8
                    } else {
                        0
                    };
                    let speed = if elapsed > 0.0 {
                        bytes_written as f64 / (1024.0 * 1024.0) / elapsed
                    } else {
                        0.0
                    };
                    let eta = if speed > 0.0 && total_bytes > 0 {
                        let remaining =
                            (total_bytes - bytes_written) as f64 / (1024.0 * 1024.0) / speed;
                        format!(
                            "{}:{:02}",
                            (remaining / 60.0) as u32,
                            (remaining % 60.0) as u32
                        )
                    } else {
                        String::new()
                    };

                    update_state(
                        device,
                        RipState {
                            device: device.to_string(),
                            status: "ripping".to_string(),
                            disc_name: disc_name.clone(),
                            disc_format: disc_format.clone(),
                            progress_pct: pct,
                            speed_mbs: speed,
                            eta,
                            errors: reader.errors,
                            last_error: String::new(),
                            output_file: filename.clone(),
                            tmdb_title: tmdb_title.clone(),
                            tmdb_year,
                            tmdb_poster: tmdb_poster.clone(),
                            tmdb_overview: tmdb_overview.clone(),
                        },
                    );

                    // Abort on error if configured
                    if cfg_read.abort_on_error && reader.errors > 0 {
                        update_state(
                            device,
                            RipState {
                                device: device.to_string(),
                                status: "error".to_string(),
                                disc_name: disc_name.clone(),
                                disc_format: disc_format.clone(),
                                errors: reader.errors,
                                last_error: "Aborted due to read errors".to_string(),
                                tmdb_title: tmdb_title.clone(),
                                tmdb_year,
                                tmdb_poster: tmdb_poster.clone(),
                                tmdb_overview: tmdb_overview.clone(),
                                ..Default::default()
                            },
                        );
                        return;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    crate::log::device_log(device, &format!("Read error: {}", e));
                    break;
                }
            }
        }

        let _ = mkv.finish();
    }

    // Record history
    {
        let title = if tmdb_title.is_empty() {
            disc_name.clone()
        } else {
            tmdb_title.clone()
        };
        let entry = serde_json::json!({
            "title": title,
            "disc_name": disc_name,
            "format": disc_format,
            "year": tmdb_year,
            "media_type": tmdb.as_ref().map(|t| t.media_type.as_str()).unwrap_or("unknown"),
            "poster_url": tmdb_poster,
            "overview": tmdb_overview,
            "staging_dir": staging,
            "date": format_epoch_date(),
        });
        crate::history::record(&cfg_read.history_dir(), &entry);
    }

    // Done
    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "done".to_string(),
            disc_name: disc_name.clone(),
            disc_format: disc_format.clone(),
            progress_pct: 100,
            output_file: staging.clone(),
            tmdb_title: tmdb_title.clone(),
            tmdb_year,
            tmdb_poster: tmdb_poster.clone(),
            tmdb_overview: tmdb_overview.clone(),
            ..Default::default()
        },
    );

    // Eject
    if cfg_read.auto_eject {
        let _ = session.eject();
    }

    // Notify
    crate::webhook::send(&cfg_read, "rip_complete", &disc_name, &staging);
}

fn format_epoch_date() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = (secs / 86400) as i64;
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == ' ' || *c == '-' || *c == '_' || *c == '.')
        .collect::<String>()
        .trim()
        .replace(' ', "_")
}
