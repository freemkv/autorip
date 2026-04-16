use libfreemkv::pes::Stream as PesStream;

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
pub static STOP_COOLDOWNS: once_cell::sync::Lazy<
    std::sync::Mutex<std::collections::HashMap<String, u64>>,
> = once_cell::sync::Lazy::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

const STOP_COOLDOWN_SECS: u64 = 15;

pub fn set_stop_cooldown(device: &str) {
    let now = crate::util::epoch_secs();
    if let Ok(mut cd) = STOP_COOLDOWNS.lock() {
        cd.insert(device.to_string(), now + STOP_COOLDOWN_SECS);
    }
}

fn is_in_cooldown(device: &str) -> bool {
    let now = crate::util::epoch_secs();
    if let Ok(cd) = STOP_COOLDOWNS.lock() {
        if let Some(&expires) = cd.get(device) {
            return now < expires;
        }
    }
    false
}

/// Poll drives for disc insertion. Only triggers on state change
/// (no disc → disc present), not on disc already being there.
pub fn drive_poll_loop(cfg: &Arc<RwLock<Config>>) {
    // Track which devices had a disc on last poll
    let mut had_disc: std::collections::HashSet<String> = std::collections::HashSet::new();

    while !crate::SHUTDOWN.load(std::sync::atomic::Ordering::Relaxed) {
        {
            // Scan /dev/sg* without opening — just check existence
            let mut current_with_disc: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            for i in 0..16u8 {
                let path = format!("/dev/sg{}", i);
                if !std::path::Path::new(&path).exists() {
                    continue;
                }
                let device = format!("sg{}", i);

                // Don't touch drives that are actively ripping
                if is_ripping(&device) {
                    current_with_disc.insert(device);
                    continue;
                }

                // Open briefly to check status, then drop immediately
                let mut drive = match libfreemkv::Drive::open(std::path::Path::new(&path)) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let disc_present = drive.drive_status() == libfreemkv::DriveStatus::DiscPresent;
                drop(drive); // close fd immediately

                // Always show drive in state (idle if no disc)
                if !disc_present {
                    if !is_ripping(&device) {
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

                // Only auto-trigger on NEW insertion (wasn't present last poll)
                let is_new_insert = !had_disc.contains(&device);

                if is_new_insert && !is_ripping(&device) && !is_in_cooldown(&device) {
                    let on_insert = cfg
                        .read()
                        .ok()
                        .map(|c| c.on_insert.clone())
                        .unwrap_or_else(|| "rip".to_string());

                    if on_insert == "nothing" {
                        // Show disc info but don't act
                        update_state(
                            &device,
                            RipState {
                                device: device.clone(),
                                status: "idle".to_string(),
                                ..Default::default()
                            },
                        );
                        continue;
                    }

                    let cfg = cfg.clone();
                    let dev_path = path.clone();
                    // Drop the drive handle — rip_disc will open its own via input()
                    drop(drive);
                    std::thread::spawn(move || {
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            rip_disc(&cfg, &device, &dev_path);
                        }))
                        .is_err()
                        {
                            crate::log::device_log(&device, "Rip thread panicked");
                            update_state(
                                &device,
                                RipState {
                                    device: device.clone(),
                                    status: "error".to_string(),
                                    last_error: "Internal error (panic)".to_string(),
                                    ..Default::default()
                                },
                            );
                        }
                    });
                }
            }

            had_disc = current_with_disc;
        }
        std::thread::sleep(std::time::Duration::from_secs(5));
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
/// Mirrors the freemkv CLI pipe() pattern: input() → headers → output() → frame loop.
/// Only opens the drive once via input().
fn rip_disc(cfg: &Arc<RwLock<Config>>, device: &str, device_path: &str) {
    let cfg_read = match cfg.read() {
        Ok(c) => c,
        Err(_) => return,
    };

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "scanning".to_string(),
            ..Default::default()
        },
    );

    crate::log::device_log(device, "Disc detected, scanning...");

    // Open input — this handles init, scan, decrypt, demux in one call.
    // Exactly like the freemkv CLI does it.
    let source_url = format!("disc://{}", device_path);
    let opts = libfreemkv::InputOptions {
        keydb_path: cfg_read.keydb_path.clone(),
        title_index: Some(0), // main title (longest, already sorted)
        raw: false,
    };

    let mut input = match libfreemkv::input(&source_url, &opts) {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("Scan failed: {}", e);
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

    // Read frames until codec headers are ready
    let mut buffered = Vec::new();
    while !input.headers_ready() {
        match input.read() {
            Ok(Some(frame)) => buffered.push(frame),
            Ok(None) => break,
            Err(e) => {
                crate::log::device_log(device, &format!("Header scan error: {}", e));
                break;
            }
        }
    }

    // Get disc info from the stream (populated by input())
    let info = input.info().clone();
    let disc_name = if info.playlist.is_empty() {
        "Unknown".to_string()
    } else {
        sanitize_filename(&info.playlist)
    };
    let disc_format = "disc".to_string(); // format detection from info

    crate::log::device_log(
        device,
        &format!("Disc: {} ({} streams)", disc_name, info.streams.len()),
    );

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

    let display_name = if tmdb_title.is_empty() {
        disc_name.clone()
    } else {
        tmdb_title.clone()
    };

    // If on_insert is "identify", stop after scanning
    if cfg_read.on_insert == "identify" {
        update_state(
            device,
            RipState {
                device: device.to_string(),
                status: "done".to_string(),
                disc_name: display_name,
                disc_format: disc_format.clone(),
                tmdb_title: tmdb_title.clone(),
                tmdb_year,
                tmdb_poster: tmdb_poster.clone(),
                tmdb_overview: tmdb_overview.clone(),
                ..Default::default()
            },
        );
        return;
    }

    let output_format = cfg_read.output_format.clone();
    let ext = match output_format.as_str() {
        "m2ts" => "m2ts",
        _ => "mkv",
    };

    let staging = cfg_read.staging_device_dir(&sanitize_filename(&display_name));
    let _ = std::fs::create_dir_all(&staging);
    let filename = format!("{}.{}", sanitize_filename(&display_name), ext);
    let output_path = format!("{}/{}", staging, filename);
    let dest_url = if output_format == "network" && !cfg_read.network_target.is_empty() {
        format!("network://{}", cfg_read.network_target)
    } else {
        format!("{}://{}", ext, output_path)
    };

    crate::log::device_log(device, &format!("Ripping to {}", filename));

    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "ripping".to_string(),
            disc_name: display_name.clone(),
            disc_format: disc_format.clone(),
            output_file: filename.clone(),
            tmdb_title: tmdb_title.clone(),
            tmdb_year,
            tmdb_poster: tmdb_poster.clone(),
            tmdb_overview: tmdb_overview.clone(),
            ..Default::default()
        },
    );

    // Build output title with codec_privates
    let mut out_title = info.clone();
    out_title.codec_privates = (0..info.streams.len())
        .map(|i| input.codec_private(i))
        .collect();

    // Open output
    let raw_output = match libfreemkv::output(&dest_url, &out_title) {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("Open output failed: {}", e);
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
    let mut output = libfreemkv::pes::CountingStream::new(raw_output);

    let total_bytes = info.size_bytes;
    let start = std::time::Instant::now();
    let mut last_update = start;
    let mut last_log = start;

    // Write buffered frames
    for frame in &buffered {
        if output.write(frame).is_err() {
            break;
        }
    }

    // Stream remaining frames — same loop as freemkv CLI
    loop {
        match input.read() {
            Ok(Some(frame)) => {
                if output.write(&frame).is_err() {
                    break;
                }

                let now = std::time::Instant::now();
                if now.duration_since(last_update).as_secs_f64() < 1.0 {
                    continue;
                }
                last_update = now;

                let bytes_done = output.bytes_written();
                let elapsed = start.elapsed().as_secs_f64();
                let pct = if total_bytes > 0 {
                    (bytes_done * 100 / total_bytes).min(100) as u8
                } else {
                    0
                };
                let speed = if elapsed > 0.0 {
                    bytes_done as f64 / (1024.0 * 1024.0) / elapsed
                } else {
                    0.0
                };
                let eta = if speed > 0.0 && total_bytes > bytes_done {
                    let remaining = (total_bytes - bytes_done) as f64 / (1024.0 * 1024.0) / speed;
                    format!(
                        "{}:{:02}",
                        (remaining / 60.0) as u32,
                        (remaining % 60.0) as u32
                    )
                } else {
                    String::new()
                };

                // Log every 60 seconds
                if now.duration_since(last_log).as_secs() >= 60 {
                    last_log = now;
                    let gb = bytes_done as f64 / 1_073_741_824.0;
                    if total_bytes > 0 {
                        let total_gb = total_bytes as f64 / 1_073_741_824.0;
                        crate::log::device_log(
                            device,
                            &format!(
                                "{:.1} GB / {:.1} GB ({}%) {:.1} MB/s ETA {}",
                                gb, total_gb, pct, speed, eta
                            ),
                        );
                    } else {
                        crate::log::device_log(device, &format!("{:.1} GB {:.1} MB/s", gb, speed));
                    }
                }

                update_state(
                    device,
                    RipState {
                        device: device.to_string(),
                        status: "ripping".to_string(),
                        disc_name: display_name.clone(),
                        disc_format: disc_format.clone(),
                        progress_pct: pct,
                        speed_mbs: speed,
                        eta,
                        output_file: filename.clone(),
                        tmdb_title: tmdb_title.clone(),
                        tmdb_year,
                        tmdb_poster: tmdb_poster.clone(),
                        tmdb_overview: tmdb_overview.clone(),
                        ..Default::default()
                    },
                );
            }
            Ok(None) => break,
            Err(e) => {
                crate::log::device_log(device, &format!("Read error: {}", e));
                break;
            }
        }
    }

    let _ = output.finish();

    let bytes_done = output.bytes_written();
    let elapsed = start.elapsed().as_secs_f64();
    let speed = if elapsed > 0.0 {
        bytes_done as f64 / (1024.0 * 1024.0) / elapsed
    } else {
        0.0
    };
    crate::log::device_log(
        device,
        &format!(
            "Complete: {:.1} GB in {:.0}s ({:.0} MB/s)",
            bytes_done as f64 / 1_073_741_824.0,
            elapsed,
            speed
        ),
    );

    // Record history
    {
        let entry = serde_json::json!({
            "title": display_name,
            "disc_name": disc_name,
            "format": disc_format,
            "year": tmdb_year,
            "media_type": tmdb.as_ref().map(|t| t.media_type.as_str()).unwrap_or("unknown"),
            "poster_url": tmdb_poster,
            "overview": tmdb_overview,
            "staging_dir": staging,
            "date": crate::util::format_date(),
        });
        crate::history::record(&cfg_read.history_dir(), &entry);
    }

    // Done
    update_state(
        device,
        RipState {
            device: device.to_string(),
            status: "done".to_string(),
            disc_name: display_name.clone(),
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

    if cfg_read.auto_eject {
        eject_drive(device_path);
    }

    crate::log::device_log(device, "Rip complete");
    crate::webhook::send(&cfg_read, "rip_complete", &display_name, &staging);
}
pub fn eject_drive(device_path: &str) {
    if let Ok(mut session) = libfreemkv::Drive::open(std::path::Path::new(device_path)) {
        let _ = session.eject();
    }
}

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == ' ' || *c == '-' || *c == '_' || *c == '.')
        .collect::<String>()
        .trim()
        .replace(' ', "_")
}
