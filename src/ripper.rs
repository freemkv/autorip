use libfreemkv::pes::Stream as PesStream;
use libfreemkv::{Disc, DiscFormat, Drive, ScanOptions};
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

/// Poll drives for disc insertion.
pub fn drive_poll_loop(cfg: &Arc<RwLock<Config>>) {
    while !crate::SHUTDOWN.load(std::sync::atomic::Ordering::Relaxed) {
        {
            let drives = libfreemkv::find_drives();
            for mut drive in drives {
                let path = drive.device_path().to_string();
                let device = path.rsplit('/').next().unwrap_or(&path).to_string();
                if drive.drive_status() == libfreemkv::DriveStatus::DiscPresent
                    && !is_ripping(&device)
                    && !is_in_cooldown(&device)
                {
                    let on_insert = cfg
                        .read()
                        .ok()
                        .map(|c| c.on_insert.clone())
                        .unwrap_or_else(|| "rip".to_string());

                    if on_insert == "nothing" {
                        continue;
                    }

                    let cfg = cfg.clone();
                    std::thread::spawn(move || {
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            rip_disc(&cfg, &device, &mut drive);
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
                            drive.unlock_tray();
                        }
                    });
                }
            }
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

/// Rip a disc from start to finish using the PES pipeline.
fn rip_disc(cfg: &Arc<RwLock<Config>>, device: &str, session: &mut Drive) {
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

    // Init (unlock + firmware)
    let _ = session.wait_ready();
    let _ = session.init();
    let _ = session.probe_disc();

    // Scan
    let scan_opts = match &cfg_read.keydb_path {
        Some(p) => ScanOptions::with_keydb(p),
        None => ScanOptions::default(),
    };
    let disc = match Disc::scan(session, &scan_opts) {
        Ok(d) => d,
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

    crate::log::device_log(
        device,
        &format!(
            "Disc: {} ({}, {} titles)",
            disc_name,
            disc_format,
            disc.titles.len()
        ),
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

    // If on_insert is "identify", stop after scanning
    let on_insert = cfg_read.on_insert.clone();
    if on_insert == "identify" {
        update_state(
            device,
            RipState {
                device: device.to_string(),
                status: "done".to_string(),
                disc_name: disc_name.clone(),
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

    // Select titles
    let titles_to_rip: Vec<usize> = if cfg_read.main_feature {
        if disc.titles.is_empty() {
            vec![]
        } else {
            vec![0] // longest title (already sorted by duration)
        }
    } else {
        (0..disc.titles.len())
            .filter(|&i| disc.titles[i].duration_secs >= cfg_read.min_length_secs as f64)
            .collect()
    };

    if titles_to_rip.is_empty() {
        let msg = "No titles found matching criteria".to_string();
        crate::log::device_log(device, &msg);
        update_state(
            device,
            RipState {
                device: device.to_string(),
                status: "error".to_string(),
                disc_name: disc_name.clone(),
                last_error: msg,
                ..Default::default()
            },
        );
        return;
    }

    let output_format = cfg_read.output_format.clone();

    crate::log::device_log(
        device,
        &format!(
            "Ripping {} title(s) to {}",
            titles_to_rip.len(),
            output_format.to_uppercase()
        ),
    );

    // Lock tray during rip
    session.lock_tray();

    let staging = cfg_read.staging_device_dir(&sanitize_filename(&disc_name));
    let _ = std::fs::create_dir_all(&staging);

    let device_path = session.device_path().to_string();

    // ISO output: raw sector copy (not PES pipeline)
    if output_format == "iso" {
        let iso_path = format!("{}/{}.iso", staging, sanitize_filename(&disc_name));
        update_state(
            device,
            RipState {
                device: device.to_string(),
                status: "ripping".to_string(),
                disc_name: disc_name.clone(),
                disc_format: disc_format.clone(),
                output_file: iso_path.clone(),
                tmdb_title: tmdb_title.clone(),
                tmdb_year,
                tmdb_poster: tmdb_poster.clone(),
                tmdb_overview: tmdb_overview.clone(),
                ..Default::default()
            },
        );

        let total_bytes = disc.capacity_sectors as u64 * 2048;
        let start = std::time::Instant::now();
        let device_str = device.to_string();
        let disc_name_c = disc_name.clone();
        let disc_format_c = disc_format.clone();
        let tmdb_title_c = tmdb_title.clone();
        let tmdb_poster_c = tmdb_poster.clone();
        let tmdb_overview_c = tmdb_overview.clone();

        let batch = libfreemkv::disc::detect_max_batch_sectors(&device_path);
        let progress = |done: u64, total: u64| {
            let elapsed = start.elapsed().as_secs_f64();
            let pct = if total > 0 {
                (done * 100 / total).min(100) as u8
            } else {
                0
            };
            let speed = if elapsed > 0.0 {
                done as f64 / (1024.0 * 1024.0) / elapsed
            } else {
                0.0
            };
            let eta = if speed > 0.0 && total > done {
                let remaining = (total - done) as f64 / (1024.0 * 1024.0) / speed;
                format!(
                    "{}:{:02}",
                    (remaining / 60.0) as u32,
                    (remaining % 60.0) as u32
                )
            } else {
                String::new()
            };
            update_state(
                &device_str,
                RipState {
                    device: device_str.clone(),
                    status: "ripping".to_string(),
                    disc_name: disc_name_c.clone(),
                    disc_format: disc_format_c.clone(),
                    progress_pct: pct,
                    speed_mbs: speed,
                    eta,
                    tmdb_title: tmdb_title_c.clone(),
                    tmdb_year,
                    tmdb_poster: tmdb_poster_c.clone(),
                    tmdb_overview: tmdb_overview_c.clone(),
                    ..Default::default()
                },
            );
        };

        let decrypt = true; // always decrypt for ISO
        let resume = true;
        match disc.copy(
            session,
            std::path::Path::new(&iso_path),
            decrypt,
            resume,
            Some(batch),
            Some(&progress),
        ) {
            Ok(()) => {
                let elapsed = start.elapsed().as_secs_f64();
                let speed = if elapsed > 0.0 {
                    total_bytes as f64 / (1024.0 * 1024.0) / elapsed
                } else {
                    0.0
                };
                crate::log::device_log(
                    device,
                    &format!(
                        "ISO complete: {:.1} GB in {:.0}s ({:.0} MB/s)",
                        total_bytes as f64 / 1_073_741_824.0,
                        elapsed,
                        speed
                    ),
                );
            }
            Err(e) => {
                let msg = format!("ISO rip failed: {}", e);
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
                session.unlock_tray();
                return;
            }
        }
    } else {
        // MKV, M2TS, or network output: PES pipeline
        let ext = match output_format.as_str() {
            "m2ts" => "m2ts",
            "network" => "mkv", // network streams MKV data
            _ => "mkv",
        };

        // Rip each title using the PES pipeline
        for &title_idx in &titles_to_rip {
            let _title = &disc.titles[title_idx];
            let filename = if titles_to_rip.len() == 1 {
                format!("{}.{}", sanitize_filename(&disc_name), ext)
            } else {
                format!(
                    "{}_t{:02}.{}",
                    sanitize_filename(&disc_name),
                    title_idx + 1,
                    ext
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
                    tmdb_title: tmdb_title.clone(),
                    tmdb_year,
                    tmdb_poster: tmdb_poster.clone(),
                    tmdb_overview: tmdb_overview.clone(),
                    ..Default::default()
                },
            );

            let source_url = format!("disc://{}", device_path);
            let dest_url = if output_format == "network" && !cfg_read.network_target.is_empty() {
                format!("network://{}", cfg_read.network_target)
            } else {
                format!("{}://{}", ext, output_path)
            };
            let opts = libfreemkv::InputOptions {
                keydb_path: cfg_read.keydb_path.clone(),
                title_index: Some(title_idx),
                raw: false,
            };

            // Open input (PES stream from disc)
            let mut input = match libfreemkv::input(&source_url, &opts) {
                Ok(s) => s,
                Err(e) => {
                    let msg = format!("Open input failed: {}", e);
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
                    session.unlock_tray();
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

            let info = input.info().clone();
            let mut out_title = info.clone();
            out_title.codec_privates = (0..info.streams.len())
                .map(|i| input.codec_private(i))
                .collect();

            // Open output (MKV file)
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
                    session.unlock_tray();
                    return;
                }
            };
            let mut output = libfreemkv::pes::CountingStream::new(raw_output);

            let total_bytes = info.size_bytes;
            let start = std::time::Instant::now();

            // Write buffered frames
            for frame in &buffered {
                if output.write(frame).is_err() {
                    break;
                }
            }

            // Stream remaining frames
            loop {
                match input.read() {
                    Ok(Some(frame)) => {
                        if output.write(&frame).is_err() {
                            break;
                        }

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
                            let remaining =
                                (total_bytes - bytes_done) as f64 / (1024.0 * 1024.0) / speed;
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
                    "Title {} complete: {:.1} GB in {:.0}s ({:.0} MB/s)",
                    title_idx + 1,
                    bytes_done as f64 / 1_073_741_824.0,
                    elapsed,
                    speed
                ),
            );
        }
    } // end else (MKV/M2TS branch)

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

    // Unlock tray; auto-eject if configured
    session.unlock_tray();
    if cfg_read.auto_eject {
        let _ = session.eject();
    }

    crate::log::device_log(device, "Rip complete");

    // Webhook
    crate::webhook::send(&cfg_read, "rip_complete", &disc_name, &staging);
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
