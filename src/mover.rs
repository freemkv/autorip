use crate::config::Config;
use crate::ripper;
use crate::tmdb;
use std::path::Path;
use std::sync::{Arc, RwLock};

pub fn run(cfg: &Arc<RwLock<Config>>) {
    loop {
        let cfg = match cfg.read() {
            Ok(c) => c,
            Err(_) => {
                std::thread::sleep(std::time::Duration::from_secs(10));
                continue;
            }
        };
        check_and_move(&cfg);
        drop(cfg);
        std::thread::sleep(std::time::Duration::from_secs(10));
    }
}

fn check_and_move(cfg: &Config) {
    // Scan staging directory for completed rips (directories with .done marker)
    let staging_root = &cfg.staging_dir;
    let entries = match std::fs::read_dir(staging_root) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.filter_map(|e| e.ok()) {
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }

        let marker_path = dir.join(".done");
        if !marker_path.exists() {
            continue;
        }

        // Read marker for TMDB metadata
        let marker: serde_json::Value = match std::fs::read_to_string(&marker_path) {
            Ok(data) => serde_json::from_str(&data).unwrap_or_default(),
            Err(_) => continue,
        };

        let disc_name = marker["disc_name"].as_str().unwrap_or("").to_string();
        let display_name = marker["title"].as_str().unwrap_or(&disc_name).to_string();
        let disc_format = marker["format"].as_str().unwrap_or("").to_string();

        // Build TMDB result from marker
        let tmdb_result = if !marker["title"].is_null() {
            Some(tmdb::TmdbResult {
                title: marker["title"].as_str().unwrap_or("").to_string(),
                year: marker["year"].as_u64().unwrap_or(0) as u16,
                poster_url: marker["poster_url"].as_str().unwrap_or("").to_string(),
                overview: marker["overview"].as_str().unwrap_or("").to_string(),
                media_type: marker["media_type"].as_str().unwrap_or("movie").to_string(),
            })
        } else {
            None
        };

        // Find ripped files
        let ripped_files: Vec<std::path::PathBuf> = match std::fs::read_dir(&dir) {
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.extension()
                        .map(|x| x == "mkv" || x == "m2ts" || x == "iso")
                        .unwrap_or(false)
                })
                .collect(),
            Err(_) => continue,
        };

        if ripped_files.is_empty() {
            continue;
        }

        let staging_dir = dir.to_string_lossy().to_string();
        crate::log::syslog(&format!("Moving: {} ({} files)", display_name, ripped_files.len()));

        // Update UI state to "moving" — find a device or use a synthetic key
        let device_key = find_device_for_staging(&staging_dir);
        if let Some(ref dev) = device_key {
            ripper::update_state(
                dev,
                ripper::RipState {
                    device: dev.clone(),
                    status: "moving".to_string(),
                    disc_name: display_name.clone(),
                    disc_format: disc_format.clone(),
                    progress_pct: 100,
                    tmdb_title: tmdb_result.as_ref().map(|t| t.title.clone()).unwrap_or_default(),
                    tmdb_year: tmdb_result.as_ref().map(|t| t.year).unwrap_or(0),
                    tmdb_poster: tmdb_result.as_ref().map(|t| t.poster_url.clone()).unwrap_or_default(),
                    tmdb_overview: tmdb_result.as_ref().map(|t| t.overview.clone()).unwrap_or_default(),
                    ..Default::default()
                },
            );
        }

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

        // Create destination directories
        let mut dest_ok = true;
        for (_, dest) in &planned_moves {
            if let Some(parent) = Path::new(dest).parent() {
                if std::fs::create_dir_all(parent).is_err() {
                    crate::log::syslog(&format!("Cannot create directory: {:?}", parent));
                    dest_ok = false;
                }
            }
        }
        if !dest_ok {
            continue;
        }

        // Move files
        let mut all_moved = true;
        for (src, dest) in &planned_moves {
            crate::log::syslog(&format!("Copying {} to {}", src.display(), dest));
            let dev_for_progress = device_key.clone();
            let name_for_progress = display_name.clone();
            let fmt_for_progress = disc_format.clone();
            let tmdb_for_progress = tmdb_result.clone();
            let on_progress = move |pct: u8, gb: f64, total_gb: f64, speed: f64| {
                if let Some(ref dev) = dev_for_progress {
                    ripper::update_state(
                        dev,
                        ripper::RipState {
                            device: dev.clone(),
                            status: "moving".to_string(),
                            disc_name: name_for_progress.clone(),
                            disc_format: fmt_for_progress.clone(),
                            progress_pct: pct,
                            progress_gb: gb,
                            speed_mbs: speed,
                            eta: if speed > 1.0 && total_gb > gb {
                                let secs = ((total_gb - gb) * 1024.0 / speed) as u32;
                                let m = secs / 60;
                                let s = secs % 60;
                                format!("{}:{:02}", m, s)
                            } else {
                                String::new()
                            },
                            tmdb_title: tmdb_for_progress.as_ref().map(|t| t.title.clone()).unwrap_or_default(),
                            tmdb_year: tmdb_for_progress.as_ref().map(|t| t.year).unwrap_or(0),
                            tmdb_poster: tmdb_for_progress.as_ref().map(|t| t.poster_url.clone()).unwrap_or_default(),
                            tmdb_overview: tmdb_for_progress.as_ref().map(|t| t.overview.clone()).unwrap_or_default(),
                            ..Default::default()
                        },
                    );
                }
            };
            if move_file(src, Path::new(dest), &on_progress) {
                crate::log::syslog(&format!("Moved to {}", dest));
            } else {
                crate::log::syslog(&format!("Failed to move {:?} to {}", src, dest));
                all_moved = false;
            }
        }

        if all_moved {
            // Remove staging directory (including .done marker)
            let _ = std::fs::remove_dir_all(&dir);
            crate::log::syslog(&format!("Move complete: {}", display_name));

            // Webhook: move_complete
            let dest_path = planned_moves.last().map(|(_, d)| d.as_str()).unwrap_or("");
            crate::webhook::send_move(cfg, &display_name, dest_path);

            if let Some(ref dev) = device_key {
                ripper::update_state(
                    dev,
                    ripper::RipState {
                        device: dev.clone(),
                        status: "idle".to_string(),
                        ..Default::default()
                    },
                );
            }
        }
    }
}

/// Find a device key that matches this staging dir, but only if it's not busy.
/// Never steal a device that's actively scanning or ripping.
fn find_device_for_staging(staging_dir: &str) -> Option<String> {
    let state = ripper::STATE.lock().ok()?;
    for (dev, rs) in state.iter() {
        if rs.status == "scanning" || rs.status == "ripping" {
            continue; // don't touch busy devices
        }
        if rs.output_file == staging_dir || rs.status == "done" {
            return Some(dev.clone());
        }
    }
    // Only fall back to idle devices
    for (dev, rs) in state.iter() {
        if rs.status == "idle" {
            return Some(dev.clone());
        }
    }
    None
}

fn build_destination(cfg: &Config, tmdb: &Option<tmdb::TmdbResult>, filename: &str) -> String {
    if let Some(ref result) = tmdb {
        let safe_title = sanitize_dir_name(&result.title);
        match result.media_type.as_str() {
            "movie" if !cfg.movie_dir.is_empty() => {
                let year_str = if result.year > 0 {
                    format!(" ({})", result.year)
                } else {
                    String::new()
                };
                let dir = format!("{}/{}{}", cfg.movie_dir, safe_title, year_str);
                let mkv_name = format!("{}.mkv", safe_title);
                format!("{}/{}", dir, mkv_name)
            }
            "tv" if !cfg.tv_dir.is_empty() => {
                let dir = format!("{}/{}/Season 1", cfg.tv_dir, safe_title);
                format!("{}/{}", dir, filename)
            }
            _ => {
                format!("{}/{}", cfg.output_dir, filename)
            }
        }
    } else {
        format!("{}/{}", cfg.output_dir, filename)
    }
}

/// Move a file: try rename first, fall back to cp in a child process.
/// Child process prevents NFS/CIFS stalls from blocking the main autorip process.
/// Calls on_progress(pct, gb_done, gb_total, speed_mbs) every few seconds.
fn move_file(src: &Path, dest: &Path, on_progress: &dyn Fn(u8, f64, f64, f64)) -> bool {
    if std::fs::rename(src, dest).is_ok() {
        return true;
    }
    let src_str = src.to_string_lossy().to_string();
    let dest_str = dest.to_string_lossy().to_string();
    let src_size = std::fs::metadata(src).map(|m| m.len()).unwrap_or(0);
    let total_gb = src_size as f64 / 1_073_741_824.0;

    let mut child = match std::process::Command::new("cp")
        .arg("--")
        .arg(&src_str)
        .arg(&dest_str)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            crate::log::syslog(&format!("Failed to spawn cp: {}", e));
            return false;
        }
    };

    let start = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    on_progress(100, total_gb, total_gb, 0.0);
                    let _ = std::fs::remove_file(src);
                    return true;
                } else {
                    crate::log::syslog(&format!("cp failed with {}", status));
                    return false;
                }
            }
            Ok(None) => {
                let dest_size = std::fs::metadata(&dest_str).map(|m| m.len()).unwrap_or(0);
                let pct = if src_size > 0 { (dest_size * 100 / src_size).min(100) as u8 } else { 0 };
                let gb = dest_size as f64 / 1_073_741_824.0;
                let elapsed = start.elapsed().as_secs_f64();
                let speed = if elapsed > 0.0 { dest_size as f64 / (1024.0 * 1024.0) / elapsed } else { 0.0 };
                on_progress(pct, gb, total_gb, speed);
                std::thread::sleep(std::time::Duration::from_secs(3));
            }
            Err(e) => {
                crate::log::syslog(&format!("Failed to wait on cp: {}", e));
                return false;
            }
        }
    }
}

fn sanitize_dir_name(name: &str) -> String {
    name.chars()
        .filter(|c| {
            c.is_ascii_alphanumeric()
                || *c == ' '
                || *c == '-'
                || *c == '_'
                || *c == '.'
                || *c == '\''
        })
        .collect::<String>()
        .trim()
        .to_string()
}
