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
            if move_file(src, Path::new(dest)) {
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

/// Find a device key that matches this staging dir, or the first idle/done device.
fn find_device_for_staging(staging_dir: &str) -> Option<String> {
    let state = ripper::STATE.lock().ok()?;
    // Check if any device has this staging dir as output_file
    for (dev, rs) in state.iter() {
        if rs.output_file == staging_dir || rs.status == "done" {
            return Some(dev.clone());
        }
    }
    // Fall back to first device
    state.keys().next().cloned()
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

/// Move a file: try rename first (instant on same filesystem), fall back to copy+delete with progress.
fn move_file(src: &Path, dest: &Path) -> bool {
    if std::fs::rename(src, dest).is_ok() {
        return true;
    }
    // Cross-filesystem: copy with progress logging
    let src_size = match std::fs::metadata(src) {
        Ok(m) => m.len(),
        Err(_) => return false,
    };
    let mut reader = match std::fs::File::open(src) {
        Ok(f) => f,
        Err(_) => return false,
    };
    let mut writer = match std::fs::File::create(dest) {
        Ok(f) => f,
        Err(_) => return false,
    };
    let mut buf = vec![0u8; 8 * 1024 * 1024]; // 8 MB buffer
    let mut copied: u64 = 0;
    let start = std::time::Instant::now();
    let mut last_log = start;
    loop {
        let n = match std::io::Read::read(&mut reader, &mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => return false,
        };
        if std::io::Write::write_all(&mut writer, &buf[..n]).is_err() {
            return false;
        }
        copied += n as u64;
        let now = std::time::Instant::now();
        if now.duration_since(last_log).as_secs() >= 10 {
            last_log = now;
            let pct = if src_size > 0 { copied * 100 / src_size } else { 0 };
            let elapsed = start.elapsed().as_secs_f64();
            let speed = if elapsed > 0.0 { copied as f64 / (1024.0 * 1024.0) / elapsed } else { 0.0 };
            crate::log::syslog(&format!(
                "Moving: {:.1} GB / {:.1} GB ({}%) {:.0} MB/s",
                copied as f64 / 1_073_741_824.0,
                src_size as f64 / 1_073_741_824.0,
                pct,
                speed
            ));
        }
    }
    let _ = std::fs::remove_file(src);
    true
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
