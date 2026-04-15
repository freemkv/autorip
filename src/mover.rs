use crate::config::Config;
use crate::history;
use crate::ripper;
use crate::tmdb;
use std::path::Path;
use std::sync::{Arc, RwLock};

pub fn run(cfg: &Arc<RwLock<Config>>) {
    loop {
        let cfg = cfg.read().unwrap();
        check_and_move(&cfg);
        drop(cfg);
        std::thread::sleep(std::time::Duration::from_secs(10));
    }
}

fn check_and_move(cfg: &Config) {
    // Find drives with status "done"
    let done_entries: Vec<ripper::RipState> = {
        let state = match ripper::STATE.lock() {
            Ok(s) => s,
            Err(_) => return,
        };
        state
            .values()
            .filter(|rs| rs.status == "done")
            .cloned()
            .collect()
    };

    for rs in &done_entries {
        let staging_dir = &rs.output_file; // staging path stored here when done
        if staging_dir.is_empty() || !Path::new(staging_dir).is_dir() {
            continue;
        }

        // Find all MKV files in the staging directory
        let mkv_files: Vec<std::path::PathBuf> = match std::fs::read_dir(staging_dir) {
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map(|x| x == "mkv").unwrap_or(false))
                .collect(),
            Err(_) => continue,
        };

        if mkv_files.is_empty() {
            continue;
        }

        // Look up TMDB metadata
        let tmdb_result = if !cfg.tmdb_api_key.is_empty() {
            tmdb::lookup(&tmdb::clean_title(&rs.disc_name), &cfg.tmdb_api_key)
        } else {
            None
        };

        // Update state to "moving"
        ripper::update_state(
            &rs.device,
            ripper::RipState {
                device: rs.device.clone(),
                status: "moving".to_string(),
                disc_name: rs.disc_name.clone(),
                disc_format: rs.disc_format.clone(),
                progress_pct: 100,
                tmdb_title: tmdb_result
                    .as_ref()
                    .map(|t| t.title.clone())
                    .unwrap_or_default(),
                tmdb_year: tmdb_result.as_ref().map(|t| t.year).unwrap_or(0),
                tmdb_poster: tmdb_result
                    .as_ref()
                    .map(|t| t.poster_url.clone())
                    .unwrap_or_default(),
                tmdb_overview: tmdb_result
                    .as_ref()
                    .map(|t| t.overview.clone())
                    .unwrap_or_default(),
                ..Default::default()
            },
        );

        let mut all_moved = true;
        let mut dest_paths: Vec<String> = Vec::new();

        for mkv_path in &mkv_files {
            let filename = mkv_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();

            let dest = build_destination(cfg, &tmdb_result, &filename);
            // Ensure parent directory exists
            if let Some(parent) = Path::new(&dest).parent() {
                let _ = std::fs::create_dir_all(parent);
            }

            if move_file(mkv_path, Path::new(&dest)) {
                dest_paths.push(dest);
            } else {
                crate::log::device_log(
                    &rs.device,
                    &format!("Failed to move {:?} to {}", mkv_path, dest),
                );
                all_moved = false;
            }
        }

        if all_moved {
            // Remove the staging directory
            let _ = std::fs::remove_dir_all(staging_dir);

            // Record history
            let title = tmdb_result
                .as_ref()
                .map(|t| t.title.clone())
                .unwrap_or_else(|| rs.disc_name.clone());
            let year = tmdb_result.as_ref().map(|t| t.year).unwrap_or(0);
            let entry = serde_json::json!({
                "title": title,
                "disc_name": rs.disc_name,
                "format": rs.disc_format,
                "year": year,
                "media_type": tmdb_result.as_ref().map(|t| t.media_type.as_str()).unwrap_or("unknown"),
                "poster_url": tmdb_result.as_ref().map(|t| t.poster_url.as_str()).unwrap_or(""),
                "overview": tmdb_result.as_ref().map(|t| t.overview.as_str()).unwrap_or(""),
                "files": dest_paths,
                "date": chrono_date_string(),
            });
            history::record(&cfg.history_dir(), &entry);

            // Mark device as idle
            ripper::update_state(
                &rs.device,
                ripper::RipState {
                    device: rs.device.clone(),
                    status: "idle".to_string(),
                    ..Default::default()
                },
            );
        }
    }
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
                // For TV, keep the original filename which may contain episode info
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

/// Move a file: try rename first (instant on same filesystem), fall back to copy+delete.
fn move_file(src: &Path, dest: &Path) -> bool {
    // Try rename first (same filesystem)
    if std::fs::rename(src, dest).is_ok() {
        return true;
    }
    // Fall back to copy + delete
    match std::fs::copy(src, dest) {
        Ok(_) => {
            let _ = std::fs::remove_file(src);
            true
        }
        Err(_) => false,
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

fn chrono_date_string() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Convert epoch seconds to a simple YYYY-MM-DD string
    // Days since epoch
    let days = (secs / 86400) as i64;
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
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
