use crate::config::Config;
use crate::tmdb;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

/// Move progress — separate from device/rip state.
/// Read by the System page's renderMoves() via SSE.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MoveState {
    pub name: String,
    pub progress_pct: u8,
    pub progress_gb: f64,
    pub total_gb: f64,
    pub speed_mbs: f64,
    pub eta: String,
}

pub static MOVE_STATE: once_cell::sync::Lazy<Mutex<Option<MoveState>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(None));

/// Per-staging-dir error surfaced to the System page so the user can act
/// on it (e.g. orphaned source files that the container can't unlink due
/// to NFS squash perms). Keyed by staging dir path; updates are
/// idempotent — same `reason` for the same path is a no-op (no log spam).
#[derive(Debug, Clone, serde::Serialize)]
pub struct MoverError {
    pub path: String,
    pub reason: String,
    pub hint: String,
}

pub static MOVE_ERRORS: once_cell::sync::Lazy<Mutex<BTreeMap<String, MoverError>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(BTreeMap::new()));

fn record_error(path: &str, reason: &str, hint: &str) {
    let Ok(mut m) = MOVE_ERRORS.lock() else {
        return;
    };
    let same_reason = m.get(path).map(|e| e.reason == reason).unwrap_or(false);
    m.insert(
        path.to_string(),
        MoverError {
            path: path.to_string(),
            reason: reason.to_string(),
            hint: hint.to_string(),
        },
    );
    if !same_reason {
        crate::log::syslog(&format!("Move blocked: {} — {}", path, reason));
    }
}

fn clear_error(path: &str) {
    if let Ok(mut m) = MOVE_ERRORS.lock() {
        m.remove(path);
    }
}

/// Outcome of moving a single file. Distinguishes between an active move
/// (Moved / MovedDirty) and a no-op re-check (Skipped) so the caller can
/// suppress webhook spam and log noise on subsequent loop ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MoveOutcome {
    /// Dest already exists with the same size as src — already moved on
    /// a previous tick. Source may or may not still be present.
    Skipped,
    /// Atomic rename succeeded — src is gone, dest has the bytes.
    Moved,
    /// Copy succeeded but unlink of src failed (perm/NFS issue). Dest has
    /// the bytes. Caller should record an error and stop trying to clean
    /// the staging dir; subsequent ticks will Skip.
    MovedDirty,
    /// Copy itself failed. Caller can retry on the next tick.
    Failed,
}

pub fn run(cfg: &Arc<RwLock<Config>>) {
    use std::sync::atomic::Ordering;
    tracing::info!("mover loop starting");
    while !crate::SHUTDOWN.load(Ordering::Relaxed) {
        let cfg_snapshot = match cfg.read() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "mover: config lock poisoned, retrying");
                std::thread::sleep(std::time::Duration::from_secs(10));
                continue;
            }
        };
        check_and_move(&cfg_snapshot);
        drop(cfg_snapshot);
        // SHUTDOWN-responsive sleep — break early on signal so SIGTERM
        // doesn't have to wait the full 10 s tick.
        for _ in 0..100 {
            if crate::SHUTDOWN.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    tracing::info!("mover loop stopping");
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
        let _disc_format = marker["format"].as_str().unwrap_or("").to_string();

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

        let dir_str = dir.to_string_lossy().to_string();

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
                    record_error(
                        &dir_str,
                        &format!("cannot create destination directory {}", parent.display()),
                        "check write permissions on the output / movie / tv directory",
                    );
                    dest_ok = false;
                }
            }
        }
        if !dest_ok {
            continue;
        }

        // Move files
        let mut outcomes: Vec<MoveOutcome> = Vec::new();
        let mut announced_moving = false;
        for (src, dest) in &planned_moves {
            let name_for_progress = display_name.clone();
            let on_progress = move |pct: u8, gb: f64, total_gb: f64, speed: f64| {
                let eta = if speed > 1.0 && total_gb > gb {
                    let secs = ((total_gb - gb) * 1024.0 / speed) as u32;
                    let m = secs / 60;
                    let s = secs % 60;
                    format!("{}:{:02}", m, s)
                } else {
                    String::new()
                };
                if let Ok(mut ms) = MOVE_STATE.lock() {
                    *ms = Some(MoveState {
                        name: name_for_progress.clone(),
                        progress_pct: pct,
                        progress_gb: gb,
                        total_gb,
                        speed_mbs: speed,
                        eta,
                    });
                }
            };
            let outcome = move_file(src, Path::new(dest), &on_progress);
            outcomes.push(outcome);
            match outcome {
                MoveOutcome::Skipped => {
                    // Quiet — already moved on a prior tick; no log noise.
                }
                MoveOutcome::Moved => {
                    if !announced_moving {
                        crate::log::syslog(&format!(
                            "Moving: {} ({} files)",
                            display_name,
                            ripped_files.len()
                        ));
                        announced_moving = true;
                    }
                    crate::log::syslog(&format!("Moved to {}", dest));
                }
                MoveOutcome::MovedDirty => {
                    if !announced_moving {
                        crate::log::syslog(&format!(
                            "Moving: {} ({} files)",
                            display_name,
                            ripped_files.len()
                        ));
                        announced_moving = true;
                    }
                    crate::log::syslog(&format!(
                        "Moved to {} but source could not be removed",
                        dest
                    ));
                }
                MoveOutcome::Failed => {
                    crate::log::syslog(&format!("Failed to move {:?} to {}", src, dest));
                }
            }
        }

        let any_failed = outcomes.iter().any(|o| matches!(o, MoveOutcome::Failed));
        let any_dirty = outcomes
            .iter()
            .any(|o| matches!(o, MoveOutcome::MovedDirty));
        let any_actively_moved = outcomes
            .iter()
            .any(|o| matches!(o, MoveOutcome::Moved | MoveOutcome::MovedDirty));

        if any_failed {
            // Leave the dir alone; mover will retry next tick.
            // Surface a summary error so the UI shows what's failing.
            record_error(
                &dir_str,
                "copy to destination failed",
                "see device_system.log for the underlying error",
            );
            continue;
        }

        // All files are accounted for (Skipped / Moved / MovedDirty). Try to
        // tear down the staging dir; if it can't be removed (typically
        // because the orphan source files can't be unlinked), surface the
        // dir on the UI with a remediation hint.
        let cleanup_err = std::fs::remove_dir_all(&dir).err();

        if cleanup_err.is_none() {
            clear_error(&dir_str);
            crate::log::syslog(&format!("Move complete: {}", display_name));
        } else if any_dirty {
            record_error(
                &dir_str,
                "destination has the file but source could not be removed",
                "manually `rm -rf` the staging dir from a host that can write to the staging share, or fix the NFS export so the container can unlink files there",
            );
        } else if let Some(e) = cleanup_err {
            record_error(
                &dir_str,
                &format!("staging cleanup failed: {}", e),
                "manually `rm -rf` the staging dir",
            );
        }

        // Webhook: only fire on cycles where we actually moved bits.
        // Skipped-only ticks are no-ops and must not re-notify.
        if any_actively_moved {
            let dest_path = planned_moves.last().map(|(_, d)| d.as_str()).unwrap_or("");
            crate::webhook::send_move(cfg, &display_name, dest_path);
        }

        // Clear move state
        if let Ok(mut ms) = MOVE_STATE.lock() {
            *ms = None;
        }
    }
}

fn build_destination(cfg: &Config, tmdb: &Option<tmdb::TmdbResult>, filename: &str) -> String {
    if let Some(result) = tmdb {
        let safe_title = crate::util::sanitize_path_display(&result.title);
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

/// Move a file with idempotent retry semantics.
///
/// 1. **Pre-flight**: if `dest` is a regular file with the same size as
///    `src`, treat the move as already done (`Skipped`). This is the
///    circuit breaker for the "cp succeeded but unlink failed" loop —
///    on the next tick we re-detect the completed dest and don't
///    re-copy 50+ GB across the network.
///
/// 2. **Atomic path**: try `rename(2)`. On the same filesystem this is
///    instant and unlinks src for free.
///
/// 3. **Cross-fs / fallback**: shell out to `cp -f --` (force, no flag
///    parsing of the path), then try to unlink src. If unlink fails
///    (typical NFS squash-perm scenario where the staging dir is owned
///    by an identity the container can't write into), return
///    `MovedDirty` so the caller can surface the orphan to the UI.
///
/// Child process prevents NFS/CIFS stalls from blocking the main
/// autorip process. Calls `on_progress(pct, gb_done, gb_total,
/// speed_mbs)` every few seconds while cp is running.
fn move_file(src: &Path, dest: &Path, on_progress: &dyn Fn(u8, f64, f64, f64)) -> MoveOutcome {
    let src_meta = std::fs::metadata(src);
    let dest_meta = std::fs::metadata(dest);

    // Pre-flight: dest already has matching content. Stops the infinite
    // re-copy loop when src can't be unlinked.
    if let (Ok(s), Ok(d)) = (&src_meta, &dest_meta) {
        if s.is_file() && d.is_file() && s.len() == d.len() && s.len() > 0 {
            return MoveOutcome::Skipped;
        }
    }
    // Pre-flight: src missing but dest present — earlier rename succeeded.
    if let (Err(_), Ok(d)) = (&src_meta, &dest_meta) {
        if d.is_file() && d.len() > 0 {
            return MoveOutcome::Moved;
        }
    }

    if std::fs::rename(src, dest).is_ok() {
        return MoveOutcome::Moved;
    }

    let src_str = src.to_string_lossy().to_string();
    let dest_str = dest.to_string_lossy().to_string();
    let src_size = src_meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let total_gb = src_size as f64 / 1_073_741_824.0;

    let mut child = match std::process::Command::new("cp")
        .arg("-f")
        .arg("--")
        .arg(&src_str)
        .arg(&dest_str)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            crate::log::syslog(&format!("Failed to spawn cp: {}", e));
            return MoveOutcome::Failed;
        }
    };

    let start = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    on_progress(100, total_gb, total_gb, 0.0);
                    return match std::fs::remove_file(src) {
                        Ok(_) => MoveOutcome::Moved,
                        Err(_) => MoveOutcome::MovedDirty,
                    };
                } else {
                    crate::log::syslog(&format!("cp failed with {}", status));
                    return MoveOutcome::Failed;
                }
            }
            Ok(None) => {
                let dest_size = std::fs::metadata(&dest_str).map(|m| m.len()).unwrap_or(0);
                let pct = if src_size > 0 {
                    (dest_size * 100 / src_size).min(100) as u8
                } else {
                    0
                };
                let gb = dest_size as f64 / 1_073_741_824.0;
                let elapsed = start.elapsed().as_secs_f64();
                let speed = if elapsed > 0.0 {
                    dest_size as f64 / (1024.0 * 1024.0) / elapsed
                } else {
                    0.0
                };
                on_progress(pct, gb, total_gb, speed);
                std::thread::sleep(std::time::Duration::from_secs(3));
            }
            Err(e) => {
                crate::log::syslog(&format!("Failed to wait on cp: {}", e));
                return MoveOutcome::Failed;
            }
        }
    }
}

// `sanitize_dir_name` moved to `crate::util::sanitize_path_display` in 0.13.0.
// Single source of truth shared with the staging path in `ripper`.

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with_dirs(movie_dir: &str, tv_dir: &str, output_dir: &str) -> Config {
        Config {
            port: 8080,
            staging_dir: "/staging".into(),
            output_dir: output_dir.into(),
            movie_dir: movie_dir.into(),
            tv_dir: tv_dir.into(),
            min_length_secs: 600,
            main_feature: true,
            auto_eject: true,
            on_insert: "rip".into(),
            output_format: "mkv".into(),
            network_target: String::new(),
            on_read_error: "stop".into(),
            max_retries: 1,
            keep_iso: false,
            abort_on_lost_secs: 0,
            tmdb_api_key: String::new(),
            keydb_path: None,
            keydb_url: String::new(),
            webhook_urls: Vec::new(),
            autorip_dir: "/config".into(),
            max_rip_duration_secs: 7200,
            min_pass_budget_secs: 3600,
            transport_recovery_delay_secs: 5,
        }
    }

    fn tmdb_movie(title: &str, year: u16) -> tmdb::TmdbResult {
        tmdb::TmdbResult {
            title: title.into(),
            year,
            poster_url: String::new(),
            overview: String::new(),
            media_type: "movie".into(),
        }
    }

    #[test]
    fn sanitize_dir_name_strips_unsafe_characters() {
        assert_eq!(
            crate::util::sanitize_path_display("Dune: Part Two"),
            "Dune Part Two"
        );
        assert_eq!(crate::util::sanitize_path_display("M*A*S*H"), "MASH");
        assert_eq!(
            crate::util::sanitize_path_display("Alien/Predator"),
            "AlienPredator"
        );
        assert_eq!(
            crate::util::sanitize_path_display("What's Up, Doc?"),
            "What's Up Doc"
        );
    }

    #[test]
    fn sanitize_dir_name_keeps_allowed_punctuation() {
        assert_eq!(
            crate::util::sanitize_path_display("Rogue One - A Star Wars Story"),
            "Rogue One - A Star Wars Story"
        );
        assert_eq!(
            crate::util::sanitize_path_display("Director_Cut.2019"),
            "Director_Cut.2019"
        );
    }

    #[test]
    fn sanitize_dir_name_trims_whitespace() {
        assert_eq!(
            crate::util::sanitize_path_display("  spaced title  "),
            "spaced title"
        );
    }

    #[test]
    fn build_destination_movie_with_year() {
        let cfg = cfg_with_dirs("/out/Movies", "/out/TV", "/out");
        let tmdb = Some(tmdb_movie("Dune Part Two", 2024));
        let dest = build_destination(&cfg, &tmdb, "disc.mkv");
        assert_eq!(dest, "/out/Movies/Dune Part Two (2024)/Dune Part Two.mkv");
    }

    #[test]
    fn build_destination_movie_without_year_falls_through() {
        let cfg = cfg_with_dirs("/out/Movies", "/out/TV", "/out");
        let tmdb = Some(tmdb_movie("Unknown Year", 0));
        let dest = build_destination(&cfg, &tmdb, "disc.mkv");
        // year=0 skips the "(YEAR)" suffix; mkv name derived from cleaned title.
        assert_eq!(dest, "/out/Movies/Unknown Year/Unknown Year.mkv");
    }

    #[test]
    fn build_destination_tv_uses_season_1_layout() {
        let cfg = cfg_with_dirs("/out/Movies", "/out/TV", "/out");
        let tmdb = Some(tmdb::TmdbResult {
            title: "Severance".into(),
            year: 2022,
            poster_url: String::new(),
            overview: String::new(),
            media_type: "tv".into(),
        });
        let dest = build_destination(&cfg, &tmdb, "sev_s01e01.mkv");
        assert_eq!(dest, "/out/TV/Severance/Season 1/sev_s01e01.mkv");
    }

    #[test]
    fn build_destination_no_tmdb_falls_to_output_dir() {
        let cfg = cfg_with_dirs("/out/Movies", "/out/TV", "/out");
        let dest = build_destination(&cfg, &None, "disc.mkv");
        assert_eq!(dest, "/out/disc.mkv");
    }

    #[test]
    fn build_destination_empty_movie_dir_falls_to_output_dir() {
        let cfg = cfg_with_dirs("", "/out/TV", "/out");
        let tmdb = Some(tmdb_movie("Movie", 2020));
        let dest = build_destination(&cfg, &tmdb, "disc.mkv");
        // movie_dir empty → fall-through to output_dir + filename.
        assert_eq!(dest, "/out/disc.mkv");
    }

    fn noop_progress(_: u8, _: f64, _: f64, _: f64) {}

    #[test]
    fn move_file_skips_when_dest_size_matches() {
        // Circuit breaker: a prior tick already cp'd the file but
        // couldn't unlink src. Re-detecting the same-size dest must NOT
        // recopy — that's the bug this fix exists for.
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("a.mkv");
        let dest = tmp.path().join("b.mkv");
        std::fs::write(&src, b"hello world").unwrap();
        std::fs::write(&dest, b"hello world").unwrap();
        let outcome = move_file(&src, &dest, &noop_progress);
        assert_eq!(outcome, MoveOutcome::Skipped);
        assert!(src.exists(), "src must remain untouched on Skipped");
        assert!(dest.exists());
    }

    #[test]
    fn move_file_moves_when_dest_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("a.mkv");
        let dest = tmp.path().join("sub/b.mkv");
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        std::fs::write(&src, b"data data data").unwrap();
        let outcome = move_file(&src, &dest, &noop_progress);
        assert_eq!(outcome, MoveOutcome::Moved);
        assert!(!src.exists(), "rename consumes src");
        assert_eq!(std::fs::read(&dest).unwrap(), b"data data data");
    }

    #[test]
    fn move_file_overwrites_when_dest_size_differs() {
        // A partial dest from a previous failed cp must NOT cause a
        // permanent stall — the new full src should overwrite it.
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("a.mkv");
        let dest = tmp.path().join("b.mkv");
        std::fs::write(&src, b"new full content").unwrap();
        std::fs::write(&dest, b"partial").unwrap();
        let outcome = move_file(&src, &dest, &noop_progress);
        assert_eq!(outcome, MoveOutcome::Moved);
        assert_eq!(std::fs::read(&dest).unwrap(), b"new full content");
    }

    #[test]
    fn move_file_returns_moved_when_src_missing_but_dest_present() {
        // Earlier atomic rename succeeded; src is gone, dest is fine.
        // Re-entering move_file (e.g. on next tick before staging
        // cleanup) must not error out.
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("a.mkv");
        let dest = tmp.path().join("b.mkv");
        std::fs::write(&dest, b"already there").unwrap();
        let outcome = move_file(&src, &dest, &noop_progress);
        assert_eq!(outcome, MoveOutcome::Moved);
    }

    #[test]
    fn record_error_dedups_same_reason_without_logging_again() {
        // Same path + same reason twice → second insert is a no-op
        // logger-wise (the syslog call is gated on reason change).
        // We assert state by checking the map snapshot.
        let path = "/tmp/fakemover-dedup-test";
        record_error(path, "stuck", "do thing");
        record_error(path, "stuck", "do thing");
        let m = MOVE_ERRORS.lock().unwrap();
        let entry = m.get(path).expect("error recorded");
        assert_eq!(entry.reason, "stuck");
        drop(m);
        clear_error(path);
        assert!(MOVE_ERRORS.lock().unwrap().get(path).is_none());
    }
}
