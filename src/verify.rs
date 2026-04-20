//! Disc verification — sector-by-sector health check.

use std::sync::Mutex;

/// Verify progress and results, pushed to UI via SSE.
#[derive(Debug, Clone, serde::Serialize)]
pub struct VerifyState {
    pub status: String, // "running", "done", "error"
    pub disc_name: String,
    pub progress_pct: u8,
    pub sectors_done: u64,
    pub sectors_total: u64,
    pub speed_mbs: f64,
    pub good: u64,
    pub slow: u64,
    pub recovered: u64,
    pub bad: u64,
    /// Sector map: list of (offset_pct, width_pct, status) for rendering
    /// status: "good", "slow", "recovered", "bad"
    pub sector_map: Vec<SectorMapEntry>,
    pub bad_ranges: Vec<BadRange>,
    pub elapsed_secs: f64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SectorMapEntry {
    pub offset_pct: f64,
    pub width_pct: f64,
    pub status: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BadRange {
    pub start_sector: u64,
    pub count: u32,
    pub gb_offset: f64,
    pub chapter: String,
    pub status: String,
}

pub static VERIFY_STATE: once_cell::sync::Lazy<Mutex<Option<VerifyState>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(None));

/// Run verify in background. Called from web API handler.
pub fn run_verify(device: &str, device_path: &str) {
    let device = device.to_string();
    let device_path = device_path.to_string();

    std::thread::spawn(move || {
        crate::log::device_log(&device, "Verify: opening drive...");

        let mut drive = match libfreemkv::Drive::open(std::path::Path::new(&device_path)) {
            Ok(d) => d,
            Err(e) => {
                crate::log::device_log(&device, &format!("Verify failed: {}", e));
                if let Ok(mut vs) = VERIFY_STATE.lock() {
                    *vs = Some(VerifyState {
                        status: "error".into(),
                        disc_name: String::new(),
                        bad_ranges: vec![BadRange {
                            start_sector: 0, count: 0, gb_offset: 0.0,
                            chapter: String::new(), status: format!("{}", e),
                        }],
                        ..empty_state()
                    });
                }
                return;
            }
        };
        let _ = drive.wait_ready();
        let _ = drive.init();

        crate::log::device_log(&device, "Verify: scanning disc...");
        let scan_opts = libfreemkv::ScanOptions::default();
        let disc = match libfreemkv::Disc::scan(&mut drive, &scan_opts) {
            Ok(d) => d,
            Err(e) => {
                crate::log::device_log(&device, &format!("Verify scan failed: {}", e));
                return;
            }
        };
        if disc.titles.is_empty() {
            crate::log::device_log(&device, "Verify: no titles");
            return;
        }

        let title = &disc.titles[0];
        let disc_name = disc.meta_title.as_deref().unwrap_or(&disc.volume_id).to_string();
        let total_sectors: u64 = title.extents.iter().map(|e| e.sector_count as u64).sum();
        let batch = libfreemkv::disc::detect_max_batch_sectors(&device_path);
        let _ = drive.probe_disc();

        crate::log::device_log(&device, &format!(
            "Verify: {} ({:.1} GB, {} sectors)",
            disc_name,
            total_sectors as f64 * 2048.0 / 1_073_741_824.0,
            total_sectors
        ));

        // Init state
        if let Ok(mut vs) = VERIFY_STATE.lock() {
            *vs = Some(VerifyState {
                status: "running".into(),
                disc_name: disc_name.clone(),
                sectors_total: total_sectors,
                ..empty_state()
            });
        }

        let start = std::time::Instant::now();
        let title_clone = title.clone();

        let result = libfreemkv::verify::verify_title(
            &mut drive,
            title,
            batch,
            Some(Box::new(move |done, total, status| {
                let elapsed = start.elapsed().as_secs_f64();
                let speed = if elapsed > 0.0 {
                    done as f64 * 2048.0 / (1024.0 * 1024.0) / elapsed
                } else {
                    0.0
                };
                let pct = if total > 0 { (done * 100 / total) as u8 } else { 0 };

                if let Ok(mut vs) = VERIFY_STATE.lock() {
                    if let Some(ref mut state) = *vs {
                        state.progress_pct = pct;
                        state.sectors_done = done;
                        state.speed_mbs = speed;
                        state.elapsed_secs = elapsed;

                        // Update counts based on status
                        match status {
                            libfreemkv::verify::SectorStatus::Bad => state.bad += 1,
                            libfreemkv::verify::SectorStatus::Slow => state.slow += 1,
                            libfreemkv::verify::SectorStatus::Recovered => state.recovered += 1,
                            _ => {}
                        }
                    }
                }
            })),
        );

        // Build sector map for UI (normalize to 0-100% of disc)
        let mut sector_map = Vec::new();
        let mut bad_ranges = Vec::new();

        for range in &result.ranges {
            let offset_pct = range.byte_offset as f64 / (total_sectors as f64 * 2048.0) * 100.0;
            let width_pct = (range.count as f64 / total_sectors as f64 * 100.0).max(0.2); // min 0.2% for visibility
            let status_str = match range.status {
                libfreemkv::verify::SectorStatus::Slow => "slow",
                libfreemkv::verify::SectorStatus::Recovered => "recovered",
                libfreemkv::verify::SectorStatus::Bad => "bad",
                _ => continue,
            };
            sector_map.push(SectorMapEntry {
                offset_pct,
                width_pct,
                status: status_str.into(),
            });

            let gb = range.byte_offset as f64 / 1_073_741_824.0;
            let chapter_info = libfreemkv::verify::VerifyResult::chapter_at_offset(
                &title_clone.chapters,
                range.byte_offset,
                title_clone.duration_secs,
                title_clone.size_bytes,
            );
            let ch_str = match chapter_info {
                Some((ch, secs)) => {
                    let m = secs as u32 / 60;
                    let s = secs as u32 % 60;
                    format!("Chapter {}, {:02}:{:02}", ch, m, s)
                }
                None => String::new(),
            };

            bad_ranges.push(BadRange {
                start_sector: range.start_lba as u64,
                count: range.count,
                gb_offset: gb,
                chapter: ch_str,
                status: status_str.into(),
            });
        }

        let verdict = if result.is_perfect() {
            "Disc is perfect."
        } else if result.bad > 0 {
            "Has unrecoverable sectors."
        } else {
            "All sectors readable."
        };
        crate::log::device_log(&device, &format!(
            "Verify complete: {:.4}% readable ({} good, {} bad) — {}",
            result.readable_pct(), result.good, result.bad, verdict
        ));

        if let Ok(mut vs) = VERIFY_STATE.lock() {
            *vs = Some(VerifyState {
                status: "done".into(),
                disc_name,
                progress_pct: 100,
                sectors_done: result.total_sectors,
                sectors_total: result.total_sectors,
                speed_mbs: 0.0,
                good: result.good,
                slow: result.slow,
                recovered: result.recovered,
                bad: result.bad,
                sector_map,
                bad_ranges,
                elapsed_secs: result.elapsed_secs,
            });
        }
    });
}

fn empty_state() -> VerifyState {
    VerifyState {
        status: String::new(),
        disc_name: String::new(),
        progress_pct: 0,
        sectors_done: 0,
        sectors_total: 0,
        speed_mbs: 0.0,
        good: 0,
        slow: 0,
        recovered: 0,
        bad: 0,
        sector_map: Vec::new(),
        bad_ranges: Vec::new(),
        elapsed_secs: 0.0,
    }
}
