//! Background mux worker — pipelines mux behind the drive thread.
//!
//! Mirrors the shape of [`crate::mover`]:
//! - A 10-second tick loop polling the staging dir for hand-off markers.
//! - A single global "live state" mutex (`MUX_STATE`) read by the
//!   System page over SSE.
//! - A `BTreeMap<String, MuxerError>` for stuck-dir surfacing.
//!
//! Hand-off contract (v0.25.3):
//!
//! 1. The drive thread (`ripper::rip_disc`) finishes sweep + patch.
//! 2. It writes a `.ripped` JSON marker inside the staging dir with
//!    everything the mux worker needs to reconstruct a `MuxInputs`
//!    (TMDB metadata, codec list, byte counts, batch size, etc.) plus
//!    the ISO filename.
//! 3. If `cfg.auto_eject` is set, it ejects the drive — the disc is
//!    no longer needed once `.ripped` is on disk.
//! 4. The drive returns to `idle`, ready for the next disc.
//! 5. This worker polls the staging dir, picks up `.ripped` markers,
//!    runs the mux against the ISO, writes `.done` (the mover's
//!    existing hand-off) and deletes `.ripped` on success. On failure
//!    it records a `MuxerError` and leaves `.ripped` in place for
//!    next-tick retry / operator inspection.
//!
//! Single-pass live-disc rips (`cfg.max_retries == 0`) stay inline —
//! there's no ISO to hand off and the drive needs to be open for the
//! whole mux. The worker is a no-op for those titles.

use crate::config::Config;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

/// Mux progress — separate from device/rip state and from
/// [`crate::mover::MoveState`]. Read by the System page's
/// `renderMuxes()` via SSE (`_mux` field on `/api/state`).
#[derive(Debug, Clone, serde::Serialize, Default)]
#[allow(dead_code)] // wired up in phase 3 (drive cut-over) + phase 4 (UI)
pub struct MuxState {
    pub name: String,
    pub progress_pct: u8,
    pub progress_gb: f64,
    pub total_gb: f64,
    pub speed_mbs: f64,
    pub eta: String,
}

#[allow(dead_code)] // wired up in phase 3 (drive cut-over) + phase 4 (UI)
pub static MUX_STATE: once_cell::sync::Lazy<Mutex<Option<MuxState>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(None));

/// Per-staging-dir error surfaced to the System page so the user can
/// act on it (e.g. `MuxFinalize` after an NFS hiccup that left the MKV
/// unseekable). Keyed by staging dir path; same `reason` for the same
/// path is idempotent — no log spam on retry ticks.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MuxerError {
    pub path: String,
    pub reason: String,
    pub hint: String,
}

pub static MUX_ERRORS: once_cell::sync::Lazy<Mutex<BTreeMap<String, MuxerError>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(BTreeMap::new()));

#[allow(dead_code)] // wired up in the rip_disc cut-over (phase 3)
pub(crate) fn record_error(path: &str, reason: &str, hint: &str) {
    let Ok(mut m) = MUX_ERRORS.lock() else {
        return;
    };
    let same_reason = m.get(path).map(|e| e.reason == reason).unwrap_or(false);
    m.insert(
        path.to_string(),
        MuxerError {
            path: path.to_string(),
            reason: reason.to_string(),
            hint: hint.to_string(),
        },
    );
    if !same_reason {
        crate::log::syslog(&format!("Mux blocked: {} — {}", path, reason));
    }
}

#[allow(dead_code)] // wired up in the rip_disc cut-over (phase 3)
pub(crate) fn clear_error(path: &str) {
    if let Ok(mut m) = MUX_ERRORS.lock() {
        m.remove(path);
    }
}

/// Worker entry point — spawn from `main` alongside the mover thread.
///
/// Currently a heartbeat loop: scans the staging dir for `.ripped`
/// markers on each tick. Phase 3 wires the actual mux invocation; for
/// now this is a placeholder that keeps the worker thread alive and
/// the staging scan plumbed so the System page can list pending mux
/// jobs as soon as the drive thread starts writing markers.
pub fn run(cfg: &Arc<RwLock<Config>>) {
    use std::sync::atomic::Ordering;
    tracing::info!("mux loop starting");
    while !crate::SHUTDOWN.load(Ordering::Relaxed) {
        let cfg_snapshot = match cfg.read() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "mux: config lock poisoned, retrying");
                std::thread::sleep(std::time::Duration::from_secs(10));
                continue;
            }
        };
        check_and_mux(&cfg_snapshot);
        drop(cfg_snapshot);
        // SHUTDOWN-responsive sleep — same pattern as the mover so
        // SIGTERM doesn't have to wait the full 10 s tick.
        for _ in 0..100 {
            if crate::SHUTDOWN.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    tracing::info!("mux loop stopping");
}

/// Find all staging dirs with a `.ripped` marker and queue each for
/// mux processing. Phase 3 will run the mux inside this function; for
/// now it's a discoverer — surfaces queue items to the System page
/// without acting on them.
fn check_and_mux(cfg: &Config) {
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
        let marker = dir.join(".ripped");
        if !marker.exists() {
            continue;
        }
        // Placeholder for phase 3: read marker, run mux, write .done,
        // delete .ripped. For now just touch a debug log on first
        // sighting per session so we know the loop is reaching here.
        tracing::debug!(staging = %dir.display(), "mux worker: .ripped marker present (phase 3 will dispatch)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_clear_error_round_trip() {
        record_error("/x/staging/Foo", "test reason", "test hint");
        {
            let m = MUX_ERRORS.lock().unwrap();
            assert!(m.contains_key("/x/staging/Foo"));
            assert_eq!(m["/x/staging/Foo"].reason, "test reason");
        }
        clear_error("/x/staging/Foo");
        let m = MUX_ERRORS.lock().unwrap();
        assert!(!m.contains_key("/x/staging/Foo"));
    }

    #[test]
    fn mux_state_default_is_empty() {
        let s = MuxState::default();
        assert!(s.name.is_empty());
        assert_eq!(s.progress_pct, 0);
    }
}
