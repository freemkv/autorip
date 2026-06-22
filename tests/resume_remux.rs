//! Integration tests for the 0.20.8 auto-resume classifier.
//!
//! These hit the pure-function path (`classify_resume`) and the
//! cleanup helper (`delete_partial_output`). Synthetic mapfiles are
//! built via libfreemkv's `Mapfile::create` + `record` + `flush` so
//! we don't hand-roll the on-disk text format.
//!
//! Deliberate gap: `Disc::scan_image` and `run_mux` end-to-end need a
//! real UDF ISO. Feeding synthetic bytes into `scan_image` reliably
//! fails (per the libfreemkv library rules). The live test bed validates the
//! full flow on a real disc; the gap is documented in
//! `src/ripper/resume.rs`.

use std::path::{Path, PathBuf};

use freemkv_autorip::ripper::resume::{ResumeClass, classify_resume, delete_partial_output};
use freemkv_autorip::ripper::staging::{self, RESTART_COUNT_FILE, ResumeAction, StagingResumeHint};

fn tmpdir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

fn make_hint(dir: PathBuf, action: ResumeAction) -> StagingResumeHint {
    StagingResumeHint { dir, action }
}

fn write_mapfile(path: &Path, size_bytes: u64, status: libfreemkv::disc::mapfile::SectorStatus) {
    use libfreemkv::disc::mapfile::Mapfile;
    let mut map = Mapfile::create(path, size_bytes, "test").expect("mapfile create");
    map.record(0, size_bytes, status).expect("mapfile record");
    map.flush().expect("mapfile flush");
}

#[test]
fn resume_classifies_clean_mapfile_as_remux() {
    let td = tmpdir();
    let dir = td.path().join("MyDisc");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("MyDisc.iso"), b"").unwrap();
    write_mapfile(
        &dir.join("MyDisc.iso.mapfile"),
        4096,
        libfreemkv::disc::mapfile::SectorStatus::Finished,
    );

    let hint = make_hint(
        dir.clone(),
        ResumeAction::ResumePreserved {
            attempt: 1,
            has_iso: true,
            has_mapfile: true,
            has_mkv: false,
        },
    );
    match classify_resume(&hint, 0) {
        ResumeClass::Remux {
            iso_path,
            mapfile_path,
            display_name,
        } => {
            assert_eq!(iso_path, dir.join("MyDisc.iso"));
            assert_eq!(mapfile_path, dir.join("MyDisc.iso.mapfile"));
            assert_eq!(display_name, "MyDisc");
        }
        other => panic!("expected Remux, got {:?}", other),
    }
}

#[test]
fn resume_classifies_partial_mapfile_as_not_remux() {
    let td = tmpdir();
    let dir = td.path().join("MyDisc");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("MyDisc.iso"), b"").unwrap();
    // NonTried = pending → bytes_pending != 0 → ineligible.
    write_mapfile(
        &dir.join("MyDisc.iso.mapfile"),
        4096,
        libfreemkv::disc::mapfile::SectorStatus::NonTried,
    );

    let hint = make_hint(
        dir,
        ResumeAction::ResumePreserved {
            attempt: 1,
            has_iso: true,
            has_mapfile: true,
            has_mkv: false,
        },
    );
    assert!(matches!(
        classify_resume(&hint, 0),
        ResumeClass::NotEligible
    ));
}

#[test]
fn resume_classifies_missing_iso_as_not_remux() {
    let td = tmpdir();
    let dir = td.path().join("MyDisc");
    std::fs::create_dir_all(&dir).unwrap();
    // mapfile only — no ISO.
    write_mapfile(
        &dir.join("MyDisc.iso.mapfile"),
        4096,
        libfreemkv::disc::mapfile::SectorStatus::Finished,
    );
    let hint = make_hint(
        dir,
        ResumeAction::ResumePreserved {
            attempt: 1,
            has_iso: false,
            has_mapfile: true,
            has_mkv: false,
        },
    );
    assert!(matches!(
        classify_resume(&hint, 0),
        ResumeClass::NotEligible
    ));
}

#[test]
fn resume_remux_deletes_partial_mkv() {
    // delete_partial_output is the cleanup helper invoked at the top
    // of resume_remux. The full run_mux happy path needs a real ISO,
    // which the live test bed exercises; here we just confirm the
    // pre-mux cleanup is correct and idempotent.
    let td = tmpdir();
    let staging = td.path().join("MyDisc");
    std::fs::create_dir_all(&staging).unwrap();
    let mkv = staging.join("MyDisc.mkv");
    let m2ts = staging.join("MyDisc.m2ts");
    std::fs::write(&mkv, b"partial").unwrap();
    std::fs::write(&m2ts, b"partial").unwrap();

    delete_partial_output(&staging, "MyDisc");

    assert!(!mkv.exists(), "MKV should be deleted");
    assert!(!m2ts.exists(), "m2ts should be deleted");
    // Idempotent — calling twice with everything gone must not panic.
    delete_partial_output(&staging, "MyDisc");
}

#[test]
fn resume_remux_writes_completed_marker_on_success() {
    // Driving `run_mux` to success requires a real UDF ISO. Instead
    // confirm that the marker-write helpers we delegate to on the
    // success path do what resume_remux expects (and that we share
    // the SAME helpers rip_disc uses — no parallel codepath).
    let td = tmpdir();
    let staging = td.path().join("MyDisc");
    std::fs::create_dir_all(&staging).unwrap();
    // Pre-populate a restart_count to verify clear_restart_count.
    std::fs::write(staging.join(RESTART_COUNT_FILE), b"2\n").unwrap();
    assert_eq!(staging::restart_count(&staging), 2);

    // Same two calls resume_remux makes on success.
    staging::write_completed_marker(&staging);
    staging::clear_restart_count(&staging);

    assert!(
        staging.join(".completed").exists(),
        ".completed marker must be present"
    );
    assert_eq!(
        staging::restart_count(&staging),
        0,
        ".restart_count must be cleared"
    );
}

#[test]
fn resume_remux_preserves_state_on_classifier_rejection() {
    // The orchestrator must NOT clear .restart_count when the
    // classifier rejects. Guards the 3-strike rule against an
    // accidental "everything looks fine to keep retrying forever"
    // bug if a future classifier tweak silently downgrades a
    // legitimate Remux to NotEligible.
    let td = tmpdir();
    let dir = td.path().join("MyDisc");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(RESTART_COUNT_FILE), b"1\n").unwrap();
    // Hint with NO mapfile → NotEligible.
    let hint = make_hint(
        dir.clone(),
        ResumeAction::ResumePreserved {
            attempt: 2,
            has_iso: true,
            has_mapfile: false,
            has_mkv: false,
        },
    );
    assert!(matches!(
        classify_resume(&hint, 0),
        ResumeClass::NotEligible
    ));
    // Counter must NOT have been touched by classify_resume.
    assert_eq!(staging::restart_count(&dir), 1);
}

/// Write a mapfile whose sectors are fully settled (bytes_pending == 0) but
/// contain some Unreadable bytes. Used to simulate a disc with bad sectors
/// that are entirely outside the main title.
fn write_mapfile_with_unreadable(path: &Path, total_bytes: u64, unreadable_bytes: u64) {
    use libfreemkv::disc::mapfile::{Mapfile, SectorStatus};
    assert!(
        unreadable_bytes < total_bytes,
        "unreadable_bytes must be less than total_bytes"
    );
    let good_bytes = total_bytes - unreadable_bytes;
    let mut map = Mapfile::create(path, total_bytes, "test").expect("mapfile create");
    map.record(0, good_bytes, SectorStatus::Finished)
        .expect("record good");
    map.record(good_bytes, unreadable_bytes, SectorStatus::Unreadable)
        .expect("record unreadable");
    map.flush().expect("mapfile flush");
}

/// Regression: abort_on_lost_secs==0 with whole-disc unreadable bytes must
/// still classify as Remux. Pre-fix, the coarse pre-filter would convert
/// the whole-disc bad-byte count to estimated lost-seconds and return
/// NotEligible whenever any unreadable bytes were present — even though those
/// sectors might be entirely outside the main title. The real per-title check
/// in `resume_remux` (run after `scan_image`) is the authoritative gate.
#[test]
fn classify_resume_allows_out_of_title_damage_when_abort_on_lost_secs_is_zero() {
    let td = tmpdir();
    let dir = td.path().join("MyDisc");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("MyDisc.iso"), b"").unwrap();

    // Disc: 50 MB total, 1 MB unreadable — enough whole-disc lost-secs
    // to have been blocked by the old pre-filter under abort_on_lost_secs=0.
    let total: u64 = 50 * 1024 * 1024;
    let bad: u64 = 1024 * 1024;
    write_mapfile_with_unreadable(&dir.join("MyDisc.iso.mapfile"), total, bad);

    let hint = make_hint(
        dir.clone(),
        ResumeAction::ResumePreserved {
            attempt: 1,
            has_iso: true,
            has_mapfile: true,
            has_mkv: false,
        },
    );

    // abort_on_lost_secs=0 → pre-filter must ALLOW; real decision deferred
    // to the title-scoped check in resume_remux.
    match classify_resume(&hint, 0) {
        ResumeClass::Remux { display_name, .. } => {
            assert_eq!(display_name, "MyDisc");
        }
        other => panic!(
            "expected Remux (out-of-title damage should not block at pre-filter), got {:?}",
            other
        ),
    }
}

/// Complementary: abort_on_lost_secs>0 keeps the coarse whole-disc
/// pre-filter — a disc whose estimated whole-disc loss already exceeds the
/// threshold is still rejected early (avoids scan_image overhead).
#[test]
fn classify_resume_rejects_heavy_damage_when_abort_on_lost_secs_positive() {
    let td = tmpdir();
    let dir = td.path().join("MyDisc");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("MyDisc.iso"), b"").unwrap();

    // Use a threshold of 1 second. The fallback bitrate is 8.25 MB/s, so
    // 1 s ≈ 8.25 MB. Write 20 MB unreadable — well above the threshold.
    let total: u64 = 100 * 1024 * 1024;
    let bad: u64 = 20 * 1024 * 1024;
    write_mapfile_with_unreadable(&dir.join("MyDisc.iso.mapfile"), total, bad);

    let hint = make_hint(
        dir,
        ResumeAction::ResumePreserved {
            attempt: 1,
            has_iso: true,
            has_mapfile: true,
            has_mkv: false,
        },
    );

    // abort_on_lost_secs=1 → coarse pre-filter fires; must reject.
    assert!(
        matches!(classify_resume(&hint, 1), ResumeClass::NotEligible),
        "heavy whole-disc damage should be rejected as NotEligible when abort_on_lost_secs>0"
    );
}
