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

/// Write a placeholder ISO of exactly `size_bytes` so it satisfies the
/// resume classifier's ISO-size gate (a settled Pass-1 ISO must be at least
/// as large as the mapfile claims). The bytes don't matter — classify_resume
/// only stats the length, it doesn't read content.
fn write_iso(path: &Path, size_bytes: u64) {
    let f = std::fs::File::create(path).expect("iso create");
    f.set_len(size_bytes).expect("iso set_len");
}

fn write_mapfile(path: &Path, size_bytes: u64, status: freemkv_engine::SectorStatus) {
    use freemkv_engine::Mapfile;
    let mut map = Mapfile::create(path, size_bytes, "test").expect("mapfile create");
    map.record(0, size_bytes, status).expect("mapfile record");
    map.flush().expect("mapfile flush");
}

#[test]
fn resume_classifies_clean_mapfile_as_remux() {
    let td = tmpdir();
    let dir = td.path().join("MyDisc");
    std::fs::create_dir_all(&dir).unwrap();
    write_iso(&dir.join("MyDisc.iso"), 4096);
    write_mapfile(
        &dir.join("MyDisc.iso.mapfile"),
        4096,
        freemkv_engine::SectorStatus::Finished,
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
            ..
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
    write_iso(&dir.join("MyDisc.iso"), 4096);
    // NonTried = pending → bytes_pending != 0 → ineligible.
    write_mapfile(
        &dir.join("MyDisc.iso.mapfile"),
        4096,
        freemkv_engine::SectorStatus::NonTried,
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
fn resume_classifies_short_iso_as_not_remux() {
    // Regression for the truncated-ISO / short total_size case: a settled
    // mapfile (bytes_pending==0) whose ISO is SHORTER than its declared
    // total_size means the ISO is incomplete (or the mapfile undercounts the
    // disc, hiding NonTried tail sectors). Either way, jumping to mux would
    // emit a truncated/zero-filled movie — reject and re-sweep fresh.
    let td = tmpdir();
    let dir = td.path().join("MyDisc");
    std::fs::create_dir_all(&dir).unwrap();
    // Mapfile claims 4096 bytes, but the ISO is only 2048 bytes long.
    write_iso(&dir.join("MyDisc.iso"), 2048);
    write_mapfile(
        &dir.join("MyDisc.iso.mapfile"),
        4096,
        freemkv_engine::SectorStatus::Finished,
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
        freemkv_engine::SectorStatus::Finished,
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
    use freemkv_engine::{Mapfile, SectorStatus};
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
    // Disc: 50 MB total, 1 MB unreadable — enough whole-disc lost-secs
    // to have been blocked by the old pre-filter under abort_on_lost_secs=0.
    let total: u64 = 50 * 1024 * 1024;
    let bad: u64 = 1024 * 1024;
    write_iso(&dir.join("MyDisc.iso"), total);
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
    // Use a threshold of 1 second. The fallback bitrate is 8.25 MB/s, so
    // 1 s ≈ 8.25 MB. Write 20 MB unreadable — well above the threshold.
    let total: u64 = 100 * 1024 * 1024;
    let bad: u64 = 20 * 1024 * 1024;
    write_iso(&dir.join("MyDisc.iso"), total);
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

/// Tight boundary check on the pre-filter's `lost_secs > abort_on_lost_secs`
/// gate. The two tests above use damage an order of magnitude past the
/// threshold, so a `/` → `%` mutant on `bad_bytes as f64 /
/// FALLBACK_BITRATE_BYTES_PER_SEC` still lands on the reject side by
/// accident (both a correct ~2.4x-over lost-secs value and a `%`-corrupted
/// one exceed a 1-second threshold). Pin the exact arithmetic instead:
/// `FALLBACK_BITRATE_BYTES_PER_SEC` is 8_250_000.0 bytes/sec, so at a
/// 10-second threshold the boundary is exactly 82_500_000 bytes. One byte
/// under must classify as Remux (deferred to the real per-title check);
/// exactly at the threshold must ALSO defer (`>`, not `>=` — the code comment
/// establishes the gate is strictly-greater); one byte over must reject.
/// A `%` in place of `/` turns 82_500_001 % 8_250_000 == 1, which is nowhere
/// near 10 and would wrongly classify as Remux; a `*` in place of `/` turns
/// even 82_499_999 bytes into an astronomically large "lost_secs" and would
/// wrongly reject. Either mutant flips one of the three assertions below.
#[test]
fn classify_resume_pre_filter_boundary_is_strictly_greater_than() {
    const FALLBACK_BITRATE_BYTES_PER_SEC: u64 = 8_250_000;
    let abort_on_lost_secs: u64 = 10;
    let boundary_bytes = FALLBACK_BITRATE_BYTES_PER_SEC * abort_on_lost_secs; // 82_500_000
    let total: u64 = boundary_bytes * 4;

    let classify_at = |bad_bytes: u64| -> ResumeClass {
        let td = tmpdir();
        let dir = td.path().join("MyDisc");
        std::fs::create_dir_all(&dir).unwrap();
        write_iso(&dir.join("MyDisc.iso"), total);
        write_mapfile_with_unreadable(&dir.join("MyDisc.iso.mapfile"), total, bad_bytes);
        let hint = make_hint(
            dir,
            ResumeAction::ResumePreserved {
                attempt: 1,
                has_iso: true,
                has_mapfile: true,
                has_mkv: false,
            },
        );
        classify_resume(&hint, abort_on_lost_secs)
    };

    assert!(
        matches!(classify_at(boundary_bytes - 1), ResumeClass::Remux { .. }),
        "one byte under the threshold must defer to the per-title check, not reject"
    );
    assert!(
        matches!(classify_at(boundary_bytes), ResumeClass::Remux { .. }),
        "exactly at the threshold the gate is strictly-greater, so this must still defer"
    );
    assert!(
        matches!(classify_at(boundary_bytes + 1), ResumeClass::NotEligible),
        "one byte over the threshold must reject at the pre-filter"
    );
}

/// Cold resume must hand `resume_remux` a FILE basename, not the staging
/// DIRECTORY name.
///
/// `rip_disc` documents the split explicitly (`src/ripper/mod.rs`, where
/// `filename` is built): the staging DIR carries the `_2` disc suffix that
/// separates the discs of a boxset, but the FILES inside it are named from the
/// plain title with no suffix, because `delete_partial_output` looks for
/// `<dir>/<display_name>.<ext>` and the mover derives the delivered filename
/// from the staged one.
///
/// `classify_resume` was taking `hint.dir.file_name()` — the suffixed
/// directory name — so on a boxset variant dir it looked for the partial under
/// the wrong name, left it in place, and muxed a SECOND file next to it. Both
/// then carry a `.done` hand-off and the mover delivers both.
#[test]
fn cold_resume_of_a_boxset_variant_dir_uses_the_file_basename_not_the_dir_name() {
    let td = tmpdir();
    // The staging dir for disc 2 of a set: sanitized title + `_2`.
    let dir = td.path().join("Boxset Movie_2");
    std::fs::create_dir_all(&dir).unwrap();
    // The files inside it are named from the plain title — no suffix.
    write_iso(&dir.join("Boxset Movie.iso"), 4096);
    write_mapfile(
        &dir.join("Boxset Movie.iso.mapfile"),
        4096,
        freemkv_engine::SectorStatus::Finished,
    );
    // A partial mux left behind by the interrupted attempt.
    let partial = dir.join("Boxset Movie.mkv");
    std::fs::write(&partial, b"partial mux output").unwrap();

    let hint = make_hint(
        dir.clone(),
        ResumeAction::ResumePreserved {
            attempt: 1,
            has_iso: true,
            has_mapfile: true,
            has_mkv: true,
        },
    );
    let display_name = match classify_resume(&hint, 0) {
        ResumeClass::Remux { display_name, .. } => display_name,
        other => panic!("expected Remux, got {:?}", other),
    };

    assert_eq!(
        display_name, "Boxset Movie",
        "cold resume passed the staging DIRECTORY name (with its `_2` disc suffix) as the \
         file basename; the invariant in rip_disc is that files inside the dir carry no suffix"
    );

    // What resume_remux does with it, in order: clear the partial, then mux to
    // `<dir>/<display_name>.<ext>`.
    delete_partial_output(&dir, &display_name);
    assert!(
        !partial.exists(),
        "the stale partial mux output was not cleared — resume looked for it under the \
         suffixed directory name"
    );
    std::fs::write(dir.join(format!("{display_name}.mkv")), b"remuxed").unwrap();

    let mkvs: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".mkv"))
        .collect();
    assert_eq!(
        mkvs.len(),
        1,
        "cold resume produced {} MKVs in one staging dir ({:?}) — the mover delivers BOTH",
        mkvs.len(),
        mkvs
    );
}
