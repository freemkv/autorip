//! Needs-review queue: rips the ripper held back because the title match wasn't
//! confident (see `ripper::rip_disc`). A held rip is a staging dir that has a
//! `.review` marker but no `.done` — so the mover skips it (it only promotes
//! `.done` dirs). The operator resolves each one here: **proceed** as-named,
//! **retitle** (pick the correct movie), or **cancel**.
//!
//! Everything keys off marker files on disk, so held rips survive a restart and
//! never block the drive (the rip is already complete and staged).

use std::path::{Path, PathBuf};

/// One rip awaiting operator review.
#[derive(Debug, Clone, serde::Serialize)]
pub struct HeldRip {
    /// Staging subdir name (the handle used to resolve it).
    pub dir: String,
    /// Title the ripper resolved (the uncertain guess).
    pub title: String,
    /// Year the ripper resolved (0 = none — a common reason it's held).
    pub year: u16,
    /// The ripped media file inside the dir (for display).
    pub file: String,
    /// Why it's held (human-readable).
    pub reason: String,
}

fn read_marker(dir: &Path) -> serde_json::Value {
    std::fs::read_to_string(dir.join(".review"))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or(serde_json::Value::Null)
}

fn media_file(dir: &Path) -> Option<String> {
    // read_dir order is platform-dependent, so when a dir holds more than
    // one media file pick deterministically (lexicographically smallest)
    // rather than returning an arbitrary one. Display-only.
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .filter_map(|e| {
            let p = e.path();
            let ext = p.extension().and_then(|x| x.to_str()).unwrap_or("");
            matches!(ext, "mkv" | "m2ts")
                .then(|| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|s| s.to_string())
                })
                .flatten()
        })
        .collect();
    names.sort();
    names.into_iter().next()
}

/// List every held rip under `staging_root` (a `.review` marker, no `.done`).
pub fn list_held(staging_root: &str) -> Vec<HeldRip> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(staging_root) else {
        return out;
    };
    for e in entries.flatten() {
        let dir = e.path();
        if !dir.is_dir() || !dir.join(".review").exists() || dir.join(".done").exists() {
            continue;
        }
        let m = read_marker(&dir);
        let title = m["title"].as_str().unwrap_or("").to_string();
        // Range-validate rather than a truncating `as u16`: a corrupt /
        // hand-edited marker with year > 65535 would otherwise WRAP (e.g.
        // 70000 → 4464) and mislabel the held rip. Out-of-range → 0
        // ("no confident year"), the same as a missing field.
        let year = m["year"]
            .as_u64()
            .and_then(|y| u16::try_from(y).ok())
            .unwrap_or(0);
        out.push(HeldRip {
            dir: dir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string(),
            title,
            year,
            file: media_file(&dir).unwrap_or_default(),
            reason: if year == 0 {
                "no confident title/year match".into()
            } else {
                "uncertain title match".into()
            },
        });
    }
    out.sort_by(|a, b| a.dir.cmp(&b.dir));
    out
}

/// Resolve a held rip. `dir` is the staging subdir name (not a path — guarded
/// against traversal). Actions:
/// * `Proceed`            — promote `.review` → `.done` as-named.
/// * `Retitle{title,year}`— rewrite the marker's title/year, then `.done`.
/// * `Cancel`             — mark `.failed` (so it isn't retried), then drop `.review`.
pub enum Resolve {
    Proceed,
    Retitle { title: String, year: u16 },
    Cancel,
}

pub fn resolve(staging_root: &str, dir: &str, action: Resolve) -> Result<(), String> {
    // Path-traversal guard: a held-rip handle is a single staging subdir
    // name. Inspect path components rather than substring-matching `..` —
    // a substring check wrongly rejects legitimate titles like
    // `Blade..Runner (1982)` while a component check still rejects `..`,
    // absolute paths, a bare `.`, and any nested path.
    if dir.is_empty()
        || Path::new(dir).components().count() != 1
        || Path::new(dir)
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        return Err("invalid dir".into());
    }
    let d: PathBuf = Path::new(staging_root).join(dir);
    let review = d.join(".review");
    if !d.is_dir() || !review.exists() {
        return Err("not a held rip".into());
    }
    match action {
        Resolve::Proceed => {
            // Carry the marker's JSON forward into a DURABLE `.done` rather than
            // a bare rename (rename alone doesn't fsync the new dirent — a crash
            // can lose it, leaving the rip invisible to the mover). Write `.done`
            // durably, then remove `.review`.
            // Read the override marker BEFORE writing `.done` / removing
            // `.review`. A swallowed read error (e.g. a transient NFS failure)
            // would otherwise write an EMPTY `.done` and delete `.review` —
            // permanent loss of the user's override. Short-circuit instead.
            let body = std::fs::read(&review).map_err(|e| e.to_string())?;
            crate::ripper::staging::write_handoff_marker(&d.join(".done"), &body)
                .map_err(|e| e.to_string())?;
            std::fs::remove_file(&review).map_err(|e| e.to_string())?;
        }
        Resolve::Retitle { title, year } => {
            // Reject a blank title at the library boundary: `resolve` is a
            // pub fn and an empty/whitespace title would write a `.done` the
            // mover promotes with no name. The web caller already guards
            // this, but a future/non-web caller must not be able to.
            if title.trim().is_empty() {
                return Err("title required".into());
            }
            let mut m = read_marker(&d);
            if !m.is_object() {
                m = serde_json::json!({});
            }
            m["title"] = serde_json::json!(title);
            m["year"] = serde_json::json!(year);
            // Only default media_type to "movie" when absent — a non-movie
            // marker (e.g. a TV title) must survive a retitle. An
            // unconditional set here would silently rewrite every retitled
            // title as a movie.
            if m.get("media_type").and_then(|v| v.as_str()).is_none() {
                m["media_type"] = serde_json::json!("movie");
            }
            // Propagate a serialization failure instead of writing an empty
            // `.done` that the mover would promote with a blank title.
            let serialized = serde_json::to_string_pretty(&m).map_err(|e| e.to_string())?;
            // Write `.done` before removing `.review` (crash-atomic: a
            // lingering `.review` is harmless since `list_held` excludes
            // dirs that have `.done`), and propagate the removal error so a
            // failed cleanup is visible instead of silently leaving both.
            crate::ripper::staging::write_handoff_marker(&d.join(".done"), serialized.as_bytes())
                .map_err(|e| e.to_string())?;
            std::fs::remove_file(&review).map_err(|e| e.to_string())?;
        }
        Resolve::Cancel => {
            // Write `.failed` BEFORE removing `.review`, and propagate any
            // IO error. The previous order (remove then write, both errors
            // discarded) could leave the dir with no `.review`/`.done`/
            // `.failed` marker at all — invisible to the mover, the UI, and
            // the re-rip guard — while still reporting success to the
            // operator.
            //
            // Write `.failed` as structured JSON `{"reason":...}` rather than a
            // raw non-JSON body. `read_failed_reason` only parses JSON, so the
            // old plain "cancelled by operator\n" body returned None and any
            // terminal-ness check keying on a parseable reason wouldn't fire
            // (M2). We go through `write_handoff_marker` (the durable
            // tmp+fsync+rename writer) rather than `write_failed_marker` because
            // that helper is best-effort/non-propagating, and the contract here
            // requires surfacing a write failure (and preserving `.review`) so a
            // failed cancel isn't reported as success. The JSON shape matches
            // `write_failed_marker`'s so `read_failed_reason` recovers the
            // reason; presence-based checks recognise it regardless.
            let failed_body = serde_json::json!({
                "reason": "cancelled by operator",
                "timestamp": crate::util::format_iso_datetime(),
            });
            let failed_str =
                serde_json::to_string_pretty(&failed_body).map_err(|e| e.to_string())?;
            crate::ripper::staging::write_handoff_marker(&d.join(".failed"), failed_str.as_bytes())
                .map_err(|e| e.to_string())?;
            std::fs::remove_file(&review).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(p: &Path, body: &str) {
        std::fs::write(p, body).unwrap();
    }

    #[test]
    fn lists_only_held_and_resolves() {
        let tmp = std::env::temp_dir().join(format!("autorip-review-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        // held: has .review, no .done
        let held = tmp.join("Some Movie");
        std::fs::create_dir_all(&held).unwrap();
        touch(&held.join(".review"), r#"{"title":"Some Movie","year":0}"#);
        touch(&held.join("Some Movie.mkv"), "x");
        // not held: has .done
        let done = tmp.join("Done Movie (2020)");
        std::fs::create_dir_all(&done).unwrap();
        touch(&done.join(".done"), "{}");

        let held_list = list_held(tmp.to_str().unwrap());
        assert_eq!(held_list.len(), 1);
        assert_eq!(held_list[0].dir, "Some Movie");
        assert_eq!(held_list[0].file, "Some Movie.mkv");
        assert_eq!(held_list[0].year, 0);

        // retitle → .done appears with the new title, .review gone
        resolve(
            tmp.to_str().unwrap(),
            "Some Movie",
            Resolve::Retitle {
                title: "Sample Movie".into(),
                year: 2024,
            },
        )
        .unwrap();
        assert!(held.join(".done").exists());
        assert!(!held.join(".review").exists());
        let m: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(held.join(".done")).unwrap()).unwrap();
        assert_eq!(m["title"], "Sample Movie");
        assert_eq!(m["year"], 2024);
        assert!(list_held(tmp.to_str().unwrap()).is_empty());

        // traversal guard
        assert!(resolve(tmp.to_str().unwrap(), "../etc", Resolve::Proceed).is_err());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn traversal_guard_rejects_escapes_accepts_dotted_titles() {
        // Component-based guard: reject anything that isn't a single
        // normal path component...
        for bad in ["..", ".", "../etc", "a/b", "/abs", "", "./x"] {
            assert!(
                resolve("/nonexistent-staging-root", bad, Resolve::Proceed).is_err(),
                "should reject {bad:?}"
            );
        }
        // ...but a legitimate title containing `..` is NOT a traversal and
        // must pass the guard (it fails later only because the dir/marker
        // doesn't exist — "not a held rip", not "invalid dir").
        let err = resolve(
            "/nonexistent-staging-root",
            "Blade..Runner (1982)",
            Resolve::Proceed,
        )
        .unwrap_err();
        assert_eq!(err, "not a held rip", "dotted title must clear the guard");
    }

    #[test]
    fn proceed_carries_marker_body_into_durable_done() {
        // Regression (finding 7): Proceed now writes a DURABLE `.done`
        // (write_handoff_marker: tmp + fsync + rename + dir-fsync) carrying the
        // `.review` JSON forward, instead of a bare rename that doesn't fsync
        // the new dirent. The `.review` is removed and the `.done` keeps the
        // body so the mover sees the title.
        let tmp = std::env::temp_dir().join(format!(
            "autorip-review-proceed-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let held = tmp.join("Keeper (2019)");
        std::fs::create_dir_all(&held).unwrap();
        let body = r#"{"title":"Keeper","year":2019,"media_type":"movie"}"#;
        touch(&held.join(".review"), body);

        resolve(tmp.to_str().unwrap(), "Keeper (2019)", Resolve::Proceed).unwrap();

        assert!(held.join(".done").exists(), ".done must be written");
        assert!(!held.join(".review").exists(), ".review must be removed");
        let m: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(held.join(".done")).unwrap()).unwrap();
        assert_eq!(m["title"], "Keeper", "marker body carried into .done");
        assert_eq!(m["year"], 2019);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn cancel_propagates_write_error_and_preserves_review() {
        // If `.failed` can't be written, Cancel must return Err and leave
        // `.review` intact (so the rip is still visibly held), rather than
        // reporting success after dropping the only marker.
        let tmp = std::env::temp_dir().join(format!(
            "autorip-review-cancel-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let held = tmp.join("Held");
        std::fs::create_dir_all(&held).unwrap();
        touch(&held.join(".review"), r#"{"title":"Held","year":0}"#);

        // Make `.failed` un-writable by pre-creating it as a directory, so
        // std::fs::write fails (can't truncate/open a dir as a file).
        std::fs::create_dir(held.join(".failed")).unwrap();

        let res = resolve(tmp.to_str().unwrap(), "Held", Resolve::Cancel);
        assert!(res.is_err(), "cancel must surface the write failure");
        // `.review` must survive so the rip stays held, not orphaned.
        assert!(held.join(".review").exists(), ".review must be preserved");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn cancel_success_writes_failed_and_drops_review() {
        let tmp = std::env::temp_dir().join(format!(
            "autorip-review-cancelok-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let held = tmp.join("Held");
        std::fs::create_dir_all(&held).unwrap();
        touch(&held.join(".review"), r#"{"title":"Held","year":0}"#);

        resolve(tmp.to_str().unwrap(), "Held", Resolve::Cancel).unwrap();
        assert!(held.join(".failed").exists());
        assert!(!held.join(".review").exists());

        // M2: the `.failed` marker is valid JSON carrying a machine-readable
        // reason, so `read_failed_reason` recovers it (the legacy non-JSON
        // "cancelled by operator\n" body parsed to None and defeated the
        // reason-keyed terminal checks).
        let reason = crate::ripper::staging::read_failed_reason(&held);
        assert_eq!(
            reason.as_deref(),
            Some("cancelled by operator"),
            "cancel must write a JSON .failed whose reason round-trips"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn media_file_is_deterministic_across_multiple() {
        let tmp = std::env::temp_dir().join(format!(
            "autorip-review-media-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        touch(&tmp.join("zeta.mkv"), "x");
        touch(&tmp.join("alpha.mkv"), "x");
        touch(&tmp.join("notes.txt"), "x");
        assert_eq!(media_file(&tmp).as_deref(), Some("alpha.mkv"));
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn retitle_preserves_non_movie_media_type() {
        let tmp =
            std::env::temp_dir().join(format!("autorip-review-mediatype-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let held = tmp.join("Some Show");
        std::fs::create_dir_all(&held).unwrap();
        // Marker already carries a non-movie media_type (e.g. a TV title).
        touch(
            &held.join(".review"),
            r#"{"title":"Some Show","year":0,"media_type":"tv"}"#,
        );
        touch(&held.join("Some Show.mkv"), "x");

        resolve(
            tmp.to_str().unwrap(),
            "Some Show",
            Resolve::Retitle {
                title: "Severance".into(),
                year: 2022,
            },
        )
        .unwrap();

        let m: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(held.join(".done")).unwrap()).unwrap();
        assert_eq!(m["title"], "Severance");
        assert_eq!(m["year"], 2022);
        // The retitle must not clobber the existing non-movie marker.
        assert_eq!(m["media_type"], "tv");

        // And when media_type is absent, retitle defaults it to "movie".
        let held2 = tmp.join("Some Movie");
        std::fs::create_dir_all(&held2).unwrap();
        touch(&held2.join(".review"), r#"{"title":"Some Movie","year":0}"#);
        resolve(
            tmp.to_str().unwrap(),
            "Some Movie",
            Resolve::Retitle {
                title: "Sample Movie".into(),
                year: 2024,
            },
        )
        .unwrap();
        let m2: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(held2.join(".done")).unwrap()).unwrap();
        assert_eq!(m2["media_type"], "movie");

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
