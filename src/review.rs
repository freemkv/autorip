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
    std::fs::read_dir(dir).ok()?.flatten().find_map(|e| {
        let p = e.path();
        let ext = p.extension().and_then(|x| x.to_str()).unwrap_or("");
        matches!(ext, "mkv" | "m2ts").then(|| {
            p.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string()
        })
    })
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
        let year = m["year"].as_u64().unwrap_or(0) as u16;
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
/// * `Cancel`             — drop `.review`, mark `.failed` so it isn't retried.
pub enum Resolve {
    Proceed,
    Retitle { title: String, year: u16 },
    Cancel,
}

pub fn resolve(staging_root: &str, dir: &str, action: Resolve) -> Result<(), String> {
    // Path-traversal guard: a held-rip handle is a single dir name.
    if dir.is_empty() || dir.contains('/') || dir.contains("..") {
        return Err("invalid dir".into());
    }
    let d: PathBuf = Path::new(staging_root).join(dir);
    let review = d.join(".review");
    if !d.is_dir() || !review.exists() {
        return Err("not a held rip".into());
    }
    match action {
        Resolve::Proceed => {
            std::fs::rename(&review, d.join(".done")).map_err(|e| e.to_string())?;
        }
        Resolve::Retitle { title, year } => {
            let mut m = read_marker(&d);
            if !m.is_object() {
                m = serde_json::json!({});
            }
            m["title"] = serde_json::json!(title);
            m["year"] = serde_json::json!(year);
            m["media_type"] = serde_json::json!("movie");
            std::fs::write(
                d.join(".done"),
                serde_json::to_string_pretty(&m).unwrap_or_default(),
            )
            .map_err(|e| e.to_string())?;
            let _ = std::fs::remove_file(&review);
        }
        Resolve::Cancel => {
            let _ = std::fs::remove_file(&review);
            let _ = std::fs::write(d.join(".failed"), "cancelled by operator\n");
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
                title: "Civil War".into(),
                year: 2024,
            },
        )
        .unwrap();
        assert!(held.join(".done").exists());
        assert!(!held.join(".review").exists());
        let m: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(held.join(".done")).unwrap()).unwrap();
        assert_eq!(m["title"], "Civil War");
        assert_eq!(m["year"], 2024);
        assert!(list_held(tmp.to_str().unwrap()).is_empty());

        // traversal guard
        assert!(resolve(tmp.to_str().unwrap(), "../etc", Resolve::Proceed).is_err());

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
