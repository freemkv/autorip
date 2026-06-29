fn main() {
    emit_git_suffix();
}

/// Bake the autorip git short hash into the build as `GIT_SUFFIX` so the
/// RUNNING build is identifiable everywhere autorip reports its version (UI
/// footer, `/api/version`, startup log, `--version`) — e.g. `1.1.1 (g2014a41)`,
/// the same shape libfreemkv stamps into every MKV. This is what tells a
/// hand-deployed test build apart from the released image. `AUTORIP_BUILD_LABEL`
/// overrides the Cargo package version (when set non-empty) so a pre-release /
/// test build can stamp a label without bumping Cargo.toml. Empty suffix when
/// git or the repo is unavailable; always emitted so `env!("GIT_SUFFIX")`
/// resolves on every target.
fn emit_git_suffix() {
    let version = std::env::var("AUTORIP_BUILD_LABEL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("CARGO_PKG_VERSION").ok())
        .unwrap_or_default();
    println!("cargo:rustc-env=AUTORIP_VERSION={version}");
    println!("cargo:rerun-if-env-changed=AUTORIP_BUILD_LABEL");

    let suffix = git_short_hash()
        .map(|h| format!(" (g{h})"))
        .unwrap_or_default();
    println!("cargo:rustc-env=GIT_SUFFIX={suffix}");

    // Re-run when HEAD (or the branch it points at) moves so the stamp stays
    // current without a clean rebuild.
    println!("cargo:rerun-if-changed=.git/HEAD");
    if let Ok(head) = std::fs::read_to_string(".git/HEAD") {
        if let Some(ref_path) = head.strip_prefix("ref: ") {
            println!("cargo:rerun-if-changed=.git/{}", ref_path.trim());
        }
    }
}

fn git_short_hash() -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["rev-parse", "--short=7", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let h = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if h.is_empty() { None } else { Some(h) }
}
