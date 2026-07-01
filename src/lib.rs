//! Library facade for the autorip binary.
//!
//! Exists so integration tests under `tests/` can reach into the
//! same modules the binary uses (e.g. `ripper::request_stop`,
//! `ripper::RipState`). The binary in `src/main.rs` continues to
//! declare its own `mod` graph; this file mirrors that graph as
//! `pub mod` so external consumers (tests) can name the items.
//!
//! Keep this file purely declarative — no logic, almost no statics.
//!
//! The one exception is `SHUTDOWN`: several modules reference
//! `crate::SHUTDOWN` directly, so the lib crate has to provide its
//! own copy. The bin still owns the authoritative one in `main.rs`;
//! the lib copy only matters for integration tests, which never
//! actually drive the long-running loops that read it.

use std::sync::atomic::AtomicBool;

pub static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Full build label (package version + git short hash, e.g. `1.1.1 (g2014a41)`).
/// Mirrors the `main.rs` const for the same reason `SHUTDOWN` is duplicated:
/// modules reference `crate::VERSION_LABEL`, so both crate roots must define it.
/// Built by `build.rs`.
pub const VERSION_LABEL: &str = concat!(env!("AUTORIP_VERSION"), env!("GIT_SUFFIX"));

pub mod config;
pub mod keysource;
pub mod log;
pub mod mover;
pub mod muxer;
pub mod observe;
pub mod review;
pub mod ripper;
pub mod tmdb;
pub mod util;
pub mod web;
pub mod webhook;
