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

pub mod config;
pub mod history;
pub mod log;
pub mod mover;
pub mod observe;
pub mod ripper;
pub mod tmdb;
pub mod util;
pub mod verify;
pub mod web;
pub mod webhook;
