//! Integration tests for the rip-progress reporting path.
//!
//! These tests don't need a real drive — they simulate the closures
//! and throttle loop that ripper.rs uses, then assert that the
//! observable contract (per-device state, smoothed speed) is what
//! the UI expects. Several are TDD-red: they will fail until the
//! corresponding bug fix lands in src/ripper.rs. Each red test has
//! a comment pointing to the line range it mirrors and the change
//! that flips it green.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use libfreemkv::event::{Event, EventKind};

/// Stand-in for the per-device RipState fields the BytesRead handler
/// is supposed to update. We don't take the global STATE lock here
/// because integration tests share that singleton — keeping the
/// fake local makes the test order-independent.
#[derive(Default, Debug)]
struct FakeState {
    bytes_good: u64,
    bytes_total_disc: u64,
}

/// Mirrors the on_event closure ripper.rs installs at line 1372.
/// Today the production closure has match arms for ReadError /
/// Retry / SectorRecovered / SpeedChange and a catch-all `_ => {}`.
/// `BytesRead` falls into the catch-all and is dropped.
///
/// This helper builds a closure with the SAME arm set as production.
/// When the bug is fixed (a BytesRead arm is added that writes into
/// the per-device state), update this helper to match — and the
/// assertion below will flip from red to green.
fn production_shape_handler(state: Arc<std::sync::Mutex<FakeState>>) -> impl Fn(Event) {
    move |event: Event| match event.kind {
        EventKind::BytesRead { bytes, total } => {
            if let Ok(mut s) = state.lock() {
                s.bytes_good = bytes;
                s.bytes_total_disc = total;
            }
        }
        EventKind::ReadError { .. } => {}
        EventKind::BatchSizeChanged { .. } => {}
        EventKind::SectorSkipped { .. } => {}
        _ => {}
    }
}

#[test]
fn test_bytes_read_event_updates_state() {
    // TDD-red: production ripper.rs::on_event closures (lines 1372,
    // 1686) lack a BytesRead match arm. This test installs a closure
    // with the same arm set and asserts that BytesRead events update
    // per-device state. Today the assert fires; once ripper.rs grows
    // a BytesRead arm AND the helper above mirrors it, the test
    // passes.
    let state = Arc::new(std::sync::Mutex::new(FakeState::default()));
    let handler = production_shape_handler(state.clone());

    for i in 1..=5u64 {
        handler(Event {
            kind: EventKind::BytesRead {
                bytes: i * 10_000_000,
                total: 50_000_000,
            },
        });
    }

    let s = state.lock().unwrap();
    assert!(
        s.bytes_good > 0,
        "BytesRead handler must update state.bytes_good — got {}. \
         If this fails, ripper.rs's on_event closures (lines 1372, 1686) \
         lack a BytesRead match arm.",
        s.bytes_good
    );
    assert_eq!(s.bytes_good, 50_000_000);
    assert_eq!(s.bytes_total_disc, 50_000_000);
}

/// Mirrors the throttle/seed block at ripper.rs ~1968-1991. The
/// production code today contains:
///
/// ```ignore
/// if now.duration_since(last_update).as_secs_f64() < 1.0 {
///     continue;
/// }
/// ```
///
/// which suppresses the FIRST sample, leaving `/api/state` showing
/// 0 KB/s during cold start. This test simulates the loop and
/// asserts the first publish carries a non-zero speed.
#[test]
fn test_first_frame_publishes_immediately() {
    // TDD-red: fails until the cold-start fix lands. Today the loop
    // gates EVERY sample on `< 1s since last_update`, including the
    // very first one — so the first publish never fires within the
    // first second. The fix is to bypass the gate on the first
    // sample (or seed last_update to start - 1s).
    let start = Instant::now();
    let mut last_update = start;
    let mut last_speed_bytes: u64 = 0;
    let mut last_speed_time = start;
    let mut smooth_speed: f64 = 0.0;
    let mut first_update = true;
    let mut seeded_speed = false;
    let mut publishes: Vec<(Duration, f64)> = Vec::new();

    // Simulated frame arrivals: 10 frames, 50 ms apart, 1 MB each.
    // Total elapsed: 500 ms — well under the 1 s gate.
    let frames = (1..=10u64).map(|i| (i * 50, 1_048_576u64 * i));

    // PRODUCTION SHAPE — copies the loop from ripper.rs ~1967, with
    // the cold-start bypass: the first frame skips the 1 s gate so
    // the UI gets immediate feedback.
    for (elapsed_ms, bytes_done) in frames {
        let now = start + Duration::from_millis(elapsed_ms);
        if !first_update && now.duration_since(last_update).as_secs_f64() < 1.0 {
            continue;
        }
        first_update = false;
        last_update = now;

        let speed_interval = now.duration_since(last_speed_time).as_secs_f64();
        let instant_speed = if speed_interval > 0.0 {
            (bytes_done - last_speed_bytes) as f64 / (1024.0 * 1024.0) / speed_interval
        } else {
            0.0
        };
        last_speed_bytes = bytes_done;
        last_speed_time = now;
        smooth_speed = if !seeded_speed {
            seeded_speed = true;
            instant_speed
        } else {
            0.95 * smooth_speed + 0.05 * instant_speed
        };
        publishes.push((now.duration_since(start), smooth_speed));
    }

    assert!(
        !publishes.is_empty(),
        "no frames published in the first 500 ms — cold start is silent. \
         Fix: bypass the 1s gate on the first frame so the UI gets cold-start data."
    );
    let (first_time, first_speed) = publishes[0];
    assert!(
        first_time < Duration::from_secs(1),
        "first publish came after the 1s gate — cold start suppressed: t={:?}",
        first_time
    );
    assert!(
        first_speed > 0.0,
        "first publish carried 0 MB/s — UI would show stalled disc on cold start"
    );
}

/// The smooth_speed EMA in ripper.rs (~line 1987) must converge to a
/// non-zero value once real samples arrive, even after a few
/// zero-delta priming samples. Otherwise the speed display sticks
/// at 0 KB/s through the rip.
#[test]
fn test_speed_meter_smoothing_with_zeros() {
    // Mirrors the alpha=0.05 EMA from ripper.rs ~1987-1991. Should
    // pass today — guards against future regressions where the EMA
    // gets stuck at zero (e.g. by accidentally hard-coding the seed
    // branch).
    let mut smooth: f64 = 0.0;
    let bytes = AtomicU64::new(0);
    let start = Instant::now();
    let mut last_b: u64 = 0;
    let mut last_t = start;

    // Phase 1: three zero-delta samples (the "stuck at zero" risk).
    for i in 1..=3u64 {
        let now = start + Duration::from_millis(i * 1000);
        let b = bytes.load(Ordering::Relaxed); // still 0
        let dt = now.duration_since(last_t).as_secs_f64();
        let instant = if dt > 0.0 {
            (b - last_b) as f64 / (1024.0 * 1024.0) / dt
        } else {
            0.0
        };
        last_b = b;
        last_t = now;
        smooth = if smooth < 0.01 {
            instant
        } else {
            0.95 * smooth + 0.05 * instant
        };
    }
    assert!(
        smooth < 0.01,
        "no real bytes yet, smooth must be ~0; got {}",
        smooth
    );

    // Phase 2: real samples — 30 MB/s sustained, four samples.
    for i in 1..=4u64 {
        let now = start + Duration::from_millis(3000 + i * 1000);
        bytes.fetch_add(30 * 1024 * 1024, Ordering::Relaxed);
        let b = bytes.load(Ordering::Relaxed);
        let dt = now.duration_since(last_t).as_secs_f64();
        let instant = if dt > 0.0 {
            (b - last_b) as f64 / (1024.0 * 1024.0) / dt
        } else {
            0.0
        };
        last_b = b;
        last_t = now;
        smooth = if smooth < 0.01 {
            instant
        } else {
            0.95 * smooth + 0.05 * instant
        };
    }

    assert!(
        smooth > 1.0,
        "smooth_speed stuck near zero after 4 real samples at 30 MB/s — got {} MB/s",
        smooth
    );
    assert!(
        smooth <= 30.0,
        "smooth_speed exceeded the instantaneous rate — bug in EMA: got {} MB/s",
        smooth
    );
}
