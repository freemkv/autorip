//! Integration tests for the rip-progress reporting path.
//!
//! These drive the REAL production event handlers — the closures
//! `rip_disc` installs on the live drive and on the mux/sweep stream —
//! via the `make_drive_event_fn` / `make_stream_event_fn` factories the
//! orchestrator now calls. Firing real `libfreemkv` events at them and
//! reading back the shared atomics proves the BytesRead→`latest_bytes_read`
//! wiring the `/api/state` speed meter depends on is actually connected.
//!
//! The previous version of this file tested a hand-written
//! `production_shape_handler` replica and a locally re-implemented EMA
//! speed loop — neither of which existed in production. Production has
//! BytesRead arms (the replica's premise that it lacked them was false),
//! stores into an `AtomicU64` (not a struct), and its speed meter is a
//! sliding-window average in `ripper::state::PassProgressState::observe`
//! (NOT an EMA, and its first sample returns 0.0, the opposite of the old
//! "first frame non-zero" assertion). The real speed meter is unit-tested
//! in `src/ripper/state.rs` (`pass_progress_*`); these tests own the
//! event-wiring half that those cannot reach.

use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use freemkv_autorip::ripper::{make_drive_event_fn, make_stream_event_fn};
use libfreemkv::event::{BatchSizeReason, Event, EventKind};

/// The drive-level handler must forward `BytesRead.bytes` into the shared
/// `latest_bytes_read` atomic the UI reads — and must reset the watchdog
/// on every event so a working-but-slow drive isn't declared stalled.
#[test]
fn drive_event_fn_publishes_bytes_read_into_shared_atomic() {
    let wdf = Arc::new(AtomicU64::new(0));
    let latest_bytes_read = Arc::new(AtomicU64::new(0));
    let handler = make_drive_event_fn("sr0".to_string(), wdf.clone(), latest_bytes_read.clone());

    assert_eq!(
        latest_bytes_read.load(Ordering::Relaxed),
        0,
        "precondition: no bytes read yet"
    );

    for i in 1..=5u64 {
        handler(Event {
            kind: EventKind::BytesRead {
                bytes: i * 10_000_000,
                total: 50_000_000,
            },
        });
    }

    assert_eq!(
        latest_bytes_read.load(Ordering::Relaxed),
        50_000_000,
        "BytesRead must update latest_bytes_read — if 0, the production \
         on_event closure dropped BytesRead and /api/state would show 0 KB/s"
    );
    assert!(
        wdf.load(Ordering::Relaxed) > 0,
        "every event must reset the watchdog frame so a working drive \
         isn't flagged stalled"
    );
}

/// A `ReadError` event must not clobber the byte counter (it only logs and
/// pets the watchdog). This pins that the byte channel and the error
/// channel are independent — a read error mid-rip must not zero the meter.
#[test]
fn drive_event_fn_read_error_does_not_disturb_byte_counter() {
    let wdf = Arc::new(AtomicU64::new(0));
    let latest_bytes_read = Arc::new(AtomicU64::new(0));
    let handler = make_drive_event_fn("sr0".to_string(), wdf.clone(), latest_bytes_read.clone());

    handler(Event {
        kind: EventKind::BytesRead {
            bytes: 12_345,
            total: 1_000_000,
        },
    });
    handler(Event {
        kind: EventKind::ReadError {
            sector: 999,
            error: libfreemkv::Error::DiscRead {
                sector: 999,
                status: Some(2),
                sense: None,
            },
        },
    });

    assert_eq!(
        latest_bytes_read.load(Ordering::Relaxed),
        12_345,
        "a ReadError must not reset latest_bytes_read"
    );
}

/// The stream-level handler (shared by the multipass ISO highway and the
/// single-pass inline path) must forward BytesRead into the same atomic,
/// AND route BatchSizeChanged → current_batch and SectorSkipped → last_lba.
/// These three signals drive distinct UI fields; the test proves each lands
/// in its own atomic.
#[test]
fn stream_event_fn_routes_each_event_to_its_own_atomic() {
    let wdf = Arc::new(AtomicU64::new(0));
    let last_lba = Arc::new(AtomicU64::new(0));
    let current_batch = Arc::new(AtomicU16::new(16));
    let latest_bytes_read = Arc::new(AtomicU64::new(0));
    let handler = make_stream_event_fn(
        "sr0".to_string(),
        wdf.clone(),
        last_lba.clone(),
        current_batch.clone(),
        latest_bytes_read.clone(),
    );

    handler(Event {
        kind: EventKind::BytesRead {
            bytes: 7_000_000,
            total: 9_000_000,
        },
    });
    handler(Event {
        kind: EventKind::BatchSizeChanged {
            new_size: 4,
            reason: BatchSizeReason::Shrunk,
        },
    });
    handler(Event {
        kind: EventKind::SectorSkipped { sector: 4242 },
    });

    assert_eq!(
        latest_bytes_read.load(Ordering::Relaxed),
        7_000_000,
        "BytesRead must land in latest_bytes_read"
    );
    assert_eq!(
        current_batch.load(Ordering::Relaxed),
        4,
        "BatchSizeChanged must update current_batch (the UI's batch field)"
    );
    assert_eq!(
        last_lba.load(Ordering::Relaxed),
        4242,
        "SectorSkipped must record the skipped LBA in last_lba"
    );
}

/// The monotonic-progress contract the speed meter relies on: BytesRead
/// carries a cumulative byte count, so successive events must leave the
/// atomic holding the LATEST value. (If the handler accidentally summed or
/// reset, the windowed-rate computation in `PassProgressState::observe`
/// would read a bogus denominator and the displayed speed would be wrong.)
#[test]
fn stream_event_fn_bytes_read_is_last_writer_wins_cumulative() {
    let wdf = Arc::new(AtomicU64::new(0));
    let last_lba = Arc::new(AtomicU64::new(0));
    let current_batch = Arc::new(AtomicU16::new(16));
    let latest_bytes_read = Arc::new(AtomicU64::new(0));
    let handler = make_stream_event_fn(
        "sr0".to_string(),
        wdf,
        last_lba,
        current_batch,
        latest_bytes_read.clone(),
    );

    for cumulative in [1_048_576u64, 2_097_152, 3_145_728, 10_485_760] {
        handler(Event {
            kind: EventKind::BytesRead {
                bytes: cumulative,
                total: 10_485_760,
            },
        });
        assert_eq!(
            latest_bytes_read.load(Ordering::Relaxed),
            cumulative,
            "latest_bytes_read must hold the most recent cumulative count"
        );
    }
}
