# Rationale for autorip/tests/reporting.rs

These integration tests drive the real production drive-level event handler
— the closure `rip_disc` installs on the live drive — via the
`make_drive_event_fn` factory the orchestrator calls. Firing real
`libfreemkv` events at it and reading back the shared atomics proves the
BytesRead -> `latest_bytes_read` wiring the `/api/state` speed meter depends
on is actually connected.

The mux/sweep stream events are now forwarded by libfreemkv's `mux_stream`
through autorip's `AutoripMuxEvents` bridge — unit-tested in
`src/ripper/mux.rs` — so the old `make_stream_event_fn` factory and its two
tests here were retired with the `run_mux` migration.

The previous version of this file tested a hand-written
`production_shape_handler` replica and a locally re-implemented EMA speed
loop — neither of which existed in production. Production has BytesRead
arms (the replica's premise that it lacked them was false), stores into an
`AtomicU64` (not a struct), and its speed meter is a sliding-window average
in `ripper::state::PassProgressState::observe` (NOT an EMA, and its first
sample returns 0.0, the opposite of the old "first frame non-zero"
assertion). The real speed meter is unit-tested in `src/ripper/state.rs`
(`pass_progress_*`); these tests own the event-wiring half that those
cannot reach.
