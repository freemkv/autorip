# `src/ripper/mux.rs` — extended comment notes

Long-form rationale relocated out of source comments by the comment-guard
pass. Each section below corresponds to a `// See docs/mux.md` pointer left
at the original comment site in `src/ripper/mux.rs`.

## Module overview

Mux orchestration — autorip's thin wrappers over libfreemkv's `mux_stream`
driver plus the machinery autorip keeps on its own side of the seam: the
hard watchdog, the shared `MuxAtomics` it reads, the `AutoripMuxEvents`
bridge that feeds those atomics + the per-frame UI state, and the
`MuxOutcome` → staging/marker classification.

As of STEP 4c-ii there are two entry points and ONE inner engine:
- `mux_iso` — multipass / resume mux from a staged ISO on disk
  (`MuxInput::Iso`, the file-backed prefetch highway inside libfreemkv).
- `mux_live` — live single-pass mux straight off the drive
  (`MuxInput::Live`, the INLINE `DiscStream` so `fill_extents`' adaptive
  batch-retry still fires; NOT the highway).

Both build a `libfreemkv::MuxInput`, hand it to `mux_stream`, and map the
`MuxOutcome` through the shared `map_iso_mux_outcome`. The header pump,
headers-ready gate, write pipeline (`WRITE_PIPELINE_DEPTH`-deep), and finish
loop the old hand-rolled `run_mux` producer/consumer owned now all live
inside `mux_stream`. The per-frame UI update still carries the multipass
identity (`pass`/`total_passes`) so the dashboard's pass/total bars don't
reset to a "fresh rip" view when the mux phase starts.

## `HARD_WATCHDOG_STALL_SECS`

20 minutes is a generous margin over the soft "drive stalled" 30s warning
and libfreemkv's per-read recovery timeout (60s). We raised this from the
pre-0.24 default of 5 min after observing real muxes with legitimate 5-10
min NFS-server commit pauses get false-positive killed mid-rip. The cost of
waiting up to 20 min before escalating a true wedge is far lower than the
cost of repeatedly killing healthy-but-slow rips.

## `total_pct_byte_weight`

Compute the Total Progress percentage during the mux phase. Uses the same
byte-weighted formula `state.rs` uses for sweep and patch — so the two
phases agree on what "total progress" means and the bar progresses smoothly
across the sweep→mux handoff instead of jumping (forward or backward).

**Total work estimate** (matches `state.rs::total_work_estimated`):

```text
    total_work = bytes_total_disc                 // sweep
               + max_retries × bytes_unreadable    // retries
               + bytes_total_disc                  // mux re-reads ISO
```

On a clean disc with `bytes_unreadable=0`, the retry term vanishes and
total_work = 2 × disc capacity — so mux opens at exactly 50%. On a damaged
disc, the retry term inflates the denominator proportionally; the bar
tracks the larger total.

**Total work done** by mux time:

```text
    total_done = bytes_total_disc                 // sweep complete
               + max_retries × bytes_unreadable    // retries complete
               + (mux_pct / 100) × bytes_total_disc
```

**Why `max_retries` and not actual-passes-run?** State.rs uses
`max_retries × bytes_unreadable` (planned × current); we mirror it here.
Autorip's retry loop short-circuits on `bytes_unreadable=0`, so on a clean
disc the retry term is `max_retries × 0 = 0` whether 0 or 5 retries actually
ran — the formula self-corrects via the shrinking `bytes_unreadable`. The
approximation is a slight over-count of retry-pass work on partially-clean
discs (we treat final `bytes_unreadable` as if it persisted through every
retry, when in reality each pass shrinks it), but it never goes backward and
matches state.rs.

**Direct mode** (`max_retries == 0`): no separate phases, total tracks the
current mux progress 1:1.

## `MuxInputs` / `SweepDamageSnapshot`

`MuxInputs` bundles the orchestrator's mux-call inputs into a struct because
the pre-split inline mux block referenced ~25 captured locals; passing them
as a struct keeps the driver signatures readable and avoids a long
positional argument list.

`SweepDamageSnapshot` carries damage fields from the final sweep/patch pass
forward so they remain visible in `/api/state` during the mux phase instead
of zeroing out. Before this snapshot, `push_state` used
`..Default::default()`, which set `errors=0, lost_video_secs=0,
damage_severity="clean", bad_ranges=[], total_lost_ms=0` on the very first
mux tick — operators polling during mux saw a damaged disc as perfectly
clean. It's populated by the orchestrator from STATE immediately after the
final `push_pass_state` call (`ripper/mod.rs`, at the mux-entry transition);
zero/empty defaults are correct for direct (single-pass) mode, where there
is no prior sweep pass with real damage data.

## `MuxOutcome`

Outcome of a mux driver (`mux_iso` / `mux_live`), used by the orchestrator
to drive the post-mux history record + final state push. `completed=false`
means the mux bailed early — either user halt, write error, or read error.
The bytes/elapsed are filled even on early exit so the history record
reflects partial progress.

### `completed` field

True iff the read loop drained `frame_rx` to natural EOF (producer dropped
its `frame_tx` after either EOF on the input stream or an unrecoverable
read error logged via `device_log`) AND the post-loop
`pipe.finish_with_halt(...)` returned `Ok`.

0.20.8 post-validation-audit semantics: `completed=true` is the
orchestrator's gate for writing `.done` / `.completed` markers in `staging`
(see `rip_disc` in `mod.rs` around the `status_label = if completed {
"complete" } else { "stopped" }` branch). It is therefore the on-disk
success signal for the resume-on-startup detector and for the mover thread.

Set to `false` on any of:
- halt during header read (early return),
- `libfreemkv::output(...)` open failure (early return),
- `Pipeline::spawn_named` failure (early return),
- producer thread spawn failure (early return),
- `break` out of the consumer-bridge loop because `pipe.send_with_halt`
  returned Err (halt or send deadline),
- `pipe.finish_with_halt` returning Err (consumer wedged or `MuxSink::close`
  propagated a finalize error from `output.finish()` — see
  `finalize_error`).

### `finalize_error` field

Set when `MuxSink::close()` failed to finalise the MKV (most commonly: the
Cues seek-back at EBML close raised an I/O error, leaving an unseekable /
structurally-invalid output). Carries the formatted error so the
orchestrator can put it in the `.failed` marker reason. `Some(_)` implies
`completed == false`.

Pre-0.20.8 the close error was swallowed (logged only) and `.done` /
`.completed` got written for unseekable MKVs — the validation audit's #1
"Reasonable tier" item.

### `read_error` field

Set (with the specific cause) when the producer thread aborted mid-stream
on a hard read error — i.e. `on_read_error=stop` saw an unrecoverable read
`Err` and dropped its sender, truncating the MKV. Distinct from
`finalize_error` (a structural MKV defect that quarantines the dir with
`.failed`): a read error leaves the disc resumable, but it is NOT a
user-initiated stop. The orchestrator uses this to report `status="error"`
with a clear `last_error` instead of the silent "stopped → idle" path that
a genuine operator halt takes — so `/api/state` signals the read failure
rather than looking like an idle, user-stopped rip.

## `SharedAtomics`

Cross-thread atomics the consumer reads on every per-frame `update_state`.
The producer's `input.on_event` callback writes `latest_bytes_read` /
`rip_last_lba` / `rip_current_batch` from the reader thread; the consumer
reads them on the writer thread. The watchdog also reads them.

- `wd_last_frame`: watchdog "last activity" timestamp. The drive + stream
  event callbacks update it from the reader thread; the consumer also
  updates it after each frame write. The watchdog reads it.
- `wd_bytes`: bytes written by the output sink. Consumer writes; watchdog
  reads (used to render the "stalled at X GB" UI).
- `input_errors`: snapshot of `input.errors` after the most recent
  `read()`. The producer updates it after every frame; the consumer reads
  it inside `apply` to surface the skip-event count. Atomic so we don't
  need to put the input stream behind a mutex.
- `input_lost_bytes`: snapshot of `input.lost_bytes` after the most recent
  `read()` — the actual bytes zero-filled past read errors. Used (not
  `input_errors`) to compute `lost_video_secs`: an AACS skip event covers a
  whole 6144-byte unit, so `errors * 2048` understates loss by the
  alignment factor. Produced/consumed like `input_errors`.

## `push_mux_state`

Extracted from `MuxSink::push_state` so BOTH the live single-pass `MuxSink`
(the frame consumer) and the ISO/multipass `AutoripMuxEvents` bridge render
an identical `RipState` — same pass/total identity, same sweep-damage
carry-forward, same `total_pct_byte_weight` denominator. Behaviour is
byte-for-byte what `MuxSink::push_state` did before the migration.

## `producer_read_error_cause`

The stream yields an `io::Error`; when the underlying fault was a coded
`libfreemkv::Error` (DiscRead, AACS/CSS decrypt manifesting mid-stream,
etc.) it reached the producer via `From<Error> for io::Error`, which
stringifies the original through `Error`'s `Display` — so the `io::Error`
message already begins with an `E####:` prefix. We surface that code in a
parenthetical annotation so an operator sees the real fault identifier in
`last_error`.

Note: reconstructing the code by `Error::from(io::Error)` does NOT work —
`From<io::Error> for Error` is unconditionally `Error::IoError`, whose
`.code()` is always `E_IO_ERROR`. The code only survives in the stringified
message, so we parse it back out of the leading token.

## `coded_error_label`

Short English label for a coded `libfreemkv` fault that reaches the mux
producer as an `io::Error`. The library `Display` is code-only, and the
code is the only thing that survives the `Error → io::Error` round-trip
(`From<io::Error> for Error` collapses everything to `E_IO_ERROR`), so we
map the parsed `u16` to text here rather than matching on an `Error`
variant. Mirrors the sweep/patch path's `non_scsi_error_label`; any
unmapped code falls back to a generic phrase that still carries the code in
the parenthetical so a new variant never leaves the operator stranded.

## `WatchdogGuard` / `spawn_mux_watchdog`

Drop guard that stops the mux watchdog thread when the owning mux call
(`mux_live` live single-pass, or `mux_iso` ISO/multipass+resume) returns.

`spawn_mux_watchdog` is shared verbatim by `mux_live` and `mux_iso` so both
paths get the identical escalation semantics. The watchdog reads
`wd_last_frame` (activity) and `wd_bytes` (good-bytes) exactly as before;
callers feed those atomics.

## `MuxAtomics`

The shared atomic counters the mux drivers (`mux_iso` / `mux_live`) feed
via the `AutoripMuxEvents` bridge and the hard watchdog reads. The
orchestrator builds these *before* calling a driver; `mux_stream`'s
reader-side events (forwarded through the bridge) write them during the
run.

## `IsoMuxSource`

Everything `mux_iso` needs to build a `libfreemkv::MuxInput::Iso`. The
orchestrator (`rip_disc` multipass branch / `resume_remux`) fills this
instead of hand-building the `build_iso_pipeline` stream — `mux_stream`
re-derives the same 3-stage highway (and re-derives the AACS key map from
`keys`/`key_fetch`) internally, so no pre-resolved map is carried on this
path.

- `iso_path`: path to the staged ISO image. `mux_stream` opens its own
  `FileSectorSource` from this (the orchestrator's validation open is a
  separate, discarded handle).
- `keys`: decryption keys (banked forensic/FMTS keys reach
  `build_iso_pipeline` through here — the FMTS gate itself is untouched at
  the call site).
- `key_fetch`: read-time fresh-key-on-failure fetch (recovers a 2nd/Nth
  CPS-unit key mid-mux). Same closure the pre-migration `build_iso_pipeline`
  call took.
- `raw`: ciphertext passthrough (unused on the production ISO path; kept
  for parity with `MuxOptions.raw`).
- `skip_errors`: skip-past-read-errors (inert on the file highway — the ISO
  is already zero-filled for any sweep-pass loss; kept for parity with
  `MuxOptions`).

## `AutoripMuxEvents`

autorip's `libfreemkv::MuxEvents` bridge for the ISO/multipass + resume
mux. It updates the SAME shared atomics the pre-migration `stream_event_fn`
(reader side) and `MuxSink` (writer side) updated, and drives the same
per-frame `update_state` UI push — so the hard watchdog keeps reading a
byte counter that advances during a healthy mux and the dashboard is
unchanged.

Atomic feed (cross-checked against what `spawn_mux_watchdog` reads):
- `on_read_progress` → `latest_bytes_read` (UI progress) + `wd_last_frame`
  (watchdog activity). Mirrors the old reader `BytesRead` `stream_event_fn`.
- `on_write_progress` → `wd_bytes` (the watchdog's "stalled at X GB" +
  hard-escalation good-byte counter) + `wd_last_frame`, then the throttled
  `push_mux_state`. Mirrors the old `MuxSink::apply`.
- `on_output_opened` → `opened` flag (drives `output_opened` in the outcome
  mapping).
- `on_sector_skipped` / `on_read_error` → refresh `wd_last_frame`;
  `on_sector_skipped` also stores the skipped LBA into `rip_last_lba` (the
  UI last_sector / playhead), bumps `input_errors`, and logs the per-skip
  `Sector N skipped (zero-filled)` line. (Fires on the LIVE inline
  single-pass path from `DiscStream::fill_extents`; ~never on the ISO
  highway.)
- `on_batch_size_changed` → `rip_current_batch` + the `Batch size → N (…)`
  device-log line. (Live inline path only; ~never fires on the highway.)

## `ui_state_from_inputs`

Builds a `UiState` from the orchestrator's `MuxInputs`. Shared by both mux
drivers (`mux_iso` / `mux_live`) for the `AutoripMuxEvents` bridge.

## `undelivered_streams_note`

The ONE wording for "this mux completed, and the file still does not match
the pre-mux plan". libfreemkv's contract: a non-empty `undelivered_streams`
means the finished file is missing streams the plan promised even though
`completed == true`, and a caller that reports a successful export must
report these too — a lossy outcome is never silent.

Two sites used to spell this out independently — here and `rip_disc`'s
completed-mux summary — writing two differently-worded lines into the SAME
per-device log for the SAME event: one lossy mux read as two, and an
operator grepping or alerting on either exact phrase saw half the story.
`map_iso_mux_outcome` produces every outcome that can carry undelivered
streams (every other construction site is empty by definition), so it is
the one emitter, and this is the one wording it emits.

## `map_iso_mux_outcome`

Maps a `mux_stream` result into autorip's `MuxOutcome` + staging decisions,
preserving the pre-migration Err classification:
- `is_halt_error` / `is_fmts_key_missing_error` are RETURNED as `Err` so the
  call site keeps its existing "preserve staging (resume)" / "FMTS
  deferral (retryable idle)" handling verbatim.
- a header-phase failure (headers never resolved → `MkvInvalid`, or any
  error before the sink opened) → `output_opened=false` +
  `finalize_error=Some`, so the orchestrator quarantines it via the
  `!output_opened` path.
- `NoStreams` (empty/undecryptable drain) → `output_opened=true` +
  `finalize_error=Some` (quarantine).
- a coded read fault mid-mux (DiscRead/Decrypt/…) → `read_error=Some` (the
  disc stays resumable — matches the old producer-read-error path).
- any other finalize / IO error → `finalize_error=Some` (quarantine).

## `mux_iso`

The STEP 4c-i migration of the hand-rolled header-pump / producer /
consumer-bridge / finish loop into `mux_stream`; its live single-pass
sibling is `mux_live` (STEP 4c-ii). It KEEPS, unchanged: the mux phase
drop-guard, the hard watchdog (`spawn_mux_watchdog`, reading
`atomics_in.wd_*`), and the per-device `Halt`. `mux_stream` owns the inner
loop; `AutoripMuxEvents` feeds the watchdog's atomics + the UI.

## `LiveMuxSource`

Everything `mux_live` needs to build a `libfreemkv::MuxInput::Live` — the
live analogue of `IsoMuxSource`. The orchestrator (`rip_disc`'s single-pass
branch) fills this instead of hand-building `DiscStream::new(...)` +
`run_mux`; `mux_stream` constructs the inline `DiscStream`, applies the
forensic `key_map`, and drives the same header-pump/finish loop internally.

- `reader`: the raw live-drive sector source (`session.drive` boxed).
  `mux_stream` moves it into `DiscStream::new` (whose reader param is
  exactly `Box<dyn SectorSource>`) — the inline reader, never the highway
  wrapper.
- `keys`: decryption keys autorip already resolved as its own app-layer
  policy (`disc.decrypt_keys()`). The driver consumes them as-is.
- `key_map`: retained pre-rip FMTS forensic key map (`fmts_key_map`).
  `mux_stream` applies it via `DiscStream::with_key_map` so single-pass
  FMTS reads only our-phase units and decrypts the forensic segment
  correctly. `None` for every non-FMTS disc, leaving the read walk
  unchanged.
- `skip_errors`: skip-past-read-errors (zero-fill + continue) — wired onto
  `DiscStream::skip_errors` (was `on_read_error == "skip"`).

## `mux_live`

The STEP 4c-ii replacement for the hand-rolled `run_mux` producer/consumer
loop. Mirrors `mux_iso` exactly — same watchdog spawn, same
`AutoripMuxEvents` bridge feeding the same `MuxAtomics`, same
`map_iso_mux_outcome` classification — differing only in building a `Live`
source (inline drive reader + forensic key map) instead of an `Iso` source
(staged file highway).

## Tests: coded read-error cause regressions

`producer_read_error_cause_preserves_coded_root_cause`: a hard producer
read error must surface the SPECIFIC coded cause, not a generic truncation
string. A coded `libfreemkv::Error` reaches the producer as an `io::Error`
whose Display already carries the `E####:` prefix; the cause string must
preserve it so an operator sees the real fault (decrypt / DiscRead / AACS)
in `last_error` without digging through the device log.

`producer_read_error_cause_carries_english_label` (regression rc4): the
mux read-error cause must carry an English description of the fault, not a
bare duplicated `E####`. Before the fix a mid-mux decrypt failure rendered
as `read error mid-stream (E7013): E7013` — a raw code with no English,
inconsistent with the sweep/patch path that labels via
`non_scsi_error_label`. The cause must now read e.g. `read error mid-stream
(E7013): decryption failed`.

## Tests: `total_pct_byte_weight` shape

`clean_disc_mux_opens_at_50_percent`: clean disc (no bad sectors), retry
term vanishes, total_work reduces to 2 × capacity. Mux opens at exactly
50%, climbs linearly to 100%. Sweep+mux symmetry — same shape as a 2-phase
pipeline regardless of `max_retries` planned.

`damaged_disc_mux_opens_below_50_percent`: damaged disc with residual
`bytes_unreadable`, retry term inflates the denominator, mux opens lower
than 50% because the rip "did more total work than just sweep+mux."

`direct_mode_passthrough`: direct-mux / single-pass mode (`max_retries ==
0`) — there are no separate phases, total tracks current 1:1.

## Tests: `push_mux_state` / `SweepDamageSnapshot` carry-forward

`sweep_damage_snapshot_non_zero_overrides_default` verifies that
`SweepDamageSnapshot` fields survive the `UiState` round-trip into
`push_state`'s `RipState` construction. The regression: `push_state` used
`..Default::default()` for the damage fields, zeroing `errors`,
`total_lost_ms`, `bad_ranges`, etc. on the first mux tick — making a
damaged disc appear perfectly clean to operators polling `/api/state`
during mux. This test asserts the contract without invoking `update_state`
(which writes to a global singleton): it inspects the `RipState` struct
literal that `push_state` would build, verifying the snapshot fields are
forwarded rather than defaulted, by replicating `push_state`'s selection
logic against `SweepDamageSnapshot`'s `Default` (all-zero) vs a non-zero
snapshot.

`push_mux_state_reports_ripping_and_disc_present`: `push_mux_state` is the
only place that writes the live per-frame `RipState` to `/api/state` during
a mux — both the single-pass `MuxSink::apply` path and the ISO/multipass
`AutoripMuxEvents` bridge funnel through it. `status: "ripping"` and
`disc_present: true` are what the "already ripping" concurrent-dispatch
gate and the live-progress UI key off: if either silently reverted to
`RipState::default()`'s "idle"/`false` (the exact shape of a `delete field`
mutant on this struct literal), a mux in progress would report the device
as idle — the operator sees nothing happening while the drive is busy, and
a second `/api/rip` could be dispatched against it. A private device key
avoids racing any other test's `STATE` entry.

## Tests: `AutoripMuxEvents` watchdog feed

`autorip_mux_events_feed_watchdog_byte_atomic`: THE watchdog preservation
check — `AutoripMuxEvents::on_write_progress` must feed `wd_bytes` (the
atomic `spawn_mux_watchdog` reads for both its hard `exit(1)` escalation
and the "stalled at X GB" UI) and refresh `wd_last_frame` — even on the
throttled early-return path — so a healthy mux keeps the counter advancing
and never false-escalates. Mutation: dropping the `wd_bytes.store(...)` in
`on_write_progress` leaves the counter at 0 and this fails.

`on_sector_skipped_stores_lba_into_rip_last_lba` (regression D):
`AutoripMuxEvents::on_sector_skipped` must store the skipped LBA into
`rip_last_lba` (the UI last_sector / playhead atomic `push_mux_state`
reads) — the pre-refactor `make_stream_event_fn` did
`last_lba.store(sector)` on every `SectorSkipped`. It must also refresh the
watchdog activity timestamp and bump `input_errors` (additive behaviour
kept from the post-refactor bridge). Mutation: reverting the handler to
`_lba` unused (dropping the `rip_last_lba.store(lba as u64, ...)`) leaves
`rip_last_lba` at 0 and the last_sector assertion fails.

`on_batch_size_changed_stores_batch_and_logs` (regression D):
`AutoripMuxEvents::on_batch_size_changed` must store the new batch into
`rip_current_batch` AND emit the batch-change device-log line the
pre-refactor `make_stream_event_fn` produced. We assert the atomic store
(the inspectable effect) and that the reason→label match is
exhaustive/panic-free for both variants (the log line is derived from it).

## Tests: `map_iso_mux_outcome` classification

`map_iso_mux_outcome_classifies_faithfully`: `map_iso_mux_outcome`
preserves the pre-migration Err classification — halt / FMTS-missing
propagate as `Err` (call-site deferral); a completed run maps to
`completed=true`; a NoStreams drain quarantines (`finalize_error=Some`,
output opened). `undelivered_streams` on the completed-run fixture is the
stream-index list the sink accepted frames for but couldn't put in the
finished container — empty here (clean completed run). Also verifies that
`Ok(..)` with `completed=false` (a clean stop or join-timeout wedge) must
NOT report as a finished mux: a mutant widening the `Ok(o) if o.completed`
guard would file a damaged rip as good.

`map_iso_mux_outcome_surfaces_undelivered_streams_on_a_completed_run`:
libfreemkv's contract on `MuxOutcome::undelivered_streams` — non-empty
means the finished output does NOT match the pre-mux plan **even with
`completed = true`**, and "a caller that reports a successful export must
report these too — a lossy outcome is never silent." Today only the
`mp4://` sink populates it (autorip never offers that destination — see
`output_scheme_for`), but `map_iso_mux_outcome` must not drop the field on
the floor. The per-device log ring is a process-global static shared by
sibling tests using `"sr-test"`; this test mints a unique device name so
reading the log back stays sound.

`the_undelivered_streams_note_has_a_single_emitter`: the note had two
independently-maintained spellings — `map_iso_mux_outcome`'s "could not be
delivered" and `rip_disc`'s completed-mux summary "were not delivered" —
both written to the same per-device log for the same event. A future
wording change to one would silently diverge from its twin, and an alert
on either phrase already missed the other. Dormant only until an `mp4://`
destination exists, which is exactly when nobody will re-read this code.
