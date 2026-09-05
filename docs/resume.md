# Auto-resume from staged ISO notes

## Module overview (from `resume.rs` module header)

Companion to `staging::resume_or_quarantine_staging`. The staging pass
classifies what's left in `<staging_dir>/<disc>/` after a container
restart and preserves partial state. This module decides what to do with
the preserved state: if Pass 1 finished cleanly to ISO + mapfile but mux
never wrote the final MKV, we can skip every disc-side operation and just
re-mux from the ISO.

The classifier (`classify_resume`) is pure: takes a hint + the configured
`abort_on_lost_secs`, inspects the staging dir, returns a verdict. The
actor (`resume_remux`) does the side effects: delete partial MKV,
`Disc::scan_image` the ISO, `mux::run_mux`, write `.completed` + clear
`.restart_count` on success.

Counter-clearing semantics: the counter is cleared only on successful
remux. Failure of `Disc::scan_image`, `mux::run_mux`, or any helper
leaves the counter intact so the next-startup pass through
`resume_or_quarantine_staging` will bump it. After `RESTART_LIMIT`
consecutive failures the partial state is promoted to `.failed` and the
loop ends.

## `FALLBACK_BITRATE_BYTES_PER_SEC`

8.25 MB/s = 66 Mbit/s — a conservative mid-range average for a UHD
Blu-ray main feature (UHD BD runs ~50-100+ Mbps). Shared by
`classify_resume`'s pre-flight estimate and `resume_remux`'s
post-`scan_image` re-validation so the two cannot silently diverge. The
authoritative title-scoped re-check in `resume_remux` (real per-title
bitrate + `bytes_bad_in_title`) is what ultimately gates the mux. Single
source of truth so the value can't drift across call sites.

## `DEFAULT_BATCH_PROBE_PATH`

Sentinel device path passed to `detect_max_batch_sectors` from the
resume-remux path. There is no live drive here — we mux from a staged
ISO — so we deliberately probe a non-optical, non-existent node. The
probe finds no SCSI peripheral type for it and falls back to the
library's default optical batch size, which is the correct read batch
for a file-backed ISO source.

## `ResumeClass::Remux::display_name`

Sanitized display name — a FILE basename (the staged ISO's
`file_stem()`), NOT the staging subdirectory's name, which carries the
`_2`-style boxset disc suffix that files inside it never take. Used for
the MKV filename, dest URL, and the `display_name` in `MuxInputs`. The
original TMDB-resolved title isn't available at resume time (no fresh
scan_disc has run yet); the sanitized form is what every other
downstream path keys on anyway.

## `ResumeClass::Remux::title_confident`

Operator-confidence carried from the fresh-rip hand-off, when known.
`Some(true)` means the rip side already decided the title is
auto-file-worthy (exact match OR an explicit operator override);
`resume_remux` ORs it into its own match check so an override whose
chosen title differs from the disc's own label isn't second-guessed into
`.review`. `None` on the cold auto-resume path (no hand-off marker, no
override concept) — `resume_remux` then relies on the match check alone.

## `classify_resume` coverage note

No live check here, intentionally: one might want to verify the mapfile's
entries span its whole `bytes_total`. That check is an identity check
that gave a false sense of protection, so it's deliberately omitted.

## `reset_status_after_ripping`

Reset the device to a terminal idle/error UI state at any early-return
site inside `resume_remux`, whether or not `status="ripping"` was ever
set. Preserves disc identity (so the dashboard tile keeps its title /
format / duration) and zeroes everything else via `Default::default()`.

Several callers (the config-poison, scan, and key-resolution early
returns) run BEFORE the `status="ripping"` update, but the reset is
still correct there — it just writes the terminal state directly. When
it runs after "ripping" was set, it un-sticks the API state so the
"already ripping" gate in `web.rs::handle_rip` doesn't reject all
subsequent /api/rip requests. Mirrors `rip_disc`'s stopped -> "idle"
pattern.

## `apply_failure_fields`

Fill the NON-success half of a `MuxHandoffOutcome` from the terminal
`_mux` state: the real reason the dir didn't advance, and whether that
reason is a retryable DEFERRAL.

Retryability is what the exit RECORDED (`failure_deferred`), never what
the status happens to read — `"idle"` is written by both the keyless
deferrals and by three hard failures (no title after key resolution, an
unreadable mapfile, an over-threshold loss abort), and inferring from it
put "will mux automatically once keys are available" on the error card
of a corrupt ISO. That is the seam this crate got wrong.

It is a function rather than two lines inline in
`remux_from_ripped_marker` so the grading is reachable from a test
without an ISO + mapfile + decrypt + mux pipeline — the same reason
`mux_handoff_success` and `build_mux_handoff_outcome` were pulled out.
An empty `last_error` records NOTHING: `crate::muxer` dispatches on
`failure_reason.is_some()`, so `Some("")` would print a blank error card
instead of falling through to its own fallback hints.

## `quarantine_incomplete_mux`

Quarantine an incomplete-mux staging dir IFF the mux died on a
structural FINALIZE failure (the MKV could not be finalized — e.g.
E6008 no muxable frames / unseekable output). A finalize failure is
terminal: re-muxing the same ISO reproduces it, so without a terminal
transition the mux worker re-dispatches the dir every ~10s tick forever
(a full UHD re-mux each time). Transition `state -> Failed` so the next
`mux_dispatch_verdict` is `SkipTerminal`.

A mid-mux READ error (`finalize_error == None`) is left resumable — the
MKV is merely truncated and a retry (fresh keys / a settled drive) can
complete it.

Returns whether the terminal `.failed` write actually LANDED on disk —
NOT merely whether this was a finalize failure. `None` (a read error,
nothing to write) returns `false`; a finalize failure whose state.json
write FAILED (unwritable staging) also returns `false`, so the caller
can tell "quarantined" from "tried to quarantine but the write was
dropped" and surface the dropped write loudly rather than silently
believing the dir is terminal. The caller records the `failure_finalize`
bit from `finalize_error.is_some()` INDEPENDENTLY of this return (the
worker backstop must still fire on a dropped write), consuming the
return only to detect a stuck quarantine.

Extracted from the mux-incomplete early-return so the terminal-vs-
resumable decision is reachable from a regression test without an ISO +
mapfile + decrypt + mux pipeline — the prior inline block had ZERO
behavioural coverage (a source-substring test was satisfied by an
earlier line, so deleting the quarantine kept it green).

## `defer_status_after_ripping`

`reset_status_after_ripping` for the exits that are DEFERRALS rather
than failures: the mux did not happen because keys are not available
yet, staging is intact, and a later pass will complete it untouched.

Terminal status is `"idle"` — same as the hard-failure exits beside it,
because that is what the dashboard needs to show — so the deferral is
recorded explicitly on `RipState::failure_deferred` instead. The
alternative, reading `status == "idle"` back out, is what
`remux_from_ripped_marker` used to do, and it told the operator that an
unreadable mapfile or a corrupt ISO would "mux automatically once keys
are available" — advice that can only ever be wrong for those causes.

## `resolve_media_type`

Resolve the `media_type` written into the resume `.done`/`.review`
marker. The mover routes by this field (movie library vs TV library) and
defaults a missing/empty value to "movie"; we resolve the same default
here so a cold auto-resume — where STATE is empty and no media_type was
carried — writes an explicit value rather than relying on the reader's
fallback. A carried "movie"/"tv" (warm `_mux` resume, seeded from the
`.ripped` hand-off) passes through unchanged, fixing the prior bug where
TV resumes were filed as movies.

## `handle_resume_fsync_failure`

Handle a durability-gate (`fsync`) failure on the resume mux output.

Both the ISO-output and the MKV/M2TS-output success paths call
`staging::fsync_output_file` before writing any success marker; a
`false` return means the output is not provably durable and we must NOT
hand it to the mover. The naive response — preserve staging, return —
is correct on the startup-scan path (which bumps `.restart_count` via
`resume_or_quarantine_staging` on the NEXT restart). But `resume_remux`
is ALSO driven by the live `_mux` worker loop (`check_and_mux`), which
leaves `.ripped` in place and re-dispatches the SAME dir on its next
tick — so a deterministic fsync failure (e.g. a wedged NFS export) would
re-mux + re-fsync the same possibly-corrupt output forever, never
consulting any restart cap.

This caps that loop the same way `resume_or_quarantine_staging` caps its
partial-state path: bump `.restart_count`, and once it reaches
`RESTART_LIMIT`, promote the dir to terminal `.failed` (which the
worker's `mux_dispatch_verdict` and `resumable_dir_blocked` both treat
as terminal, stopping the re-dispatch) and drop the `.ripped` hand-off
so it can't be re-queued. Below the limit it leaves staging intact for
the next retry.

Returns `true` when the dir was promoted to `.failed` (terminal), and
`false` when staging was preserved for another attempt. The caller
resets device status and returns either way.

## `ResumeMuxingGuard`

RAII exclusion lock for the cold operator-resume mux path.

The `_mux` worker path (`muxer::check_and_mux`) already writes `.muxing`
and holds its own `MuxingGuard` for the duration of the dispatch, so it
must NOT have a second guard write/clear the same marker underneath it.
Every OTHER caller of `resume_remux` — the cold operator-resume path
(`ResumeMode::Require` -> `find_resumable_for_disc` -> `resume_remux`) —
runs a multi-minute mux with only `<name>.iso` + `<name>.iso.mapfile` on
disk and NO governing marker. Without `.muxing`, `disc_owned_by_worker`
/ `resumable_dir_blocked` return false, so a concurrent
`ResumeMode::Wipe` of the same disc `remove_dir_all`s the staging dir
and deletes the ISO out from under this in-flight mux (the exact data
loss the Wipe guard at `mod.rs` was added to prevent), and a second cold
resume double-muxes the same ISO. Writing `.muxing` here closes both
holes; the terminal marker writers (`write_completed_marker` /
`write_failed_marker`) already clear it, and this guard's `Drop` clears
it on every early-return / panic path too.

## `resume_title_confident`

Resume's call-site wiring into the shared `title_is_confident` policy
(mod.rs). Pulled out of `resume_remux` as its own function so the
argument plumbing is directly unit-testable without a real
`Disc::scan_image` (which `resume_remux` itself still requires — see the
documented gap in the module tests): a mutant that swapped an argument,
or a future edit that quietly reintroduced resume's own copy of the
disjunction instead of calling through, would be caught here even
though `resume_remux` end-to-end cannot be driven from a synthetic
fixture.

`carried_confident` is the fresh-rip completion's full
`title_is_confident` verdict, carried across the `.ripped` hand-off
(`None` on a cold auto-resume, which has no hand-off). It fills
`title_is_confident`'s `overridden` parameter: OR-ing in a prior `true`
composes identically whether that `true` came from an operator override
or a genuine TMDB match, so this is not a semantic mismatch, just
parameter reuse.

## `resume_effective_abort`

The loss threshold a resumed rip is judged against — for EVERY loss gate
in `resume_remux`, which is the whole point of it being a function.

`.accept-loss` raises the threshold to unlimited: the operator has
looked at the recorded damage and chosen to deliver this rip as-is.

A resume has two loss gates — the sweep gate (mapfile Unreadable
sectors) and the mux gate (decrypt/codec loss, judged against sweep +
demux loss combined). They used to be written independently, and only
the first honoured the override: the second RECOMPUTED the threshold
from raw config, so any non-zero `demux_lost_secs` re-armed
`mux_loss_aborts` against a total that ALREADY INCLUDED the sweep loss
the operator had just accepted, and quarantined the dir to
`.aborted-loss`. The one-shot marker was consumed before either gate
ran, so the consent was gone: press Accept again, get the same answer
again. One override, one threshold, both gates.

## `remux_from_ripped_marker` and `MuxHandoffOutcome`

Run a mux-from-staging pass as if it were an auto-resume, against a
synthetic device key. Used by `crate::muxer` to dispatch the `.ripped`
hand-off from the drive thread without itself re-implementing
scan_image + run_mux + history bookkeeping. The device key is `"_mux"`
— underscore-prefixed so the UI tile grid ignores it, but `update_state`
/ `device_log` / halt-token plumbing all still work through the existing
per-device shape.

Returns true on a clean mux (`.completed` marker written, `.ripped`
safe to delete). False on any failure path that left `.ripped` in place
for next-tick retry.

`MuxHandoffOutcome` — Result of a `.ripped` hand-off mux. `success`
mirrors the prior `bool` return; the rest carries the mux-derived
display fields the `_mux` done-state computed (codecs/duration/
output_file) so the origin-device's secondary done-state update in
`crate::muxer` can show the same codec badge, duration, and output path
the inline fresh-rip done card does. These are captured from the
synthetic `_mux` STATE entry just before it's removed; empty when the
mux didn't succeed.

`bad_ranges` / `bad_ranges_truncated`: the full bad-ranges drilldown
list (plus its truncation count), captured off the `_mux` done-state —
which recomputed it from the mapfile in `resume_remux`. The
`RippedMarker` carries only the summary counts (`sweep_num_bad_ranges`,
`sweep_largest_gap_ms`), not the list, so without plumbing these through,
the origin device's secondary done card would show the damage count but
an empty drilldown — diverging from the inline fresh-rip and cold
auto-resume done cards, which both populate `bad_ranges`.

`lost_video_secs` / `errors` / `total_lost_ms` / `main_lost_ms`:
combined sweep + mux-time loss figures, captured off the `_mux`
done-state — which folded demux/decrypt-time loss into the sweep-only
mapfile totals (`done_errors` / `done_lost_video_secs` /
`done_*_lost_ms` in `resume_remux`). The `RippedMarker` carries only
sweep-phase loss (`rip_lost_video_secs`, `sweep_*`), so without plumbing
these through the origin device's secondary done card would understate
the loss in the delivered MKV whenever a mux-phase decrypt/codec skip
added loss the sweep never saw — diverging from the `_mux` tile and the
completion webhook, which are correct.

`failure_reason`: on a NON-success mux, the real reason the dir didn't
advance — read off the `_mux` device state (`no keys` / `key service
down` / finalize-write failure) so the mux worker's error card surfaces
the actual cause instead of falling back to a stale `.aborted-loss`
marker + a generic "finalize/write" hint. Empty on success.

`failure_retryable`: true when the non-success was a keyless DEFERRAL
(the synthetic `_mux` device was left "idle" — a clean, retryable state:
the ISO stays staged and will mux automatically once keys land) rather
than a hard finalize/write failure. Drives which hint the error card
shows.

`failure_finalize`: true when the non-success was a STRUCTURAL FINALIZE
failure (the MKV could not be finalized — E6008 no muxable frames /
unseekable output), as distinct from a resumable mid-mux read error
(truncated MKV) or a keyless deferral. This is the ONLY class the mux
worker may quarantine (`state -> Failed`): a read error and a deferral
both leave staging resumable, so gating the worker's quarantine on
`!failure_retryable` (the prior code) false-quarantined a recoverable
read error — a lost rip that would have succeeded on retry. Read off
the `_mux` device's `failure_finalize` bit, which `resume_remux` sets
only on the finalize exit.

## `mux_handoff_success`

Whether `resume_remux` finished this staging dir cleanly: it wrote
`.completed`. Anything else (halt, `scan_image` failure, mux loop break)
leaves `.completed` absent.

Probes via `snapshot_staging_disc` (3-retry, NFS-resilient) rather than
a bare `Path::exists()`. On NFS with a cold attribute cache — the
scenario `snapshot_staging_disc` exists to defend against — a bare
`.exists()` can false-negative immediately after `write_completed_marker`,
making `check_and_mux` record a spurious `MuxerError` (the success
path's `clear_error` is skipped) that sticks on the System page even
though the MKV was fully written. This mirrors `check_and_mux`'s
completion guard.

Pulled out of `remux_from_ripped_marker` so it has exactly one
implementation: this expression was previously duplicated verbatim
inside a test (`completion_detection_tests`) that only proved the
expression itself was correct, never that `remux_from_ripped_marker`
actually used it — the exact "hand-rolled copy" trap.

## `build_mux_handoff_outcome`

Build the initial `MuxHandoffOutcome` from the success signal. Pulled
out of `remux_from_ripped_marker` as its own function so the `success`
field assignment is directly unit-testable — a mutant that drops
`success` from this struct literal (falling back to `Default`'s
`false`) would leave a genuinely successful resumed mux reporting
`success: false`, which never flips the origin device's tile off
"ripping" (see `crate::muxer::check_and_mux`'s use of `outcome.success`)
and can wedge the "already ripping" gate for that device indefinitely.

## `failure_retryability_tests` module

A hard failure must not be advertised to the operator as a deferral
that will fix itself.

`remux_from_ripped_marker` graded the `_mux` device by `status ==
"idle"`, and three hard-failure exits in `resume_remux` write exactly
that — so a corrupt ISO, an unreadable mapfile or an over-threshold loss
abort all came back `failure_retryable = true` and the muxer's error
card said "no decryption keys yet — the disc stays staged and will mux
automatically". Drive the REAL producers (both terminal-state writers)
and grade with the REAL predicate.

The grading in `graded()` only protects the operator if
`remux_from_ripped_marker` still routes through it. Driving that
function needs a real ISO + mapfile + decrypt + mux pipeline, so the
final test in this module pins the wiring at source level — the same
technique this file already uses for `resume_remux`'s webhook and
`handoff_marker_name` call sites. Without this, inlining
`outcome.failure_retryable = rs.status == "idle"` back into the
non-success branch reinstates the bug with every test still green.

## `resume_remux_log_archive_tests::...archives_prior_device_log`

Regression: resume_remux did not archive the prior session's per-device
log on entry (unlike scan_disc and rip_disc), so on the common "scan
then resume" path the resumed-mux log entries interleaved with the
prior scan's log.

This drives resume_remux through its real entry path: a prior log entry
is seeded, then resume_remux runs with a Remux classification pointing
at a non-openable ISO (it archives, logs the resume line, then aborts
on ISO open). The in-memory live ring (keyed by the unique device name,
independent of AUTORIP_DIR) must afterwards contain only the new
operation's entries — the prior line must have been archived out.

## `resume_remux_webhook_tests::success_path_fires_completion_webhook`

Regression: the success path of `resume_remux` must fire the
`rip_complete` webhook, exactly as `rip_disc`'s terminal branch does.
Both the cold auto-resume (`?resume=yes`) path and the `_mux` worker
`.ripped` hand-off complete through `resume_remux`, so without this call
an operator with completion webhooks configured (Discord, Plex, etc.)
silently received nothing on any rip that finished via resume or the
mux worker.

A full behavioural test would need a real openable ISO + mapfile +
decrypt + mux pipeline plus a mock HTTP endpoint, which is out of
proportion to a single call site. Instead this pins, at source level,
that the success region of `resume_remux` (between the "Auto-resume
complete" log line and the `auto_eject` honoring) invokes `send_rich`,
so a refactor can't silently drop it again.

## `resume_iso_auto_eject_tests`

Regression: the ISO-output success path of `resume_remux` must honor
`auto_eject`, exactly as the resume MKV terminal and the fresh-rip ISO
terminal (`mod.rs`) do. When `resume_remux` is entered for a real device
(operator clicked Resume with `output_format=iso`) a disc is physically
present, and an operator with `auto_eject=true` expects it ejected on
completion. Pre-fix the ISO branch returned without ejecting, so the
finished disc stayed in the drive. The synthetic `_mux` worker device
reaches this branch too, so the guard must skip underscore-prefixed
devices (the drive thread already ejected and may now hold a different
disc) — mirroring the MKV terminal.

A full behavioural test would need a real openable ISO + mapfile plus an
actual eject syscall against a device — out of proportion to one call
site (same rationale as `success_path_fires_completion_webhook`).
Instead this pins, at source level, that the ISO success region (between
its "ISO output complete" log line and the `run_mux` call that begins
the MKV path) honors `cfg_read.auto_eject` with the synthetic device
guard.

## `post_mux_loss_reporting_tests::resume_reports_demux_loss_on_accepted_rip`

Regression: a resume must report sweep loss + demux loss to the
operator, not the sweep mapfile alone. Mux-time (demux/decrypt) loss is
real loss the fresh single-pass path also surfaces (mod.rs
`final_lost_secs = mux_outcome.lost_video_secs`). Previously the resume
done card and `rip_complete` webhook sourced `errors` /
`lost_video_secs` solely from `done_sweep_damage`, so any demux-time
loss (undecryptable sectors, codec corruption) was invisible and the
disc was filed as clean.

A behavioural test would need a real lossy ISO + mux pipeline; instead
this pins, at source level, that the success region folds the demux
loss into the reported figures.

## `post_mux_loss_reporting_tests::resume_incomplete_mux_surfaces_read_error_not_silent_idle`

Regression: the mux-incomplete early-return in `resume_remux` must
route through `incomplete_mux_status` (mirroring rip_disc in mod.rs) so
a mid-mux finalize error or hard producer read error surfaces.
Previously this branch hardcoded `status="idle"` with `last_error=None`,
silently discarding `mux_outcome.read_error` / `finalize_error`: an
auto-resume mux that died on an ISO/drive read error showed plain
"idle" — visually identical to a clean /api/stop — leaving the operator
no indication of the failure or that staging was still resumable.

A behavioural test would need a mux pipeline that fails mid-stream (same
rationale as the gate tests above); instead this pins, at source level,
that the early-return consults both causes and no longer hardcodes the
idle/None verdict.

## `incomplete_mux_finalize_quarantines_read_error_stays_resumable`

FIX 1 — the BEHAVIOURAL regression the source-substring test above
cannot give: the auto-resume terminal-finalize quarantine had ZERO real
coverage. `resume_incomplete_mux_surfaces_read_error_not_silent_idle`
only asserts a substring ("mux_outcome.finalize_error.as_deref()") that
is ALSO present on the earlier `incomplete_mux_status(...)` line, so
deleting the quarantine block left it green (vacuous). This drives the
production helper the block now calls (`quarantine_incomplete_mux`) and
proves the two arms with real staging state + the worker's own dispatch
verdict:

* a finalize_error -> `state -> Failed`, and the next
  `mux_dispatch_verdict` is `SkipTerminal` (never re-dispatched);
* a read_error (finalize_error == None) -> the dir is LEFT resumable
  (`state` stays `Ripped`), and the verdict stays `Dispatch`.

Deleting the `write_failed_marker` call inside
`quarantine_incomplete_mux` flips the finalize arm to `Dispatch` and
fails this test — the coverage the vacuous test lacked.

## `quarantine_incomplete_mux_returns_false_when_write_dropped`

FIX-4: `quarantine_incomplete_mux` returns whether the terminal write
actually LANDED, not merely whether the failure was a finalize. On an
unwritable staging mount a finalize failure can't persist `.failed`; the
function must return `false` there so the caller surfaces the dropped
write instead of silently believing the dir went terminal.

Red-before-green: the prior body `return true` for any `Some(finalize)`
ignored the write result, so this dropped-write case wrongly returned
true.

## `finalize_finalize_threads_ripstate_to_worker_gate_and_persists_failed`

FIX-2/FIX-6 production wiring, END TO END through the handoff: a
terminal finalize failure recorded on `RipState` must thread
`RipState.failure_finalize -> MuxHandoffOutcome.failure_finalize -> the
worker's `mux_failure_is_terminal` gate -> a persisted `state: Failed`
that stops re-dispatch. The existing `mux_failure_is_terminal` tests
construct `MuxHandoffOutcome` DIRECTLY, so reverting the
`apply_failure_fields` threading (`outcome.failure_finalize =
rs.failure_finalize`) left the suite green — this drives that exact
line.

Red-before-green: delete the `failure_finalize` line in
`apply_failure_fields` and `outcome.failure_finalize` reads false -> the
gate returns false -> the `mux_failure_is_terminal` assertion and the
SkipTerminal assertion both fail.

## `sweep_loss_abort_quarantines_to_resumable_aborted_loss`

FIX-1: the §3 sweep-loss abort path must quarantine to a RESUMABLE
`.aborted-loss`, exactly as the §4 mux-time loss gate does. Without a
marker the `_mux` worker re-Dispatched the same over-threshold dir every
tick forever (the `.ripped` hand-off survived). This pins, at source
level, that the sweep-loss abort region now calls
`mark_aborted_on_loss`; a companion behavioural assertion proves the
marker flips the worker verdict to SkipAbortedLoss so the re-dispatch
loop stops.

Red-before-green: the pre-fix region wrote only
`reset_status_after_ripping` and `return`, so the
`mark_aborted_on_loss` assertion fails.

## `completed_mux_with_loss_gated_by_abort_on_lost_secs`

v1.2.0 invariant (restored 2026-07-01, "a loss is a loss"): a COMPLETED
mux carrying mux-time (demux/decrypt) loss is gated on
`abort_on_lost_secs` just like read-time loss — missing data is missing
data. Over threshold -> quarantine to a RESUMABLE `.aborted-loss` (a
keydb refresh + re-mux can complete it); within threshold -> hand off
(`.done` if title-confident, else `.review`). The loss is always
reported, never silently dropped.

A behavioural test would need a real lossy ISO + mux pipeline (same
rationale as the sibling gate tests); instead this pins, at source
level, that the success region (a) reports the demux loss, (b) hands
off via a marker within threshold, and (c) gates mux-time loss over
`abort_on_lost_secs` to a resumable `.aborted-loss`.

## `sweep_damage_marker_tests::ripped_marker_sweep_fields_round_trip`

Regression: resume_remux previously passed
SweepDamageSnapshot::default() (all zeros) so a resumed mux showed zero
damage even when the original sweep had bad sectors.

Fix: RippedMarker gained sweep_* fields (serde-defaulted for
back-compat) populated at hand-off time. This test verifies round-trip:
a marker with non-zero sweep_* fields serializes and deserializes
correctly, and the resulting values are what remux_from_ripped_marker
would carry into SweepDamageSnapshot.

## `ripped_marker_title_confident_round_trips`

Regression: an operator title override (or any high-confidence
fresh-rip verdict) must survive the `.ripped` hand-off so the mux
worker's resume_remux auto-files into `.done` instead of holding it for
review. Before the fix, RippedMarker did not carry the verdict and
resume_remux recomputed it from `is_confident_match(disc_label, title,
year)` alone — which an override (chosen title != disc label) fails by
construction, forcing the deliberate pick into `.review`.

## `resolve_done_codecs_prefers_post_mux_then_snapshot`

Regression: the resumed-rip done card must carry codecs. The `_mux`
worker path seeds an empty codecs into STATE and only fills it during
muxing, so the done state must prefer the post-mux STATE value over the
(empty) pre-mux snapshot. The user-triggered path has codecs in the
pre-mux snapshot already; either way the done card must not be blank.

## `resolve_media_type_defaults_empty_to_movie`

Regression: resume `.done`/`.review` markers omitted `media_type`, so
the mover defaulted every resumed rip to "movie" and filed TV-show
resumes under the movie library. The resume path now resolves
media_type the same way the mover reads it: a carried "tv"/"movie"
passes through, and an empty value (cold auto-resume, no carried
metadata) becomes "movie".

## codec/loss capture test (near `resolve_media_type_defaults_empty_to_movie`)

Regression: the origin device's secondary done-state update (in
`crate::muxer::check_and_mux`) was dropping the codec badge, duration,
and output_file because `remux_from_ripped_marker` returned a bare
`bool` and the worker had nothing to plumb them from — the `_mux` STATE
entry it would read had already been removed on success. The fix
captures those three mux-derived fields off the `_mux` done-state just
before removal and returns them in `MuxHandoffOutcome`. This exercises
that exact capture expression against the real STATE map, including the
combined sweep + mux-time loss figures (`lost_video_secs` / `errors` /
`total_lost_ms` / `main_lost_ms`), which the `_mux` done-state folds
demux/decrypt loss into and the origin device's done card must take
instead of the marker's sweep-only subset.

## `fsync_failure_at_limit_dropped_write_preserves_cap`

FIX (fsync cap preservation): at `RESTART_LIMIT`, if the terminal
`.failed` write does NOT land (unwritable staging),
`handle_resume_fsync_failure` must NOT tear down the restart cap and
must NOT report a terminal quarantine. Clearing the counter on a
dropped write resets the loop bound to zero, so the dir re-muxes from a
fresh cap forever — defeating the round-1 bound.

Force the write to fail by making `state.json` a directory (the
tmp->final rename can't clobber a dir); the legacy `.restart_count`
file still bumps.

Red-before-green: the unfixed code discards the return, calls
`clear_restart_count` (-> 0) and `return true`, so BOTH the
`!quarantined` and the `restart_count == RESTART_LIMIT` assertions
fail.

## `fsync_dropped_write_raises_operator_card`

OPERATOR-CARD PARITY: on the cold operator-resume path (a real device,
NOT `"_mux"`), a dropped terminal write (unwritable staging mount) must
raise an operator card the same LOUD way
`persist_terminal_mux_quarantine` does at the muxer site — syslog +
device_log alone don't reach the System page. `check_and_mux`
(muxer.rs) never sees this path (only the `"_mux"` worker call routes
through it), so without a direct `record_error` call here the operator
has no visible signal that the staging mount is unwritable.

Red-before-green: before the fix, `handle_resume_fsync_failure` never
calls `crate::muxer::record_error`, so `MUX_ERRORS` has no entry for
this staging dir and the assertion fails.

## `loss_abort_dropped_write_raises_operator_card`

OPERATOR-CARD PARITY (loss-abort gates): the §3/§4 loss-abort gates now
check whether `.aborted-loss` landed, mirroring
`fsync_dropped_write_raises_operator_card`. A dropped write raises the
same `record_error` operator card so a persistent write failure can't
cause silent infinite re-dispatch.

Red-before-green: before the fix the gates called `mark_aborted_on_loss`
and discarded the result, so a dropped write raised no card.

## `the_accept_loss_override_raises_the_threshold_for_every_resume_gate`

Catches the mutation that recomputes the abort threshold from raw
config at one of `resume_remux`'s loss gates while the other honours
`.accept-loss` — the two-gates-one-run disagreement.

A resume has a sweep gate (§3) and a mux gate (§4). The mux gate used to
call `effective_abort_secs` directly, so an operator's "Accept &
deliver" passed §3 and was then quarantined by §4 against a total that
already contained the very loss they had accepted. The marker is
one-shot and had already been consumed, so the consent was gone with
it.

## `resume_has_exactly_one_threshold_computation`

Catches the mutation that re-introduces a SECOND, hand-rolled threshold
computation in this file — the shape the defect had.

Every loss gate in `resume_remux` must route through
`resume_effective_abort`, so `super::effective_abort_secs` may be
named exactly once here: inside that function. Comment lines are
stripped first so the explanation above cannot satisfy (or break) the
pin.

## `the_accept_loss_marker_is_consumed_only_once_the_rip_is_delivered`

Catches the mutation that moves the `.accept-loss` consumption back to
`resume_remux`'s ENTRY.

The marker must be read at entry but CLEARED only where the override is
spent — at the hand-off, beside `write_completed_marker`. Clearing it
up front threw the operator's consent away on any unrelated transient
failure in between (poisoned config lock, ISO scan error, key failure,
incomplete mux, failed fsync); the automatic retry then ran with the
raw threshold, aborted on the very loss that had been accepted, and
bumped `.restart_count` — enough laps and the dir walks to `.failed`.

## `resume_remux` callers and behavior

Callers and what they actually provide:
- `handle_rip_request` (real device) passes a `ResumeClass::Remux` from
  `find_resumable_for_disc` and the spawn site has already registered
  the per-device `Halt` token (same as `rip_disc`).
- `remux_from_ripped_marker` (the `_mux` worker path) passes a
  freshly-built `ResumeClass::Remux` but does NOT register a `Halt`
  token for the `_mux` pseudo-device.

So neither precondition is relied upon here: a non-`Remux`
classification is handled by the early-return below (logged as a caller
bug rather than assumed away), and the halt-token lookups in the mux
loop tolerate an absent token (the `_mux` path).

On success: writes `.completed` + clears `.restart_count`. On any
failure (scan_image, mux open, mux loop): preserves the partial state
and leaves the counter intact so the next-startup pass promotes the dir
to `.failed` once `RESTART_LIMIT` is reached.

## `classify_resume` eligibility details

Eligibility for `Remux` requires ALL of:
- hint action is `ResumePreserved`
- `has_iso && has_mapfile` (the boolean fields the staging snapshot
  already computed)
- mapfile loads cleanly via `Mapfile::load`
- mapfile `stats().bytes_pending == 0` — no NonTried / NonTrimmed /
  NonScraped left, i.e. every sector has a terminal verdict
- if the disc has a muxable title (UDF read via `Disc::scan_image` is
  deferred to the actor for cost reasons; the classifier approximates
  with the whole-disc `Unreadable` bytes), the bad bytes converted to
  title-seconds (via the 66 Mbps fallback bitrate, same constant
  `rip_disc` uses) are within `abort_on_lost_secs`.

The conservative bitrate fallback is intentional: at classification time
we don't have a `DiscTitle` to call `bytes_bad_in_title` against. The
actor re-validates with the real titles after `scan_image` and aborts
the resume if the per-title check fails.
