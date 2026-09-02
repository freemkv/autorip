# Staging directory bookkeeping notes

Long-form design rationale for `src/ripper/staging.rs`, kept out of the
source file to respect the comment-guard's line caps.

## Unified per-disc state (module header)

Since 1.6.9 the per-disc lifecycle is ONE atomic `state.json` (a
`StagingState` enum + data + an `outputs[]` plan), NOT the old marker
FILES — see the "Unified per-disc state" section in `staging.rs`, which is
the source of truth. The old marker names (`.done`/`.completed`/`.failed`/
`.ripped`/…) survive only as `StagingState` values, as the marker-name
constants used by the one-time legacy-upgrade read-fallback, and in tests.
Steady-state writes never create those files; they go through the `mark_*`
/ `write_*` / `mutate_state` transition helpers, each doing a single
crash-atomic `tmp → fsync → rename → dir-fsync` rewrite of `state.json`.

Lifecycle at a glance: `Sweeping` (owned, sweeping) → `Ripped` (handed to
the mux worker) → `Done`/`Review` (handed to the mover) → `Completed`;
terminal `Failed`; resumable `AbortedLoss`. `muxing` / `accept_loss` /
`restart_count` are orthogonal fields. `restart_count` is the three-strike
gate against an infinite container restart loop from a deterministic
post-startup crash.

## `ABORTED_LOSS_MARKER`

Resumable-failure marker for an abort-on-loss outcome (main-movie /
demux loss exceeded `abort_on_lost_secs` after retries). UNLIKE the
terminal `.failed`, this dir is RECOVERABLE: the full ISO + mapfile are on
disk, so a raised `abort_on_lost_secs`, a code change, or a fresh patch
pass (drive reload) may bring the loss under threshold on a later attempt.
The resume scan re-enters such a dir instead of quarantining it. Carries a
JSON `{reason, attempt, timestamp}` body; `attempt` is the count of
abort-on-loss outcomes so far. A loss-abort is DETERMINISTIC (a plain
re-rip won't change the media damage), so the dir stays resumable
INDEFINITELY — it is never auto-promoted to a terminal `.failed` by
attempt count. The operator resolves it: Accept the loss (deliver as-is)
or run another recovery pass. The `attempt` counter is informational (how
many times it has aborted).

## `SWEEPING_MARKER`

In-progress marker written by `rip_disc` at staging-dir creation (before
Pass 1) and replaced by `.ripped` (or `.failed`) on exit. Its presence
means a sweep+patch is actively running (or crashed mid-sweep) and the
dir is OWNED by the ripper, not orphaned partial state. Carries a JSON
heartbeat/started timestamp so a future stale-heartbeat policy can tell a
live sweep from a dead one. Without it the multi-hour sweep window has no
governing marker: the resume scan restart-counts a healthy long rip toward
`.failed`, and the mover WARNs every 10s tick on the absent `.done`.

## `mutate_state` residual race

RESIDUAL RACE (documented, partially mitigated — needs a follow-up
design): this read-modify-write is lock-free. The "one writer at a time"
invariant is enforced only by the advisory `.muxing`/`.sweeping`
ownership markers, and not every caller honours them. The mux worker's
terminal `write_failed_marker` (state → Failed) and the web
`handle_accept_loss` handler's `apply_accept_loss_reopen` (state →
Ripped) both mutate the SAME dir's state.json with no shared lock, and
the worker does not hold the physical device's claim (it runs on the
synthetic `_mux` device), so the device claim does not serialise them. A
fully correct fix is a per-dir advisory lock (or flock) wrapping the
whole read→modify→write in every writer — broad restructuring,
deliberately NOT attempted here. As a minimal mitigation the primary
observed window is closed at the call site: `handle_accept_loss` refuses
(409) while `.muxing` is set, and the worker clears `.muxing` atomically
with its terminal write, so the accept path only proceeds once the dir is
stable. Other lock-free writers on the same dir remain theoretically
racy under concurrent mutation.

## `write_failed_marker`

A dropped write here is the dangerous case: the dir stays in its prior
state (typically `Ripped`), so the mux worker re-dispatches it every ~10s
tick forever — the exact loop this quarantine exists to break, silently
reopened. The return value lets the quarantine call sites surface a stuck
quarantine LOUDLY (operator signal) instead of swallowing it; the
legacy-marker cleanup stays best-effort. Not `#[must_use]`: the one-shot
startup/auto-resume call sites already get the loud `tracing::error!`
above and have nothing to retry; only the mux worker's per-tick loop
consults the return to raise an operator card.

## `clear_sweeping_marker`

A graceful stop is NOT a crash, so an interrupted sweep must become plain
resumable partial state that the startup classifier does NOT
restart-count (the exact "don't walk a healthy rip to `.failed`"
invariant). The faithful unified representation of "resumable, not
owned, no marker" is the ABSENCE of `state.json` (the legacy model
literally had no marker here) — so when the dir is still `Sweeping`,
remove `state.json`, leaving the ISO/mapfile artifacts. Once the sweep
has already advanced (`Ripped`/terminal), this is a no-op, so it is safe
to call unconditionally.

## `clear_inprogress_markers`

Called on GRACEFUL shutdown (SIGTERM: operator redeploy, reboot,
Watchtower update, `docker stop`). A clean stop is NOT a crash, so every
interrupted dir must be left clean-resumable — otherwise the startup
classifier reads the leftover marker as an in-progress crash and bumps
`.restart_count` toward a false `.failed`. This is the belt-and-suspenders
to the rip-thread cancel: even if a rip drain overruns docker's
stop-grace and the process is SIGKILLed before the
SweepingGuard/MuxingGuard `Drop` runs, the markers are already gone, so
the restart never counts the stop. Only a TRUE ungraceful crash
(panic=abort / OOM / power loss — none of which reach this path) can
leave a marker behind to be counted.

## `mark_handoff`

This is the single hand-off writer; every completion path (ISO,
MKV-resume, inline-mux) routes through it, so the `season`/`tmdb_id`
propagation is identical on every path (the pre-unification bug where
only the ISO path carried them). The `io::Result` return mirrors the old
`write_handoff_marker` shape at the call sites, but is always `Ok` here
since the underlying write is best-effort-logged; kept infallible so
callers that gated on the write result now gate on `true`.

## `durability_gate_passes`

`false` means the output is not provably on stable storage, so `.done`
and `.completed` must be withheld and the staging dir preserved for a
retry — writing them anyway hands the mover a page-cache-only, possibly
truncated file and files it into the operator's library as a finished
title. The fsync is injected so the decision is testable without a
filesystem. It is NOT evaluated for a network sink, which is the point:
an eagerly evaluated `is_network || fsync(path)` would still stat a path
that does not exist.

## `fsync_output_file`

The library's mux `finish()` only flushes its `BufWriter` down to the OS
— the bytes can still be sitting in the page cache when autorip writes
the staging markers and the mover acts on them. On a crash or power loss
in that window the marker says "done" but the file on disk is truncated.
`sync_all()` (fsync) closes that gap. The library's mux `finish()`
swallows an fsync timeout/halt (returns Ok to bound the hang), so
durability cannot be assumed from a successful mux alone — this fsync is
the gate.

## `StagingSnapshot::has_aborted_loss`

`.aborted-loss` resumable-failure marker present — a rip aborted because
main-movie loss exceeded `abort_on_lost_secs`, either read-time
(unreadable sectors, pre-mux gate) or mux-time (decrypt/codec loss,
post-mux gate), but the ISO + mapfile are intact so it's RECOVERABLE
(raised threshold, fresh patch, keydb refresh, code change). Distinct
from terminal `.failed`: the resume scan re-enters such a dir
indefinitely (a loss-abort is deterministic, never promoted to terminal
by attempt count). `attempt` carries the abort count parsed from the
marker (0 if unparseable), kept solely to inform the UI.

## `staging_name_for_disc`

Returns `base` when it is free or already belongs to this disc, and
`base_2`, `base_3`, ... when it is taken by a DIFFERENT disc — the discs
of a boxset all resolve to one title, and before this they silently
shared one directory, so inserting disc 2 after disc 1 finished was read
as "already ripped" and disc 2 was never read at all.

Deliberately does NOT uniquify for the same disc: re-inserting one disc
after a container restart must still find its own dir, or every restart
would re-sweep a finished disc into a fresh directory.

An EMPTY `raw_label` means the caller does not know which disc this is
(a state entry seeded by the mux/mover paths rather than by a drive
scan). That is the mirror of an unlabelled dir and takes the same
conservative answer — plain `base`, the pre-existing behaviour. Without
this an unknown label would compare unequal to every recorded label and
send such a caller off to a fresh `base_2` that no rip ever created.

## `resume_or_quarantine_staging` decision table

- `.completed` exists → idle/clean, leave alone. (The mover will
  pick it up via `.done` if that's also present.)
- `.failed` exists → leave alone; the orchestrator will surface
  the reason in `RipState` once a device claims the dir.
- Partial state (ISO and/or mapfile and/or partial MKV present,
  no completion/failed marker):
  - read `.restart_count`. If `>= RESTART_LIMIT`, write `.failed`
    with a "restart loop detected" reason and clear the counter.
  - else bump the counter; leave the partial state in place so the
    next rip on the same disc can reuse the mapfile/ISO (libfreemkv's
    `sweep_opts.resume` path on transport-failure retries).
- Empty/junk subdir with no recognisable artefacts → wipe.

## `DISC_LABEL_FILE`

The disc's RAW volume label (UDF `meta_title`, else `volume_id`),
recorded in its staging dir at creation.

The dir itself is named for the TMDB-resolved title, which is
deliberately NOT unique: `tmdb::clean_title` strips "disc 1".."disc 4"
before the lookup, so every disc of a boxset resolves to the same title
and wants the same directory. The raw label is the thing that still
tells them apart, so it is recorded here and used to decide whether an
existing dir belongs to THIS disc or merely to one with the same title.

Absent in dirs written before this existed. A missing label reads as
"same disc", which preserves the old skip-on-`.completed` behaviour for
legacy staging rather than re-ripping it on upgrade.
