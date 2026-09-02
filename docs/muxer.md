# Mux worker — hand-off contract and rationale

Long-form rationale relocated out of `src/muxer.rs` doc comments to satisfy
the comment-guard caps. Each section is pointed to from a short comment at
the relevant spot in the source.

## Hand-off contract (unified `state.json`, since 1.6.9)

The staging lifecycle is one `state.json` per disc (a `StagingState` enum +
data), NOT the old marker FILES. The `.ripped`/`.done` names survive only as
`StagingState` values and as a legacy read-fallback; write paths go through
`crate::ripper::staging` transition helpers.

1. The drive thread (`ripper::rip_disc`) finishes sweep + patch.
2. It transitions the dir to `state: Ripped` (via `write_marker` →
   `staging::try_write_state`), recording everything the mux worker needs to
   reconstruct a `MuxInputs` (TMDB metadata, byte counts, batch size, ISO
   filename, plus the `outputs[]` plan for a TV disc).
3. If `cfg.auto_eject` is set, it ejects the drive — the disc is no longer
   needed once the ISO + `state: Ripped` are on disk.
4. The drive returns to `idle`, ready for the next disc.
5. This worker polls the staging dir, dispatches `state: Ripped` dirs
   (`mux_dispatch_verdict`), muxes against the ISO, then transitions to
   `state: Done`/`Review` (the mover's hand-off) via `staging::mark_handoff`.
   On failure it records a `MuxerError` and leaves the dir in `Ripped` for
   next-tick retry / operator inspection.

## `RippedMarker::title_confident` and `resume_remux`

True when the fresh-rip path decided the title is trustworthy enough to
auto-file (`.done`) — either an exact normalized match with a year OR an
explicit operator override via the '✎ change' picker. The mux worker's
`resume_remux` ORs this into its own match check so an operator's deliberate
pick isn't second-guessed when the chosen title differs from the disc's own
(often cryptic) label.

## `MUX_DISMISSED` — why dismissal must survive re-scans

The dismissal is lifted when the dir is freshly DISPATCHED (a new mux
attempt may produce a new error worth showing) or when the dir is pruned
(gone from staging). Without this, the move-errors' "reappears if still
blocked" model would make an old loss-abort card un-dismissable — the exact
"old errors hanging around" complaint.

## `mux_dispatch_verdict` ordering

Order matters and mirrors `check_and_mux`'s former inline guards:

1. `None` snapshot ⇒ `SkipUnknown` (don't dispatch on a degraded listing).
2. `.completed` OR `.failed` ⇒ `SkipTerminal` — terminal regardless of
   whether `.ripped` still lingers (the terminal-mux-failure `.ripped`+`.failed`
   re-mux loop, da16f00, lives or dies on this arm).
3. `.aborted-loss` ⇒ `SkipAbortedLoss` — a completed mux whose delivered
   loss exceeded `abort_on_lost_secs`. Checked AFTER `.failed`/`.completed`
   (so a promoted-to-terminal dir stays terminal) and BEFORE the `.ripped`
   dispatch, because the loss-abort leaves `.ripped` in place: without this
   arm the worker re-muxes the whole ISO every tick to reproduce the exact
   same deterministic loss. Mirrors the drive-side classifier's ordering in
   `staging.rs` (`.failed` before `.aborted-loss`).
4. `.ripped` absent ⇒ `SkipNoMarker`.
5. otherwise ⇒ `Dispatch`.

`has_ripped` is read from the same primed `read_dir` view as the snapshot so
a cold-cache NFS miss can't race `.ripped` to "absent" while the snapshot
surfaces a terminal marker — see `StagingSnapshot::has_ripped`.

## Terminal-failure-class (`MuxFailureClass`) {#terminal-failure-class}

Whether a mux-worker failure is TERMINAL — the staging dir should be
quarantined (`state → Failed`) so `mux_dispatch_verdict` stops re-Dispatching
it every tick — vs left resumable. Pure projection so the decision is
unit-testable without a real mux pipeline. Terminal IFF a structural
FINALIZE failure surfaced (`is_finalize` — the MKV could not be finalized,
e.g. E6008 no muxable frames / unseekable output). NOT terminal for: an
aborted-loss (owns its own resumable state), or ANY non-finalize failure.

The prior gate was `!aborted_loss && has_worker_reason && !failure_retryable`
— but `failure_retryable` (`RipState::failure_deferred`) is set true ONLY on
the keyless deferral path. A genuinely RESUMABLE non-deferral failure — a
mid-mux read error (truncated MKV), an fsync failure below RESTART_LIMIT, the
unreadable-mapfile TOCTOU — has `failure_retryable == false`, so that gate
FALSE-QUARANTINED it (state → Failed) even though `resume_remux`'s own gate
leaves it resumable: a lost rip that would have succeeded on retry. Gating on
the SAME finalize-error signal `resume_remux` uses (threaded through as
`MuxHandoffOutcome::failure_finalize`) quarantines ONLY a structural finalize
error. `has_worker_reason` is kept as a defensive precondition (a finalize
always carries a reason).

The three flags (`aborted_loss`, `has_worker_reason`, `is_finalize`) are
passed as named struct fields rather than positional bools: they are
same-typed and adjacent, so a positional call could transpose two of them
and still compile — silently inverting the terminal-vs-resumable verdict,
the highest-stakes bug class on this path. Named construction makes a
transposition a compile error.

## `persist_terminal_mux_quarantine` {#quarantine-persistence}

A dropped terminal write leaves the dir in its prior `Ripped` state, so the
worker re-Dispatches it every tick (a full re-mux each time) — the exact
loop this quarantine exists to break, silently reopened. It returns whether
the terminal state actually landed so the alarm can't be lost by discarding
the `write_failed_marker` return (the round-1 gap this closes).

## `should_revert_origin_to_done` {#origin-revert-rules}

Two rules:

1. A real origin device only needs the revert if it is STILL "ripping" — the
   inline-mux FALLBACK path (the `.ripped` marker write failed, so
   `rip_disc` muxed inline while leaving the tile "ripping"). On the normal
   `.ripped` hand-off path the tile is already "done" (the read finished)
   and this is a no-op, so the synthetic `_mux` worker can never push a real
   "done" tile back through "ripping" (bug #1).
2. A synthetic underscore-prefixed `origin` (defensive — should not occur,
   the marker's `origin_device` is the physical drive) is never reverted:
   those carry no user-visible tile.

`status == None` (the device entry vanished — re-used / cleared) ⇒ no
revert, matching the prior `.unwrap_or(false)`.

## Test rationale

### `.muxing` entry-side TOCTOU fix {#muxing-toctou}

Stamping `.muxing` only just before the mux (after `read_marker`) left a
window where `is_muxing == false` and a concurrent web entry raced the
muxer's state.json write. Pins, at source level, that `write_muxing_marker`
precedes `read_marker(&dir)` in the worker loop. Red-before-green: the
pre-fix order (stamp after `read_marker`) reverses the two indices and fails
the assertion.

### FIX-2: `failure_finalize` replaces `!failure_retryable` {#fix-2}

The worker's quarantine gate is fed from the `MuxHandoffOutcome` the resume
path returns. A resumable, NON-finalize worker failure (a mid-mux read
error — truncated MKV, resumable) carries `failure_finalize == false`, so it
must NOT be quarantined; a structural finalize failure carries
`failure_finalize == true`, so it MUST be. The prior gate keyed on
`!failure_retryable`, which is false for BOTH — so it false-quarantined the
read error (a lost rip that would retry-succeed).

### FIX-3: alarm on a dropped terminal write {#fix-3}

The mux worker's terminal-quarantine site consumes `write_failed_marker`'s
return. When the state.json write LANDS, the dir goes terminal and no
operator card is raised. When it does NOT land (unwritable staging), the
site must surface a LOUD operator card so the stuck quarantine is visible
instead of silently re-dispatching forever. Red-before-green: if the site
reverts to discarding the return (no alarm on a failed write), the
`MUX_ERRORS` assertion goes RED.

### Unreadable staging root must not render as an empty queue {#silent-empty-queue}

An unreadable staging root is not an empty mux queue. When the share is down
(NFS timeout, permissions lost, the mount not yet up at container start) the
System page rendered "no jobs queued" and there was nothing anywhere — no
log line, no error card — to say the list was a guess. That is the
failure-that-looks-like-success class: an operator sees a queue they believe
is empty and concludes the mux worker is idle.

### Per-entry `read_dir` errors must not be silently dropped {#per-entry-error}

A source-pin because the branch needs a dentry-level failure (an NFS ESTALE
on one entry of an otherwise-healthy directory) that cannot be synthesised
locally. `staging::resume_or_quarantine_staging` already carries the same
defense with the same reasoning: dropping a per-entry error silently removes
a whole disc subdir from the queue, and the operator watches a queued title
simply disappear.
