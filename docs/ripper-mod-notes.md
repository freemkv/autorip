# `src/ripper/mod.rs` — extended comment notes

Long-form rationale relocated out of source comments by the comment-guard
pass. Each section below corresponds to a `// See docs/ripper-mod-notes.md`
pointer left at the original comment site in `src/ripper/mod.rs`.

## Module history

This module was originally a single 4350-line `ripper.rs`. The state
types, thread/halt bookkeeping, and staging-dir helpers were later
lifted into sibling sub-modules (`state`, `session`, `staging`).

## `ScanWatchdog` design

Closes the "scan_disc had NO watchdog" incident: `libfreemkv::Disc::scan`
and the subsequent `resolve_keys_from_drive` are blocking calls with no
autorip-side liveness signal, so a wedged drive read during scan, or a
hung keyserver round-trip during resolve, would show the UI stuck on
"scanning" forever with nothing in the log. `ScanWatchdog` arms a thread
that emits a WARN every 15s ("scan still running, Ns elapsed, last
phase=X") until the returned guard is dropped (mirrors the rip watchdog
and `mux.rs` watchdog drop-guard design). The caller advances the phase
via `ScanWatchdog::enter_resolve` so the WARN pinpoints whether the time
is going into the structure scan or the key resolve.

## `is_halt_error`

True when an `io::Error` from a mux construction call
(`build_iso_pipeline` / `DiscStream::new`) is a user Stop
(`Error::Halted`, code E6010) — vs a structural failure that must be
quarantined. `Error` Displays as `E<code>` (Halted has no payload) or
`E<code>: <data>`, so the code is ALWAYS the leading token: match it
EXACTLY, never a substring-scan — a data payload that merely contains
the digits (e.g. a `NoDiscKey` disc-hash rendering to `E7022:
…E6010…`) must not be misread as a halt.

## `is_fmts_key_missing_error`

True when an `io::Error` from a mux construction call
(`build_iso_pipeline`) is a missing-FMTS-forensic-key error
(`Error::FmtsKeyMissing`, code E7026): the disc's base AACS keys
resolved but its online-only per-index-phase forensic keys did not, so
the mux can't proceed. On the resume / deferred-mux path this is
treated as a retryable DEFERRAL (preserve the ISO, wait for keys), not
a hard failure — mirroring the no-keys capture deferral. Uses the same
exact-leading-token match as `is_halt_error`, never a substring scan,
so a data payload that merely contains the digits can't false-match.

## `key_readiness`

Human-readable key readiness for the dashboard tile, decided at scan
time. Unencrypted, or keys present → "Ready to rip"; encrypted with no
keys → "Missing keys — <reason>", where the reason reflects WHAT
happened when we tried to resolve (key service unreachable / no key /
disc-data anomaly / couldn't read the disc's key files), or — for a
local source — the concise libfreemkv AACS failure heading.
Capture-without-keys overrides to a proceed-anyway state. The tile
keys its action button off the "Missing keys" prefix: any other value
(including "Capture without keys") leaves the green Rip button up.

## `FmtsGate`

What the pre-rip FMTS forensic-key gate should do, given whether the
complete (base + per-index-phase) map resolved and the operator's
capture setting. The forensic index keys are online-only and were
historically not resolved until mux — so a base-keyed-but-forensic-
missing FMTS disc used to sweep for ~an hour, THEN fail at mux.
Resolving the full map up front lets us decide before the sweep,
honouring `capture_without_keys` exactly like the base gate.

## `FmtsGatePlan`

The side-effect routing for each `FmtsGate` outcome, split out as a
pure function so `rip_disc`'s gate handling is unit-testable (and
mutation-verifiable) without driving a whole rip.

* `defer_forensic_mux` — arrange the deferred-capture mux-skip. Base
  AACS keys are present (so the no-keys `keys_missing` skip won't
  fire), but the online-only forensic index keys are not — muxing now
  would emit forensic garbage. So BOTH mux-skip points honour this
  flag: single-pass surfaces the "enable multi-pass" guidance;
  multi-pass preserves the ISO for resume (and `resume_remux`
  re-defers on `FmtsKeyMissing`), exactly like the no-keys capture flow.
* `quarantine` — write the `.failed` marker + clear the restart count
  so the skipped disc's staging dir is cleaned up like every other
  early-failure exit in `rip_disc`, instead of being left empty and
  marker-less until a restart.

## `should_retry_online_keys`

Should the rip re-attempt online key resolution before proceeding? The
outage retry re-reads the disc through the key service and REPLACES
the `Disc` the rest of the rip runs against, so it must fire on
exactly one situation: an ENCRYPTED disc that the online key service
left with NO keys, with the operator's capture-without-keys escape
hatch off.

- not online-backed → nothing to retry (local keydb / drive-derived keys).
- `capture_without_keys` → the operator asked for the raw ISO NOW and
  the forensic mux is deferred; retrying would stall that path waiting
  for a key it was told not to wait for.
- not encrypted, or keys already resolved → there is no key to fetch,
  and entering the retry path would swap in a re-read disc for no reason.

Every term is an AND. Relax one to an OR and an unencrypted or
already-keyed disc takes the outage path, so the rip continues against
a disc whose decrypt state is not what the caller assumed — garbage
output filed as a completed title (rule 1). Drop the `!` and
capture-without-keys loses its untouched ISO-now path.

## `retry_online_keys_on_outage`

Given an online key resolution that produced NO key for an encrypted
disc, classify the key service and — on a transient outage —
bounded-retry the resolution. Returns the (possibly re-resolved) disc,
its final `KeyOutcome`, and the reachability verdict when the disc is
STILL keyless: `None` if the service was reachable (a genuine no-key —
the caller keeps its existing behaviour), or `Some(transient)` when
the service never recovered within the retries (the caller surfaces a
retryable state instead of a permanent "no keys" error).

Precondition: the caller has already resolved once and got `NoKey` on
an encrypted, still-keyless disc with `key_source == "online"`.

## `forget_removed_device`

Tear down every piece of per-device state for a drive that vanished
from the enumeration (hot-unplug), returning `true` if the teardown
ran. Extracted from the hot-plug reconcile in `drive_poll_loop` so the
decision is testable without driving the 30-second poll loop.

A device whose worker still holds it is never torn down, and the
caller must keep polling it. That takes TWO facts, not one:

* `is_busy` is `STATE[device].status == "scanning" | "ripping"` — it
  IS the double-rip guard, so removing the STATE entry of a drive that
  is mid-rip makes every later `is_busy()` return false and opens the
  rip/scan dispatch guards for a second concurrent rip on the same
  physical drive. A device only has to miss ONE enumeration pass for
  that to happen, and a SCSI/USB reset during wedge recovery does
  exactly that while a multi-hour rip runs. The 60s
  `device_first_seen` grace on re-add delays the re-probe; it does not
  prevent it.
* `rip_thread_running` is the thread-liveness fact `is_busy` cannot
  give. A worker writes its TERMINAL status and then keeps running:
  auto-eject (`Drive::open` + `session.eject()` — slow hardware I/O,
  and opening a tray is itself a documented trigger for
  re-enumeration), the eject-failure log lines, guard drops. Gating on
  status alone tore the session, the STATE row and the log ring out
  from under that tail. `forget_device_session_state` already refuses
  to reap a live `JoinHandle` for this reason; both halves of one
  teardown now ask the same question.

So: defer the whole teardown while busy, rather than tearing down but
preserving STATE, or force-failing the rip. Preserving only STATE
would evict the log ring and the session out from under a live rip
thread that is still writing to both — trading a double-rip for a
truncated device log and a lost `DriveSession`. Force-failing the rip
would let one missed enumeration pass kill a good rip that is still
making progress; the rip thread has its own watchdog and its own I/O
errors to decide that with.

The cost of this choice: if the drive really was unplugged, its state
(STATE row, session, log ring, overrides) lingers until the rip thread
notices the dead device and exits — the UI shows a phantom drive for
that window, and a rip thread that hangs forever leaks it for the
container's lifetime. Adding the liveness half extends that accepted
cost from the ripping phase to the worker's tail; it self-heals on the
next rescan the moment the thread exits (a panicked thread reads as
finished). That is still the cheaper failure: a stale UI row versus
two rips on one drive, or a live worker's session and log ring pulled
out from under it.

## `insert_tick`

Decide both halves of a tick's response to an observed disc. The two
answers have to agree, and that is the whole point of this function.
`dispatch` is suppressed during the post-Stop cooldown so a
stop-then-reinsert does not immediately re-rip. `latch` feeds
`had_disc`, which is what makes the NEXT tick's `is_new_insert` false —
and `is_new_insert` gates the only auto-scan/auto-rip trigger the poll
loop has. Latching a disc this tick declined to dispatch therefore
retires the trigger permanently: the disc is remembered as handled
when nothing handled it, and no later tick will ever act on it again
until the operator physically ejects and reinserts.

So: latch only what was dispatched, or what was already latched.
`POLL_INTERVAL_SECS` (5) equals `STOP_COOLDOWN` (5s), so at most one
tick is deferred this way before the cooldown expires and the disc is
picked up normally.

## `drive_poll_loop` architectural note (0.13.2)

autorip is dumb — it never touches hardware paths, sysfs, SCSI, or
USB. The lib's `list_drives()` does the platform-specific enumeration
(sg/disk/CdRom paths, peripheral-type filtering, INQUIRY for
vendor/model). The lib's `drive_has_disc(path)` does the disc-presence
probe with internal wedge-recovery (SCSI reset → USB reset) hidden
from the caller.

## `staging_basename_for_device`

Callers that already hold the two identity values locally
(`scan_disc`, `rip_disc`) call `staging::staging_basename` directly;
nobody re-derives.

## `disc_already_completed`

Does the currently-scanned disc already have a `.completed` staging
dir? Title-matches the scanned disc name against staging dir basenames
(same exact/prefix convention as `find_resumable_for_disc`) and
reports whether a match carries the process-level `.completed`
marker. Used only to gate the unattended auto-rip path so a container
restart doesn't re-rip a disc that already finished.

## `staging_dir_matches_disc`

True if a staging-dir basename is the resume/completion match for a
sanitized disc name. EXACT equality only: staging dirs are created
with the exact sanitized disc name (no year/suffix), so a prefix match
never legitimately fires — it only invites collisions where a shorter
title's name is a prefix of a longer one with a separator ("Redshift"
sanitizes to "Redshift", "Redshift 2" to "Redshift_2"). Exact equality
is collision-free. Both `disc_already_completed` and
`find_resumable_for_disc` route through this so the rule can't drift
apart between the two call sites.

## `staging_disc_completed`

Pure core of `disc_already_completed`: does a staging dir whose
basename exactly matches `sanitized` carry `.completed` AND not
`.review`? Split out (no `STATE`/`Config` reads) so the M4
held-for-review gating is unit-testable.

`.completed` alone is NOT enough: the auto-resume mux writes
`.completed` even for a rip HELD for operator review (it writes
`.review` instead of `.done`, then `.completed`). Treating such a dir
as "already ripped" would make the unattended insert path skip it as
finished while it's actually awaiting operator confirmation. A
held-for-review disc is therefore NOT "already completed": require
`.completed` AND absence of `.review` (M4). The review UI's
`list_held` keys on `.review` independently, so it still surfaces the
dir.

## `disc_owned_by_worker`

Does the currently-scanned disc have a staging dir that is OWNED by
the mux worker (`.ripped` hand-off pending, or `.muxing` lock held)?
Used by the unattended Default auto-insert path to refuse a fresh
sweep on such a dir: a fresh sweep would truncate the ISO the mux
worker is reading (or is about to read). Mirrors
`disc_already_completed`'s lookup (exact-name match, NFS-resilient
listing) but checks the owner markers instead of `.completed`.

## `resumable_dir_blocked`

Is this staging dir blocked from drive-resume (Remux) by an owner,
held, or terminal marker? Pure projection of the snapshot booleans so
the H1/M3 skip rules are unit-testable without seeding `STATE`/`Config`.

- `.ripped` / `.muxing` — OWNED by the mux worker. Returning Remux
  would double-mux the same output (the worker is already on it) and,
  on `.muxing`, race the worker for the ISO it's reading (H1).
- `.review` — HELD for operator review; the operator hasn't resolved
  the title match. Re-muxing would overwrite the held output before
  they decide (M3).
- `.failed` — TERMINAL; a prior attempt gave up (the ISO may be
  partial/aborted). Don't silently re-mux past it; the operator must
  explicitly Wipe + re-rip. Keyed on PRESENCE (`has_failed`) so a
  non-JSON `.failed` body is still honoured (M3).

## `end_of_recovery_lost_ms`

The end-of-recovery loss figure in milliseconds, and whether it is
trustworthy. Pure and separate from the rip loop deliberately. This
decision lived inline inside a function that needs a drive, a mapfile
and live config, so no test could reach it — which is how it shipped
stepping over a failed promotion. A test of `loss_aborts` alone does
NOT guard it: the bug was never in the gate, it was that the caller
handed the gate a number it should not have trusted.

`promotion_intact == false` means the damage record is incomplete, so
no figure derived from it can be believed and the answer is NaN —
which `loss_aborts` treats as abort under every threshold, including
the operator's accept-loss override. Mirrors `freemkv_engine`'s own
end-of-recovery gate.

## `end_of_recovery_loss`

Measure the loss the end-of-recovery abort gate decides on, reading
the ALREADY-PROMOTED mapfile (the caller has marked every leftover
"maybe" sector Unreadable, so Unreadable here means confirmed-lost,
not not-yet-tried).

The seam for the gate that decides whether a damaged multipass rip is
quarantined as `.aborted-loss` or muxed and filed into the library.
Both conditions below were inline in `rip_disc` where no test could
reach them, and a mutation run flipped each without a single failure:
report zero for a disc WITH confirmed unreadable in-title sectors and
the rip looks clean to `loss_aborts`, gets muxed, gets `.done` +
`.completed`, and the mover files a damaged movie into the library
(rule 1 — the failure the comments around this block say already
shipped once).

Zero is reported only when there is genuinely nothing to report:
* no unreadable bytes anywhere in the image (a clean rip), or
* unreadable bytes exist but none fall inside the muxed title — a
  scratched menu or trailer must not abort an MKV rip, though for ISO
  output, where the whole image is the deliverable, it does count.

The zero-unreadable case short-circuits BEFORE `promotion_intact` is
consulted, matching the previous inline order: a promotion that failed
with nothing to promote leaves a clean rip clean.

## `find_resumable_for_disc`

Look at the staging dirs for a Remux-eligible entry whose dir basename
matches (exact, prefix-either-way) the sanitized display_name of the
currently-scanned disc. Returns the `ResumeClass::Remux` payload if
found, else None.

Single-drive convention: the host has one drive, one inserted disc at
a time. There is at most one staging dir matching the disc by title
prefix. If somehow two match we pick the first; in a multi-drive
future this needs disambiguation by stable disc fingerprint (UDF
volume_id) instead of sanitized title.

## `is_safe_staging_segment`

True if `seg` is safe to use as a single staging-directory path
segment. Rejects values that could escape the staging root or resolve
to it: empty, all-dots (`.`, `..`, `...`), anything containing a path
separator, and absolute paths.

`display_name` derives from untrusted disc bytes / TMDB JSON, and the
consequence of getting this wrong is `join("..")` + `remove_dir_all`
deleting the PARENT of staging.

This is deliberately an INDEPENDENT check, not a restatement of the
sanitizer's. `util::sanitize_path_compact` does keep `.` as a
character, but it finishes with `util::ensure_safe_segment`, which
already collapses empty / all-dots results to `SAFE_FALLBACK`
("untitled") — so a segment that really came through the sanitizer
cannot be `..`. (An earlier version of this comment claimed the
sanitizer "does not reject these", which stopped being true when
`ensure_safe_segment` was added and made this guard look like the only
thing standing between a hostile disc label and the parent directory.)
The guard stays because it is cheap and because it must also hold for
segments assembled from other sources — config, marker files,
re-derived names — that never passed through the sanitizer at all.

## `resumable_for_disc`

Detect whether `display_name`'s disc has resumable staging state and
of what kind. Mirrors `find_resumable_for_disc`'s directory matching
but classifies by `bytes_pending` rather than only accepting complete
ISOs: `bytes_pending == 0` → `Resumable::Remux`, `> 0` →
`Resumable::Sweep`. Pure (no STATE, no side effects); used by both the
scan-time detector and the `?resume=yes` action.

## `SweepingGuard`

RAII guard that clears the `.sweeping` in-progress marker on drop.

`.sweeping` is written immediately after the staging dir is created
(before Pass 1) and governs the whole multi-hour sweep+patch window.
The terminal-marker writers (`write_failed_marker` /
`write_completed_marker`, and the `.ripped` hand-off in `muxer`) all
clear it first, so on those success/`.ripped`/`.failed` paths this
guard's clear is an idempotent no-op. It only fires on `rip_disc`'s
many early-return error branches (disk-space preflight, Pass 1 halt /
failure, transport-recovery exhausted, ISO-open / mux-build failures,
durability-gate failures) and on panic — every one of which previously
leaked a stale `.sweeping`. A leaked `.sweeping` makes the next
startup's `resume_or_quarantine_staging` classify the dir `InProgress`
forever (never restart-counted, never cold-resumed), stranding dirs
that hold a complete ISO + clean mapfile. Holding the guard for the
whole `rip_disc` body guarantees the marker is cleared on every exit.

## `install_rip_halt`

Install this rip attempt's initial `Halt`.

The spawn site (`/api/rip`, `/api/scan`, and the disc-insert poll loop)
registers a fresh token before the thread starts, so an HTTP Stop
during the scan phase has something to flip. `rip_disc` replaces it
here, and again at the drive-backed swap once the drive is open.

The replacement must CARRY the outgoing token's cancel.
`handle_rip_request` checks `device_halt(device).is_cancelled()` after
the scan and before calling `rip_disc`; a Stop that lands in the gap
between that check and this line cancels the spawn-site token, and a
blind insert then throws it away — the operator's Stop is silently
discarded and the rip runs to completion. There is no stale token to
worry about: `HaltGuard` unregisters on every exit from `rip_disc`,
and the spawn site registers a fresh one per attempt, so whatever is
here is THIS attempt's token.

## `abort_post_mux_preserving_staging`

Report a post-mux failure that leaves the staging dir RESUMABLE, and
hand the drive back.

The two hand-off failures late in `rip_disc` (the fsync durability
gate and the `.done`/`.review` marker write) are not disc failures:
the MKV is in staging and a later attempt can finish the job, so
neither writes `.failed` and neither is terminal for the disc. They
ARE terminal for this rip attempt, though, and every other exit from
`rip_disc` says so by leaving a terminal status behind. Reporting only
through `last_error` and returning with `status` still `"ripping"`
leaves `is_busy()` true forever: the poll loop then skips that drive
on every tick for the container's lifetime while `/api/state` renders
a healthy in-progress rip.

## `fire_rip_complete_webhook`

Fire the drive-free `rip_complete` webhook: the disc read is finished
and the drive is free — the operator can load the next disc while any
mux runs on a separate worker. This is the FIRST of the three
pipeline-stage hooks (rip → mux → move) and is deliberately distinct
from `mux_complete` (the `.mkv` is produced) and `move_complete` (it
lands in the library).

Called at each point `rip_disc` decides whether to auto-eject — that
decision point IS "rip done", whether or not the physical eject
actually happens (auto_eject may be off). Damage figures (`errors`,
main-feature loss) are read live from `STATE`; `size_gb` is the staged
ISO's size. The read-side elapsed/throughput aren't tracked at the
eject point (the mux worker re-derives its own), so they're reported
as 0 — same as the `.ripped` marker's `rip_elapsed_secs`.

## End-of-recovery promotion (in `rip_disc`'s multi-pass sweep)

Promotes still-NonTrimmed bytes to Unreadable, feeding the abort check
below via the SAME in-memory map (a prior re-load design saw zero
Unreadable bytes pre-flush). Only multi-pass has a "final pass".

A user STOP is NOT recovery-exhaustion: skip promotion and the abort
check so un-retried ranges stay recoverable for a later Resume (a
prior bug promoted them anyway and wrote a spurious `.aborted-loss`).

## Mux reader/stream notes (in `rip_disc`)

DiscStream gets the per-device `Halt` at construction (covering the
CSS crack too). Stop interrupts `fill_extents` at the next retry
boundary, required during dense bad-sector regions where a frame may
never emit.

Multipass ISO path: wrap the reader in `PrefetchedSectorSource` so
read+decrypt run on a producer thread (~70→124 MB/s on the testbed).
Single-pass keeps the inline reader, since prefetch would bypass
`fill_extents`'s retry.

Reader-side stream events are forwarded by `mux_stream`'s
`AutoripMuxEvents` bridge for both ISO and live paths, so the old
shared closure is gone; its atomics are now handed to the bridge via
`MuxAtomics` below.

Mux-phase progress denominator: multipass/resume reads the WHOLE disc,
so disc capacity is correct there. Single-pass streams only the
title's extents, so disc capacity there plateaued the bar — scope to
the extent sum.

## `should_auto_eject`

Two rules, both load-bearing for the rip→mux→move state machine:
1. Only when the operator enabled `auto_eject`.
2. NEVER for a synthetic, underscore-prefixed device (`_mux`, etc.).
   Those carry the background mux/move work AFTER the drive thread has
   already handed off (and possibly already ejected). The synthetic
   `_mux` worker reaching a completion path must not issue a second
   eject against whatever disc the physical drive now holds. The real
   drive ejects exactly once, at the `.ripped` read-complete hand-off.

Centralizing this here makes the "fires once, at read-complete, never
from the mux worker" contract a single unit-testable predicate instead
of an inline `&&` duplicated across the fresh-rip and resume paths.

## `mux_progress_denominator`

Pick the mux-phase progress denominator (used for percent + ETA).

Multipass / resume mux reads the whole disc-capacity ISO, so its read
position climbs to `disc_capacity_bytes` (passed in as `total_bytes`)
— keep that. The single-pass path (`max_retries == 0`) streams ONLY
the selected title's extents over the live drive, so `DiscStream`'s
`BytesRead` caps at `Σ sector_count × 2048` (the title extent byte
sum). Using disc capacity as the denominator there made the bar
plateau at `title_size ÷ disc_capacity`. Scope to the extent sum so
single-pass progress reaches 100%. Falls back to `total_bytes` if the
title has no extents (e.g. degenerate scan).

## `abort_lost_bytes`

The unreadable byte count that the abort gate scopes to: whole-disc
for an ISO deliverable, in-title only for an MKV (a scratched
menu/trailer outside the muxed title does not count for an MKV mux).
This is the RAW source of truth the `abort_on_lost_secs == 0`
("perfect") gate keys on — no bitrate, no float — so a zero-bitrate
title can never hide unreadable loss.

## `abort_lost_ms`

Milliseconds of loss that the post-retry abort check should weigh.

For a raw ISO rip the whole disc is the deliverable, so every
unreadable byte counts. For an MKV/m2ts mux only the bytes that fall
inside the muxed title's extents matter — a scratched menu / trailer
that lives OUTSIDE the title must not count, otherwise an
`abort_on_lost_secs=0` ("perfect rip") setting would abort a
fully-recovered main movie just because some out-of-title sector was
lost (the Top Gun false-positive). Mirrors the per-pass loop-exit
gate's `mux_scope_bad` scoping.

## `loss_aborts`

`should_abort_for_loss` lives in freemkv-engine; keeping a second
local name is how the two crates drifted, so tests call the engine
directly. Contract: strictly `>`, so NaN always aborts (fail-safe,
since `NaN > x` is false).

The flawless-rip loss gate. `abort_on_lost_secs == 0` means ZERO —
abort on ANY lost byte (unreadable OR undecryptable), keyed on the raw
byte count so no bitrate/float rounding can let a sub-second or
zero-bitrate loss slip through ("0 means ZERO, not <1s"). `> 0` keeps
the time-based tolerance: abort only when the loss exceeds N seconds.
A NaN `lost_ms` always aborts (fail-safe: an unquantifiable loss must
never pass as success).

## `mux_loss_aborts`

Whether mux-time (decrypt/codec) loss must quarantine the rip instead
of filing it as done.

This is the SOLE enforcement point for mux-time loss: the pre-mux gate
reads only the mapfile Unreadable set, and decrypt/codec skips never
appear there. It was four inline conditions inside `rip_disc`, where
nothing could reach it — a mutation run flipped every one of them
(`&&`→`||`, `>`→`<`, dropped `!`) and the whole suite still passed, in
both directions: silently filing a rip with concealed in-title loss as
`.done`, and quarantining a clean rip.

- ISO output is exempt: whole-disc, gated at 100% elsewhere.
- Only fires when the MUX itself contributed loss; read-time loss
  alone has already passed the pre-mux gate, so re-gating it here
  would double-count.
- `effective_abort == 0` means ZERO tolerance: any loss at all aborts.

## `uses_multipass`

Does this `max_retries` setting select the MULTI-PASS rip route? One
predicate for a decision taken in eight places along `rip_disc`,
because they must all agree or the rip is incoherent. Multipass
(`max_retries > 0`) sweeps the disc to an intermediate ISO with a
mapfile sidecar, runs patch passes over it, promotes leftover
NonTrimmed sectors to Unreadable at the end of recovery, and muxes
FROM the ISO. Single-pass (`max_retries == 0`) streams the selected
title straight off the drive: no ISO, no mapfile, and therefore no
mapfile-derived loss accounting (its abort gate runs after the mux, on
the demux skip count).

Disagreement between the sites is not cosmetic. Taking the multipass
route at `max_retries == 0` looks for a mapfile that was never
written, and the unloadable-mapfile fail-safe then forces
`main_lost_ms = NaN`, which aborts EVERY single-pass rip. Taking the
single-pass route at `max_retries > 0` skips the mapfile loss
accounting entirely and reports a damaged rip as clean (rule 1).

## `done_card_lost_ms`

The done card's `total_lost_ms` / `main_lost_ms`, in ONE place.

Single-pass mode (`max_retries == 0`) has NO mapfile, so
`sweep_damage_snapshot` is the all-zero `Default`. Publishing its
`0.0` starves the ms-branch of `classify_damage`: a rip that skipped a
handful of sectors but lost >1 s of low-bitrate video is rated
"cosmetic" (10 < the 51 sector threshold) instead of "moderate" — a
damaged rip filed as clean. `final_lost_secs` IS the in-title loss for
single-pass (the demux-skip estimate), so that is what the card must
carry. Multipass keeps the snapshot's value, which is genuinely
computed from the mapfile's per-range durations, plus any demux-time
extra.

This lived twice as an inline `if !uses_multipass(...)` inside a
40-field `RipState` literal, and the test that claimed to guard it
only called `damage_severity_for` with two literals — so
reintroducing the starved value left the suite green. One function,
one test.

## `title_is_confident`

Is the resolved title trustworthy enough to auto-file the finished rip
into the library, or must it be HELD for operator review?

One disjunction decides `.done` vs `.review` for BOTH completion
routes (the MKV path and the ISO path), so it lives in one place:

- **No TMDB API key configured** → no rip can ever produce a confident
  match, so gating on the match would park every rip in `.review`
  forever. Operators running keyless expect the disc-label filename,
  so treat it as confident.
- **Operator override** → the human picked the title from the Ripper
  card. Nothing is more confident than that.
- Otherwise → the TMDB match must be exact-title-plus-year
  (`is_confident_match`).

Each term stands alone; require them together instead and an operator
with a key configured never auto-files anything. Getting it wrong the
other way is a rule-2 hazard: a GUESSED title is filed into the
library, where the mover can land it on top of an existing file.

## `plan_mux_outputs`

Decide the deliverables a captured disc produces: the list of titles
to mux out of the ISO and the staging filename of each. This is the
single "what do we extract?" decision — a movie yields exactly one
output (the main title, keeping the movie path byte-identical), a TV
disc under `tv_auto` yields one per selected episode title, numbered
`S{NN}E{MM}` from TMDB (degrading to plain sequential numbering when
TMDB has no data).

`movie_filename` is the single-title staging leaf the movie path
already derives; it is reused verbatim for the movie/one-title case so
nothing about that path changes. `title_confident` gates the TMDB
episode lookup: an unconfident title has no trustworthy `tmdb_id`, so
we number sequentially.

## `output_is_iso_image`

Whether the rip's deliverable is the whole-disc ISO itself rather than
a muxed MKV/M2TS title.

The settings UI advertises `output_format == "iso"` as "ISO copies the
whole disc; the other formats mux selected titles". For that promise
to hold the orchestrator must hand the operator the disc image it
swept (the intermediate `<name>.iso`), NOT re-mux the selected title
to an `.mkv` and prune the ISO. So in ISO mode we skip the title mux
entirely and deliver the ISO: the abort gate already scopes loss
whole-disc (`abort_lost_ms`), and the mover already validates + moves
`.iso` files. This is the single predicate every deliverable/prune/
mux-skip decision keys off so the two completion routes (`rip_disc`'s
inline terminal and `resume::resume_remux`) can't diverge.

## `effective_abort_secs`

Effective main-movie-loss tolerance for the abort gate.

ISO output is a whole-disc image and must be byte-complete, so the
`abort_on_lost_secs` per-title tolerance is **ignored** — forced to 0
("require 100%"). This makes the behaviour match the UI, where the
field is hidden for ISO (`hideIf output_format=iso`) and documented as
"IGNORED for an ISO rip, which is kept whole as-is". Without this an
`abort_on_lost_secs` value configured for a previous MKV rip would
silently leak into an ISO rip and accept a lossy image. MUXED output
(MKV / M2TS / Network) uses the configured value unchanged.

## `iso_output_needs_multipass`

Whether an `output_format == "iso"` rip must be rejected because it
was requested in single-pass mode (`max_retries == 0`).

ISO output is whole-disc: the deliverable is the entire image and its
abort accounting counts every unreadable sector, including damage
OUTSIDE any title's extents (see `abort_lost_ms` and the multi-pass /
resume pre-mux gates). Single-pass streams only the selected title to
the muxer — it never reads out-of-title sectors and captures no
whole-disc ISO, so there is no whole-disc image to deliver and its
rip-phase loss accounting is in-title only. Allowing single-pass ISO
would therefore hand over an incomplete image and silently miss
out-of-title damage that the multi-pass / resume ISO paths capture and
gate on (whole-disc scope) under `abort_on_lost_secs=0`. Only
multi-pass captures a real ISO and applies whole-disc scope, so ISO
output requires it.

## `SweepReadAction` / `sweep_transport_retry`

Pass-1 transport-failure gating decision (mirrors the Pass-1 sweep
error arm). This is a decision-MIRROR, not a wired gate: the actual
transport recovery is control flow interleaved with drive drop/reopen
and operator logging in `rip_disc`'s Pass-1 loop (a hardware
touch-point that stays inline), so this classifier is `#[cfg(test)]`
— it exists to characterize the gating for the future engine
inversion, not to run in production.

Pass 1 — and ONLY Pass 1 — retries after a USB-bridge transport crash
by dropping and re-opening the drive across a USB re-enumeration; the
patch passes have no such retry (any patch error breaks the retry
loop). This pure classifier pins that gating so the future inversion
knows Pass 1 owns the transport-recovery retry and the patch passes do
not.

Precedence matches the loop: a user halt cancels regardless of cause;
a non-transport error fails the rip; a transport error retries until
`MAX_PASS1_ATTEMPTS` is exhausted.

## `prune_intermediate_iso`

Prune the disc-sized intermediate ISO and its mapfile sidecar on a
successful multipass completion, unless `keep_iso` is set.

Shared by both completion routes — `rip_disc`'s inline terminal path
and the resume / `.ripped` hand-off path (`resume::resume_remux`) —
so the `keep_iso=false` disk reclaim can't diverge between them. The
mover frees the ISO when it tears down a `.done` staging dir, but a
low-confidence `.review` hold (mover skips it) or a no-output-dir
setup never relocates, so without this prune a 90+ GB UHD ISO would
leak in those cases.

Gated on `max_retries > 0`: an intermediate ISO only exists in
multipass mode (direct mode rips disc → MKV with no ISO). A `NotFound`
removal is silent (already gone / never written); any other error is
surfaced to the device log without failing the rip.

## `header_phase_outcome_is_failure`

Whether a `run_mux` outcome that never opened its output
(`output_opened == false`) is a terminal failure that must be
quarantined (`.failed` marker + `status="failed"`) rather than a
clean, resumable stop.

`output_opened == false` covers two header-phase exits:
* clean stop during header read (halt / cancelled): `finalize_error`
  is `None` — leave the staging dir resumable, surface nothing.
* structurally-unusable stream: `finalize_error` is `Some` — the
  header buffer overflowed before codec_privates resolved, or EOF / a
  read error hit before `headers_ready()` (header-resolution-
  incomplete). No output exists and the dir can never succeed, so it
  must be quarantined or resume-on-startup would re-resume it forever
  and the device tile would stay stuck in its prior `status="ripping"`.

## `header_phase_disposition`

Route a mux outcome by its header phase. Folded into one predicate so
`output_opened` is consulted EXACTLY once: the caller used to test
`!mux_outcome.output_opened` itself and then re-test it inside
`header_phase_outcome_is_failure`, and dropping the `!` on the outer
test alone sent a SUCCESSFUL mux down the no-output path (nothing
filed, the MKV stranded in staging) while letting a header-phase
failure fall through to the completion path, which writes `.done` for
a file that was never opened — rule 1 in both directions.

## `skip_read_errors`

Does the operator's `on_read_error` setting mean "skip the bad sectors
and keep going" (zero-filling the gap), or "stop the rip"?

`"skip"` is the only value that enables concealment. Anything else —
notably the `"stop"` an operator picks when they will not accept a
silently zero-filled movie — must leave `skip_errors` false, so a read
error truncates the mux and surfaces on `/api/state` instead of being
buried in a file reported as complete.

## `incomplete_mux_status`

Decide the log prefix, `/api/state` status, and
`last_error`/`failure_reason` for a mux that finished with
`completed == false`. Three cases, in priority order:
1. `finalize_error` → the MKV is structurally broken (Cues/trailer
   never landed). `status="failed"`; the caller quarantines with
   `.failed`.
2. `read_error` → a hard producer read error truncated the MKV under
   `on_read_error=stop`. `status="error"` with the cause, so
   `/api/state` signals the failure. This is NOT a user halt: the
   caller leaves staging resumable (no `.failed`), but the operator
   must still see why it stopped.
3. neither → a genuine user-initiated halt / wedge. The pre-existing
   "stopped → idle" path, with no `last_error`.

`read_error` is only consulted when `finalize_error` is `None`: a
structural finalize failure is the stronger signal (a broken file on
disk) and already implies the body was truncated.

## `aacs_failure_message`

User-facing message for the "encrypted disc, no keys resolved"
failure. Switches on the libfreemkv error surfaced via
`Disc::aacs_error` so the UI tells the user *which* failure they're
looking at instead of always printing the same generic "check KEYDB"
line.

Render format (locked rc.6 messaging standard):
`Error: E<code> <message>` — the `Error:` level word, the
language-neutral `E<code>` token, then a single plain-English sentence
naming the failure and any remediation. One line; built via
`error_line`.

Dispatch is **code-based** (`e.code()`) using named libfreemkv
constants. Codes outside the named set fall through to the 7xxx
catch-all rather than breaking the build when libfreemkv adds a new
variant.

## `keyless_failure_message`

Operator-facing message for the "encrypted disc, no usable keys"
failure, dispatched from the *whole disc* rather than just its AACS
slot.

CSS (DVD) and AACS (Blu-ray/UHD) record their resolution failures in
separate fields: a CSS known-plaintext crack failure lands in
`disc.css_error` (`Error::CssKeyMissing`), while AACS lands in
`disc.aacs_error`. The two are mutually exclusive in practice — a disc
is either CSS- or AACS-encrypted — so prefer `css_error` when present
and otherwise fall back to the AACS dispatch. Without this, a CSS
crack failure (where `aacs_error` is `None`) would hit the
AACS-oriented defensive fallback and mislead the operator into
checking their KEYDB.

## `deferred_keyless_message`

Keyless-deferral message for the resume / deferred-mux path (a raw ISO
was staged but the mux found no usable keys). Mirrors the fresh-rip
outage classifier (`retry_online_keys_on_outage`): when the configured
source is the online key service and that service is unreachable or
rate-limited, report THAT — a transient outage that will retry —
instead of a permanent "no keys were found; check the key source in
Settings". A keyserver that is merely DOWN otherwise sends the
operator hunting through Settings for a key source that is actually
fine (the exact confusion seen when the online key service returned
502s during a deferred mux).

## `disk_space_preflight_message`

Operator-facing message for the multipass disk-space preflight
failure.

This is NOT a libfreemkv `Error` — there is no `Error::IoError`/E5000
raised by the preflight; it's a local autorip guard. Its text lands
directly in `/api/state`'s `last_error` and is rendered as-is in the
web UI red banner, so it must read like the other clean operator
strings in this module. It deliberately carries NO raw `EXXXX:` code
prefix: a hand-assembled "E5000:" would be an unlocalised literal that
the freemkv CLI would route to the `error.E5000` key while the
dashboard showed the raw code as diagnostic noise, and would diverge
silently if the real `E_IO_ERROR` display convention ever changed.

`required` and `avail` are byte counts; `staging` is the staging path.

## `format_pass_error`

Translate a libfreemkv read-error into a user-facing message for
/api/state's last_error field. Raw libfreemkv errors like
`E6000: 19965280 0x02/0x04/0x3e` are diagnostic-grade — fine for logs,
terrible for the UI. This helper renders the same condition as e.g.:
"Pass 1 failed at 40.7 GB (sector 19,965,280) — drive firmware
unresponsive (HARDWARE_ERROR). Power-cycle the drive and retry the
rip."

## `format_lib_error`

Render a libfreemkv setup/scan/mux error into a plain-English,
operator-facing line for `last_error` / `failure_reason` (the UI red
banner) and the device log.

The library's `Display` is deliberately code-only (`E1002: /dev/sg0`,
`E6009`) — language-neutral, but useless to a human and a direct
rubric violation if surfaced verbatim. This helper maps the variants
autorip reaches off the drive-open / identify / scan / open-ISO /
mux-build paths into "what failed, why if known, what to do next"
without leaking a raw `E####` code or an internal path.

`phase` is a short human label for where it failed ("Cannot open
drive", "Disc scan", "Open ISO", "Mux setup") that leads the sentence.

## `open_drive_with_backoff`

Open a drive during transport-failure recovery, retrying with
exponential backoff because firmware may not be ready for several
seconds after a USB-bridge crash re-enumerates the device (whether on
a new sg path or the original one). Shared by both recovery arms so
same-path recovery gets the same 3-attempt backoff as new-path.

Returns `Some(drive)` on success, or `None` once recovery is exhausted
(the caller should `break 'pass1` — all per-attempt and
STRATEGY_FAILURE logging is emitted here).

TODO(step1-followup): this backoff re-open plus the Pass-1 retry
blocks aren't folded into `DiscSession::recover` yet — entangled with
autorip's UI/control-flow. Left in autorip per contract Q3 default.

## `log_init_recovery_failure`

Emit the post-`Drive::init` failure diagnostic for a transport-recovery
re-open. Shared by both the new-path and same-path recovery arms so
they log consistently: an ILLEGAL REQUEST (ASC=0x20) after init means
the drive firmware is wedged and needs a physical power-cycle, so we
surface the USER_ACTION_REQUIRED line; anything else is a plain
STRATEGY_FAILURE.

## `an_incomplete_damage_record_aborts_regardless_of_tolerance` test

An incomplete damage record must abort, not deliver.

The end-of-recovery promotion (NonTrimmed/NonScraped -> Unreadable) is
what MAKES residual loss visible: the abort gate reads Unreadable
ranges only. A failed `record()` leaves those bytes invisible to the
gate, and a failed `flush()` leaves them invisible to the mux and
resume paths, which re-read the mapfile from disk. Both were logged
and stepped over, so a damaged rip shipped as clean — the code's own
comment said "a state left unpromoted is loss the gate cannot see" and
then did exactly that.

An earlier version of this test asserted `loss_aborts(.., NaN, ..)`
and PASSED with the fix reverted, because the bug was never in the
gate — it was that the caller handed the gate a number it should not
have trusted. So this calls the caller's own decision function.

## `fmts_gate_plan_routes_side_effects` test

The FMTS gate's side-effect routing (defects 1 + 2). `rip_disc` drives
the deferred-mux flag and the Skip-path staging quarantine from this
plan, so pinning it here mutation-verifies both fixes without running
a whole rip: CaptureOnly must set `defer_forensic_mux` (mux
skipped-not-run, multi-pass preserves the ISO) — NOT quarantine; Skip
must `quarantine` (write `.failed` + clear the restart count); Proceed
does neither.

## `is_fmts_key_missing_error_matches_only_the_leading_code_token` test

The FMTS-forensic-key-missing error classifier (defect 1, resume
half). A `build_iso_pipeline` failure with this code is a retryable
DEFERRAL (preserve the ISO, wait for keys), so `resume_remux` must
detect it exactly — and, like `is_halt_error`, match only the leading
`E<code>` token so a data payload that merely contains the digits
can't false-match.

## `sweeping_guard_clears_marker_on_drop` test

Convergence H1 regression: `SweepingGuard` is the RAII cleanup for the
`.sweeping` in-progress marker. Many `rip_disc` early-return error
branches exit without reaching a terminal-marker writer; before this
guard each of those leaked a stale `.sweeping`, which made the next
startup's `resume_or_quarantine_staging` classify the dir `InProgress`
forever (never restart-counted, never cold-resumed). The guard's
`Drop` must clear the marker on every exit path so a dir holding a
complete ISO + clean mapfile can still be picked up on restart.

## `sweeping_guard_is_idempotent_after_terminal_marker` test

Convergence H1: on the success / `.ripped` / `.failed` paths a
terminal writer already clears `.sweeping` before the guard drops, so
the guard's clear must be an idempotent no-op (not error, not
resurrect state) — and must not disturb a terminal marker that
superseded `.sweeping`.

## `prune_removes_iso_and_mapfile_when_keep_iso_false` test

Regression guard for the divergent disk-reclamation bug: the inline
(`rip_disc`) and resume (`resume::resume_remux`) completion paths now
share `prune_intermediate_iso`, so a `keep_iso=false` multipass
completion frees the disc-sized ISO + mapfile on BOTH routes. Before
the fix, a `.review` (low-confidence) or no-mover resume leaked a
90+ GB ISO that the inline path would have freed.

## `staging_match_is_exact_not_prefix` test

Resume / completion matching is EXACT, never prefix. A disc named
"Redshift" (sanitized "Redshift") must not match a sibling staging dir
"Redshift_2" (from "Redshift 2") — a prefix match there would resume
onto a different title's partial ISO/mapfile. This locks in the
already-fixed HIGH bug; a regression to `starts_with` would fail here.

## `header_phase_finalize_error_is_terminal_failure` test

Regression: a `run_mux` header-phase exit with `output_opened=false`
AND `finalize_error=Some` (header buffer overflow before
codec_privates resolved, or EOF / read error before `headers_ready()`)
must be classified as a terminal failure so the orchestrator
quarantines the staging dir (`.failed`) and flips the device tile to
`status="failed"`. A clean stop during headers (`finalize_error=None`)
must NOT be classified as a failure — it stays resumable. Before the
fix the `finalize_error=Some` case took the bare early-return: reason
dropped, no marker, tile stuck in `status="ripping"`.

## `read_error_surfaces_as_error_status_not_silent_idle` test

Regression: single-pass `on_read_error=stop` with a hard read error.
`run_mux` returns `completed=false` with `read_error=Some` and no
`finalize_error`. The orchestrator's incomplete-mux branch must map
that to `status="error"` with a non-empty cause — NOT the silent
"stopped → idle" path a genuine user halt takes — so `/api/state`
signals the read failure rather than looking like an idle, user-stopped
rip with no `last_error`.

## `staging_free_bytes_none_for_missing_path_some_for_real` test

The disk-space pre-flight in `rip_disc` branches on
`staging_free_bytes`: `Some(avail)` runs the 2×-capacity gate, `None`
must take the diagnostic-log branch (NOT silently skip). This locks in
the contract that branch relies on — a missing / unmounted staging
path yields `None` so the operator gets a "preflight skipped" warning
instead of a silent slide into a mid-rip ENOSPC. A real, existing path
must yield `Some`.

## `halt_guard_unregisters_on_drop` test

The `HaltGuard` created at the top of `rip_disc` must unregister the
device's halt-map entry on EVERY exit path — including the
early-return error branches that leaked it in the v0.13.6 class of
bug. Dropping the guard (what happens on any return/panic) must remove
the entry so a subsequent rip starts with a fresh, uncancelled token
rather than inheriting the prior attempt's state.

## Multipass recovery-loop characterization tests

CHARACTERIZATION TESTS — golden baseline for the multipass recovery
loop's decisions, pinning `rip_disc`'s CURRENT behavior so a later
inversion into freemkv-engine can be proven behavior-preserving.

### `char_pass_ordering_sweep_then_n_patch`

PASS ORDERING. `max_retries = N` (N > 0) plans exactly one Pass-1
sweep then N patch passes, with `total_passes = N + 2` (sweep + N
patch + mux). `max_retries = 0` is single-pass: no sweep, no patch, no
ISO, `total_passes = 0`.

### `char_convergence_mkv_scopes_to_title`

SCOPE-AWARE CONVERGENCE — MKV output. Only bad bytes INSIDE the muxed
title's extents count; out-of-title damage (menus/trailers) does not
earn retry passes, so the loop converges when in-title bad == 0 even
with damage elsewhere on the disc.

### `char_convergence_iso_scopes_whole_disc`

SCOPE-AWARE CONVERGENCE — ISO output. The deliverable is the
whole-disc image, so EVERY bad byte counts (including out-of-title).
The loop only converges when the whole disc is clean.

### `char_no_progress_stops_retries`

NO-PROGRESS EXHAUSTION. A patch pass that recovers zero bytes stops
the retry loop (no future pass with the same drive state will help); a
pass that recovers anything keeps going.

### `char_patch_pass_decision_matrix`

The unified convergence decision, composed from the two loop gates.
`mux_scope_bad==0` ⇒ Converged (whatever the recovery). Otherwise
`recovered==Some(0)` ⇒ NoProgress; `None` (loop-top, no pass yet) or
`Some(n>0)` ⇒ Continue. This is the golden strategy the future engine
inversion must reproduce.

### `char_promotion_nontrimmed_to_unreadable`

PROMOTION DECISION. The end-of-recovery promotion is NonTrimmed →
Unreadable, and it runs BEFORE the loss/abort gate (so bytes that
stayed "maybe" across every pass are finalized as lost and counted by
the abort check). The promoted `Unreadable` status is one the scope
check still treats as bad; the source `NonTrimmed` is too (a range
only leaves the bad set by becoming `Finished`).

### `char_promotion_finalizes_loss_for_abort_gate`

PROMOTION — end-to-end via the pinned decisions. Drives a real mapfile
through the promotion using `end_of_recovery_promotion()` +
`bad_sector_statuses()`: a NonTrimmed range becomes Unreadable, and
the scope-aware bad-byte count then finalizes it as lost (feeding the
abort gate). No drive required — the promotion is a pure mapfile
operation.

### `char_pass1_transport_retry_gating`

PASS-1-ONLY TRANSPORT-RETRY GATING. The USB-bridge transport-crash
retry (drop + reopen the drive across a USB re-enumeration, resume
from mapfile) wraps the Pass-1 sweep ONLY. This pins its precedence: a
user halt cancels regardless of cause; a non-transport error fails; a
transport error retries until MAX_PASS1_ATTEMPTS is exhausted. The
patch passes have no such retry (documented below).

### `char_patch_passes_have_no_transport_retry`

PASS-1-ONLY (negative side): the patch passes carry no transport-retry
concept at all — any patch error breaks the retry loop and proceeds to
promotion/abort on what was recovered. There is no patch analogue of
`sweep_transport_retry`, which is exactly the gating asymmetry this
characterization records for the future inversion: Pass 1 owns the
USB-re-enumeration recovery; the patch passes do not. (The patch
loop's error → break is control flow in `rip_disc`; its bound is
pinned by `plan_passes(..).patch_passes` in
`char_pass_ordering_sweep_then_n_patch`.)

## `disk_space_preflight_message_has_no_raw_error_code_prefix` test

The disk-space preflight message is operator-facing (it lands in
last_error and the web UI red banner as-is). It must NOT carry a raw
"EXXXX:" code prefix — there is no libfreemkv Error raised here, so a
hand-assembled "E5000:" would be unlocalised diagnostic noise to the
operator. Guards against re-introducing the prefix.

## `key_service_transient_status_mapping` test

The reachability verdict → operator status-line mapping (the state
side of the down-vs-no-key fix). A transient verdict (Down /
RateLimited) maps to a distinct, retryable message; a reachable
service (Up) maps to `None` so the caller keeps the existing "no keys
found" behaviour. Paired with `keysource::classify_reachability`
(502/timeout→Down, 429→RateLimited, 404/422→Up), this closes the full
outcome→state chain.

## `final_done_card_uses_in_title_loss_not_whole_disc` test

The `status=done` state update must report in-title-scoped loss (what
abort_lost_ms returns), NOT whole-disc `bytes_unreadable /
title_bytes_per_sec`. Out-of-title damage (scratched menus / trailers)
would inflate the 'done' card even though the abort gate correctly
accepted the rip.

This test verifies the contract indirectly via `abort_lost_ms`: given
out-of-title-only damage, the in-title loss is 0 ms — so the done card
should show 0s lost, not the whole-disc value.

## `single_pass_done_card_main_lost_ms_tracks_final_lost_secs` test

Regression: single-pass (max_retries == 0) has no mapfile, so
`sweep_damage_snapshot` is the all-zero Default. The done-state
`main_lost_ms` must be derived from `final_lost_secs` (the demux-skip
estimate), mirroring the `total_lost_ms` branch — NOT taken from the
zero snapshot, which would always render "(0s in main movie)" even
when in-title sectors were skipped. This replicates the selection
logic at rip_disc's done-state update.

## `multipass_done_card_main_lost_ms_uses_snapshot_plus_demux` test

Multipass (max_retries > 0) keeps the snapshot's mapfile-derived sweep
loss but additionally folds in the demux-time loss — matching the
single-pass path (which surfaces demux loss via `final_lost_secs`) and
the resume path (resume.rs, which adds `demux_lost_secs * 1000`). The
whole-disc `final_lost_secs` value must NOT replace the snapshot.

## `accepted_done_card_folds_demux_loss_into_headline` test

Regression for the cross-path-asymmetry bug: an ACCEPTED fresh
multipass done card must fold demux-time loss into the headline
`errors` / `lost_video_secs`, matching the resume path
(`done_errors`/`done_lost_video_secs` in resume.rs) and the
single-pass path. Before the fix, multipass reported sweep-mapfile
figures ALONE, so a fresh multipass rip looked cleaner than a resume
of the identical ISO. Single-pass must remain unchanged (its `final_*`
already equals the demux figures), with no double-counting.

## `single_pass_lost_secs` (test helper)

Pins the loss-from-skip-count → threshold-decision math used by the
loss gate, exercising `should_abort_for_loss` over a skip-count
derived loss. (The function names below retain their "single-pass"
wording; note the mux itself never aborts on loss — these tests pin
the pure threshold helper, not a post-mux gate.) Derivation:
`lost_secs = skip_sectors * 2048 / title_bytes_per_sec`,
`abort = should_abort_for_loss(lost_secs * 1000, threshold_ms)`.

## `handoff_status_is_done_read_complete` test

Regression (bug #1 / bug #2): the `.ripped` marker hand-off must write
status="done" — NOT "ripping" (and not "idle"). The DISC READ is
complete the instant sweep + patch finish; the mux is a SEPARATE phase
that runs off the staged ISO and is tracked by the synthetic `_mux`
device + the System-tab Mux queue. So the real drive tile must show a
completed (read-done) card immediately, and auto_eject fires here (the
disc is no longer needed). Pre-fix this wrote "ripping", leaving the
tile stuck on "Ripping" for the entire mux and making auto-eject LOOK
like it waited for the mux.

We can't drive `rip_disc` in a unit test (it needs a live drive), but
we pin the invariant that the hand-off status is "done" so a future
refactor back to "ripping"/"idle" is caught. The companion
`mux_worker_does_not_revert_done_origin_device` test (muxer.rs) covers
the other half: the `_mux` worker can't push a real "done" tile back
to "ripping".

## `auto_eject_is_once_at_handoff_not_at_mux` test

The eject is "exactly once at read-complete": the fresh-rip `.ripped`
hand-off is the ONLY place the physical drive ejects on the multipass
path, and the LATER mux worker (synthetic `_mux`) is refused. This
pins both halves against the predicate so a refactor that lets the mux
worker re-eject (or that ejects twice) is caught.

## `config_lock_poisoned_marks_error_not_stuck_scanning` test

Regression: a poisoned config `RwLock` must NOT leave the tile wedged
in "scanning". `try_claim_active` sets status="scanning" before
scan_disc / rip_disc run, and both bail out early on `cfg.read()`
failure. Pre-fix that early return was bare (`Err(_) => return`), so
the device stayed "scanning" forever with an empty last_error. The
shared `mark_config_lock_poisoned` helper that both sites now call
must flip the state to "error" with a populated last_error.

## `promotion_uses_in_memory_map_and_flush_persists_to_disk` test

Regression: end-of-recovery promotion must flush the promoted mapfile
so the abort check (which now uses the in-memory map) sees Unreadable
bytes — not the pre-promotion NonTrimmed state.

Before the fix the two-block design:
1. Promoted NonTrimmed → Unreadable in memory, dropped `map` without
   flushing (pre-promotion state stays on disk).
2. Re-loaded from disk → got the pre-promotion file → zero Unreadable
   bytes → abort check silently skipped.

After the fix both steps share one `map` load; the abort check queries
the already-promoted in-memory map, and the flush persists it to disk.

## `handoff_update_state_carries_damage_fields` test

Regression: the `.ripped` marker hand-off update_state must preserve
non-zero damage fields (errors, total_lost_ms, main_lost_ms,
bad_ranges, largest_gap_ms) from the sweep phase so /api/state doesn't
silently zero them during the hand-off window.

Pre-fix the hand-off RipState used `..Default::default()` which zeroed
those fields. The fix reads them from STATE (populated by the last
push_pass_state call) and carries them into the new RipState.

We simulate this by: (1) seeding STATE with damage-populated data, (2)
reading it back exactly as the hand-off code does, and (3) asserting
the result is non-zero.

## `resumable_for_disc_detects_partial_sweep` test

Regression: `resumable_for_disc` (the scan-complete tile's
Resume-button detector) must find an existing resumable staging dir.
It previously walked the staging root with `read_dir(...).flatten()`,
which silently drops per-`DirEntry` I/O errors — on a cold NFS cache a
transient ESTALE/EIO on a single entry made the disc's own dir vanish
and the function return None, hiding the Resume button. It now routes
through `list_staging_basenames` (3-retry NFS defense) like the other
staging walkers. This test pins the happy path so the wiring can't
silently revert to a bare `read_dir().flatten()`.

## `resumable_for_disc_blocked_by_failed_or_review` test

R3 finding 1 regression: `resumable_for_disc` must return None (no
Resume affordance) when the disc's staging dir carries a terminal
`.failed` or a held `.review` marker, even though its mapfile still
shows pending bytes (Resumable::Sweep-worthy). Offering Resume on a
`.failed` dir was the data-stranding bug: the Sweep-resume branch
re-rips WITHOUT clearing the stale `.failed`, so the successful
re-rip's `.ripped` is shadowed by the lingering `.failed`
(terminal-by-presence) and the mux worker skips it forever. This
mirrors the Remux-branch `resumable_dir_blocked` policy: a
terminal/held dir forces the operator to explicitly Wipe.

## `resumable_for_disc_blocked_when_owned_by_mux_worker` test

Owner decision #2 regression: `resumable_for_disc` must return None
when the disc's staging dir is owned by the mux worker (`.ripped`
sweep-done handoff or in-flight `.muxing`). Resuming such a dir would
re-enter the sweep path on a disc the worker is mid-mux on — racing
fresh sweep writes against the mux's reads and overwriting the staged
ISO. Mirrors the sibling Wipe guard (`staging_disc_owned_by_worker`):
a worker-owned dir is off-limits until the mux finishes, even with a
Sweep-worthy mapfile.

## `staging_disc_completed_excludes_held_for_review` test

M4: a rip HELD for review writes BOTH `.review` and `.completed`. The
"already ripped" check must NOT treat it as completed — otherwise the
unattended insert path skips a disc that's actually awaiting operator
confirmation. Gating is `.completed` AND not `.review`.

## `staging_disc_completed_uses_snapshot_with_leftover_artifacts` test

R2 finding 2 regression: `staging_disc_completed` must read its
markers through the NFS-resilient `snapshot_staging_disc` (3-retry
read_dir), not bare `path.join(MARKER).exists()`. We can't provoke a
real NFS cold-cache false-negative in a unit test, but we can pin that
the detection now keys off the same snapshot view every other caller
uses and still works with leftover artifacts present (the
crash/cold-cache window where the ISO+mapfile are still on disk
alongside `.completed`). Under the old `.exists()` path this same dir
would be "completed"; the snapshot path must agree so the Default
auto-insert guard can't false-negative and re-rip a finished disc,
truncating the staged ISO.

## `list_held_still_sees_completed_review_dir` test

M4 sanity: the review UI's `list_held` still surfaces a held dir even
when `.completed` is also present (it keys on `.review` and absence of
`.done`, independent of `.completed`).

## `staging_disc_owned_by_worker_detects_ripped_and_muxing` test

H1: a `.ripped` or `.muxing` staging dir is OWNED by the mux worker.
The drive auto-insert path must recognise it so it doesn't run a fresh
sweep that truncates the ISO the worker is reading.

## `unquantifiable_loss_aborts_under_any_threshold` test

A tolerance-configured rip must NOT accept loss it could not measure.

Regression: the two inputs to the gate were both computed inside a
`title_bytes_per_sec > 0.0` guard, so on a disc with no measurable
bitrate the byte count stayed 0 AND the ms stayed 0.0 — confirmed
unreadable sectors then read as a clean rip at threshold 0, and as
"0ms lost" under any seconds tolerance. The byte count is now computed
unconditionally (it needs no bitrate), and an unmeasurable time reads
as NaN, which fails safe to abort in both branches. autorip and
freemkv-engine previously returned opposite verdicts for this case.

## `resumable_dir_blocked_skips_owned_held_and_terminal` test

H1 + M3: the drive-resume (Remux) selector must skip dirs that are
owned (`.ripped`/`.muxing`), held (`.review`), or terminal (`.failed`),
while still resuming a plain ISO+mapfile dir. Drives the pure
`resumable_dir_blocked` against real snapshots.

## `mux_loss_gate_fires_only_on_mux_contributed_loss_over_threshold` test

The mux-time loss gate is the sole enforcement point for decrypt/codec
loss — the pre-mux gate reads only the mapfile Unreadable set, where
those skips never appear. It lived as four inline conditions inside
`rip_disc`; a mutation run flipped every one of them and the whole
suite still passed, in both directions (concealed loss filed as
`.done`, and a clean rip quarantined). Table-drives every axis.

## `title_confidence_is_key_absent_or_overridden_or_exact_match` test

The three ways a rip earns `.done`, and the one way it doesn't. This
is a DISJUNCTION and each term matters on its own. Require them
together instead and an operator with a TMDB key configured never
auto-files anything (every rip parks in `.review`); relax it the other
way and a guessed title is filed straight into the library, where the
mover can land it on top of an existing file.

## `handoff_marker_is_done_only_for_a_confident_title` test

Confident → hand to the mover; not confident → hold in staging. Both
completion paths select their marker through this one mapping, so if
it inverts, EVERY finished rip either auto-files under a guess or
never leaves staging.

## `online_key_retry_fires_only_for_an_encrypted_keyless_online_disc` test

The outage retry re-reads the disc and REPLACES the `Disc` the rest of
the rip runs against, so it must fire on exactly one situation: an
encrypted disc the online key service left keyless,
capture-without-keys off. Every other combination must leave the
scanned disc untouched.

## `only_on_read_error_skip_enables_zero_fill` test

`on_read_error` is the operator's choice between "conceal it" and
"stop". Only the literal `"skip"` enables the zero-fill; an operator
who configured `stop` must never get read errors silently filled in
and the rip reported complete.

## `header_phase_routes_opened_failed_and_clean_stop_apart` test

One predicate routes the mux outcome, so `output_opened` is consulted
exactly once. Dropping the `!` on the old outer test sent a
SUCCESSFUL mux down the no-output path (nothing filed, the MKV
stranded in staging) and let a header-phase failure fall through to
the completion path, which writes `.done` for a file that was never
opened.

## `multipass_starts_at_one_retry` test

The boundary at 0/1. Every route decision along `rip_disc` keys off
this one predicate — the reader, the mux entry point, the mapfile loss
accounting, the end-of-recovery abort block — and they must all agree.
Off by one in either direction and a single-pass rip hunts for a
mapfile that was never written (whose fail-safe then aborts EVERY
single-pass rip), or a multipass rip skips its loss accounting and
reports damage as zero.

## `end_of_recovery_loss_counts_confirmed_in_title_damage` test

The measurement the abort gate decides on. Reporting zero for a disc
with confirmed unreadable in-title sectors is the shipped-broken-once
failure: the rip passes the gate as clean, gets muxed, gets `.done` +
`.completed`, and the mover files a damaged movie into the library.

## `end_of_recovery_loss_is_zero_for_a_clean_image` test

A clean image reports nothing — the symmetric direction. If this ever
returned non-zero, every flawless rip at `abort_on_lost_secs=0` would
be quarantined as `.aborted-loss` instead of delivered.

## `end_of_recovery_loss_scopes_to_the_deliverable` test

Out-of-title damage (a scratched menu or trailer) is real damage to
the disc but not to the MKV being muxed, so it must NOT abort an MKV
rip — while for ISO output, where the whole image IS the deliverable,
the same damage counts.

## `end_of_recovery_loss_distrusts_a_broken_promotion_only_when_damage_exists` test

A failed promotion means the damage record is incomplete, so no figure
derived from it can be trusted: the seconds figure must come back NaN
(which aborts under every threshold) rather than a comfortable number.
But with nothing unreadable at all there was nothing to promote, and a
clean rip stays clean — that ordering is deliberate and pinned here.

## `disc_two_of_a_boxset_is_not_skipped_as_already_ripped` test

THE boxset bug: insert disc 2 after disc 1 has finished and disc 2 is
silently never ripped.

`tmdb::clean_title` strips "disc 1".."disc 4" before the lookup, so
every disc of a set resolves to ONE title. The staging dir was named
from that title alone, so `disc_already_completed` found disc 1's
`.completed`, logged "already ripped — skipping", and the drive never
read disc 2. With `on_insert=rip` that is the product's core workflow
failing silently.

The other half matters just as much: re-inserting the SAME disc must
still land on its own dir. If it didn't, every container restart with
a disc in the drive would re-sweep a finished rip.

## `a_legacy_unlabelled_staging_dir_still_counts_as_the_inserted_disc` test

The upgrade path: a staging dir written before `.disc-label` existed
carries no label, so it must keep reading as "this disc" — an upgrade
must not re-rip finished staging or orphan a partial one. The first
disc to rip into it adopts it, and only THEN does a different disc
move aside.

## `rip_entry_halt_carries_a_stop_that_landed_in_the_dispatch_gap` test

A Stop that lands in the gap between `handle_rip_request`'s
`is_cancelled()` check and `rip_disc`'s own halt registration must be
honoured, not discarded.

The spawn site registers a `Halt` before the rip thread starts so an
HTTP Stop during the scan phase has a token to flip. `rip_disc` then
installed its own with a blind `register_halt`, so a `/api/stop` that
cancelled the spawn-site token microseconds too late was overwritten
and lost: the operator's Stop did nothing and the rip ran to
completion. `swap_halt_carrying_cancel` exists for exactly this race
and is already used at the drive-backed swap ~565 lines later.

## `post_mux_durability_abort_releases_the_drive` test

The fsync durability gate late in `rip_disc` bails without writing
`.done`/`.completed`, on purpose — the output is not provably durable,
so staging stays resumable. But it reported only through `last_error`
and returned with `status` still `"ripping"`.

`is_busy()` IS `status == "scanning" | "ripping"`, so the drive stays
busy for the container's lifetime: the poll loop skips it on every
tick, /api/rip 409s, and `/api/state` renders a healthy in-progress rip
that will never end. Every other exit from `rip_disc` leaves a terminal
status; these must too.

## `hot_unplug_teardown_keeps_the_double_rip_guard_for_a_busy_drive` test

A drive that is mid-rip must NOT have its STATE entry deleted just
because one enumeration pass missed it.

`is_busy` (the double-rip guard) is `STATE[device].status ==
"scanning" | "ripping"`. The hot-plug reconcile's removal branch used
to delete that entry unconditionally, so a device that dropped out of
a single `list_drives()` pass during a hours-long rip (a SCSI/USB
reset in wedge recovery does exactly this) made `is_busy` return
false, opening the rip/scan dispatch guards for a second concurrent
rip on the same drive.

## `the_completion_tail_logs_and_notifies_before_ejecting` test

The fresh-rip completion tail must log and notify BEFORE it ejects,
and must route the eject through the shared predicate.

`eject_drive` calls `archive_device_log` partway through, so anything
logged after it lands in the NEXT rip's log ring instead of the
archived per-rip log — the archived log for a completed rip was
missing its own "Rip complete" line, and the completion webhook fired
after the archive too. The eject is also the one completion terminal
that tested `cfg_read.auto_eject` inline instead of `should_auto_eject`,
which is the predicate whose whole stated purpose is to be the single
place that decision lives ("never from the mux worker"). Driving
`rip_disc` needs a real drive, so pin the ordering and the predicate
at source level.

## `hot_unplug_teardown_defers_while_the_rip_thread_is_still_unwinding` test

The teardown must be gated on the WORKER, not on the status the worker
already wrote.

A rip thread writes its terminal status ("done"/"idle") and then keeps
running its tail on the same thread: `eject_drive` (`Drive::open` +
`session.eject()` — real, slow hardware I/O), the eject-failure device
log lines, and its guard drops. `is_busy` is `status == "scanning" |
"ripping"`, so for that whole window it reads FALSE while the worker
is very much alive — and `forget_removed_device` used to take that as
permission to `drop_session`, evict the STATE row and forget the log
ring out from under it. `forget_device_session_state`'s doc states
this exact hazard and defends RIP_THREADS/HALTS against it with
`JoinHandle::is_finished()`; this pins the other half of the same
teardown to the same fact.

## `a_disc_seen_during_the_stop_cooldown_is_still_ripped_once_it_expires` test

A disc seen during the 5 s post-Stop cooldown must still be ripped
once the cooldown expires.

The trigger is gated on `is_new_insert`, which is false for anything
in `had_disc`. So if a tick that declines to dispatch still latches
the device, the disc is recorded as handled when nothing handled it,
and the poll loop — which has no other auto-rip trigger — will never
act on it again until a physical eject and reinsert. On an unattended
daemon that is a silent, permanent stall.

Mutation: return `latch: true` unconditionally from `insert_tick` (the
pre-fix rule) and the tick-2 assertion goes red.

## `forget_removed_device_recovers_a_poisoned_state_lock` test

Catches the mutation that puts `if let Ok(mut s) = STATE.lock()` back
into `forget_removed_device`.

On a poisoned STATE that spelling SILENTLY SKIPPED the removal — and
still returned `true`, so the teardown reported success and left the
row behind, with not one log line to say so. A poisoned STATE means a
worker panicked while holding it, which is exactly when a drive is
most likely to have vanished from enumeration. The stale row then
outlives the device for the container's lifetime: a phantom drive in
the dashboard, and `is_busy` / `try_claim_active` still answering from
it. Every other STATE / HALTS / RIP_THREADS site in this crate
recovers the guard with `unwrap_or_else(|e| e.into_inner())`;
divergence here is the defect.

Proven structurally: poisoning the process-global STATE inside the
test binary would break every other test that shares it.

## `list_staging_basenames`

Lists the immediate-child basenames of the staging root with the same
NFS cold-cache discipline as `staging::snapshot_staging_disc`.

`disc_already_completed` and `find_resumable_for_disc` both walk the
staging root to find the current disc's per-disc subdir. The naive
`read_dir(...).flatten()` silently drops per-`DirEntry` I/O errors,
which on a Watchtower restart with a cold NFS attribute cache can make
the matching subdir vanish from the listing for one scan — exactly the
degradation `snapshot_staging_disc` already defends against (observed
2026-05-15). A dropped entry would make `disc_already_completed`
return false (re-sweeping an already-done disc) or
`find_resumable_for_disc` return None (falling through to a fresh
sweep instead of resuming).

Defense: retry `read_dir` up to 3 times (500 ms apart) whenever a pass
fails to open OR yields any per-entry error, and return the UNION of
every basename seen across attempts. A clean pass (opened, zero entry
errors) is trusted immediately. Returns `None` only when no `read_dir`
attempt ever opened the directory — callers then behave exactly as the
old `.ok()? / return false` did (no listing → no match), rather than
acting on a half-listing that dropped the disc's own dir.

The union (rather than the single largest-count pass) matters because
different degraded passes can surface disjoint partial views of the
same mount: a disc's subdir present in an earlier, smaller pass but
absent from a later, larger one would otherwise be silently dropped,
defeating the whole point of the retry.
