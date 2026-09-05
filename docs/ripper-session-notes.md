# `src/ripper/session.rs` — extended comment notes

Long-form rationale relocated out of source comments by the comment-guard
pass. Each section below corresponds to a `// See docs/ripper-session-notes.md`
pointer left at the original comment site in `src/ripper/session.rs`.

## Module history: the 0.18 round-2 halt rework

The old `HALT_FLAGS` + `STOP_FLAGS` + `register_halt` / `request_stop` /
`stop_requested` / `reset_stop_flag` machinery is gone. Each rip-thread
spawn site now allocates a single `libfreemkv::Halt` token, registers it
in `HALTS` keyed by device, and threads `halt.clone()` through every
cancellable phase (sweep / patch / mux). The HTTP `/api/stop/{device}`
handler looks up the device's `Halt` and calls `.cancel()`; phase loops
poll `halt.is_cancelled()` at their tops.

## `RegisterError::PriorThreadRunning`

A prior rip thread for this device is still *running*. We refuse to
overwrite it (dropping a running handle breaks drain-before-wipe — the
v0.13.6 bug class). The new handle the caller passed in is returned
untouched so the caller can reap it.

## `register_rip_thread` reap-or-reject semantics

Production calls this (via `spawn_rip_thread`) from the poll-loop and
web spawn sites; the integration tests under `tests/halt_drain.rs` also
call it to plug a synthetic thread into the same machinery `handle_stop`
uses.

The device map holds at most one handle. For a pre-existing entry:

* **Prior handle finished** → it is removed and joined *inside the
  lock*. `JoinHandle::is_finished() == true` guarantees `join()` returns
  without blocking, so holding `RIP_THREADS` across the join cannot
  deadlock. This quietly reaps the common benign case (a completed scan
  thread still registered when the rip thread spawns) that previously
  logged a scary "prior thread not reaped" warning. The new handle then
  takes its place and we return `Ok(())`.
* **Prior handle still running** → we must NOT overwrite it; a dropped
  running handle can never be joined, so a later stop/eject/shutdown
  drain returns before the thread actually exits (staging could be
  wiped mid-write — the v0.13.6 bug class). We leave the running prior
  in place and return `Err(RegisterError::PriorThreadRunning(new))`,
  handing the new handle back so the caller reaps the worker it just
  spawned. The spawn sites gate on `crate::ripper::try_claim_active`,
  which now consults thread liveness as well as STATE status, so this
  branch should be unreachable — but it is the last line of defense and
  is defended rather than trusted. (It was previously justified by
  STATE status ALONE, which is not a liveness fact: a worker writes its
  terminal status and then keeps unwinding, so this branch was
  reachable from an unauthenticated LAN POST.)

## `spawn_rip_thread`: register-before-run gate

**Registration is decided BEFORE the worker does any work.** The
spawned thread parks on a one-shot channel and runs `f` only once this
function has confirmed it won the `RIP_THREADS` slot. The previous
order — spawn, *then* register — meant a duplicate spawn ran `f` to
completion (for the rip role, `handle_rip_request`: a full multi-hour
disc rip) before `register_rip_thread` rejected it and the `join()`
below reaped it. That turned a "refused" spawn into: an HTTP worker
thread blocked for the whole rip, a second worker writing the same
staging dir as the incumbent, and finally a `rollback_failed_spawn` +
500 for a rip that had actually finished. Parking first makes the
rejection cost a channel `recv` instead of an hour of disk work.

The gate cannot hang either side. The worker's only pre-`f` action is
the `recv`, and every exit from this function either sends (accepted)
or drops the sender (rejected, or a panic between spawn and register)
— a dropped sender wakes the `recv` with `Err` and the worker returns
without touching anything. So the `join()` in the rejection branch is
bounded by a thread that is already on its way out.

## `join_rip_thread`: why the handle is polled in place

This used to open with `take_rip_thread`, poll the handle it had
removed, and stash it back on timeout or self-join. That REMOVED THE
DEVICE'S ONLY LIVENESS FACT for the whole drain — up to 60 s for
`/api/stop` — while the worker was still running, and every gate that
asks `rip_thread_running` read `false` for that window:

* `POST /api/stop/sr0` lands on a worker in its terminal tail (it has
  written `status = "done"` and is still inside auto-eject). `is_busy`
  is already false. `handle_stop` takes the handle and starts polling.
* A concurrent `POST /api/rip/sr0` — unauthenticated, this server binds
  `0.0.0.0` — now sees NEITHER fact: `is_busy` false,
  `rip_thread_running` false. It wins `try_claim_active_checked`,
  `register_halt` clobbers the incumbent's `Halt` (so the incumbent's
  trailing `unregister_halt` deletes the NEW rip's token and
  `/api/stop` can no longer cancel it), `register_rip_thread` finds the
  slot EMPTY because we took the handle, and a full duplicate rip runs
  against the incumbent's staging dir.

Round 1's TOCTOU argument (see `try_claim_active_checked`) only covered
a handle APPEARING between the liveness read and the `STATE` lock. It
never covered one DISAPPEARING out from under a live worker, which is
what the drain itself was doing.

Polling in place also removes the re-stash entirely, and with it the
separate defect where the timeout path `insert`ed unconditionally and
could REPLACE a newer live handle with the stale one it was holding —
making the live rip unjoinable, so the next stop reported a clean drain
while that worker was still mid-write.

Implementation: every 25 ms, take `RIP_THREADS` briefly, look at the
entry for `device`, and drop the lock again. Nothing is ever held
across the sleep or across `join()`. `join()` runs only once
`is_finished()` is true, so it cannot block.

## `HALTS`: per-device cooperative-cancel tokens

Replaces the 0.17 `HALT_FLAGS` + `STOP_FLAGS` pair (two parallel
`Arc<AtomicBool>` registries that the old `request_stop` flipped in
lockstep). One token, one bit, one source of truth — every phase that
holds a clone observes Stop on its next poll.

## `rollback_failed_spawn`: why the generation, and not a liveness check

A spawn can fail for two very different reasons: the OS refused the
thread (nothing is registered for this device — the case this function
exists for), or `spawn_rip_thread` refused to displace a still-running
incumbent. In the second case the "failure" belongs to the duplicate,
and an unconditional rollback would vandalise the WINNER:
`unregister_halt` deletes the token `/api/stop` needs to cancel the
live rip, and the idle push overwrites a running rip's state.

The previous guard answered that with *liveness* — "is any worker
thread running for this device?" — and returned early if so. That is
the wrong question, and it introduced a wedge of its own: the early
return left the LOSER's claim standing. No thread, no Halt,
`status == "scanning"`, `is_busy()` true forever if the incumbent never
writes state again (which is precisely the case in the incumbent's
terminal tail, the window this whole race lives in). All four device
routes then answer 409 indefinitely and only `/api/stop` can recover
the device.

The generation answers the *right* question — "is the claim I made
still the claim in force?" — and it is exactly what `RipState::claim_gen`
was added for. It is correct under every interleaving with no liveness
read at all: the loser always clears its own claim, and can never clear
the winner's, because a winner's claim necessarily bumped the
generation past the loser's. `update_state` carries the generation
forward, so an ordinary progress write does not look like a re-claim.

## `DISC_IDENTITY`: last-known disc identity per device

The UDF Volume Identifier of the disc that was scanned into the
device's `DriveSession`. Kept in a separate table (not on
`DriveSession`) so it OUTLIVES the session: the transport-failure
recovery path drops the session before it calls `rediscover_drive`, and
the rediscovery needs the identity to reject a neighbouring drive that
merely happens to have an unrelated disc loaded (see `rediscover_drive`).
Populated automatically by `store_session` from
`session.disc.volume_id`.

## `cache_disc_identity`: clearing an empty label

An empty label is not a discriminator — storing it would match every
other label-less disc — but it must still CLEAR any previous disc's
entry. Skipping the write outright (what this did before) left the
PREVIOUS disc's volume id cached against a device that now holds a
different, unlabelled disc, and `rediscover_drive` would then "confirm"
a shifted candidate against a disc that is no longer in the drive: the
exact wrong-disc attachment the identity check exists to prevent.

## `rip_thread_running`: the fact `is_busy` cannot give

`is_busy` is `STATE[device].status == "scanning" | "ripping"`, and a
worker writes its TERMINAL status and then keeps running its tail on
the same thread — auto-eject (`Drive::open` + `session.eject()`, real
hardware I/O), the eject-failure device log lines, guard drops.
Teardown paths that would pull state out from under that tail must ask
this too, exactly as `forget_device_session_state` asks the handle
itself rather than trusting the status.

`false` for a device with no registered handle: nothing is running, so
nothing is deferred. Residual window (unchanged by this predicate, and
narrower than the one it closes): `join_rip_thread` takes the handle
OUT of the map for the duration of an off-thread drain, so while
another thread sits in that poll a live worker reads as not-running
here. The self-join drain — the auto-eject tail, which is the tail
that actually races the hot-unplug rescan — re-stashes the handle
immediately and so is covered.

## `forget_device_session_state`: what it evicts and why

Evicts the per-device state this module owns, on hot-unplug teardown:
the cached `DISC_IDENTITY` entry, and — only if the rip thread has
already exited — its `RIP_THREADS` handle and its `HALTS` token. Called
from `super::state::forget_device_state`, which is the single teardown
entry point; see its doc for the full per-device inventory.

A *running* handle is deliberately left in place. Dropping it makes the
thread unjoinable, so a later stop/eject/shutdown drain returns while
the worker is still mid-write and staging can be wiped underneath it —
the v0.13.6 bug class that `register_rip_thread` and `join_rip_thread`
are both built around. `forget_removed_device` only calls in here for a
device that is not `is_busy`, but "not busy" is a STATE *status*, not a
thread-liveness fact: the worker clears its status and then keeps
unwinding (eject, log flush, guard drops). So we ask the handle itself,
and leave a live one for the next `register_rip_thread` reap or the
shutdown `join_all_rip_threads` to collect.

The `Halt` is evicted on the same condition, and only then: while a
thread is still running its token must stay reachable so `/api/stop`
can cancel it. Once the thread is gone nothing polls the token, and a
leftover entry would shadow the next rip's fresh one.

## `session_is_scanned`: why this check exists

Used by `handle_rip_request` to skip a redundant `scan_disc` call when
the disc has just been scanned (e.g. ON_INSERT=scan ran on disc
insertion, then the user clicked Rip). Without this check the scan ran
twice — clearing the TMDB poster + title in the UI and burning 10-30 s
redoing identify + lookup + full title scan.

Returns false if the device has no session, or the session exists but
was created without `scanned=true` (currently impossible — every
`store_session` call site passes true — but keeps the check honest if
that invariant ever loosens). Recovers on a poisoned `SESSIONS` lock
(`unwrap_or_else` into_inner) rather than abandoning the check.

## `vid_for_log`: why sanitisation is needed here too

`log::sanitize_log_msg`'s own doc names the UDF volume-id as the string
it exists to defend against, but it only ran on `device_log`'s path —
these `tracing` fields reach the human-readable `autorip.log` and
stderr (so `docker logs` / `tail`) formatted with plain `Display` and
no escaping, so a crafted disc could inject ANSI into an operator's
terminal through the rediscovery path.

## Test: `a_volume_id_reaches_a_log_field_with_no_terminal_escapes`

A disc-supplied volume-id must not carry terminal escapes into a log.
`log::sanitize_log_msg`'s own doc names the UDF volume-id as the string
it exists to defend against, but it only ran on `device_log`'s path.
The rediscovery `tracing` fields reach `autorip.log` and stderr — so
`docker logs` and `tail` — formatted with plain `Display` and no
escaping, so a crafted disc could paint an operator's terminal.

The expectation is control-bytes-out, taken from what a terminal must
never receive, not from what the sanitizer happens to do.

## Test: `rollback_scoped_to_its_own_claim_spares_the_winner_and_clears_the_loser`

Catches the mutation that drops the generation check from
`rollback_failed_spawn` (making it roll back unconditionally), and the
mutation that restores the round-1 `rip_thread_running` early return
(which left the loser's own claim standing forever).

A spawn can fail for two unrelated reasons, and only one of them means
"the claim I made is still the claim in force". When `spawn_rip_thread`
refuses to displace a still-running incumbent, the loser's caller runs
this same rollback. It must do BOTH of these, and the round-1 guard did
only the first:

* not vandalise the WINNER — no unregistering the `Halt` that
  `/api/stop` needs to cancel the live rip, no idling a running rip's
  state row;
* still clear the LOSER's own claim, or the device sits at
  `status="scanning"` with no thread and no Halt, `is_busy()` true, and
  every route answering 409 until someone POSTs `/api/stop`.

## Test: `forgetting_a_removed_device_reaps_its_finished_thread_and_identity`

Regression: hot-unplug teardown must not leak this module's per-device
maps. `forget_device_state` used to evict only `TITLE_OVERRIDES` +
`STOP_COOLDOWNS` (and said so in a doc that claimed they were the only
other per-device state), leaving the device's finished `JoinHandle` in
`RIP_THREADS`, its cached volume id in `DISC_IDENTITY` and its token in
`HALTS` for the container's lifetime — one set per device path the
kernel ever handed out.

## Test: `forgetting_a_device_leaves_a_still_running_thread_registered`

The other half of the contract: a rip thread that is still RUNNING must
keep its registration. Dropping a live `JoinHandle` makes the thread
unjoinable, so a later stop/eject/shutdown drain returns while the
worker is still mid-write and staging is wiped underneath it — the
v0.13.6 bug class. Teardown is allowed to be slower, never unsafe.

## Test: `session_helpers_recover_from_poison`

Regression: `take_session`, `drop_session`, and `session_is_scanned`
must recover from a poisoned `SESSIONS` lock (unwrap_or_else
into_inner), not silently no-op as the old `.lock().ok()?` / `if let
Ok(..)` forms did. A silent no-op in `drop_session` would leak a stale
session; in `take_session` it discards a usable one; in
`session_is_scanned` it wedges the check at `false` forever, forcing a
redundant re-scan on every rip request for the rest of the process.
Poisoning the lock is permanent for the test binary (no reset), so
every SESSIONS consumer must tolerate it — this test is the one place
that's pinned. We poison the lock (panic while holding it, caught) and
assert none of the three helpers panics.

## Test: `join_rip_thread_called_on_its_own_thread_returns_at_once`

Catches the mutation that deletes `join_rip_thread`'s self-join branch
(or turns it into an ordinary poll), and the mutation that has it
UNREGISTER the handle on the way out.

`eject_drive` is called from the rip's own auto-eject tail, so
`join_rip_thread` regularly runs ON the thread it is being asked to
join. `is_finished()` can never become true from there, so an ordinary
poll burns the entire 60 s stop budget inside a rip that is doing
nothing wrong and then logs "did not drain". And the handle must stay
registered while we sit in that tail: it is the fact that keeps a
concurrent `/api/rip` from claiming a device whose worker is still
holding the drive.
</content>
