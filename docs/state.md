# ripper/state.rs — moved rationale

Design rationale and incident history moved out of doc comments in
`src/ripper/state.rs` to keep the pub-doc contracts short. Pointers in
the source refer back to the sections below.

## RipState::disc_label

`disc_name` is the TMDB-resolved title and is deliberately NOT unique per
disc: `tmdb::clean_title` strips "disc 1".."disc 4" before the lookup, so
every disc of a boxset resolves to one title and wants one staging
directory. `disc_label` is the field that still tells them apart, and it
is what
[`staging::staging_name_for_disc`](crate::ripper::staging::staging_name_for_disc)
uses to decide whether an existing staging dir belongs to THIS disc.

## RipState::failure_deferred

Set only by the deferral exits themselves. The alternative — inferring it
from `status == "idle"` — is wrong, and was: `"idle"` is also what several
HARD failures in `resume_remux` write (no title after key resolution, an
unreadable mapfile, an over-threshold loss abort), so a corrupt ISO was
presented to the operator as "no keys yet, it'll mux itself". Status says
how the device LOOKS; this says what happened.

## RipState::failure_finalize

Recorded explicitly for the same reason as `failure_deferred`: the
terminal `status` string cannot distinguish a finalize failure (terminal)
from a read error (resumable) reliably enough for the mux worker's
quarantine decision, and inferring terminality from `!failure_deferred`
false-quarantines a resumable read error. Set only by the mux-incomplete
finalize exit in `resume_remux`.

## RipState::main_at_risk_ms

Unlike `main_lost_ms` (terminal `Unreadable`-only — correct for the abort
verdict, but trivially 0 mid-rip), this counts pending in-feature data as
movie-at-risk, so the two-pill UI's `Maybe N · <time>` is honest during
the rip: `0:00` when the pending bytes are out-of-feature, a real ms
figure when they're in the movie. It melts toward `main_lost_ms` as retry
passes resolve pending sectors to `Finished` or `Unreadable`.

## STOP_COOLDOWNS

`Instant`, not an `epoch_secs()` deadline. This is a 5-second interval,
and a wall-clock deadline is only as good as the wall clock: a backward
step bigger than the window (NTP correction, host clock reset, VM
snapshot resume) keeps `now < expires` true until the clock catches up,
and `insert_tick` suppresses the auto-scan/auto-rip dispatch for that
whole time — an unattended box quietly ignoring the disc in the drive.
`Instant` cannot step backwards.

## forget_device_state

The complete per-device inventory, and who evicts each map — this list is
the contract, so add to it when a new per-device map appears:

| map | where | evicted by |
|---|---|---|
| `STATE` | `state.rs` | `forget_removed_device` directly |
| log ring | `log.rs` | `forget_removed_device` (`log::forget_device`) |
| `SESSIONS` | `session.rs` | `forget_removed_device` (`drop_session`) |
| `TITLE_OVERRIDES` | `state.rs` | here |
| `STOP_COOLDOWNS` | `state.rs` | here |
| `DISC_IDENTITY` | `session.rs` | here, via `forget_device_session_state` |
| `RIP_THREADS` | `session.rs` | here, via `forget_device_session_state` (finished handles only) |
| `HALTS` | `session.rs` | here, via `forget_device_session_state` (with the handle) |

The doc this replaces claimed `TITLE_OVERRIDES` and `STOP_COOLDOWNS` were
"the only other per-device state". They are not: `DISC_IDENTITY` had no
remover anywhere in the crate, and a torn-down device's finished
`JoinHandle` sat in `RIP_THREADS` forever — one entry per device path the
kernel ever handed out.

## try_claim_active_checked

Folding the busy-check and the status-set into ONE `STATE` lock closes a
TOCTOU: the web handlers previously did a separate `is_busy`-style check
and then a separate `update_state`, so two concurrent POSTs could both
observe `idle` and both launch a rip on the same device (orphaned halt
token + concurrent writes to one staging dir).

Why `known` matters: `device` on the web path is caller-supplied (the URL
segment of `/api/scan/{device}`, `/api/rip/{device}`,
`/api/eject/{device}`) and is only shape-checked by
`web.rs::is_valid_device_name` — a 3..=64-character ASCII-alphanumeric
check, explicitly documented there as NOT an existence check. This server
binds `0.0.0.0:8080` with no authentication, so any LAN host can loop
`POST /api/scan/<random-alnum-name>`. Before this guard, every such call
reached the old `try_claim_active`'s `.entry(...).or_insert_with(..)` and
created a brand-new, permanent `RipState` — and, once `handle_scan`'s
`spawn_rip_thread` follows a successful claim, a permanent `JoinHandle` in
`session::RIP_THREADS` too. Nothing ever prunes either: the hot-unplug
sweep in `drive_poll_loop` only removes devices that were previously in
its own enumerated `drive_paths` list, and a forged name never enters
that list. The two maps would grow without bound until the process is
OOM-killed.

A device that legitimately exists always has a STATE entry already —
`drive_poll_loop` pushes one (idle or with a disc) on every poll tick for
every device it enumerates, before the operator can ever see it in the
dashboard (the UI only offers Scan/Rip/Eject for devices `/api/state`
already reports) — so gating the *new-entry* path on `known` does not
affect any real device, only a name nothing has ever enumerated.

`known` should be `true` for the poll loop's own internal claim (its
`device` always comes from a just-enumerated `drive_paths` entry) and for
any caller that has cross-checked `device` against the live drive list.
Callers that receive `device` verbatim from an untrusted request and have
NOT done that cross-check must pass `false`.

**Why the claim needs TWO facts, not one.** The claim refuses a device
whose `STATE` status is scanning/ripping **and** a device whose rip
thread is still alive. Those are independent facts. A worker writes its
TERMINAL status (`done` / `error`) and then keeps running its tail on the
same thread: auto-eject (`Drive::open` + `eject()`, real hardware I/O),
the eject-failure device-log lines, guard drops. For that whole window
`is_busy` is false while the worker still owns the drive, the staging dir
and the device's `Halt` token.

Checking status alone was directly reachable from the network. This
server binds `0.0.0.0` with no authentication, so a `POST /api/rip/sr0`
landing in that tail won the claim, `spawn_rip_after_claim` overwrote the
incumbent's `Halt` with a fresh token (so the incumbent's trailing
`unregister_halt` then deleted the NEW rip's token and `/api/stop` could
not cancel it), and the duplicate worker ran until `register_rip_thread`
rejected it. The other half of that fix lives in
`super::session::spawn_rip_thread`, which now decides registration before
the worker runs any work; this half stops the duplicate from being
admitted in the first place.

**Ordering, and why it cannot deadlock.** The liveness question is asked
BEFORE `STATE` is locked, so `RIP_THREADS` and `STATE` are never held at
the same time and no lock-order inversion is possible; `rip_thread_running`
only reads `JoinHandle::is_finished`, never `join`, so it cannot block
either. Splitting the two checks is not a TOCTOU in the dangerous
direction: a handle can only APPEAR for a device after some caller wins
this very claim, so a `false -> true` transition between our read and our
`STATE` lock implies a competitor already took the `STATE` lock and our
own claim fails anyway. The benign direction (`true -> false`: the worker
exited in between) merely admits a claim for a device that is now
genuinely free, which is correct.

The accepted cost is the same one `forget_removed_device` already
accepts: a worker that hangs forever keeps its device unclaimable. A
finished handle reads as not-running, so an ordinary completed rip frees
the device the instant the thread exits, with no reaping required first.

## try_claim_active_refuses_a_device_whose_worker_is_still_unwinding (test)

Catches the mutation that deletes the thread-liveness half of the claim
(leaving only the STATE-status check it had before). The device is left
with a TERMINAL status — exactly the state a worker writes just before it
starts unwinding (auto-eject, log archive, guard drops) — while its
thread is still on the CPU. `is_busy` is false for that whole window, so
a status-only claim admitted an unauthenticated LAN `POST
/api/rip/{device}` into a device another worker still owns: the new claim
overwrote the incumbent's `Halt` token (so `/api/stop` could no longer
cancel either rip) and launched a second worker against the same staging
dir.

The second half of the test is just as load-bearing: once the worker
really has exited, the device must be claimable again immediately. A
"fix" that latched the device shut would break every normal
rip-then-rip-again sequence.

## a_drain_in_flight_never_makes_a_live_worker_claimable (test)

Catches the mutation that puts `take_rip_thread` back at the top of
`join_rip_thread` (i.e. drains by REMOVING the handle and stashing it
back afterwards) — the H1 duplicate-rip window. The drain is what `POST
/api/stop/{device}` does, for up to 60 s. While it ran, the handle was
OUT of `RIP_THREADS`, so the device's only liveness fact read `false`.
Land that on a worker in its terminal tail (status already "done", so
`is_busy` is false too) and a concurrent `POST /api/rip/{device}` —
unauthenticated, this server binds 0.0.0.0 — wins the claim, clobbers the
incumbent's `Halt`, finds an empty registration slot and runs a full
duplicate rip on the same drive and the same staging dir. So: a claim
must be refused for the WHOLE life of the worker thread, including while
another thread is draining it.

## the_stop_cooldown_is_not_measured_on_the_wall_clock (test)

The post-Stop cooldown is a SHORT INTERVAL, so it must be measured on the
monotonic clock, not the wall clock. It was stored as an absolute
`epoch_secs()` deadline. A backward wall clock step larger than the 5s
window — an NTP correction, a container host clock reset, a VM resuming
from a snapshot — leaves `now < expires` true for as long as the clock
takes to catch back up, and the device stays wedged in "just stopped,
ignore this insert" for that whole time: `insert_tick` suppresses the
auto-scan/auto-rip dispatch, so an unattended box quietly ignores the disc
sitting in the drive. `Instant` cannot step backwards, which is the whole
reason it exists.

Proven structurally: stepping the system clock inside the test process is
not portable. The behavioural halves (active when set, inactive once the
deadline has passed) are pinned in `a_stop_cooldown_expires` below.

## byte_offset_in_title_survives_an_overflowing_extent (test)

A disc-supplied extent must never be able to panic the rip thread.
`start_lba` and `sector_count` come straight from the UDF allocation
descriptors — untrusted, disc-controlled data. `start_lba + sector_count`
is `u32` arithmetic: a corrupt or hostile image with `start_lba` near
`u32::MAX` overflows, which panics in debug (killing the rip thread
mid-rip, on damage attribution of all things) and wraps in release,
making the containment test answer nonsense. An extent that cannot
express its own end simply does not contain the LBA.

## single_pass_done_card_total_lost_ms_drives_severity (test)

Catches the mutation that feeds the done card the STARVED
`sweep_damage_snapshot.total_lost_ms` (0.0 in single-pass, which has no
mapfile) instead of the real in-title loss — a damaged rip filed as clean.

The previous version of this test only called `damage_severity_for` with
two literals. That pins the classifier, which was never the thing at
risk: the wiring lives in `rip_disc`, and reintroducing the bug there
left this test green. The decision now lives in one function
(`super::super::done_card_lost_ms`) and this drives it, then pushes the
result through the REAL `update_state` — which is where
`damage_severity` is actually computed for the card.

## PassProgressState::frozen_bytes_lost

`bytes_unreadable` snapshotted on this pass's first `push_pass_state`
callback, frozen for the rest of the pass. The total-progress denominator
(`max_retries × bytes_lost`) uses this frozen value instead of the live
mapfile figure: during Pass 1 `bytes_unreadable` grows from 0 as new bad
sectors are discovered, so a live read inflated the denominator mid-pass
and made `total_pct` visibly stall or regress. Re-snapshotted each pass
(a fresh `PassProgressState` is built per pass), so the estimate still
tightens pass-to-pass.
