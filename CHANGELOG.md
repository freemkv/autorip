# Changelog

## 0.13.41 (2026-04-29)

### Debug logging for sector-0 regression diagnosis

- Add debug logging to Drive::read and Disc::copy first reads.
- No functional changes.

## 0.13.40 (2026-04-28)

### ECC-block sweep, multi-pass recovery, USB bridge crash fix

Picks up libfreemkv 0.13.39 with the new Pass 1 ECC-block sweep and
mapfile-based recovery pipeline.

- Pass 1 reads 32-sector batches, skips bad ECC blocks (NonTrimmed), never
  retries — prevents Initio INIC-1618L USB bridge crash.
- Pass 2+ recovers NonTrimmed sectors one at a time via Disc::patch().
- 60-second hot-plug grace period for freshly inserted drives.
- `/api/version` endpoint for deploy verification.
- Removed `skip_forward`, `cautious_pause_ms`, and `BPT1_EXIT_THRESHOLD` from
  ripper configuration — all handled by libfreemkv now.

## 0.13.26 (2026-04-27)

### Sync release — picks up libfreemkv 0.13.26 (DiscRead SCSI status/sense)

New error format now shows SCSI status and sense in errors:
- `E6000: sector 0x{status}/0x{sense_key}/0x{asc}`
- Enables recovery loop to distinguish drive wedge from bad sectors

## 0.13.25 (2026-04-27)

### Sync release — picks up libfreemkv 0.13.25 (dead-code cleanup)

No autorip functional changes.

## 0.13.24 (2026-04-27)

### `bytes_maybe` excludes NonTried (= ahead of read head)

v0.13.23's `RipState.bytes_maybe` read `mapfile.stats().bytes_pending`
which conflated `NonTried` (sectors Pass 1 hasn't reached) with
`NonTrimmed`/`NonScraped` (sectors flagged for Pass 2-N retry). At
pct=0 the entire 78 GB unread disc surfaced as "Maybe" in the yellow
pill — wrong UX. v0.13.24 consumes libfreemkv 0.13.24's new
`MapStats.bytes_retryable` field, which is just NonTrimmed +
NonScraped. Maybe pill now starts at 0 GB on a clean rip and grows
in spurts when hysteresis hits a marginal sector. Drops as Pass 2-N
either recovers (joins Good) or gives up (joins Lost).

### UI polish (web dashboard)

  - `fmtMs` escalates to `H:MM:SS` / `M:SS` for large durations.
    "10817 s" now renders as "3:00:17". Below 1 s still uses
    millisecond precision for tight read-trace traces.
  - Pill row gets the same vertical breathing room (14px margin-top)
    as the gap between the per-pass and total bars.
  - Total line drops the redundant "Total ETA" prefix → just "ETA"
    matching the per-pass line's terse format.
  - Total line drops "Recovered X / Y GB" — the green Good pill
    already shows the same number without duplicating it.

### cargo fmt cleanup

Same lint cleanup as libfreemkv 0.13.24 — picks up rustfmt fixes
that have been red on `main` CI since v0.13.18.

## 0.13.23 (2026-04-27)

### Consume libfreemkv 0.13.23 SCSI sense plumbing

libfreemkv 0.13.23 stops discarding the drive's CHECK CONDITION sense
data — pre-fix every drive-reported error was being misclassified as
a transport wedge, which made `Disc::copy`'s hysteresis bail before
it could engage. The library fix unblocks the recovery path on
damaged discs.

### Three-bucket reporting in RipState + UI

`RipState` now exposes the mapfile state as three explicit buckets:

  - **GOOD** (`bytes_good`) — Finished sectors. Terminal success.
  - **MAYBE** (`bytes_maybe`) — Pending sectors. Pass 2-N may recover them.
  - **LOST** (`bytes_lost`) — Unreadable sectors. Terminal failure.

with companion playback-time fields (`total_maybe_ms`,
`total_lost_ms`). The legacy `bytes_bad` field is replaced by
`bytes_lost` (terminal-only); pending bytes were previously folded
into `bytes_bad` which made the UI show pending sectors as "errors"
even though they were still scheduled for retry.

Web dashboard renders three colored pills:

  - green "Good · X.X GB"
  - yellow "Maybe · X.X MB · ~Y.Ys"
  - red "{Severity} · X.X MB · ~Y.Ys" (severity = damage_severity from v0.13.22)

Pills are hidden when the corresponding bucket is empty.

### Pass 2-N: reverse direction + bpt=1 (no behaviour change, just confirmed)

The Pass 2-N reverse-direction alternation (odd retry_n = reverse,
even = forward) and bpt=1 (from v0.13.22's taper drop) are kept and
documented as the canonical retry strategy. No multi-attempt retry,
no read-speed throttling, no USB-layer reset — empirically tested and
discarded.

## 0.13.22 (2026-04-26)

### Consume libfreemkv 0.13.22 hysteresis Block↔Single recovery

libfreemkv 0.13.22 replaces v0.13.21's bisect-on-fail with a hysteresis
state machine that drops straight to bpt=1 on a block read failure,
stays there until 10K consecutive good reads, then returns to Block
mode. ~3× faster recovery on dense damage clusters; same 100% sector
recovery rate.

### Drop multi-pass batch taper

Pass 2..N now uses `block_sectors_pass = 1` unconditionally. With
hysteresis, every NonTrimmed range from Pass 1 is already a single
sector, so the v0.13.15 taper (60 → 30 → 15 → 7 → 1) was cosmetic —
`Disc::patch`'s internal min(block_sectors, range.size) capped it to
1 anyway. Direction alternation (forward/reverse) is preserved.

### Damage severity in RipState + UI

New `damage_severity` field on `RipState`, computed from `errors` and
`total_lost_ms` via `libfreemkv::classify_damage`:

| Severity  | Bad sectors | Lost playback |
|-----------|-------------|---------------|
| Clean     | 0           | 0             |
| Cosmetic  | 1–50        | <1s           |
| Moderate  | 51–499      | 1s–30s        |
| Serious   | ≥500        | ≥30s          |

`update_state` recomputes severity on every push so the UI badge
stays in sync. The web dashboard renders a colored pill ("Cosmetic" /
"Moderate" / "Serious") next to the existing "X unreadable · ~Y lost"
line; clean rips show no pill.

### Wallclock-cadence progress callbacks

Pass 1's progress callback now fires every 2s of wallclock (`PROGRESS_TICK`)
in addition to the outer-loop iteration boundary. In Single mode the
outer loop can sit on a single block for 30+ seconds; the wallclock
tick keeps the UI live.

## 0.13.21 (2026-04-26)

### Sync release — picks up libfreemkv 0.13.21 bisect-on-fail + timeout fix

No autorip code changes. Consumes libfreemkv 0.13.21 which:
- Replaces `Disc::copy`'s skip-forward with bisect-on-fail (recovers
  data the drive can read individually but fails as multi-sector
  blocks — empirically the BU40N's bad-zone pattern).
- Bumps the caller-side READ timeout from 1.5 s → 10 s, fixing the
  cold-start cancel cycle that wedged the Initio bridge.

This is the v0.13.18-20 wedge fix for real. Pass 1 should now recover
~99 % of a damaged-disc rip, with multi-pass becoming a fast no-op
when bisect already cleaned up.

## 0.13.20 (2026-04-26)

### Sync release — picks up libfreemkv 0.13.20 architecture changes

Consumes libfreemkv 0.13.20's SCSI rewrite (sync blocking SG_IO,
no userspace abort/reset, no fd recovery dance). autorip itself
doesn't touch the SCSI transport directly, so this is a transparent
dep bump. The full 0.13.19 development bundle below ships in this
release — 0.13.19 was held during development and never tagged.

Bug fixes + UI changes from the held 0.13.19:

### Fix: total ETA math (multipass)

`push_pass_state` was computing `total_work_estimated = capacity +
4 × bytes_pending + capacity` with a hardcoded retry count and using
`bytes_pending` (everything not yet read) instead of `bytes_unreadable`
(only the *bad* set). At the start of Pass 1, `bytes_pending == capacity`,
so total work resolved to `~6 × capacity` and the total ETA showed as
roughly `6 × pass_eta` (e.g., 9 h vs the real 1.5 h).

Now: `total_work = capacity + cfg.max_retries × bytes_unreadable
+ mux_estimate` (mux estimate skipped entirely in single-pass mode where
no ISO is produced). Cumulative-done formula uses `bytes_unreadable`
consistently across passes too. New field `PassContext::max_retries` so
the progress callback no longer needs the cfg lock.

### UI: matching bar styles + breathing room

The pair of progress bars introduced in 0.13.18 looked like two
unrelated components — different heights, different opacities, cramped
text rows. Polishing:

- Total bar matches the pass bar's geometry (height 6 px,
  border-radius 3 px) so they read as a pair. Accent colour + 0.7
  opacity still flag it as the secondary/aggregate signal.
- More vertical breathing room: bar → text gap 4 → 7 px;
  pass-block → total-block 6 → 14 px.
- Wider horizontal separator between stats (` · ` → `  ·  `).

### Settings: Single Pass / Multi Pass selector (progressive disclosure)

The previous "Retry Passes" (number) + "Keep Intermediate ISO" (bool)
fields were confusing — Single Pass mode had a "0" sitting in a number
input and a Keep-ISO toggle that only mattered for Multi Pass. Replaced
with a `Rip Mode` radio:

- **Single Pass** — direct disc → MKV. No retries, no ISO. Sub-options
  hidden — minimal UI for the common case.
- **Multi Pass** — disc → ISO → retry bad sectors → mux. Selecting this
  reveals retry-pass count + keep-ISO inline.

Implementation is purely UI-side: backend still stores `max_retries`
(int) and `keep_iso` (bool). `renderSettings` derives a virtual
`rip_mode` from `max_retries`; `saveSettings` translates back before
POST. No settings-file migration needed — old `settings.json` keeps
working unchanged.

## 0.13.18 (2026-04-26)

### Fix: UI shows two distinct progress bars (pass + total)

v0.13.16 collapsed pass_progress_pct, total_progress_pct, pass_eta,
total_eta, speed, and recovered-bytes into a single bar with a single
text line ("sg5 · pass 1/7 · copying...30% · ETA 1:20 · Total 0% ·
Total ETA 8:03 · 16.4 MB/s · Recovered 1.7 / 78.8 GB"). User
correctly identified this as unreadable: the text line ran off-screen
and there was no visual ranking between current-pass progress and the
much-longer total progress.

Fix in `web.rs`:
- Per-pass bar (full-height, `--green`, with bad-range red overlay) +
  its own text line: `<pct>% · ETA H:MM · NN MB/s`.
- Total bar (thinner 4 px, `--accent`, opacity 0.85, no overlay) + its
  own text line: `Total Y% · Total ETA H:MM · Recovered A.B / C.D GB`.
- New `renderTotalBar(p)` helper. Pass bar continues to use existing
  `renderBar(s,p)`.
- JS still does NO math — both percentages and both ETAs are read
  directly from `RipState`. Speed/recovered formatting unchanged.

This is a UI-only change; backend, ripping logic, and SCSI code are
untouched. Settings page reorganization from 0.13.16 retained.

## 0.13.17 (2026-04-26)

### Fix: hot-plug — autorip picks up unplug/replug without container restart

`drive_poll_loop` cached the drive list at startup ("design conversation
for 0.14" comment in pre-0.13.17 code). When the user unplugged a wedged
drive and replugged, the kernel re-enumerated it (often at a new
`/dev/sg*` slot), but autorip's cached path list never refreshed —
`/api/state` stayed empty until container restart. Lost ~30 minutes of
wall time across yesterday's testing alone.

Fix: every 30 s the poll loop calls `libfreemkv::list_drives()` and
reconciles against the cached path list:
- New devices → log `"drive enumerated (hot-plug)"` and start polling them.
- Devices that disappeared → log `"drive removed (hot-unplug)"`,
  `drop_session`, remove from `STATE` map.

Cross-platform via libfreemkv's existing `list_drives()` (Linux sg/macOS
disk/Windows CdRom enumeration). No platform-specific udev integration.

## 0.13.16 (2026-04-26)

### Fix: UI lies. Bar reads `pass_progress_pct` directly. (RIP_DESIGN.md §16)

v0.13.15 shipped a backend `progress_pct = pos/total` fix but the web
JS still computed `pct = bytes_good/bytes_total_disc` and rendered THAT.
The UI bar froze at 30% during the bad zone while the backend correctly
reported 50% (and growing). The user saw "stalled," and we had to query
the API directly to see the real numbers.

Fixes:
- New `RipState` fields: `pass_progress_pct`, `pass_eta`,
  `total_progress_pct`, `total_eta`. Server is the single source of
  truth; JS reads them directly with NO math.
- Web UI dashboard renders 5 user-visible numbers: pass %, pass ETA,
  total %, total ETA, recovered (`bytes_good / bytes_total_disc`).
- `speed_mbs` now tracks rate of `work_done` advancement (the bar
  motion) rather than `bytes_good` accrual. v0.13.15 had this wrong
  too — speed read 0 KB/s during skip-forward zones even though the
  bar was moving.

### Fix: settings page reorganized into 3 logical groups

`Ripping` was a grab-bag. Split into:

- **Disc Lifecycle**: `on_insert`, `auto_eject` (pre/post disc events)
- **Ripping**: `main_feature`, `min_length_secs`, `output_format`,
  `network_target` (what artifact to produce)
- **Recovery**: `on_read_error`, `max_retries`, `keep_iso` (bad-sector
  handling)

`on_read_error` is a bad-sector knob — moved out of Ripping into
Recovery. `auto_eject` is the lifecycle counterpart to `on_insert` —
moved into Lifecycle.

### Adopt: libfreemkv 0.13.16 `Progress` trait architecture

Both Pass 1 (`Disc::copy`) and retry passes (`Disc::patch`) now use the
new single-shape callback (`Progress` trait + `PassProgress` struct).
Per-pass and total ETAs computed from the same speed observation, with
total ETA factoring in estimated retry work + a 200 MB/s mux estimate.

### Deferred to v0.13.17

- `output_format` matrix (explicit branches for MKV/M2TS/ISO/Network ×
  multipass/direct).
- Mux phase as a visible bar phase (DiscStream emits `PassKind::Mux`).

## 0.13.15 (2026-04-26)

### Fix: pos-based progress display (RIP_DESIGN.md §15 Fix C)

`progress_gb` and `progress_pct` now track libfreemkv's `pos` (sweep
position) rather than `bytes_good` (clean reads). The old display froze
when Pass 1 hit a bad zone and skip-forwarded the rest — the user saw
"30 % / 0 KB/s" while Pass 1 was actually marching to end-of-disc. Now
the bar advances during skip-forward so live state matches reality.
`bytes_good` remains exposed in RipState for the "real data recovered"
number. Device-log lines now report both: `swept X GB (Y%) good Z GB`.

### Fix: per-pass wallclock cap (RIP_DESIGN.md §15 Fix A)

Replaced v0.13.12's whole-rip wallclock cap with a per-pass cap:
each pass (Pass 1 sweep + every retry) gets its own
`max(disc_runtime_secs, 3600)` budget. New `spawn_pass_watcher` helper
spawns a per-pass watcher with its own `pass_halt` Arc that the watcher
forwards user-stop into. On cap-fire: writes
`last_error = "Pass N exceeded {budget} budget"` and flips a shared
`cap_fired_any` flag.

### Fix: mux only on natural completion (RIP_DESIGN.md §15 Fix B)

Mux is now skipped — and `status=error` set — if any pass cap-fired
during the rip. ISO is retained in staging for manual salvage. Mux
runs only when:
1. User did NOT press stop, AND
2. No pass cap-fired, AND
3. Pass loop exited naturally (max_retries reached, all clean, or
   recovered=0 on a non-wedged pass).

### Fix: per-pass retry strategy (RIP_DESIGN.md §15 Fix D + G)

Pass 2..N now use:
- **Block-size taper**: pass `n` (1-indexed retry) uses `batch >> n`
  sectors, minimum 2. Last retry pass always uses 1 sector.
  For `batch=60` on the BU40N, `max_retries=4`: `[60, 30, 15, 7, 1]`.
- **Reverse-direction alternation**: retry 1 = reverse, retry 2 =
  forward, retry 3 = reverse, ... Reverse walk approaches the
  post-bad-zone NonTrimmed range from end-of-disc, where the drive
  hasn't yet wedged on a bad sector.

Both vary per-pass, set via the new `PatchOptions::block_sectors`
+ `PatchOptions::reverse` (libfreemkv 0.13.15).

### Fix: drive-wedged early-exit per pass (RIP_DESIGN.md §15 Fix E)

`PatchOptions::wedged_threshold = 50` — `Disc::patch` exits early if
50 consecutive failures occur with zero successes in the same pass.
Saves the wallclock cap for productive retry passes (different
direction, smaller block). Surfaces via `PatchResult::wedged_exit`,
which we log as `(drive wedged — abandoned this pass)` in the
device log.

### Fix: 30 s drive settle between Pass 1 and Pass 2 (RIP_DESIGN.md §15 Fix F)

Sleep 30 s after Pass 1 returns and before Pass 2 spawns, giving the
drive's internal ECC state time to recover. The BU40N (and other
Initio-bridge drives) wedge after grinding on bad sectors during
Pass 1; immediately hammering the same bad zone with Pass 2 retries
keeps the drive wedged. Cheap insurance.

### Version sync — consume libfreemkv 0.13.15

3-arg `on_progress`, `PatchOptions::reverse`, `wedged_threshold`,
`PatchResult::wedged_exit`, new trace events.

## 0.13.14 (2026-04-25)

### Fix: enable libfreemkv trace targets in tracing subscriber

The v0.13.13 telemetry from `freemkv::scsi` and `freemkv::disc` was being
silently dropped because `observe.rs:50`'s default filter was
`autorip=info,libfreemkv=warn`. Trace events at `target = "freemkv::scsi"`
got filtered out at warn level. Updated the default filter to
`autorip=info,libfreemkv=warn,freemkv::scsi=trace,freemkv::disc=trace` so
the per-call SCSI + Disc::copy events appear in `/api/debug` JSONL during
a live rip. Override via `AUTORIP_LOG_LEVEL` env var if needed.

### Version sync — consume libfreemkv 0.13.14

No-op consumer bump (libfreemkv 0.13.14 has no functional changes).

## 0.13.13 (2026-04-25)

### Fix: Pass 1 progress total uses `disc.capacity_bytes`, not title size

`ripper.rs:1339` computed `total_bytes` from `disc.titles[0].size_bytes`
(the chosen movie's playlist size estimate), but Pass 1 reads the WHOLE
disc and reports progress against that total. With size_bytes=0 (or any
value smaller than the disc) the UI showed "0.0 GB / 0.0 GB" during
Pass 1, masking real progress and hiding the v0.13.12 hang. Now uses
`disc.capacity_bytes`. The mux phase below already overrides via
`info.size_bytes`, so the title-level total still flows through where
relevant.

### Version sync — consume libfreemkv 0.13.13

Picks up the new tracing instrumentation in `SgIoTransport::execute`
(Linux) + `Disc::copy`. All trace events flow through the existing
tracing subscriber and surface in `/api/debug` JSONL — filter via
`q=freemkv::scsi` or `q=freemkv::disc` to see per-call timing in flight.

## 0.13.12 (2026-04-25)

### Fix: wallclock-budget watcher (RIP_DESIGN.md §6 Fix 3)

`rip_disc` now spawns a watcher thread that fires the halt flag if the
total rip wallclock exceeds `max(disc_runtime_secs, 3600)`. 1× disc
runtime is the worst-case ceiling per design — a 3-hour movie should
rip in at most 3 hours; anything longer is unproductive grinding the
in-pipeline retry path can't recover from. On expiration:

- Sets `halt` so the in-flight `Disc::copy` / `Disc::patch` exits cleanly.
- Writes `last_error = "exceeded {N}h {NN}m rip budget"` to RipState
  for UI surfacing.
- Logs `Wallclock budget exceeded ({budget}); halting rip` to the
  per-device log.
- Self-clears via a `WallclockGuard` Drop on `rip_disc` return.

### Fix: consume libfreemkv 0.13.12 PatchResult counters

Pass 2..N's per-pass log line now includes `blocks attempted=N read_ok=N
read_failed=N` so the v0.13.11 mystery (Dune 2: "100 minutes recovered 0
bytes") becomes diagnosable from the live device log.

### Fix: stop drain comment drift

`web.rs:1513` said "35 s drain budget" but the code at `:1520` uses
`Duration::from_secs(60)`. CHANGELOG 0.13.11 corrected the warning text;
this corrects the design comment + adds the v0.13.8 rationale (slower
drains under heavy ECC retry on the BU40N).

### Version sync — consume libfreemkv 0.13.12

Picks up Fix 1 (stall-guard deletion), Fix 2 (async SCSI recovery on
Linux + cross-platform try_recover on Windows + macOS), Fix 4
(`PatchResult` instrumentation), and the `PatchOptions::full_recovery`
honor + `CopyOptions::stall_secs` deletion. Also drops the
`stall_secs: None` line from autorip's `CopyOptions` construction since
the field no longer exists upstream.

## 0.13.11 (2026-04-25)

### Version sync — consume libfreemkv 0.13.11

Picks up libfreemkv's revert of the v0.13.10 SgIoTransport timeout
path. v0.13.10's "fd-dead-after-one-timeout" caused Pass 1 to
finish in 45 ms with 0 bytes good on Dune 2; v0.13.11 keeps the
transport alive across timeouts. Stall guard from v0.13.9 still
caps the worst-case stall at 120 s.

## 0.13.10 (2026-04-25)

### Fix: Pass 1 RipState now reports preferred_batch / current_batch

Previously /api/state showed `batch=0/0` during Pass 1 (Disc::copy):
the per-pass `set_pass_progress` and `push_pass_state` called
`update_state` with `..Default::default()`, leaving the batch fields
at their default 0. The kernel-reported preferred batch (from
`detect_max_batch_sectors` at ripper.rs:1411) is now threaded
through `PassContext` and surfaced in RipState during every Pass 1
and retry-pass progress update.

Pass 1 doesn't shrink the batch (Disc::copy uses a fixed batch
size), so `current_batch == preferred_batch` throughout. The
DiscStream adaptive batch halver still runs only during the mux
phase and is reported by the direct-mode stream loop.

Note: `last_sector` during Pass 1 is still 0 — it requires a
libfreemkv on_progress signature change (Fn(u64,u64,u64)) and is
deferred to v0.14.

## 0.13.9 (2026-04-25)

### Cosmetic + version sync

- Warning message "rip thread did not drain within 35s of stop"
  corrected to "60s" — matches the v0.13.8 drain timeout. Same value
  in `eject_drive`'s warning. No behavior change.
- Picks up libfreemkv 0.13.9: `Disc::copy` stall guard + the
  `SgIoTransport` reopen-after-timeout fix that prevents the silent
  Pass 1 hang observed on Dune 2 in the v0.13.8 live test.

## 0.13.8 (2026-04-25)

### Fix: stop drain races, post-stop "error" leak, hardening

Hotfix for two issues seen during live v0.13.7 validation on the Dell
host (BU40N) where stop-drain worked but the post-stop UX surfaced as
"error":

- Drain timeout 35 s -> 60 s (handle_stop, eject_drive, main shutdown).
  35 s wasn't enough for `Disc::copy`'s in-flight 30 s SCSI READ +
  unwind on halt; the join timed out and `wipe_staging` raced the
  rip thread, producing `E5000: No such file or directory`.
- Halt-aware error handling. `Disc::copy` and `Disc::patch` Err arms
  now check the halt flag first: an IO error during a stop is logged
  as "Pass N cancelled (halt)" and does NOT update state to "error",
  so the post-stop state stays cleanly idle (set by handle_stop). A
  real (non-halt) error still surfaces as "error" with the underlying
  message.
- Structural follow-up to v0.13.7: introduced `ripper::spawn_rip_thread`
  helper that bundles `Builder::new().name(...).spawn(...) +
  register_rip_thread` into one call. All three rip-related spawn
  sites (poll-loop, handle_scan, handle_rip) now use it. New
  `tests/spawn_registration.rs` pins the contract that v0.13.6 first
  violated. See the post-mortem follow-up in freemkv-private.

## 0.13.7 (2026-04-25)

### Fix: /api/rip and /api/scan threads now register for stop-drain

The 0.13.6 stop-drain fix only registered the rip thread when it was
spawned by the poll-loop on disc-insert (`on_insert=rip`). Threads
spawned by the HTTP handlers `/api/rip/{dev}` and `/api/scan/{dev}`
still used the old un-registered `std::thread::spawn(...)` pattern, so
`handle_stop`'s `join_rip_thread` returned `Err(())` immediately
(no handle in the map) and the response came back in milliseconds —
exactly the staging-wipe-races-rip-thread race the 0.13.6 fix was
supposed to close, just on a different code path.

Live testing on the Dell host with v0.13.6 confirmed the bug:
`/api/stop/sg4` returned in 27 ms while the rip thread was clearly
still running (status went to `error` 6 s later as the unfilled file
write surfaced).

`handle_rip` (web.rs:~1376) and `handle_scan` (web.rs:~1336) now use
`std::thread::Builder::new().name(...).spawn(...)` and register the
returned `JoinHandle` via `ripper::register_rip_thread`, matching the
pattern in the poll-loop spawn site. `handle_stop`'s join logic is
unchanged; it now actually has a handle to wait on.

## 0.13.6 (2026-04-25)

### Real-time direct-mode progress + bounded stop-drain

Two production bugs from the v0.13.5 test rig, both resolved here on
top of libfreemkv 0.13.6's new `BytesRead` event and `Drive::read`
retry strip.

**Per-device progress in direct mode.** Pre-0.13.6 the per-device UI
sat at "0 KB/s, 0%" for the entire duration of a direct (no-mapfile)
rip. Two causes:
- libfreemkv never emitted `EventKind::BytesRead`. Fixed in 0.13.6 of
  the lib.
- The first state update was gated behind a 1 s throttle, so even
  once events flowed, the cold-start frame was a zero. The
  smooth-speed seed was an implicit "if 0 then replace" hack that
  fought the throttle gate.

`Drive::on_event` and `DiscStream::on_event` now consume
`EventKind::BytesRead` (ripper.rs:1372, 1686) and stash the latest
cumulative byte count into an `Arc<AtomicU64>`. The main rip loop
prefers it over `output.bytes_written()` whenever the lib is feeding
us progress. Cold-start is fixed via a `first_update` flag that
bypasses the throttle for the first frame and a `seeded_speed` flag
that replaces the seed-when-zero hack with explicit one-shot
seeding. UI now goes live within the first read tick.

**Stop is now drain-bounded, not best-effort.** Pre-0.13.6,
`POST /api/stop/<device>` flipped the rip thread's halt flag and
returned 200 immediately, then the staging-wipe ran while the rip
thread was still inside an in-flight CDB — racing the wipe with
`Disc::copy`'s file writes. On a wedge-class drive this manifested
as "stop returns OK, files reappear in staging seconds later, next
rip resumes from corrupt mapfile."

The rip thread is now spawned with a retained `JoinHandle` registered
into a global `RIP_THREADS` map. `handle_stop` joins the rip thread
(35 s bounded timeout — covers libfreemkv 0.13.6's 30 s recovery
timeout plus a small grace) BEFORE wiping staging or resetting state.
`main`'s SIGTERM / SIGINT handler joins all rip threads before
exit. `eject_drive` synchronizes with the rip thread before
`drop_session`. Eject is up to 30 s slower in the worst case but
no longer races the rip; staging wipes always win.

### Dead match arms removed

`Retry`, `SectorRecovered`, and `SpeedChange` are no longer emitted
by libfreemkv 0.13.6 (the recovery loop that produced them is gone).
The corresponding match arms in `Drive::on_event` / `DiscStream::on_event`
are deleted.

### Tests

- New `src/lib.rs` so integration tests can reach internals;
  `main.rs` and `lib.rs` compile against the same module graph.
- New `tests/reporting.rs`, `tests/halt_drain.rs`, `tests/end_to_end.rs`
  — 9 integration tests covering `BytesRead` state propagation,
  first-frame publish bypassing throttle, smoothing seed,
  halt-flag set, stop-drain join, eject sync, JSON state
  round-trip, route dispatch.
- 9 integration + 59 unit tests green; no regressions.

### Consumes libfreemkv 0.13.6
Cargo.toml dep pin `0.13.5` → `0.13.6`.

### Version sync
0.13.6 ecosystem release (libfreemkv + freemkv + bdemu + autorip all
on 0.13.6).

## 0.13.5 (2026-04-25)

### Stop is a true reset; startup sweeps stale staging

Two bugs surfaced during the 0.13.4 production test of Dune: Part Two and
are fixed here.

**Startup staging sweep.** Prior autorip processes killed mid-rip leave
their partial ISO + mapfile + MKV in `/staging/<disc>/`. 0.12.5's
"every rip starts fresh" logic only cleaned stale data when the *same*
disc was re-inserted; unrelated orphans (yesterday's MKV, unrelated
dirs from other discs) accumulated forever. On `drive_poll_loop` entry
we now wipe every subdirectory of `cfg.staging_dir` unconditionally —
at startup there are no live sessions, so every entry is orphaned.

**Stop → full reset.** `POST /api/stop/<device>` previously only
signalled the rip thread to abort and flipped status to `"idle"`. The
in-progress ISO / mapfile stayed on disk, so the next rip's resume-safe
path could pick up half-written bytes. Now stop = reset: wipe
`cfg.staging_dir/*` and collapse the state entry back to a fresh
`RipState { status: "idle", disc_present, ..Default::default() }`. The
next rip starts on a clean disk.

**Stale log text.** The `drive_has_disc failed` WARN still referenced
the 0.13.2/3 "recovery exhausted" wording, which misleadingly implied
the lib had tried something. libfreemkv 0.13.4 rolled recovery back;
updated the text to "drive firmware unresponsive; physical reconnect
or host reboot required".

Dep pin `0.13.4` → `0.13.5` (sync bump; libfreemkv has no functional
changes).

## 0.13.4 (2026-04-25)

### Consume libfreemkv 0.13.4 — wedge recovery rolled back

libfreemkv 0.13.4 removes the SCSI-reset + USBDEVFS_RESET escalation
from `drive_has_disc` after production testing on the LG BU40N
confirmed no userspace software recovery clears the firmware-level
wedge class we see in practice. autorip's poll loop now surfaces the
raw wedge error to the user on the first occurrence (previously hidden
behind minutes of silent internal retries).

On the UI: `list_drives()` falls back to kernel-cached sysfs identity
strings when the live INQUIRY returns empty, so a wedged drive still
shows up with its vendor/model instead of vanishing. No autorip source
changes; dep pin `0.13.3` → `0.13.4`.

## 0.13.3 (2026-04-24)

### Consume libfreemkv 0.13.3 — wedge recovery actually runs now

autorip 0.13.2 deployed clean but the underlying
`libfreemkv::drive_has_disc` never escalated to SCSI/USB reset on the
real production wedge signature (`E4000: 0x00/0xff/0x00`) because
libfreemkv's `is_wedge_signature` was gated on the INQUIRY opcode.
The poll loop's "recovery exhausted" warning was firing on the raw
pass-through error without recovery ever having been attempted.

libfreemkv 0.13.3 drops the opcode gate; any `status=0xff` TUR error
now triggers the full SCSI reset → USB reset → retry probe chain.
Cargo.toml dep pin `0.13.2` → `0.13.3`. No autorip source changes.

## 0.13.2 (2026-04-24)

### autorip is dumb again — all hardware code moved to libfreemkv

Architectural cleanup. autorip pre-0.13.2 was reimplementing drive
discovery (sysfs walking, SCSI type-5 filtering, `/dev/sg*` path
construction) and the wedge-recovery escalation that 0.13.1 added,
all of which are libfreemkv's job. 0.13.2 deletes those copies and
calls libfreemkv's new public probes:

- Removed `enumerate_optical_drives()` (sysfs walk + type-5 filter +
  fallback). Now `libfreemkv::list_drives()`.
- Removed `try_recover_wedge()`, `is_wedge_signature()`, the wedge
  signature constants (`WEDGE_ERROR_CODE`, `WEDGE_STATUS_HEX`),
  `USB_RESET_SETTLE_SECS`. Now folded inside
  `libfreemkv::drive_has_disc(path)`, hidden from callers.
- Removed direct calls to `libfreemkv::scsi::reset` / `scsi::usb_reset`
  — those are `pub(crate)` in 0.13.2 and unreachable from autorip
  anyway.
- The poll loop's per-tick `Drive::open` is gone. autorip used to call
  the 2-second firmware-reset preamble of `Drive::open` 4 times every
  5 s just to check disc presence — exactly the hot-loop pattern that
  produced the production wedge at 23:51 UTC. Replaced with
  `drive_has_disc(path)` (single TEST UNIT READY, ~50 ms).

The poll loop is now a flat iteration over a startup-cached
`list_drives()` snapshot, with `drive_has_disc` as the per-tick
probe. ~80 lines deleted, ~30 added; net negative.

### Self-recovery preserved

The wedge that triggered the v0.13.1 emergency hotfix is still
self-recoverable — `drive_has_disc` does the SCSI reset + USB reset
escalation internally. autorip never sees a wedge error unless
recovery has been exhausted; logs say `drive_has_disc failed
(recovery exhausted)` with full structured fields when that happens.

### Version sync
0.13.2 ecosystem release (libfreemkv + freemkv + bdemu + autorip all
on 0.13.2).

## 0.13.1 (2026-04-24)

### Self-recover from wedged USB drives + fix `/api/debug` file path

Two fixes uncovered the moment v0.13.0's structured logging went live on
the BU40N test rig.

- **SCSI reset retry on wedge signature.** Drive::open returning the
  signature `E4000` (SCSI error) with INQUIRY status `0xff` (kernel
  "no response from device") is the reliable signature of a wedged USB
  drive — what an unplug-replug fixes physically. The poll loop now
  attempts a single `libfreemkv::scsi::reset()` + reopen on that exact
  signature before falling through to the throttled-warn path. Logs
  `wedged-drive signature — attempting scsi::reset() + reopen` at info,
  then either `drive recovered after scsi::reset()` (success) or
  `reopen still failing after scsi::reset()` (still bad). Lets the
  daemon self-heal from an entire class of post-upgrade wedges without
  operator intervention.
- **`/api/debug` JSONL path fix.** v0.13.0's `observe.rs` initialized
  the JSONL stream with `tracing_appender::rolling::daily`, which
  writes to `autorip.jsonl.YYYY-MM-DD`. The `/api/debug` endpoint
  expected `autorip.jsonl` (no suffix) — first call returned empty
  because the file didn't exist by that name. Switched the JSONL sink
  to `rolling::never` (fixed path `autorip.jsonl`). The human-readable
  `autorip.log` keeps daily rolling — that's an operator-tail file,
  not an API-served one. JSONL grows unbounded; an external log
  rotator (or a future autorip self-rotation pass) handles long-term.

### Consume libfreemkv 0.13.0
- `ScanOptions::with_keydb` removed; three call sites in `ripper.rs`
  and `verify.rs` migrated to struct literal.
- `AudioStream` gains `purpose: LabelPurpose` field. `format_codecs`
  in `ripper.rs` renders purpose + secondary inline (English literals
  per the autorip i18n stance — moves to `strings::get` once autorip
  adopts the same locale infrastructure as the freemkv CLI).
- `SubtitleStream` gains `qualifier: LabelQualifier`. Not currently
  rendered by autorip's UI (subtitle metadata isn't surfaced).

### Version sync
0.13.x ecosystem; libfreemkv at 0.13.0, freemkv CLI at 0.13.0, bdemu at
0.13.0. autorip at 0.13.1 (one ahead because of the two follow-up fixes
above).

## 0.13.0 (2026-04-24)

### Stop being blind: structural observability rebuild

Every commit since 0.11.13 has been a hand-instrumented fix for a thing
discovered in production because the app told us nothing. Inventory at the
start of this release: 97 log call sites, 60 silent failure paths, and the
drive poll loop logged zero of its decisions. Diagnosing today's "No drives
detected" required reading source + poking `/proc` + reading `/sys`.

This release replaces the ad-hoc `eprintln` + per-device file scheme with
the `tracing` ecosystem (`tracing` 0.1, `tracing-subscriber` 0.3,
`tracing-appender` 0.2). Every event is now structured, leveled, optionally
filtered, and written to three sinks:

- **`{AUTORIP_DIR}/logs/autorip.log`** — daily-rolled, human-readable. The
  file an operator tails when something is going on.
- **`{AUTORIP_DIR}/logs/autorip.jsonl`** — daily-rolled, JSON Lines, one
  event per line. The file you `jq` for post-mortems and the file the new
  `/api/debug` endpoint streams.
- **stderr** — captured by Docker as the container log.

Filter via `AUTORIP_LOG_LEVEL` (env-filter syntax). Default
`autorip=info,libfreemkv=warn`. For deep dives,
`AUTORIP_LOG_LEVEL=autorip=debug`.

Existing `log::syslog` and `log::device_log` API preserved as shims — the
97 call sites stay put. They emit a tracing event AND keep writing the
per-device `.log` files the web UI scrapes via `/api/logs/{device}`.

#### Drive poll loop instrumentation

Every silent skip is now a structured event:

- `Drive::open` failure → `warn!(device, error, …)` once per device, then
  `debug!` on continued failure (no log spam from a permanently-locked sg).
- sysfs type-5 reject → `debug!(device, sysfs_type, …)`.
- `disc inserted` / `disc removed` / `drive present` / `drive disappeared`
  state transitions all log at info.
- Spawned scan/rip threads get a `name` (`rip-sg4`) so panic backtraces
  carry context.

This single change is what makes today's "No drives detected" diagnosable
in one query: `curl -s host:8080/api/debug?level=warn | jq`.

#### `/api/debug` endpoint

`GET /api/debug?n=N&level=L&device=D&q=substr` tails `autorip.jsonl`,
filtered. Returns raw JSONL (newline-separated objects), so:

```sh
curl -s autorip:8080/api/debug?level=warn&device=sg4 | jq .
```

works as expected. Web UI Debug tab consumes the same endpoint. Default 500
lines, max 5000.

### Critical / High audit fixes (each one its own ghost)

- **C1: Cron job that spawned `autorip --update-keydb` is removed.** Cron
  stripped env, ran the binary as a fresh daemon, raced the live process
  for `/dev/sg*` and port 8080. Cumulative effect after a multi-day uptime
  was 30+ ghost daemons fighting over the optical drive — the actual root
  cause of today's "No drives detected." KEYDB updates run from the live
  process's hourly thread; that's the single source of truth now.
- **C2: Web bind failure → SHUTDOWN.** Pre-0.13 a port-already-in-use
  failure left the daemon running with no UI, restart policy oblivious.
  Now `web::run` flips the SHUTDOWN flag on bind failure so `main` exits
  non-zero and the container's restart policy recovers us.
- **C3: `session.disc.take().unwrap()` panic surface eliminated.** Every
  current code path sets `Some(disc)`, but a future regression would have
  panicked in a spawned thread. Now an explicit match logs and updates UI
  state to error.
- **H1: Version stamped at startup.** `autorip starting (v0.13.0, …)` plus
  a structured `version=… os=… arch=…` event. Today's incident left logs
  saying "config.rs:45:52 panic" — a line that doesn't exist in current
  source — because there was no record of which build emitted it.
- **H2: Healthcheck.** Dockerfile `HEALTHCHECK` + compose example
  `healthcheck:` section. Hits `/api/state`. Together with `restart:
  unless-stopped`, Docker auto-recovers a wedged container.
- **H3: `update_state_with(device, |s| …)` partial-update helper.** Three
  past regressions (v0.11.20 watchdog, v0.11.17 errors-on-completion,
  v0.12.0 pass-progress) were the same shape: `RipState { …,
  ..Default::default() }` silently zeroed a field the UI was rendering.
  The watchdog tick now uses the closure form — fields not explicitly set
  stay where they were.
- **H5: Drive init failures surfaced.** `let _ = drive.wait_ready()` and
  `let _ = drive.init()` in scan/rip/verify now log warn events with the
  underlying error, so degraded-drive scans don't fail later with a
  cryptic library error.

### Medium fixes

- **M3: sg enumeration via sysfs.** `0..16u8` hardcoded loop replaced with
  `read_dir("/sys/class/scsi_generic")` so sg16+ are seen and the order
  doesn't shuffle when sg numbers cross 9 → 10. Falls back to the old
  probe if `/sys` isn't mounted (dev hosts).
- **M1+M2: Sanitizer / duration helpers consolidated in `util`.**
  `sanitize_path_compact` (snake_case for staging filenames) and
  `sanitize_path_display` (human-readable for library destinations) are
  now the single source of truth — pre-0.13 there were two slightly
  different copies in `ripper` and `mover` that drifted (one replaced
  spaces, the other didn't). Same for `format_duration_hm`.
- **M4: History filename precision.** `{seconds}.json` could collide
  between two rapid rips. Now `{nanoseconds}_{device}.json`.
- **L3: Mover + KEYDB threads respect SHUTDOWN.** Pre-0.13 they slept in
  10 s / 1 h chunks regardless of signal — SIGTERM had to wait the full
  tick. Both now break out within ~1 s.
- **L5: `chrono_timestamp` renamed to `unix_timestamp_nanos`** — there is
  no `chrono` crate dep; the name was misleading.

### SHUTDOWN-responsive sleeps

`drive_poll_loop` and `mover::run` both moved from monolithic `sleep`
calls to 100 ms-tick loops that check SHUTDOWN. SIGTERM is now observed in
~1 s rather than waiting the full poll/move interval.

### What didn't change

`rip_complete` webhook payload still reports `output_path: <staging dir>`
and `move_complete` reports the final destination. The naming is
misleading (a Discord/Jellyfin user gets a path their library never sees
in `rip_complete`) but renaming the field is a webhook contract break and
isn't worth the churn for a v0.13 — flagged in audit notes for v0.14.

Two known v0.14 follow-ups: in-process mover (replace the `cp`
subprocess with `std::fs::copy` + chunked progress), and abstracting
`Drive::open` behind a trait so the poll loop can be unit-tested end-to-end.

### Tests

- `update_state_with_preserves_untouched_fields` — guards the H3 regression class
- `enumerate_optical_drives_returns_sorted_unique` — guards M3
- `sanitize_path_compact_*` / `sanitize_path_display_*` — guards M1
- `format_duration_hm_*` — guards M2

47 → 59 tests.

## 0.12.5 (2026-04-24)

### Stop silent resume — every rip starts fresh

Pass 1 of a multi-pass rip used to open `CopyOptions` with `resume: true`, so
if a prior run's `*.iso` + `*.iso.mapfile` were still sitting in staging (from
a Stop, error, eject-mid-rip, or container crash) the next rip inserted the
same disc silently picked up from the prior mapfile's `bytes_good`. Observed
on a cold rip of Dune: Part Two as "30 % · 24.0 / 78.8 GB" reported 10 s in.

- `ripper::rip_disc` now calls `Disc::copy` with `resume: false`. The library
  wipes the mapfile and recreates the ISO, so `bytes_good` starts at 0 and
  grows only with reads from this invocation. Progress display is truthful.
- No change to multi-pass semantics within a single run — Pass 1 still
  produces the ddrescue mapfile, Passes 2..N still patch bad ranges from it,
  mux still reads the finished ISO.

Resume-across-process-restart capability is gone for now. Trash cleanup of
stale ISO+mapfile on terminal failures (Stop, error, panic, eject, restart)
is the follow-up — tracked as a larger staging-lifecycle rework.

## 0.12.0 (2026-04-24)

### Multipass regression fixes observed in live v0.11.22 rip

Everything shipped in 0.11.22 was a display-layer regression: the underlying rip worked, the UI didn't. Each of these is now guarded by a unit test in `ripper::tests` so reintroducing the same class of bug will fail CI.

- **`bytes_bad` semantics.** 0.11.22 summed `Unreadable + NonTried + NonTrimmed + NonScraped`, so the UI showed the entire un-read disc as "bad" during pass 1 (saw ~73 GB "bad" on a 79 GB disc at 6% progress). Now `bytes_bad` is only `Unreadable` — confirmed given-up ranges. `NonTried` = not-yet-attempted (work in progress). Matches user expectation of "bad = lost."
- **`speed_mbs` / `eta` during passes.** Were always 0 / empty. The main rip loop's speed tracker doesn't run during `Disc::copy` or `Disc::patch`. New `PassProgressState` in the progress callback samples bytes + time per tick, computes smoothed speed + ETA. Shipped with a regression test.
- **`errors` / `lost_video_secs` during passes.** Weren't populated live — yellow "N sectors skipped" banner never surfaced during multipass. Now read from mapfile on every callback.
- **Bad-range list in the UI.** Was including `NonTrimmed` / `NonScraped` (work-in-progress), which made the table fill with "bad" rows during pass 1 that hadn't actually been given up on. Now `Unreadable` only.

### UI redesign for multi-pass

0.11.22's blue "Ripping pass N/M · X / Y GB good · Z MB bad" banner duplicated the Rip step's own progress line and was visually noisy. Folded the pass info into the Rip step:

- Rip step shows `● Rip · pass N/M · copying|retrying|muxing` with the progress bar, `GB / total · speed · ETA` stats line, and a small yellow sub-line with unreadable count + ms lost (when > 0).
- Dropped the separate blue banner.
- `fmtMs` / `passLabelFor` helpers centralize the formatting.

### History record

- `bad_bytes` now also only counts `Unreadable`, matching UI semantics. Interrupted multipass rips no longer log the unread-but-not-bad portion as lost.

### Testing

- New `ripper::tests` module with 9 tests guarding `build_bad_ranges` (status filtering, sorting, truncation), `byte_offset_in_title` (single/multi-extent/out-of-range), and `PassProgressState` (speed tracker nonzero on positive delta).
- `cargo test --bin autorip` runs them. CI picks this up automatically.

### Rust 2024 edition
- Bumped `edition = "2024"`. Match-ergonomics fixes in `ripper.rs` and `mover.rs` (removed redundant `ref` bindings).
- No behavior change.

### Consumes libfreemkv 0.12.0

## 0.11.22 (2026-04-24)

### Multi-pass UI completed

Follow-up to 0.11.21 — every item from the original multi-pass design is now shipped. No more "come back later" TODOs on the UI side.

- **Live mapfile stats during passes.** `Disc::copy` and `Disc::patch` now receive a progress callback that re-reads the sidecar mapfile every ~1.5 s and pushes full pass state (bytes_good, bytes_bad, bad_ranges, total_lost_ms, largest_gap_ms) into `RipState`. Pass progress is no longer frozen between pass transitions.
- **`BadRange` data model.** New serialized struct: `lba`, `count`, `duration_ms`, `chapter`, `time_offset_secs`. Chapter + timestamp come from walking the title's extents and falling through to `VerifyResult::chapter_at_offset` — unreadable regions outside the main feature get `chapter: null`.
- **Progress bar overlay.** Green fill for `bytes_good / bytes_total_disc`, red ticks at each bad range's LBA position (min 0.3% width so single-sector regions are still visible on a 72 GB UHD).
- **Collapsible bad-range list.** Below the progress bar: `N bad ranges · M ms total · largest L ms` summary; expands to a table of LBA / sector count / ms duration / chapter+timestamp. Capped at 50 entries with a "+X smaller" footer.
- **Recovery settings section.** UI controls for `max_retries` (0-10) and `keep_iso` (bool). Persist to `settings.json`; override env vars. No more env-only config.
- **History record captures multi-pass stats.** `num_bad_ranges` and `largest_gap_ms` now written alongside `errors` and `lost_video_secs`. Both derived from the mapfile in multi-pass mode; falls through to the DiscStream counter for direct rips.
- **Time formatter** — `fmtMs` adapts: `<1 ms` / `NN ms` / `N.NN s` used consistently in the error banner and bad-range list.

### Version sync
0.11.22 ecosystem release (libfreemkv + freemkv + bdemu + autorip all on 0.11.22).

## 0.11.21 (2026-04-24)

### Multi-pass rip — disc → ISO → patch → ISO → MKV

When `max_retries > 0`, autorip now runs the full ddrescue-style multi-pass flow from libfreemkv 0.11.21:
1. `Disc::copy` with `skip_on_error=true, skip_forward=true` → disc → ISO + ddrescue-format mapfile. 64 KB block reads, exponential skip-forward on failure, zero-fill bad ranges. A damaged disc completes pass 1 in minutes instead of hours.
2. Up to `max_retries` calls to `Disc::patch` retry each bad range with full drive recovery enabled. Stops early if a pass recovers zero bytes (structure-protected sectors like Dune P2 never yield).
3. Drive released. ISO muxed to MKV via existing `DiscStream + IsoSectorReader` pipeline.
4. ISO pruned unless `keep_iso=true`.

When `max_retries == 0`, the existing direct `disc → MKV` flow is unchanged — no ISO intermediate, no retry capability, fastest path.

### New config
- `MAX_RETRIES` (env, 0..=10, default `1`) — retry passes after pass 1.
- `KEEP_ISO` (env, bool, default `false`) — preserve the intermediate ISO after mux.

### New RipState fields
- `pass` / `total_passes` — current pass number and total.
- `bytes_good` / `bytes_bad` / `bytes_total_disc` — from mapfile stats during each pass.

### UI
- Status label shows `pass N/M · copying|retrying|muxing`.
- Pass-progress banner during pass 1 and retries with live good/bad byte counts.

### Version sync
- 0.11.21 ecosystem release (libfreemkv + freemkv + bdemu + autorip all on 0.11.21).

## 0.11.20 (2026-04-24)

### Stop actually stops + UI shows real adaptive state during stalls

Two bugs in the v0.11.17 state-tracking + watchdog work surfaced during a 12+ hour rip of a damaged UHD disc. Fixing both and wiring the new libfreemkv 0.11.18 halt flag so Stop is effective inside dense bad-sector regions.

- **Wire libfreemkv 0.11.18 `DiscStream::set_halt`**. After `DiscStream::new`, pass the same halt Arc that `Drive::halt_flag()` provides. Stop now interrupts `fill_extents` inside the stream's internal retry loop rather than only at PES-frame boundaries (which may never arrive in a bad zone).
- **Fix duplicate `wd_last_frame` Arc.** The watchdog thread was reading an Arc that was shadowed by a second declaration inside the watchdog setup block, so event-callback updates (sector skip / recover / batch-size-change) were invisible to the stall detector. One Arc now, used by the event callbacks, main rip loop, and watchdog alike.
- **Preserve adaptive state through watchdog updates.** The watchdog's `update_state` used `..Default::default()` which wiped `current_batch`, `preferred_batch`, `last_sector`, and `lost_video_secs` every 15 s — so the UI showed 0/0 batch and no forward LBA even while the library was actively working through a bad zone. Now reads these from the current STATE and carries them forward.

### Consumes libfreemkv 0.11.18
Upgraded the dep pin. No other API changes in the lib.

## 0.11.19 (2026-04-24)

### Per-rip log archives + ISO-8601 timestamps

The device log is append-only across rips, and uses wall-clock-only `[HH:MM:SS]` timestamps. This broke post-mortem on a 12+h rip that crossed midnight, and the archived history record had yesterday's stalled-forever saga interleaved with tonight's fresh run — hard to tell which was which.

- **Per-rip archive.** On scan start and on eject, the current `logs/device_{dev}.log` is moved to `logs/rips/{dev}_{YYYY-MM-DDTHH-MM-SSZ}.log`. Each rip attempt produces one self-contained file. No retention policy yet — archive dir just grows; simple to prune later.
- **ISO-8601 timestamps in every log line.** `[2026-04-24T03:54:27Z] msg` instead of `[03:54:27] msg`. Archives sort correctly, midnight is unambiguous.
- **No library change.** Fully autorip-side — text format is the only public interface and it changed compatibly (older log-parsers looking for wall-clock `[HH:MM:SS]` will need to update).
- **Failure mode:** archive rename failures log to stderr and continue. A log-system bug can never break a rip.

`archive_device_log` replaces `clear_device_log` at both call sites (`ripper.rs:350` scan start, `ripper.rs:1405` eject). The in-memory 500-line buffer still clears at those points so the web UI "live log" view starts fresh for each rip.

## 0.11.18 (2026-04-24)

### Cheap sysfs pre-filter in drive poll loop

USB optical drives re-enumerate between `/dev/sg4` and `/dev/sg5` on reconnect, so the compose can't hardcode the path. The right deployment fix is to bind-mount the host's `/dev` live (`volumes: - /dev:/dev`) instead of an explicit `devices:` list. But that exposes every host sg node — including non-optical ones like RAID controllers — to autorip's poll loop, and `Drive::open` in libfreemkv runs an unconditional 2-second reset sequence on every open. Four PERC disks alone would saturate a 5-second poll cycle with reset sleeps.

- **`drive_poll_loop` now reads `/sys/class/scsi_generic/sg{N}/device/type`** and skips anything that isn't type 5 (CD/DVD/BD). Non-optical sg nodes never reach `Drive::open`, so no reset dance fires for them.
- **Graceful fallback** — if sysfs can't be read, we proceed to `Drive::open` as before. No regression for setups where `/sys` isn't bind-mounted.
- **No library change needed** — this is a 2-line guard in autorip.

## 0.11.17 (2026-04-23)

### Trustworthy rip feedback during bad-sector recovery

Real-world trigger: a damaged UHD disc produced 622 skipped sectors over 12+ hours. The UI showed "stalled 15h 0m" the whole time because the watchdog only counted PES frame writes, and there was no way to see how much video was actually being lost. All evidence disappeared on stop — no history record, live counter reset to 0.

- **Watchdog counts sector events, not just frame writes.** Drive and DiscStream event callbacks reset `wd_last_frame` on every event. A long run of skipped sectors no longer falsely reports as stalled — it shows forward motion because it is moving forward.
- **`lost_video_secs` in `RipState`** — computed from the title's actual bitrate (`size_bytes / duration_secs`), not the old hardcoded `8_250_000` (BD sustained). UHD/DVD/BD all get correct numbers. Web UI prefers this over the client-side approximation.
- **`last_sector`, `current_batch`, `preferred_batch` in `RipState`** — track forward LBA progress and the adaptive batch sizer's current read size. UI shows a blue "Recovering · batch N/60" banner when the library has shrunk after a read failure, distinguishable from normal "Ripping" and from "stalled".
- **History record on Stop too** — every rip attempt produces a `{ts}.json` regardless of status. Fields added: `status` ("complete" / "stopped"), `errors`, `lost_video_secs`, `last_sector`. The `.done` marker still only lands on completion (mover gate unchanged).
- **Final `update_state` preserves `errors` / `lost_video_secs` / `last_sector`** — previously `..Default::default()` wiped the skip count on completion so a damaged-disc rip finished showing 0 errors.
- **Webhook `rip_complete` payload adds `errors` + `lost_video_secs`** so external systems see the actual loss.

### libfreemkv 0.11.17 — adaptive batch sizer
- Dep bumped to 0.11.17. Rip recovery now pays the descent cost once per bad region instead of once per bad sector.
- Match `BatchSizeChanged { new_size, reason }` events from the DiscStream callback; drop the removed `BinarySearch` arm.

### Local dev
- `libfreemkv` dep now uses `{ version = "0.11", path = "../libfreemkv" }` — matches the README's "clone side-by-side" workflow. Cargo strips the path when publishing.

## 0.11.16 (2026-04-21)

### SectorReader API cleanup
- libfreemkv 0.11.16: single `read_sectors()` method with recovery flag.

## 0.11.15 (2026-04-21)

### Lint cleanup
- Fix all `cargo fmt` and `cargo clippy -D warnings` issues.
- Webhook `send_rich` refactored to use `RipEvent` struct (too-many-arguments).
- Remove unused `verify::clear()`.
- Fix unnecessary double-references in verify logging.

## 0.11.14 (2026-04-21)

### Audit fixes: verify, rip error handling, logging
- **Fix: verify keydb** — passes keydb_path from config so encrypted discs scan correctly.
- **Fix: verify stop** — stop button now stops verify (was only stopping rip).
- **Fix: verify live counts** — good/bad/slow/recovered update in real-time, delta-based for batch reads.
- **Fix: verify busy/concurrent guard** — checks is_busy() and is_running() before starting.
- **Fix: verify panic guard** — catch_unwind wraps verify thread, sets error state on panic.
- **Fix: buffered frame write errors** — logged and stop-checked instead of silently dropped.
- **Fix: watchdog during buffered writes** — updates timestamp to prevent false stall detection.
- **Fix: halt flag cleanup** — removed from HALT_FLAGS on completion and early return.
- **Fix: output.finish() error logging** — MKV finalization errors now logged.

## 0.11.13 (2026-04-21)

### Fix: fast reads only in rip path
- All rip reads use 5s fast timeout. Binary search starts immediately on batch failure. Max 15s per bad sector instead of 10 minutes.

## 0.11.12 (2026-04-21)

### Halt + sector logging + light recovery
- **Stop actually stops** — halt flag interrupts Drive::read() recovery in <30s.
- **Sector-level logging** — read errors, retries, binary search, recovered/skipped sectors all logged.
- **Light recovery** — binary search single sectors: 3x5s (15s max) instead of 10-min full recovery.
- **On Read Error** — stop/skip setting wired to DiscStream.skip_errors.

## 0.11.11 (2026-04-20)

### Binary search recovery + UI fix
- Binary search error recovery for marginal disc sectors (libfreemkv 0.11.11).
- Verify UI: clean percentage formatting, simplified layout.

## 0.11.10 (2026-04-20)

### Verify rewrite + skip mode
- **Verify rewrite** — correct live counters (good/bad/slow), damage assessment (MB + seconds of video), stop flag.
- **On Read Error** setting — stop (default) or skip (zero-fill). Radio buttons in Settings.
- **Verify UI** — real-time stats, sector map bar, bad sector warnings with MB/seconds context.
- **UI fixes** — _verify/_move filtered from device tabs, verify shows Stop button and verifying status, no Eject during active operations.

## 0.11.9 (2026-04-20)

### Fast verify + UI fixes
- Verify uses fast reads (5s timeout) — bad sectors detected in seconds not minutes.
- Fixed: _verify/_move no longer show as device tabs.
- Fixed: Verify shows Stop button, hides Rip/Eject, status shows verifying.

## 0.11.8 (2026-04-20)

### Disc verify
- **Verify button** on Ripper page — sector-by-sector disc health check before ripping.
- **Sector map** — defrag-style visualization: green bar with red/yellow markers for bad/slow sectors.
- **Stats display** — Good/Slow/Recovered/Bad counts, readable percentage, chapter-mapped bad ranges.
- **Mover state separation** — mover no longer touches ripper state, fixes UI flickering.

## 0.11.7 (2026-04-19)

### TrueHD audio fix
- libfreemkv 0.11.7: TrueHD parser rewrite — 12-bit length mask, AC-3 frame skipping, cross-PES buffering. Zero decode errors.

## 0.11.6 (2026-04-18)

### TrueHD fix + History revamp
- **TrueHD audio fix** — libfreemkv 0.11.6 strips BD-TS access unit headers. Fixes corrupt TrueHD/Atmos audio in ripped MKVs.
- **History page** — card layout with poster, title, format badge, date, rip stats (size, speed, elapsed). Expandable log per rip.

## 0.11.5 (2026-04-18)

### MKV container fixes
- **MKV title tag** — writes TMDB/disc title instead of playlist filename.
- All libfreemkv 0.11.5 MKV fixes: timestamps normalized to 0, correct frame rate, HDR colour metadata, chapters (BD + DVD), proper default track disposition.
- Rips now play correctly in Jellyfin with chapters, HDR tone mapping, and correct audio auto-selection.

## 0.11.4 (2026-04-18)

### Webhooks
- **Configurable webhook URLs** — add/remove URLs in Settings. POST JSON payload on rip complete and move complete.
- **Rich payload** — title, year, format, poster, duration, codecs, size, speed, elapsed time, output path.
- **move_complete event** — fires after file is moved to final destination (NAS/library).
- Works with Discord, Jellyfin, n8n, Zapier, or any HTTP endpoint.

## 0.11.3 (2026-04-18)

### Unified versioning
- All freemkv repos now share the same version number.
- Updated libfreemkv dependency to 0.11.

## 0.11.2 (2026-04-18)

### Smoother speed/ETA display
- **95/5 EMA smoothing** — speed and ETA no longer jump every second. Each 1-second sample contributes only 5% to the displayed value. Sustained changes take ~20 seconds to reflect.

### Fixes
- **Move queue cleanup** — System page Move Queue now clears automatically when move completes instead of showing stale entries.

## 0.11.1 (2026-04-18)

### Mover progress bar
- **Live move progress on System page** — Move Queue shows animated progress bar with percentage, speed (MB/s), and ETA while files are being moved to their destination. Updates every second via SSE.

### Ripper page simplified
- **Scanning → Ripping → Done** — removed Verified/Moving steps from ripper. Moving is a background system concern, not shown on the ripper page.

### Local time in logs
- **Browser-local timestamps** — device logs and system logs display times in the user's local timezone instead of UTC.

## 0.11.0 (2026-04-18)

### Dual-layer disc fix
- **UDF extent allocation** — read actual UDF allocation descriptors instead of assuming contiguous m2ts files. Fixes truncated rips (~37%) on all dual-layer UHD/BD discs.
- **Read error propagation** — SCSI read errors surface as errors instead of silent EOF.

### Drive session persistence
- **Single drive session** — scan and rip share one Drive instance. No double-open, no double-init, no riplock from re-initialization.
- **DriveSession** — persists across scan → rip transitions. Survives eject/stop for clean state management.

### Marker-based mover
- **`.done` marker** — rip writes JSON marker on completion. Mover scans staging directories for markers instead of relying on in-memory state. Survives container restart, stop button, eject.
- **Move progress** — custom copy loop logs progress every 10 seconds (GB, %, MB/s) to system log.
- **Move queue UI** — system page shows pending moves from staging markers.

### UI improvements
- **Duration + codecs** — now-playing card shows movie length and primary video/audio codec.
- **No format badge during identify** — UHD/BD badge only appears after full scan confirms format.
- **Instantaneous speed** — EMA-smoothed (80/20) instead of lifetime average. Shows real throughput.
- **Adaptive speed units** — MB/s above 1, KB/s below.
- **ETA capped** — blank when over 99 hours instead of millions.
- **No duplicate checkmarks** — step indicators show icon only, no trailing text.
- **Eject clears log** — fresh log for next disc.
- **History** — only completed rips recorded, no duplicates from mover.

### Fast disc identification
- **Disc::identify()** — 3-second scan (UDF only) for disc name + TMDB poster. Full scan runs separately.
- **TMDB before rip** — user sees title + poster immediately, full scan runs in background.

## 0.10.0 (2026-04-16)

### Engine rewrite for libfreemkv 0.10.4
- **PES pipeline** — replaced deleted IOStream/MkvStream API with current input()/output() PES pipeline
- **DVD + BD + UHD** — full support for all disc types via unified DiscStream
- **CSS decryption** — DVD rips auto-decrypt through libfreemkv's CSS key hierarchy

### Output format settings
- **Output format** — choose MKV, M2TS, ISO, or Network from web UI settings
- **Network output** — stream rips directly to a remote server (no local disk needed)
- **ISO output** — full disc image with AACS/CSS decryption via Disc::copy()

### Production hardening
- **Graceful shutdown** — SIGTERM/SIGINT handler, stops poll loop, unlocks trays
- **Panic recovery** — rip threads wrapped in catch_unwind, errors reported to UI
- **No unwrap() in locks** — RwLock/Mutex errors handled gracefully throughout
- **File safety** — history recorded before file moves, destination verified first
- **on_insert "identify"** — scan and display disc info without ripping

### Cleanup
- **Shared util module** — deduplicated date formatting from 3 files
- **Mover handles all formats** — moves .mkv, .m2ts, .iso files (was .mkv only)

## 0.9.2 (2026-04-15)

- **libfreemkv 0.9** dependency update

## 0.9.0 (2026-04-14)

### Settings + polish
- **KEYDB URL setting** — configure update URL from web UI Settings page
- **Settings page polish** — larger checkboxes, breathing room, readable toggles
- **Version in footer**
- **Honest README** — documents KEYDB setup requirement

### Platform
- **Rust 1.86 MSRV** pinned
- **Drop aarch64 release** — cross doesn't compile on Rust 1.86
- **Docker build fix** — upgrade Rust 1.82 → 1.86 for edition2024

### Fixes
- System page: KEYDB only, remove MakeMKV leftovers
- Web UI footer link corrected to freemkv/autorip
- Docker build context paths fixed
- Switch to crates.io dep for libfreemkv
- Use Drive objects directly, lock/unlock tray during rip
- Eject via libfreemkv DriveSession::eject()

## 0.1.0 (2026-04-12)

### Initial release

- **Automatic ripping** -- detect disc insertion via udev, rip unattended
- **Direct library API** -- uses libfreemkv directly, no subprocess or text parsing
- **Web UI** -- real-time SSE progress, drive cards, Now Playing, history
- **Light/dark mode** -- CSS-based theme toggle, matches Python autorip design
- **Settings** -- web-editable config with env var defaults + JSON overlay
- **TMDB integration** -- automatic title lookup, poster, year
- **File organization** -- staging -> Movies/Title (Year)/Title.mkv
- **History** -- JSON record of all rips with metadata
- **Webhooks** -- push notifications on rip complete/fail
- **Per-device logging** -- in-memory buffer + file logs
- **Docker** -- Dockerfile, docker-compose, udev rules, entrypoint
- **DVD + Blu-ray + 4K UHD** -- all formats via libfreemkv
