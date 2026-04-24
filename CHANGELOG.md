# Changelog

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
