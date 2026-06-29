# Changelog

## [1.2.0] — 2026-06-28

### Added

- **Version now carries the build's git short hash** — `--version`, the UI
  footer, `/api/version`, and the startup log report e.g. `1.2.0 (g2014a41)`
  (the same shape libfreemkv stamps into MKVs), so a running build — including a
  hand-deployed test build — is always identifiable instead of hiding behind a
  bare package version.

### Changed

- **The mux never aborts on mux-time loss.** A disc that swept and patched is
  always handed to the muxer, and the muxer always delivers. Earlier versions
  ran a *second* loss check after muxing and could quarantine an
  already-finished file when demux/decrypt loss exceeded `abort_on_lost_secs` —
  but with libfreemkv 1.2.0 that loss is concealed into a decode-clean file
  (NULL-TS fill + drop-to-keyframe) and merely tallied, so failing the disc at
  that point only stranded a good rip. `abort_on_lost_secs` now governs the
  **rip** phase alone (unreadable sectors, before the mux); the mux is never
  gated by it. (The drive/pipeline status split into explicit DeviceStage /
  PipelineStage models is staged for a later 1.2.x — see the in-code
  `TODO(1.2.0)`.)
- **One key-resolution path.** autorip resolves AACS keys through
  `Disc::inputs()` (libfreemkv) instead of its own duplicate `key_files()` /
  `volume_id()` readers, so the service and the CLI capture a disc's inputs
  identically. The stale mapfile-VID read was dropped in favour of the disc's
  own Volume ID.

### Fixed

- **Resume re-injects the mapfile VID for an uncatalogued-disc ISO.** Resuming a
  mux from a swept ISO whose disc wasn't in the catalogue now reconstructs the
  Volume ID from the mapfile so AACS resolution has the input it needs, instead
  of failing the resume.
- **A failed mux shows the real reason.** When the mux worker genuinely fails,
  the System tab surfaces the actual reason from the staging marker instead of a
  generic "mux worker dispatch did not complete (see _mux device log)", so the
  operator doesn't have to read device logs.

- **The dashboard is no longer cached across releases.** The single-page UI
  (HTML + inline JS) was served with no `Cache-Control`, so browsers kept
  running the *old* page — old client-side validation, old error handling, old
  everything — after a new autorip version deployed. It's now served `no-store`,
  so a release takes effect on the next page load instead of requiring a manual
  hard-refresh.
- **Retry passes no longer show the previous pass's progress.** When pass 1
  ended and a retry pass began, the per-pass bar stayed frozen at "pass 1/N ·
  99% · ETA 0s" through the 30 s drive-settle (until the first retry read). The
  new pass now flips to "pass N · retrying · 0%" immediately, before the settle.
  The cumulative total bar is unaffected.
- **Quieter retry-pass logs.** Dropped `bytes_unreadable=…` from the per-pass
  log lines — it is always `0` until the final pass promotes pending sectors, so
  it was pure noise mid-rip.

## [1.1.0]

Inherits libfreemkv 1.1.0, including the **post-read decrypt-verify gate**
(undecryptable units are caught during the rip and re-read) and the
**DVD movie-not-menu** fix.

### Added

- **"Accept damage & deliver" — operator off-ramp on a loss-abort.** When a rip
  aborts because main-movie loss exceeds the threshold, the card now offers a
  one-click Accept: the *existing* swept ISO is re-muxed and delivered as-is (the
  loss gate is bypassed for that one delivery), with **no re-rip**. "Run another
  pass" is the Resume button (continues Pass N from the mapfile, recovering only
  the bad core). Pairs with the resume fixes below.
- **Live patch progress is no longer a black box.** During a retry pass the
  bad-range drilldown now lists the *located* Maybe ranges (LBA + sectors +
  chapter) being worked, instead of staying empty until a sector is terminally
  given up on.
- **ISO output now requires a 100% byte-complete image.** The per-title
  "Max Acceptable Main Movie Loss" tolerance is a muxed-output (MKV / M2TS /
  Network) setting and is now ignored for an ISO rip (forced to 0): a value left
  over from a previous MKV rip can no longer silently let an ISO accept loss. The
  Settings UI already hid the field for ISO; the abort logic now matches it.

### Changed

- **Rip progress is now two states — Good and Maybe, never a third.** The live
  card no longer shows `Feature` / `Cosmetic` / `Moderate` / `Serious` /
  `No chance` / `Lost` pills. **Good** = whole-disc bytes read *and* verify-clean;
  **Maybe** = every byte not yet good (pending, NonTrimmed, currently-unreadable,
  undecryptable — all folded together). Nothing is called "lost" mid-rip: a later
  pass, or a freshly power-cycled drive, still recovers it, so there is no live
  terminal bucket. "Bad" is a **verdict**, decided once after the final pass
  (main-feature lost time vs `abort_on_lost_secs`), not a pill. The Maybe pill's
  bytes are whole-disc but its **time is the main-feature lost time** at ms
  precision — `Maybe 990 MB · 0:00` means 990 MB pending with zero movie impact
  (passes), while `Maybe 12 KB · ~1 ms` is a few movie sectors (fails a 0
  threshold). A handful of sectors reads as `~1 ms`, never `0`.
- **Abort-on-loss is resumable, never terminal — and you can accept the loss.**
  A rip that aborts because main-movie loss exceeds the threshold keeps its
  complete swept ISO and stays *resumable indefinitely* (the old "exhaust N
  attempts → terminal `.failed` → re-rip the whole disc from scratch" loop is
  gone — a deterministic media defect won't fix itself, so re-sweeping 50 GB to
  reach the same bad sector was pure waste). The abort card now offers
  **Accept damage & deliver**: a one-shot operator override that re-muxes the
  *existing* ISO and delivers the movie as-is, missing only the unreadable
  section — for an imperceptible loss (a few frames / ~1 ms) that's the right
  call, and it's yours to make. Operator cancel and durability/structural-mux
  failures stay terminal.
- **Live patch progress is no longer a black box.** During retry passes the
  drilldown now lists the *located* ranges being worked (LBA, sectors, chapter),
  so "pass 3, no movement" shows exactly which bad region the drive is grinding.
- **"Max Acceptable Main Movie Loss"** moved under the MKV/muxed-output settings
  and shown in seconds.

### Fixed

- **A loss-abort no longer destroys the swept ISO.** Previously a rip that
  aborted on main-movie loss was retried a few times and then promoted to a
  terminal `.failed`, which locked out resume — and the next trigger re-swept the
  whole disc from scratch, **overwriting the complete 50+ GB ISO** and discarding
  all recovery progress. A loss-abort is deterministic media damage, so it now
  stays **resumable indefinitely** (never auto-promoted to `.failed` by attempt
  count) and the unattended path **refuses to re-sweep over** a loss-aborted ISO.
  The operator resolves it: **Accept** (deliver as-is) or **Resume** (run another
  recovery pass on the bad core). The complete ISO is never thrown away.
- **The live "Maybe" pill now shows honest main-movie time at risk.** It counts
  in-feature *pending* sectors (not just terminally-unreadable ones), so a rip in
  progress reads `Maybe N · ~Xms` when the movie is affected and `Maybe N · 0:00`
  when the pending bytes are out-of-feature — instead of a premature `0:00` /
  "Feature clean" while a bad range was still unresolved. The single-source
  `RipProgress` computation replaces three drifting copies.
- **Clearer abort message.** A sub-second main-movie loss now reads e.g. "1 ms"
  instead of a confusing "0.00s", and a zero threshold reads "perfect rip
  required" instead of "threshold 0s".
- **Resume can no longer race an in-flight mux.** A staging directory owned by
  the mux worker (sweep handed off, or mux in progress) is no longer offered for
  sweep-resume, so a manually triggered resume can't overwrite the staged ISO
  while the muxer is still reading it.
- keydb save writes directly to the service path (removed the
  validate-then-relocate workaround).

## [1.0.0-rc.5.1]

### Fixed

- **Mover no longer warns on every poll for an in-progress staging dir.**
  The mover emitted a spurious ".done marker" warning on every 10-second
  poll when it encountered a staging directory that was still being
  written to. The warning is now suppressed for directories actively in
  use; it fires only when a directory is genuinely stranded (i.e. present
  with no corresponding active rip after a restart or crash).

## [1.0.0-rc.4.2]

Windows durability fixes.

### Fixed

- **Windows re-mux loop.** The post-mux durability gate opened the
  finished output read-only, so on Windows the flush (`FlushFileBuffers`)
  was rejected with `ERROR_ACCESS_DENIED`; the `.done` marker was never
  written and auto-resume re-muxed the same disc indefinitely. The gate
  now opens the output read+write so the flush succeeds on every platform.
- **Windows free-space preflight.** `staging_free_bytes` was a no-op on
  Windows, so the staging out-of-space check never ran; it now reads free
  space via `GetDiskFreeSpaceExW`.
- **Windows log noise.** Directory fsync (a POSIX concept) is now a no-op
  on Windows instead of failing to open the directory and warning on every
  marker and mapfile write.

## [1.0.0-rc.4] — UNRELEASED

Plain-English failure reasons, accurate loss accounting on done cards,
and a round of resume/abort and hot-unplug correctness fixes.

### Fixed

- **No more re-mux loop.** A DVD that hit a post-mux loss abort could be
  re-muxed indefinitely; the `.failed` marker is now terminal in the mux
  worker, so an aborted disc stays aborted.
- **Readable failure reasons.** Mux and scan read errors, AACS handshake
  failures, and CSS crack failures are now reported as English text with
  the specific cause (and the failing keydb path on a key error) instead
  of a bare `E`-code. Pass 1 exhaustion and non-SCSI pass errors are
  likewise labeled.
- **Accurate loss accounting.** Done cards report combined sweep + mux
  (demux-skip) loss; single-pass done cards no longer show `0s`
  main-movie loss or under-classify damage severity, and bad-range
  drilldowns are populated. Fresh and resumed multipass rips gate on
  post-mux demux-skip loss, so a disc with decrypt/demux loss can't be
  accepted as perfect. `NaN` loss is treated as an abort.
- **Resume correctness.** The resume path enforces the same
  abort-on-loss gate, honors `auto_eject` and the `iso` output format,
  carries title/metadata/codecs into the done card, and no longer leaks
  halt tokens.
- **Single-pass.** `abort_on_lost_secs` is now enforced in single-pass
  rips, the loss gate scales by bytes skipped rather than skip count,
  single-pass ISO output is rejected so abort scope matches multi-pass,
  and read-error truncation surfaces on `/api/state`.
- **ISO output.** `output_format=iso` now delivers a disc image instead
  of muxing an MKV.
- **Hot-unplug cleanup.** Title overrides, stop cooldowns, the device
  log ring, and first-seen tracking are evicted when a drive is
  unplugged.
- **Durability.** NFS `DirEntry` read errors are no longer silently
  dropped across staging, resume, and mover scans; staging basenames are
  unioned across NFS retries; mux header-phase failures are quarantined
  rather than silently swallowed; a poisoned config lock surfaces an
  error state instead of panicking; and the hand-off marker is never
  written empty.
- The raw `E5000` code prefix was dropped from the disk-space preflight
  message, and that preflight warns when it can't read free space.

## [1.0.0-rc.2]

Second release candidate for 1.0. Adds end-to-end DVD/CSS support and a bare-run
mode, on top of concurrency, durability, and web-handler hardening.

### Added

- **DVD/CSS support.** autorip rips and muxes CSS-protected DVDs end-to-end.
  AACS key resolution is skipped for DVDs; the CSS title key is recovered
  keylessly from the swept disc by libfreemkv's Stevenson attack. No
  `keydb.cfg` is required for DVDs.
- **Bare-run mode.** `autorip` (or `autorip serve`) runs the daemon directly
  with no container bootstrap, storing config under `~/.config/autorip`. Useful
  for the downloadable static binary on a bare Linux host. See `INSTALL.md` for
  install instructions, non-root drive access via the `cdrom` group or a udev
  rule, and a hardened systemd unit.
- **Static-binary releases.** Each tagged release attaches
  `autorip-x86_64-linux` and `autorip-aarch64-linux` static binaries with a
  `.sha256` checksum, alongside the existing Docker image.
- **Runtime debug-logging toggle.** `POST /api/debug {"enabled":true/false}`
  swaps the active tracing filter without a container restart, surfacing
  libfreemkv debug events (mux stalls, sector retries) in `docker logs`.
- **`.completed` restart guard.** The muxer checks for a `.completed` marker
  before re-processing a staging directory, so a container restart cannot
  trigger a duplicate mux on a disc that already finished successfully.

### Changed

- Built on libfreemkv 1.0.0-rc.2, inheriting correct DVD MPEG-2 muxing,
  HEVC/H.264/VC-1 param-set keyframe correctness, short-read rejection, and
  `BlockDuration` timescale fix. Output MKVs record `freemkv 1.0.0-rc.2` in
  their Writing-application field.
- `Config` implements a manual `Debug` that redacts `tmdb_api_key`,
  `keydb_url`, `keyserver_url`, and `keyserver_secret`, so diagnostic log
  output does not leak secrets.
- Staging-directory relocation at startup uses existence (`Path::exists`) to
  decide whether `/staging` is mounted, not a write probe. A transient NFS
  hiccup at container start no longer orphans an in-progress ISO by relocating
  staging to the config directory mid-rip.

### Fixed

- `POST /api/stop` during the mux phase no longer quarantines a resumable disc
  as `.failed`. Stop-versus-failure is now classified on typed error variants
  (`Halted`, `PipelineJoinTimeout`, `PipelineConsumerPanicked`) rather than
  error-message strings, so a routine operator stop keeps the disc resumable.
- Abort-on-loss after retries are exhausted now writes a `.failed` staging
  marker, preventing the muxer from retrying a disc that was deliberately
  abandoned due to unrecoverable data loss.
- The eject-then-clear-session sequence is now performed atomically under the
  device lock, eliminating a TOCTOU race where a disc insert between eject and
  state clear could produce a stale session.
- `/api/settings` POST validates string-enum fields (including `output_format`,
  `on_insert`, and `on_read_error`) and applies numeric clamps, rejecting
  malformed values before mutating the in-memory config.
- Mux staging-directory scan handles `DirEntry` I/O errors per-entry (logs and
  skips) instead of aborting the entire scan on a single unreadable entry.

### Security

- CSS disc/title keys inherited from libfreemkv are redacted in autorip's logs
  (logged as `<redacted>` with a 1-byte fingerprint).
- `settings.json` is persisted with owner-only (0600) permissions, since it
  may hold `keyserver_secret` and `tmdb_api_key`.

## [1.0.0-rc.1]

First release candidate for 1.0 — the first tagged 1.0 milestone of the rip
service (see "Pre-1.0 development" for the consolidated feature list).

## Pre-1.0 development

Versions 0.x were the development series leading up to 1.0. The highlights,
condensed:

- **Unattended ripping service.** Detect a disc on insert (udev), scan and
  identify it (TMDB lookup for title/poster/year), rip it, mux to MKV, and move
  the finished file to the library — all hands-off. Web dashboard with live
  SSE progress, per-device drive cards, settings UI, history, and webhooks.
- **Multipass orchestration.** Single-pass (direct disc→MKV) and multi-pass
  (disc→ISO→retry bad ranges→mux) rip modes, with an abort-on-loss threshold
  for the main feature and three-bucket Good/Maybe/Lost progress reporting.
- **Parallel pipeline.** Rip, mux, and move run as independent staged workers,
  so the drive frees the moment sweep+patch finish and the next disc can be
  ripped while the previous one muxes and moves — the killer unattended flow.
- **Resilient staging + move.** `.ripped`/`.done`/`.completed`/`.failed` markers
  make rips resumable across container restarts; format-aware post-copy
  validation (EBML/TS sync checks) catches a truncated copy without depending on
  NFS attribute freshness; an opt-in in-container NFS mount self-heals stale
  host mounts on restart.
- **Deployment.** Curated minimal `FROM scratch` Docker image plus a
  downloadable static binary with a bare-run mode (`autorip serve`,
  `~/.config/autorip`) and a hardened systemd unit. Auto-downloading keydb
  updater for AACS discs; DVDs (CSS) need no key file.
- **Security/UX hardening.** HTML-escaped dashboard output (stored-XSS fix),
  redacted secrets in `GET /api/settings` and `Debug`, SSRF guards on outbound
  targets, validated settings, and a cross-origin POST guard. Runtime
  debug-logging toggle. Release builds use thin LTO.

