# Changelog

## [1.1.0-beta.1] — UNRELEASED

Inherits libfreemkv 1.1.0-beta.1, including the **DVD movie-not-menu** fix.

### Fixed

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

