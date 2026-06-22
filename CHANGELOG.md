# Changelog

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

