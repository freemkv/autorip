# Changelog

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
