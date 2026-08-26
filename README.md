[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/freemkv/autorip/actions/workflows/ci.yml/badge.svg)](https://github.com/freemkv/autorip/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/freemkv/autorip/branch/dev/graph/badge.svg)](https://codecov.io/gh/freemkv/autorip)
[![Latest Release](https://img.shields.io/github/v/release/freemkv/autorip?label=latest&color=brightgreen)](https://github.com/freemkv/autorip/releases/latest)

# freemkv-autorip

Automatic disc ripper. Insert a disc, get an MKV.

Uses [libfreemkv](https://github.com/freemkv/libfreemkv) directly -- no subprocess, no text parsing.
Works with DVD, Blu-ray, and 4K UHD discs. DVDs (CSS) work out of the box; Blu-ray and UHD (AACS) require a `keydb.cfg`.

## Quick Start

### Docker (recommended)

```bash
curl -O https://raw.githubusercontent.com/freemkv/autorip/main/docker-compose.example.yml
# Edit the volume mounts and device passthrough to match your host
docker-compose up -d
```

Open http://localhost:8080 and set the TMDB key, library paths and rip
options in Settings. They are config fields, not environment variables.

**First-time setup:** DVDs (CSS) work out of the box — no key setup needed. For Blu-ray and UHD (AACS) discs, go to Settings and enter a KEYDB Update URL; autorip will fetch and refresh `keydb.cfg` automatically. TMDB API key is optional (enables automatic title/poster lookup).

### Bare binary (no Docker)

Prefer not to run a container? Download a single static binary and run it
as a systemd service. See [INSTALL.md](INSTALL.md) for the bare-metal
setup — drive permissions (add the service user to the `cdrom` group or a
udev rule instead of `--privileged`), a sample systemd unit, and local
path configuration (the in-container NFS auto-mount is Docker-only).

```bash
curl -sLO https://github.com/freemkv/autorip/releases/latest/download/autorip-x86_64-linux
chmod +x autorip-x86_64-linux && sudo mv autorip-x86_64-linux /usr/local/bin/autorip
```

### Build from source

```bash
# Clone both repos side-by-side (autorip depends on ../libfreemkv)
git clone https://github.com/freemkv/libfreemkv
git clone https://github.com/freemkv/autorip
cd autorip
cargo build --release
```

## Features

- **Automatic** -- detects disc insertion via udev, rips unattended
- **Web UI** -- real-time progress, settings, history, TMDB metadata
- **DVD + Blu-ray + 4K UHD** -- all formats, all codecs, AACS + CSS decryption
- **Multi-pass recovery** -- damaged discs rip via `disc → ISO → patch → MKV` with a ddrescue-format mapfile; retry only bad ranges, not the whole disc
- **Bad-range visualization** -- progress bar overlays red ticks at unreadable regions; collapsible list shows LBA / sector count / ms of video lost / chapter
- **TMDB integration** -- automatic title lookup, poster, year, organized output
- **File organization** -- Movies/Title (Year)/Title (Year).mkv; TV/Show (Year)/Season NN/Show S{NN}E{MM} - Name.mkv
- **Webhooks** -- push notifications on rip complete/fail
- **Docker** -- single container with udev, cron, web UI

## Configuration

Almost everything is configured in the **web UI** (Settings) or by editing
`config.json` in `AUTORIP_DIR` — *not* by environment variable. Output and
library paths, the TMDB key, `min_length`, `main_feature`, `auto_eject`,
`on_insert`, `on_read_error`, `max_retries` and `keep_iso` are all config
fields; setting them in the environment has no effect.

Only these environment variables are read:

| Variable | Default | Description |
|----------|---------|-------------|
| `AUTORIP_DIR` | `/config` | Config, logs and state directory |
| `PORT` | `8080` | Web UI port |
| `AUTORIP_LOG_LEVEL` | | Log verbosity override |
| `AUTORIP_SKIP_DISKCHECK` | | Skip the startup free-space check |
| `RIP_USER` | | User to drop privileges to |
| `NFS_HOST`, `NFS_EXPORT`, `NFS_MOUNTPOINT`, `NFS_OPTS` | | Optional NFS mount for final output |

## Rip flow

**Direct (`MAX_RETRIES=0`)** — fastest, no ISO intermediate:

```
disc  →  decrypt  →  demux  →  codec parse  →  MKV
```

**Multi-pass (`MAX_RETRIES>=1`)** — damaged-disc recovery:

```
disc  →  ISO + mapfile       (pass 1: fast sweep, skip-forward on failure)
      →  ISO + mapfile'      (pass 2..N: retry bad ranges with full drive recovery)
drive closed
ISO   →  decrypt  →  demux  →  codec  →  MKV
```

Pass 1 uses 64 KB ECC-aligned reads with exponential skip-forward on block failure (ddrescue algorithm). Each retry patches good bytes into the existing ISO at exact offsets; the mapfile is ddrescue-format plain text, flushed per-block so a crash resumes cleanly. Final MKV mux reads from local ISO — no drive involvement.

Trade-off: multi-pass uses ~2× peak disk (ISO + MKV both present during mux) and adds ~2-3 min for the mux stage. Direct mode is strictly faster when retry isn't needed.

## Deployment notes

**Use docker-compose, not Docker Swarm.** Swarm mode can't grant the `--privileged` + raw device access that SCSI ioctls on `/dev/sg*` require (cgroup devices controller blocks the open even with `CAP_SYS_RAWIO` + `SecurityContext` overrides). Compose works; a minimal example is in [`docker-compose.example.yml`](docker-compose.example.yml).

If you're running behind a reverse proxy (Caddy/Traefik/nginx) on an existing Docker network, override the default network so autorip joins the existing bridge rather than auto-creating a stack-local one. The example compose file includes a commented snippet.

## Architecture

```
libfreemkv (Rust library)
    └── freemkv-autorip (this binary)
        ├── Drive watcher (udev + polling)
        ├── Rip engine (direct library API)
        │   ├── Direct path: DiscStream(drive) → MKV
        │   └── Multi-pass: freemkv_engine::sweep + freemkv_engine::patch → ISO → DiscStream(ISO) → MKV
        ├── File organizer (TMDB → Movies/Title/)
        ├── Web dashboard (embedded HTTP + SSE)
        └── Webhooks
```

## API Reference

All endpoints are served on port 8080 (configurable via `PORT`). The web UI is a thin client over these same endpoints -- full programmatic control is possible.

`{device}` is the SCSI device name (e.g. `sg5`).

### General

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` or `/index.html` | Serve dashboard HTML |
| GET | `/api/state` | Current state for all devices (JSON) |
| GET | `/api/version` | `{"version":"X.Y.Z"}` |
| GET | `/api/settings` | Config as JSON, with secrets redacted and webhook URLs masked |
| POST | `/api/settings` | Partial JSON merge of config fields |
| GET | `/api/system` | Move/mux queues and errors, truncation count, syslog, debug flag |
| GET | `/events` | SSE stream pushing same JSON as `/api/state` every 1s |

### History & Logs

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/logs/{device}` | Device log, plain text (up to the 500-line in-memory ring) |
| GET | `/api/debug[?n=&level=&device=&q=]` | Filtered JSONL debug events |

### Device Actions

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/scan/{device}` | Scan disc metadata |
| POST | `/api/rip/{device}` | Start a full rip |
| POST | `/api/stop/{device}` | Stop rip/verify, wipe staging, reset to idle |
| POST | `/api/eject/{device}` | Eject disc tray, reset state |
| POST | `/api/update-keydb` | Download KEYDB.cfg from configured URL |

## Getting a debug log (for bug reports)

1. **Turn on debug logging** (no restart): `curl -X POST http://<host>:8080/api/debug -d '{"enabled":true}'`
   (or the Debug toggle in the web UI). This raises both autorip and the rip
   library to debug, including per-stage phase markers and heartbeats.
2. **Reproduce** the issue (insert the disc / start the rip).
3. **Collect the log** — easiest is the stable JSONL file:
   ```bash
   docker cp autorip:/config/logs/autorip.jsonl ./autorip-debug.jsonl
   ```
   (or `curl 'http://<host>:8080/api/debug?n=5000' > autorip-debug.log`, or grab
   `{AUTORIP_DIR}/logs/rips/<device>_<timestamp>.log` for a single rip's story).
4. **Attach** it to your report. If a rip or scan hangs, the watchdog logs
   `stalled Ns, last phase=…` every 15 s and the last `phase=… "alive"` heartbeat
   names the loop + position. **No key material, bearer tokens, or keydb
   credentials are ever written to logs** — secrets are redacted at every sink.

To pin a level for a whole session, set `AUTORIP_LOG_LEVEL=autorip=debug,libfreemkv=debug`
in the compose file (this overrides and disables the `/api/debug` toggle).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT -- see [LICENSE](LICENSE).
