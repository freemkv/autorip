[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](LICENSE)
[![CI](https://github.com/freemkv/autorip/actions/workflows/ci.yml/badge.svg)](https://github.com/freemkv/autorip/actions/workflows/ci.yml)

# freemkv-autorip

Automatic disc ripper. Insert a disc, get an MKV.

Uses [libfreemkv](https://github.com/freemkv/libfreemkv) directly -- no subprocess, no text parsing.
Works with DVD, Blu-ray, and 4K UHD discs.

## Quick Start

### Docker (recommended)

```bash
curl -O https://raw.githubusercontent.com/freemkv/autorip/main/docker-compose.example.yml
# Edit environment variables (TMDB_API_KEY, paths, etc.)
docker-compose up -d
```

Open http://localhost:8080

**First-time setup:** Go to Settings and enter your KEYDB Update URL to enable Blu-ray/UHD decryption. DVD ripping works out of the box. TMDB API key is optional (enables automatic title/poster lookup).

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
- **File organization** -- Movies/Title (Year)/Title.mkv
- **Webhooks** -- push notifications on rip complete/fail
- **Docker** -- single container with udev, cron, web UI

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `AUTORIP_DIR` | `/config` | Config/history/logs directory |
| `OUTPUT_DIR` | `/output` | Where finished MKVs go |
| `MOVIE_DIR` | | Organized movie library |
| `TV_DIR` | | Organized TV library |
| `STAGING_DIR` | `/staging` | Temporary rip directory |
| `TMDB_API_KEY` | | TMDB API key for metadata |
| `MIN_LENGTH` | `600` | Minimum title length (seconds) |
| `MAIN_FEATURE` | `true` | Rip longest title only |
| `AUTO_EJECT` | `true` | Eject after rip |
| `ON_INSERT` | `rip` | `nothing` / `identify` / `rip` |
| `ON_READ_ERROR` | `stop` | `stop` (abort on bad sector) or `skip` (zero-fill and continue) — direct mode only |
| `MAX_RETRIES` | `1` | Retry passes after the initial disc→ISO pass. `0` = single-pass direct disc→MKV (fastest, no retry). `1..=10` = multi-pass |
| `KEEP_ISO` | `false` | Preserve the intermediate ISO + mapfile in staging after MKV mux |
| `PORT` | `8080` | Web UI port |

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
        │   └── Multi-pass: Disc::copy + Disc::patch → ISO → DiscStream(ISO) → MKV
        ├── File organizer (TMDB → Movies/Title/)
        ├── Web dashboard (embedded HTTP + SSE)
        └── Webhooks
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

AGPL-3.0 -- see [LICENSE](LICENSE).
