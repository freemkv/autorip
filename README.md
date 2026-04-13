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
| `PORT` | `8080` | Web UI port |

## Architecture

```
libfreemkv (Rust library)
    └── freemkv-autorip (this binary)
        ├── Drive watcher (udev + polling)
        ├── Rip engine (direct library API)
        ├── File organizer (TMDB → Movies/Title/)
        ├── Web dashboard (embedded HTTP + SSE)
        └── Webhooks
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

AGPL-3.0 -- see [LICENSE](LICENSE).
