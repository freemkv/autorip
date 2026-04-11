# freemkv-autorip

Automatic disc ripper. Insert a disc, get an MKV.

Uses [libfreemkv](https://github.com/freemkv/libfreemkv) directly — no subprocess, no text parsing.

## Features

- **Automatic** — detects disc insertion via udev, rips unattended
- **Web UI** — real-time progress, settings, history, TMDB metadata
- **DVD + Blu-ray + 4K UHD** — all formats, all codecs, AACS + CSS decryption
- **TMDB integration** — automatic title lookup, poster, year, organized output
- **File organization** — Movies/Title (Year)/Title.mkv
- **Webhooks** — push notifications on rip complete/fail
- **Docker** — single container with udev, cron, web UI

## Quick Start

```bash
docker-compose up -d
```

Open http://localhost:8080

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| AUTORIP_DIR | /config | Config/history/logs directory |
| OUTPUT_DIR | /output | Where finished MKVs go |
| MOVIE_DIR | | Organized movie library |
| TV_DIR | | Organized TV library |
| STAGING_DIR | /staging | Temporary rip directory |
| TMDB_API_KEY | | TMDB API key for metadata |
| MIN_LENGTH | 600 | Minimum title length (seconds) |
| MAIN_FEATURE | true | Rip longest title only |
| AUTO_EJECT | true | Eject after rip |
| ON_INSERT | rip | nothing / identify / rip |
| PORT | 8080 | Web UI port |

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

## License

AGPL-3.0-only
