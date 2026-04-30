# freemkv CLI — Rules

## i18n only — no hardcoded English

All user-facing text comes from `strings.rs` (locale JSON files). Never hardcode English strings in Rust code.

- Use `strings::get("key")` for static strings.
- Use `strings::fmt("key", &[("var", "value")])` for parameterized strings.
- If a string key doesn't exist, add it to `locales/en.json` (and `locales/es.json`).
- Error display: `strings::get(&format!("error.E{}", err.code()))` with fallback to `err.to_string()`.

## Architecture

- **CLI is dumb.** All logic lives in libfreemkv. CLI only handles I/O, display, and flag parsing.
- **PES pipeline.** `pipe()` uses `input()` / `output()` — PES frames flow through.
- **disc.copy() for ISO.** `disc_to_iso()` calls `Disc::copy()`, not a stream.
- **Multipass: one invocation = one pass.** `--multipass` enables mapfile
  read/write. Same command for all passes — library auto-detects from mapfile:
  ```
  freemkv disc:// iso://out.iso --multipass        # Pass 1: sweep (no mapfile)
  freemkv disc:// iso://out.iso --multipass        # Pass 2+: patch (mapfile with bad ranges)
  freemkv disc:// iso://out.iso --multipass        # Done (mapfile clean → exit)
  freemkv iso://out.iso mkv://Movie.mkv            # Mux
  ```
  Also supports `disc:// null:// --multipass` for algorithm testing without disk space.
  No retry loop in the CLI or the lib. Autorip orchestrates its own loop.
- **No process::exit in pipe.** Functions return bool/Result. Only `main()` exits.
- **Progress is a CLI concern.** Library emits `PassProgress` via trait. CLI formats display.
- **Progress display**: `GB/GB (%)  speed  ETA  % readable` — smart unit scaling (B/s, KB/s, MB/s, stalled), instantaneous speed (windowed), and `% readable` = good/(good+bad).

## Tracing

Set `RUST_LOG=warn` (or `info`, `debug`, `trace`) to enable structured logging. The CLI initializes `tracing_subscriber::fmt` only when `RUST_LOG` is set.

## Public repo rules

- **No internal docs.** Audit reports, test plans, roadmaps, TODOs go in freemkv-private, never here.
- **No Co-Authored-By** in commit messages. One contributor: MattJackson.
- **No private references.** No Gitea URLs, no /data/code paths, no internal IPs in code (examples in docs are fine).
