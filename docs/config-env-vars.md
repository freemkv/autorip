# `Config` bootstrap env vars and the pre-0.25.7 deprecation

**Bootstrap-only env vars** (v0.25.7 cleanup, "no dupes" rule): only these
env vars influence `Config` — everything else is read from `settings.json`
(or, on first boot, the hardcoded `Config::default` values):

- `PORT` — web bind port. Can't change after the server is listening, so
  it must be set before the daemon starts.
- `AUTORIP_DIR` — where `settings.json` itself lives. Chicken-and-egg
  with everything else.
- `AUTORIP_LOG_LEVEL` (read inside `observe::init`, not here) — tracing
  filter is built before web is up.
- `RIP_USER`, `NFS_*` (read inside `autorip --bootstrap` in `main.rs`,
  not here) — mount/user setup runs before the daemon starts.

Operator-facing knobs (AUTO_EJECT, MAX_RETRIES, KEEP_ISO, MIN_LENGTH,
MAIN_FEATURE, OUTPUT_FORMAT, NETWORK_TARGET, ON_READ_ERROR,
ABORT_ON_LOST_SECS, MOVIE_DIR / TV_DIR / STAGING_DIR / OUTPUT_DIR,
TMDB_API_KEY, KEYDB_*, the new FREEMKV_THREADS / LOG_RETENTION_DAYS): all
UI now, no env-var reads, no duplication. Pre-0.25.7 deployments that set
these in docker-compose.yml will see the env values silently ignored —
operators must set them via the Settings page.
