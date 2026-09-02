# `default_autorip_dir` fallback order rationale

Resolve where autorip keeps all its state (settings.json, logs, keys,
staging, output). Identical logic on EVERY OS — no per-platform branches —
and always returns a REAL ABSOLUTE path the UI/logs can show verbatim.

Order:
1. `AUTORIP_DIR` — explicit override. The Docker image sets this to
   `/config` (its bind mount), so the container is handled here.
2. A writable `/config` — the container bind mount, for older Docker
   deployments that didn't set `AUTORIP_DIR`. On a fresh native Windows /
   macOS box this directory does not exist, so it is skipped — autorip
   never creates `C:\config` at the drive root.
3. A `config` folder NEXT TO the executable — the self-contained default
   for a downloaded binary. `current_exe()` is absolute on every OS, so
   this is a real absolute path (the download folder + `config`), never a
   relative `.\config`. Move the folder, the app's state moves with it.
4. Last resort: the absolute working directory + `config`.
