# log.rs — relocated rationale

Design notes moved out of `src/log.rs` comments to stay under the
comment-guard's per-block caps. Each is pointed to by a short `//` in the
source.

## sanitize_device invariant

Enforced at the single construction point (`device_log_path`) so no
caller — web routes, SCSI enumeration, or the literal `"system"` syslog
channel, or any future caller — can write or rename outside `logs/` via a
`/`, `\`, or `..` in the device name. Reachable callers today are all
well-behaved (web gates on `is_valid_device_name`), so this exists as a
hard invariant, not a fix for a live exploit.

## forget_device: why entries accumulate without it

Distinct device strings — e.g. enumeration churn, or paths that change
across reconnects — would each get their own `LOGS` entry. Without
`forget_device` being called on hot-unplug, those entries would
accumulate as dead weight in the map for the lifetime of the container,
since there's no eject/scan boundary for a hot-unplugged drive to trigger
`archive_device_log`.

## ENV_LOCK: incident history

Cargo runs tests in parallel threads within ONE process, so any test that
re-points `AUTORIP_DIR` races every other test that resolves a log path.
`archive_device_log_moves_to_rips_dir` failed intermittently for exactly
that reason: another test swapped the dir between its `device_log` write
and the `exists()` assertion on the resolved path, so the path pointed
into the other test's tempdir.

`ENV_LOCK` lives at crate scope, NOT inside `log::tests`, because the
racing writers are in other modules (e.g. `ripper::resume`). EVERY test
that sets `AUTORIP_DIR` must hold this guard for the whole test.

Acquire it through `env_guard`, never directly: serializing the writers is
only half the problem. Nothing used to RESTORE `AUTORIP_DIR`, and most
tests delete their tempdir at the end, so the process-wide var was left
pointing at a deleted directory for every later test in the run. Unguarded
readers (any test that reaches `device_log`/`syslog` transitively) then
resolved paths into it — which is what made
`find_iso_tests::pairs_despite_extra_entries` and
`resume_lock_and_fsync_tests::fsync_failure_below_limit_preserves_and_bumps`
fail intermittently after the write race itself was fixed.
