# Notes for `tests/resume_remux.rs`

## Deliberate coverage gap

`Disc::scan_image` and `run_mux` end-to-end need a real UDF ISO. Feeding
synthetic bytes into `scan_image` reliably fails (per the libfreemkv
library rules). The live test bed validates the full flow on a real
disc; the gap is also documented in `src/ripper/resume.rs`.

## `classify_resume_allows_out_of_title_damage_when_abort_on_lost_secs_is_zero`

Regression: abort_on_lost_secs==0 with whole-disc unreadable bytes must
still classify as Remux. Pre-fix, the coarse pre-filter would convert
the whole-disc bad-byte count to estimated lost-seconds and return
NotEligible whenever any unreadable bytes were present — even though
those sectors might be entirely outside the main title. The real
per-title check in `resume_remux` (run after `scan_image`) is the
authoritative gate.

## `classify_resume_pre_filter_boundary_is_strictly_greater_than`

Tight boundary check on the pre-filter's `lost_secs > abort_on_lost_secs`
gate. The two damage tests above use damage an order of magnitude past
the threshold, so a `/` → `%` mutant on `bad_bytes as f64 /
FALLBACK_BITRATE_BYTES_PER_SEC` still lands on the reject side by
accident (both a correct ~2.4x-over lost-secs value and a
`%`-corrupted one exceed a 1-second threshold). This test pins the
exact arithmetic instead: `FALLBACK_BITRATE_BYTES_PER_SEC` is
8_250_000.0 bytes/sec, so at a 10-second threshold the boundary is
exactly 82_500_000 bytes. One byte under must classify as Remux
(deferred to the real per-title check); exactly at the threshold must
ALSO defer (`>`, not `>=` — the code comment establishes the gate is
strictly-greater); one byte over must reject. A `%` in place of `/`
turns 82_500_001 % 8_250_000 == 1, which is nowhere near 10 and would
wrongly classify as Remux; a `*` in place of `/` turns even
82_499_999 bytes into an astronomically large "lost_secs" and would
wrongly reject. Either mutant flips one of the three assertions in the
test.

## `cold_resume_of_a_boxset_variant_dir_uses_the_file_basename_not_the_dir_name`

Cold resume must hand `resume_remux` a FILE basename, not the staging
DIRECTORY name.

`rip_disc` documents the split explicitly (`src/ripper/mod.rs`, where
`filename` is built): the staging DIR carries the `_2` disc suffix that
separates the discs of a boxset, but the FILES inside it are named from
the plain title with no suffix, because `delete_partial_output` looks
for `<dir>/<display_name>.<ext>` and the mover derives the delivered
filename from the staged one.

`classify_resume` was taking `hint.dir.file_name()` — the suffixed
directory name — so on a boxset variant dir it looked for the partial
under the wrong name, left it in place, and muxed a SECOND file next to
it. Both then carry a `.done` hand-off and the mover delivers both.
