# Mover internals

Design rationale and incident history for `src/mover.rs`, moved out of
inline doc comments to keep those within the comment-guard's per-item cap.

## `MoveStateGuard`

The clear used to be the last statement of the per-directory loop body.
Every failure branch (`any_collision`, `any_size_mismatch`,
`any_post_copy_invalid`, `any_failed`) `continue`s past it, and the
progress bars had already been published by the copy's `on_progress`
callback — so a blocked delivery left "60%, ETA 1:23" on the System page
forever. An RAII guard is used rather than a clear on each branch so a
new early exit cannot reintroduce the leak.

## `clear_stale_dest_error`

The "partial copy could not be removed" row is the one row the mover
keys by destination rather than by staging dir, and it had no automatic
remover at all: `clear_error`'s only other call site is the staging
clean-teardown arm (keyed by the staging dir), and `prune_move_errors`
deliberately skips keys outside the staging root. Every rip has its own
destination filename, so a library mount with a persistent unlink fault
added one PERMANENT key per rip, unbounded, on a daemon nobody is
watching — the operator's clear button was the only way out.

A move that ends with validated bytes at `dest` is proof that whatever
the row described is gone, which is exactly the self-healing contract
`clear_move_error` already documents: dismiss a solved error and it
stays gone, while a still-real one re-records within a tick.

## `prune_move_errors`

`clear_error` runs at exactly one place — the clean-teardown arm of the
per-dir loop — so an entry can only be cleared by a *later pass over the
same dir*. Both hints the mover prints for a blocked teardown tell the
operator to `rm -rf` that staging dir by hand; the moment they do, the
dir stops appearing in the pass, no later tick ever revisits the key,
and the row it told them to fix is pinned on the System page until they
also find the clear button. The self-healing story only holds for dirs
that still exist.

`seen` is every staging child this pass listed. A key is dropped only if
it is a direct child of `staging_root` AND was absent from that listing:

* Keys outside the staging root are left alone. One `record_error` call
  site keys by DESTINATION path instead (the "partial copy could not be
  removed" row), which lives on the library mount — a different mount,
  with a different liveness story — and is not this function's business.
* The caller only reaches here after `read_dir(staging_root)` SUCCEEDED,
  so a dropped staging mount returns early and prunes nothing. That is
  the case where "the dir isn't there" must not mean "the problem went
  away": every row would vanish at once, precisely when the operator
  needs them.

## `same_head_and_tail`

Used by `dest_claim` and the collision guard to tell an idempotent
re-move (the dest IS this rip's output, copied on a prior tick whose
unlink failed) apart from a second disc (a boxset or a wrong title match
routed two DIFFERENT discs to the same path, and their muxes happen to
be the same byte length).

Fixed-size reads keep this O(1) — we never read the whole multi-GB file.
A false "same" would require two different discs to be byte-identical
in both their first and last 64 KiB AND identical in length; that is not
a realistic mux collision. On any read error this conservatively returns
`false` (treat as NOT confirmed identical, i.e. a real collision), so a
probe failure can never green-light clobbering a different file.

## `copy_counting`

This is what lets the mover show real progress: the move loop reads
`written` (bytes WE have pushed) instead of `stat()`-ing the
destination. On an NFS share under concurrent rip+mover I/O a dest stat
blocks for minutes or reads a stale `0`, which used to freeze the System
page move telemetry at 0% for the entire copy (pre-0.26.x bug this
replaces). Counting our own writes can't stall and can't go stale.

`std::fs::copy`'s kernel fast paths (`copy_file_range`/`sendfile`) don't
apply across filesystems, and staging→library is the only path that
reaches here — same-filesystem moves take the `rename(2)` fast path —
so a plain buffered loop loses no acceleration in practice.

## `copy_counting_cancellable`

The only reason this seam exists is testability: `crate::SHUTDOWN` is a
process-global `AtomicBool` that the mover, muxer and poll loop all
read, so a test that set it to drive the abort path would abort every
other mover test running in parallel in the same binary. The loop body
is the same code production runs; only the source of the abort bit
differs, and `copy_counting` is the one line that binds it to
`crate::SHUTDOWN`.

## `check_post_copy`

Replaces the v0.25.x `check_post_copy_sizes` helper, which used
`std::fs::metadata` directly on the dest immediately after cp closed —
that read could be served from the NFS attribute cache and produced
phantom SizeMismatch failures on an NFS share (the file was intact, the
stat lied). A v0.25.2 release-test rip hit this on a 58 GiB mkv that
had landed byte-for-byte.

## `mover_tick`

The move is injected so this is testable. It matters because the bug
was a property of WHEN the lock is released, not of the move itself:
`run()` used to pass the `RwLockReadGuard` straight into
`check_and_move`, so the guard lived for the whole pass — including a
cross-filesystem NFS copy of an 80 GB MKV. Any `POST /api/settings`
taking `cfg.write()` blocked for the duration, and std's RwLock is
write-preferring on Linux, so one queued writer then blocks new READERS
too: the entire unauthenticated web UI stopped answering for as long as
the copy took, triggered by any LAN client.

## `prune_stranded_warned`

Mirrors `prune_move_errors`: the set is inserted-into on every
stranded-dir warn but was never pruned, so it grew unbounded for the
daemon's lifetime (every pre-1.6.9 leftover / emptied dir the operator
eventually `rm -rf`s stayed pinned in memory forever). `seen` is every
staging child this pass listed; an entry is dropped once its dir no
longer appears — the operator removed it, so the one-time-warn dedup no
longer needs to remember it.

Reached only after `read_dir(staging_root)` SUCCEEDED (the caller
returns early otherwise), so a dropped staging mount prunes nothing —
the same liveness guarantee `prune_move_errors` relies on.

## `collect_ripped_files`

Don't `.filter_map(|e| e.ok())` away per-entry errors: on a cold-cache or
degraded NFS mount a single `DirEntry` I/O error can silently drop the only
`.mkv`. Each error is surfaced via `record_error` (keyed by the staging dir,
the same key the teardown arms use).

The `bool` half of the return is the one the staging dir's LIFE depends on.
A dropped entry is a file this pass cannot see — never planned, never
copied, never delivered — and the caller goes on to `remove_dir_all` the
directory once the entries it COULD see are accounted for. `false` here is
what stops that: an incomplete listing is not a completed move.

## `routing_media_type`

A disc that rips cleanly but gets no TMDB match writes `media_type: ""`
into its `.done` marker (the muxer hand-off's serde default). That empty
string is `Some("")` at the marker parse, so it slips past the
`unwrap_or("movie")` there and reaches `build_destination` /
`destination_root` as `""`, which matches neither the `"movie"` nor `"tv"`
arm and falls through to the `output_dir` ROOT — dumping the file at the
bare library root with no per-title folder (the Drive 4K UHD mis-file,
2026-07-22).

An ABSENT media_type already defaults to `"movie"` (both at the marker
parse and in `review.rs`); an EMPTY one must too, so an unmatched-but-titled
disc files as a movie under `movies/Title (Year)/`. A non-empty but
non-movie/tv value (e.g. `"collection"`) still falls through to the
fallback — only the empty case is coalesced. Sharing this one helper keeps
`build_destination` and `destination_root` in lock-step (guarded by
`destination_root_matches_build_destination_root`).

## `dest_claim`

The destination-domain answer to the question `dir_is_same_disc` asks in
the staging domain, and deliberately the SAME evidence the collision guard
uses further down: sizes must match and a head+tail content probe must
confirm it. That is what makes a retried move idempotent — a re-move of the
same disc re-claims the name it already wrote instead of walking the suffix
upward and littering the library with `Movie_2.mkv`, `Movie_3.mkv`.

The third state is the important one. A stat we could not perform is NOT
evidence of a different disc — a transient NFS ESTALE/EIO or a dropped
mount must not be answered by inventing a `_2` and quietly delivering
beside a file we never managed to look at. `Unknown` stops the variant
search dead; the caller keeps the base name and the collision guard defers
the move to a later tick exactly as it did before suffixes existed.

A zero-length dest is claimable: the collision guard already treats an
empty dest as no file at all and overwrites it, and diverging here would
strand every interrupted copy behind a permanent suffix bump.

## `resolve_media_root`

`Path::join` is deliberate: this joins a real base directory (in the move
path, a live filesystem root) with a subdir, so it must respect the host
FS — the integration tests drive genuine `create_dir_all` + rename against
tempdir roots on every CI OS. autorip only ever runs on Linux, where join
yields forward slashes, so production destinations are always Unix paths
regardless.

This is the core of the 2026-06 "Mercy" fix: previously the movie/tv
branches used `cfg.movie_dir` / `cfg.tv_dir` STANDALONE, so a relative
"movies" resolved against the container root `/` → `/movies` (the ephemeral
overlay) instead of under the NFS mount at output_dir.

## `destination_root`

Returned so the mover can validate the root is present + writable BEFORE
creating any subdir tree under it — the guard against silently writing an
80 GB rip into a container overlay when the NAS bind-mount has vanished.
Selection must stay in lock-step with `build_destination`; the join is the
"Mercy" fix — see `resolve_media_root`.

The two `is_empty()` guards in the body are documentation, not behaviour:
with an empty dir the arm would return `resolve_media_root(output_dir,
"")`, and that helper returns `output_dir` verbatim — the same string as
the fall-through. They exist to mirror `build_destination`, where the
guards ARE load-bearing (an empty dir there must not fabricate a per-title
/ Season 1 tree at the library root).

## `destination_root_for`

A `.iso` image uses `iso_dir` (resolved under `output_dir`) when it is
set; every other file uses the movie/tv/output root from
`destination_root`. Extension-aware because one `keep_iso` delivery files
the MKV under the movie/tv root AND its companion ISO under `iso_dir` —
two DISTINCT roots that must BOTH clear the Mercy-incident writability
guard before any bytes move.

## `validate_destination_root`

This is the code hardening for the 2026-06 "Mercy" incident: the
docker-compose lost its NAS bind-mount, so `/movies` resolved to the
container's writable overlay. The mover silently `create_dir_all`'d
`/movies/Mercy (2024)/` there and wrote ~80 GB into the ephemeral layer,
logging a relative-path "success". Requiring the root to pre-exist makes
a missing mount a hard, loud error — a mount point is provisioned
out-of-band (the bind target), never auto-created by the mover.

Crucially this does NOT create the root (that's the whole point); it only
probes. The per-title subdir under a confirmed-present root is still
created on demand by the caller.

## `check_configured_destinations`

Fail-loud-EARLY destination check, mirroring the CLI's
`preflight_validate` intent: validates every configured destination root
(movie/tv/output) that is non-empty, returning a `(root, reason)` for each
that is missing, not a directory, relative, or not writable. Used at
startup and after a settings save to warn the operator BEFORE a rip
finishes and the per-move guard blocks it.

Non-fatal by design: a mount can be transiently down at boot/save time,
and the per-move `validate_destination_root` gate is the hard stop that
preserves output in staging. This just surfaces the problem early.

## `move_file`

1. **Pre-flight**: if `dest` is a regular file with the same size as
   `src`, matching head/tail content, and passing `check_post_copy`, treat
   the move as already done (`Skipped`). This is the circuit breaker for
   the "cp succeeded but unlink failed" loop — on the next tick we
   re-detect the completed dest and don't re-copy 50+ GB across the
   network. A same-size dest with different content is a `Collision`; a
   same-size dest that fails post-copy validation falls through to a real
   copy.
2. **Atomic path**: try `rename(2)`. On the same filesystem this is
   instant and unlinks src for free.
3. **Cross-fs / fallback**: `copy_counting` (a buffered chunked copy) on a
   worker thread, then try to unlink src. If unlink fails (typical NFS
   squash-perm scenario), return `MovedDirty` so the caller can surface
   the orphan to the UI.

Worker thread + polling loop here prevents NFS/CIFS stalls from blocking
the main autorip thread. Calls `on_progress(pct, gb_done, gb_total,
speed_mbs)` every 1s while the copy is running.

## `the_config_lock_is_free_while_the_move_runs` (test)

`run()` used to pass the `RwLockReadGuard` itself into `check_and_move`,
so the guard lived for the whole pass — including a cross-filesystem copy
of an 80 GB MKV. Any `POST /api/settings` blocked for the duration, and
std's RwLock is write-preferring on Linux, so one queued writer then
blocks new READERS too: the entire unauthenticated web UI stops answering
for however long the copy takes.

An earlier version of this test built its own snapshot inline and PASSED
with the fix reverted, because it never exercised `run()`'s shape. This
one observes the lock from INSIDE the injected move, which is the only
place the property is actually observable.

## Test: `resolve_media_root_joins_natively`

Runs the media-root rules on NATIVE paths, so Windows exercises this logic
instead of skipping it. The absolute-wins case is the load-bearing one: it
is what keeps a configured absolute library path from being buried under
`output_dir` (the "Mercy" incident was the relative half of the same
rule). Because the join is native, the assertions compare against
`Path::join`'s own output rather than a hard-coded separator.

## Test: `resolve_media_root_semantics`

`resolve_media_root` unit semantics: relative joins, absolute wins,
trailing slashes normalize, empty sub -> output_dir. POSIX-only because
the fixtures are POSIX paths: on Windows "/mnt/media" is drive-RELATIVE,
not absolute, so the absolute-wins case never triggers there.
`resolve_media_root_joins_natively` covers the same rules with real paths
on whichever platform is running.

## Test: `build_destination_empty_tv_dir_falls_to_output_dir`

Companion to `build_destination_empty_movie_dir_falls_to_output_dir`: the
TV arm's `!cfg.tv_dir.is_empty()` guard is load-bearing in exactly the
same way, and nothing pinned it. With an EMPTY `tv_dir` the arm must not
fire — the file falls through to the output root under its own sanitized
name. If the guard is dropped, `resolve_media_root` collapses the "tv
root" back to `output_dir` and the mover invents a `Title/Season 1/` tree
at the bare library root, while `destination_root` keeps validating plain
`output_dir` — the two functions drift apart and the pre-flight guards a
directory the write does not use.

## Test: `destination_root_and_build_destination_agree_including_empty_dirs`

The lock-step contract stated as a property over the whole configured-dir
matrix, including the empty-dir edges the older
`destination_root_matches_build_destination_root` test never covered:
whatever root the pre-flight validates, `build_destination` must write
UNDER it — and when the media dir for the routed type is empty, BOTH
functions must take the plain `output_dir` fall-through (dest is exactly
`output_dir/<leaf>`, with no per-title tree).

## Test: `prune_stranded_warned_drops_vanished_dirs_keeps_present`

FIX 4 — `STRANDED_WARNED` (the one-time-warn dedup set for stranded
staging dirs) was inserted-into but NEVER pruned, growing unbounded for
the daemon's lifetime — the same leak its sibling `MOVE_ERRORS` already
bounds via `prune_move_errors`. `prune_stranded_warned` mirrors that: once
a dir is gone from the pass's `seen` set (the operator `rm -rf`'d it),
its dedup entry is dropped; entries still present are retained.

## Test: `a_successful_move_clears_a_stale_destination_keyed_error`

`MOVE_ERRORS` row is the ONLY row the mover keys by destination path
rather than by staging dir, and nothing automatic ever removed it:
`clear_error` is called only from the staging clean-teardown arm, and
`prune_move_errors` deliberately skips keys outside the staging root. On
a library mount with a persistent unlink fault, every rip produced a NEW
permanent key (each disc has its own destination filename) and the map
grew for the container's lifetime. A later *successful* move to that
destination is proof the row is stale — the self-healing behaviour
`clear_move_error`'s doc already promises for the staging-keyed rows.

## Test: `move_file_copy_failure_leaves_no_partial_dest`

Partial-dest cleanup contract (already-landed fix). When the copy path
fails, `move_file` must NOT leave a partial/garbage destination behind —
otherwise the next mover tick sees a phantom size-mismatch Collision and
wedges the move permanently.

Forcing a *mid-copy* truncation deterministically needs fault injection (a
writer that fails after N bytes), which isn't available here. Instead
this forces the copy branch (rename must fail first) and a copy failure,
asserting the outcome is `Failed` with no leftover dest. The complementary
"stale partial dest doesn't wedge the next tick" case is covered by
`move_file_overwrites_when_dest_size_differs` (a pre-existing partial is
cleanly overwritten).

## Test: `move_file_does_not_report_moved_on_non_notfound_src_stat_error`

The pre-flight "src missing, dest present" branch must require a genuine
`NotFound` on the src stat, not just any error. A bare `Err(_)` also
matches EACCES/EIO/ESTALE — none of which prove src is gone. If the stat
fails for one of those reasons while src is still physically present
(here: its parent directory loses traversal permission), src has NOT been
consumed by an earlier rename. Reporting `Moved` anyway would let the
caller tear down the staging dir — deleting the still-present,
possibly-only-good src — on the strength of a dest that could just as
easily be a stale partial from an unrelated earlier attempt.

## Test: `cross_device_copy_and_unlink_end_to_end` (see below near line ~2792)

Cross-device (EXDEV) copy+unlink SUCCESS path, driven end-to-end through
`move_file` against a SEPARATE real filesystem. `move_file`'s fast path
is `fs::rename`; the copy+validate+unlink fallback only runs when rename
fails with EXDEV. Within one tempdir rename always succeeds, so this
branch is unreachable without a second mount. This test probes for two
distinct filesystems among well-known mount points and SKIPS (documenting
the gap) when it can't find them, rather than faking the EXDEV condition.

KNOWN GAP: when no second filesystem is available (typical dev laptop /
sandboxed CI), the `move_file` copy-success -> post-copy validate ->
`remove_file(src)` -> `Moved` path, and the `MovedDirty` (copy ok, unlink
fails) branch, are NOT exercised end-to-end. The constituent pieces ARE
covered: `copy_counting` success/atomicity (its own tests),
`check_post_copy_*` validation (its own tests), and the copy-FAILURE
cleanup path (`move_file_copy_failure_leaves_no_partial_dest`). Closing
the gap fully needs a real EXDEV mount or an injectable rename seam.
POSIX-only: the whole premise is `st_dev` and EXDEV. `dev_id_of` returns
None off unix, so the fixture cannot even identify a second filesystem,
let alone force a cross-device rename.

## Test: `check_and_move_gives_a_discs_companion_files_the_same_variant`

A rip can deliver an MKV and its companion ISO. When a second disc of the
same title takes the `_2` suffix, BOTH files must take the same one —
`Title_2.mkv` alongside `Title_2.iso`. Resolving the variant per file
would split the pair (`_2` and `_3`) the moment one of the two happened to
be free at the destination, which is exactly the state a partially failed
first move leaves behind.

## Test: `check_and_move_files_only_outputs_for_a_tv_dir`

Regression: `state.json`'s `outputs[]` is the AUTHORITATIVE deliverable
list for a TV rip (`is_tv_plan`). A leftover partial episode whose
mux/fsync failed and whose delete was swallowed can still be sitting on
disk under its raw name; without the `ripped_files.retain(...)` filter it
would be promoted into the library as if it were a complete episode. Here
the staging dir holds two *planned* episodes (S05E01, S05E02) plus a
leftover `S05E03` that never made it into `outputs[]`: only the two
planned episodes may reach the output tree.

## Test: `check_and_move_keeps_iso_for_a_tv_dir_with_keep_iso`

Regression: the TV filter restricts promoted files to those named in
`outputs[]` — but `outputs[]` never lists the intermediate `.iso`, only
the muxed episodes. Before the `is_iso_file(name) ||` clause was added, a
`keep_iso=true` TV rip's ISO was silently dropped by this filter, then
destroyed by the staging-dir teardown that follows a successful move —
even though the operator explicitly asked to keep it.

## `walkdir_files`

Minimal recursive file walk used only by
`check_and_move_files_only_outputs_for_a_tv_dir` to scan the whole output
tree for a leaked leftover file.

## Test: `dest_with_variant_suffixes_the_stem_and_leaves_variant_one_alone`

POSIX-only: `with_file_name` re-renders the parent with the platform
separator, so a POSIX fixture comes back mixed on Windows. The RULES —
suffix on the stem, extension preserved, directory untouched — are
asserted natively in `dest_with_variant_suffixes_the_stem_natively`.

## Test: `copy_counting_failure_leaves_no_file_at_final_name`

Regression (temp + rename atomicity): a failed/interrupted copy must NOT
leave any file at the FINAL dest name — the bytes land on a sibling
`.part-<pid>` temp and only `rename(2)` over the real name once fully
written + fsynced. A truncated file at the real name would fail the
mover's post-copy size check and wedge the move.

## Test: `copy_counting_aborts_between_chunks_when_shutdown_is_requested`

SIGTERM must be observed BETWEEN CHUNKS, not at the end of the copy.
`move_file`'s shutdown branch joins the copy worker and its comment
promised the join was "bounded to its current chunk write" — but the
copy loop polled nothing, so the join waited out the whole remaining
multi-GB copy while `docker stop`'s 10s grace expired and SIGKILL landed
mid-write. Drive the real loop with the abort signal already raised and
require it to give up.

## Test: `check_and_move_second_disc_of_a_title_is_filed_beside_the_first`

Two DIFFERENT discs route to the same `Title (Year)` path and their
muxes happen to be the SAME byte length — the boxset case, and also what
a wrong TMDB match looks like from here. The size-only guard used to
wave this through to Skipped, then `remove_dir_all` deleted the NEW rip's
staging while the library kept the OLD file. The content probe catches
it: a different disc is no longer an operator error, it is disc 2 — it
gets `Title (Year)_2.mkv` and is FILED. The invariant that matters is
unchanged and still asserted here: the existing library file is never
overwritten.

## Test: `check_and_move_defers_on_non_notfound_dest_stat_error_and_never_clobbers`

The collision guard's stat classification: ONLY `NotFound` means "there
is no destination, the move is safe". Any other stat error (ESTALE, EIO,
EACCES, a dropped NFS mount) must defer the entry to a later tick. This
is the worst outcome in the crate if it regresses: treating a transient
stat error as "no dest" skips the collision check entirely and hands the
path straight to `move_file`, whose own guards ALSO can't stat the dest —
so `rename(2)` (needs no read access to the victim, only write access to
the directory) silently overwrites a good library file with an unrelated
disc's rip. Driven through the real `check_and_move`, not a helper: an
unreadable (mode 000) destination makes the dest stat fail with EACCES
rather than ENOENT, exactly the shape a degraded mount produces.

## Test: `check_and_move_collision_probe_window_is_large_enough_to_see_a_2kb_diff`

The same-size content probe must compare a window large enough to be
meaningful. Two different discs' muxes that happen to share a byte
length AND their first/last kilobyte are not a realistic pair, but two
that share their first/last 64 KiB are not realistic either — the whole
point of the constant. Shrink the window and the probe starts calling
distinct files identical, which routes a second disc down the IDEMPOTENT
path: `move_file` returns `Skipped`, `remove_dir_all` then deletes the
new rip, and the library keeps only the first disc's data. The probe is
what tells "this is my own file, re-claim its name" from "this is
another disc, take the next `_N`". Here the two files differ only at
byte 2000 — inside the real 64 KiB window, outside anything much smaller.

## Test: `move_file_copy_failure_with_no_dest_records_no_partial_error`

A failed copy that left NOTHING at the destination must not raise a
"partial copy could not be removed" error. `copy_counting` only ever
publishes the final name via `rename(2)`, so "no file at dest" is the
normal shape of a failed copy — and that error row is keyed by the
DESTINATION path, which no later tick clears, so a bogus one sticks on
the System page until the operator dismisses it by hand.

## Test: `move_file_copy_failure_keeps_pre_existing_dest`

DATA LOSS regression: a failed copy must never delete a destination that
PRE-DATES the attempt. `dest` legitimately pre-exists as a `MovedDirty`
leftover — a cross-fs copy that SUCCEEDED and passed post-copy
validation, but whose unlink of `src` failed on an earlier tick. That
file is a complete, good copy and may be the only readable one.

Shape that reaches the failure-cleanup arm with such a dest: `src`
develops a persistent read fault. `fresh_metadata` opens the file, so the
stat fails EACCES (NOT NotFound) — which skips BOTH pre-flights.
`rename(2)` then fails (unwritable staging dir), the copy fails at
`File::open(src)`, and the cleanup arm runs. Pre-fix it unconditionally
`remove_file`d `dest`, destroying the only good copy while `src` sat
stuck and unreadable.

## Test: `a_blocked_pass_clears_the_move_progress_bar`

A pass that ends in one of the four failure branches must still clear
the move progress bar. The clear used to be the last statement of the
loop body and all four branches `continue` past it, so a blocked
delivery left the System page showing a live-looking "60%, ETA 1:23" for
a move that had already given up — for the life of the process, since
nothing else ever writes `None`.

Reaching `any_failed` deterministically, without depending on file
permissions: occupy the destination PATH with a DIRECTORY. Its stat is
neither `Ok(file)` nor `NotFound`, so the variant resolver goes
`uncertain` (no `_2` rename) and the collision guard's stat arm defers
the entry as `Failed`.

## Test: `a_per_entry_listing_error_marks_the_listing_incomplete`

A per-entry listing error must mark the listing INCOMPLETE, not just
drop the entry. The dropped entry is a media file this pass cannot see:
it is never planned, never copied, never delivered. If the entries the
pass COULD see all move successfully, the caller reaches
`remove_dir_all` and deletes the staging dir — including the file that
was never moved. Only the single-entry case is caught today, by the
`ripped_files.is_empty()` skip; a dir where one entry errors and the
rest succeed loses data and logs "Move complete".

## Test: `the_teardown_is_gated_on_a_complete_listing`

The flag only saves the file if the teardown is actually gated on it.
Inducing a real `DirEntry` error from a test needs a fault-injecting
filesystem, so this pins the wiring at source level (the technique this
crate already uses for `resume_remux`'s webhook and marker call sites):
the `!listing_complete` guard must stand between the copy-outcome guards
and `remove_dir_all`. Deleting it puts the data loss straight back.

## Test: `move_errors_for_a_vanished_staging_dir_are_pruned`

`MOVE_ERRORS` rows for a staging dir the operator removed by hand — which
is exactly what both blocked-teardown hints tell them to do — must be
pruned, because the only `clear_error` call site is reached by revisiting
that same dir on a later pass, and a removed dir is never revisited.

## `mover_verdict`

Builds a single staging disc dir, runs the real `check_and_move`, and
reports whether the MKV reached the library and staging was cleaned.
`done_body`: `None` = no `.done` marker; `Some(bytes)` = that exact
`.done` content. `with_mkv`: whether a valid EBML MKV is staged. `extra`:
extra marker filenames to drop in (e.g. `.completed`).

## Test: `done_absence_sweeping_governed_via_snapshot`

Convergence round 4 (M3): the governed-marker probe must route through
`snapshot_staging_disc` (NFS-attribute-cache-resilient, 3x-retried
read_dir) rather than bare per-marker `exists()` calls, so a cold-cache
mount right after a container restart can't false-negative a durably
present `.sweeping` into a `Fault` and WARN-flood the multi-hour sweep
window (the original 182-warn bug). A dir whose ONLY entry is `.sweeping`
classifies InProgress — and the same snapshot the rest of the resume
machinery uses agrees it's owned.

## Test: `done_absence_vanished_dir_is_in_progress_not_fault`

Regression: a staging dir that vanished between the `.done` read and the
governing-marker probe (a finished move, or `/api/stop` cleanup) must be
treated as InProgress, not a stranded-dir Fault — that transition is a
normal lifecycle event and must not emit a spurious WARN.

## Test: `done_absence_vanished_dir_ignores_sibling_markers`

Precedence guard for the TOCTOU fix: the vanished-dir check runs BEFORE
the governing-marker probe. A marker on a SIBLING dir must not leak into
a vanished dir's classification, and a marker placed on the vanished
dir's would-be path doesn't exist (the dir is gone) so it can't rescue a
genuine stranded condition into a false InProgress.

## Test: `done_absence_present_dir_without_marker_is_fault`

A dir that EXISTS but carries no governing marker is a genuine stranded
condition (Fault) — the vanished-dir early-return must NOT swallow it.
This pins that the `!dir.exists()` guard is specifically about
disappearance, not "absent marker".

## `classify_done_absence`

The governing-marker probe is advisory, not a lock: between the `.done`
read and the `exists()` calls the ripper/mux worker can land a marker,
or the whole staging dir can be removed (a finished move, or
`/api/stop` cleanup). That TOCTOU is inherent to a best-effort
classification on a hot staging path and is handled deliberately: if
the dir itself has vanished (NotFound) it's treated as InProgress, not
Fault — a dir that disappeared out from under us is a normal lifecycle
transition, not a stranded-dir condition worth a WARN. Probing the dir
once (rather than each marker) also collapses the race window.
