# Queue-view single-flight cache (`src/web.rs`)

## `sse_emits_a_json_state_first_frame` (test)

The `/events` stream loops forever, so `roundtrip` (which reads to EOF)
would hang. Run the production handler on its own thread, read exactly the
first `data: …\n\n` frame on the client, then drop the socket — the
handler's next write fails and it returns.

## `QueueViewSnapshot`

Cached result of `build_queue_views`, shared across every concurrent
`/events` (SSE) client and `/api/state` poller.

`/events` holds one thread per client for the life of the connection (up to
`MAX_SSE_CLIENTS`) and each thread independently rebuilds the Mux/Move queue
view every second — a fresh `read_dir` over the whole staging directory plus
a handful of `Path::exists()`/marker reads per subdirectory (see
`build_queue_views` / `crate::muxer::pending_queue`). With a large staging
backlog and several dashboard tabs open, that is the same filesystem the
ripper and mover are actively writing to, scanned redundantly by every open
tab in lockstep. The queue view only changes when a rip/mux/move transitions
state, so a sub-second-stale shared snapshot is invisible in a UI that
itself only polls once a second — trading a small, bounded staleness window
for turning N concurrent full-directory scans per second into at most one.

## `QueueViewCache` fields

- `snapshot`: `None` only between the moment a key's FIRST scan starts and
  the moment it finishes — there is simply nothing to serve yet.
- `refresh_started`: when the scan that currently owns this key STARTED, if
  one does. Single-flight marker, deliberately a timestamp rather than a
  bool: a bool can only be cleared by the refresher that set it, so a
  refresher that never returns latches it forever. Trusted for
  `QUEUE_VIEW_REFRESH_DEADLINE`, after which the owner is presumed dead and
  the next caller may take the key over.
- `refreshers`: how many threads are inside `scan_queue_views` for this key
  right now. Maintained by `RefreshGuard`, so it is decremented on panic
  too, and capped at `QUEUE_VIEW_MAX_REFRESHERS` so repeated takeovers of a
  key that stays wedged cannot pile threads up without bound.

## `RefreshGuard`

RAII owner of a key's single-flight marker. Drop — not the happy path — is
what releases the marker, so a scan that panics inside `read_dir` (or
anywhere else in `build_queue_views`) hands the key straight back instead of
stranding it until the deadline.

## `QUEUE_VIEW_CACHE`

Keyed by `staging_dir` rather than a single slot: the staging path can
change at runtime (a Settings edit), and — just as importantly — this keeps
two DIFFERENT staging dirs from evicting each other's cached scan (a real
scenario if the operator ever repoints staging, and exactly the shape our
own parallel tests exercise with distinct tempdirs).

## `QUEUE_VIEW_REFRESHED`

Signalled when an in-flight scan completes. Only the cold-start case (no
snapshot to serve at all) ever waits on it; a caller that has ANY snapshot,
however stale, is served immediately instead.

## `QUEUE_VIEW_COLD_WAIT`

Safety valve for the cold-start wait: a caller with NOTHING to serve gives
up after this long and returns an empty queue view rather than parking on
the condvar for as long as the in-flight scan takes. It deliberately does
NOT scan for itself — that abandons single-flight, and on a wedged staging
mount it consumes one HTTP worker thread (plus its admission token) per
give-up, which is how `/api/state` starts 503-ing and the container
HEALTHCHECK restarts the daemon mid-rip. Recovery from a dead scanner is
`QUEUE_VIEW_REFRESH_DEADLINE`'s job instead. Never hit in normal operation.

## `QUEUE_VIEW_REFRESH_DEADLINE`

How long the per-key single-flight marker is TRUSTED. A refresher that has
held it longer than this is presumed dead — panicked before the drop guard
could run, or wedged inside `read_dir` on an unresponsive mount — and the
next caller that needs fresh data is allowed to take the marker over.

Without this, `refreshing` is a latch: a refresher that never returns makes
every later caller take the serve-stale branch forever, so the queue views
freeze for the process lifetime.

## `QUEUE_VIEW_MAX_REFRESHERS`

Hard ceiling on threads inside `scan_queue_views` for ONE key at a time. The
deadline above permits a takeover; this bounds how many takeovers can pile
up when the takeover ALSO wedges (a mount that is down stays down). Two: the
original plus one retry. When the mount recovers both publish, the count
drops back to zero, and normal single-flight resumes.

## `QUEUE_VIEW_CACHE_RETAIN`

How long an idle key is RETAINED in the map — deliberately far longer than
the serve-freshness TTL (`QUEUE_VIEW_CACHE_TTL`). The Phase-3 prune exists
only to stop distinct ephemeral staging paths from growing the map without
bound; it must NOT evict a still-active key just because its snapshot aged
past the sub-second TTL. Evicting there throws away the stale snapshot that
stale-while-revalidate relies on, so the key's next (slow) refresh has
nothing to serve and forces a concurrent `/api/state` reader to block for
the whole scan — the healthcheck stall the cache exists to prevent. Retain
by recency-of-refresh instead: a key polled each second stays warm; one
nobody reads ages out and is pruned.

## `build_queue_views_cached`

`build_queue_views`, but shared across concurrent callers within
`QUEUE_VIEW_CACHE_TTL` instead of re-scanning the staging directory once per
caller. Used by `get_state_json` (the per-second SSE/`/api/state` payload);
`handle_system_info`'s on-demand `/api/system` panel calls the uncached
`build_queue_views` directly so a manual refresh always sees the latest disk
state.

The cache mutex is NEVER held across the scan. `build_queue_views` does
`read_dir` plus a stat per entry, and this function backs `/api/state` —
which `--healthcheck` probes and the Dockerfile HEALTHCHECK runs — so a
staging dir that is slow to enumerate must not be able to park every other
caller (and get the container restarted mid-rip). Single-flight is
preserved without the lock: a per-key marker means exactly one caller
scans, while everyone else is served the previous snapshot
(stale-while-revalidate) and never blocks. Only a genuinely cold key — no
snapshot at all — makes callers wait, and they wait on a condvar with the
map lock released, not on the scan's mutex.

Three separate bounds keep a refresher that never comes back from wedging
the whole daemon, and they are deliberately distinct:

- `QUEUE_VIEW_REFRESH_DEADLINE` bounds the MARKER. A single-flight bool can
  only be cleared by the thread that set it, so a thread stuck in `read_dir`
  latches it and every later caller serves stale forever. A timestamp lets
  a later caller decide the owner is dead and take the key over, which is
  what unfreezes the views.
- `QUEUE_VIEW_MAX_REFRESHERS` bounds the takeovers that deadline permits, so
  a mount that stays down cannot accumulate one wedged thread per deadline.
- `QUEUE_VIEW_COLD_WAIT` bounds an individual COLD caller, which returns an
  empty view rather than scanning: the give-up path must not start work, or
  the caller bound becomes an accumulation rate.

## `QUEUE_DISPLAY_CAP`

Cap on how many queue entries are serialized so a staging dir holding a
pathological number of subdirs can't produce an unbounded response. Shared
by `build_queue_views` (the actual truncation) and `handle_system_info`
(the "+N more" math) so the displayed list and its overflow count can never
drift apart.

## `build_queue_views`

Builds the Mux-queue and Move-queue display lists from the staging dir.
Shared by `get_state_json` (the live SSE/`/api/state` payload) and
`handle_system_info` (the `/api/system` panel) so both endpoints derive the
two queues from one place and can never disagree on membership.

Returns `(mux_queue, move_queue, mux_full_count, move_full_count)`: the
first two are capped at `QUEUE_DISPLAY_CAP` for display, the last two are
the uncapped totals from the SAME scan so callers can compute a "+N more"
overflow count that always matches the displayed lists (one snapshot, no
TOCTOU between count and list).

Mutual exclusion is guaranteed by the unified `state.json` itself: the Move
queue selects dirs in `StagingState::Done` (legacy `.done` file as
fallback), and `crate::muxer::pending_queue` (the Mux queue) skips any dir
that is not a clean `Ripped` hand-off — i.e. muxing, terminal, or already
handed to the mover (`.done`/`.review`/`.muxing`/`.completed`/`.failed`). So
a given staging dir lands in at most one of the two lists.

The Move queue additionally excludes the staging dir currently being moved
(`crate::mover::ACTIVE_MOVE_DIR`): that dir keeps its `.done` throughout the
copy and is already shown as its live per-artifact progress bars (`_move`),
so listing it here as a "(moving)" row too is the double-render bug. The
exclusion is by exact on-disk basename, so it holds regardless of any title
punctuation.

## `queue_scan_probe` (test-only)

Test-only seam around the staging-dir scan a cache miss performs. Keyed by
staging dir so tests that arm it are isolated from every other test in this
process (the cache itself, `STATE`, and the log dir are all process-global
here, so a shared fixture name would be a real race). Lets a test (a) make
one specific dir's scan artificially slow and (b) count how many scans a dir
actually received.
