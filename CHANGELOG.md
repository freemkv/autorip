# Changelog

## [1.6.9] — 2026-08-22

### Added

- **Proper TV support.** A series disc now resolves to the show itself and files
  into the Jellyfin/Plex layout `TV/Show (Year)/Season NN/`. The season number is
  read from the disc label (`Endeavour Season 5 Disc 2`, `GOT_S3_DISC1`, …), the
  series folder carries the year, and the season subfolder is zero-padded
  (`Season 05`). Previously every TV disc was dumped into a hardcoded `Season 1`
  folder with no year — so seasons collided and scrapers mis-matched. A disc with
  no season marker still defaults to `Season 01` rather than landing loose.
- **TMDB id on every match.** A resolved title now carries its TMDB numeric id, so
  downstream tooling can fetch anything else it needs straight from TMDB by id
  without re-searching.

### Fixed

- **A season-labelled disc now resolves to the show, not a same-named film.** When
  the disc label carries a season marker, the title match prefers the TV series
  over a film of the same name (e.g. the ITV series *Endeavour* rather than a
  film called *Endeavour*), so the disc files as TV instead of being mis-filed as
  a movie. The preference is a tie-break only — an exact film match still wins
  over a fuzzy series one.

- **Discs that returned *no* TMDB match now resolve.** The title lookup used to
  send the disc label to TMDB as-is, and a single stray token would zero out the
  entire search — e.g. `Batman v Superman: Dawn of Justice: UE` (Ultimate Edition)
  matched nothing and dropped to the needs-review queue. The lookup now cleans the
  label and, if the full title finds nothing, progressively peels trailing
  junk-shaped tokens (edition/region/packaging codes like `UE`, `SE`, `UPT1`,
  `3D`) and retries — while never trimming a real sequel marker, so `Alien 3` and
  `Rocky II` are untouched. The auto-file confidence check uses the same logic, so
  a title the lookup can resolve no longer gets parked in review.

## [1.6.8] — 2026-08-21

### Added

- **Webhooks now fire per pipeline stage: Rip, Mux, Move.** A rip goes through
  three stages — the disc is read (**Rip**: the drive is now free), the ISO is
  muxed to an `.mkv` (**Mux**), and the file is moved into the library
  (**Move**). Each webhook row now has a checkbox for each stage, in any
  combination, so you can (for example) get a **Rip** notification the moment a
  drive frees up to load the next disc, while a library scanner fires only on
  **Move**. New hooks default to all three checked. Existing configurations are
  read unchanged and gain the new **Mux** stage enabled by default, so a hook
  that previously notified on completion keeps doing so. The payload adds a
  `"mux_complete"` event name alongside `"rip_complete"` and `"move_complete"`.

### Fixed

- **The Rip webhook now fires when the drive is free, not when the mux
  finishes.** Previously the "rip complete" notification was sent at the end of
  the *mux* — up to 20+ minutes after the disc had actually finished reading —
  so it was useless for the one thing it should signal: that the drive is free
  and you can load the next disc. It now fires at the point the read completes
  and the disc is ejected, on the drive worker, decoupled from the mux that
  continues on a separate worker. The end-of-mux notification is still sent, now
  as the distinct **Mux** stage.

- **The Ripper tab's activity banner now shows during moves, too.** The banner
  that tells you a previous rip is still muxing/moving in the background (when
  the drive tile itself reads idle) only lit up for muxes — it silently missed
  moves, because it still checked the pre-1.6.7 single-object move shape after
  the move state became an array of per-artifact bars. A move in progress now
  shows the banner just like a mux does.

## [1.6.7] — 2026-08-21

### Added

- **Each webhook chooses which events fire it.** Every webhook row now has a
  **Rip** and a **Move** checkbox, so one hook can post only on rip complete,
  another only on move complete, and a third on both — instead of every hook
  firing on both events. New hooks default to both checked (the prior
  behaviour). Existing configurations are read unchanged: a webhook stored as
  a bare URL still fires on both events. Delivery is unchanged — an HTTP
  `POST` of the same JSON payload per event.

- **The Move card shows one progress bar per artifact.** A rip that keeps its
  ISO moves two files — the muxed title and its companion image — and each now
  gets its own labelled bar (`Title (mkv)` and `Title (iso)`) rather than a
  single combined bar. Moves run one at a time, so the active file's bar
  climbs while the other reads 0% until its turn, then 100%.

### Fixed

- **A title being moved no longer appears twice in the Move card.** The
  actively-moving job was shown both as its live progress and as a second
  "(moving)" queue row, because the queue was de-duplicated against the title
  by a client-side string match that broke whenever the title contained a
  character the filesystem strips (a colon, slash, etc. — e.g.
  `X-Men: Apocalypse`). The active job is now excluded from the queue on the
  server by its exact on-disk name, so it is listed exactly once regardless of
  punctuation. (Surfaced by the 1.6.6 move-state change, which made the queue
  refresh in lockstep with the active move.)

- **A failed webhook delivery reports the real reason.** A socket-level
  failure (a receiver that resets or refuses the connection) logged the
  useless `Webhook failed <host>: io: uncategorized error`. When the OS
  supplies an error code the log now carries its description —
  `Webhook failed <host>: io: Connection reset by peer (os error 54)` — while
  still never including the token-bearing URL.

## [1.6.6] — 2026-08-20

### Changed

- **Webhooks may now point at private / LAN addresses.** The SSRF guard that
  rejected any webhook URL resolving to RFC1918, loopback, or link-local
  space (`webhook URL rejected: refusing to connect to non-public address …
  (SSRF guard)`) has been removed for webhooks. A webhook is a blind
  fire-and-forget notification with no response channel, and pointing one at
  a LAN service — Home Assistant, a NAS, an internal automation endpoint — is
  the intended use, which the guard actively prevented. Delivery now uses an
  un-pinned agent with the standard resolver (redirects still blocked,
  timeouts unchanged). The `keydb`, `keyserver`, and raw network-output
  targets stay fully SSRF-guarded: those POST key material, stream decrypted
  disc content, and read responses back, so their private-address block and
  DNS pinning are unchanged.

## [1.6.5] — 2026-08-20

### Added

- **Kept and whole-disc ISO images can be filed in a folder of their
  own.** A new `iso_dir` setting collects `keep_iso` intermediates and
  `output_format=iso` rips into one flat folder — named `Title (Year).iso`,
  no per-title subfolder — instead of dropping them beside the muxed title.
  It resolves under the output directory with the same relative-join /
  absolute-wins rules as the Movies and TV folders, so it can point at a
  separate archive volume, which is the usual home for images that run
  25–56 GB. Left empty (the default), images stay alongside the title as
  before. The field appears on the dashboard under Output Directory.

- **The dashboard is usable on a phone.** The Now-Playing card becomes a
  compact horizontal card — a small poster beside the title and info —
  rather than stacking into a full-width giant poster, tap targets and the
  action row are sized for touch, and the wordmark shrinks (and steps out
  of the way under 400px) so the nav tabs get their room back. The desktop
  layout is unchanged.

### Fixed

- **A rip done without a TMDB key landed in the output root instead of the
  movie library.** A no-match rip wrote `unknown` as its media type, but
  the mover treats the empty string as the no-match sentinel that routes to
  Movies — so `unknown`, being neither empty nor movie/tv, fell through to
  a dump at the output root. Running without a TMDB key is a supported
  mode; it now writes the empty string the mover expects and reaches the
  movie library.

- **An operator resume could mux a title short.** The manual resume path
  checked only that no bytes were still pending, not that the staged ISO
  was still as long as the mapfile's recorded total. A crash, OOM-kill or
  full disk that truncated an otherwise fully-swept image would then be
  muxed short. Resume now applies the same length check the automatic guard
  already does and re-sweeps rather than resume onto a truncated image.

- **A failed review action looked like it worked.** The review
  Proceed / Cancel / Retitle buttons read the server's reply but never
  checked whether it succeeded, so a failed resolve just silently refreshed
  the list. The error is now surfaced, matching the search box.

- **A drive still finishing up would wrongly accept new work — and could
  run a duplicate rip.** A rip worker publishes its final status ("done" /
  "error") and then keeps running on the same thread to eject, flush logs
  and drop its guards; for that whole window the device read as free. Since
  the server listens on the LAN with no authentication, a request landing
  in that window won the claim and — worse — could start a second full rip
  into the first one's staging directory while clobbering its Stop token.
  Claiming a device now consults the worker's liveness, not just its
  status; registration is decided before any disk work happens; and the
  loser of a spawn race can no longer clear the winner's claim or idle its
  state. This is the same class of defect closed across the mover, the
  hot-unplug teardown and the poll loop.

- **A Stop that timed out reported success while a worker kept writing.** A
  drain that timed out still reset the drive card to "idle" and answered
  `ok:true`, so the card went quiet mid-write and the operator's next click
  came back 409 with nothing to explain it. A timed-out Stop now says so —
  in the log, on the device card, and in its reply. A crashed scan or a
  poisoned lock during teardown could likewise leave a claim or a phantom
  drive row standing forever with no log line; those now recover and are
  reported.

- **A blank dashboard could never recover on its own.** `GET /api/state`
  was the one status reader that gave up on a poisoned lock and returned
  `{}` with HTTP 200 — a blank dashboard whose healthcheck stays green
  forever, so the container never restarts out of it. The map is perfectly
  readable on poison; it is now served.

- **A degraded share reported "no jobs queued" when it simply could not be
  read.** The mux queue swallowed a failed directory read and every
  per-entry error, so an NFS mount that was down or had lost permissions
  rendered as an empty queue with nothing to say the list was a guess. The
  failure is now logged so an empty queue is attributable.

- **The mover could delete files it never delivered, and mislabel what was
  left behind.** A single failed directory entry was skipped with a bare
  `continue`, and the pass still ran its destructive cleanup — removing a
  file it had never enumerated — while logging "Move complete". The cleanup
  is now gated on a complete listing: deliver what enumerated, withhold the
  destructive half, retry next tick. A pre-existing destination is no
  longer deleted, and "left behind" rows are cleared by a later successful
  move and recorded only when something genuinely remains.

- **Consent to accept a lossy rip could be spent by an unrelated hiccup, or
  ignored outright.** Accepting loss on a resumed rip was honoured by the
  sweep gate but re-armed against by the mux gate in the same run, so the
  rip was quarantined with the one-shot consent already gone. The consent
  marker was also consumed too early — any transient failure spent it — and
  an accept-loss request that armed on disk but then failed to spawn left
  the override armed for the next disc. Both gates now share one threshold,
  the consent is cleared only where it is actually spent, and a failed
  accept-loss disarms and says so.

- **A resumed boxset disc muxed a stray second copy.** Cold resume derived
  the title from the staging directory, which carries a boxset's `_2`
  suffix that the files inside do not, so it muxed a second MKV and the
  mover delivered both. An unreadable ISO is now reported as a named fault
  rather than looking like a disc with no encryption.

### Security

- **Disc-supplied text can no longer smuggle control characters into the
  log.** Volume IDs at the rediscovery sites and the on-disc title in the
  mux worker — which falls back to the raw disc label whenever TMDB finds
  nothing — are now sanitised before they are logged. Also closed: a u32
  overflow on disc-supplied extents, a percent-decoder that accepted a sign
  as a hex digit, an unauthenticated LAN route that took the media type
  unclamped, and the TMDB client following a redirect with the API key
  still in the query string.

### Changed

- One deliberate behaviour change: a scan, rip, eject or accept-loss
  request that arrives while a worker is still unwinding now answers 409
  rather than being admitted. Automatic ripping is otherwise unchanged.

## [1.6.4] — 2026-08-15

### Fixed

- **A stalled upload could restart the daemon mid-rip.** A client that sent
  request headers and then stalled parked a connection slot; enough of them
  starved `GET /api/state` — the container healthcheck — whose failed retries
  restart the daemon. Body-carrying requests are now held to a lower cap than
  bodyless ones, so a healthcheck always has a slot to answer on.

- **Log retention reclaimed nothing from the main log.** Daily-rotated files
  (`autorip.log.YYYY-MM-DD`) were skipped by a check that read the date as the
  file extension, so the log the operator actually reads grew for the life of
  the container. Rotated files are now matched correctly.

- **A redirected webhook was reported as delivered.** With redirects disabled a
  3xx returned success, so an `http` webhook that redirected to `https` logged
  "Webhook sent" while nothing was ever delivered; only a 2xx now counts. A
  failed thread spawn also no longer permanently leaks a delivery slot.

### Security

- **Two more paths that reach the unauthenticated API are held to URL-free
  logging.** The resolver's address cap and the ureq error-masking catch-all —
  which keep token-bearing URLs out of the debug endpoint — are now covered by
  tests, and a fourth key-database-update error path that logged a raw ureq
  error (whose display can embed the URL) now goes through the same masking as
  its siblings.

### Changed

- Rebuilt against libfreemkv 1.6.4 (the single-clip audio-tail fix); automatic
  ripping is otherwise unchanged.

## [1.6.3] — 2026-08-10

### Changed

- **Housekeeping only — nothing about automatic ripping has changed.** The HTTP
  client behind webhooks, the metadata lookup and the key-database download
  moved to its current release, and two crates that were declared but never used
  were removed from the build.

### Security

- **Failure messages still cannot leak the URLs they came from, and the pinned
  connections are now proven by a test.** Webhook, metadata and key-database
  URLs can carry a token in the path or the query, and the summaries that reach
  the log and the status endpoints are built from a fixed description of the
  failure rather than from the underlying error, which would quote the whole
  URL. Both that and the address pinning behave as before; each now has a test
  that fails if it stops.

## [1.6.2] — 2026-08-08

### Added

- **Track languages can be chosen once instead of on every disc.** Preferred
  audio, subtitle and forced-subtitle languages are honoured when a disc is
  scanned. A disc carrying none of them falls back to the previous behaviour
  rather than ripping without a track.

### Fixed

- **A stray moment of sound at the end of an HD-DVD title, and a click at every
  chapter break on a DVD.** Where a title is stitched from segments, a few
  frames of sound arriving just before or just after the picture that marks the
  join were timed against the wrong segment — placing one trailing frame hours
  past the end of an HD-DVD title, and squeezing about half a second of sound
  into an instant at each of a DVD's eight chapter breaks. Sound is now timed
  against the segment it belongs to. Blu-ray was never affected.

## [1.6.1] — 2026-08-07

### Fixed

- **Blu-ray titles built from several clips ran minutes long, with sound
  drifting ahead of picture.** Rips of such titles were the wrong length and
  lost A/V sync partway through. Fixed in `libfreemkv`; autorip picks it up by
  consuming the new version. Single-clip titles, DVDs and HD-DVDs were never
  affected.
- **A decrypted DVD image could lose most of its title list.** Fixed in
  `libfreemkv`.
- **Chapter marks and durations on NTSC DVDs ran about 0.1% short.** Fixed in
  `libfreemkv`.

## [1.6.0] — 2026-08-03

### Changed

- Internal: autorip's mux paths (ISO/multipass, resume, and live single-pass)
  now run through `libfreemkv::mux_stream` instead of a hand-rolled inner loop,
  and drive/ISO bring-up goes through `DiscSession` / `scan_iso`. No user-facing
  change; the FMTS key-resolution gate is now interruptible.
- The recovery pipeline now runs on the shared **`freemkv-engine`** crate:
  autorip drives `freemkv_engine::{sweep, patch}` (with `SweepOptions` /
  `PatchOptions`) and reads the map through `freemkv_engine::{Mapfile,
  SectorStatus}` instead of libfreemkv's now-removed `Disc::sweep` / `Disc::patch`.
  Damage classification comes from `freemkv_engine::classify_damage`, and
  progress speed + ETA are derived by the engine's `SpeedEstimator` (autorip's
  local windowed-speed math was deleted and promoted into the engine, so every
  front-end shares one derivation).
- Builds against libfreemkv 1.6.0. The rip service keeps every audio/subtitle
  stream (the new stream-selection knob defaults to keep-everything; autorip has
  no selection UI yet). Moving autorip's higher-level rip *orchestration* onto
  the engine's `Sink`/`run` surface (it currently drives the passes directly, so
  its staging/resume/watchdog can advance at pass boundaries) is a later 1.6.x
  step.

## [1.5.2] — 2026-07-22

### Fixed

- CSS DVDs no longer rip to corrupt video. The rip handed the mux a disc-wide
  key of `None` on a CSS detection miss, muxing scrambled sectors as plaintext;
  key resolution now happens per-title at read time (via libfreemkv 1.5.2), so
  the same disc decrypts correctly. Verified: 0 decode errors (was 328k).
- Single-pass FMTS (AACS 2.1 UHD) now decrypts correctly. The forensic key gate
  and read plan were nested in the multipass-only path, so a single-pass FMTS rip
  muxed the alternate device-group half as garbage. Key resolution is now a shared
  pre-decode step both rip paths use, with the forensic keys threaded into the
  single-pass reader.
- A rip with no TMDB match now files under the movie library instead of the
  share root. An empty `media_type` matched neither the movie nor TV branch and
  fell through to `output_dir`; it now coalesces to `movie`, so the disc files as
  `movies/Title (Year)/…`.
- TMDB lookups no longer fail on a label with a parenthesized year (`Drive
  (2011) - 4K Ultra HD` queried TMDB as `Drive (2011)` → 0 results). `clean_title`
  strips a parenthesized 4-digit year; a bare year (`Blade Runner 2049`) is kept.

## [1.5.0] — 2026-07-19

Version sync with the workspace; inherits libfreemkv 1.5.0. No functional change to
the service — the new extraction sinks (`audio://`, `sub://`, `chapters://`,
`json://`) are library/CLI features and are not part of autorip's rip path.

## [1.4.5] — 2026-07-18

### Fixed

- **FMTS (AACS 2.1) rips are clean end to end.** Inherits libfreemkv 1.4.5's
  read-only-our-variant FMTS mux, wired into **both** autorip mux paths: the resume
  / multi-pass highway and the live single-pass `DiscStream`. The pre-rip forensic
  key gate now retains the resolved map and hands it to the single-pass mux, so a
  `rip_mode = "single"` FMTS rip no longer leaks the alternate device-group half
  into the demuxer.

### Changed

- **All forensic keys are resolved before the sweep** (fail-fast), honoring the
  **Capture Discs Without Keys** setting — a base-keyed but forensic-missing FMTS
  disc no longer sweeps for an hour and then fails at mux.

## [1.4.4] — 2026-07-17

### Fixed

- **The online key request now carries enough samples.** autorip sampled only 4
  content units, below the online source's `MIN_SAMPLE_UNITS` (8) floor, so every
  online lookup was skipped and surfaced as "key service down." The sample count is
  now tied to `MIN_SAMPLE_UNITS` with a compile-time floor.

## [1.4.3] — 2026-07-17

Version sync with the workspace; inherits libfreemkv 1.4.3 (AACS 2.1
forensic-variant online lookups).

## [1.4.2] — 2026-07-15

### Added

- **Blu-ray 3D output.** A rip whose main feature carries an MVC dependent
  (right-eye) view is written with a `.mk3d` extension (byte-identical Matroska)
  so media servers/players surface it as stereoscopic 3D. Applies to fresh rips
  and resumed re-muxes alike.

### Fixed

- **A recovery mapfile that can't be loaded at the abort-decision point now
  forces an abort**, instead of silently reporting zero loss and delivering a
  possibly-lossy rip as perfect. An unreadable damage record is treated as
  unquantifiable loss (fail-safe).
- **Resume and fresh-rip abort gates converged.** The resume re-mux path now
  uses the same byte-aware loss gate as a fresh rip, so a zero-bitrate title with
  real unreadable bytes can no longer slip through on resume when a fresh rip of
  the same disc + config would abort.
- **Device logs strip terminal control/escape bytes** from disc-supplied strings
  (UDF volume-id, Blu-ray title) across every sink, closing an ANSI-escape
  injection into `docker logs` / the on-disk log.
- **The stop path recovers a poisoned rip-thread lock** rather than dropping the
  `JoinHandle`, so a panicked rip worker is still drained before staging is
  wiped (no regression of the stop-without-drain bug).

### Changed

- Inherits **libfreemkv 1.4.2** (mux no longer nulls decryptable video or storms
  the key server on a bad-encoded region; decrypt / TS-structure separated).

## [1.4.1] — 2026-07-14

### Added

- **3D rips get a `.mk3d` extension** (Matroska stereoscopic video, RFC 9559
  §27.18.3) — byte-identical Matroska, only the extension differs, so media
  servers and players surface the rip as stereoscopic 3D. 2D / UHD rips are
  unchanged (`.mkv`), and the `m2ts` passthrough is unchanged (`.m2ts`). Applied
  on both the fresh-rip and resume-remux paths.

### Fixed

- Inherits **libfreemkv 1.4.1**: the mux no longer conceals decryptable video as
  loss when a disc carries the occasional authored-bad (non-conforming) TS packet
  — the fix for the false "corruption" seen on some UHD titles.

## [1.4.0] — 2026-07-13

### Added

- **3D discs rip to a single MVC-track MKV** automatically — the mux worker
  reads the SSIF for a 3D clip and preserves both eyes, via libfreemkv 1.4.0.
  No new configuration; 2D/UHD rips are unchanged.

## [1.3.2] — 2026-07-10

Version sync with the workspace; inherits libfreemkv 1.3.2.

## [1.3.1] — 2026-07-10

### Licensing

- **Relicensed to the MIT License, from 1.3.1 onwards** (releases up to and
  including 1.3.0 remain under AGPL-3.0).

Version sync with the workspace; inherits libfreemkv 1.3.1.

## [1.3.0] — 2026-07-08

### Added

- **Recognizes AACS 2.1 (FMTS) and HD-DVD discs.** The rip loop now labels and
  handles the FMTS and HD-DVD disc formats as first-class rather than falling
  through as an unknown format.

### Changed

- Inherits **libfreemkv 1.3.0** (AACS 2.1 / FMTS as a first-class format,
  partial HD-DVD support with VC-1 muxing, and display-order timestamps for
  program-stream H.264 / VC-1 / HEVC) and **freemkv-keysources 1.3.0**.
- Trimmed dead settings-UI JavaScript (unused codec / resolution maps,
  `renderTotalBar`, `fmtChapterTime`, and a duplicate error update).

### Fixed

- **The rip thread survives a poisoned config lock.** The key-fetch factory now
  recovers the lock instead of panicking the rip thread, matching the service's
  graceful-degradation convention.
- **Corrected the resume total-pass count** — dropped a redundant `.max(2)` on
  `total_passes` (`max_retries + 2` is already ≥ 2).

## [1.2.2] — 2026-07-04

### Fixed

- **A down online key service is no longer reported as a missing key.** When
  `key_source = "online"` resolves no key for an encrypted disc, autorip now runs
  one bounded reachability probe against the keyserver (SSRF-pinned GET, ~8 s
  timeout, zero redirects) and classifies the result: a transport error or
  502/503/504 is a transient **outage**, a 429 is **rate-limited**, and any other
  HTTP answer (2xx/3xx/non-429 4xx, including a 404 auth wall) means the service
  is **up** and the key genuinely does not exist. On a transient verdict autorip
  bounded-retries key resolution (3 attempts, 8/16/32 s backoff) instead of
  permanently failing the disc, and if the service never recovers it parks the
  disc in a distinct retryable state ("Key service unavailable — temporary
  outage, not a missing key; will retry." / "Key service rate-limited (quota) —
  will retry later.") rather than the permanent "no keys found" error. Wired into
  both the scan-time key-readiness tile and the live rip path's keys-missing
  gate; never hammers the drive or the service, never ejects.

### Added

- **Clear stuck move errors from the System tab.** Each move-queue error now
  carries a ✕ to dismiss it, plus **Clear all** and **Refresh** in the errors
  header. Clearing removes the entry from the in-memory error map; the mover
  re-records any move that is still genuinely failing on its next tick, so a
  resolved or stale error stays gone while a persistent one reappears — no
  container restart needed to clear the display.

## [1.2.0] — 2026-07-01

### Added

- **Live patch view rework.** The rip card now shows a tall disc "defrag" map
  (green = recovered, red = still bad) with a "you are here" marker tracking the
  section being patched, a smooth fractional sweep fill, and a textual
  `N sections · X sectors (Y MB) remaining` line. The ETA is always shown (no
  more blank gaps at low patch rates), the Good/Maybe pills are fixed width so
  the row doesn't jitter, and the elapsed counter sits before the Stop button so
  its growth never shoves the button.
- **Mux is its own view, not a rip pass.** The rip tab shows `pass 2/6` (sweep +
  retries), never counting the mux; the map paints immediately at pass start
  (red ranges + at-risk time) instead of sitting blank through the 30 s settle.
- **Live Debug Log box** in the device view (the patch walk + per-read timings),
  shown when debug logging is enabled.
- **Loss-abort off-ramp.** When a rip can't finish perfectly, the System tab
  offers explicit "Run one more pass" / "Accept & deliver" actions instead of
  stalling on a stuck "scanning".
- **Version now carries the build's git short hash** — `--version`, the UI
  footer, `/api/version`, and the startup log report e.g. `1.2.0 (g2014a41)`
  (the same shape libfreemkv stamps into MKVs), so a running build — including a
  hand-deployed test build — is always identifiable instead of hiding behind a
  bare package version.
- **Unlocker matrix in the device log.** After each disc scan, the device log shows
  a one-line summary of which unlockers actually ran:
  e.g. `Unlockers (yes = ran this rip) — LibreDrive: yes, AACS: no, CSS: no`.
  Operator-visible confirmation that the right authentication paths fired, without
  reading structured logs. Driven by `Disc::unlocker_matrix()` in libfreemkv 1.2.0.

### Changed

- **autorip never reads the mapfile.** The live rip view renders entirely from
  libfreemkv's `PassProgress` (the rendered located drilldown + at-risk movie
  time), and the per-UI-tick mapfile reload is gone — the disc map, section
  count and at-risk time all come from the library. If the mapfile format ever
  changes (e.g. to a mapdb), no client code changes. The one-time done-card
  verdict is the only remaining mapfile read.
- **250 ms UI refresh** (was 1.5 s) so the map, sectors-remaining and speed
  update live; the speed/ETA window is time-based so finer samples only smooth
  it. Disc errors now render via `freemkv-i18n`, and each session stamps its
  build SHA into the log.
- **Batch patch reads (32 sectors).** autorip's patch block size was realigned
  to libfreemkv's canonical 32 (it had drifted to single-sector), so a bad range
  reads its good skip-ahead overshoot in bulk and only the real damage pays the
  single-sector cost.
- **Breadth-first recovery — fast-capture pass first.** The first retry pass now
  runs libfreemkv's `fast_capture`: it reads every bad section once and grabs the
  readable blocks (the sweep's good skip-ahead overshoot) across the WHOLE disc
  before any single section's slow per-sector grind — instead of grinding section
  1 to exhaustion before even touching section 2. Later passes do the granular
  retry on what's left. No data is dropped: a failed block stays `NonTrimmed` for
  a granular pass (covered by a libfreemkv fixture test).
- **Speed always reads, down to bytes/sec.** A crawling patch now shows e.g.
  `512 B/s` instead of a blank `0 KB/s`, and `0 B/s` when work is genuinely frozen
  grinding one sector's ECC. The patch speed holds a fixed 10s window
  (responsive) instead of growing to 60s, so a fast-capture burst shows
  immediately; the steady sweep keeps its growing window.

- **The mux never aborts on mux-time loss.** A disc that was swept and patched
  is always handed to the muxer, and the muxer always delivers. Earlier versions
  ran a *second* loss check after muxing and could quarantine an
  already-finished file when demux/decrypt loss exceeded `abort_on_lost_secs` —
  but with libfreemkv 1.2.0 that loss is concealed into a decode-clean file
  (NULL-TS fill + drop-to-keyframe) and merely tallied, so failing the disc at
  that point only stranded a good rip. `abort_on_lost_secs` now governs the
  **rip** phase alone (unreadable sectors, before the mux); the mux is never
  gated by it. (The drive/pipeline status split into explicit DeviceStage /
  PipelineStage models is staged for a later 1.2.x — see the in-code
  `TODO(1.2.0)`.)
- **One key-resolution path.** autorip resolves AACS keys through
  `Disc::inputs()` (libfreemkv) instead of its own duplicate `key_files()` /
  `volume_id()` readers, so the service and the CLI capture a disc's inputs
  identically. The stale mapfile-VID read was dropped in favour of the disc's
  own Volume ID.
- **Pass-1 progress bar leaves the un-read region blank.** During the sweep,
  sectors that had not yet been attempted were rendered red — visually identical to
  known-bad sectors. Unread means unknown, not bad; the bar now leaves that region
  empty until sectors are actually assessed.
- **Device log clears at scan, not rip.** The per-device log previously cleared
  when a rip started, discarding the scan context — disc identity, unlocker matrix,
  key-resolution outcome — before it was visible during the rip. The log now clears
  at the start of a scan, so the full scan context carries through into the live
  rip view.
- **Pass-done log shows recovery buckets and wedge state.** Each completed pass
  logs recovered / still-bad / unreadable sector counts alongside a wedge flag — a
  compact per-pass verdict instead of a bare sector total.
- **ETA capped at ">6h" on near-zero recovery rate.** A stalled drive previously
  produced multi-day or astronomically large displayed ETAs. The ETA is now capped
  at ">6h" when the recovery rate is near zero; the wedge indicator flags the
  fast-fail state separately.
- **Defrag-map caret tracks the largest bad range.** The "you are here" marker in
  the disc map now follows the largest bad range being worked, matching the
  largest-first dispatch order of the libfreemkv 1.2.0 Pass-N handler chain.
- **`Cache-Control: no-store` extended to JSON and plain-text responses.** JSON API
  responses (`/api/state`, `/api/version`, etc.) and plain-text endpoints now carry
  `no-store` alongside the HTML dashboard, which already did. autorip is a local
  control app; none of its responses are appropriate to cache.

### Fixed

- **Resume no longer false-fails on operator actions.** A graceful shutdown — an
  operator redeploy, a host reboot, a Watchtower auto-update, a `docker stop` —
  is never counted as a crash. The interrupted rip used to leave a
  `.sweeping`/`.muxing` marker that the startup classifier read as an
  in-progress crash and counted toward the restart limit, so a few redeploys
  *during* a rip walked a perfectly healthy resumable rip to a false `.failed`
  (resume "vanished"). autorip now cancels active rips on `SIGTERM` (so their
  guards clear the markers) and clears any in-progress markers up front, with a
  generous `stop_grace_period` — so only a true ungraceful crash (panic=abort /
  OOM / power-loss) can ever increment the count. A cleanly-stopped resumable dir
  is likewise never counted.
- **Resume re-injects the mapfile VID for an uncatalogued-disc ISO.** Resuming a
  mux from a swept ISO whose disc wasn't in the catalogue now reconstructs the
  Volume ID from the mapfile so AACS resolution has the input it needs, instead
  of failing the resume.
- **A failed mux shows the real reason.** When the mux worker genuinely fails,
  the System tab surfaces the actual reason from the staging marker instead of a
  generic "mux worker dispatch did not complete (see _mux device log)", so the
  operator doesn't have to read device logs.
- **Removed the bad-ranges detail table from the rip card.** Its summary total
  was computed in the wrong unit (showing "<1 ms total" while the ranges summed
  to tens of seconds), and it duplicated the at-a-glance Good/Maybe pills. The
  table is gone; the pills and the in-bar red bad-range overlay remain.

- **The dashboard is no longer cached across releases.** The single-page UI
  (HTML + inline JS) was served with no `Cache-Control`, so browsers kept
  running the *old* page — old client-side validation, old error handling, old
  everything — after a new autorip version deployed. It's now served `no-store`,
  so a release takes effect on the next page load instead of requiring a manual
  hard-refresh.
- **Retry passes no longer show the previous pass's progress.** When pass 1
  ended and a retry pass began, the per-pass bar stayed frozen at "pass 1/N ·
  99% · ETA 0s" through the 30 s drive-settle (until the first retry read). The
  new pass now flips to "pass N · retrying · 0%" immediately, before the settle.
  The cumulative total bar is unaffected.
- **Quieter retry-pass logs.** Dropped `bytes_unreadable=…` from the per-pass
  log lines — it is always `0` until the final pass promotes pending sectors, so
  it was pure noise mid-rip.

## [1.1.0]

Inherits libfreemkv 1.1.0, including the **post-read decrypt-verify gate**
(undecryptable units are caught during the rip and re-read) and the
**DVD movie-not-menu** fix.

### Added

- **"Accept damage & deliver" — operator off-ramp on a loss-abort.** When a rip
  aborts because main-movie loss exceeds the threshold, the card now offers a
  one-click Accept: the *existing* swept ISO is re-muxed and delivered as-is (the
  loss gate is bypassed for that one delivery), with **no re-rip**. "Run another
  pass" is the Resume button (continues Pass N from the mapfile, recovering only
  the bad core). Pairs with the resume fixes below.
- **Live patch progress is no longer a black box.** During a retry pass the
  bad-range drilldown now lists the *located* Maybe ranges (LBA + sectors +
  chapter) being worked, instead of staying empty until a sector is terminally
  given up on.
- **ISO output now requires a 100% byte-complete image.** The per-title
  "Max Acceptable Main Movie Loss" tolerance is a muxed-output (MKV / M2TS /
  Network) setting and is now ignored for an ISO rip (forced to 0): a value left
  over from a previous MKV rip can no longer silently let an ISO accept loss. The
  Settings UI already hid the field for ISO; the abort logic now matches it.

### Changed

- **Rip progress is now two states — Good and Maybe, never a third.** The live
  card no longer shows `Feature` / `Cosmetic` / `Moderate` / `Serious` /
  `No chance` / `Lost` pills. **Good** = whole-disc bytes read *and* verify-clean;
  **Maybe** = every byte not yet good (pending, NonTrimmed, currently-unreadable,
  undecryptable — all folded together). Nothing is called "lost" mid-rip: a later
  pass, or a freshly power-cycled drive, still recovers it, so there is no live
  terminal bucket. "Bad" is a **verdict**, decided once after the final pass
  (main-feature lost time vs `abort_on_lost_secs`), not a pill. The Maybe pill's
  bytes are whole-disc but its **time is the main-feature lost time** at ms
  precision — `Maybe 990 MB · 0:00` means 990 MB pending with zero movie impact
  (passes), while `Maybe 12 KB · ~1 ms` is a few movie sectors (fails a 0
  threshold). A handful of sectors reads as `~1 ms`, never `0`.
- **Abort-on-loss is resumable, never terminal — and you can accept the loss.**
  A rip that aborts because main-movie loss exceeds the threshold keeps its
  complete swept ISO and stays *resumable indefinitely* (the old "exhaust N
  attempts → terminal `.failed` → re-rip the whole disc from scratch" loop is
  gone — a deterministic media defect won't fix itself, so re-sweeping 50 GB to
  reach the same bad sector was pure waste). The abort card now offers
  **Accept damage & deliver**: a one-shot operator override that re-muxes the
  *existing* ISO and delivers the movie as-is, missing only the unreadable
  section — for an imperceptible loss (a few frames / ~1 ms) that's the right
  call, and it's yours to make. Operator cancel and durability/structural-mux
  failures stay terminal.
- **Live patch progress is no longer a black box.** During retry passes the
  drilldown now lists the *located* ranges being worked (LBA, sectors, chapter),
  so "pass 3, no movement" shows exactly which bad region the drive is grinding.
- **"Max Acceptable Main Movie Loss"** moved under the MKV/muxed-output settings
  and shown in seconds.

### Fixed

- **A loss-abort no longer destroys the swept ISO.** Previously a rip that
  aborted on main-movie loss was retried a few times and then promoted to a
  terminal `.failed`, which locked out resume — and the next trigger re-swept the
  whole disc from scratch, **overwriting the complete 50+ GB ISO** and discarding
  all recovery progress. A loss-abort is deterministic media damage, so it now
  stays **resumable indefinitely** (never auto-promoted to `.failed` by attempt
  count) and the unattended path **refuses to re-sweep over** a loss-aborted ISO.
  The operator resolves it: **Accept** (deliver as-is) or **Resume** (run another
  recovery pass on the bad core). The complete ISO is never thrown away.
- **The live "Maybe" pill now shows honest main-movie time at risk.** It counts
  in-feature *pending* sectors (not just terminally-unreadable ones), so a rip in
  progress reads `Maybe N · ~Xms` when the movie is affected and `Maybe N · 0:00`
  when the pending bytes are out-of-feature — instead of a premature `0:00` /
  "Feature clean" while a bad range was still unresolved. The single-source
  `RipProgress` computation replaces three drifting copies.
- **Clearer abort message.** A sub-second main-movie loss now reads e.g. "1 ms"
  instead of a confusing "0.00s", and a zero threshold reads "perfect rip
  required" instead of "threshold 0s".
- **Resume can no longer race an in-flight mux.** A staging directory owned by
  the mux worker (sweep handed off, or mux in progress) is no longer offered for
  sweep-resume, so a manually triggered resume can't overwrite the staged ISO
  while the muxer is still reading it.
- keydb save writes directly to the service path (removed the
  validate-then-relocate workaround).

## [1.0.0-rc.5.1]

### Fixed

- **Mover no longer warns on every poll for an in-progress staging dir.**
  The mover emitted a spurious ".done marker" warning on every 10-second
  poll when it encountered a staging directory that was still being
  written to. The warning is now suppressed for directories actively in
  use; it fires only when a directory is genuinely stranded (i.e. present
  with no corresponding active rip after a restart or crash).

## [1.0.0-rc.4.2]

Windows durability fixes.

### Fixed

- **Windows re-mux loop.** The post-mux durability gate opened the
  finished output read-only, so on Windows the flush (`FlushFileBuffers`)
  was rejected with `ERROR_ACCESS_DENIED`; the `.done` marker was never
  written and auto-resume re-muxed the same disc indefinitely. The gate
  now opens the output read+write so the flush succeeds on every platform.
- **Windows free-space preflight.** `staging_free_bytes` was a no-op on
  Windows, so the staging out-of-space check never ran; it now reads free
  space via `GetDiskFreeSpaceExW`.
- **Windows log noise.** Directory fsync (a POSIX concept) is now a no-op
  on Windows instead of failing to open the directory and warning on every
  marker and mapfile write.

## [1.0.0-rc.4] — UNRELEASED

Plain-English failure reasons, accurate loss accounting on done cards,
and a round of resume/abort and hot-unplug correctness fixes.

### Fixed

- **No more re-mux loop.** A DVD that hit a post-mux loss abort could be
  re-muxed indefinitely; the `.failed` marker is now terminal in the mux
  worker, so an aborted disc stays aborted.
- **Readable failure reasons.** Mux and scan read errors, AACS handshake
  failures, and CSS crack failures are now reported as English text with
  the specific cause (and the failing keydb path on a key error) instead
  of a bare `E`-code. Pass 1 exhaustion and non-SCSI pass errors are
  likewise labeled.
- **Accurate loss accounting.** Done cards report combined sweep + mux
  (demux-skip) loss; single-pass done cards no longer show `0s`
  main-movie loss or under-classify damage severity, and bad-range
  drilldowns are populated. Fresh and resumed multipass rips gate on
  post-mux demux-skip loss, so a disc with decrypt/demux loss can't be
  accepted as perfect. `NaN` loss is treated as an abort.
- **Resume correctness.** The resume path enforces the same
  abort-on-loss gate, honors `auto_eject` and the `iso` output format,
  carries title/metadata/codecs into the done card, and no longer leaks
  halt tokens.
- **Single-pass.** `abort_on_lost_secs` is now enforced in single-pass
  rips, the loss gate scales by bytes skipped rather than skip count,
  single-pass ISO output is rejected so abort scope matches multi-pass,
  and read-error truncation surfaces on `/api/state`.
- **ISO output.** `output_format=iso` now delivers a disc image instead
  of muxing an MKV.
- **Hot-unplug cleanup.** Title overrides, stop cooldowns, the device
  log ring, and first-seen tracking are evicted when a drive is
  unplugged.
- **Durability.** NFS `DirEntry` read errors are no longer silently
  dropped across staging, resume, and mover scans; staging basenames are
  unioned across NFS retries; mux header-phase failures are quarantined
  rather than silently swallowed; a poisoned config lock surfaces an
  error state instead of panicking; and the hand-off marker is never
  written empty.
- The raw `E5000` code prefix was dropped from the disk-space preflight
  message, and that preflight warns when it can't read free space.

## [1.0.0-rc.2]

Second release candidate for 1.0. Adds end-to-end DVD/CSS support and a bare-run
mode, on top of concurrency, durability, and web-handler hardening.

### Added

- **DVD/CSS support.** autorip rips and muxes CSS-protected DVDs end-to-end.
  AACS key resolution is skipped for DVDs; the CSS title key is recovered
  keylessly from the swept disc by libfreemkv's Stevenson attack. No
  `keydb.cfg` is required for DVDs.
- **Bare-run mode.** `autorip` (or `autorip serve`) runs the daemon directly
  with no container bootstrap, storing config under `~/.config/autorip`. Useful
  for the downloadable static binary on a bare Linux host. See `INSTALL.md` for
  install instructions, non-root drive access via the `cdrom` group or a udev
  rule, and a hardened systemd unit.
- **Static-binary releases.** Each tagged release attaches
  `autorip-x86_64-linux` and `autorip-aarch64-linux` static binaries with a
  `.sha256` checksum, alongside the existing Docker image.
- **Runtime debug-logging toggle.** `POST /api/debug {"enabled":true/false}`
  swaps the active tracing filter without a container restart, surfacing
  libfreemkv debug events (mux stalls, sector retries) in `docker logs`.
- **`.completed` restart guard.** The muxer checks for a `.completed` marker
  before re-processing a staging directory, so a container restart cannot
  trigger a duplicate mux on a disc that already finished successfully.

### Changed

- Built on libfreemkv 1.0.0-rc.2, inheriting correct DVD MPEG-2 muxing,
  HEVC/H.264/VC-1 param-set keyframe correctness, short-read rejection, and
  `BlockDuration` timescale fix. Output MKVs record `freemkv 1.0.0-rc.2` in
  their Writing-application field.
- `Config` implements a manual `Debug` that redacts `tmdb_api_key`,
  `keydb_url`, `keyserver_url`, and `keyserver_secret`, so diagnostic log
  output does not leak secrets.
- Staging-directory relocation at startup uses existence (`Path::exists`) to
  decide whether `/staging` is mounted, not a write probe. A transient NFS
  hiccup at container start no longer orphans an in-progress ISO by relocating
  staging to the config directory mid-rip.

### Fixed

- `POST /api/stop` during the mux phase no longer quarantines a resumable disc
  as `.failed`. Stop-versus-failure is now classified on typed error variants
  (`Halted`, `PipelineJoinTimeout`, `PipelineConsumerPanicked`) rather than
  error-message strings, so a routine operator stop keeps the disc resumable.
- Abort-on-loss after retries are exhausted now writes a `.failed` staging
  marker, preventing the muxer from retrying a disc that was deliberately
  abandoned due to unrecoverable data loss.
- The eject-then-clear-session sequence is now performed atomically under the
  device lock, eliminating a TOCTOU race where a disc insert between eject and
  state clear could produce a stale session.
- `/api/settings` POST validates string-enum fields (including `output_format`,
  `on_insert`, and `on_read_error`) and applies numeric clamps, rejecting
  malformed values before mutating the in-memory config.
- Mux staging-directory scan handles `DirEntry` I/O errors per-entry (logs and
  skips) instead of aborting the entire scan on a single unreadable entry.

### Security

- CSS disc/title keys inherited from libfreemkv are redacted in autorip's logs
  (logged as `<redacted>` with a 1-byte fingerprint).
- `settings.json` is persisted with owner-only (0600) permissions, since it
  may hold `keyserver_secret` and `tmdb_api_key`.

## [1.0.0-rc.1]

First release candidate for 1.0 — the first tagged 1.0 milestone of the rip
service (see "Pre-1.0 development" for the consolidated feature list).

## Pre-1.0 development

Versions 0.x were the development series leading up to 1.0. The highlights,
condensed:

- **Unattended ripping service.** Detect a disc on insert (udev), scan and
  identify it (TMDB lookup for title/poster/year), rip it, mux to MKV, and move
  the finished file to the library — all hands-off. Web dashboard with live
  SSE progress, per-device drive cards, settings UI, history, and webhooks.
- **Multipass orchestration.** Single-pass (direct disc→MKV) and multi-pass
  (disc→ISO→retry bad ranges→mux) rip modes, with an abort-on-loss threshold
  for the main feature and three-bucket Good/Maybe/Lost progress reporting.
- **Parallel pipeline.** Rip, mux, and move run as independent staged workers,
  so the drive frees the moment sweep+patch finish and the next disc can be
  ripped while the previous one muxes and moves — the killer unattended flow.
- **Resilient staging + move.** `.ripped`/`.done`/`.completed`/`.failed` markers
  make rips resumable across container restarts; format-aware post-copy
  validation (EBML/TS sync checks) catches a truncated copy without depending on
  NFS attribute freshness; an opt-in in-container NFS mount self-heals stale
  host mounts on restart.
- **Deployment.** Curated minimal `FROM scratch` Docker image plus a
  downloadable static binary with a bare-run mode (`autorip serve`,
  `~/.config/autorip`) and a hardened systemd unit. Auto-downloading keydb
  updater for AACS discs; DVDs (CSS) need no key file.
- **Security/UX hardening.** HTML-escaped dashboard output (stored-XSS fix),
  redacted secrets in `GET /api/settings` and `Debug`, SSRF guards on outbound
  targets, validated settings, and a cross-origin POST guard. Runtime
  debug-logging toggle. Release builds use thin LTO.

