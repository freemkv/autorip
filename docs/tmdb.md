# tmdb.rs — design notes

Overflow rationale for comments that would otherwise exceed the
comment-guard's per-block caps. Each section is pointed to from a short
`// See docs/tmdb.md — <topic>` comment at the relevant spot in
`src/tmdb.rs`.

## AGENT: no pinned resolver

Unlike `web::guarded_agent`, the shared TMDB `AGENT` pins no DNS resolver.
The host is the hard-coded `api.themoviedb.org`, not an operator-supplied
URL, so there is no SSRF/rebinding surface to close here.

## read_capped_bytes: why it's a pure function over `Read`

Pulled out of `read_capped_json` as a pure function over any `Read` — not
tied to `ureq::Response`, which can't be constructed in a unit test
without a live HTTP server — so the cap boundary itself (accept at
exactly `cap`, reject at `cap + 1`) is directly testable against an
in-memory `Cursor`.

## lookup: progressive fallback rationale

TMDB's `search/multi` text search zeroes out on a single unparseable
trailing token — the disc "Batman v Superman: Dawn of Justice: UE"
returns ZERO hits, but dropping the "UE" (Ultimate Edition) returns the
exact 2016 film. Disc labels carry an un-enumerable tail of
edition/region/packaging codes (UE, SE, UPT1, G51, BD3, 3D, …), so
instead of an ever-growing blocklist, `lookup` queries the cleaned label
and, on no confident match, peels junk-shaped trailing tokens one at a
time and re-queries (`query_variants`). Pure numbers and roman numerals
are NEVER peeled — they are sequel markers ("Alien 3", "Rocky II"), and
trimming them would mis-file a sequel as the original film.

Returns the first confident match (exact normalized title + a year)
across the variants; failing that, the best non-exact guess from the
earliest variant that returned anything, so the needs-review card still
shows a suggestion. The confidence bar itself is unchanged — only recall
improves.

## is_confident_match: why it shares query_variants with lookup

Sharing `query_variants` is what keeps this auto-file gate in lockstep
with the fallback lookup: without it, a title resolved by peeling "UE"
off the label would be found by `lookup` but then REJECTED here (the
full label never exact-matches), parking every edition disc in review.

## rank_search_results: why it's pulled out of search()

Pulled out as its own function (taking already-fetched JSON rather than
making the HTTP call itself) so this — the actual decision logic behind
the manual "needs review" correction picker — can be driven directly in
a test without a network round trip, instead of leaving it exercised
only via the untestable `search()` entry point.

## pick_best: the Wraithline bug

`search/multi` mixes movies, TV, people, and collections, and does NOT
always rank the obvious film first — e.g. "Wraithline Part Two" can
surface a dateless franchise/collection entry ahead of the 2024 film.
The old `results.first()` path then took that entry and ended up with
`year == 0`, which the mover turns into a yearless library folder
(`Wraithline Part Two/` instead of `Wraithline: Part Two (2024)/`). Fix:
keep only movie/TV entries, prefer ones that carry a release year, and
break ties on TMDB popularity.

## strip_paren_year: why parenthesized years are stripped

Retail meta-titles annotate the release year in parentheses; TMDB's text
search returns ZERO hits when that annotation is left in the query
(`Drive (2011)` matches nothing, `Drive` matches). A 4-digit year in
parentheses is virtually never part of a real movie title, so removing
it is safe. A BARE (unparenthesized) year is left untouched so titles
like "Blade Runner 2049" and "1917" are unaffected.

## SEASON_WORDS: why Volume/Vol/Part are excluded

Deliberately EXCLUDES "Volume"/"Vol"/"Part": those ARE part of real film
titles ("Neon Reaper: Vol. 2", "Void Marshals Vol. 2", "Wraithline: Part
Two") and peeling them would mis-resolve the movie.

## is_trailing_junk: scope of what's peeled

Edition/region/format words and obvious mixed-alphanumeric codes are
peelable — but NEVER a bare number or roman numeral (sequel markers like
"Alien 3", "Rocky II"). The full label is always queried first and the
exact-match gate guards every variant, so this only needs to avoid that
one class of meaningful-token collision.

## query_variants: casing and dedup

Peeling is done on the RAW label so `is_trailing_junk` sees original
case, then each surviving prefix is run through `clean_title`. Deduped
and capped (`MAX_QUERY_VARIANTS`) so a pathological label can't fan out
into an unbounded burst of TMDB requests.

## align_disc_offset: design rationale

A multi-disc season never tells a single disc how many episodes the
*earlier* discs held, so a disc can't count where it starts. The
caller's uniform-split guess (`fallback = (disc-1)*count + 1`) is right
for the common even boxset but wrong when discs carry different episode
counts (e.g. 6 + 4), which collides disc 2's numbers with disc 1's.

This finds the offset rather than counting it: slide this disc's runtime
sequence along the season's episode runtimes and pick the start whose
runtimes fit best. A disc holding *any* distinctively-timed episode (a
double-length finale, a short clip show) is pinned to its true position
regardless of disc order or how the earlier discs split. When the
runtimes carry no distinguishing signal — every episode ~the same
length, so every offset fits equally — the result ties and it returns
`fallback`, which is exactly the case (a uniform season) where the
uniform-split guess is right.

Deliberately conservative: it returns `fallback` on any absence of
signal (no episodes, no known runtimes, a disc that can't fit the
season, or a tie), so it never numbers *worse* than today — it only ever
repairs a case today gets wrong. Its known soft spot is partial TMDB
runtime data (some episodes known, some not); that path is scored on the
average per-pair distance and is the first thing to revisit if this
misbehaves — the whole function is pure and isolated so a future fix
stays local.
