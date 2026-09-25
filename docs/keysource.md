# keysource.rs — design notes

Long-form rationale moved out of `src/keysource.rs` doc comments to satisfy
the comment-guard prose caps. Pointers in the source link back here.

## `build_iso_key_fetch`: why the mid-mux fetch seam exists

The upfront resolve validates only ONE unit key (the key service returns the
one UK that opens the sample it was sent). A disc whose feature spans a
second CPS unit would otherwise drop that unit's content as decrypt loss —
the exact 0.44s-in-the-main-movie failure this seam fixes. Wiring the
`KeyFetch` closure lets the mux send the server the failing unit's data and
get that unit's key on demand, recovering the 2nd/Nth CPS-unit key mid-mux.

Mirrors `freemkv::pipe::build_iso_key_fetch`: read the ISO's AACS inputs
(inf + MKB + version) ONCE, then reuse them per fetch with the failing units
swapped in as `samples`. The VID is all-zero (an ISO carries no live-drive
AACS handshake); the key service resolves the disc from its own catalog.
`make_sources` is invoked per fetch (the cold path, ~once per CPS unit),
rebuilding the SAME sources the upfront resolve used, so `online`/`local`
config is honored identically.

## `IsoKeyFetch`: why it's an enum, not a log line

`build_iso_key_fetch`'s two negative outcomes both collapse to `None` at the
call site, but only one is normal: a non-AACS ISO has nothing to fetch,
whereas an ISO that could not be READ (ESTALE on the staging mount, a
truncated file, EACCES) is a fault that used to vanish into the same
`.ok()?` — the mux then dropped the 2nd CPS unit as decrypt loss with no
line anywhere saying why.

The type exists so the distinction can be asserted directly in tests. An
earlier version of the regression test drove the real function under a
capturing `tracing` subscriber and asserted on captured log output; that
passed alone and failed in the full suite, because sibling tests dispatch
the same `warn!` callsite with no subscriber installed and `tracing` caches
`Interest::never` for the whole process. Testing the decision instead of its
rendering has no such race.

## `ServiceReachability`: the down-vs-no-key fix

This is the crux of the "down vs no-key" fix: when the online source
resolves NO key, autorip alone can't tell whether the service HAD the key
but was unreachable (a 502 outage, connect-refused, timeout) or genuinely
has none. The real `/decode` POST's HTTP outcome answers that, with a single
bounded probe as the fallback when no POST reached the network.

The enum is per-OUTCOME, not a three-way up/down/quota, because the
operator-facing message is written straight from it and the outcomes need
different messages AND different retry decisions:

| verdict | from | transient? |
| --- | --- | --- |
| `Unreachable` | transport failure (refused / DNS / timeout / TLS) | yes |
| `ServerError(code)` | HTTP 5xx | yes |
| `RateLimited` | HTTP 429 | yes |
| `NoKeyForDisc` | HTTP 422 | **no** |
| `NotLicensed` | HTTP 404 | no |
| `Unexpected(code)` | any other non-2xx | no |
| `Answered` | 2xx/3xx, or any probe that got a status | no |
| `NotAsked` | URL empty / wrong scheme / SSRF-blocked | no |

`NoKeyForDisc` is the one that motivated the split. The key service answers
422 "licensed but unresolved" only after exhausting every candidate source
(disc-keyed, device keys, brute UK/VK/MK/PK — ~30s in the observed case), so
it is the most *definitive* answer the service can give. Reporting it as an
outage ("the service was down, not the disc — wait a few minutes and try
again") both misstates the cause and sends the operator into an endless
retry on a disc that will never resolve.

### Why the library's error code can't carry this

`freemkv-keysources::classify_http_status` maps 401/403 to
`KeyServiceUnauthorized`, 429 to `KeyServiceRateLimited`, and **everything
else** — 400, 404, 422, 5xx alike — to `KeyServiceUnavailable` (E7028).
E7028's own documentation says it means "the source never got as far as
answering the question", which is precisely what a 422 is NOT. So the error
type autorip receives cannot represent the distinction, and rendering
E7028's catalog text is what produced the wrong message. `ServiceReachability`
is built from `DecodeReachability::Status(u16)`, which DOES carry the status,
so autorip classifies from that and only falls back to the error code when no
HTTP status is available.

### Probe verdicts are deliberately coarser than decode verdicts

`classify_reachability` (the probe) never returns `NoKeyForDisc`,
`NotLicensed` or `Unexpected`. The probe POSTs an EMPTY body and names no
disc, so its 422/404 says only "the service is up"; reading a per-disc
verdict out of a disc-less request would recreate the same conflation in
mirror image. `reachability_from_decode` — describing a POST that DID carry
the disc — owns the per-disc arms.

## `reachability_for_unprobeable_url`: two failure shapes, opposite verdicts

`validate_fetch_url` fails for two unrelated reasons and the probe used to
answer `Up` to both:

* A permanent verdict on the URL — empty, not http(s), no host, or an
  address the SSRF guard blocks. The online source was already dropped for
  such a URL, so `NotAsked` is right: terminal (calling it an outage would
  park every disc forever on a config mistake) but named for what actually
  happened — nothing was ever sent — rather than borrowing "no key".
* A failed LOOKUP — DNS timed out (including the `MAX_INFLIGHT` fail-fast),
  the resolver errored, or the host resolved to nothing. That is the same
  evidence `ProbeOutcome::Transport` is built from: we never reached the
  service, so it is `Unreachable`. Calling it terminal made a DNS blip
  finalise a rippable disc as permanently keyless, which is precisely what
  the transient path exists to prevent — the disc parks, retries, and rips
  when the network returns.
