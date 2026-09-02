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
has none. A single bounded probe against the configured `keyserver_url`
answers that — and only the `Up` verdict keeps the pre-fix "no keys found"
behaviour.

## `reachability_for_unprobeable_url`: two failure shapes, opposite verdicts

`validate_fetch_url` fails for two unrelated reasons and the probe used to
answer `Up` to both:

* A permanent verdict on the URL — empty, not http(s), no host, or an
  address the SSRF guard blocks. The online source was already dropped for
  such a URL, so a resulting no-key is genuine and `Up` is right: calling
  it an outage would park every disc forever on a config mistake.
* A failed LOOKUP — DNS timed out (including the `MAX_INFLIGHT` fail-fast),
  the resolver errored, or the host resolved to nothing. That is the same
  evidence `ProbeOutcome::Transport` is built from: we never reached the
  service. Reporting `Up` here made a DNS blip finalise a rippable disc as
  permanently keyless, which is precisely what the `Down` path exists to
  prevent — the disc parks, retries, and rips when the network returns.
