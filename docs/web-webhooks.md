# Webhook URL masking and resolution (`src/web.rs`)

## `resolve_webhook_entries`

Resolves an incoming `webhook_urls` array (from POST /api/settings) against
the currently-stored entries, replacing each redacted URL placeholder with its
real (token-bearing) value while preserving the per-entry `post_rip`/
`post_mux`/`post_move` flags the client sent. Only the URL is ever masked, so
the flags always come straight from `incoming`.

URL matching is BY STABLE `#idx` (falling back to origin prefix), never by
array position: the UI can delete or reorder rows between GET and POST, so a
positional match would bind a masked entry to a different stored secret.

- A masked URL whose `#idx` (or, for older clients, origin) resolves to
  exactly one stored entry takes that entry's real URL.
- A non-masked URL is taken verbatim (a newly-entered secret).
- `Err(url)` is returned when a masked URL is ambiguous — it matches 0 stored
  entries (the row it referred to was deleted) or >1 (two stored hooks share
  an origin) — so the caller can reject the save instead of guessing.
- Entries with a blank/whitespace URL are dropped.

## Resolution timing in `handle_settings_post`

webhook_urls are intentionally NOT SSRF-validated (unlike keydb_url,
keyserver_url, network_target) — a webhook is a fire-and-forget
notification, and pointing one at a LAN service (Home Assistant, a NAS) is
the intended use (see `webhook_agent`).

Masked placeholders are resolved BEFORE the config write guard is taken: it
used to run INSIDE `cfg.write()`, so a rejected ambiguous entry left ~20
earlier fields already mutated. Resolution is pure string matching, so it's
cheap to do here.

Race note: `cfg.read()` here and `cfg.write()` later never overlap; a
racing POST at worst gives last-write-wins on a resolution computed one
snapshot earlier — it never binds a masked entry to the WRONG secret.
