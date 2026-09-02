# Cross-origin POST defense (`is_cross_origin_post` / `is_cross_origin`)

Lightweight cross-origin defense for state-changing POST routes in `src/web.rs`.

This service is intentionally unauthenticated on the LAN and is driven both by a
browser dashboard and by operator `curl`/monitoring scripts (which send no Origin
header). So the policy is deliberately permissive: if an `Origin` (or, failing
that, `Referer`) header is PRESENT and its host does NOT match the request's Host
header, reject with 403. If no such header is present we ALLOW the request, so
curl and monitoring keep working. This is defense-in-depth against a browser on
the same LAN being used to forge state-changing requests (CSRF); it is not an
authentication mechanism.

`is_cross_origin` is the pure decision function over the raw `Origin`/`Referer`
and `Host` header values, returning `true` when the request should be rejected:
- Absent/empty Origin → allow (curl/monitoring).
- Unparseable Origin or absent Host → can't prove cross-origin, so allow.
- Origin carries the scheme, which fixes the default port so the schemeless
  Host header normalizes to match it — otherwise `http://host` wouldn't match
  `host:80`, falsely 403'ing a same-origin request.
