# `tests/end_to_end.rs` — why route dispatch isn't tested here

Route dispatch and device-name validation used to be covered by this file
via a hand-rolled `dispatch`/`Route` replica with its own copy of
`is_valid_device_name`. That replica had drifted from production: the
replica required `starts_with("sg")`, but production accepts any
ASCII-alphanumeric device, so `/api/stop/sr0` is valid in production but
the replica rejected it. The replica's SSE branch also accepted both
`/api/sse` and `/events`, passing regardless of which route production
actually served.

Those concerns are now driven against the real `handle_request` in the
in-crate `web::web_tests::http` module (it pins `/events` as the served
SSE route and exercises the real validator), so the drift-prone replica
was deleted rather than maintained.
