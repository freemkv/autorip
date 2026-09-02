# Container healthcheck

`run_healthcheck` replaces the v0.25.5 `curl --fail http://127.0.0.1:8080/api/state`
Docker `HEALTHCHECK` so the deployed image doesn't need `curl` installed —
freeing ~3 MB on the Option C / `FROM scratch` build and removing one more
"why is this here" surface from the runtime image.

It reads the same `PORT` env var the web server binds to (default 8080),
using a 2 s connect timeout and a 2 s read timeout — both well under the
5 s timeout the Dockerfile `HEALTHCHECK` gives us.
