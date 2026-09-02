# Observability internals (src/observe.rs)

## Why tracing exists

Pre-0.13 the codebase had ~60 silent failure paths (`Err(_) => continue`,
`let _ = …`, `unwrap_or_default`) and ad-hoc `eprintln!` for the rest.
Diagnosing "No drives detected" required reading source + poking `/proc`
+ reading `/sys` because the running process produced zero observable
evidence of the poll loop's decisions. Tracing fixes that structurally:
every silent path is replaced by a structured event, every lifecycle
function is wrapped in a span, and the JSONL stream is machine-queryable.

## FILTER_OFF

`autorip=info` for the binary's own narration, `libfreemkv=warn` so the
library is quiet — warnings + errors only. Two modes, one flag: prod =
warnings only; dev = full debug (see FILTER_ON).

## FILTER_ON

`debug` globally, plus `mux=debug` and `stream=debug` so the
`target: "mux"` / `target: "stream"` events (writeback seeks,
WAIT_AFTER latency, fill_extents stalls — the events the user actually
wants when diagnosing a jumpy mux) are visible. `freemkv::scsi` /
`freemkv::disc` inherit `libfreemkv=debug` here so SCSI CDB events
surface at debug (not the per-CDB trace firehose — ~800 lines/sec
during sweep would drown the useful signal and could itself slow
throughput via stdout contention; raise to `freemkv::scsi=trace` via
AUTORIP_LOG_LEVEL if you need per-CDB forensics for a drive issue).
Producer's per-frame log is at `trace` and stays muted here on purpose.

libfreemkv namespaces its events on `freemkv::*` targets (not
`libfreemkv::*`), so the `freemkv=debug` directive — NOT
`libfreemkv=debug` — is what surfaces `freemkv::scan` phase markers,
`freemkv::heartbeat` liveness beats, `freemkv::css/disc/drive/scsi`,
etc. Both directives are kept: `libfreemkv=debug` covers any event that
uses the bare crate target.

## init() concurrency contract

Contract: call exactly once, early in `main`, before any threads are
spawned. The leading `GUARDS.get().is_some()` check makes a
*sequential* second call a no-op, but it is not a synchronization
barrier — two concurrent callers could both pass the check, and the
loser's `tracing_subscriber::...init()` would then panic (global
subscriber already installed). Single-call-from-main is the contract,
not a thread-safe guard.

No `Result`: log-dir creation is best-effort (`create_dir_all` errors
are ignored — the appenders below will simply fail to write into a
missing dir), the file appenders are lazy (no file is opened at init
time, so there is no open-time error to surface), and the stderr layer
is always in the stack — events surface in `docker logs` regardless of
whether the file sinks can be written.
