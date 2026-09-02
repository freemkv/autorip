# Log retention filename matching

`is_prunable_log_name` deliberately does not use `path.extension() == "log"`,
which is what this check used to be. `tracing-appender`'s daily rotation
writes `autorip.log.2026-05-01`, whose extension is the date — so every
rolled daily was skipped and `log_retention_days` reclaimed only
`device_*.log` and the per-rip logs. The central human-readable log grew for
the container's lifetime while the prune reported zero files removed, on
every install, at the shipped log level.

Matching on the `.log` component instead covers both the live file and every
rolled name, and still excludes `autorip.jsonl` — whose unbounded growth is a
separate, documented decision.
