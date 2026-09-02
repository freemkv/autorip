# Why `lib.rs` duplicates `SHUTDOWN` and `VERSION_LABEL`

Several modules reference `crate::SHUTDOWN` and `crate::VERSION_LABEL`
directly, so the lib crate has to provide its own copies rather than
importing them from the bin. `src/main.rs` still owns the authoritative
`SHUTDOWN` and builds `VERSION_LABEL` via `build.rs`; the lib copies only
matter for integration tests, which never actually drive the long-running
loops that read `SHUTDOWN`.
