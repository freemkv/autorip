# Fail-open lock-poison regression net

## `no_fail_open_lock_poison_forms_in_src` (`src/main.rs` test module)

A round-4 regression net: it fails if ANY fail-open lock-poison form
reappears in non-test code. It greps every non-test `.rs` under `src/` for
all of the known forms at once, so the next syntactic variant can't slip
through.

Each prior audit pass converted the obvious form of the day and missed
another SYNTACTIC form — first `.lock().ok()`, then
`match … Err(_) => <default>`, then `if let Ok(..) = X.lock()`, then
`.map(…).unwrap_or_default()`. Greppping for all of them together is what
closes that recurring gap.

Correct handling is recover-via-`unwrap_or_else(|e| e.into_inner())`. The
surfaced-HTTP-500 / logged-retry handlers use `match … Err(_) => …` (no
`let Ok`, no `.ok()`), which is deliberately NOT matched here. Test code is
stripped first, so a `#[cfg(test)]` block that mirrors a production
`if let Ok` for a poison-recovery unit test is exempt.
