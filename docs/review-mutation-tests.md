# review.rs: mutation-testing rationale for two guard tests

## `list_held_excludes_dir_with_neither_review_nor_done`

`list_held`'s OR-guard has no dedicated test for a dir with NEITHER
`.review` NOR `.done` — e.g. a perfectly normal, still-actively
ripping/muxing staging dir. `lists_only_held_and_resolves`'s "not
held" fixture happens to have `.done` present, which makes two of
the three OR terms simultaneously true — so mutating the guard's
first `||` to `&&` (`!dir.is_dir() && !review.exists()`, merged with
`|| done.exists()`) still excludes that fixture by coincidence (both
merged terms are true anyway). A dir with no markers at all is the
one input that distinguishes a real OR from that mutation: it must
still be excluded from the held list.

## `traversal_guard_rejects_escapes_against_a_real_existing_parent`

The `traversal_guard_rejects_escapes_accepts_dotted_titles` test uses a
NONEXISTENT `staging_root`, so for every "bad" input it can't tell "the
traversal guard rejected this" from "the directory happened not to exist" —
`Path::new(staging_root).join(bad)` also fails `!d.is_dir()` independently,
producing the SAME `Err(_)` either way. Confirmed by hand: mutating the
guard's first `||` to `&&` (so `dir.is_empty() && count() != 1`, leaving only
the non-Normal-component check to actually reject anything) still passes
`traversal_guard_rejects_escapes_accepts_dotted_titles` for EVERY entry,
including `"a/b"` — because `Path::new("a/b").components()` are both
`Normal`, so the surviving third clause doesn't catch it either, yet the
downstream `!d.is_dir()` check does (there is no real
`/nonexistent-staging-root/a/b` directory) and produces an indistinguishable
`Err`.

This test closes that gap: `staging_root` REALLY EXISTS, and a `.review`
marker is planted in its PARENT — the exact directory `".."` and
`"a/b"`-shaped escapes would land on if the guard were bypassed. If the
guard doesn't fire, `resolve` would find a real, `.review`-bearing directory
at that escaped path and proceed to act on it (writing `.done` into the
PARENT of the staging root) instead of erroring — so asserting the SPECIFIC
`"invalid dir"` message, and that the parent is untouched, actually proves
the guard — not a downstream coincidence — produced the rejection.
