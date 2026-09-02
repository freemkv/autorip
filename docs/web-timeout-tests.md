# ureq timeout regression tests (`src/web.rs` test module)

## `a_slow_but_progressing_keydb_body_is_not_killed_by_the_header_deadline`

A KEYDB body that is SLOW but PROGRESSING must be allowed to finish.

ureq 2's `timeout_read` was a per-read bound, so a slow transfer never
tripped it. ureq 3's `timeout_recv_response` — what the 2→3 migration
replaced it with — is an ABSOLUTE deadline anchored at header completion
that also caps the body, so `guarded_agent`'s 30s became a hard 30s ceiling
on the whole download. Measured against a real socket: with a 2s bound, a
server trickling a byte every 500ms dies at 2.0s having delivered four
bytes.

NOTE what this does NOT prove: delete `timeout_recv_body` entirely and this
test still passes, because with no body timeout at all the slow body also
arrives. Removal is caught by its sibling
(`a_stalled_body_is_cut_off_by_the_idle_bound_not_the_total_budget`), which
was proven red at 30s and green at 1s. This one guards the complementary
property, and the two are only meaningful together.

What this guards is the NEW idle knob: `timeout_recv_body` is ROLLING, so a
body that keeps arriving must survive even when the transfer takes many
times the idle bound. Wire it up as a total instead — the easy mistake —
and this test fails. It is a guard on the fix, not a reproduction of the
original defect: the budget change itself (`guarded_get`'s 30s →
`KEYDB_TRANSFER_BUDGET`) has no automated proof here, because `guarded_get`
resolves and rejects loopback before it connects, so no local listener can
stand in for a keydb mirror.

## `a_stalled_body_is_cut_off_by_the_idle_bound_not_the_total_budget`

The other half: a peer that sends headers and then NOTHING must be cut off
by the rolling idle bound, not held until the (much larger) total budget
expires. This is the protection ureq 2's `timeout_read` gave and the
migration dropped; without `timeout_recv_body` a dead peer would be held
for the whole KEYDB budget.
