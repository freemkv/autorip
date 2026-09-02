# Poisoned-STATE recovery test (`src/web.rs` test module)

## `get_state_json_recovers_a_poisoned_state_lock`

Catches the mutation that restores `get_state_json`'s `Err(_) => return
"{}"` bail-out on a poisoned STATE.

A source-pin, like the handler pins elsewhere in this module, because the
only way to exercise the behaviour is to poison the process-global
`ripper::STATE`, and a `Mutex` stays poisoned for the life of the process —
it would panic every other test in this binary that locks STATE with
`unwrap`.

What it pins is the whole point of the defect: this was the ONE STATE
consumer that abandoned on poison instead of recovering the guard like its
ten siblings. STATE is poisoned by the first panic taken while its guard is
held, so from that moment `GET /api/state` answered `{}` with HTTP 200
forever: a blank dashboard, and — because `main.rs::run_healthcheck` only
checks for an `HTTP/1.1 200` status line — a permanently green Docker
HEALTHCHECK that never restarts the container. The map's contents are still
perfectly readable; serving them is both correct and the house convention.
