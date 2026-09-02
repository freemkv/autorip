# join_bounded test rationale

`join_bounded` is the only thing between a wedged mover/muxer thread and
either hanging shutdown forever or abandoning an in-flight file move before
it finishes. Both failure directions matter: hanging keeps the container
from restarting, and abandoning early can leave the operator's media
half-moved.

It had no test at all, and a mutation run flipped every part of it. Note the
`delete !` mutant is the subtle one — it skips the loop body entirely and
falls through to an unconditional `join()`, silently turning the bounded
join back into an unbounded one. A healthy-thread test cannot see that,
because joining works fine there. Only timing a thread that outlives its
deadline exposes it.
