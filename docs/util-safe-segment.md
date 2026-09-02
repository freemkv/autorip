# `ensure_safe_segment`

Make a filtered/trimmed string safe to use as a *single* path segment.

Input is attacker-controllable (disc UDF volume label from physical
media; TMDB title from external HTTP), so the result must never be a
segment that the OS interprets specially:

- empty (`""`) — `Path::join("")` resolves to the parent itself, so a
  `remove_dir_all` on the joined path would wipe the staging/library
  root and every in-progress rip under it.
- `"."` / `".."` / any all-dots run (`"..."`) — directory traversal:
  `join("..")` escapes one level up.
- leading dots — hidden files and broken resume prefix-matching.

Leading dots are stripped; if what remains is empty or consists solely
of dots, a deterministic safe fallback is substituted. Keeping this in
the sanitizers covers every call site rather than each caller patching it.
