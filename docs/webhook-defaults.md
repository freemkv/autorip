# Webhook per-stage default (`default_true`)

`#[serde(default)]` helper: a webhook with an unspecified `post_rip` /
`post_mux` / `post_move` flag fires on that stage. This is the
backward-compat default — before per-stage selection existed every webhook
fired on completion, so an entry that omits a flag (a legacy config, or a
client that doesn't send it) must keep firing. A pre-1.6.8 config that only
carries `post_rip`/`post_move` therefore gains `post_mux = true` on load, so
the mux-stage notification (which used to ride the single `rip_complete`
event) is never silently dropped.
