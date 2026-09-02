# `realistic_mid_range_duration_survives_unclamped` rationale

A realistic operator-configured duration comfortably under the 30-day
ceiling but well above an hour must survive `load_saved` UNCLAMPED.
Unlike `load_saved_clamps_pathological_durations` (which feeds
`u64::MAX` and asserts against the SAME `30 * 24 * 3600` literal the
production code uses — so a collapsed constant still passes
tautologically), this pins an absolute value: if `MAX_DURATION_SECS`
ever regressed to something near an hour (e.g. `30 + 24 + 3600`,
~61 minutes), the UHD default of 8h (28800s) — or this test's 6h —
would get silently clamped down and this assertion would catch it.
