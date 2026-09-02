# `disc_variant`

THE "another disc of the same title" naming rule, in one place.

`tmdb::clean_title` strips "disc 1".."disc 4" before the TMDB lookup, so
every disc of a boxset resolves to ONE title and wants ONE name — for its
staging directory AND for its delivered file. Both need the same answer:
keep the plain title when it is free or already this disc's, and step to
`_2`, `_3`, ... when it belongs to a DIFFERENT disc.

This yields the variant numbers (1, 2, 3, ... `MAX_DISC_VARIANTS`) and
returns the first one `claimable` accepts. Callers supply what "claimable"
means in their domain — a staging dir carrying this disc's `.disc-label`, a
library file whose bytes are this disc's output — and render the number with
`disc_variant_name`. `None` means every variant is taken by some other
disc; callers must treat that as an error, never as licence to overwrite.

One function on purpose. Staging naming and output naming are the same
policy, and the original bug (disc 2 silently skipped as "already ripped")
exists because that policy was open-coded at ten call sites, so hardening
any one of them fixed nothing.
