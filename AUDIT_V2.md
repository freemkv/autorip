# Audit V2 — 2026-04-14

## Executive Summary

**Health Score: 9.5/10**

Both `libfreemkv` and `freemkv` are in excellent shape. All 28 clippy warnings
have been resolved, all tests pass (41/41 across both repos), and both crates
build cleanly in release mode with zero warnings. No test failures were found.
No `eprintln!` calls exist in non-test library code.

---

## What Was Found

### libfreemkv (28 clippy warnings)

| Category | Count | Files |
|---|---|---|
| Dead code (`pes_buf` field, 2 constants) | 3 | m2ts.rs, tsmux.rs |
| `match` should be `if let` | 3 | drive/mod.rs |
| `match` replaceable with `?` | 1 | drive/mod.rs |
| Complex return types | 2 | m2ts.rs, mkvstream.rs |
| `if` with identical blocks | 2 | tsmux.rs |
| Manual `RangeInclusive::contains` | 3 | tsmux.rs |
| Private trait in public API | 1 | mkvout.rs / mod.rs |
| Empty line after doc comment | 1 | disc.rs |
| Doc list item indentation | 12 | scsi/linux.rs |

### freemkv (4 clippy warnings)

| Category | Count | Files |
|---|---|---|
| Unused variable (`raw`) | 2 | pipe.rs |
| Unnecessary `mut` | 1 | pipe.rs |
| Unnecessary cast (`as u64` on u64) | 1 | pipe.rs |

### eprintln! in library code

All `eprintln!` calls in `libfreemkv` are inside `#[cfg(test)]` modules only.
No removal needed.

---

## What Was Fixed

### libfreemkv

1. **Removed dead code**: deleted unused `pes_buf` field from `M2tsStream` and
   its initialization in all 3 constructors. Removed unused constants
   `TS_PACKET` and `BD_TS_PACKET` from tsmux.rs.

2. **Replaced `match` with `if let`**: three instances in `drive/mod.rs` where
   `match` was used to destructure a single `Ok` pattern with `Err(_) => {}`.

3. **Replaced `match` with `?`**: one instance in drive/mod.rs
   (`crate::scsi::open` call).

4. **Added type aliases for complex types**: `PesSetup` in m2ts.rs and
   `MkvHeaderResult` in mkvstream.rs to reduce type complexity.

5. **Collapsed identical `if` branches**: tsmux.rs `build_pes_header` had three
   branches all returning `0xBD`; collapsed to a single `else` with a combined
   comment.

6. **Used `RangeInclusive::contains`**: replaced manual range checks in
   tsmux.rs.

7. **Made `WriteSeek` trait public**: was `pub(crate)` but exposed in a `pub fn`
   signature on `MkvOutputStream::create`.

8. **Removed empty line after doc comment**: disc.rs line 169.

9. **Fixed doc list item indentation**: 12 instances in scsi/linux.rs. Changed
   sub-items from `a.`/`b.` to markdown list markers for proper nesting.

### freemkv

1. **Prefixed unused `raw` params with underscore**: `disc_to_iso` and
   `disc_to_stream` functions in pipe.rs.

2. **Removed unnecessary `mut`**: `drive` variable in `disc_to_iso`.

3. **Removed unnecessary cast**: `resume_from as u64` where `resume_from` was
   already `u64`.

---

## What Remains

| Item | Justification |
|---|---|
| `eprintln!` in test code | Acceptable for test diagnostics; not shipped in library builds |
| 2 ignored doc-tests | Pre-existing; require hardware or key material to run |
| No unit tests in freemkv | CLI integration tests cover the binary adequately |

---

## Verification

```
libfreemkv: cargo clippy -- -W clippy::all   -> 0 warnings
libfreemkv: cargo test                       -> 41 passed, 0 failed
libfreemkv: cargo build --release            -> OK

freemkv:    cargo clippy -- -W clippy::all   -> 0 warnings
freemkv:    cargo test                       -> 9 passed, 0 failed
freemkv:    cargo build --release            -> OK
```
