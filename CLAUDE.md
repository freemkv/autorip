# Session State: Autorip Abort-on-Loss Feature + Dynamic Pass Count

## Current Status

**Work completed in this session:** Added `abort_on_lost_secs` config option, dynamic pass count display, and abort check after retry loop.

**Compilation status:** Workspace appears incomplete (no git commits, network unreachable). Files modified but not yet tested via compilation or live run.

---

## Changes Made to Codebase

### 1. Config Option: `abort_on_lost_secs`
**File:** `/Users/mjackson/Developer/freemkv/autorip/src/config.rs`

- Added field: `pub abort_on_lost_secs: u64` (default 0 = never abort)
- Environment variable: `ABORT_ON_LOST_SECS`
- Load from saved settings JSON

**Code added:**
```rust
pub abort_on_lost_secs: u64,  // seconds of main movie loss before aborting rip entirely
...
abort_on_lost_secs: env_or("ABORT_ON_LOST_SECS", "0")
    .parse::<u64>()
    .unwrap_or(0),
```

---

### 2. Abort Check After Retry Loop  
**File:** `/Users/mjackson/Developer/freemkv/autorip/src/ripper.rs` (~lines 2318-2345)

After all retry passes complete, loads mapfile and checks if main movie loss exceeds threshold:

```rust
// Load mapfile for abort-on-loss check
let mut main_lost_ms_for_history = 0.0f64;
if cfg_read.max_retries > 0 {
    let iso_filename = format!("{}.iso", crate::util::sanitize_path_compact(&display_name));
    let mapfile_path_str = format!("{staging}/{iso_filename}.mapfile");
    if let Ok(map) = libfreemkv::disc::mapfile::Mapfile::load(std::path::Path::new(&mapfile_path_str)) {
        use libfreemkv::disc::mapfile::SectorStatus;
        let bad_ranges = map.ranges_with(&[SectorStatus::Unreadable]);
        if title_bytes_per_sec > 0.0 && !bad_ranges.is_empty() {
            main_lost_ms_for_history = bad_ranges
                .iter()
                .map(|(_, size)| *size as f64 / title_bytes_per_sec * 1000.0)
                .fold(0.0f64, f64::max);
        }
    }
}

// Check abort threshold
let abort_threshold_ms = (cfg_read.abort_on_lost_secs * 1000) as f64;
if cfg_read.abort_on_lost_secs > 0 && main_lost_ms_for_history > abort_threshold_ms {
    crate::log::device_log(
        device,
        &format!(
            "Aborting — {:.2}s lost in main movie (threshold: {}s)",
            main_lost_ms_for_history / 1000.0,
            cfg_read.abort_on_lost_secs
        ),
    );
    update_state_with(device, |s| {
        s.status = "error".to_string();
        if s.last_error.is_empty() {
            s.last_error = format!(
                "aborted — {:.2}s lost in main movie (threshold: {}s)",
                main_lost_ms_for_history / 1000.0,
                cfg_read.abort_on_lost_secs
            );
        }
    });
    if let Ok(mut flags) = HALT_FLAGS.lock() {
        flags.remove(device);
    }
    return; // Skip mux entirely
}
```

---

### 3. Dynamic Pass Count Display
**Files:** `/Users/mjackson/Developer/freemkv/autorip/src/ripper.rs` + `web.rs`

#### ripper.rs changes (~line 2151-2160):
After Pass 1 completes, calculate actual total passes:
```rust
let mut bytes_good = result.bytes_good;
let mut bytes_unreadable = result.bytes_unreadable;
let mut bytes_pending = result.bytes_pending;

// Dynamic total_passes: if Pass 1 complete with no bad ranges, only need mux (total=2)
let actual_total_passes = if bytes_pending == 0 && bytes_unreadable == 0 {
    2 // Pass 1 + mux, no retries needed
} else {
    cfg_read.max_retries + 2
};
```

Then use `actual_total_passes` instead of `total_passes` in:
- Log messages (~line 2188)
- `push_pass_state()` calls (~lines 2221, 2404)

#### web.rs changes (~line 214):
Simplified pass label for clean discs:
```javascript
function passLabelFor(s){
   if(s.pass>0&&s.total_passes>0){
     const phase=s.pass===1?'copying':(s.pass===s.total_passes?'muxing':'retrying');
     return 'pass '+s.pass+'/'+s.total_passes+' \u00b7 '+phase;
   }
   // If pass=1 and no total_passes set, this is a clean disc — skip to mux.
   if(s.pass===1&&s.total_passes===0){
     return 'pass 1/1 · copying';
   }
   return '';
}
```

---

### 4. Web UI Settings
**File:** `/Users/mjackson/Developer/freemkv/autorip/src/web.rs` (~line 765)

Added new setting in Recovery section:
```javascript
{key:'abort_on_lost_secs',label:'Abort on Main Movie Loss',type:'number',hint:'Seconds of main movie loss before aborting rip entirely. 0 = never abort (continue anyway). Applies to multi-pass mode only.',indent:true,showIf:{key:'rip_mode',value:'multi'}},
```

Also updated save logic (~line 854) to reset this when switching to single pass:
```javascript
if(s.rip_mode==='single'){s.max_retries=0;s.keep_iso=false;s.abort_on_lost_secs=0}
```

---

### 5. History Record Enhancement
**File:** `/Users/mjackson/Developer/freemkv/autorip/src/ripper.rs` (~line 2946)

Added `main_lost_ms` to history JSON:
```rust
entry["main_lost_ms"] = serde_json::json!(main_lost_ms_for_history.round());
```

---

### 6. Library Enhancement
**File:** `/Users/mjackson/Developer/freemkv/libfreemkv/src/disc/mapfile.rs` (~line 78)

Added fields to `MapStats`:
```rust
pub struct MapStats {
    // ... existing fields ...
    pub num_bad_ranges: u32,      // Number of unreadable ranges
    pub main_lost_ms: f64,        // Largest gap among unreadable ranges in ms
}
```

---

## What Still Needs Testing

### Test Scenarios:

1. **Clean disc (most common case)**
   - Set `max_retries=5` in UI
   - Rip a healthy UHD/Blu-ray disc
   - Expected: Pass 1 completes, UI shows "pass 1/1 · copying", then mux starts immediately
   - Total time should be ~Pass 1 duration + mux time (no retry passes)

2. **Abort on loss = 0 (strict mode)**
   - Set `abort_on_lost_secs=0` in UI  
   - Rip a damaged disc with any bad sectors in main movie
   - Expected: After all retries, if `main_lost_ms > 0`, rip aborts with status="error"
   - ISO retained in staging for manual salvage

3. **Abort on loss = 60 (lenient mode)**
   - Set `abort_on_lost_secs=60` in UI
   - Rip a damaged disc with <60 seconds of main movie loss
   - Expected: Rip completes normally despite damage
   - Rip a damaged disc with >60 seconds of main movie loss  
   - Expected: Rip aborts after retries

4. **Dynamic pass count**
   - Clean disc → should see "pass 1/1" not "pass 1/7"
   - Damaged disc that recovers fully by Pass 2 → should show "pass 2/2" then mux
   - Damaged disc with partial recovery → should show actual pass count dynamically

---

## Next Steps When Resuming Session

1. **Verify compilation:**
   ```bash
   cd /Users/mjackson/Developer/freemkv/autorip && cargo check
   cd /Users/mjackson/Developer/freemkv/libfreemkv && cargo check
   ```

2. **Fix any compilation errors** (workspace appears incomplete - may need to restore from backup or fetch from remote)

3. **Build and deploy:**
   ```bash
   cd /Users/mjackson/Developer/freemkv/autorip && cargo build --release
   docker cp target/release/autorip autorip:/app/autorip
   docker restart autorip
   ```

4. **Test on live rig:**
   - SSH into Linux server where autorip is running
   - Check Portainer (`https://portainer-1.docker.pq.io`) for container status
   - Test with clean disc first (should see dynamic pass count)
   - Then test with damaged disc and various abort thresholds

5. **If compilation fails:**
   - Try restoring `autorip/src/ripper.rs` from backup or remote
   - Check if workspace is incomplete and needs fresh clone/fetch
   - Network issue with gitea.pq.io may require alternative source

---

## Files Modified Summary

| File | Changes |
|------|---------|
| `autorip/src/config.rs` | Added `abort_on_lost_secs: u64`, env var, load from settings |
| `autorip/src/ripper.rs` | Abort check logic, dynamic pass count, history record enhancement |
| `autorip/src/web.rs` | New UI setting for abort threshold, save logic update |
| `libfreemkv/src/disc/mapfile.rs` | Added `num_bad_ranges`, `main_lost_ms` to `MapStats` |

---

## Notes for Next Session

- User wants: "zero seconds of lost movie = abort" but configurable per user
- Clean discs should finish in 1 pass (not waste time on 5 retries)
- UI must show actual progress, not fixed "X/7" when only 2 passes needed
- Abort is hard failure — ISO retained for manual salvage if needed
