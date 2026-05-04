# freemkv — multi-disc media ripping toolchain

Rust workspace for optical-disc backup. **autorip** is the production
component (a docker service that auto-detects optical drives and rips
inserted discs); the others are libraries and CLIs it composes.

## Workspace layout

| Crate / dir | Role |
|---|---|
| `autorip/` | Web-orchestrated rip service. `src/{config,ripper,web,log,util}.rs`, `Cargo.toml`, `docker-compose.example.yml`. **Most active code.** |
| `libfreemkv/` | Core library — mapfile, multipass recovery, sector-level retry, AACS decryption |
| `freemkv/` | CLI — disc-info, drive-info, rip, remux, update-keys |
| `bdemu/` | Blu-ray disc emulation (testing) |
| `freemkv-tools/` | Utilities |
| `freemkv-private/scripts/precommit.sh` | The canonical pre-commit (matches CI's Rust 1.86 toolchain) |

## Hot edit files (autorip)

- `src/config.rs` — `Config` struct, env-var parsing, JSON persistence
- `src/ripper.rs` — main rip loop, retry passes, abort-on-loss check
- `src/web.rs` — HTTP handlers, settings UI, POST routes
- `src/log.rs` — `device_log()` per-device logging
- `src/util.rs` — `sanitize_path_compact()` and other helpers

## Build & test

```bash
# Local build
cd autorip && cargo build --release

# Match CI's Rust 1.86 toolchain (catches drift from newer local toolchain)
freemkv-private/scripts/precommit.sh                 # all crates: fmt + clippy + tests
freemkv-private/scripts/precommit.sh autorip         # one crate
freemkv-private/scripts/precommit.sh --no-tests      # quick fmt+clippy only
```

Don't push if precommit fails. Don't `--no-verify`. CI uses Rust 1.86;
the Mac default (e.g. 1.94) silently accepts lints 1.86 rejects.

## Release process — TAG ORDER MATTERS

Critical: **bump `Cargo.toml` BEFORE creating the tag.** The verify CI
job compares Cargo.toml version to git tag and fails on mismatch.
v0.17.2 had this bug and required a delete-and-retag.

## v0.17.3 Deployment Status ✅ SUCCESS

**Completed:** May 4, 2026

- ✅ Implementation: abort check after retry loop in `src/ripper.rs`
- ✅ Fixed semantics: `abort_on_lost_secs=0` means "perfect rip required" (not "never abort")
- ✅ CI passed with formatting fixes (`cargo fmt`)
- ✅ Release workflow completed successfully on commit `800a5cc7`
- ✅ Docker image pushed to GHCR as `ghcr.io/freemkv/autorip:v0.17.3` and `:latest`
- ✅ Watchtower auto-deployed to production (~30s after GHCR push)

**Production Verification:**
```bash
curl https://rip.docker.internal.pq.io/api/version
# Returns: {"version":"0.17.3"}

curl https://rip.docker.internal.pq.io/api/state
# Returns: Detected drives: ['sg4'], disc_present=true, status=idle
```

**Drive Detection:**
- USB optical drive (HL-DT-ST BD-RE BU40N) detected at `/dev/sg4`
- Container running with `privileged: true` and `/dev:/dev` bind mount
- Drive poll loop starting with `drive_count=1`

**Files Modified in v0.17.3:**
1. `src/ripper.rs`: Added abort check after retry loop (lines ~2289-2350)
   - Loads mapfile after all retries complete
   - Calculates main movie loss from Unreadable sectors
   - Aborts if loss > threshold, proceeds with mux otherwise
2. `src/web.rs`: Updated UI hint text to clarify "Max Acceptable Main Movie Loss"
3. CLAUDE.md: Added deployment status and corrected semantics documentation

**Testing Recommendations:**
1. Set `abort_on_lost_secs=0` (perfect rip) or a small value like 5s
2. Rip a damaged disc — it should retry up to max_retries times, then abort if loss > threshold
3. Rip a clean disc — multi-pass exits early when no bad sectors remain, proceeds directly to mux

---

## Release process — TAG ORDER MATTERS (UPDATED)

Critical: **bump `Cargo.toml` BEFORE creating the tag.** The verify CI
job compares Cargo.toml version to git tag and fails on mismatch.
v0.17.2 had this bug and required a delete-and-retag.

```bash
# 1. Bump Cargo.toml + commit + push
cd autorip
# edit Cargo.toml: version = "X.Y.Z"
git -C /Users/mjackson/Developer/freemkv add autorip/Cargo.toml
git -C /Users/mjackson/Developer/freemkv commit -m "vX.Y.Z: bump version"
git -C /Users/mjackson/Developer/freemkv push

# 2. Tag THE COMMIT WITH THE BUMP (use that specific SHA)
git -C /Users/mjackson/Developer/freemkv tag vX.Y.Z <bump_commit_sha>
git -C /Users/mjackson/Developer/freemkv push origin vX.Y.Z
```

CI runs **verify → ci → build → docker**. Watchtower auto-deploys the
new image to production within ~30s of GHCR push.

## Production

- **Service URL**: `https://rip.docker.internal.pq.io`
- **Container**: `media-autorip` on classe (`docker.internal.pq.io`)
- **Image**: `ghcr.io/freemkv/autorip:latest` (also `:vX.Y.Z`)
- **Host SSH**: `ssh docker` (passwordless sudo)

```bash
# Verify version after release (Watchtower lag ~30s)
for i in {1..6}; do
  v=$(curl -s https://rip.docker.internal.pq.io/api/version | jq -r '.version')
  echo "poll $i: v$v"; [ "$v" = "X.Y.Z" ] && break; sleep 30
done

# State (drives, disc_present)
curl -s https://rip.docker.internal.pq.io/api/state | jq .
```

## Container requirements

- **`privileged: true` is REQUIRED** for optical SCSI drive access.
  Without it the container starts fine but `drive_count=0` and the
  UI reports "No drives detected." Verify it's in
  `docker-compose.yml` (line 6 in the example).
- Bind mount `/dev:/dev`.

## Portainer API (container ops without SSH)

Token: `ptr_f8I/jLRmscKjCcA7vbq1DebmTr++3GKxzOYrT07QECo=`

```bash
TOKEN="ptr_f8I/jLRmscKjCcA7vbq1DebmTr++3GKxzOYrT07QECo="
HDR="X-API-Key: $TOKEN"
BASE="https://portainer-1.docker.pq.io/api/endpoints/1/docker"

# Find media-autorip
curl -s -H "$HDR" "$BASE/containers/json?all=true" \
  | jq '.[] | select(.Names == ["/media-autorip"])'

# Tail logs (replace {id})
curl -s -H "$HDR" "$BASE/containers/{id}/logs?stdout=1&stderr=1&tail=100"
```

**Portainer's exec API returns null** — useless for shell access. Use
`ssh docker 'sudo docker exec media-autorip <cmd>'` instead.

## Don't-do list (paid-for lessons)

1. **Don't tag before bumping `Cargo.toml`.** Verify job fails. v0.17.2
   was this bug — delete + retag + force-push needed.
2. **Don't deploy without `privileged: true`.** Drive enumeration silently
   returns 0; UI shows "No drives detected" with no error.
3. **Don't trust Portainer's exec API.** Use SSH + docker exec.
4. **Don't skip precommit.** CI's Rust 1.86 catches what the newer local
   toolchain misses.
5. **`abort_on_lost_secs=0` means "require perfect rip"**, not "never
   abort". Multi-pass mode auto-exits early when bytes_unreadable=0.
   Default is 0 (perfect-required); set to e.g. 30 to tolerate up to
   30s of main-movie loss before aborting after retries exhausted.

## Key feature flags / config

### abort_on_lost_secs v0.17.3 semantics (CORRECTED)

**`abort_on_lost_secs=0`**: Require perfect rip — abort if ANY data loss in main movie after retries exhausted  
**`abort_on_lost_secs=5`**: Tolerate up to 5 seconds of missing data in main movie  
**`abort_on_lost_secs=30`**: Tolerate up to 30 seconds of missing data (default for production)

Only applies in multi-pass mode (`rip_mode = "multi"`). Multi-pass automatically exits early when `bytes_unreadable == 0`.

### rip_mode

- `"single"`: No retries, direct disc→MKV
- `"multi"`: Retry passes + ISO intermediate + abort check after retries

## Quick references

- GHCR: `ghcr.io/freemkv/autorip` (`:latest`, `:vX.Y.Z`)
- GitHub Actions API: `api.github.com/repos/freemkv/autorip/actions/runs`
- License: AGPL-3.0
