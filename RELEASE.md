# Release Process

> **Unified releases:** autorip ships at the same version as libfreemkv /
> keysources / freemkv / bdemu. Use the one-shot release orchestrator
> (see the main `freemkv/RELEASE.md` for the full fast-release model).
> autorip git-tag-pins libfreemkv +
> keysources via a committed `[patch.crates-io]`, so its build does NOT wait on
> crates.io — it starts the instant the lib tags exist. The steps below are the
> autorip-only tag/deploy view.

## Quick Reference

```bash
# 1. Pre-commit (run locally, Rust 1.97 — matches CI)
# --all-targets because ci.yml lints test code too; a warning reachable only
# from a #[cfg(test)] module passes without it and fails on push.
cargo +1.97 fmt --check && cargo +1.97 clippy --all-targets -- -D warnings && cargo +1.97 test

# 2. Tag and push a RELEASE tag. NOT an -rc tag: release.yml filters those
#    out ("!v*-rc*"), so pushing v1.0.0-rc.1 by hand triggers nothing at all.
#    Every push to `qa` already stamps its own v<version>-rc<N> automatically
#    (qa.yml's rc-tag job) purely so a run can be named.
git tag -a v1.0.0 -m "v1.0.0"
git push origin v1.0.0

# 3. Wait for CI + Release (~3 min)
gh run list --repo freemkv/autorip --limit 1

# 4. Pull the new image on your deployment host
docker compose pull && docker compose up -d
```

## Detailed Steps

### Step 1: Pre-commit Locally

Run lint + tests before pushing:

```bash
# Whole workspace (Rust 1.97 — matches CI)
cargo +1.97 fmt --check && cargo +1.97 clippy --all-targets -- -D warnings && cargo +1.97 test

# Single crate
cargo +1.97 clippy -p freemkv-autorip --all-targets -- -D warnings && cargo +1.97 test -p freemkv-autorip
```

This runs:
- `cargo fmt --check`
- `cargo clippy -- -D warnings`
- `cargo test`

### Step 2: Commit and Tag

```bash
# Commit changes
git add -A
git commit -m "description"

# Tag with semver — a RELEASE tag, not an -rc one
git tag -a v1.0.0 -m "v1.0.0"

# Push commit AND tag
git push origin main v1.0.0
```

**Important:** Push the tag! release.yml only runs when a tag is pushed, not on
every commit — and it runs only for RELEASE tags. Its trigger is `v*` minus
`!v*-rc*`, so a hand-pushed `v1.0.0-rc.1` fires nothing: no verify, no test, no
build, no image. The `-rc` tags exist so a `qa` run can be named, and qa.yml
stamps them for you.

### Step 3: Wait for CI

```bash
# Check status
gh run list --repo freemkv/autorip --limit 1
```

Flow (fast-release — test is a parallel tripwire, NOT a gate):
```
push tag → verify → { test (parallel),  build matrix → docker → GHCR }
```
`build` does NOT `needs: test`; the docker image starts as soon as the x86_64
musl build leg finishes. Tag → image is typically ~3 min.

### Step 4: Deploy

Pull the new image on your deployment host:

```bash
cd /path/to/your/autorip/compose
docker compose pull
docker compose up -d
```

## Troubleshooting

### Release didn't build
- Check the tag was pushed: `git tag` and `git push origin <tag>`
- CI must pass before Release runs

### Container still running old version
- Force pull: `docker compose pull`
- Force restart: `docker compose up -d`

## GitHub Actions Status

| Workflow | Trigger | Pushes to GHCR? |
|----------|---------|----------------|
| CI | Every push | No |
| Release | Tag push | Yes (`latest` + tag) |

## Image Tags

| Push | Image |
|------|-------|
| `main` branch | Not built |
| `v*-rc*` tag (auto-stamped by a `qa` push) | Not built — release.yml excludes these |
| `v*` release tag | `ghcr.io/freemkv/autorip:latest` + the tag |
