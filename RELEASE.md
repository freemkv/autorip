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
# 1. Pre-commit (run locally, Rust 1.86 — matches CI)
cargo +1.86 fmt --check && cargo +1.86 clippy --locked -- -D warnings && cargo +1.86 test

# 2. Tag and push
git tag -a v1.0.0-rc.1 -m "v1.0.0-rc.1"
git push origin v1.0.0-rc.1

# 3. Wait for CI + Release (~3 min)
gh run list --repo freemkv/autorip --limit 1

# 4. Pull the new image on your deployment host
docker compose pull && docker compose up -d
```

## Detailed Steps

### Step 1: Pre-commit Locally

Run lint + tests before pushing:

```bash
# Whole workspace (Rust 1.86 — matches CI)
cargo +1.86 fmt --check && cargo +1.86 clippy --locked -- -D warnings && cargo +1.86 test

# Single crate
cargo +1.86 clippy -p freemkv-autorip --locked -- -D warnings && cargo +1.86 test -p freemkv-autorip
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

# Tag with semver
git tag -a v1.0.0-rc.1 -m "v1.0.0-rc.1"

# Push commit AND tag
git push origin main v1.0.0-rc.1
```

**Important:** Push the tag! Release workflow only runs when a tag is pushed, not on every commit.

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
| `v1.0.0-rc.1` tag | `ghcr.io/freemkv/autorip:v1.0.0-rc.1` |
| Tag push | `ghcr.io/freemkv/autorip:latest` + tag |
