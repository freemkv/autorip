# Release Process

## Quick Reference

```bash
# 1. Pre-commit (run locally)
cd freemkv && freemkv-private/scripts/precommit.sh [libfreemkv|autorip]

# 2. Tag and push
git tag -a v0.13.27 -m "v0.13.27"
git push origin v0.13.27

# 3. Wait for CI + Release (~3 min)
gh run list --repo freemkv/autorip --limit 1

# 4. Watchtower pulls automatically (~30 min)
#    Or force restart on classe:
cd /srv/media-autorip && sudo docker compose up -d

# 5. Verify
curl http://rip.docker.internal.pq.io/api/state
```

## Detailed Steps

### Step 1: Pre-commit Locally

Run lint + tests before pushing:

```bash
# All crates
freemkv-private/scripts/precommit.sh

# Single crate
freemkv-private/scripts/precommit.sh autorip
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
git tag -a v0.13.27 -m "v0.13.27"

# Push commit AND tag
git push origin main v0.13.27
```

**Important:** Push the tag! Release workflow only runs when a tag is pushed, not on every commit.

### Step 3: Wait for CI

```bash
# Check status
gh run list --repo freemkv/autorip --limit 1
```

Flow:
```
push tag → CI workflow (lint + test) → Release workflow (docker build + push)
```

Expected: ~3 min total.

### Step 4: Deploy to Server

Watchtower on classe auto-pulls every ~30 min. To force immediate update:

```bash
# SSH to docker server
ssh docker.internal.pq.io

# Pull latest and restart
cd /srv/media-autorip
sudo docker compose pull
sudo docker compose up -d
```

Or just restart:
```bash
sudo docker compose restart
```

### Step 5: Verify

```bash
# Check API responds
curl http://rip.docker.internal.pq.io/api/state

# Check version (if exposed)
curl http://rip.docker.internal.pq.io/api/system
```

## Troubleshooting

### Release didn't build
- Check the tag was pushed: `git tag` and `git push origin v0.13.27`
- CI must pass before Release runs

### Container still running old version
- Force pull: `sudo docker compose pull`
- Force restart: `sudo docker compose up -d`

### Can't SSH to docker server
- Use Portainer UI: https://portainer.docker.internal.pq.io
- Or ask for SSH access

## GitHub Actions Status

| Workflow | Trigger | Pushes to GHCR? |
|----------|---------|----------------|
| CI | Every push | No |
| Release | Tag push | Yes (`latest` + tag) |

## Image Tags

| Push | Image |
|------|-------|
| `main` branch | Not built |
| `v0.13.27` tag | `ghcr.io/freemkv/autorip:0.13.27` |
| Tag push | `ghcr.io/freemkv/autorip:latest` + tag |