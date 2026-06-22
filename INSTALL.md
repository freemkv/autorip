# Installing autorip as a bare binary (no Docker)

autorip ships as a **single static binary** (linked against musl, no
shared-library dependencies). Docker remains the recommended path and is
unchanged — but you can also download one file and run it directly as a
daemon. This document covers the bare-metal install: drive permissions,
a systemd unit, and how local paths differ from the Docker setup.

autorip is **Linux-only** — it talks to the kernel SCSI generic layer
(`/dev/sg*`) and udev for live optical-drive detection. There is no
macOS or Windows build.

## 1. Download

Release assets are at
<https://github.com/freemkv/autorip/releases/latest>:

| Asset | Platform |
|-------|----------|
| `autorip-x86_64-linux` | Linux x86_64 (static musl) |
| `autorip-aarch64-linux` | Linux arm64 (static musl) |

Each binary has a matching `<asset>.sha256`. The `*.tar.gz` archives are
still published alongside if you prefer them.

```bash
ASSET=autorip-x86_64-linux        # or autorip-aarch64-linux

curl -sLO "https://github.com/freemkv/autorip/releases/latest/download/${ASSET}"
curl -sLO "https://github.com/freemkv/autorip/releases/latest/download/${ASSET}.sha256"

sha256sum -c "${ASSET}.sha256"    # verify (optional, recommended)

chmod +x "${ASSET}"
sudo mv "${ASSET}" /usr/local/bin/autorip
autorip --version
```

## 2. Drive permissions — do NOT run as root

The Docker image runs `--privileged`; that is a container workaround, not
something you should replicate on the host. For a bare install, grant
the **service user** access to the optical device instead.

The simplest option is the `cdrom` group, which most distros already
attach to `/dev/sr0`:

```bash
sudo usermod -aG cdrom autorip      # service user (see systemd unit below)
```

If your distro does **not** grant the `cdrom` group access to the SCSI
generic node (`/dev/sg*`) — which is what autorip actually opens for
SCSI ioctls — add a udev rule:

```bash
# /etc/udev/rules.d/99-autorip.rules
# Grant the cdrom group read/write on the SCSI generic node behind the
# optical drive. GROUP="cdrom", MODE="0660" — no root, no privileged.
KERNEL=="sg[0-9]*", SUBSYSTEM=="scsi_generic", ATTRS{type}=="5", GROUP="cdrom", MODE="0660"
KERNEL=="sr[0-9]*", SUBSYSTEM=="block", GROUP="cdrom", MODE="0660"
```

`type=="5"` is the SCSI peripheral type for optical drives (CD/DVD/BD),
so the rule only ever touches the disc drive. Reload and re-trigger:

```bash
sudo udevadm control --reload-rules
sudo udevadm trigger
```

Confirm the service user can see the drive:

```bash
sudo -u autorip ls -l /dev/sg* /dev/sr*
```

## 3. Local paths (no in-container NFS auto-mount)

The Docker image bundles `mount.nfs4` and auto-mounts a staging NFS
export inside the container. **A bare install does none of that** — it
uses whatever local (or already-mounted) paths you give it. Mount your
NFS/SMB share with the host's normal `/etc/fstab` or autofs if you want
network staging; autorip just reads and writes ordinary directories.

Configure paths with environment variables (same names as the Docker
README's config table):

| Variable | Suggested bare-install value | Purpose |
|----------|------------------------------|---------|
| `AUTORIP_DIR` | `/var/lib/autorip` | config / history / logs |
| `OUTPUT_DIR` | `/srv/media/output` | finished MKVs |
| `STAGING_DIR` | `/srv/media/staging` | temporary rip / ISO working dir |
| `MOVIE_DIR` | `/srv/media/Movies` | organized library (optional) |
| `TV_DIR` | `/srv/media/TV` | organized library (optional) |
| `PORT` | `8080` | web UI / API port |

```bash
sudo mkdir -p /var/lib/autorip /srv/media/{output,staging,Movies,TV}
sudo chown -R autorip:autorip /var/lib/autorip /srv/media
```

A quick foreground smoke test before wiring up systemd:

```bash
sudo -u autorip env \
  AUTORIP_DIR=/var/lib/autorip \
  OUTPUT_DIR=/srv/media/output \
  STAGING_DIR=/srv/media/staging \
  PORT=8080 \
  autorip
# open http://localhost:8080
```

## 4. systemd unit

Create the service user, then install the unit.

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin \
  --groups cdrom autorip
```

`/etc/systemd/system/autorip.service`:

```ini
[Unit]
Description=freemkv autorip — automatic optical disc ripper
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=autorip
Group=autorip
# Supplementary group for /dev/sg* and /dev/sr0 access (see §2).
SupplementaryGroups=cdrom

Environment=AUTORIP_DIR=/var/lib/autorip
Environment=OUTPUT_DIR=/srv/media/output
Environment=STAGING_DIR=/srv/media/staging
# Optional:
# Environment=MOVIE_DIR=/srv/media/Movies
# Environment=TV_DIR=/srv/media/TV
# Environment=TMDB_API_KEY=...
Environment=PORT=8080

ExecStart=/usr/local/bin/autorip
Restart=on-failure
RestartSec=5

# Hardening — autorip needs only the optical device and its own dirs.
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/var/lib/autorip /srv/media
# Allow access to the optical drive device nodes (no full --privileged).
DeviceAllow=block-sr rw
DeviceAllow=char-sg rw
DevicePolicy=closed

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now autorip
sudo systemctl status autorip
journalctl -u autorip -f
```

Then browse to `http://<host>:8080`. KEYDB and TMDB are configured from
the Settings UI exactly as in the Docker deployment. DVDs (CSS) work
with no key setup; Blu-ray and UHD (AACS) need a `keydb.cfg`, which
autorip can fetch and refresh from the KEYDB Update URL in Settings.

## Notes

- The Docker image is unaffected by any of the above — `docker-compose`
  with `privileged: true` remains the recommended deployment and the
  Dockerfiles are unchanged.
- If you front autorip with a reverse proxy, point it at the local
  `PORT`; autorip serves plain HTTP.
