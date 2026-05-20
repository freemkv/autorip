#!/bin/sh
#
# DEPRECATED in v0.25.7. The autorip image no longer ships bash, nor
# does it COPY this file in. The Rust `autorip --bootstrap` subcommand
# replaces it. Kept in the repo for operator reference only.
#
# Operators with `entrypoint: ["/bin/bash", "-c", "exec /entrypoint.sh"]`
# overrides in their docker-compose.yml must remove the override (the
# Dockerfile ENTRYPOINT is now `["/usr/local/bin/autorip", "--bootstrap"]`)
# or rewrite it to invoke the binary directly.
#
set -e

AUTORIP_DIR="${AUTORIP_DIR:-/config}"
RIP_USER="${RIP_USER:-autorip}"

# Create directories
mkdir -p "$AUTORIP_DIR/logs" "$AUTORIP_DIR/freemkv" "$AUTORIP_DIR/history" /staging

# v0.25.4: optional in-container NFS mount. The historical pattern is
# to bind-mount a host NFS share into the container — but if the host
# mount goes stale (Unraid disk spindown, idle TCP drop, mover/parity
# activity), Watchtower's next restart can't bind-mount and the
# container is stranded in `Created`. Mounting NFS *inside* the
# container instead means each container start gets a fresh NFS
# session: stale state self-heals on restart.
#
# Activate by setting NFS_HOST + NFS_EXPORT + NFS_MOUNTPOINT in the
# environment. NFS_OPTS overrides the default mount options (which
# include the resilience knobs vers=4.1,nconnect=4,nolock,actimeo=3).
# When NFS_HOST is unset the entrypoint is a no-op here and the
# container falls back to whatever the compose `volumes:` line
# bind-mounted. Both patterns are documented in
# docker-compose.example.yml.
if [ -n "${NFS_HOST:-}" ] && [ -n "${NFS_EXPORT:-}" ] && [ -n "${NFS_MOUNTPOINT:-}" ]; then
    NFS_OPTS="${NFS_OPTS:-vers=4.1,nconnect=4,nolock,actimeo=3,hard,_netdev}"
    mkdir -p "$NFS_MOUNTPOINT"
    if mountpoint -q "$NFS_MOUNTPOINT"; then
        echo "entrypoint: $NFS_MOUNTPOINT already mounted (skipping)"
    else
        echo "entrypoint: mounting ${NFS_HOST}:${NFS_EXPORT} -> $NFS_MOUNTPOINT ($NFS_OPTS)"
        if mount -t nfs -o "$NFS_OPTS" "${NFS_HOST}:${NFS_EXPORT}" "$NFS_MOUNTPOINT"; then
            echo "entrypoint: NFS mount OK"
        else
            echo "entrypoint: NFS mount FAILED — proceeding with empty dir. Mover writes will fail until the share is reachable; staging stays safe." >&2
        fi
    fi
fi

# Create rip user if running as root
if [ "$(id -u)" = "0" ]; then
    id -u "$RIP_USER" &>/dev/null || useradd -M -s /bin/bash "$RIP_USER"
    chown -R "$RIP_USER" /staging "$AUTORIP_DIR"
fi

# Symlink freemkv config (KEYDB.cfg location)
FREEMKV_CFG="/home/$RIP_USER/.config/freemkv"
mkdir -p "$(dirname "$FREEMKV_CFG")"
rm -rf "$FREEMKV_CFG"
ln -sfn "$AUTORIP_DIR/freemkv" "$FREEMKV_CFG"

# Save env vars for udev-triggered runs
env | grep -E '^(TMDB_API_KEY|STAGING_DIR|OUTPUT_DIR|MOVIE_DIR|TV_DIR|MIN_LENGTH|MAIN_FEATURE|AUTO_EJECT|ON_INSERT|ABORT_ON_ERROR|AUTORIP_DIR|PORT|KEYDB_PATH|AUTORIP_LOG_LEVEL)' > /etc/autorip.env

# Setup udev rule for disc detection
cat > /etc/udev/rules.d/99-autorip.rules << 'UDEV'
ACTION=="change", SUBSYSTEM=="block", KERNEL=="sr[0-9]*", ENV{ID_CDROM_MEDIA}=="1", ENV{ID_CDROM_MEDIA_STATE}!="blank", RUN+="/usr/local/bin/udev-trigger.sh %k"
UDEV

# Log cleanup moved into the autorip process itself in v0.25.6 (see
# main.rs::prune_old_logs). cron + cron.d are no longer needed; the
# image dropped the package as part of the v0.25.6 alpine slim-down.

# Start autorip
exec /usr/local/bin/autorip
