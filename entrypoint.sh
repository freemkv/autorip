#!/bin/bash
set -e

AUTORIP_DIR="${AUTORIP_DIR:-/config}"
RIP_USER="${RIP_USER:-autorip}"

# Create directories
mkdir -p "$AUTORIP_DIR/logs" "$AUTORIP_DIR/freemkv" "$AUTORIP_DIR/history" /staging

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

# Log cleanup cron (4am daily). KEYDB updates happen inside the live autorip
# process (main.rs spawns a daily updater thread) — never spawn a second
# `autorip` binary from cron, it races the live process for /dev/sg* and the
# web port and silently breaks the UI. See CHANGELOG 0.13.0 for the incident.
mkdir -p /etc/cron.d
echo "0 4 * * * root find $AUTORIP_DIR/logs -name '*.log' -mtime +${LOG_RETENTION_DAYS:-30} -delete" > /etc/cron.d/autorip

# Start cron
service cron start 2>/dev/null || true

# Start autorip
exec /usr/local/bin/autorip
