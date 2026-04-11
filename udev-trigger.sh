#!/bin/bash
# Triggered by udev on disc insertion. Notifies autorip web server.
[ -f /etc/autorip.env ] && . /etc/autorip.env
PORT="${PORT:-8080}"
curl -sf "http://localhost:${PORT}/api/rip/$1" -X POST &>/dev/null &
