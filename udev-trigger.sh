#!/bin/sh
# Triggered by udev on disc insertion. Notifies autorip web server.
# Uses busybox `sh` + `wget` so the FROM scratch image doesn't need
# bash or curl (v0.25.7 image diet).
[ -f /etc/autorip.env ] && . /etc/autorip.env
PORT="${PORT:-8080}"
wget -q -O- --method=POST "http://localhost:${PORT}/api/rip/$1" >/dev/null 2>&1 &
