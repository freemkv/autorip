#!/usr/bin/env bash
# autorip is frozen (retired at 1.8.0): build it against the sibling commits it
# last built green with (dev tips of 2026-09-28), not moving dev tips.
set -euo pipefail
declare -A pin=(
  [libfreemkv]=7533d876c458180cc2a468dec119ef0a2f836a63
  [freemkv-keysources]=05281249d1695f63c02ec53b3abf27d4f68608dc
  [freemkv-engine]=8246ee0aa647f850fac14644073390b0f246ec01
  [freemkv-i18n]=9e8e062b6b3d88a5b7a7e29c5f8cb8e255797730
  [freemkv-unlock]=6d91b7ce02024be84c51ddd731589d056b17b1b2
)
for r in "${!pin[@]}"; do
  [ -e "$r" ] && continue
  git init -q "$r"
  git -C "$r" fetch -q --depth 1 "https://github.com/freemkv/$r" "${pin[$r]}"
  git -C "$r" checkout -q --detach FETCH_HEAD
done
mkdir -p autorip/.cargo
{
  echo '[patch.crates-io]'
  for c in libfreemkv freemkv-keysources freemkv-engine freemkv-i18n freemkv-unlock; do
    echo "$c = { path = \"../$c\" }"
  done
} > autorip/.cargo/config.toml
