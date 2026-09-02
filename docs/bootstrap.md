# Container bootstrap

`run_bootstrap` replaces the v0.25.5 `entrypoint.sh` so the final image can
drop `bash`, `shadow` (`useradd`), and the shell scripts themselves.

Behaviour mirrors the prior shell entrypoint:

- Create per-instance dirs under `$AUTORIP_DIR` (logs, freemkv, history)
  and `/staging`.
- If running as root, ensure the `rip` user exists (writes `/etc/passwd` +
  `/etc/group` lines directly so we don't need `useradd`) and chown the
  working dirs.
- Symlink `/home/<rip>/.config/freemkv` → `$AUTORIP_DIR/freemkv` so
  libfreemkv finds the KEYDB at its canonical path.
- Snapshot relevant env vars to `/etc/autorip.env` for the
  `udev-trigger.sh` rip-on-insert path.
- Write the udev rule.
- If `NFS_HOST` + `NFS_EXPORT` + `NFS_MOUNTPOINT` are set, mount NFS
  inside the container via `/sbin/mount.nfs4` (bundled by the Option C
  harvest stage) so each container start gets a fresh NFS session and
  stale handles self-heal on restart.

All steps log to stderr (`observe::init` hasn't run yet) and are
non-fatal — a transient mount failure shouldn't trip the restart loop;
the mover will simply fail to write to the empty dir until the next
container start retries the mount.

Linux-only: it manipulates `/etc/passwd`, chowns the working dirs, and
mounts NFS — all container-init concerns that don't exist on macOS or
Windows, where the daemon runs directly. cfg-gated out of those builds.
