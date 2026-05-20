mod config;
mod history;
mod log;
mod mover;
mod muxer;
mod observe;
mod ripper;
mod tmdb;
mod util;
mod verify;
mod web;
mod webhook;

use std::io::Read as _;
use std::sync::atomic::{AtomicBool, Ordering};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

fn main() {
    // v0.25.7: tiny built-in subcommands so the deployed image doesn't
    // need curl (HEALTHCHECK) or bash + nfs-utils helpers + a separate
    // entrypoint script (--bootstrap). Each subcommand exits before
    // observe::init so they don't spam the tracing sinks on every
    // 30-second healthcheck.
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match argv.first().map(String::as_str) {
        Some("--healthcheck") => {
            std::process::exit(run_healthcheck());
        }
        Some("--version") | Some("-V") => {
            println!("autorip {}", env!("CARGO_PKG_VERSION"));
            std::process::exit(0);
        }
        Some("--help") | Some("-h") => {
            println!(
                "autorip {} — automated optical-disc rip service\n\n\
                 Usage:\n  \
                   autorip                  Run the daemon (env-var driven)\n  \
                   autorip --bootstrap      Initialize container env, then run the daemon\n  \
                   autorip --healthcheck    Probe http://127.0.0.1:$PORT/api/state (exit 0/1)\n  \
                   autorip --version        Print version and exit",
                env!("CARGO_PKG_VERSION")
            );
            std::process::exit(0);
        }
        Some("--bootstrap") => {
            // Bootstrap then fall through to the daemon below. Errors
            // are logged but non-fatal — the daemon's panic hook will
            // catch anything genuinely catastrophic later.
            run_bootstrap();
        }
        Some(other) => {
            eprintln!("autorip: unknown argument '{other}' (try --help)");
            std::process::exit(2);
        }
        None => {}
    }

    // Tracing FIRST — before any log call, panic hook, or thread spawn.
    // Sets up stderr + autorip.log + autorip.jsonl sinks. Filter via
    // AUTORIP_LOG_LEVEL env (default `autorip=info,libfreemkv=warn`).
    observe::init();

    // Signal handler for graceful shutdown
    #[cfg(unix)]
    unsafe {
        libc::signal(
            libc::SIGTERM,
            handle_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGINT,
            handle_signal as *const () as libc::sighandler_t,
        );
    }

    // Panic hook — log any thread panic to the system log so we can debug
    // post-mortem without a live stderr. Without this the user just sees "UI
    // crashed" with no clue which thread or path blew up.
    std::panic::set_hook(Box::new(|info| {
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let msg = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
            .unwrap_or("<non-string panic>");
        let thread = std::thread::current()
            .name()
            .unwrap_or("<unnamed>")
            .to_string();
        // Both: structured event for the JSONL stream (greppable post-mortem)
        // AND the legacy syslog line so the per-device file + UI keep working.
        tracing::error!(thread = %thread, location = %loc, message = %msg, "panic");
        log::syslog(&format!("PANIC in thread '{thread}' at {loc}: {msg}"));
    }));

    log::syslog(&format!(
        "autorip starting (v{}, edition 2024)",
        env!("CARGO_PKG_VERSION")
    ));
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        target = std::env::consts::OS,
        arch = std::env::consts::ARCH,
        "autorip starting"
    );

    // Load config
    let cfg = config::load();

    // Ensure KEYDB exists — download on first boot if URL is configured
    if libfreemkv::keydb::default_path()
        .ok()
        .map(|p| p.exists())
        .unwrap_or(false)
    {
        log::syslog("KEYDB found");
    } else {
        let url = cfg
            .read()
            .ok()
            .map(|c| c.keydb_url.clone())
            .unwrap_or_default();
        if !url.is_empty() {
            log::syslog("KEYDB not found, downloading...");
            match ureq::get(&url).call() {
                Ok(resp) => {
                    let mut buf = Vec::new();
                    if resp
                        .into_reader()
                        .take(100 * 1024 * 1024)
                        .read_to_end(&mut buf)
                        .is_ok()
                    {
                        match libfreemkv::keydb::save(&buf) {
                            Ok(r) => {
                                log::syslog(&format!("KEYDB downloaded: {} entries", r.entries))
                            }
                            Err(e) => log::syslog(&format!("KEYDB save failed: {e}")),
                        }
                    }
                }
                Err(e) => log::syslog(&format!("KEYDB download failed: {e}")),
            }
        }
    }

    // Start mover thread.
    let _mover_handle = std::thread::spawn({
        let cfg = cfg.clone();
        move || mover::run(&cfg)
    });

    // Start mux worker thread — pipelines mux behind the drive so a
    // disc can be ripped on one device while a prior title muxes in
    // the background. v0.25.3 scaffold; phase 3 wires the actual mux
    // dispatch. Today the loop scans staging for `.ripped` markers and
    // logs only — no behavioural change until the drive thread starts
    // writing those markers.
    let _muxer_handle = std::thread::spawn({
        let cfg = cfg.clone();
        move || muxer::run(&cfg)
    });

    // Start web server thread
    let _web_handle = std::thread::spawn({
        let cfg = cfg.clone();
        move || web::run(&cfg)
    });

    // Start KEYDB auto-update thread. Single source of truth for periodic
    // KEYDB refresh — pre-0.13 there was also a cron entry that spawned a
    // second `autorip` binary, which raced this thread for /dev/sg* and
    // port 8080. Cron path was removed; this is now the only daily updater.
    let _keydb_handle = std::thread::spawn({
        let cfg2 = cfg.clone();
        move || {
            tracing::info!("keydb update thread starting (24h interval)");
            'outer: loop {
                // 24h sleep in 1s chunks so SHUTDOWN is observed within ~1s.
                for _ in 0..(24 * 3600) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    if SHUTDOWN.load(Ordering::Relaxed) {
                        break 'outer;
                    }
                }
                let url = cfg2
                    .read()
                    .ok()
                    .map(|c| c.keydb_url.clone())
                    .unwrap_or_default();
                if url.is_empty() {
                    continue;
                }
                tracing::info!(url = %url, "keydb: starting daily update");
                match ureq::get(&url).call() {
                    Ok(resp) => {
                        let mut buf = Vec::new();
                        if resp
                            .into_reader()
                            .take(100 * 1024 * 1024)
                            .read_to_end(&mut buf)
                            .is_ok()
                        {
                            match libfreemkv::keydb::save(&buf) {
                                Ok(r) => {
                                    log::syslog(&format!("KEYDB updated: {} entries", r.entries))
                                }
                                Err(e) => log::syslog(&format!("KEYDB update failed: {e}")),
                            }
                        } else {
                            tracing::warn!("keydb: response read failed");
                        }
                    }
                    Err(e) => log::syslog(&format!("KEYDB update failed: {e}")),
                }
            }
            tracing::info!("keydb update thread stopping");
        }
    });

    // Log prune thread — replaces the v0.25.5 cron-based daily cleanup
    // (./entrypoint.sh used to drop a line in /etc/cron.d). Moving this
    // in-process let us drop the cron package + the cron service from
    // the image (alpine swap in v0.25.6), shrinking the deployed
    // container by ~5 MB and eliminating a runtime dependency.
    //
    // v0.25.7: retention_days now comes from the Settings UI
    // (`cfg.log_retention_days`) instead of the LOG_RETENTION_DAYS
    // env var — operator can change it live without redeploying.
    // Re-read each tick so a saved-settings update takes effect on
    // the next daily run rather than requiring a restart.
    let _log_prune_handle = std::thread::spawn({
        let cfg = cfg.clone();
        move || {
            tracing::info!("log prune thread starting (24h interval)");
            'outer: loop {
                // Wait first; on a fresh container the logs dir has only
                // a few minutes of data and pruning is a no-op anyway.
                for _ in 0..(24 * 3600) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    if SHUTDOWN.load(Ordering::Relaxed) {
                        break 'outer;
                    }
                }
                let (log_dir, retention_days) = cfg
                    .read()
                    .ok()
                    .map(|c| (c.log_dir(), c.log_retention_days))
                    .unwrap_or_default();
                if log_dir.is_empty() {
                    continue;
                }
                prune_old_logs(&log_dir, retention_days);
            }
            tracing::info!("log prune thread stopping");
        }
    });

    // Main loop: poll drives (checks SHUTDOWN flag internally)
    ripper::drive_poll_loop(&cfg);

    // Drain any rip threads that are still mid-flight so we don't
    // exit the process while libfreemkv is holding a SCSI session
    // and writing into staging. Bounded so a stuck drive can't
    // pin shutdown indefinitely.
    ripper::join_all_rip_threads(std::time::Duration::from_secs(60));

    log::syslog("autorip stopped");
}

#[cfg(unix)]
extern "C" fn handle_signal(_sig: libc::c_int) {
    if SHUTDOWN.load(Ordering::Relaxed) {
        // Second signal — force exit
        unsafe { libc::_exit(1) };
    }
    SHUTDOWN.store(true, Ordering::Relaxed);
}

/// Probe the local HTTP API and exit 0 (healthy) or 1 (unhealthy).
///
/// Replaces the v0.25.5 `curl --fail http://127.0.0.1:8080/api/state`
/// HEALTHCHECK so the deployed image doesn't need curl installed —
/// freeing ~3 MB on the Option C / `FROM scratch` build and removing
/// one more "why is this here" surface from the runtime image.
///
/// Reads the same `PORT` env var the web server binds to (default
/// 8080). 2 s connect, 2 s read — both well under the 5 s timeout
/// the Dockerfile HEALTHCHECK gives us.
fn run_healthcheck() -> i32 {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::time::Duration;

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);
    let addr: SocketAddr = match format!("127.0.0.1:{port}").parse() {
        Ok(a) => a,
        Err(_) => return 1,
    };
    let mut stream = match TcpStream::connect_timeout(&addr, Duration::from_secs(2)) {
        Ok(s) => s,
        Err(_) => return 1,
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));

    // Minimal HTTP/1.1 request — no Host header niceties required by
    // tiny_http for the /api/state endpoint to respond.
    let req = b"GET /api/state HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    if stream.write_all(req).is_err() {
        return 1;
    }

    // We only need to see the status line.
    let mut buf = [0u8; 64];
    let n = match stream.read(&mut buf) {
        Ok(n) => n,
        Err(_) => return 1,
    };
    let line = &buf[..n];
    if line.starts_with(b"HTTP/1.1 200") || line.starts_with(b"HTTP/1.0 200") {
        0
    } else {
        1
    }
}

/// Container bootstrap — replaces the v0.25.5 `entrypoint.sh` so the
/// final image can drop `bash`, `shadow` (`useradd`), and the shell
/// scripts themselves.
///
/// Behaviour mirrors the prior shell entrypoint:
/// - Create per-instance dirs under `$AUTORIP_DIR` (logs, freemkv,
///   history) and `/staging`.
/// - If running as root, ensure the `rip` user exists (writes
///   `/etc/passwd` + `/etc/group` lines directly so we don't need
///   `useradd`) and chown the working dirs.
/// - Symlink `/home/<rip>/.config/freemkv` → `$AUTORIP_DIR/freemkv`
///   so libfreemkv finds the KEYDB at its canonical path.
/// - Snapshot relevant env vars to `/etc/autorip.env` for the
///   `udev-trigger.sh` rip-on-insert path.
/// - Write the udev rule.
/// - If `NFS_HOST` + `NFS_EXPORT` + `NFS_MOUNTPOINT` are set, mount
///   NFS inside the container via `/sbin/mount.nfs4` (bundled by the
///   Option C harvest stage) so each container start gets a fresh
///   NFS session and stale handles self-heal on restart.
///
/// All steps log to stderr (observe::init hasn't run yet) and are
/// non-fatal — a transient mount failure shouldn't trip the
/// restart loop; the mover will simply fail to write to the empty
/// dir until the next container start retries the mount.
fn run_bootstrap() {
    use std::io::Write;
    use std::os::unix::fs::symlink;

    let autorip_dir = std::env::var("AUTORIP_DIR").unwrap_or_else(|_| "/config".to_string());
    let rip_user = std::env::var("RIP_USER").unwrap_or_else(|_| "autorip".to_string());

    // Working directories
    for sub in ["logs", "freemkv", "history"] {
        let p = format!("{autorip_dir}/{sub}");
        if let Err(e) = std::fs::create_dir_all(&p) {
            eprintln!("bootstrap: mkdir {p}: {e}");
        }
    }
    if let Err(e) = std::fs::create_dir_all("/staging") {
        eprintln!("bootstrap: mkdir /staging: {e}");
    }

    // User creation (no useradd binary — just append to /etc/passwd +
    // /etc/group). Idempotent: skip if a line already starts with the
    // username. Only runs when uid == 0; we never demote anyway since
    // the container needs root for SCSI + mount(2).
    if unsafe { libc::getuid() } == 0 {
        ensure_user_entry(&rip_user);
        if let Err(e) = chown_recursive(std::path::Path::new("/staging"), &rip_user) {
            eprintln!("bootstrap: chown /staging: {e}");
        }
        if let Err(e) = chown_recursive(std::path::Path::new(&autorip_dir), &rip_user) {
            eprintln!("bootstrap: chown {autorip_dir}: {e}");
        }
    }

    // Symlink for KEYDB lookup path
    let freemkv_cfg = format!("/home/{rip_user}/.config/freemkv");
    if let Some(parent) = std::path::Path::new(&freemkv_cfg).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::remove_file(&freemkv_cfg);
    let _ = std::fs::remove_dir_all(&freemkv_cfg);
    if let Err(e) = symlink(format!("{autorip_dir}/freemkv"), &freemkv_cfg) {
        eprintln!("bootstrap: symlink {freemkv_cfg}: {e}");
    }

    // Snapshot env for the udev-triggered rip-on-insert path
    if let Ok(mut f) = std::fs::File::create("/etc/autorip.env") {
        for (k, v) in std::env::vars() {
            if matches!(
                k.as_str(),
                "TMDB_API_KEY"
                    | "STAGING_DIR"
                    | "OUTPUT_DIR"
                    | "MOVIE_DIR"
                    | "TV_DIR"
                    | "MIN_LENGTH"
                    | "MAIN_FEATURE"
                    | "AUTO_EJECT"
                    | "ON_INSERT"
                    | "ABORT_ON_ERROR"
                    | "AUTORIP_DIR"
                    | "PORT"
                    | "KEYDB_PATH"
                    | "AUTORIP_LOG_LEVEL"
            ) {
                let _ = writeln!(f, "{k}={v}");
            }
        }
    }

    // udev rule (the kernel's udev daemon runs on the host; container
    // sees disc-insert events via the shared /dev mount + udev-trigger.sh
    // calling our HTTP API)
    let _ = std::fs::create_dir_all("/etc/udev/rules.d");
    let udev_rule = "ACTION==\"change\", SUBSYSTEM==\"block\", KERNEL==\"sr[0-9]*\", \
                     ENV{ID_CDROM_MEDIA}==\"1\", ENV{ID_CDROM_MEDIA_STATE}!=\"blank\", \
                     RUN+=\"/usr/local/bin/udev-trigger.sh %k\"\n";
    let _ = std::fs::write("/etc/udev/rules.d/99-autorip.rules", udev_rule);

    // In-container NFS mount (v0.25.4 feature, kept). When NFS_HOST is
    // unset this is a no-op and the operator's docker-compose volumes:
    // line is the source of truth instead.
    if let (Ok(host), Ok(export), Ok(mountpoint)) = (
        std::env::var("NFS_HOST"),
        std::env::var("NFS_EXPORT"),
        std::env::var("NFS_MOUNTPOINT"),
    ) {
        if !host.is_empty() && !export.is_empty() && !mountpoint.is_empty() {
            let opts = std::env::var("NFS_OPTS")
                .unwrap_or_else(|_| "vers=4.1,nconnect=4,nolock,actimeo=3,hard,_netdev".into());
            let _ = std::fs::create_dir_all(&mountpoint);
            if !is_mountpoint(&mountpoint) {
                let source = format!("{host}:{export}");
                eprintln!("bootstrap: mounting {source} -> {mountpoint} ({opts})");
                let status = std::process::Command::new("/sbin/mount.nfs4")
                    .arg("-o")
                    .arg(&opts)
                    .arg(&source)
                    .arg(&mountpoint)
                    .status();
                match status {
                    Ok(s) if s.success() => eprintln!("bootstrap: NFS mount OK"),
                    Ok(s) => {
                        eprintln!(
                            "bootstrap: NFS mount FAILED ({s}); container will start with empty {mountpoint}"
                        )
                    }
                    Err(e) => eprintln!("bootstrap: NFS mount FAILED ({e})"),
                }
            } else {
                eprintln!("bootstrap: {mountpoint} already mounted, skipping");
            }
        }
    }
}

fn ensure_user_entry(user: &str) {
    use std::io::Write;
    let passwd = std::fs::read_to_string("/etc/passwd").unwrap_or_default();
    if !passwd.lines().any(|l| l.starts_with(&format!("{user}:"))) {
        if let Ok(mut f) = std::fs::OpenOptions::new().append(true).open("/etc/passwd") {
            let _ = writeln!(f, "{user}:x:1000:1000::/home/{user}:/bin/sh");
        }
    }
    let group = std::fs::read_to_string("/etc/group").unwrap_or_default();
    if !group.lines().any(|l| l.starts_with(&format!("{user}:"))) {
        if let Ok(mut f) = std::fs::OpenOptions::new().append(true).open("/etc/group") {
            let _ = writeln!(f, "{user}:x:1000:");
        }
    }
}

fn chown_recursive(path: &std::path::Path, user: &str) -> std::io::Result<()> {
    use std::ffi::CString;
    let c_user =
        CString::new(user).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    // Look up uid/gid. With a freshly-written /etc/passwd line above
    // we know uid=gid=1000, but resolving keeps this honest if the
    // entry was already there with different IDs.
    let pwd = unsafe { libc::getpwnam(c_user.as_ptr()) };
    if pwd.is_null() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("getpwnam({user}) failed"),
        ));
    }
    let uid = unsafe { (*pwd).pw_uid };
    let gid = unsafe { (*pwd).pw_gid };

    fn walk(p: &std::path::Path, uid: libc::uid_t, gid: libc::gid_t) -> std::io::Result<()> {
        use std::os::unix::ffi::OsStrExt;
        let c_path = std::ffi::CString::new(p.as_os_str().as_bytes())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        if unsafe { libc::lchown(c_path.as_ptr(), uid, gid) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        if p.is_dir() {
            for entry in std::fs::read_dir(p)?.flatten() {
                walk(&entry.path(), uid, gid)?;
            }
        }
        Ok(())
    }
    walk(path, uid, gid)
}

fn is_mountpoint(path: &str) -> bool {
    let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
    mounts
        .lines()
        .any(|l| l.split_whitespace().nth(1) == Some(path))
}

/// Delete `.log` files under `log_dir` older than `retention_days`.
/// Replaces the v0.25.5 cron-based cleanup so the deployed image
/// doesn't need a cron daemon. Single-shot; the caller drives the
/// daily cadence.
fn prune_old_logs(log_dir: &str, retention_days: u64) {
    let cutoff = std::time::SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(retention_days * 86_400));
    let Some(cutoff) = cutoff else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return;
    };
    let mut pruned = 0u32;
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("log") {
            continue;
        }
        let Ok(meta) = entry.metadata() else { continue };
        let Ok(mtime) = meta.modified() else { continue };
        if mtime < cutoff && std::fs::remove_file(&path).is_ok() {
            pruned += 1;
        }
    }
    if pruned > 0 {
        log::syslog(&format!(
            "log prune: removed {pruned} files older than {retention_days}d from {log_dir}"
        ));
    }
}
