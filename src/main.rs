mod config;
mod keysource;
mod log;
mod mover;
mod muxer;
mod observe;
mod review;
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
                   autorip                  Run the daemon (bare — config under ~/.config/autorip)\n  \
                   autorip serve            Same as no-arg: run the daemon without container bootstrap\n  \
                   autorip --bootstrap      Initialize container env (NFS mount), then run the daemon\n  \
                   autorip --healthcheck    Probe http://127.0.0.1:$PORT/api/state (exit 0/1)\n  \
                   autorip --version        Print version and exit",
                env!("CARGO_PKG_VERSION")
            );
            std::process::exit(0);
        }
        Some("--bootstrap") => {
            // Bootstrap then fall through to the daemon below. Errors
            // are logged but non-fatal — the daemon's panic hook will
            // catch anything genuinely catastrophic later. Container-init
            // is Linux-only; on macOS/Windows this is a no-op and the
            // daemon runs directly.
            #[cfg(unix)]
            run_bootstrap();
            #[cfg(not(unix))]
            eprintln!("autorip: --bootstrap is Linux-only; running the daemon directly");
        }
        // Bare run (no Docker): run the daemon WITHOUT the container bootstrap
        // (no NFS mount, no chown, no /staging). Config/staging/output default
        // under ~/.config/autorip (see config::default_autorip_dir). `serve`
        // is an explicit alias for the bare no-arg invocation.
        Some("serve") => {}
        Some(other) => {
            eprintln!("autorip: unknown argument '{other}' (try --help)");
            std::process::exit(2);
        }
        None => {}
    }

    // Panic hook FIRST — before observe::init, so a panic DURING tracing
    // setup (bad AUTORIP_LOG_LEVEL filter, unwritable log dir) still hits
    // the post-mortem path instead of unwinding with only the default Rust
    // message. The hook's tracing::error! is a best-effort no-op before
    // init, but log::syslog still records to the per-device file + the
    // in-memory ring, so it remains useful for these earliest failures.
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

    // Tracing — sets up stderr + autorip.log + autorip.jsonl sinks. Filter
    // via AUTORIP_LOG_LEVEL env (default `autorip=info,libfreemkv=warn`).
    // The panic hook above is already installed, so a panic in here is
    // captured post-mortem.
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

    // Rotate the system log if it has grown large across many restarts —
    // it has no per-rip archive boundary, so this is its only bound.
    log::rotate_system_log_if_large();

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

    // The local KEYDB only matters for the `local` key source. In `online`
    // mode keys come from the key service and a local keydb would only shadow
    // it (libfreemkv default-search), so skip the download entirely.
    let online_keys = cfg
        .read()
        .ok()
        .map(|c| c.key_source == "online")
        .unwrap_or(false);

    // Ensure KEYDB exists — download on first boot if URL is configured
    if online_keys {
        log::syslog("Online key source — skipping local KEYDB download");
    } else if libfreemkv::keydb::default_path()
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
            // Route through the SSRF guard (validate_fetch_url + pinned
            // resolver) — a bare ureq::get here would let an operator-set
            // keydb_url reach loopback / RFC1918 / cloud-metadata.
            match web::guarded_get(&url) {
                Ok(resp) => {
                    let mut buf = Vec::new();
                    match resp
                        .into_reader()
                        .take(100 * 1024 * 1024)
                        .read_to_end(&mut buf)
                    {
                        Ok(_) => match libfreemkv::keydb::save(&buf) {
                            Ok(r) => {
                                log::syslog(&format!("KEYDB downloaded: {} entries", r.entries))
                            }
                            Err(e) => log::syslog(&format!("KEYDB save failed: {e}")),
                        },
                        // Match the daily-refresh path, which logs "response
                        // read failed"; without this a truncated transfer /
                        // disk-full on first boot was dropped silently.
                        Err(e) => log::syslog(&format!("KEYDB download read failed: {e}")),
                    }
                }
                Err(e) => log::syslog(&format!(
                    "KEYDB download failed for {}: {e}",
                    crate::webhook::webhook_url_origin(&url)
                )),
            }
        }
    }

    // Start mover thread. Joined on shutdown (see end of main) so an
    // in-flight file move isn't truncated into a partial OUTPUT_DIR file.
    let mover_handle = std::thread::spawn({
        let cfg = cfg.clone();
        move || mover::run(&cfg)
    });

    // Start mux worker thread — pipelines mux behind the drive so a
    // disc can be ripped on one device while a prior title muxes in
    // the background. Picks up the `.ripped` markers the ripper writes
    // into staging and muxes each ISO to MKV, writing `.done` on success.
    // Joined on shutdown (see end of main) so an in-flight mux isn't
    // killed mid-write, leaving a truncated MKV that looks valid.
    let muxer_handle = std::thread::spawn({
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
                // Online key source resolves out-of-band; no local keydb to keep
                // fresh (and refreshing one would only shadow the service).
                let (online, url) = cfg2
                    .read()
                    .ok()
                    .map(|c| (c.key_source == "online", c.keydb_url.clone()))
                    .unwrap_or((false, String::new()));
                if online || url.is_empty() {
                    continue;
                }
                tracing::info!(url_origin = %crate::webhook::webhook_url_origin(&url), "keydb: starting daily update");
                // SSRF-guarded fetch (see web::guarded_get) — the daily
                // refresh must not bypass the address allow-list that the
                // settings save and manual update already enforce.
                match web::guarded_get(&url) {
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
                    Err(e) => log::syslog(&format!(
                        "KEYDB update failed for {}: {e}",
                        crate::webhook::webhook_url_origin(&url)
                    )),
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

    // Drain the mover and muxer too. Both loop on SHUTDOWN (set by the
    // signal handler above) and return after the current work unit, so
    // joining them lets an in-flight file move or mux finish instead of
    // being killed when the process exits — a truncated OUTPUT_DIR file
    // or a partial MKV can look valid to downstream consumers. Bounded
    // (generous deadline, mirroring the rip-thread drain) so a wedged
    // NFS write or stuck mux can't pin shutdown forever.
    join_bounded(mover_handle, "mover", std::time::Duration::from_secs(120));
    join_bounded(muxer_handle, "muxer", std::time::Duration::from_secs(120));

    log::syslog("autorip stopped");
}

/// Join `handle`, but give up after `timeout` so a wedged worker can't
/// pin shutdown indefinitely. Polls `is_finished` because the std
/// library has no join-with-timeout; same shape as the rip-thread
/// drain in `ripper::session`. The worker is expected to observe
/// `SHUTDOWN` and return after its current work unit; the timeout is a
/// backstop for a genuinely stuck I/O path (e.g. an NFS write stall).
fn join_bounded(handle: std::thread::JoinHandle<()>, name: &str, timeout: std::time::Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while !handle.is_finished() {
        if std::time::Instant::now() >= deadline {
            tracing::warn!(
                thread = name,
                "did not drain within timeout; exiting anyway"
            );
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    let _ = handle.join();
}

#[cfg(unix)]
extern "C" fn handle_signal(_sig: libc::c_int) {
    if SHUTDOWN.load(Ordering::Acquire) {
        // Second signal — force exit
        unsafe { libc::_exit(1) };
    }
    // Release on the store / Acquire on the load so the flag is reliably
    // visible to the main loop's shutdown poll on weakly-ordered targets
    // (aarch64 container hosts).
    SHUTDOWN.store(true, Ordering::Release);
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

    // We only need to see the status line, but a single read() is not
    // guaranteed to return the full 12-byte status line (a short first TCP
    // segment would make the probe falsely report unhealthy). Loop until we
    // have enough bytes to decide, EOF, or the 2 s read timeout fires.
    const STATUS_LEN: usize = "HTTP/1.1 200".len();
    let mut buf = [0u8; 64];
    let mut filled = 0usize;
    while filled < STATUS_LEN {
        match stream.read(&mut buf[filled..]) {
            Ok(0) => break, // EOF before a full status line
            Ok(n) => filled += n,
            Err(_) => return 1,
        }
    }
    let line = &buf[..filled];
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
///
/// Linux-only: it manipulates `/etc/passwd`, chowns the working dirs, and
/// mounts NFS — all container-init concerns that don't exist on macOS or
/// Windows, where the daemon runs directly. cfg-gated out of those builds.
#[cfg(unix)]
fn run_bootstrap() {
    use std::io::Write;
    use std::os::unix::fs::symlink;

    let autorip_dir = std::env::var("AUTORIP_DIR").unwrap_or_else(|_| "/config".to_string());
    // RIP_USER is interpolated raw into /etc/passwd, /etc/group and the
    // KEYDB symlink path. A value containing a newline or colon would
    // corrupt the account database (inject an extra entry). Validate against
    // a conservative POSIX-portable username shape; fall back to the default
    // on anything malformed.
    let rip_user = match std::env::var("RIP_USER") {
        Ok(u) if is_valid_username(&u) => u,
        Ok(u) => {
            eprintln!(
                "bootstrap: RIP_USER {u:?} is not a valid username (^[a-z_][a-z0-9_-]{{0,31}}$); using 'autorip'"
            );
            "autorip".to_string()
        }
        Err(_) => "autorip".to_string(),
    };

    // Working directories
    for sub in ["logs", "freemkv"] {
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
        if let Err(e) = std::fs::create_dir_all(parent) {
            eprintln!("bootstrap: mkdir {}: {e}", parent.display());
        }
    }
    let _ = std::fs::remove_file(&freemkv_cfg);
    let _ = std::fs::remove_dir_all(&freemkv_cfg);
    if let Err(e) = symlink(format!("{autorip_dir}/freemkv"), &freemkv_cfg) {
        eprintln!("bootstrap: symlink {freemkv_cfg}: {e}");
    }

    // Snapshot env for the udev-triggered rip-on-insert path.
    // udev-trigger.sh does `. /etc/autorip.env`, so each line is sourced as
    // a shell assignment. A raw value containing a newline (e.g. a fat-
    // fingered TMDB_API_KEY) would otherwise become an extra sourced line.
    // Single-quote every value and escape embedded single quotes so the
    // value is always a single, inert shell token.
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
                let _ = writeln!(f, "{k}={}", shell_single_quote(&v));
            }
        }
    }

    // udev rule (the kernel's udev daemon runs on the host; container
    // sees disc-insert events via the shared /dev mount + udev-trigger.sh
    // calling our HTTP API)
    if let Err(e) = std::fs::create_dir_all("/etc/udev/rules.d") {
        eprintln!("bootstrap: mkdir /etc/udev/rules.d: {e}");
    }
    let udev_rule = "ACTION==\"change\", SUBSYSTEM==\"block\", KERNEL==\"sr[0-9]*\", \
                     ENV{ID_CDROM_MEDIA}==\"1\", ENV{ID_CDROM_MEDIA_STATE}!=\"blank\", \
                     RUN+=\"/usr/local/bin/udev-trigger.sh %k\"\n";
    if let Err(e) = std::fs::write("/etc/udev/rules.d/99-autorip.rules", udev_rule) {
        eprintln!("bootstrap: write udev rule: {e}");
    }

    // In-container NFS mount (v0.25.4 feature, kept). When NFS_HOST is
    // unset this is a no-op and the operator's docker-compose volumes:
    // line is the source of truth instead.
    if let (Ok(host), Ok(export), Ok(mountpoint)) = (
        std::env::var("NFS_HOST"),
        std::env::var("NFS_EXPORT"),
        std::env::var("NFS_MOUNTPOINT"),
    ) {
        if !host.is_empty() && !export.is_empty() && !mountpoint.is_empty() {
            // Default keeps `hard` (no silent I/O errors once mounted)
            // but adds `retry=1` so the foreground mount.nfs4 retry
            // window is short, and we wrap the child in a bounded wait
            // below. Together an unreachable server at container start
            // degrades to an empty mountpoint instead of stalling the
            // daemon's startup for the full retry window. Operators can
            // still override the whole string via NFS_OPTS.
            let opts = std::env::var("NFS_OPTS").unwrap_or_else(|_| {
                "vers=4.1,nconnect=4,nolock,actimeo=3,hard,retry=1,_netdev".into()
            });
            let _ = std::fs::create_dir_all(&mountpoint);
            if !is_mountpoint(&mountpoint) {
                let source = format!("{host}:{export}");
                eprintln!("bootstrap: mounting {source} -> {mountpoint} ({opts})");
                let child = std::process::Command::new("/sbin/mount.nfs4")
                    .arg("-o")
                    .arg(&opts)
                    .arg(&source)
                    .arg(&mountpoint)
                    .spawn();
                match child {
                    Ok(child) => match wait_bounded(child, std::time::Duration::from_secs(30)) {
                        Some(s) if s.success() => eprintln!("bootstrap: NFS mount OK"),
                        Some(s) => eprintln!(
                            "bootstrap: NFS mount FAILED ({s}); container will start with empty {mountpoint}"
                        ),
                        None => eprintln!(
                            "bootstrap: NFS mount TIMED OUT after 30s (server unreachable?); \
                             container will start with empty {mountpoint}"
                        ),
                    },
                    Err(e) => eprintln!("bootstrap: NFS mount FAILED to spawn ({e})"),
                }
            } else {
                eprintln!("bootstrap: {mountpoint} already mounted, skipping");
            }
        }
    }
}

/// Wrap a value in single quotes for safe inclusion in a POSIX-shell
/// `KEY=value` line that will be `.`-sourced. Embedded single quotes are
/// escaped via the standard `'\''` idiom, so the result is always exactly
/// one shell token regardless of newlines, spaces, or metacharacters.
fn shell_single_quote(v: &str) -> String {
    let mut out = String::with_capacity(v.len() + 2);
    out.push('\'');
    for c in v.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

/// Validate a Unix username against `^[a-z_][a-z0-9_-]{0,31}$` — the
/// conservative POSIX-portable shape. Rejects anything with a colon or
/// newline (the chars that would corrupt /etc/passwd or /etc/group when
/// the name is interpolated into a record), as well as empty / overlong
/// values.
fn is_valid_username(user: &str) -> bool {
    let mut chars = user.chars();
    let Some(first) = chars.next() else {
        return false; // empty
    };
    if !(first.is_ascii_lowercase() || first == '_') {
        return false;
    }
    if user.len() > 32 {
        return false;
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

#[cfg(unix)]
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

#[cfg(unix)]
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

    fn lchown_path(p: &std::path::Path, uid: libc::uid_t, gid: libc::gid_t) -> std::io::Result<()> {
        use std::os::unix::ffi::OsStrExt;
        let c_path = std::ffi::CString::new(p.as_os_str().as_bytes())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        if unsafe { libc::lchown(c_path.as_ptr(), uid, gid) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    fn walk(p: &std::path::Path, uid: libc::uid_t, gid: libc::gid_t) -> std::io::Result<()> {
        // lchown the entry itself (does NOT follow a symlink target — the
        // deliberate choice this fn already made).
        lchown_path(p, uid, gid)?;
        // Recurse only into REAL directories. entry.file_type() does NOT
        // follow symlinks, so a symlink-to-directory reports is_dir()==false
        // (is_symlink()==true) and is treated as a leaf — this stops a
        // symlink under /staging or $AUTORIP_DIR pointing at an external
        // tree from steering the walk (and chown) outside the intended tree.
        if let Ok(entries) = std::fs::read_dir(p) {
            for entry in entries.flatten() {
                let ft = entry.file_type()?;
                if ft.is_dir() {
                    walk(&entry.path(), uid, gid)?;
                } else {
                    // Files and symlinks: lchown the entry, never descend.
                    lchown_path(&entry.path(), uid, gid)?;
                }
            }
        }
        Ok(())
    }
    walk(path, uid, gid)
}

/// Strip trailing slashes from a mount path for comparison, preserving a
/// bare "/". So "/mnt/nfs/" and "/mnt/nfs" compare equal.
fn normalize_mount_path(s: &str) -> &str {
    let t = s.trim_end_matches('/');
    if t.is_empty() { "/" } else { t }
}

/// Wait for `child` to exit, but give up after `timeout` and kill it so
/// an unreachable NFS server can't block bootstrap (and thus the daemon)
/// for the full foreground-mount retry window. Returns `Some(status)` if
/// the child exited in time, `None` if it was killed on timeout.
fn wait_bounded(
    mut child: std::process::Child,
    timeout: std::time::Duration,
) -> Option<std::process::ExitStatus> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(_) => return None,
        }
    }
}

fn is_mountpoint(path: &str) -> bool {
    // Normalize trailing slashes on both sides: an operator-set
    // NFS_MOUNTPOINT of "/mnt/nfs/" must still match "/mnt/nfs" in
    // /proc/mounts, otherwise the check returns false and mount.nfs4 runs
    // against an already-mounted dir (on a hard mount that can hang).
    let want = normalize_mount_path(path);
    let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
    mounts
        .lines()
        .filter_map(|l| l.split_whitespace().nth(1))
        .any(|mp| normalize_mount_path(mp) == want)
}

/// Delete `.log` files under `log_dir` older than `retention_days`.
/// Replaces the v0.25.5 cron-based cleanup so the deployed image
/// doesn't need a cron daemon. Single-shot; the caller drives the
/// daily cadence.
fn prune_old_logs(log_dir: &str, retention_days: u64) {
    // `retention_days * 86_400` can overflow u64 for an absurd operator
    // value (panic in debug → kills the prune thread; silent wraparound in
    // release → a bogus recent cutoff that could delete fresh logs). Guard
    // both the multiply and the subtraction.
    let cutoff = retention_days.checked_mul(86_400).and_then(|secs| {
        std::time::SystemTime::now().checked_sub(std::time::Duration::from_secs(secs))
    });
    let Some(cutoff) = cutoff else {
        return;
    };
    // Recurse so the archive subdir (logs/rips/, where archive_device_log
    // writes per-rip files — the dir that actually grows over time) is
    // pruned too, not just the top-level live logs.
    let pruned = prune_dir_recursive(std::path::Path::new(log_dir), cutoff);
    if pruned > 0 {
        log::syslog(&format!(
            "log prune: removed {pruned} files older than {retention_days}d from {log_dir}"
        ));
    }
}

/// Recursively delete `.log` files under `dir` older than `cutoff`, returning
/// the count removed. Subdirectories are descended into (so `logs/rips/` is
/// covered); non-`.log` files are left alone. IO errors on individual entries
/// are skipped, not propagated — pruning is best-effort and must never break
/// the daemon.
fn prune_dir_recursive(dir: &std::path::Path, cutoff: std::time::SystemTime) -> u32 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut pruned = 0u32;
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_dir() {
            pruned += prune_dir_recursive(&path, cutoff);
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("log") {
            continue;
        }
        let Ok(meta) = entry.metadata() else { continue };
        let Ok(mtime) = meta.modified() else { continue };
        if mtime < cutoff && std::fs::remove_file(&path).is_ok() {
            pruned += 1;
        }
    }
    pruned
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_usernames_accepted() {
        for u in [
            "autorip",
            "rip",
            "_svc",
            "a",
            "rip-user_1",
            "abcdefghijklmnopqrstuvwxyz012345",
        ] {
            assert!(is_valid_username(u), "{u:?} should be valid");
        }
    }

    #[test]
    fn invalid_usernames_rejected() {
        for u in [
            "",                                  // empty
            "1rip",                              // leading digit
            "-rip",                              // leading dash
            "Rip",                               // uppercase
            "rip:x",                             // colon (passwd injection)
            "rip\nroot:x:0:0",                   // newline injection
            "abcdefghijklmnopqrstuvwxyz0123456", // 33 chars, too long
            "rip user",                          // space
        ] {
            assert!(!is_valid_username(u), "{u:?} should be rejected");
        }
    }

    #[test]
    fn shell_single_quote_wraps_and_escapes() {
        assert_eq!(shell_single_quote("plain"), "'plain'");
        assert_eq!(shell_single_quote("a b"), "'a b'");
        // Newline stays inside the single quotes — cannot start a new line.
        assert_eq!(shell_single_quote("a\nb"), "'a\nb'");
        // Embedded single quote uses the '\'' idiom.
        assert_eq!(shell_single_quote("a'b"), "'a'\\''b'");
        // Shell metacharacters are inert inside single quotes.
        assert_eq!(shell_single_quote("$(rm -rf /)"), "'$(rm -rf /)'");
    }

    #[test]
    fn normalize_mount_path_trims_trailing_slash() {
        assert_eq!(normalize_mount_path("/mnt/nfs/"), "/mnt/nfs");
        assert_eq!(normalize_mount_path("/mnt/nfs"), "/mnt/nfs");
        assert_eq!(normalize_mount_path("/mnt/nfs///"), "/mnt/nfs");
        assert_eq!(normalize_mount_path("/"), "/");
        assert_eq!(normalize_mount_path("///"), "/");
    }

    #[test]
    fn prune_recurses_into_subdirs_and_only_touches_old_logs() {
        // Repo-local scratch, never /tmp (wiped on reboot; remove_dir_all
        // cleanup is skipped if the test is killed). Anchor to the crate's
        // own target/ dir so artifacts are cleaned by `cargo clean`.
        let d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-scratch")
            .join(format!("autorip-prune-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let rips = d.join("rips");
        std::fs::create_dir_all(&rips).unwrap();

        // Old archived log in the subdir (the dir that actually grows).
        let old = rips.join("sg0_old.log");
        std::fs::write(&old, b"x").unwrap();
        // A non-.log file in the subdir must be left alone.
        let keep_nonlog = rips.join("notes.txt");
        std::fs::write(&keep_nonlog, b"x").unwrap();
        // A fresh top-level log must survive a cutoff in the past.
        let fresh = d.join("device_sg0.log");
        std::fs::write(&fresh, b"x").unwrap();

        // Backdate the archived log well past the cutoff.
        let old_time = std::time::SystemTime::now() - std::time::Duration::from_secs(40 * 86_400);
        filetime_set(&old, old_time);

        let cutoff = std::time::SystemTime::now() - std::time::Duration::from_secs(30 * 86_400);
        let pruned = prune_dir_recursive(&d, cutoff);

        assert_eq!(pruned, 1, "only the old archived log should be pruned");
        assert!(!old.exists(), "old archived log should be gone");
        assert!(keep_nonlog.exists(), "non-.log file must be kept");
        assert!(fresh.exists(), "fresh log must be kept");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// Set a file's mtime via libc::utimes (no extra crate dependency).
    fn filetime_set(path: &std::path::Path, t: std::time::SystemTime) {
        use std::os::unix::ffi::OsStrExt;
        let secs = t.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as libc::time_t;
        let tv = libc::timeval {
            tv_sec: secs,
            tv_usec: 0,
        };
        let times = [tv, tv];
        let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        let rc = unsafe { libc::utimes(c_path.as_ptr(), times.as_ptr()) };
        assert_eq!(rc, 0, "utimes failed");
    }
}
