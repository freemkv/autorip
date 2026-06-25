//! Where AACS keys come from.
//!
//! libfreemkv does no key lookup — it is handed a [`libfreemkv::Key`] and
//! derives down the AACS chain to decrypt. autorip resolves that key from one
//! or more *published key sources* (`freemkv_keysources`): the mapfile cache
//! (the resume fast-path) is tried first, then the configured source — a local
//! keydb (`local`) or a remote key service (`online`).
//!
//! The flow is the same for a live drive and a staged ISO: scan the disc
//! KEYLESS (structure + AACS inputs, no resolution), build [`DiscInputs`] from
//! its key files (+ content samples when a source needs them), then try each
//! source's candidate keys via [`libfreemkv::Disc::decrypt_with`] — the first
//! that derives unit keys wins. The only drive-vs-ISO difference is the
//! [`DiscKeyAccess`] impl.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use freemkv_keysources::{
    DiscInputs, KeySource, KeydbSource, MapfileSource, MultiSource, OnlineSource,
};
use libfreemkv::{read_encrypted_units, resolve_and_apply};

use crate::config::Config;

/// Is this resolved address one a key-service request must never reach?
/// Blocks loopback, link-local (incl. the 169.254.169.254 cloud metadata
/// endpoint), private RFC1918 / ULA, unspecified, and other non-global
/// ranges. Defense-in-depth at the request use-site: the web store-side
/// guard rejects most of these at save time, but `keyserver_url` is POSTed
/// verbatim at rip time, so we re-check here too.
fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local() // 169.254.0.0/16 — cloud metadata lives here
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_unspecified()
                || v4.is_multicast()
                || v4.octets()[0] == 0 // 0.0.0.0/8
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 0x40) // 100.64/10 CGNAT
                || v4.octets()[0] >= 240 // 240.0.0.0/4 Class-E reserved
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // fc00::/7 unique-local
                // to_ipv4() catches both ::ffff:a.b.c.d (mapped) AND ::a.b.c.d
                // (compatible); to_ipv4_mapped() misses the deprecated :: form.
                || v6.to_ipv4().map(|v4| is_blocked_ip(IpAddr::V4(v4))).unwrap_or(false)
        }
    }
}

/// Validate a key-service base URL before it is handed to `OnlineSource`.
/// Requires http(s), extracts the host, and rejects any host that is a
/// literal blocked IP or that resolves to one (SSRF / cloud-metadata
/// exfiltration guard). Returns the input on success so call sites can
/// gate construction.
fn validate_keyserver_url(raw: &str) -> Result<(), String> {
    let url = raw.trim();
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .ok_or_else(|| format!("keyserver URL must be http(s): {url}"))?;

    // host[:port] is everything before the first '/', '?' or '#'.
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .rsplit('@') // drop any userinfo
        .next()
        .unwrap_or("");
    if authority.is_empty() {
        return Err(format!("keyserver URL has no host: {url}"));
    }

    // Split host / port, handling bracketed IPv6 literals ([::1]:443).
    let (host, port): (String, u16) = if let Some(end) = authority.strip_prefix('[') {
        let (h, tail) = end
            .split_once(']')
            .ok_or_else(|| format!("malformed IPv6 host: {authority}"))?;
        let port = tail
            .strip_prefix(':')
            .and_then(|p| p.parse().ok())
            .unwrap_or(443);
        (h.to_string(), port)
    } else if let Some((h, p)) = authority.rsplit_once(':') {
        // Only treat the trailing segment as a port if it parses; otherwise
        // it's part of a bare IPv6 (which would have been bracketed) — fall
        // back to treating the whole thing as the host.
        match p.parse::<u16>() {
            Ok(port) => (h.to_string(), port),
            Err(_) => (authority.to_string(), 443),
        }
    } else {
        (authority.to_string(), 443)
    };

    // Literal IP? classify directly — no DNS, no rebind window.
    if let Ok(ip) = host.parse::<IpAddr>() {
        return if is_blocked_ip(ip) {
            Err(format!(
                "keyserver host {host} is a blocked/internal address"
            ))
        } else {
            Ok(())
        };
    }
    // Hostname — resolve (with a bounded deadline so a hung resolver can't
    // freeze the rip thread) and reject if ANY resolved address is blocked.
    let addrs = crate::web::resolve_with_timeout(&host, port)
        .map_err(|e| format!("keyserver host {host} did not resolve: {e}"))?;
    let mut saw_any = false;
    for sa in addrs {
        saw_any = true;
        if is_blocked_ip(sa.ip()) {
            return Err(format!(
                "keyserver host {host} resolves to a blocked/internal address ({})",
                sa.ip()
            ));
        }
    }
    if !saw_any {
        return Err(format!("keyserver host {host} resolved to no addresses"));
    }
    Ok(())
}

/// How many 6144-byte aligned encrypted units a sample-needing source is given.
pub const SAMPLE_UNITS: usize = 4;

/// What happened when resolving keys for a disc — carried back so the UI can
/// tell the user *why*, instead of a generic "missing keys".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyOutcome {
    /// A source's key derived unit keys — the disc now carries keys.
    Resolved,
    /// Couldn't read the disc's key files, or the disc reported no titles.
    MissingInputs,
    /// No configured source produced a key that decrypts this disc.
    NoKey,
    /// A source itself failed (e.g. key service unreachable / unreadable keydb).
    Unreachable,
}

/// The configured keydb path, or the service's standard default location.
///
/// This is the single source of truth for *where autorip's keydb lives* — both
/// the key *reads* (the scan/decrypt path) and the keydb *writes* (first-boot
/// download, daily refresh, the web "Update KEYDB" button) MUST resolve through
/// here so they agree. See [`save_keydb`] / [`keydb_exists`].
pub fn keydb_path(cfg: &Config) -> PathBuf {
    cfg.keydb_path
        .clone()
        .map(Into::into)
        .or_else(service_default_keydb)
        .unwrap_or_else(|| PathBuf::from("keydb.cfg"))
}

/// autorip's default keydb location: `$HOME/.config/freemkv/keydb.cfg`.
///
/// The container bind-mounts the keydb to `/root/.config/freemkv`, so the
/// service resolves it under the standard per-user config dir — NOT
/// libfreemkv's `default_path()`, which is local to the executable (correct for
/// the portable CLI, wrong for a containerized service whose binary and keydb
/// live in different places).
fn service_default_keydb() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config/freemkv/keydb.cfg"))
}

/// Does autorip's keydb already exist at the service-canonical path?
///
/// The startup gate (main.rs) MUST use this — not
/// `libfreemkv::keydb::default_path()`, which points at the exe-local path the
/// reads never consult. Using the same resolver as the reads means the
/// "already have a keydb, skip download" decision is made against the file the
/// rip will actually load. (Bug f750a5e fixed the reads but left the startup
/// gate and the writes on the exe-local path.)
pub fn keydb_exists(cfg: &Config) -> bool {
    keydb_path(cfg).exists()
}

/// Validate and persist raw keydb bytes to autorip's service-canonical path.
///
/// `libfreemkv::keydb::save` does the right *validation* (zip/gz/plain
/// extraction, entry-count check, crash-safe atomic write) but writes to its
/// own exe-local `default_path()` — the wrong target for a containerized
/// service whose keydb is bind-mounted at `$HOME/.config/freemkv/keydb.cfg` and
/// whose reads resolve through [`keydb_path`]. We reuse libfreemkv for the
/// validation+decompression (no duplicated zip/gz logic, no extra deps), then
/// relocate the validated file onto the service path with an atomic rename so
/// the write target and the read target agree.
///
/// Returns the libfreemkv `UpdateResult` with its `path` field rewritten to the
/// service-canonical destination actually written.
pub fn save_keydb(
    cfg: &Config,
    data: &[u8],
) -> std::result::Result<libfreemkv::keydb::UpdateResult, libfreemkv::Error> {
    // libfreemkv validates + decompresses + writes to its exe-local default.
    let mut result = libfreemkv::keydb::save(data)?;
    let src = result.path.clone();
    let dest = keydb_path(cfg);

    // Already the canonical target (e.g. an operator who set keydb_path to the
    // exe-local path, or a single-binary deployment) — nothing to relocate.
    if src == dest {
        return Ok(result);
    }

    if let Some(dir) = dest.parent() {
        if let Err(e) = std::fs::create_dir_all(dir) {
            tracing::warn!(error = %e, dest = %dest.display(), "keydb: create dest dir failed");
            let _ = std::fs::remove_file(&src);
            return Err(libfreemkv::Error::KeydbWrite {
                path: dest.display().to_string(),
            });
        }
    }

    // Prefer a same-filesystem atomic rename; fall back to copy+remove when the
    // validated file and the bind-mounted dest live on different mounts (the
    // common container case: exe on the image layer, keydb on a bind volume —
    // rename across mounts returns EXDEV).
    let relocated = std::fs::rename(&src, &dest).or_else(|_| {
        std::fs::copy(&src, &dest).map(|_| ()).inspect(|_| {
            let _ = std::fs::remove_file(&src);
        })
    });
    if let Err(e) = relocated {
        tracing::warn!(error = %e, src = %src.display(), dest = %dest.display(),
            "keydb: relocate to service path failed; keydb may be at exe-local path");
        let _ = std::fs::remove_file(&src);
        return Err(libfreemkv::Error::KeydbWrite {
            path: dest.display().to_string(),
        });
    }

    result.path = dest;
    Ok(result)
}

/// ScanOptions for a **live-drive** structure scan. Lookup-free (the library
/// resolves no keys), plus the AACS host credentials for the authenticated
/// handshake — sourced from the keydb, *independent of `key_source`* (a locked
/// drive needs the cert even in online mode; an unlocked / LibreDrive drive
/// takes the OEM Volume-ID path and ignores them).
pub fn drive_scan_opts(cfg: &Config) -> libfreemkv::ScanOptions {
    drive_scan_opts_for_keydb(&keydb_path(cfg))
}

/// Live-drive [`ScanOptions`](libfreemkv::ScanOptions) with host credentials
/// sourced from a specific keydb path — the handshake's only keydb dependency
/// (an unlocked / LibreDrive drive ignores them).
pub fn drive_scan_opts_for_keydb(keydb: &Path) -> libfreemkv::ScanOptions {
    let host_certs = KeydbSource::new(keydb).host_certs();
    let credentials =
        (!host_certs.is_empty()).then_some(libfreemkv::DriveCredentials { host_certs });
    libfreemkv::ScanOptions {
        credentials,
        ..Default::default()
    }
}

/// ScanOptions for an **ISO** structure scan — no handshake, no credentials.
pub fn iso_scan_opts() -> libfreemkv::ScanOptions {
    libfreemkv::ScanOptions::default()
}

/// Build the ordered key-source list from config.
///
/// The mapfile cache (when a rip mapfile exists) is tried first — it holds
/// already-resolved unit keys, so a resume needs no keydb parse and no network.
/// Then the configured source: `online` → the remote key service, anything
/// else → the local keydb (explicit path, else the standard location).
pub fn build_sources(cfg: &Config, mapfile: Option<&Path>) -> Vec<Box<dyn KeySource>> {
    let mut sources: Vec<Box<dyn KeySource>> = Vec::new();
    if let Some(mf) = mapfile {
        sources.push(Box::new(MapfileSource::new(mf)));
    }
    match cfg.key_source.as_str() {
        "online" => match validate_keyserver_url(&cfg.keyserver_url) {
            Ok(()) => sources.push(Box::new(OnlineSource::new(
                cfg.keyserver_url.clone(),
                cfg.keyserver_secret.clone(),
            ))),
            // SSRF defense-in-depth: refuse to POST disc-key material to an
            // internal / metadata address. Drop the online source entirely
            // (the mapfile cache, if present, still applies) rather than
            // hand `OnlineSource` a URL we won't trust. The web store-side
            // guard normally rejects these at save time; this covers a
            // value that slipped past it or predates that guard.
            Err(e) => {
                tracing::error!(
                    phase = "key_resolve",
                    url_origin = %crate::webhook::webhook_url_origin(&cfg.keyserver_url),
                    "keyserver URL rejected (SSRF guard): {e} — online key source disabled for this rip"
                );
            }
        },
        "local" => sources.push(Box::new(KeydbSource::new(keydb_path(cfg)))),
        other => {
            // key_source is user-edited config; a typo ("onlnie") would
            // silently resolve keydb-only when the operator meant online.
            // Fall back to the local keydb but make the fallback visible.
            tracing::warn!(
                key_source = %other,
                "unrecognised key_source; falling back to local keydb"
            );
            sources.push(Box::new(KeydbSource::new(keydb_path(cfg))));
        }
    }
    sources
}

/// Whether the configured (non-mapfile) source talks to a remote key service —
/// used by the UI to announce a potentially slow keyserver round-trip.
pub fn uses_online(cfg: &Config) -> bool {
    cfg.key_source == "online"
}

/// How a disc's key-resolution inputs are obtained. Decouples [`resolve_keys`]
/// from WHERE the disc lives — a live drive or a staged ISO — so the resolution
/// logic is written once. See [`DriveAccess`] and [`IsoAccess`].
pub trait DiscKeyAccess {
    /// The disc's `Unit_Key_RO.inf` + `MKB` bytes.
    fn key_files(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
    /// The 16-byte AACS Volume ID, if available.
    fn volume_id(&self) -> Option<[u8; 16]>;
    /// Up to `n` encrypted aligned units sampled from the disc's content.
    fn sample_units(&mut self, title: &libfreemkv::DiscTitle, n: usize) -> Vec<Vec<u8>>;
}

/// Resolve keys for `disc` via the ordered `sources`, reading inputs through
/// `access`. Returns the disc with keys applied (`Resolved`) or unchanged.
///
/// The disc must have been scanned KEYLESS (see [`drive_scan_opts`] /
/// [`iso_scan_opts`]). Each
/// source offers candidate keys; the first whose [`libfreemkv::Disc::decrypt_with`]
/// derives unit keys wins. A wrong candidate (e.g. a device-key set that does
/// not apply to this disc's MKB) is rejected by `decrypt_with` and the next
/// candidate / source is tried. `decrypt_with` only mutates the disc on success,
/// so a rejected candidate leaves it untouched.
pub fn resolve_keys<A: DiscKeyAccess>(
    sources: Vec<Box<dyn KeySource>>,
    access: &mut A,
    mut disc: libfreemkv::Disc,
) -> (libfreemkv::Disc, KeyOutcome) {
    let Some((inf, mkb)) = access.key_files() else {
        tracing::warn!(phase = "key_resolve", "could not read disc key files");
        return (disc, KeyOutcome::MissingInputs);
    };
    let vid = access.volume_id().unwrap_or_else(|| {
        tracing::warn!(
            phase = "key_resolve",
            "no Volume ID available; using all-zero VID — key derivation may fail"
        );
        [0u8; 16]
    });

    // Read content samples only if some source needs ciphertext validation:
    // an online key service, OR a keydb (it can hand out a per-disc terminal
    // `Key::Unit` that `decrypt_with` applies as-is — a hash-matching but
    // wrong UK is only disproved by descrambling real ciphertext). A mapfile
    // alone keys on already-validated unit keys and needs no sample.
    let need_samples = sources.iter().any(|s| s.needs_samples());
    let samples = if need_samples {
        match disc.titles.iter().max_by_key(|t| t.size_bytes).cloned() {
            Some(title) => access.sample_units(&title, SAMPLE_UNITS),
            None => {
                tracing::warn!(
                    phase = "key_resolve",
                    "no titles — cannot sample for key service"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let inputs = DiscInputs {
        disc_hash: libfreemkv::aacs::disc_hash_hex(&libfreemkv::aacs::disc_hash(&inf)),
        volume_id: vid,
        mkb,
        unit_key_ro: inf,
        samples,
        // The disc's own title (UDF/ISO volume id, else BDMV name) so the key
        // service can catalog hash→title from our ripped discs.
        volume_label: {
            let v = disc.volume_id.trim();
            if v.is_empty() {
                disc.meta_title.clone()
            } else {
                Some(v.to_string())
            }
        },
    };

    // One ordered driver, one shared loop: hand each source's candidates (one at
    // a time) to `Disc::decrypt_with` — which validates and only keeps a key that
    // decrypts — and stop at the first that takes.
    let mut sources = MultiSource::new(sources);
    if resolve_and_apply(&mut sources, &inputs, &mut disc) {
        tracing::info!(phase = "key_resolve", "key resolved — disc now keyed");
        return (disc, KeyOutcome::Resolved);
    }

    // Every source exhausted with no key. A source that FAILED (an unreachable
    // key service) is reported distinctly from a clean "no key for this disc".
    let outcome = if sources.errored() {
        KeyOutcome::Unreachable
    } else {
        KeyOutcome::NoKey
    };
    (disc, outcome)
}

/// [`DiscKeyAccess`] backed by a live optical drive. `vid` is the Volume ID
/// from the structure scan (the drive read it during AACS auth).
pub struct DriveAccess<'a> {
    drive: &'a mut libfreemkv::Drive,
    vid: Option<[u8; 16]>,
}

impl<'a> DriveAccess<'a> {
    pub fn new(drive: &'a mut libfreemkv::Drive, vid: Option<[u8; 16]>) -> Self {
        Self { drive, vid }
    }
}

impl DiscKeyAccess for DriveAccess<'_> {
    fn key_files(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        match libfreemkv::Disc::read_aacs_inputs_from_drive(self.drive) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(
                    phase = "key_resolve",
                    error = %e,
                    "read_aacs_inputs_from_drive failed"
                );
                None
            }
        }
    }
    fn volume_id(&self) -> Option<[u8; 16]> {
        self.vid
    }
    fn sample_units(&mut self, title: &libfreemkv::DiscTitle, n: usize) -> Vec<Vec<u8>> {
        read_encrypted_units(self.drive, title, n)
    }
}

/// [`DiscKeyAccess`] backed by a staged ISO + its mapfile (the resume path).
/// The Volume ID is recovered from the mapfile (the ISO doesn't carry it).
pub struct IsoAccess<'a> {
    iso_path: &'a Path,
    mapfile_path: &'a Path,
}

impl<'a> IsoAccess<'a> {
    pub fn new(iso_path: &'a Path, mapfile_path: &'a Path) -> Self {
        Self {
            iso_path,
            mapfile_path,
        }
    }
}

impl DiscKeyAccess for IsoAccess<'_> {
    fn key_files(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        match libfreemkv::Disc::read_aacs_inputs(self.iso_path) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(
                    phase = "key_resolve",
                    path = %self.iso_path.display(),
                    error = %e,
                    "read_aacs_inputs failed"
                );
                None
            }
        }
    }
    fn volume_id(&self) -> Option<[u8; 16]> {
        libfreemkv::disc::mapfile::Mapfile::load(self.mapfile_path)
            .ok()
            .and_then(|m| m.vid())
    }
    fn sample_units(&mut self, title: &libfreemkv::DiscTitle, n: usize) -> Vec<Vec<u8>> {
        match libfreemkv::FileSectorSource::open(self.iso_path) {
            Ok(mut r) => read_encrypted_units(&mut r, title, n),
            Err(err) => {
                // Without samples an online key request fires with no
                // units_b64 and can fail later as NoKey with no visible cause;
                // surface the real reason here.
                tracing::warn!(
                    phase = "key_resolve",
                    path = %self.iso_path.display(),
                    %err,
                    "could not open ISO to sample units"
                );
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes tests that drive `save_keydb` → `libfreemkv::keydb::save`,
    /// which writes to the single process-wide exe-local `default_path()`
    /// (`<exe_dir>/keydb.cfg`) before `save_keydb` relocates it. Two such tests
    /// running in parallel race on that shared file: one renames it out from
    /// under the other, surfacing a spurious `KeydbWrite`. The lock makes the
    /// write→relocate sequence atomic across the test binary.
    static SAVE_KEYDB_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn ssrf_guard_blocks_metadata_and_internal_hosts() {
        // Cloud metadata endpoint — the canonical SSRF target.
        assert!(validate_keyserver_url("http://169.254.169.254/latest/meta-data").is_err());
        // Loopback and RFC1918.
        assert!(validate_keyserver_url("https://127.0.0.1:8443/keys").is_err());
        // RFC1918 ranges (10/8, 192.168/16, 172.16/12). Built from octets so the
        // literal dotted-quads don't trip the public leak-guard — these are
        // generic examples, not infrastructure.
        for oct in [[10u8, 0, 0, 1], [192, 168, 1, 5], [172, 20, 4, 4]] {
            let url = format!("https://{}.{}.{}.{}/keys", oct[0], oct[1], oct[2], oct[3]);
            assert!(
                validate_keyserver_url(&url).is_err(),
                "RFC1918 {url} must be rejected"
            );
        }
        // IPv6 loopback / link-local (bracketed).
        assert!(validate_keyserver_url("https://[::1]:443/k").is_err());
        assert!(validate_keyserver_url("https://[fe80::1]/k").is_err());
        // IPv4-mapped IPv6 loopback.
        assert!(validate_keyserver_url("https://[::ffff:127.0.0.1]/k").is_err());
        // Non-http scheme rejected.
        assert!(validate_keyserver_url("ftp://example.com/keys").is_err());
        // No host.
        assert!(validate_keyserver_url("https:///keys").is_err());
    }

    #[test]
    fn ssrf_guard_allows_public_literal_ip() {
        // A public literal IP must pass (no DNS needed, deterministic).
        assert!(validate_keyserver_url("https://8.8.8.8/keys").is_ok());
        assert!(validate_keyserver_url("https://1.1.1.1:443").is_ok());
    }

    #[test]
    fn ssrf_classifier_ranges() {
        use std::net::{Ipv4Addr, Ipv6Addr};
        assert!(is_blocked_ip(Ipv4Addr::new(169, 254, 169, 254).into()));
        assert!(is_blocked_ip(Ipv4Addr::new(10, 0, 0, 1).into()));
        assert!(is_blocked_ip(Ipv4Addr::new(127, 0, 0, 1).into()));
        assert!(is_blocked_ip(Ipv4Addr::new(100, 64, 0, 1).into())); // CGNAT
        assert!(is_blocked_ip(Ipv4Addr::new(0, 0, 0, 0).into()));
        assert!(!is_blocked_ip(Ipv4Addr::new(8, 8, 8, 8).into()));
        assert!(!is_blocked_ip(Ipv4Addr::new(1, 1, 1, 1).into()));
        assert!(is_blocked_ip(Ipv6Addr::LOCALHOST.into()));
        assert!(!is_blocked_ip(
            "2606:4700:4700::1111".parse::<Ipv6Addr>().unwrap().into()
        ));
    }

    /// Regression: to_ipv4_mapped() missed the deprecated IPv4-compatible form
    /// (::a.b.c.d) and both v4+v6 multicast and Class-E were absent.
    /// These must all be blocked — divergence from web.rs's is_blocked_ip is
    /// a SSRF middle-layer gap.
    #[test]
    fn ssrf_classifier_ipv4_compat_multicast_class_e() {
        use std::net::{Ipv4Addr, Ipv6Addr};

        // IPv4-compatible ::127.0.0.1 (deprecated form, segments 0:0:0:0:0:0:7f00:1).
        // to_ipv4_mapped() returns None for this; to_ipv4() returns Some(127.0.0.1).
        let ipv4_compat_loopback = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0x7f00, 0x0001);
        assert!(
            is_blocked_ip(ipv4_compat_loopback.into()),
            "::127.0.0.1 (IPv4-compatible) must be blocked"
        );

        // IPv4-compatible mapping of an RFC1918 address (deprecated ::a.b.c.d form).
        let ipv4_compat_private = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0x0a00, 0x0001);
        assert!(
            is_blocked_ip(ipv4_compat_private.into()),
            "IPv4-compatible RFC1918 address must be blocked"
        );

        // IPv4 multicast 224.0.0.1.
        assert!(
            is_blocked_ip(Ipv4Addr::new(224, 0, 0, 1).into()),
            "IPv4 multicast must be blocked"
        );
        // IPv4 multicast 239.255.255.255 (upper boundary).
        assert!(
            is_blocked_ip(Ipv4Addr::new(239, 255, 255, 255).into()),
            "IPv4 multicast upper boundary must be blocked"
        );

        // IPv6 multicast ff02::1.
        assert!(
            is_blocked_ip(Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 1).into()),
            "IPv6 multicast must be blocked"
        );

        // Class-E 240.0.0.0/4 (reserved, not public).
        assert!(
            is_blocked_ip(Ipv4Addr::new(240, 0, 0, 1).into()),
            "Class-E 240.0.0.1 must be blocked"
        );
        assert!(
            is_blocked_ip(Ipv4Addr::new(255, 255, 255, 254).into()),
            "Class-E 255.255.255.254 must be blocked"
        );

        // Sanity: public addresses must still be allowed.
        assert!(!is_blocked_ip(Ipv4Addr::new(8, 8, 8, 8).into()));
        assert!(!is_blocked_ip(
            "2606:4700:4700::1111".parse::<Ipv6Addr>().unwrap().into()
        ));
    }

    /// Cross-side agreement: autorip's sample selector (`read_encrypted_units`)
    /// hands the key service only units the service's own gate accepts —
    /// because both sides call the SAME predicate,
    /// `libfreemkv::aacs::is_aacs_scrambled`.
    #[test]
    fn sample_units_are_all_aacs_scrambled() {
        use std::io::Write;

        // Synthetic ISO: 1200 sectors of scrambled (non-TS) content — no 0x47 at
        // any TS sync offset, so every aligned unit reads as AACS-scrambled.
        const SECTORS: usize = 1200;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&vec![0xE5u8; SECTORS * 2048]).unwrap();
        tmp.flush().unwrap();
        let mut reader = libfreemkv::FileSectorSource::open(tmp.path()).unwrap();

        let title = libfreemkv::DiscTitle {
            playlist: "00800.mpls".into(),
            playlist_id: 800,
            duration_secs: 0.0,
            size_bytes: (SECTORS * 2048) as u64,
            clips: Vec::new(),
            streams: Vec::new(),
            chapters: Vec::new(),
            extents: vec![libfreemkv::Extent {
                start_lba: 0,
                sector_count: SECTORS as u32,
            }],
            content_format: libfreemkv::ContentFormat::BdTs,
            codec_privates: Vec::new(),
        };

        let units = read_encrypted_units(&mut reader, &title, SAMPLE_UNITS);
        assert_eq!(units.len(), SAMPLE_UNITS, "should collect 4 sample units");
        for u in &units {
            assert_eq!(u.len(), 6144);
            assert!(
                libfreemkv::aacs::is_aacs_scrambled(u),
                "selector must only emit units the key service accepts"
            );
        }

        // The converse: a clear unit (TS syncs intact) is NOT scrambled.
        let mut clear = vec![0u8; 6144];
        let mut off = 4;
        while off < 6144 {
            clear[off] = 0x47;
            off += 192;
        }
        assert!(!libfreemkv::aacs::is_aacs_scrambled(&clear));
    }

    /// Regression for the rc.6 keydb path split: autorip's keydb *writes* and
    /// the startup *existence check* must land on the same service-canonical
    /// path the *reads* resolve through (keydb_path / keydb_exists), NOT
    /// libfreemkv's exe-local default_path. Before the fix, save() wrote to the
    /// exe dir while reads looked under $HOME/.config/freemkv, so a fresh
    /// container "downloaded" a keydb every boot yet every AACS rip still failed.
    #[test]
    fn save_keydb_writes_to_service_path_and_existence_agrees() {
        let _guard = SAVE_KEYDB_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("keys").join("keydb.cfg");

        let mut cfg = Config::default();
        cfg.keydb_path = Some(dest.to_string_lossy().into_owned());

        // The path the reads will resolve and the gate must check.
        assert_eq!(keydb_path(&cfg), dest);
        assert!(!keydb_exists(&cfg), "no keydb written yet");

        // A minimal valid keydb body (one `0x...` entry line) — libfreemkv's
        // save() validation accepts `0x`-prefixed lines as entries.
        let body = b"0xDEADBEEFDEADBEEFDEADBEEFDEADBEEF\n";
        let result = save_keydb(&cfg, body).expect("save_keydb must succeed");

        // It wrote to the service path, not the libfreemkv exe-local default.
        assert_eq!(result.path, dest, "save must target the service path");
        assert!(dest.exists(), "keydb file must exist at the service path");
        assert!(
            keydb_exists(&cfg),
            "startup existence gate must now see the keydb the write produced"
        );
        assert_eq!(result.entries, 1, "one 0x entry");

        // And the libfreemkv exe-local default must NOT be where it ended up
        // (the bug was the two paths diverging).
        if let Ok(exe_local) = libfreemkv::keydb::default_path() {
            assert_ne!(
                exe_local, dest,
                "test only meaningful when the exe-local default differs from the dest"
            );
            // The validated file was relocated off the exe-local path.
            assert!(
                !exe_local.exists() || exe_local != dest,
                "the validated keydb must not be stranded at the exe-local path"
            );
        }

        // Content round-trips: the bytes at the service path are the keydb text.
        let written = std::fs::read_to_string(&dest).unwrap();
        assert!(
            written.contains("0xDEADBEEF"),
            "keydb content must be present"
        );
    }

    // --- keydb path resolution: the four resolvers must agree (rc.6 WS3) -----

    /// With no explicit `keydb_path`, all the resolvers fall through to the
    /// SAME service default (`$HOME/.config/freemkv/keydb.cfg`): `keydb_path`,
    /// `service_default_keydb`, and the read path (`drive_scan_opts` builds its
    /// `KeydbSource` from `keydb_path`). The rc.6 bug was the reads going to
    /// `$HOME/.config/freemkv` while the writes/gate went to libfreemkv's
    /// exe-local default; this pins they resolve to one location.
    ///
    /// Reads the ambient `$HOME` rather than mutating it — mutating the global
    /// env races with `libfreemkv::keydb::default_path()` in sibling tests.
    #[test]
    fn keydb_resolvers_all_agree_on_service_default() {
        let Some(home) = std::env::var_os("HOME") else {
            // No HOME in this environment — the default falls back to a bare
            // relative "keydb.cfg"; assert that fallback instead.
            let cfg = Config::default();
            assert_eq!(keydb_path(&cfg), PathBuf::from("keydb.cfg"));
            return;
        };

        let cfg = Config::default();
        assert_eq!(
            cfg.keydb_path, None,
            "default config must carry no explicit keydb_path"
        );

        let expected = PathBuf::from(home).join(".config/freemkv/keydb.cfg");
        assert_eq!(
            service_default_keydb(),
            Some(expected.clone()),
            "service default must live under $HOME/.config/freemkv"
        );
        assert_eq!(
            keydb_path(&cfg),
            expected,
            "keydb_path with no override must resolve to the service default, \
             NOT a bare relative path or libfreemkv's exe-local default"
        );
        // The existence gate resolves through the same path the reads use.
        assert_eq!(keydb_exists(&cfg), expected.exists());
    }

    /// An explicit `keydb_path` in config overrides the service default, and the
    /// existence gate + the read path both honor that override (so an operator
    /// who points autorip at a non-standard keydb gets reads, writes, and the
    /// startup gate all aimed at the same file).
    #[test]
    fn explicit_keydb_path_overrides_default_and_gate_honors_it() {
        let tmp = tempfile::tempdir().unwrap();
        let explicit = tmp.path().join("custom").join("mykeys.cfg");

        let mut cfg = Config::default();
        cfg.keydb_path = Some(explicit.to_string_lossy().into_owned());

        assert_eq!(
            keydb_path(&cfg),
            explicit,
            "explicit keydb_path must win over the service default"
        );
        assert!(!keydb_exists(&cfg), "file not created yet");

        std::fs::create_dir_all(explicit.parent().unwrap()).unwrap();
        std::fs::write(&explicit, b"0xAAAA\n").unwrap();
        assert!(
            keydb_exists(&cfg),
            "existence gate must see the file at the explicit path"
        );
    }

    /// `save_keydb` is idempotent on the dest path: when the validated file is
    /// already AT the service-canonical destination (operator pointed
    /// `keydb_path` straight at the exe-local default, or a single-binary
    /// deploy), the early-return src==dest branch leaves the file in place and
    /// reports that path — no spurious relocate, no data loss.
    #[test]
    fn save_keydb_is_idempotent_when_src_equals_dest() {
        let _guard = SAVE_KEYDB_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // Point keydb_path at exactly libfreemkv's exe-local default so
        // save()'s own write target equals our dest → src==dest branch.
        let Ok(exe_local) = libfreemkv::keydb::default_path() else {
            // No exe-local default resolvable in this environment — skip.
            return;
        };
        let mut cfg = Config::default();
        cfg.keydb_path = Some(exe_local.to_string_lossy().into_owned());
        assert_eq!(keydb_path(&cfg), exe_local);

        let result = save_keydb(&cfg, b"0xBEEF\n").expect("save must succeed");
        assert_eq!(
            result.path, exe_local,
            "src==dest path must report the canonical (== exe-local here) target"
        );
        assert!(exe_local.exists(), "file must be present at the dest");
        // Clean up the file we wrote to the shared exe-local default so we don't
        // leak state into other tests / runs.
        let _ = std::fs::remove_file(&exe_local);
    }

    // --- KeyOutcome reporting via resolve_keys (rc.6 WS3) --------------------

    /// A minimal keyless, encrypted `Disc` for driving `resolve_keys` outcome
    /// classification. No real AACS state — the outcome (MissingInputs / NoKey /
    /// Unreachable) is decided by the fixtures' behavior, not the disc.
    fn keyless_encrypted_disc() -> libfreemkv::Disc {
        libfreemkv::Disc {
            volume_id: "TEST_DISC".into(),
            meta_title: None,
            format: libfreemkv::DiscFormat::BluRay,
            capacity_sectors: 0,
            capacity_bytes: 0,
            layers: 1,
            titles: Vec::new(),
            region: libfreemkv::disc::DiscRegion::Free,
            aacs: None,
            css: None,
            encrypted: true,
            aacs_error: None,
            css_error: None,
            content_format: libfreemkv::ContentFormat::BdTs,
        }
    }

    /// A `KeySource` fixture that hands out NO keys and never errors — models a
    /// source that simply has no key for this disc (e.g. an empty keydb).
    struct NoKeySource;
    impl KeySource for NoKeySource {
        fn next_key(&mut self, _inputs: &DiscInputs) -> Option<libfreemkv::Key> {
            None
        }
    }

    /// A `KeySource` fixture that hands out no keys but reports `errored()` —
    /// models a source that FAILED (e.g. key service unreachable / unreadable
    /// keydb), the distinction `resolve_keys` maps to `Unreachable`.
    struct ErroringSource;
    impl KeySource for ErroringSource {
        fn next_key(&mut self, _inputs: &DiscInputs) -> Option<libfreemkv::Key> {
            None
        }
        fn errored(&self) -> bool {
            true
        }
    }

    /// `DiscKeyAccess` fixture whose `key_files()` returns the given option;
    /// `volume_id` is a fixed all-zero VID; `sample_units` yields nothing (none
    /// of the outcome tests use a sample-needing source).
    struct FixtureAccess {
        key_files: Option<(Vec<u8>, Vec<u8>)>,
    }
    impl DiscKeyAccess for FixtureAccess {
        fn key_files(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
            self.key_files.clone()
        }
        fn volume_id(&self) -> Option<[u8; 16]> {
            Some([0u8; 16])
        }
        fn sample_units(&mut self, _t: &libfreemkv::DiscTitle, _n: usize) -> Vec<Vec<u8>> {
            Vec::new()
        }
    }

    /// `key_files()` returning None → `MissingInputs`, regardless of what sources
    /// are configured (we never even reach key resolution).
    #[test]
    fn resolve_keys_reports_missing_inputs_when_key_files_unreadable() {
        let mut access = FixtureAccess { key_files: None };
        let sources: Vec<Box<dyn KeySource>> = vec![Box::new(NoKeySource)];
        let (_disc, outcome) = resolve_keys(sources, &mut access, keyless_encrypted_disc());
        assert_eq!(
            outcome,
            KeyOutcome::MissingInputs,
            "unreadable key files must report MissingInputs, not NoKey"
        );
    }

    /// Key files present, sources exhausted with NO key and NO error →
    /// `NoKey` (a clean "no source has a key for this disc").
    #[test]
    fn resolve_keys_reports_no_key_when_sources_exhausted_clean() {
        let mut access = FixtureAccess {
            key_files: Some((b"inf".to_vec(), b"mkb".to_vec())),
        };
        let sources: Vec<Box<dyn KeySource>> = vec![Box::new(NoKeySource)];
        let (_disc, outcome) = resolve_keys(sources, &mut access, keyless_encrypted_disc());
        assert_eq!(outcome, KeyOutcome::NoKey);
    }

    /// Key files present, a source FAILED (errored) and produced no key →
    /// `Unreachable`. This is the distinction the dashboard tile renders as
    /// "no key source could be reached" rather than "no key for this disc".
    #[test]
    fn resolve_keys_reports_unreachable_when_a_source_errored() {
        let mut access = FixtureAccess {
            key_files: Some((b"inf".to_vec(), b"mkb".to_vec())),
        };
        // Mix a clean no-key source with a failed one — `errored()` is an
        // any() across the MultiSource, so the failure must dominate.
        let sources: Vec<Box<dyn KeySource>> =
            vec![Box::new(NoKeySource), Box::new(ErroringSource)];
        let (_disc, outcome) = resolve_keys(sources, &mut access, keyless_encrypted_disc());
        assert_eq!(
            outcome,
            KeyOutcome::Unreachable,
            "a failed source must surface as Unreachable, not NoKey"
        );
    }

    /// The four `KeyOutcome` variants are distinct — a regression guard so a
    /// future refactor can't accidentally collapse e.g. Unreachable into NoKey
    /// (which would erase the "check your keyserver/network" guidance).
    #[test]
    fn key_outcome_variants_are_distinct() {
        let all = [
            KeyOutcome::Resolved,
            KeyOutcome::MissingInputs,
            KeyOutcome::NoKey,
            KeyOutcome::Unreachable,
        ];
        for (i, a) in all.iter().enumerate() {
            for (j, b) in all.iter().enumerate() {
                assert_eq!(
                    i == j,
                    a == b,
                    "{a:?} vs {b:?} equality must track identity"
                );
            }
        }
    }

    // --- build_sources ordering / SSRF / fallback (rc.6 WS3) -----------------

    /// `build_sources` puts the mapfile source FIRST (the resume fast-path) when
    /// a mapfile is supplied, ahead of the configured local keydb.
    #[test]
    fn build_sources_puts_mapfile_first_then_configured() {
        let tmp = tempfile::tempdir().unwrap();
        let mf = tmp.path().join("disc.map");
        let mut cfg = Config::default();
        cfg.key_source = "local".into();
        let sources = build_sources(&cfg, Some(&mf));
        assert_eq!(sources.len(), 2, "mapfile + local keydb");
        // The configured local keydb lands AFTER the mapfile (it labels itself
        // "keydb"); index 0 is therefore the resume mapfile cache, which must be
        // tried first. (MapfileSource keeps the default "source" label, so we
        // pin the order via the keydb's distinguishing label at index 1.)
        assert_eq!(
            sources[1].label(),
            "keydb",
            "the configured local keydb must come after the resume mapfile cache"
        );
        // Sanity: with no mapfile, only the configured keydb is present.
        let no_mf = build_sources(&cfg, None);
        assert_eq!(no_mf.len(), 1);
        assert_eq!(no_mf[0].label(), "keydb");
    }

    /// An online `key_source` with an SSRF-blocked URL drops the online source
    /// entirely (rather than handing OnlineSource a URL we won't trust). With no
    /// mapfile that leaves ZERO sources — the rip then surfaces NoKey instead of
    /// exfiltrating disc-key material to an internal address.
    #[test]
    fn build_sources_drops_online_source_on_ssrf_blocked_url() {
        let mut cfg = Config::default();
        cfg.key_source = "online".into();
        cfg.keyserver_url = "http://169.254.169.254/keys".into();
        let sources = build_sources(&cfg, None);
        assert!(
            sources.is_empty(),
            "SSRF-blocked online URL must yield no usable source"
        );
    }

    /// An unrecognised `key_source` (operator typo like "onlnie") falls back to
    /// the local keydb rather than silently producing no source.
    #[test]
    fn build_sources_unknown_key_source_falls_back_to_local_keydb() {
        let mut cfg = Config::default();
        cfg.key_source = "onlnie".into();
        let sources = build_sources(&cfg, None);
        assert_eq!(sources.len(), 1, "fallback to a single local keydb source");
        assert!(!uses_online(&cfg), "a typo'd source is not 'online'");
    }
}
