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
    read_sample_units, resolve_and_apply,
};

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

/// The configured keydb path, or the standard default location.
fn keydb_path(cfg: &Config) -> PathBuf {
    cfg.keydb_path
        .clone()
        .map(Into::into)
        .or_else(|| libfreemkv::keydb::default_path().ok())
        .unwrap_or_else(|| PathBuf::from("keydb.cfg"))
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

    // Read content samples only if some source validates against ciphertext
    // (an online key service); a keydb / mapfile keys on disc identity alone and
    // the keydb's UK-first ordering already hands the terminal key out first.
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
        libfreemkv::Disc::read_aacs_inputs_from_drive(self.drive).ok()
    }
    fn volume_id(&self) -> Option<[u8; 16]> {
        self.vid
    }
    fn sample_units(&mut self, title: &libfreemkv::DiscTitle, n: usize) -> Vec<Vec<u8>> {
        read_sample_units(self.drive, title, n)
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
        libfreemkv::Disc::read_aacs_inputs(self.iso_path).ok()
    }
    fn volume_id(&self) -> Option<[u8; 16]> {
        libfreemkv::disc::mapfile::Mapfile::load(self.mapfile_path)
            .ok()
            .and_then(|m| m.vid())
    }
    fn sample_units(&mut self, title: &libfreemkv::DiscTitle, n: usize) -> Vec<Vec<u8>> {
        match libfreemkv::FileSectorSource::open(self.iso_path) {
            Ok(mut r) => read_sample_units(&mut r, title, n),
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

    /// Cross-side agreement: autorip's sample selector (`read_sample_units`)
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

        let units = read_sample_units(&mut reader, &title, SAMPLE_UNITS);
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
}
