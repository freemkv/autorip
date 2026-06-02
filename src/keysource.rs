//! Where AACS keys come from.
//!
//! Pluggable by design. Today there are two sources — `local` (an on-disk key
//! database that libfreemkv resolves against during the scan) and `online` (a
//! remote key service) — and more can be added as `KeySource` variants without
//! touching the rip loop.
//!
//! A source either resolves keys *inline during the scan* (local: it just hands
//! libfreemkv a `keydb` path) or *from the disc after a structure scan*
//! (sample-based sources: scan keyless, take a few on-disc samples, ask the
//! source for the Unit Key, then re-scan with it). The rip/resume code drives
//! both shapes through this one type.

use std::path::{Path, PathBuf};

use crate::config::Config;

/// How many 6144-byte aligned units a sample-based source is given.
pub const SAMPLE_UNITS: usize = 4;

/// A configured AACS key source.
pub enum KeySource {
    /// Resolve against a local key database (libfreemkv does it during scan).
    Local { keydb_path: Option<PathBuf> },
    /// Resolve via a remote key service from the disc's files + samples.
    Online(OnlineKeyService),
}

impl KeySource {
    /// Select the source from configuration.
    pub fn from_config(cfg: &Config) -> Self {
        match cfg.key_source.as_str() {
            "online" => KeySource::Online(OnlineKeyService::new(
                cfg.keyserver_url.clone(),
                cfg.keyserver_secret.clone(),
            )),
            _ => KeySource::Local {
                keydb_path: cfg.keydb_path.clone().map(Into::into),
            },
        }
    }

    /// `ScanOptions` for the structure scan. A local source carries the key
    /// database so keys resolve inline; a sample-based source scans keyless and
    /// resolves afterward via [`resolve`](Self::resolve).
    pub fn scan_options(&self) -> libfreemkv::ScanOptions {
        match self {
            KeySource::Local { keydb_path } => libfreemkv::ScanOptions {
                keydb_path: keydb_path.clone(),
                ..Default::default()
            },
            // Online resolves keys out-of-band via the key service. Disable the
            // local keydb entirely so a keydb that happens to sit in a default
            // search path can't shadow the service (the radio means "server,
            // not local keydb").
            KeySource::Online(_) => libfreemkv::ScanOptions {
                disable_keydb: true,
                ..Default::default()
            },
        }
    }

    /// Whether this source resolves a Unit Key from on-disc samples taken AFTER
    /// a structure scan, rather than inline during the scan.
    pub fn needs_samples(&self) -> bool {
        matches!(self, KeySource::Online(_))
    }

    /// Resolve a Unit Key for a disc from its key files + on-disc content
    /// samples. Returns `None` if unavailable. A local source resolves inline at
    /// scan time, so it returns `None` here.
    pub fn resolve(
        &self,
        inf: &[u8],
        mkb: &[u8],
        vid: Option<[u8; 16]>,
        units: &[Vec<u8>],
    ) -> Option<[u8; 16]> {
        match self {
            KeySource::Local { .. } => None,
            KeySource::Online(svc) => svc.fetch(inf, mkb, vid, units),
        }
    }
}

/// Client for a remote key service. autorip treats it as an opaque third party:
/// it sends the disc's files and a few on-disc samples and receives a Unit Key
/// or nothing. It makes no assumptions about how the service produces the key.
pub struct OnlineKeyService {
    base_url: String,
    secret: String,
}

impl OnlineKeyService {
    fn new(base_url: String, secret: String) -> Self {
        Self { base_url, secret }
    }

    /// `POST <base_url>/decode` with a JSON body of base64 fields
    /// (`inf_b64`, `mkb_b64`, `vid_b64`, `units_b64`); on a `{"UK":"<32-hex>"}`
    /// reply, return the 16-byte key. Any other outcome is `None`.
    fn fetch(
        &self,
        inf: &[u8],
        mkb: &[u8],
        vid: Option<[u8; 16]>,
        units: &[Vec<u8>],
    ) -> Option<[u8; 16]> {
        use base64::Engine;
        use std::time::Duration;

        if self.base_url.is_empty() {
            return None;
        }
        let url = format!("{}/decode", self.base_url.trim_end_matches('/'));
        let b64 = base64::engine::general_purpose::STANDARD;
        let mut body = serde_json::json!({
            "inf_b64": b64.encode(inf),
            "mkb_b64": b64.encode(mkb),
        });
        if let Some(vid) = vid {
            body["vid_b64"] = serde_json::Value::String(b64.encode(vid));
        }
        if !units.is_empty() {
            body["units_b64"] = serde_json::Value::Array(
                units
                    .iter()
                    .map(|u| serde_json::Value::String(b64.encode(u)))
                    .collect(),
            );
        }

        let mut req = ureq::post(&url).timeout(Duration::from_secs(30));
        if !self.secret.is_empty() {
            req = req.set("Authorization", &format!("Bearer {}", self.secret));
        }
        tracing::info!(
            phase = "keyservice_query",
            url = %url,
            inf = inf.len(),
            mkb = mkb.len(),
            has_vid = vid.is_some(),
            units = units.len(),
            "querying key service"
        );
        let resp = match req.send_json(body) {
            Ok(r) => r,
            Err(ureq::Error::Status(code, _)) => {
                tracing::warn!(
                    phase = "keyservice_query",
                    status = code,
                    "key service returned no key"
                );
                return None;
            }
            Err(e) => {
                tracing::warn!(
                    phase = "keyservice_query",
                    error = %e,
                    "key service unreachable"
                );
                return None;
            }
        };
        let json: serde_json::Value = match resp.into_json() {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!(phase = "keyservice_query", error = %e, "key service reply unreadable");
                return None;
            }
        };
        match json.get("UK").and_then(|u| u.as_str()).and_then(parse_uk) {
            Some(uk) => {
                tracing::info!(phase = "keyservice_query", "key service returned a key");
                Some(uk)
            }
            None => {
                tracing::warn!(
                    phase = "keyservice_query",
                    "key service reply had no usable key"
                );
                None
            }
        }
    }
}

/// Read up to `n` encrypted 6144-byte aligned units off `reader` at the title's
/// first extent, raw (no decrypt). These are the on-disc content samples a
/// sample-based source needs. Empty if the title has no extents or the read
/// fails. Works for any `SectorSource` (live drive or a staged ISO).
pub fn read_sample_units(
    reader: &mut dyn libfreemkv::SectorSource,
    title: &libfreemkv::DiscTitle,
    n: usize,
) -> Vec<Vec<u8>> {
    const UNIT_LEN: usize = 6144;
    const UNIT_SECTORS: u16 = 3; // 6144 / 2048
    let Some(ext) = title.extents.first() else {
        return Vec::new();
    };
    let count = (n as u16).saturating_mul(UNIT_SECTORS);
    let mut buf = vec![0u8; count as usize * 2048];
    if reader
        .read_sectors(ext.start_lba, count, &mut buf, false)
        .is_err()
    {
        return Vec::new();
    }
    (0..n)
        .filter_map(|i| {
            let o = i * UNIT_LEN;
            (o + UNIT_LEN <= buf.len()).then(|| buf[o..o + UNIT_LEN].to_vec())
        })
        .collect()
}

/// How a disc's key-resolution inputs are obtained, and how the disc is
/// re-scanned once a Unit Key is known. Decouples [`resolve_keys`] from WHERE
/// the disc lives — a live drive or a staged ISO — so the resolution logic is
/// written once. See [`DriveAccess`] and [`IsoAccess`].
pub trait DiscKeyAccess {
    /// The disc's `Unit_Key_RO.inf` + `MKB` bytes.
    fn key_files(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
    /// The 16-byte AACS Volume ID, if available.
    fn volume_id(&self) -> Option<[u8; 16]>;
    /// Up to `n` encrypted aligned units sampled from the disc's content.
    fn sample_units(&mut self, title: &libfreemkv::DiscTitle, n: usize) -> Vec<Vec<u8>>;
    /// Re-scan the disc supplying `uk`, so its decryption keys populate.
    fn rescan(&mut self, uk: [u8; 16]) -> Option<libfreemkv::Disc>;
}

/// Resolve a Unit Key for `disc` via `ks`, reading inputs + re-scanning through
/// `access`. Returns the re-scanned disc (now carrying keys) on success, or
/// `disc` unchanged for a source that resolves inline (local) or on a miss.
///
/// This is the single code path for both the live-drive and resume-from-ISO
/// flows; the only difference between them is the `DiscKeyAccess` impl.
pub fn resolve_keys<A: DiscKeyAccess>(
    ks: &KeySource,
    access: &mut A,
    disc: libfreemkv::Disc,
) -> libfreemkv::Disc {
    if !ks.needs_samples() {
        return disc;
    }
    let Some(title) = disc.titles.first().cloned() else {
        tracing::warn!(
            phase = "keyservice_resolve",
            "no titles — skipping key service"
        );
        return disc;
    };
    let Some((inf, mkb)) = access.key_files() else {
        tracing::warn!(
            phase = "keyservice_resolve",
            "could not read disc key files — skipping key service"
        );
        return disc;
    };
    let vid = access.volume_id();
    let units = access.sample_units(&title, SAMPLE_UNITS);
    tracing::info!(
        phase = "keyservice_resolve",
        has_vid = vid.is_some(),
        units = units.len(),
        "resolving key via key service"
    );
    match ks.resolve(&inf, &mkb, vid, &units) {
        Some(uk) => {
            tracing::info!(
                phase = "keyservice_resolve",
                "key resolved — re-scanning disc"
            );
            access.rescan(uk).unwrap_or(disc)
        }
        None => disc,
    }
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
    fn rescan(&mut self, uk: [u8; 16]) -> Option<libfreemkv::Disc> {
        let opts = libfreemkv::ScanOptions {
            unit_key: Some(uk),
            ..Default::default()
        };
        libfreemkv::Disc::scan(self.drive, &opts).ok()
    }
}

/// [`DiscKeyAccess`] backed by a staged ISO + its mapfile (the resume path).
/// The Volume ID is recovered from the mapfile (the ISO doesn't carry it).
pub struct IsoAccess<'a> {
    iso_path: &'a Path,
    mapfile_path: &'a Path,
    capacity: u32,
}

impl<'a> IsoAccess<'a> {
    pub fn new(iso_path: &'a Path, mapfile_path: &'a Path, capacity: u32) -> Self {
        Self {
            iso_path,
            mapfile_path,
            capacity,
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
            Err(_) => Vec::new(),
        }
    }
    fn rescan(&mut self, uk: [u8; 16]) -> Option<libfreemkv::Disc> {
        let opts = libfreemkv::ScanOptions {
            unit_key: Some(uk),
            ..Default::default()
        };
        let mut r = libfreemkv::FileSectorSource::open(self.iso_path).ok()?;
        libfreemkv::Disc::scan_image(&mut r, self.capacity, &opts).ok()
    }
}

/// Parse a 32-char hex Unit Key into 16 bytes.
fn parse_uk(hex: &str) -> Option<[u8; 16]> {
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(hex.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_uk_roundtrip() {
        assert_eq!(
            parse_uk("1deb13ba851d8fbc01e169dca7d2f258").unwrap(),
            [
                0x1d, 0xeb, 0x13, 0xba, 0x85, 0x1d, 0x8f, 0xbc, 0x01, 0xe1, 0x69, 0xdc, 0xa7, 0xd2,
                0xf2, 0x58
            ]
        );
        assert!(parse_uk("deadbeef").is_none());
        assert!(parse_uk("zz").is_none());
    }
}
