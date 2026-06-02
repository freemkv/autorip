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

use std::path::PathBuf;

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
            KeySource::Online(_) => libfreemkv::ScanOptions::default(),
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
        let resp = req.send_json(body).ok()?;
        let json: serde_json::Value = resp.into_json().ok()?;
        parse_uk(json.get("UK")?.as_str()?)
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
