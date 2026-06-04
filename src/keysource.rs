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

use std::path::{Path, PathBuf};

use freemkv_keysources::{DiscInputs, KeySource, KeydbSource, MapfileSource, OnlineSource};

use crate::config::Config;

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

/// [`libfreemkv::ScanOptions`] for the structure scan: always KEYLESS. The
/// library captures disc structure + AACS inputs (Unit_Key_RO.inf, MKB, VID)
/// but resolves no keys; autorip resolves them afterward through the sources.
/// `disable_keydb` also stops any keydb sitting in a default search path from
/// resolving inline behind autorip's back.
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
    libfreemkv::ScanOptions { credentials }
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
        "online" => sources.push(Box::new(OnlineSource::new(
            cfg.keyserver_url.clone(),
            cfg.keyserver_secret.clone(),
        ))),
        _ => sources.push(Box::new(KeydbSource::new(keydb_path(cfg)))),
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
/// The disc must have been scanned KEYLESS (see [`keyless_scan_opts`]). Each
/// source offers candidate keys; the first whose [`libfreemkv::Disc::decrypt_with`]
/// derives unit keys wins. A wrong candidate (e.g. a device-key set that does
/// not apply to this disc's MKB) is rejected by `decrypt_with` and the next
/// candidate / source is tried. `decrypt_with` only mutates the disc on success,
/// so a rejected candidate leaves it untouched.
pub fn resolve_keys<A: DiscKeyAccess>(
    sources: &[Box<dyn KeySource>],
    access: &mut A,
    mut disc: libfreemkv::Disc,
) -> (libfreemkv::Disc, KeyOutcome) {
    let Some((inf, mkb)) = access.key_files() else {
        tracing::warn!(phase = "key_resolve", "could not read disc key files");
        return (disc, KeyOutcome::MissingInputs);
    };
    let vid = access.volume_id().unwrap_or([0u8; 16]);

    // Read content samples only if some source validates against ciphertext
    // (an online key service); a keydb / mapfile keys on identity alone.
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
    };

    let mut had_source_error = false;
    for src in sources {
        let cands = match src.resolve(&inputs) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(phase = "key_resolve", error = %e, "key source failed");
                had_source_error = true;
                continue;
            }
        };
        for key in cands {
            if disc.decrypt_with(key).is_ok() {
                tracing::info!(phase = "key_resolve", "key resolved — disc now keyed");
                return (disc, KeyOutcome::Resolved);
            }
        }
    }

    // No candidate decrypted. A hard source error (e.g. an unreadable keydb)
    // is reported distinctly from a clean "no key for this disc".
    let outcome = if had_source_error {
        KeyOutcome::Unreachable
    } else {
        KeyOutcome::NoKey
    };
    (disc, outcome)
}

/// Read up to `n` ENCRYPTED 6144-byte aligned units from `title`'s main body,
/// raw (no decrypt) — so a key service can byte-validate a key against them.
///
/// "Encrypted" is decided by `libfreemkv::aacs::is_aacs_scrambled` — the SAME
/// function the key service uses to gate the request — so both ends agree. It
/// reads the raw TS syncs: an AACS-scrambled unit's body packets have their
/// `0x47` syncs destroyed, while a clear unit keeps them. A clip OPENS with
/// clear navigation units (PAT/PMT, menus); only the feature body is scrambled.
/// Handing a clear unit proves nothing (nothing to decrypt), so we collect only
/// scrambled ones — sampling the LARGEST extent (the feature body) at depth:
/// midpoint first, scanning forward.
pub fn read_sample_units(
    reader: &mut dyn libfreemkv::SectorSource,
    title: &libfreemkv::DiscTitle,
    n: usize,
) -> Vec<Vec<u8>> {
    const UNIT_LEN: usize = 6144;
    const UNIT_SECTORS: u32 = 3; // 6144 / 2048
    const CHUNK_UNITS: u32 = 15; // 45 sectors/read — under the drive transfer cap
    const MAX_CHUNKS_PER_EXTENT: u32 = 4; // ~60 units scanned at each extent's midpoint

    let mut out: Vec<Vec<u8>> = Vec::new();
    for ext in &title.extents {
        let total_units = ext.sector_count / UNIT_SECTORS;
        if total_units == 0 {
            continue;
        }
        let mut unit = total_units / 2; // midpoint of this extent (past clear nav)
        for _ in 0..MAX_CHUNKS_PER_EXTENT {
            if unit >= total_units {
                break;
            }
            let units_this = CHUNK_UNITS.min(total_units - unit);
            let lba = ext.start_lba + unit * UNIT_SECTORS;
            let count = (units_this * UNIT_SECTORS) as u16;
            let mut buf = vec![0u8; count as usize * 2048];
            // `false` = no recovery retries. The reader is the raw drive/file
            // (no decrypt decorator), so these are the on-disc encrypted bytes.
            if reader.read_sectors(lba, count, &mut buf, false).is_err() {
                break;
            }
            for i in 0..units_this as usize {
                let o = i * UNIT_LEN;
                if o + UNIT_LEN > buf.len() {
                    break;
                }
                let u = &buf[o..o + UNIT_LEN];
                if libfreemkv::aacs::is_aacs_scrambled(u) {
                    out.push(u.to_vec());
                    if out.len() >= n {
                        return out;
                    }
                }
            }
            unit += units_this;
        }
    }
    out
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
            Err(_) => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
