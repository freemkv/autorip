//! Online keyserver client — the `key_source = "online"` path.
//!
//! Instead of a local `keydb.cfg`, autorip POSTs a disc's `Unit_Key_RO.inf`
//! and `MKB` to a configurable keyserver and gets back the final Unit Key.
//! The server's contract: `POST <url>/decode` with
//! `{"inf_b64": "...", "mkb_b64": "..."}` → `{"UK":"<32-hex>"}` on success,
//! or an empty 404 on any miss/error.
//!
//! libfreemkv stays network-free; this is the only place autorip talks to the
//! keyserver. The returned UK is fed back into libfreemkv via
//! `ScanOptions { unit_key: Some(..) }`.

use base64::Engine;
use std::time::Duration;

/// Fetch the Unit Key for a disc from the keyserver.
///
/// `base_url` is the configured keyserver (any compatible host). `secret`
/// is the API secret (sent as `Authorization: Bearer <secret>`); empty = no
/// auth header. Returns the 16-byte UK, or `None` on any failure (server
/// miss/404, network error, malformed response) — the caller treats `None`
/// as "no key available". autorip neither validates nor interprets the
/// secret; the server owns that.
pub fn fetch_uk(base_url: &str, secret: &str, inf: &[u8], mkb: &[u8]) -> Option<[u8; 16]> {
    let url = format!("{}/decode", base_url.trim_end_matches('/'));
    let b64 = base64::engine::general_purpose::STANDARD;
    let body = serde_json::json!({
        "inf_b64": b64.encode(inf),
        "mkb_b64": b64.encode(mkb),
    });

    let mut req = ureq::post(&url).timeout(Duration::from_secs(30));
    if !secret.is_empty() {
        req = req.set("Authorization", &format!("Bearer {secret}"));
    }
    let resp = req.send_json(body).ok()?;
    let json: serde_json::Value = resp.into_json().ok()?;
    parse_uk(json.get("UK")?.as_str()?)
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
