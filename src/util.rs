/// Current epoch seconds.
pub fn epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Howard Hinnant's civil-from-days. Returns (year, month, day) UTC.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Format epoch seconds as YYYY-MM-DD.
pub fn format_date() -> String {
    let (y, m, d) = civil_from_days((epoch_secs() / 86400) as i64);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// Format current UTC time as ISO-8601 (YYYY-MM-DDTHH:MM:SSZ).
pub fn format_iso_datetime() -> String {
    let secs = epoch_secs();
    let (y, mo, d) = civil_from_days((secs / 86400) as i64);
    let day = (secs % 86400) as u32;
    let h = day / 3600;
    let mi = (day % 3600) / 60;
    let s = day % 60;
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

/// Filesystem-safe ISO timestamp: YYYY-MM-DDTHH-MM-SSZ (colons replaced).
/// Used for rip-archive filenames so they sort correctly and are portable.
pub fn format_iso_datetime_filename() -> String {
    format_iso_datetime().replace(':', "-")
}

// ─── Filename / display helpers ──────────────────────────────────────────────
//
// Pre-0.13 these lived in `ripper::sanitize_filename`, `mover::sanitize_dir_name`,
// `ripper::format_duration`, and `ripper::format_codecs`. The two sanitizers
// drifted (one replaced spaces with `_`, the other kept them) and a single rip
// could produce a `Dune_Part_Two` staging dir but a `Dune Part Two (2024)`
// destination dir — same logic, two implementations. Consolidated here as
// the single source of truth.

/// Sanitize a name for use as a filesystem path segment with **no spaces**.
/// Used for staging directories and file basenames where snake_case is
/// preferred (so logs and shell commands don't need quoting).
///
/// Keeps `[A-Za-z0-9 \-_.]`, drops everything else, then collapses spaces
/// to underscores.
pub fn sanitize_path_compact(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == ' ' || *c == '-' || *c == '_' || *c == '.')
        .collect::<String>()
        .trim()
        .replace(' ', "_")
}

/// Sanitize a name for a user-visible directory (e.g. the final library
/// destination `Movies/Dune Part Two (2024)/`). Spaces preserved; apostrophes
/// kept (filesystems handle them, omitting them mangles "What's Up Doc").
pub fn sanitize_path_display(name: &str) -> String {
    name.chars()
        .filter(|c| {
            c.is_ascii_alphanumeric()
                || *c == ' '
                || *c == '-'
                || *c == '_'
                || *c == '.'
                || *c == '\''
        })
        .collect::<String>()
        .trim()
        .to_string()
}

/// Format a number of seconds as `Xh YYm`. Used by the rip card and the
/// disc info banner.
pub fn format_duration_hm(secs: f64) -> String {
    let h = (secs / 3600.0) as u32;
    let m = ((secs % 3600.0) / 60.0) as u32;
    format!("{}h {:02}m", h, m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_from_days_epoch() {
        // Unix epoch day 0 = 1970-01-01.
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn civil_from_days_leap_year_march() {
        // 2024-03-01 is day 19783 from Unix epoch (verified via Python datetime).
        assert_eq!(civil_from_days(19783), (2024, 3, 1));
    }

    #[test]
    fn civil_from_days_far_future() {
        // 2026-04-24 = day 20567 from epoch.
        assert_eq!(civil_from_days(20567), (2026, 4, 24));
    }

    #[test]
    fn format_iso_datetime_shape() {
        // Can't assert exact value (depends on wall clock) but can assert shape.
        let s = format_iso_datetime();
        assert_eq!(s.len(), 20); // "YYYY-MM-DDTHH:MM:SSZ"
        assert!(s.ends_with('Z'));
        assert_eq!(s.as_bytes()[10], b'T');
        assert_eq!(s.as_bytes()[4], b'-');
        assert_eq!(s.as_bytes()[13], b':');
    }

    #[test]
    fn format_iso_datetime_filename_no_colons() {
        // Filesystem-safe variant replaces `:` with `-`.
        let s = format_iso_datetime_filename();
        assert!(!s.contains(':'));
        assert!(s.ends_with('Z'));
    }

    #[test]
    fn format_date_shape() {
        let s = format_date();
        assert_eq!(s.len(), 10); // "YYYY-MM-DD"
        assert_eq!(s.as_bytes()[4], b'-');
        assert_eq!(s.as_bytes()[7], b'-');
    }

    // ─── Sanitizer + duration helpers ────────────────────────────────────

    #[test]
    fn sanitize_path_compact_collapses_spaces_to_underscore() {
        assert_eq!(sanitize_path_compact("Dune Part Two"), "Dune_Part_Two");
        assert_eq!(sanitize_path_compact("V for Vendetta"), "V_for_Vendetta");
    }

    #[test]
    fn sanitize_path_compact_strips_unsafe_chars() {
        assert_eq!(sanitize_path_compact("Dune: Part Two"), "Dune_Part_Two");
        assert_eq!(sanitize_path_compact("M*A*S*H"), "MASH");
        assert_eq!(sanitize_path_compact("Alien/Predator"), "AlienPredator");
    }

    #[test]
    fn sanitize_path_compact_keeps_dots_dashes_underscores() {
        assert_eq!(sanitize_path_compact("Movie-2024.4K"), "Movie-2024.4K");
    }

    #[test]
    fn sanitize_path_display_keeps_spaces_and_apostrophes() {
        assert_eq!(sanitize_path_display("What's Up Doc"), "What's Up Doc");
        assert_eq!(
            sanitize_path_display("Rogue One - A Star Wars Story"),
            "Rogue One - A Star Wars Story"
        );
    }

    #[test]
    fn sanitize_path_display_strips_unsafe_chars() {
        assert_eq!(sanitize_path_display("Dune: Part Two"), "Dune Part Two");
        assert_eq!(sanitize_path_display("M*A*S*H"), "MASH");
    }

    #[test]
    fn sanitize_path_display_trims_whitespace() {
        assert_eq!(sanitize_path_display("  spaced title  "), "spaced title");
    }

    #[test]
    fn format_duration_hm_zero() {
        assert_eq!(format_duration_hm(0.0), "0h 00m");
    }

    #[test]
    fn format_duration_hm_under_minute() {
        assert_eq!(format_duration_hm(30.0), "0h 00m");
    }

    #[test]
    fn format_duration_hm_pads_minutes() {
        assert_eq!(format_duration_hm(3600.0 + 5.0 * 60.0), "1h 05m");
    }

    #[test]
    fn format_duration_hm_two_hours() {
        assert_eq!(format_duration_hm(2.0 * 3600.0 + 30.0 * 60.0), "2h 30m");
    }
}
