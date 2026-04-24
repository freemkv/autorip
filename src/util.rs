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
}
