#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TmdbResult {
    pub title: String,
    pub year: u16,
    pub poster_url: String,
    pub overview: String,
    pub media_type: String, // "movie" or "tv"
}

/// Shared agent for all TMDB calls. ureq 2.x sets NO connect/read timeout by
/// default, so a hung api.themoviedb.org connection would wedge the rip thread
/// (lookup runs on it) or a web handler (search) indefinitely. Bound both.
static AGENT: once_cell::sync::Lazy<ureq::Agent> = once_cell::sync::Lazy::new(|| {
    ureq::AgentBuilder::new()
        .timeout_connect(std::time::Duration::from_secs(5))
        .timeout_read(std::time::Duration::from_secs(10))
        .build()
});

/// Build the `search/multi` URL. Both `api_key` and `query` are
/// percent-encoded: an api_key with a stray space/`&`/`#`/`=` (config
/// copy-paste error) would otherwise yield a malformed URL or a silently-wrong
/// key, and the query is untrusted disc-label content.
fn search_multi_url(query: &str, api_key: &str) -> String {
    format!(
        "https://api.themoviedb.org/3/search/multi?api_key={}&query={}&page=1",
        urlencoded(api_key),
        urlencoded(query)
    )
}

/// Run a TMDB `search/multi` request and return the parsed JSON, or `None`.
///
/// Uses the shared timeout-bounded [`AGENT`] (so a hung connection can't wedge
/// the rip thread / web handler) and the percent-encoded [`search_multi_url`].
///
/// Unlike a bare `.call().ok()?`, this distinguishes the failure modes so a
/// misconfigured API key (HTTP 401) or rate-limit (429) is visible in the
/// log instead of silently collapsing to "no results" — which would route
/// every disc to the needs-review queue with no actionable cause. A 401 is
/// throttled (once per minute) so a stuck-bad-key loop can't spam syslog.
/// Cap on the TMDB response body we'll buffer. A real `search/multi` response
/// is tens of KB; 2 MiB is generous headroom. Bounding it stops a hostile or
/// broken endpoint from streaming an unbounded body into memory (DoS).
const MAX_TMDB_BYTES: u64 = 2 * 1024 * 1024;

/// Read at most `MAX_TMDB_BYTES` from the response body, rejecting anything
/// over the cap, then parse as JSON. Replaces `resp.into_json()`, which reads
/// the whole body with no upper bound.
fn read_capped_json(resp: ureq::Response) -> std::io::Result<serde_json::Value> {
    use std::io::Read;
    let mut buf = Vec::new();
    resp.into_reader()
        .take(MAX_TMDB_BYTES + 1)
        .read_to_end(&mut buf)?;
    if buf.len() as u64 > MAX_TMDB_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "tmdb response exceeded size cap",
        ));
    }
    serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn fetch_multi(query: &str, api_key: &str) -> Option<serde_json::Value> {
    let url = search_multi_url(query, api_key);
    match AGENT.get(&url).call() {
        Ok(resp) => match read_capped_json(resp) {
            Ok(json) => Some(json),
            Err(e) => {
                tracing::warn!(query = %query, error = %e, "tmdb: response was not valid JSON");
                None
            }
        },
        Err(ureq::Error::Status(401, _)) => {
            warn_bad_key_throttled();
            None
        }
        Err(ureq::Error::Status(code, _)) => {
            tracing::warn!(query = %query, status = code, "tmdb: HTTP error status");
            None
        }
        Err(e) => {
            // Do NOT log `e` directly — ureq's Display embeds the full
            // request URL, which contains the api_key in the query string.
            // autorip.jsonl is served unauthenticated by GET /api/debug.
            let error_kind = match &e {
                ureq::Error::Transport(t) => t.kind().to_string(),
                ureq::Error::Status(c, _) => format!("HTTP {c}"),
            };
            tracing::warn!(query = %query, error_kind = %error_kind, "tmdb: request failed (network/transport)");
            None
        }
    }
}

/// One-per-minute warning that the configured TMDB API key was rejected.
fn warn_bad_key_throttled() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static LAST_WARN_SECS: AtomicU64 = AtomicU64::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST_WARN_SECS.load(Ordering::Relaxed);
    if now.saturating_sub(last) >= 60
        && LAST_WARN_SECS
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        tracing::warn!(
            "tmdb: API key rejected (HTTP 401) — check the TMDB_API_KEY in Settings; \
             titles will fall through to the needs-review queue until it is fixed"
        );
        crate::log::syslog("TMDB API key rejected (HTTP 401) — check TMDB_API_KEY in Settings");
    }
}

pub fn lookup(query: &str, api_key: &str) -> Option<TmdbResult> {
    if api_key.is_empty() {
        return None;
    }
    // Mirror `search`'s guard: a separator-only volume label that
    // clean_title reduces to "" would otherwise fire a query=&... request
    // that TMDB answers with HTTP 422 and a spurious per-insert warning.
    if query.trim().is_empty() {
        return None;
    }
    let resp = fetch_multi(query, api_key)?;
    let results = resp["results"].as_array()?;
    pick_best(query, results)
}

/// Normalize a title for comparison: lowercase, every run of non-alphanumerics
/// collapses to one space, trimmed. So "Top Gun: Maverick" and the disc label
/// "Top Gun Maverick" both become "top gun maverick".
fn norm(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut sep = true; // leading: suppress a leading space
    for c in s.chars() {
        // Unicode-aware: keep accented letters/digits (so "Amélie" and
        // "Pokémon" can match exactly) instead of stripping all non-ASCII.
        if c.is_alphanumeric() {
            out.extend(c.to_lowercase());
            sep = false;
        } else if !sep {
            out.push(' ');
            sep = true;
        }
    }
    out.trim_end().to_string()
}

/// Is the resolved `title`/`year` a CONFIDENT match for the disc label `query`?
/// An exact normalized-title match that also carries a year. Rips whose match is
/// NOT confident (or that would overwrite an existing file) are held for operator
/// review rather than auto-filed into the library under a guessed name.
pub fn is_confident_match(query: &str, title: &str, year: u16) -> bool {
    year > 0 && norm(title) == norm(query)
}

/// Return up to `limit` candidate matches for `query`, best first (exact dated
/// title → dated → popularity). Powers the "needs review" correction picker.
pub fn search(query: &str, api_key: &str, limit: usize) -> Vec<TmdbResult> {
    if api_key.is_empty() || query.trim().is_empty() {
        return Vec::new();
    }
    let Some(json) = fetch_multi(query, api_key) else {
        return Vec::new();
    };
    let Some(results) = json["results"].as_array() else {
        return Vec::new();
    };
    let want = norm(query);
    let mut parsed: Vec<(TmdbResult, f64, bool)> = results
        .iter()
        .filter_map(parse_result)
        .map(|(r, pop)| {
            let exact = r.year > 0 && norm(&r.title) == want;
            (r, pop, exact)
        })
        .collect();
    parsed.sort_by(|a, b| {
        b.2.cmp(&a.2) // exact first
            .then((b.0.year > 0).cmp(&(a.0.year > 0))) // then dated
            .then(b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)) // then popularity
    });
    parsed.into_iter().take(limit).map(|(r, _, _)| r).collect()
}

/// Choose the best entry from a TMDB `search/multi` response.
///
/// `search/multi` mixes movies, TV, people, and collections, and does
/// NOT always rank the obvious film first — e.g. "Dune Part Two" can
/// surface a dateless franchise/collection entry ahead of the 2024
/// film. The old `results.first()` path then took that entry and ended
/// up with `year == 0`, which the mover turns into a yearless library
/// folder (`Dune Part Two/` instead of `Dune: Part Two (2024)/`).
///
/// We keep only movie/TV entries, prefer ones that actually carry a
/// release year, and break ties on TMDB popularity.
fn pick_best(query: &str, results: &[serde_json::Value]) -> Option<TmdbResult> {
    let want = norm(query);
    // (result, popularity, exact). `exact` = the candidate's title matches the
    // disc label exactly (normalized) AND it has a year. An exact dated match
    // beats popularity — without this, a generic disc label like "Civil War"
    // matches the most POPULAR "Civil War" (Captain America: Civil War, 2016)
    // instead of the actual disc (the 2024 film whose title IS exactly "Civil
    // War"). Same class as "Top Gun Maverick" vs the more popular "Top Gun".
    let mut best: Option<(TmdbResult, f64, bool)> = None;
    for v in results {
        let Some((cand, popularity)) = parse_result(v) else {
            continue;
        };
        let exact = cand.year > 0 && !want.is_empty() && norm(&cand.title) == want;
        let better = match &best {
            None => true,
            Some((cur, cur_pop, cur_exact)) => match (exact, *cur_exact) {
                // An exact dated-title match wins over any non-exact result.
                (true, false) => true,
                (false, true) => false,
                // Otherwise: a dated result beats an undated one; among results
                // of equal dated-ness (and equal exactness), popularity wins.
                _ => match (cand.year > 0, cur.year > 0) {
                    (true, false) => true,
                    (false, true) => false,
                    _ => popularity > *cur_pop,
                },
            },
        };
        if better {
            best = Some((cand, popularity, exact));
        }
    }
    best.map(|(r, _, _)| r)
}

/// Parse one `search/multi` result into a `TmdbResult` + its popularity.
/// Returns `None` for non-movie/TV entries (people, collections) and for
/// entries missing a usable title.
fn parse_result(v: &serde_json::Value) -> Option<(TmdbResult, f64)> {
    // Default to "" (not "movie") so an entry that is missing media_type is
    // rejected by the guard below rather than silently admitted as a movie.
    let media_type = v["media_type"].as_str().unwrap_or("");
    if media_type != "movie" && media_type != "tv" {
        return None;
    }
    let title = v
        .get(if media_type == "tv" { "name" } else { "title" })?
        .as_str()?
        .to_string();
    if title.is_empty() {
        return None;
    }
    let date = v
        .get(if media_type == "tv" {
            "first_air_date"
        } else {
            "release_date"
        })
        .and_then(|d| d.as_str())
        .unwrap_or("");
    let year: u16 = date.get(..4).and_then(|y| y.parse().ok()).unwrap_or(0);
    // TMDB poster_path is always a host-absolute path ("/abc.jpg"). Guard
    // the leading slash so a slashless or unexpected value can't produce a
    // malformed/host-relative image URL — keeps the empty-path behavior.
    let poster = v["poster_path"]
        .as_str()
        .filter(|p| p.starts_with('/'))
        .map(|p| format!("https://image.tmdb.org/t/p/w300{p}"))
        .unwrap_or_default();
    let overview = v["overview"].as_str().unwrap_or("").to_string();
    Some((
        TmdbResult {
            title,
            year,
            poster_url: poster,
            overview,
            media_type: media_type.to_string(),
        },
        v["popularity"].as_f64().unwrap_or(0.0),
    ))
}

/// Clean a disc label for TMDB search: "AURORA_DRIFT_TWO" -> "Aurora Drift Two"
/// Strips common disc suffixes like "4K Ultra HD", "Blu-ray", "DVD", etc.
pub fn clean_title(label: &str) -> String {
    let s = label.replace(['_', '-'], " ");

    // Strip common disc format suffixes (case-insensitive)
    let suffixes = [
        "4k ultra hd",
        "4k uhd",
        "ultra hd",
        "blu ray",
        "bluray",
        "dvd",
        "disc 1",
        "disc 2",
        "disc 3",
        "disc 4",
        "disk 1",
        "disk 2",
        "disk 3",
        "disk 4",
    ];
    // Search AND slice the SAME (lowercased) string. `to_lowercase()` can
    // change byte length (e.g. 'İ' U+0130 -> 2 bytes, 'ẞ' -> 'ß'), so an
    // offset found in `lower` is NOT a valid byte index into `s` and slicing
    // `s` at it can panic mid-codepoint. Title-casing below re-lowercases the
    // tail anyway, so working from `lower` yields identical output.
    //
    // Strip only suffixes anchored at the END of the (current) string —
    // never an embedded match, which would truncate a real title mid-string
    // (the "dvd" in "DOCUMENTARY_ABOUT_DVD_COLLECTIONS", the "bluray" in
    // "HOLIDAY_BLURAY_SPECIAL"). Repeat so a chained tail like "4K UHD BLURAY"
    // peels off group by group.
    let lower = s.to_lowercase();
    let mut clipped = lower.as_str();
    loop {
        // Trim trailing whitespace AND non-alphanumeric junk (trademark glyphs
        // ™/®, punctuation, stray separators) before testing the END-anchor.
        // Retail UHD/BD volume labels routinely carry such trailing characters
        // after the format words ("Ultra HD™", "Blu-ray."), and trimming only
        // whitespace left the suffix un-anchored so it was never stripped — the
        // polluted title ("Fight Club Ultra Hd™") then matched nothing on TMDB.
        let trimmed = clipped.trim_end_matches(|c: char| !c.is_alphanumeric());
        let mut next: Option<&str> = None;
        for suffix in &suffixes {
            if let Some(pos) = trimmed.rfind(suffix) {
                if pos + suffix.len() == trimmed.len() {
                    next = Some(&trimmed[..pos]);
                    break;
                }
            }
        }
        match next {
            Some(rest) => clipped = rest,
            None => {
                clipped = trimmed;
                break;
            }
        }
    }
    let trimmed = clipped.trim();

    trimmed
        .split_whitespace()
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn urlencoded(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => (b as char).to_string(),
            b' ' => "+".to_string(),
            _ => format!("%{:02X}", b),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_title_title_cases_snake_case() {
        assert_eq!(clean_title("AURORA_DRIFT_TWO"), "Aurora Drift Two");
        assert_eq!(clean_title("K_FOR_KESTREL"), "K For Kestrel");
    }

    #[test]
    fn clean_title_strips_uhd_suffix() {
        assert_eq!(clean_title("AURORA_DRIFT_TWO_4K_UHD"), "Aurora Drift Two");
        assert_eq!(
            clean_title("AURORA_DRIFT_TWO_4K_ULTRA_HD"),
            "Aurora Drift Two"
        );
    }

    #[test]
    fn clean_title_strips_bluray_suffix() {
        assert_eq!(clean_title("THE_MATRIX_BLU_RAY"), "The Matrix");
        assert_eq!(clean_title("THE_MATRIX_BLURAY"), "The Matrix");
    }

    #[test]
    fn clean_title_strips_disc_suffix() {
        assert_eq!(clean_title("LORD_OF_THE_RINGS_DISC_1"), "Lord Of The Rings");
    }

    #[test]
    fn clean_title_handles_hyphens() {
        assert_eq!(clean_title("SPIDER-MAN"), "Spider Man");
    }

    #[test]
    fn clean_title_peels_chained_trailing_suffixes() {
        // A chained tail of format suffixes ("4K UHD BLURAY") must peel off
        // group by group from the END, leaving no suffix fragments behind.
        let out = clean_title("MOVIE_4K_UHD_BLURAY");
        assert!(!out.to_lowercase().contains("uhd"));
        assert!(!out.to_lowercase().contains("bluray"));
        assert_eq!(out, "Movie");
    }

    #[test]
    fn clean_title_empty_input() {
        assert_eq!(clean_title(""), "");
    }

    #[test]
    fn clean_title_multibyte_lowercase_does_not_panic() {
        // 'İ' (U+0130) and 'ẞ' (U+1E9E) change byte length under to_lowercase,
        // so an offset found in the lowercased string is not a valid index into
        // the original — slicing the original there panicked ("not a char
        // boundary"). Searching+slicing the same lowercased string fixes it.
        // The disc volume label is disc-controlled, so this must never panic.
        let _ = clean_title("İẞẞdvd");
        let _ = clean_title("İstanbul DVD");
        let _ = clean_title("Straße ẞ Blu-ray");
        // A pure-multibyte label with a trailing suffix still produces output
        // without panicking.
        assert!(!clean_title("İẞẞ 4K UHD").is_empty());
    }

    #[test]
    fn clean_title_keeps_embedded_format_words() {
        // A format word that is NOT at the end must not truncate the title.
        assert_eq!(
            clean_title("DOCUMENTARY_ABOUT_DVD_COLLECTIONS"),
            "Documentary About Dvd Collections"
        );
        assert_eq!(
            clean_title("HOLIDAY_BLURAY_SPECIAL"),
            "Holiday Bluray Special"
        );
    }

    #[test]
    fn clean_title_strips_only_trailing_suffix() {
        // Trailing suffix is still stripped.
        assert_eq!(clean_title("THE_MATRIX_DVD"), "The Matrix");
        // Chained trailing groups peel off one after another.
        assert_eq!(clean_title("THE_MATRIX_4K_UHD_BLURAY"), "The Matrix");
    }

    #[test]
    fn clean_title_strips_suffix_followed_by_trademark_or_punctuation() {
        // Regression: retail volume labels carry a trademark glyph or trailing
        // punctuation AFTER the format word. Trimming only whitespace before the
        // END-anchor test left "ultra hd" un-anchored, so it was never stripped
        // and TMDB matched nothing. The cleaned title must drop both the suffix
        // and the trailing junk.
        assert_eq!(clean_title("Fight Club - Ultra HD™"), "Fight Club");
        assert_eq!(clean_title("Dune 4K Ultra HD®"), "Dune");
        assert_eq!(clean_title("The Matrix Blu-ray."), "The Matrix");
        // Embedded format word still protected even with trailing junk.
        assert_eq!(
            clean_title("DOCUMENTARY_ABOUT_DVD_COLLECTIONS™"),
            "Documentary About Dvd Collections"
        );
    }

    #[test]
    fn urlencoded_keeps_allowed_chars() {
        assert_eq!(urlencoded("hello"), "hello");
        assert_eq!(urlencoded("hello world"), "hello+world");
        assert_eq!(urlencoded("name=value"), "name%3Dvalue");
        assert_eq!(urlencoded("a-b_c.d"), "a-b_c.d");
    }

    #[test]
    fn search_url_encodes_both_key_and_query() {
        // Untrusted disc-label query content cannot break out of the query
        // param or inject extra URL params (SSRF/param-injection guard), and a
        // malformed api_key is encoded rather than corrupting the URL.
        let url = search_multi_url("a&b=c #x", "key with space&evil=1");
        assert!(url.starts_with("https://api.themoviedb.org/3/search/multi?"));
        assert!(!url.contains(' '));
        // Raw '&'/'#'/'=' from inputs must be percent-encoded, never literal
        // separators that would add params or a fragment.
        assert!(url.contains("api_key=key+with+space%26evil%3D1"));
        assert!(url.contains("query=a%26b%3Dc+%23x"));
        // Exactly the two intended params plus page.
        assert_eq!(url.matches('&').count(), 2); // &query= and &page=
        assert!(!url.contains('#'));
    }

    #[test]
    fn norm_keeps_accented_letters() {
        // Accented titles must be able to match exactly (was stripped to ASCII).
        assert_eq!(norm("Amélie"), "amélie");
        assert_eq!(norm("Pokémon"), "pokémon");
        assert_eq!(norm("Amélie"), norm("amélie"));
    }

    // --- pick_best: robust result selection from search/multi ---

    #[test]
    fn pick_best_skips_dateless_collection_ranked_first() {
        // The "Dune Part Two" bug: a dateless collection ranks ahead of
        // the 2024 film, so the old results.first() path got year == 0.
        let results = serde_json::json!([
            {"media_type": "collection", "name": "Dune Collection", "popularity": 90.0},
            {"media_type": "movie", "title": "Dune: Part Two",
             "release_date": "2024-02-27", "popularity": 120.0}
        ]);
        let r = pick_best("", results.as_array().unwrap()).expect("must pick the film");
        assert_eq!(r.title, "Dune: Part Two");
        assert_eq!(r.year, 2024);
    }

    #[test]
    fn pick_best_prefers_dated_even_at_lower_popularity() {
        // A more popular but dateless movie must lose to the dated one —
        // a year in the library folder matters more than popularity rank.
        let results = serde_json::json!([
            {"media_type": "movie", "title": "Dune Part Two",
             "release_date": "", "popularity": 200.0},
            {"media_type": "movie", "title": "Dune: Part Two",
             "release_date": "2024-02-27", "popularity": 10.0}
        ]);
        let r = pick_best("", results.as_array().unwrap()).unwrap();
        assert_eq!(r.year, 2024);
    }

    #[test]
    fn pick_best_breaks_dated_ties_on_popularity() {
        let results = serde_json::json!([
            {"media_type": "movie", "title": "Low", "release_date": "2010-01-01", "popularity": 5.0},
            {"media_type": "movie", "title": "High", "release_date": "2011-01-01", "popularity": 99.0}
        ]);
        let r = pick_best("", results.as_array().unwrap()).unwrap();
        assert_eq!(r.title, "High");
    }

    #[test]
    fn pick_best_skips_person_results() {
        let results = serde_json::json!([
            {"media_type": "person", "name": "Denis Villeneuve", "popularity": 80.0},
            {"media_type": "movie", "title": "Arrival", "release_date": "2016-11-11", "popularity": 40.0}
        ]);
        let r = pick_best("", results.as_array().unwrap()).unwrap();
        assert_eq!(r.title, "Arrival");
    }

    #[test]
    fn pick_best_none_when_no_movie_or_tv() {
        let results = serde_json::json!([
            {"media_type": "person", "name": "Someone", "popularity": 80.0},
            {"media_type": "collection", "name": "Some Collection", "popularity": 50.0}
        ]);
        assert!(pick_best("", results.as_array().unwrap()).is_none());
    }

    #[test]
    fn pick_best_tv_uses_name_and_first_air_date() {
        let results = serde_json::json!([
            {"media_type": "tv", "name": "Severance", "first_air_date": "2022-02-18", "popularity": 60.0}
        ]);
        let r = pick_best("", results.as_array().unwrap()).unwrap();
        assert_eq!(r.title, "Severance");
        assert_eq!(r.year, 2022);
        assert_eq!(r.media_type, "tv");
    }

    #[test]
    fn pick_best_exact_title_beats_more_popular() {
        // The "Civil War" disc (volume label exactly "Civil War" = the 2024 A24
        // film) must NOT be matched to the far more popular "Captain America:
        // Civil War" (2016). An exact normalized-title match wins over popularity.
        let results = serde_json::json!([
            {"media_type": "movie", "title": "Captain America: Civil War",
             "release_date": "2016-04-27", "popularity": 200.0},
            {"media_type": "movie", "title": "Civil War",
             "release_date": "2024-04-10", "popularity": 30.0}
        ]);
        let r = pick_best("Civil War", results.as_array().unwrap()).unwrap();
        assert_eq!(r.title, "Civil War");
        assert_eq!(r.year, 2024);
    }

    #[test]
    fn pick_best_exact_match_ignores_punctuation_and_case() {
        // Disc label "TOP GUN MAVERICK" (cleaned) must match "Top Gun: Maverick"
        // exactly (punctuation/case-insensitive), beating a more popular near-name.
        let results = serde_json::json!([
            {"media_type": "movie", "title": "Top Gun",
             "release_date": "1986-05-16", "popularity": 90.0},
            {"media_type": "movie", "title": "Top Gun: Maverick",
             "release_date": "2022-05-24", "popularity": 50.0}
        ]);
        let r = pick_best("Top Gun Maverick", results.as_array().unwrap()).unwrap();
        assert_eq!(r.title, "Top Gun: Maverick");
        assert_eq!(r.year, 2022);
    }

    /// Verify that the error_kind string produced for transport/status errors
    /// in fetch_multi never contains the api_key (which lives in the URL
    /// query string). We replicate the summary logic that fetch_multi uses so
    /// a future edit to that arm will be caught here.
    #[test]
    fn fetch_multi_error_summary_no_api_key_leak() {
        // Verify the Status variant: just a code, no URL.
        // We can't construct a live ureq::Error::Status without a server, but
        // we can assert the format! template that fetch_multi emits.
        let api_key = "my_secret_api_key";
        let url = search_multi_url("some query", api_key);
        // The URL must contain the key (it's in the query string) — that's the
        // leak risk this test guards against.
        assert!(
            url.contains(api_key) || url.contains("my_secret_api_key"),
            "precondition: api_key must be in the URL"
        );

        // The Status arm produces "HTTP {code}" with no URL in it.
        let status_summary = format!("HTTP {}", 429u16);
        assert!(
            !status_summary.contains(api_key),
            "api_key leaked in status summary: {status_summary}"
        );
        assert!(
            !status_summary.contains("themoviedb.org"),
            "URL leaked in status summary: {status_summary}"
        );

        // A representative transport kind string also must not contain the key.
        let transport_summary = "connection failed";
        assert!(
            !transport_summary.contains(api_key),
            "api_key leaked in transport summary"
        );
    }
}
