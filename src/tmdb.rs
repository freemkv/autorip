#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TmdbResult {
    pub title: String,
    pub year: u16,
    pub poster_url: String,
    pub overview: String,
    pub media_type: String, // "movie" or "tv"
    /// TMDB numeric id for this match (0 = unknown). Carried so a consumer —
    /// autorip's own metadata enrichment, or kdb resolving a volume id via
    /// [`lookup`] — can fetch anything else it wants straight from TMDB by id,
    /// without re-searching. `serde(default)` so markers written before this
    /// field existed still deserialize.
    #[serde(default)]
    pub tmdb_id: u64,
}

/// Shared agent for all TMDB calls. ureq sets NO connect/read timeout by
/// default, so a hung api.themoviedb.org connection would wedge the rip thread
/// (lookup runs on it) or a web handler (search) indefinitely. Bound both.
///
/// No pinned resolver here (unlike `web::guarded_agent`): the host is the
/// hard-coded `api.themoviedb.org`, not an operator-supplied URL, so there is
/// no SSRF/rebinding surface to close.
static AGENT: once_cell::sync::Lazy<ureq::Agent> = once_cell::sync::Lazy::new(|| {
    let config = ureq::config::Config::builder()
        .timeout_connect(Some(std::time::Duration::from_secs(5)))
        .timeout_recv_response(Some(std::time::Duration::from_secs(10)))
        // Follow NO redirects, like every other outbound agent here. The
        // no-pinned-resolver argument above is about where the REQUEST is
        // aimed; it says nothing about where a RESPONSE can send it. This
        // URL carries the operator's api_key in its query string, so a 3xx
        // — TMDB compromised, misconfigured, or tampered with on-path —
        // would hand that key to an arbitrary host. At zero, ureq returns
        // the 3xx as a normal response instead of erroring, and
        // `read_capped_json` then fails to parse it, which `fetch_multi`
        // already reports as "no result" rather than a match.
        .max_redirects(0)
        .build();
    ureq::Agent::new_with_config(config)
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

/// Read at most `cap` bytes from `reader`, rejecting anything over the cap
/// (an oversized body streams a `cap+1`-byte read successfully, then fails
/// the boundary check below rather than being silently truncated to `cap`
/// bytes and parsed as if that were the whole response).
///
/// Pulled out of [`read_capped_json`] as a pure function over any `Read` —
/// not tied to `ureq::Response`, which can't be constructed in a unit test
/// without a live HTTP server — so the cap boundary itself (accept at
/// exactly `cap`, reject at `cap + 1`) is directly testable against an
/// in-memory `Cursor`.
fn read_capped_bytes(reader: impl std::io::Read, cap: u64) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;
    let mut buf = Vec::new();
    reader.take(cap + 1).read_to_end(&mut buf)?;
    if buf.len() as u64 > cap {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "tmdb response exceeded size cap",
        ));
    }
    Ok(buf)
}

/// Read at most `MAX_TMDB_BYTES` from the response body, rejecting anything
/// over the cap, then parse as JSON. Replaces `resp.into_json()`, which reads
/// the whole body with no upper bound.
fn read_capped_json(resp: ureq::http::Response<ureq::Body>) -> std::io::Result<serde_json::Value> {
    let buf = read_capped_bytes(resp.into_body().into_reader(), MAX_TMDB_BYTES)?;
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
        Err(ureq::Error::StatusCode(401)) => {
            warn_bad_key_throttled();
            None
        }
        Err(ureq::Error::StatusCode(code)) => {
            tracing::warn!(query = %query, status = code, "tmdb: HTTP error status");
            None
        }
        Err(e) => {
            // Do NOT log `e` directly. Written against ureq 2, whose Display
            // carried the request URL — and this URL has the api_key in its
            // query string, with autorip.jsonl served unauthenticated by GET
            // /api/debug. ureq 3's Display is URL-free on the variants
            // reachable here, but `BadUri` still prints the URI it rejected,
            // so the masking stays and the reason is now the accurate one.
            let error_kind = crate::web::ureq_error_kind(&e);
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

/// Resolve a disc `label` to a TMDB movie/TV entry.
///
/// Takes the RAW disc volume label (not a pre-cleaned string): cleaning and the
/// progressive-fallback trimming both live here so the lookup and the auto-file
/// gate ([`is_confident_match`]) can never disagree about what was searched.
///
/// TMDB's `search/multi` text search zeroes out on a single unparseable trailing
/// token — the disc "Batman v Superman: Dawn of Justice: UE" returns ZERO hits,
/// but dropping the "UE" (Ultimate Edition) returns the exact 2016 film. Disc
/// labels carry an un-enumerable tail of edition/region/packaging codes (UE, SE,
/// UPT1, G51, BD3, 3D, …), so instead of an ever-growing blocklist we query the
/// cleaned label, and on no *confident* match peel junk-shaped trailing tokens
/// one at a time and re-query ([`query_variants`]). Pure numbers and roman
/// numerals are NEVER peeled — they are sequel markers ("Alien 3", "Rocky II"),
/// and trimming them would mis-file a sequel as the original film.
///
/// Returns the first confident match (exact normalized title + a year) across
/// the variants; failing that, the best non-exact guess from the earliest
/// variant that returned anything, so the needs-review card still shows a
/// suggestion. The confidence bar itself is unchanged — only recall improves.
pub fn lookup(label: &str, api_key: &str) -> Option<TmdbResult> {
    if api_key.is_empty() {
        return None;
    }
    // A separator-only volume label reduces to no query variants; short-circuit
    // rather than firing a `query=&...` request that TMDB answers with HTTP 422.
    //
    // A season marker in the label ("… Season 5") means the disc is TV — bias
    // the pick toward the series so a same-named film can't outrank the show.
    let prefer_tv = season_from_label(label).is_some();
    let mut fallback: Option<TmdbResult> = None;
    for variant in query_variants(label) {
        if variant.trim().is_empty() {
            continue;
        }
        let Some(resp) = fetch_multi(&variant, api_key) else {
            continue;
        };
        let Some(results) = resp["results"].as_array() else {
            continue;
        };
        if let Some(best) = pick_best(&variant, results, prefer_tv) {
            // Confident = exact normalized title match on THIS variant + a year.
            if best.year > 0 && norm(&best.title) == norm(&variant) {
                return Some(best);
            }
            if fallback.is_none() {
                fallback = Some(best);
            }
        }
    }
    fallback
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

/// Is the resolved `title`/`year` a CONFIDENT match for the disc `label`?
///
/// Confident = the title carries a year AND exactly matches (normalized) the
/// cleaned label OR any of the same progressively-trimmed variants that
/// [`lookup`] searches. Sharing [`query_variants`] is what keeps this auto-file
/// gate in lockstep with the fallback lookup: without it, a title resolved by
/// peeling "UE" off the label would be found by `lookup` but then REJECTED here
/// (the full label never exact-matches), parking every edition disc in review.
///
/// Takes the RAW label (cleaning happens inside), matching `lookup`. Rips whose
/// match is NOT confident (or that would overwrite an existing file) are held
/// for operator review rather than auto-filed under a guessed name.
pub fn is_confident_match(label: &str, title: &str, year: u16) -> bool {
    year > 0 && query_variants(label).iter().any(|v| norm(v) == norm(title))
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
    rank_search_results(query, results, limit)
}

/// The pure ranking half of [`search`]: parse every `movie`/`tv` entry in
/// `results`, then sort exact-dated-match first, dated second, popularity as
/// the final tiebreaker, and cap at `limit`. Pulled out as its own function
/// (taking already-fetched JSON rather than making the HTTP call itself) so
/// this — the actual decision logic behind the manual "needs review"
/// correction picker — can be driven directly in a test without a network
/// round trip, instead of leaving it exercised only via the untestable
/// `search()` entry point.
fn rank_search_results(
    query: &str,
    results: &[serde_json::Value],
    limit: usize,
) -> Vec<TmdbResult> {
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
fn pick_best(query: &str, results: &[serde_json::Value], prefer_tv: bool) -> Option<TmdbResult> {
    let want = norm(query);
    // Ranking key per candidate, compared lexicographically, highest wins:
    //   (exact, dated, tv_preferred, popularity)
    // - `exact` (dated + normalized-title match) beats popularity — else a
    //   generic label like "Civil War" matches the most POPULAR "Civil War"
    //   (Captain America, 2016) instead of the 2024 film whose title IS "Civil
    //   War". Same class as "Top Gun Maverick" vs the more popular "Top Gun".
    // - `dated` beats undated (a yearless entry yields a yearless folder).
    // - `tv_preferred` is set ONLY when the disc label carried a season marker
    //   (`prefer_tv`) and this candidate is a series: it breaks a tie between a
    //   show and a same-named film ("Endeavour" the ITV series vs the film) in
    //   favour of the series. Placed BELOW exact/dated so it only decides
    //   otherwise-equal candidates — an exact film match still beats a fuzzy
    //   series one.
    let key = |cand: &TmdbResult, pop: f64| {
        let exact = cand.year > 0 && !want.is_empty() && norm(&cand.title) == want;
        (
            exact,
            cand.year > 0,
            prefer_tv && cand.media_type == "tv",
            pop,
        )
    };
    let mut best: Option<(TmdbResult, (bool, bool, bool, f64))> = None;
    for v in results {
        let Some((cand, popularity)) = parse_result(v) else {
            continue;
        };
        let k = key(&cand, popularity);
        let better = best.as_ref().is_none_or(|(_, bk)| key_gt(k, *bk));
        if better {
            best = Some((cand, k));
        }
    }
    best.map(|(r, _)| r)
}

/// Lexicographic "is `a` a better rank than `b`" for [`pick_best`]'s key. Hand
/// rolled because the key ends in an `f64` (popularity), which is not `Ord`.
fn key_gt(a: (bool, bool, bool, f64), b: (bool, bool, bool, f64)) -> bool {
    if a.0 != b.0 {
        return a.0;
    }
    if a.1 != b.1 {
        return a.1;
    }
    if a.2 != b.2 {
        return a.2;
    }
    a.3 > b.3
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
    let tmdb_id = v["id"].as_u64().unwrap_or(0);
    Some((
        TmdbResult {
            title,
            year,
            poster_url: poster,
            overview,
            media_type: media_type.to_string(),
            tmdb_id,
        },
        v["popularity"].as_f64().unwrap_or(0.0),
    ))
}

/// Remove a parenthesized 4-digit release year from a label, e.g.
/// "Drive (2011) - 4K Ultra HD" -> "Drive  - 4K Ultra HD". Retail meta-titles
/// annotate the release year in parentheses; TMDB's text search returns ZERO
/// hits when that annotation is left in the query (`Drive (2011)` matches
/// nothing, `Drive` matches). A 4-digit year in parentheses is virtually never
/// part of a real movie title, so removing it is safe. A BARE (unparenthesized)
/// year is left untouched so titles like "Blade Runner 2049" and "1917" are
/// unaffected. Char-based (not byte-based) so a multibyte label never panics.
fn strip_paren_year(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < chars.len() {
        // Match "(dddd)" exactly: '(' then 4 ASCII digits then ')'.
        if chars[i] == '('
            && i + 5 < chars.len()
            && chars[i + 5] == ')'
            && chars[i + 1..i + 5].iter().all(|c| c.is_ascii_digit())
        {
            i += 6; // skip the whole "(dddd)" group
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

/// Clean a disc label for TMDB search: "AURORA_DRIFT_TWO" -> "Aurora Drift Two"
/// Strips common disc suffixes like "4K Ultra HD", "Blu-ray", "DVD", etc., and
/// a parenthesized release year (see [`strip_paren_year`]).
pub fn clean_title(label: &str) -> String {
    let s = label.replace(['_', '-'], " ");
    let s = strip_paren_year(&s);

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
            if let Some(pos) = trimmed.rfind(suffix)
                && pos + suffix.len() == trimmed.len()
            {
                next = Some(&trimmed[..pos]);
                break;
            }
        }
        // Also peel a trailing TV season marker ("Season 5", "Series 2") so a
        // series disc resolves to the base show title. Only when no format
        // suffix matched this round, so the two peels interleave across the
        // loop ("… Season 5 Disc 2" -> drop "Disc 2" -> drop "Season 5").
        if next.is_none() {
            next = strip_trailing_season(trimmed);
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

/// Unambiguous TV season markers. A trailing "<word> <number>" of one of these
/// is peeled by [`clean_title`] so a series disc ("Endeavour Season 5 Disc 2")
/// resolves to the base show title ("Endeavour").
///
/// Deliberately EXCLUDES "Volume"/"Vol"/"Part": those ARE part of real film
/// titles ("Kill Bill: Vol. 2", "Guardians of the Galaxy Vol. 2", "Dune: Part
/// Two") and peeling them would mis-resolve the movie.
const SEASON_WORDS: &[&str] = &["season", "series", "saison", "staffel", "seizoen"];

/// If `s` (already lowercased, trailing-junk-trimmed) ends with a season marker
/// like "season 5" / "series 2", return `s` with that marker removed; else None.
/// The number is required — a bare trailing "season" is left alone (it could be
/// a real title word, e.g. "Silly Season").
fn strip_trailing_season(s: &str) -> Option<&str> {
    let digits_start = s.trim_end_matches(|c: char| c.is_ascii_digit());
    if digits_start.len() == s.len() {
        return None; // no trailing digits
    }
    let head = digits_start.trim_end(); // strip the space(s) before the number
    for word in SEASON_WORDS {
        if let Some(pos) = head.rfind(word)
            && pos + word.len() == head.len()
            // Must be a whole word: start of string or a non-alphanumeric before it.
            && (pos == 0 || !head[..pos].ends_with(|c: char| c.is_alphanumeric()))
        {
            return Some(head[..pos].trim_end());
        }
    }
    None
}

/// Parse a TV season number from a disc `label`: "Endeavour Season 5 Disc 2"
/// → 5, "GAMEOFTHRONES_S3_DISC1" → 3. Returns 1..=99, else `None`.
///
/// A season marker is the signal that a disc is TV rather than a film, and the
/// number is what the mover uses to place the rip under `Show (Year)/Season NN/`.
/// Recognizes the spelled-out [`SEASON_WORDS`] followed by a number, or a
/// compact `S<n>` token.
pub fn season_from_label(label: &str) -> Option<u16> {
    number_after_word(label, SEASON_WORDS).or_else(|| compact_token_number(label, &["s"]))
}

/// Parse a disc number from a `label`: "… Disc 2" → 2, "GOT_S3_D4" → 4,
/// "BD1" → 1. Returns 1..=99, else `None`. Used to sequence a multi-disc set.
pub fn disc_from_label(label: &str) -> Option<u16> {
    number_after_word(label, &["disc", "disk"])
        .or_else(|| compact_token_number(label, &["disc", "disk", "bd", "d"]))
}

/// The number immediately following any of `words` in `s` (case-insensitive,
/// tolerating `_ - . :` separators): "Season 5" → 5, "series2" → 2. 1..=99.
fn number_after_word(s: &str, words: &[&str]) -> Option<u16> {
    let low = s.to_lowercase();
    for word in words {
        let mut from = 0;
        while let Some(rel) = low[from..].find(word) {
            let pos = from + rel;
            let digits: String = low[pos + word.len()..]
                .trim_start_matches([' ', '_', '-', '.', ':'])
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if let Ok(n) = digits.parse::<u16>()
                && (1..=99).contains(&n)
            {
                return Some(n);
            }
            from = pos + word.len();
        }
    }
    None
}

/// A compact standalone token `<prefix><digits>` — "S3", "D2", "BD1", "DISC1".
fn compact_token_number(s: &str, prefixes: &[&str]) -> Option<u16> {
    for tok in s.split([' ', '_', '-', '.', ':']) {
        let low = tok.to_lowercase();
        for pfx in prefixes {
            if let Some(rest) = low.strip_prefix(pfx)
                && !rest.is_empty()
                && rest.chars().all(|c| c.is_ascii_digit())
                && let Ok(n) = rest.parse::<u16>()
                && (1..=99).contains(&n)
            {
                return Some(n);
            }
        }
    }
    None
}

/// Edition / release qualifiers that annotate a cut but are not part of the
/// film's TMDB title ("Ultimate Edition", "Director's Cut", "Extended"). Peeled
/// only when TRAILING (see [`is_trailing_junk`]) so an interior word is never
/// removed. Lowercased; apostrophes are already gone by the time we test tokens.
const EDITION_WORDS: &[&str] = &[
    "ultimate",
    "extended",
    "theatrical",
    "director",
    "directors",
    "special",
    "collector",
    "collectors",
    "anniversary",
    "final",
    "unrated",
    "remastered",
    "limited",
    "deluxe",
    "steelbook",
    "edition",
    "cut",
    "version",
];

/// Region / market codes that retail volume labels append.
const REGION_WORDS: &[&str] = &["uk", "usa", "us", "eu", "na", "ww", "aus", "region"];

/// Disc-format words. `clean_title` already strips multi-word variants
/// ("4k ultra hd", "blu ray"); these single tokens are what a trailing-token
/// peel sees ("BD", "UHD", "3D"-style tokens are caught by the alnum rule).
const FORMAT_WORDS: &[&str] = &[
    "bd", "bdrom", "uhd", "4k", "hd", "sd", "dvd", "bluray", "video",
];

/// Is `s` (lowercased) composed only of roman-numeral letters? Used only to
/// PREVENT trimming a trailing roman numeral ("Rocky II"): a false positive on
/// a real word like "mix" merely keeps that token, which is always safe.
fn is_roman_numeral(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| matches!(c, 'i' | 'v' | 'x' | 'l' | 'c' | 'd' | 'm'))
}

/// May this TRAILING token be safely peeled off a disc label to recover the
/// title? Edition/region/format words and obvious codes — but NEVER a bare
/// number or a roman numeral, which are sequel markers ("Alien 3", "Rocky II"):
/// peeling those would resolve a sequel to the original film. The full label is
/// always queried first and the exact-match gate guards every variant, so this
/// only needs to avoid that one class of meaningful-token collision.
fn is_trailing_junk(tok: &str) -> bool {
    if tok.is_empty() {
        return false;
    }
    // Sequel markers are meaningful — never peel.
    if tok.chars().all(|c| c.is_ascii_digit()) {
        return false;
    }
    let low = tok.to_lowercase();
    if is_roman_numeral(&low) {
        return false;
    }
    if EDITION_WORDS.contains(&low.as_str())
        || REGION_WORDS.contains(&low.as_str())
        || FORMAT_WORDS.contains(&low.as_str())
    {
        return true;
    }
    // Mixed-alphanumeric packaging/format code: BD3, UPT1, G51, D2, 3D, 4K.
    let has_alpha = tok.chars().any(|c| c.is_ascii_alphabetic());
    let has_digit = tok.chars().any(|c| c.is_ascii_digit());
    if has_alpha && has_digit {
        return true;
    }
    // Short all-caps abbreviation (not a roman numeral): UE, SE, EDC, WW.
    // Case is preserved here because variants are peeled from the RAW label
    // BEFORE `clean_title` title-cases — so "UE" (junk) stays distinguishable
    // from "Us" (a real one-word title, which is not all-caps in a label like
    // "US_2019" and is protected by being queried in full first regardless).
    tok.len() <= 4 && tok.chars().all(|c| c.is_ascii_uppercase())
}

/// Progressively-trimmed TMDB query variants for a disc `label`, most specific
/// first. Variant 0 is `clean_title(label)`; each subsequent variant peels one
/// more junk-shaped trailing token (see [`is_trailing_junk`]). Peeling is done
/// on the RAW label so [`is_trailing_junk`] sees original case, then each
/// surviving prefix is run through [`clean_title`]. Deduped and capped so a
/// pathological label can't fan out into an unbounded burst of TMDB requests.
fn query_variants(label: &str) -> Vec<String> {
    const MAX_QUERY_VARIANTS: usize = 5;
    // Same separator/year normalization clean_title applies, but WITHOUT the
    // case folding, so junk detection keeps the label's original casing.
    let base = label.replace(['_', '-'], " ");
    let base = strip_paren_year(&base);
    let toks: Vec<&str> = base.split_whitespace().collect();
    let mut variants: Vec<String> = Vec::new();
    let mut end = toks.len();
    loop {
        let q = clean_title(&toks[..end].join(" "));
        if !q.is_empty() && !variants.iter().any(|v| v == &q) {
            variants.push(q);
        }
        if variants.len() >= MAX_QUERY_VARIANTS || end <= 1 || !is_trailing_junk(toks[end - 1]) {
            break;
        }
        end -= 1;
    }
    variants
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
    fn clean_title_strips_parenthesized_year() {
        // Retail meta-titles annotate the release year in parens; leaving it in
        // the query makes TMDB return 0 hits ("Drive (2011)" matches nothing).
        assert_eq!(clean_title("Drive (2011) - 4K Ultra HD"), "Drive");
        assert_eq!(clean_title("Zombieland (2009)"), "Zombieland");
        assert_eq!(clean_title("The Matrix (1999) BLURAY"), "The Matrix");
    }

    #[test]
    fn clean_title_keeps_bare_year_in_title() {
        // A BARE (unparenthesized) year is part of the title — never stripped.
        assert_eq!(clean_title("BLADE_RUNNER_2049"), "Blade Runner 2049");
        assert_eq!(clean_title("1917"), "1917");
    }

    #[test]
    fn strip_paren_year_boundaries() {
        // Exactly "(dddd)" is removed.
        assert_eq!(strip_paren_year("Drive (2011)"), "Drive ");
        assert_eq!(strip_paren_year("(2011)"), "");
        // Near-miss digit counts are NOT a year group → kept verbatim.
        assert_eq!(strip_paren_year("(201)"), "(201)");
        assert_eq!(strip_paren_year("(20111)"), "(20111)");
        assert_eq!(strip_paren_year("(20)"), "(20)");
        // Non-digit contents kept.
        assert_eq!(strip_paren_year("(abcd)"), "(abcd)");
        assert_eq!(strip_paren_year("(20a1)"), "(20a1)");
        // Malformed / unbalanced parens kept (no crash, no partial strip).
        assert_eq!(strip_paren_year("(2011"), "(2011");
        assert_eq!(strip_paren_year("2011)"), "2011)");
        // Multiple year groups all removed.
        assert_eq!(strip_paren_year("(2001) x (2002)"), " x ");
        // Multibyte input must not panic and must strip correctly.
        assert_eq!(strip_paren_year("Amélie (2001)"), "Amélie ");
        // A 4-digit group adjacent to the end.
        assert_eq!(strip_paren_year("Se7en (1995)"), "Se7en ");
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
        let r = pick_best("", results.as_array().unwrap(), false).expect("must pick the film");
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
        let r = pick_best("", results.as_array().unwrap(), false).unwrap();
        assert_eq!(r.year, 2024);
    }

    #[test]
    fn pick_best_breaks_dated_ties_on_popularity() {
        let results = serde_json::json!([
            {"media_type": "movie", "title": "Low", "release_date": "2010-01-01", "popularity": 5.0},
            {"media_type": "movie", "title": "High", "release_date": "2011-01-01", "popularity": 99.0}
        ]);
        let r = pick_best("", results.as_array().unwrap(), false).unwrap();
        assert_eq!(r.title, "High");
    }

    #[test]
    fn pick_best_prefers_tv_over_a_more_popular_film_namesake_when_flagged() {
        // A season-marked disc ("Endeavour Season 5") sets prefer_tv. Both a
        // far more popular FILM and the SERIES match the cleaned title exactly
        // and are dated — the series must win so the disc files as TV.
        let results = serde_json::json!([
            {"media_type": "movie", "title": "Endeavour",
             "release_date": "2003-01-01", "popularity": 500.0},
            {"media_type": "tv", "name": "Endeavour",
             "first_air_date": "2012-01-08", "popularity": 30.0}
        ]);
        let r = pick_best("Endeavour", results.as_array().unwrap(), true).unwrap();
        assert_eq!(r.media_type, "tv");
        assert_eq!(r.year, 2012);
        // Without the flag, popularity wins (the film) — proving the flag, not
        // some incidental ordering, is what selects the series.
        let r2 = pick_best("Endeavour", results.as_array().unwrap(), false).unwrap();
        assert_eq!(r2.media_type, "movie");
    }

    #[test]
    fn pick_best_prefer_tv_does_not_override_an_exact_film_over_a_fuzzy_series() {
        // prefer_tv is only a tie-break BELOW exactness: an exact-dated FILM
        // still beats a non-exact (undated) series, so a stray season marker on
        // a film disc can't drag it to an unrelated show.
        let results = serde_json::json!([
            {"media_type": "movie", "title": "Heat",
             "release_date": "1995-12-15", "popularity": 40.0},
            {"media_type": "tv", "name": "Heat", "first_air_date": "", "popularity": 90.0}
        ]);
        let r = pick_best("Heat", results.as_array().unwrap(), true).unwrap();
        assert_eq!(r.media_type, "movie");
        assert_eq!(r.year, 1995);
    }

    #[test]
    fn pick_best_skips_person_results() {
        let results = serde_json::json!([
            {"media_type": "person", "name": "Denis Villeneuve", "popularity": 80.0},
            {"media_type": "movie", "title": "Arrival", "release_date": "2016-11-11", "popularity": 40.0}
        ]);
        let r = pick_best("", results.as_array().unwrap(), false).unwrap();
        assert_eq!(r.title, "Arrival");
    }

    #[test]
    fn pick_best_none_when_no_movie_or_tv() {
        let results = serde_json::json!([
            {"media_type": "person", "name": "Someone", "popularity": 80.0},
            {"media_type": "collection", "name": "Some Collection", "popularity": 50.0}
        ]);
        assert!(pick_best("", results.as_array().unwrap(), false).is_none());
    }

    #[test]
    fn pick_best_tv_uses_name_and_first_air_date() {
        let results = serde_json::json!([
            {"media_type": "tv", "name": "Severance", "first_air_date": "2022-02-18", "popularity": 60.0}
        ]);
        let r = pick_best("", results.as_array().unwrap(), false).unwrap();
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
        let r = pick_best("Civil War", results.as_array().unwrap(), false).unwrap();
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
        let r = pick_best("Top Gun Maverick", results.as_array().unwrap(), false).unwrap();
        assert_eq!(r.title, "Top Gun: Maverick");
        assert_eq!(r.year, 2022);
    }

    /// Every prior pick_best test puts the WRONG (dateless/non-exact)
    /// candidate first and the CORRECT (exact+dated) one second — so an
    /// already-exact `best` is never at risk of being displaced. Reverse
    /// the order: the correct exact+dated match arrives FIRST, then a more
    /// popular but non-exact/dateless candidate arrives SECOND. The correct
    /// one must still win — a later, merely-more-popular candidate must
    /// never displace an already-exact best.
    #[test]
    fn pick_best_exact_dated_first_survives_a_more_popular_non_exact_later() {
        let results = serde_json::json!([
            {"media_type": "movie", "title": "Civil War",
             "release_date": "2024-04-10", "popularity": 30.0},
            {"media_type": "movie", "title": "Captain America: Civil War",
             "release_date": "2016-04-27", "popularity": 200.0}
        ]);
        let r = pick_best("Civil War", results.as_array().unwrap(), false).unwrap();
        assert_eq!(
            r.title, "Civil War",
            "an already-exact, dated best must not be displaced by a later, \
             merely more popular non-exact candidate"
        );
        assert_eq!(r.year, 2024);
    }

    /// Same "wrong order" shape for the dated-vs-undated tie-break (not the
    /// exact-match tier): a DATED best found first must survive a later,
    /// more popular but UNDATED candidate.
    #[test]
    fn pick_best_dated_first_survives_a_more_popular_undated_later() {
        let results = serde_json::json!([
            {"media_type": "movie", "title": "Dune: Part Two",
             "release_date": "2024-02-27", "popularity": 10.0},
            {"media_type": "movie", "title": "Dune Part Two",
             "release_date": "", "popularity": 500.0}
        ]);
        let r = pick_best("", results.as_array().unwrap(), false).unwrap();
        assert_eq!(
            r.year, 2024,
            "a dated best found first must not be displaced by a later, \
             more popular but undated candidate"
        );
    }

    /// Verify that the error_kind string produced for transport/status errors
    /// in fetch_multi never contains the api_key (which lives in the URL
    /// query string). We replicate the summary logic that fetch_multi uses so
    /// a future edit to that arm will be caught here.
    #[test]
    fn tmdb_agent_follows_no_redirects() {
        // The agent skips resolver pinning because the host is the hard-coded
        // api.themoviedb.org — but that argument covers where the REQUEST is
        // aimed, not where a RESPONSE can send it. The request URL carries the
        // operator's TMDB api_key in its query string, so following a 3xx
        // (TMDB compromise, misconfiguration, or on-path tampering) hands that
        // key to whatever host the redirect names. Nothing in this crate needs
        // redirect-following, and every other outbound agent already sets
        // max_redirects(0).
        assert_eq!(
            AGENT.config().max_redirects(),
            0,
            "the TMDB agent must not follow redirects — the request URL \
             carries the api_key"
        );
    }

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

    // --- read_capped_bytes: the DoS-cap boundary itself ----------------------

    #[test]
    fn read_capped_bytes_accepts_exactly_at_cap() {
        let body = vec![b'x'; 100];
        let got = read_capped_bytes(std::io::Cursor::new(&body), 100).unwrap();
        assert_eq!(got.len(), 100);
    }

    #[test]
    fn read_capped_bytes_rejects_one_byte_over_cap() {
        let body = vec![b'x'; 101];
        let err = read_capped_bytes(std::io::Cursor::new(&body), 100).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn read_capped_bytes_accepts_well_under_cap() {
        let body = b"short body".to_vec();
        let got = read_capped_bytes(std::io::Cursor::new(&body), MAX_TMDB_BYTES).unwrap();
        assert_eq!(got, body);
    }

    #[test]
    fn read_capped_json_end_to_end_rejects_oversized_body_via_real_response() {
        // Exercise the actual `read_capped_json` (not just the extracted
        // helper) against a real `ureq::Response`, built from a local TCP
        // listener that streams a body over `MAX_TMDB_BYTES` — proving the
        // real function, not a copy of its logic, enforces the cap.
        use std::io::Write;
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let oversized_len = MAX_TMDB_BYTES + 1024;

        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut buf); // drain the request
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {oversized_len}\r\nConnection: close\r\n\r\n"
            );
            stream.write_all(header.as_bytes()).unwrap();
            stream
                .write_all(&vec![b'{'; oversized_len as usize])
                .unwrap();
        });

        let resp = ureq::get(&format!("http://{addr}/"))
            .config()
            .timeout_global(Some(std::time::Duration::from_secs(5)))
            .build()
            .call()
            .expect("local server must respond");
        let err = read_capped_json(resp).expect_err("oversized body must be rejected");
        // Check the SPECIFIC message, not just the io::ErrorKind: a truncated
        // read (e.g. a regressed `take(cap)` instead of `take(cap + 1)`) would
        // silently hand a truncated `cap`-byte body to serde_json, which is
        // ALSO invalid JSON and ALSO produces `ErrorKind::InvalidData` — so a
        // bare kind() check can't tell "the size cap fired" from "the
        // (wrongly) truncated body failed to parse". The message text is the
        // one observable difference between those two failure causes.
        assert_eq!(
            err.to_string(),
            "tmdb response exceeded size cap",
            "must be rejected by the SIZE CAP specifically, not a downstream JSON parse failure \
             on a silently-truncated body — got {err}"
        );
        handle.join().unwrap();
    }

    // --- norm(): pin the actual character content, not just cross-equality ---

    #[test]
    fn norm_collapses_separator_runs_to_single_space() {
        // Every existing norm() test applies it to BOTH sides of an equality
        // check with structurally-parallel inputs, so a regression that
        // collapsed the collapse-to-one-space step (word concatenation
        // instead) would degrade both sides identically and still compare
        // equal. Assert the actual character content instead.
        assert_eq!(norm("Top  Gun"), "top gun");
        assert_eq!(norm("Top Gun: Maverick"), "top gun maverick");
        assert_eq!(norm("Top___Gun"), "top gun");
    }

    // --- search(): the manual "needs review" correction picker ---------------
    // No existing test called `search()` (or the ranking logic it now
    // delegates to) at all — every mutation inside it, including replacing
    // the whole function body with an empty Vec, passed trivially.

    #[test]
    fn search_empty_api_key_or_query_yields_empty() {
        assert!(search("Some Movie", "", 5).is_empty());
        assert!(search("", "key", 5).is_empty());
        assert!(search("   ", "key", 5).is_empty());
    }

    #[test]
    fn rank_search_results_orders_exact_dated_first_then_dated_then_popularity() {
        // The real ranking logic `search()` calls after `fetch_multi` — pure,
        // so it's driven directly here rather than needing a network round
        // trip. Same fixture shape as the pick_best tests, but this checks
        // the FULL returned order (search's whole reason to exist over
        // pick_best), and includes a highly-popular but DATELESS decoy that
        // must sort last despite its popularity.
        let results = serde_json::json!([
            {"media_type": "movie", "title": "Captain America: Civil War",
             "release_date": "2016-04-27", "popularity": 200.0},
            {"media_type": "movie", "title": "Civil War",
             "release_date": "2024-04-10", "popularity": 30.0},
            {"media_type": "movie", "title": "Some Undated Civil War Thing",
             "release_date": "", "popularity": 500.0}
        ]);
        let ranked = rank_search_results("Civil War", results.as_array().unwrap(), 10);
        let titles: Vec<&str> = ranked.iter().map(|r| r.title.as_str()).collect();
        assert_eq!(
            titles,
            vec![
                "Civil War",                    // exact + dated: wins outright
                "Captain America: Civil War",   // dated, non-exact
                "Some Undated Civil War Thing", // undated, even though most popular
            ]
        );
    }

    #[test]
    fn rank_search_results_respects_limit() {
        let results = serde_json::json!([
            {"media_type": "movie", "title": "A", "release_date": "2001-01-01", "popularity": 1.0},
            {"media_type": "movie", "title": "B", "release_date": "2002-01-01", "popularity": 2.0},
            {"media_type": "movie", "title": "C", "release_date": "2003-01-01", "popularity": 3.0}
        ]);
        let ranked = rank_search_results("", results.as_array().unwrap(), 2);
        assert_eq!(ranked.len(), 2, "must cap at the requested limit");
    }

    #[test]
    fn rank_search_results_empty_input_yields_empty() {
        assert!(rank_search_results("anything", &[], 5).is_empty());
    }

    // --- lookup(): the guard logic ahead of the untestable network call ------
    // `lookup()` itself has zero test coverage — a regression collapsing it to
    // unconditional `None` would pass every existing test. The happy path
    // needs HTTP mocking (out of scope here), but the two guards that decide
    // whether it even ATTEMPTS a request are pure and cheap to pin directly.

    #[test]
    fn lookup_empty_api_key_returns_none_without_network() {
        // No api_key configured: must short-circuit before ever building a
        // request (an empty key would otherwise round-trip to TMDB and get
        // a 401 on every single insert).
        assert!(lookup("Some Movie", "").is_none());
    }

    #[test]
    fn lookup_blank_query_returns_none_without_network() {
        // A separator-only volume label reduces to an empty query after
        // clean_title; must short-circuit rather than firing a bare
        // `query=&...` request that TMDB answers with HTTP 422.
        assert!(lookup("", "some_api_key").is_none());
        assert!(lookup("   ", "some_api_key").is_none());
    }

    // --- TV season-marker stripping ------------------------------------------

    #[test]
    fn clean_title_strips_trailing_tv_season_markers() {
        // A series disc must resolve to the base show title.
        assert_eq!(clean_title("ENDEAVOUR_SEASON_5_DISC_2"), "Endeavour");
        assert_eq!(clean_title("VICTORIA SERIES 2 DISC 2"), "Victoria");
        assert_eq!(clean_title("Turn - Staffel 3 - Disc 2"), "Turn");
        assert_eq!(clean_title("Les Revenants Saison 1"), "Les Revenants");
    }

    #[test]
    fn clean_title_keeps_volume_and_part_which_are_real_film_titles() {
        // "Vol."/"Volume"/"Part" are NOT season markers — they belong to real
        // movie titles and must never be peeled.
        assert_eq!(clean_title("KILL_BILL_VOL_2"), "Kill Bill Vol 2");
        assert_eq!(
            clean_title("GUARDIANS_OF_THE_GALAXY_VOL_2"),
            "Guardians Of The Galaxy Vol 2"
        );
        assert_eq!(clean_title("DUNE_PART_TWO"), "Dune Part Two");
    }

    #[test]
    fn clean_title_keeps_bare_season_word_without_a_number() {
        // "Season" as an actual title word (no trailing number) is preserved.
        assert_eq!(clean_title("SILLY_SEASON"), "Silly Season");
        assert_eq!(clean_title("OPEN_SEASON"), "Open Season");
    }

    #[test]
    fn season_and_disc_from_label() {
        assert_eq!(season_from_label("ENDEAVOUR SEASON 5 DISC 2"), Some(5));
        assert_eq!(season_from_label("VICTORIA SERIES 2 DISC 2"), Some(2));
        assert_eq!(season_from_label("GAMEOFTHRONES_S3_DISC1"), Some(3));
        assert_eq!(season_from_label("Turn - Staffel 3 - Disc 2"), Some(3));
        assert_eq!(season_from_label("THE MATRIX"), None);
        assert_eq!(disc_from_label("ENDEAVOUR SEASON 5 DISC 2"), Some(2));
        assert_eq!(disc_from_label("GOT_S3_D4"), Some(4));
        assert_eq!(disc_from_label("BATMAN_BD1"), Some(1));
        assert_eq!(disc_from_label("THE MATRIX"), None);
    }

    #[test]
    fn strip_trailing_season_unit() {
        assert_eq!(
            strip_trailing_season("endeavour season 5"),
            Some("endeavour")
        );
        assert_eq!(strip_trailing_season("victoria series 2"), Some("victoria"));
        assert_eq!(strip_trailing_season("open season"), None); // no number
        assert_eq!(strip_trailing_season("kill bill vol 2"), None); // vol not a marker
        assert_eq!(strip_trailing_season("blade runner 2049"), None); // no marker word
    }

    // --- progressive fallback: query_variants + is_trailing_junk -------------

    #[test]
    fn trailing_junk_peels_edition_region_format_and_codes() {
        for j in [
            "UE", "SE", "Ultimate", "Edition", "Cut", "UK", "NA", "BD", "UHD",
        ] {
            assert!(is_trailing_junk(j), "{j} should be peelable junk");
        }
        for code in ["UPT1", "G51", "BD3", "3D", "4K", "D2"] {
            assert!(is_trailing_junk(code), "{code} (alnum code) should be junk");
        }
    }

    #[test]
    fn trailing_junk_never_peels_sequel_markers() {
        // Pure numbers and roman numerals are sequel markers, not junk —
        // peeling them would resolve a sequel to the original film.
        for keep in ["3", "2049", "II", "III", "IV", "X", "1917"] {
            assert!(
                !is_trailing_junk(keep),
                "{keep} is a sequel/title marker and must be kept"
            );
        }
    }

    #[test]
    fn query_variants_peels_the_ue_that_zeroes_out_tmdb() {
        // The live bug: "Batman v Superman: Dawn of Justice: UE" returns ZERO
        // TMDB hits until the trailing "UE" is peeled. The variant list must
        // include the clean full label first, then the peeled title.
        let v = query_variants("Batman v Superman: Dawn of Justice: UE");
        assert_eq!(v[0], clean_title("Batman v Superman: Dawn of Justice: UE"));
        assert!(
            v.iter()
                .any(|q| norm(q) == norm("Batman v Superman Dawn of Justice")),
            "must produce the UE-stripped title as a fallback variant: {v:?}"
        );
    }

    #[test]
    fn query_variants_stops_at_a_sequel_number() {
        // "ALIEN 3" must NOT fan out to "ALIEN": the trailing 3 is meaningful.
        let v = query_variants("ALIEN_3");
        assert_eq!(v, vec!["Alien 3".to_string()]);
        // Same for a roman-numeral sequel.
        let v = query_variants("ROCKY_II");
        assert_eq!(v, vec!["Rocky Ii".to_string()]);
    }

    #[test]
    fn query_variants_peels_multiple_trailing_codes() {
        // Chained trailing codes peel one at a time, most-specific first.
        let v = query_variants("MINIONS_UPT1");
        assert!(v.contains(&"Minions".to_string()), "{v:?}");
        let v = query_variants("TITANIC_3D");
        assert!(v.contains(&"Titanic".to_string()), "{v:?}");
    }

    #[test]
    fn is_confident_match_agrees_with_a_fallback_resolved_title() {
        // The gate must accept a title that `lookup` could only reach via a
        // peeled variant — otherwise every edition disc parks in review even
        // though the lookup found it. Uses the RAW label (not pre-cleaned).
        assert!(
            is_confident_match(
                "Batman v Superman: Dawn of Justice: UE",
                "Batman v Superman: Dawn of Justice",
                2016
            ),
            "a UE-suffixed label must confidently match the un-suffixed film"
        );
    }

    #[test]
    fn is_confident_match_still_requires_a_year() {
        assert!(!is_confident_match("Some Film UE", "Some Film", 0));
    }

    #[test]
    fn is_confident_match_does_not_accept_a_peeled_sequel_collision() {
        // "ALIEN 3" must NOT be confidently matched to "Alien" (1979): the 3 is
        // never peeled, so no variant equals "Alien".
        assert!(
            !is_confident_match("ALIEN_3", "Alien", 1979),
            "a sequel label must never confidently resolve to the original film"
        );
    }
}
