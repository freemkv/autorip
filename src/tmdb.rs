#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TmdbResult {
    pub title: String,
    pub year: u16,
    pub poster_url: String,
    pub overview: String,
    pub media_type: String, // "movie" or "tv"
}

pub fn lookup(query: &str, api_key: &str) -> Option<TmdbResult> {
    if api_key.is_empty() {
        return None;
    }

    let url = format!(
        "https://api.themoviedb.org/3/search/multi?api_key={}&query={}&page=1",
        api_key,
        urlencoded(query)
    );

    let resp: serde_json::Value = ureq::get(&url).call().ok()?.into_json().ok()?;
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
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            sep = false;
        } else if !sep {
            out.push(' ');
            sep = true;
        }
    }
    out.trim_end().to_string()
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
    let media_type = v["media_type"].as_str().unwrap_or("movie");
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
    let poster = v["poster_path"]
        .as_str()
        .map(|p| format!("https://image.tmdb.org/t/p/w300{}", p))
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
    ];
    let lower = s.to_lowercase();
    let mut end = s.len();
    for suffix in &suffixes {
        if let Some(pos) = lower.find(suffix) {
            if pos < end {
                end = pos;
            }
        }
    }
    let trimmed = s[..end].trim();

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
    fn clean_title_picks_earliest_suffix_match() {
        // Multiple suffix candidates — must cut at the earliest one so we
        // don't leave suffix fragments in the cleaned title.
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
    fn urlencoded_keeps_allowed_chars() {
        assert_eq!(urlencoded("hello"), "hello");
        assert_eq!(urlencoded("hello world"), "hello+world");
        assert_eq!(urlencoded("name=value"), "name%3Dvalue");
        assert_eq!(urlencoded("a-b_c.d"), "a-b_c.d");
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
}
