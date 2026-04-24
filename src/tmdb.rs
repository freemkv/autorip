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
    let first = results.first()?;

    let media_type = first["media_type"].as_str().unwrap_or("movie");
    let title = first
        .get(if media_type == "tv" { "name" } else { "title" })?
        .as_str()?
        .to_string();
    let date = first
        .get(if media_type == "tv" {
            "first_air_date"
        } else {
            "release_date"
        })?
        .as_str()
        .unwrap_or("");
    let year: u16 = date.get(..4).and_then(|y| y.parse().ok()).unwrap_or(0);
    let poster = first["poster_path"]
        .as_str()
        .map(|p| format!("https://image.tmdb.org/t/p/w300{}", p))
        .unwrap_or_default();
    let overview = first["overview"].as_str().unwrap_or("").to_string();

    Some(TmdbResult {
        title,
        year,
        poster_url: poster,
        overview,
        media_type: media_type.to_string(),
    })
}

/// Clean a disc label for TMDB search: "DUNE_PART_TWO" -> "Dune Part Two"
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
        assert_eq!(clean_title("DUNE_PART_TWO"), "Dune Part Two");
        assert_eq!(clean_title("V_FOR_VENDETTA"), "V For Vendetta");
    }

    #[test]
    fn clean_title_strips_uhd_suffix() {
        assert_eq!(clean_title("DUNE_PART_TWO_4K_UHD"), "Dune Part Two");
        assert_eq!(clean_title("DUNE_PART_TWO_4K_ULTRA_HD"), "Dune Part Two");
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
}
